package service

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"order-summary-service/internal/cache"
	"order-summary-service/internal/date"
	"order-summary-service/internal/models"
	"order-summary-service/internal/repository"
)

type ConsumerService interface {
	Run(ctx context.Context) error
	Close() error
}

type consumerService struct {
	consumer *kafka.Consumer
	repo     repository.Repository
	rdb      cache.Cache
	cache    cache.Cache
	ch       Closer

	batchSize         int
	batchInterval     time.Duration
	redisTimeout      time.Duration
	clickhouseTimeout time.Duration
	idempotencyTTL    time.Duration
	pendingTTL        time.Duration

	customerBatch []customerBatchItem
	orderBatch    []orderBatchItem
	nextFlush     time.Time
}

// ConsumerConfig holds only what the service needs at runtime.
type ConsumerConfig struct {
	KafkaCustomerTopic string
	KafkaOrderTopic    string
	BatchSize          int
	BatchInterval      time.Duration
	RedisTimeout       time.Duration
	ClickHouseTimeout  time.Duration
	IdempotencyTTL     time.Duration
	PendingTTL         time.Duration
}

type customerBatchItem struct {
	params  repository.CreateCustomerParams
	message *kafka.Message
}

type orderBatchItem struct {
	params  repository.CreateOrderParams
	message *kafka.Message
}

type batchKind int

const (
	batchUnknown batchKind = iota
	batchCustomer
	batchOrder
	batchDuplicate
)

type batchResult struct {
	kind           batchKind
	customerParams repository.CreateCustomerParams
	orderParams    repository.CreateOrderParams
}

func NewConsumerService(
	consumer *kafka.Consumer,
	repo repository.Repository,
	rdb cache.Cache,
	ch Closer,
	cacheClient cache.Cache,
	cfg ConsumerConfig,
) (ConsumerService, error) {
	topics := filterTopics([]string{cfg.KafkaCustomerTopic, cfg.KafkaOrderTopic})
	if len(topics) == 0 {
		return nil, ErrNoTopics
	}
	if err := consumer.SubscribeTopics(topics, nil); err != nil {
		return nil, err
	}

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}
	pendingTTL := cfg.PendingTTL
	if pendingTTL <= 0 {
		pendingTTL = 30 * time.Second
	}

	return &consumerService{
		consumer:          consumer,
		repo:              repo,
		rdb:               rdb,
		cache:             cacheClient,
		ch:                ch,
		batchSize:         batchSize,
		batchInterval:     cfg.BatchInterval,
		redisTimeout:      cfg.RedisTimeout,
		clickhouseTimeout: cfg.ClickHouseTimeout,
		idempotencyTTL:    cfg.IdempotencyTTL,
		pendingTTL:        pendingTTL,
		customerBatch:     make([]customerBatchItem, 0, batchSize),
		orderBatch:        make([]orderBatchItem, 0, batchSize),
		nextFlush:         time.Now().Add(cfg.BatchInterval),
	}, nil
}

func (s *consumerService) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			s.customerBatch = s.flushCustomerBatch(ctx, s.customerBatch)
			s.orderBatch = s.flushOrderBatch(ctx, s.orderBatch)
			return ctx.Err()
		default:
		}

		event := s.consumer.Poll(250)
		now := time.Now()
		if now.After(s.nextFlush) {
			s.customerBatch = s.flushCustomerBatch(ctx, s.customerBatch)
			s.orderBatch = s.flushOrderBatch(ctx, s.orderBatch)
			s.nextFlush = now.Add(s.batchInterval)
		}

		if event == nil {
			continue
		}

		switch e := event.(type) {
		case *kafka.Message:
			// log.Printf("message received topic=%s partition=%d offset=%d key=%s bytes=%d", *e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset, string(e.Key), len(e.Value))
			batchType, err := s.processMessage(ctx, e.Value)
			if err != nil {
				log.Printf("message processing error: %v", err)
				continue
			}
			switch batchType.kind {
			case batchCustomer:
				s.customerBatch = append(s.customerBatch, customerBatchItem{
					params:  batchType.customerParams,
					message: e,
				})
				if len(s.customerBatch) >= s.batchSize {
					s.customerBatch = s.flushCustomerBatch(ctx, s.customerBatch)
				}
			case batchOrder:
				s.orderBatch = append(s.orderBatch, orderBatchItem{
					params:  batchType.orderParams,
					message: e,
				})
				if len(s.orderBatch) >= s.batchSize {
					s.orderBatch = s.flushOrderBatch(ctx, s.orderBatch)
				}
			case batchDuplicate:
				if _, err := s.consumer.CommitMessage(e); err != nil {
					log.Printf("commit error topic=%s err=%v", *e.TopicPartition.Topic, err)
				} else {
					log.Printf("committed topic=%s partition=%d offset=%d", *e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset)
				}
			}
		case kafka.Error:
			if e.Code() == kafka.ErrPartitionEOF {
				log.Printf("partition EOF: %v", e)
				continue
			}
			log.Printf("kafka error: %v", e)
		default:
			log.Printf("kafka event: %v", e)
		}
	}
}

func (s *consumerService) Close() error {
	if s.consumer != nil {
		_ = s.consumer.Close()
	}
	if s.rdb != nil {
		_ = s.rdb.Close()
	}
	if s.ch != nil {
		_ = s.ch.Close()
	}
	return nil
}

func (s *consumerService) processMessage(ctx context.Context, payload []byte) (batchResult, error) {
	eventType, err := models.DetectEventType(payload)
	if err != nil {
		return batchResult{kind: batchUnknown}, err
	}

	// log.Printf("event type=%s", eventType)
	switch eventType {
	case models.EventTypeCustomerCreated:
		evt, err := models.ParseCustomerEvent(payload)
		if err != nil {
			return batchResult{kind: batchUnknown}, err
		}
		if duplicate, err := s.checkIdempotency(ctx, evt.EventID); err != nil {
			return batchResult{kind: batchUnknown}, err
		} else if duplicate {
			log.Printf("duplicate customer event_id=%s customer_id=%s", evt.EventID, evt.CustomerID)
			return batchResult{kind: batchDuplicate}, nil
		}
		return batchResult{
			kind: batchCustomer,
			customerParams: repository.CreateCustomerParams{
				CustomerID:    evt.CustomerID,
				CreatedAt:     evt.EventTime,
				UpdatedAt:     evt.EventTime,
				SourceEventID: evt.EventID,
			},
		}, nil
	case models.EventTypeOrderCreated:
		evt, err := models.ParseOrderEvent(payload)
		if err != nil {
			return batchResult{kind: batchUnknown}, err
		}
		if duplicate, err := s.checkIdempotency(ctx, evt.EventID); err != nil {
			return batchResult{kind: batchUnknown}, err
		} else if duplicate {
			log.Printf("duplicate order event_id=%s customer_id=%s", evt.EventID, evt.CustomerID)
			return batchResult{kind: batchDuplicate}, nil
		}
		return batchResult{
			kind: batchOrder,
			orderParams: repository.CreateOrderParams{
				OrderID:       evt.OrderID,
				CustomerID:    evt.CustomerID,
				OrderTime:     evt.EventTime,
				TotalAmount:   evt.TotalAmount,
				Currency:      evt.Currency,
				SourceEventID: evt.EventID,
			},
		}, nil
	default:
		return batchResult{kind: batchUnknown}, nil
	}
}

func (s *consumerService) checkIdempotency(ctx context.Context, eventID string) (bool, error) {
	redisCtx, redisCancel := context.WithTimeout(ctx, s.redisTimeout)
	marked, err := s.rdb.CheckAndMarkEvent(redisCtx, eventID, s.idempotencyTTL)
	redisCancel()
	if err != nil {
		return false, err
	}
	return !marked, nil
}

func (s *consumerService) flushCustomerBatch(ctx context.Context, batch []customerBatchItem) []customerBatchItem {
	if len(batch) == 0 {
		return batch
	}

	params := make([]repository.CreateCustomerParams, 0, len(batch))
	for _, item := range batch {
		params = append(params, item.params)
	}

	chCtx, chCancel := context.WithTimeout(ctx, s.clickhouseTimeout)
	err := s.repo.CreateCustomerBatch(chCtx, params)
	chCancel()
	if err != nil {
		log.Printf("clickhouse insert customer batch error count=%d err=%v", len(params), err)
		s.unmarkCustomerBatch(context.Background(), params)
		return batch
	}

	for _, item := range batch {
		if _, err := s.consumer.CommitMessage(item.message); err != nil {
			log.Printf("commit error topic=%s err=%v", *item.message.TopicPartition.Topic, err)
		}
	}

	log.Printf("customer batch committed count=%d", len(batch))
	return batch[:0]
}

func (s *consumerService) flushOrderBatch(ctx context.Context, batch []orderBatchItem) []orderBatchItem {
	if len(batch) == 0 {
		return batch
	}

	params := make([]repository.CreateOrderParams, 0, len(batch))
	for _, item := range batch {
		params = append(params, item.params)
	}

	chCtx, chCancel := context.WithTimeout(ctx, s.clickhouseTimeout)
	err := s.repo.CreateOrderBatch(chCtx, params)
	chCancel()
	if err != nil {
		log.Printf("clickhouse insert order batch error count=%d err=%v", len(params), err)
		s.unmarkOrderBatch(context.Background(), params)
		return batch
	}

	for _, item := range batch {
		if _, err := s.consumer.CommitMessage(item.message); err != nil {
			log.Printf("commit error topic=%s err=%v", *item.message.TopicPartition.Topic, err)
		}
	}

	s.invalidateOrderBatchCache(params)

	log.Printf("order batch committed count=%d", len(batch))
	return batch[:0]
}

func (s *consumerService) unmarkCustomerBatch(ctx context.Context, params []repository.CreateCustomerParams) {
	for _, item := range params {
		unmarkCtx, unmarkCancel := context.WithTimeout(ctx, s.redisTimeout)
		if err := s.rdb.UnmarkEvent(unmarkCtx, item.SourceEventID); err != nil {
			log.Printf("idempotency unmark error event_id=%s err=%v", item.SourceEventID, err)
		}
		unmarkCancel()
	}
}

func (s *consumerService) unmarkOrderBatch(ctx context.Context, params []repository.CreateOrderParams) {
	for _, item := range params {
		unmarkCtx, unmarkCancel := context.WithTimeout(ctx, s.redisTimeout)
		if err := s.rdb.UnmarkEvent(unmarkCtx, item.SourceEventID); err != nil {
			log.Printf("idempotency unmark error event_id=%s err=%v", item.SourceEventID, err)
		}
		unmarkCancel()
	}
}

func (s *consumerService) invalidateOrderBatchCache(params []repository.CreateOrderParams) {
	if s.cache == nil {
		return
	}
	cacheDate := date.ToStartOfDay(time.Now().UTC()).Format("2006-01-02")
	seen := make(map[string]struct{}, len(params))
	for _, item := range params {
		seen[item.CustomerID] = struct{}{}
	}
	for customerID := range seen {
		ctx, cancel := context.WithTimeout(context.Background(), s.redisTimeout)
		if err := s.cache.InvalidateMonthly(ctx, customerID, cacheDate); err != nil {
			log.Printf("cache invalidate error customer_id=%s err=%v", customerID, err)
		}
		cancel()

		hot, err := s.cache.IsMonthlyHot(context.Background(), customerID, cacheDate)
		if err != nil {
			log.Printf("hot check error customer_id=%s err=%v", customerID, err)
			continue
		}
		if !hot {
			continue
		}
		pending, err := s.cache.TrySetPending(context.Background(), customerID, cacheDate, s.pendingTTL)
		if err != nil {
			log.Printf("pending set error customer_id=%s err=%v", customerID, err)
			continue
		}
		if pending {
			job := cache.RefreshJob{CustomerID: customerID, AsOfDate: cacheDate}
			if err := s.cache.EnqueueRefreshJob(context.Background(), job); err != nil {
				log.Printf("refresh enqueue error customer_id=%s err=%v", customerID, err)
			}
		}
	}
}

func filterTopics(topics []string) []string {
	var out []string
	for _, topic := range topics {
		trimmed := strings.TrimSpace(topic)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

var ErrNoTopics = kafka.NewError(kafka.ErrUnknownTopicOrPart, "no topics configured", false)

// Closer matches clickhouse.Conn.
type Closer interface {
	Close() error
}
