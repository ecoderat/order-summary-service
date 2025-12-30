package main

import (
	"context"
	"log"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"order-summary-service/internal/config"
	"order-summary-service/internal/db"
	"order-summary-service/internal/models"
	"order-summary-service/internal/redisclient"
	"order-summary-service/internal/repository"
)

type customerBatchItem struct {
	params  repository.CreateCustomerParams
	message *kafka.Message
}

type orderBatchItem struct {
	params  repository.CreateOrderParams
	message *kafka.Message
}

func main() {
	cfg := config.Load("kafka-consumer-service")
	log.Printf("starting %s", cfg.ServiceName)
	log.Printf("kafka brokers=%s group_id=%s topics=%s,%s", cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.KafkaCustomerTopic, cfg.KafkaOrderTopic)
	log.Printf("redis addr=%s db=%d", cfg.RedisAddr, cfg.RedisDB)
	log.Printf("clickhouse addr=%s db=%s user=%s protocol=%s", cfg.ClickHouseAddr, cfg.ClickHouseDB, cfg.ClickHouseUser, cfg.ClickHouseProtocol)
	log.Printf("batch size=%d batch interval=%s", cfg.ConsumerBatchSize, cfg.ConsumerBatchInterval)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ch, err := db.New(ctx, &cfg)
	if err != nil {
		log.Fatalf("clickhouse connect error: %v", err)
	}
	defer func() {
		if err := ch.Close(); err != nil {
			log.Printf("clickhouse close error: %v", err)
		}
	}()

	repo := repository.NewRepository(ch)

	rdb := redisclient.New(cfg.RedisAddr, cfg.RedisDB)
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Printf("redis close error: %v", err)
		}
	}()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        cfg.KafkaBrokers,
		"group.id":                 cfg.KafkaGroupID,
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       false,
		"enable.auto.offset.store": false,
	})
	if err != nil {
		log.Fatalf("kafka consumer error: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("kafka consumer close error: %v", err)
		}
	}()

	topics := filterTopics([]string{cfg.KafkaCustomerTopic, cfg.KafkaOrderTopic})
	if len(topics) == 0 {
		log.Fatalf("no topics configured")
	}
	if err := consumer.SubscribeTopics(topics, nil); err != nil {
		log.Fatalf("kafka subscribe error: %v", err)
	}

	customerBatch := make([]customerBatchItem, 0, cfg.ConsumerBatchSize)
	orderBatch := make([]orderBatchItem, 0, cfg.ConsumerBatchSize)
	nextFlush := time.Now().Add(cfg.ConsumerBatchInterval)

	for {
		select {
		case <-ctx.Done():
			log.Printf("shutting down %s", cfg.ServiceName)
			customerBatch = flushCustomerBatch(ctx, cfg, rdb, repo, consumer, customerBatch)
			orderBatch = flushOrderBatch(ctx, cfg, rdb, repo, consumer, orderBatch)
			return
		default:
		}

		event := consumer.Poll(250)
		now := time.Now()
		if now.After(nextFlush) {
			customerBatch = flushCustomerBatch(ctx, cfg, rdb, repo, consumer, customerBatch)
			orderBatch = flushOrderBatch(ctx, cfg, rdb, repo, consumer, orderBatch)
			nextFlush = now.Add(cfg.ConsumerBatchInterval)
		}

		if event == nil {
			continue
		}

		switch e := event.(type) {
		case *kafka.Message:
			// log.Printf("message received topic=%s partition=%d offset=%d key=%s bytes=%d", *e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset, string(e.Key), len(e.Value))
			batchType, err := processMessage(ctx, cfg, rdb, e.Value)
			if err != nil {
				log.Printf("message processing error: %v", err)
				continue
			}
			switch batchType.kind {
			case batchCustomer:
				customerBatch = append(customerBatch, customerBatchItem{
					params:  batchType.customerParams,
					message: e,
				})
				if len(customerBatch) >= cfg.ConsumerBatchSize {
					customerBatch = flushCustomerBatch(ctx, cfg, rdb, repo, consumer, customerBatch)
				}
			case batchOrder:
				orderBatch = append(orderBatch, orderBatchItem{
					params:  batchType.orderParams,
					message: e,
				})
				if len(orderBatch) >= cfg.ConsumerBatchSize {
					orderBatch = flushOrderBatch(ctx, cfg, rdb, repo, consumer, orderBatch)
				}
			case batchDuplicate:
				if _, err := consumer.CommitMessage(e); err != nil {
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

func processMessage(ctx context.Context, cfg config.Config, rdb *redisclient.Client, payload []byte) (batchResult, error) {
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
		if duplicate, err := checkIdempotency(ctx, cfg, rdb, evt.EventID); err != nil {
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
		if duplicate, err := checkIdempotency(ctx, cfg, rdb, evt.EventID); err != nil {
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

func checkIdempotency(ctx context.Context, cfg config.Config, rdb *redisclient.Client, eventID string) (bool, error) {
	redisCtx, redisCancel := context.WithTimeout(ctx, cfg.RedisTimeout)
	marked, err := rdb.CheckAndMarkEvent(redisCtx, eventID, cfg.IdempotencyTTL)
	redisCancel()
	if err != nil {
		return false, err
	}
	return !marked, nil
}

func flushCustomerBatch(ctx context.Context, cfg config.Config, rdb *redisclient.Client, repo repository.Repository, consumer *kafka.Consumer, batch []customerBatchItem) []customerBatchItem {
	if len(batch) == 0 {
		return batch
	}

	params := make([]repository.CreateCustomerParams, 0, len(batch))
	for _, item := range batch {
		params = append(params, item.params)
	}

	chCtx, chCancel := context.WithTimeout(ctx, cfg.ClickHouseTimeout)
	err := repo.CreateCustomerBatch(chCtx, params)
	chCancel()
	if err != nil {
		log.Printf("clickhouse insert customer batch error count=%d err=%v", len(params), err)
		unmarkBatch(context.Background(), cfg, rdb, params)
		return batch
	}

	for _, item := range batch {
		if _, err := consumer.CommitMessage(item.message); err != nil {
			log.Printf("commit error topic=%s err=%v", *item.message.TopicPartition.Topic, err)
		}
	}

	log.Printf("customer batch committed count=%d", len(batch))
	return batch[:0]
}

func flushOrderBatch(ctx context.Context, cfg config.Config, rdb *redisclient.Client, repo repository.Repository, consumer *kafka.Consumer, batch []orderBatchItem) []orderBatchItem {
	if len(batch) == 0 {
		return batch
	}

	params := make([]repository.CreateOrderParams, 0, len(batch))
	for _, item := range batch {
		params = append(params, item.params)
	}

	chCtx, chCancel := context.WithTimeout(ctx, cfg.ClickHouseTimeout)
	err := repo.CreateOrderBatch(chCtx, params)
	chCancel()
	if err != nil {
		log.Printf("clickhouse insert order batch error count=%d err=%v", len(params), err)
		unmarkOrderBatch(context.Background(), cfg, rdb, params)
		return batch
	}

	for _, item := range batch {
		if _, err := consumer.CommitMessage(item.message); err != nil {
			log.Printf("commit error topic=%s err=%v", *item.message.TopicPartition.Topic, err)
		}
	}

	log.Printf("order batch committed count=%d", len(batch))
	return batch[:0]
}

func unmarkBatch(ctx context.Context, cfg config.Config, rdb *redisclient.Client, params []repository.CreateCustomerParams) {
	for _, item := range params {
		unmarkCtx, unmarkCancel := context.WithTimeout(ctx, cfg.RedisTimeout)
		if err := rdb.UnmarkEvent(unmarkCtx, item.SourceEventID); err != nil {
			log.Printf("idempotency unmark error event_id=%s err=%v", item.SourceEventID, err)
		}
		unmarkCancel()
	}
}

func unmarkOrderBatch(ctx context.Context, cfg config.Config, rdb *redisclient.Client, params []repository.CreateOrderParams) {
	for _, item := range params {
		unmarkCtx, unmarkCancel := context.WithTimeout(ctx, cfg.RedisTimeout)
		if err := rdb.UnmarkEvent(unmarkCtx, item.SourceEventID); err != nil {
			log.Printf("idempotency unmark error event_id=%s err=%v", item.SourceEventID, err)
		}
		unmarkCancel()
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
