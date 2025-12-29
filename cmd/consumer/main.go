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

func main() {
	cfg := config.Load("kafka-consumer-service")
	log.Printf("starting %s", cfg.ServiceName)
	log.Printf("kafka brokers=%s group_id=%s topics=%s,%s", cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.KafkaCustomerTopic, cfg.KafkaOrderTopic)
	log.Printf("redis addr=%s db=%d", cfg.RedisAddr, cfg.RedisDB)
	log.Printf("clickhouse addr=%s db=%s user=%s protocol=%s", cfg.ClickHouseAddr, cfg.ClickHouseDB, cfg.ClickHouseUser, cfg.ClickHouseProtocol)

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

	for {
		select {
		case <-ctx.Done():
			log.Printf("shutting down %s", cfg.ServiceName)
			return
		default:
		}

		event := consumer.Poll(250)
		if event == nil {
			continue
		}

		switch e := event.(type) {
		case *kafka.Message:
			log.Printf("message received topic=%s partition=%d offset=%d key=%s bytes=%d", *e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset, string(e.Key), len(e.Value))
			shouldCommit := handleMessage(ctx, cfg, rdb, repo, e.Value)
			if !shouldCommit {
				log.Printf("message not committed topic=%s partition=%d offset=%d", *e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset)
				continue
			}
			if _, err := consumer.CommitMessage(e); err != nil {
				log.Printf("commit error topic=%s err=%v", *e.TopicPartition.Topic, err)
			} else {
				log.Printf("committed topic=%s partition=%d offset=%d", *e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset)
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

func handleMessage(ctx context.Context, cfg config.Config, rdb *redisclient.Client, repo repository.Repository, payload []byte) bool {
	eventType, err := models.DetectEventType(payload)
	if err != nil {
		log.Printf("detect event type error: %v", err)
		return false
	}

	log.Printf("event type=%s", eventType)
	switch eventType {
	case models.EventTypeCustomerCreated:
		evt, err := models.ParseCustomerEvent(payload)
		if err != nil {
			log.Printf("parse customer event error: %v", err)
			return false
		}
		return handleCustomerEvent(ctx, cfg, rdb, repo, evt)
	case models.EventTypeOrderCreated:
		evt, err := models.ParseOrderEvent(payload)
		if err != nil {
			log.Printf("parse order event error: %v", err)
			return false
		}
		return handleOrderEvent(ctx, cfg, rdb, repo, evt)
	default:
		log.Printf("unknown event_type=%s", eventType)
		return false
	}
}

func handleCustomerEvent(ctx context.Context, cfg config.Config, rdb *redisclient.Client, repo repository.Repository, evt models.CustomerEvent) bool {
	log.Printf("customer processing event_id=%s customer_id=%s", evt.EventID, evt.CustomerID)
	redisCtx, redisCancel := context.WithTimeout(ctx, cfg.RedisTimeout)
	marked, err := rdb.CheckAndMarkEvent(redisCtx, evt.EventID, cfg.IdempotencyTTL)
	redisCancel()
	if err != nil {
		log.Printf("idempotency check error event_id=%s err=%v", evt.EventID, err)
		return false
	}
	if !marked {
		log.Printf("duplicate customer event_id=%s customer_id=%s", evt.EventID, evt.CustomerID)
		return true
	}

	chCtx, chCancel := context.WithTimeout(ctx, cfg.ClickHouseTimeout)
	err = repo.CreateCustomer(chCtx, repository.CreateCustomerParams{
		CustomerID:    evt.CustomerID,
		CreatedAt:     evt.EventTime,
		UpdatedAt:     evt.EventTime,
		SourceEventID: evt.EventID,
	})
	chCancel()
	if err != nil {
		log.Printf("clickhouse insert customer error event_id=%s err=%v", evt.EventID, err)
		unmarkCtx, unmarkCancel := context.WithTimeout(context.Background(), cfg.RedisTimeout)
		if err := rdb.UnmarkEvent(unmarkCtx, evt.EventID); err != nil {
			log.Printf("idempotency unmark error event_id=%s err=%v", evt.EventID, err)
		}
		unmarkCancel()
		return false
	}

	log.Printf("customer ingested event_id=%s customer_id=%s event_time=%s", evt.EventID, evt.CustomerID, evt.EventTime.Format(time.RFC3339Nano))
	return true
}

func handleOrderEvent(ctx context.Context, cfg config.Config, rdb *redisclient.Client, repo repository.Repository, evt models.OrderEvent) bool {
	log.Printf("order processing event_id=%s customer_id=%s order_id=%s", evt.EventID, evt.CustomerID, evt.OrderID)
	redisCtx, redisCancel := context.WithTimeout(ctx, cfg.RedisTimeout)
	marked, err := rdb.CheckAndMarkEvent(redisCtx, evt.EventID, cfg.IdempotencyTTL)
	redisCancel()
	if err != nil {
		log.Printf("idempotency check error event_id=%s err=%v", evt.EventID, err)
		return false
	}
	if !marked {
		log.Printf("duplicate order event_id=%s customer_id=%s", evt.EventID, evt.CustomerID)
		return true
	}

	chCtx, chCancel := context.WithTimeout(ctx, cfg.ClickHouseTimeout)
	err = repo.CreateOrder(chCtx, repository.CreateOrderParams{
		OrderID:       evt.OrderID,
		CustomerID:    evt.CustomerID,
		OrderTime:     evt.EventTime,
		TotalAmount:   evt.TotalAmount,
		Currency:      evt.Currency,
		SourceEventID: evt.EventID,
	})
	chCancel()
	if err != nil {
		log.Printf("clickhouse insert order error event_id=%s err=%v", evt.EventID, err)
		unmarkCtx, unmarkCancel := context.WithTimeout(context.Background(), cfg.RedisTimeout)
		if err := rdb.UnmarkEvent(unmarkCtx, evt.EventID); err != nil {
			log.Printf("idempotency unmark error event_id=%s err=%v", evt.EventID, err)
		}
		unmarkCancel()
		return false
	}

	log.Printf("order ingested event_id=%s customer_id=%s order_id=%s event_time=%s", evt.EventID, evt.CustomerID, evt.OrderID, evt.EventTime.Format(time.RFC3339Nano))
	return true
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
