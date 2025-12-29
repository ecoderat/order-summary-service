package main

import (
	"context"
	"log"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"order-summary-service/internal/clickhouseclient"
	"order-summary-service/internal/config"
	"order-summary-service/internal/models"
	"order-summary-service/internal/redisclient"
)

func main() {
	cfg := config.Load("kafka-consumer-service")
	log.Printf("starting %s", cfg.ServiceName)
	log.Printf("kafka brokers=%s group_id=%s topics=%s,%s", cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.KafkaCustomerTopic, cfg.KafkaOrderTopic)
	log.Printf("redis addr=%s db=%d", cfg.RedisAddr, cfg.RedisDB)
	log.Printf("clickhouse addr=%s db=%s user=%s protocol=%s", cfg.ClickHouseAddr, cfg.ClickHouseDB, cfg.ClickHouseUser, cfg.ClickHouseProtocol)

	ch, err := clickhouseclient.New(cfg.ClickHouseAddr, cfg.ClickHouseDB, cfg.ClickHouseUser, cfg.ClickHousePassword, cfg.ClickHouseProtocol)
	if err != nil {
		log.Fatalf("clickhouse connect error: %v", err)
	}
	defer func() {
		if err := ch.Close(); err != nil {
			log.Printf("clickhouse close error: %v", err)
		}
	}()

	rdb := redisclient.New(cfg.RedisAddr, cfg.RedisDB)
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Printf("redis close error: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	brokers := splitCSV(cfg.KafkaBrokers)
	topics := []string{cfg.KafkaCustomerTopic, cfg.KafkaOrderTopic}

	var wg sync.WaitGroup
	for _, topic := range topics {
		if topic == "" {
			continue
		}
		wg.Add(1)
		go func(tp string) {
			defer wg.Done()
			consumeTopic(ctx, brokers, cfg.KafkaGroupID, tp, rdb, ch)
		}(topic)
	}

	wg.Wait()
	log.Printf("shutting down %s", cfg.ServiceName)
}

func consumeTopic(ctx context.Context, brokers []string, groupID, topic string, rdb *redisclient.Client, ch *clickhouseclient.Client) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,
	})
	log.Printf("reader started topic=%s", topic)
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("reader close error: %v", err)
		}
	}()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("fetch error topic=%s err=%v", topic, err)
			continue
		}

		log.Printf("message received topic=%s partition=%d offset=%d key=%s bytes=%d", topic, msg.Partition, msg.Offset, string(msg.Key), len(msg.Value))
		shouldCommit := handleMessage(ctx, rdb, ch, msg.Value)
		if !shouldCommit {
			log.Printf("message not committed topic=%s partition=%d offset=%d", topic, msg.Partition, msg.Offset)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit error topic=%s err=%v", topic, err)
		} else {
			log.Printf("committed topic=%s partition=%d offset=%d", topic, msg.Partition, msg.Offset)
		}
	}
}

const idempotencyTTL = 60 * 24 * time.Hour
const redisTimeout = 2 * time.Second
const clickhouseTimeout = 15 * time.Second

func handleMessage(ctx context.Context, rdb *redisclient.Client, ch *clickhouseclient.Client, payload []byte) bool {
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
		return handleCustomerEvent(ctx, rdb, ch, evt)
	case models.EventTypeOrderCreated:
		evt, err := models.ParseOrderEvent(payload)
		if err != nil {
			log.Printf("parse order event error: %v", err)
			return false
		}
		return handleOrderEvent(ctx, rdb, ch, evt)
	default:
		log.Printf("unknown event_type=%s", eventType)
		return false
	}
}

func handleCustomerEvent(ctx context.Context, rdb *redisclient.Client, ch *clickhouseclient.Client, evt models.CustomerEvent) bool {
	log.Printf("customer processing event_id=%s customer_id=%s", evt.EventID, evt.CustomerID)
	redisCtx, redisCancel := context.WithTimeout(ctx, redisTimeout)
	marked, err := rdb.CheckAndMarkEvent(redisCtx, evt.EventID, idempotencyTTL)
	redisCancel()
	if err != nil {
		log.Printf("idempotency check error event_id=%s err=%v", evt.EventID, err)
		return false
	}
	if !marked {
		log.Printf("duplicate customer event_id=%s customer_id=%s", evt.EventID, evt.CustomerID)
		return true
	}

	chCtx, chCancel := context.WithTimeout(ctx, clickhouseTimeout)
	err = ch.InsertCustomer(chCtx, evt.CustomerID, evt.EventTime, evt.EventTime, evt.EventID)
	chCancel()
	if err != nil {
		log.Printf("clickhouse insert customer error event_id=%s err=%v", evt.EventID, err)
		unmarkCtx, unmarkCancel := context.WithTimeout(context.Background(), redisTimeout)
		if err := rdb.UnmarkEvent(unmarkCtx, evt.EventID); err != nil {
			log.Printf("idempotency unmark error event_id=%s err=%v", evt.EventID, err)
		}
		unmarkCancel()
		return false
	}

	log.Printf("customer ingested event_id=%s customer_id=%s event_time=%s", evt.EventID, evt.CustomerID, evt.EventTime.Format(time.RFC3339Nano))
	return true
}

func handleOrderEvent(ctx context.Context, rdb *redisclient.Client, ch *clickhouseclient.Client, evt models.OrderEvent) bool {
	log.Printf("order processing event_id=%s customer_id=%s order_id=%s", evt.EventID, evt.CustomerID, evt.OrderID)
	redisCtx, redisCancel := context.WithTimeout(ctx, redisTimeout)
	marked, err := rdb.CheckAndMarkEvent(redisCtx, evt.EventID, idempotencyTTL)
	redisCancel()
	if err != nil {
		log.Printf("idempotency check error event_id=%s err=%v", evt.EventID, err)
		return false
	}
	if !marked {
		log.Printf("duplicate order event_id=%s customer_id=%s", evt.EventID, evt.CustomerID)
		return true
	}

	chCtx, chCancel := context.WithTimeout(ctx, clickhouseTimeout)
	err = ch.InsertOrder(chCtx, evt.OrderID, evt.CustomerID, evt.EventTime, evt.TotalAmount, evt.Currency, evt.EventID)
	chCancel()
	if err != nil {
		log.Printf("clickhouse insert order error event_id=%s err=%v", evt.EventID, err)
		unmarkCtx, unmarkCancel := context.WithTimeout(context.Background(), redisTimeout)
		if err := rdb.UnmarkEvent(unmarkCtx, evt.EventID); err != nil {
			log.Printf("idempotency unmark error event_id=%s err=%v", evt.EventID, err)
		}
		unmarkCancel()
		return false
	}

	log.Printf("order ingested event_id=%s customer_id=%s order_id=%s event_time=%s", evt.EventID, evt.CustomerID, evt.OrderID, evt.EventTime.Format(time.RFC3339Nano))
	return true
}

func splitCSV(input string) []string {
	parts := strings.Split(input, ",")
	var out []string
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}
