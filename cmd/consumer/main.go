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

	"order-summary-service/internal/config"
	"order-summary-service/internal/models"
)

func main() {
	cfg := config.Load("kafka-consumer-service")
	log.Printf("starting %s", cfg.ServiceName)

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
			consumeTopic(ctx, brokers, cfg.KafkaGroupID, tp)
		}(topic)
	}

	wg.Wait()
	log.Printf("shutting down %s", cfg.ServiceName)
}

func consumeTopic(ctx context.Context, brokers []string, groupID, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,
	})
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

		if !handleMessage(msg.Value) {
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit error topic=%s err=%v", topic, err)
		}
	}
}

func handleMessage(payload []byte) bool {
	eventType, err := models.DetectEventType(payload)
	if err != nil {
		log.Printf("detect event type error: %v", err)
		return false
	}

	switch eventType {
	case models.EventTypeCustomerCreated:
		evt, err := models.ParseCustomerEvent(payload)
		if err != nil {
			log.Printf("parse customer event error: %v", err)
			return false
		}
		log.Printf("customer event_id=%s customer_id=%s event_time=%s", evt.EventID, evt.CustomerID, evt.EventTime.Format(time.RFC3339Nano))
		return true
	case models.EventTypeOrderCreated:
		evt, err := models.ParseOrderEvent(payload)
		if err != nil {
			log.Printf("parse order event error: %v", err)
			return false
		}
		log.Printf("order event_id=%s customer_id=%s event_time=%s", evt.EventID, evt.CustomerID, evt.EventTime.Format(time.RFC3339Nano))
		return true
	default:
		log.Printf("unknown event_type=%s", eventType)
		return false
	}
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
