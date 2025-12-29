package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"order-summary-service/internal/config"
)

const tickInterval = 3 * time.Second

type customerEvent struct {
	EventID    string `json:"event_id"`
	EventTime  string `json:"event_time"`
	EventType  string `json:"event_type"`
	CustomerID string `json:"customer_id"`
}

type orderEvent struct {
	EventID     string  `json:"event_id"`
	EventTime   string  `json:"event_time"`
	EventType   string  `json:"event_type"`
	OrderID     string  `json:"order_id"`
	CustomerID  string  `json:"customer_id"`
	TotalAmount float64 `json:"total_amount"`
	Currency    string  `json:"currency"`
}

func main() {
	cfg := config.Load("kafka-producer-service")
	log.Printf("starting %s", cfg.ServiceName)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(splitCSV(cfg.KafkaBrokers)...),
		BatchTimeout: 200 * time.Millisecond,
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("writer close error: %v", err)
		}
	}()

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("shutting down %s", cfg.ServiceName)
			return
		case <-ticker.C:
			customerID := randomCustomerID()
			if err := sendCustomerEvent(ctx, writer, cfg.KafkaCustomerTopic, customerID); err != nil {
				log.Printf("customer send error: %v", err)
				continue
			}
			if err := sendOrderEvent(ctx, writer, cfg.KafkaOrderTopic, customerID); err != nil {
				log.Printf("order send error: %v", err)
			}
		}
	}
}

func sendCustomerEvent(ctx context.Context, writer *kafka.Writer, topic, customerID string) error {
	event := customerEvent{
		EventID:    uuid.NewString(),
		EventTime:  time.Now().UTC().Format(time.RFC3339Nano),
		EventType:  "CUSTOMER_CREATED",
		CustomerID: customerID,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(customerID),
		Value: payload,
		Time:  time.Now().UTC(),
	}
	return writer.WriteMessages(ctx, msg)
}

func sendOrderEvent(ctx context.Context, writer *kafka.Writer, topic, customerID string) error {
	event := orderEvent{
		EventID:     uuid.NewString(),
		EventTime:   time.Now().UTC().Format(time.RFC3339Nano),
		EventType:   "ORDER_CREATED",
		OrderID:     uuid.NewString(),
		CustomerID:  customerID,
		TotalAmount: randomAmount(),
		Currency:    "TRY",
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(customerID),
		Value: payload,
		Time:  time.Now().UTC(),
	}
	return writer.WriteMessages(ctx, msg)
}

func randomCustomerID() string {
	return "C" + uuid.NewString()[:8]
}

func randomAmount() float64 {
	return 10 + rand.Float64()*5000
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
