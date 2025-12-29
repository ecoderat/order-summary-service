package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"

	"order-summary-service/internal/config"
)

const tickInterval = 3 * time.Second

var customerIDs = []string{
	"C12345678",
	"C23456789",
	"C34567890",
	"C45678901",
	"C56789012",
}

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
	rand.Seed(time.Now().UnixNano())

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.KafkaBrokers,
	})
	if err != nil {
		log.Fatalf("kafka producer error: %v", err)
	}
	defer producer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("shutting down %s", cfg.ServiceName)
			return
		case <-ticker.C:
			customerID := randomCustomerID()
			if err := sendCustomerEvent(producer, cfg.KafkaCustomerTopic, customerID); err != nil {
				log.Printf("customer send error: %v", err)
				continue
			}
			if err := sendOrderEvent(producer, cfg.KafkaOrderTopic, customerID); err != nil {
				log.Printf("order send error: %v", err)
			}
		}
	}
}

func sendCustomerEvent(producer *kafka.Producer, topic, customerID string) error {
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

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(customerID),
		Value:          payload,
	}

	return producer.Produce(msg, nil)
}

func sendOrderEvent(producer *kafka.Producer, topic, customerID string) error {
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

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(customerID),
		Value:          payload,
	}

	return producer.Produce(msg, nil)
}

func randomCustomerID() string {
	return customerIDs[rand.Intn(len(customerIDs))]
}

func randomAmount() float64 {
	return 10 + rand.Float64()*5000
}
