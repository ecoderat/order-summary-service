package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"

	"order-summary-service/internal/config"
)

const currencyDefault = "TRY"

type producerConfig struct {
	Interval               time.Duration
	Size                   int
	CustomerRatio          float64
	DupPercent             int
	CustomerUpgradePercent int
	OrderUpgradePercent    int
	CustomerPoolSize       int
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

type customerPool struct {
	ids []string
	max int
}

type customerEventStore struct {
	byCustomer map[string]customerEvent
}

type orderEventStore struct {
	byCustomer map[string]orderEvent
}

func main() {
	cfg := config.Load("kafka-producer-service")
	flags := parseFlags()
	log.Printf("starting %s", cfg.ServiceName)
	log.Printf("interval=%s size=%d ratio=%.2f dup=%d%% customer_upgrade=%d%% order_upgrade=%d%% customer_pool=%d",
		flags.Interval, flags.Size, flags.CustomerRatio, flags.DupPercent,
		flags.CustomerUpgradePercent, flags.OrderUpgradePercent, flags.CustomerPoolSize)

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.KafkaBrokers,
	})
	if err != nil {
		log.Fatalf("kafka producer error: %v", err)
	}
	defer producer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool := &customerPool{max: flags.CustomerPoolSize}
	custStore := &customerEventStore{byCustomer: make(map[string]customerEvent)}
	orderStore := &orderEventStore{byCustomer: make(map[string]orderEvent)}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	ticker := time.NewTicker(flags.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			producer.Flush(5000)
			return
		case <-ticker.C:
			customerCount, orderCount := splitCounts(flags.Size, flags.CustomerRatio)
			log.Printf("tick send customer=%d order=%d", customerCount, orderCount)
			for i := 0; i < customerCount; i++ {
				customerID := pool.pick(rng, rollPercent(rng, flags.CustomerUpgradePercent))
				customerEvt := buildCustomerEvent(rng, customerID, custStore, flags.DupPercent)
				if err := sendEvent(producer, cfg.KafkaCustomerTopic, customerEvt.CustomerID, customerEvt); err != nil {
					log.Printf("customer send error: %v", err)
				}
			}
			for i := 0; i < orderCount; i++ {
				customerID := pool.pick(rng, true)
				orderEvt := buildOrderEvent(rng, customerID, orderStore, flags.DupPercent, flags.OrderUpgradePercent)
				if err := sendEvent(producer, cfg.KafkaOrderTopic, orderEvt.CustomerID, orderEvt); err != nil {
					log.Printf("order send error: %v", err)
				}
			}
			producer.Flush(1000)
		}
	}
}

func parseFlags() producerConfig {
	cfg := producerConfig{}
	flag.DurationVar(&cfg.Interval, "interval", 3*time.Second, "Tick interval")
	flag.IntVar(&cfg.Size, "size", 100, "Total events per tick")
	flag.Float64Var(&cfg.CustomerRatio, "customer-ratio", 0.5, "Share of customer events (0-1)")
	flag.IntVar(&cfg.DupPercent, "dup-percent", 0, "Duplication percent (0-100)")
	flag.IntVar(&cfg.CustomerUpgradePercent, "customer-upgrade-percent", 0, "Customer upgrade percent (0-100)")
	flag.IntVar(&cfg.OrderUpgradePercent, "order-upgrade-percent", 0, "Order upgrade percent (0-100)")
	flag.IntVar(&cfg.CustomerPoolSize, "customer-pool", 50, "Customer ID pool size")
	flag.Parse()

	if cfg.Interval <= 0 {
		cfg.Interval = 3 * time.Second
	}
	if cfg.Size <= 0 {
		cfg.Size = 1
	}
	if cfg.CustomerRatio < 0 {
		cfg.CustomerRatio = 0
	}
	if cfg.CustomerRatio > 1 {
		cfg.CustomerRatio = 1
	}
	cfg.DupPercent = clampPercent(cfg.DupPercent)
	cfg.CustomerUpgradePercent = clampPercent(cfg.CustomerUpgradePercent)
	cfg.OrderUpgradePercent = clampPercent(cfg.OrderUpgradePercent)
	if cfg.CustomerPoolSize <= 0 {
		cfg.CustomerPoolSize = 1
	}

	return cfg
}

func buildCustomerEvent(rng *rand.Rand, customerID string, store *customerEventStore, dupPercent int) customerEvent {
	if dupPercent > 0 && rollPercent(rng, dupPercent) {
		if evt, ok := store.get(customerID); ok {
			return evt
		}
	}
	return store.put(customerEvent{
		EventID:    uuid.NewString(),
		EventTime:  time.Now().UTC().Format(time.RFC3339Nano),
		EventType:  "CUSTOMER_CREATED",
		CustomerID: customerID,
	})
}

func buildOrderEvent(rng *rand.Rand, customerID string, store *orderEventStore, dupPercent, upgradePercent int) orderEvent {
	if dupPercent > 0 && rollPercent(rng, dupPercent) {
		if evt, ok := store.get(customerID); ok {
			return evt
		}
	}

	orderID := uuid.NewString()
	if upgradePercent > 0 && rollPercent(rng, upgradePercent) {
		if evt, ok := store.get(customerID); ok {
			orderID = evt.OrderID
		}
	}

	return store.put(orderEvent{
		EventID:     uuid.NewString(),
		EventTime:   time.Now().UTC().Format(time.RFC3339Nano),
		EventType:   "ORDER_CREATED",
		OrderID:     orderID,
		CustomerID:  customerID,
		TotalAmount: randomAmount(rng),
		Currency:    currencyDefault,
	})
}

func sendEvent(producer *kafka.Producer, topic, key string, event any) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          payload,
	}
	return producer.Produce(msg, nil)
}

func (p *customerPool) pick(rng *rand.Rand, forceExisting bool) string {
	if len(p.ids) == 0 {
		id := newCustomerID()
		p.ids = append(p.ids, id)
		return id
	}

	if forceExisting {
		return p.ids[rng.Intn(len(p.ids))]
	}

	if len(p.ids) < p.max {
		id := newCustomerID()
		p.ids = append(p.ids, id)
		return id
	}

	return p.ids[rng.Intn(len(p.ids))]
}

func newCustomerID() string {
	return fmt.Sprintf("C%s", uuid.NewString()[:8])
}

func (s *customerEventStore) get(customerID string) (customerEvent, bool) {
	evt, ok := s.byCustomer[customerID]
	return evt, ok
}

func (s *customerEventStore) put(evt customerEvent) customerEvent {
	s.byCustomer[evt.CustomerID] = evt
	return evt
}

func (s *orderEventStore) get(customerID string) (orderEvent, bool) {
	evt, ok := s.byCustomer[customerID]
	return evt, ok
}

func (s *orderEventStore) put(evt orderEvent) orderEvent {
	s.byCustomer[evt.CustomerID] = evt
	return evt
}

func randomAmount(rng *rand.Rand) float64 {
	return 10 + rng.Float64()*5000
}

func rollPercent(rng *rand.Rand, percent int) bool {
	return percent > 0 && rng.Intn(100) < percent
}

func clampPercent(value int) int {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}

func splitCounts(total int, customerRatio float64) (int, int) {
	if total <= 0 {
		return 0, 0
	}
	customerCount := int(float64(total) * customerRatio)
	if customerCount > total {
		customerCount = total
	}
	orderCount := total - customerCount
	return customerCount, orderCount
}
