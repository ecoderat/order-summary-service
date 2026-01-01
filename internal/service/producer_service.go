package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

const currencyDefault = "TRY"

type ProducerService interface {
	Run(ctx context.Context) error
	Close() error
}

type ProducerConfig struct {
	Interval               time.Duration
	Size                   int
	CustomerRatio          float64
	DupPercent             int
	CustomerUpgradePercent int
	OrderUpgradePercent    int
	CustomerPoolSize       int
	FixedCustomerID        string
}

type producerService struct {
	producer *kafka.Producer
	cfg      ProducerConfig

	customerTopic string
	orderTopic    string

	pool       *customerPool
	custStore  *customerEventStore
	orderStore *orderEventStore
	rng        *rand.Rand
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

func NewProducerService(producer *kafka.Producer, customerTopic, orderTopic string, cfg ProducerConfig) (ProducerService, error) {
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

	return &producerService{
		producer:      producer,
		cfg:           cfg,
		customerTopic: customerTopic,
		orderTopic:    orderTopic,
		pool:          &customerPool{max: cfg.CustomerPoolSize},
		custStore:     &customerEventStore{byCustomer: make(map[string]customerEvent)},
		orderStore:    &orderEventStore{byCustomer: make(map[string]orderEvent)},
		rng:           rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

func (s *producerService) Run(ctx context.Context) error {
	log.Printf("interval=%s size=%d ratio=%.2f dup=%d%% customer_upgrade=%d%% order_upgrade=%d%% customer_pool=%d",
		s.cfg.Interval, s.cfg.Size, s.cfg.CustomerRatio, s.cfg.DupPercent,
		s.cfg.CustomerUpgradePercent, s.cfg.OrderUpgradePercent, s.cfg.CustomerPoolSize)
	if s.cfg.FixedCustomerID != "" {
		log.Printf("fixed customer_id=%s", s.cfg.FixedCustomerID)
	}

	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			customerCount, orderCount := splitCounts(s.cfg.Size, s.cfg.CustomerRatio)
			log.Printf("tick send customer=%d order=%d", customerCount, orderCount)
			for i := 0; i < customerCount; i++ {
				customerID := s.pickCustomerID(rollPercent(s.rng, s.cfg.CustomerUpgradePercent))
				customerEvt := s.buildCustomerEvent(customerID)
				if err := s.sendEvent(s.customerTopic, customerEvt.CustomerID, customerEvt); err != nil {
					log.Printf("customer send error: %v", err)
				}
			}
			for i := 0; i < orderCount; i++ {
				customerID := s.pickCustomerID(true)
				orderEvt := s.buildOrderEvent(customerID)
				if err := s.sendEvent(s.orderTopic, orderEvt.CustomerID, orderEvt); err != nil {
					log.Printf("order send error: %v", err)
				}
			}
			s.producer.Flush(1000)
		}
	}
}

func (s *producerService) Close() error {
	if s.producer != nil {
		s.producer.Flush(5000)
		s.producer.Close()
	}
	return nil
}

func (s *producerService) buildCustomerEvent(customerID string) customerEvent {
	if s.cfg.DupPercent > 0 && rollPercent(s.rng, s.cfg.DupPercent) {
		if evt, ok := s.custStore.get(customerID); ok {
			return evt
		}
	}

	return s.custStore.put(customerEvent{
		EventID:    uuid.NewString(),
		EventTime:  time.Now().UTC().Format(time.RFC3339Nano),
		EventType:  "CUSTOMER_CREATED",
		CustomerID: customerID,
	})
}

func (s *producerService) buildOrderEvent(customerID string) orderEvent {
	if s.cfg.DupPercent > 0 && rollPercent(s.rng, s.cfg.DupPercent) {
		if evt, ok := s.orderStore.get(customerID); ok {
			return evt
		}
	}

	orderID := uuid.NewString()
	if s.cfg.OrderUpgradePercent > 0 && rollPercent(s.rng, s.cfg.OrderUpgradePercent) {
		if evt, ok := s.orderStore.get(customerID); ok {
			orderID = evt.OrderID
		}
	}

	return s.orderStore.put(orderEvent{
		EventID:     uuid.NewString(),
		EventTime:   time.Now().UTC().Format(time.RFC3339Nano),
		EventType:   "ORDER_CREATED",
		OrderID:     orderID,
		CustomerID:  customerID,
		TotalAmount: randomAmount(s.rng),
		Currency:    currencyDefault,
	})
}

func (s *producerService) pickCustomerID(forceExisting bool) string {
	if s.cfg.FixedCustomerID != "" {
		return s.cfg.FixedCustomerID
	}
	return s.pool.pick(s.rng, forceExisting)
}

func (s *producerService) sendEvent(topic, key string, event any) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          payload,
	}
	return s.producer.Produce(msg, nil)
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
