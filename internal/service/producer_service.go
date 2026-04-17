package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"order-summary-service/internal/models"
)

const (
	currencyDefault        = "TRY"
	tickFlushTimeoutMs     = 1000
	shutdownFlushTimeoutMs = 5000
)

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
	producer kafkaProducer
	cfg      ProducerConfig
	logger   *logrus.Logger

	customerTopic string
	orderTopic    string

	pool       *customerPool
	custStore  *customerEventStore
	orderStore *orderEventStore
	rng        *rand.Rand

	closeOnce sync.Once
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

type kafkaProducer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Flush(timeoutMs int) int
	Close()
	Events() chan kafka.Event
}

func NewProducerService(producer *kafka.Producer, customerTopic, orderTopic string, logger *logrus.Logger, cfg ProducerConfig) (ProducerService, error) {
	if logger == nil {
		logger = logrus.StandardLogger()
	}
	cfg = cfg.normalized()

	return &producerService{
		producer:      producer,
		cfg:           cfg,
		logger:        logger,
		customerTopic: customerTopic,
		orderTopic:    orderTopic,
		pool:          &customerPool{max: cfg.CustomerPoolSize},
		custStore:     &customerEventStore{byCustomer: make(map[string]customerEvent)},
		orderStore:    &orderEventStore{byCustomer: make(map[string]orderEvent)},
		rng:           rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

func (s *producerService) Run(ctx context.Context) error {
	s.logConfig()

	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	deliveryStats := &deliveryStats{}
	go s.drainDeliveryReports(ctx, deliveryStats)

	var lastDeliveryOK uint64
	var lastDeliveryErr uint64

	for {
		select {
		case <-ctx.Done():
			s.flushBestEffort(shutdownFlushTimeoutMs)
			return ctx.Err()
		case <-ticker.C:
			customerCount, orderCount := s.tickOnce()
			s.flushBestEffort(tickFlushTimeoutMs)

			deliveredOK, deliveredErr := deliveryStats.snapshot()
			s.logger.WithFields(logrus.Fields{
				"customer":      customerCount,
				"order":         orderCount,
				"delivered_ok":  deliveredOK - lastDeliveryOK,
				"delivered_err": deliveredErr - lastDeliveryErr,
			}).Info("tick summary")
			lastDeliveryOK = deliveredOK
			lastDeliveryErr = deliveredErr
		}
	}
}

func (s *producerService) Close() error {
	s.closeOnce.Do(func() {
		if s.producer != nil {
			s.flushBestEffort(shutdownFlushTimeoutMs)
			s.producer.Close()
		}
	})
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
		EventType:  models.EventTypeCustomerCreated,
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
		EventType:   models.EventTypeOrderCreated,
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

func (s *producerService) logConfig() {
	s.logger.WithFields(logrus.Fields{
		"interval":         s.cfg.Interval.String(),
		"size":             s.cfg.Size,
		"ratio":            s.cfg.CustomerRatio,
		"dup_percent":      s.cfg.DupPercent,
		"customer_upgrade": s.cfg.CustomerUpgradePercent,
		"order_upgrade":    s.cfg.OrderUpgradePercent,
		"customer_pool":    s.cfg.CustomerPoolSize,
	}).Info("producer config")
	if s.cfg.FixedCustomerID != "" {
		s.logger.WithField("customer_id", s.cfg.FixedCustomerID).Info("fixed customer id")
	}
}

func (s *producerService) tickOnce() (int, int) {
	customerCount, orderCount := splitCounts(s.cfg.Size, s.cfg.CustomerRatio)
	s.logger.WithFields(logrus.Fields{
		"customer": customerCount,
		"order":    orderCount,
	}).Info("tick send")
	s.produceCustomers(customerCount)
	s.produceOrders(orderCount)
	return customerCount, orderCount
}

func (s *producerService) produceCustomers(count int) {
	for i := 0; i < count; i++ {
		customerID := s.pickCustomerID(rollPercent(s.rng, s.cfg.CustomerUpgradePercent))
		customerEvt := s.buildCustomerEvent(customerID)
		if err := s.sendEvent(s.customerTopic, customerEvt.CustomerID, customerEvt); err != nil {
			s.logger.WithError(err).Error("customer send error")
		}
	}
}

func (s *producerService) produceOrders(count int) {
	for i := 0; i < count; i++ {
		customerID := s.pickCustomerID(true)
		orderEvt := s.buildOrderEvent(customerID)
		if err := s.sendEvent(s.orderTopic, orderEvt.CustomerID, orderEvt); err != nil {
			s.logger.WithError(err).Error("order send error")
		}
	}
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

func (s *producerService) flushBestEffort(timeoutMs int) {
	if s.producer == nil {
		return
	}
	s.producer.Flush(timeoutMs)
}

func (s *producerService) drainDeliveryReports(ctx context.Context, stats *deliveryStats) {
	if s.producer == nil {
		return
	}
	events := s.producer.Events()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			switch msg := ev.(type) {
			case *kafka.Message:
				if msg.TopicPartition.Error != nil {
					stats.addErr()
					s.logger.WithError(msg.TopicPartition.Error).Warn("delivery failed")
				} else {
					stats.addOK()
				}
			case kafka.Error:
				stats.addErr()
				s.logger.WithError(msg).Warn("producer error")
			}
		}
	}
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

func (c ProducerConfig) normalized() ProducerConfig {
	cfg := c
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

type deliveryStats struct {
	ok  uint64
	err uint64
}

func (s *deliveryStats) addOK() {
	atomic.AddUint64(&s.ok, 1)
}

func (s *deliveryStats) addErr() {
	atomic.AddUint64(&s.err, 1)
}

func (s *deliveryStats) snapshot() (uint64, uint64) {
	return atomic.LoadUint64(&s.ok), atomic.LoadUint64(&s.err)
}
