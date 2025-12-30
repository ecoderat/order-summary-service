package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"order-summary-service/internal/config"
	"order-summary-service/internal/db"
	"order-summary-service/internal/redisclient"
	"order-summary-service/internal/repository"
	"order-summary-service/internal/service"
)

func main() {
	cfg := config.Load("kafka-consumer-service")
	log.Printf("starting %s", cfg.ServiceName)

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

	svc, err := service.NewConsumerService(
		consumer,
		repo,
		rdb,
		ch,
		service.ConsumerConfig{
			KafkaCustomerTopic: cfg.KafkaCustomerTopic,
			KafkaOrderTopic:    cfg.KafkaOrderTopic,
			BatchSize:          cfg.ConsumerBatchSize,
			BatchInterval:      cfg.ConsumerBatchInterval,
			RedisTimeout:       cfg.RedisTimeout,
			ClickHouseTimeout:  cfg.ClickHouseTimeout,
			IdempotencyTTL:     cfg.IdempotencyTTL,
		},
	)
	if err != nil {
		log.Fatalf("consumer service error: %v", err)
	}

	if err := svc.Run(ctx); err != nil && ctx.Err() == nil {
		log.Printf("consumer run error: %v", err)
	}
}
