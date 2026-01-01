package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"

	"order-summary-service/internal/cache"
	"order-summary-service/internal/config"
	"order-summary-service/internal/db"
	"order-summary-service/internal/repository"
	"order-summary-service/internal/service"
)

func main() {
	cfg := config.Load("kafka-consumer-service")
	logger := logrus.StandardLogger()
	logger.WithField("service", cfg.ServiceName).Info("starting")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ch, err := db.New(ctx, &cfg)
	if err != nil {
		logger.WithError(err).Fatal("clickhouse connect error")
	}
	defer func() {
		if err := ch.Close(); err != nil {
			logger.WithError(err).Error("clickhouse close error")
		}
	}()

	repo := repository.NewRepository(ch)

	rdb := cache.New(cfg.RedisAddr, cfg.RedisDB)
	defer func() {
		if err := rdb.Close(); err != nil {
			logger.WithError(err).Error("redis close error")
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
		logger.WithError(err).Fatal("kafka consumer error")
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.WithError(err).Error("kafka consumer close error")
		}
	}()

	svc, err := service.NewConsumerService(
		consumer,
		repo,
		rdb,
		ch,
		logger,
		service.ConsumerConfig{
			KafkaCustomerTopic: cfg.KafkaCustomerTopic,
			KafkaOrderTopic:    cfg.KafkaOrderTopic,
			BatchSize:          cfg.ConsumerBatchSize,
			BatchInterval:      cfg.ConsumerBatchInterval,
			RedisTimeout:       cfg.RedisTimeout,
			ClickHouseTimeout:  cfg.ClickHouseTimeout,
			IdempotencyTTL:     cfg.IdempotencyTTL,
			PendingTTL:         cfg.PendingTTL,
		},
	)
	if err != nil {
		logger.WithError(err).Fatal("consumer service error")
	}

	if err := svc.Run(ctx); err != nil && ctx.Err() == nil {
		logger.WithError(err).Error("consumer run error")
	}
}
