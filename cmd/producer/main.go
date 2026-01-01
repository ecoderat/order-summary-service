package main

import (
	"context"
	"flag"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"

	"order-summary-service/internal/config"
	"order-summary-service/internal/service"
)

func main() {
	cfg := config.Load("kafka-producer-service")
	logger := logrus.StandardLogger()
	logger.WithField("service", cfg.ServiceName).Info("starting")

	producerCfg := parseFlags()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.KafkaBrokers,
	})
	if err != nil {
		logger.WithError(err).Fatal("kafka producer error")
	}

	svc, err := service.NewProducerService(producer, cfg.KafkaCustomerTopic, cfg.KafkaOrderTopic, logger, producerCfg)
	if err != nil {
		logger.WithError(err).Fatal("producer service error")
	}
	defer func() {
		if err := svc.Close(); err != nil {
			logger.WithError(err).Error("producer close error")
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := svc.Run(ctx); err != nil && ctx.Err() == nil {
		logger.WithError(err).Error("producer run error")
	}
}

func parseFlags() service.ProducerConfig {
	cfg := service.ProducerConfig{}
	flag.DurationVar(&cfg.Interval, "interval", 3*time.Second, "Tick interval")
	flag.IntVar(&cfg.Size, "size", 100, "Total events per tick")
	flag.Float64Var(&cfg.CustomerRatio, "customer-ratio", 0.5, "Share of customer events (0-1)")
	flag.IntVar(&cfg.DupPercent, "dup-percent", 0, "Duplication percent (0-100)")
	flag.IntVar(&cfg.CustomerUpgradePercent, "customer-upgrade-percent", 0, "Customer upgrade percent (0-100)")
	flag.IntVar(&cfg.OrderUpgradePercent, "order-upgrade-percent", 0, "Order upgrade percent (0-100)")
	flag.IntVar(&cfg.CustomerPoolSize, "customer-pool", 50, "Customer ID pool size")
	flag.StringVar(&cfg.FixedCustomerID, "customer-id", "", "Fixed customer ID for all events (optional)")
	flag.Parse()
	return cfg
}
