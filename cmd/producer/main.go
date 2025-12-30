package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"order-summary-service/internal/config"
	"order-summary-service/internal/service"
)

func main() {
	cfg := config.Load("kafka-producer-service")
	log.Printf("starting %s", cfg.ServiceName)

	producerCfg := parseFlags()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.KafkaBrokers,
	})
	if err != nil {
		log.Fatalf("kafka producer error: %v", err)
	}

	svc, err := service.NewProducerService(producer, cfg.KafkaCustomerTopic, cfg.KafkaOrderTopic, producerCfg)
	if err != nil {
		log.Fatalf("producer service error: %v", err)
	}
	defer func() {
		if err := svc.Close(); err != nil {
			log.Printf("producer close error: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := svc.Run(ctx); err != nil && ctx.Err() == nil {
		log.Printf("producer run error: %v", err)
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
	flag.Parse()
	return cfg
}
