package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	ServiceName           string
	KafkaBrokers          string
	KafkaGroupID          string
	KafkaCustomerTopic    string
	KafkaOrderTopic       string
	RedisAddr             string
	RedisDB               int
	RedisTimeout          time.Duration
	ClickHouseAddr        string
	ClickHouseDB          string
	ClickHouseUser        string
	ClickHousePassword    string
	ClickHouseProtocol    string
	ClickHouseTimeout     time.Duration
	UseTLS                bool
	DBMaxConnLifetime     time.Duration
	DBMaxConns            int
	DBMinConns            int
	ConsumerBatchSize     int
	ConsumerBatchInterval time.Duration
	CacheTTL              time.Duration
	HTTPAddr              string
	IdempotencyTTL        time.Duration
}

func Load(serviceName string) Config {
	return Config{
		ServiceName:           serviceName,
		KafkaBrokers:          envOrDefault("KAFKA_BROKERS", "localhost:9092"),
		KafkaGroupID:          envOrDefault("KAFKA_GROUP_ID", "order-summary-consumer"),
		KafkaCustomerTopic:    envOrDefault("KAFKA_CUSTOMER_TOPIC", "customer_events"),
		KafkaOrderTopic:       envOrDefault("KAFKA_ORDER_TOPIC", "order_events"),
		RedisAddr:             envOrDefault("REDIS_ADDR", "localhost:6379"),
		RedisDB:               envIntOrDefault("REDIS_DB", 0),
		RedisTimeout:          time.Duration(envIntOrDefault("REDIS_TIMEOUT_SECONDS", 2)) * time.Second,
		ClickHouseAddr:        envOrDefault("CLICKHOUSE_ADDR", "localhost:9000"),
		ClickHouseDB:          envOrDefault("CLICKHOUSE_DB", "default"),
		ClickHouseUser:        envOrDefault("CLICKHOUSE_USER", "default"),
		ClickHousePassword:    envOrDefault("CLICKHOUSE_PASSWORD", "password"),
		ClickHouseProtocol:    envOrDefault("CLICKHOUSE_PROTOCOL", "tcp"),
		ClickHouseTimeout:     time.Duration(envIntOrDefault("CLICKHOUSE_TIMEOUT_SECONDS", 15)) * time.Second,
		UseTLS:                envOrDefault("CLICKHOUSE_USE_TLS", "false") == "true",
		DBMaxConnLifetime:     time.Duration(envIntOrDefault("DB_MAX_CONN_LIFETIME_MINUTES", 60)) * time.Minute,
		DBMaxConns:            envIntOrDefault("DB_MAX_CONNS", 10),
		DBMinConns:            envIntOrDefault("DB_MIN_CONNS", 5),
		ConsumerBatchSize:     envIntOrDefault("CONSUMER_BATCH_SIZE", 1000),
		ConsumerBatchInterval: time.Duration(envIntOrDefault("CONSUMER_BATCH_INTERVAL_MS", 2000)) * time.Millisecond,
		CacheTTL:              time.Duration(envIntOrDefault("CACHE_TTL_HOURS", 28)) * time.Hour,
		HTTPAddr:              envOrDefault("HTTP_ADDR", ":8080"),
		IdempotencyTTL:        time.Duration(envIntOrDefault("IDEMPOTENCY_TTL_HOURS", 60*24)) * time.Hour,
	}
}

func envOrDefault(key, def string) string {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	return val
}

func envIntOrDefault(key string, def int) int {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return def
	}
	return parsed
}
