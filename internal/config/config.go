package config

import (
	"os"
	"strconv"
)

type Config struct {
	ServiceName        string
	KafkaBrokers       string
	KafkaGroupID       string
	KafkaCustomerTopic string
	KafkaOrderTopic    string
	RedisAddr          string
	RedisDB            int
	ClickHouseAddr     string
	ClickHouseDB       string
	ClickHouseUser     string
	ClickHousePassword string
	ClickHouseProtocol string
	HTTPAddr           string
}

func Load(serviceName string) Config {
	return Config{
		ServiceName:        serviceName,
		KafkaBrokers:       envOrDefault("KAFKA_BROKERS", "localhost:9092"),
		KafkaGroupID:       envOrDefault("KAFKA_GROUP_ID", "order-summary-consumer"),
		KafkaCustomerTopic: envOrDefault("KAFKA_CUSTOMER_TOPIC", "customer_events"),
		KafkaOrderTopic:    envOrDefault("KAFKA_ORDER_TOPIC", "order_events"),
		RedisAddr:          envOrDefault("REDIS_ADDR", "localhost:6379"),
		RedisDB:            envIntOrDefault("REDIS_DB", 0),
		ClickHouseAddr:     envOrDefault("CLICKHOUSE_ADDR", "127.0.0.1:9000"),
		ClickHouseDB:       envOrDefault("CLICKHOUSE_DB", "default"),
		ClickHouseUser:     envOrDefault("CLICKHOUSE_USER", "default"),
		ClickHousePassword: envOrDefault("CLICKHOUSE_PASSWORD", "password"),
		ClickHouseProtocol: envOrDefault("CLICKHOUSE_PROTOCOL", "native"),
		HTTPAddr:           envOrDefault("HTTP_ADDR", ":8080"),
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
