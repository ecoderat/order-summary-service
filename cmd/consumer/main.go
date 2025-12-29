package main

import (
	"fmt"

	"order-summary-service/internal/config"
)

func main() {
	cfg := config.Load("kafka-consumer-service")
	fmt.Printf("config: %+v\n", cfg)
}
