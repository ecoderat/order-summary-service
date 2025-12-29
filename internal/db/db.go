package db

import (
	"context"
	"crypto/tls"
	"fmt"
	"order-summary-service/internal/config"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// New creates a ClickHouse connection with pool settings.
func New(ctx context.Context, cfg *config.Config) (clickhouse.Conn, error) {
	options := &clickhouse.Options{
		Addr: []string{cfg.ClickHouseAddr},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouseDB,
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePassword,
		},
		TLS: func() *tls.Config {
			if cfg.UseTLS {
				return &tls.Config{InsecureSkipVerify: true}
			}
			return nil
		}(),
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		ConnMaxLifetime:  cfg.DBMaxConnLifetime,
		MaxOpenConns:     cfg.DBMaxConns,
		MaxIdleConns:     cfg.DBMinConns,
		DialTimeout:      5 * time.Second,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, err
	}

	return conn, nil
}
