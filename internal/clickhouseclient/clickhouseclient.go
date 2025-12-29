package clickhouseclient

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type Client struct {
	conn clickhouse.Conn
}

type MonthlySummary struct {
	CustomerID string
	WindowFrom time.Time
	WindowTo   time.Time
	OrderCount uint64
	TotalSpend float64
	Currency   string
}

func New(addr, database, username, password, protocol string) (*Client, error) {
	chProtocol := clickhouse.Native
	if protocol == "http" {
		chProtocol = clickhouse.HTTP
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Protocol:    chProtocol,
		DialTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(context.Background()); err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) InsertCustomer(ctx context.Context, customerID string, createdAt, updatedAt time.Time, sourceEventID string) error {
	query := `INSERT INTO customers_current (customer_id, created_at, updated_at, source_event_id)`
	eventUUID, err := uuid.Parse(sourceEventID)
	if err != nil {
		return err
	}
	batch, err := c.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	if err := batch.Append(customerID, createdAt, updatedAt, eventUUID); err != nil {
		return err
	}
	return batch.Send()
}

func (c *Client) InsertOrder(ctx context.Context, orderID, customerID string, orderTime time.Time, totalAmount float64, currency, sourceEventID string) error {
	query := `INSERT INTO orders_current (order_id, customer_id, order_time, total_amount, currency, source_event_id)`
	eventUUID, err := uuid.Parse(sourceEventID)
	if err != nil {
		return err
	}
	amount := decimal.NewFromFloat(totalAmount).Round(2)
	batch, err := c.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	if err := batch.Append(orderID, customerID, orderTime, amount, currency, eventUUID); err != nil {
		return err
	}
	return batch.Send()
}

func (c *Client) CustomerExistsFinal(ctx context.Context, customerID string) (bool, error) {
	query := `
SELECT 1
FROM customers_current FINAL
WHERE customer_id = ?
LIMIT 1`

	row := c.conn.QueryRow(ctx, query, customerID)
	var one uint8
	if err := row.Scan(&one); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *Client) MonthlySummaryFinal(ctx context.Context, customerID string) (MonthlySummary, bool, error) {
	query := `
WITH
  toDate(now('UTC')) AS window_to,
  (window_to - 30)   AS window_from
SELECT
  customer_id,
  window_from,
  window_to,
  count() AS order_count,
  sum(total_amount) AS total_spend,
  any(currency)     AS currency
FROM orders_current FINAL
WHERE customer_id = ?
  AND order_time >= toDateTime(window_from, 'UTC')
  AND order_time <  toDateTime(window_to + 1, 'UTC')
GROUP BY customer_id`

	row := c.conn.QueryRow(ctx, query, customerID)
	var summary MonthlySummary
	if err := row.Scan(
		&summary.CustomerID,
		&summary.WindowFrom,
		&summary.WindowTo,
		&summary.OrderCount,
		&summary.TotalSpend,
		&summary.Currency,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return MonthlySummary{}, false, nil
		}
		return MonthlySummary{}, false, err
	}
	return summary, true, nil
}
