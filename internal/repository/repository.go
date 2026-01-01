package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type MonthlySummary struct {
	CustomerID string
	WindowFrom time.Time
	WindowTo   time.Time
	OrderCount uint64
	TotalSpend decimal.Decimal
	Currency   string
}

var ErrMonthlySummaryNotFound = errors.New("monthly summary not found")

type CreateCustomerParams struct {
	CustomerID    string
	CreatedAt     time.Time
	UpdatedAt     time.Time
	SourceEventID string
}

type CreateOrderParams struct {
	OrderID       string
	CustomerID    string
	OrderTime     time.Time
	TotalAmount   float64
	Currency      string
	SourceEventID string
}

// Repository defines the methods for interacting with the data store.
type Repository interface {
	CreateCustomerBatch(ctx context.Context, customers []CreateCustomerParams) error
	CreateOrderBatch(ctx context.Context, orders []CreateOrderParams) error
	MonthlySummaryFinal(ctx context.Context, customerID string, windowFrom, windowTo time.Time) (MonthlySummary, error)
	CustomerExistsFinal(ctx context.Context, customerID string) (bool, error)
}

type repository struct {
	conn clickhouse.Conn
}

// NewRepository creates a new Repository instance.
func NewRepository(conn clickhouse.Conn) Repository {
	return &repository{conn: conn}
}

// CreateCustomerBatch inserts multiple customer records in a batch.
func (r *repository) CreateCustomerBatch(ctx context.Context, customers []CreateCustomerParams) error {
	query := `
		INSERT INTO customers_current (customer_id, created_at, updated_at, source_event_id)
		VALUES (?, ?, ?, ?)
	`

	batch, err := r.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}

	for _, customer := range customers {
		eventUUID, err := uuid.Parse(customer.SourceEventID)
		if err != nil {
			return err
		}

		err = batch.Append(
			customer.CustomerID,
			customer.CreatedAt,
			customer.UpdatedAt,
			eventUUID,
		)
		if err != nil {
			return err
		}
	}

	return batch.Send()
}

// CreateOrderBatch inserts multiple order records in a batch.
func (r *repository) CreateOrderBatch(ctx context.Context, orders []CreateOrderParams) error {
	query := `
		INSERT INTO orders_current (order_id, customer_id, order_time, total_amount, currency, source_event_id)
		VALUES (?, ?, ?, ?, ?, ?)
	`

	batch, err := r.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}

	for _, order := range orders {
		eventUUID, err := uuid.Parse(order.SourceEventID)
		if err != nil {
			return err
		}

		amount := decimal.NewFromFloat(order.TotalAmount).Round(2)

		err = batch.Append(
			order.OrderID,
			order.CustomerID,
			order.OrderTime,
			amount,
			order.Currency,
			eventUUID,
		)
		if err != nil {
			return err
		}
	}

	return batch.Send()
}

// CustomerExistsFinal checks if a customer exists using FINAL modifier.
func (r *repository) CustomerExistsFinal(ctx context.Context, customerID string) (bool, error) {
	query := `
	SELECT 1
	FROM customers_current FINAL
	WHERE customer_id = ?
	LIMIT 1`

	row := r.conn.QueryRow(ctx, query, customerID)

	var one uint8
	if err := row.Scan(&one); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// MonthlySummaryFinal retrieves the monthly summary for a customer using FINAL modifier.
func (r *repository) MonthlySummaryFinal(ctx context.Context, customerID string, windowFrom, windowTo time.Time) (MonthlySummary, error) {
	windowToExclusive := windowTo.AddDate(0, 0, 1)
	query := `
	SELECT
		customer_id,
		? AS window_from,
		? AS window_to,
		count() AS order_count,
		sum(total_amount) AS total_spend,
		any(currency)     AS currency
	FROM orders_current FINAL
	WHERE customer_id = ?
		AND order_time >= ?
		AND order_time <  ?
	GROUP BY customer_id
	`

	row := r.conn.QueryRow(ctx, query, windowFrom, windowTo, customerID, windowFrom, windowToExclusive)

	var summary MonthlySummary
	err := row.Scan(
		&summary.CustomerID,
		&summary.WindowFrom,
		&summary.WindowTo,
		&summary.OrderCount,
		&summary.TotalSpend,
		&summary.Currency,
	)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return MonthlySummary{}, ErrMonthlySummaryNotFound
	} else if err != nil {
		return MonthlySummary{}, err
	}

	return summary, nil
}
