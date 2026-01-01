package mockrepository

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"

	"order-summary-service/internal/repository"
)

type Repository struct {
	mock.Mock
}

// Interface compliance check
var _ repository.Repository = &Repository{}

func (m *Repository) CreateCustomerBatch(ctx context.Context, customers []repository.CreateCustomerParams) error {
	args := m.Called(ctx, customers)
	return args.Error(0)
}

func (m *Repository) CreateOrderBatch(ctx context.Context, orders []repository.CreateOrderParams) error {
	args := m.Called(ctx, orders)
	return args.Error(0)
}

func (m *Repository) MonthlySummaryFinal(ctx context.Context, customerID string, windowFrom, windowTo time.Time) (repository.MonthlySummary, error) {
	args := m.Called(ctx, customerID, windowFrom, windowTo)
	if summary, ok := args.Get(0).(repository.MonthlySummary); ok {
		return summary, args.Error(1)
	}
	return repository.MonthlySummary{}, args.Error(1)
}

func (m *Repository) CustomerExistsFinal(ctx context.Context, customerID string) (bool, error) {
	args := m.Called(ctx, customerID)
	return args.Bool(0), args.Error(1)
}
