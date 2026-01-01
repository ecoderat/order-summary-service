package mockservice

import (
"context"

"github.com/stretchr/testify/mock"

"order-summary-service/internal/service"
)

type MonthlySummaryService struct {
	mock.Mock
}

// Interface compliance check
var _ service.MonthlySummaryService = &MonthlySummaryService{}

func (m *MonthlySummaryService) GetMonthlySummary(ctx context.Context, customerID string) (service.MonthlySummary, error) {
	args := m.Called(ctx, customerID)
	if summary, ok := args.Get(0).(service.MonthlySummary); ok {
		return summary, args.Error(1)
	}
	return service.MonthlySummary{}, args.Error(1)
}
