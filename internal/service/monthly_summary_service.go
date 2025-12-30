package service

import (
	"context"
	"errors"
	"time"

	"github.com/sirupsen/logrus"

	"order-summary-service/internal/repository"
)

var ErrCustomerNotFound = errors.New("customer not found")

type MonthlySummaryService interface {
	GetMonthlySummary(ctx context.Context, customerID string) (MonthlySummary, error)
}

type MonthlySummary struct {
	CustomerID string
	WindowFrom time.Time
	WindowTo   time.Time
	OrderCount uint64
	TotalSpend float64
	Currency   string
}

type monthlySummaryService struct {
	repo repository.Repository
}

func NewMonthlySummaryService(repo repository.Repository) MonthlySummaryService {
	return &monthlySummaryService{repo: repo}
}

func (s *monthlySummaryService) GetMonthlySummary(ctx context.Context, customerID string) (MonthlySummary, error) {
	start := time.Now()
	logger := logrus.WithField("customer_id", customerID)

	exists, err := s.repo.CustomerExistsFinal(ctx, customerID)
	if err != nil {
		logger.WithError(err).Error("customer exists query failed")
		return MonthlySummary{}, err
	}
	if !exists {
		logger.Warn("customer not found")
		return MonthlySummary{}, ErrCustomerNotFound
	}

	windowTo := utcDate(time.Now().UTC())
	windowFrom := windowTo.AddDate(0, 0, -30)

	summary, found, err := s.repo.MonthlySummaryFinal(ctx, customerID)
	if err != nil {
		logger.WithError(err).Error("monthly summary query failed")
		return MonthlySummary{}, err
	}
	if !found {
		logger.WithFields(logrus.Fields{
			"window_from": windowFrom.Format("2006-01-02"),
			"window_to":   windowTo.Format("2006-01-02"),
			"duration_ms": time.Since(start).Milliseconds(),
		}).Info("monthly summary empty")
		return MonthlySummary{
			CustomerID: customerID,
			WindowFrom: windowFrom,
			WindowTo:   windowTo,
			OrderCount: 0,
			TotalSpend: 0,
			Currency:   "",
		}, nil
	}

	totalSpend := summary.TotalSpend.InexactFloat64()
	logger.WithFields(logrus.Fields{
		"order_count": summary.OrderCount,
		"total_spend": totalSpend,
		"currency":    summary.Currency,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Info("monthly summary ready")

	return MonthlySummary{
		CustomerID: summary.CustomerID,
		WindowFrom: windowFrom,
		WindowTo:   windowTo,
		OrderCount: summary.OrderCount,
		TotalSpend: totalSpend,
		Currency:   summary.Currency,
	}, nil
}

func utcDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
