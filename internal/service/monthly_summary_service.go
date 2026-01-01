package service

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/sirupsen/logrus"

	"order-summary-service/internal/cache"
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
	Source     string
}

type monthlySummaryService struct {
	repo     repository.Repository
	cache    cache.Cache
	cacheTTL time.Duration
}

func NewMonthlySummaryService(repo repository.Repository, cacheClient cache.Cache, cacheTTL time.Duration) MonthlySummaryService {
	return &monthlySummaryService{repo: repo, cache: cacheClient, cacheTTL: cacheTTL}
}

func (s *monthlySummaryService) GetMonthlySummary(ctx context.Context, customerID string) (MonthlySummary, error) {
	start := time.Now()
	logger := logrus.WithField("customer_id", customerID)

	windowTo := utcDate(time.Now().UTC())
	windowFrom := windowTo.AddDate(0, 0, -30)
	dateKey := windowTo.Format("2006-01-02")

	if s.cache != nil {
		if payload, hit, err := s.cache.CacheGet(ctx, customerID, dateKey); err != nil {
			logger.WithError(err).Warn("cache get failed")
		} else if hit {
			cached, err := decodeCacheEntry(payload)
			if err != nil {
				logger.WithError(err).Warn("cache decode failed")
			} else {
				logger.WithField("duration_ms", time.Since(start).Milliseconds()).Info("monthly summary cache hit")
				return MonthlySummary{
					CustomerID: cached.CustomerID,
					WindowFrom: cached.WindowFrom,
					WindowTo:   cached.WindowTo,
					OrderCount: cached.OrderCount,
					TotalSpend: cached.TotalSpend,
					Currency:   cached.Currency,
					Source:     "cache",
				}, nil
			}
		}
	}

	exists, err := s.repo.CustomerExistsFinal(ctx, customerID)
	if err != nil {
		logger.WithError(err).Error("customer exists query failed")
		return MonthlySummary{}, err
	}
	if !exists {
		logger.Warn("customer not found")
		return MonthlySummary{}, ErrCustomerNotFound
	}

	summary, found, err := s.repo.MonthlySummaryFinal(ctx, customerID)
	if err != nil {
		logger.WithError(err).Error("monthly summary query failed")
		return MonthlySummary{}, err
	}
	if !found {
		result := MonthlySummary{
			CustomerID: customerID,
			WindowFrom: windowFrom,
			WindowTo:   windowTo,
			OrderCount: 0,
			TotalSpend: 0,
			Currency:   "",
			Source:     "db",
		}
		logger.WithFields(logrus.Fields{
			"window_from": windowFrom.Format("2006-01-02"),
			"window_to":   windowTo.Format("2006-01-02"),
			"duration_ms": time.Since(start).Milliseconds(),
		}).Info("monthly summary empty")
		s.cacheSet(customerID, dateKey, result, logger)
		return result, nil
	}

	totalSpend := summary.TotalSpend.InexactFloat64()
	result := MonthlySummary{
		CustomerID: summary.CustomerID,
		WindowFrom: windowFrom,
		WindowTo:   windowTo,
		OrderCount: summary.OrderCount,
		TotalSpend: totalSpend,
		Currency:   summary.Currency,
		Source:     "db",
	}
	logger.WithFields(logrus.Fields{
		"order_count": result.OrderCount,
		"total_spend": result.TotalSpend,
		"currency":    result.Currency,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Info("monthly summary ready")

	s.cacheSet(customerID, dateKey, result, logger)
	return result, nil
}

func utcDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

type cacheEntry struct {
	CustomerID string    `json:"customer_id"`
	WindowFrom time.Time `json:"window_from"`
	WindowTo   time.Time `json:"window_to"`
	OrderCount uint64    `json:"order_count"`
	TotalSpend float64   `json:"total_spend"`
	Currency   string    `json:"currency"`
}

func decodeCacheEntry(payload string) (cacheEntry, error) {
	var entry cacheEntry
	if err := json.Unmarshal([]byte(payload), &entry); err != nil {
		return cacheEntry{}, err
	}
	return entry, nil
}

func (s *monthlySummaryService) cacheSet(customerID, dateKey string, result MonthlySummary, logger *logrus.Entry) {
	if s.cache == nil {
		return
	}
	entry := cacheEntry{
		CustomerID: result.CustomerID,
		WindowFrom: result.WindowFrom,
		WindowTo:   result.WindowTo,
		OrderCount: result.OrderCount,
		TotalSpend: result.TotalSpend,
		Currency:   result.Currency,
	}
	payload, err := json.Marshal(entry)
	if err != nil {
		logger.WithError(err).Warn("cache encode failed")
		return
	}
	if err := s.cache.CacheSet(context.Background(), customerID, dateKey, string(payload), s.cacheTTL); err != nil {
		logger.WithError(err).Warn("cache set failed")
	}
}
