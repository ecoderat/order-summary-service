package service

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"order-summary-service/internal/cache"
	"order-summary-service/internal/date"
	"order-summary-service/internal/repository"
)

const (
	cacheBestEffortTimeout = 300 * time.Millisecond
	jitterMin              = 50 * time.Millisecond
	jitterMax              = 150 * time.Millisecond
)

var (
	ErrCustomerNotFound = errors.New("customer not found")
	seedOnce            sync.Once
)

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
	repo      repository.Repository
	cache     cache.Cache
	cacheTTL  time.Duration
	hotTTL    time.Duration
	lockTTL   time.Duration
	lockRetry int
	logger    *logrus.Logger
}

func NewMonthlySummaryService(repo repository.Repository, cacheClient cache.Cache, logger *logrus.Logger, cacheTTL, hotTTL, lockTTL time.Duration) MonthlySummaryService {
	if logger == nil {
		logger = logrus.StandardLogger()
	}
	if cacheTTL == 0 {
		cacheTTL = 28 * time.Hour
	}
	if hotTTL == 0 {
		hotTTL = 2 * time.Hour
	}
	if lockTTL == 0 {
		lockTTL = 5 * time.Second
	}
	return &monthlySummaryService{
		repo:      repo,
		cache:     cacheClient,
		cacheTTL:  cacheTTL,
		hotTTL:    hotTTL,
		lockTTL:   lockTTL,
		lockRetry: 2,
		logger:    logger,
	}
}

func (s *monthlySummaryService) GetMonthlySummary(ctx context.Context, customerID string) (MonthlySummary, error) {
	start := time.Now()
	log := s.logger.WithField("customer_id", customerID)

	nowUTC := time.Now().UTC()
	windowTo := date.ToStartOfDay(nowUTC)
	windowFrom := windowTo.AddDate(0, 0, -30)
	dateKey := cache.MonthlyDateString(windowTo)

	s.markHotBestEffort(ctx, customerID, dateKey, log)
	if result, ok := s.tryCache(ctx, customerID, dateKey, start, log, "monthly summary cache hit"); ok {
		return result, nil
	}

	cached, ok, release := s.acquireLockOrWaitForCache(ctx, customerID, dateKey, start, log)
	if release != nil {
		defer release()
	}

	if ok {
		return cached, nil
	}

	exists, err := s.repo.CustomerExistsFinal(ctx, customerID)
	if err != nil {
		log.WithError(err).Error("customer exists query failed")
		return MonthlySummary{}, err
	}

	if !exists {
		log.Warn("customer not found")
		return MonthlySummary{}, ErrCustomerNotFound
	}

	summary, err := s.repo.MonthlySummaryFinal(ctx, customerID, windowFrom, windowTo)
	if err != nil && !errors.Is(err, repository.ErrMonthlySummaryNotFound) {
		log.WithError(err).Error("monthly summary query failed")
		return MonthlySummary{}, err
	} else if err != nil {
		result := emptySummary(customerID, windowFrom, windowTo)
		log.WithFields(logrus.Fields{
			"window_from": windowFrom.Format("2006-01-02"),
			"window_to":   windowTo.Format("2006-01-02"),
			"duration_ms": time.Since(start).Milliseconds(),
		}).Info("monthly summary empty")
		s.setCacheBestEffort(customerID, dateKey, result, log)
		return result, nil
	}

	result := mapSummary(summary, windowFrom, windowTo)
	log.WithFields(logrus.Fields{
		"order_count": result.OrderCount,
		"total_spend": result.TotalSpend,
		"currency":    result.Currency,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Info("monthly summary ready")

	s.setCacheBestEffort(customerID, dateKey, result, log)
	return result, nil
}

func (s *monthlySummaryService) tryCache(ctx context.Context, customerID, dateKey string, start time.Time, log *logrus.Entry, message string) (MonthlySummary, bool) {
	payload, hit, err := s.cache.CacheGet(ctx, customerID, dateKey)
	if err != nil {
		log.WithError(err).Warn("cache get failed")
		return MonthlySummary{}, false
	}

	if !hit {
		return MonthlySummary{}, false
	}

	entry, err := cache.DecodeMonthlySummary(payload)
	if err != nil {
		log.WithError(err).Warn("cache decode failed")
		return MonthlySummary{}, false
	}

	log.WithField("duration_ms", time.Since(start).Milliseconds()).Info(message)
	return resultFromEntry(entry, "cache"), true
}

func (s *monthlySummaryService) acquireLockOrWaitForCache(ctx context.Context, customerID, dateKey string, start time.Time, log *logrus.Entry) (MonthlySummary, bool, func()) {
	locked, err := s.cache.AcquireMonthlyLock(ctx, customerID, dateKey, s.lockTTL)
	if err != nil {
		log.WithError(err).Warn("lock acquire failed")
		return MonthlySummary{}, false, nil
	}

	if locked {
		return MonthlySummary{}, false, func() {
			s.releaseLockBestEffort(customerID, dateKey, log)
		}
	}

	for i := 0; i < s.lockRetry; i++ {
		if !jitterSleep(ctx) {
			return MonthlySummary{}, false, nil
		}
		if result, ok := s.tryCache(ctx, customerID, dateKey, start, log, "monthly summary cache hit after retry"); ok {
			return result, true, nil
		}
	}

	return MonthlySummary{}, false, nil
}

func (s *monthlySummaryService) markHotBestEffort(ctx context.Context, customerID, dateKey string, log *logrus.Entry) {
	if err := s.cache.MarkMonthlyHot(ctx, customerID, dateKey, s.hotTTL); err != nil {
		log.WithError(err).Warn("hot marker set failed")
	}
}

func (s *monthlySummaryService) setCacheBestEffort(customerID, dateKey string, result MonthlySummary, log *logrus.Entry) {
	entry := cache.MonthlySummaryEntry{
		CustomerID: result.CustomerID,
		WindowFrom: result.WindowFrom,
		WindowTo:   result.WindowTo,
		OrderCount: result.OrderCount,
		TotalSpend: result.TotalSpend,
		Currency:   result.Currency,
	}

	payload, err := cache.EncodeMonthlySummary(entry)
	if err != nil {
		log.WithError(err).Warn("cache encode failed")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), cacheBestEffortTimeout)
	defer cancel()

	if err := s.cache.CacheSet(ctx, customerID, dateKey, payload, s.cacheTTL); err != nil {
		log.WithError(err).Warn("cache set failed")
	}
}

func (s *monthlySummaryService) releaseLockBestEffort(customerID, dateKey string, log *logrus.Entry) {
	ctx, cancel := context.WithTimeout(context.Background(), cacheBestEffortTimeout)
	defer cancel()

	if err := s.cache.ReleaseMonthlyLock(ctx, customerID, dateKey); err != nil {
		log.WithError(err).Warn("lock release failed")
	}
}

func emptySummary(customerID string, windowFrom, windowTo time.Time) MonthlySummary {
	return MonthlySummary{
		CustomerID: customerID,
		WindowFrom: windowFrom,
		WindowTo:   windowTo,
		OrderCount: 0,
		TotalSpend: 0,
		Currency:   "",
		Source:     "db",
	}
}

func mapSummary(summary repository.MonthlySummary, windowFrom, windowTo time.Time) MonthlySummary {
	return MonthlySummary{
		CustomerID: summary.CustomerID,
		WindowFrom: windowFrom,
		WindowTo:   windowTo,
		OrderCount: summary.OrderCount,
		TotalSpend: summary.TotalSpend.InexactFloat64(),
		Currency:   summary.Currency,
		Source:     "db",
	}
}

func resultFromEntry(entry cache.MonthlySummaryEntry, source string) MonthlySummary {
	return MonthlySummary{
		CustomerID: entry.CustomerID,
		WindowFrom: entry.WindowFrom,
		WindowTo:   entry.WindowTo,
		OrderCount: entry.OrderCount,
		TotalSpend: entry.TotalSpend,
		Currency:   entry.Currency,
		Source:     source,
	}
}

func jitterSleep(ctx context.Context) bool {
	duration := jitterMin + time.Duration(rand.Intn(int(jitterMax-jitterMin)+1))
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
