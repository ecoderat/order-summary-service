package main

import (
	"context"
	"errors"
	"os/signal"
	"syscall"
	"time"

	"order-summary-service/internal/cache"
	"order-summary-service/internal/config"
	"order-summary-service/internal/date"
	"order-summary-service/internal/db"
	"order-summary-service/internal/repository"

	"github.com/sirupsen/logrus"
)

const (
	dequeueTimeout = 5 * time.Second
	debounceDelay  = 500 * time.Millisecond
)

func main() {
	cfg := config.Load("cache-refresher")
	logger := logrus.StandardLogger()
	logger.WithField("service", cfg.ServiceName).Info("starting")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ch, err := db.New(ctx, &cfg)
	if err != nil {
		logger.WithError(err).Fatal("clickhouse connect error")
	}
	defer func() {
		if err := ch.Close(); err != nil {
			logger.WithError(err).Error("clickhouse close error")
		}
	}()

	repo := repository.NewRepository(ch)
	cacheClient := cache.New(cfg.RedisAddr, cfg.RedisDB)
	defer func() {
		if err := cacheClient.Close(); err != nil {
			logger.WithError(err).Error("cache close error")
		}
	}()

	for {
		select {
		case <-ctx.Done():
			logger.WithField("service", cfg.ServiceName).Info("shutting down")
			return
		default:
		}

		job, ok, err := cacheClient.DequeueRefreshJob(ctx, dequeueTimeout)
		if err != nil {
			logger.WithError(err).Error("refresh dequeue error")
			continue
		}
		if !ok {
			continue
		}

		if job.CustomerID == "" || job.AsOfDate == "" {
			logger.WithField("job", job).Error("invalid refresh job")
			continue
		}

		time.Sleep(debounceDelay)

		asOfDate, err := time.Parse("2006-01-02", job.AsOfDate)
		if err != nil {
			logger.WithField("as_of_date", job.AsOfDate).Error("invalid asof date")
			_ = cacheClient.ClearPending(context.Background(), job.CustomerID, job.AsOfDate)
			continue
		}

		exists, err := repo.CustomerExistsFinal(ctx, job.CustomerID)
		if err != nil {
			logger.WithError(err).WithField("customer_id", job.CustomerID).Error("customer exists error")
			_ = cacheClient.ClearPending(context.Background(), job.CustomerID, job.AsOfDate)
			continue
		}
		if !exists {
			logger.WithField("customer_id", job.CustomerID).Error("customer not found")
			_ = cacheClient.ClearPending(context.Background(), job.CustomerID, job.AsOfDate)
			continue
		}

		windowTo := date.ToStartOfDay(asOfDate)
		windowFrom := windowTo.AddDate(0, 0, -30)

		summary, err := repo.MonthlySummaryFinal(ctx, job.CustomerID, windowFrom, windowTo)
		found := true
		if err != nil && !errors.Is(err, repository.ErrMonthlySummaryNotFound) {
			logger.WithError(err).WithField("customer_id", job.CustomerID).Error("monthly summary error")
			_ = cacheClient.ClearPending(context.Background(), job.CustomerID, job.AsOfDate)
			continue
		} else if errors.Is(err, repository.ErrMonthlySummaryNotFound) {
			found = false
			summary = repository.MonthlySummary{}
		}

		entry := cache.MonthlySummaryEntry{
			CustomerID: job.CustomerID,
			WindowFrom: windowFrom,
			WindowTo:   windowTo,
			OrderCount: 0,
			TotalSpend: 0,
			Currency:   "",
		}
		if found {
			entry.OrderCount = summary.OrderCount
			entry.TotalSpend = summary.TotalSpend.InexactFloat64()
			entry.Currency = summary.Currency
		}

		payload, err := cache.EncodeMonthlySummary(entry)
		if err != nil {
			logger.WithError(err).WithField("customer_id", job.CustomerID).Error("cache encode error")
			_ = cacheClient.ClearPending(context.Background(), job.CustomerID, job.AsOfDate)
			continue
		}

		if err := cacheClient.CacheSet(ctx, job.CustomerID, job.AsOfDate, payload, cfg.CacheTTL); err != nil {
			logger.WithError(err).WithField("customer_id", job.CustomerID).Error("cache set error")
			_ = cacheClient.ClearPending(context.Background(), job.CustomerID, job.AsOfDate)
			continue
		}

		if err := cacheClient.ClearPending(context.Background(), job.CustomerID, job.AsOfDate); err != nil {
			logger.WithError(err).WithField("customer_id", job.CustomerID).Error("clear pending error")
		}

		logger.WithField("customer_id", job.CustomerID).WithField("as_of_date", job.AsOfDate).Info("refreshed cache")
	}
}
