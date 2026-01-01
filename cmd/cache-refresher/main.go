package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"order-summary-service/internal/cache"
	"order-summary-service/internal/config"
	"order-summary-service/internal/db"
	"order-summary-service/internal/repository"
)

const (
	dequeueTimeout = 5 * time.Second
	debounceDelay  = 500 * time.Millisecond
)

func main() {
	cfg := config.Load("cache-refresher")
	log.Printf("starting %s", cfg.ServiceName)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ch, err := db.New(ctx, &cfg)
	if err != nil {
		log.Fatalf("clickhouse connect error: %v", err)
	}
	defer func() {
		if err := ch.Close(); err != nil {
			log.Printf("clickhouse close error: %v", err)
		}
	}()

	repo := repository.NewRepository(ch)
	cacheClient := cache.New(cfg.RedisAddr, cfg.RedisDB)
	defer func() {
		if err := cacheClient.Close(); err != nil {
			log.Printf("cache close error: %v", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Printf("shutting down %s", cfg.ServiceName)
			return
		default:
		}

		job, ok, err := cacheClient.DequeueRefreshJob(ctx, dequeueTimeout)
		if err != nil {
			log.Printf("refresh dequeue error: %v", err)
			continue
		}
		if !ok {
			continue
		}

		if job.CustomerID == "" || job.AsOfDate == "" {
			log.Printf("invalid refresh job: %+v", job)
			continue
		}

		time.Sleep(debounceDelay)

		asOfDate, err := time.Parse("2006-01-02", job.AsOfDate)
		if err != nil {
			log.Printf("invalid asof date %s", job.AsOfDate)
			_ = cacheClient.ClearPending(context.Background(), job.CustomerID, job.AsOfDate)
			continue
		}

		exists, err := repo.CustomerExistsFinal(ctx, job.CustomerID)
		if err != nil {
			log.Printf("customer exists error customer_id=%s err=%v", job.CustomerID, err)
			_ = cacheClient.ClearPending(context.Background(), job.CustomerID, job.AsOfDate)
			continue
		}
		if !exists {
			log.Printf("customer not found customer_id=%s", job.CustomerID)
			_ = cacheClient.ClearPending(context.Background(), job.CustomerID, job.AsOfDate)
			continue
		}

		windowTo := utcDate(asOfDate)
		windowFrom := windowTo.AddDate(0, 0, -30)

		summary, found, err := repo.MonthlySummaryFinal(ctx, job.CustomerID, windowFrom, windowTo)
		if err != nil {
			log.Printf("monthly summary error customer_id=%s err=%v", job.CustomerID, err)
			_ = cacheClient.ClearPending(context.Background(), job.CustomerID, job.AsOfDate)
			continue
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
			log.Printf("cache encode error customer_id=%s err=%v", job.CustomerID, err)
			_ = cacheClient.ClearPending(context.Background(), job.CustomerID, job.AsOfDate)
			continue
		}

		if err := cacheClient.CacheSet(ctx, job.CustomerID, job.AsOfDate, payload, cfg.CacheTTL); err != nil {
			log.Printf("cache set error customer_id=%s err=%v", job.CustomerID, err)
			_ = cacheClient.ClearPending(context.Background(), job.CustomerID, job.AsOfDate)
			continue
		}

		if err := cacheClient.ClearPending(context.Background(), job.CustomerID, job.AsOfDate); err != nil {
			log.Printf("clear pending error customer_id=%s err=%v", job.CustomerID, err)
		}

		log.Printf("refreshed cache customer_id=%s asof=%s", job.CustomerID, job.AsOfDate)
	}
}

func utcDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
