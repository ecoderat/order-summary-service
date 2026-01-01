package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v3"
	"github.com/sirupsen/logrus"

	"order-summary-service/internal/cache"
	"order-summary-service/internal/config"
	"order-summary-service/internal/controller"
	"order-summary-service/internal/db"
	"order-summary-service/internal/repository"
	"order-summary-service/internal/service"
)

func main() {
	cfg := config.Load("api-service")
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

	summaryService := service.NewMonthlySummaryService(repo, cacheClient, logger, cfg.CacheTTL, cfg.HotTTL, cfg.LockTTL)
	summaryController := controller.NewMonthlySummaryController(summaryService, logger)

	app := fiber.New()
	app.Get("/v1/customers/:customerId/monthly-order-summary", summaryController.GetMonthlySummary)

	go func() {
		<-ctx.Done()
		_ = app.Shutdown()
	}()

	logger.WithField("addr", cfg.HTTPAddr).Info("api listening")
	if err := app.Listen(cfg.HTTPAddr); err != nil {
		logger.WithError(err).Fatal("http server error")
	}
}
