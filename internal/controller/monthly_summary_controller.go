package controller

import (
	"errors"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/sirupsen/logrus"

	"order-summary-service/internal/service"
)

type MonthlySummaryController interface {
	HandleFiber(c fiber.Ctx) error
}

type monthlySummaryController struct {
	svc    service.MonthlySummaryService
	logger *logrus.Logger
}

type monthlySummaryResponse struct {
	CustomerID string  `json:"customer_id"`
	WindowFrom string  `json:"window_from"`
	WindowTo   string  `json:"window_to"`
	OrderCount uint64  `json:"order_count"`
	TotalSpend float64 `json:"total_spend"`
	Currency   string  `json:"currency"`
	Source     string  `json:"source"`
}

func NewMonthlySummaryController(svc service.MonthlySummaryService, logger *logrus.Logger) MonthlySummaryController {
	if logger == nil {
		logger = logrus.StandardLogger()
	}
	return &monthlySummaryController{svc: svc, logger: logger}
}

func (c *monthlySummaryController) HandleFiber(ctx fiber.Ctx) error {
	start := time.Now()
	customerID := ctx.Params("customerId")
	logger := c.logger.WithFields(logrus.Fields{
		"method":      ctx.Method(),
		"path":        ctx.Path(),
		"customer_id": customerID,
		"request_id":  ctx.GetRespHeader("X-Request-ID"),
	})

	if customerID == "" {
		logger.Warn("missing customer id")
		return ctx.SendStatus(fiber.StatusNotFound)
	}

	summary, err := c.svc.GetMonthlySummary(ctx.Context(), customerID)
	if err != nil {
		if errors.Is(err, service.ErrCustomerNotFound) {
			logger.WithField("duration_ms", time.Since(start).Milliseconds()).Warn("customer not found")
			return ctx.SendStatus(fiber.StatusNotFound)
		}
		logger.WithError(err).WithField("duration_ms", time.Since(start).Milliseconds()).Error("monthly summary failed")
		return ctx.SendStatus(fiber.StatusInternalServerError)
	}

	resp := monthlySummaryResponse{
		CustomerID: summary.CustomerID,
		WindowFrom: formatDate(summary.WindowFrom),
		WindowTo:   formatDate(summary.WindowTo),
		OrderCount: summary.OrderCount,
		TotalSpend: summary.TotalSpend,
		Currency:   summary.Currency,
		Source:     "db",
	}

	logger.WithFields(logrus.Fields{
		"order_count": summary.OrderCount,
		"total_spend": summary.TotalSpend,
		"currency":    summary.Currency,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Info("monthly summary served")

	return ctx.JSON(resp)
}

func formatDate(value time.Time) string {
	return value.UTC().Format("2006-01-02")
}
