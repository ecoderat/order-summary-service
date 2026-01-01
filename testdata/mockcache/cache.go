package mockcache

import (
	"context"
	"order-summary-service/internal/cache"
	"time"

	"github.com/stretchr/testify/mock"
)

type Cache struct {
	mock.Mock
}

// Interface compliance check
var _ cache.Cache = &Cache{}

func (m *Cache) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *Cache) CheckAndMarkEvent(ctx context.Context, eventID string, ttl time.Duration) (bool, error) {
	args := m.Called(ctx, eventID, ttl)
	return args.Bool(0), args.Error(1)
}

func (m *Cache) UnmarkEvent(ctx context.Context, eventID string) error {
	args := m.Called(ctx, eventID)
	return args.Error(0)
}

func (m *Cache) InvalidateMonthly(ctx context.Context, customerID, dateUTC string) error {
	args := m.Called(ctx, customerID, dateUTC)
	return args.Error(0)
}

func (m *Cache) CacheGet(ctx context.Context, customerID, dateUTC string) (string, bool, error) {
	args := m.Called(ctx, customerID, dateUTC)
	if val, ok := args.Get(0).(string); ok {
		return val, args.Bool(1), args.Error(2)
	}
	return "", args.Bool(1), args.Error(2)
}

func (m *Cache) CacheSet(ctx context.Context, customerID, dateUTC, payload string, ttl time.Duration) error {
	args := m.Called(ctx, customerID, dateUTC, payload, ttl)
	return args.Error(0)
}

func (m *Cache) MarkMonthlyHot(ctx context.Context, customerID, dateUTC string, ttl time.Duration) error {
	args := m.Called(ctx, customerID, dateUTC, ttl)
	return args.Error(0)
}

func (m *Cache) IsMonthlyHot(ctx context.Context, customerID, dateUTC string) (bool, error) {
	args := m.Called(ctx, customerID, dateUTC)
	return args.Bool(0), args.Error(1)
}

func (m *Cache) TrySetPending(ctx context.Context, customerID, dateUTC string, ttl time.Duration) (bool, error) {
	args := m.Called(ctx, customerID, dateUTC, ttl)
	return args.Bool(0), args.Error(1)
}

func (m *Cache) ClearPending(ctx context.Context, customerID, dateUTC string) error {
	args := m.Called(ctx, customerID, dateUTC)
	return args.Error(0)
}

func (m *Cache) EnqueueRefreshJob(ctx context.Context, job cache.RefreshJob) error {
	args := m.Called(ctx, job)
	return args.Error(0)
}

func (m *Cache) DequeueRefreshJob(ctx context.Context, timeout time.Duration) (cache.RefreshJob, bool, error) {
	args := m.Called(ctx, timeout)
	if job, ok := args.Get(0).(cache.RefreshJob); ok {
		return job, args.Bool(1), args.Error(2)
	}
	return cache.RefreshJob{}, args.Bool(1), args.Error(2)
}

func (m *Cache) AcquireMonthlyLock(ctx context.Context, customerID, dateUTC string, ttl time.Duration) (bool, error) {
	args := m.Called(ctx, customerID, dateUTC, ttl)
	return args.Bool(0), args.Error(1)
}

func (m *Cache) ReleaseMonthlyLock(ctx context.Context, customerID, dateUTC string) error {
	args := m.Called(ctx, customerID, dateUTC)
	return args.Error(0)
}
