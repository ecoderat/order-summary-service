package cache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	idemKeyPrefix    = "idem:event:"
	cacheKeyPrefix   = "cache:monthly:"
	hotKeyPrefix     = "hot:monthly:"
	pendingKeyPrefix = "refresh_pending:monthly:"
	lockKeyPrefix    = "lock:monthly:"
	refreshQueueKey  = "refresh_queue:monthly"
	dateLayout       = "2006-01-02"
)

type Cache interface {
	CheckAndMarkEvent(ctx context.Context, eventID string, ttl time.Duration) (bool, error)
	UnmarkEvent(ctx context.Context, eventID string) error
	InvalidateMonthly(ctx context.Context, customerID, dateUTC string) error
	CacheGet(ctx context.Context, customerID, dateUTC string) (string, bool, error)
	CacheSet(ctx context.Context, customerID, dateUTC, payload string, ttl time.Duration) error
	MarkMonthlyHot(ctx context.Context, customerID, dateUTC string, ttl time.Duration) error
	IsMonthlyHot(ctx context.Context, customerID, dateUTC string) (bool, error)
	TrySetPending(ctx context.Context, customerID, dateUTC string, ttl time.Duration) (bool, error)
	ClearPending(ctx context.Context, customerID, dateUTC string) error
	EnqueueRefreshJob(ctx context.Context, job RefreshJob) error
	DequeueRefreshJob(ctx context.Context, timeout time.Duration) (RefreshJob, bool, error)
	AcquireMonthlyLock(ctx context.Context, customerID, dateUTC string, ttl time.Duration) (bool, error)
	ReleaseMonthlyLock(ctx context.Context, customerID, dateUTC string) error
	Close() error
}

type cache struct {
	rdb *redis.Client
}

type RefreshJob struct {
	CustomerID string `json:"customer_id"`
	AsOfDate   string `json:"asof_date"`
}

type MonthlySummaryEntry struct {
	CustomerID string    `json:"customer_id"`
	WindowFrom time.Time `json:"window_from"`
	WindowTo   time.Time `json:"window_to"`
	OrderCount uint64    `json:"order_count"`
	TotalSpend float64   `json:"total_spend"`
	Currency   string    `json:"currency"`
}

func New(addr string, db int) *cache {
	return &cache{
		rdb: redis.NewClient(&redis.Options{
			Addr: addr,
			DB:   db,
		}),
	}
}

func (c *cache) Close() error {
	return c.rdb.Close()
}

func (c *cache) CheckAndMarkEvent(ctx context.Context, eventID string, ttl time.Duration) (bool, error) {
	key := idemKeyPrefix + eventID
	set, err := c.rdb.SetNX(ctx, key, "1", ttl).Result()
	if err != nil {
		return false, err
	}
	return set, nil
}

func (c *cache) UnmarkEvent(ctx context.Context, eventID string) error {
	key := idemKeyPrefix + eventID
	return c.rdb.Del(ctx, key).Err()
}

func (c *cache) InvalidateMonthly(ctx context.Context, customerID, dateUTC string) error {
	key := MonthlyCacheKey(customerID, dateUTC)
	return c.rdb.Del(ctx, key).Err()
}

func MonthlyCacheKey(customerID, dateUTC string) string {
	return cacheKeyPrefix + customerID + ":" + dateUTC
}

func MonthlyHotKey(customerID, dateUTC string) string {
	return hotKeyPrefix + customerID + ":" + dateUTC
}

func MonthlyPendingKey(customerID, dateUTC string) string {
	return pendingKeyPrefix + customerID + ":" + dateUTC
}

func MonthlyLockKey(customerID, dateUTC string) string {
	return lockKeyPrefix + customerID + ":" + dateUTC
}

func MonthlyDateString(t time.Time) string {
	return t.UTC().Format(dateLayout)
}

func (c *cache) CacheGet(ctx context.Context, customerID, dateUTC string) (string, bool, error) {
	key := MonthlyCacheKey(customerID, dateUTC)
	val, err := c.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return val, true, nil
}

func (c *cache) CacheSet(ctx context.Context, customerID, dateUTC, payload string, ttl time.Duration) error {
	key := MonthlyCacheKey(customerID, dateUTC)
	return c.rdb.Set(ctx, key, payload, ttl).Err()
}

func (c *cache) MarkMonthlyHot(ctx context.Context, customerID, dateUTC string, ttl time.Duration) error {
	key := MonthlyHotKey(customerID, dateUTC)
	return c.rdb.Set(ctx, key, "1", ttl).Err()
}

func (c *cache) IsMonthlyHot(ctx context.Context, customerID, dateUTC string) (bool, error) {
	key := MonthlyHotKey(customerID, dateUTC)
	exists, err := c.rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return exists == 1, nil
}

func (c *cache) TrySetPending(ctx context.Context, customerID, dateUTC string, ttl time.Duration) (bool, error) {
	key := MonthlyPendingKey(customerID, dateUTC)
	set, err := c.rdb.SetNX(ctx, key, "1", ttl).Result()
	if err != nil {
		return false, err
	}
	return set, nil
}

func (c *cache) ClearPending(ctx context.Context, customerID, dateUTC string) error {
	key := MonthlyPendingKey(customerID, dateUTC)
	return c.rdb.Del(ctx, key).Err()
}

func (c *cache) EnqueueRefreshJob(ctx context.Context, job RefreshJob) error {
	payload, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return c.rdb.LPush(ctx, refreshQueueKey, payload).Err()
}

func (c *cache) DequeueRefreshJob(ctx context.Context, timeout time.Duration) (RefreshJob, bool, error) {
	result, err := c.rdb.BRPop(ctx, timeout, refreshQueueKey).Result()
	if err == redis.Nil {
		return RefreshJob{}, false, nil
	}
	if err != nil {
		return RefreshJob{}, false, err
	}
	if len(result) != 2 {
		return RefreshJob{}, false, nil
	}
	var job RefreshJob
	if err := json.Unmarshal([]byte(result[1]), &job); err != nil {
		return RefreshJob{}, false, err
	}
	return job, true, nil
}

func (c *cache) AcquireMonthlyLock(ctx context.Context, customerID, dateUTC string, ttl time.Duration) (bool, error) {
	key := MonthlyLockKey(customerID, dateUTC)
	set, err := c.rdb.SetNX(ctx, key, "1", ttl).Result()
	if err != nil {
		return false, err
	}
	return set, nil
}

func (c *cache) ReleaseMonthlyLock(ctx context.Context, customerID, dateUTC string) error {
	key := MonthlyLockKey(customerID, dateUTC)
	return c.rdb.Del(ctx, key).Err()
}

func EncodeMonthlySummary(entry MonthlySummaryEntry) (string, error) {
	payload, err := json.Marshal(entry)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func DecodeMonthlySummary(payload string) (MonthlySummaryEntry, error) {
	var entry MonthlySummaryEntry
	if err := json.Unmarshal([]byte(payload), &entry); err != nil {
		return MonthlySummaryEntry{}, err
	}
	return entry, nil
}
