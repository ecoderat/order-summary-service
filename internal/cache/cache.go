package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	idemKeyPrefix  = "idem:event:"
	cacheKeyPrefix = "cache:monthly:"
)

type Cache interface {
	CheckAndMarkEvent(ctx context.Context, eventID string, ttl time.Duration) (bool, error)
	UnmarkEvent(ctx context.Context, eventID string) error
	InvalidateMonthly(ctx context.Context, customerID, dateUTC string) error
	CacheGet(ctx context.Context, customerID, dateUTC string) (string, bool, error)
	CacheSet(ctx context.Context, customerID, dateUTC, payload string, ttl time.Duration) error
	Close() error
}

type cache struct {
	rdb *redis.Client
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
	key := CacheKeyMonthly(customerID, dateUTC)
	return c.rdb.Del(ctx, key).Err()
}

func CacheKeyMonthly(customerID, dateUTC string) string {
	return cacheKeyPrefix + customerID + ":" + dateUTC
}

func (c *cache) CacheGet(ctx context.Context, customerID, dateUTC string) (string, bool, error) {
	key := CacheKeyMonthly(customerID, dateUTC)
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
	key := CacheKeyMonthly(customerID, dateUTC)
	return c.rdb.Set(ctx, key, payload, ttl).Err()
}
