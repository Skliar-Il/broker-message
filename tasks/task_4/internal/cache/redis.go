package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Skliar-Il/broker-message/tasks/task_4/internal/metrics"
	"github.com/redis/go-redis/v9"
)

// Redis wraps a Redis client and increments Metrics counters on each operation.
type Redis struct {
	rdb     *redis.Client
	metrics *metrics.M
	ttl     time.Duration
}

// NewRedis creates a Redis cache wrapper. ttl is the expiration for cached values.
func NewRedis(ctx context.Context, url string, m *metrics.M, ttl time.Duration) (*Redis, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	rdb := redis.NewClient(opt)
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Redis{rdb: rdb, metrics: m, ttl: ttl}, nil
}

// Get returns the value for key. Returns ("", ErrMiss) on a cache miss.
func (r *Redis) Get(ctx context.Context, key string) (string, error) {
	val, err := r.rdb.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		r.metrics.CacheMisses.Add(1)
		return "", ErrMiss
	}
	if err != nil {
		r.metrics.CacheMisses.Add(1)
		return "", err
	}
	r.metrics.CacheHits.Add(1)
	return val, nil
}

// Set stores key → value with the configured TTL.
func (r *Redis) Set(ctx context.Context, key, value string) error {
	return r.rdb.Set(ctx, key, value, r.ttl).Err()
}

// Del removes key from the cache.
func (r *Redis) Del(ctx context.Context, key string) error {
	return r.rdb.Del(ctx, key).Err()
}

// FlushAll removes all keys (used for prefill reset between runs).
func (r *Redis) FlushAll(ctx context.Context) error {
	return r.rdb.FlushAll(ctx).Err()
}

// Close closes the underlying connection.
func (r *Redis) Close() error { return r.rdb.Close() }

// ErrMiss is returned when a key is not present in the cache.
var ErrMiss = errors.New("cache miss")
