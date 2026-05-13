package store

import (
	"context"
	"errors"

	"github.com/Skliar-Il/broker-message/tasks/task_4/internal/cache"
	"github.com/Skliar-Il/broker-message/tasks/task_4/internal/db"
)

// CacheAside implements the Lazy Loading / Cache-Aside / Write-Around strategy.
//
// Read path:  check cache → on miss, load from DB and populate cache.
// Write path: write directly to DB and invalidate the cache entry so the next
//
//	read re-fetches the fresh value (write-around).
type CacheAside struct {
	c *cache.Redis
	d *db.SQLite
}

func NewCacheAside(c *cache.Redis, d *db.SQLite) *CacheAside {
	return &CacheAside{c: c, d: d}
}

func (s *CacheAside) Get(ctx context.Context, key string) (string, error) {
	val, err := s.c.Get(ctx, key)
	if err == nil {
		return val, nil
	}
	if !errors.Is(err, cache.ErrMiss) {
		return "", err
	}
	// Cache miss: load from DB and populate cache.
	val, err = s.d.Get(ctx, key)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return "", ErrNotFound
		}
		return "", err
	}
	_ = s.c.Set(ctx, key, val)
	return val, nil
}

func (s *CacheAside) Set(ctx context.Context, key, value string) error {
	if err := s.d.Set(ctx, key, value); err != nil {
		return err
	}
	// Invalidate cache so the next read reloads from DB (write-around).
	_ = s.c.Del(ctx, key)
	return nil
}

func (s *CacheAside) Flush(_ context.Context) error { return nil }

func (s *CacheAside) Close() error {
	_ = s.c.Close()
	return s.d.Close()
}

// ErrNotFound is returned when a key does not exist in either cache or DB.
var ErrNotFound = errors.New("key not found")
