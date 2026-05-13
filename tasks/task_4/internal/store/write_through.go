package store

import (
	"context"
	"errors"

	"github.com/Skliar-Il/broker-message/tasks/task_4/internal/cache"
	"github.com/Skliar-Il/broker-message/tasks/task_4/internal/db"
)

// WriteThrough implements the Write-Through caching strategy.
//
// Read path:  check cache → on miss, load from DB and populate cache.
// Write path: write synchronously to both DB and cache, keeping them always in sync.
type WriteThrough struct {
	c *cache.Redis
	d *db.SQLite
}

func NewWriteThrough(c *cache.Redis, d *db.SQLite) *WriteThrough {
	return &WriteThrough{c: c, d: d}
}

func (s *WriteThrough) Get(ctx context.Context, key string) (string, error) {
	val, err := s.c.Get(ctx, key)
	if err == nil {
		return val, nil
	}
	if !errors.Is(err, cache.ErrMiss) {
		return "", err
	}
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

func (s *WriteThrough) Set(ctx context.Context, key, value string) error {
	// Write to DB first to ensure durability, then update cache.
	if err := s.d.Set(ctx, key, value); err != nil {
		return err
	}
	return s.c.Set(ctx, key, value)
}

func (s *WriteThrough) Flush(_ context.Context) error { return nil }

func (s *WriteThrough) Close() error {
	_ = s.c.Close()
	return s.d.Close()
}
