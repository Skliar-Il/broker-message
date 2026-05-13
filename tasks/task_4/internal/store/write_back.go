package store

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Skliar-Il/broker-message/tasks/task_4/internal/cache"
	"github.com/Skliar-Il/broker-message/tasks/task_4/internal/db"
	"github.com/Skliar-Il/broker-message/tasks/task_4/internal/metrics"
)

// WriteBack implements the Write-Back (write-behind) caching strategy.
//
// Read path:  check cache → on miss, load from DB and populate cache.
// Write path: write only to cache and add the key to an in-memory dirty set.
//
// A background flusher goroutine persists dirty entries to the DB:
//   - on every FlushInterval tick, OR
//   - whenever the dirty set reaches BatchSize.
type WriteBack struct {
	c       *cache.Redis
	d       *db.SQLite
	metrics *metrics.M

	mu            sync.Mutex
	dirty         map[string]string
	flushInterval time.Duration
	batchSize     int

	stopCh chan struct{}
	doneCh chan struct{}
}

// WriteBackConfig holds tunable parameters for the flusher goroutine.
type WriteBackConfig struct {
	FlushInterval time.Duration
	BatchSize     int
}

func NewWriteBack(c *cache.Redis, d *db.SQLite, m *metrics.M, cfg WriteBackConfig) *WriteBack {
	wb := &WriteBack{
		c:             c,
		d:             d,
		metrics:       m,
		dirty:         make(map[string]string),
		flushInterval: cfg.FlushInterval,
		batchSize:     cfg.BatchSize,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
	go wb.flusher()
	return wb
}

func (s *WriteBack) Get(ctx context.Context, key string) (string, error) {
	val, err := s.c.Get(ctx, key)
	if err == nil {
		return val, nil
	}
	if !errors.Is(err, cache.ErrMiss) {
		return "", err
	}
	// Check dirty set before hitting the DB (value may not be flushed yet).
	s.mu.Lock()
	v, ok := s.dirty[key]
	s.mu.Unlock()
	if ok {
		// Count dirty-set read as a cache hit since the value came from in-memory buffer.
		s.metrics.CacheHits.Add(1)
		return v, nil
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

func (s *WriteBack) Set(ctx context.Context, key, value string) error {
	if err := s.c.Set(ctx, key, value); err != nil {
		return err
	}
	s.mu.Lock()
	s.dirty[key] = value
	dirtyLen := int64(len(s.dirty))
	s.mu.Unlock()

	// Track peak dirty size.
	for {
		cur := s.metrics.WBMaxDirty.Load()
		if dirtyLen <= cur {
			break
		}
		if s.metrics.WBMaxDirty.CompareAndSwap(cur, dirtyLen) {
			break
		}
	}

	// Trigger flush if batch threshold is reached.
	if int(dirtyLen) >= s.batchSize {
		s.triggerFlush(ctx)
	}
	return nil
}

// Flush persists all remaining dirty entries synchronously.
func (s *WriteBack) Flush(ctx context.Context) error {
	return s.flushDirty(ctx)
}

// DirtyLen returns the current number of unflushed dirty keys.
func (s *WriteBack) DirtyLen() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.dirty))
}

func (s *WriteBack) Close() error {
	close(s.stopCh)
	<-s.doneCh
	_ = s.flushDirty(context.Background())
	_ = s.c.Close()
	return s.d.Close()
}

// flusher is the background goroutine that drains dirty on a timer.
func (s *WriteBack) flusher() {
	defer close(s.doneCh)
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			_ = s.flushDirty(context.Background())
		}
	}
}

// triggerFlush performs a non-blocking flush outside the ticker.
func (s *WriteBack) triggerFlush(ctx context.Context) {
	go func() { _ = s.flushDirty(ctx) }()
}

func (s *WriteBack) flushDirty(ctx context.Context) error {
	s.mu.Lock()
	if len(s.dirty) == 0 {
		s.mu.Unlock()
		return nil
	}
	// Snapshot and reset dirty set atomically.
	batch := s.dirty
	s.dirty = make(map[string]string, len(batch))
	s.mu.Unlock()

	return s.d.BatchSet(ctx, batch)
}
