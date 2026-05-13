// Package metrics provides shared atomic counters used by cache, db, and store packages.
package metrics

import "sync/atomic"

// M holds all benchmark counters. A single instance is created per run and
// passed to cache.Redis, db.SQLite, and the store constructors.
type M struct {
	CacheHits   atomic.Int64
	CacheMisses atomic.Int64
	DBGets      atomic.Int64
	DBSets      atomic.Int64
	// WBMaxDirty is the peak size of the Write-Back dirty set during a run.
	WBMaxDirty atomic.Int64
	// WBFinalDirty is the number of dirty keys remaining just before the final flush.
	WBFinalDirty atomic.Int64
}

// HitRate returns the cache hit rate in [0, 1].
func (m *M) HitRate() float64 {
	hits := m.CacheHits.Load()
	misses := m.CacheMisses.Load()
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}

// Reset zeroes all counters.
func (m *M) Reset() {
	m.CacheHits.Store(0)
	m.CacheMisses.Store(0)
	m.DBGets.Store(0)
	m.DBSets.Store(0)
	m.WBMaxDirty.Store(0)
	m.WBFinalDirty.Store(0)
}
