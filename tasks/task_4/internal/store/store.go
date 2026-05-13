package store

import "context"

// Store is the unified interface for all three caching strategies.
type Store interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string) error
	// Flush persists any buffered writes to the DB (no-op for Cache-Aside and Write-Through).
	Flush(ctx context.Context) error
	Close() error
}
