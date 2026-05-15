package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/Skliar-Il/broker-message/tasks/task_4/internal/metrics"
	_ "modernc.org/sqlite"
)

// SQLite wraps a SQLite database and increments Metrics counters on each operation.
type SQLite struct {
	db      *sql.DB
	metrics *metrics.M
}

// NewSQLite opens (or creates) a SQLite database at path and initialises the kv table.
func NewSQLite(path string, m *metrics.M) (*SQLite, error) {
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// WAL gives better read/write concurrency.
	if _, err := database.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;`); err != nil {
		return nil, fmt.Errorf("pragma: %w", err)
	}
	if _, err := database.Exec(`CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}
	return &SQLite{db: database, metrics: m}, nil
}

// Get returns the value for key. Returns ("", ErrNotFound) when absent.
func (s *SQLite) Get(ctx context.Context, key string) (string, error) {
	s.metrics.DBGets.Add(1)
	var val string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM kv WHERE key = ?`, key).Scan(&val)
	if err == sql.ErrNoRows {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	return val, nil
}

// Set inserts or updates key → value.
func (s *SQLite) Set(ctx context.Context, key, value string) error {
	s.metrics.DBSets.Add(1)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key, value,
	)
	return err
}

// BatchSet inserts or updates multiple key-value pairs in a single transaction.
func (s *SQLite) BatchSet(ctx context.Context, pairs map[string]string) error {
	if len(pairs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	// Use multi-row UPSERT statements to persist many keys with fewer DB executions.
	const maxPairsPerStmt = 400 // 400 * 2 placeholders keeps us safely under SQLite limits.
	keys := make([]string, 0, len(pairs))
	vals := make([]string, 0, len(pairs))
	for k, v := range pairs {
		keys = append(keys, k)
		vals = append(vals, v)
	}

	for start := 0; start < len(keys); start += maxPairsPerStmt {
		end := start + maxPairsPerStmt
		if end > len(keys) {
			end = len(keys)
		}

		var b strings.Builder
		b.WriteString(`INSERT INTO kv (key, value) VALUES `)

		args := make([]any, 0, (end-start)*2)
		for i := start; i < end; i++ {
			if i > start {
				b.WriteString(", ")
			}
			b.WriteString("(?, ?)")
			args = append(args, keys[i], vals[i])
		}
		b.WriteString(` ON CONFLICT(key) DO UPDATE SET value = excluded.value`)

		if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
			_ = tx.Rollback()
			return err
		}
		// BatchSet counts physical DB write statements, not per-key updates.
		s.metrics.DBSets.Add(1)
	}
	return tx.Commit()
}

// Reset removes all rows from the kv table.
func (s *SQLite) Reset(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM kv`)
	return err
}

// Close closes the underlying database.
func (s *SQLite) Close() error { return s.db.Close() }

// ErrNotFound is returned when a key is not present in the DB.
var ErrNotFound = fmt.Errorf("key not found in db")
