package dedup

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/Skliar-Il/broker-message/core/storage"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

const dedupKeyPrefix = "dedup:"

// Store tracks recently seen idempotency keys (producer-side dedup).
type Store struct {
	mu       sync.Mutex
	lru      map[uuid.UUID]dedupEntry
	order    []uuid.UUID
	capacity int
	ttl      time.Duration
	db       *storage.Badger
	log      zerolog.Logger
}

type dedupEntry struct {
	serverMsgID uuid.UUID
	seq         uint64
	expiresAt   time.Time
}

// New creates a dedup store backed by optional Badger persistence.
func New(db *storage.Badger, capacity int, ttl time.Duration, log zerolog.Logger) *Store {
	if capacity < 1024 {
		capacity = 1024
	}
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	return &Store{
		lru:      make(map[uuid.UUID]dedupEntry, capacity),
		order:    make([]uuid.UUID, 0, capacity),
		capacity: capacity,
		ttl:      ttl,
		db:       db,
		log:      log.With().Str("component", "dedup").Logger(),
	}
}

// Check returns a prior entry if idempotency key was already processed.
func (s *Store) Check(idempotencyID uuid.UUID) (dedupEntry, bool) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpiredLocked(now)
	if e, ok := s.lru[idempotencyID]; ok {
		return e, true
	}
	if s.db != nil {
		if e, ok := s.loadFromDB(idempotencyID); ok {
			s.lru[idempotencyID] = e
			return e, true
		}
	}
	return dedupEntry{}, false
}

// Remember records a successful publish for idempotency deduplication.
func (s *Store) Remember(idempotencyID, serverMsgID uuid.UUID, seq uint64) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	e := dedupEntry{
		serverMsgID: serverMsgID,
		seq:         seq,
		expiresAt:   now.Add(s.ttl),
	}
	s.lru[idempotencyID] = e
	s.order = append(s.order, idempotencyID)
	s.persistLocked(idempotencyID, e)
	for len(s.order) > s.capacity {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.lru, oldest)
		if s.db != nil {
			_ = s.db.Delete(dedupDBKey(oldest))
		}
	}
}

// SeenOrRemember returns (entry, duplicate=true) if idempotency key was already processed.
// On first sight it records serverMsgID and seq for the topic message.
func (s *Store) SeenOrRemember(idempotencyID, serverMsgID uuid.UUID, seq uint64) (dedupEntry, bool) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	s.evictExpiredLocked(now)

	if e, ok := s.lru[idempotencyID]; ok {
		return e, true
	}

	// Check persistent store
	if s.db != nil {
		if e, ok := s.loadFromDB(idempotencyID); ok {
			s.lru[idempotencyID] = e
			s.touchLocked(idempotencyID)
			return e, true
		}
	}

	e := dedupEntry{
		serverMsgID: serverMsgID,
		seq:         seq,
		expiresAt:   now.Add(s.ttl),
	}
	s.lru[idempotencyID] = e
	s.order = append(s.order, idempotencyID)
	s.persistLocked(idempotencyID, e)

	for len(s.order) > s.capacity {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.lru, oldest)
		if s.db != nil {
			_ = s.db.Delete(dedupDBKey(oldest))
		}
	}
	return e, false
}

func (s *Store) Size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.lru)
}

func (s *Store) touchLocked(id uuid.UUID) {
	for i, k := range s.order {
		if k == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			s.order = append(s.order, id)
			return
		}
	}
}

func (s *Store) evictExpiredLocked(now time.Time) {
	var kept []uuid.UUID
	for _, id := range s.order {
		e, ok := s.lru[id]
		if !ok || now.After(e.expiresAt) {
			delete(s.lru, id)
			if s.db != nil {
				_ = s.db.Delete(dedupDBKey(id))
			}
			continue
		}
		kept = append(kept, id)
	}
	s.order = kept
}

func dedupDBKey(id uuid.UUID) []byte {
	return append([]byte(dedupKeyPrefix), id[:]...)
}

func (s *Store) persistLocked(id uuid.UUID, e dedupEntry) {
	if s.db == nil {
		return
	}
	buf := make([]byte, 16+8+8)
	copy(buf[0:16], e.serverMsgID[:])
	binary.BigEndian.PutUint64(buf[16:24], e.seq)
	binary.BigEndian.PutUint64(buf[24:32], uint64(e.expiresAt.UnixNano()))
	if err := s.db.Set(dedupDBKey(id), buf); err != nil {
		s.log.Warn().Err(err).Msg("dedup: persist failed")
	}
}

func (s *Store) loadFromDB(id uuid.UUID) (dedupEntry, bool) {
	raw, err := s.db.Get(dedupDBKey(id))
	if err != nil {
		return dedupEntry{}, false
	}
	if len(raw) < 32 {
		return dedupEntry{}, false
	}
	var srv uuid.UUID
	copy(srv[:], raw[0:16])
	seq := binary.BigEndian.Uint64(raw[16:24])
	exp := time.Unix(0, int64(binary.BigEndian.Uint64(raw[24:32])))
	if time.Now().After(exp) {
		_ = s.db.Delete(dedupDBKey(id))
		return dedupEntry{}, false
	}
	return dedupEntry{serverMsgID: srv, seq: seq, expiresAt: exp}, true
}
