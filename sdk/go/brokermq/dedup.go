package brokermq

import (
	"sync"

	"github.com/google/uuid"
)

// SeenCache drops duplicate deliveries by server message id.
type SeenCache struct {
	mu   sync.Mutex
	cap  int
	keys []uuid.UUID
	set  map[uuid.UUID]struct{}
}

func NewSeenCache(capacity int) *SeenCache {
	if capacity < 1024 {
		capacity = 1024
	}
	return &SeenCache{cap: capacity, set: make(map[uuid.UUID]struct{}, capacity)}
}

func (c *SeenCache) Seen(id uuid.UUID) bool {
	if id == uuid.Nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.set[id]; ok {
		return true
	}
	c.set[id] = struct{}{}
	c.keys = append(c.keys, id)
	if len(c.keys) > c.cap {
		oldest := c.keys[0]
		c.keys = c.keys[1:]
		delete(c.set, oldest)
	}
	return false
}
