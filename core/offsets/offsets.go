package offsets

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/Skliar-Il/broker-message/core/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

const offsetKeyPrefix = "offset:"

type Offsets struct {
	mu      sync.Mutex
	offsets map[string]uint64
	db      *storage.Badger
	log     zerolog.Logger
}

func NewOffsets(db *storage.Badger, log zerolog.Logger) *Offsets {
	return &Offsets{
		offsets: make(map[string]uint64),
		db:      db,
		log:     log.With().Str("component", "offsets").Logger(),
	}
}

func buildKey(groupID string) string {
	return fmt.Sprintf("%s%s", offsetKeyPrefix, groupID)
}

func (o *Offsets) LoadFromStorage() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.db.Scan([]byte(offsetKeyPrefix), func(key, value []byte) error {
		if len(value) < 8 {
			return nil
		}
		o.offsets[string(key)] = binary.BigEndian.Uint64(value)
		return nil
	})
}

func (o *Offsets) Get(groupID string) uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.offsets[buildKey(groupID)]
}

func (o *Offsets) Set(groupID string, offset uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	key := buildKey(groupID)
	o.offsets[key] = offset
	if err := o.persist(key, offset); err != nil {
		o.log.Error().Err(err).Str("key", key).Msg("offsets: persist set failed")
	}
}

func (o *Offsets) Inc(groupID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	key := buildKey(groupID)
	o.offsets[key]++
	if err := o.persist(key, o.offsets[key]); err != nil {
		o.log.Error().Err(err).Str("key", key).Msg("offsets: persist inc failed")
	}
}

func (o *Offsets) Delete(groupID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	key := buildKey(groupID)
	delete(o.offsets, key)
	if err := o.db.Delete([]byte(key)); err != nil {
		o.log.Error().Err(err).Str("key", key).Msg("offsets: persist delete failed")
	}
}

func (o *Offsets) persist(key string, val uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, val)
	return errors.Wrap(o.db.Set([]byte(key), buf), "offsets: set key")
}
