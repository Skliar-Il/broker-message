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
	mu             sync.Mutex
	offsets        map[string]uint64
	pendingWrites  uint64
	flushThreshold uint64
	log            zerolog.Logger
	db             *storage.Badger
}

func NewOffsets(db *storage.Badger, flushThreshold uint64, log zerolog.Logger) *Offsets {
	if flushThreshold == 0 {
		flushThreshold = 10
	}
	return &Offsets{
		offsets:        make(map[string]uint64),
		db:             db,
		flushThreshold: flushThreshold,
		log:            log.With().Str("component", "offsets").Logger(),
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
	o.offsets[buildKey(groupID)] = offset
	o.maybeFlush()
}

func (o *Offsets) Inc(groupID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.offsets[buildKey(groupID)]++
	o.maybeFlush()
}

func (o *Offsets) Delete(groupID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.offsets, buildKey(groupID))
	o.maybeFlush()
}

func (o *Offsets) Flush() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.flush()
}

func (o *Offsets) maybeFlush() {
	o.pendingWrites++
	if o.pendingWrites >= o.flushThreshold {
		if err := o.flush(); err != nil {
			o.log.Error().Err(err).Msg("auto-flush failed")
		}
		o.pendingWrites = 0
	}
}

func (o *Offsets) flush() error {
	buf := make([]byte, 8)
	for key, val := range o.offsets {
		binary.BigEndian.PutUint64(buf, val)
		if err := o.db.Set([]byte(key), buf); err != nil {
			return errors.Wrap(err, "flush offset key")
		}
	}
	return nil
}
