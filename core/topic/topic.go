package topic

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Skliar-Il/broker-message/core/offsets"
	"github.com/Skliar-Il/broker-message/core/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

const (
	msgKeyPrefix = "msg:"
	seqKey       = "meta:seq"
	subChanSize  = 256
	flushEvery   = 10
)

type Message struct {
	Seq       uint64
	Topic     string
	Payload   []byte
	Timestamp time.Time
}

type subscriber struct {
	ch chan *Message
}

type Topic struct {
	name    string
	seq     uint64
	mu      sync.RWMutex
	subs    []*subscriber
	db      *storage.Badger
	offsets *offsets.Offsets
	log     zerolog.Logger
}

func New(name string, dataDir string, log zerolog.Logger) (*Topic, error) {
	db, err := storage.OpenBadger(dataDir)
	if err != nil {
		return nil, errors.Wrapf(err, "topic %q: open storage", name)
	}

	offs := offsets.NewOffsets(db, flushEvery, log)
	if err := offs.LoadFromStorage(); err != nil {
		_ = db.Close()
		return nil, errors.Wrapf(err, "topic %q: load offsets", name)
	}

	t := &Topic{
		name:    name,
		db:      db,
		offsets: offs,
		log:     log.With().Str("topic", name).Logger(),
	}
	if err := t.loadSeq(); err != nil {
		t.log.Warn().Err(err).Msg("topic: could not restore seq, starting at 0")
	}
	return t, nil
}

func (t *Topic) Name() string              { return t.name }
func (t *Topic) CurrentSeq() uint64        { return atomic.LoadUint64(&t.seq) }
func (t *Topic) Offsets() *offsets.Offsets { return t.offsets }

func (t *Topic) Close() error {
	if err := t.offsets.Flush(); err != nil {
		t.log.Error().Err(err).Msg("topic: flush offsets on close")
	}
	return t.db.Close()
}

func (t *Topic) loadSeq() error {
	seq, err := t.db.GetUint64([]byte(seqKey))
	if err != nil {
		return err
	}
	atomic.StoreUint64(&t.seq, seq)
	t.log.Debug().Uint64("seq", seq).Msg("topic: seq restored")
	return nil
}

func (t *Topic) msgKey(seq uint64) []byte {
	return []byte(fmt.Sprintf("%s%016X", msgKeyPrefix, seq))
}

func (t *Topic) msgPrefix() []byte {
	return []byte(msgKeyPrefix)
}

func encodeMessage(m *Message) []byte {
	buf := make([]byte, 16+len(m.Payload))
	binary.BigEndian.PutUint64(buf[0:8], m.Seq)
	binary.BigEndian.PutUint64(buf[8:16], uint64(m.Timestamp.UnixNano()))
	copy(buf[16:], m.Payload)
	return buf
}

func decodeMessage(topicName string, raw []byte) (*Message, error) {
	if len(raw) < 16 {
		return nil, errors.New("decode message: buffer too short")
	}
	m := &Message{
		Seq:       binary.BigEndian.Uint64(raw[0:8]),
		Timestamp: time.Unix(0, int64(binary.BigEndian.Uint64(raw[8:16]))),
		Topic:     topicName,
		Payload:   make([]byte, len(raw)-16),
	}
	copy(m.Payload, raw[16:])
	return m, nil
}

func (t *Topic) Publish(payload []byte) error {
	seq := atomic.AddUint64(&t.seq, 1)

	msg := &Message{
		Seq:       seq,
		Topic:     t.name,
		Payload:   payload,
		Timestamp: time.Now(),
	}

	if err := t.db.Set(t.msgKey(seq), encodeMessage(msg)); err != nil {
		return errors.Wrap(err, "publish: persist message")
	}
	if err := t.db.SetUint64([]byte(seqKey), seq); err != nil {
		t.log.Warn().Err(err).Uint64("seq", seq).Msg("publish: persist seq key failed")
	}

	t.mu.RLock()
	for _, sub := range t.subs {
		select {
		case sub.ch <- msg:
		default:
			t.log.Warn().Uint64("seq", seq).Msg("publish: subscriber channel full, message dropped")
		}
	}
	t.mu.RUnlock()

	t.log.Debug().Uint64("seq", seq).Int("bytes", len(payload)).Msg("publish: ok")
	return nil
}

func (t *Topic) Subscribe() (<-chan *Message, func()) {
	sub := &subscriber{ch: make(chan *Message, subChanSize)}

	t.mu.Lock()
	t.subs = append(t.subs, sub)
	t.mu.Unlock()

	t.log.Debug().Msg("subscribe: subscriber added")

	unsub := func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		for i, s := range t.subs {
			if s == sub {
				t.subs = append(t.subs[:i], t.subs[i+1:]...)
				close(sub.ch)
				t.log.Debug().Msg("subscribe: subscriber removed")
				return
			}
		}
	}

	return sub.ch, unsub
}

func (t *Topic) GetMessages(fromSeq uint64, limit int) ([]*Message, error) {
	if limit <= 0 {
		return nil, nil
	}

	startKey := t.msgKey(fromSeq)
	prefix := t.msgPrefix()
	msgs := make([]*Message, 0, limit)

	err := t.db.ScanFrom(startKey, prefix, func(_, value []byte) error {
		m, err := decodeMessage(t.name, value)
		if err != nil {
			return err
		}
		msgs = append(msgs, m)
		if len(msgs) >= limit {
			return storage.ErrStopScan
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "get messages")
	}

	return msgs, nil
}
