// Package envelope defines the binary wire format embedded in MQTT PUBLISH payloads.
//
// Layout (big-endian):
//
//	magic          [4]  "BMQ1"
//	version        [1]  0x01
//	idempotency_id [16] UUID (producer-assigned for dedup on publish)
//	server_msg_id  [16] UUID (broker-assigned; zero on publish from client)
//	publish_ts_ns  [8]  unix nanos
//	user_props_len [2]  length of trailing user_props blob (often 0)
//	user_props     [N]
//	user_payload   [rest]
package envelope

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	Magic   = "BMQ1"
	Version = byte(1)

	headerFixedLen = 4 + 1 + 16 + 16 + 8 + 2 // 47 bytes before user_props + payload
)

var (
	ErrTooShort      = errors.New("envelope: buffer too short")
	ErrBadMagic      = errors.New("envelope: invalid magic")
	ErrBadVersion    = errors.New("envelope: unsupported version")
	ErrInvalidUUID   = errors.New("envelope: invalid uuid bytes")
)

// Envelope is the broker wire wrapper around user payload bytes.
type Envelope struct {
	IdempotencyID uuid.UUID
	ServerMsgID   uuid.UUID
	PublishTS     time.Time
	UserProps     []byte
	Payload       []byte
}

// NewPublish builds an envelope for an outbound publish from a client SDK.
func NewPublish(idempotencyID uuid.UUID, payload []byte) Envelope {
	if idempotencyID == uuid.Nil {
		idempotencyID = uuid.Must(uuid.NewV7())
	}
	return Envelope{
		IdempotencyID: idempotencyID,
		ServerMsgID:   uuid.Nil,
		PublishTS:     time.Now(),
		Payload:       payload,
	}
}

// WithServerMsgID returns a copy with broker-assigned message id set.
func (e Envelope) WithServerMsgID(id uuid.UUID) Envelope {
	e.ServerMsgID = id
	return e
}

// Encode serializes the envelope to a single byte slice suitable for MQTT payload.
func (e Envelope) Encode() []byte {
	propsLen := len(e.UserProps)
	total := headerFixedLen + propsLen + len(e.Payload)
	out := make([]byte, total)
	copy(out[0:4], Magic)
	out[4] = Version
	copy(out[5:21], e.IdempotencyID[:])
	copy(out[21:37], e.ServerMsgID[:])
	binary.BigEndian.PutUint64(out[37:45], uint64(e.PublishTS.UnixNano()))
	binary.BigEndian.PutUint16(out[45:47], uint16(propsLen))
	if propsLen > 0 {
		copy(out[47:47+propsLen], e.UserProps)
	}
	copy(out[47+propsLen:], e.Payload)
	return out
}

// Decode parses wire bytes into Envelope.
func Decode(data []byte) (Envelope, error) {
	if len(data) < headerFixedLen {
		return Envelope{}, ErrTooShort
	}
	if string(data[0:4]) != Magic {
		return Envelope{}, ErrBadMagic
	}
	if data[4] != Version {
		return Envelope{}, fmt.Errorf("%w: %d", ErrBadVersion, data[4])
	}
	var idem, srv uuid.UUID
	copy(idem[:], data[5:21])
	copy(srv[:], data[21:37])
	if idem == uuid.Nil {
		return Envelope{}, ErrInvalidUUID
	}
	ts := time.Unix(0, int64(binary.BigEndian.Uint64(data[37:45])))
	propsLen := int(binary.BigEndian.Uint16(data[45:47]))
	if len(data) < headerFixedLen+propsLen {
		return Envelope{}, ErrTooShort
	}
	props := make([]byte, propsLen)
	copy(props, data[47:47+propsLen])
	payload := make([]byte, len(data)-headerFixedLen-propsLen)
	copy(payload, data[47+propsLen:])
	return Envelope{
		IdempotencyID: idem,
		ServerMsgID:   srv,
		PublishTS:     ts,
		UserProps:     props,
		Payload:       payload,
	}, nil
}

// TryDecode returns (env, true) if data looks like an envelope, else (zero, false).
func TryDecode(data []byte) (Envelope, bool) {
	if len(data) < headerFixedLen || string(data[0:4]) != Magic {
		return Envelope{}, false
	}
	env, err := Decode(data)
	if err != nil {
		return Envelope{}, false
	}
	return env, true
}
