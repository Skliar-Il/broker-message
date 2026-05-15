package brokermq

import (
	"time"

	"github.com/google/uuid"
)

type Options struct {
	ClientID      string
	Username      string
	Password      string
	QoS           byte
	KeepAlive     time.Duration
	SeenCacheSize int
	IdempotencyID uuid.UUID
}

type Option func(*Options)

func WithClientID(id string) Option {
	return func(o *Options) { o.ClientID = id }
}

func WithCredentials(user, pass string) Option {
	return func(o *Options) { o.Username = user; o.Password = pass }
}

func WithQoS(qos byte) Option {
	return func(o *Options) { o.QoS = qos }
}

func WithIdempotencyKey(id uuid.UUID) Option {
	return func(o *Options) { o.IdempotencyID = id }
}

func WithSeenCacheSize(n int) Option {
	return func(o *Options) { o.SeenCacheSize = n }
}

func defaultOptions() Options {
	return Options{
		ClientID:      "brokermq-client",
		QoS:           1,
		KeepAlive:     60 * time.Second,
		SeenCacheSize: 10_000,
	}
}
