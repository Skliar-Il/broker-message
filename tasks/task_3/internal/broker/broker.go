package broker

import "context"

type Handler func(payload []byte)

type Client interface {
	Publish(ctx context.Context, payload []byte) error
	Consume(ctx context.Context, h Handler) error
	Reset(ctx context.Context) error
	Close() error
}
