package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Rabbit struct {
	url   string
	queue string

	mu       sync.Mutex
	conn     *amqp.Connection
	pubCh    *amqp.Channel
	confirms chan amqp.Confirmation
}

func NewRabbit(ctx context.Context, url, queue string) (*Rabbit, error) {
	r := &Rabbit{url: url, queue: queue}
	if err := r.dial(ctx); err != nil {
		return nil, err
	}
	if _, err := r.pubCh.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("queue declare: %w", err)
	}
	return r, nil
}

func (r *Rabbit) dial(ctx context.Context) error {
	conn, err := amqp.Dial(r.url)
	if err != nil {
		return fmt.Errorf("amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("amqp channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("amqp confirm: %w", err)
	}
	r.conn = conn
	r.pubCh = ch
	r.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1024))
	return nil
}

func (r *Rabbit) Reset(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, err := r.pubCh.QueuePurge(r.queue, false)
	return err
}

func (r *Rabbit) Publish(ctx context.Context, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.pubCh.PublishWithContext(ctx, "", r.queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         payload,
	}); err != nil {
		return err
	}
	select {
	case c, ok := <-r.confirms:
		if !ok {
			return errors.New("confirm channel closed")
		}
		if !c.Ack {
			return errors.New("publish nack")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return errors.New("publish confirm timeout")
	}
}

func (r *Rabbit) Consume(ctx context.Context, h Handler) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return fmt.Errorf("consume channel: %w", err)
	}
	defer ch.Close()
	if err := ch.Qos(256, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}
	deliveries, err := ch.Consume(r.queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return nil
			}
			h(d.Body)
			_ = d.Ack(false)
		}
	}
}

func (r *Rabbit) Close() error {
	if r.pubCh != nil {
		_ = r.pubCh.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}
