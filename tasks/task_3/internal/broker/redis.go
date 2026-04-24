package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	rdb     *redis.Client
	stream  string
	group   string
	consume string
}

func NewRedis(ctx context.Context, url, stream string) (*Redis, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}
	rdb := redis.NewClient(opt)
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Redis{
		rdb:     rdb,
		stream:  stream,
		group:   "bench-group",
		consume: fmt.Sprintf("bench-%d", time.Now().UnixNano()),
	}, nil
}

func (r *Redis) Reset(ctx context.Context) error {
	if err := r.rdb.Del(ctx, r.stream).Err(); err != nil {
		return err
	}
	if err := r.rdb.XGroupCreateMkStream(ctx, r.stream, r.group, "$").Err(); err != nil {
		if !isBusyGroup(err) {
			return fmt.Errorf("xgroup create: %w", err)
		}
	}
	return nil
}

func isBusyGroup(err error) bool {
	return err != nil && (errors.Is(err, redis.Nil) || (err.Error() == "BUSYGROUP Consumer Group name already exists"))
}

func (r *Redis) Publish(ctx context.Context, payload []byte) error {
	return r.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: r.stream,
		Values: map[string]any{"d": payload},
	}).Err()
}

func (r *Redis) Consume(ctx context.Context, h Handler) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		res, err := r.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    r.group,
			Consumer: r.consume,
			Streams:  []string{r.stream, ">"},
			Count:    256,
			Block:    500 * time.Millisecond,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if errors.Is(err, context.Canceled) {
				return err
			}
			return err
		}
		for _, s := range res {
			ackIDs := make([]string, 0, len(s.Messages))
			for _, m := range s.Messages {
				if v, ok := m.Values["d"]; ok {
					switch vv := v.(type) {
					case string:
						h([]byte(vv))
					case []byte:
						h(vv)
					}
				}
				ackIDs = append(ackIDs, m.ID)
			}
			if len(ackIDs) > 0 {
				_ = r.rdb.XAck(ctx, r.stream, r.group, ackIDs...).Err()
				_ = r.rdb.XDel(ctx, r.stream, ackIDs...).Err()
			}
		}
	}
}

func (r *Redis) Close() error { return r.rdb.Close() }
