package rmb

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	Local Tag = iota
	Reply
	Remote
)

type Tag int

type Envelope struct {
	Message
	Tag Tag
}
type Backend interface {
	QueueReply(ctx context.Context, msg Message) error // method name
	QueueRemote(ctx context.Context, msg Message) error
	Next(ctx context.Context, timeout time.Duration) (Envelope, error)
}

type BackendInterface interface {
	HGet(ctx context.Context, key string, field string) (string, error)
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	HDel(ctx context.Context, key string, field string) (int64, error)
	LPush(ctx context.Context, key string, value []byte) (int64, error)
	BLPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error)
	Incr(ctx context.Context, key string) (int64, error)
	HSet(ctx context.Context, key string, field string, value []byte) (int64, error)
}

type RedisBackend struct {
	client *redis.Client
}

func (r RedisBackend) HGet(ctx context.Context, key string, field string) (string, error) {
	return r.client.HGet(ctx, key, field).Result()
}

func (r RedisBackend) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return r.client.HGetAll(ctx, key).Result()
}

func (r RedisBackend) HDel(ctx context.Context, key string, field string) (int64, error) {
	return r.client.HDel(ctx, key, field).Result()
}

func (r RedisBackend) LPush(ctx context.Context, key string, value []byte) (int64, error) {
	return r.client.LPush(ctx, key, value).Result()
}

func (r RedisBackend) BLPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	return r.client.BLPop(ctx, timeout, keys...).Result()
}

func (r RedisBackend) Incr(ctx context.Context, key string) (int64, error) {
	return r.client.Incr(ctx, key).Result()
}

func (r RedisBackend) HSet(ctx context.Context, key string, field string, value []byte) (int64, error) {
	return r.client.HSet(ctx, key, field, value).Result()
}
