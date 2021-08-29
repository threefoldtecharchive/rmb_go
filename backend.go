package rmb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

var (
	ErrNotAvailable = fmt.Errorf("not available")

	tagsMap = map[string]Tag{
		"msgbus.system.local":  Local,
		"msgbus.system.remote": Remote,
		"msgbus.system.reply":  Reply,
	}
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
	Next(ctx context.Context, timeout time.Duration) (Envelope, error)
	QueueReply(ctx context.Context, msg Message) error // method name
	QueueRemote(ctx context.Context, msg Message) error

	IncrementID(ctx context.Context, id int) (int64, error)

	PushToBacklog(ctx context.Context, msg Message, id string) error
	PopMessageFromBacklog(ctx context.Context, id string) (Message, error)

	QueueCommand(ctx context.Context, msg Message) error
	PushProcessedMessage(ctx context.Context, msg Message) error

	QueueRetry(ctx context.Context, msg Message) error
	PopRetryMessages(ctx context.Context, olderThan time.Duration) ([]Message, error)

	PopExpiredBacklogMessages(ctx context.Context) ([]Message, error)
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
	// looks like it's implemented as a pool
	client *redis.Client
}

func NewRedisBackend(redisServer string) *RedisBackend {
	return &RedisBackend{
		client: redis.NewClient(&redis.Options{
			Addr:     redisServer,
			Password: "", // no password set
			DB:       0,  // use default DB
		}),
	}
}
func (r *RedisBackend) Next(ctx context.Context, timeout time.Duration) (Envelope, error) {
	res, err := r.client.BLPop(ctx, timeout, "msgbus.system.local", "msgbus.system.remote", "msgbus.system.reply").Result()

	if err == redis.Nil {
		return Envelope{}, ErrNotAvailable
	} else if err != nil {
		return Envelope{}, errors.Wrap(err, "failed to get next message")
	}

	var envelope Envelope
	if err := json.Unmarshal([]byte(res[1]), &envelope); err != nil {
		return envelope, err
	}

	envelope.Tag = tagsMap[res[0]]
	return envelope, nil
}

func (r *RedisBackend) QueueReply(ctx context.Context, msg Message) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "failed to encode into json")
	}

	_, err = r.client.LPush(ctx, "msgbus.system.reply", bytes).Result()
	if err != nil {
		return errors.Wrap(err, "failed to push reply message to redis")
	}
	return nil
}

func (r *RedisBackend) QueueRemote(ctx context.Context, msg Message) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "failed to encode into json")
	}

	_, err = r.client.LPush(ctx, "msgbus.system.remote", bytes).Result()
	if err != nil {
		return errors.Wrap(err, "failed to push remote message to redis")
	}
	return nil
}

func (r *RedisBackend) IncrementID(ctx context.Context, id int) (int64, error) {
	cnt, err := r.client.Incr(ctx, fmt.Sprintf("msgbus.counter.%d", id)).Result()
	if err != nil {
		return 0, errors.Wrap(err, "failed to increment redis counter")
	}
	return cnt, nil
}

func (r *RedisBackend) PushToBacklog(ctx context.Context, msg Message, id string) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "failed to encode into json")
	}

	_, err = r.client.HSet(ctx, "msgbus.system.backlog", id, bytes).Result()
	if err != nil {
		return errors.Wrap(err, "failed to add to redis msgbus.system.backlog")
	}
	return nil
}

func (r *RedisBackend) PopMessageFromBacklog(ctx context.Context, id string) (Message, error) {
	msg := Message{}

	bytes, err := r.client.HGet(ctx, "msgbus.system.backlog", id).Result()

	if err == redis.Nil {
		return msg, ErrNotAvailable
	} else if err != nil {
		// looks like most errors here doesn't need wrapping, looks like a string from its parent
		return msg, errors.Wrap(err, "error fetching message from backlog")
	}

	if err := json.Unmarshal([]byte(bytes), &msg); err != nil {
		return msg, errors.Wrap(err, "couldn't parse json")
	}
	return msg, nil
}

func (r *RedisBackend) QueueCommand(ctx context.Context, msg Message) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "failed to encode into json")
	}

	_, err = r.client.LPush(ctx, fmt.Sprintf("msgbus.%s", msg.Command), bytes).Result()
	return err
}

func (r *RedisBackend) PushProcessedMessage(ctx context.Context, msg Message) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "failed to encode into json")
	}

	_, err = r.client.LPush(ctx, msg.Retqueue, bytes).Result()
	return err
}

func (r *RedisBackend) QueueRetry(ctx context.Context, msg Message) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "failed to encode into json")
	}

	_, err = r.client.HSet(ctx, "msgbus.system.retry", msg.ID, bytes).Result()
	return err
}

func (r *RedisBackend) PopRetryMessages(ctx context.Context, olderThan time.Duration) ([]Message, error) {
	lines, err := r.client.HGetAll(ctx, "msgbus.system.retry").Result()

	if err != nil {
		return nil, errors.Wrap(err, "couldn't read retry messages")
	}

	msgs := []Message{}

	now := time.Now().Unix()
	for key, value := range lines {
		var msg Message
		if err := json.Unmarshal([]byte(value), &msg); err != nil {
			// should it be popped off the retry queue?
			log.Error().Err(errors.Wrap(err, "couldn't parse json")).Msg("handling retry queue")
			continue
		}
		if now > msg.Epoch+int64(olderThan/time.Second) {
			if _, err := r.client.HDel(ctx, "msgbus.system.retry", key).Result(); err != nil {
				log.Error().Err(err).Msg("error deleting retry message")
			} else {
				msgs = append(msgs, msg)
			}
		}
	}
	return msgs, err
}

func (r *RedisBackend) PopExpiredBacklogMessages(ctx context.Context) ([]Message, error) {
	lines, err := r.client.HGetAll(ctx, "msgbus.system.backlog").Result()

	if err != nil {
		return nil, errors.Wrap(err, "couldn't read backlog messages")
	}

	msgs := []Message{}

	now := time.Now().Unix()
	for key, value := range lines {
		var msg Message
		if err := json.Unmarshal([]byte(value), &msg); err != nil {
			// should it be popped off the backlog queue?
			log.Error().Err(errors.Wrap(err, "couldn't parse json")).Msg("handling backlog queue")
			continue
		}

		if msg.Epoch+msg.Expiration < now {
			if _, err := r.client.HDel(ctx, "msgbus.system.backlog", key).Result(); err != nil {
				log.Error().Err(err).Msg("error deleting backlog expired message")
			} else {
				msg.ID = key
				msgs = append(msgs, msg)
			}
		}
	}
	return msgs, err
}
