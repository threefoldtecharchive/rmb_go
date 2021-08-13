package client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/threefoldtech/rmb-go/pkg/rmb"
)

type MessageBusClient struct {
	Ctx    context.Context
	Client *redis.Client
}

func Prepare(command string, dst []int, exp int64, num_retry int) rmb.Message {
	msg := rmb.Message{
		Version:    1,
		Id:         "",
		Command:    command,
		Expiration: exp,
		Retry:      num_retry,
		Data:       "",
		Twin_src:   0,
		Twin_dst:   dst,
		Retqueue:   uuid.New().String(),
		Schema:     "",
		Epoch:      time.Now().Unix(),
		Err:        "",
	}
	return msg
}

func (bus *MessageBusClient) Send(msg rmb.Message, payload string) error {
	update := msg
	update.Data = base64.StdEncoding.EncodeToString([]byte(payload))
	request, err := json.Marshal(update)
	if err != nil {
		return errors.Wrap(err, "couldn't encode into json")
	}
	bus.Client.LPush(bus.Ctx, "msgbus.system.local", request)
	return nil
}

func (bus *MessageBusClient) Read(msg rmb.Message) []rmb.Message {
	log.Info().Str("return_queue", msg.Retqueue).Msg("Waiting reply")
	responses := []rmb.Message{}
	println(msg.Retqueue)
	for len(responses) < len(msg.Twin_dst) {
		results, err := bus.Client.BLPop(bus.Ctx, 20000000000, msg.Retqueue).Result()
		if err != nil {
			log.Error().Err(err).Msg("error fetching from redis")
			break
		}
		response_json := results[1]
		response_msg := rmb.Message{}
		if err := json.Unmarshal([]byte(response_json), &response_msg); err != nil {
			log.Error().Err(err).Msg("error decoding entry from redis")
			break
		}
		decoded, err := base64.StdEncoding.DecodeString(response_msg.Data)
		response_msg.Data = string(decoded)
		responses = append(responses, response_msg)
	}
	return responses
}
