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
	"github.com/threefoldtech/rmb"
)

type MessageBusClient struct {
	Ctx    context.Context
	Client *redis.Client
}

func Prepare(command string, dst []int, exp int64, numRetry int) rmb.Message {
	msg := rmb.Message{
		Version:    1,
		ID:         "",
		Command:    command,
		Expiration: exp,
		Retry:      numRetry,
		Data:       "",
		TwinSrc:    0,
		TwinDst:    dst,
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
	for len(responses) < len(msg.TwinDst) {
		results, err := bus.Client.BLPop(bus.Ctx, 20000000000, msg.Retqueue).Result()
		if err != nil {
			log.Error().Err(err).Msg("error fetching from redis")
			break
		}
		responseJSON := results[1]
		responseMsg := rmb.Message{}
		if err := json.Unmarshal([]byte(responseJSON), &responseMsg); err != nil {
			log.Error().Err(err).Msg("error decoding entry from redis")
			break
		}
		decoded, err := base64.StdEncoding.DecodeString(responseMsg.Data)
		if err != nil {
			log.Error().Err(err).Msg("failed to decode response message data")
		}
		responseMsg.Data = string(decoded)
		responses = append(responses, responseMsg)
	}
	return responses
}
