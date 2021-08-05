package rmb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type Message struct {
	Version    int    `json:"ver"`
	Id         string `json:"uid"`
	Command    string `json:"cmd"`
	Expiration int64  `json:"exp"`
	Retry      int    `json:"try"`
	Data       string `json:"dat"`
	Twin_src   int    `json:"src"`
	Twin_dst   []int  `json:"dst"`
	Retqueue   string `json:"ret"`
	Schema     string `json:"shm"`
	Epoch      int64  `json:"now"`
	Err        string `json:"err"`
}

type HSetEntry struct {
	Key   string  `json:"key"`
	Value Message `json:"value"`
}

type SubstrateTwin struct {
	Version int    `json:"version"`
	Id      int    `json:"id"`
	Account string `json:"account"`
	Ip      string `json:"ip"`
}

type MBusCtx struct {
	Debug        int
	RedisAddress string
	MyId         int
	Subaddr      string
}

type App struct {
	debug     bool
	substrate string
	redis     *redis.Client
	// what is ctx
	ctx  context.Context
	twin int
}

func remoteUrl(twinIp string) string {
	return fmt.Sprintf("http://%s:8051/zbus-remote", twinIp)
}

func replyUrl(twinIp string) string {
	return fmt.Sprintf("http://%s:8051/zbus-reply", twinIp)
}

func errorReply(message string) []byte {
	return []byte(fmt.Sprintf("{\"status\": \"error\", \"message\": \"%s\"}", message))
}

func successReply() []byte {
	return []byte("{\"status\": \"accepted\"}")
}

func (a *App) validate_input(msg Message) error {
	if msg.Version != 1 {
		return errors.New("protocol version mismatch")
	}

	if msg.Command == "" {
		return errors.New("missing command request")
	}

	if len(msg.Twin_dst) == 0 {
		return errors.New("missing twin destination")
	}

	if msg.Retqueue == "" {
		return errors.New("return queue not defined")
	}
	return nil
}

func (a *App) resolve(twinId int) (string, error) {
	url := fmt.Sprintf("%s/twin/%d", a.substrate, twinId)
	resp, err := http.Get(url)
	if err != nil {
		return "", nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	twinInfo := SubstrateTwin{}

	if err := json.Unmarshal(body, &twinInfo); err != nil {
		return "", errors.Wrap(err, "couldn't parse json response")
	}

	return twinInfo.Ip, nil
}

func (a *App) handle_from_local_prepare_item(msg Message, dst int) error {
	update := msg
	update.Twin_src = a.twin
	update.Twin_dst = []int{dst}

	if a.debug {
		fmt.Printf("[+] resolving twin: %d\n", dst)
	}

	dstIp, err := a.resolve(dst)

	if err != nil {
		return err
	}

	id, err := a.redis.Incr(a.ctx, fmt.Sprintf("msgbus.counter.%d", dst)).Result()

	if err != nil {
		return err
	}

	update.Id = fmt.Sprintf("%d.%d", dst, id)
	update.Retqueue = "msgbus.system.reply"

	if a.debug {
		fmt.Printf("[+] forwarding to %s\n", dstIp)
	}

	output, err := json.Marshal(update)
	if err != nil {
		return err
	}

	resp, err := http.Post(remoteUrl(dstIp), "application/json", bytes.NewBuffer(output))

	if err != nil {
		a.request_needs_retry(msg, update)
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Warn().Err(err).Msg("couldn't read response after sending to target msgbus")
	} else {
		log.Debug().Str("response", string(body)).Int("status", resp.StatusCode).Msg("message sent to target msgbus")
	}

	value, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = a.redis.HSet(a.ctx, "msgbus.system.backlog", update.Id, value).Result()
	if err != nil {
		return err
	}
	return nil
}

func (a *App) handle_from_local_prepare(msg Message) error {
	var reserr error = nil
	for _, dst := range msg.Twin_dst {
		if err := a.handle_from_local_prepare_item(msg, dst); err != nil {
			reserr = err
		}
	}
	return reserr
}

func (a *App) handle_from_local(value string) error {
	msg := Message{}

	if err := json.Unmarshal([]byte(value), &msg); err != nil {
		return errors.Wrap(err, "couldn't parse json")
	}

	if err := a.validate_input(msg); err != nil {
		return errors.Wrap(err, "local: couldn't validate input")
	}

	return a.handle_from_local_prepare(msg)
}

func (a *App) handle_from_remote(value string) error {
	msg := Message{}

	if err := json.Unmarshal([]byte(value), &msg); err != nil {
		return errors.Wrap(err, "couldn't parse json")
	}

	if err := a.validate_input(msg); err != nil {
		return errors.Wrap(err, "local: couldn't validate input")
	}

	log.Info().Str("queue", fmt.Sprintf("msgbus.%s\n", msg.Command)).Msg("forwarding to local service")

	// forward to local service
	a.redis.LPush(a.ctx, fmt.Sprintf("msgbus.%s", msg.Command), value)
	return nil
}

func (a *App) handle_from_reply_for_me(msg Message) error {
	log.Info().Msg("message reply for me, fetching backlog")

	retval, err := a.redis.HGet(a.ctx, "msgbus.system.backlog", msg.Id).Result()

	if err == redis.Nil {
		return errors.New(fmt.Sprintf("couldn't find key %s", msg.Id))
	} else if err != nil {
		return err
	}
	original := Message{}

	if err := json.Unmarshal([]byte(retval), &original); err != nil {
		return errors.Wrap(err, "couldn't parse json")
	}
	update := msg

	// restore return queue name for the caller
	update.Retqueue = original.Retqueue

	value, err := json.Marshal(update)

	if err != nil {
		return err
	}
	// forward reply to original sender
	a.redis.LPush(a.ctx, update.Retqueue, value)

	// remove from backlog
	a.redis.HDel(a.ctx, "msgbus.system.backlog", msg.Id)
	return nil
}

func (a *App) handle_from_reply_forward(msg Message, value string) error {
	// reply have only one destination (source)
	dst := msg.Twin_dst[0]

	dstIp, err := a.resolve(dst)
	if err != nil {
		return err
	}

	log.Info().Str("destination", dstIp).Msg("forwarding reply")

	// forward to reply agent
	_, err = http.Post(replyUrl(dstIp), "application/json", bytes.NewBuffer([]byte(value)))

	if err != nil {
		return err
	}

	return nil
}

func (a *App) request_needs_retry(msg Message, update Message) error {
	log.Info().Msg("could not send message to remote msgbus")

	// restore 'update' to original state
	update.Retqueue = msg.Retqueue

	if update.Retry == 0 {
		log.Info().Msg("no more retry, replying with error")
		update.Err = "could not send request and all retries done"
		output, err := json.Marshal(update)
		if err != nil {
			return errors.Wrap(err, "couldn't parse json")
		}
		a.redis.LPush(a.ctx, update.Retqueue, output)
		return nil
	}
	log.Info().Int("retry", update.Retry).Msg("updating retry and adding to retry list")

	// remove one retry
	update.Retry -= 1
	update.Epoch = time.Now().Unix()

	value, err := json.Marshal(update)

	if err != nil {
		return errors.Wrap(err, "couldn't parse json")
	}

	a.redis.HSet(a.ctx, "msgbus.system.retry", update.Id, value)
	return nil
}

func (a *App) handle_from_reply(value string) error {
	msg := Message{}

	if err := json.Unmarshal([]byte(value), &msg); err != nil {
		return errors.Wrap(err, "couldn't parse json")
	}

	if err := a.validate_input(msg); err != nil {
		return errors.Wrap(err, "local: couldn't validate input")
	}

	if msg.Twin_dst[0] == a.twin {
		return a.handle_from_reply_for_me(msg)

	} else if msg.Twin_src == a.twin {
		return a.handle_from_reply_forward(msg, value)
	}

	return nil
}

func (a *App) handle_internal_hgetall(lines map[string]string) []HSetEntry {
	entries := []HSetEntry{}

	// build usable list from redis response
	for key, value := range lines {
		message := Message{}

		if err := json.Unmarshal([]byte(value), &message); err != nil {
			log.Error().Err(errors.Wrap(err, "couldn't parse json")).Msg("handling retry queue")
			continue
		}

		entries = append(entries, HSetEntry{
			Key:   key,
			Value: message,
		})
	}

	return entries
}

func (a *App) handle_retry() error {
	log.Debug().Msg("checking retries")

	lines, err := a.redis.HGetAll(a.ctx, "msgbus.system.retry").Result()

	if err != nil {
		return errors.Wrap(err, "couldn't read retry messages")
	}

	entries := a.handle_internal_hgetall(lines)

	now := time.Now().Unix()

	// iterate over each entries
	for _, entry := range entries {
		if now > entry.Value.Epoch+5 { // 5 sec debug
			log.Info().Str("key", entry.Key).Msg("retry needed")

			// remove from retry list
			a.redis.HDel(a.ctx, "msgbus.system.retry", entry.Key)

			// re-call sending function, which will succeed
			// or put it back to retry
			a.handle_from_local_prepare_item(entry.Value, entry.Value.Twin_dst[0])
		}
	}
	return nil
}

func (a *App) handle_scrubbing() error {
	log.Debug().Msg("scrubbing")

	lines, err := a.redis.HGetAll(a.ctx, "msgbus.system.backlog").Result()

	if err != nil {
		return errors.Wrap(err, "couldn't read backlog messages")
	}

	entries := a.handle_internal_hgetall(lines)

	now := time.Now().Unix()

	// iterate over each entries
	for _, entry := range entries {
		if entry.Value.Expiration == 0 {
			// avoid infinite expiration, fallback to 1h
			entry.Value.Expiration = 3600
		}

		if entry.Value.Epoch+entry.Value.Expiration < now {
			log.Debug().Str("key", entry.Key).Msg("expired")

			entry.Value.Err = fmt.Sprintf("request timeout (expiration reached, %d)", entry.Value.Expiration)
			output, err := json.Marshal(entry.Value)
			if err != nil {
				log.Error().Err(err).Msg("couldn't parse json")
				continue
			}
			a.redis.LPush(a.ctx, entry.Value.Retqueue, output)
			a.redis.HDel(a.ctx, "msgbus.system.backlog", entry.Key)
		}
	}
	return nil
}

func (a *App) run_server() {
	log.Info().Int("twin", a.twin).Msg("initializing agent server")

	for {
		res, err := a.redis.BLPop(a.ctx, 1000000000, "msgbus.system.local", "msgbus.system.remote", "msgbus.system.reply").Result()

		if err == redis.Nil {
			a.handle_retry()
			a.handle_scrubbing()
			continue
		} else if err != nil {
			log.Error().Err(err).Msg("error fetching messages from redis")
			continue
		}

		log.Debug().Str("queue", res[0]).Str("message", res[1]).Msg("found a redis payload")

		if res[0] == "msgbus.system.reply" {
			if err := a.handle_from_reply(res[1]); err != nil {
				log.Err(err).Msg("handle_from_reply")
			}
		}
		if res[0] == "msgbus.system.local" {
			if err := a.handle_from_local(res[1]); err != nil {
				log.Err(err).Msg("handle_from_local")
			}
		}
		if res[0] == "msgbus.system.remote" {
			if err := a.handle_from_remote(res[1]); err != nil {
				log.Err(err).Msg("handle_from_remote")
			}
		}
	}
}

func (a *App) remote(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Write(errorReply("couldn't read body"))
		return
	}

	log.Debug().Str("request_body", string(body)).Msg("request from external agent")

	msg := Message{}

	if err := json.Unmarshal(body, &msg); err != nil {
		w.Write(errorReply("couldn't parse json"))
		return
	}
	// locking?
	_, err = a.redis.LPush(a.ctx, "msgbus.system.remote", body).Result()

	if err != nil {
		err = errors.Wrap(err, "couldn't push entry to reply queue")
		w.Write(errorReply(err.Error()))
	}

	w.Write(successReply())

}

func (a *App) reply(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Write(errorReply("couldn't read body"))
		return
	}
	log.Debug().Str("request_body", string(body)).Msg("request from external agent")

	msg := Message{}

	if err := json.Unmarshal(body, &msg); err != nil {
		w.Write(errorReply("couldn't parse json"))
		return
	}
	// locking?
	_, err = a.redis.LPush(a.ctx, "msgbus.system.reply", body).Result()
	if err != nil {
		err = errors.Wrap(err, "couldn't push entry to reply queue")
		w.Write(errorReply(err.Error()))
	}
	w.Write(successReply())

}

func Setup(router *mux.Router, debug bool, substrate string, redisServer string, twin int) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisServer,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	a := App{
		debug:     debug,
		substrate: substrate,
		redis:     rdb,
		twin:      twin,
		ctx:       context.Background(),
	}
	go a.run_server()
	router.HandleFunc("/zbus-reply", a.reply)
	router.HandleFunc("/zbus-remote", a.remote)
	http.Handle("/", router)
}
