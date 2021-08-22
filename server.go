package rmb

import (
	"context"
	"encoding/json"
	"fmt"
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
	ID         string `json:"uid"`
	Command    string `json:"cmd"`
	Expiration int64  `json:"exp"`
	Retry      int    `json:"try"`
	Data       string `json:"dat"`
	TwinSrc    int    `json:"src"`
	TwinDst    []int  `json:"dst"`
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
	ID      int    `json:"id"`
	Account string `json:"account"`
	IP      string `json:"ip"`
}

type MBusCtx struct {
	Debug        int
	RedisAddress string
	MyID         int
	Subaddr      string
}

type App struct {
	debug bool
	redis BackendInterface
	// what is ctx
	ctx      context.Context
	twin     int
	resolver TwinResolverInterface
	server   *http.Server
}

func errorReply(message string) []byte {
	return []byte(fmt.Sprintf("{\"status\": \"error\", \"message\": \"%s\"}", message))
}

func successReply() []byte {
	return []byte("{\"status\": \"accepted\"}")
}

func (a *App) validateInput(msg Message) error {
	if msg.Version != 1 {
		return errors.New("protocol version mismatch")
	}

	if msg.Command == "" {
		return errors.New("missing command request")
	}

	if len(msg.TwinDst) == 0 {
		return errors.New("missing twin destination")
	}

	if msg.Retqueue == "" {
		return errors.New("return queue not defined")
	}
	return nil
}

func (a *App) handleFromLocalPrepareItem(msg Message, dst int) error {
	update := msg
	update.TwinSrc = a.twin
	update.TwinDst = []int{dst}

	id, err := a.redis.Incr(a.ctx, fmt.Sprintf("msgbus.counter.%d", dst))

	if err != nil {
		return err
	}

	update.ID = fmt.Sprintf("%d.%d", dst, id)
	update.Retqueue = "msgbus.system.reply"

	output, err := json.Marshal(update)
	if err != nil {
		return err
	}

	c, err := a.resolver.Resolve(dst)

	if err != nil {
		return errors.Wrap(err, "couldn't get twin ip")
	}
	err = c.SendRemote(output)

	if err != nil {
		a.requestNeedsRetry(msg, update)
		return err
	}

	value, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = a.redis.HSet(a.ctx, "msgbus.system.backlog", update.ID, value)
	if err != nil {
		return err
	}
	return nil
}

func (a *App) handleFromLocalPrepare(msg Message) {
	for _, dst := range msg.TwinDst {
		if err := a.handleFromLocalPrepareItem(msg, dst); err != nil {
			log.Error().Err(err).Msg("failed to handle message in handle_from_local_prepare")
		}
	}
}

func (a *App) handleFromLocal(value string) error {
	msg := Message{}

	if err := json.Unmarshal([]byte(value), &msg); err != nil {
		return errors.Wrap(err, "couldn't parse json")
	}

	if err := a.validateInput(msg); err != nil {
		return errors.Wrap(err, "local: couldn't validate input")
	}

	a.handleFromLocalPrepare(msg)
	return nil
}

func (a *App) handleFromRemote(value string) error {
	msg := Message{}

	if err := json.Unmarshal([]byte(value), &msg); err != nil {
		return errors.Wrap(err, "couldn't parse json")
	}

	if err := a.validateInput(msg); err != nil {
		return errors.Wrap(err, "local: couldn't validate input")
	}

	log.Info().Str("queue", fmt.Sprintf("msgbus.%s", msg.Command)).Msg("forwarding to local service")

	// forward to local service
	a.redis.LPush(a.ctx, fmt.Sprintf("msgbus.%s", msg.Command), []byte(value))
	return nil
}

func (a *App) handleFromReplyForMe(msg Message) error {
	log.Info().Msg("message reply for me, fetching backlog")

	retval, err := a.redis.HGet(a.ctx, "msgbus.system.backlog", msg.ID)

	if err == redis.Nil {
		return errors.New(fmt.Sprintf("couldn't find key %s", msg.ID))
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
	a.redis.HDel(a.ctx, "msgbus.system.backlog", msg.ID)
	return nil
}

func (a *App) handleFromReplyForward(msg Message, value string) error {
	// reply have only one destination (source)
	dst := msg.TwinDst[0]

	r, err := a.resolver.Resolve(dst)

	if err != nil {
		return errors.Wrap(err, "couldn't resolve twin ip")
	}

	// forward to reply agent
	err = r.SendReply([]byte(value))

	if err != nil {
		return err
	}

	return nil
}

func (a *App) requestNeedsRetry(msg Message, update Message) error {
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
	update.Retry--
	update.Epoch = time.Now().Unix()

	value, err := json.Marshal(update)

	if err != nil {
		return errors.Wrap(err, "couldn't parse json")
	}

	a.redis.HSet(a.ctx, "msgbus.system.retry", update.ID, value)
	return nil
}

func (a *App) handleFromReply(value string) error {
	msg := Message{}

	if err := json.Unmarshal([]byte(value), &msg); err != nil {
		return errors.Wrap(err, "couldn't parse json")
	}

	if err := a.validateInput(msg); err != nil {
		return errors.Wrap(err, "local: couldn't validate input")
	}

	if msg.TwinDst[0] == a.twin {
		return a.handleFromReplyForMe(msg)

	} else if msg.TwinSrc == a.twin {
		return a.handleFromReplyForward(msg, value)
	}

	return nil
}

func (a *App) handleInternalHgetall(lines map[string]string) []HSetEntry {
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

func (a *App) handleRetry() error {
	log.Debug().Msg("checking retries")

	lines, err := a.redis.HGetAll(a.ctx, "msgbus.system.retry")

	if err != nil {
		return errors.Wrap(err, "couldn't read retry messages")
	}

	entries := a.handleInternalHgetall(lines)

	now := time.Now().Unix()

	// iterate over each entries
	for _, entry := range entries {
		if now > entry.Value.Epoch+5 { // 5 sec debug
			log.Info().Str("key", entry.Key).Msg("retry needed")

			// remove from retry list
			a.redis.HDel(a.ctx, "msgbus.system.retry", entry.Key)

			// re-call sending function, which will succeed
			// or put it back to retry
			a.handleFromLocalPrepareItem(entry.Value, entry.Value.TwinDst[0])
		}
	}
	return nil
}

func (a *App) handleScrubbing() error {
	log.Debug().Msg("scrubbing")

	lines, err := a.redis.HGetAll(a.ctx, "msgbus.system.backlog")

	if err != nil {
		return errors.Wrap(err, "couldn't read backlog messages")
	}

	entries := a.handleInternalHgetall(lines)

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

func (a *App) runServer(ctx context.Context) {
	log.Info().Int("twin", a.twin).Msg("initializing agent server")

	for {
		res, err := a.redis.BLPop(a.ctx, time.Second, "msgbus.system.local", "msgbus.system.remote", "msgbus.system.reply")

		if err == nil {
			log.Debug().Str("queue", res[0]).Str("message", res[1]).Msg("found a redis payload")
		}

		if err == redis.Nil {
			a.handleRetry()
			a.handleScrubbing()
			continue
		} else if err != nil {
			log.Error().Err(err).Msg("error fetching messages from redis")
			time.Sleep(time.Second)
		} else if res[0] == "msgbus.system.reply" {
			if err := a.handleFromReply(res[1]); err != nil {
				log.Err(err).Msg("handle_from_reply")
			}
		} else if res[0] == "msgbus.system.local" {
			if err := a.handleFromLocal(res[1]); err != nil {
				log.Err(err).Msg("handle_from_local")
			}
		} else if res[0] == "msgbus.system.remote" {
			if err := a.handleFromRemote(res[1]); err != nil {
				log.Err(err).Msg("handle_from_remote")
			}
		}

		select {
		case <-ctx.Done():
			log.Debug().Msg("stopping runServer as the context is done")
			return
		default:
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
	_, err = a.redis.LPush(a.ctx, "msgbus.system.remote", body)

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
	_, err = a.redis.LPush(a.ctx, "msgbus.system.reply", body)
	if err != nil {
		err = errors.Wrap(err, "couldn't push entry to reply queue")
		w.Write(errorReply(err.Error()))
	}
	w.Write(successReply())

}
func (a *App) Serve(ctx context.Context) error {
	a.ctx = ctx
	go a.runServer(ctx)
	var serverErr error
	go func() {
		if serverErr = a.server.ListenAndServe(); serverErr != http.ErrServerClosed {
			log.Error().Err(serverErr).Msg("server ListenAndServe failed")
		}
	}()
	interval := 2 * time.Second
	timer := time.NewTimer(interval)
	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("shutting down the server")
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			err := a.Cancel(ctx)
			if err != nil {
				log.Err(err)
			}
			if serverErr != nil && serverErr != http.ErrServerClosed {
				return errors.Wrap(serverErr, "server ListenAndServe failed")
			} else if err != nil {
				return errors.Wrap(err, "error while stopping server")
			}
			return err
		case <-timer.C:
			timer.Reset(interval)
		}
	}
}

func (a *App) Cancel(ctx context.Context) error {
	return a.server.Shutdown(ctx)
}

func NewServer(debug bool, substrate string, redisServer string, twin int) *App {
	router := mux.NewRouter()
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisServer,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	backend := RedisBackend{client: rdb}
	a := &App{
		debug: debug,
		redis: backend,
		twin:  twin,
		ctx:   context.Background(),
		resolver: TwinExplorerResolver{
			substrate: substrate,
		},
		server: &http.Server{
			Handler: router,
			Addr:    "0.0.0.0:8051",
		},
	}
	router.HandleFunc("/zbus-reply", a.reply)
	router.HandleFunc("/zbus-remote", a.remote)
	http.Handle("/", router)
	return a
}
func Setup(router *mux.Router, debug bool, substrate string, redisServer string, twin int) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisServer,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	backend := RedisBackend{client: rdb}
	a := App{
		debug: debug,
		redis: backend,
		twin:  twin,
		ctx:   context.Background(),
		resolver: TwinExplorerResolver{
			substrate: substrate,
		},
	}
	go a.runServer(context.Background())
	router.HandleFunc("/zbus-reply", a.reply)
	router.HandleFunc("/zbus-remote", a.remote)
	http.Handle("/", router)
}
