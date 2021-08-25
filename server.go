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
	redis    BackendInterface
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

func (a *App) handleFromLocalPrepareItem(ctx context.Context, msg Message, dst int) error {
	if msg.Epoch == 0 {
		msg.Epoch = time.Now().Unix()
	}
	update := msg
	update.TwinSrc = a.twin
	update.TwinDst = []int{dst}

	id, err := a.redis.Incr(ctx, fmt.Sprintf("msgbus.counter.%d", dst))

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
		a.requestNeedsRetry(ctx, msg, update)
		return errors.Wrap(err, "couldn't get twin ip")
	}
	err = c.SendRemote(output)

	if err != nil {
		a.requestNeedsRetry(ctx, msg, update)
		return err
	}

	value, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = a.redis.HSet(ctx, "msgbus.system.backlog", update.ID, value)
	if err != nil {
		return err
	}
	return nil
}

func (a *App) handleFromLocalPrepare(ctx context.Context, msg Message) {
	for _, dst := range msg.TwinDst {
		if err := a.handleFromLocalPrepareItem(ctx, msg, dst); err != nil {
			log.Error().Err(err).Msg("failed to handle message in handle_from_local_prepare")
		}
	}
}

func (a *App) handleFromLocal(ctx context.Context, value string) error {
	msg := Message{}

	if err := json.Unmarshal([]byte(value), &msg); err != nil {
		return errors.Wrap(err, "couldn't parse json")
	}

	if err := a.validateInput(msg); err != nil {
		return errors.Wrap(err, "local: couldn't validate input")
	}

	a.handleFromLocalPrepare(ctx, msg)
	return nil
}

func (a *App) handleFromRemote(ctx context.Context, value string) error {
	msg := Message{}

	if err := json.Unmarshal([]byte(value), &msg); err != nil {
		return errors.Wrap(err, "couldn't parse json")
	}

	if err := a.validateInput(msg); err != nil {
		return errors.Wrap(err, "local: couldn't validate input")
	}

	log.Info().Str("queue", fmt.Sprintf("msgbus.%s", msg.Command)).Msg("forwarding to local service")

	// forward to local service
	a.redis.LPush(ctx, fmt.Sprintf("msgbus.%s", msg.Command), []byte(value))
	return nil
}

func (a *App) handleFromReplyForMe(ctx context.Context, msg Message) error {
	log.Info().Msg("message reply for me, fetching backlog")

	retval, err := a.redis.HGet(ctx, "msgbus.system.backlog", msg.ID)

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
	a.redis.LPush(ctx, update.Retqueue, value)

	// remove from backlog
	a.redis.HDel(ctx, "msgbus.system.backlog", msg.ID)
	return nil
}

func (a *App) handleFromReplyForward(ctx context.Context, msg Message, value string) error {
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

func (a *App) requestNeedsRetry(ctx context.Context, msg Message, update Message) error {
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
		a.redis.LPush(ctx, update.Retqueue, output)
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

	a.redis.HSet(ctx, "msgbus.system.retry", update.ID, value)
	return nil
}

func (a *App) handleFromReply(ctx context.Context, value string) error {
	msg := Message{}

	if err := json.Unmarshal([]byte(value), &msg); err != nil {
		return errors.Wrap(err, "couldn't parse json")
	}

	if err := a.validateInput(msg); err != nil {
		return errors.Wrap(err, "local: couldn't validate input")
	}

	if msg.TwinDst[0] == a.twin {
		return a.handleFromReplyForMe(ctx, msg)

	} else if msg.TwinSrc == a.twin {
		return a.handleFromReplyForward(ctx, msg, value)
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

func (a *App) handleRetry(ctx context.Context) error {
	log.Debug().Msg("checking retries")

	lines, err := a.redis.HGetAll(ctx, "msgbus.system.retry")

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
			a.redis.HDel(ctx, "msgbus.system.retry", entry.Key)

			// re-call sending function, which will succeed
			// or put it back to retry
			a.handleFromLocalPrepareItem(ctx, entry.Value, entry.Value.TwinDst[0])
		}
	}
	return nil
}

func (a *App) handleScrubbing(ctx context.Context) error {
	log.Debug().Msg("scrubbing")

	lines, err := a.redis.HGetAll(ctx, "msgbus.system.backlog")

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
			a.redis.LPush(ctx, entry.Value.Retqueue, output)
			a.redis.HDel(ctx, "msgbus.system.backlog", entry.Key)
		}
	}
	return nil
}

func (a *App) runServer(ctx context.Context) {
	log.Info().Int("twin", a.twin).Msg("initializing agent server")

	for {
		res, err := a.redis.BLPop(ctx, time.Second, "msgbus.system.local", "msgbus.system.remote", "msgbus.system.reply")

		if err == nil {
			log.Debug().Str("queue", res[0]).Str("message", res[1]).Msg("found a redis payload")
		}

		if err == redis.Nil {
			a.handleRetry(ctx)
			a.handleScrubbing(ctx)
			continue
		} else if err != nil {
			log.Error().Err(err).Msg("error fetching messages from redis")
			time.Sleep(time.Second)
		} else if res[0] == "msgbus.system.reply" {
			if err := a.handleFromReply(ctx, res[1]); err != nil {
				log.Err(err).Msg("handle_from_reply")
			}
		} else if res[0] == "msgbus.system.local" {
			if err := a.handleFromLocal(ctx, res[1]); err != nil {
				log.Err(err).Msg("handle_from_local")
			}
		} else if res[0] == "msgbus.system.remote" {
			if err := a.handleFromRemote(ctx, res[1]); err != nil {
				log.Err(err).Msg("handle_from_remote")
			}
		}

		select {
		case <-ctx.Done():
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
	_, err = a.redis.LPush(r.Context(), "msgbus.system.remote", body)

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
	_, err = a.redis.LPush(r.Context(), "msgbus.system.reply", body)
	if err != nil {
		err = errors.Wrap(err, "couldn't push entry to reply queue")
		w.Write(errorReply(err.Error()))
	}
	w.Write(successReply())

}
func (a *App) Serve(root context.Context) error {
	ctx, cancel := context.WithCancel(root)
	defer cancel()

	go a.runServer(ctx)

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		a.server.Shutdown(shutdownCtx)
	}()

	if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}

func NewServer(substrate string, redisServer string, twin int) (*App, error) {
	router := mux.NewRouter()
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisServer,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	backend := RedisBackend{client: rdb}
	resolver, err := NewTwinExplorerResolver(substrate)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get a client to explorer resolver")
	}
	a := &App{
		redis:    backend,
		twin:     twin,
		resolver: resolver,
		server: &http.Server{
			Handler: router,
			Addr:    "0.0.0.0:8051",
		},
	}
	router.HandleFunc("/zbus-reply", a.reply)
	router.HandleFunc("/zbus-remote", a.remote)
	return a, nil
}
