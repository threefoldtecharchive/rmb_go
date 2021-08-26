package rmb

import (
	"context"
	"encoding/json"
	"fmt"
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

func (a *Message) Valid() error {
	if a.Version != 1 {
		return errors.New("protocol version mismatch")
	}

	if a.Command == "" {
		return errors.New("missing command request")
	}

	if len(a.TwinDst) == 0 {
		return errors.New("missing twin destination")
	}

	if a.Retqueue == "" {
		return errors.New("return queue not defined")
	}

	return nil
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
	backend  Backend
	twin     int
	resolver TwinResolver
	server   *http.Server
}

func errorReply(w http.ResponseWriter, status int, message string) {
	w.WriteHeader(status)
	fmt.Fprintf(w, "{\"status\": \"error\", \"message\": \"%s\"}", message)
}

func successReply(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "{\"status\": \"accepted\"}")
}

func (a *App) respondWithError(ctx context.Context, msg Message, err error) error {
	return nil
}

func (a *App) msgNeedsRetry(ctx context.Context, msg Message, err error) error {
	if msg.Retry == 0 {
		if err := a.respondWithError(ctx, msg, errors.Wrap(err, "all retries done")); err != nil {
			return errors.Wrap(err, "failed to respond to the caller with the proper err")
		}
	} else {
		if err := a.backend.QueueRetry(ctx, msg); err != nil {
			return errors.Wrap(err, "failed to queue msg for retry")
		}
	}
	return nil
}

func (a *App) handleFromLocalItem(ctx context.Context, msg Message, dst int) error {
	if msg.Epoch == 0 {
		msg.Epoch = time.Now().Unix()
	}
	update := msg
	update.TwinSrc = a.twin
	update.TwinDst = []int{dst}

	err := error(nil)
	defer func() {
		if err != nil {
			if repErr := a.msgNeedsRetry(ctx, msg, err); repErr != nil {
				log.Error().Err(repErr).Msg("failed while processing message retry")
				log.Error().Err(err).Str("id", msg.ID).Msg("original error")
			}
		}
	}()

	id, err := a.backend.IncrementID(ctx, dst)
	if err != nil {
		return err
	}

	update.ID = fmt.Sprintf("%d.%d", dst, id)
	// anything better?
	update.Retqueue = "msgbus.system.reply"

	c, err := a.resolver.Resolve(dst)

	if err != nil {
		return errors.Wrap(err, "couldn't get twin ip")
	}
	err = c.SendRemote(update)

	if err != nil {
		return err
	}

	err = a.backend.PushToBacklog(ctx, msg, update.ID)
	if err != nil {
		return err
	}
	return nil
}

func (a *App) handleFromLocal(ctx context.Context, msg Message) error {
	for _, dst := range msg.TwinDst {
		if err := a.handleFromLocalItem(ctx, msg, dst); err != nil {
			log.Error().Err(err).Msg("failed to handle message in handle_from_local_prepare")
		}
	}
	// remove nil or concatenate errors
	return nil
}

func (a *App) handleFromRemote(ctx context.Context, msg Message) error {

	log.Info().Str("queue", fmt.Sprintf("msgbus.%s", msg.Command)).Msg("forwarding to local service")

	// forward to local service
	a.redis.LPush(ctx, fmt.Sprintf("msgbus.%s", msg.Command), nil)
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

func (a *App) handleFromReplyForward(ctx context.Context, msg Message) error {
	// reply have only one destination (source)
	dst := msg.TwinDst[0]

	r, err := a.resolver.Resolve(dst)

	if err != nil {
		return errors.Wrap(err, "couldn't resolve twin ip")
	}

	// forward to reply agent
	err = r.SendReply(msg)

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

func (a *App) handleFromReply(ctx context.Context, msg Message) error {

	if msg.TwinDst[0] == a.twin {
		return a.handleFromReplyForMe(ctx, msg)

	} else if msg.TwinSrc == a.twin {
		return a.handleFromReplyForward(ctx, msg)
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
			a.handleFromLocalItem(ctx, entry.Value, entry.Value.TwinDst[0])
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
		select {
		case <-ctx.Done():
			return
		default:
		}

		envelope, err := a.backend.Next(ctx, time.Second)

		if errors.Is(err, ErrNotAvailable) {
			// no next message to process
			if err := a.handleRetry(ctx); err != nil {
				log.Error().Err(err).Msg("unexpected error while retrying")
			}
			if err := a.handleScrubbing(ctx); err != nil {
				log.Error().Err(err).Msg("unexpected error while scrubbing")
			}

			continue
		} else if err != nil {
			// there are another error that we probably need to report.
			log.Error().Err(err).Msg("unexpected error while waiting for next message to process")
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}

		if err := envelope.Valid(); err != nil {
			log.Error().Err(err).Msg("received invalid message")
			//TODO: try to send the reply with the validation error
			// back to the caller, even if u can't process the mssage.
		}

		switch envelope.Tag {
		case Reply:
			if err := a.handleFromReply(ctx, envelope.Message); err != nil {
				log.Err(err).Msg("handle_from_reply")
			}
		case Remote:
			if err := a.handleFromRemote(ctx, envelope.Message); err != nil {
				log.Err(err).Msg("handle_from_remote")
			}
		case Local:
			if err := a.handleFromLocal(ctx, envelope.Message); err != nil {
				log.Err(err).Msg("handle_from_local")
			}
		}
	}
}

func (a *App) remote(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		errorReply(w, http.StatusBadRequest, "couldn't parse json")
		return
	}

	if err := a.backend.QueueRemote(r.Context(), msg); err != nil {
		errorReply(w, http.StatusInternalServerError, "couldn't queue message for processing")
		return
	}

	successReply(w)
}

func (a *App) reply(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		errorReply(w, http.StatusBadRequest, "couldn't parse json")
		return
	}

	if err := a.backend.QueueReply(r.Context(), msg); err != nil {
		err = errors.Wrap(err, "couldn't push entry to reply queue")
		errorReply(w, http.StatusInternalServerError, err.Error())
		return
	}
	successReply(w)
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
	resolver, err := NewTwinResolver(substrate)
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
