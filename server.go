package rmb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/threefoldtech/substrate-client"
)

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

func IsValidUUID(uuid string) bool {
	r := regexp.MustCompile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-4[a-fA-F0-9]{3}-[8|9|aA|bB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}$")
	return r.MatchString(uuid)
}

func errorReply(w http.ResponseWriter, status int, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(ErrorReply{
		"error",
		msg,
	}); err != nil {
		log.Error().Err(err).Str("msg", msg).Int("status", status).Msg("failed to encode json")
		fmt.Fprint(w, "{\"status\": \"error\", \"message\": \"non encodable error happened\"}")
	}
}

func successReply(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "{\"status\": \"accepted\"}")
}

func (a *App) respondWithError(ctx context.Context, msg Message, err error) error {
	msg.Err = err.Error()
	return a.backend.PushProcessedMessage(ctx, msg)
}

func (a *App) msgNeedsRetry(ctx context.Context, msg Message, err error) error {
	if msg.Retry <= 0 {
		if err := a.respondWithError(ctx, msg, errors.Wrap(err, "all retries done")); err != nil {
			return errors.Wrap(err, "failed to respond to the caller with the proper err")
		}
	} else {
		msg.Retry--
		if err := a.backend.QueueRetry(ctx, msg); err != nil {
			return errors.Wrap(err, "failed to queue msg for retry")
		}
	}
	return nil
}

func (a *App) handleFromLocalItem(ctx context.Context, msg Message, dst int) error {
	msg.Epoch = time.Now().Unix()
	update := msg
	update.TwinSrc = a.twin
	update.TwinDst = []int{dst}

	var err error = nil
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
	// time is set here to minimize the interval on which the signature is checked
	// it's set before for checking when the messages expires when pushed to the backlog
	update.Epoch = time.Now().Unix()
	err = update.Sign(a.identity)
	if err != nil {
		return errors.Wrap(err, "couldn't sign message")
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
	return nil
}

func (a *App) handleFromRemote(ctx context.Context, msg Message) error {
	log.Debug().Str("queue", fmt.Sprintf("msgbus.%s", msg.Command)).Msg("forwarding to local service")

	// forward to local service
	return a.backend.QueueCommand(ctx, msg)
}

func (a *App) handleFromReplyForProxy(ctx context.Context, msg Message) error {
	log.Debug().Msg("message reply for proxy")

	err := a.backend.PushProcessedMessage(ctx, msg)
	if err != nil {
		return errors.Wrap(err, "error pushing the reply message")
	}
	return nil
}

func (a *App) handleFromReplyForMe(ctx context.Context, msg Message) error {
	log.Debug().Msg("message reply for me, fetching backlog")

	original, err := a.backend.PopMessageFromBacklog(ctx, msg.ID)
	if err != nil {
		return errors.Wrap(err, "error fetching message from backend")
	}
	// restore return queue name for the caller
	msg.Retqueue = original.Retqueue

	err = a.backend.PushProcessedMessage(ctx, msg)
	if err != nil {
		return errors.Wrap(err, "error pushing the reply message")
	}
	return nil
}

func (a *App) handleFromReplyForward(ctx context.Context, msg Message) error {
	// reply have only one destination (source)
	dst := msg.TwinDst[0]

	r, err := a.resolver.Resolve(dst)

	if err != nil {
		return errors.Wrap(err, "couldn't resolve twin ip")
	}
	msg.Epoch = time.Now().Unix()
	err = msg.Sign(a.identity)
	if err != nil {
		return errors.Wrap(err, "couldn't sign reply message")
	}

	// forward to reply agent
	err = r.SendReply(msg)

	if err != nil {
		return errors.Wrap(err, "error forwarding reply from local service to the caller rmb")
	}

	return nil
}

func (a *App) handleFromReply(ctx context.Context, msg Message) error {

	if msg.Proxy {
		return a.handleFromReplyForProxy(ctx, msg)
	} else if msg.TwinDst[0] == a.twin {
		return a.handleFromReplyForMe(ctx, msg)
	} else if msg.TwinSrc == a.twin {
		return a.handleFromReplyForward(ctx, msg)
	}

	return nil
}

func (a *App) handleRetry(ctx context.Context) error {
	entries, err := a.backend.PopRetryMessages(ctx, 5*time.Second)
	if err != nil {
		return errors.Wrap(err, "couldn't read retry messages")
	}

	// iterate over each entries
	for _, entry := range entries {
		log.Debug().Str("key", entry.ID).Msg("retry needed")

		err := a.handleFromLocalItem(ctx, entry, entry.TwinDst[0])
		if err != nil {
			// just log the error, repushing to retry queue happens inside handleFromLocalItem
			log.Warn().Err(err).Msg("error handling message in retry queue")
		}
	}
	return nil
}

func (a *App) handleScrubbing(ctx context.Context) error {
	entries, err := a.backend.PopExpiredBacklogMessages(ctx)

	if err != nil {
		return errors.Wrap(err, "couldn't read backlog messages")
	}

	// iterate over each entries
	for _, entry := range entries {
		log.Debug().Str("key", entry.ID).Msg("expired")
		if repErr := a.respondWithError(ctx, entry, fmt.Errorf("request timeout (expiration reached, %d)", entry.Expiration)); repErr != nil {
			log.Error().Err(repErr).Msg("error responding to rmb called with error")
			log.Error().Err(err).Msg("original error")
		}
	}
	return nil
}

func (a *App) worker(ctx context.Context, in <-chan Envelope) {
	for {
		var envelope Envelope
		select {
		case envelope = <-in:
		case <-ctx.Done():
			return
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

func (a *App) runServer(ctx context.Context) {
	log.Info().Int("twin", a.twin).Msg("initializing agent server")

	// start the workers
	ch := make(chan Envelope)
	for i := 0; i < a.workers; i++ {
		go a.worker(ctx, ch)
	}

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
			continue
		}

		if err := envelope.Valid(); err != nil {
			log.Error().Err(err).Msg("received invalid message")
			a.respondWithError(ctx, envelope.Message, errors.Wrap(err, "received invalid message"))
			continue
		}

		select {
		case <-ctx.Done():
			return
		case ch <- envelope:
		}
	}
}
func (a *App) remote(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		errorReply(w, http.StatusBadRequest, "couldn't parse json")
		return
	}
	if err := msg.ValidateEpoch(); err != nil {
		errorReply(w, http.StatusBadRequest, err.Error())
		return
	}
	pk, err := a.resolver.PublicKey(msg.TwinSrc)
	if errors.Is(err, substrate.ErrNotFound) {
		errorReply(w, http.StatusBadRequest, "source twin %d not found", msg.TwinSrc)
		return
	} else if err != nil {
		errorReply(w, http.StatusBadGateway, "couldn't get twin %d public key: %s", msg.TwinSrc, err.Error())
		return
	}
	if err := msg.Verify(pk); err != nil {
		errorReply(w, http.StatusBadRequest, err.Error())
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

	if err := msg.ValidateEpoch(); err != nil {
		errorReply(w, http.StatusBadRequest, err.Error())
		return
	}

	pk, err := a.resolver.PublicKey(msg.TwinSrc)
	if errors.Is(err, substrate.ErrNotFound) {
		errorReply(w, http.StatusBadRequest, "source twin %d not found", msg.TwinSrc)
		return
	} else if err != nil {
		errorReply(w, http.StatusBadGateway, "couldn't get twin %d public key: %s", msg.TwinSrc, err.Error())
		return
	}
	if err := msg.Verify(pk); err != nil {
		errorReply(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := a.backend.QueueReply(r.Context(), msg); err != nil {
		err = errors.Wrap(err, "couldn't push entry to reply queue")
		errorReply(w, http.StatusInternalServerError, err.Error())
		return
	}
	successReply(w)
}

func (a *App) run(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		errorReply(w, http.StatusBadRequest, "couldn't parse json")
		return
	}

	if err := msg.ValidateEpoch(); err != nil {
		errorReply(w, http.StatusBadRequest, err.Error())
		return
	}

	pk, err := a.resolver.PublicKey(msg.TwinSrc)
	if errors.Is(err, substrate.ErrNotFound) {
		errorReply(w, http.StatusBadRequest, "source twin %d not found", msg.TwinSrc)
		return
	} else if err != nil {
		errorReply(w, http.StatusBadGateway, "couldn't get twin %d public key: %s", msg.TwinSrc, err.Error())
		return
	}
	if err := msg.Verify(pk); err != nil {
		errorReply(w, http.StatusBadRequest, err.Error())
		return
	}

	msg.Proxy = true
	msg.Retqueue = uuid.New().String()
	if err := a.backend.QueueRemote(r.Context(), msg); err != nil {
		errorReply(w, http.StatusInternalServerError, "couldn't queue message for processing")
		return
	}

	ret := MessageIdentifier{
		Retqueue: msg.Retqueue,
	}

	json.NewEncoder(w).Encode(&ret)
}

func (a *App) getResult(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var msgIdentifier MessageIdentifier
	if err := json.NewDecoder(r.Body).Decode(&msgIdentifier); err != nil {
		errorReply(w, http.StatusBadRequest, "couldn't parse json")
		return
	}

	if valid := IsValidUUID(msgIdentifier.Retqueue); !valid {
		errorReply(w, http.StatusBadRequest, "Invalid Retqueue, it should be a valid UUID")
		return
	}

	response, err := a.backend.GetMessageReply(r.Context(), msgIdentifier)
	if err != nil {
		errorReply(w, http.StatusInternalServerError, err.Error())
		return
	}
	for idx := range response {
		response[idx].Epoch = time.Now().Unix()
		if err := response[idx].Sign(a.identity); err != nil {
			log.Error().Err(err).Msg("failed to sign reply")
			errorReply(w, http.StatusInternalServerError, "signing failed")
			return
		}
	}
	json.NewEncoder(w).Encode(&response)
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


func NewServer(resolver TwinResolver, backend RedisBackend, workers int, identity substrate.Identity) (*App, error) {
	router := mux.NewRouter()
	backend := NewRedisBackend(redisServer)
	sub, err := substrate.NewSubstrate(substrateURL)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get substrate client")
	}
	twin, err := sub.GetTwinByPubKey(identity.PublicKey())
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get twin associated with mnemonics")
	}
	resolver, err := NewSubstrateResolver(sub)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get a client to explorer resolver")
	}

	a := &App{
		backend:  backend,
		identity: identity,
		twin:     int(twin),
		resolver: NewCacheResolver(resolver, 5*time.Minute),
		server: &http.Server{
			Handler: router,
			Addr:    "0.0.0.0:8051",
		},
		workers: workers,
	}
	router.HandleFunc("/zbus-reply", a.reply)
	router.HandleFunc("/zbus-remote", a.remote)
	router.HandleFunc("/zbus-cmd", a.run)
	router.HandleFunc("/zbus-result", a.getResult)

	return a, nil
}
