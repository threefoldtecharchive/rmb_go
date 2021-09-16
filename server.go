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

func errorReply(w http.ResponseWriter, status int, message string) {
	w.WriteHeader(status)
	fmt.Fprintf(w, "{\"status\": \"error\", \"message\": \"%s\"}", message)
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
	if msg.Epoch == 0 {
		msg.Epoch = time.Now().Unix()
	}
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
	log.Info().Str("queue", fmt.Sprintf("msgbus.%s", msg.Command)).Msg("forwarding to local service")

	// forward to local service
	return a.backend.QueueCommand(ctx, msg)
}

func (a *App) handleFromReplyForMe(ctx context.Context, msg Message) error {
	log.Info().Msg("message reply for me, fetching backlog")

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

	// forward to reply agent
	err = r.SendReply(msg)

	if err != nil {
		return errors.Wrap(err, "error forwarding reply from local service to the caller rmb")
	}

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

func (a *App) handleRetry(ctx context.Context) error {
	log.Debug().Msg("checking retries")

	entries, err := a.backend.PopRetryMessages(ctx, 5*time.Second)
	if err != nil {
		return errors.Wrap(err, "couldn't read retry messages")
	}

	// iterate over each entries
	for _, entry := range entries {
		log.Info().Str("key", entry.ID).Msg("retry needed")

		err := a.handleFromLocalItem(ctx, entry, entry.TwinDst[0])
		if err != nil {
			// just log the error, repushing to retry queue happens inside handleFromLocalItem
			log.Warn().Err(err).Msg("error handling message in retry queue")
		}
	}
	return nil
}

func (a *App) handleScrubbing(ctx context.Context) error {
	log.Debug().Msg("scrubbing")

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
			continue
		}

		if err := envelope.Valid(); err != nil {
			log.Error().Err(err).Msg("received invalid message")
			a.respondWithError(ctx, envelope.Message, errors.Wrap(err, "received invalid message"))
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

func (a *App) run(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		errorReply(w, http.StatusBadRequest, "couldn't parse json")
		return
	}

	msg.TwinSrc = a.twin
	msg.Retqueue = uuid.New().String()

	err := a.backend.PushToLocal(r.Context(), msg)

	if err != nil {
		log.Error().Err(err).Msg("Can't push the message to local")
		errorReply(w, http.StatusInternalServerError, err.Error())
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

func NewServer(substrate string, redisServer string, twin int) (*App, error) {
	router := mux.NewRouter()
	backend := NewRedisBackend(redisServer)
	resolver, err := NewTwinResolver(substrate)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get a client to explorer resolver")
	}
	a := &App{
		backend:  backend,
		twin:     twin,
		resolver: resolver,
		server: &http.Server{
			Handler: router,
			Addr:    "0.0.0.0:8051",
		},
	}
	router.HandleFunc("/zbus-reply", a.reply)
	router.HandleFunc("/zbus-remote", a.remote)
	router.HandleFunc("/zbus-cmd", a.run)
	router.HandleFunc("/zbus-result", a.getResult)

	return a, nil
}
