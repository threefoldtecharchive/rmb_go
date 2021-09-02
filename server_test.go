package rmb

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

type BackendMock struct {
	replies        []Message
	remotes        []Message
	locals         []Message
	retries        []Message
	backlog        map[string]Message
	commandMsgs    map[string][]Message
	commandReplies map[string][]Message
	ids            map[int]int
}

func NewBackendMock() *BackendMock {
	r := &BackendMock{
		replies:        make([]Message, 0),
		remotes:        make([]Message, 0),
		locals:         make([]Message, 0),
		retries:        make([]Message, 0),
		backlog:        make(map[string]Message),
		commandMsgs:    make(map[string][]Message),
		commandReplies: make(map[string][]Message),
		ids:            make(map[int]int),
	}
	return r
}

func (r *BackendMock) Next(ctx context.Context, timeout time.Duration) (Envelope, error) {
	start, now := time.Now().Unix(), time.Now().Unix()
	for now < start+int64(timeout/1000000000) { // convert from nanoseconds to seconds
		now = time.Now().Unix()
		if len(r.locals) > 0 {
			f, rest := r.locals[0], r.locals[1:]
			r.locals = rest
			return Envelope{
				Tag:     Local,
				Message: f,
			}, nil
		}
		if len(r.remotes) > 0 {
			f, rest := r.remotes[0], r.remotes[1:]
			r.remotes = rest
			return Envelope{
				Tag:     Remote,
				Message: f,
			}, nil
		}
		if len(r.replies) > 0 {
			f, rest := r.replies[0], r.replies[1:]
			r.replies = rest
			return Envelope{
				Tag:     Reply,
				Message: f,
			}, nil
		}

	}
	return Envelope{}, redis.Nil
}

func (r *BackendMock) QueueReply(ctx context.Context, msg Message) error {
	r.replies = append(r.replies, msg)
	return nil
}

func (r *BackendMock) QueueRemote(ctx context.Context, msg Message) error {
	r.remotes = append(r.remotes, msg)
	return nil
}

func (r *BackendMock) IncrementID(ctx context.Context, id int) (int64, error) {
	old, ok := r.ids[id]
	if !ok {
		old = 0
	}
	r.ids[id] = old + 1
	return int64(old), nil
}

func (r *BackendMock) PushToBacklog(ctx context.Context, msg Message, id string) error {
	r.backlog[id] = msg
	return nil
}

func (r *BackendMock) PopMessageFromBacklog(ctx context.Context, id string) (Message, error) {
	msg, ok := r.backlog[id]
	if !ok {
		msg = Message{}
		return msg, ErrNotAvailable
	}

	return msg, nil
}

func (r *BackendMock) QueueCommand(ctx context.Context, msg Message) error {
	cmd := msg.Command
	r.commandMsgs[cmd] = append(r.commandMsgs[cmd], msg)
	return nil
}

func (r *BackendMock) PushProcessedMessage(ctx context.Context, msg Message) error {
	r.commandReplies[msg.Retqueue] = append(r.commandReplies[msg.Retqueue], msg)
	return nil
}

func (r *BackendMock) QueueRetry(ctx context.Context, msg Message) error {
	r.retries = append(r.retries, msg)
	return nil
}

func (r *BackendMock) PopRetryMessages(ctx context.Context, olderThan time.Duration) ([]Message, error) {
	newlist := make([]Message, 0)
	now := time.Now().Unix()
	msgs := []Message{}

	for _, msg := range r.retries {
		if now > msg.Epoch+int64(olderThan/time.Second) {
			msgs = append(msgs, msg)
		} else {
			newlist = append(newlist, msg)
		}
	}
	r.retries = newlist
	return msgs, nil
}

func (r *BackendMock) PopExpiredBacklogMessages(ctx context.Context) ([]Message, error) {
	newbacklog := make(map[string]Message)
	msgs := []Message{}

	now := time.Now().Unix()
	for key, msg := range r.backlog {

		if msg.Epoch+msg.Expiration < now {
			msg.ID = key
			msgs = append(msgs, msg)
		} else {
			newbacklog[key] = msg
		}
	}
	r.backlog = newbacklog
	return msgs, nil
}

type ResolverMock struct {
	twin map[int]*TwinClientMock
}

func NewResolverMock() ResolverMock {
	r := ResolverMock{
		twin: make(map[int]*TwinClientMock),
	}
	return r
}

type TwinClientMock struct {
	remote []Message
	reply  []Message
	timeID int
}

func (r ResolverMock) Resolve(timeID int) (TwinClient, error) {
	log.Debug().Int("twin", timeID).Msg("resolving mock timeID")
	e, ok := r.twin[timeID]
	if ok == false {
		e = &TwinClientMock{
			timeID: timeID,
			remote: make([]Message, 0),
			reply:  make([]Message, 0),
		}
		r.twin[timeID] = e
	}
	return e, nil
}

func (c *TwinClientMock) SendRemote(data Message) error {
	log.Debug().Int("twin", c.timeID).Msg("sending remote")
	c.remote = append(c.remote, data)
	return nil
}

func (c *TwinClientMock) SendReply(data Message) error {
	c.reply = append(c.reply, data)
	return nil
}
func (c *TwinClientMock) PopRemote() Message {
	last := c.remote[len(c.remote)-1]
	c.remote = c.remote[:len(c.remote)-1]
	return last
}

func (c *TwinClientMock) PopReply() Message {
	last := c.reply[len(c.reply)-1]
	c.reply = c.reply[:len(c.reply)-1]
	return last
}
func setup() (a App, s BackendMock, r ResolverMock) {
	backend := NewBackendMock()
	resolver := NewResolverMock()
	app := App{
		backend:  backend,
		twin:     1,
		resolver: resolver,
	}

	return app, *backend, resolver
}

func TestHandleFromLocalPrepareItem(t *testing.T) {
	app, _, resolver := setup()
	secondTwin, _ := resolver.Resolve(2)
	secondTwinMock := secondTwin.(*TwinClientMock)
	msg := Message{
		Version:    1,
		ID:         "",
		Command:    "griddb.twins.get",
		Expiration: 0,
		Retry:      2,
		Data:       base64.StdEncoding.EncodeToString([]byte("2")),
		TwinSrc:    0,
		TwinDst:    []int{2},
		Retqueue:   uuid.New().String(),
		Schema:     "",
		Epoch:      time.Now().Unix(),
		Err:        "",
	}
	err := app.handleFromLocalItem(context.TODO(), msg, 2)
	if err != nil {
		log.Err(err).Msg("error while handling from local perpare item")
	}
	log.Debug().Int("len_twin_remote_2:", len(secondTwinMock.remote)).Int("len_twin_reply_2:", len(secondTwinMock.reply)).Msg("queue data")
	received := secondTwinMock.PopRemote()
	assert.Equal(t, received.Retqueue, "msgbus.system.reply")
	assert.Equal(t, received.Epoch, msg.Epoch)
	assert.Equal(t, received.Command, msg.Command)
	assert.Equal(t, received.Retry, msg.Retry)
	assert.Equal(t, received.Data, msg.Data)
}

func TestHandleFromLocal(t *testing.T) {
	app, _, resolver := setup()
	secondTwin, _ := resolver.Resolve(2)
	secondTwinMock := secondTwin.(*TwinClientMock)
	fourthTwin, _ := resolver.Resolve(4)
	fourthTwinMock := fourthTwin.(*TwinClientMock)
	msg := Message{
		Version:    1,
		ID:         "",
		Command:    "griddb.twins.get",
		Expiration: 0,
		Retry:      2,
		Data:       base64.StdEncoding.EncodeToString([]byte("2")),
		TwinSrc:    0,
		TwinDst:    []int{2, 4},
		Retqueue:   uuid.New().String(),
		Schema:     "",
		Epoch:      time.Now().Unix(),
		Err:        "",
	}
	err := app.handleFromLocal(context.TODO(), msg)
	if err != nil {
		log.Err(err).Msg("error while handling from local perpare item")
	}
	received := secondTwinMock.PopRemote()
	assert.Equal(t, received.Retqueue, "msgbus.system.reply")
	assert.Equal(t, received.Epoch, msg.Epoch)
	assert.Equal(t, received.Command, msg.Command)
	assert.Equal(t, received.Retry, msg.Retry)
	assert.Equal(t, received.Data, msg.Data)
	received = fourthTwinMock.PopRemote()
	assert.Equal(t, received.Retqueue, "msgbus.system.reply")
	assert.Equal(t, received.Epoch, msg.Epoch)
	assert.Equal(t, received.Command, msg.Command)
	assert.Equal(t, received.Retry, msg.Retry)
	assert.Equal(t, received.Data, msg.Data)
}

func TestHandleFromRemote(t *testing.T) {
	app, backend, _ := setup()
	msg := Message{
		Version:    1,
		ID:         "",
		Command:    "griddb.twins.get",
		Expiration: 0,
		Retry:      2,
		Data:       base64.StdEncoding.EncodeToString([]byte("2")),
		TwinSrc:    0,
		TwinDst:    []int{2, 4},
		Retqueue:   uuid.New().String(),
		Schema:     "",
		Epoch:      time.Now().Unix(),
		Err:        "",
	}
	err := app.handleFromRemote(context.TODO(), msg)
	if err != nil {
		log.Err(err).Msg("error while handling from local perpare item")
	}
	q, ok := backend.commandMsgs["griddb.twins.get"]
	if !ok || len(q) == 0 {
		log.Err(err).Msg("didn't find command in griddb.twins.get")
	}
	res := q[0]
	assert.Equal(t, res.Retqueue, msg.Retqueue)
	assert.Equal(t, res.Epoch, msg.Epoch)
	assert.Equal(t, res.Command, msg.Command)
	assert.Equal(t, res.Retry, msg.Retry)
	assert.Equal(t, res.Data, msg.Data)
}

func TestHandleFromReplyForMe(t *testing.T) {
	/*
		     * 1. the server sent a message and received a reply
			 * 2. the original message is stored in the backlog queue
			 * 3. the original return queue is fetched from the backlog
			 * 4. the reply message is pushed to this reply queue
			 * 5. the message is deleted from the backlog
	*/

	app, backend, _ := setup()
	msg := Message{
		Version:    1,
		ID:         "9.7",
		Command:    "griddb.twins.get",
		Expiration: 0,
		Retry:      2,
		Data:       base64.StdEncoding.EncodeToString([]byte("2")),
		TwinSrc:    0,
		TwinDst:    []int{2, 4},
		Retqueue:   uuid.New().String(),
		Schema:     "",
		Epoch:      time.Now().Unix(),
		Err:        "",
	}
	update := msg
	update.Retqueue = "msgbug.system.reply"
	update.Data = base64.StdEncoding.EncodeToString([]byte("result"))
	backend.PushToBacklog(context.TODO(), msg, msg.ID)
	err := app.handleFromReplyForMe(context.TODO(), update)
	if err != nil {
		log.Err(err).Msg("error while handling from local perpare item")
	}
	update.Retqueue = msg.Retqueue
	q := backend.commandReplies[update.Retqueue]
	if len(q) == 0 {
		log.Err(err).Msg("didn't find anything in msgbus.griddb.twins.get")
	}
	res := q[0]
	assert.Equal(t, res.Retqueue, update.Retqueue)
	assert.Equal(t, res.Epoch, update.Epoch)
	assert.Equal(t, res.Command, update.Command)
	assert.Equal(t, res.Retry, update.Retry)
	assert.Equal(t, res.Data, update.Data)
}
