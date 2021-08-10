package rmb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

type BackendMock struct {
	lists map[string][]string
	dicts map[string]map[string]string
	ints  map[string]int64
}

func NewBackendMock() *BackendMock {
	r := &BackendMock{
		lists: make(map[string][]string),
		dicts: make(map[string]map[string]string),
		ints:  make(map[string]int64),
	}
	return r
}

func (r *BackendMock) HGet(ctx context.Context, key string, field string) (string, error) {
	i, ok := r.dicts[key]
	if ok == false {
		i = make(map[string]string)
	}
	v, ok := i[field]
	if ok == false {
		v = ""
	}
	return v, nil
}

func (r *BackendMock) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	i, ok := r.dicts[key]
	if ok == false {
		i = make(map[string]string)
	}

	return i, nil
}

func (r *BackendMock) HDel(ctx context.Context, key string, field string) (int64, error) { // int8
	i, ok := r.dicts[key]
	if ok == false {
		return 1, nil
	}

	_, ok = i[field]
	if ok {
		delete(i, field)
		return 1, nil
	}
	return 0, nil
}

func (r *BackendMock) LPush(ctx context.Context, key string, value []byte) (int64, error) {
	log.Debug().Str("key", key).Str("value", string(value)).Msg("pushing to queue")
	_, ok := r.lists[key]
	if ok == false {
		r.lists[key] = make([]string, 0)
	}
	r.lists[key] = append(r.lists[key], string(value))
	log.Debug().Int("len", len(r.lists[key])).Msg("length after adding")
	return 1, nil
}

func (r *BackendMock) BLPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	start, now := time.Now().Unix(), time.Now().Unix()
	log.Debug().Int("length", len(r.lists[keys[0]])).Msg("debug") //
	for now < start+int64(timeout/1000000000) {                   // convert from nanoseconds to seconds
		now = time.Now().Unix()
		for _, key := range keys {
			i, ok := r.lists[key]
			if ok == false {
				continue
			}
			if len(key) > 0 {
				value := i[len(i)-1]
				r.lists[key] = i[:len(i)-1] // pop the element
				return []string{key, value}, nil
			}
		}

	}
	return nil, redis.Nil
}

func (r *BackendMock) Incr(ctx context.Context, key string) (int64, error) {
	_, ok := r.ints[key]
	if ok == false {
		r.ints[key] = 0
	}
	r.ints[key] += 1
	return r.ints[key], nil
}

func (r *BackendMock) HSet(ctx context.Context, key string, field string, value []byte) (int64, error) {
	_, ok := r.dicts[key]
	if ok == false {
		r.dicts[key] = make(map[string]string)
	}

	r.dicts[key][field] = string(value)
	return 1, nil
}

func (r *BackendMock) Get(ctx context.Context, key string) (int64, error) {
	_, ok := r.ints[key]
	if ok == false {
		r.ints[key] = 0
	}
	r.ints[key] += 1
	return r.ints[key], nil
}

type ResolverMock struct {
	twin map[int]*TwinCommunicationMock
}

func NewResolverMock() ResolverMock {
	r := ResolverMock{
		twin: make(map[int]*TwinCommunicationMock),
	}
	return r
}

type TwinCommunicationMock struct {
	remote [][]byte
	reply  [][]byte
	twinId int
}

func (r ResolverMock) Resolve(twinId int) (TwinCommunicationChannelInterace, error) {
	log.Debug().Int("twin", twinId).Msg("resolving mock twinId")
	e, ok := r.twin[twinId]
	if ok == false {
		e = &TwinCommunicationMock{
			twinId: twinId,
			remote: make([][]byte, 0),
			reply:  make([][]byte, 0),
		}
		r.twin[twinId] = e
	}
	return e, nil
}

func (c *TwinCommunicationMock) SendRemote(data []byte) error {
	log.Debug().Int("twin", c.twinId).Msg("sending remote")
	c.remote = append(c.remote, data)
	return nil
}

func (c *TwinCommunicationMock) SendReply(data []byte) error {
	c.reply = append(c.reply, data)
	return nil
}
func (c *TwinCommunicationMock) PopRemote() []byte {
	last := c.remote[len(c.remote)-1]
	c.remote = c.remote[:len(c.remote)-1]
	return last
}

func (c *TwinCommunicationMock) PopReply() []byte {
	last := c.reply[len(c.reply)-1]
	c.reply = c.reply[:len(c.reply)-1]
	return last
}
func setup() (a App, s BackendMock, r ResolverMock) {
	backend := NewBackendMock()
	resolver := NewResolverMock()
	app := App{
		debug:    true,
		redis:    backend,
		twin:     1,
		ctx:      context.Background(),
		resolver: resolver,
	}

	return app, *backend, resolver
}

func TestHandleFromLocalPrepareItem(t *testing.T) {
	app, _, resolver := setup()
	secondTwin, _ := resolver.Resolve(2)
	secondTwinMock := secondTwin.(*TwinCommunicationMock)
	msg := Message{
		Version:    1,
		Id:         "",
		Command:    "griddb.twins.get",
		Expiration: 0,
		Retry:      2,
		Data:       base64.StdEncoding.EncodeToString([]byte("2")),
		Twin_src:   0,
		Twin_dst:   []int{2},
		Retqueue:   uuid.New().String(),
		Schema:     "",
		Epoch:      time.Now().Unix(),
		Err:        "",
	}
	err := app.handle_from_local_prepare_item(msg, 2)
	if err != nil {
		log.Err(err).Msg("error while handling from local perpare item")
	}
	log.Debug().Int("len_twin_remote_2:", len(secondTwinMock.remote)).Int("len_twin_reply_2:", len(secondTwinMock.reply)).Msg("queue data")
	received := secondTwinMock.PopRemote()
	recmsg := Message{}
	json.Unmarshal(received, &recmsg)
	assert.Equal(t, recmsg.Retqueue, "msgbus.system.reply")
	assert.Equal(t, recmsg.Epoch, msg.Epoch)
	assert.Equal(t, recmsg.Command, msg.Command)
	assert.Equal(t, recmsg.Retry, msg.Retry)
	assert.Equal(t, recmsg.Data, msg.Data)
}

func TestHandleFromLocal(t *testing.T) {
	app, _, resolver := setup()
	secondTwin, _ := resolver.Resolve(2)
	secondTwinMock := secondTwin.(*TwinCommunicationMock)
	fourthTwin, _ := resolver.Resolve(4)
	fourthTwinMock := fourthTwin.(*TwinCommunicationMock)
	msg := Message{
		Version:    1,
		Id:         "",
		Command:    "griddb.twins.get",
		Expiration: 0,
		Retry:      2,
		Data:       base64.StdEncoding.EncodeToString([]byte("2")),
		Twin_src:   0,
		Twin_dst:   []int{2, 4},
		Retqueue:   uuid.New().String(),
		Schema:     "",
		Epoch:      time.Now().Unix(),
		Err:        "",
	}
	msgString, err := json.Marshal(msg)
	if err != nil {
		log.Err(err).Msg("error while parsing json")
	}
	err = app.handle_from_local(string(msgString))
	if err != nil {
		log.Err(err).Msg("error while handling from local perpare item")
	}
	received := secondTwinMock.PopRemote()
	recmsg := Message{}
	json.Unmarshal(received, &recmsg)
	assert.Equal(t, recmsg.Retqueue, "msgbus.system.reply")
	assert.Equal(t, recmsg.Epoch, msg.Epoch)
	assert.Equal(t, recmsg.Command, msg.Command)
	assert.Equal(t, recmsg.Retry, msg.Retry)
	assert.Equal(t, recmsg.Data, msg.Data)
	received = fourthTwinMock.PopRemote()
	recmsg = Message{}
	json.Unmarshal(received, &recmsg)
	assert.Equal(t, recmsg.Retqueue, "msgbus.system.reply")
	assert.Equal(t, recmsg.Epoch, msg.Epoch)
	assert.Equal(t, recmsg.Command, msg.Command)
	assert.Equal(t, recmsg.Retry, msg.Retry)
	assert.Equal(t, recmsg.Data, msg.Data)
}

func TestHandleFromRemote(t *testing.T) {
	app, backend, _ := setup()
	msg := Message{
		Version:    1,
		Id:         "",
		Command:    "griddb.twins.get",
		Expiration: 0,
		Retry:      2,
		Data:       base64.StdEncoding.EncodeToString([]byte("2")),
		Twin_src:   0,
		Twin_dst:   []int{2, 4},
		Retqueue:   uuid.New().String(),
		Schema:     "",
		Epoch:      time.Now().Unix(),
		Err:        "",
	}
	msgString, err := json.Marshal(msg)
	if err != nil {
		log.Err(err).Msg("error while parsing json")
	}
	err = app.handle_from_remote(string(msgString))
	if err != nil {
		log.Err(err).Msg("error while handling from local perpare item")
	}
	res, err := backend.BLPop(app.ctx, 1000000000, "msgbus.griddb.twins.get")
	if err != nil {
		log.Err(err).Msg("didn't find anything in msgbus.griddb.twins.get")
	}
	assert.Equal(t, res[0], "msgbus.griddb.twins.get")
	assert.Equal(t, res[1], string(msgString))
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
		Id:         "9.7",
		Command:    "griddb.twins.get",
		Expiration: 0,
		Retry:      2,
		Data:       base64.StdEncoding.EncodeToString([]byte("2")),
		Twin_src:   0,
		Twin_dst:   []int{2, 4},
		Retqueue:   uuid.New().String(),
		Schema:     "",
		Epoch:      time.Now().Unix(),
		Err:        "",
	}
	update := msg
	update.Retqueue = "msgbug.system.reply"
	update.Data = base64.StdEncoding.EncodeToString([]byte("result"))
	updateString, err := json.Marshal(update)
	if err != nil {
		log.Err(err).Msg("error while parsing json")
	}
	msgString, err := json.Marshal(msg)
	backend.HSet(app.ctx, "msgbus.system.backlog", msg.Id, msgString)
	err = app.handle_from_reply_for_me(update)
	if err != nil {
		log.Err(err).Msg("error while handling from local perpare item")
	}
	update.Retqueue = msg.Retqueue
	res, err := backend.BLPop(app.ctx, 1000000000, update.Retqueue)
	if err != nil {
		log.Err(err).Msg("didn't find anything in msgbus.griddb.twins.get")
	}
	updateString, err = json.Marshal(update)
	if err != nil {
		log.Err(err).Msg("error while handling from local perpare item")
	}
	assert.Equal(t, res[0], update.Retqueue)
	assert.Equal(t, res[1], string(updateString))
}
