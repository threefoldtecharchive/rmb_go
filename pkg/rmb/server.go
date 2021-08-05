package rmb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

type Message struct {
	Version    int    `json:"ver"`
	Id         string `json:"uid"`
	Command    string `json:"cmd"`
	Expiration int    `json:"exp"`
	Retry      int    `json:"try"`
	Data       string `json:"dat"`
	Twin_src   int    `json:"src"`
	Twin_dst   []int  `json:"ds"`
	Retqueue   string `json:"ret"`
	Schema     string `json:"shm"`
	Epoch      int    `json:"now"`
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
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	twinInfo := SubstrateTwin{}

	if err := json.Unmarshal(body, &twinInfo); err != nil {
		fmt.Println(err)
		return "", errors.Wrap(err, "couldn't parse json response")
	}

	return twinInfo.Ip, nil
}

func (a *App) handle_from_local_prepare_item(msg Message, dst int) error {
	update := msg
	update.Twin_src = a.twin
	update.Twin_dst = []int{dst}

	if a.debug {
		fmt.Println("[+] resolving twin: %d", dst)
	}

	dstIp, err := a.resolve(dst)

	if err != nil {
		return err
	}

	id := a.redis.Incr(a.ctx, fmt.Sprintf("msgbus.counter.%d", dst))
	update.Id = fmt.Sprintf("%d.%d", dst, id)
	update.Retqueue = "msgbus.system.reply"

	if a.debug {
		fmt.Println("[+] forwarding to %d", dstIp)
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

	if a.debug {
		fmt.Println("[+] message sent to target msgbus")
		fmt.Println(resp)
	}

	value, err := json.Marshal(update)
	if err != nil {
		return err
	}

	a.redis.HSet(a.ctx, "msgbus.system.backlog", update.Id, value)

	return nil
}

func (a *App) handle_from_local_prepare(msg Message) error {
	var reserr error = nil
	for dst := range msg.Twin_dst {
		if err := a.handle_from_local_prepare_item(msg, dst); err != nil {
			reserr = err
		}
	}
	return reserr
}

func (a *App) handle_from_local(value string) error {
	msg := Message{}

	if err := json.Unmarshal([]byte(value), &msg); err != nil {
		fmt.Println(err)
		return errors.Wrap(err, "couldn't parse json")
	}

	if a.debug {
		fmt.Println(msg)
	}

	if err := a.validate_input(msg); err != nil {
		return errors.Wrap(err, "local: couldn't validate input")
	}

	return a.handle_from_local_prepare(msg)
}

func (a *App) handle_from_remote(value string) error {
	msg := Message{}

	if err := json.Unmarshal([]byte(value), &msg); err != nil {
		fmt.Println(err)
		return errors.Wrap(err, "couldn't parse json")
	}

	if err := a.validate_input(msg); err != nil {
		return errors.Wrap(err, "local: couldn't validate input")
	}

	fmt.Println("[+] forwarding to local service: msgbus." + msg.Command)

	// forward to local service
	a.redis.LPush(a.ctx, fmt.Sprintf("msgbus.%s", msg.Command), value)

	return nil
}

func (a *App) request_needs_retry(msg Message, update Message) error {
	fmt.Println("[-] could not send message to remote msgbus")

	// restore 'update' to original state
	update.Retqueue = msg.Retqueue

	if update.Retry == 0 {
		fmt.Println("[-] no more retry, replying with error")
		update.Err = "could not send request and all retries done"
		output := json.Marshal(update)
		a.redis.LPush(update.Retqueue, output)
		return nil
	}

	fmt.Printf("[-] retry set to %d, adding to retry list", update.Retry)

	// remove one retry
	update.Retry -= 1
	update.Epoch = time.now().Unix()

	value := join.Marshal(update)
	a.redis.HSet(a.ctx, "msgbus.system.retry", update.id, value)?
}

func (a *App) handle_from_reply(value string) error {
	msg := Message{}

	if err := json.Unmarshal([]byte(value), &msg); err != nil {
		fmt.Println(err)
		return errors.Wrap(err, "couldn't parse json")
	}

	if a.debug {
		fmt.Println(msg)
	}

	if err := a.validate_input(msg); err != nil {
		return errors.Wrap(err, "local: couldn't validate input")
	}

	if msg.Twin_dst[0] == config.myid {
		handle_from_reply_for_me(msg, r, value)

	} else if msg.twin_src == config.myid {
		handle_from_reply_forward(msg, r, config, value)
	}

	return nil
}

func (a *App) run_server() {
	fmt.Println("[+] twin id: %d", a.twin)

	fmt.Println("[+] initializing agent server")
	for {
		m := a.redis.BLPop(a.ctx, 0, "msgbus.system.local", "msgbus.system.remote", "msgbus.system.reply")

		// can len be 0?

		res, err := m.Result()
		if err != nil {
			fmt.Println(errors.Wrap(err, "error blpop"))
		}

		if res[0] == "msgbus.system.reply" {
			handle_from_reply(res[1])
		}
		if res[0] == "msgbus.system.local" {
			handle_from_local(res[1])
		}
		if res[0] == "msgbus.system.remote" {
			handle_from_remote(res[1])
		}
	}
}

func (a *App) remote(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Write(errorReply("couldn't read body"))
		return
	}
	if a.debug {
		fmt.Println("[+] request from external agent")
		fmt.Println(body)
	}

	msg := Message{}

	if err := json.Unmarshal(body, &msg); err != nil {
		w.Write(errorReply("couldn't parse json"))
		return
	}
	// locking?
	a.redis.LPush(a.ctx, "msgbus.system.remote", body)
	w.Write(successReply())

}

func (a *App) reply(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Write(errorReply("couldn't read body"))
		return
	}
	if a.debug {
		fmt.Println("[+] request from external agent")
		fmt.Println(body)
	}

	msg := Message{}

	if err := json.Unmarshal(body, &msg); err != nil {
		w.Write(errorReply("couldn't parse json"))
		return
	}
	// locking?
	a.redis.LPush(a.ctx, "msgbus.system.reply", body)
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
	router.HandleFunc("/reply", a.reply)
	router.HandleFunc("/remote", a.remote)
	http.Handle("/", router)
}
