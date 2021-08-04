package rmb

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
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
	Id      int    `json:"iD"`
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
	redis     string
	twin      int
}

func (a *App) reply(w http.ResponseWriter, r *http.Request) {
	if a.debug {
		fmt.Println("[+] this is a debug message")
	}
	w.Write([]byte("hi"))
}

func (a *App) remote(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("hello"))
}

func Setup(router *mux.Router, debug bool, substrate string, redis string, twin int) {
	a := App{
		debug,
		substrate,
		redis,
		twin,
	}
	router.HandleFunc("/reply", a.reply)
	router.HandleFunc("/remote", a.remote)
	http.Handle("/", router)
}
