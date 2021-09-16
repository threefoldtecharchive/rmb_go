package rmb

import "net/http"

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

type MessageIdentifier struct {
	Retqueue string `json:"retqueue"`
}

type App struct {
	backend  Backend
	twin     int
	resolver TwinResolver
	server   *http.Server
}
