package rmb

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

router := mux.NewRouter().StrictSlash(true)


type App struct {
	
}








func main() {

}
