package main

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/threefoldtech/rmb/client"
)

func testClient() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	mb := client.MessageBusClient{
		Client: rdb,
		Ctx:    context.Background(),
	}

	msgTwin := client.Prepare("griddb.twins.get", []int{9}, 0, 2)
	mb.Send(msgTwin, "9")
	responseTwin := mb.Read(msgTwin)
	fmt.Printf("Result Received for reply:%s\n", msgTwin.Retqueue)
	for _, result := range responseTwin {
		fmt.Println(result)
	}

}

func main() {
	testClient()
}
