package main

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/threefoldtech/rmb-go/pkg/client"
)

func test_client() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	mb := client.MessageBusClient{
		Client: rdb,
		Ctx:    context.Background(),
	}

	msg_twin := client.Prepare("griddb.twins.get", []int{9}, 0, 2)
	mb.Send(msg_twin, "9")
	response_twin := mb.Read(msg_twin)
	fmt.Println(fmt.Sprintf("Result Received for reply:%s", msg_twin.Retqueue))
	for _, result := range response_twin {
		fmt.Println(result)
	}

}

func main() {
	test_client()
}
