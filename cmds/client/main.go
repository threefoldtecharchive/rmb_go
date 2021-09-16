package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/threefoldtech/rmb"
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

	msgTwin := client.Prepare("zos.statistics.get", []int{2, 44}, 0, 2)
	mb.Send(msgTwin, "70")
	responseTwin := mb.Read(msgTwin)
	fmt.Printf("Result Received for reply:%s\n", msgTwin.Retqueue)
	for _, result := range responseTwin {
		fmt.Println(result)
	}

}

func testHttpClient() (rmb.Message, error) {
	msgTwin := client.Prepare("zos.statistics.get", []int{2, 70}, 0, 2)
	msgTwin.Data = base64.StdEncoding.EncodeToString([]byte("70"))

	var buffer bytes.Buffer
	if err := json.NewEncoder(&buffer).Encode(msgTwin); err != nil {
		return msgTwin, err
	}
	resp, err := http.Post("http://localhost:8051/zbus-cmd", "application/json", &buffer)
	if err != nil {
		fmt.Println(err.Error())
		return msgTwin, err
	}

	defer resp.Body.Close()
	return msgTwin, nil
}

func testGetResult(msg rmb.Message) error {
	var buffer bytes.Buffer
	if err := json.NewEncoder(&buffer).Encode(msg); err != nil {
		return err
	}
	resp, err := http.Post("http://localhost:8051/zbus-result", "application/json", &buffer)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	fmt.Println(string(body))
	return nil
}

func main() {
	// testClient()
	msg, _ := testHttpClient()
	fmt.Println(msg)
	time.Sleep(6 * time.Second)
	testGetResult(msg)
}
