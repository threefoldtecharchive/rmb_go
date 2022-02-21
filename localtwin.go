package rmb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type TwinLocationData struct {
	Data []TwinLocation `json:"data"`
}

type TwinLocation struct {
	Id       int    `json:"id"`
	Location string `json:"location"`
}

type LocalTwinResolver struct {
	data []TwinLocation
}

func NewLocalTwinResolver(config string) (TwinResolver, error) {
	log.Debug().Str("config", config).Msg("Local config")
	jsonResult := TwinLocationData{}
	if err := json.Unmarshal([]byte(config), &jsonResult); err != nil {
		return nil, err
	}

	return &LocalTwinResolver{
		data: jsonResult.Data,
	}, nil
}

func (twinLocationResolver *LocalTwinResolver) Resolve(twin int) (TwinClient, error) {
	var data *TwinLocation
	for _, v := range twinLocationResolver.data {
		if v.Id == twin {
			data = &v
			break
		}
	}

	log.Debug().Interface("location", data).Msg("lctn")

	if data == nil {
		return nil, errors.New(fmt.Sprintf("twin %s not found in config", twin))
	}

	return &LocalTwin{
		location: data.Location,
	}, nil
}

type LocalTwin struct {
	location string
}

func (localTwin *LocalTwin) SendRemote(msg Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*10))
	defer cancel()

	var buffer bytes.Buffer
	if err := json.NewEncoder(&buffer).Encode(msg); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, remoteURL(localTwin.location), &buffer)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// body
		return fmt.Errorf("failed to send remote: %s (%s)", resp.Status, localTwin.readError(resp.Body))
	}

	return nil
}

func (localTwin *LocalTwin) readError(r io.Reader) string {
	var body struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}

	if err := json.NewDecoder(r).Decode(&body); err != nil {
		return fmt.Sprintf("failed to read response body: %s", err)
	}

	return body.Message
}

func (localTwin *LocalTwin) SendReply(msg Message) error {
	var buffer bytes.Buffer
	if err := json.NewEncoder(&buffer).Encode(msg); err != nil {
		return err
	}
	resp, err := http.Post(replyURL(localTwin.location), "application/json", &buffer)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// body
		return fmt.Errorf("failed to send remote: %s (%s)", resp.Status, localTwin.readError(resp.Body))
	}

	return err
}
