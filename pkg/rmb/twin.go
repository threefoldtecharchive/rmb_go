package rmb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type TwinResolverInterface interface {
	Resolve(twinId int) (TwinCommunicationChannelInterace, error)
}

type TwinCommunicationChannelInterace interface {
	SendRemote(data []byte) error
	SendReply(data []byte) error
}

type TwinExplorerResolver struct {
	substrate string
}

type TwinCommunicationChannel struct {
	dstIp string
}

func remoteUrl(twinIp string) string {
	return fmt.Sprintf("http://%s:8051/zbus-remote", twinIp)
}

func replyUrl(twinIp string) string {
	return fmt.Sprintf("http://%s:8051/zbus-reply", twinIp)
}

func (r TwinExplorerResolver) Resolve(twinId int) (TwinCommunicationChannelInterace, error) {
	log.Debug().Int("twin", twinId).Msg("resolving twin")

	url := fmt.Sprintf("%s/twin/%d", r.substrate, twinId)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("explorer returned non-200 response: %s", resp.Status))
	}
	body, err := io.ReadAll(resp.Body)

	log.Debug().Str("status", resp.Status).Str("body", string(body)).Msg("result of twin request")

	if err != nil {
		return nil, err
	}
	twinInfo := SubstrateTwin{}

	if err := json.Unmarshal(body, &twinInfo); err != nil {
		return nil, errors.Wrap(err, "couldn't parse json response")
	}

	log.Debug().Str("ip", twinInfo.Ip).Msg("resolved twin ip")

	return TwinCommunicationChannel{
		dstIp: twinInfo.Ip,
	}, nil
}

func (c TwinCommunicationChannel) SendRemote(data []byte) error {
	resp, err := http.Post(remoteUrl(c.dstIp), "application/json", bytes.NewBuffer(data))
	// check on response for non-communication errors?

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Warn().Err(err).Msg("couldn't read body of remote request")
	} else {
		log.Debug().Str("response", string(body)).Int("status", resp.StatusCode).Msg("message sent to target msgbus")
	}
	return err
}

func (c TwinCommunicationChannel) SendReply(data []byte) error {
	resp, err := http.Post(replyUrl(c.dstIp), "application/json", bytes.NewBuffer(data))
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Warn().Err(err).Msg("couldn't read body of reply request")
	} else {
		log.Debug().Str("response", string(body)).Int("status", resp.StatusCode).Msg("message sent to target msgbus")
	}
	return err
}
