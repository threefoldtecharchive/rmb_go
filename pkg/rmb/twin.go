package rmb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/rs/zerolog/log"
	"github.com/threefoldtech/zos/pkg/substrate"
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

	client, err := substrate.NewSubstrate(r.substrate)
	if err != nil {
		return nil, err
	}
	twin, err := client.GetTwin(uint32(twinId))
	if err != nil {
		return nil, err
	}
	log.Debug().Str("ip", twin.IP).Msg("resolved twin ip")

	return &TwinCommunicationChannel{
		dstIp: twin.IP,
	}, nil
}

func (c *TwinCommunicationChannel) SendRemote(data []byte) error {
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

func (c *TwinCommunicationChannel) SendReply(data []byte) error {
	resp, err := http.Post(replyUrl(c.dstIp), "application/json", bytes.NewBuffer(data))
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Warn().Err(err).Msg("couldn't read body of reply request")
	} else {
		log.Debug().Str("response", string(body)).Int("status", resp.StatusCode).Msg("message sent to target msgbus")
	}
	return err
}
