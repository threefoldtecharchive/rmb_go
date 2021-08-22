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
	Resolve(timeID int) (TwinCommunicationChannelInterace, error)
}

type TwinCommunicationChannelInterace interface {
	SendRemote(data []byte) error
	SendReply(data []byte) error
}

type TwinExplorerResolver struct {
	substrate string
}

type TwinCommunicationChannel struct {
	dstIP string
}

func remoteURL(timeIP string) string {
	return fmt.Sprintf("http://%s:8051/zbus-remote", timeIP)
}

func replyURL(timeIP string) string {
	return fmt.Sprintf("http://%s:8051/zbus-reply", timeIP)
}

func (r TwinExplorerResolver) Resolve(timeID int) (TwinCommunicationChannelInterace, error) {
	log.Debug().Int("twin", timeID).Msg("resolving twin")

	client, err := substrate.NewSubstrate(r.substrate)
	if err != nil {
		return nil, err
	}
	twin, err := client.GetTwin(uint32(timeID))
	if err != nil {
		return nil, err
	}
	log.Debug().Str("ip", twin.IP).Msg("resolved twin ip")

	return &TwinCommunicationChannel{
		dstIP: twin.IP,
	}, nil
}

func (c *TwinCommunicationChannel) SendRemote(data []byte) error {
	resp, err := http.Post(remoteURL(c.dstIP), "application/json", bytes.NewBuffer(data))
	// check on response for non-communication errors?
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Warn().Err(err).Msg("couldn't read body of remote request")
	} else {
		log.Debug().Str("response", string(body)).Int("status", resp.StatusCode).Msg("message sent to target msgbus")
	}
	return err
}

func (c *TwinCommunicationChannel) SendReply(data []byte) error {
	resp, err := http.Post(replyURL(c.dstIP), "application/json", bytes.NewBuffer(data))

	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Warn().Err(err).Msg("couldn't read body of reply request")
	} else {
		log.Debug().Str("response", string(body)).Int("status", resp.StatusCode).Msg("message sent to target msgbus")
	}
	return err
}
