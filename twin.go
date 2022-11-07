package rmb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/rs/zerolog/log"
	"github.com/threefoldtech/substrate-client"
)

type TwinResolver interface {
	Resolve(twin int) (TwinClient, error)
	PublicKey(twin int) ([]byte, error)
}

type TwinClient interface {
	SendRemote(msg Message) error
	SendReply(msg Message) error
}

type cacheResolver struct {
	TwinResolver
	cache *cache.Cache
}

type substrateResolver struct {
	client *substrate.Substrate
}

type twinClient struct {
	dstIP string
}

func remoteURL(timeIP string) string {
	return fmt.Sprintf("http://%s:8051/zbus-remote", timeIP)
}

func replyURL(timeIP string) string {
	return fmt.Sprintf("http://%s:8051/zbus-reply", timeIP)
}

func NewCacheResolver(resolver TwinResolver, expiration time.Duration) TwinResolver {
	return &cacheResolver{
		TwinResolver: resolver,
		cache:        cache.New(expiration, time.Minute),
	}
}

func (c *cacheResolver) Resolve(twin int) (TwinClient, error) {
	key := fmt.Sprint(twin)
	cached, ok := c.cache.Get(key)
	if ok {
		log.Debug().Int("twin", twin).Msg("cache hit")
		return cached.(TwinClient), nil
	}

	client, err := c.TwinResolver.Resolve(twin)
	if err != nil {
		return nil, err
	}

	c.cache.Set(key, client, cache.DefaultExpiration)
	return client, nil
}

func (c *cacheResolver) PublicKey(twin int) ([]byte, error) {
	key := fmt.Sprintf("pk:%d", twin)
	cached, ok := c.cache.Get(key)
	if ok {
		log.Debug().Int("twin", twin).Msg("public key cache hit")
		return cached.([]byte), nil
	}

	pk, err := c.TwinResolver.PublicKey(twin)
	if err != nil {
		return nil, err
	}

	c.cache.Set(key, pk, cache.DefaultExpiration)
	return pk, nil
}

func NewSubstrateResolver(client *substrate.Substrate) (TwinResolver, error) {
	return &substrateResolver{
		client: client,
	}, nil
}

func (r substrateResolver) Resolve(timeID int) (TwinClient, error) {
	log.Debug().Int("twin", timeID).Msg("resolving twin")

	twin, err := r.client.GetTwin(uint32(timeID))
	if err != nil {
		return nil, err
	}
	log.Debug().Str("ip", twin.IP).Msg("resolved twin ip")

	return &twinClient{
		dstIP: twin.IP,
	}, nil
}

func (r substrateResolver) PublicKey(twinID int) ([]byte, error) {
	log.Debug().Int("twin", twinID).Msg("resolving twin")

	twin, err := r.client.GetTwin(uint32(twinID))
	if err != nil {
		return nil, err
	}

	return twin.Account.PublicKey(), nil
}

func (c *twinClient) readError(r io.Reader) string {
	var body struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}

	if err := json.NewDecoder(r).Decode(&body); err != nil {
		return fmt.Sprintf("failed to read response body: %s", err)
	}

	return body.Message
}

func (c *twinClient) SendRemote(msg Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*10))
	defer cancel()

	var buffer bytes.Buffer
	if err := json.NewEncoder(&buffer).Encode(msg); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, remoteURL(c.dstIP), &buffer)
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
		return fmt.Errorf("failed to send remote: %s (%s)", resp.Status, c.readError(resp.Body))
	}

	return nil
}

func (c *twinClient) SendReply(msg Message) error {
	var buffer bytes.Buffer
	if err := json.NewEncoder(&buffer).Encode(msg); err != nil {
		return err
	}
	resp, err := http.Post(replyURL(c.dstIP), "application/json", &buffer)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// body
		return fmt.Errorf("failed to send remote: %s (%s)", resp.Status, c.readError(resp.Body))
	}

	return err
}
