package rmb

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/threefoldtech/substrate-client"
)

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
	Proxy      bool   `json:"pxy"`
	Err        string `json:"err"`
	Signature  string `json:"sig"`
}

type MessageIdentifier struct {
	Retqueue string `json:"retqueue"`
}

type ErrorReply struct {
	Status  string `json:",omitempty"`
	Message string `json:",omitempty"`
}

type App struct {
	backend  Backend
	identity substrate.Identity
	twin     int
	resolver TwinResolver
	server   *http.Server
	workers  int
}

func (m *Message) Sign(s substrate.Identity) error {
	data, err := m.challenge()
	if err != nil {
		return err
	}

	sig, err := s.Sign(data)
	if err != nil {
		return err
	}
	prefix, err := sigTypeToChar(s.Type())
	if err != nil {
		return err
	}
	m.Signature = hex.EncodeToString(append([]byte{prefix}, sig...))
	return nil
}

func (m *Message) ValidateEpoch() error {
	if time.Since(time.Unix(m.Epoch, 0)) > 20*time.Second {
		return fmt.Errorf("message is too old, sent since %s, sent time: %d, now: %d", time.Since(time.Unix(m.Epoch, 0)).String(), m.Epoch, time.Now().Unix())
	}
	return nil
}

func (m *Message) Verify(publicKey []byte) error {
	if m.Signature == "" {
		return errors.New("signature field is empty, visit the project github repo for instructions to update")
	}
	data, err := m.challenge()
	if err != nil {
		return err
	}
	decoded, err := hex.DecodeString(m.Signature)
	if err != nil {
		return errors.Wrap(err, "couldn't decode signature")
	}
	signatureType, err := charToSigType(decoded[0])
	if err != nil {
		return err
	}
	verifier, err := constructVerifier(publicKey, signatureType)
	if err != nil {
		return err
	}
	if !verifier.Verify(data, decoded[1:]) {
		return fmt.Errorf("couldn't verify signature")
	}
	return nil
}

func (m *Message) challenge() ([]byte, error) {
	hash := md5.New()

	if _, err := fmt.Fprintf(hash, "%d", m.Version); err != nil {
		return nil, err
	}

	if _, err := fmt.Fprintf(hash, "%s", m.ID); err != nil {
		return nil, err
	}

	if _, err := fmt.Fprintf(hash, "%s", m.Command); err != nil {
		return nil, err
	}

	if _, err := fmt.Fprintf(hash, "%s", m.Data); err != nil {
		return nil, err
	}

	if _, err := fmt.Fprintf(hash, "%d", m.TwinSrc); err != nil {
		return nil, err
	}

	for _, dst := range m.TwinDst {
		if _, err := fmt.Fprintf(hash, "%d", dst); err != nil {
			return nil, err
		}
	}

	if _, err := fmt.Fprintf(hash, "%s", m.Retqueue); err != nil {
		return nil, err
	}

	if _, err := fmt.Fprintf(hash, "%d", m.Epoch); err != nil {
		return nil, err
	}

	if _, err := fmt.Fprintf(hash, "%t", m.Proxy); err != nil {
		return nil, err
	}

	return hash.Sum(nil), nil
}
