package rmb

import (
	"encoding/hex"
	"fmt"
	"net/http"

	"github.com/pkg/errors"
	"github.com/threefoldtech/substrate-client"
)

type Message struct {
	Version       int    `json:"ver"`
	ID            string `json:"uid"`
	Command       string `json:"cmd"`
	Expiration    int64  `json:"exp"`
	Retry         int    `json:"try"`
	Data          string `json:"dat"`
	TwinSrc       int    `json:"src"`
	TwinDst       []int  `json:"dst"`
	Retqueue      string `json:"ret"`
	Schema        string `json:"shm"`
	Epoch         int64  `json:"now"`
	Proxy         bool   `json:"pxy"`
	Err           string `json:"err"`
	Signature     string `json:"sig"`
	SignatureType string `json:"typ"`
}

type MessageIdentifier struct {
	Retqueue string `json:"retqueue"`
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
	data, err := challenge(m)
	if err != nil {
		return err
	}

	sig, err := s.Sign(data)
	if err != nil {
		return err
	}
	m.Signature = hex.EncodeToString(sig)
	m.SignatureType = s.Type()
	return nil
}

func (m *Message) Verify(publicKey []byte) error {
	if m.Signature == "" {
		return errors.New("signature field is empty, visit the project github repo for instructions to update")
	}

	data, err := challenge(m)
	if err != nil {
		return err
	}
	verifier, err := ConstructVerifier(publicKey, m.SignatureType)
	if err != nil {
		return err
	}
	decoded, err := hex.DecodeString(m.Signature)
	if err != nil {
		return errors.Wrap(err, "couldn't decode signature")
	}
	if !verifier.Verify(data, decoded) {
		return fmt.Errorf("couldn't verify signature")
	}
	return nil
}
