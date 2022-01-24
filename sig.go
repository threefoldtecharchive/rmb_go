package rmb

import (
	"crypto/ed25519"
	"crypto/md5"
	"encoding/hex"
	"fmt"

	sr25519 "github.com/ChainSafe/go-schnorrkel"
	"github.com/threefoldtech/substrate-client"

	"github.com/gtank/merlin"
	"github.com/rs/zerolog/log"
)

const (
	SignatureTypeEd25519 = "ed25519"
	SignatureTypeSr25519 = "sr25519"
)

type Verifier interface {
	Verify(msg []byte, sig []byte) bool
}

type Ed25519VerifyingKey []byte
type Sr25519VerifyingKey []byte

func (k Ed25519VerifyingKey) Verify(msg []byte, sig []byte) bool {
	return ed25519.Verify([]byte(k), msg, sig)
}

func signingContext(msg []byte) *merlin.Transcript {
	return sr25519.NewSigningContext([]byte("substrate"), msg)
}

func (k Sr25519VerifyingKey) verify(pub sr25519.PublicKey, msg []byte, signature []byte) bool {
	var sigs [64]byte
	copy(sigs[:], signature)
	sig := new(sr25519.Signature)
	if err := sig.Decode(sigs); err != nil {
		return false
	}
	return pub.Verify(sig, signingContext(msg))
}

func (k Sr25519VerifyingKey) pubKey() (*sr25519.PublicKey, error) {
	var pubBytes [32]byte
	copy(pubBytes[:], k)
	pk := new(sr25519.PublicKey)

	if err := pk.Decode(pubBytes); err != nil {
		return nil, err
	}
	return pk, nil
}

func (k Sr25519VerifyingKey) Verify(msg []byte, sig []byte) bool {
	pk, err := k.pubKey()
	if err != nil {
		log.Error().Str("pk", hex.EncodeToString(k)).Err(err).Msg("failed to get sr25519 key from bytes returned from substrate")
		return false
	}
	return k.verify(*pk, msg, sig)
}

func challenge(m *Message) ([]byte, error) {
	hash := md5.New()
	if _, err := fmt.Fprintf(hash, "%s", m.Command); err != nil {
		return nil, err
	}

	if _, err := fmt.Fprintf(hash, "%d", m.Data); err != nil {
		return nil, err
	}

	return hash.Sum(nil), nil
}

func ConstructSigner(mnemonics string, key_type string) (substrate.Identity, error) {
	if key_type == "ed25519" {
		return substrate.NewIdentityFromEd25519Phrase(mnemonics)
	} else if key_type == "sr25519" {
		return substrate.NewIdentityFromSr25519Phrase(mnemonics)
	} else {
		return nil, fmt.Errorf("unrecognized key type %s", key_type)
	}
}
func ConstructVerifier(publicKey []byte, key_type string) (Verifier, error) {
	if key_type == SignatureTypeEd25519 {
		return Ed25519VerifyingKey(publicKey), nil
	} else if key_type == SignatureTypeSr25519 {
		return Sr25519VerifyingKey(publicKey), nil
	} else {
		return nil, fmt.Errorf("unrecognized key type %s", key_type)
	}
}