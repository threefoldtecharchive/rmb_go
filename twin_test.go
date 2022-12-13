package rmb

import (
	"fmt"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/threefoldtech/substrate-client"
)

func TestResolveTwinIP(t *testing.T) {
	mgr := substrate.NewManager("wss://tfchain.dev.grid.tf/ws")
	sub, err := mgr.Substrate()
	if err != nil {
		t.Errorf("error opening substrate connection: %v", err)
	}
	defer sub.Close()
	resolver, err := NewSubstrateResolver(sub)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	r, err := resolver.Resolve(1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if r.(*twinClient).dstIP != "::11" {
		t.Errorf("expected ::11 found %s", r.(*twinClient).dstIP)
	}
}

func TestResolveFail(t *testing.T) {
	mgr := substrate.NewManager("wss://tfchain.dev.grid.tf/ws")
	sub, err := mgr.Substrate()
	if err != nil {
		t.Errorf("error opening substrate connection: %v", err)
	}
	defer sub.Close()
	resolver, err := NewSubstrateResolver(sub)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	r, err := resolver.Resolve(9845856)
	log.Debug().Err(err).Str("result", fmt.Sprintf("%v", r)).Msg("after requesting non-existent twin")
	if err == nil {
		t.Errorf("twin shouldn't be found but err is null")
	}
	if r != nil {
		t.Errorf("r should be nil when the twin is not found")
	}
}
