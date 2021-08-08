package rmb

import (
	"fmt"
	"testing"

	"github.com/rs/zerolog/log"
)

func TestResolveTwinIp(t *testing.T) {
	resolver := TwinExplorerResolver{
		substrate: "https://explorer.devnet.grid.tf",
	}
	r, err := resolver.Resolve(1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if r.(*TwinCommunicationChannel).dstIp != "192.168.0.154" {
		t.Errorf("expected 192.168.0.154 found %s", r.(*TwinCommunicationChannel).dstIp)
	}
}

func TestResolveFail(t *testing.T) {
	resolver := TwinExplorerResolver{
		substrate: "https://explorer.devnet.grid.tf",
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
