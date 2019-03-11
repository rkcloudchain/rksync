package identity

import (
	"os"
	"testing"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/tests/runner"
)

var idMapper identity.Identity
var selfIdentity common.PeerIdentityType

func TestMain(m *testing.M) {
	cfg := runner.GetOrg1IdentityConfig()

	var err error
	selfIdentity, err = runner.GetIdentity(cfg)
	if err != nil {
		panic(err)
	}

	idMapper, err = identity.NewIdentity(cfg, selfIdentity, func(_ common.PKIidType) {})
	if err != nil {
		panic(err)
	}

	gr := m.Run()
	os.Exit(gr)
}
