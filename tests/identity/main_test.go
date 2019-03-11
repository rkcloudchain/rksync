package identity

import (
	"os"
	"testing"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/tests/runner"
	"github.com/rkcloudchain/rksync/tests/util"
)

var idMapper identity.Identity
var selfIdentity common.PeerIdentityType

func TestMain(m *testing.M) {
	certfile := util.GetIdentityPath("signcert.org1.pem")
	keyfile := util.GetIdentityPath("signkey.org1")
	cafile := util.GetIdentityPath("ca.org1.pem")

	cfg := &config.IdentityConfig{
		ID:          "peer0.org1",
		Certificate: certfile,
		Key:         keyfile,
		CAs:         []string{cafile},
	}

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
