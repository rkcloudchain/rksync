package gossip

import (
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/rpc"
)

type gossipService struct {
	selfIdentity common.PeerIdentityType
	idMapper     identity.Identity
	srv          rpc.Server
	conf         *config.GossipConfig
}
