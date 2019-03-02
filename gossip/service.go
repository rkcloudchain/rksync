package gossip

import (
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/rpc"
	"google.golang.org/grpc"
)

// NewGossipService creates a gossip instance attached to a gRPC server
func NewGossipService(gConf *config.GossipConfig, idConf *config.IdentityConfig, s *grpc.Server,
	selfIdentity common.PeerIdentityType, secureDialOpts func() []grpc.DialOption) (Gossip, error) {

	g := &gossipService{
		selfIdentity: selfIdentity,
		conf:         gConf,
	}

	var err error
	g.idMapper, err = identity.NewIdentity(idConf, selfIdentity)
	if err != nil {
		return nil, err
	}

	g.srv = rpc.NewServer(s, g.idMapper, selfIdentity, secureDialOpts)
	g.emitter = newBatchingEmitter(gConf.PropagateIterations, gConf.MaxPropagationBurstSize,
		gConf.MaxPropagationBurstLatency, g.sendGossipBatch)

	return nil, nil
}

type gossipService struct {
	selfIdentity common.PeerIdentityType
	idMapper     identity.Identity
	srv          *rpc.Server
	conf         *config.GossipConfig
	emitter      batchingEmitter
}

func (g *gossipService) Gossip(msg *protos.RKSyncMessage) {
	// TODO: msg tag legal

	signMsg := &protos.SignedRKSyncMessage{
		RKSyncMessage: msg,
	}

	signer := func(msg []byte) ([]byte, error) {
		return g.idMapper.Sign(msg)
	}

	_, err := signMsg.Sign(signer)
	if err != nil {
		logging.Warningf("Failed signing message: %v", err)
		return
	}

	if g.conf.PropagateIterations == 0 {
		return
	}
	g.emitter.Add(&emittedRKSyncMessage{
		SignedRKSyncMessage: signMsg,
		filter: func(_ common.PKIidType) bool {
			return true
		},
	})
}

func (g *gossipService) sendGossipBatch(a []interface{}) {

}

func (g *gossipService) gossipBatch(msgs []*emittedRKSyncMessage) {

}
