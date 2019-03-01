package discovery

import (
	"github.com/rkcloudchain/rksync/common"
)

// Discovery is the interface that represents a discovery module
type Discovery interface {
	Lookup(pkiID common.PKIidType) *common.RemotePeer

	// Stop this instance
	Stop()

	// InitiateSync makes the instance ask a given number of peers
	// for their membership information
	InitiateSync(num int)
}
