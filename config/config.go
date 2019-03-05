package config

import (
	"crypto/tls"
	"time"
)

// Configuration defaults
var (
	// strong TLS cipher suites
	DefaultTLSCipherSuites = []uint16{
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
	}
)

// GossipConfig is the configuration of the rksync component
type GossipConfig struct {
	BindPort                   int           // Port we bind to
	ID                         string        // ID of this instance
	BootstrapPeers             []string      // Peers we connect to at startup
	PropagateIterations        int           // Number of times a message is pushed to remote peer
	PropagatePeerNum           int           // Number of peers selected to push message to
	Endpoint                   string        // Peer endpoint
	MaxPropagationBurstSize    int           // Max number of messages stored until it triggers a push to remote peers
	MaxPropagationBurstLatency time.Duration // Max time between consecutive message pushes
	PullInterval               time.Duration // Determines frequency of pull phases
	PullPeerNum                int           // Number of peers to pull from
	PublishCertPeriod          time.Duration // Time from startup certifiates are included in Alive messages
	PublishStateInfoInterval   time.Duration // Determines frequency of pushing state info messages to peers
	RequestStateInfoInterval   time.Duration // Determines frequency of pulling state info message from peers
}

// IdentityConfig defines the identity parameters for peer
type IdentityConfig struct {
	Certificate string
	Key         string
	CAs         []string
}

// TLSConfig defines the TLS parameters for a gRPC server or gRPC client instance
type TLSConfig struct {
	Certificate       string
	Key               string
	ServerRootCAs     []string
	ClientRootCAs     []string
	UseTLS            bool
	RequireClientCert bool
}

// ChannelConfig is a configuration item of the channel
type ChannelConfig struct {
	ID                          string
	PublishStateInfoInterval    time.Duration
	PullPeerNum                 int
	PullInterval                time.Duration
	RequestStateInfoInterval    time.Duration
	StateInfoCacheSweepInterval time.Duration
}
