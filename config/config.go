package config

import "crypto/tls"

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

// Config is the configuration of the rksync component
type Config struct {
	BindAddress         string   // Address we bind to
	BindPort            int      // Port we bind to
	BootstrapPeers      []string // Peers we connect to at startup
	PropagateIterations int      // Number of times a message is pushed to remote peer
	PropagatePeerNum    int      // Number of peers selected to push message to
	Endpoint            string   // Peer endpoint
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
