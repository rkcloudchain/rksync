/*
Copyright Rockontrol Corp. All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"crypto/tls"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Configuration defaults
var (
	MaxSendMsgSize         = 100 * 1024 * 1024
	MaxRecvMsgSize         = 100 * 1024 * 1024
	DefaultKeepaliveConfig = &KeepaliveConfig{
		ClientInterval:    time.Duration(1) * time.Minute,
		ClientTimeout:     time.Duration(20) * time.Second,
		ServerInterval:    time.Duration(2) * time.Hour,
		ServerTimeout:     time.Duration(20) * time.Second,
		ServerMinInterval: time.Duration(1) * time.Minute,
	}
	// strong TLS cipher suites
	DefaultTLSCipherSuites = []uint16{
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
	}
	// default connection timeout
	DefaultConnectionTimeout = 5 * time.Second
	DefaultFileSystemPath    = "/var/rksync/production"
)

// Config defines the parameters for rksync
type Config struct {
	BindAddress    string // Address we bind to
	BindPort       int    // Port we bind to
	FileSystemPath string // Path on the file system where rksync will store data.
	Gossip         *GossipConfig
	Identity       *IdentityConfig
	Server         *ServerConfig
}

// GossipConfig is the configuration of the rksync component
type GossipConfig struct {
	FileSystem                 FileSystem    // File system
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
	ID      string // ID of this instance
	HomeDir string

	certFile string
	caFiles  []string
}

// GetCertificate returns the certificate file associated with the configuration
func (c *IdentityConfig) GetCertificate() string {
	return c.certFile
}

// GetCACerts returns the ca certificate files associated with the configuration
func (c *IdentityConfig) GetCACerts() []string {
	return c.caFiles
}

// MakeFilesAbs makes files absolute relative to 'HomeDir' if not already absolute
func (c *IdentityConfig) MakeFilesAbs() error {
	if c.HomeDir == "" {
		return errors.New("HomeDir must be provided")
	}
	var err error
	c.certFile, err = util.MakeFileAbs("csp/signcerts/cert.pem", c.HomeDir)
	if err != nil {
		return err
	}
	if _, err := os.Stat(c.certFile); err != nil {
		return err
	}

	caCertsDir, err := util.MakeFileAbs("csp/cacerts", c.HomeDir)
	if err != nil {
		return err
	}
	fi, err := os.Stat(caCertsDir)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return errors.Errorf("%s: is not a directory", caCertsDir)
	}

	files, err := ioutil.ReadDir(caCertsDir)
	if err != nil {
		return err
	}

	c.caFiles = make([]string, 0)
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		cafile, err := util.MakeFileAbs(file.Name(), caCertsDir)
		if err != nil {
			return err
		}
		c.caFiles = append(c.caFiles, cafile)
	}

	return nil
}

// ServerConfig defines the parameters for configuring a GRPCServer instance
type ServerConfig struct {
	ConnectionTimeout time.Duration
	SecOpts           *TLSConfig
	KaOpts            *KeepaliveConfig
}

// KeepaliveConfig is used to set the gRPC keepalive settings for both
// clients and servers
type KeepaliveConfig struct {
	// ClientInterval is the duration after which if the client does not see
	// any activity from the server it pings the server to see if it is alive
	ClientInterval time.Duration
	// ClientTimeout is the duration the client waits for a response
	// from the server after sending a ping before closing the connection
	ClientTimeout time.Duration
	// ServerInterval is the duration after which if the server does not see
	// any activity from the client it pings the client to see if it is alive
	ServerInterval time.Duration
	// ServerTimeout is the duration the server waits for a response
	// from the client after sending a ping before closing the connection
	ServerTimeout time.Duration
	// ServerMinInterval is the minimum permitted time between client pings.
	// If clients send pings more frequently, the server will disconnect them
	ServerMinInterval time.Duration
}

// TLSConfig defines the TLS parameters for a gRPC server or gRPC client instance
type TLSConfig struct {
	Certificate       string
	Key               string
	ServerRootCAs     []string
	ClientRootCAs     []string
	UseTLS            bool
	RequireClientCert bool
	CipherSuites      []uint16
}

// ServerKeepaliveOptions returns gRPC keepalive options for server.
func ServerKeepaliveOptions(ka *KeepaliveConfig) []grpc.ServerOption {
	if ka == nil {
		ka = DefaultKeepaliveConfig
	}
	var serverOpts []grpc.ServerOption
	kap := keepalive.ServerParameters{
		Time:    ka.ServerInterval,
		Timeout: ka.ServerTimeout,
	}
	serverOpts = append(serverOpts, grpc.KeepaliveParams(kap))
	kep := keepalive.EnforcementPolicy{
		MinTime:             ka.ServerMinInterval,
		PermitWithoutStream: true,
	}
	serverOpts = append(serverOpts, grpc.KeepaliveEnforcementPolicy(kep))
	return serverOpts
}

// ClientKeepaliveOptions returns gRPC keepalive options for clients.
func ClientKeepaliveOptions(ka *KeepaliveConfig) []grpc.DialOption {
	if ka == nil {
		ka = DefaultKeepaliveConfig
	}

	var dialOpts []grpc.DialOption
	kap := keepalive.ClientParameters{
		Time:                ka.ClientInterval,
		Timeout:             ka.ClientTimeout,
		PermitWithoutStream: true,
	}
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(kap))
	return dialOpts
}

// FileSystem enables the rksync to communicate with file system.
type FileSystem interface {
	// Create creates the named file
	Create(chainID, filename string) (File, error)

	// OpenFile opens a file using the given flags and the given mode.
	OpenFile(chainID, filename string, flag int, perm os.FileMode) (File, error)

	// Stat returns a FileInfo describing the named file.
	Stat(chainID, filename string) (os.FileInfo, error)
}

// File represents a file in the filesystem
type File interface {
	io.Closer
	io.ReaderAt
	io.Writer
}
