/*
Copyright Rockontrol Corp. All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"crypto/tls"
	"encoding/pem"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/logging"
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
	DefaultHomeDir           = "/var/rksync/production"
)

// Config defines the parameters for rksync
type Config struct {
	HomeDir  string // The service's home directory
	Gossip   *GossipConfig
	Identity *IdentityConfig
	Server   *ServerConfig
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
	ID string // ID of this instance

	keyStoreDir     string
	cert            []byte
	rootCAs         [][]byte
	intermediateCAs [][]byte
}

// GetCertificate returns the certificate file associated with the configuration
func (c *IdentityConfig) GetCertificate() []byte {
	return c.cert
}

// GetKeyStoreDir returns the key store directory
func (c *IdentityConfig) GetKeyStoreDir() string {
	return c.keyStoreDir
}

// GetRootCAs returns the root ca certificates associated with the configuration
func (c *IdentityConfig) GetRootCAs() [][]byte {
	return c.rootCAs
}

// GetIntermediateCAs returns the intermediate ca certificates associated with the configuration
func (c *IdentityConfig) GetIntermediateCAs() [][]byte {
	return c.intermediateCAs
}

// MakeFilesAbs makes files absolute relative to 'HomeDir' if not already absolute
func (c *IdentityConfig) MakeFilesAbs(homedir string) error {
	if homedir == "" {
		return errors.New("HomeDir must be provided")
	}

	c.keyStoreDir = filepath.Join(homedir, "csp", "keystore")

	err := c.setupRootCAs(homedir)
	if err != nil {
		return err
	}

	err = c.setupIntermediateCAs(homedir)
	if err != nil {
		return err
	}

	return c.setupCertificate(homedir)
}

func (c *IdentityConfig) setupCertificate(homedir string) error {
	var err error
	certDir, err := util.MakeFileAbs("csp/signcerts", homedir)
	if err != nil {
		return err
	}

	signcert, err := getPemMaterialFromDir(certDir)
	if err != nil || len(signcert) == 0 {
		return errors.WithMessagef(err, "could not load a valid signer certificate from directory %s", certDir)
	}

	c.cert = signcert[0]
	return nil
}

func (c *IdentityConfig) setupRootCAs(homedir string) error {
	rootCACertsDir, err := util.MakeFileAbs("csp/cacerts", homedir)
	if err != nil {
		return err
	}

	cacerts, err := getPemMaterialFromDir(rootCACertsDir)
	if err != nil || len(cacerts) == 0 {
		return errors.WithMessagef(err, "could not load a valid ca certificate from directory %s", rootCACertsDir)
	}

	c.rootCAs = cacerts
	return nil
}

func (c *IdentityConfig) setupIntermediateCAs(homedir string) error {
	caCertsDir, err := util.MakeFileAbs("csp/intermediatecerts", homedir)
	if err != nil {
		return err
	}

	intermediatecerts, err := getPemMaterialFromDir(caCertsDir)
	if os.IsNotExist(err) {
		logging.Debugf("Intermediate certs folder not found at [%s]. Skipping. [%s]", caCertsDir, err)
	} else if err != nil {
		return errors.WithMessagef(err, "failed loading intermediate ca certs at [%s]", caCertsDir)
	}

	c.intermediateCAs = intermediatecerts
	return nil
}

func getPemMaterialFromDir(dir string) ([][]byte, error) {
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return nil, err
	}

	content := make([][]byte, 0)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read directory %s", dir)
	}

	for _, f := range files {
		fullName := filepath.Join(dir, f.Name())
		f, err := os.Stat(fullName)
		if err != nil {
			logging.Warningf("Failed to stat %s: %s", fullName, err)
			continue
		}
		if f.IsDir() {
			continue
		}

		item, err := readPemFile(fullName)
		if err != nil {
			logging.Warningf("Failed reading file %s: %s", fullName, err)
			continue
		}

		content = append(content, item)
	}

	return content, nil
}

func readPemFile(file string) ([]byte, error) {
	fileData, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read file %s", file)
	}

	b, _ := pem.Decode(fileData)
	if b == nil {
		return nil, errors.Errorf("No pem data for file %s", file)
	}

	return fileData, nil
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

// FileMeta contains file metadata
type FileMeta struct {
	Name     string
	Metadata []byte
	Leader   bool
}

// FileSystem enables the rksync to communicate with file system.
type FileSystem interface {
	// Create creates the named file
	Create(chainID string, fmeta FileMeta) (File, error)

	// OpenFile opens a file using the given flags and the given mode.
	OpenFile(chainID string, fmeta FileMeta, flag int, perm os.FileMode) (File, error)

	// Stat returns a FileInfo describing the named file.
	Stat(chainID string, fmeta FileMeta) (os.FileInfo, error)

	// Chtimes changes the modification times of the file
	Chtimes(chainID string, fmeta FileMeta, mtime time.Time) error
}

// File represents a file in the filesystem
type File interface {
	io.Closer
	io.ReaderAt
	io.Writer
	io.Reader
	io.Seeker
}
