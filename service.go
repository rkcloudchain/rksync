/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package rksync

import (
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/gossip"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/server"
	"github.com/rkcloudchain/rksync/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	channelAllowedChars = "[a-z][a-z0-9.-]*"
	maxLength           = 249
)

// New creates a rksync service instance
func New(cfg *config.Config) (*SyncService, error) {
	if cfg.HomeDir == "" {
		cfg.HomeDir = config.DefaultHomeDir
	}
	if cfg.Gossip == nil {
		return nil, errors.New("Gossip configuration cannot be nil")
	}
	if cfg.Identity == nil {
		return nil, errors.New("Identity configuration cannot be nil")
	}
	err := validateGossipConfig(cfg.Gossip)
	if err != nil {
		return nil, err
	}
	srv := &SyncService{cfg: cfg}

	srv.chainFilePath = filepath.Join(srv.cfg.HomeDir, "channels")
	if s, err := os.Stat(srv.chainFilePath); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(srv.chainFilePath, 0755); err != nil {
				return nil, errors.Errorf("Could not create chain file path: %s", err)
			}
		} else {
			return nil, errors.Errorf("Could not stat chain file path: %s", err)
		}
	} else if !s.IsDir() {
		return nil, errors.Errorf("RKSync chain file path exists but not a dir: %s", srv.chainFilePath)
	}

	srv.listenAddr = getListenAddress(srv.cfg)
	if srv.cfg.Server == nil {
		srv.cfg.Server = &config.ServerConfig{
			SecOpts: &config.TLSConfig{},
			KaOpts:  &config.KeepaliveConfig{},
		}
	}

	if cfg.Server.SecOpts.UseTLS {
		srv.clientCreds, err = clientTransportCredentials(srv.cfg)
		if err != nil {
			return nil, errors.Errorf("Failed to set TLS client certificate (%s)", err)
		}
	}

	srv.selfIdentity, err = serializeIdentity(cfg.Identity, cfg.HomeDir)
	if err != nil {
		return nil, errors.Errorf("Failed serializing self identity: %v", err)
	}

	return srv, nil
}

// SyncService encapsulates rksync component
type SyncService struct {
	gossip.Gossip
	cfg           *config.Config
	listenAddr    string
	chainFilePath string
	selfIdentity  common.PeerIdentityType
	clientCreds   credentials.TransportCredentials
}

// Start starts rksync service
func (srv *SyncService) Start() error {
	grpcServer, err := server.NewGRPCServer(srv.listenAddr, srv.cfg.Server)
	if err != nil {
		logging.Errorf("Failed to create grpc server (%s)", err)
		return err
	}

	srv.Gossip, err = gossip.NewGossipService(srv.cfg.Gossip, srv.cfg.Identity, grpcServer.Server(), srv.selfIdentity, func() []grpc.DialOption {
		return srv.secureDialOpts(srv.cfg.Server)
	})
	if err != nil {
		return errors.Errorf("Failed creating RKSync service (%s)", err)
	}

	serve := make(chan error)
	go func() {
		var grpcErr error
		if grpcErr = grpcServer.Start(); grpcErr != nil {
			grpcErr = errors.Errorf("grpc server exited with error: %s", grpcErr)
		} else {
			logging.Info("RKSycn server exited")
		}

		serve <- grpcErr
	}()

	go srv.initializeChannel()
	return <-serve
}

// Stop the rksync service
func (srv *SyncService) Stop() {
	if srv.Gossip != nil {
		srv.Stop()
	}
}

// CreateChannel creates a channel
func (srv *SyncService) CreateChannel(chainID string, files []common.FileSyncInfo) error {
	logging.Debugf("Creating channel, ID: %s", chainID)

	if err := validateChannelID(chainID); err != nil {
		return errors.Errorf("Bad channel id: %s", err)
	}

	chainState, err := srv.Gossip.CreateChannel(chainID, files)
	if err != nil {
		return err
	}

	err = srv.rewriteChainConfigFile(chainID, chainState)
	if err != nil {
		srv.CloseChannel(chainID)
		return err
	}

	return nil
}

// AddMemberToChan adds a member to the channel
func (srv *SyncService) AddMemberToChan(chainID string, nodeID string, cert *x509.Certificate) error {
	if chainID == "" {
		return errors.New("Channel ID must be provided")
	}
	if nodeID == "" {
		return errors.New("Node ID must be provided")
	}
	if cert == nil {
		return errors.New("Node certificate must be provided")
	}

	pkiID, err := srv.GetPKIidOfCert(nodeID, cert)
	if err != nil {
		return err
	}

	chainState, err := srv.Gossip.AddMemberToChan(chainID, pkiID)
	if err != nil {
		return err
	}

	return srv.rewriteChainConfigFile(chainID, chainState)
}

// AddFileToChan adds a file to the channel
func (srv *SyncService) AddFileToChan(chainID string, filepath string, filemode string) error {
	if chainID == "" {
		return errors.New("Channel ID must be provided")
	}
	if filepath == "" {
		return errors.New("File path must be provided")
	}
	if filemode == "" {
		return errors.New("File mode must be provided")
	}

	chainState, err := srv.Gossip.AddFileToChan(chainID, common.FileSyncInfo{Path: filepath, Mode: filemode})
	if err != nil {
		return err
	}

	return srv.rewriteChainConfigFile(chainID, chainState)
}

func (srv *SyncService) initializeChannel() {
	dirs, err := util.ListSubdirs(srv.chainFilePath)
	if err != nil {
		logging.Error(err.Error())
		return
	}

	for _, dir := range dirs {
		path := filepath.Join(srv.chainFilePath, dir, "config.pb")
		if _, err := os.Stat(path); err != nil {
			logging.Errorf("Error reading channel %s config file: %s", dir, err)
			continue
		}

		csBytes, err := ioutil.ReadFile(path)
		if err != nil {
			logging.Errorf("Error reading channel %s config file: %s", dir, err)
			continue
		}

		chainState := &protos.ChainState{}
		err = proto.Unmarshal(csBytes, chainState)
		if err != nil {
			logging.Errorf("Error unmarshalling channel %s state message: %s", dir, err)
			continue
		}

		err = srv.InitializeChannel(dir, chainState)
		if err != nil {
			logging.Errorf("Error initializing channel %s: %s", dir, err)
		}
	}
}

func (srv *SyncService) rewriteChainConfigFile(chainID string, chainState *protos.ChainState) error {
	dir := filepath.Join(srv.chainFilePath, chainID)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return errors.Wrapf(err, "Create directory %s failed: %s", dir, err)
		}
	}

	csBytes, err := proto.Marshal(chainState)
	if err != nil {
		return err
	}

	filename := filepath.Join(dir, "config.pb")
	fi, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer fi.Close()

	n, err := fi.Write(csBytes)
	if err != nil || n < 1 {
		return errors.Errorf("Rewrite channel config file failed: %s", err)
	}
	return nil
}

func (srv *SyncService) secureDialOpts(cfg *config.ServerConfig) []grpc.DialOption {
	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(config.MaxRecvMsgSize),
		grpc.MaxCallSendMsgSize(config.MaxSendMsgSize),
	))

	dialOpts = append(dialOpts, config.ClientKeepaliveOptions(cfg.KaOpts)...)
	if cfg.SecOpts.UseTLS {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(srv.clientCreds))
	} else {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	}

	return dialOpts
}

func serializeIdentity(cfg *config.IdentityConfig, homedir string) (common.PeerIdentityType, error) {
	if cfg.ID == "" {
		return nil, errors.New("Node id must be provided")
	}
	if err := cfg.MakeFilesAbs(homedir); err != nil {
		return nil, errors.Wrap(err, "Failed to make identity file absolute")
	}

	certPEM, err := ioutil.ReadFile(cfg.GetCertificate())
	if err != nil {
		return nil, err
	}

	cert, err := util.GetX509CertificateFromPEM(certPEM)
	if err != nil {
		return nil, err
	}

	pb := &pem.Block{Bytes: cert.Raw, Type: "CERTIFICATE"}
	pemBytes := pem.EncodeToMemory(pb)
	if pemBytes == nil {
		return nil, errors.New("Encoding of identity failed")
	}

	sID := &protos.SerializedIdentity{NodeId: cfg.ID, IdBytes: pemBytes}
	idBytes, err := proto.Marshal(sID)
	if err != nil {
		return nil, errors.Wrapf(err, "could not marshal a SerializedIdentity structure for identity %v", sID)
	}

	return idBytes, nil
}

func getListenAddress(cfg *config.Config) string {
	if cfg.BindAddress == "" {
		cfg.BindAddress = "0.0.0.0"
	}
	if cfg.BindPort == 0 {
		cfg.BindPort = 8053
	}

	lisAddr := net.JoinHostPort(cfg.BindAddress, strconv.Itoa(cfg.BindPort))
	return lisAddr
}

func validateChannelID(chainID string) error {
	re, _ := regexp.Compile(channelAllowedChars)
	if len(chainID) <= 0 {
		return errors.New("Channel ID illegal, cannot be empty")
	}
	if len(chainID) > maxLength {
		return errors.Errorf("Channel ID illegal, cannot be longer than %d", maxLength)
	}

	matched := re.FindString(chainID)
	if len(matched) != len(chainID) {
		return errors.Errorf("Channel ID %s contains illegal characters", chainID)
	}

	return nil
}
