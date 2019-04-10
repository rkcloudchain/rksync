/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package rksync

import (
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"regexp"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/channel"
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

// Serve creates a rksync service instance
func Serve(l net.Listener, cfg *config.Config) (*Server, error) {
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

	srv := &Server{cfg: cfg}

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

	grpcServer, err := server.NewGRPCServerFromListener(l, srv.cfg.Server)
	if err != nil {
		logging.Errorf("Failed to create grpc server (%s)", err)
		return nil, err
	}

	srv.gossip, err = gossip.NewGossipService(srv.cfg.Gossip, srv.cfg.Identity, grpcServer.Server(), srv.selfIdentity, func() []grpc.DialOption {
		return srv.secureDialOpts(srv.cfg.Server)
	})
	if err != nil {
		return nil, errors.Errorf("Failed creating RKSync service (%s)", err)
	}

	go func() {
		if err := grpcServer.Start(); err != nil {
			logging.Errorf("grpc server exited with error: %s", err)
		} else {
			logging.Info("RKSycn server exited")
		}
	}()

	go srv.initializeChannel()
	return srv, nil
}

// Server encapsulates rksync component
type Server struct {
	gossip        gossip.Gossip
	cfg           *config.Config
	chainFilePath string
	selfIdentity  common.PeerIdentityType
	clientCreds   credentials.TransportCredentials
}

// Stop the rksync service
func (srv *Server) Stop() {
	if srv.gossip != nil {
		srv.gossip.Stop()
	}
}

// CreateChannel creates a channel
func (srv *Server) CreateChannel(chainID string, files []common.FileSyncInfo) error {
	logging.Debugf("Creating channel, ID: %s", chainID)

	if err := validateChannelID(chainID); err != nil {
		return errors.Errorf("Bad channel id: %s", err)
	}

	mac := channel.GenerateMAC(srv.gossip.SelfPKIid(), chainID)
	chainState, err := srv.gossip.CreateChain(mac, chainID, files)
	if err != nil {
		return err
	}

	err = srv.rewriteChainConfigFile(mac, chainState)
	if err != nil {
		srv.gossip.CloseChain(mac, false)
		return err
	}

	return nil
}

// CloseChannel closes an channel
func (srv *Server) CloseChannel(chainID string) error {
	if chainID == "" {
		return errors.New("Channel ID must be provided")
	}

	mac := channel.GenerateMAC(srv.gossip.SelfPKIid(), chainID)
	err := srv.gossip.CloseChain(mac, true)
	if err != nil {
		return err
	}

	dir := filepath.Join(srv.chainFilePath, mac.String())
	return os.RemoveAll(dir)
}

// AddMemberToChan adds a member to the channel
func (srv *Server) AddMemberToChan(chainID string, nodeID string, cert *x509.Certificate) error {
	if chainID == "" {
		return errors.New("Channel ID must be provided")
	}
	if nodeID == "" {
		return errors.New("Node ID must be provided")
	}
	if cert == nil {
		return errors.New("Node certificate must be provided")
	}

	pkiID, err := srv.gossip.GetPKIidOfCert(nodeID, cert)
	if err != nil {
		return err
	}

	mac := channel.GenerateMAC(srv.gossip.SelfPKIid(), chainID)
	chainState, err := srv.gossip.AddMemberToChain(mac, pkiID)
	if err != nil {
		return err
	}

	return srv.rewriteChainConfigFile(mac, chainState)
}

// RemoveMemberWithChan removes member contained in the channel
func (srv *Server) RemoveMemberWithChan(chainID string, nodeID string, cert *x509.Certificate) error {
	if chainID == "" {
		return errors.New("Channel ID must be provided")
	}
	if nodeID == "" {
		return errors.New("Node ID must be provided")
	}
	if cert == nil {
		return errors.New("Node certificate must be provided")
	}

	pkiID, err := srv.gossip.GetPKIidOfCert(nodeID, cert)
	if err != nil {
		return err
	}

	mac := channel.GenerateMAC(srv.gossip.SelfPKIid(), chainID)
	chainState, err := srv.gossip.RemoveMemberWithChain(mac, pkiID)
	if err != nil {
		return err
	}

	return srv.rewriteChainConfigFile(mac, chainState)
}

// AddFileToChan adds a file to the channel
func (srv *Server) AddFileToChan(chainID string, files []*common.FileSyncInfo) error {
	if chainID == "" {
		return errors.New("Channel ID must be provided")
	}
	if len(files) == 0 {
		return errors.New("files can't be nil or empty")
	}

	mac := channel.GenerateMAC(srv.gossip.SelfPKIid(), chainID)
	chainState, err := srv.gossip.AddFileToChain(mac, files)
	if err != nil {
		return err
	}

	return srv.rewriteChainConfigFile(mac, chainState)
}

// RemoveFileWithChan removes file contained in the channel
func (srv *Server) RemoveFileWithChan(chainID string, filenames []string) error {
	if chainID == "" {
		return errors.New("Channel ID must be provided")
	}
	if len(filenames) == 0 {
		return errors.New("files can't be nil or empty")
	}

	mac := channel.GenerateMAC(srv.gossip.SelfPKIid(), chainID)
	chainState, err := srv.gossip.RemoveFileWithChain(mac, filenames)
	if err != nil {
		return err
	}

	return srv.rewriteChainConfigFile(mac, chainState)
}

func (srv *Server) initializeChannel() {
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

		mac, err := hex.DecodeString(dir)
		if err != nil {
			logging.Errorf("Error decoding directory string: %s", err)
			continue
		}

		err = srv.gossip.InitializeChain(common.ChainMac(mac), chainState)
		if err != nil {
			logging.Errorf("Error initializing channel %s: %s", dir, err)
		}
	}
}

func (srv *Server) rewriteChainConfigFile(chainMac common.ChainMac, chainState *protos.ChainState) error {
	dir := filepath.Join(srv.chainFilePath, chainMac.String())
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

func (srv *Server) secureDialOpts(cfg *config.ServerConfig) []grpc.DialOption {
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
