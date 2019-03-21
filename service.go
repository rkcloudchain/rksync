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
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"syscall"

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
	rkSyncSvc           gossip.Gossip
	clientCreds         credentials.TransportCredentials
	fileSystemPath      string
	channelAllowedChars = "[a-z][a-z0-9.-]*"
	maxLength           = 249
)

// InitRKSyncService initialize rksync service
func InitRKSyncService(cfg config.Config) error {
	if cfg.FileSystemPath == "" {
		cfg.FileSystemPath = config.DefaultFileSystemPath
	}
	if s, err := os.Stat(cfg.FileSystemPath); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(cfg.FileSystemPath, 0755); err != nil {
				return errors.Errorf("Could not create rksync store data path: %s", err)
			}
		} else {
			return errors.Errorf("Could not stat rksync store data path: %s", err)
		}
	} else if !s.IsDir() {
		return errors.Errorf("RKSync store data path exists but not a dir: %s", cfg.FileSystemPath)
	}
	fileSystemPath = cfg.FileSystemPath

	listenAddr := getListenAddress(&cfg)
	grpcServer, err := server.NewGRPCServer(listenAddr, cfg.Server)
	if err != nil {
		logging.Errorf("Failed to create grpc server (%s)", err)
		return err
	}

	if cfg.Server.SecOpts.UseTLS {
		clientCreds, err = clientTransportCredentials(&cfg)
		if err != nil {
			return errors.Errorf("Failed to set TLS client certificate (%s)", err)
		}
	}

	serializedIdentity, err := serializeIdentity(cfg.Identity)
	if err != nil {
		return errors.Errorf("Failed serializing self identity: %v", err)
	}

	err = validateGossipConfig(&cfg)
	if err != nil {
		return err
	}
	rkSyncSvc, err = gossip.NewGossipService(cfg.Gossip, cfg.Identity, grpcServer.Server(), serializedIdentity, func() []grpc.DialOption {
		return secureDialOpts(&cfg)
	})
	if err != nil {
		return errors.Errorf("Failed creating RKSync service (%s)", err)
	}
	defer rkSyncSvc.Stop()

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

	go initializeChannel()
	go handleSignals(map[os.Signal]func(){
		syscall.SIGINT:  func() { serve <- nil },
		syscall.SIGTERM: func() { serve <- nil },
	})

	return <-serve
}

// CreateChannel creates a channel
func CreateChannel(chainID string, files []common.FileSyncInfo) error {
	logging.Debugf("Creating channel, ID: %s", chainID)
	if rkSyncSvc == nil {
		return errors.New("You need initialize RKSync service first")
	}

	if err := validateChannelID(chainID); err != nil {
		return errors.Errorf("Bad channel id: %s", err)
	}

	chainState, err := rkSyncSvc.CreateChannel(chainID, files)
	if err != nil {
		return err
	}

	err = rewriteChainConfigFile(chainID, chainState)
	if err != nil {
		rkSyncSvc.CloseChannel(chainID)
		return err
	}

	return nil
}

// AddMemberToChan adds a member to the channel
func AddMemberToChan(chainID string, nodeID string, cert *x509.Certificate) error {
	if chainID == "" {
		return errors.New("Channel ID must be provided")
	}
	if nodeID == "" {
		return errors.New("Node ID must be provided")
	}
	if cert == nil {
		return errors.New("Node certificate must be provided")
	}

	pkiID, err := rkSyncSvc.GetPKIidOfCert(nodeID, cert)
	if err != nil {
		return err
	}

	chainState, err := rkSyncSvc.AddMemberToChan(chainID, pkiID)
	if err != nil {
		return err
	}

	return rewriteChainConfigFile(chainID, chainState)
}

// AddFileToChan adds a file to the channel
func AddFileToChan(chainID string, filepath string, filemode string) error {
	if chainID == "" {
		return errors.New("Channel ID must be provided")
	}
	if filepath == "" {
		return errors.New("File path must be provided")
	}
	if filemode == "" {
		return errors.New("File mode must be provided")
	}

	chainState, err := rkSyncSvc.AddFileToChan(chainID, common.FileSyncInfo{Path: filepath, Mode: filemode})
	if err != nil {
		return err
	}

	return rewriteChainConfigFile(chainID, chainState)
}

func initializeChannel() {
	dirs, err := util.ListSubdirs(fileSystemPath)
	if err != nil {
		logging.Error(err.Error())
		return
	}

	for _, dir := range dirs {
		path := filepath.Join(fileSystemPath, dir, "config.pb")
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

		err = rkSyncSvc.InitializeChannel(dir, chainState)
		if err != nil {
			logging.Errorf("Error initializing channel %s: %s", dir, err)
		}
	}
}

func rewriteChainConfigFile(chainID string, chainState *protos.ChainState) error {
	dir := filepath.Join(fileSystemPath, chainID)
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

func secureDialOpts(cfg *config.Config) []grpc.DialOption {
	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(config.MaxRecvMsgSize),
		grpc.MaxCallSendMsgSize(config.MaxSendMsgSize),
	))

	dialOpts = append(dialOpts, config.ClientKeepaliveOptions(cfg.Server.KaOpts)...)
	if cfg.Server.SecOpts.UseTLS {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(clientCreds))
	} else {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	}

	return dialOpts
}

func serializeIdentity(cfg *config.IdentityConfig) (common.PeerIdentityType, error) {
	if cfg.ID == "" {
		return nil, errors.New("Node id must be provided")
	}
	if err := cfg.MakeFilesAbs(); err != nil {
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

func handleSignals(handlers map[os.Signal]func()) {
	var signals []os.Signal
	for sig := range handlers {
		signals = append(signals, sig)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, signals...)

	for sig := range signalChan {
		logging.Infof("Received signal: %d (%s)", sig, sig)
		handlers[sig]()
	}
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
