package rksync

import (
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/golang/protobuf/proto"
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
	rkSyncSvc   gossip.Gossip
	clientCreds credentials.TransportCredentials
)

// InitRKSyncService initialize rksync service
func InitRKSyncService(cfg *config.Config) error {
	listenAddr := getListenAddress(cfg)
	grpcServer, err := server.NewGRPCServer(listenAddr, *cfg.Server)
	if err != nil {
		logging.Errorf("Failed to create grpc server (%s)", err)
		return err
	}

	if cfg.Server.SecOpts.UseTLS {
		clientCreds, err = clientTransportCredentials(cfg)
		if err != nil {
			logging.Fatalf("Failed to set TLS client certificate (%s)", err)
		}
	}

	serializedIdentity, err := serializeIdentity(cfg)
	if err != nil {
		logging.Fatalf("Failed serializing self identity: %v", err)
	}

	rkSyncSvc, err = gossip.NewGossipService(cfg.Gossip, cfg.Identity, grpcServer.Server(), serializedIdentity, func() []grpc.DialOption {
		return secureDialOpts(cfg)
	})
	if err != nil {
		logging.Fatalf("Failed creating RKSync service (%s)", err)
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

	go handleSignals(map[os.Signal]func(){
		syscall.SIGINT:  func() { serve <- nil },
		syscall.SIGTERM: func() { serve <- nil },
	})

	return <-serve
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

func serializeIdentity(cfg *config.Config) (common.PeerIdentityType, error) {
	if cfg.Gossip.ID == "" {
		return nil, errors.New("Node id must be provided")
	}
	if cfg.Identity.Certificate == "" || cfg.Identity.Key == "" {
		return nil, errors.New("Identity config must contain both Key and Certificate")
	}

	certPEM, err := ioutil.ReadFile(cfg.Identity.Certificate)
	if err != nil {
		return nil, err
	}

	_, err = util.GetX509CertificateFromPEM(certPEM)
	if err != nil {
		return nil, err
	}

	sID := &protos.SerializedIdentity{NodeId: cfg.Gossip.ID, IdBytes: certPEM}
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
