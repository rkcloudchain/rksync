/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package util

import (
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/util"
)

// DefaultGossipConfig returns a default gossip configuration
func DefaultGossipConfig(bootstrap []string, endpoint string) *config.GossipConfig {
	return &config.GossipConfig{
		BootstrapPeers:             bootstrap,
		Endpoint:                   endpoint,
		PropagateIterations:        1,
		PropagatePeerNum:           3,
		MaxPropagationBurstSize:    10,
		MaxPropagationBurstLatency: 10 * time.Millisecond,
		PullInterval:               4 * time.Second,
		PullPeerNum:                3,
		PublishCertPeriod:          20 * time.Second,
		PublishStateInfoInterval:   4 * time.Second,
		RequestStateInfoInterval:   4 * time.Second,
	}
}

// GetIdentity gets peer identity
func GetIdentity(cfg *config.IdentityConfig) (common.PeerIdentityType, error) {
	cert, err := util.GetX509CertificateFromPEM(cfg.GetCertificate())
	if err != nil {
		return nil, err
	}

	block := &pem.Block{Bytes: cert.Raw, Type: "CERTIFICATE"}
	idBytes := pem.EncodeToMemory(block)
	if idBytes == nil {
		return nil, errors.New("Encoding of identity failed")
	}

	sid := &protos.SerializedIdentity{NodeId: cfg.ID, IdBytes: idBytes}
	selfIdentity, err := proto.Marshal(sid)
	if err != nil {
		return nil, err
	}

	return selfIdentity, nil
}

// GetTLSPath returns the path to the tls files
func GetTLSPath(filename string) string {
	wd, _ := os.Getwd()
	for !strings.HasSuffix(wd, "rksync") {
		wd = filepath.Dir(wd)
	}
	return filepath.Join(wd, "tests", "fixtures", "tls", filename)
}
