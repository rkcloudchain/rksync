/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package rksync

import (
	"time"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/config"
)

func validateGossipConfig(cfg *config.GossipConfig) error {
	if len(cfg.BootstrapPeers) == 0 {
		return errors.New("At least one bootstrap peer needs to be provided")
	}
	if cfg.Endpoint == "" {
		return errors.New("Must specify the endpoint address of the peer")
	}
	if cfg.FileSystem == nil {
		return errors.New("Must specify the FileSystem interface")
	}
	if cfg.PropagateIterations == 0 {
		cfg.PropagateIterations = 1
	}
	if cfg.PropagatePeerNum == 0 {
		cfg.PropagatePeerNum = 3
	}
	if cfg.MaxPropagationBurstSize == 0 {
		cfg.MaxPropagationBurstSize = 10
	}
	if cfg.MaxPropagationBurstLatency == time.Duration(0) {
		cfg.MaxPropagationBurstLatency = 10 * time.Millisecond
	}
	if cfg.PullInterval == time.Duration(0) {
		cfg.PullInterval = 4 * time.Second
	}
	if cfg.PullPeerNum == 0 {
		cfg.PullPeerNum = 3
	}
	if cfg.PublishCertPeriod == time.Duration(0) {
		cfg.PublishCertPeriod = 20 * time.Second
	}
	if cfg.PublishStateInfoInterval == time.Duration(0) {
		cfg.PublishStateInfoInterval = 4 * time.Second
	}
	if cfg.RequestStateInfoInterval == time.Duration(0) {
		cfg.RequestStateInfoInterval = 4 * time.Second
	}

	return nil
}
