package rksync

import (
	"time"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/config"
)

func validateGossipConfig(cfg *config.Config) error {
	if len(cfg.Gossip.BootstrapPeers) == 0 {
		return errors.New("At least one bootstrap peer needs to be provided")
	}
	if cfg.Gossip.Endpoint == "" {
		return errors.New("Must specify the endpoint address of the peer")
	}
	if cfg.Gossip.PropagateIterations == 0 {
		cfg.Gossip.PropagateIterations = 1
	}
	if cfg.Gossip.PropagatePeerNum == 0 {
		cfg.Gossip.PropagatePeerNum = 3
	}
	if cfg.Gossip.MaxPropagationBurstSize == 0 {
		cfg.Gossip.MaxPropagationBurstSize = 10
	}
	if cfg.Gossip.MaxPropagationBurstLatency == time.Duration(0) {
		cfg.Gossip.MaxPropagationBurstLatency = 10 * time.Millisecond
	}
	if cfg.Gossip.PullInterval == time.Duration(0) {
		cfg.Gossip.PullInterval = 4 * time.Second
	}
	if cfg.Gossip.PullPeerNum == 0 {
		cfg.Gossip.PullPeerNum = 3
	}
	if cfg.Gossip.PublishCertPeriod == time.Duration(0) {
		cfg.Gossip.PublishCertPeriod = 20 * time.Second
	}
	if cfg.Gossip.PublishStateInfoInterval == time.Duration(0) {
		cfg.Gossip.PublishStateInfoInterval = 4 * time.Second
	}
	if cfg.Gossip.RequestStateInfoInterval == time.Duration(0) {
		cfg.Gossip.RequestStateInfoInterval = 4 * time.Second
	}

	return nil
}
