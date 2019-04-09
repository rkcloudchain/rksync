/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package filter

import (
	"testing"

	"github.com/rkcloudchain/rksync/common"
	"github.com/stretchr/testify/assert"
)

func TestSelectPolicy(t *testing.T) {
	assert.True(t, SelectAllPolicy(common.NetworkMember{}))
}

func TestCombineRoutingFilters(t *testing.T) {
	member := common.NetworkMember{
		Endpoint: "bar",
	}

	a := func(member common.NetworkMember) bool {
		return member.Endpoint == "bar"
	}

	b := func(member common.NetworkMember) bool {
		return member.Endpoint == "foo"
	}

	assert.True(t, CombineRoutingFilters(a, SelectAllPolicy)(member))
	assert.False(t, CombineRoutingFilters(a, b)(member))
}

func TestSelecdtPeers(t *testing.T) {
	a := common.NetworkMember{Endpoint: "a"}
	b := common.NetworkMember{Endpoint: "b"}
	c := common.NetworkMember{Endpoint: "c"}
	d := common.NetworkMember{Endpoint: "d"}

	peers := []common.NetworkMember{a, b, c, d}

	pa := func(member common.NetworkMember) bool {
		return member.Endpoint == "a"
	}
	pb := func(member common.NetworkMember) bool {
		return member.Endpoint == "b"
	}

	assert.Len(t, SelectPeers(3, peers, SelectAllPolicy), 3)
	assert.Len(t, SelectPeers(3, peers, CombineRoutingFilters(pa, pb)), 0)
	assert.Len(t, SelectPeers(3, peers, CombineRoutingFilters(SelectAllPolicy, pa)), 1)
	assert.Len(t, SelectPeers(3, peers, CombineRoutingFilters(SelectAllPolicy, pb)), 1)
}

func TestSelectAllPeers(t *testing.T) {
	a := common.NetworkMember{Endpoint: "a"}
	b := common.NetworkMember{Endpoint: "b"}
	c := common.NetworkMember{Endpoint: "c"}
	d := common.NetworkMember{Endpoint: "d"}
	e := common.NetworkMember{Endpoint: "e"}
	f := common.NetworkMember{Endpoint: "f"}
	g := common.NetworkMember{Endpoint: "g"}

	peers := []common.NetworkMember{a, b, c, d, e, f, g}
	filter := func(member common.NetworkMember) bool {
		return member.Endpoint == "a" || member.Endpoint == "b" || member.Endpoint == "c" || member.Endpoint == "d"
	}

	assert.Len(t, SelectAllPeers(peers, filter), 4)
}
