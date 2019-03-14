/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package filter

import (
	"math/rand"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/util"
)

// RoutingFilter defines a predicate on a NetworkMember
// It is used to assert whether a given NetworkMember should be
// selected for be given a message
type RoutingFilter func(common.NetworkMember) bool

// SelectAllPolicy selects all members given
var SelectAllPolicy = func(common.NetworkMember) bool {
	return true
}

// CombineRoutingFilters returns the logical AND of given routing filters
func CombineRoutingFilters(filters ...RoutingFilter) RoutingFilter {
	return func(member common.NetworkMember) bool {
		for _, filter := range filters {
			if !filter(member) {
				return false
			}
		}
		return true
	}
}

// SelectPeers returns a slice of peers that match the routing filter
func SelectPeers(k int, peerPool []common.NetworkMember, filter RoutingFilter) []*common.NetworkMember {
	var res []*common.NetworkMember
	rand.Seed(int64(util.RandomUInt64()))

	for _, index := range rand.Perm(len(peerPool)) {
		if len(res) == k {
			break
		}
		peer := peerPool[index]

		if !filter(peer) {
			continue
		}
		p := &common.NetworkMember{PKIID: peer.PKIID, Endpoint: peer.Endpoint}
		res = append(res, p)
	}
	return res
}
