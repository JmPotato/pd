// Copyright 2019 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

// StoreCreateOption is used to create store.
type StoreCreateOption func(store *StoreInfo)

// SetStoreAddress sets the address for the store.
func SetStoreAddress(address, statusAddress, peerAddress string) StoreCreateOption {
	return func(store *StoreInfo) {
		meta := typeutil.DeepClone(store.meta, StoreFactory)
		meta.Address = address
		meta.StatusAddress = statusAddress
		meta.PeerAddress = peerAddress
		store.meta = meta
	}
}

// SetStoreLabels sets the labels for the store.
func SetStoreLabels(labels []*metapb.StoreLabel) StoreCreateOption {
	return func(store *StoreInfo) {
		meta := typeutil.DeepClone(store.meta, StoreFactory)
		meta.Labels = labels
		store.meta = meta
	}
}

// SetStoreStartTime sets the start timestamp for the store.
func SetStoreStartTime(startTS int64) StoreCreateOption {
	return func(store *StoreInfo) {
		meta := typeutil.DeepClone(store.meta, StoreFactory)
		meta.StartTimestamp = startTS
		store.meta = meta
	}
}

// SetStoreVersion sets the version for the store.
func SetStoreVersion(githash, version string) StoreCreateOption {
	return func(store *StoreInfo) {
		meta := typeutil.DeepClone(store.meta, StoreFactory)
		meta.Version = version
		meta.GitHash = githash
		store.meta = meta
	}
}

// SetStoreDeployPath sets the deploy path for the store.
func SetStoreDeployPath(deployPath string) StoreCreateOption {
	return func(store *StoreInfo) {
		meta := typeutil.DeepClone(store.meta, StoreFactory)
		meta.DeployPath = deployPath
		store.meta = meta
	}
}

// SetStoreState sets the state for the store.
func SetStoreState(state metapb.StoreState, physicallyDestroyed ...bool) StoreCreateOption {
	return func(store *StoreInfo) {
		meta := typeutil.DeepClone(store.meta, StoreFactory)
		switch state {
		case metapb.StoreState_Up:
			meta.State = metapb.StoreState_Up
			meta.NodeState = metapb.NodeState_Serving
		case metapb.StoreState_Offline:
			if len(physicallyDestroyed) != 0 {
				meta.State = metapb.StoreState_Offline
				meta.NodeState = metapb.NodeState_Removing
				meta.PhysicallyDestroyed = physicallyDestroyed[0]
			} else {
				panic("physicallyDestroyed should be set when set store state to offline")
			}
		case metapb.StoreState_Tombstone:
			meta.State = metapb.StoreState_Tombstone
			meta.NodeState = metapb.NodeState_Removed
		}
		store.meta = meta
	}
}

// SetNodeState sets the node state for the store.
// Only used for testing.
func SetNodeState(nodeState metapb.NodeState) StoreCreateOption {
	return func(store *StoreInfo) {
		meta := typeutil.DeepClone(store.meta, StoreFactory)
		meta.NodeState = nodeState
		store.meta = meta
	}
}

// PauseLeaderTransfer prevents the store from been selected as source or target store of TransferLeader.
func PauseLeaderTransfer(d constant.Direction) StoreCreateOption {
	return func(store *StoreInfo) {
		switch d {
		case constant.In:
			store.pauseLeaderTransferIn = true
		case constant.Out:
			store.pauseLeaderTransferOut = true
		}
	}
}

// ResumeLeaderTransfer cleans a store's pause state. The store can be selected as source or target of TransferLeader again.
func ResumeLeaderTransfer(d constant.Direction) StoreCreateOption {
	return func(store *StoreInfo) {
		switch d {
		case constant.In:
			store.pauseLeaderTransferIn = false
		case constant.Out:
			store.pauseLeaderTransferOut = false
		}
	}
}

// SlowStoreEvicted marks a store as a slow store and prevents transferring
// leader to the store
func SlowStoreEvicted() StoreCreateOption {
	return func(store *StoreInfo) {
		store.slowStoreEvicted = true
	}
}

// SlowTrendEvicted marks a store as a slow store by trend and prevents transferring
// leader to the store
func SlowTrendEvicted() StoreCreateOption {
	return func(store *StoreInfo) {
		store.slowTrendEvicted = true
	}
}

// SlowTrendRecovered cleans the evicted by slow trend state of a store.
func SlowTrendRecovered() StoreCreateOption {
	return func(store *StoreInfo) {
		store.slowTrendEvicted = false
	}
}

// SlowStoreRecovered cleans the evicted state of a store.
func SlowStoreRecovered() StoreCreateOption {
	return func(store *StoreInfo) {
		store.slowStoreEvicted = false
	}
}

// SetLeaderCount sets the leader count for the store.
func SetLeaderCount(leaderCount int) StoreCreateOption {
	return func(store *StoreInfo) {
		store.leaderCount = leaderCount
	}
}

// SetRegionCount sets the Region count for the store.
func SetRegionCount(regionCount int) StoreCreateOption {
	return func(store *StoreInfo) {
		store.regionCount = regionCount
	}
}

// SetLearnerCount sets the learner count for the store.
func SetLearnerCount(learnerCount int) StoreCreateOption {
	return func(store *StoreInfo) {
		store.learnerCount = learnerCount
	}
}

// SetWitnessCount sets the witness count for the store.
func SetWitnessCount(witnessCount int) StoreCreateOption {
	return func(store *StoreInfo) {
		store.witnessCount = witnessCount
	}
}

// SetPendingPeerCount sets the pending peer count for the store.
func SetPendingPeerCount(pendingPeerCount int) StoreCreateOption {
	return func(store *StoreInfo) {
		store.pendingPeerCount = pendingPeerCount
	}
}

// SetLeaderSize sets the leader size for the store.
func SetLeaderSize(leaderSize int64) StoreCreateOption {
	return func(store *StoreInfo) {
		store.leaderSize = leaderSize
	}
}

// SetRegionSize sets the Region size for the store.
func SetRegionSize(regionSize int64) StoreCreateOption {
	return func(store *StoreInfo) {
		store.regionSize = regionSize
	}
}

// SetLeaderWeight sets the leader weight for the store.
func SetLeaderWeight(leaderWeight float64) StoreCreateOption {
	return func(store *StoreInfo) {
		store.leaderWeight = leaderWeight
	}
}

// SetRegionWeight sets the Region weight for the store.
func SetRegionWeight(regionWeight float64) StoreCreateOption {
	return func(store *StoreInfo) {
		store.regionWeight = regionWeight
	}
}

// SetLastHeartbeatTS sets the time of last heartbeat for the store.
func SetLastHeartbeatTS(lastHeartbeatTS time.Time) StoreCreateOption {
	return func(store *StoreInfo) {
		store.meta.LastHeartbeat = lastHeartbeatTS.UnixNano()
	}
}

// SetLastPersistTime updates the time of last persistent.
func SetLastPersistTime(lastPersist time.Time) StoreCreateOption {
	return func(store *StoreInfo) {
		store.lastPersistTime = lastPersist
	}
}

// SetStoreStats sets the statistics information for the store.
func SetStoreStats(stats *pdpb.StoreStats) StoreCreateOption {
	return func(store *StoreInfo) {
		store.updateRawStats(stats)
	}
}

// SetNewStoreStats sets the raw statistics information for the store.
func SetNewStoreStats(stats *pdpb.StoreStats) StoreCreateOption {
	return func(store *StoreInfo) {
		// There is no clone in default store stats, we create new one to avoid to modify others.
		// And range cluster cannot use HMA because the last value is not cached
		store.storeStats = &storeStats{
			rawStats: stats,
		}
	}
}

// SetMinResolvedTS sets min resolved ts for the store.
func SetMinResolvedTS(minResolvedTS uint64) StoreCreateOption {
	return func(store *StoreInfo) {
		store.minResolvedTS = minResolvedTS
	}
}

// ResetStoreLimit resets the store limit for a store.
func ResetStoreLimit(limitType storelimit.Type, ratePerSec ...float64) StoreCreateOption {
	return func(store *StoreInfo) {
		store.mu.Lock()
		defer store.mu.Unlock()
		rate := float64(0)
		if len(ratePerSec) > 0 {
			rate = ratePerSec[0]
		}
		store.limiter.Reset(rate, limitType)
	}
}

// SetStoreLimit set the store for a store, it may switch the store limit mode.
func SetStoreLimit(limit storelimit.StoreLimit) StoreCreateOption {
	return func(store *StoreInfo) {
		store.mu.Lock()
		defer store.mu.Unlock()
		store.limiter = limit
	}
}

// SetLastAwakenTime sets last awaken time for the store.
func SetLastAwakenTime(lastAwaken time.Time) StoreCreateOption {
	return func(store *StoreInfo) {
		store.lastAwakenTime = lastAwaken
	}
}

// SetStoreMeta sets the meta for the store.
// NOTICE: LastHeartbeat is not persisted each time, so it is not set by this function. Please use SetLastHeartbeatTS instead.
func SetStoreMeta(newMeta *metapb.Store) StoreCreateOption {
	return func(store *StoreInfo) {
		meta := typeutil.DeepClone(store.meta, StoreFactory)
		meta.Version = newMeta.GetVersion()
		meta.GitHash = newMeta.GetGitHash()
		meta.Address = newMeta.GetAddress()
		meta.StatusAddress = newMeta.GetStatusAddress()
		meta.PeerAddress = newMeta.GetPeerAddress()
		meta.StartTimestamp = newMeta.GetStartTimestamp()
		meta.DeployPath = newMeta.GetDeployPath()
		meta.State = newMeta.GetState()
		meta.Labels = newMeta.GetLabels()
		meta.NodeState = newMeta.GetNodeState()
		meta.PhysicallyDestroyed = newMeta.GetPhysicallyDestroyed()
		store.meta = meta
	}
}
