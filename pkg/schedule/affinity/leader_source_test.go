// Copyright 2025 TiKV Project Authors.
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

package affinity

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
)

type leaderSourceTestEnv struct {
	manager *Manager
	stores  *core.StoresInfo
	store   storage.Storage
	ctx     context.Context
	// restoreDebounce overrides the manager's restore debounce after
	// NewManager. Zero keeps the default (max-store-down-time).
	restoreDebounce time.Duration
}

func newLeaderSourceTestEnv(t *testing.T, storeCount int) *leaderSourceTestEnv {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	memStorage := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := 1; i <= storeCount; i++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{
			Id:        uint64(i),
			Address:   fmt.Sprintf("s%d", i),
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
		})
		storeInfos.PutStore(storeInfo.Clone(core.SetLastHeartbeatTS(time.Now())))
	}
	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, memStorage, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, memStorage, storeInfos, conf, regionLabeler)
	re.NoError(err)
	env := &leaderSourceTestEnv{manager: manager, stores: storeInfos, store: memStorage, ctx: ctx}
	return env
}

// newRestartedManager simulates a PD restart: a fresh manager (and labeler)
// is built over the same storage, so groups are loaded from scratch exactly
// like after a process restart.
func newRestartedManager(t *testing.T, env *leaderSourceTestEnv) *Manager {
	re := require.New(t)
	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(env.ctx, env.store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(env.ctx, env.store, env.stores, conf, regionLabeler)
	re.NoError(err)
	return manager
}

func (e *leaderSourceTestEnv) setStoreHealth(t *testing.T, storeID uint64, healthy bool) {
	re := require.New(t)
	storeInfo := e.stores.GetStore(storeID)
	re.NotNil(storeInfo)
	if healthy {
		e.stores.PutStore(storeInfo.Clone(core.SetLastHeartbeatTS(time.Now())))
	} else {
		// Two hours without heartbeat marks the store as down.
		e.stores.PutStore(storeInfo.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Hour))))
	}
}

// regionOn builds a region whose peers all live on the given stores, with the
// leader on the first store.
func regionOn(id uint64, start, end []byte, storeIDs ...uint64) *core.RegionInfo {
	peers := make([]*metapb.Peer, 0, len(storeIDs))
	for i, storeID := range storeIDs {
		peers = append(peers, &metapb.Peer{Id: id*100 + uint64(i), StoreId: storeID, Role: metapb.PeerRole_Voter})
	}
	return core.NewRegionInfo(
		&metapb.Region{Id: id, StartKey: start, EndKey: end, Peers: peers},
		peers[0],
	)
}

// TestExplicitTargetSurvivesDegradedObservation covers requirement 2 and 4:
// a degraded group (e.g. right after a restart, or with an evicted-leader
// store) must keep its explicit target; first-win observations must not
// overwrite it.
func TestExplicitTargetSurvivesDegradedObservation(t *testing.T) {
	re := require.New(t)
	env := newLeaderSourceTestEnv(t, 3)

	re.NoError(env.manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
	_, err := env.manager.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
	re.NoError(err)

	state := env.manager.GetAffinityGroupState("g")
	re.Equal(LeaderSourceExplicit, state.LeaderSource)
	re.True(state.hasExplicitTarget())
	re.Equal(uint64(1), state.ExplicitLeaderStoreID)
	re.ElementsMatch([]uint64{1, 2, 3}, state.ExplicitVoterStoreIDs)

	// Degrade the group (e.g. the leader store gets evict-leader marked) and
	// throw a differently-placed Region at the checker path.
	env.manager.DegradeAffinityGroup("g")
	env.manager.ObserveAvailableRegion(regionOn(10, []byte("a"), []byte("b"), 3, 1, 2), env.manager.GetAffinityGroupState("g"))

	state = env.manager.GetAffinityGroupState("g")
	re.False(state.AffinitySchedulingAllowed)
	re.Equal(LeaderSourceExplicit, state.LeaderSource)
	re.Equal(uint64(1), state.LeaderStoreID, "explicit leader must not be overwritten while degraded")
	re.ElementsMatch([]uint64{1, 2, 3}, state.VoterStoreIDs, "explicit voters must not be overwritten while degraded")
	re.Equal(uint64(1), state.ExplicitLeaderStoreID)

	// Once the degradation clears, the availability check restores the group
	// and the explicit target is enforced again.
	env.manager.RestoreAffinityGroup("g")
	state = env.manager.GetAffinityGroupState("g")
	re.True(state.AffinitySchedulingAllowed)
	re.Equal(LeaderSourceExplicit, state.LeaderSource)
	re.Equal(uint64(1), state.LeaderStoreID)
}

// TestExplicitTargetFallsBackWhenExpiredAndRestores covers requirement 3: when
// the explicit-target stores go down the group expires and first-win takes
// over, but the explicit target is preserved and restored once the stores
// recover.
func TestExplicitTargetFallsBackWhenExpiredAndRestores(t *testing.T) {
	re := require.New(t)
	env := newLeaderSourceTestEnv(t, 3)
	env.restoreDebounce = 0
	env.manager.restoreDebounce = 0

	re.NoError(env.manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
	_, err := env.manager.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
	re.NoError(err)

	// Store 1 (the explicit leader) goes down hard.
	env.setStoreHealth(t, 1, false)
	env.manager.checkGroupsAvailability()

	state := env.manager.GetAffinityGroupState("g")
	re.False(state.AffinitySchedulingAllowed)

	// Observation takes over as a fallback; the explicit target is kept. The
	// observed Region only has peers on healthy stores — the existing
	// checkHasUnavailableStore guard rejects targets on unavailable stores.
	env.manager.ObserveAvailableRegion(regionOn(10, []byte("a"), []byte("b"), 2, 3), env.manager.GetAffinityGroupState("g"))
	state = env.manager.GetAffinityGroupState("g")
	re.Equal(LeaderSourceFallback, state.LeaderSource)
	re.Equal(uint64(2), state.LeaderStoreID, "effective distribution falls back to the observation")
	re.ElementsMatch([]uint64{2, 3}, state.VoterStoreIDs)
	re.Equal(uint64(1), state.ExplicitLeaderStoreID, "explicit target must be preserved")
	re.ElementsMatch([]uint64{1, 2, 3}, state.ExplicitVoterStoreIDs, "explicit target must be preserved")

	// The explicit-target store recovers: the group returns to the explicit
	// target and becomes available.
	env.setStoreHealth(t, 1, true)
	env.manager.checkGroupsAvailability()

	state = env.manager.GetAffinityGroupState("g")
	re.True(state.AffinitySchedulingAllowed)
	re.Equal(LeaderSourceExplicit, state.LeaderSource)
	re.Equal(uint64(1), state.LeaderStoreID)
	re.ElementsMatch([]uint64{1, 2, 3}, state.VoterStoreIDs)
}

// TestSkipExistCheckKeepsExplicitTarget covers requirement 5: re-registering
// the same key ranges with skip_exist_check must not reset the explicit target.
func TestSkipExistCheckKeepsExplicitTarget(t *testing.T) {
	re := require.New(t)
	env := newLeaderSourceTestEnv(t, 3)

	re.NoError(env.manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
	_, err := env.manager.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
	re.NoError(err)

	// Re-registration with skip_existing is a no-op for existing groups.
	re.NoError(env.manager.CreateAffinityGroupsIfNotExists([]GroupKeyRanges{{GroupID: "g"}}))

	state := env.manager.GetAffinityGroupState("g")
	re.Equal(LeaderSourceExplicit, state.LeaderSource)
	re.Equal(uint64(1), state.LeaderStoreID)
	re.ElementsMatch([]uint64{1, 2, 3}, state.VoterStoreIDs)
	re.Equal(uint64(1), state.ExplicitLeaderStoreID)
	re.ElementsMatch([]uint64{1, 2, 3}, state.ExplicitVoterStoreIDs)
}

// TestGroupStateLeaderSourceValues covers requirement 6: the leader_source
// values reported on GroupState.
func TestGroupStateLeaderSourceValues(t *testing.T) {
	re := require.New(t)
	env := newLeaderSourceTestEnv(t, 2)

	re.NoError(env.manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
	state := env.manager.GetAffinityGroupState("g")
	re.Empty(state.LeaderSource, "a fresh group has no distribution yet")

	// First-win observation marks the fallback source.
	env.manager.ObserveAvailableRegion(regionOn(10, []byte("a"), []byte("b"), 1, 2), env.manager.GetAffinityGroupState("g"))
	state = env.manager.GetAffinityGroupState("g")
	re.Equal(LeaderSourceFallback, state.LeaderSource)

	// An explicit PUT marks the explicit source.
	_, err := env.manager.UpdateAffinityGroupPeers("g", 2, []uint64{1, 2})
	re.NoError(err)
	state = env.manager.GetAffinityGroupState("g")
	re.Equal(LeaderSourceExplicit, state.LeaderSource)
}

// TestLegacyGroupWithoutSourceMigratesToExplicit verifies that groups persisted
// before leader-source tracking are loaded as explicit targets, so an upgrade
// plus restart preserves their distributions.
func TestLegacyGroupWithoutSourceMigratesToExplicit(t *testing.T) {
	re := require.New(t)
	env := newLeaderSourceTestEnv(t, 3)

	// Persist a group in the legacy format: effective distribution without any
	// leader-source marker.
	re.NoError(env.manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "legacy"}}))
	legacy := &Group{ID: "legacy", LeaderStoreID: 2, VoterStoreIDs: []uint64{2, 3}}
	re.NoError(env.manager.storage.RunInTxn(env.manager.ctx, func(txn kv.Txn) error {
		return env.manager.storage.SaveAffinityGroup(txn, legacy.ID, legacy)
	}))

	// Simulate a restart: a fresh manager loads the same storage.
	restarted := newRestartedManager(t, env)

	state := restarted.GetAffinityGroupState("legacy")
	re.NotNil(state)
	re.Equal(LeaderSourceExplicit, state.LeaderSource, "legacy distribution must be preserved as explicit")
	re.Equal(uint64(2), state.LeaderStoreID)
	re.ElementsMatch([]uint64{2, 3}, state.VoterStoreIDs)
	re.Equal(uint64(2), state.ExplicitLeaderStoreID)
	re.ElementsMatch([]uint64{2, 3}, state.ExplicitVoterStoreIDs)

	// A first-win observation after the restart must not overwrite it.
	restarted.ObserveAvailableRegion(regionOn(10, []byte("a"), []byte("b"), 1, 2, 3), restarted.GetAffinityGroupState("legacy"))
	state = restarted.GetAffinityGroupState("legacy")
	re.Equal(uint64(2), state.LeaderStoreID)
	re.Equal(LeaderSourceExplicit, state.LeaderSource)
}

// TestFirstAvailabilityCheckEvaluatesDegradedGroups verifies that the first
// availability pass after initialization re-evaluates groups even when the
// unavailable-store set is empty, so groups loaded as degraded (restart) become
// available without waiting for an unrelated store change.
func TestFirstAvailabilityCheckEvaluatesDegradedGroups(t *testing.T) {
	re := require.New(t)
	env := newLeaderSourceTestEnv(t, 3)

	re.NoError(env.manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
	_, err := env.manager.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
	re.NoError(err)

	// Simulate a restart: fresh manager over the same storage; groups are
	// loaded as degraded even though every store is healthy.
	restarted := newRestartedManager(t, env)
	state := restarted.GetAffinityGroupState("g")
	re.NotNil(state)
	re.False(state.AffinitySchedulingAllowed, "groups start degraded after a restart")

	// First availability pass: healthy stores make the group available again,
	// with the explicit target untouched.
	restarted.checkGroupsAvailability()
	state = restarted.GetAffinityGroupState("g")
	re.True(state.AffinitySchedulingAllowed)
	re.Equal(LeaderSourceExplicit, state.LeaderSource)
	re.Equal(uint64(1), state.LeaderStoreID)
	re.ElementsMatch([]uint64{1, 2, 3}, state.VoterStoreIDs)
}

// TestRestartedExplicitTargetSurvivesFirstWin reproduces the pgw3 cold-restore
// failure mode: groups with balanced explicit targets go through a restart,
// heartbeats with arbitrary leader placements arrive first, and the targets
// must not be rewritten.
func TestRestartedExplicitTargetSurvivesFirstWin(t *testing.T) {
	re := require.New(t)
	env := newLeaderSourceTestEnv(t, 3)

	// Three groups with balanced explicit targets across stores 1/2/3.
	targets := []struct {
		id     string
		leader uint64
		voters []uint64
	}{
		{"g1", 1, []uint64{1, 2, 3}},
		{"g2", 2, []uint64{2, 3, 1}},
		{"g3", 3, []uint64{3, 1, 2}},
	}
	for _, target := range targets {
		re.NoError(env.manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: target.id}}))
		_, err := env.manager.UpdateAffinityGroupPeers(target.id, target.leader, target.voters)
		re.NoError(err)
	}

	// Restart onto a fresh manager, then deliver first-win observations with
	// all leaders converging on store 2 (as elections would after a cold
	// restart).
	restarted := newRestartedManager(t, env)
	for i, target := range targets {
		region := regionOn(uint64(100+i), []byte{byte(i)}, []byte{byte(i + 1)}, 2, 2, 2)
		restarted.ObserveAvailableRegion(region, restarted.GetAffinityGroupState(target.id))
	}

	for _, target := range targets {
		state := restarted.GetAffinityGroupState(target.id)
		re.Equal(target.leader, state.LeaderStoreID, "group %s target must survive the restart", target.id)
		re.ElementsMatch(target.voters, state.VoterStoreIDs, "group %s target must survive the restart", target.id)
		re.Equal(LeaderSourceExplicit, state.LeaderSource)
	}

	// The first availability pass makes the groups available without touching
	// the targets.
	restarted.checkGroupsAvailability()
	for _, target := range targets {
		state := restarted.GetAffinityGroupState(target.id)
		re.True(state.AffinitySchedulingAllowed)
		re.Equal(target.leader, state.LeaderStoreID)
		re.ElementsMatch(target.voters, state.VoterStoreIDs)
	}
}

// TestSyncGroupFromEtcdCarriesLeaderSource verifies that the etcd sync path
// (used by the scheduling-service watcher) carries the leader source and the
// explicit target.
func TestSyncGroupFromEtcdCarriesLeaderSource(t *testing.T) {
	re := require.New(t)
	env := newLeaderSourceTestEnv(t, 3)

	// Follower manager without any groups.
	follower := newRestartedManager(t, env)

	// A fallback group: effective distribution derived from an observation.
	fallbackGroup := &Group{
		ID:            "fb",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{2, 3},
		LeaderSource:  LeaderSourceFallback,
	}
	follower.SyncGroupFromEtcd(fallbackGroup)
	state := follower.GetAffinityGroupState("fb")
	re.NotNil(state)
	re.Equal(LeaderSourceFallback, state.LeaderSource)
	re.Empty(state.ExplicitLeaderStoreID)

	// An explicit group: source and explicit target both propagate.
	explicitGroup := &Group{
		ID:                    "ex",
		LeaderStoreID:         1,
		VoterStoreIDs:         []uint64{1, 2, 3},
		LeaderSource:          LeaderSourceExplicit,
		ExplicitLeaderStoreID: 1,
		ExplicitVoterStoreIDs: []uint64{1, 2, 3},
	}
	follower.SyncGroupFromEtcd(explicitGroup)
	state = follower.GetAffinityGroupState("ex")
	re.NotNil(state)
	re.Equal(LeaderSourceExplicit, state.LeaderSource)
	re.Equal(uint64(1), state.ExplicitLeaderStoreID)
	re.ElementsMatch([]uint64{1, 2, 3}, state.ExplicitVoterStoreIDs)

	// A group in the legacy format is migrated the same way as on load.
	legacyGroup := &Group{ID: "legacy", LeaderStoreID: 3, VoterStoreIDs: []uint64{3, 1}}
	follower.SyncGroupFromEtcd(legacyGroup)
	state = follower.GetAffinityGroupState("legacy")
	re.NotNil(state)
	re.Equal(LeaderSourceExplicit, state.LeaderSource)
	re.Equal(uint64(3), state.ExplicitLeaderStoreID)
	re.ElementsMatch([]uint64{1, 3}, state.ExplicitVoterStoreIDs)
}

// TestRestoreDebounceGuardsAgainstFlapping verifies that after a fallback the
// restore to the explicit target only happens after the explicit-target stores
// have stayed healthy for the whole debounce window, and that a flap during
// the window restarts the wait.
func TestRestoreDebounceGuardsAgainstFlapping(t *testing.T) {
	re := require.New(t)
	env := newLeaderSourceTestEnv(t, 3)
	env.manager.restoreDebounce = 2 * time.Second

	re.NoError(env.manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
	_, err := env.manager.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
	re.NoError(err)

	// Store 1 goes down: the group expires and falls back to an observation.
	env.setStoreHealth(t, 1, false)
	env.manager.checkGroupsAvailability()
	env.manager.ObserveAvailableRegion(regionOn(10, []byte("a"), []byte("b"), 2, 3), env.manager.GetAffinityGroupState("g"))
	state := env.manager.GetAffinityGroupState("g")
	re.Equal(LeaderSourceFallback, state.LeaderSource)

	// Store 1 recovers: the first tick arms the debounce window without
	// restoring.
	env.setStoreHealth(t, 1, true)
	env.manager.checkGroupsAvailability()
	state = env.manager.GetAffinityGroupState("g")
	re.Equal(LeaderSourceFallback, state.LeaderSource, "debounce must delay the restore")

	// A flap during the window restarts the wait.
	env.setStoreHealth(t, 1, false)
	env.manager.checkGroupsAvailability()
	env.setStoreHealth(t, 1, true)
	env.manager.checkGroupsAvailability()
	state = env.manager.GetAffinityGroupState("g")
	re.Equal(LeaderSourceFallback, state.LeaderSource, "flapping must restart the debounce window")

	// Once the window elapses, the group returns to the explicit target.
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		env.manager.checkGroupsAvailability()
		state = env.manager.GetAffinityGroupState("g")
		if state.LeaderSource == LeaderSourceExplicit {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	state = env.manager.GetAffinityGroupState("g")
	re.Equal(LeaderSourceExplicit, state.LeaderSource)
	re.True(state.AffinitySchedulingAllowed)
	re.Equal(uint64(1), state.LeaderStoreID)
	re.ElementsMatch([]uint64{1, 2, 3}, state.VoterStoreIDs)
}
