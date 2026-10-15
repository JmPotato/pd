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

package handlers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

// affinityLeaderSourceSuite exercises the explicit leader target contract of
// affinity groups end to end: explicit targets survive PD restarts, PD leader
// migrations, degraded periods, and store outages, and are restored once the
// explicit-target stores recover.
type affinityLeaderSourceSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestAffinityLeaderSourceSuite(t *testing.T) {
	suite.Run(t, new(affinityLeaderSourceSuite))
}

func (suite *affinityLeaderSourceSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T(), func(conf *config.Config, _ string) {
		conf.Schedule.AffinityScheduleLimit = 4
		conf.LabelProperty = config.LabelPropertyConfig{
			"reject-leader": {{Key: "reject", Value: "leader"}},
		}
	})
	suite.env.Env = tests.NonMicroserviceEnv
}

func (suite *affinityLeaderSourceSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *affinityLeaderSourceSuite) TearDownTest() {
	suite.env.RunFuncInStartedEnvs(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		if leader == nil {
			return
		}
		manager, err := leader.GetServer().GetAffinityManager()
		if err != nil {
			return
		}
		allGroups := manager.GetAllAffinityGroupStates()
		groupIDs := make([]string, 0, len(allGroups))
		for _, group := range allGroups {
			groupIDs = append(groupIDs, group.ID)
		}
		if len(groupIDs) > 0 {
			re.NoError(manager.DeleteAffinityGroups(groupIDs, true))
		}
	})
}

// withFastAvailabilityCheck speeds up the availability check loop to 100ms so
// tests can wait for availability transitions through the API.
func (suite *affinityLeaderSourceSuite) withFastAvailabilityCheck(re *require.Assertions) {
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval", "return(true)"))
	suite.T().Cleanup(func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/affinity/changeAvailabilityCheckInterval"))
	})
}

// mustPutStoreWithHeartbeat puts a store and controls its last-heartbeat time:
// a recent time keeps it healthy, a stale one marks it down.
func mustPutStoreWithHeartbeat(re *require.Assertions, cluster *tests.TestCluster, store *metapb.Store, lastHeartbeat time.Time) {
	tests.MustPutStore(re, cluster, store)

	leader := cluster.GetLeaderServer()
	re.NotNil(leader)
	raftCluster := leader.GetServer().GetRaftCluster()
	re.NotNil(raftCluster)
	storeInfo := raftCluster.GetStore(store.GetId())
	re.NotNil(storeInfo)

	stats := &pdpb.StoreStats{
		Capacity:  uint64(10 * units.GiB),
		UsedSize:  uint64(1 * units.GiB),
		Available: uint64(9 * units.GiB),
	}
	newStore := storeInfo.Clone(
		core.SetStoreStats(stats),
		core.SetLastHeartbeatTS(lastHeartbeat),
	)
	raftCluster.GetBasicCluster().PutStore(newStore)
	raftCluster.UpdateAllStoreStatus()

	sche := cluster.GetSchedulingPrimaryServer()
	if sche != nil {
		sche.GetCluster().PutStore(newStore)
		sche.GetCluster().UpdateAllStoreStatus()
	}
}

// mustPutStoreWithHeartbeat is defined via mustPutStoreWithHeartbeat below;
// peer updates reuse the shared mustUpdateAffinityGroupPeers helper.

// balancedGroups
// balancedGroups matches the pgw3 failure mode: three groups with explicit
// leaders spread over three stores.
var balancedGroups = []struct {
	id     string
	leader uint64
	voters []uint64
	start  []byte
	end    []byte
}{
	{"pg-g1", 1, []uint64{1, 2, 3}, []byte{0x00}, []byte{0x40}},
	{"pg-g2", 2, []uint64{2, 3, 1}, []byte{0x40}, []byte{0x80}},
	{"pg-g3", 3, []uint64{3, 1, 2}, []byte{0x80}, []byte{0xc0}},
}

// setupExplicitGroups creates healthy stores and registers the balanced groups
// with explicit peer targets through the HTTP API.
func (suite *affinityLeaderSourceSuite) setupExplicitGroups(re *require.Assertions, cluster *tests.TestCluster) {
	serverAddr := cluster.GetLeaderServer().GetAddr()
	for i := uint64(1); i <= 3; i++ {
		mustPutHealthyStore(re, cluster, &metapb.Store{
			Id:        i,
			Address:   fmt.Sprintf("tikv%d", i),
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
		})
	}
	for _, target := range balancedGroups {
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				target.id: {Ranges: []handlers.AffinityKeyRange{{StartKey: target.start, EndKey: target.end}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)
		mustUpdateAffinityGroupPeers(re, serverAddr, target.id, &handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: target.leader,
			VoterStoreIDs: target.voters,
		})
	}
	for _, target := range balancedGroups {
		state := mustGetAffinityGroup(re, serverAddr, target.id)
		re.Equal(affinity.LeaderSourceExplicit, state.LeaderSource)
		re.Equal(target.leader, state.LeaderStoreID)
		re.ElementsMatch(target.voters, state.VoterStoreIDs)
	}
}

// assertBalancedTargets verifies via HTTP that every group still reports its
// explicit target and source.
func (suite *affinityLeaderSourceSuite) assertBalancedTargets(re *require.Assertions, cluster *tests.TestCluster) {
	serverAddr := cluster.GetLeaderServer().GetAddr()
	for _, target := range balancedGroups {
		state := mustGetAffinityGroup(re, serverAddr, target.id)
		re.Equal(affinity.LeaderSourceExplicit, state.LeaderSource, "group %s", target.id)
		re.Equal(target.leader, state.LeaderStoreID, "group %s", target.id)
		re.ElementsMatch(target.voters, state.VoterStoreIDs, "group %s", target.id)
		re.Equal(target.leader, state.ExplicitLeaderStoreID, "group %s", target.id)
		re.ElementsMatch(target.voters, state.ExplicitVoterStoreIDs, "group %s", target.id)
	}
}

// TestAffinityExplicitTargetSurvivesPDRestart reproduces the pgw3 cold-restore
// failure mode at the API level: balanced explicit targets must survive a full
// PD restart, and first-win observations must not rewrite them afterwards.
func (suite *affinityLeaderSourceSuite) TestAffinityExplicitTargetSurvivesPDRestart() {
	re := suite.Require()
	suite.withFastAvailabilityCheck(re)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	leaderServer := cluster.GetServer(cluster.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())
	defer func() {
		cluster.Destroy()
		cancel()
	}()

	suite.setupExplicitGroups(re, cluster)

	// Soft PD restart: stop every server without wiping its data directory and
	// start it again, exactly like a controlled PD restart in production.
	for name, server := range cluster.GetServers() {
		re.NoError(server.Stop(), name)
	}
	for name, server := range cluster.GetServers() {
		re.NoError(server.Run(), name)
	}
	re.NotEmpty(cluster.WaitLeader())

	// After the restart the groups are loaded from storage with their explicit
	// targets and source marker intact (availability may still be degraded).
	suite.assertBalancedTargets(re, cluster)

	// First-win observations with arbitrarily placed Regions (all leaders on
	// store 2, as elections would produce) must not rewrite the targets, no
	// matter whether the groups are still degraded or already available again.
	manager := cluster.GetLeaderServer().GetServer().GetRaftCluster().GetAffinityManager()
	re.NotNil(manager)
	for i, target := range balancedGroups {
		region := core.NewRegionInfo(
			&metapb.Region{
				Id:       uint64(1000 + i),
				StartKey: []byte{byte(i)},
				EndKey:   []byte{byte(i + 1)},
				Peers: []*metapb.Peer{
					{Id: uint64(2000 + i), StoreId: 2, Role: metapb.PeerRole_Voter},
				},
			},
			&metapb.Peer{Id: uint64(2000 + i), StoreId: 2, Role: metapb.PeerRole_Voter},
		)
		manager.ObserveAvailableRegion(region, manager.GetAffinityGroupState(target.id))
	}
	suite.assertBalancedTargets(re, cluster)

	// The availability pass makes the groups available (phase leaves pending)
	// without touching the targets. Real TiKV stores refresh their heartbeats
	// after a restart; emulate that here.
	for i := uint64(1); i <= 3; i++ {
		mustPutHealthyStore(re, cluster, &metapb.Store{
			Id:        i,
			Address:   fmt.Sprintf("tikv%d", i),
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
		})
	}
	testutil.Eventually(re, func() bool {
		for _, target := range balancedGroups {
			state := mustGetAffinityGroup(re, cluster.GetLeaderServer().GetAddr(), target.id)
			if state.Phase == affinity.PhasePending {
				return false
			}
		}
		return true
	})
	suite.assertBalancedTargets(re, cluster)
}

// TestAffinityExplicitTargetRestoredAfterStoreOutage covers the full outage
// cycle: explicit target -> target store down (expired, fallback takes over) ->
// store recovers -> explicit target restored.
func (suite *affinityLeaderSourceSuite) TestAffinityExplicitTargetRestoredAfterStoreOutage() {
	re := suite.Require()
	suite.withFastAvailabilityCheck(re)
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()
		manager := leader.GetServer().GetRaftCluster().GetAffinityManager()
		re.NotNil(manager)

		for i := uint64(1); i <= 3; i++ {
			mustPutHealthyStore(re, cluster, &metapb.Store{
				Id:        i,
				Address:   fmt.Sprintf("tikv%d", i),
				State:     metapb.StoreState_Up,
				NodeState: metapb.NodeState_Serving,
			})
		}
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"outage": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{}, EndKey: []byte{}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)
		mustUpdateAffinityGroupPeers(re, serverAddr, "outage", &handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1, 2, 3},
		})
		state := mustGetAffinityGroup(re, serverAddr, "outage")
		re.Equal(affinity.LeaderSourceExplicit, state.LeaderSource)

		// Store 1 goes down hard; the availability pass expires the group.
		mustPutStoreWithHeartbeat(re, cluster, &metapb.Store{
			Id:        1,
			Address:   "tikv1",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
		}, time.Now().Add(-2*time.Hour))
		testutil.Eventually(re, func() bool {
			state = mustGetAffinityGroup(re, serverAddr, "outage")
			return state.Phase == affinity.PhasePending
		})

		// A healthy Region (peers on stores 2/3) takes over as a fallback; the
		// explicit target is preserved and visible through the API.
		region := core.NewRegionInfo(
			&metapb.Region{
				Id:       100,
				StartKey: []byte{},
				EndKey:   []byte{},
				Peers: []*metapb.Peer{
					{Id: 21, StoreId: 2, Role: metapb.PeerRole_Voter},
					{Id: 22, StoreId: 3, Role: metapb.PeerRole_Voter},
				},
			},
			&metapb.Peer{Id: 21, StoreId: 2, Role: metapb.PeerRole_Voter},
		)
		manager.ObserveAvailableRegion(region, manager.GetAffinityGroupState("outage"))
		state = mustGetAffinityGroup(re, serverAddr, "outage")
		re.Equal(affinity.LeaderSourceFallback, state.LeaderSource)
		re.Equal(uint64(2), state.LeaderStoreID)
		re.Equal(uint64(1), state.ExplicitLeaderStoreID)
		re.ElementsMatch([]uint64{1, 2, 3}, state.ExplicitVoterStoreIDs)

		// Store 1 recovers: the explicit target is restored.
		mustPutStoreWithHeartbeat(re, cluster, &metapb.Store{
			Id:        1,
			Address:   "tikv1",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
		}, time.Now())
		// The restore is debounced (max-store-down-time) so the explicit target
		// stores must stay healthy for that window first. Real TiKV stores keep
		// heartbeating; emulate that with a background refresher.
		stopRefresh := make(chan struct{})
		refreshDone := make(chan struct{})
		go func() {
			defer close(refreshDone)
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			put := func(storeID uint64) {
				raftCluster := leader.GetServer().GetRaftCluster()
				if raftCluster == nil {
					return
				}
				storeInfo := raftCluster.GetStore(storeID)
				if storeInfo == nil {
					return
				}
				newStore := storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
				raftCluster.GetBasicCluster().PutStore(newStore)
				raftCluster.UpdateAllStoreStatus()
			}
			for {
				select {
				case <-stopRefresh:
					return
				case <-ticker.C:
					for id := uint64(1); id <= 3; id++ {
						put(id)
					}
				}
			}
		}()
		testutil.Eventually(re, func() bool {
			state = mustGetAffinityGroup(re, serverAddr, "outage")
			return state.LeaderSource == affinity.LeaderSourceExplicit &&
				state.LeaderStoreID == 1 &&
				state.Phase != affinity.PhasePending
		}, testutil.WithWaitFor(120*time.Second))
		close(stopRefresh)
		<-refreshDone
		re.ElementsMatch([]uint64{1, 2, 3}, state.VoterStoreIDs)
	})
}

// TestAffinityLeaderEvictedKeepsExplicitTarget covers the degraded marking
// family (evict-leader / evict-slow-store / reject-leader) — the exact
// scenario from the pgw3 field reproduction where evict-slow-store fired
// during import and the old PD rewrote 42 groups' targets. With the fix:
// while a marking is on, the explicit target is never rewritten; if the
// group degrades but its explicit-target stores are healthy, it returns to
// available immediately; and lifting the marking changes nothing.
func (suite *affinityLeaderSourceSuite) TestAffinityLeaderEvictedKeepsExplicitTarget() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()
		manager := leader.GetServer().GetRaftCluster().GetAffinityManager()
		re.NotNil(manager)
		raftCluster := leader.GetServer().GetRaftCluster()
		re.NotNil(raftCluster)

		for i := uint64(1); i <= 3; i++ {
			mustPutHealthyStore(re, cluster, &metapb.Store{
				Id:        i,
				Address:   fmt.Sprintf("tikv%d", i),
				State:     metapb.StoreState_Up,
				NodeState: metapb.NodeState_Serving,
			})
		}
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"evicted": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{}, EndKey: []byte{}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)
		mustUpdateAffinityGroupPeers(re, serverAddr, "evicted", &handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1, 2, 3},
		})

		// A healthy Region that does not match the explicit target: observations
		// of it must never change the explicit target below.
		region := core.NewRegionInfo(
			&metapb.Region{
				Id:       100,
				StartKey: []byte{},
				EndKey:   []byte{},
				Peers: []*metapb.Peer{
					{Id: 21, StoreId: 2, Role: metapb.PeerRole_Voter},
					{Id: 22, StoreId: 3, Role: metapb.PeerRole_Voter},
				},
			},
			&metapb.Peer{Id: 21, StoreId: 2, Role: metapb.PeerRole_Voter},
		)

		// While a marking is on, the group may legitimately stay degraded (the
		// marked store cannot take leaders) — but the explicit target must not
		// be rewritten, no matter how many availability ticks pass.
		assertTargetIntactWhileMarked := func() {
			manager.ObserveAvailableRegion(region, manager.GetAffinityGroupState("evicted"))
			time.Sleep(12 * time.Second)
			state := mustGetAffinityGroup(re, serverAddr, "evicted")
			re.Equal(uint64(1), state.LeaderStoreID, "explicit leader must survive the degraded marking")
			re.ElementsMatch([]uint64{1, 2, 3}, state.VoterStoreIDs)
			re.Equal(affinity.LeaderSourceExplicit, state.LeaderSource)
		}
		assertRestored := func(when string) {
			state := mustGetAffinityGroup(re, serverAddr, "evicted")
			re.NotEqual(affinity.PhasePending, state.Phase, when)
			re.Equal(uint64(1), state.LeaderStoreID, when)
			re.ElementsMatch([]uint64{1, 2, 3}, state.VoterStoreIDs, when)
			re.Equal(affinity.LeaderSourceExplicit, state.LeaderSource, when)
		}

		var state *affinity.GroupState
		for _, cycle := range []struct {
			name string
			mark func()
			lift func()
		}{
			{"reject-leader", func() {
				store := &metapb.Store{
					Id:        1,
					Address:   "tikv1",
					State:     metapb.StoreState_Up,
					NodeState: metapb.NodeState_Serving,
					Labels:    []*metapb.StoreLabel{{Key: "reject", Value: "leader"}},
				}
				raftCluster.GetBasicCluster().PutStore(core.NewStoreInfo(store, core.SetLastHeartbeatTS(time.Now())))
				raftCluster.UpdateAllStoreStatus()
			}, func() {
				raftCluster.GetBasicCluster().PutStore(core.NewStoreInfo(healthyMetapbStore(), core.SetLastHeartbeatTS(time.Now())))
				raftCluster.UpdateAllStoreStatus()
			}},
			{"evict-slow-store", func() {
				// The slow-store flag is a core-level state, not a label — this
				// is what the evict-slow-store scheduler does in the field.
				raftCluster.GetBasicCluster().PutStore(core.NewStoreInfo(healthyMetapbStore(), core.SetLastHeartbeatTS(time.Now()), core.SlowStoreEvicted()))
				raftCluster.UpdateAllStoreStatus()
			}, func() {
				raftCluster.GetBasicCluster().PutStore(core.NewStoreInfo(healthyMetapbStore(), core.SetLastHeartbeatTS(time.Now())))
				raftCluster.UpdateAllStoreStatus()
			}},
		} {
			// Keep the voter stores' heartbeats fresh: only the marked store
			// itself may be unavailable.
			refreshVoters := func() {
				for i := uint64(2); i <= 3; i++ {
					mustPutStoreWithHeartbeat(re, cluster, &metapb.Store{
						Id:        i,
						Address:   fmt.Sprintf("tikv%d", i),
						State:     metapb.StoreState_Up,
						NodeState: metapb.NodeState_Serving,
					}, time.Now())
				}
			}
			refreshVoters()
			cycle.mark()
			testutil.Eventually(re, func() bool {
				state = mustGetAffinityGroup(re, serverAddr, "evicted")
				return state.Phase == affinity.PhasePending
			})
			assertTargetIntactWhileMarked()
			refreshVoters()
			cycle.lift()
			testutil.Eventually(re, func() bool {
				state = mustGetAffinityGroup(re, serverAddr, "evicted")
				return state.Phase != affinity.PhasePending
			})
			assertRestored("after lifting " + cycle.name)
		}
	})
}

func healthyMetapbStore() *metapb.Store {
	return &metapb.Store{
		Id:        1,
		Address:   "tikv1",
		State:     metapb.StoreState_Up,
		NodeState: metapb.NodeState_Serving,
	}
}

// TestAffinityExplicitTargetSurvivesPDLeaderMigration covers a PD leader
// switchover: the new leader loads the groups from storage and keeps the
// explicit targets and sources.
func (suite *affinityLeaderSourceSuite) TestAffinityExplicitTargetSurvivesPDLeaderMigration() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := tests.NewTestCluster(ctx, 2)
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	leaderServer := cluster.GetServer(cluster.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())
	defer func() {
		cluster.Destroy()
		cancel()
	}()

	suite.setupExplicitGroups(re, cluster)

	// Switch the PD leader.
	oldLeaderAddr := leaderServer.GetAddr()
	err = leaderServer.ResignLeaderWithRetry()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	newLeader := cluster.GetLeaderServer()
	re.NotNil(newLeader)
	re.NotEqual(oldLeaderAddr, newLeader.GetAddr(), "the PD leader should have migrated")

	// The explicit targets and sources are intact via the new leader.
	newAddr := newLeader.GetAddr()
	for _, target := range balancedGroups {
		var state affinity.GroupState
		err := testutil.ReadGetJSON(re, tests.TestDialClient, getAffinityGroupURL(newAddr, target.id), &state)
		re.NoError(err)
		re.Equal(affinity.LeaderSourceExplicit, state.LeaderSource, "group %s", target.id)
		re.Equal(target.leader, state.LeaderStoreID, "group %s", target.id)
		re.ElementsMatch(target.voters, state.VoterStoreIDs, "group %s", target.id)
	}
}

// TestAffinityGroupStatusReportsLeaderSource verifies the leader_source values
// reported by the status API across the group lifecycle.
func (suite *affinityLeaderSourceSuite) TestAffinityGroupStatusReportsLeaderSource() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		serverAddr := leader.GetAddr()
		manager := leader.GetServer().GetRaftCluster().GetAffinityManager()
		re.NotNil(manager)

		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"lifecycle": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{}, EndKey: []byte{}}}},
			},
		}
		mustCreateAffinityGroups(re, serverAddr, &createReq)

		// Fresh group: no distribution yet.
		state := mustGetAffinityGroup(re, serverAddr, "lifecycle")
		re.Empty(state.LeaderSource)

		// Fallback after a first-win observation.
		mustPutHealthyStore(re, cluster, &metapb.Store{
			Id:        1,
			Address:   "tikv1",
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
		})
		region := core.NewRegionInfo(
			&metapb.Region{
				Id:       100,
				StartKey: []byte{},
				EndKey:   []byte{},
				Peers:    []*metapb.Peer{{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter}},
			},
			&metapb.Peer{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
		)
		manager.ObserveAvailableRegion(region, manager.GetAffinityGroupState("lifecycle"))
		state = mustGetAffinityGroup(re, serverAddr, "lifecycle")
		re.Equal(affinity.LeaderSourceFallback, state.LeaderSource)

		// Explicit after a PUT.
		mustUpdateAffinityGroupPeers(re, serverAddr, "lifecycle", &handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1},
		})
		state = mustGetAffinityGroup(re, serverAddr, "lifecycle")
		re.Equal(affinity.LeaderSourceExplicit, state.LeaderSource)
	})
}
