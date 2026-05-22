// Copyright 2026 TiKV Project Authors.
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

package main

import (
	"context"
	stderrors "errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/tools/pd-heartbeat-bench/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestRegionsInitAddsLearnerPeers(t *testing.T) {
	cfg := &config.Config{
		StoreCount:     6,
		RegionCount:    2,
		Replica:        3,
		ExtraPeerCount: 2,
		ExtraPeerRole:  "learner",
	}

	rs := new(Regions)
	rs.init(cfg)

	for _, region := range rs.regions {
		require.Len(t, region.Region.Peers, 4)
		require.Equal(t, region.Region.Peers[0], region.Leader)
		require.Equal(t, "Learner", region.Region.Peers[3].GetRole().String())
	}
}

func TestRegionsInitKeepsLeaderAsVoter(t *testing.T) {
	cfg := &config.Config{
		StoreCount:     6,
		RegionCount:    1,
		Replica:        3,
		ExtraPeerCount: 1,
		ExtraPeerRole:  "learner",
	}

	rs := new(Regions)
	rs.init(cfg)

	require.Equal(t, rs.regions[0].Region.Peers[0], rs.regions[0].Leader)
	require.Equal(t, "Voter", rs.regions[0].Leader.GetRole().String())
}

func TestRegionsInitDistributesExtraPeersDeterministically(t *testing.T) {
	cfg := &config.Config{
		StoreCount:     10,
		RegionCount:    3,
		Replica:        3,
		ExtraPeerCount: 5,
		ExtraPeerRole:  "learner",
	}

	rs := new(Regions)
	rs.init(cfg)

	require.Len(t, rs.regions[0].Region.Peers, 5)
	require.Len(t, rs.regions[1].Region.Peers, 5)
	require.Len(t, rs.regions[2].Region.Peers, 4)
}

func TestBuildReportBucketsRequestUsesNonEmptyKeys(t *testing.T) {
	cfg := &config.Config{StoreCount: 4, RegionCount: 3, Replica: 3}
	rs := new(Regions)
	rs.init(cfg)

	req := buildReportBucketsRequest(rs.regions[1], 2000)

	require.NotNil(t, req.GetBuckets())
	require.Equal(t, rs.regions[1].Region.GetId(), req.GetBuckets().GetRegionId())
	require.Equal(t, rs.regions[1].Region.GetRegionEpoch(), req.GetRegionEpoch())
	require.Len(t, req.GetBuckets().GetKeys(), 2)
	require.NotEmpty(t, req.GetBuckets().GetKeys()[0])
	require.NotEmpty(t, req.GetBuckets().GetKeys()[1])
	require.Equal(t, uint64(2000), req.GetBuckets().GetPeriodInMs())
}

func TestReportBucketsWorkersRespectConfiguredStreamCount(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &config.Config{StoreCount: 4, RegionCount: 10, Replica: 3}
	rs := new(Regions)
	rs.init(cfg)
	status := newBucketReporterStatus()
	var created atomic.Int64

	startReportBucketsWorkers(ctx, 3, time.Hour, rs, status, func(context.Context) (reportBucketsClient, error) {
		created.Add(1)
		return &blockingReportBucketsClient{ctx: ctx}, nil
	}, nil)

	require.Eventually(t, func() bool {
		return created.Load() == 3 && status.activeStreams.Load() == 3
	}, time.Second, 10*time.Millisecond)
}

// v2.1 (2026-05-20): regression test for (d) — explicit RegionApproximateSizeMiB and
// RegionApproximateKeys propagate to the synthetic heartbeat. Without this, PD's
// RegionInfo stays bytesUnit/keysUnit-sized and never produces the line-cluster heap.
func TestRegionsInitAppliesContentFidelityKnobs(t *testing.T) {
	cfg := &config.Config{
		StoreCount:               4,
		RegionCount:              3,
		Replica:                  3,
		RegionApproximateSizeMiB: 96,
		RegionApproximateKeys:    1_000_000,
	}

	rs := new(Regions)
	rs.init(cfg)

	for _, region := range rs.regions {
		require.Equal(t, uint64(96)*uint64(1<<20), region.ApproximateSize)
		require.Equal(t, uint64(1_000_000), region.ApproximateKeys)
	}
}

// v2.1 (2026-05-20): regression test for the legacy default — when knobs are zero, the
// existing 128B / 8keys placeholder behaviour is preserved (backwards compat).
func TestRegionsInitFallsBackToLegacyPlaceholdersWhenKnobsZero(t *testing.T) {
	cfg := &config.Config{StoreCount: 4, RegionCount: 2, Replica: 3}
	rs := new(Regions)
	rs.init(cfg)

	for _, region := range rs.regions {
		require.Equal(t, uint64(128), region.ApproximateSize)
		require.Equal(t, uint64(8), region.ApproximateKeys)
	}
}

// v2.1 (2026-05-20): regression test for the update() preservation path. Hot regions
// not selected into updateFlow this round must keep their seeded flow values across
// subsequent rounds. Without the preservation logic, PD HotPeerCache would see
// alternating hot/cold and never escalate them into long-lived entries.
func TestRegionsUpdatePreservesHotFlowAcrossRounds(t *testing.T) {
	cfg := &config.Config{
		StoreCount:             4,
		RegionCount:             100,
		Replica:                 3,
		HotRegionRatio:          1.0, // every region is hot so we don't rely on a specific shuffle
		HotWriteBytesPerRegion:  1_000_000,
		HotReadBytesPerRegion:   2_000_000,
		HotWriteKeysPerRegion:   5_000,
		HotReadKeysPerRegion:    10_000,
		ReportRatio:             1.0,
		FlowUpdateRatio:         0.05, // only 5% of regions go through updateFlow each round
	}

	rs := new(Regions)
	rs.init(cfg)
	options := config.NewOptions(cfg)

	// First update: 5% in updateFlow get jittered hot values; the other 95% should keep
	// their seeded hot values rather than being reset to 0 by the reportRegions reset.
	rs.update(cfg, options)

	awakenAny := rs.awakenRegions.Load().([]*pdpb.RegionHeartbeatRequest)
	zeros := 0
	for _, r := range awakenAny {
		if r.BytesWritten == 0 && r.BytesRead == 0 && r.KeysWritten == 0 && r.KeysRead == 0 {
			zeros++
		}
	}
	require.Zero(t, zeros, "hot regions outside updateFlow must keep their seeded flow, got %d reset to zero", zeros)
}

// v2.1 (2026-05-20): regression test for (e) — HotRegionRatio picks roughly the right
// fraction of regions and seeds their initial flow fields from the Hot*PerRegion knobs.
func TestRegionsInitMarksHotRegionsByRatioAndSeedsFlow(t *testing.T) {
	cfg := &config.Config{
		StoreCount:             4,
		RegionCount:             100,
		Replica:                 3,
		HotRegionRatio:          0.30,
		HotWriteBytesPerRegion:  1_000_000,
		HotReadBytesPerRegion:   2_000_000,
		HotWriteKeysPerRegion:   5_000,
		HotReadKeysPerRegion:    10_000,
	}

	rs := new(Regions)
	rs.init(cfg)

	require.Equal(t, 30, countHotRegions(rs), "expected 30 hot regions for ratio 0.3 of 100")

	hot, cold := 0, 0
	for i, region := range rs.regions {
		if rs.hotRegion[i] {
			hot++
			require.Equal(t, uint64(1_000_000), region.BytesWritten, "hot region BytesWritten should be seeded")
			require.Equal(t, uint64(2_000_000), region.BytesRead, "hot region BytesRead should be seeded")
			require.Equal(t, uint64(5_000), region.KeysWritten)
			require.Equal(t, uint64(10_000), region.KeysRead)
		} else {
			cold++
			require.Equal(t, uint64(0), region.BytesWritten, "cold region should not be seeded")
			require.Equal(t, uint64(0), region.BytesRead)
		}
	}
	require.Equal(t, 30, hot)
	require.Equal(t, 70, cold)
}

// v2.1 (2026-05-20): regression test for (f) — when a gate channel is supplied, workers
// must not call the factory until the gate is closed (= first heartbeat round done).
// Without this, ReportBuckets races RegionHeartbeat and PD drops the bucket.
func TestReportBucketsWorkersBlockUntilGateClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &config.Config{StoreCount: 4, RegionCount: 10, Replica: 3}
	rs := new(Regions)
	rs.init(cfg)
	status := newBucketReporterStatus()
	var created atomic.Int64

	gate := make(chan struct{})
	startReportBucketsWorkers(ctx, 3, time.Hour, rs, status, func(context.Context) (reportBucketsClient, error) {
		created.Add(1)
		return &blockingReportBucketsClient{ctx: ctx}, nil
	}, gate)

	// Give the goroutines a chance to run. They should be blocked on the gate, so no
	// stream factory call yet.
	require.Never(t, func() bool {
		return created.Load() > 0
	}, 100*time.Millisecond, 20*time.Millisecond, "no stream should be created while gate is closed")

	// Release the gate: workers should now open their streams.
	close(gate)
	require.Eventually(t, func() bool {
		return created.Load() == 3 && status.activeStreams.Load() == 3
	}, time.Second, 10*time.Millisecond)
}

func TestRunMinResolvedTSReporterReportsIndependently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls atomic.Int64
	done := runMinResolvedTSReporter(ctx, 3, time.Millisecond, func(context.Context, uint64) error {
		calls.Add(1)
		return nil
	})

	require.Eventually(t, func() bool {
		return calls.Load() >= 6
	}, time.Second, 10*time.Millisecond)

	cancel()
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestIntervalForAggregateQPS(t *testing.T) {
	require.Equal(t, time.Second, intervalForAggregateQPS(100, 100, time.Minute))
	require.Equal(t, 500*time.Millisecond, intervalForAggregateQPS(200, 100, time.Minute))
	require.Equal(t, time.Minute, intervalForAggregateQPS(0, 100, time.Minute))
}

func TestExpectedReportBucketsShutdownNeedsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.True(t, isExpectedReportBucketsShutdown(ctx, context.Canceled))
	require.False(t, isExpectedReportBucketsShutdown(context.Background(), context.Canceled))
	require.False(t, isExpectedReportBucketsShutdown(ctx, stderrors.New("send failed")))
}

// v3.1.4 (2026-05-21): tests for service discovery + leader-aware reconnect.

// mockPDServer is a minimal pdpb.PDServer used to drive resolvePDLeader and
// streamSlot tests without spinning up a real PD. It supports configurable
// GetMembers responses (so we can move the "leader" between mocks) and an
// optional RegionHeartbeat handler that the test can install to assert
// stream-level interactions.
type mockPDServer struct {
	pdpb.UnimplementedPDServer
	mu         sync.Mutex
	members    *pdpb.GetMembersResponse
	membersErr error

	heartbeatRecv func(*pdpb.RegionHeartbeatRequest)
	closeStreams  atomic.Bool
}

func (m *mockPDServer) setMembers(resp *pdpb.GetMembersResponse, err error) {
	m.mu.Lock()
	m.members = resp
	m.membersErr = err
	m.mu.Unlock()
}

func (m *mockPDServer) GetMembers(ctx context.Context, req *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.membersErr != nil {
		return nil, m.membersErr
	}
	return m.members, nil
}

func (m *mockPDServer) RegionHeartbeat(stream pdpb.PD_RegionHeartbeatServer) error {
	for {
		if m.closeStreams.Load() {
			return io.EOF
		}
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if cb := m.heartbeatRecv; cb != nil {
			cb(req)
		}
	}
}

func startMockPD(t *testing.T) (*mockPDServer, string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &mockPDServer{}
	gs := grpc.NewServer()
	pdpb.RegisterPDServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	stop := func() {
		gs.Stop()
		lis.Close()
	}
	return srv, "http://" + lis.Addr().String(), stop
}

func TestResolvePDLeaderFirstEndpointIsLeader(t *testing.T) {
	srv, addr, stop := startMockPD(t)
	defer stop()
	srv.setMembers(&pdpb.GetMembersResponse{
		Header: &pdpb.ResponseHeader{ClusterId: 1},
		Leader: &pdpb.Member{ClientUrls: []string{addr}},
	}, nil)

	cfg := &config.Config{PDAddr: addr}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ep, cc, cli, err := resolvePDLeader(ctx, cfg)
	require.NoError(t, err)
	require.Equal(t, addr, ep, "leaderURL with scheme must round-trip through canonicalEndpoint")
	require.NotNil(t, cli)
	require.NotNil(t, cc)
	cc.Close()
}

func TestResolvePDLeaderFollowerRedirectsToLeader(t *testing.T) {
	// 2 mock PDs; first is a follower that knows the second is leader.
	follower, followerAddr, stopF := startMockPD(t)
	defer stopF()
	leader, leaderAddr, stopL := startMockPD(t)
	defer stopL()

	follower.setMembers(&pdpb.GetMembersResponse{
		Header: &pdpb.ResponseHeader{ClusterId: 1},
		Leader: &pdpb.Member{ClientUrls: []string{leaderAddr}},
	}, nil)
	leader.setMembers(&pdpb.GetMembersResponse{
		Header: &pdpb.ResponseHeader{ClusterId: 1},
		Leader: &pdpb.Member{ClientUrls: []string{leaderAddr}},
	}, nil)

	cfg := &config.Config{PDAddr: followerAddr + "," + leaderAddr}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ep, cc, cli, err := resolvePDLeader(ctx, cfg)
	require.NoError(t, err)
	require.Equal(t, leaderAddr, ep, "must redirect to leader endpoint, preserving scheme")
	require.NotNil(t, cli)
	cc.Close()
}

func TestResolvePDLeaderFallsThroughOnFirstEndpointDown(t *testing.T) {
	// First endpoint dead, second is leader.
	leader, leaderAddr, stopL := startMockPD(t)
	defer stopL()
	leader.setMembers(&pdpb.GetMembersResponse{
		Header: &pdpb.ResponseHeader{ClusterId: 1},
		Leader: &pdpb.Member{ClientUrls: []string{leaderAddr}},
	}, nil)

	// Pick an almost-certainly-unused port for the dead endpoint.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	deadAddr := "http://" + lis.Addr().String()
	lis.Close() // free the port — gRPC dial will fail to connect / GetMembers will time out

	cfg := &config.Config{PDAddr: deadAddr + "," + leaderAddr}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	ep, cc, cli, err := resolvePDLeader(ctx, cfg)
	require.NoError(t, err)
	require.Equal(t, leaderAddr, ep)
	require.NotNil(t, cli)
	cc.Close()
}

func TestResolvePDLeaderReturnsErrWhenAllDown(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	deadAddr := "http://" + lis.Addr().String()
	lis.Close()

	cfg := &config.Config{PDAddr: deadAddr}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	_, _, _, err = resolvePDLeader(ctx, cfg)
	require.Error(t, err)
}

func TestCanonicalEndpoint(t *testing.T) {
	require.Equal(t, "http://127.0.0.1:2379", canonicalEndpoint("http://127.0.0.1:2379"))
	require.Equal(t, "http://127.0.0.1:2379", canonicalEndpoint("http://127.0.0.1:2379/"))
	require.Equal(t, "http://127.0.0.1:2379", canonicalEndpoint("  http://127.0.0.1:2379  "))
	require.Equal(t, "https://x.y.z:2379", canonicalEndpoint("https://x.y.z:2379"))
}

// runOneRound full-resend semantics: when fullResendNeeded is true,
// (a) the whole leader-owned region set is sent (no report-ratio subset, no
// burst-cycle fraction trim), (b) no smooth pacing is applied, (c) the
// fullResend bit is cleared (CompareAndSwap consumed it), and (d) the round
// metric increments.
func TestRunOneRoundFullResendBypassesShaping(t *testing.T) {
	cfg := &config.Config{
		StoreCount:             2,
		RegionCount:            20,
		Replica:                1,
		SmoothHeartbeatPacing:  true, // would normally pace; fullResend must override
		BurstCycle:             true,
		BurstCyclePeriodSec:    600,
		BurstCycleHighSec:      60,
		BurstCycleRampSec:      60,
		BurstCycleHighFraction: 0.10,
		BurstCycleLowFraction:  0.10, // would normally cut regions to 10%
	}
	rs := new(Regions)
	rs.init(cfg)
	forceAllRegionsToStore1(rs)

	// Simulate a steady-round update having previously narrowed awakenRegions
	// to a tiny subset. fullResend must IGNORE this and send rs.regions in full.
	rs.awakenRegions.Store(rs.regions[:2])

	stream := &timingHeartbeatStream{}
	slot := makeTestSlot(t, context.Background(), cfg, stream)
	slot.fullResendNeeded.Store(true)
	statsCh := make(chan storeRoundStat, 1)

	before := testCounterValue(t, heartbeatFullResendRounds)

	start := time.Now()
	runOneRound(context.Background(), cfg, rs, slot, 1, 100*time.Millisecond, statsCh)
	elapsed := time.Since(start)

	sends := stream.snapshot()
	require.Len(t, sends, 20, "fullResend must send EVERY leader-owned region, not the awakenRegions subset nor the burst-cycle fraction")
	require.LessOrEqual(t, elapsed, 30*time.Millisecond, "fullResend must skip per-region pacing (got %s; expected near-instant burst)", elapsed)
	require.False(t, slot.fullResendNeeded.Load(), "CompareAndSwap must have cleared the bit")
	require.Equal(t, before+1, testCounterValue(t, heartbeatFullResendRounds), "full-resend round metric must increment")

	select {
	case st := <-statsCh:
		require.Equal(t, 20, st.sent)
		require.True(t, st.fullResend)
	default:
		t.Fatalf("expected statsCh to receive one storeRoundStat")
	}
}

// testCounterValue reads a single prometheus.Counter's current value via
// Write() (the documented test API). Used to detect Inc() side-effects.
func testCounterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, c.Write(m))
	return m.GetCounter().GetValue()
}

// streamSlot.reconnect pointer-equality guard: after one goroutine rebuilds
// the stream, a second goroutine calling reconnect with the OLD stream
// pointer must observe s.stream != observed and return a no-op nil.
func TestReconnectPointerEqualityGuard(t *testing.T) {
	srv, addr, stop := startMockPD(t)
	defer stop()
	srv.setMembers(&pdpb.GetMembersResponse{
		Header: &pdpb.ResponseHeader{ClusterId: 1},
		Leader: &pdpb.Member{ClientUrls: []string{addr}},
	}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &config.Config{
		PDAddr:                addr,
		AutoReconnect:         true,
		FullResendOnReconnect: true,
	}
	slot := newStreamSlot(ctx, cfg, 1)
	oldStream := slot.stream

	// First reconnect with the live stream pointer rebuilds. After it returns,
	// s.stream != oldStream.
	require.NoError(t, slot.reconnect(oldStream))
	require.NotEqual(t, oldStream, slot.stream, "first reconnect must replace the stream")

	// Second reconnect with the (now stale) oldStream must be a no-op nil.
	preReconnects := slot.reconnects.Load()
	require.NoError(t, slot.reconnect(oldStream))
	require.Equal(t, preReconnects, slot.reconnects.Load(), "stale-pointer reconnect must NOT bump the reconnect counter")
}

// streamSlot.GetCli (and the in-line use in runMinResolvedTSReporter) must
// observe the post-reconnect cli, not a snapshot of the boot-time cli.
func TestGetCliReflectsPostReconnectLeader(t *testing.T) {
	// Two PDs; first is initial leader, then leadership transfers to the second.
	srvA, addrA, stopA := startMockPD(t)
	defer stopA()
	srvB, addrB, stopB := startMockPD(t)
	defer stopB()

	srvA.setMembers(&pdpb.GetMembersResponse{
		Header: &pdpb.ResponseHeader{ClusterId: 1},
		Leader: &pdpb.Member{ClientUrls: []string{addrA}},
	}, nil)
	srvB.setMembers(&pdpb.GetMembersResponse{
		Header: &pdpb.ResponseHeader{ClusterId: 1},
		Leader: &pdpb.Member{ClientUrls: []string{addrB}},
	}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &config.Config{
		PDAddr:                addrA + "," + addrB,
		AutoReconnect:         true,
		FullResendOnReconnect: true,
	}
	slot := newStreamSlot(ctx, cfg, 1)
	bootCli := slot.GetCli()
	require.NotNil(t, bootCli)
	require.Equal(t, addrA, slot.currentEp)

	// Simulate leader transfer: A now points at B; reconnect should re-resolve.
	srvA.setMembers(&pdpb.GetMembersResponse{
		Header: &pdpb.ResponseHeader{ClusterId: 1},
		Leader: &pdpb.Member{ClientUrls: []string{addrB}},
	}, nil)
	require.NoError(t, slot.reconnect(slot.stream))

	require.Equal(t, addrB, slot.currentEp, "currentEp must move to new leader")
	newCli := slot.GetCli()
	require.NotEqual(t, bootCli, newCli, "GetCli must observe the post-reconnect cli (not a stale snapshot)")
}

// reconnect signalling kicks the worker so the post-reconnect round runs
// immediately instead of waiting up to a full tickInterval.
func TestReconnectSignalsKickChannel(t *testing.T) {
	srv, addr, stop := startMockPD(t)
	defer stop()
	srv.setMembers(&pdpb.GetMembersResponse{
		Header: &pdpb.ResponseHeader{ClusterId: 1},
		Leader: &pdpb.Member{ClientUrls: []string{addr}},
	}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &config.Config{
		PDAddr:                addr,
		AutoReconnect:         true,
		FullResendOnReconnect: true,
	}
	slot := newStreamSlot(ctx, cfg, 1)

	// Drain any pre-existing signal.
	select {
	case <-slot.kickCh:
	default:
	}

	require.NoError(t, slot.reconnect(slot.stream))
	select {
	case <-slot.kickCh:
		// expected: reconnect pushed a kick.
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected kickCh signal within 100ms after reconnect")
	}
	require.True(t, slot.fullResendNeeded.Load(), "fullResendOnReconnect must arm the bit")
}

type blockingReportBucketsClient struct {
	ctx context.Context
}

func (c *blockingReportBucketsClient) Send(*pdpb.ReportBucketsRequest) error {
	return nil
}

func (c *blockingReportBucketsClient) CloseAndRecv() (*pdpb.ReportBucketsResponse, error) {
	<-c.ctx.Done()
	return nil, io.EOF
}

// v2.3 (2026-05-20): tests for smooth heartbeat pacing.

// timingHeartbeatStream is a minimal pdpb.PD_RegionHeartbeatClient that records the
// wall-clock instant of each Send call so the test can verify the pacing distribution.
type timingHeartbeatStream struct {
	mu        sync.Mutex
	sendTimes []time.Time
	ctx       context.Context
}

func (s *timingHeartbeatStream) Send(*pdpb.RegionHeartbeatRequest) error {
	s.mu.Lock()
	s.sendTimes = append(s.sendTimes, time.Now())
	s.mu.Unlock()
	return nil
}

func (s *timingHeartbeatStream) Recv() (*pdpb.RegionHeartbeatResponse, error) {
	return nil, io.EOF
}

func (s *timingHeartbeatStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *timingHeartbeatStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *timingHeartbeatStream) CloseSend() error             { return nil }
func (s *timingHeartbeatStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}
func (s *timingHeartbeatStream) SendMsg(any) error { return nil }
func (s *timingHeartbeatStream) RecvMsg(any) error { return nil }

func (s *timingHeartbeatStream) snapshot() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]time.Time, len(s.sendTimes))
	copy(out, s.sendTimes)
	return out
}

func TestComputeSmoothPaceDelay(t *testing.T) {
	cfgOff := &config.Config{SmoothHeartbeatPacing: false}
	cfgOn := &config.Config{SmoothHeartbeatPacing: true}

	require.Equal(t, time.Duration(0), computeSmoothPaceDelay(cfgOff, time.Second, 100), "smooth disabled = 0")
	require.Equal(t, time.Duration(0), computeSmoothPaceDelay(cfgOn, 0, 100), "paceInterval=0 = 0")
	require.Equal(t, time.Duration(0), computeSmoothPaceDelay(cfgOn, time.Second, 0), "regions=0 = 0")
	require.Equal(t, time.Duration(0), computeSmoothPaceDelay(cfgOn, time.Second, 1_000_000_000), "delay < 1µs = 0 (don't pace at sub-µs)")
	require.Equal(t, 10*time.Millisecond, computeSmoothPaceDelay(cfgOn, time.Second, 100), "1s/100 = 10ms")
	// Online-typical: 60s outer tick, 81.6k regions per store => 735µs per region.
	got := computeSmoothPaceDelay(cfgOn, 60*time.Second, 81600)
	require.InDelta(t, 735*time.Microsecond, got, float64(time.Microsecond), "60s/81600 ≈ 735µs ±1µs")
}

func TestPaceJitterIsWithinTenPercent(t *testing.T) {
	const d = 100 * time.Millisecond
	for range 200 {
		got := paceJitter(d)
		require.GreaterOrEqual(t, got, d-d/10, "jitter > -10%%")
		require.LessOrEqual(t, got, d+d/10, "jitter < +10%%")
	}
	require.Equal(t, time.Duration(0), paceJitter(0), "zero in zero out")
}

// makeTestSlot builds a streamSlot suitable for unit-testing runOneRound. We
// skip resolvePDLeader entirely — the slot is wired to a caller-supplied
// stream so send() exercises the same code path production uses without
// needing a real gRPC server.
func makeTestSlot(t *testing.T, ctx context.Context, cfg *config.Config, stream pdpb.PD_RegionHeartbeatClient) *streamSlot {
	t.Helper()
	return &streamSlot{
		storeID: 1,
		cfg:     cfg,
		ctx:     ctx,
		stream:  stream,
		kickCh:  make(chan struct{}, 1),
	}
}

func forceAllRegionsToStore1(rs *Regions) {
	for _, r := range rs.regions {
		if len(r.Region.Peers) == 0 {
			continue
		}
		r.Leader = r.Region.Peers[0]
		r.Leader.StoreId = 1
	}
}

func TestRunOneRoundSmoothPacesAcrossInterval(t *testing.T) {
	// 5 regions across a 100 ms paceInterval => per-region delay = 20 ms
	// total expected wall-clock = 4 inter-send sleeps × 20 ms = 80 ms (last send no sleep)
	// allow ±50% slack for jitter + scheduler noise.
	cfg := &config.Config{
		StoreCount:            2,
		RegionCount:           5,
		Replica:               1,
		SmoothHeartbeatPacing: true,
	}
	rs := new(Regions)
	rs.init(cfg)
	forceAllRegionsToStore1(rs)
	stream := &timingHeartbeatStream{}
	slot := makeTestSlot(t, context.Background(), cfg, stream)
	statsCh := make(chan storeRoundStat, 1)

	start := time.Now()
	runOneRound(context.Background(), cfg, rs, slot, 1, 100*time.Millisecond, statsCh)
	elapsed := time.Since(start)

	sends := stream.snapshot()
	require.Len(t, sends, 5, "all regions sent")
	require.GreaterOrEqual(t, elapsed, 50*time.Millisecond, "smooth pacing produces measurable wait (>= 50ms for 4 × 20ms gaps)")
	require.LessOrEqual(t, elapsed, 250*time.Millisecond, "should not balloon past ~2× expected even with jitter")

	// Inter-send gaps should be close to 20 ms each (with ±10%% jitter).
	for i := 1; i < len(sends); i++ {
		gap := sends[i].Sub(sends[i-1])
		require.GreaterOrEqual(t, gap, 15*time.Millisecond, "gap >= ~min jitter")
		require.LessOrEqual(t, gap, 60*time.Millisecond, "gap <= 60ms (jitter + scheduler slop)")
	}
}

func TestRunOneRoundBurstyWhenSmoothDisabled(t *testing.T) {
	// Same setup as the smooth test, but SmoothHeartbeatPacing=false: all sends should
	// complete in milliseconds (no pacing).
	cfg := &config.Config{
		StoreCount:            2,
		RegionCount:           5,
		Replica:               1,
		SmoothHeartbeatPacing: false, // legacy bursty
	}
	rs := new(Regions)
	rs.init(cfg)
	forceAllRegionsToStore1(rs)
	stream := &timingHeartbeatStream{}
	slot := makeTestSlot(t, context.Background(), cfg, stream)
	statsCh := make(chan storeRoundStat, 1)

	start := time.Now()
	runOneRound(context.Background(), cfg, rs, slot, 1, 100*time.Millisecond, statsCh)
	elapsed := time.Since(start)

	sends := stream.snapshot()
	require.Len(t, sends, 5)
	require.LessOrEqual(t, elapsed, 30*time.Millisecond, "legacy bursty mode must NOT pace; all 5 sends in a few ms")
}

func TestRunOneRoundRespectsContextCancel(t *testing.T) {
	// Smooth pacing must exit promptly when ctx is cancelled.
	cfg := &config.Config{
		StoreCount:            2,
		RegionCount:           1000,
		Replica:               1,
		SmoothHeartbeatPacing: true,
	}
	rs := new(Regions)
	rs.init(cfg)
	forceAllRegionsToStore1(rs)
	stream := &timingHeartbeatStream{}

	ctx, cancel := context.WithCancel(context.Background())
	slot := makeTestSlot(t, ctx, cfg, stream)
	statsCh := make(chan storeRoundStat, 1)
	// paceInterval = 10s across 1000 regions => 10ms per region, so a full natural run
	// would be 10s. Cancelling after 50ms should exit within ~100ms total.
	done := make(chan struct{})
	go func() {
		runOneRound(ctx, cfg, rs, slot, 1, 10*time.Second, statsCh)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("runOneRound did not exit within 500ms of ctx cancel")
	}

	sends := stream.snapshot()
	require.Less(t, len(sends), 50, "should have sent only a handful before cancel; got %d", len(sends))
}
