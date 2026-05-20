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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/tools/pd-heartbeat-bench/config"
	"go.etcd.io/etcd/pkg/v3/report"
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

func TestHandleRegionHeartbeatSmoothPacesAcrossInterval(t *testing.T) {
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
	// Force every region's leader onto store 1 so handleRegionHeartbeat picks them all up.
	for _, r := range rs.regions {
		if len(r.Region.Peers) == 0 {
			continue
		}
		r.Leader = r.Region.Peers[0]
		r.Leader.StoreId = 1
	}
	stream := &timingHeartbeatStream{}
	rep := report.NewReport("%4.4f")
	done := rep.Stats()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	start := time.Now()
	go rs.handleRegionHeartbeat(context.Background(), cfg, wg, stream, 1, rep, 100*time.Millisecond)
	wg.Wait()
	close(rep.Results())
	<-done
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

func TestHandleRegionHeartbeatBurstyWhenSmoothDisabled(t *testing.T) {
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
	for _, r := range rs.regions {
		if len(r.Region.Peers) == 0 {
			continue
		}
		r.Leader = r.Region.Peers[0]
		r.Leader.StoreId = 1
	}
	stream := &timingHeartbeatStream{}
	rep := report.NewReport("%4.4f")
	done := rep.Stats()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	start := time.Now()
	// paceInterval is passed but should be ignored when smooth=false
	go rs.handleRegionHeartbeat(context.Background(), cfg, wg, stream, 1, rep, 100*time.Millisecond)
	wg.Wait()
	close(rep.Results())
	<-done
	elapsed := time.Since(start)

	sends := stream.snapshot()
	require.Len(t, sends, 5)
	require.LessOrEqual(t, elapsed, 30*time.Millisecond, "legacy bursty mode must NOT pace; all 5 sends in a few ms")
}

func TestHandleRegionHeartbeatRespectsContextCancel(t *testing.T) {
	// Smooth pacing must exit promptly when ctx is cancelled.
	cfg := &config.Config{
		StoreCount:            2,
		RegionCount:           1000,
		Replica:               1,
		SmoothHeartbeatPacing: true,
	}
	rs := new(Regions)
	rs.init(cfg)
	for _, r := range rs.regions {
		if len(r.Region.Peers) == 0 {
			continue
		}
		r.Leader = r.Region.Peers[0]
		r.Leader.StoreId = 1
	}
	stream := &timingHeartbeatStream{}
	rep := report.NewReport("%4.4f")
	done := rep.Stats()
	wg := &sync.WaitGroup{}
	wg.Add(1)

	ctx, cancel := context.WithCancel(context.Background())
	// paceInterval = 10s across 1000 regions => 10ms per region, so a full natural run
	// would be 10s. Cancelling after 50ms should exit within ~100ms total.
	go rs.handleRegionHeartbeat(ctx, cfg, wg, stream, 1, rep, 10*time.Second)
	time.Sleep(50 * time.Millisecond)
	cancel()
	waitCh := make(chan struct{})
	go func() { wg.Wait(); close(waitCh) }()
	select {
	case <-waitCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("handleRegionHeartbeat did not exit within 500ms of ctx cancel")
	}
	close(rep.Results())
	<-done

	sends := stream.snapshot()
	require.Less(t, len(sends), 50, "should have sent only a handful before cancel; got %d", len(sends))
}
