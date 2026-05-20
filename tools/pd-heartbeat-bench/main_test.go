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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/tools/pd-heartbeat-bench/config"
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
