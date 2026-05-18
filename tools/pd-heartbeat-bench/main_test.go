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
	})

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
