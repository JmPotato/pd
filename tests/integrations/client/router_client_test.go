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

package client_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

func TestRouterClientGetRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, cleanup, err := server.NewTestServer(re, assertutil.CheckerWithNilAssert(re))
	re.NoError(err)
	defer cleanup()

	grpcPDClient := testutil.MustNewGrpcClient(re, srv.GetAddr())
	server.MustWaitLeader(re, []*server.Server{srv})
	bootstrapServer(re, newHeader(), grpcPDClient)
	srv.GetRaftCluster().GetOpts().(*config.PersistOptions).SetRegionBucketEnabled(true)

	client := setupCli(ctx, re, srv.GetEndpoints(), pd.WithEnableRouterClient(true))
	defer client.Close()

	regionHeartbeat, err := grpcPDClient.RegionHeartbeat(ctx)
	re.NoError(err)
	defer regionHeartbeat.CloseSend()
	reportBucket, err := grpcPDClient.ReportBuckets(ctx)
	re.NoError(err)
	defer reportBucket.CloseSend()

	regionID := regionIDAllocator.alloc()
	region := &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers:    peers,
	}
	re.NoError(regionHeartbeat.Send(&pdpb.RegionHeartbeatRequest{
		Header: newHeader(),
		Region: region,
		Leader: peers[0],
	}))
	region2 := &metapb.Region{
		Id: regionIDAllocator.alloc(),
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		StartKey: []byte("z"),
		EndKey:   []byte("zz"),
		Peers:    peers,
	}
	re.NoError(regionHeartbeat.Send(&pdpb.RegionHeartbeatRequest{
		Header: newHeader(),
		Region: region2,
		Leader: peers[0],
	}))

	testutil.Eventually(re, func() bool {
		r, err := client.GetRegion(ctx, []byte("b"))
		re.NoError(err)
		return r != nil &&
			region.GetId() == r.Meta.GetId() &&
			peers[0].GetId() == r.Leader.GetId() &&
			r.Buckets == nil
	})

	buckets := &metapb.Buckets{
		RegionId:   regionID,
		Version:    1,
		Keys:       [][]byte{[]byte("a"), []byte("z")},
		PeriodInMs: 2000,
		Stats: &metapb.BucketStats{
			ReadBytes:  []uint64{1},
			ReadKeys:   []uint64{1},
			ReadQps:    []uint64{1},
			WriteBytes: []uint64{1},
			WriteKeys:  []uint64{1},
			WriteQps:   []uint64{1},
		},
	}
	re.NoError(reportBucket.Send(&pdpb.ReportBucketsRequest{
		Header:  newHeader(),
		Buckets: buckets,
	}))
	testutil.Eventually(re, func() bool {
		r, err := client.GetRegion(ctx, []byte("b"), pd.WithBuckets())
		re.NoError(err)
		return r != nil && r.Buckets != nil && r.Buckets.GetRegionId() == regionID
	})

	testutil.Eventually(re, func() bool {
		prev, err := client.GetPrevRegion(ctx, []byte("z"))
		re.NoError(err)
		return prev != nil && prev.Meta.GetId() == regionID
	})

	byID, err := client.GetRegionByID(ctx, regionID)
	re.NoError(err)
	re.NotNil(byID)
	re.Equal(regionID, byID.Meta.GetId())

	missing, err := client.GetRegionByID(ctx, regionIDAllocator.alloc())
	re.NoError(err)
	re.Nil(missing)

	// Verify QueryRegion returns deep-copied results: mutating one result must not
	// affect another result backed by the same RegionResponse.
	r1, err := client.GetRegion(ctx, []byte("b"))
	re.NoError(err)
	r2, err := client.GetRegion(ctx, []byte("b"))
	re.NoError(err)
	re.NotNil(r1)
	re.NotNil(r2)
	r1.Meta.Id = 0
	re.Equal(regionID, r2.Meta.GetId())

	srv.GetRaftCluster().HandleRegionHeartbeat(core.NewRegionInfo(region, peers[0], core.SetBuckets(nil)))
}

func TestRouterClientFallbackOnUnimplementedStream(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, cleanup, err := server.NewTestServer(re, assertutil.CheckerWithNilAssert(re))
	re.NoError(err)
	defer cleanup()

	grpcPDClient := testutil.MustNewGrpcClient(re, srv.GetAddr())
	server.MustWaitLeader(re, []*server.Server{srv})
	bootstrapServer(re, newHeader(), grpcPDClient)

	regionHeartbeat, err := grpcPDClient.RegionHeartbeat(ctx)
	re.NoError(err)
	defer regionHeartbeat.CloseSend()

	regionID := regionIDAllocator.alloc()
	region := &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers:    peers,
	}
	re.NoError(regionHeartbeat.Send(&pdpb.RegionHeartbeatRequest{
		Header: newHeader(),
		Region: region,
		Leader: peers[0],
	}))

	var queryRegionStreams atomic.Int32
	streamInterceptor := func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		if method == "/pdpb.PD/QueryRegion" {
			queryRegionStreams.Add(1)
			return nil, status.Error(codes.Unimplemented, "old server")
		}
		return streamer(ctx, desc, cc, method, opts...)
	}

	client := setupCli(ctx, re, srv.GetEndpoints(),
		pd.WithEnableRouterClient(true),
		pd.WithGRPCDialOptions(grpc.WithStreamInterceptor(streamInterceptor)),
	)
	defer client.Close()

	testutil.Eventually(re, func() bool {
		r, err := client.GetRegion(ctx, []byte("b"))
		re.NoError(err)
		return r != nil && r.Meta.GetId() == regionID
	})
	re.Positive(queryRegionStreams.Load())
}
