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

package cases

import (
	"context"
	"math/rand"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	pd "github.com/tikv/pd/client"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// DirectGRPCCase is a gRPC case that needs the raw kvproto client.
type DirectGRPCCase interface {
	Case
	unaryDirect(context.Context, pdpb.PDClient, healthpb.HealthClient) error
}

// DirectGRPCCreateFn is function type to create DirectGRPCCase.
type DirectGRPCCreateFn func() DirectGRPCCase

// DirectGRPCCaseFnMap is the map for all direct gRPC case creation functions.
var DirectGRPCCaseFnMap = map[string]DirectGRPCCreateFn{
	"Check":            newCheck(),
	"GetGCSafePoint":   newGetGCSafePoint(),
	"GetClusterInfo":   newGetClusterInfo(),
	"MemberList":       newGetMembersWithName("MemberList"),
	"GetMembers":       newGetMembers(),
	"AskBatchSplit":    newAskBatchSplit(),
	"ReportBatchSplit": newReportBatchSplit(),
}

type check struct {
	*baseCase
}

func newCheck() func() DirectGRPCCase {
	return func() DirectGRPCCase {
		return &check{baseCase: &baseCase{name: "Check", cfg: newConfig()}}
	}
}

func (*check) unaryDirect(ctx context.Context, _ pdpb.PDClient, cli healthpb.HealthClient) error {
	_, err := cli.Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

type getGCSafePoint struct {
	*baseCase
}

func newGetGCSafePoint() func() DirectGRPCCase {
	return func() DirectGRPCCase {
		return &getGCSafePoint{baseCase: &baseCase{name: "GetGCSafePoint", cfg: newConfig()}}
	}
}

func (*getGCSafePoint) unaryDirect(ctx context.Context, cli pdpb.PDClient, _ healthpb.HealthClient) error {
	_, err := cli.GetGCSafePoint(ctx, &pdpb.GetGCSafePointRequest{Header: directHeader()})
	return err
}

type getClusterInfo struct {
	*baseCase
}

func newGetClusterInfo() func() DirectGRPCCase {
	return func() DirectGRPCCase {
		return &getClusterInfo{baseCase: &baseCase{name: "GetClusterInfo", cfg: newConfig()}}
	}
}

func (*getClusterInfo) unaryDirect(ctx context.Context, cli pdpb.PDClient, _ healthpb.HealthClient) error {
	_, err := cli.GetClusterInfo(ctx, &pdpb.GetClusterInfoRequest{})
	return err
}

type getMembers struct {
	*baseCase
}

func newGetMembers() func() DirectGRPCCase {
	return newGetMembersWithName("GetMembers")
}

func newGetMembersWithName(name string) func() DirectGRPCCase {
	return func() DirectGRPCCase {
		return &getMembers{baseCase: &baseCase{name: name, cfg: newConfig()}}
	}
}

func (*getMembers) unaryDirect(ctx context.Context, cli pdpb.PDClient, _ healthpb.HealthClient) error {
	_, err := cli.GetMembers(ctx, &pdpb.GetMembersRequest{Header: directHeader()})
	return err
}

type getResourceGroup struct {
	*baseCase
}

func newGetResourceGroup() func() GRPCCase {
	return func() GRPCCase {
		return &getResourceGroup{baseCase: &baseCase{name: "GetResourceGroup", cfg: newConfig()}}
	}
}

func (*getResourceGroup) unary(ctx context.Context, cli pd.Client) error {
	_, err := cli.GetResourceGroup(ctx, "default")
	return err
}

type askBatchSplit struct {
	*baseCase
}

func newAskBatchSplit() func() DirectGRPCCase {
	return func() DirectGRPCCase {
		return &askBatchSplit{baseCase: &baseCase{name: "AskBatchSplit", cfg: newConfig()}}
	}
}

func (*askBatchSplit) unaryDirect(ctx context.Context, cli pdpb.PDClient, _ healthpb.HealthClient) error {
	req := buildAskBatchSplitRequest()
	resp, err := cli.GetRegion(ctx, &pdpb.GetRegionRequest{Header: directHeader(), RegionKey: req.GetRegion().GetStartKey()})
	if err == nil && resp.GetRegion() != nil {
		req.Region = resp.GetRegion()
	}
	_, err = cli.AskBatchSplit(ctx, req)
	return err
}

type reportBatchSplit struct {
	*baseCase
}

func newReportBatchSplit() func() DirectGRPCCase {
	return func() DirectGRPCCase {
		return &reportBatchSplit{baseCase: &baseCase{name: "ReportBatchSplit", cfg: newConfig()}}
	}
}

func (*reportBatchSplit) unaryDirect(ctx context.Context, cli pdpb.PDClient, _ healthpb.HealthClient) error {
	_, err := cli.ReportBatchSplit(ctx, buildReportBatchSplitRequest())
	return err
}

func directHeader() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{ClusterId: clusterID}
}

func buildAskBatchSplitRequest() *pdpb.AskBatchSplitRequest {
	return &pdpb.AskBatchSplitRequest{
		Header:     directHeader(),
		Region:     buildSyntheticSplitRegion(),
		SplitCount: 1,
	}
}

func buildReportBatchSplitRequest() *pdpb.ReportBatchSplitRequest {
	return &pdpb.ReportBatchSplitRequest{
		Header:  directHeader(),
		Regions: buildSyntheticBatchSplitRegions(),
	}
}

func buildSyntheticSplitRegion() *metapb.Region {
	regionCount := totalRegion
	if regionCount <= 0 {
		regionCount = 1
	}
	index := rand.Intn(regionCount)
	startKey, endKey := generateRegionKeyRange(1)
	return &metapb.Region{
		Id:          uint64(generateRegionKeyID(index) + int(time.Now().UnixNano()%1000)),
		StartKey:    startKey,
		EndKey:      endKey,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
}

func buildSyntheticBatchSplitRegions() []*metapb.Region {
	regionCount := totalRegion
	if regionCount < 2 {
		regionCount = 2
	}
	startIndex := rand.Intn(regionCount - 1)
	return []*metapb.Region{
		{
			Id:          uint64(generateRegionKeyID(startIndex) + int(time.Now().UnixNano()%1000)),
			StartKey:    generateRegionKey(startIndex),
			EndKey:      generateRegionKey(startIndex + 1),
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		},
		{
			Id:          uint64(generateRegionKeyID(startIndex+1) + int(time.Now().UnixNano()%1000) + 1),
			StartKey:    generateRegionKey(startIndex + 1),
			EndKey:      generateRegionKey(startIndex + 2),
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		},
	}
}
