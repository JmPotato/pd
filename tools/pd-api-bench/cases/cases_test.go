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
	"testing"

	"github.com/stretchr/testify/require"
	pdHttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/codec"
)

func TestGRPCCaseMapIncludesBatchScanRegions(t *testing.T) {
	create, ok := GRPCCaseFnMap["BatchScanRegions"]
	require.True(t, ok)
	require.Equal(t, "BatchScanRegions", create().getName())
}

func TestSetKeyFormatTableMatchesHeartbeatBenchKeys(t *testing.T) {
	restoreKeyFormat(t)
	require.NoError(t, SetKeyFormat("table"))

	require.Equal(t, codec.GenerateTableKey(42), generateKeyForSimulator(42))
}

func TestSetKeyFormatRejectsUnknownFormat(t *testing.T) {
	restoreKeyFormat(t)
	require.Error(t, SetKeyFormat("unknown"))
}

func TestSetKeyFormatEmptyUsesRaw(t *testing.T) {
	restoreKeyFormat(t)
	require.NoError(t, SetKeyFormat("table"))
	require.NoError(t, SetKeyFormat(""))

	require.Equal(t, []byte("0000000042"), generateKeyForSimulator(42)[:10])
}

func TestGenerateRegionKeyIDUsesTableRegionIndexDirectly(t *testing.T) {
	restoreKeyFormat(t)
	require.NoError(t, SetKeyFormat("table"))

	require.Equal(t, 42, generateRegionKeyID(42))
}

func TestGenerateRegionKeyIDKeepsRawSpacing(t *testing.T) {
	restoreKeyFormat(t)
	require.NoError(t, SetKeyFormat("raw"))

	require.Equal(t, 169, generateRegionKeyID(42))
}

func TestGenerateRegionKeyRangeSupportsSmallClusters(t *testing.T) {
	restoreKeyFormat(t)
	require.NoError(t, SetKeyFormat("table"))

	setTotalRegionForTest(t, 1)

	startKey, endKey := generateRegionKeyRange(128)
	require.Equal(t, codec.GenerateTableKey(0), startKey)
	require.Equal(t, codec.GenerateTableKey(128), endKey)
}

func TestGenerateRegionKeyRangeKeepsRawSpacing(t *testing.T) {
	restoreKeyFormat(t)
	require.NoError(t, SetKeyFormat("raw"))

	setTotalRegionForTest(t, 1000)

	startKey, endKey := generateRegionKeyRange(1000)
	require.Equal(t, []byte("0000000001"), startKey[:10])
	require.Equal(t, []byte("0000004001"), endKey[:10])
}

func TestLoadTotalRegionUsesCountOnlyStatsRequest(t *testing.T) {
	setTotalRegionForTest(t, 0)

	httpCli := &recordingRegionStatusClient{
		resp: &pdHttp.RegionStats{Count: 8200000},
	}

	require.NoError(t, loadTotalRegion(context.Background(), httpCli))
	require.True(t, httpCli.onlyCount)
	require.Equal(t, 8200000, totalRegion)
}

func TestBatchScanRegionsUsesConfiguredBatchSize(t *testing.T) {
	c := newBatchScanRegions()().(*batchScanRegions)

	applyCaseConfig(c, &Config{BatchSize: 10})

	require.Equal(t, 10, c.regionSample)
}

func TestScanRegionsUsesConfiguredLimit(t *testing.T) {
	c := newScanRegions()().(*scanRegions)

	applyCaseConfig(c, &Config{Limit: 100})

	require.Equal(t, 100, c.regionSample)
}

func TestDeleteAndTxnCaseNames(t *testing.T) {
	require.Equal(t, "Delete", newDeleteKV()().getName())
	require.Equal(t, "Txn", newTxnKV()().getName())
}

func TestDirectCaseMapIncludesClinicTopMethods(t *testing.T) {
	for _, name := range []string{
		"Check",
		"GetGCSafePoint",
		"GetClusterInfo",
		"MemberList",
		"GetMembers",
		"AskBatchSplit",
		"ReportBatchSplit",
	} {
		create, ok := DirectGRPCCaseFnMap[name]
		require.Truef(t, ok, "%s should be registered", name)
		require.Equal(t, name, create().getName())
	}
	require.Contains(t, GRPCCaseFnMap, "GetAllStores")
	require.Contains(t, GRPCCaseFnMap, "GetResourceGroup")
}

func TestGetMembersAliases(t *testing.T) {
	require.IsType(t, newGetMembers()(), DirectGRPCCaseFnMap["MemberList"]())
	require.IsType(t, newGetMembers()(), DirectGRPCCaseFnMap["GetMembers"]())
}

func TestGetAllStoresAliasKeepsConfiguredCaseName(t *testing.T) {
	require.Equal(t, "GetAllStores", GRPCCaseFnMap["GetAllStores"]().getName())
	require.Equal(t, "GetStores", GRPCCaseFnMap["GetStores"]().getName())
}

func TestAskBatchSplitBuildsRegionRequest(t *testing.T) {
	restoreKeyFormat(t)
	require.NoError(t, SetKeyFormat("table"))
	setTotalRegionForTest(t, 100)

	req := buildAskBatchSplitRequest()

	require.NotNil(t, req.GetRegion())
	require.Equal(t, uint32(1), req.GetSplitCount())
	require.NotEmpty(t, req.GetRegion().GetStartKey())
	require.NotEmpty(t, req.GetRegion().GetEndKey())
}

func TestReportBatchSplitBuildsReportRequest(t *testing.T) {
	restoreKeyFormat(t)
	require.NoError(t, SetKeyFormat("table"))
	setTotalRegionForTest(t, 100)

	req := buildReportBatchSplitRequest()

	require.Len(t, req.GetRegions(), 2)
	left := req.GetRegions()[0]
	right := req.GetRegions()[1]
	require.NotEmpty(t, left.GetStartKey())
	require.NotEmpty(t, left.GetEndKey())
	require.Equal(t, left.GetEndKey(), right.GetStartKey())
}

func restoreKeyFormat(t *testing.T) {
	t.Helper()
	oldKeyFormat := keyFormat
	t.Cleanup(func() {
		keyFormat = oldKeyFormat
	})
}

func setTotalRegionForTest(t *testing.T, value int) {
	t.Helper()
	oldTotalRegion := totalRegion
	totalRegion = value
	t.Cleanup(func() {
		totalRegion = oldTotalRegion
	})
}

type recordingRegionStatusClient struct {
	onlyCount bool
	resp      *pdHttp.RegionStats
}

func (c *recordingRegionStatusClient) GetRegionStatusByKeyRange(
	_ context.Context,
	_ *pdHttp.KeyRange,
	onlyCount bool,
) (*pdHttp.RegionStats, error) {
	c.onlyCount = onlyCount
	return c.resp, nil
}
