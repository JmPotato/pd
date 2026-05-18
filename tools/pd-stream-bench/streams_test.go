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
	"testing"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestBuildResourceManagerTokenRequest(t *testing.T) {
	req := buildResourceManagerTokenRequest(42, 5000)

	require.Equal(t, uint64(42), req.GetClientUniqueId())
	require.Equal(t, uint64(5000), req.GetTargetRequestPeriodMs())
	require.Len(t, req.GetRequests(), 1)
	require.Equal(t, "default", req.GetRequests()[0].GetResourceGroupName())
	require.Equal(t, rmpb.RequestUnitType_RU, req.GetRequests()[0].GetRuItems().GetRequestRU()[0].GetType())
	require.Equal(t, float64(1), req.GetRequests()[0].GetRuItems().GetRequestRU()[0].GetValue())
}

func TestConnectionFanoutComputesIdleConnections(t *testing.T) {
	require.Equal(t, 10, computeIdleConnections(20, 10))
	require.Equal(t, 0, computeIdleConnections(5, 10))
}

func TestStatusCountersTrackActiveStreams(t *testing.T) {
	counters := newStatusCounters()

	counters.incActive("metastorage_watch")
	counters.incActive("metastorage_watch")
	counters.decActive("metastorage_watch")
	counters.incReconnect("metastorage_watch")
	counters.incError("metastorage_watch")

	require.Equal(t, int64(1), counters.active("metastorage_watch"))
	require.Equal(t, int64(1), counters.reconnects("metastorage_watch"))
	require.Equal(t, int64(1), counters.errors("metastorage_watch"))
}

func TestWaitForEtcdWatchEventRequiresRealEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchCh := make(chan clientv3.WatchResponse, 1)
	watchCh <- clientv3.WatchResponse{Events: []*clientv3.Event{{}}}

	require.NoError(t, waitForEtcdWatchEvent(ctx, watchCh))
}
