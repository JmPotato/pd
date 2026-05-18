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
	"bytes"
	"context"
	stderrors "errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTSOStreamConfigComputesTotalStreams(t *testing.T) {
	cfg := tsoStreamConfig{
		DirectStreamMode:    true,
		TiDBClientInstances: 32,
		TSOStreamsPerClient: 5,
		MaxTotalTSOStreams:  160,
	}

	require.NoError(t, cfg.Validate())
	require.Equal(t, 160, cfg.TotalStreams())
}

func TestTSOStreamConfigRejectsTooManyStreams(t *testing.T) {
	cfg := tsoStreamConfig{
		DirectStreamMode:    true,
		TiDBClientInstances: 33,
		TSOStreamsPerClient: 5,
		MaxTotalTSOStreams:  160,
	}

	require.Error(t, cfg.Validate())
}

func TestLogicalQPSLimiterDistributesAcrossStreams(t *testing.T) {
	limiter := newLogicalTSOLimiter(100, 4)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for range 4 {
		require.NoError(t, limiter.WaitN(ctx, 1))
	}

	require.Less(t, time.Since(start), 100*time.Millisecond)
}

func TestDirectStreamModeKeepsLegacyModeUntouched(t *testing.T) {
	cfg := tsoStreamConfig{DirectStreamMode: false, LogicalTSOQPS: 0}

	require.NoError(t, cfg.Validate())
	require.Equal(t, 0, cfg.TotalStreams())
}

func TestWriteDirectTSOIntervalStatsPrintsCountLine(t *testing.T) {
	stats := &directTSOStats{}
	stats.successCount.Add(100)
	stats.activeStreams.Store(160)
	stats.reconnects.Store(2)
	stats.errors.Store(1)
	var buf bytes.Buffer

	last := writeDirectTSOIntervalStats(&buf, stats, 40)

	require.Equal(t, int64(100), last)
	require.Contains(t, buf.String(), "count:60,")
	require.Contains(t, buf.String(), "active:160")
	require.Contains(t, buf.String(), "reconnect:2")
	require.Contains(t, buf.String(), "error:1")
}

func TestExpectedDirectTSOShutdownNeedsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.True(t, isExpectedDirectTSOShutdown(ctx, context.Canceled))
	require.False(t, isExpectedDirectTSOShutdown(context.Background(), context.Canceled))
	require.False(t, isExpectedDirectTSOShutdown(ctx, stderrors.New("network reset")))
}
