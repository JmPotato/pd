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

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseNormalizesBarePDEndpoint(t *testing.T) {
	cfg := NewConfig()

	err := cfg.Parse([]string{"--pd-endpoints", "127.0.0.1:12379"})

	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:12379", cfg.PDAddr)
}

func TestParseKeepsSchemePDEndpoint(t *testing.T) {
	cfg := NewConfig()

	err := cfg.Parse([]string{"--pd-endpoints", "https://127.0.0.1:12379"})

	require.NoError(t, err)
	require.Equal(t, "https://127.0.0.1:12379", cfg.PDAddr)
}

func TestParseWithoutConfigUsesDefaults(t *testing.T) {
	cfg := NewConfig()

	err := cfg.Parse(nil)

	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:2379", cfg.PDAddr)
	require.Equal(t, defaultRound, cfg.Round)
	require.Equal(t, defaultStoreCount, cfg.StoreCount)
	require.Equal(t, defaultRegionCount, cfg.RegionCount)
	require.Equal(t, defaultReplica, cfg.Replica)
	require.Equal(t, float64(defaultReportRatio), cfg.ReportRatio)
	require.Equal(t, defaultInitialVersion, int(cfg.InitEpochVer))
}

func TestConfigParsesExtraPeerCountAndRole(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "heartbeat.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
store-count = 100
region-count = 8159500
replica = 3
extra-peer-count = 2280000
extra-peer-role = "learner"
`), 0o600))

	cfg := NewConfig()
	require.NoError(t, cfg.Parse([]string{"--config", path}))

	require.Equal(t, 2280000, cfg.ExtraPeerCount)
	require.Equal(t, 0.0, cfg.ExtraPeerRatio)
	require.Equal(t, "learner", cfg.ExtraPeerRole)
}

func TestConfigRejectsBothExtraPeerCountAndRatio(t *testing.T) {
	cfg := NewConfig()
	cfg.StoreCount = 10
	cfg.RegionCount = 100
	cfg.Replica = 3
	cfg.ExtraPeerCount = 1
	cfg.ExtraPeerRatio = 0.1

	require.Error(t, cfg.Validate())
}

func TestConfigDefaultsExtraPeerRoleToLearner(t *testing.T) {
	cfg := NewConfig()
	cfg.StoreCount = 10
	cfg.RegionCount = 100
	cfg.Replica = 3
	cfg.ExtraPeerCount = 1

	require.NoError(t, cfg.Validate())
	require.Equal(t, "learner", cfg.ExtraPeerRole)
}

func TestConfigParsesReportRateKnobs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "heartbeat.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
region-heartbeat-qps = 100
store-heartbeat-qps = 10
report-min-resolved-ts-qps = 99
report-buckets-streams = 100
report-buckets-interval-ms = 5000
`), 0o600))

	cfg := NewConfig()
	require.NoError(t, cfg.Parse([]string{"--config", path}))

	require.Equal(t, 100, cfg.RegionHeartbeatQPS)
	require.Equal(t, 10, cfg.StoreHeartbeatQPS)
	require.Equal(t, 99, cfg.ReportMinResolvedTSQPS)
	require.Equal(t, 100, cfg.ReportBucketsStreams)
	require.Equal(t, 5000, cfg.ReportBucketsIntervalMS)
}
