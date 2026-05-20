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

// v2.1 (2026-05-20): the new content-fidelity / hot-region / bucket-gate knobs round-trip
// through toml.
func TestConfigParsesV21ContentFidelityKnobs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "heartbeat.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
region-approximate-size-mib = 96
region-approximate-keys = 1000000
hot-region-ratio = 0.05
hot-write-bytes-per-region = 16000000
hot-read-bytes-per-region = 2000000
hot-write-keys-per-region = 50000
hot-read-keys-per-region = 10000
store-capacity-gib = 32768
buckets-after-first-heartbeat-round = false
`), 0o600))

	cfg := NewConfig()
	require.NoError(t, cfg.Parse([]string{"--config", path}))

	require.Equal(t, 96, cfg.RegionApproximateSizeMiB)
	require.Equal(t, 1000000, cfg.RegionApproximateKeys)
	require.InDelta(t, 0.05, cfg.HotRegionRatio, 1e-9)
	require.Equal(t, uint64(16000000), cfg.HotWriteBytesPerRegion)
	require.Equal(t, uint64(2000000), cfg.HotReadBytesPerRegion)
	require.Equal(t, uint64(50000), cfg.HotWriteKeysPerRegion)
	require.Equal(t, uint64(10000), cfg.HotReadKeysPerRegion)
	require.Equal(t, 32768, cfg.StoreCapacityGiB)
	require.False(t, cfg.BucketsAfterFirstHeartbeatRound)
}

// v2.1 (2026-05-20): when knobs are absent from toml, defaults preserve legacy behaviour
// (zero content fidelity, gate ON for safety).
func TestConfigV21KnobDefaults(t *testing.T) {
	cfg := NewConfig()
	require.NoError(t, cfg.Parse(nil))

	require.Equal(t, 0, cfg.RegionApproximateSizeMiB)
	require.Equal(t, 0, cfg.RegionApproximateKeys)
	require.Equal(t, 0.0, cfg.HotRegionRatio)
	require.Equal(t, uint64(0), cfg.HotWriteBytesPerRegion)
	require.Equal(t, 0, cfg.StoreCapacityGiB)
	require.True(t, cfg.BucketsAfterFirstHeartbeatRound, "default must gate bucket workers")
	// v2.3 default: smooth pacing OFF (preserves v2.2 bursty behaviour).
	require.False(t, cfg.SmoothHeartbeatPacing, "default must keep legacy bursty mode")
}

// v2.3 (2026-05-20): smooth-heartbeat-pacing toml knob round-trips.
func TestConfigParsesV23SmoothHeartbeatPacing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "heartbeat.toml")
	require.NoError(t, os.WriteFile(path, []byte(`smooth-heartbeat-pacing = true
`), 0o600))

	cfg := NewConfig()
	require.NoError(t, cfg.Parse([]string{"--config", path}))
	require.True(t, cfg.SmoothHeartbeatPacing)
}

func TestConfigRejectsHotRegionRatioOutOfRange(t *testing.T) {
	cfg := NewConfig()
	cfg.StoreCount = 10
	cfg.RegionCount = 100
	cfg.Replica = 3
	cfg.HotRegionRatio = 1.5

	require.Error(t, cfg.Validate())
}

func TestConfigRejectsNegativeContentKnobs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(c *Config)
	}{
		{"negative size", func(c *Config) { c.RegionApproximateSizeMiB = -1 }},
		{"negative keys", func(c *Config) { c.RegionApproximateKeys = -1 }},
		{"negative capacity", func(c *Config) { c.StoreCapacityGiB = -1 }},
		{"negative hot ratio", func(c *Config) { c.HotRegionRatio = -0.1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewConfig()
			cfg.StoreCount = 10
			cfg.RegionCount = 100
			cfg.Replica = 3
			tc.apply(cfg)
			require.Error(t, cfg.Validate())
		})
	}
}
