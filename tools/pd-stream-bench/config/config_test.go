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

func TestConfigParsesStreamTargets(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stream.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
pd = "127.0.0.1:2379"
metastorage-watch-streams = 330
acquire-token-buckets-streams = 145
etcd-watch-streams = 40
lease-keepalive-streams = 40
connection-fanout-target = 1000
stream-request-interval-ms = 5000
`), 0o600))

	cfg := NewConfig()
	require.NoError(t, cfg.Parse([]string{"--config", path}))

	require.Equal(t, "http://127.0.0.1:2379", cfg.PDAddr)
	require.Equal(t, 330, cfg.MetaStorageWatchStreams)
	require.Equal(t, 145, cfg.AcquireTokenBucketsStreams)
	require.Equal(t, 40, cfg.EtcdWatchStreams)
	require.Equal(t, 40, cfg.LeaseKeepaliveStreams)
	require.Equal(t, 1000, cfg.ConnectionFanoutTarget)
	require.Equal(t, 5000, cfg.StreamRequestIntervalMS)
}

func TestConfigRejectsNegativeStreams(t *testing.T) {
	cfg := NewConfig()
	cfg.MetaStorageWatchStreams = -1

	require.Error(t, cfg.Validate())
}

// v2.1 (2026-05-20): single-endpoint toml stays at length-1 Endpoints; backward compat.
func TestConfigSingleEndpointPopulatesEndpointsLen1(t *testing.T) {
	cfg := NewConfig()
	require.NoError(t, cfg.Parse([]string{"--pd", "https://pd0:2379"}))

	require.Equal(t, "https://pd0:2379", cfg.PDAddr)
	require.Equal(t, []string{"https://pd0:2379"}, cfg.Endpoints)
	require.Equal(t, "https://pd0:2379", cfg.EndpointFor(0))
	require.Equal(t, "https://pd0:2379", cfg.EndpointFor(7), "single-endpoint round-robin always returns the same addr")
}

// v2.1 (2026-05-20): comma-separated `pd =` in toml splits into Endpoints; PDAddr is
// canonicalized to the first. Round-robin via EndpointFor distributes the workerIDs.
func TestConfigMultiEndpointFromTomlSplitsAndRoundRobins(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stream.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
pd = "https://pd0:2379,https://pd1:2379,https://pd2:2379"
metastorage-watch-streams = 6
`), 0o600))

	cfg := NewConfig()
	require.NoError(t, cfg.Parse([]string{"--config", path}))

	require.Equal(t, "https://pd0:2379", cfg.PDAddr, "PDAddr canonicalizes to first endpoint")
	require.Equal(t, []string{
		"https://pd0:2379",
		"https://pd1:2379",
		"https://pd2:2379",
	}, cfg.Endpoints)

	// workerID round-robin
	require.Equal(t, "https://pd0:2379", cfg.EndpointFor(0))
	require.Equal(t, "https://pd1:2379", cfg.EndpointFor(1))
	require.Equal(t, "https://pd2:2379", cfg.EndpointFor(2))
	require.Equal(t, "https://pd0:2379", cfg.EndpointFor(3))
	require.Equal(t, "https://pd0:2379", cfg.EndpointFor(-3), "negative ID is mirrored to keep distribution stable")
}

// v2.1 (2026-05-20): whitespace and trailing comma in `pd =` are tolerated. Empty parts
// are dropped so an accidental trailing comma doesn't leave an empty endpoint behind.
func TestConfigMultiEndpointTrimsAndDropsEmpty(t *testing.T) {
	cfg := NewConfig()
	require.NoError(t, cfg.Parse([]string{"--pd", "  https://pd0:2379 , https://pd1:2379 , "}))

	require.Equal(t, []string{
		"https://pd0:2379",
		"https://pd1:2379",
	}, cfg.Endpoints)
}

// v2.1 (2026-05-20): an empty PDAddr is rejected by Validate so misconfigured topologies
// surface early instead of dying at first dial.
func TestConfigRejectsEmptyPDAddr(t *testing.T) {
	cfg := NewConfig()

	require.Error(t, cfg.Parse([]string{"--pd", ""}))
}
