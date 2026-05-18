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
