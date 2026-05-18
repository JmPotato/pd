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

package filler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFillerRejectsUnsafeNamespace(t *testing.T) {
	for _, namespace := range []string{"", "/", "/pd", "/pd/"} {
		require.Error(t, ValidateCleanupNamespace(namespace), namespace)
	}
	require.NoError(t, ValidateCleanupNamespace("/pd-stress/filler"))
}

func TestFillerBuildsDeterministicKeys(t *testing.T) {
	require.Equal(t, "/pd-stress/filler/0007/000000000042", BuildKey("/pd-stress/filler", 7, 42))
	require.Equal(t, "/pd-stress/filler/0007/000000000042", BuildKey("/pd-stress/filler/", 7, 42))
}

func TestFillerStopsAtTargetBytes(t *testing.T) {
	cfg := Config{
		Namespace:         "/pd-stress/filler",
		TargetDBSizeBytes: 250,
		KeyShards:         4,
		ValueSizeBytes:    50,
	}

	entries := PlanPrefillEntries(cfg)

	require.NotEmpty(t, entries)
	require.GreaterOrEqual(t, EstimateEntriesBytes(entries), int64(250))
	require.Less(t, EstimateEntriesBytes(entries[:len(entries)-1]), int64(250))
}

func TestForEachPrefillEntryStreamsDeterministicEntries(t *testing.T) {
	cfg := Config{
		Namespace:         "/pd-stress/filler",
		TargetDBSizeBytes: 130,
		KeyShards:         2,
		ValueSizeBytes:    10,
	}

	var entries []Entry
	err := ForEachPrefillEntry(cfg, func(entry Entry) error {
		entries = append(entries, entry)
		return nil
	})

	require.NoError(t, err)
	require.NotEmpty(t, entries)
	require.Equal(t, "/pd-stress/filler/0000/000000000000", entries[0].Key)
	require.Equal(t, "/pd-stress/filler/0001/000000000001", entries[1].Key)
	require.GreaterOrEqual(t, EstimateEntriesBytes(entries), int64(130))
}

func TestCleanupUsesPrefixDelete(t *testing.T) {
	namespace, err := CleanupPrefix("/pd-stress/filler/")

	require.NoError(t, err)
	require.Equal(t, "/pd-stress/filler", namespace)
}

func TestRunSteadyStartsPutAndTxnWorkersIndependently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newRecordingSteadyClient()
	done := make(chan error, 1)

	go func() {
		done <- runSteadyWithClient(ctx, client, Config{
			Namespace:      "/pd-stress/filler",
			KeyShards:      4,
			ValueSizeBytes: 1,
			SteadyPutQPS:   1000,
			SteadyTxnQPS:   1000,
		})
	}()

	require.Eventually(t, func() bool { return client.putStarted() }, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return client.txnStarted() }, time.Second, 10*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

type recordingSteadyClient struct {
	mu  sync.Mutex
	put bool
	txn bool
}

func newRecordingSteadyClient() *recordingSteadyClient {
	return &recordingSteadyClient{}
}

func (c *recordingSteadyClient) putStarted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.put
}

func (c *recordingSteadyClient) txnStarted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.txn
}

func (c *recordingSteadyClient) Put(ctx context.Context, _, _ string) error {
	c.mu.Lock()
	c.put = true
	c.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func (c *recordingSteadyClient) TxnPut(ctx context.Context, _, _ string) error {
	c.mu.Lock()
	c.txn = true
	c.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}
