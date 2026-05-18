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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/time/rate"
)

const (
	DefaultNamespace      = "/pd-stress/filler"
	defaultKeyShards      = 1024
	defaultValueSizeBytes = 4096
)

// Config is the etcd backend filler configuration.
type Config struct {
	Namespace             string `toml:"namespace" json:"namespace"`
	TargetDBSizeBytes     int64  `toml:"target-db-size-bytes" json:"target-db-size-bytes"`
	KeyShards             int    `toml:"key-shards" json:"key-shards"`
	ValueSizeBytes        int    `toml:"value-size-bytes" json:"value-size-bytes"`
	WriteQPSDuringPrefill int    `toml:"write-qps-during-prefill" json:"write-qps-during-prefill"`
	SteadyPutQPS          int    `toml:"steady-put-qps" json:"steady-put-qps"`
	SteadyTxnQPS          int    `toml:"steady-txn-qps" json:"steady-txn-qps"`
}

// Entry is a deterministic filler key-value pair.
type Entry struct {
	Key   string
	Value string
}

type steadyClient interface {
	Put(context.Context, string, string) error
	TxnPut(context.Context, string, string) error
}

type etcdSteadyClient struct {
	cli *clientv3.Client
}

func (c etcdSteadyClient) Put(ctx context.Context, key string, value string) error {
	_, err := c.cli.Put(ctx, key, value)
	return err
}

func (c etcdSteadyClient) TxnPut(ctx context.Context, key string, value string) error {
	_, err := c.cli.Txn(ctx).Then(clientv3.OpPut(key, value)).Commit()
	return err
}

// Adjust fills defaults.
func (c *Config) Adjust() {
	if c.Namespace == "" {
		c.Namespace = DefaultNamespace
	}
	c.Namespace = normalizeNamespace(c.Namespace)
	if c.KeyShards == 0 {
		c.KeyShards = defaultKeyShards
	}
	if c.ValueSizeBytes == 0 {
		c.ValueSizeBytes = defaultValueSizeBytes
	}
}

// Validate validates filler config.
func (c *Config) Validate() error {
	c.Adjust()
	if err := ValidateCleanupNamespace(c.Namespace); err != nil {
		return err
	}
	if c.TargetDBSizeBytes < 0 {
		return errors.Errorf("target-db-size-bytes can not be negative")
	}
	if c.KeyShards <= 0 {
		return errors.Errorf("key-shards must be positive")
	}
	if c.ValueSizeBytes <= 0 {
		return errors.Errorf("value-size-bytes must be positive")
	}
	if c.WriteQPSDuringPrefill < 0 || c.SteadyPutQPS < 0 || c.SteadyTxnQPS < 0 {
		return errors.Errorf("filler qps values can not be negative")
	}
	return nil
}

// ValidateCleanupNamespace rejects namespaces that can delete PD metadata.
func ValidateCleanupNamespace(namespace string) error {
	namespace = normalizeNamespace(namespace)
	switch namespace {
	case "", "/", "/pd":
		return errors.Errorf("unsafe filler namespace %q", namespace)
	default:
		if strings.HasPrefix(namespace, "/pd/") {
			return errors.Errorf("unsafe filler namespace %q", namespace)
		}
		return nil
	}
}

// CleanupPrefix returns the normalized prefix used for cleanup.
func CleanupPrefix(namespace string) (string, error) {
	namespace = normalizeNamespace(namespace)
	if err := ValidateCleanupNamespace(namespace); err != nil {
		return "", err
	}
	return namespace, nil
}

// BuildKey builds a deterministic filler key.
func BuildKey(namespace string, shard, seq int64) string {
	return fmt.Sprintf("%s/%04d/%012d", normalizeNamespace(namespace), shard, seq)
}

// PlanPrefillEntries returns deterministic entries up to the target byte estimate.
func PlanPrefillEntries(cfg Config) []Entry {
	var entries []Entry
	_ = ForEachPrefillEntry(cfg, func(entry Entry) error {
		entries = append(entries, entry)
		return nil
	})
	return entries
}

// ForEachPrefillEntry calls fn for deterministic entries up to the target byte estimate.
func ForEachPrefillEntry(cfg Config, fn func(Entry) error) error {
	cfg.Adjust()
	value := BuildValue(cfg.ValueSizeBytes)
	var estimated int64
	for seq := int64(0); estimated < cfg.TargetDBSizeBytes; seq++ {
		entry := Entry{
			Key:   BuildKey(cfg.Namespace, seq%int64(cfg.KeyShards), seq),
			Value: value,
		}
		if err := fn(entry); err != nil {
			return err
		}
		estimated += int64(len(entry.Key) + len(entry.Value))
	}
	return nil
}

// EstimateEntriesBytes estimates key plus value bytes.
func EstimateEntriesBytes(entries []Entry) int64 {
	var total int64
	for _, entry := range entries {
		total += int64(len(entry.Key) + len(entry.Value))
	}
	return total
}

// BuildValue returns a deterministic value of exactly size bytes.
func BuildValue(size int) string {
	if size <= 0 {
		return ""
	}
	return strings.Repeat("x", size)
}

// RunPrefill writes filler keys until the target byte estimate is reached.
func RunPrefill(ctx context.Context, cli *clientv3.Client, cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	limiter := newLimiter(cfg.WriteQPSDuringPrefill)
	if err := ForEachPrefillEntry(cfg, func(entry Entry) error {
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return err
			}
		}
		if _, err := cli.Put(ctx, entry.Key, entry.Value); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if cfg.SteadyPutQPS <= 0 && cfg.SteadyTxnQPS <= 0 {
		return nil
	}
	return runSteady(ctx, cli, cfg)
}

// Cleanup deletes only keys under the configured namespace.
func Cleanup(ctx context.Context, cli *clientv3.Client, namespace string) error {
	prefix, err := CleanupPrefix(namespace)
	if err != nil {
		return err
	}
	_, err = cli.Delete(ctx, prefix, clientv3.WithPrefix())
	return err
}

func runSteady(ctx context.Context, cli *clientv3.Client, cfg Config) error {
	return runSteadyWithClient(ctx, etcdSteadyClient{cli: cli}, cfg)
}

func runSteadyWithClient(ctx context.Context, cli steadyClient, cfg Config) error {
	steadyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	if cfg.SteadyPutQPS > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- runSteadyWorker(steadyCtx, cfg, cfg.SteadyPutQPS, 0, cli.Put)
		}()
	}
	if cfg.SteadyTxnQPS > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- runSteadyWorker(steadyCtx, cfg, cfg.SteadyTxnQPS, 1, cli.TxnPut)
		}()
	}
	if cfg.SteadyPutQPS <= 0 && cfg.SteadyTxnQPS <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		cancel()
		wg.Wait()
		return ctx.Err()
	case err := <-errCh:
		cancel()
		wg.Wait()
		return err
	}
}

func runSteadyWorker(ctx context.Context, cfg Config, qps int, offset int64, write func(context.Context, string, string) error) error {
	limiter := newLimiter(qps)
	seq := time.Now().UnixNano()*2 + offset
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := limiter.Wait(ctx); err != nil {
			return err
		}
		key := BuildKey(cfg.Namespace, seq%int64(cfg.KeyShards), seq)
		if err := write(ctx, key, BuildValue(cfg.ValueSizeBytes)); err != nil {
			return err
		}
		seq += 2
	}
}

func newLimiter(qps int) *rate.Limiter {
	if qps <= 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(qps), qps)
}

func normalizeNamespace(namespace string) string {
	namespace = strings.TrimRight(namespace, "/")
	if namespace == "" {
		return ""
	}
	if !strings.HasPrefix(namespace, "/") {
		return "/" + namespace
	}
	return namespace
}
