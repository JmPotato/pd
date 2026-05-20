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
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/tools/pd-stream-bench/config"
	"go.uber.org/zap"
)

func main() {
	cfg := config.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	default:
		log.Fatal("parse config failed", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-sc
		cancel()
	}()

	if cfg.CheckOnly {
		if err := checkCapability(ctx, cfg); err != nil {
			log.Fatal("stream capability check failed", zap.Error(err))
		}
		log.Info("stream capability check passed")
		return
	}

	counters := newStatusCounters()
	startStatusServer(ctx, cfg.StatusAddr, counters)
	startWorkers(ctx, cfg, counters)
	idleCount := computeIdleConnections(cfg.ConnectionFanoutTarget,
		cfg.MetaStorageWatchStreams+cfg.AcquireTokenBucketsStreams+cfg.EtcdWatchStreams+cfg.LeaseKeepaliveStreams)
	idleConns, err := openIdleConnections(ctx, cfg, counters, idleCount)
	if err != nil {
		log.Fatal("open idle connection fanout failed", zap.Error(err))
	}
	defer closeIdleConnections(idleConns)
	log.Info("pd-stream-bench started",
		zap.Strings("endpoints", cfg.Endpoints),
		zap.Int("metastorage-watch-streams", cfg.MetaStorageWatchStreams),
		zap.Int("acquire-token-buckets-streams", cfg.AcquireTokenBucketsStreams),
		zap.Int("etcd-watch-streams", cfg.EtcdWatchStreams),
		zap.Int("lease-keepalive-streams", cfg.LeaseKeepaliveStreams),
		zap.Int("idle-connections", idleCount))
	<-ctx.Done()
}
