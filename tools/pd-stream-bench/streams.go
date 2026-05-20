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
	"crypto/tls"
	stderrors "errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/errors"
	meta_storagepb "github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/grpcutil"
	"github.com/tikv/pd/client/tlsutil"
	"github.com/tikv/pd/tools/pd-stream-bench/config"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	streamMetaStorageWatch    = "metastorage_watch"
	streamAcquireTokenBuckets = "acquire_token_buckets"
	streamEtcdWatch           = "etcd_watch"
	streamLeaseKeepalive      = "lease_keepalive"
)

type statusCounters struct {
	mu           sync.RWMutex
	activeByType map[string]int64
	errorByType  map[string]int64
	reconnByType map[string]int64
	connections  int64
}

func newStatusCounters() *statusCounters {
	return &statusCounters{
		activeByType: make(map[string]int64),
		errorByType:  make(map[string]int64),
		reconnByType: make(map[string]int64),
	}
}

func (s *statusCounters) incActive(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeByType[name]++
}

func (s *statusCounters) decActive(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeByType[name]--
}

func (s *statusCounters) incError(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errorByType[name]++
}

func (s *statusCounters) incReconnect(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconnByType[name]++
}

func (s *statusCounters) active(name string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.activeByType[name]
}

func (s *statusCounters) errors(name string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.errorByType[name]
}

func (s *statusCounters) reconnects(name string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reconnByType[name]
}

func (s *statusCounters) setConnections(count int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connections = count
}

func (s *statusCounters) metricsText() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.activeByType)+len(s.errorByType)+len(s.reconnByType))
	seen := make(map[string]struct{})
	for name := range s.activeByType {
		seen[name] = struct{}{}
		names = append(names, name)
	}
	for name := range s.errorByType {
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			names = append(names, name)
		}
	}
	for name := range s.reconnByType {
		if _, ok := seen[name]; !ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	text := "# HELP pd_stream_bench_active_streams active stream count\n# TYPE pd_stream_bench_active_streams gauge\n"
	for _, name := range names {
		text += fmt.Sprintf("pd_stream_bench_active_streams{type=%q} %d\n", name, s.activeByType[name])
	}
	text += "# HELP pd_stream_bench_errors_total stream error count\n# TYPE pd_stream_bench_errors_total counter\n"
	for _, name := range names {
		text += fmt.Sprintf("pd_stream_bench_errors_total{type=%q} %d\n", name, s.errorByType[name])
	}
	text += "# HELP pd_stream_bench_reconnects_total stream reconnect count\n# TYPE pd_stream_bench_reconnects_total counter\n"
	for _, name := range names {
		text += fmt.Sprintf("pd_stream_bench_reconnects_total{type=%q} %d\n", name, s.reconnByType[name])
	}
	text += fmt.Sprintf("pd_stream_bench_active_connections %d\n", s.connections)
	return text
}

func buildResourceManagerTokenRequest(clientID uint64, intervalMS int) *rmpb.TokenBucketsRequest {
	return &rmpb.TokenBucketsRequest{
		ClientUniqueId:        clientID,
		TargetRequestPeriodMs: uint64(intervalMS),
		Requests: []*rmpb.TokenBucketRequest{
			{
				ResourceGroupName: "default",
				Request: &rmpb.TokenBucketRequest_RuItems{
					RuItems: &rmpb.TokenBucketRequest_RequestRU{
						RequestRU: []*rmpb.RequestUnitItem{
							{Type: rmpb.RequestUnitType_RU, Value: 1},
						},
					},
				},
				ConsumptionSinceLastRequest: &rmpb.Consumption{RRU: 1},
			},
		},
	}
}

func computeIdleConnections(target int, activeStreams int) int {
	if target <= activeStreams {
		return 0
	}
	return target - activeStreams
}

func loadTLSConfig(cfg *config.Config) (*tls.Config, error) {
	if cfg.CAPath == "" {
		return nil, nil
	}
	return tlsutil.TLSConfig{
		CAPath:   cfg.CAPath,
		CertPath: cfg.CertPath,
		KeyPath:  cfg.KeyPath,
	}.ToTLSConfig()
}

// newGRPCConn dials a single PD endpoint. v2.1 (2026-05-20): the endpoint is now an
// explicit argument so workers can pin to a specific PD instance (round-robin via
// cfg.EndpointFor). Passing cfg.PDAddr (= first endpoint) preserves single-endpoint
// callers like checkCapability that don't care about distribution.
func newGRPCConn(ctx context.Context, cfg *config.Config, addr string) (*grpc.ClientConn, error) {
	tlsCfg, err := loadTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	return grpcutil.GetClientConn(ctx, addr, tlsCfg)
}

// newEtcdClient dials a single PD's etcd endpoint. v2.1 (2026-05-20): see newGRPCConn.
func newEtcdClient(cfg *config.Config, addr string) (*clientv3.Client, error) {
	tlsCfg, err := loadTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	return clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: 3 * time.Second,
		TLS:         tlsCfg,
	})
}

func startStatusServer(ctx context.Context, addr string, counters *statusCounters) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(counters.metricsText()))
	})
	server := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := server.ListenAndServe(); err != nil && !stderrors.Is(err, http.ErrServerClosed) {
			log.Error("status server failed", zap.Error(err))
		}
	}()
}

func runReconnectingWorker(ctx context.Context, name string, counters *statusCounters, run func(context.Context) error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := run(ctx); err != nil {
			counters.incError(name)
			counters.incReconnect(name)
			log.Error("stream worker disconnected", zap.String("type", name), zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func runMetaStorageWatch(ctx context.Context, cfg *config.Config, counters *statusCounters, workerID int, addr string) error {
	conn, err := newGRPCConn(ctx, cfg, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	stream, err := meta_storagepb.NewMetaStorageClient(conn).Watch(ctx, &meta_storagepb.WatchRequest{
		Header: &meta_storagepb.RequestHeader{Source: "pd-stream-bench"},
		Key:    []byte(fmt.Sprintf("/pd-stress/metastorage-watch/%d", workerID)),
	})
	if err != nil {
		return err
	}
	counters.incActive(streamMetaStorageWatch)
	defer counters.decActive(streamMetaStorageWatch)
	for {
		if _, err := stream.Recv(); err != nil {
			return err
		}
	}
}

func runAcquireTokenBuckets(ctx context.Context, cfg *config.Config, counters *statusCounters, workerID int, addr string) error {
	conn, err := newGRPCConn(ctx, cfg, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	stream, err := rmpb.NewResourceManagerClient(conn).AcquireTokenBuckets(ctx)
	if err != nil {
		return err
	}
	counters.incActive(streamAcquireTokenBuckets)
	defer counters.decActive(streamAcquireTokenBuckets)
	errCh := make(chan error, 1)
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				errCh <- err
				return
			}
		}
	}()
	ticker := time.NewTicker(time.Duration(cfg.StreamRequestIntervalMS) * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := stream.Send(buildResourceManagerTokenRequest(uint64(workerID), cfg.StreamRequestIntervalMS)); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return err
		case <-ticker.C:
		}
	}
}

func runEtcdWatch(ctx context.Context, cfg *config.Config, counters *statusCounters, workerID int, addr string) error {
	cli, err := newEtcdClient(cfg, addr)
	if err != nil {
		return err
	}
	defer cli.Close()
	watchCh := cli.Watch(ctx, fmt.Sprintf("/pd-stress/etcd-watch/%d", workerID))
	counters.incActive(streamEtcdWatch)
	defer counters.decActive(streamEtcdWatch)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resp, ok := <-watchCh:
			if !ok {
				return errors.New("etcd watch channel closed")
			}
			if err := resp.Err(); err != nil {
				return err
			}
		}
	}
}

func runLeaseKeepalive(ctx context.Context, cfg *config.Config, counters *statusCounters, workerID int, addr string) error {
	cli, err := newEtcdClient(cfg, addr)
	if err != nil {
		return err
	}
	defer cli.Close()
	lease, err := cli.Grant(ctx, 60)
	if err != nil {
		return err
	}
	keepaliveCh, err := cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		return err
	}
	counters.incActive(streamLeaseKeepalive)
	defer counters.decActive(streamLeaseKeepalive)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-keepaliveCh:
			if !ok {
				return errors.Errorf("lease keepalive channel closed for worker %d", workerID)
			}
		}
	}
}

// openIdleConnections opens `count` idle gRPC connections. v2.1 (2026-05-20): connections
// are round-robined across cfg.Endpoints so the fanout target (1000 conn) is split evenly
// across PD instances, not piled onto PD0.
func openIdleConnections(ctx context.Context, cfg *config.Config, counters *statusCounters, count int) ([]*grpc.ClientConn, error) {
	conns := make([]*grpc.ClientConn, 0, count)
	for i := range count {
		addr := cfg.EndpointFor(i)
		conn, err := newGRPCConn(ctx, cfg, addr)
		if err != nil {
			for _, conn := range conns {
				_ = conn.Close()
			}
			return nil, err
		}
		conns = append(conns, conn)
	}
	counters.setConnections(int64(count + cfg.MetaStorageWatchStreams + cfg.AcquireTokenBucketsStreams + cfg.EtcdWatchStreams + cfg.LeaseKeepaliveStreams))
	return conns, nil
}

func closeIdleConnections(conns []*grpc.ClientConn) {
	for _, conn := range conns {
		_ = conn.Close()
	}
}

// checkCapability validates each configured stream type against EVERY endpoint
// (v2.1 (2026-05-20)). Previously it only checked the first endpoint, which would mask
// a misconfigured follower until the stream worker tried to dial it for the first time
// and failed mid-window.
func checkCapability(ctx context.Context, cfg *config.Config) error {
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for _, addr := range cfg.Endpoints {
		if cfg.MetaStorageWatchStreams > 0 {
			conn, err := newGRPCConn(checkCtx, cfg, addr)
			if err != nil {
				return errors.Annotatef(err, "MetaStorage.Watch endpoint %s", addr)
			}
			stream, err := meta_storagepb.NewMetaStorageClient(conn).Watch(checkCtx, &meta_storagepb.WatchRequest{
				Header: &meta_storagepb.RequestHeader{Source: "pd-stream-bench-check"},
				Key:    []byte("/pd-stress/metastorage-watch/check"),
			})
			if err == nil {
				err = recvMetaStorageCheck(checkCtx, stream)
			}
			_ = conn.Close()
			if err != nil {
				return errors.Annotatef(err, "MetaStorage.Watch endpoint %s", addr)
			}
		}
		if cfg.AcquireTokenBucketsStreams > 0 {
			conn, err := newGRPCConn(checkCtx, cfg, addr)
			if err != nil {
				return errors.Annotatef(err, "ResourceManager.AcquireTokenBuckets endpoint %s", addr)
			}
			stream, err := rmpb.NewResourceManagerClient(conn).AcquireTokenBuckets(checkCtx)
			if err == nil {
				err = stream.Send(buildResourceManagerTokenRequest(0, cfg.StreamRequestIntervalMS))
			}
			if err == nil {
				err = recvAcquireTokenBucketsCheck(checkCtx, stream)
			}
			_ = conn.Close()
			if err != nil {
				return errors.Annotatef(err, "ResourceManager.AcquireTokenBuckets endpoint %s", addr)
			}
		}
		if cfg.EtcdWatchStreams > 0 {
			if err := checkEtcdWatchCapability(checkCtx, cfg, addr); err != nil {
				return errors.Annotatef(err, "etcd watch endpoint %s", addr)
			}
		}
		if cfg.LeaseKeepaliveStreams > 0 {
			cli, err := newEtcdClient(cfg, addr)
			if err != nil {
				return errors.Annotatef(err, "lease keepalive endpoint %s", addr)
			}
			lease, err := cli.Grant(checkCtx, 60)
			if err == nil {
				_, err = cli.KeepAlive(checkCtx, lease.ID)
			}
			_ = cli.Close()
			if err != nil {
				return errors.Annotatef(err, "lease keepalive endpoint %s", addr)
			}
		}
	}
	return nil
}

func checkEtcdWatchCapability(ctx context.Context, cfg *config.Config, addr string) error {
	cli, err := newEtcdClient(cfg, addr)
	if err != nil {
		return err
	}
	defer cli.Close()

	key := fmt.Sprintf("/pd-stress/etcd-watch/check-%d", time.Now().UnixNano())
	resp, err := cli.Get(ctx, key)
	if err != nil {
		return err
	}
	watchCh := cli.Watch(ctx, key, clientv3.WithRev(resp.Header.GetRevision()+1))
	if _, err := cli.Put(ctx, key, "check"); err != nil {
		return err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = cli.Delete(cleanupCtx, key)
	}()
	return waitForEtcdWatchEvent(ctx, watchCh)
}

func waitForEtcdWatchEvent(ctx context.Context, watchCh clientv3.WatchChan) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resp, ok := <-watchCh:
			if !ok {
				return errors.New("etcd watch channel closed")
			}
			if err := resp.Err(); err != nil {
				return err
			}
			if len(resp.Events) > 0 {
				return nil
			}
		}
	}
}

func recvMetaStorageCheck(ctx context.Context, stream meta_storagepb.MetaStorage_WatchClient) error {
	recvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if isExpectedCheckTimeout(err) {
			return nil
		}
		return err
	case <-recvCtx.Done():
		return nil
	}
}

func recvAcquireTokenBucketsCheck(ctx context.Context, stream rmpb.ResourceManager_AcquireTokenBucketsClient) error {
	recvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if isExpectedCheckTimeout(err) {
			return nil
		}
		return err
	case <-recvCtx.Done():
		return nil
	}
}

func isExpectedCheckTimeout(err error) bool {
	if err == nil {
		return false
	}
	code := status.Code(err)
	return code == codes.DeadlineExceeded || code == codes.Canceled
}

// startWorkers spawns each configured stream worker, round-robining its target endpoint
// across cfg.Endpoints. v2.1 (2026-05-20): the endpoint distribution is what spreads gRPC
// stream handler goroutines across all 3 PD instances so leader's goroutine count stays
// in the 5.3-5.8k target band instead of overshooting to 7k+.
func startWorkers(ctx context.Context, cfg *config.Config, counters *statusCounters) {
	for i := 0; i < cfg.MetaStorageWatchStreams; i++ {
		workerID := i
		addr := cfg.EndpointFor(workerID)
		go runReconnectingWorker(ctx, streamMetaStorageWatch, counters, func(ctx context.Context) error {
			return runMetaStorageWatch(ctx, cfg, counters, workerID, addr)
		})
	}
	for i := 0; i < cfg.AcquireTokenBucketsStreams; i++ {
		workerID := i
		addr := cfg.EndpointFor(workerID)
		go runReconnectingWorker(ctx, streamAcquireTokenBuckets, counters, func(ctx context.Context) error {
			return runAcquireTokenBuckets(ctx, cfg, counters, workerID, addr)
		})
	}
	for i := 0; i < cfg.EtcdWatchStreams; i++ {
		workerID := i
		addr := cfg.EndpointFor(workerID)
		go runReconnectingWorker(ctx, streamEtcdWatch, counters, func(ctx context.Context) error {
			return runEtcdWatch(ctx, cfg, counters, workerID, addr)
		})
	}
	for i := 0; i < cfg.LeaseKeepaliveStreams; i++ {
		workerID := i
		addr := cfg.EndpointFor(workerID)
		go runReconnectingWorker(ctx, streamLeaseKeepalive, counters, func(ctx context.Context) error {
			return runLeaseKeepalive(ctx, cfg, counters, workerID, addr)
		})
	}
}

func exit(code int) {
	os.Exit(code)
}
