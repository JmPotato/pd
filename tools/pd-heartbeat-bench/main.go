// Copyright 2019 TiKV Project Authors.
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
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/docker/go-units"
	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/pflag"
	"github.com/tikv/pd/client/grpcutil"
	pdHttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/tlsutil"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/tools/pd-heartbeat-bench/config"
	"github.com/tikv/pd/tools/pd-heartbeat-bench/metrics"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	bytesUnit            = 128
	keysUint             = 8
	queryUnit            = 8
	hotByteUnit          = 16 * units.KiB
	hotKeysUint          = 256
	hotQueryUnit         = 256
	regionReportInterval = 60 // 60s
	storeReportInterval  = 10 // 10s
	legacyCapacity       = 4 * units.TiB
)

// v3.1 (2026-05-21): client-perceived reconnect + error counters. Exported on
// the gin /metrics endpoint (default prometheus registry), so the bench-side
// ops_tail_errs_sidecar.py can join them with operator events at collect time.
// store_id label kept low-cardinality (100 stores), well within Prometheus
// scrape budget.
var (
	heartbeatStreamReconnects = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd_heartbeat_bench",
			Name:      "stream_reconnects_total",
			Help:      "RegionHeartbeat stream reconnect count, per store.",
		}, []string{"store_id"})
	heartbeatStreamErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd_heartbeat_bench",
			Name:      "stream_errors_total",
			Help:      "RegionHeartbeat stream send/reconnect error count, per store and phase (send_initial, send_retry, reconnect_failed).",
		}, []string{"store_id", "phase"})
	heartbeatFullResendRounds = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "pd_heartbeat_bench",
			Name:      "full_resend_rounds_total",
			Help:      "Full heartbeat resend rounds triggered by stream reconnect (TiKV-style burst).",
		})
)

func init() {
	prometheus.MustRegister(heartbeatStreamReconnects)
	prometheus.MustRegister(heartbeatStreamErrors)
	prometheus.MustRegister(heartbeatFullResendRounds)
}

// computeStoreCapacity returns the per-store capacity bytes. If cfg.StoreCapacityGiB > 0 it is
// honoured; otherwise we auto-scale to 2x the per-store live data (RegionCount * Replica *
// RegionApproximateSize / StoreCount), with the legacy 4 TiB as the floor so we don't regress
// the small-cluster default. Required because lifting RegionApproximateSizeMiB to TiKV-realistic
// values (96 MiB) makes 8.16M regions overflow the legacy cap and underflow store.Available.
func computeStoreCapacity(cfg *config.Config) uint64 {
	if cfg.StoreCapacityGiB > 0 {
		return uint64(cfg.StoreCapacityGiB) * uint64(units.GiB)
	}
	if cfg.StoreCount <= 0 || cfg.RegionCount <= 0 || cfg.Replica <= 0 {
		return legacyCapacity
	}
	sizePerRegion := uint64(cfg.RegionApproximateSizeMiB) * uint64(units.MiB)
	if sizePerRegion == 0 {
		sizePerRegion = bytesUnit
	}
	perStoreLive := uint64(cfg.RegionCount) * uint64(cfg.Replica) * sizePerRegion / uint64(cfg.StoreCount)
	auto := 2 * perStoreLive
	if auto < legacyCapacity {
		return legacyCapacity
	}
	return auto
}

var (
	clusterID  uint64
	maxVersion uint64 = 1
)

type reportBucketsClient interface {
	Send(*pdpb.ReportBucketsRequest) error
	CloseAndRecv() (*pdpb.ReportBucketsResponse, error)
}

type reportBucketsStreamFactory func(context.Context) (reportBucketsClient, error)

type bucketReporterStatus struct {
	activeStreams atomic.Int64
	sendSuccess   atomic.Int64
	sendErrors    atomic.Int64
	reconnects    atomic.Int64
}

func newBucketReporterStatus() *bucketReporterStatus {
	return &bucketReporterStatus{}
}

// newClient resolves the current PD leader via resolvePDLeader and returns a
// pdpb.PDClient bound to it. We previously dialed only the first endpoint of a
// comma-separated --pd-endpoints, which silently routed writes (Bootstrap /
// PutStore) to a follower if that happened to be a non-leader — the failures
// then surfaced as cryptic "not leader" RPC errors at process startup. The
// per-slot streams open their own leader-aware connections via newStreamSlot;
// this one-shot cli is consumed only at boot (initClusterID, bootstrap,
// putStores) so we accept that it may stale on leader transfer afterwards.
func newClient(ctx context.Context, cfg *config.Config) (pdpb.PDClient, error) {
	_, _, cli, err := resolvePDLeaderWithRetry(ctx, cfg)
	return cli, err
}

func initClusterID(ctx context.Context, cli pdpb.PDClient) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cctx, cancel := context.WithCancel(ctx)
			res, err := cli.GetMembers(cctx, &pdpb.GetMembersRequest{})
			cancel()
			if err != nil {
				continue
			}
			if res.GetHeader().GetError() != nil {
				continue
			}
			clusterID = res.GetHeader().GetClusterId()
			log.Info("init cluster ID successfully", zap.Uint64("cluster-id", clusterID))
			return
		}
	}
}

func header() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: clusterID,
	}
}

func bootstrap(ctx context.Context, cli pdpb.PDClient) {
	cctx, cancel := context.WithCancel(ctx)
	isBootstrapped, err := cli.IsBootstrapped(cctx, &pdpb.IsBootstrappedRequest{Header: header()})
	cancel()
	if err != nil {
		log.Fatal("check if cluster has already bootstrapped failed", zap.Error(err))
	}
	if isBootstrapped.GetBootstrapped() {
		log.Info("already bootstrapped")
		return
	}

	store := &metapb.Store{
		Id:      1,
		Address: fmt.Sprintf("localhost:%d", 2),
		Version: "6.4.0-alpha",
	}
	region := &metapb.Region{
		Id:          1,
		Peers:       []*metapb.Peer{{StoreId: 1, Id: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	req := &pdpb.BootstrapRequest{
		Header: header(),
		Store:  store,
		Region: region,
	}
	cctx, cancel = context.WithCancel(ctx)
	resp, err := cli.Bootstrap(cctx, req)
	cancel()
	if err != nil {
		log.Fatal("failed to bootstrap the cluster", zap.Error(err))
	}
	if resp.GetHeader().GetError() != nil {
		log.Fatal("failed to bootstrap the cluster", zap.String("err", resp.GetHeader().GetError().String()))
	}
	log.Info("bootstrapped")
}

func putStores(ctx context.Context, cfg *config.Config, cli pdpb.PDClient, stores *Stores) {
	storeHeartbeatInterval := intervalForAggregateQPS(cfg.StoreHeartbeatQPS, cfg.StoreCount, storeReportInterval*time.Second)
	for i := uint64(1); i <= uint64(cfg.StoreCount); i++ {
		store := &metapb.Store{
			Id:      i,
			Address: fmt.Sprintf("localhost:%d", i),
			Version: "6.4.0-alpha",
		}
		cctx, cancel := context.WithCancel(ctx)
		resp, err := cli.PutStore(cctx, &pdpb.PutStoreRequest{Header: header(), Store: store})
		cancel()
		if err != nil {
			log.Fatal("failed to put store", zap.Uint64("store-id", i), zap.Error(err))
		}
		if resp.GetHeader().GetError() != nil {
			log.Fatal("failed to put store", zap.Uint64("store-id", i), zap.String("err", resp.GetHeader().GetError().String()))
		}
		go func(ctx context.Context, storeID uint64) {
			heartbeatTicker := time.NewTicker(storeHeartbeatInterval)
			defer heartbeatTicker.Stop()
			for {
				select {
				case <-heartbeatTicker.C:
					stores.heartbeat(ctx, cli, storeID)
				case <-ctx.Done():
					return
				}
			}
		}(ctx, i)
	}
}

func extraPeerCountForRegion(cfg *config.Config, regionIndex int) int {
	extraPeerCount := cfg.ExtraPeerCount
	if cfg.ExtraPeerRatio > 0 {
		extraPeerCount = int(float64(cfg.RegionCount) * cfg.ExtraPeerRatio)
	}
	if extraPeerCount <= 0 {
		return 0
	}
	base := extraPeerCount / cfg.RegionCount
	if regionIndex < extraPeerCount%cfg.RegionCount {
		base++
	}
	return base
}

func extraPeerRole(role string) metapb.PeerRole {
	if role == "learner" {
		return metapb.PeerRole_Learner
	}
	return metapb.PeerRole_Voter
}

// Regions simulates all regions to heartbeat.
type Regions struct {
	regions       []*pdpb.RegionHeartbeatRequest
	awakenRegions atomic.Value

	updateRound int

	updateLeader []int
	updateEpoch  []int
	updateSpace  []int
	updateFlow   []int

	// v2.1 (2026-05-20): hotRegion[i] = true marks region i as a hot region. When the region
	// is in updateFlow, the update() path uses cfg.HotWrite*/HotRead* values to populate
	// BytesWritten/Read/KeysWritten/Read, driving PD HotPeerCache / LabelStatistics
	// long-lived heap. Set at init() based on cfg.HotRegionRatio. Independent of (and
	// composable with) the legacy HotStoreCount mechanism.
	hotRegion []bool
}

func (rs *Regions) init(cfg *config.Config) {
	rs.regions = make([]*pdpb.RegionHeartbeatRequest, 0, cfg.RegionCount)
	rs.updateRound = 0

	// v2.1 (2026-05-20): initial ApproximateSize/Keys come from config when set (>0),
	// otherwise we keep the legacy 128B / 8keys placeholders for backwards compat.
	initialSize := uint64(bytesUnit)
	initialKeys := uint64(keysUint)
	if cfg.RegionApproximateSizeMiB > 0 {
		initialSize = uint64(cfg.RegionApproximateSizeMiB) * uint64(units.MiB)
	}
	if cfg.RegionApproximateKeys > 0 {
		initialKeys = uint64(cfg.RegionApproximateKeys)
	}

	// v2.1 (2026-05-20): precompute hot-region marker bitmap so update() can apply the
	// configured hot-flow values without re-rolling each round. Deterministic given seed.
	rs.hotRegion = make([]bool, cfg.RegionCount)
	if cfg.HotRegionRatio > 0 {
		hotCount := int(float64(cfg.RegionCount) * cfg.HotRegionRatio)
		// Pick the first hotCount of a shuffled index set. Using a local rng so we don't
		// disturb the global random sequence used by update().
		indexes := make([]int, cfg.RegionCount)
		for i := range indexes {
			indexes[i] = i
		}
		rand.Shuffle(cfg.RegionCount, func(i, j int) {
			indexes[i], indexes[j] = indexes[j], indexes[i]
		})
		for _, idx := range indexes[:hotCount] {
			rs.hotRegion[idx] = true
		}
	}

	// Generate regions
	id := uint64(1)
	now := uint64(time.Now().Unix())

	for i := range cfg.RegionCount {
		region := &pdpb.RegionHeartbeatRequest{
			Header: header(),
			Region: &metapb.Region{
				Id:          id,
				StartKey:    codec.GenerateTableKey(int64(i)),
				EndKey:      codec.GenerateTableKey(int64(i + 1)),
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: maxVersion},
			},
			ApproximateSize: initialSize,
			Interval: &pdpb.TimeInterval{
				StartTimestamp: now,
				EndTimestamp:   now + regionReportInterval,
			},
			QueryStats:      &pdpb.QueryStats{},
			ApproximateKeys: initialKeys,
			Term:            1,
		}
		id += 1
		if i == 0 {
			region.Region.StartKey = []byte("")
		}
		if i == cfg.RegionCount-1 {
			region.Region.EndKey = []byte("")
		}

		peers := make([]*metapb.Peer, 0, cfg.Replica+extraPeerCountForRegion(cfg, i))
		for j := range cfg.Replica {
			peers = append(peers, &metapb.Peer{Id: id, StoreId: uint64((i+j)%cfg.StoreCount + 1)})
			id += 1
		}
		extraPeers := extraPeerCountForRegion(cfg, i)
		for j := range extraPeers {
			peers = append(peers, &metapb.Peer{
				Id:      id,
				StoreId: uint64((i+cfg.Replica+j)%cfg.StoreCount + 1),
				Role:    extraPeerRole(cfg.ExtraPeerRole),
			})
			id += 1
		}

		region.Region.Peers = peers
		region.Leader = peers[0]
		// v2.1 (2026-05-20): seed hot-region flow at init so PD's HotPeerCache sees a hot
		// signal on the very first heartbeat round even before update() applies a flow tick.
		if rs.hotRegion[i] {
			region.BytesWritten = cfg.HotWriteBytesPerRegion
			region.BytesRead = cfg.HotReadBytesPerRegion
			region.KeysWritten = cfg.HotWriteKeysPerRegion
			region.KeysRead = cfg.HotReadKeysPerRegion
		}
		rs.regions = append(rs.regions, region)
	}
}

func (rs *Regions) update(cfg *config.Config, options *config.Options) {
	rs.updateRound += 1

	// Generate sample index
	indexes := make([]int, cfg.RegionCount)
	for i := range indexes {
		indexes[i] = i
	}
	reportRegions := pick(indexes, cfg.RegionCount, options.GetReportRatio())

	reportCount := len(reportRegions)
	rs.updateFlow = pick(reportRegions, reportCount, options.GetFlowUpdateRatio())
	rs.updateLeader = randomPick(reportRegions, reportCount, options.GetLeaderUpdateRatio())
	rs.updateEpoch = randomPick(reportRegions, reportCount, options.GetEpochUpdateRatio())
	rs.updateSpace = randomPick(reportRegions, reportCount, options.GetSpaceUpdateRatio())
	var (
		updatedStatisticsMap = make(map[int]*pdpb.RegionHeartbeatRequest)
		awakenRegions        []*pdpb.RegionHeartbeatRequest
	)

	// update leader
	for _, i := range rs.updateLeader {
		region := rs.regions[i]
		region.Leader = region.Region.Peers[rs.updateRound%cfg.Replica]
	}
	// update epoch
	for _, i := range rs.updateEpoch {
		region := rs.regions[i]
		region.Region.RegionEpoch.Version += 1
		if region.Region.RegionEpoch.Version > maxVersion {
			maxVersion = region.Region.RegionEpoch.Version
		}
	}
	// update space
	for _, i := range rs.updateSpace {
		region := rs.regions[i]
		region.ApproximateSize = uint64(bytesUnit * rand.Float64())
		region.ApproximateKeys = uint64(keysUint * rand.Float64())
	}
	// update flow
	// v2.1 (2026-05-20): a region is "hot" if (a) its leader sits in a hot store (legacy
	// HotStoreCount path) OR (b) it was marked hot at init time via HotRegionRatio. The two
	// mechanisms compose — operators can opt into either or both.
	hotStoreCount := uint64(options.GetHotStoreCount())
	for _, i := range rs.updateFlow {
		region := rs.regions[i]
		hotByStore := hotStoreCount > 0 && region.Leader.StoreId <= hotStoreCount
		hotByRegion := i < len(rs.hotRegion) && rs.hotRegion[i]
		switch {
		case hotByRegion && (cfg.HotWriteBytesPerRegion|cfg.HotReadBytesPerRegion|cfg.HotWriteKeysPerRegion|cfg.HotReadKeysPerRegion) != 0:
			// Use the per-region knobs verbatim (jittered ±20% so PD's stat path sees
			// non-constant input and doesn't dedupe). These represent per-interval traffic.
			jitter := func(v uint64) uint64 {
				if v == 0 {
					return 0
				}
				return uint64(float64(v) * (0.8 + 0.4*rand.Float64()))
			}
			region.BytesWritten = jitter(cfg.HotWriteBytesPerRegion)
			region.BytesRead = jitter(cfg.HotReadBytesPerRegion)
			region.KeysWritten = jitter(cfg.HotWriteKeysPerRegion)
			region.KeysRead = jitter(cfg.HotReadKeysPerRegion)
			region.QueryStats = &pdpb.QueryStats{
				Get: uint64(hotQueryUnit * (1 + rand.Float64()) * 10),
				Put: uint64(hotQueryUnit * (1 + rand.Float64()) * 60),
			}
		case hotByStore:
			region.BytesWritten = uint64(hotByteUnit * (1 + rand.Float64()) * 60)
			region.BytesRead = uint64(hotByteUnit * (1 + rand.Float64()) * 10)
			region.KeysWritten = uint64(hotKeysUint * (1 + rand.Float64()) * 60)
			region.KeysRead = uint64(hotKeysUint * (1 + rand.Float64()) * 10)
			region.QueryStats = &pdpb.QueryStats{
				Get: uint64(hotQueryUnit * (1 + rand.Float64()) * 10),
				Put: uint64(hotQueryUnit * (1 + rand.Float64()) * 60),
			}
		default:
			region.BytesWritten = uint64(bytesUnit * rand.Float64())
			region.BytesRead = uint64(bytesUnit * rand.Float64())
			region.KeysWritten = uint64(keysUint * rand.Float64())
			region.KeysRead = uint64(keysUint * rand.Float64())
			region.QueryStats = &pdpb.QueryStats{
				Get: uint64(queryUnit * rand.Float64()),
				Put: uint64(queryUnit * rand.Float64()),
			}
		}
		updatedStatisticsMap[i] = region
	}
	// update interval
	for _, region := range rs.regions {
		region.Interval.StartTimestamp = region.Interval.EndTimestamp
		region.Interval.EndTimestamp = region.Interval.StartTimestamp + regionReportInterval
	}
	// v2.1 (2026-05-20): hot regions get their flow signal preserved across rounds even
	// when not selected into updateFlow this round. Without this, PD's HotPeerCache would
	// observe a hot region as alternating hot/cold and never escalate it into a long-lived
	// hot peer entry — defeating the purpose of HotRegionRatio.
	hotKnobsSet := (cfg.HotWriteBytesPerRegion | cfg.HotReadBytesPerRegion | cfg.HotWriteKeysPerRegion | cfg.HotReadKeysPerRegion) != 0
	for _, i := range reportRegions {
		region := rs.regions[i]
		// reset the statistics of the region which is not updated
		if _, exist := updatedStatisticsMap[i]; !exist {
			if i < len(rs.hotRegion) && rs.hotRegion[i] && hotKnobsSet {
				// keep the seeded hot flow signal
			} else {
				region.BytesWritten = 0
				region.BytesRead = 0
				region.KeysWritten = 0
				region.KeysRead = 0
				region.QueryStats = &pdpb.QueryStats{}
			}
		}
		awakenRegions = append(awakenRegions, region)
	}

	rs.awakenRegions.Store(awakenRegions)
}

// streamSlot (v3.1.4, 2026-05-21) wraps a per-store RegionHeartbeat stream with
// **leader-aware** reconnect support. send() and drainRecv() detect a broken
// stream (PD silently closes after leader transfer); reconnect() rediscovers
// the current PD leader and rebuilds cli+stream against the new leader endpoint,
// because raw pdpb.PDClient has no service discovery and would otherwise loop
// forever connecting to the stale ex-leader.
//
// Concurrency model:
//   - send() and drainRecv() each observe a specific stream pointer; both call
//     reconnect(observed). reconnect() bails as a no-op if s.stream != observed,
//     so only the first caller actually rebuilds — the other returns nil.
//   - kickCh carries a single buffered signal that runStoreWorker consumes to
//     immediately run a fullResend round on top of the next ticker.Reset().
type streamSlot struct {
	storeID uint64
	cfg     *config.Config
	ctx     context.Context

	mu               sync.Mutex
	cli              pdpb.PDClient
	cc               *grpc.ClientConn // tracked so we can Close() on rebuild
	currentEp        string           // canonical endpoint URL cli is bound to
	stream           pdpb.PD_RegionHeartbeatClient
	fullResendNeeded atomic.Bool
	reconnects       atomic.Uint64

	// kickCh: reconnect() signals here so the per-store worker runs its
	// post-reconnect full heartbeat round immediately instead of waiting up
	// to a full tickInterval (60s by default). Buffered=1 absorbs the case
	// where multiple reconnects happen before the worker drains it.
	kickCh chan struct{}
}

// GetCli returns the current leader-bound PDClient under the slot mutex. Used
// by long-running side-channel reporters (ReportMinResolvedTS) so they pick up
// the new leader's cli after a reconnect, instead of holding a snapshot of the
// initial cli that goes stale on leader transfer.
func (s *streamSlot) GetCli() pdpb.PDClient {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cli
}

// canonicalEndpoint normalises a PD endpoint string so two URLs that point at
// the same node compare equal regardless of trailing slash / surrounding
// whitespace. Scheme is preserved (grpcutil.GetClientConn relies on url.Parse
// returning a non-empty Host, which only works when the scheme is present).
func canonicalEndpoint(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimRight(s, "/")
	return s
}

// resolvePDLeader (v3.1.4, 2026-05-21) discovers the current PD leader by
// probing each configured --pd-endpoints entry until one returns a leader
// member. Previously the function stripped scheme off the leader's ClientUrl
// before dialing, which silently broke grpcutil.GetClientConn — url.Parse
// rejects bare host:port with `first path segment in URL cannot contain colon`.
// We now keep the full URL throughout and compare endpoints via
// canonicalEndpoint.
//
// Returns (leader_endpoint, leader_cc, leader_cli, nil) on success. The caller
// owns the returned *grpc.ClientConn and must Close() it when the cli is
// retired.
func resolvePDLeader(ctx context.Context, cfg *config.Config) (string, *grpc.ClientConn, pdpb.PDClient, error) {
	tlsConfig, err := cfg.Security.ToTLSConfig()
	if err != nil {
		return "", nil, nil, err
	}
	eps := cfg.SplitEndpoints()
	if len(eps) == 0 {
		return "", nil, nil, stderrors.New("no PD endpoints configured")
	}
	var lastErr error
	for _, ep := range eps {
		cc, err := grpcutil.GetClientConn(ctx, ep, tlsConfig)
		if err != nil {
			lastErr = err
			continue
		}
		cli := pdpb.NewPDClient(cc)
		mctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		resp, err := cli.GetMembers(mctx, &pdpb.GetMembersRequest{})
		cancel()
		if err != nil || resp == nil || resp.GetLeader() == nil || len(resp.GetLeader().GetClientUrls()) == 0 {
			cc.Close()
			if err == nil {
				err = stderrors.New("no leader info in GetMembers response")
			}
			lastErr = err
			continue
		}
		leaderURL := canonicalEndpoint(resp.GetLeader().GetClientUrls()[0])
		if canonicalEndpoint(ep) == leaderURL {
			return leaderURL, cc, cli, nil
		}
		// Reached a peer that knows a different leader; dial leader directly.
		cc.Close()
		leaderCc, err := grpcutil.GetClientConn(ctx, leaderURL, tlsConfig)
		if err != nil {
			lastErr = err
			continue
		}
		return leaderURL, leaderCc, pdpb.NewPDClient(leaderCc), nil
	}
	if lastErr == nil {
		lastErr = stderrors.New("no PD endpoint reachable")
	}
	return "", nil, nil, lastErr
}

// resolvePDLeaderWithRetry wraps resolvePDLeader in a 1s retry loop until ctx
// is cancelled. Used at boot (newClient, newStreamSlot) where a single
// transient PD blip should not kill the whole bench.
func resolvePDLeaderWithRetry(ctx context.Context, cfg *config.Config) (string, *grpc.ClientConn, pdpb.PDClient, error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastErr error
	for {
		ep, cc, cli, err := resolvePDLeader(ctx, cfg)
		if err == nil {
			return ep, cc, cli, nil
		}
		lastErr = err
		log.Warn("resolve PD leader failed; retrying in 1s", zap.Error(err))
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return "", nil, nil, lastErr
		}
	}
}

// newStreamSlot constructs a streamSlot, retrying leader resolution + initial
// stream creation on transient failures. v3.1.3 used log.Fatal here, which
// turned every startup-time PD blip into a bench-wide crash; we now mirror
// initClusterID's 1s retry loop with an upper bound so a permanently-broken
// config still fails fast.
func newStreamSlot(ctx context.Context, cfg *config.Config, storeID uint64) *streamSlot {
	const maxAttempts = 60 // ≈1 minute of patience
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var (
		ep     string
		cc     *grpc.ClientConn
		cli    pdpb.PDClient
		stream pdpb.PD_RegionHeartbeatClient
		err    error
	)
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ep, cc, cli, err = resolvePDLeader(ctx, cfg)
		if err == nil {
			stream, err = cli.RegionHeartbeat(ctx)
			if err == nil {
				break
			}
			cc.Close()
			cc = nil
		}
		log.Warn("newStreamSlot setup failed; retrying in 1s",
			zap.Uint64("store-id", storeID), zap.Int("attempt", attempt), zap.Error(err))
		select {
		case <-ticker.C:
		case <-ctx.Done():
			log.Fatal("newStreamSlot ctx cancelled",
				zap.Uint64("store-id", storeID), zap.Error(ctx.Err()))
		}
	}
	if err != nil {
		log.Fatal("newStreamSlot exhausted retries",
			zap.Uint64("store-id", storeID), zap.Int("max-attempts", maxAttempts), zap.Error(err))
	}
	s := &streamSlot{
		storeID:   storeID,
		cfg:       cfg,
		ctx:       ctx,
		cli:       cli,
		cc:        cc,
		currentEp: ep,
		stream:    stream,
		kickCh:    make(chan struct{}, 1),
	}
	go s.drainRecv(stream)
	return s
}

// drainRecv consumes Recv() responses until the stream errors. PD closes the
// stream silently on leader transfer (the client-side Send buffer keeps
// accepting writes for a while), so Recv() is the authoritative liveness
// signal. v3.1.4: pointer-equality is the sole reconnect guard — drainRecv
// observes the exact stream it was started for and asks reconnect to rebuild
// only that stream; a concurrent send()-triggered reconnect for the same
// stream wins the race and drainRecv's reconnect call becomes a no-op.
func (s *streamSlot) drainRecv(stream pdpb.PD_RegionHeartbeatClient) {
	for {
		_, err := stream.Recv()
		if err == nil {
			continue
		}
		// Randomised backoff: prevents 100 stores from synchronously
		// reconnect-storming PD right after a leader transfer. We sleep
		// before reconnect so the send() path (if it's also retrying) has
		// time to be the single caller that does the rebuild.
		select {
		case <-time.After(time.Duration(200+rand.Intn(300)) * time.Millisecond):
		case <-s.ctx.Done():
			return
		}
		if s.cfg.AutoReconnect {
			if rerr := s.reconnect(stream); rerr != nil {
				heartbeatStreamErrors.WithLabelValues(strconv.FormatUint(s.storeID, 10), "drainrecv_reconnect_failed").Inc()
				log.Warn("drainRecv reconnect failed", zap.Uint64("store-id", s.storeID), zap.Error(rerr))
			}
		}
		return
	}
}

// send transmits one heartbeat. On send error it triggers reconnect (which
// pointer-equality-guards against duplicate rebuilds) and returns the original
// send error — the caller (runOneRound) breaks out of the current round so the
// next round runs as a full resend on the new stream. We no longer transparently
// retry inside send: the v3.1.3 internal retry mixed "this single send is OK"
// with "stream has been recovered" semantics, which made runOneRound continue
// streaming the steady-state subset on the new leader instead of aborting and
// running a faithful full resend on the next round.
func (s *streamSlot) send(req *pdpb.RegionHeartbeatRequest) error {
	s.mu.Lock()
	observed := s.stream
	err := observed.Send(req)
	s.mu.Unlock()
	if err == nil {
		return nil
	}
	heartbeatStreamErrors.WithLabelValues(strconv.FormatUint(s.storeID, 10), "send").Inc()
	if s.cfg.AutoReconnect {
		log.Warn("region heartbeat send error; triggering reconnect",
			zap.Uint64("store-id", s.storeID), zap.Error(err))
		if rerr := s.reconnect(observed); rerr != nil {
			heartbeatStreamErrors.WithLabelValues(strconv.FormatUint(s.storeID, 10), "reconnect_failed").Inc()
			log.Warn("send-triggered reconnect failed", zap.Uint64("store-id", s.storeID), zap.Error(rerr))
		}
	}
	return err
}

// reconnect rebuilds the per-slot RegionHeartbeat stream, re-resolving the PD
// leader if it has moved. The observed parameter is the stream pointer the
// caller saw an error on; if s.stream has already been advanced past observed
// (meaning another goroutine — typically the other of send/drainRecv — already
// reconnected), this call is a no-op. This is the v3.1.4 pointer-equality
// guard that replaces the v3.1.3 unconditional rebuild, which during a leader
// transfer ended up tearing down freshly-built streams to make a 3rd one.
func (s *streamSlot) reconnect(observed pdpb.PD_RegionHeartbeatClient) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stream != observed {
		// Another goroutine already rebuilt past this point.
		return nil
	}
	_ = s.stream.CloseSend()
	newEp, newCc, newCli, err := resolvePDLeader(s.ctx, s.cfg)
	if err != nil {
		// Fall back to existing cli — better than nothing. The next round's
		// send error will retrigger this path.
		log.Warn("resolvePDLeader failed during reconnect; retrying on existing cli",
			zap.Uint64("store-id", s.storeID), zap.Error(err))
	} else if newEp != s.currentEp {
		if s.cc != nil {
			_ = s.cc.Close()
		}
		s.cc = newCc
		s.cli = newCli
		s.currentEp = newEp
		log.Info("PD leader endpoint switched",
			zap.Uint64("store-id", s.storeID), zap.String("new-endpoint", newEp))
	} else {
		// Same leader as before — release the freshly-built cc, keep current cli.
		_ = newCc.Close()
	}
	newStream, err := s.cli.RegionHeartbeat(s.ctx)
	if err != nil {
		return err
	}
	s.stream = newStream
	s.reconnects.Add(1)
	heartbeatStreamReconnects.WithLabelValues(strconv.FormatUint(s.storeID, 10)).Inc()
	if s.cfg.FullResendOnReconnect {
		s.fullResendNeeded.Store(true)
		// Signal the worker so it runs the post-reconnect round immediately
		// instead of waiting up to a full tickInterval (real TiKV bursts
		// immediately on stream re-establishment, not at the next tick).
		select {
		case s.kickCh <- struct{}{}:
		default:
		}
	}
	go s.drainRecv(newStream)
	log.Info("region heartbeat stream reconnected",
		zap.Uint64("store-id", s.storeID),
		zap.String("endpoint", s.currentEp),
		zap.Uint64("total-reconnects", s.reconnects.Load()),
		zap.Bool("full-resend-on-next-round", s.cfg.FullResendOnReconnect))
	return nil
}

// storeRoundStat aggregates one store's per-round counters for the cluster-wide
// stats reporter goroutine. Lossy delivery (non-blocking send) is fine since
// PD-side Prometheus is the canonical metric source.
type storeRoundStat struct {
	storeID    uint64
	sent       int
	errs       int
	elapsed    time.Duration
	fullResend bool
}

// runStoreWorker (v3.1.4) drives one store's heartbeat stream. Each store has
// its own ticker, started after a phase offset so PD sees a continuous arrival
// pattern instead of cluster-wide synchronized rounds. On post-reconnect kick
// (slot.kickCh) the worker runs an immediate round AND resets its ticker, so
// the next steady-state round is one full tickInterval after the burst —
// mirroring real TiKV which fires a full heartbeat the instant the new PD
// stream is ready, then resumes its own regular schedule.
func runStoreWorker(
	ctx context.Context,
	cfg *config.Config,
	rs *Regions,
	slot *streamSlot,
	storeID uint64,
	startDelay time.Duration,
	tickInterval time.Duration,
	firstRoundCh chan<- struct{},
	statsCh chan<- storeRoundStat,
) {
	if startDelay > 0 {
		select {
		case <-time.After(startDelay):
		case <-ctx.Done():
			return
		}
	}
	runOneRound(ctx, cfg, rs, slot, storeID, tickInterval, statsCh)
	select {
	case firstRoundCh <- struct{}{}:
	default:
	}
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			runOneRound(ctx, cfg, rs, slot, storeID, tickInterval, statsCh)
		case <-slot.kickCh:
			// Reconnect just succeeded — run the full-resend round NOW and
			// re-phase the ticker so the next steady round is one tick away.
			runOneRound(ctx, cfg, rs, slot, storeID, tickInterval, statsCh)
			ticker.Reset(tickInterval)
		case <-ctx.Done():
			return
		}
	}
}

// runOneRound emits one round's worth of region heartbeats. On fullResend
// (set by reconnect → fullResendNeeded), this faithfully simulates real TiKV
// behaviour after PD leader switch: send a heartbeat for EVERY region this
// store leads, bursting them with no per-region pacing and bypassing the
// steady-state burst-cycle fraction. Steady-state rounds keep their usual
// envelope (report-ratio subset, burst-cycle fraction, optional smooth
// pacing).
//
// v3.1.4 (2026-05-21): full-resend semantics changed from "trim by burst-cycle"
// to "all leader-owned regions". The old behaviour calibrated to a ~44k
// per-store-per-min target derived from steady-state online observation, but
// the bench's stated purpose (the user-prompt phrasing was 全量上报心跳) is to
// reproduce the transient post-transfer burst, which on real TiKV bypasses
// any rate-shaping — it pushes the full leader set as fast as the stream
// will accept it. See docs/big-pd-pressure/heartbeat-bench-reconnect-review.md
// §full-resend semantics.
func runOneRound(
	ctx context.Context,
	cfg *config.Config,
	rs *Regions,
	slot *streamSlot,
	storeID uint64,
	paceInterval time.Duration,
	statsCh chan<- storeRoundStat,
) {
	fullResend := slot.fullResendNeeded.CompareAndSwap(true, false)

	// Region source selection:
	//   fullResend → rs.regions (ALL regions; mirrors TiKV post-reconnect)
	//   steady     → rs.awakenRegions (the report-ratio subset chosen by
	//                runClusterUpdater.update())
	var (
		regions  []*pdpb.RegionHeartbeatRequest
		toUpdate []*pdpb.RegionHeartbeatRequest
	)
	if fullResend {
		toUpdate = rs.regions
	} else {
		updatedRegions := rs.awakenRegions.Load()
		if updatedRegions == nil {
			toUpdate = rs.regions
		} else {
			toUpdate = updatedRegions.([]*pdpb.RegionHeartbeatRequest)
		}
	}
	for _, region := range toUpdate {
		if region.Leader.StoreId == storeID {
			regions = append(regions, region)
		}
	}

	// Steady-state envelope shaping (burst-cycle + per-region pacing). Skipped
	// on fullResend so the post-reconnect round bursts at line rate.
	perRegionDelay := time.Duration(0)
	if !fullResend {
		if cfg.BurstCycle && len(regions) > 0 {
			fraction := computeBurstCycleFraction(cfg, time.Now())
			effective := int(float64(len(regions)) * fraction)
			if effective < 1 {
				effective = 1
			}
			if effective < len(regions) {
				startOff := 0
				if span := len(regions) - effective; span > 0 {
					startOff = rand.Intn(span + 1)
				}
				regions = regions[startOff : startOff+effective]
			}
		}
		perRegionDelay = computeSmoothPaceDelay(cfg, paceInterval, len(regions))
	}

	start := time.Now()
	sent, errs := 0, 0
	lastIdx := len(regions) - 1
	for i, region := range regions {
		if err := slot.send(region); err != nil {
			errs++
			// send() already triggered reconnect (pointer-equality guarded).
			// End this round; the kickCh path or the next ticker fire will
			// run a full-resend round on the new stream.
			log.Warn("region heartbeat send failed; ending round",
				zap.Uint64("store-id", storeID), zap.Error(err))
			break
		}
		sent++
		if perRegionDelay > 0 && i < lastIdx {
			select {
			case <-ctx.Done():
				return
			case <-time.After(paceJitter(perRegionDelay)):
			}
		}
	}
	elapsed := time.Since(start)
	if fullResend {
		heartbeatFullResendRounds.Inc()
		log.Info("full heartbeat resend round complete (post-reconnect)",
			zap.Uint64("store-id", storeID),
			zap.Int("regions-sent", sent),
			zap.Int("errors", errs),
			zap.Duration("elapsed", elapsed))
	}
	select {
	case statsCh <- storeRoundStat{storeID, sent, errs, elapsed, fullResend}:
	default:
	}
}

// runClusterUpdater drives cluster-wide region/store state mutations on a
// single tick (the original code did this once per global heartbeat round; with
// per-store tickers we need an explicit driver since there's no shared barrier).
func runClusterUpdater(
	ctx context.Context,
	cfg *config.Config,
	rs *Regions,
	stores *Stores,
	options *config.Options,
	tickInterval time.Duration,
) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if cfg.Round != 0 && rs.updateRound > cfg.Round {
				exit(0)
			}
			rs.update(cfg, options)
			go stores.update(rs)
		case <-ctx.Done():
			return
		}
	}
}

// runStatsReporter aggregates per-store round stats and emits a summary every
// tickInterval. Replaces the in-loop log.Info("region heartbeat stats", ...).
func runStatsReporter(
	ctx context.Context,
	statsCh <-chan storeRoundStat,
	interval time.Duration,
) {
	var (
		mu              sync.Mutex
		totalSent       int
		totalErrs       int
		roundsFinished  int
		maxElapsed      time.Duration
		fullResendCount int
	)
	go func() {
		for s := range statsCh {
			mu.Lock()
			totalSent += s.sent
			totalErrs += s.errs
			roundsFinished++
			if s.elapsed > maxElapsed {
				maxElapsed = s.elapsed
			}
			if s.fullResend {
				fullResendCount++
			}
			mu.Unlock()
		}
	}()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			mu.Lock()
			log.Info("region heartbeat stats (per-store ticker, v3.1)",
				zap.Int("rounds-finished", roundsFinished),
				zap.Int("regions-sent", totalSent),
				zap.Int("errors", totalErrs),
				zap.Duration("max-store-elapsed", maxElapsed),
				zap.Float64("aggregate-rps", float64(totalSent)/interval.Seconds()),
				zap.Int("full-resend-rounds", fullResendCount))
			totalSent, totalErrs, roundsFinished, fullResendCount = 0, 0, 0, 0
			maxElapsed = 0
			mu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

// computeSmoothPaceDelay returns the per-region sleep used in smooth pacing mode.
// Zero means no pacing (bursty / legacy). Extracted so unit tests can validate the
// math without spinning up a fake gRPC stream.
//
// v2.3 (2026-05-20): pace target is the outer-tick interval (which IS the per-region
// cadence in this bench design), divided by the number of regions this worker will
// send. Result: 60s / 81600 = 735µs per send when cfg defaults * 100 stores * 8.16M.
// 0 returns trigger:
//   - smooth pacing disabled in config
//   - paceInterval <= 0 (degenerate / passed by misuse)
//   - regions == 0 (worker has nothing to do)
//   - delay would round to < 1µs (no point pacing at that resolution; just burst)
func computeSmoothPaceDelay(cfg *config.Config, paceInterval time.Duration, regions int) time.Duration {
	if !cfg.SmoothHeartbeatPacing {
		return 0
	}
	if paceInterval <= 0 || regions <= 0 {
		return 0
	}
	d := paceInterval / time.Duration(regions)
	if d < time.Microsecond {
		return 0
	}
	return d
}

// v2.5 (2026-05-20): collective burst cycle phase math — see config doc.
// Returns the report fraction in [0, 1] for the given wall-clock time. Returns
// 1.0 when burst-cycle is disabled (caller will then send the whole region slice).
// Phase layout (sec = wall_clock_unix mod BurstCyclePeriodSec):
//   [0, HighSec)                         → HighFraction
//   [HighSec, HighSec + RampSec)         → linear interp High → Low
//   [HighSec + RampSec, Period - RampSec)→ LowFraction
//   [Period - RampSec, Period)           → linear interp Low → High
// Matches the structure observed on online cluster 10989049060142230334 via Clinic
// PromQL pd_scheduler_region_heartbeat{type="report"}: 10 min cycle with ~1 min HIGH
// peaking at 74 K hb/s aggregate, ~7 min LOW at 39 K hb/s, ~1 min linear ramp each side.
func computeBurstCycleFraction(cfg *config.Config, now time.Time) float64 {
	if !cfg.BurstCycle || cfg.BurstCyclePeriodSec <= 0 {
		return 1.0
	}
	period := cfg.BurstCyclePeriodSec
	high := cfg.BurstCycleHighSec
	ramp := cfg.BurstCycleRampSec
	sec := now.Unix() % int64(period)
	t := float64(sec)
	hi := cfg.BurstCycleHighFraction
	lo := cfg.BurstCycleLowFraction
	switch {
	case t < float64(high):
		return hi
	case t < float64(high+ramp):
		x := (t - float64(high)) / float64(ramp)
		return hi + (lo-hi)*x
	case t < float64(period-ramp):
		return lo
	default:
		x := (t - float64(period-ramp)) / float64(ramp)
		return lo + (hi-lo)*x
	}
}

// paceJitter returns a per-iteration sleep with ±10% jitter around perRegionDelay,
// to prevent 100 stores synchronizing their inter-send sleeps and burst-firing
// together. Returns 0 if delay is 0 (caller skips sleep entirely).
func paceJitter(perRegionDelay time.Duration) time.Duration {
	if perRegionDelay <= 0 {
		return 0
	}
	// jitterRange = ±10% of perRegionDelay, expressed as [0, perRegionDelay/5] then
	// shifted by -perRegionDelay/10 to center on zero.
	maxJitterPlus := int64(perRegionDelay/5) + 1
	j := rand.Int63n(maxJitterPlus) - int64(perRegionDelay/10)
	return perRegionDelay + time.Duration(j)
}

// Stores contains store stats with lock.
type Stores struct {
	stat     []atomic.Value
	capacity uint64 // v2.1 (2026-05-20): configurable per-store capacity
}

// newStoresWithCapacity respects the configured (or auto-computed) per-store
// capacity so `store.Available -= region.ApproximateSize` doesn't underflow when
// RegionApproximateSizeMiB pushes per-store live data past the legacy 4 TiB cap.
func newStoresWithCapacity(storeCount int, capacityBytes uint64) *Stores {
	if capacityBytes == 0 {
		capacityBytes = legacyCapacity
	}
	return &Stores{
		stat:     make([]atomic.Value, storeCount+1),
		capacity: capacityBytes,
	}
}

func (s *Stores) heartbeat(ctx context.Context, cli pdpb.PDClient, storeID uint64) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cli.StoreHeartbeat(cctx, &pdpb.StoreHeartbeatRequest{Header: header(), Stats: s.stat[storeID].Load().(*pdpb.StoreStats)})
}

func (s *Stores) update(rs *Regions) {
	stats := make([]*pdpb.StoreStats, len(s.stat))
	now := uint64(time.Now().Unix())
	cap := s.capacity
	if cap == 0 {
		cap = legacyCapacity
	}
	for i := range stats {
		stats[i] = &pdpb.StoreStats{
			StoreId:    uint64(i),
			Capacity:   cap,
			Available:  cap,
			QueryStats: &pdpb.QueryStats{},
			PeerStats:  make([]*pdpb.PeerStat, 0),
			Interval: &pdpb.TimeInterval{
				StartTimestamp: now - storeReportInterval,
				EndTimestamp:   now,
			},
		}
	}
	var toUpdate []*pdpb.RegionHeartbeatRequest
	updatedRegions := rs.awakenRegions.Load()
	if updatedRegions == nil {
		toUpdate = rs.regions
	} else {
		toUpdate = updatedRegions.([]*pdpb.RegionHeartbeatRequest)
	}
	for _, region := range toUpdate {
		for _, peer := range region.Region.Peers {
			store := stats[peer.StoreId]
			store.UsedSize += region.ApproximateSize
			store.Available -= region.ApproximateSize
			store.RegionCount += 1
		}
		store := stats[region.Leader.StoreId]
		if region.BytesWritten != 0 {
			store.BytesWritten += region.BytesWritten
			store.BytesRead += region.BytesRead
			store.KeysWritten += region.KeysWritten
			store.KeysRead += region.KeysRead
			store.QueryStats.Get += region.QueryStats.Get
			store.QueryStats.Put += region.QueryStats.Put
			store.PeerStats = append(store.PeerStats, &pdpb.PeerStat{
				RegionId:     region.Region.Id,
				ReadKeys:     region.KeysRead,
				ReadBytes:    region.BytesRead,
				WrittenKeys:  region.KeysWritten,
				WrittenBytes: region.BytesWritten,
				QueryStats:   region.QueryStats,
			})
		}
	}
	for i := range stats {
		s.stat[i].Store(stats[i])
	}
}

// countHotRegions reports the number of regions flagged hot at init() time. Useful for the
// startup log so operators can confirm HotRegionRatio took effect.
func countHotRegions(rs *Regions) int {
	n := 0
	for _, h := range rs.hotRegion {
		if h {
			n++
		}
	}
	return n
}

func randomPick(slice []int, total int, ratio float64) []int {
	rand.Shuffle(total, func(i, j int) {
		slice[i], slice[j] = slice[j], slice[i]
	})
	return append(slice[:0:0], slice[0:int(float64(total)*ratio)]...)
}

func pick(slice []int, total int, ratio float64) []int {
	return append(slice[:0:0], slice[0:int(float64(total)*ratio)]...)
}

type minResolvedTSReportFunc func(context.Context, uint64) error

func reportMinResolvedTSForStores(ctx context.Context, storeCount int, report minResolvedTSReportFunc) {
	wg := &sync.WaitGroup{}
	for i := 1; i <= storeCount; i++ {
		id := uint64(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := report(ctx, id); err != nil {
				log.Error("send resolved TS error", zap.Uint64("store-id", id), zap.Error(err))
			}
		}()
	}
	wg.Wait()
}

func runMinResolvedTSReporter(
	ctx context.Context,
	storeCount int,
	interval time.Duration,
	report minResolvedTSReportFunc,
) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		resolvedTSTicker := time.NewTicker(interval)
		defer resolvedTSTicker.Stop()
		for {
			select {
			case <-resolvedTSTicker.C:
				reportMinResolvedTSForStores(ctx, storeCount, report)
			case <-ctx.Done():
				return
			}
		}
	}()
	return done
}

func intervalForAggregateQPS(qps int, units int, fallback time.Duration) time.Duration {
	if qps <= 0 || units <= 0 {
		return fallback
	}
	interval := time.Duration(units) * time.Second / time.Duration(qps)
	if interval < time.Millisecond {
		return time.Millisecond
	}
	return interval
}

func buildReportBucketsRequest(region *pdpb.RegionHeartbeatRequest, periodMs uint64) *pdpb.ReportBucketsRequest {
	return &pdpb.ReportBucketsRequest{
		Header:      header(),
		RegionEpoch: region.GetRegion().GetRegionEpoch(),
		Buckets: &metapb.Buckets{
			RegionId:   region.GetRegion().GetId(),
			Version:    uint64(time.Now().UnixNano()),
			Keys:       [][]byte{region.GetRegion().GetStartKey(), region.GetRegion().GetEndKey()},
			PeriodInMs: periodMs,
			Stats: &metapb.BucketStats{
				ReadBytes:  []uint64{1},
				ReadKeys:   []uint64{1},
				ReadQps:    []uint64{1},
				WriteBytes: []uint64{1},
				WriteKeys:  []uint64{1},
				WriteQps:   []uint64{1},
			},
		},
	}
}

func startReportBucketsWorkers(
	ctx context.Context,
	streamCount int,
	interval time.Duration,
	regions *Regions,
	status *bucketReporterStatus,
	factory reportBucketsStreamFactory,
	gate <-chan struct{},
) {
	if streamCount <= 0 {
		return
	}
	if interval <= 0 {
		interval = time.Second
	}
	for i := 0; i < streamCount; i++ {
		workerID := i
		go runReportBucketsWorker(ctx, workerID, interval, regions, status, factory, gate)
	}
}

func runReportBucketsWorker(
	ctx context.Context,
	workerID int,
	interval time.Duration,
	regions *Regions,
	status *bucketReporterStatus,
	factory reportBucketsStreamFactory,
	gate <-chan struct{},
) {
	// v2.1 (2026-05-20): wait for the gate (typically the first RegionHeartbeat round to
	// finish) before opening the bucket stream. Otherwise the bucket arrives at PD before
	// PD knows the region and gets dropped with `the store of the bucket in region is not
	// found` (5/18 run logged 6,100 such drops in the first round). nil gate = legacy
	// behaviour (start immediately) for backwards compat.
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := runReportBucketsStream(ctx, workerID, interval, regions, status, factory); err != nil {
			if isExpectedReportBucketsShutdown(ctx, err) {
				return
			}
			status.reconnects.Add(1)
			log.Error("report buckets stream disconnected", zap.Int("worker-id", workerID), zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func isExpectedReportBucketsShutdown(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() == nil {
		return false
	}
	if stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Cause(err) == context.Canceled || errors.Cause(err) == context.DeadlineExceeded {
		return true
	}
	code := status.Code(err)
	return code == codes.Canceled || code == codes.DeadlineExceeded
}

func runReportBucketsStream(
	ctx context.Context,
	workerID int,
	interval time.Duration,
	regions *Regions,
	status *bucketReporterStatus,
	factory reportBucketsStreamFactory,
) error {
	stream, err := factory(ctx)
	if err != nil {
		return err
	}
	status.activeStreams.Add(1)
	defer status.activeStreams.Add(-1)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	seq := workerID
	for {
		if len(regions.regions) == 0 {
			return nil
		}
		region := regions.regions[seq%len(regions.regions)]
		req := buildReportBucketsRequest(region, uint64(interval/time.Millisecond))
		if err := stream.Send(req); err != nil {
			status.sendErrors.Add(1)
			return err
		}
		status.sendSuccess.Add(1)
		seq += 1
		select {
		case <-ctx.Done():
			_, err := stream.CloseAndRecv()
			if err != nil && !stderrors.Is(err, context.Canceled) && !stderrors.Is(err, io.EOF) {
				return err
			}
			return nil
		case <-ticker.C:
		}
	}
}

// newLeaderAwareReportBucketsStreamFactory (v3.1.4) returns a factory and a
// closer. The factory re-resolves the PD leader (with a 1s coalescing window
// so a thundering herd of N workers restarting in lockstep after a leader
// transfer issues at most one GetMembers per second). All bucket workers
// share a single leader-bound *grpc.ClientConn that this returns; on actual
// leader move the old conn is Close()d and replaced.
//
// The v3.1.3 factory was bound to the boot-time `cli` and never followed
// leader transfers, so once leader moved every ReportBuckets stream errored
// indefinitely.
func newLeaderAwareReportBucketsStreamFactory(cfg *config.Config) (reportBucketsStreamFactory, func()) {
	var (
		mu          sync.Mutex
		cc          *grpc.ClientConn
		cli         pdpb.PDClient
		ep          string
		lastResolve time.Time
	)
	refresh := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		if cli != nil && time.Since(lastResolve) < time.Second {
			return nil
		}
		newEp, newCc, newCli, err := resolvePDLeader(ctx, cfg)
		if err != nil {
			return err
		}
		if ep != newEp {
			if cc != nil {
				_ = cc.Close()
			}
			cc, cli, ep = newCc, newCli, newEp
		} else {
			_ = newCc.Close()
		}
		lastResolve = time.Now()
		return nil
	}
	factory := func(ctx context.Context) (reportBucketsClient, error) {
		if err := refresh(ctx); err != nil {
			return nil, err
		}
		mu.Lock()
		c := cli
		mu.Unlock()
		return c.ReportBuckets(ctx)
	}
	closer := func() {
		mu.Lock()
		defer mu.Unlock()
		if cc != nil {
			_ = cc.Close()
			cc = nil
		}
	}
	return factory, closer
}

func main() {
	statistics.Denoising = false
	cfg := config.NewConfig()
	err := cfg.Parse(os.Args[1:])
	defer logutil.LogPanic()

	switch errors.Cause(err) {
	case nil:
	case pflag.ErrHelp:
		exit(0)
	default:
		log.Fatal("parse cmd flags error", zap.Error(err))
	}

	// New zap logger
	err = logutil.SetupLogger(&cfg.Log, &cfg.Logger, &cfg.LogProps, logutil.RedactInfoLogOFF)
	if err == nil {
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	} else {
		log.Fatal("initialize logger error", zap.Error(err))
	}

	maxVersion = cfg.InitEpochVer
	options := config.NewOptions(cfg)
	// let PD have enough time to start
	time.Sleep(5 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()
	cli, err := newClient(ctx, cfg)
	if err != nil {
		log.Fatal("create client error", zap.Error(err))
	}

	initClusterID(ctx, cli)
	go runHTTPServer(cfg, options)
	regions := new(Regions)
	regions.init(cfg)
	log.Info("finish init regions",
		zap.Int("region-count", cfg.RegionCount),
		zap.Int("hot-region-count", countHotRegions(regions)),
		zap.Int("region-size-mib", cfg.RegionApproximateSizeMiB),
		zap.Int("region-keys", cfg.RegionApproximateKeys),
	)
	storeCap := computeStoreCapacity(cfg)
	log.Info("store capacity", zap.Uint64("per-store-bytes", storeCap), zap.Uint64("per-store-gib", storeCap/uint64(units.GiB)))
	stores := newStoresWithCapacity(cfg.StoreCount, storeCap)
	stores.update(regions)
	bootstrap(ctx, cli)
	putStores(ctx, cfg, cli, stores)
	log.Info("finish put stores")
	// v3.1.4 (2026-05-21): pdHttp.NewClient takes a []string of endpoints which
	// it feeds to PD's ServiceDiscovery — that machinery follows leader
	// transfers automatically. Previously we passed []string{cfg.PDAddr} so
	// the comma-joined multi-endpoint string was treated as a single weird
	// endpoint; ServiceDiscovery's leader following degraded to whatever the
	// first character of the string resolved to.
	httpCli := pdHttp.NewClient("tools-heartbeat-bench", cfg.SplitEndpoints(), pdHttp.WithTLSConfig(loadTLSConfig(cfg)))
	go deleteOperators(ctx, httpCli)

	// v3.1.4 (2026-05-21): per-store stream slots with auto-reconnect. The
	// v3.1 `clis := slots[id].cli` snapshot was removed — that map captured
	// the initial leader cli at slot construction and never updated on
	// reconnect, so ReportMinResolvedTS continued targeting the ex-leader
	// after a transfer. Long-lived side channels now dereference
	// slots[id].GetCli() at call time.
	slots := make(map[uint64]*streamSlot, cfg.StoreCount)
	for i := 1; i <= cfg.StoreCount; i++ {
		id := uint64(i)
		slots[id] = newStreamSlot(ctx, cfg, id)
	}
	header := &pdpb.RequestHeader{
		ClusterId: clusterID,
	}
	heartbeatTickInterval := intervalForAggregateQPS(cfg.RegionHeartbeatQPS, cfg.RegionCount, regionReportInterval*time.Second)
	if cfg.SmoothHeartbeatPacing {
		log.Info("smooth heartbeat pacing enabled",
			zap.Duration("outer-tick-interval", heartbeatTickInterval),
			zap.Int("region-count", cfg.RegionCount),
			zap.Int("store-count", cfg.StoreCount))
	}
	var staggerStep time.Duration
	if cfg.StaggerBurst && cfg.StoreCount > 0 {
		staggerStep = heartbeatTickInterval / time.Duration(cfg.StoreCount)
		log.Info("stagger-burst enabled (per-store startup phase offset, v3.1)",
			zap.Duration("outer-tick-interval", heartbeatTickInterval),
			zap.Duration("stagger-step", staggerStep),
			zap.Int("store-count", cfg.StoreCount))
	}
	if cfg.BurstCycle {
		log.Info("burst-cycle enabled",
			zap.Int("period-sec", cfg.BurstCyclePeriodSec),
			zap.Int("high-sec", cfg.BurstCycleHighSec),
			zap.Int("ramp-sec", cfg.BurstCycleRampSec),
			zap.Float64("high-fraction", cfg.BurstCycleHighFraction),
			zap.Float64("low-fraction", cfg.BurstCycleLowFraction))
	}
	if cfg.AutoReconnect {
		log.Info("auto-reconnect enabled (leader-transfer-driven stream resync, v3.1)")
	}
	if cfg.FullResendOnReconnect {
		log.Info("full-resend-on-reconnect enabled (TiKV-style burst after reconnect, v3.1)")
	}
	runMinResolvedTSReporter(ctx, cfg.StoreCount, intervalForAggregateQPS(cfg.ReportMinResolvedTSQPS, cfg.StoreCount, time.Second), func(ctx context.Context, id uint64) error {
		// v3.1.4: dereference the slot's current leader cli at call time so
		// ReportMinResolvedTS follows leader transfers.
		cli := slots[id].GetCli()
		_, err := cli.ReportMinResolvedTS(ctx, &pdpb.ReportMinResolvedTsRequest{
			Header:        header,
			StoreId:       id,
			MinResolvedTs: uint64(time.Now().Unix()),
		})
		return err
	})
	bucketStatus := newBucketReporterStatus()
	var bucketGate chan struct{}
	if cfg.BucketsAfterFirstHeartbeatRound {
		bucketGate = make(chan struct{})
	}
	bucketFactory, bucketFactoryCloser := newLeaderAwareReportBucketsStreamFactory(cfg)
	defer bucketFactoryCloser()
	startReportBucketsWorkers(
		ctx,
		cfg.ReportBucketsStreams,
		time.Duration(cfg.ReportBucketsIntervalMS)*time.Millisecond,
		regions,
		bucketStatus,
		bucketFactory,
		bucketGate,
	)
	metrics.InitMetric2Collect(cfg.MetricsAddr)

	// v3.1 main loop: per-store ticker + cluster updater + stats reporter.
	statsCh := make(chan storeRoundStat, cfg.StoreCount*4)
	go runStatsReporter(ctx, statsCh, heartbeatTickInterval)
	go runClusterUpdater(ctx, cfg, regions, stores, options, heartbeatTickInterval)

	firstRoundCh := make(chan struct{}, cfg.StoreCount)
	go func() {
		// Wait for all stores to finish their first round, then unblock bucket
		// workers exactly once (replaces the in-loop firstRoundDone flag).
		for i := 0; i < cfg.StoreCount; i++ {
			select {
			case <-firstRoundCh:
			case <-ctx.Done():
				return
			}
		}
		if bucketGate != nil {
			close(bucketGate)
			log.Info("first heartbeat round complete on all stores; releasing report-buckets workers")
		}
	}()

	for i := 1; i <= cfg.StoreCount; i++ {
		id := uint64(i)
		startDelay := time.Duration(0)
		if staggerStep > 0 {
			startDelay = staggerStep * time.Duration(i-1)
		}
		go runStoreWorker(ctx, cfg, regions, slots[id], id, startDelay, heartbeatTickInterval, firstRoundCh, statsCh)
	}

	<-ctx.Done()
	log.Info("got signal to exit")
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func exit(code int) {
	metrics.OutputConclusion()
	os.Exit(code)
}

func deleteOperators(ctx context.Context, httpCli pdHttp.Client) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := httpCli.DeleteOperators(ctx)
			if err != nil {
				log.Error("fail to delete operators", zap.Error(err))
			}
		}
	}
}

func runHTTPServer(cfg *config.Config, options *config.Options) {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(cors.Default())
	engine.Use(gzip.Gzip(gzip.DefaultCompression))
	engine.GET("metrics", utils.PromHandler())
	// profile API
	pprof.Register(engine)
	engine.PUT("config", func(c *gin.Context) {
		newCfg := cfg.Clone()
		newCfg.HotStoreCount = options.GetHotStoreCount()
		newCfg.FlowUpdateRatio = options.GetFlowUpdateRatio()
		newCfg.LeaderUpdateRatio = options.GetLeaderUpdateRatio()
		newCfg.EpochUpdateRatio = options.GetEpochUpdateRatio()
		newCfg.SpaceUpdateRatio = options.GetSpaceUpdateRatio()
		newCfg.ReportRatio = options.GetReportRatio()
		if err := c.BindJSON(&newCfg); err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		if err := newCfg.Validate(); err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		options.SetOptions(newCfg)
		c.String(http.StatusOK, "Successfully updated the configuration")
	})
	engine.GET("config", func(c *gin.Context) {
		output := cfg.Clone()
		output.HotStoreCount = options.GetHotStoreCount()
		output.FlowUpdateRatio = options.GetFlowUpdateRatio()
		output.LeaderUpdateRatio = options.GetLeaderUpdateRatio()
		output.EpochUpdateRatio = options.GetEpochUpdateRatio()
		output.SpaceUpdateRatio = options.GetSpaceUpdateRatio()
		output.ReportRatio = options.GetReportRatio()

		c.IndentedJSON(http.StatusOK, output)
	})
	engine.GET("metrics-collect", func(c *gin.Context) {
		second := c.Query("second")
		if second == "" {
			c.String(http.StatusBadRequest, "missing second")
			return
		}
		secondInt, err := strconv.Atoi(second)
		if err != nil {
			c.String(http.StatusBadRequest, "invalid second")
			return
		}
		metrics.CollectMetrics(metrics.WarmUpRound, time.Duration(secondInt)*time.Second)
		c.IndentedJSON(http.StatusOK, "Successfully collect metrics")
	})

	engine.Run(cfg.StatusAddr)
}

func loadTLSConfig(cfg *config.Config) *tls.Config {
	if len(cfg.Security.CAPath) == 0 {
		return nil
	}
	caData, err := os.ReadFile(cfg.Security.CAPath)
	if err != nil {
		log.Error("fail to read ca file", zap.Error(err))
	}
	certData, err := os.ReadFile(cfg.Security.CertPath)
	if err != nil {
		log.Error("fail to read cert file", zap.Error(err))
	}
	keyData, err := os.ReadFile(cfg.Security.KeyPath)
	if err != nil {
		log.Error("fail to read key file", zap.Error(err))
	}

	tlsConf, err := tlsutil.TLSConfig{
		SSLCABytes:   caData,
		SSLCertBytes: certData,
		SSLKEYBytes:  keyData,
	}.ToTLSConfig()
	if err != nil {
		log.Fatal("failed to load tlc config", zap.Error(err))
	}

	return tlsConf
}
