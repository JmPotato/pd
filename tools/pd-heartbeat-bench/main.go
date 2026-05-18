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
	"go.etcd.io/etcd/pkg/v3/report"
	"go.uber.org/zap"
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
	capacity             = 4 * units.TiB
)

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

func newClient(ctx context.Context, cfg *config.Config) (pdpb.PDClient, error) {
	tlsConfig, err := cfg.Security.ToTLSConfig()
	if err != nil {
		return nil, err
	}
	cc, err := grpcutil.GetClientConn(ctx, cfg.PDAddr, tlsConfig)
	if err != nil {
		return nil, err
	}
	return pdpb.NewPDClient(cc), nil
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
}

func (rs *Regions) init(cfg *config.Config) {
	rs.regions = make([]*pdpb.RegionHeartbeatRequest, 0, cfg.RegionCount)
	rs.updateRound = 0

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
			ApproximateSize: bytesUnit,
			Interval: &pdpb.TimeInterval{
				StartTimestamp: now,
				EndTimestamp:   now + regionReportInterval,
			},
			QueryStats:      &pdpb.QueryStats{},
			ApproximateKeys: keysUint,
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
	for _, i := range rs.updateFlow {
		region := rs.regions[i]
		if region.Leader.StoreId <= uint64(options.GetHotStoreCount()) {
			region.BytesWritten = uint64(hotByteUnit * (1 + rand.Float64()) * 60)
			region.BytesRead = uint64(hotByteUnit * (1 + rand.Float64()) * 10)
			region.KeysWritten = uint64(hotKeysUint * (1 + rand.Float64()) * 60)
			region.KeysRead = uint64(hotKeysUint * (1 + rand.Float64()) * 10)
			region.QueryStats = &pdpb.QueryStats{
				Get: uint64(hotQueryUnit * (1 + rand.Float64()) * 10),
				Put: uint64(hotQueryUnit * (1 + rand.Float64()) * 60),
			}
		} else {
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
	for _, i := range reportRegions {
		region := rs.regions[i]
		// reset the statistics of the region which is not updated
		if _, exist := updatedStatisticsMap[i]; !exist {
			region.BytesWritten = 0
			region.BytesRead = 0
			region.KeysWritten = 0
			region.KeysRead = 0
			region.QueryStats = &pdpb.QueryStats{}
		}
		awakenRegions = append(awakenRegions, region)
	}

	rs.awakenRegions.Store(awakenRegions)
}

func createHeartbeatStream(ctx context.Context, cfg *config.Config) (pdpb.PDClient, pdpb.PD_RegionHeartbeatClient) {
	cli, err := newClient(ctx, cfg)
	if err != nil {
		log.Fatal("create client error", zap.Error(err))
	}
	stream, err := cli.RegionHeartbeat(ctx)
	if err != nil {
		log.Fatal("create stream error", zap.Error(err))
	}

	go func() {
		// do nothing
		for {
			stream.Recv()
		}
	}()
	return cli, stream
}

func (rs *Regions) handleRegionHeartbeat(wg *sync.WaitGroup, stream pdpb.PD_RegionHeartbeatClient, storeID uint64, rep report.Report) {
	defer wg.Done()
	var regions, toUpdate []*pdpb.RegionHeartbeatRequest
	updatedRegions := rs.awakenRegions.Load()
	if updatedRegions == nil {
		toUpdate = rs.regions
	} else {
		toUpdate = updatedRegions.([]*pdpb.RegionHeartbeatRequest)
	}
	for _, region := range toUpdate {
		if region.Leader.StoreId != storeID {
			continue
		}
		regions = append(regions, region)
	}

	start := time.Now()
	var err error
	for _, region := range regions {
		err = stream.Send(region)
		rep.Results() <- report.Result{Start: start, End: time.Now(), Err: err}
		if err == io.EOF {
			log.Error("receive eof error", zap.Uint64("store-id", storeID), zap.Error(err))
			err := stream.CloseSend()
			if err != nil {
				log.Error("fail to close stream", zap.Uint64("store-id", storeID), zap.Error(err))
			}
			return
		}
		if err != nil {
			log.Error("send result error", zap.Uint64("store-id", storeID), zap.Error(err))
			return
		}
	}
	log.Info("store finish one round region heartbeat", zap.Uint64("store-id", storeID), zap.Duration("cost-time", time.Since(start)), zap.Int("reported-region-count", len(regions)))
}

// Stores contains store stats with lock.
type Stores struct {
	stat []atomic.Value
}

func newStores(storeCount int) *Stores {
	return &Stores{
		stat: make([]atomic.Value, storeCount+1),
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
	for i := range stats {
		stats[i] = &pdpb.StoreStats{
			StoreId:    uint64(i),
			Capacity:   capacity,
			Available:  capacity,
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
) {
	if streamCount <= 0 {
		return
	}
	if interval <= 0 {
		interval = time.Second
	}
	for i := 0; i < streamCount; i++ {
		workerID := i
		go runReportBucketsWorker(ctx, workerID, interval, regions, status, factory)
	}
}

func runReportBucketsWorker(
	ctx context.Context,
	workerID int,
	interval time.Duration,
	regions *Regions,
	status *bucketReporterStatus,
	factory reportBucketsStreamFactory,
) {
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

func newReportBucketsStreamFactory(cli pdpb.PDClient) reportBucketsStreamFactory {
	return func(ctx context.Context) (reportBucketsClient, error) {
		return cli.ReportBuckets(ctx)
	}
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
	log.Info("finish init regions")
	stores := newStores(cfg.StoreCount)
	stores.update(regions)
	bootstrap(ctx, cli)
	putStores(ctx, cfg, cli, stores)
	log.Info("finish put stores")
	clis := make(map[uint64]pdpb.PDClient, cfg.StoreCount)
	httpCli := pdHttp.NewClient("tools-heartbeat-bench", []string{cfg.PDAddr}, pdHttp.WithTLSConfig(loadTLSConfig(cfg)))
	go deleteOperators(ctx, httpCli)
	streams := make(map[uint64]pdpb.PD_RegionHeartbeatClient, cfg.StoreCount)
	for i := 1; i <= cfg.StoreCount; i++ {
		clis[uint64(i)], streams[uint64(i)] = createHeartbeatStream(ctx, cfg)
	}
	header := &pdpb.RequestHeader{
		ClusterId: clusterID,
	}
	heartbeatTicker := time.NewTicker(intervalForAggregateQPS(cfg.RegionHeartbeatQPS, cfg.RegionCount, regionReportInterval*time.Second))
	defer heartbeatTicker.Stop()
	runMinResolvedTSReporter(ctx, cfg.StoreCount, intervalForAggregateQPS(cfg.ReportMinResolvedTSQPS, cfg.StoreCount, time.Second), func(ctx context.Context, id uint64) error {
		cli := clis[id]
		_, err := cli.ReportMinResolvedTS(ctx, &pdpb.ReportMinResolvedTsRequest{
			Header:        header,
			StoreId:       id,
			MinResolvedTs: uint64(time.Now().Unix()),
		})
		return err
	})
	bucketStatus := newBucketReporterStatus()
	startReportBucketsWorkers(
		ctx,
		cfg.ReportBucketsStreams,
		time.Duration(cfg.ReportBucketsIntervalMS)*time.Millisecond,
		regions,
		bucketStatus,
		newReportBucketsStreamFactory(cli),
	)
	withMetric := metrics.InitMetric2Collect(cfg.MetricsAddr)
	for {
		select {
		case <-heartbeatTicker.C:
			if cfg.Round != 0 && regions.updateRound > cfg.Round {
				exit(0)
			}
			rep := newReport(cfg)
			r := rep.Stats()

			startTime := time.Now()
			wg := &sync.WaitGroup{}
			for i := 1; i <= cfg.StoreCount; i++ {
				id := uint64(i)
				wg.Add(1)
				go regions.handleRegionHeartbeat(wg, streams[id], id, rep)
			}
			if withMetric {
				metrics.CollectMetrics(regions.updateRound, time.Second)
			}
			wg.Wait()

			since := time.Since(startTime).Seconds()
			close(rep.Results())
			regions.result(cfg.RegionCount, since)
			stats := <-r
			log.Info("region heartbeat stats",
				metrics.RegionFields(stats, zap.Uint64("max-epoch-version", maxVersion))...)
			log.Info("store heartbeat stats", zap.String("max", fmt.Sprintf("%.4fs", since)))
			metrics.CollectRegionAndStoreStats(&stats, &since)
			regions.update(cfg, options)
			go stores.update(regions) // update stores in background, unusually region heartbeat is slower than store update.
		case <-ctx.Done():
			log.Info("got signal to exit")
			switch sig {
			case syscall.SIGTERM:
				exit(0)
			default:
				exit(1)
			}
		}
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

func newReport(cfg *config.Config) report.Report {
	p := "%4.4f"
	if cfg.Sample {
		return report.NewReportSample(p)
	}
	return report.NewReport(p)
}

func (rs *Regions) result(regionCount int, sec float64) {
	if rs.updateRound == 0 {
		// There was no difference in the first round
		return
	}

	updated := make(map[int]struct{})
	for _, i := range rs.updateLeader {
		updated[i] = struct{}{}
	}
	for _, i := range rs.updateEpoch {
		updated[i] = struct{}{}
	}
	for _, i := range rs.updateSpace {
		updated[i] = struct{}{}
	}
	for _, i := range rs.updateFlow {
		updated[i] = struct{}{}
	}
	inactiveCount := regionCount - len(updated)

	log.Info("update speed of each category", zap.String("rps", fmt.Sprintf("%.4f", float64(regionCount)/sec)),
		zap.String("save-tree", fmt.Sprintf("%.4f", float64(len(rs.updateLeader))/sec)),
		zap.String("save-kv", fmt.Sprintf("%.4f", float64(len(rs.updateEpoch))/sec)),
		zap.String("save-space", fmt.Sprintf("%.4f", float64(len(rs.updateSpace))/sec)),
		zap.String("save-flow", fmt.Sprintf("%.4f", float64(len(rs.updateFlow))/sec)),
		zap.String("skip", fmt.Sprintf("%.4f", float64(inactiveCount)/sec)))
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
