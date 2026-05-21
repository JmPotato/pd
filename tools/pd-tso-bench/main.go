// Copyright 2017 TiKV Project Authors.
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
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/influxdata/tdigest"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = 3 * time.Second
)

var (
	pdAddrs                        = flag.String("pd", "127.0.0.1:2379", "pd address")
	clientNumber                   = flag.Int("client", 1, "the number of pd clients involved in each benchmark")
	concurrency                    = flag.Int("c", 1000, "concurrency")
	count                          = flag.Int("count", 1, "the count number that the test will run")
	duration                       = flag.Duration("duration", 60*time.Second, "how many seconds the test will last")
	dcLocation                     = flag.String("dc", "global", "which dc-location this bench will request")
	verbose                        = flag.Bool("v", false, "output statistics info every interval and output metrics info at the end")
	interval                       = flag.Duration("interval", time.Second, "interval to output the statistics")
	caPath                         = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs")
	certPath                       = flag.String("cert", "", "path of file that contains X509 certificate in PEM format")
	keyPath                        = flag.String("key", "", "path of file that contains X509 key in PEM format")
	maxBatchWaitInterval           = flag.Duration("batch-interval", 0, "the max batch wait interval")
	enableTSOFollowerProxy         = flag.Bool("enable-tso-follower-proxy", false, "whether enable the TSO Follower Proxy")
	enableFaultInjection           = flag.Bool("enable-fault-injection", false, "whether enable fault injection")
	faultInjectionRate             = flag.Float64("fault-injection-rate", 0.01, "the failure rate [0.0001, 1]. 0.01 means 1% failure rate")
	maxTSOSendIntervalMilliseconds = flag.Int("max-send-interval-ms", 0, "max tso send interval in milliseconds, 60s by default")
	logicalTSOQPS                  = flag.Int("logical-tso-qps", 0, "aggregate logical TSO qps target in direct stream mode")
	tidbClientInstances            = flag.Int("tidb-client-instances", 1, "TiDB client instance count in direct stream mode")
	tsoStreamsPerClient            = flag.Int("tso-streams-per-client", 1, "TSO streams per TiDB client in direct stream mode")
	maxTotalTSOStreams             = flag.Int("max-total-tso-streams", 0, "maximum total TSO streams in direct stream mode")
	directStreamMode               = flag.Bool("direct-stream-mode", false, "use long-lived raw PD.Tso streams instead of pd.Client.GetLocalTS")
	keyspaceID                     = flag.Uint("keyspace-id", 0, "the id of the keyspace to access")
	keyspaceName                   = flag.String("keyspace-name", "", "the name of the keyspace to access")
	useTSOServerProxy              = flag.Bool("use-tso-server-proxy", false, "whether send tso requests to tso server proxy instead of tso service directly")
	statusAddr                     = flag.String("status-addr", "0.0.0.0:20182", "status address that exposes /metrics for client-perceived reconnect/error counters (v3.1)")
	wg                             sync.WaitGroup
)

var promServer *httptest.Server

// v3.1 (2026-05-21): expose client-perceived stream reconnect + error counters
// so ops_tail_errs_sidecar.py can join them with operator events at collect time.
var (
	tsoStreamReconnects = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "pd_tso_bench",
			Name:      "stream_reconnects_total",
			Help:      "Direct TSO stream reconnect count (sum across all streams).",
		})
	tsoStreamErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "pd_tso_bench",
			Name:      "stream_errors_total",
			Help:      "Direct TSO stream error count (sum across all streams).",
		})
	tsoStreamSuccess = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "pd_tso_bench",
			Name:      "stream_success_total",
			Help:      "Direct TSO logical TS successfully obtained.",
		})
)

func init() {
	prometheus.MustRegister(tsoStreamReconnects)
	prometheus.MustRegister(tsoStreamErrors)
	prometheus.MustRegister(tsoStreamSuccess)
}

type tsoStreamConfig struct {
	DirectStreamMode    bool
	LogicalTSOQPS       int
	TiDBClientInstances int
	TSOStreamsPerClient int
	MaxTotalTSOStreams  int
}

func tsoStreamConfigFromFlags() tsoStreamConfig {
	return tsoStreamConfig{
		DirectStreamMode:    *directStreamMode,
		LogicalTSOQPS:       *logicalTSOQPS,
		TiDBClientInstances: *tidbClientInstances,
		TSOStreamsPerClient: *tsoStreamsPerClient,
		MaxTotalTSOStreams:  *maxTotalTSOStreams,
	}
}

func (c tsoStreamConfig) TotalStreams() int {
	if !c.DirectStreamMode {
		return 0
	}
	return c.TiDBClientInstances * c.TSOStreamsPerClient
}

func (c tsoStreamConfig) Validate() error {
	if c.LogicalTSOQPS < 0 {
		return errors.Errorf("logical-tso-qps can not be negative")
	}
	if !c.DirectStreamMode {
		return nil
	}
	if c.TiDBClientInstances <= 0 {
		return errors.Errorf("tidb-client-instances must be positive")
	}
	if c.TSOStreamsPerClient <= 0 {
		return errors.Errorf("tso-streams-per-client must be positive")
	}
	if c.MaxTotalTSOStreams > 0 && c.TotalStreams() > c.MaxTotalTSOStreams {
		return errors.Errorf("total TSO streams %d exceeds max-total-tso-streams %d", c.TotalStreams(), c.MaxTotalTSOStreams)
	}
	return nil
}

type directTSOStats struct {
	activeStreams atomic.Int64
	successCount  atomic.Int64
	reconnects    atomic.Int64
	errors        atomic.Int64
}

func collectMetrics(server *httptest.Server) string {
	time.Sleep(1100 * time.Millisecond)
	res, _ := http.Get(server.URL)
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	return string(body)
}

func main() {
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-sc
		cancel()
	}()

	// v3.1 (2026-05-21): /metrics status server (covers both direct-stream and
	// classic bench modes). Independent of bench()'s in-process httptest server
	// so ops_tail_errs_sidecar.py can scrape it the whole run.
	if *statusAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			log.Info("tso-bench status server starting", zap.String("addr", *statusAddr))
			if err := http.ListenAndServe(*statusAddr, mux); err != nil {
				log.Warn("tso-bench status server exited", zap.Error(err))
			}
		}()
	}

	for i := range *count {
		fmt.Printf("\nStart benchmark #%d, duration: %+vs\n", i, duration.Seconds())
		if *directStreamMode {
			benchDirectTSO(ctx)
		} else {
			bench(ctx)
		}
	}
}

func benchDirectTSO(mainCtx context.Context) {
	cfg := tsoStreamConfigFromFlags()
	if err := cfg.Validate(); err != nil {
		log.Fatal("invalid direct TSO stream config", zap.Error(err))
	}
	fmt.Printf("Create %d TiDB-like client(s), %d total TSO stream(s), logical TSO qps target: %d\n",
		cfg.TiDBClientInstances, cfg.TotalStreams(), cfg.LogicalTSOQPS)
	pdClients := make([]pd.Client, cfg.TiDBClientInstances)
	for idx := range pdClients {
		pdCli, err := createPDClient(mainCtx)
		if err != nil {
			log.Fatal(fmt.Sprintf("create pd client #%d failed: %v", idx, err))
		}
		pdClients[idx] = pdCli
	}
	defer func() {
		for _, pdCli := range pdClients {
			pdCli.Close()
		}
	}()

	clusterID := pdClients[0].GetClusterID(mainCtx)
	limiter := newLogicalTSOLimiter(cfg.LogicalTSOQPS, cfg.TotalStreams())
	stats := &directTSOStats{}
	ctx, cancel := context.WithCancel(mainCtx)
	defer cancel()
	var streamWG sync.WaitGroup
	for clientIdx, pdCli := range pdClients {
		for streamIdx := 0; streamIdx < cfg.TSOStreamsPerClient; streamIdx++ {
			streamWG.Add(1)
			go runDirectTSOStream(ctx, &streamWG, pdCli, clusterID, limiter, stats, clientIdx, streamIdx)
		}
	}
	statsDone := runDirectTSOStatsPrinter(ctx, stats, *interval, os.Stdout)

	timer := time.NewTimer(*duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
	cancel()
	streamWG.Wait()
	<-statsDone
	fmt.Printf("Direct TSO streams: active=%d success=%d reconnect=%d error=%d\n",
		stats.activeStreams.Load(), stats.successCount.Load(), stats.reconnects.Load(), stats.errors.Load())
}

func runDirectTSOStatsPrinter(ctx context.Context, stats *directTSOStats, interval time.Duration, writer io.Writer) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		var last int64
		for {
			select {
			case <-ticker.C:
				last = writeDirectTSOIntervalStats(writer, stats, last)
			case <-ctx.Done():
				return
			}
		}
	}()
	return done
}

func writeDirectTSOIntervalStats(writer io.Writer, stats *directTSOStats, lastSuccess int64) int64 {
	currentSuccess := stats.successCount.Load()
	fmt.Fprintf(writer, "count:%d, active:%d, reconnect:%d, error:%d\n",
		currentSuccess-lastSuccess,
		stats.activeStreams.Load(),
		stats.reconnects.Load(),
		stats.errors.Load())
	return currentSuccess
}

func newLogicalTSOLimiter(qps int, streams int) *rate.Limiter {
	if qps <= 0 {
		return rate.NewLimiter(rate.Inf, max(1, streams))
	}
	return rate.NewLimiter(rate.Limit(qps), max(1, streams))
}

func runDirectTSOStream(
	ctx context.Context,
	wg *sync.WaitGroup,
	pdCli pd.Client,
	clusterID uint64,
	limiter *rate.Limiter,
	stats *directTSOStats,
	clientIdx int,
	streamIdx int,
) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := runOneDirectTSOStream(ctx, pdCli, clusterID, limiter, stats); err != nil {
			if isExpectedDirectTSOShutdown(ctx, err) {
				return
			}
			stats.reconnects.Add(1)
			stats.errors.Add(1)
			tsoStreamReconnects.Inc()
			tsoStreamErrors.Inc()
			log.Error("direct TSO stream disconnected", zap.Int("client", clientIdx), zap.Int("stream", streamIdx), zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func isExpectedDirectTSOShutdown(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() == nil {
		return false
	}
	if errors.Cause(err) == context.Canceled || errors.Cause(err) == context.DeadlineExceeded {
		return true
	}
	code := status.Code(err)
	return code == codes.Canceled || code == codes.DeadlineExceeded
}

func runOneDirectTSOStream(
	ctx context.Context,
	pdCli pd.Client,
	clusterID uint64,
	limiter *rate.Limiter,
	stats *directTSOStats,
) error {
	conn := pdCli.GetServiceDiscovery().GetServingEndpointClientConn()
	if conn == nil {
		var err error
		conn, err = pdCli.GetServiceDiscovery().GetOrCreateGRPCConn(pdCli.GetServiceDiscovery().GetServingURL())
		if err != nil {
			return err
		}
	}
	stream, err := pdpb.NewPDClient(conn).Tso(ctx)
	if err != nil {
		return err
	}
	stats.activeStreams.Add(1)
	defer stats.activeStreams.Add(-1)
	for {
		if err := limiter.Wait(ctx); err != nil {
			return err
		}
		if err := stream.Send(&pdpb.TsoRequest{
			Header:     &pdpb.RequestHeader{ClusterId: clusterID},
			Count:      1,
			DcLocation: *dcLocation,
		}); err != nil {
			return err
		}
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		count := int64(resp.GetCount())
		stats.successCount.Add(count)
		tsoStreamSuccess.Add(float64(count))
	}
}

func bench(mainCtx context.Context) {
	promServer = httptest.NewServer(promhttp.Handler())

	// Initialize all clients
	fmt.Printf("Create %d client(s) for benchmark\n", *clientNumber)
	pdClients := make([]pd.Client, *clientNumber)
	for idx := range pdClients {
		pdCli, err := createPDClient(mainCtx)
		if err != nil {
			log.Fatal(fmt.Sprintf("create pd client #%d failed: %v", idx, err))
		}
		pdClients[idx] = pdCli
	}

	ctx, cancel := context.WithCancel(mainCtx)
	// To avoid the first time high latency.
	for idx, pdCli := range pdClients {
		_, _, err := pdCli.GetLocalTS(ctx, *dcLocation)
		if err != nil {
			log.Fatal("get first time tso failed", zap.Int("client-number", idx), zap.Error(err))
		}
	}

	durCh := make(chan time.Duration, 2*(*concurrency)*(*clientNumber))

	if *enableFaultInjection {
		fmt.Printf("Enable fault injection, failure rate: %f\n", *faultInjectionRate)
		wg.Add(*clientNumber)
		for i := range *clientNumber {
			go reqWorker(ctx, pdClients, i, durCh)
		}
	} else {
		wg.Add((*concurrency) * (*clientNumber))
		for i := range *clientNumber {
			for range *concurrency {
				go reqWorker(ctx, pdClients, i, durCh)
			}
		}
	}

	wg.Add(1)
	go showStats(ctx, durCh)

	timer := time.NewTimer(*duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
	cancel()

	wg.Wait()

	for _, pdCli := range pdClients {
		pdCli.Close()
	}
}

var latencyTDigest *tdigest.TDigest = tdigest.New()

func showStats(ctx context.Context, durCh chan time.Duration) {
	defer wg.Done()

	statCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	s := newStats()
	total := newStats()

	fmt.Println()
	for {
		select {
		case <-ticker.C:
			// runtime.GC()
			if *verbose {
				fmt.Println(s.counter())
			}
			total.merge(s)
			s = newStats()
		case d := <-durCh:
			s.update(d)
		case <-statCtx.Done():
			fmt.Println("\nTotal:")
			fmt.Println(total.counter())
			fmt.Println(total.percentage())
			// Calculate the percentiles by using the tDigest algorithm.
			fmt.Printf("P0.5: %.4fms, P0.8: %.4fms, P0.9: %.4fms, P0.99: %.4fms\n\n", latencyTDigest.Quantile(0.5), latencyTDigest.Quantile(0.8), latencyTDigest.Quantile(0.9), latencyTDigest.Quantile(0.99))
			if *verbose {
				fmt.Println(collectMetrics(promServer))
			}
			return
		}
	}
}

const (
	twoDur          = time.Millisecond * 2
	fiveDur         = time.Millisecond * 5
	tenDur          = time.Millisecond * 10
	thirtyDur       = time.Millisecond * 30
	fiftyDur        = time.Millisecond * 50
	oneHundredDur   = time.Millisecond * 100
	twoHundredDur   = time.Millisecond * 200
	fourHundredDur  = time.Millisecond * 400
	eightHundredDur = time.Millisecond * 800
	oneThousandDur  = time.Millisecond * 1000
)

type stats struct {
	maxDur          time.Duration
	minDur          time.Duration
	totalDur        time.Duration
	count           int
	submilliCnt     int
	milliCnt        int
	twoMilliCnt     int
	fiveMilliCnt    int
	tenMSCnt        int
	thirtyCnt       int
	fiftyCnt        int
	oneHundredCnt   int
	twoHundredCnt   int
	fourHundredCnt  int
	eightHundredCnt int
	oneThousandCnt  int
}

func newStats() *stats {
	return &stats{
		minDur: time.Hour,
		maxDur: 0,
	}
}

func (s *stats) update(dur time.Duration) {
	s.count++
	s.totalDur += dur
	latencyTDigest.Add(float64(dur.Nanoseconds())/1e6, 1)

	if dur > s.maxDur {
		s.maxDur = dur
	}
	if dur < s.minDur {
		s.minDur = dur
	}

	if dur > oneThousandDur {
		s.oneThousandCnt++
		return
	}

	if dur > eightHundredDur {
		s.eightHundredCnt++
		return
	}

	if dur > fourHundredDur {
		s.fourHundredCnt++
		return
	}

	if dur > twoHundredDur {
		s.twoHundredCnt++
		return
	}

	if dur > oneHundredDur {
		s.oneHundredCnt++
		return
	}

	if dur > fiftyDur {
		s.fiftyCnt++
		return
	}

	if dur > thirtyDur {
		s.thirtyCnt++
		return
	}

	if dur > tenDur {
		s.tenMSCnt++
		return
	}

	if dur > fiveDur {
		s.fiveMilliCnt++
		return
	}

	if dur > twoDur {
		s.twoMilliCnt++
		return
	}

	if dur > time.Millisecond {
		s.milliCnt++
		return
	}

	s.submilliCnt++
}

func (s *stats) merge(other *stats) {
	if s.maxDur < other.maxDur {
		s.maxDur = other.maxDur
	}
	if s.minDur > other.minDur {
		s.minDur = other.minDur
	}

	s.count += other.count
	s.totalDur += other.totalDur
	s.submilliCnt += other.submilliCnt
	s.milliCnt += other.milliCnt
	s.twoMilliCnt += other.twoMilliCnt
	s.fiveMilliCnt += other.fiveMilliCnt
	s.tenMSCnt += other.tenMSCnt
	s.thirtyCnt += other.thirtyCnt
	s.fiftyCnt += other.fiftyCnt
	s.oneHundredCnt += other.oneHundredCnt
	s.twoHundredCnt += other.twoHundredCnt
	s.fourHundredCnt += other.fourHundredCnt
	s.eightHundredCnt += other.eightHundredCnt
	s.oneThousandCnt += other.oneThousandCnt
}

func (s *stats) counter() string {
	return fmt.Sprintf(
		"count: %d, max: %.4fms, min: %.4fms, avg: %.4fms\n<1ms: %d, >1ms: %d, >2ms: %d, >5ms: %d, >10ms: %d, >30ms: %d, >50ms: %d, >100ms: %d, >200ms: %d, >400ms: %d, >800ms: %d, >1s: %d",
		s.count, float64(s.maxDur.Nanoseconds())/float64(time.Millisecond), float64(s.minDur.Nanoseconds())/float64(time.Millisecond), float64(s.totalDur.Nanoseconds())/float64(s.count)/float64(time.Millisecond),
		s.submilliCnt, s.milliCnt, s.twoMilliCnt, s.fiveMilliCnt, s.tenMSCnt, s.thirtyCnt, s.fiftyCnt, s.oneHundredCnt, s.twoHundredCnt, s.fourHundredCnt,
		s.eightHundredCnt, s.oneThousandCnt)
}

func (s *stats) percentage() string {
	return fmt.Sprintf(
		"count: %d, <1ms: %2.2f%%, >1ms: %2.2f%%, >2ms: %2.2f%%, >5ms: %2.2f%%, >10ms: %2.2f%%, >30ms: %2.2f%%, >50ms: %2.2f%%, >100ms: %2.2f%%, >200ms: %2.2f%%, >400ms: %2.2f%%, >800ms: %2.2f%%, >1s: %2.2f%%", s.count,
		s.calculate(s.submilliCnt), s.calculate(s.milliCnt), s.calculate(s.twoMilliCnt), s.calculate(s.fiveMilliCnt), s.calculate(s.tenMSCnt), s.calculate(s.thirtyCnt), s.calculate(s.fiftyCnt),
		s.calculate(s.oneHundredCnt), s.calculate(s.twoHundredCnt), s.calculate(s.fourHundredCnt), s.calculate(s.eightHundredCnt), s.calculate(s.oneThousandCnt))
}

func (s *stats) calculate(count int) float64 {
	return float64(count) * 100 / float64(s.count)
}

func reqWorker(ctx context.Context, pdClients []pd.Client, clientIdx int, durCh chan time.Duration) {
	defer wg.Done()

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		err                    error
		maxRetryTime           int           = 120
		sleepIntervalOnFailure time.Duration = 1000 * time.Millisecond
		totalSleepBeforeGetTS  time.Duration
	)
	pdCli := pdClients[clientIdx]

	for {
		if pdCli == nil || (*enableFaultInjection && shouldInjectFault()) {
			if pdCli != nil {
				pdCli.Close()
			}
			pdCli, err = createPDClient(ctx)
			if err != nil {
				log.Error(fmt.Sprintf("re-create pd client #%d failed: %v", clientIdx, err))
				select {
				case <-reqCtx.Done():
				case <-time.After(100 * time.Millisecond):
				}
				continue
			}
			pdClients[clientIdx] = pdCli
		}

		totalSleepBeforeGetTS = 0
		start := time.Now()

		i := 0
		for ; i < maxRetryTime; i++ {
			var ticker *time.Ticker
			if *maxTSOSendIntervalMilliseconds > 0 {
				sleepBeforeGetTS := time.Duration(rand.Intn(*maxTSOSendIntervalMilliseconds)) * time.Millisecond
				if sleepBeforeGetTS > 0 {
					ticker = time.NewTicker(sleepBeforeGetTS)
					select {
					case <-reqCtx.Done():
					case <-ticker.C:
						totalSleepBeforeGetTS += sleepBeforeGetTS
					}
				}
			}
			_, _, err = pdCli.GetLocalTS(reqCtx, *dcLocation)
			if errors.Cause(err) == context.Canceled {
				if ticker != nil {
					ticker.Stop()
				}

				return
			}
			if err == nil {
				if ticker != nil {
					ticker.Stop()
				}
				break
			}
			log.Error(fmt.Sprintf("%v", err))
			time.Sleep(sleepIntervalOnFailure)
		}
		if err != nil {
			log.Fatal(fmt.Sprintf("%v", err))
		}
		dur := time.Since(start) - time.Duration(i)*sleepIntervalOnFailure - totalSleepBeforeGetTS

		select {
		case <-reqCtx.Done():
			return
		case durCh <- dur:
		}
	}
}

func createPDClient(ctx context.Context) (pd.Client, error) {
	var (
		pdCli pd.Client
		err   error
	)

	opts := make([]pd.ClientOption, 0)
	if *useTSOServerProxy {
		opts = append(opts, pd.WithTSOServerProxyOption(true))
	}
	opts = append(opts, pd.WithGRPCDialOptions(
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
		}),
	))

	if len(*keyspaceName) > 0 {
		apiCtx := pd.NewAPIContextV2(*keyspaceName)
		pdCli, err = pd.NewClientWithAPIContext(ctx, apiCtx, []string{*pdAddrs}, pd.SecurityOption{
			CAPath:   *caPath,
			CertPath: *certPath,
			KeyPath:  *keyPath,
		}, opts...)
	} else {
		pdCli, err = pd.NewClientWithKeyspace(ctx, uint32(*keyspaceID), []string{*pdAddrs}, pd.SecurityOption{
			CAPath:   *caPath,
			CertPath: *certPath,
			KeyPath:  *keyPath,
		}, opts...)
	}
	if err != nil {
		return nil, err
	}

	pdCli.UpdateOption(pd.MaxTSOBatchWaitInterval, *maxBatchWaitInterval)
	pdCli.UpdateOption(pd.EnableTSOFollowerProxy, *enableTSOFollowerProxy)
	return pdCli, err
}

func shouldInjectFault() bool {
	return rand.Intn(10000) < int(*faultInjectionRate*10000)
}
