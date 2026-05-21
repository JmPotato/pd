package config

import (
	"math"
	"strings"
	"sync/atomic"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	flag "github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/utils/configutil"
	"go.uber.org/zap"
)

const (
	defaultStoreCount        = 50
	defaultRegionCount       = 1000000
	defaultHotStoreCount     = 0
	defaultReplica           = 3
	defaultLeaderUpdateRatio = 0.06
	defaultEpochUpdateRatio  = 0.0
	defaultSpaceUpdateRatio  = 0.0
	defaultFlowUpdateRatio   = 0.0
	defaultReportRatio       = 1
	defaultRound             = 0
	defaultSample            = false
	defaultInitialVersion    = 1

	// v2.1 (2026-05-20) content fidelity defaults. Zero means "skip" -> use the legacy
	// 128B / 8keys placeholder behaviour. Non-zero values let synthetic heartbeats populate
	// PD's RegionInfo / RegionStatistics / HotPeerCache long-lived caches, which is required
	// to push leader live heap to the line-cluster 25-40 GB range that triggers lfstack
	// contention and the lease keepalive starvation warn we want to reproduce.
	defaultRegionApproximateSizeMiB     = 0
	defaultRegionApproximateKeys        = 0
	defaultHotRegionRatio               = 0.0
	defaultHotWriteBytesPerRegion       = 0
	defaultHotReadBytesPerRegion        = 0
	defaultHotWriteKeysPerRegion        = 0
	defaultHotReadKeysPerRegion         = 0
	defaultStoreCapacityGiB             = 0 // 0 -> compute from RegionCount * Replica * Size
	defaultBucketsAfterFirstHeartbeat   = true

	// v2.3 (2026-05-20): default is bursty (false) to preserve v2.2 behaviour as
	// observed in pd-stress-20260520-141301. Operators who want to reproduce online's
	// uniform 136k hbs/s arrival pattern instead of bench's 60s-idle + 1s-burst should
	// flip to true and re-run the envelope check (workflow §20.6). NOT the same as
	// raising region-heartbeat-qps — that changes per-region cadence; this keeps
	// per-region cadence == regionReportInterval and only spreads SENDS within the
	// outer-tick window.
	defaultSmoothHeartbeatPacing = false

	// v2.4 (2026-05-20): stagger-burst — round-robin per-store burst windows across
	// the outer tick. Store i starts its burst at offset (i-1) * outer_tick / StoreCount,
	// then bursts all its regions tightly (no inner pacing) and sleeps until the next
	// outer tick. This reproduces real-TiKV behaviour (each store has its own ticker;
	// 100 stores' tickers stagger naturally) and yields "at any instant only ~8 stores
	// active" — leaving the other ~92 stores' gRPC handler goroutines parked, which is
	// the structural prerequisite for idle P → findRunnableGCWorker → lfstack contention
	// seen in online stepdown. Mutually exclusive with smooth-heartbeat-pacing.
	defaultStaggerBurst          = false
	defaultAutoReconnect         = true
	defaultFullResendOnReconnect = true

	// v2.5 (2026-05-20): burst-cycle — overlay a wall-clock-phase-aware fraction on
	// each outer-tick's effective region count, modelling the COLLECTIVE 10-min burst
	// cycle observed on online cluster 10989049060142230334 (verified via Clinic PromQL
	// `pd_scheduler_region_heartbeat{type="report", status="ok"}`):
	//   HIGH segment (60s @ ~80K/s aggregate ingress = 60% of leader_count reported)
	//   ramp_down (60s linear interp HIGH → LOW)
	//   LOW segment (420s @ ~40K/s aggregate = 31% of leader_count reported)
	//   ramp_up (60s linear interp LOW → HIGH)
	// Total cycle 600s. Online stepdown observed at LOW→HIGH transition wall-clock
	// where alloc rate spikes 2x and triggers mark assist → 48 P enter findRunnableGC
	// Worker → lfstack.pop contention → keepAliveWorker starvation.
	// Requires smooth-heartbeat-pacing=true; mutually exclusive with stagger-burst.
	defaultBurstCycle              = false
	defaultBurstCyclePeriodSec     = 600
	defaultBurstCycleHighSec       = 60
	defaultBurstCycleRampSec       = 60
	defaultBurstCycleHighFraction  = 0.60
	defaultBurstCycleLowFraction   = 0.31

	defaultLogFormat = "text"
)

// Config is the heartbeat-bench configuration.
type Config struct {
	flagSet    *flag.FlagSet
	configFile string
	PDAddr     string
	StatusAddr string

	Log      log.Config `toml:"log" json:"log"`
	Logger   *zap.Logger
	LogProps *log.ZapProperties

	Security configutil.SecurityConfig `toml:"security" json:"security"`

	InitEpochVer      uint64  `toml:"epoch-ver" json:"epoch-ver"`
	StoreCount        int     `toml:"store-count" json:"store-count"`
	HotStoreCount     int     `toml:"hot-store-count" json:"hot-store-count"`
	RegionCount       int     `toml:"region-count" json:"region-count"`
	Replica           int     `toml:"replica" json:"replica"`
	LeaderUpdateRatio float64 `toml:"leader-update-ratio" json:"leader-update-ratio"`
	EpochUpdateRatio  float64 `toml:"epoch-update-ratio" json:"epoch-update-ratio"`
	SpaceUpdateRatio  float64 `toml:"space-update-ratio" json:"space-update-ratio"`
	FlowUpdateRatio   float64 `toml:"flow-update-ratio" json:"flow-update-ratio"`
	ReportRatio       float64 `toml:"report-ratio" json:"report-ratio"`
	Sample            bool    `toml:"sample" json:"sample"`
	Round             int     `toml:"round" json:"round"`
	MetricsAddr       string  `toml:"metrics-addr" json:"metrics-addr"`

	ExtraPeerCount int     `toml:"extra-peer-count" json:"extra-peer-count"`
	ExtraPeerRatio float64 `toml:"extra-peer-ratio" json:"extra-peer-ratio"`
	ExtraPeerRole  string  `toml:"extra-peer-role" json:"extra-peer-role"`

	RegionHeartbeatQPS      int `toml:"region-heartbeat-qps" json:"region-heartbeat-qps"`
	StoreHeartbeatQPS       int `toml:"store-heartbeat-qps" json:"store-heartbeat-qps"`
	ReportMinResolvedTSQPS  int `toml:"report-min-resolved-ts-qps" json:"report-min-resolved-ts-qps"`
	ReportBucketsStreams    int `toml:"report-buckets-streams" json:"report-buckets-streams"`
	ReportBucketsIntervalMS int `toml:"report-buckets-interval-ms" json:"report-buckets-interval-ms"`

	// v2.1 (2026-05-20) content fidelity knobs — see big-pd-pressure design doc §2.4 for
	// why these are required to reproduce the line cluster lease keepalive starvation.

	// RegionApproximateSizeMiB / RegionApproximateKeys: initial values written into every
	// synthetic region's heartbeat so PD's RegionInfo carries realistic size/keys. Zero =
	// keep the legacy 128B / 8keys placeholders. Recommended: 96 MiB / 1_000_000 to match
	// TiKV default region.
	RegionApproximateSizeMiB int `toml:"region-approximate-size-mib" json:"region-approximate-size-mib"`
	RegionApproximateKeys    int `toml:"region-approximate-keys" json:"region-approximate-keys"`

	// HotRegionRatio: fraction in [0,1] of regions marked as hot at init() time. Independent
	// of HotStoreCount (which keys hot detection off the leader's store id). When >0 and the
	// region is in updateFlow, the heartbeat uses the HotWrite*/HotRead* values below
	// (interpreted as per-interval bytes/keys, scaled across the regionReportInterval). Drives
	// PD HotPeerCache / LabelStatistics long-lived heap.
	HotRegionRatio         float64 `toml:"hot-region-ratio" json:"hot-region-ratio"`
	HotWriteBytesPerRegion uint64  `toml:"hot-write-bytes-per-region" json:"hot-write-bytes-per-region"`
	HotReadBytesPerRegion  uint64  `toml:"hot-read-bytes-per-region" json:"hot-read-bytes-per-region"`
	HotWriteKeysPerRegion  uint64  `toml:"hot-write-keys-per-region" json:"hot-write-keys-per-region"`
	HotReadKeysPerRegion   uint64  `toml:"hot-read-keys-per-region" json:"hot-read-keys-per-region"`

	// StoreCapacityGiB: per-store capacity to report. Zero means auto-compute as
	// 2 * RegionCount * Replica * RegionApproximateSize / StoreCount so we never wrap on
	// `store.Available -= region.ApproximateSize`. Must be set explicitly when
	// RegionApproximateSizeMiB pushes total simulated cluster size past the legacy 4 TiB cap.
	StoreCapacityGiB int `toml:"store-capacity-gib" json:"store-capacity-gib"`

	// BucketsAfterFirstHeartbeatRound: when true (default), ReportBuckets workers wait for
	// the first full RegionHeartbeat round to complete before sending their first bucket
	// report. Fixes the race where buckets arrive at PD before regions are known (PD logs
	// `the store of the bucket in region is not found` and drops the bucket; 5/18 saw 6,100
	// dropped). Set false to reproduce the legacy racing behaviour.
	BucketsAfterFirstHeartbeatRound bool `toml:"buckets-after-first-heartbeat-round" json:"buckets-after-first-heartbeat-round"`

	// SmoothHeartbeatPacing: when true, each per-store worker spreads its region heartbeat
	// sends uniformly across the outer-ticker interval (with ±10 % jitter so 100 stores
	// don't synchronize-burst). When false (default), each worker sends all its regions in
	// a tight loop on each outer tick — the legacy bench behaviour that produces a ~1 s
	// burst every 60 s, very different from online TiKV's uniform delivery (~136 k hbs/s
	// steady).
	//
	// This is the v2.3 reproduction-envelope knob: bursty mode has 59 s of idle followed
	// by a CPU/alloc-rate spike; online has continuous moderate alloc rate. Switch to true
	// when investigating whether smooth alloc rate matters for lfstack contention
	// reproduction. NOTE: do NOT also raise `region-heartbeat-qps` to "make it smoother" —
	// that changes the per-region cadence to non-online values (5/20 v2 mistake). Keep
	// `region-heartbeat-qps = 0` so the outer ticker stays at 60 s and only pacing
	// distribution changes.
	SmoothHeartbeatPacing bool `toml:"smooth-heartbeat-pacing" json:"smooth-heartbeat-pacing"`

	// StaggerBurst: when true, the N store-workers each delay their burst start by
	// (id-1) * outer_tick / StoreCount so that one store fires every outer_tick/N seconds
	// (e.g. 60s / 100 stores = 600ms). Each worker still bursts its regions tightly once
	// it starts (no per-region pacing). At any instant only ceil(burst_duration / 600ms)
	// stores are active; the remaining stores' RegionHeartbeat handler goroutines on PD
	// are parked, leaving large idle P pools for findRunnableGCWorker during GC mark.
	// This is the closest structural match to real TiKV behaviour. Cannot combine with
	// SmoothHeartbeatPacing (Validate enforces this).
	StaggerBurst bool `toml:"stagger-burst" json:"stagger-burst"`

	// AutoReconnect (v3.1, 2026-05-21): when a per-store RegionHeartbeat stream
	// returns an error (typically io.EOF or "not leader" after PD leader transfer),
	// transparently re-acquire a new stream from the same cli (which internally
	// follows the new leader via service discovery). Without this, the v3.0 worker
	// would return on first send error and silently stop heartbeating that store
	// for the remainder of the run.
	AutoReconnect bool `toml:"auto-reconnect" json:"auto-reconnect"`

	// FullResendOnReconnect (v3.1, 2026-05-21): immediately after AutoReconnect
	// re-acquires a stream, mark the worker so its next round bypasses the
	// burst-cycle fraction trim and pacing and burst-sends ALL regions owned by
	// that store. Mirrors real TiKV behaviour: on PD reconnect event TiKV pushes
	// a full region heartbeat round, producing the post-stepdown spike observed
	// online (e.g. cluster 10989049060142230334 at 14:10 / 14:18 stepdowns).
	FullResendOnReconnect bool `toml:"full-resend-on-reconnect" json:"full-resend-on-reconnect"`

	// BurstCycle (v2.5, 2026-05-20): overlay a wall-clock-phase-aware fraction on each
	// outer-tick's effective region count, reproducing the online 10-min collective
	// burst cycle (see Clinic PromQL on cluster 10989049060142230334). When enabled,
	// each worker computes (wall_clock % BurstCyclePeriodSec) and applies the matching
	// phase's report fraction to its region slice before sending. Requires
	// SmoothHeartbeatPacing=true to spread the (variable) per-tick count uniformly
	// across the outer-tick window; mutex exclusive with StaggerBurst.
	BurstCycle             bool    `toml:"burst-cycle" json:"burst-cycle"`
	BurstCyclePeriodSec    int     `toml:"burst-cycle-period-sec" json:"burst-cycle-period-sec"`
	BurstCycleHighSec      int     `toml:"burst-cycle-high-sec" json:"burst-cycle-high-sec"`
	BurstCycleRampSec      int     `toml:"burst-cycle-ramp-sec" json:"burst-cycle-ramp-sec"`
	BurstCycleHighFraction float64 `toml:"burst-cycle-high-fraction" json:"burst-cycle-high-fraction"`
	BurstCycleLowFraction  float64 `toml:"burst-cycle-low-fraction" json:"burst-cycle-low-fraction"`
}

// NewConfig return a set of settings.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("heartbeat-bench", flag.ContinueOnError)
	fs := cfg.flagSet
	fs.ParseErrorsWhitelist.UnknownFlags = true
	fs.StringVar(&cfg.configFile, "config", "", "config file")
	fs.StringVar(&cfg.PDAddr, "pd-endpoints", "127.0.0.1:2379", "pd address")
	fs.StringVar(&cfg.Log.File.Filename, "log-file", "", "log file path")
	fs.StringVar(&cfg.StatusAddr, "status-addr", "127.0.0.1:20180", "status address")
	fs.StringVar(&cfg.Security.CAPath, "cacert", "", "path of file that contains list of trusted TLS CAs")
	fs.StringVar(&cfg.Security.CertPath, "cert", "", "path of file that contains X509 certificate in PEM format")
	fs.StringVar(&cfg.Security.KeyPath, "key", "", "path of file that contains X509 key in PEM format")
	fs.Uint64Var(&cfg.InitEpochVer, "epoch-ver", 1, "the initial epoch version value")
	fs.IntVar(&cfg.ExtraPeerCount, "extra-peer-count", 0, "extra synthetic peers to add across regions")
	fs.Float64Var(&cfg.ExtraPeerRatio, "extra-peer-ratio", 0, "extra synthetic peer ratio by region count")
	fs.StringVar(&cfg.ExtraPeerRole, "extra-peer-role", "", "extra synthetic peer role: learner or voter")
	fs.IntVar(&cfg.RegionHeartbeatQPS, "region-heartbeat-qps", 0, "aggregate region heartbeat qps target")
	fs.IntVar(&cfg.StoreHeartbeatQPS, "store-heartbeat-qps", 0, "aggregate store heartbeat qps target")
	fs.IntVar(&cfg.ReportMinResolvedTSQPS, "report-min-resolved-ts-qps", 0, "aggregate ReportMinResolvedTS qps target")
	fs.IntVar(&cfg.ReportBucketsStreams, "report-buckets-streams", 0, "active ReportBuckets stream count")
	fs.IntVar(&cfg.ReportBucketsIntervalMS, "report-buckets-interval-ms", 1000, "per-stream ReportBuckets send interval in milliseconds")
	fs.IntVar(&cfg.RegionApproximateSizeMiB, "region-approximate-size-mib", 0, "synthetic region approximate size in MiB; 0 keeps legacy 128B placeholder")
	fs.IntVar(&cfg.RegionApproximateKeys, "region-approximate-keys", 0, "synthetic region approximate keys; 0 keeps legacy 8-keys placeholder")
	fs.Float64Var(&cfg.HotRegionRatio, "hot-region-ratio", 0, "fraction in [0,1] of regions marked hot at init")
	fs.Uint64Var(&cfg.HotWriteBytesPerRegion, "hot-write-bytes-per-region", 0, "BytesWritten per heartbeat interval for hot regions")
	fs.Uint64Var(&cfg.HotReadBytesPerRegion, "hot-read-bytes-per-region", 0, "BytesRead per heartbeat interval for hot regions")
	fs.Uint64Var(&cfg.HotWriteKeysPerRegion, "hot-write-keys-per-region", 0, "KeysWritten per heartbeat interval for hot regions")
	fs.Uint64Var(&cfg.HotReadKeysPerRegion, "hot-read-keys-per-region", 0, "KeysRead per heartbeat interval for hot regions")
	fs.IntVar(&cfg.StoreCapacityGiB, "store-capacity-gib", 0, "per-store capacity in GiB; 0 auto-computes from region count")
	fs.BoolVar(&cfg.BucketsAfterFirstHeartbeatRound, "buckets-after-first-heartbeat-round", defaultBucketsAfterFirstHeartbeat, "gate ReportBuckets workers on first heartbeat round complete (fixes race)")
	fs.BoolVar(&cfg.SmoothHeartbeatPacing, "smooth-heartbeat-pacing", defaultSmoothHeartbeatPacing, "spread per-store region heartbeats uniformly across the outer-tick window with ±10%% jitter (v2.3 experimental); false = legacy bursty")
	fs.BoolVar(&cfg.StaggerBurst, "stagger-burst", defaultStaggerBurst, "stagger per-store startup by (id-1)*outer_tick/StoreCount (v2.4, rewritten v3.1 to be a startup-only phase offset that composes with smooth-pacing and burst-cycle)")
	fs.BoolVar(&cfg.AutoReconnect, "auto-reconnect", defaultAutoReconnect, "re-acquire RegionHeartbeat stream on send error (typically PD leader transfer / not-leader). (v3.1)")
	fs.BoolVar(&cfg.FullResendOnReconnect, "full-resend-on-reconnect", defaultFullResendOnReconnect, "burst-send all regions on the round after a reconnect, bypassing burst-cycle fraction and pacing. Models TiKV leader-transfer-driven full heartbeat resend. Requires --auto-reconnect. (v3.1)")
	fs.BoolVar(&cfg.BurstCycle, "burst-cycle", defaultBurstCycle, "overlay wall-clock-phase-aware fraction on per-tick region count to reproduce online 10-min collective burst cycle (v2.5). Requires smooth-heartbeat-pacing=true.")
	fs.IntVar(&cfg.BurstCyclePeriodSec, "burst-cycle-period-sec", defaultBurstCyclePeriodSec, "burst cycle total period in seconds (v2.5; default 600 = online 10 min)")
	fs.IntVar(&cfg.BurstCycleHighSec, "burst-cycle-high-sec", defaultBurstCycleHighSec, "burst cycle HIGH segment duration in seconds (v2.5; default 60)")
	fs.IntVar(&cfg.BurstCycleRampSec, "burst-cycle-ramp-sec", defaultBurstCycleRampSec, "burst cycle linear ramp duration each side (v2.5; default 60)")
	fs.Float64Var(&cfg.BurstCycleHighFraction, "burst-cycle-high-fraction", defaultBurstCycleHighFraction, "fraction of region count to report during HIGH segment (v2.5; default 0.60)")
	fs.Float64Var(&cfg.BurstCycleLowFraction, "burst-cycle-low-fraction", defaultBurstCycleLowFraction, "fraction of region count to report during LOW segment (v2.5; default 0.31)")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", "127.0.0.1:9090", "the address to pull metrics")

	return cfg
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return errors.WithStack(err)
	}

	// Load config file if specified.
	var meta *toml.MetaData
	if c.configFile != "" {
		meta, err = configutil.ConfigFromFile(c, c.configFile)
		if err != nil {
			return err
		}
	}

	// Parse again to replace with command line options.
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.flagSet.Arg(0))
	}

	c.Adjust(meta)
	return c.Validate()
}

// Adjust is used to adjust configurations
func (c *Config) Adjust(meta *toml.MetaData) {
	if len(c.Log.Format) == 0 {
		c.Log.Format = defaultLogFormat
	}
	c.PDAddr = normalizePDAddr(c.PDAddr)
	if !isDefined(meta, "round") {
		configutil.AdjustInt(&c.Round, defaultRound)
	}

	if !isDefined(meta, "store-count") {
		configutil.AdjustInt(&c.StoreCount, defaultStoreCount)
	}
	if !isDefined(meta, "region-count") {
		configutil.AdjustInt(&c.RegionCount, defaultRegionCount)
	}

	if !isDefined(meta, "hot-store-count") {
		configutil.AdjustInt(&c.HotStoreCount, defaultHotStoreCount)
	}
	if !isDefined(meta, "replica") {
		configutil.AdjustInt(&c.Replica, defaultReplica)
	}

	if !isDefined(meta, "leader-update-ratio") {
		configutil.AdjustFloat64(&c.LeaderUpdateRatio, defaultLeaderUpdateRatio)
	}
	if !isDefined(meta, "epoch-update-ratio") {
		configutil.AdjustFloat64(&c.EpochUpdateRatio, defaultEpochUpdateRatio)
	}
	if !isDefined(meta, "space-update-ratio") {
		configutil.AdjustFloat64(&c.SpaceUpdateRatio, defaultSpaceUpdateRatio)
	}
	if !isDefined(meta, "flow-update-ratio") {
		configutil.AdjustFloat64(&c.FlowUpdateRatio, defaultFlowUpdateRatio)
	}
	if !isDefined(meta, "report-ratio") {
		configutil.AdjustFloat64(&c.ReportRatio, defaultReportRatio)
	}
	if !isDefined(meta, "sample") {
		c.Sample = defaultSample
	}
	if !isDefined(meta, "epoch-ver") {
		c.InitEpochVer = defaultInitialVersion
	}

	if !isDefined(meta, "region-approximate-size-mib") {
		configutil.AdjustInt(&c.RegionApproximateSizeMiB, defaultRegionApproximateSizeMiB)
	}
	if !isDefined(meta, "region-approximate-keys") {
		configutil.AdjustInt(&c.RegionApproximateKeys, defaultRegionApproximateKeys)
	}
	if !isDefined(meta, "hot-region-ratio") {
		configutil.AdjustFloat64(&c.HotRegionRatio, defaultHotRegionRatio)
	}
	if !isDefined(meta, "store-capacity-gib") {
		configutil.AdjustInt(&c.StoreCapacityGiB, defaultStoreCapacityGiB)
	}
	if !isDefined(meta, "buckets-after-first-heartbeat-round") {
		c.BucketsAfterFirstHeartbeatRound = defaultBucketsAfterFirstHeartbeat
	}
	if !isDefined(meta, "smooth-heartbeat-pacing") {
		c.SmoothHeartbeatPacing = defaultSmoothHeartbeatPacing
	}
	if !isDefined(meta, "stagger-burst") {
		c.StaggerBurst = defaultStaggerBurst
	}
	if !isDefined(meta, "auto-reconnect") {
		c.AutoReconnect = defaultAutoReconnect
	}
	if !isDefined(meta, "full-resend-on-reconnect") {
		c.FullResendOnReconnect = defaultFullResendOnReconnect
	}
	if !isDefined(meta, "burst-cycle") {
		c.BurstCycle = defaultBurstCycle
	}
	if !isDefined(meta, "burst-cycle-period-sec") {
		configutil.AdjustInt(&c.BurstCyclePeriodSec, defaultBurstCyclePeriodSec)
	}
	if !isDefined(meta, "burst-cycle-high-sec") {
		configutil.AdjustInt(&c.BurstCycleHighSec, defaultBurstCycleHighSec)
	}
	if !isDefined(meta, "burst-cycle-ramp-sec") {
		configutil.AdjustInt(&c.BurstCycleRampSec, defaultBurstCycleRampSec)
	}
	if !isDefined(meta, "burst-cycle-high-fraction") {
		configutil.AdjustFloat64(&c.BurstCycleHighFraction, defaultBurstCycleHighFraction)
	}
	if !isDefined(meta, "burst-cycle-low-fraction") {
		configutil.AdjustFloat64(&c.BurstCycleLowFraction, defaultBurstCycleLowFraction)
	}
}

func isDefined(meta *toml.MetaData, key string) bool {
	return meta != nil && meta.IsDefined(key)
}

func normalizePDAddr(addr string) string {
	if addr == "" || strings.Contains(addr, "://") {
		return addr
	}
	return "http://" + addr
}

// Validate is used to validate configurations
func (c *Config) Validate() error {
	if c.StoreCount <= 0 {
		return errors.Errorf("store-count must be positive")
	}
	if c.RegionCount <= 0 {
		return errors.Errorf("region-count must be positive")
	}
	if c.Replica <= 0 {
		return errors.Errorf("replica must be positive")
	}
	if c.HotStoreCount < 0 || c.HotStoreCount > c.StoreCount {
		return errors.Errorf("hot-store-count must be in [0, store-count]")
	}
	if c.ReportRatio < 0 || c.ReportRatio > 1 {
		return errors.Errorf("report-ratio must be in [0, 1]")
	}
	if c.LeaderUpdateRatio > c.ReportRatio || c.LeaderUpdateRatio < 0 {
		return errors.Errorf("leader-update-ratio can not be negative or larger than report-ratio")
	}
	if c.EpochUpdateRatio > c.ReportRatio || c.EpochUpdateRatio < 0 {
		return errors.Errorf("epoch-update-ratio can not be negative or larger than report-ratio")
	}
	if c.SpaceUpdateRatio > c.ReportRatio || c.SpaceUpdateRatio < 0 {
		return errors.Errorf("space-update-ratio can not be negative or larger than report-ratio")
	}
	if c.FlowUpdateRatio > c.ReportRatio || c.FlowUpdateRatio < 0 {
		return errors.Errorf("flow-update-ratio can not be negative or larger than report-ratio")
	}
	if c.ExtraPeerCount < 0 {
		return errors.Errorf("extra-peer-count can not be negative")
	}
	if c.ExtraPeerRatio < 0 {
		return errors.Errorf("extra-peer-ratio can not be negative")
	}
	if c.ExtraPeerCount > 0 && c.ExtraPeerRatio > 0 {
		return errors.Errorf("only one of extra-peer-count and extra-peer-ratio can be configured")
	}
	extraPeerCount := c.ExtraPeerCount
	if c.ExtraPeerRatio > 0 {
		extraPeerCount = int(math.Floor(float64(c.RegionCount) * c.ExtraPeerRatio))
	}
	if extraPeerCount > 0 && c.ExtraPeerRole == "" {
		c.ExtraPeerRole = "learner"
	}
	switch c.ExtraPeerRole {
	case "", "learner", "voter":
	default:
		return errors.Errorf("extra-peer-role must be learner or voter")
	}
	maxInt := int(^uint(0) >> 1)
	if c.RegionCount > (maxInt-extraPeerCount)/c.Replica {
		return errors.Errorf("total peer count overflows int")
	}
	if c.RegionHeartbeatQPS < 0 {
		return errors.Errorf("region-heartbeat-qps can not be negative")
	}
	if c.StoreHeartbeatQPS < 0 {
		return errors.Errorf("store-heartbeat-qps can not be negative")
	}
	if c.ReportMinResolvedTSQPS < 0 {
		return errors.Errorf("report-min-resolved-ts-qps can not be negative")
	}
	if c.ReportBucketsStreams < 0 {
		return errors.Errorf("report-buckets-streams can not be negative")
	}
	if c.ReportBucketsIntervalMS < 0 {
		return errors.Errorf("report-buckets-interval-ms can not be negative")
	}
	if c.RegionApproximateSizeMiB < 0 {
		return errors.Errorf("region-approximate-size-mib can not be negative")
	}
	if c.RegionApproximateKeys < 0 {
		return errors.Errorf("region-approximate-keys can not be negative")
	}
	if c.HotRegionRatio < 0 || c.HotRegionRatio > 1 {
		return errors.Errorf("hot-region-ratio must be in [0, 1]")
	}
	// v3.0.3 (2026-05-21): stagger-burst is orthogonal to smooth-pacing and
	// burst-cycle — it only delays each store-worker's start by (i-1)*tick/N.
	// With smooth-pacing on, the worker still spreads its regions uniformly
	// over the outer tick; stagger just shifts the phase across stores so the
	// per-store send pattern matches the online cluster (Grafana "Region
	// heartbeat report" shows clear cross-store phase offset). Original v2.4
	// mutex assumption (stagger == burst-once-per-tick) no longer holds.
	if c.BurstCycle {
		if !c.SmoothHeartbeatPacing {
			return errors.Errorf("burst-cycle requires smooth-heartbeat-pacing=true so the (variable) per-tick region count is spread uniformly across the outer-tick window")
		}
		if c.BurstCyclePeriodSec <= 0 {
			return errors.Errorf("burst-cycle-period-sec must be positive")
		}
		if c.BurstCycleHighSec < 0 || c.BurstCycleRampSec < 0 {
			return errors.Errorf("burst-cycle-high-sec and burst-cycle-ramp-sec must be non-negative")
		}
		if c.BurstCycleHighSec+2*c.BurstCycleRampSec >= c.BurstCyclePeriodSec {
			return errors.Errorf("burst-cycle-high-sec + 2*burst-cycle-ramp-sec must be less than burst-cycle-period-sec (leaves LOW segment >0)")
		}
		if c.BurstCycleHighFraction < 0 || c.BurstCycleHighFraction > 1 {
			return errors.Errorf("burst-cycle-high-fraction must be in [0, 1]")
		}
		if c.BurstCycleLowFraction < 0 || c.BurstCycleLowFraction > 1 {
			return errors.Errorf("burst-cycle-low-fraction must be in [0, 1]")
		}
		if c.BurstCycleLowFraction > c.BurstCycleHighFraction {
			return errors.Errorf("burst-cycle-low-fraction must be <= burst-cycle-high-fraction")
		}
	}
	if c.StoreCapacityGiB < 0 {
		return errors.Errorf("store-capacity-gib can not be negative")
	}
	return nil
}

// Clone creates a copy of current config.
func (c *Config) Clone() *Config {
	cfg := &Config{}
	*cfg = *c
	return cfg
}

// Options is the option of the heartbeat-bench.
type Options struct {
	HotStoreCount atomic.Value
	ReportRatio   atomic.Value

	LeaderUpdateRatio atomic.Value
	EpochUpdateRatio  atomic.Value
	SpaceUpdateRatio  atomic.Value
	FlowUpdateRatio   atomic.Value
}

// NewOptions creates a new option.
func NewOptions(cfg *Config) *Options {
	o := &Options{}
	o.HotStoreCount.Store(cfg.HotStoreCount)
	o.LeaderUpdateRatio.Store(cfg.LeaderUpdateRatio)
	o.EpochUpdateRatio.Store(cfg.EpochUpdateRatio)
	o.SpaceUpdateRatio.Store(cfg.SpaceUpdateRatio)
	o.FlowUpdateRatio.Store(cfg.FlowUpdateRatio)
	o.ReportRatio.Store(cfg.ReportRatio)
	return o
}

// GetHotStoreCount returns the hot store count.
func (o *Options) GetHotStoreCount() int {
	return o.HotStoreCount.Load().(int)
}

// GetLeaderUpdateRatio returns the leader update ratio.
func (o *Options) GetLeaderUpdateRatio() float64 {
	return o.LeaderUpdateRatio.Load().(float64)
}

// GetEpochUpdateRatio returns the epoch update ratio.
func (o *Options) GetEpochUpdateRatio() float64 {
	return o.EpochUpdateRatio.Load().(float64)
}

// GetSpaceUpdateRatio returns the space update ratio.
func (o *Options) GetSpaceUpdateRatio() float64 {
	return o.SpaceUpdateRatio.Load().(float64)
}

// GetFlowUpdateRatio returns the flow update ratio.
func (o *Options) GetFlowUpdateRatio() float64 {
	return o.FlowUpdateRatio.Load().(float64)
}

// GetReportRatio returns the report ratio.
func (o *Options) GetReportRatio() float64 {
	return o.ReportRatio.Load().(float64)
}

// SetOptions sets the option.
func (o *Options) SetOptions(cfg *Config) {
	o.HotStoreCount.Store(cfg.HotStoreCount)
	o.LeaderUpdateRatio.Store(cfg.LeaderUpdateRatio)
	o.EpochUpdateRatio.Store(cfg.EpochUpdateRatio)
	o.SpaceUpdateRatio.Store(cfg.SpaceUpdateRatio)
	o.FlowUpdateRatio.Store(cfg.FlowUpdateRatio)
	o.ReportRatio.Store(cfg.ReportRatio)
}
