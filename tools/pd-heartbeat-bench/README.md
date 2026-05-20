# pd-heartbeat-bench

`pd-heartbeat-bench` creates synthetic stores and regions, then keeps TiKV-like heartbeat traffic against PD.

## v2 Stress Options

```toml
store-count = 100
region-count = 8159500
replica = 3

report-min-resolved-ts-qps = 99
report-buckets-streams = 100
report-buckets-interval-ms = 1000

leader-update-ratio = 0.0
```

Optional metadata fidelity run:

```toml
extra-peer-count = 2280000
extra-peer-role = "learner"
```

Rate knobs are aggregate targets. If a QPS knob is `0`, the tool keeps its previous default interval for that reporter.

## v2.1 Content-Fidelity Options (2026-05-20)

Added to reproduce the lease keepalive starvation observed on online clusters where PD
leader live heap reaches 25-40 GB. The v2 baseline produced only ~15 GB live heap because
synthetic regions were 128 B / 8 keys placeholders; PD's RegionInfo / RegionStatistics /
HotPeerCache long-lived caches never grew to the line-cluster size. Without the v2.1
knobs, mark wall-time stays under the 3.33 s lease-keepalive warn threshold and the
phenomenon cannot be reproduced.

```toml
# Initial region content. Zero keeps the legacy 128 B / 8 keys placeholder.
# Recommended: TiKV defaults (96 MiB / 1M keys) — makes PD RegionInfo carry the
# realistic per-region weight.
region-approximate-size-mib = 96
region-approximate-keys = 1000000

# Hot region marking. HotRegionRatio = 0.05 = 5% of regions are flagged hot at init;
# when those regions are in updateFlow each round, the heartbeat uses the per-region
# traffic values below (interpreted as per-interval bytes/keys, jittered ±20%). Hot
# regions outside updateFlow keep their seeded values across rounds so PD's
# HotPeerCache sees a stable hot signal. Composable with the legacy
# `hot-store-count` mechanism (set both to test either path independently).
hot-region-ratio = 0.05
hot-write-bytes-per-region = 16000000
hot-read-bytes-per-region = 2000000
hot-write-keys-per-region = 50000
hot-read-keys-per-region = 10000

# Per-store capacity. 0 = auto-compute as `2 * RegionCount * Replica *
# region-approximate-size / StoreCount`, with the legacy 4 TiB as floor. Must be set
# (or left auto) when `region-approximate-size-mib` lifts simulated cluster size past
# the legacy 4 TiB store cap; otherwise `store.Available -= region.ApproximateSize`
# underflows.
store-capacity-gib = 0

# Bucket-vs-heartbeat race fix. true (default) makes ReportBuckets workers wait for
# the first heartbeat round to complete before sending; false reproduces the legacy
# race that caused PD to drop ~6,100 buckets in the 5/18 run with
# `the store of the bucket in region is not found`. Leave true unless explicitly
# studying the race.
buckets-after-first-heartbeat-round = true
```

CLI flags mirror the toml field names verbatim (e.g.
`--region-approximate-size-mib 96`).

## v2.3 Smooth Heartbeat Pacing (2026-05-20)

`smooth-heartbeat-pacing` (default `false`) controls whether each per-store worker
spreads its region heartbeats uniformly across the outer-tick window or bursts them
on the tick:

```toml
# legacy bursty (default): every outer tick (60s by default), each of the 100 stores
# pushes ALL its regions in a tight loop within milliseconds. Net effect at PD: 59s
# idle + 1s of 8.16M-hb burst. Easy to operate but not what TiKV does.
smooth-heartbeat-pacing = false

# v2.3 smooth pacing: each store spreads its 81.6k sends across the 60s window with
# ±10% jitter per inter-send sleep so 100 stores don't synchronize-burst. Per-region
# delay = outer_tick_interval / regions_for_this_store ≈ 60s / 81600 = 735µs.
# PD sees steady ~136k hbs/s rather than burst pattern. Matches online TiKV.
smooth-heartbeat-pacing = true
```

**Important**: smooth pacing does NOT change the per-region cadence — every region
still heartbeats every `regionReportInterval` (60s). It changes the temporal
distribution of arrivals at PD within each 60s window. This is structurally
different from raising `region-heartbeat-qps` (which changes the per-region
cadence itself and was the 2026-05-20 v2 misstep that crushed online-realistic
GC frequency).

Use case: the big-pd-pressure stress test's reproduction envelope (workflow
§20.5 / §20.6) requires leader CPU 8-15 cores AND mark phase 3-5s, both of
which are hard to hit with bursty mode because the burst transiently spikes CPU
to ~47 cores. v2.3 smooth pacing is the recommended way to reach the envelope
without going through the "raise qps" anti-pattern.

Build:

```shell
make pd-heartbeat-bench
```
