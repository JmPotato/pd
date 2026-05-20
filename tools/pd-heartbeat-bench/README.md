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

Build:

```shell
make pd-heartbeat-bench
```
