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

## v2.4 Stagger-Burst & v2.5 Burst-Cycle (2026-05-20)

`stagger-burst` (default `false`): each per-store worker waits
`(store_id - 1) * outer_tick / StoreCount` before its first heartbeat round.
This is a one-shot startup phase offset — once the worker's own ticker is
running the offset is permanent. With smooth-pacing, the result is a
continuous Region-heartbeat arrival pattern at PD that mirrors what online
clusters look like through Grafana's "Region heartbeat report" panel.

`burst-cycle` (default `false`, requires `smooth-heartbeat-pacing = true`):
overlays a wall-clock-phase-aware fraction on each tick's region count to
reproduce the 10-minute collective HIGH/LOW cycle observed on cluster
10989049060142230334. Knobs: `burst-cycle-period-sec` (default 600),
`burst-cycle-high-sec` (60), `burst-cycle-ramp-sec` (60),
`burst-cycle-high-fraction` (0.60), `burst-cycle-low-fraction` (0.31).

These two knobs are orthogonal and compose. They are mutually exclusive with
neither each other nor smooth-pacing (the v2.4 mutex assumption was removed in
v3.1 — see `Adjust` source for the surviving invariants).

## v3.1 Service Discovery & Leader-Aware Reconnect (2026-05-21)

The v3.1 stream model gives every store its own RegionHeartbeat stream and a
mini service-discovery layer so the bench survives PD leader transfers without
silent stalls.

```toml
auto-reconnect = true             # default true
full-resend-on-reconnect = true   # default true
```

**Behaviour on PD leader switch.** When the per-store stream's `Recv()`
returns an error (PD closes the stream during leader transfer), the slot:

1. Backs off 200-500 ms (randomised; prevents 100 stores reconnect-storming
   PD simultaneously).
2. Calls `resolvePDLeader`, which probes each `--pd-endpoints` entry in turn
   with a `GetMembers` RPC, follows the response's `leader.client_urls` to
   the actual leader, and rebuilds the underlying gRPC `ClientConn` against
   the new endpoint.
3. Arms `fullResendNeeded` and pushes a one-shot kick into the worker's
   `kickCh`, causing the worker to run the post-reconnect round **immediately**
   (not at the next tick boundary).
4. The post-reconnect round sends **every** region this store leads, bypassing
   both the burst-cycle fraction and per-region pacing — matching real TiKV's
   behaviour, which fires a full heartbeat the instant the new PD stream is
   ready. The ticker is then reset so the next steady round is one full
   `tickInterval` after the burst.

**Bug fixes vs v3.1.3.** The original v3.1.3 implementation had four
correctness bugs (see
`docs/big-pd-pressure/heartbeat-bench-reconnect-review.md`):

1. `resolvePDLeader` stripped the scheme off the leader's `ClientUrls`
   before passing to `grpcutil.GetClientConn`, which fails URL parsing on
   bare `host:port` — so the "redirect to leader" path always errored out.
   v3.1.4 keeps the full URL throughout and compares via `canonicalEndpoint`.
2. `normalizePDAddr` added scheme only to the head of a comma-separated
   `--pd-endpoints` string, leaving subsequent entries scheme-less. v3.1.4
   normalises each entry independently.
3. `clis[id] = slots[id].cli` captured the boot-time PDClient and never
   updated on reconnect, so `ReportMinResolvedTS` continued targeting the
   ex-leader after a transfer. v3.1.4 reads `slot.GetCli()` at call time.
4. The bucket factory was bound to a single boot-time `cli`. v3.1.4 uses a
   leader-aware factory that re-resolves on stream restart (with a 1 s
   coalescing window so a herd of N workers doesn't storm `GetMembers`).

**Concurrency model.** `send()` and `drainRecv()` race-trigger `reconnect()`
on the same broken stream. `reconnect(observed)` is guarded by pointer
equality (`s.stream != observed → no-op return nil`), so the loser of the
race becomes a free no-op instead of tearing down the freshly-built stream.

**Observability.** The bench exports its own metrics on `status-addr/metrics`
(default `127.0.0.1:20180/metrics`):

| Metric | Labels | Meaning |
| --- | --- | --- |
| `pd_heartbeat_bench_stream_reconnects_total` | `store_id` | Successful reconnects per store. |
| `pd_heartbeat_bench_stream_errors_total` | `store_id`, `phase` (`send`, `reconnect_failed`, `drainrecv_reconnect_failed`) | Send + reconnect failure counts. |
| `pd_heartbeat_bench_full_resend_rounds_total` |  | Total post-reconnect full-resend rounds across the cluster. |

Build:

```shell
make pd-heartbeat-bench
```
