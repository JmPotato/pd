# pd-heartbeat-bench v3.1.3 → v3.1.4 review

**Date:** 2026-05-21
**Scope:** service discovery & leader-aware reconnect in `tools/pd-heartbeat-bench`
**Reviewer goal:** verify that the bench correctly simulates TiKV's "trigger a
full heartbeat round after PD leader switch" (全量上报心跳) behaviour against a
real PD cluster.

## TL;DR

The v3.1.3 implementation looked complete (588 LoC change in one commit) but
**could not have worked in any realistic deployment**. The root cause: stripping
the URL scheme off PD's `leader.client_urls` before passing the result back into
`grpcutil.GetClientConn`, which fails at `url.Parse` with `first path segment
in URL cannot contain colon` on bare `host:port` input. Every reconnect that
hit the "leader moved" path therefore failed silently.

Three additional bugs compounded the problem:

- `normalizePDAddr` added scheme only to the head of a comma-separated
  `--pd-endpoints` string, so multi-endpoint inputs always had scheme-less
  tail entries that `resolvePDLeader` couldn't dial.
- The `clis[id] = slots[id].cli` snapshot captured the boot-time PDClient and
  never updated on reconnect, so `ReportMinResolvedTS` continued targeting
  the ex-leader after a transfer.
- The `ReportBuckets` factory closure was bound to a single boot-time `cli`,
  so bucket reporting died on first leader transfer.

Per-region semantics also diverged from real TiKV: the v3.1.1 "full resend"
round trimmed regions by `computeBurstCycleFraction` (e.g. 54 %) instead of
sending all leader-owned regions. The author's own comment justified this with
a calibration to ~68 k ops/min observed online, but that figure is a
**steady-state** measurement, not a post-transfer burst. The bench's stated
purpose (the user prompt was `全量上报心跳`) is to reproduce the transient
post-transfer burst, which on real TiKV pushes the full leader set at line
rate.

This review documents every issue found, the fix landed in v3.1.4, and the
tests that lock the fix in.

## Issues found, by severity

### Critical — production-broken

#### C1. `resolvePDLeader` URL handling

The function probed each endpoint with `grpcutil.GetClientConn`, then on the
"leader is a different peer" path:

```go
leaderURL := resp.Leader.ClientUrls[0]
leaderEp := strings.TrimPrefix(strings.TrimPrefix(leaderURL, "https://"), "http://")
if leaderEp == ep {
    return ep, cc, cli, nil
}
cc.Close()
leaderCc, err := grpcutil.GetClientConn(ctx, leaderEp, tlsConfig)
```

Three defects:

1. `leaderEp` had its scheme stripped, so the comparison `leaderEp == ep`
   never matched when `ep` came from `cfg.PDAddr` (which `normalizePDAddr`
   forces to have a scheme).
2. The "redirect to leader" branch dialed `leaderEp` without a scheme.
   `grpcutil.GetClientConn` does `url.Parse(addr); grpc.DialContext(u.Host)`.
   `url.Parse("10.0.0.1:2379")` returns
   `"first path segment in URL cannot contain colon"`. Every leader-redirect
   call therefore errored; the function fell through to the next endpoint,
   which had the same problem.
3. Practically, every `newStreamSlot` at boot ended up either (a) walking
   every endpoint and returning the final error, or (b) successfully
   "redirecting to itself" with a torn-down conn — neither of which is what
   the implementation intended.

**Fix.** Keep the full `leaderURL` (with scheme) throughout
`resolvePDLeader`. Compare endpoints via a `canonicalEndpoint(s)` helper
that trims whitespace and the trailing `/` but preserves scheme. Add a
3-second `GetMembers` timeout so a stuck peer doesn't block the whole probe
loop.

```go
leaderURL := canonicalEndpoint(resp.GetLeader().GetClientUrls()[0])
if canonicalEndpoint(ep) == leaderURL {
    return leaderURL, cc, cli, nil
}
cc.Close()
leaderCc, err := grpcutil.GetClientConn(ctx, leaderURL, tlsConfig)
```

**Tests.** `TestResolvePDLeaderFirstEndpointIsLeader`,
`TestResolvePDLeaderFollowerRedirectsToLeader`,
`TestResolvePDLeaderFallsThroughOnFirstEndpointDown`,
`TestResolvePDLeaderReturnsErrWhenAllDown`,
`TestCanonicalEndpoint`.

#### C2. `normalizePDAddr` multi-endpoint scheme

`normalizePDAddr` returned the input unchanged if it already contained
`"://"`, and otherwise prefixed `"http://"` to the entire string. With
multi-endpoint input the result was structurally wrong:

| Input | Pre-v3.1.4 output | Post-split |
| --- | --- | --- |
| `127.0.0.1:2379,127.0.0.1:2380` | `http://127.0.0.1:2379,127.0.0.1:2380` | `["http://127.0.0.1:2379", "127.0.0.1:2380"]` |
| `http://127.0.0.1:2379,127.0.0.1:2380` | unchanged | same as above |

The scheme-less tail entries broke `resolvePDLeader` (C1) and `pdHttp`
service discovery (which calls `url.Parse` internally too).

**Fix.** Split on `,`, normalise each entry independently, rejoin. Export
a `Config.SplitEndpoints()` helper so callers iterating endpoints don't
re-implement parsing.

**Tests.** `TestParseNormalizesAllCommaSeparatedEndpoints`,
`TestSplitEndpoints` (in `config/config_test.go`).

#### C3. `ReportMinResolvedTS` stale cli snapshot

```go
clis := make(map[uint64]pdpb.PDClient, cfg.StoreCount)
for i := 1; i <= cfg.StoreCount; i++ {
    slots[id] = newStreamSlot(ctx, cfg, id)
    clis[id] = slots[id].cli  // ← snapshot of boot-time leader
}
```

`slot.cli` mutates on reconnect (when the leader endpoint changes), but
`clis[id]` keeps the boot-time pointer forever. After the first leader
transfer, `ReportMinResolvedTS` continued targeting the ex-leader.

**Fix.** Add `streamSlot.GetCli()` which reads `s.cli` under `s.mu`. Drop
the `clis` map entirely; have the `runMinResolvedTSReporter` callback
dereference `slots[id].GetCli()` at call time.

**Tests.** `TestGetCliReflectsPostReconnectLeader`.

#### C4. `ReportBuckets` factory bound to boot-time cli

The bucket factory closure was constructed once at boot:

```go
newReportBucketsStreamFactory(cli)
```

`cli` here was the single PDClient returned by `newClient` — bound to the
first `--pd-endpoints` entry forever. After leader transfer, every bucket
worker's `Send` errored indefinitely; the worker retry loop kept asking
the factory for a new stream against the (still stale) `cli`, so bucket
reporting silently died.

**Fix.** Introduce `newLeaderAwareReportBucketsStreamFactory(cfg)` which
re-resolves the leader on each factory call (with a 1-second coalescing
window so a herd of N workers reconnecting in lockstep doesn't issue N
concurrent `GetMembers` calls). All bucket workers share a single
leader-bound `*grpc.ClientConn` that the factory mutates on actual leader
move; the old conn is `Close()`d cleanly.

### High — semantic divergence from real TiKV

#### H1. Full-resend trimmed by burst-cycle fraction

`runOneRound` v3.1.1 applied `computeBurstCycleFraction` even when
`fullResend = true`. The author's justification (in the comment):

> Real TiKV after PD reconnect bursts its currently-owned region set, not
> "all regions ever known", so the active-region fraction modelled by
> burst-cycle is still the right population to send. Skipping fraction
> inflated per-store ops/min to ~109 k vs online peak of ~68 k.

This conflates two things. "Currently-owned" means *every* region this
store currently leads (TiKV's RegionInfo cache), not the wall-clock-phase
fraction the bench uses to shape **steady-state** PD ingress. The ~68 k
peak the comment cites is a steady-state measurement; the post-transfer
burst on real TiKV pushes the full leader set at line rate, which is
exactly what the bench is supposed to simulate.

**Fix.** When `fullResend` is true, `runOneRound` uses `rs.regions` (the
full population, not `rs.awakenRegions`), skips the burst-cycle fraction
trim, and sets `perRegionDelay = 0` so the round bursts at line rate.
Steady rounds keep their full shaping envelope.

**Tests.** `TestRunOneRoundFullResendBypassesShaping` asserts (a) all 20
seeded regions sent (vs. 2 in `awakenRegions` or 2 after burst-cycle
trim), (b) round completes in <30 ms (no pacing), (c)
`fullResendNeeded` bit is cleared, (d)
`pd_heartbeat_bench_full_resend_rounds_total` increments.

#### H2. Post-reconnect round waited up to a full tickInterval

`runStoreWorker` v3.1 selected on only `ticker.C` + `ctx.Done`. When a
mid-round reconnect set `fullResendNeeded`, the actual full-resend round
didn't run until the next ticker fire — up to 60 seconds after the
leader transfer event. Real TiKV fires its post-reconnect heartbeat the
instant the new stream is up.

**Fix.** Add `streamSlot.kickCh` (buffered=1). `reconnect()` pushes a
non-blocking signal there after the rebuild. `runStoreWorker` selects on
`{ticker.C, kickCh, ctx.Done}`; on kick, it runs the full-resend round
immediately and calls `ticker.Reset(tickInterval)` so the next steady
round is one full interval after the burst (not bursted-then-ticked-soon-
after-the-burst, which would double-emit).

**Tests.** `TestReconnectSignalsKickChannel`.

### Medium — robustness

#### M1. `send()` and `drainRecv()` double-reconnect race

Both code paths observe stream errors after a leader transfer and call
`reconnect()`. The v3.1.3 `reconnect()` unconditionally closed `s.stream`,
re-resolved leader, rebuilt cli + stream. So in the common race where
`send()` and `drainRecv()` both detect the error within a window, the
second caller tore down the freshly-built stream to make a third one.

**Fix.** Replace the unconditional rebuild with a pointer-equality guard:
`reconnect(observed pdpb.PD_RegionHeartbeatClient) error` returns nil
immediately if `s.stream != observed` (someone else already rebuilt past
this point). Both `send` and `drainRecv` now pass the exact stream they
saw the error on; only the first caller actually rebuilds.

**Tests.** `TestReconnectPointerEqualityGuard`.

#### M2. `log.Fatal` on transient slot setup failure

`newStreamSlot` called `log.Fatal` on any `resolvePDLeader` or initial
`RegionHeartbeat` failure. A single 1-second PD blip at bench startup
crashed the entire 100-store bench.

**Fix.** Mirror `initClusterID`'s 1-second retry loop with a 60-attempt
upper bound. `ctx` cancellation still terminates promptly.

#### M3. `pdHttp.NewClient` got the comma-joined PDAddr as a single endpoint

```go
pdHttp.NewClient("tools-heartbeat-bench", []string{cfg.PDAddr}, ...)
```

`pdHttp.NewClient` takes `[]string` of endpoints and feeds them to PD's
`ServiceDiscovery`. Passing `[]string{"http://ep1,http://ep2"}` made
service discovery treat the comma-joined string as one weird endpoint;
its leader-following degraded to whatever the first character resolved
to.

**Fix.** Pass `cfg.SplitEndpoints()`. PD's `ServiceDiscovery` then
follows leader transfers correctly for the `deleteOperators` HTTP path.

### Low — hygiene

#### L1. Dead code

- `createHeartbeatStream` — replaced by `newStreamSlot` in v3.1 but the
  old function and its tight-looping `for { stream.Recv() }` goroutine
  were left in source. Deleted.
- `newReportBucketsStreamFactory(cli)` — superseded by the leader-aware
  factory. Deleted.
- `(rs *Regions).handleRegionHeartbeat` — superseded by `runOneRound`
  but the old function was kept alive solely to satisfy three smooth-pacing
  unit tests. Tests retargeted to `runOneRound` via a helper
  `makeTestSlot` that builds a `streamSlot` with a mock stream; the
  duplicated implementation is now gone.
- `newStores`, `newReport`, `(*Regions).result` — pre-existing unused
  helpers from the v2 design. Deleted alongside L1 to keep the file
  reviewable.

#### L2. Missing tests for v3.1 reconnect path

The v3.1.3 commit landed 588 LoC of new code with zero new tests. v3.1.4
adds nine: see the per-issue "Tests" callouts.

## Items NOT changed in this pass

- The v2.4 stagger-burst mutex with smooth-pacing was already loosened by
  v3.0.3 (the worker keeps its own ticker after the startup offset).
  Nothing to do.
- The deterministic global `math/rand` seed is intentional for repeatability
  across runs; leaving it alone.
- The bucket factory's GetMembers coalescing window (1 s) is a conservative
  default that prevents 100 workers from storming PD; if reconnect storms
  become a problem in practice we can replace it with a `singleflight`
  group, but for now the simpler implementation is sufficient.

## Verification

```
go build  ./tools/pd-heartbeat-bench/...
go test   ./tools/pd-heartbeat-bench/...           # all pass
go test   -race ./tools/pd-heartbeat-bench/...     # race-clean
```

Specifically the new tests covering the fixes above:

```
TestParseNormalizesAllCommaSeparatedEndpoints/all_bare
TestParseNormalizesAllCommaSeparatedEndpoints/scheme_on_head_only
TestParseNormalizesAllCommaSeparatedEndpoints/mixed_schemes_preserved
TestParseNormalizesAllCommaSeparatedEndpoints/whitespace_trimmed
TestSplitEndpoints/{single,multi,drops_empty,empty_input}
TestResolvePDLeaderFirstEndpointIsLeader
TestResolvePDLeaderFollowerRedirectsToLeader
TestResolvePDLeaderFallsThroughOnFirstEndpointDown
TestResolvePDLeaderReturnsErrWhenAllDown
TestCanonicalEndpoint
TestRunOneRoundFullResendBypassesShaping
TestReconnectPointerEqualityGuard
TestGetCliReflectsPostReconnectLeader
TestReconnectSignalsKickChannel
```

## What this still cannot tell us

These tests use in-memory mock PDs, so they verify the bench's view of the
world (URL handling, retry semantics, pointer-equality guards, factory
contracts). They do **not** verify behaviour against a real PD cluster
undergoing a real leader transfer — that requires running the bench against
the staging cluster and watching the metrics:

- `pd_heartbeat_bench_stream_reconnects_total{store_id=*}` should increment
  on every observed leader transfer (one per store, modulo race coalescing).
- `pd_heartbeat_bench_full_resend_rounds_total` should jump by ≈ store_count
  in the seconds following each transfer.
- PD-side `pd_scheduler_region_heartbeat{type="report",status="ok"}` should
  show the post-transfer burst (the line that the bench is supposed to
  reproduce).

The bench-side counters are exported on `cfg.StatusAddr/metrics` (default
`127.0.0.1:20180/metrics`); ops_tail_errs_sidecar.py joins them with
operator events at collect time.
