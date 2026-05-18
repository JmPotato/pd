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

Build:

```shell
make pd-heartbeat-bench
```
