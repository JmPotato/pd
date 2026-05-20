# pd-stream-bench

`pd-stream-bench` keeps long-lived PD streams and idle gRPC connections open to reproduce production-like goroutine and connection shape.

Build:

```shell
make pd-stream-bench
```

Capability check:

```shell
./bin/pd-stream-bench \
  --pd=http://127.0.0.1:2379 \
  --metastorage-watch-streams=1 \
  --acquire-token-buckets-streams=1 \
  --etcd-watch-streams=1 \
  --lease-keepalive-streams=1 \
  --check-only
```

`--check-only` opens the configured stream types and verifies etcd watch by writing and observing one temporary key under `/pd-stress/etcd-watch/`.

v2 baseline shape:

```toml
pd = "https://pd-lb.example.com:2379"
status-addr = "127.0.0.1:20181"
cacert = "/path/to/ca.pem"
cert = "/path/to/client.pem"
key = "/path/to/client-key.pem"

metastorage-watch-streams = 330
acquire-token-buckets-streams = 145
etcd-watch-streams = 40
lease-keepalive-streams = 40
connection-fanout-target = 1000
stream-request-interval-ms = 1000
```

## v2.1 Multi-Endpoint Mode (2026-05-20)

`pd` accepts a comma-separated list of endpoints. After parse, the stream workers and
the idle gRPC connection fanout are distributed round-robin (by workerID) across the
listed endpoints. `--check-only` validates each stream type against EVERY endpoint so
a misconfigured follower surfaces immediately instead of mid-run.

```toml
pd = "https://pd0:2379,https://pd1:2379,https://pd2:2379"
metastorage-watch-streams = 330  # 110 per endpoint
acquire-token-buckets-streams = 145  # ~48 per endpoint
etcd-watch-streams = 40  # ~13 per endpoint
lease-keepalive-streams = 40
connection-fanout-target = 1000  # ~333 idle conn per endpoint
```

Single-endpoint configs continue to work unchanged: `pd = "https://pd-lb:2379"` puts
all workers on that address. The distribution matters for the big-pd-pressure stress
test where the v1/v2 single-endpoint setup pushed leader goroutine count past the
5.8 k upper bound (precheck at 5/18 saw 7,317); distributing across 3 PDs matches
the online db-pd-0 / db-pd-1 / db-pd-2 shape (5,300 / 1,200 / 1,500 goroutines
respectively).

Metrics are exposed at `/metrics` on `status-addr`.
