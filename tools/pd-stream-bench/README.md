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

Metrics are exposed at `/metrics` on `status-addr`.
