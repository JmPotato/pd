# pd-tso-bench

pd-tso-bench is a tool to benchmark GetTS performance.

## Build

1. [Go](https://golang.org/) Version 1.23 or later
2. In the root directory of the [PD project](https://github.com/tikv/pd), use the `make` command to compile and generate `bin/pd-tso-bench`

## Usage

This section describes how to benchmark the GetTS performance.

### Flags description

```shell
-c int
  concurrency (default 1000)
-cacert string
  path of file that contains list of trusted SSL CAs
-cert string
  path of file that contains X509 certificate in PEM format
-client int
  the number of pd clients involved in each benchmark (default 1)
-count int
  the count number that the test will run (default 1)
-dc string
  which dc-location this bench will request (default "global")
-duration duration
  how many seconds the test will last (default 1m0s)
-interval duration
  interval to output the statistics (default 1s)
-key string
  path of file that contains X509 key in PEM format
-direct-stream-mode
  use long-lived raw PD.Tso streams instead of pd.Client.GetLocalTS
-logical-tso-qps int
  aggregate logical TSO qps target in direct stream mode
-tidb-client-instances int
  TiDB client instance count in direct stream mode (default 1)
-tso-streams-per-client int
  TSO streams per TiDB client in direct stream mode (default 1)
-max-total-tso-streams int
  maximum total TSO streams in direct stream mode
-pd string
  pd address (default "127.0.0.1:2379")
-v output statistics info every interval and output metrics info at the end
```

Benchmark the GetTS performance:

```shell
    ./pd-tso-bench -v -duration 5s
```

Simulate production-like TSO stream fanout:

```shell
./bin/pd-tso-bench \
  --pd=http://127.0.0.1:2379 \
  --direct-stream-mode \
  --logical-tso-qps=44000 \
  --tidb-client-instances=32 \
  --tso-streams-per-client=5 \
  --max-total-tso-streams=160
```

Direct stream mode prints interval `count:` lines so the aggregate logical TSO QPS can be calculated as `count / interval`. Normal benchmark shutdown does not count canceled streams as reconnects or errors.

It will print some benchmark results like:

```shell
Start benchmark #0, duration: 5s
Create 3 client(s) for benchmark
count:907656, max:3, min:0, >1ms:545903, >2ms:7191, >5ms:0, >10ms:0, >30ms:0 >50ms:0 >100ms:0 >200ms:0 >400ms:0 >800ms:0 >1s:0
count:892034, max:4, min:0, >1ms:585632, >2ms:11359, >5ms:0, >10ms:0, >30ms:0 >50ms:0 >100ms:0 >200ms:0 >400ms:0 >800ms:0 >1s:0
count:909465, max:5, min:0, >1ms:564465, >2ms:9572, >5ms:14, >10ms:0, >30ms:0 >50ms:0 >100ms:0 >200ms:0 >400ms:0 >800ms:0 >1s:0
count:867047, max:6, min:0, >1ms:546294, >2ms:22527, >5ms:1728, >10ms:0, >30ms:0 >50ms:0 >100ms:0 >200ms:0 >400ms:0 >800ms:0 >1s:0
count:482854, max:9, min:0, >1ms:277221, >2ms:162617, >5ms:15097, >10ms:0, >30ms:0 >50ms:0 >100ms:0 >200ms:0 >400ms:0 >800ms:0 >1s:0

Total:
count:4059056, max:9, min:0, >1ms:2519515, >2ms:213266, >5ms:16839, >10ms:0, >30ms:0 >50ms:0 >100ms:0 >200ms:0 >400ms:0 >800ms:0 >1s:0
count:4059056, >1ms:62.07%, >2ms:5.25%, >5ms:0.41%, >10ms:0.00%, >30ms:0.00% >50ms:0.00% >100ms:0.00% >200ms:0.00% >400ms:0.00% >800ms:0.00% >1s:0.00%
```
