# PD region meta consistency checker

`pd_region_meta_checker.py` 用于检查同一 PD 集群中 Leader 与各 Follower 本地缓存的 region meta 是否一致。脚本通过 HTTP API 直接访问每个 PD 实例，以小批量、全局串行、限速的方式扫描 Region，并输出 JSON 报告。

一致数据在流式比较后立即释放；只有发生差异的 Key Range 片段才会进入自动清理的临时 JSONL，并按 Region ID 做有界外部归并。因此正常路径的内存和临时磁盘占用不随 Region 总数增长。

该脚本以 [`v8.5.4-20260625-8b1b130`](https://github.com/tikv/pd/releases/tag/v8.5.4-20260625-8b1b130) 的 PD HTTP API 行为为基线。用于其他版本前，应先确认本文列出的 API 和 Follower 本地读取能力仍然兼容。

## 检查范围

脚本比较以下 region meta 字段：

- Region ID
- Key Range：`start_key`、`end_key`
- Epoch：`conf_ver`、`version`
- Peers：`id`、`store_id`、`role`、`is_witness`
- Region Leader Peer：`id`、`store_id`、`role`、`is_witness`

脚本不比较流量、大小、`pending_peers`、`down_peers`、Buckets 等心跳统计字段。Peer 在数组中的原始顺序也不参与比较；脚本会先按 Peer 内容排序。

这里的 `leader_peer` 表示某个 Region 当前的 Leader Peer，不是 PD Leader。报告中的 `reference` 只是扫描开始时的 PD Leader 身份信息，不表示其 region meta 一定是正确值，也不会被当作多数派基准。

## 工作方式

1. 从第一个 `PD_URL` 调用 `/pd/api/v1/members`，获取成员、PD Leader、cluster ID 和各成员公布的 `client_urls`。
2. 直接访问每个 PD 实例，扫描前后分别调用 `/pd/api/v1/regions/count`。
3. 通过 `/pd/api/v1/regions/key` 按 Key Range 分页，所有实例共用一个全局限速器，并发固定为 `1`，各实例之间采用 round-robin 顺序。
4. 本地请求携带 `PD-Allow-Follower-Handle: true`、`PD-Redirector: pd-region-meta-checker` 和 `X-Caller-ID: pd-region-meta-checker`，读取目标 PD 实例的本地 Region cache，不发起 GetRegion 类 gRPC 请求。
5. 按各节点 Region 的 Key Range 结束边界对齐流；完全相同的片段立即释放，不写工作文件。
6. 将有差异的片段写入临时 JSONL，以固定大小的内存缓冲分块排序，并用有限 fan-in 归并；临时数据和最终报告分别受硬性容量上限保护。
7. 如果任一实例的“扫描前数量、实际扫描数量、扫描后数量”不一致，本轮失败。只有显式设置 `--scan-retries` 时才会丢弃结果并重扫整个集群。
8. 对差异片段按 Region ID 分组，生成 `missing_on`、`key_range`、`epoch`、`peers` 和 `leader_peer` 差异。
9. 等待固定的 1 秒后，对 Region ID 最小的前 `--confirm-limit` 个差异调用 `/pd/api/v1/region/id/{id}` 二次确认。
10. 再次读取成员信息。如果结束时的 cluster ID、PD Leader ID，或任一成员的 ID、名称、规范化 `client_urls` 与开始时不同，本轮检查失败。该检查只比较开始和结束两个时点，无法发现期间发生后又恢复的变化。

整个过程只发送 HTTP GET 请求，不修改 PD 或集群状态。不过，它仍会消耗 PD 的 HTTP 处理、JSON 序列化、网络和短时 Region tree 读锁资源，因此应遵循本文的线上使用建议。

## 运行要求

- Python 3.8 或更高版本。
- Linux、macOS 等支持 POSIX interval timer 的系统；整轮硬超时依赖 `signal.setitimer`。
- 不需要安装任何 pip 依赖。
- 集群至少包含两个 PD 成员。
- 执行脚本的机器必须能直连每个被使用的 PD `client_url`。
- Follower 的 Region Syncer 必须处于可提供本地 Region 读取的状态。
- 执行机需要能在 `--work-dir` 所在文件系统创建临时目录，并为差异片段和最终报告预留受参数上限约束的空间。
- HTTPS、mTLS 或 API 鉴权集群需要准备相应的 CA、客户端证书或 Authorization Header。

建议在独立运维机上执行，不要默认在 PD 主机上运行，避免检查进程的 CPU、网络和报告 I/O 与 PD Server 争用。

## 快速开始

脚本无需编译。在 PD 仓库根目录执行：

```bash
./tools/pd-region-meta-checker/pd_region_meta_checker.py \
  http://10.0.0.1:2379 \
  --output region-meta-report.json
```

只提供一个 URL 时，该 URL 仅作为 seed。脚本会从 `/members` 发现全部成员，并使用每个成员公布的第一个 `client_urls` 地址，因此这些地址必须能从执行机直接访问。

也可以显式提供所有 PD 地址：

```bash
./tools/pd-region-meta-checker/pd_region_meta_checker.py \
  http://10.0.0.1:2379 \
  http://10.0.0.2:2379 \
  http://10.0.0.3:2379 \
  --output region-meta-report.json
```

提供多个 URL 时，每个 URL 必须与 `/members` 中某个成员公布的 `client_urls` 精确匹配，并且每个成员恰好匹配一个 URL。不能使用额外地址、重复地址或任意的 NAT/LB 映射地址替代成员公布的地址。

报告文件是单行紧凑 JSON，可以使用 `jq` 格式化：

```bash
jq . region-meta-report.json
```

如果不指定 `--output`，JSON 写入 stdout，扫描进度和错误写入 stderr：

```bash
./tools/pd-region-meta-checker/pd_region_meta_checker.py \
  http://10.0.0.1:2379 \
  >region-meta-report.json \
  2>region-meta-checker.log
```

这种 shell 重定向会在脚本启动前截断目标文件，失败时可能留下空文件或不完整 JSON，不具备原子写入保证。线上使用优先选择 `--output region-meta-report.json`；只有 `--output` 才会在完整写完后原子替换目标文件。

## HTTPS、mTLS 和鉴权

使用自签名 CA：

```bash
./tools/pd-region-meta-checker/pd_region_meta_checker.py \
  https://pd-1.example.com:2379 \
  --cacert /path/to/ca.pem \
  --output region-meta-report.json
```

使用 mTLS 时，`--cert` 和 `--key` 必须同时提供：

```bash
./tools/pd-region-meta-checker/pd_region_meta_checker.py \
  https://pd-1.example.com:2379 \
  --cacert /path/to/ca.pem \
  --cert /path/to/client.pem \
  --key /path/to/client-key.pem \
  --output region-meta-report.json
```

需要 HTTP Authorization Header 时，文件中必须包含完整 Header 值，而不只是 token。Authorization Header 只应通过 HTTPS 发送，避免凭据明文传输：

```bash
printf '%s\n' 'Bearer REPLACE_WITH_TOKEN' > /secure/path/pd-authorization
chmod 600 /secure/path/pd-authorization

./tools/pd-region-meta-checker/pd_region_meta_checker.py \
  https://pd-1.example.com:2379 \
  --authorization-file /secure/path/pd-authorization \
  --output region-meta-report.json
```

脚本不支持把用户名或密码嵌入 PD URL，也不提供跳过 TLS 证书校验的选项。

## 参数说明

查看脚本当前支持的参数：

```bash
./tools/pd-region-meta-checker/pd_region_meta_checker.py --help
```

| 参数 | 默认值 | 限制与含义 |
| --- | ---: | --- |
| `PD_URL [PD_URL ...]` | 必填 | 一个或多个 `http://`、`https://` 根 URL。不能包含用户名、密码、Path、Query 或 Fragment。 |
| `--batch-size` | `128` | 每次 `/regions/key` 请求最多读取的 Region 数，范围 `1..1024`。值越小，单次锁和响应越小，但请求总数与运行时间越长。 |
| `--interval` | `0.05` | 任意两次 HTTP 请求之间的最小全局间隔，单位秒，必须大于等于 `0`。该间隔由所有节点共享，不是每节点各自限速。 |
| `--timeout` | `10.0` | 单次 HTTP 请求的 socket timeout，单位秒，必须大于 `0`；不是整个检查的总超时。 |
| `--max-runtime` | `14400` | 从开始发现成员到报告完整写出的整轮 wall-clock 硬上限，单位秒，必须大于 `0`。到期后中断当前请求、清理本轮临时文件并退出 `2`。 |
| `--retries` | `0` | 单个 HTTP 请求的额外重试次数，范围 `0..10`。默认遇到网络错误或 HTTP `429/500/502/503/504` 立即失败，避免在 PD 已有压力时放大请求；只有确认环境有余量后才应显式启用重试。 |
| `--scan-retries` | `0` | Region 数量不稳定时，额外进行的整集群重扫次数，范围 `0..3`。默认不自动重扫，避免请求量意外翻倍。 |
| `--confirm-limit` | `128` | 最多二次确认的差异 Region 数，范围 `0..1024`，按 Region ID 升序选取。`0` 表示禁用确认，不表示无限制。 |
| `--cacert` | 系统 CA | HTTPS CA bundle。省略时使用操作系统默认 CA。 |
| `--cert` | 无 | HTTPS 客户端证书，必须与 `--key` 同时提供。 |
| `--key` | 无 | HTTPS 客户端私钥，必须与 `--cert` 同时提供。 |
| `--authorization-file` | 无 | 仅包含一行完整 Authorization Header 值的文件，例如 `Bearer xxx`。所有 supplied URL 和成员 URL 都必须为 HTTPS，否则请求发出前即失败。 |
| `--work-dir` | 系统临时目录 | 已存在的目录；脚本在其下创建权限隔离且自动清理的本轮工作目录。 |
| `--max-temporary-disk-mib` | `1024` | 差异排序及归并所用临时 JSON 数据的硬上限，必须为正整数。包括归并时短暂并存的输入和输出。 |
| `--max-output-mib` | `1024` | 最终 JSON 报告的硬上限，必须为正整数。超过上限时退出 `2`，指定的既有输出文件保持不变。 |
| `--output` | `-` | `-` 表示 stdout；指定文件时，其父目录必须已经存在。脚本先写同目录临时 JSON，再原子替换目标文件；已经存在的目标文件会被覆盖。 |

## 线上参数建议

任何全量 Region HTTP 扫描都会产生非零的 PD CPU、JSON 序列化、网络和短时 Region tree 读锁开销。脚本通过串行、小批量和全局限速限制影响，但不能承诺“零扰动”；如果 PD 没有明确的资源余量，不应运行。

默认参数以降低峰值影响为优先，仅应在生产低峰期、从独立运维机运行：

```bash
--batch-size 128 --interval 0.05 --timeout 10 --max-runtime 14400 --retries 0 --scan-retries 0
```

如果集群负载较高，可以进一步降低单次压力，但运行时间会增加：

```bash
--batch-size 64 --interval 0.1
```

`--interval 0` 只用于隔离测试环境的算法基线，不能用于生产。把 `--batch-size` 提高到 `256/512/1024` 会增大单次锁内扫描和 JSON 序列化突发；即使总时间更短，也不等于线上影响更小。如果扫描期间 Region 变化频繁，应在更稳定的低峰窗口重跑，而不是首先提高 `--scan-retries`。

分页请求数可以粗略估算为：

```text
PD 成员数 × ceil(每个成员的 Region 数 / batch-size)
```

另外还有每轮的 Region count、成员查询，以及最多：

```text
PD 成员数 × min(差异 Region 数, confirm-limit)
```

次二次确认请求。例如 3 个 PD、每个 100 万个 Region、`batch-size=128` 时，分页请求为 23,439 次。默认全局间隔 `0.05s` 仅节流时间约为 19.5 分钟，实际时间还要加上请求延迟、显式整轮重扫和二次确认。

每个响应最大允许 8 MiB。如果命中该限制，应降低 `--batch-size`，而不是绕过响应大小保护。

## 百万 Region 资源评估

### 实测条件

- PD 版本：`v8.5.4-20260625-8b1b130`，commit `8b1b1307f98e2286429d25e44133cd65541b1c63`。
- 集群：TiUP Playground `v1.17.0` 启动 3 个 PD；Heartbeat Bench 注入 3 Store、每个 Region 3 Peer 的连续 Key Range。每个规模都使用全新的 PD 数据目录，在三个实例的本地 `/regions/count` 完全相等后停止 Heartbeat Bench，再开始检查。
- 执行机：AWS EC2 `r7i.4xlarge`，16 vCPU、123 GiB 可见内存、无 swap，Ubuntu 22.04.5、Python 3.10.12；系统盘为 400 GiB gp3、6,000 IOPS、250 MiB/s。脚本和 PD 之间使用 loopback，Heartbeat 注入时间不计入脚本耗时。
- 为隔离脚本自身固定工作量，表中使用 `--batch-size 128 --interval 0 --retries 0 --scan-retries 0`。`--interval 0` 不是生产参数。
- Wall time、user/sys CPU 和最大 RSS 来自 GNU `/usr/bin/time -v`；PD CPU 是三个 PD 进程在检查窗口内 `process_cpu_seconds_total` 增量之和。测试集群没有业务负载，但后台任务仍计入 PD CPU，因此这些值不是生产 SLA。

三个 PD 的 region meta 完全一致时，实测结果如下：

| 每个 PD 的 Region 数 | 每节点批次数 | HTTP 请求数 | Wall time | 脚本 user / sys CPU | 脚本最大 RSS | 一致报告大小 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | 7,813 | 23,447 | 44.59 s | 30.99 / 1.15 s | 22.63 MiB | 约 1.5 KiB |
| 2,000,000 | 15,625 | 46,883 | 89.44 s | 62.56 / 2.03 s | 23.13 MiB | 约 1.5 KiB |
| 4,000,000 | 31,250 | 93,758 | 179.21 s | 125.62 / 3.75 s | 23.00 MiB | 约 1.5 KiB |
| 8,000,000 | 62,500 | 187,508 | 359.54 s | 250.07 / 8.19 s | 23.00 MiB | 1,566 bytes |

一致路径的 HTTP 和 PD 侧观测如下。Response Body 不包含 HTTP Header、TCP、TLS；`go_memstats_sys_bytes` 是三个 PD 在检查前后的合计差值，不是峰值，也不能与业务并发场景直接等同：

| 每个 PD 的 Region 数 | HTTP Response Body | 三个 PD CPU 增量合计 | 三个 PD `sys` 前后差值 | 三个 PD GC 次数增量 |
| ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | 1,331,423,157 bytes（1.24 GiB） | 16.35 s | +0.735 GiB | 2 |
| 2,000,000 | 2,667,009,060 bytes（2.48 GiB） | 42.56 s | +1.798 GiB | 4 |
| 4,000,000 | 5,360,680,938 bytes（4.99 GiB） | 96.10 s | +2.978 GiB | 6 |
| 8,000,000 | 10,763,024,688 bytes（10.02 GiB） | 256.52 s | +3.807 GiB | 9 |

Wall time、请求数和 Response Body 随 Region 数近似线性增长；脚本最大 RSS 保持在 22.63–23.13 MiB，一致路径的 `temporary_disk_peak_bytes` 均为 `0`。8,000,000 Region 注入末段的 Heartbeat Bench RSS 采样约为 7.14 GiB，注入一轮耗时 22 min 52.74 s；检查开始前该进程已经停止。检查前后三个 PD 的 RSS 合计分别约为 32.78 GiB 和 35.77 GiB，8,000,000 Region 的 PD 数据目录约 1.3 GiB。

`--interval 0` 在 8,000,000 Region 测试中形成约 522 HTTP requests/s、28.55 MiB/s Response Body 和三个 PD 合计约 0.71 CPU core 的持续压力，同时观察到 3.807 GiB 的 Go runtime `sys` 增长。这个模式只用于隔离环境压力基线，不能用于生产，也不能用它推断默认限速下的 PD 内存变化。

### 差异输出容量样本

在独立的本地 TiUP 故障注入测试中，PD Leader 有 4,000,000 个 Region，两个 Follower 各有 3,886,373 个，报告包含 113,627 个 `missing_on` 差异。脚本耗时 138.45 s，user/sys CPU 为 99.35/3.57 s，最大 RSS 为 43.45 MiB，JSON 报告为 9,547,423 bytes（9.11 MiB）；128 个差异完成二次确认，其余 113,499 个明确标为未确认。这个样本平均约 84 bytes/差异，但实例名称、地址和差异字段不同会显著改变单条大小，不能把该平均值当成通用容量公式。

隔离测试机还可以使用按请求生成 region meta 的 fixture 检查脚本流式路径；该命令会消耗显著的本机 CPU 和 loopback 流量，不应在 PD 主机上运行：

```bash
python3 tools/pd-region-meta-checker/benchmark_streaming.py 8000000
```

benchmark 使用临时目录保存并校验报告，结束后只在 stdout 输出一行资源指标 JSON。它依赖 Unix `resource` 模块，适用于 Linux 和 macOS。

### 默认限速下的时间和平均负载

对稳定且一致的 3 PD 集群，`batch-size=128`、不重试、不二次确认时，请求数是：

```text
3 × ceil(每个 PD 的 Region 数 / 128) + 8
```

其中 `8` 是扫描前后 6 次 count 和开始/结束 2 次成员查询。默认 `--interval 0.05` 限制所有实例合计最多约 20 个请求/秒；它约束相邻请求开始时间，而不是给每个实例各自 20 QPS。由此得到的确定性时间下界如下，网络延迟、PD 处理时间、重试、差异确认或不稳定数据都会使实际时间更长：

| 每个 PD 的 Region 数 | 基础请求数 | 默认限速时间下界 |
| ---: | ---: | ---: |
| 1,000,000 | 23,447 | 19 min 32.30 s |
| 2,000,000 | 46,883 | 39 min 04.10 s |
| 4,000,000 | 93,758 | 1 h 18 min 07.85 s |
| 8,000,000 | 187,508 | 2 h 36 min 15.35 s |

以 8,000,000 Region 的实测 Body 和 CPU 工作量均匀摊入默认时间下界，三个 PD 合计平均 Body 流量约为 1.095 MiB/s，即每个实例约 0.365 MiB/s；三个 PD 的 CPU 增量合计平均约占一个 CPU core 的 2.74%，脚本 user CPU 平均约占一个 core 的 2.67%。这些只是同一隔离测试工作量的时间摊薄值，不是生产保证：单次 128 Region 的扫描、JSON 序列化和短时读锁峰值不会因为请求间休眠而消失，Go GC 与内存保留也不随请求间隔线性缩放，生产 PD 的 Region 形状、网络和并行业务同样不同。

测试前后的 `heap_alloc`、`heap_inuse` 和 GC 快照会受到 GC 时点及后台任务影响，不能把它们的差值当成扫描的瞬时内存峰值。测试也没有业务流量可用于测量 TSO、调度或 TiDB 请求尾延迟，因此不能据此批准生产执行或声称“对生产无扰动”。代码路径能够证明的是：默认每次 `ScanRegions` 最多在 Region tree 读锁内收集 128 个 `RegionInfo` 指针，随后在锁外序列化完整响应；PD 的瞬时额外内存是 `O(batch + response)`，而不是 `O(Region 总数)`，但具体峰值必须在等价负载的隔离环境中测量。

### 内存、磁盘和输出上界

- 一致路径实测最大 RSS 为 22.63–23.13 MiB，Region 总数从 1,000,000 增加到 8,000,000 时没有按总数增长；每次最多同时处理各实例的一个小批次。
- 差异路径额外使用固定 8 MiB 排序缓冲和有界归并文件。临时差异数据默认硬上限 1 GiB，最终 JSON 默认硬上限 1 GiB；两者位于不同阶段和可能不同文件系统，规划空间时应按最多约 2 GiB 加文件系统余量考虑。
- 一致报告约 1.5 KiB。存在差异时，报告大小由差异 Region 数、实例标识长度以及 `key_range`、`epoch`、`peers`、`leader_peer` 的实际内容决定，而不是由集群总 Region 数直接决定。
- 达到临时数据或输出上限时，脚本退出 `2`，自动清理本轮工作目录；使用 `--output` 时不会用半成品覆盖已有报告。若业务要求列出超过默认上限的全部差异，必须先在独立运维机上确认磁盘容量，再显式提高上限。

### 生产底线

基于现有 HTTP API，检查每个 PD 的本地 region meta 必须让每个实例扫描并序列化全部 Region，因此不存在“严格零扰动”的全量检查。如果生产要求是任何额外 PD CPU、网络或读锁都不可接受，就不应运行该脚本；需要先实现 PD 侧的有界摘要/校验 API，或在隔离的等价快照上检查。

允许非零但严格受控的诊断开销时，应从独立运维机在业务低峰运行默认参数，并在启动前确认现有 PD/TiDB SLO、告警、Region Syncer 和资源水位正常。运行期间使用现有生产阈值监控 `process_cpu_seconds_total`、`go_memstats_heap_alloc_bytes`、`go_memstats_heap_inuse_bytes`、GC、业务请求延迟以及 `pd_region_syncer_status{type="sync_index"}`/`{type="last_index"}`；任何指标越过既有 SLO 或告警阈值都立即发送 Ctrl-C。不要为了跑完而临时放宽生产阈值、提高批量、取消限速或启用整轮自动重扫。

## 退出码与状态

| 退出码 | JSON `status` | 含义 |
| ---: | --- | --- |
| `0` | `consistent` | 报告保留的数据中没有差异；也可能是初扫发现瞬时差异，但二次确认时已经全部消失。 |
| `1` | `inconsistent` | 至少一个差异的二次读取结果与初扫完全相同，并且报告中仍保留差异。 |
| `2` | `incomplete` 或无 JSON | 差异未获得稳定证据，或出现参数、网络、TLS、API、内存不足、JSON 输出、成员变化、扫描不稳定等错误。 |
| `130` | 无 JSON | 用户通过 Ctrl-C 中断。 |

自动化调用时不要把退出码 `1` 当成脚本执行失败，它表示检查成功并确认存在不一致。示例：

```bash
set +e
./tools/pd-region-meta-checker/pd_region_meta_checker.py \
  http://10.0.0.1:2379 \
  --output region-meta-report.json
rc=$?
set -e

case "$rc" in
  0) echo "region meta is consistent" ;;
  1) echo "stable region meta differences found" ;;
  2) echo "check incomplete or failed" ;;
  130) echo "check interrupted" ;;
  *) echo "unexpected exit code: $rc" ;;
esac
```

运行时错误通常只写入 stderr，并且不会生成新的 JSON 报告；例如参数错误、连接失败或扫描结束前成员发生变化。`status=incomplete` 则表示脚本仍然生成了报告，但当前证据不足以判定为一致或稳定不一致。

## 输出结构

实际输出是单行 JSON。下面的示例为了阅读经过 `jq` 格式化。

### 顶层字段

| 字段 | 含义 |
| --- | --- |
| `status` | `consistent`、`inconsistent` 或 `incomplete`。 |
| `generated_at` | 报告生成时间，UTC ISO 8601。 |
| `cluster_id` | `/members` 返回的 PD cluster ID。 |
| `reference` | 扫描开始时的 PD Leader，包括 `name`、`member_id`、`url`；仅用于标识，不代表正确值基准。 |
| `settings` | 实际批量、请求间隔、timeout、固定并发度、确认/文件上限、HTTP 请求与响应 Body 字节数、临时磁盘峰值和快照语义。该对象不包含 `--retries` 和 `--scan-retries`。 |
| `nodes` | 各 PD 实例的名称、ID、URL、PD 角色、扫描 Region 数、批次数、整轮尝试次数和起止时间。PD Leader 排在第一项，其余成员按 member ID 排序。 |
| `confirmation` | 二次确认范围与结果。 |
| `summary` | 最终报告保留的不同 Region 数，以及各差异字段涉及的 Region 数。`by_field` 只包含计数非零的字段。已确认项来自二次读取，未确认项仍来自交错全量扫描。 |
| `differences` | 按 Region ID 升序排列的全部报告保留差异。`--confirm-limit` 只限制二次确认数量，不限制这里输出的差异数量；超过确认上限的条目没有在扫描结束时刷新。 |

`settings.http_requests` 包含实际发生的重试、成员查询、count、分页和二次确认请求。`http_response_bytes` 只统计 HTTP Response Body，不包含 Header、TCP、TLS 等协议开销。`temporary_disk_peak_bytes` 统计差异排序/归并工作文件，不包含最终报告在原子写入期间使用的同目录临时文件。

### `confirmation.result`

| 值 | 含义 |
| --- | --- |
| `not_needed` | 初扫没有差异，不需要二次确认。 |
| `stable` | 至少一个已检查差异在延迟 1 秒后的二次读取结果与初扫完全相同。此时可能仍有超过 `confirm-limit`、尚未确认的差异。 |
| `resolved` | 所有初始差异都已检查，并且二次检查时全部消失。 |
| `confirmation_disabled` | `--confirm-limit=0`，存在差异但没有执行二次确认，最终状态为 `incomplete`。 |
| `changed_during_recheck` | 已检查差异发生变化，且没有足够稳定证据；也可能仍有超过确认上限的差异，最终状态为 `incomplete`。 |

`confirmation` 始终包含 `initial_differences`、`limit`、`delay_seconds`、`result` 和 `final_differences`。执行过二次确认时，还会按情况包含：

`final_differences` 是二次确认覆盖已检查条目之后仍保留的差异数量；未确认条目仍是初扫观察值。

- `checked_regions`：实际二次检查的 Region 数。
- `unconfirmed_regions`：超过 `confirm-limit`、未二次检查的 Region 数。
- `stable_regions`：二次检查后与初扫差异完全相同的 Region ID。
- `resolved_regions`：二次检查后差异消失的 Region ID。
- `changed_regions`：仍有差异，但内容相对初扫发生变化的 Region ID。

### 差异字段

每个 `differences` 元素一定包含 `region_id`，并且只包含真正不同的字段：

| 字段 | 结构与含义 |
| --- | --- |
| `missing_on` | 缺少该 Region 的实例数组，例如 `pd-2@10.0.0.2:2379`。 |
| `key_range` | 每个现存实例的 `start_key`、`end_key`。Key 使用大写十六进制，空字符串表示无界。 |
| `epoch` | 每个现存实例的 `conf_ver`、`version`。 |
| `peers` | 每个现存实例的完整 Peer 数组，包含全部 Peer ID、Store ID、数值 Role 和 Witness 状态。 |
| `leader_peer` | 每个现存实例的 Region Leader Peer，可能为 `null`。 |

如果 Region 在某个实例上缺失，该实例只会出现在 `missing_on` 中，不会出现在该 Region 的其他字段 Map 中。实例键的格式固定为：

```text
<PD member name>@<client URL host:port>
```

Peer `role` 使用 kvproto 的数值：

| 数值 | 含义 |
| ---: | --- |
| `0` | Voter |
| `1` | Learner |
| `2` | IncomingVoter |
| `3` | DemotingVoter |

## 输出样例

### 一致

```json
{
  "status": "consistent",
  "generated_at": "2026-08-04T02:00:00+00:00",
  "cluster_id": 7319927181134452011,
  "reference": {
    "name": "pd-0",
    "member_id": 1001,
    "url": "http://10.0.0.1:2379"
  },
  "settings": {
    "batch_size": 128,
    "request_interval_seconds": 0.05,
    "request_timeout_seconds": 10.0,
    "max_runtime_seconds": 14400.0,
    "global_concurrency": 1,
    "confirmation_limit": 128,
    "temporary_disk_limit_mib": 1024,
    "output_limit_mib": 1024,
    "http_requests": 22,
    "http_response_bytes": 445000,
    "temporary_disk_peak_bytes": 0,
    "snapshot_semantics": "bounded round-robin scans; differences are rechecked, but the result is not an atomic snapshot"
  },
  "nodes": [
    {
      "name": "pd-0",
      "member_id": 1001,
      "url": "http://10.0.0.1:2379",
      "role": "leader",
      "region_count": 1000,
      "batches": 8,
      "scan_attempts": 1,
      "started_at": "2026-08-04T01:59:58+00:00",
      "finished_at": "2026-08-04T01:59:59+00:00"
    },
    {
      "name": "pd-1",
      "member_id": 1002,
      "url": "http://10.0.0.2:2379",
      "role": "follower",
      "region_count": 1000,
      "batches": 8,
      "scan_attempts": 1,
      "started_at": "2026-08-04T01:59:58+00:00",
      "finished_at": "2026-08-04T02:00:00+00:00"
    }
  ],
  "confirmation": {
    "initial_differences": 0,
    "limit": 128,
    "delay_seconds": 1.0,
    "result": "not_needed",
    "final_differences": 0
  },
  "summary": {
    "different_regions": 0,
    "by_field": {}
  },
  "differences": []
}
```

### 稳定不一致

下面是从完整报告中提取 `{status, confirmation, summary, differences}` 后的样例。Region 42 的 Epoch 和 Peers 不一致，Region 43 在 `pd-1` 上缺失；确认上限为 1，因此报告仍列出两个差异，但只确认了 Region 42。Region 43 是初扫观察值，没有在扫描结束时重新读取。

```json
{
  "status": "inconsistent",
  "confirmation": {
    "initial_differences": 2,
    "limit": 1,
    "delay_seconds": 1.0,
    "checked_regions": 1,
    "unconfirmed_regions": 1,
    "stable_regions": [42],
    "resolved_regions": [],
    "changed_regions": [],
    "result": "stable",
    "final_differences": 2
  },
  "summary": {
    "different_regions": 2,
    "by_field": {
      "missing_on": 1,
      "epoch": 1,
      "peers": 1
    }
  },
  "differences": [
    {
      "region_id": 42,
      "epoch": {
        "pd-0@10.0.0.1:2379": {
          "conf_ver": 3,
          "version": 8
        },
        "pd-1@10.0.0.2:2379": {
          "conf_ver": 3,
          "version": 7
        }
      },
      "peers": {
        "pd-0@10.0.0.1:2379": [
          {
            "id": 101,
            "store_id": 1,
            "role": 0,
            "is_witness": false
          }
        ],
        "pd-1@10.0.0.2:2379": [
          {
            "id": 101,
            "store_id": 1,
            "role": 0,
            "is_witness": false
          },
          {
            "id": 102,
            "store_id": 2,
            "role": 0,
            "is_witness": false
          }
        ]
      }
    },
    {
      "region_id": 43,
      "missing_on": [
        "pd-1@10.0.0.2:2379"
      ]
    }
  ]
}
```

### 未完成

`--confirm-limit=0` 会禁用确认。如果存在差异，脚本仍输出全部差异，但状态和退出码分别为 `incomplete`、`2`：

```json
{
  "status": "incomplete",
  "confirmation": {
    "initial_differences": 1,
    "limit": 0,
    "delay_seconds": 1.0,
    "result": "confirmation_disabled",
    "unconfirmed_regions": 1,
    "final_differences": 1
  },
  "summary": {
    "different_regions": 1,
    "by_field": {
      "epoch": 1
    }
  },
  "differences": [
    {
      "region_id": 42,
      "epoch": {
        "pd-0@10.0.0.1:2379": {
          "conf_ver": 3,
          "version": 8
        },
        "pd-1@10.0.0.2:2379": {
          "conf_ver": 3,
          "version": 7
        }
      }
    }
  ]
}
```

## 常用 `jq` 查询

查看状态和汇总：

```bash
jq '{status, summary, confirmation}' region-meta-report.json
```

列出全部不一致 Region ID：

```bash
jq -r '.differences[].region_id' region-meta-report.json
```

只查看缺失 Region：

```bash
jq '.differences[] | select(has("missing_on"))' region-meta-report.json
```

只查看 Epoch 或 Key Range 不一致：

```bash
jq '.differences[] | select(has("epoch") or has("key_range"))' region-meta-report.json
```

只查看 Peer 或 Region Leader Peer 不一致：

```bash
jq '.differences[] | select(has("peers") or has("leader_peer"))' region-meta-report.json
```

查看没有二次确认的差异数量：

```bash
jq '.confirmation.unconfirmed_regions // 0' region-meta-report.json
```

## 正确理解结果

### 不是原子快照

报告表示在一次受限、交错扫描和二次确认窗口内观察到的结果，不是所有 PD 在同一时刻的原子快照。扫描前后 Region 数量相同，也不能排除扫描期间发生“数量不变但 region meta 更新”的情况。

因此：

- `consistent` 表示本轮观察没有留下差异，不等价于线性一致性证明。
- `inconsistent` 表示至少有一个差异的二次读取与初扫完全相同，是较强的不一致证据，但不证明两个读取时点之间从未变化。
- `incomplete` 表示证据不足或检查未完整完成，不能解释为一致。

### `--confirm-limit` 不截断输出

`--confirm-limit` 只限制二次确认请求数量，不限制 `differences` 中列出的 Region 数量。默认情况下，脚本输出全量扫描观察到的所有差异，但只二次确认 Region ID 最小的前 128 个。其余条目仍是 round-robin 全量扫描时的观察值，并非扫描结束时重新读取的值。

退出码 `1` 也不表示所有输出差异都经过确认：只要确认集合中至少存在一个 `stable_regions`，状态就会是 `inconsistent`。应同时查看 `checked_regions`、`unconfirmed_regions`、`stable_regions` 和 `changed_regions`。

如果需要确认更多差异，可以提高 `--confirm-limit`，但这会额外产生最多“增加的确认数 × PD 成员数”个 HTTP 请求。当前最大值为 1024，不能一次确认超过 1024 个 Region。

## 安全与运维注意事项

- 报告包含 cluster ID、PD 名称和地址、Region ID、Key Range、Peer ID、Store ID 等拓扑信息，应按内部诊断数据保护，不要直接发布到公开 Issue、聊天或日志系统。
- Authorization 文件应设置为仅当前用户可读，只通过 HTTPS 将 Header 发送给 PD，并在使用后按组织安全要求保管或删除。
- 建议使用 `--output` 写文件，避免大量差异直接刷到终端或被 shell 日志收集。
- 目标输出目录必须预先创建。指定文件时，脚本只在报告完整生成后原子替换目标文件。
- 一致片段不落盘；差异片段的临时 JSON 峰值取决于差异规模，而不是全量 Region 数。提前检查 `--work-dir` 文件系统空间，并保留 `--max-temporary-disk-mib` 的硬上限。
- `--output=-` 时脚本先在隔离工作目录生成完整 JSON，再复制到 stdout；接收端中断仍可能得到部分流。指定输出路径时，脚本在目标目录写入临时 JSON，完整写入并 `fsync` 后原子替换目标文件。
- 运行期间应避免重启、扩缩容或主动切换 PD Leader。脚本会拒绝结束时与开始时 cluster ID、PD Leader ID 或成员 ID/名称/`client_urls` 不同的检查，但无法发现期间发生后又恢复的变化。
- 单 seed 模式依赖 PD 公布的 `client_urls`。在容器、NAT 或跨网络环境中，这些地址可能无法从执行机访问，应先验证网络可达性。
- 使用负载均衡地址作为多个成员的替代地址会破坏“直连各实例本地 cache”的前提。
- 手工调用这些 API 时如果没有同时携带 `PD-Allow-Follower-Handle: true` 和 `PD-Redirector`，Follower 请求可能由 PD Leader 处理，得到三个节点看似相同的假结果。脚本已对所有本地 cache 请求固定携带这些 Header。
- Follower Region Syncer 不可用或尚未完成同步时，本地读取 API 可能返回 `500/503`。不要把这种情况解释为 region meta 一致。
- 每 100 个扫描批次会向 stderr 输出一次进度；JSON stdout 不会被这些进度信息污染。

## 常见问题

### `direct endpoint for PD member ... is missing or ambiguous`

显式传入的 URL 没有和 `/members` 中每个成员的 `client_urls` 一一匹配。可以改用一个可访问的 seed，让脚本自动发现地址；也可以按 `/members` 返回值传入每个成员的精确 URL。

### `unstable Region set after ... cluster-wide scan attempt(s)`

扫描期间 Region 数量持续变化，所有整轮尝试都未获得稳定数量。等待集群稳定并在业务低峰期使用默认或更保守参数重跑；不要仅为了跑完而增大 `--batch-size`、减小 `--interval` 或增加 `--scan-retries`。如果当前 API 在可接受的负载下始终无法完成，就不能从本工具获得可靠结论。

### `PD membership or leader changed during the scan`

结束时的 cluster ID、PD Leader ID 或任一成员的 ID、名称、规范化 `client_urls` 与开始时不同，本轮结果已被拒绝。等待 PD 拓扑稳定后重新执行。期间发生后又恢复的拓扑变化不在开始/结束签名检查范围内。

### HTTP `401` 或 `403`

检查 Authorization 文件是否包含完整 Header 值，例如 `Bearer <token>`，并确认该凭据可以访问所有成员的 PD HTTP API。只通过 HTTPS 发送 Authorization Header。

### HTTP `500` 或 `503`

确认目标 Follower 已启动并正常运行 Region Syncer，检查 PD 日志和对应 HTTP API 是否可以在携带 `PD-Allow-Follower-Handle: true` 时本地读取。脚本会按 `--retries` 重试，持续失败最终退出 `2`。

### `response exceeds 8 MiB`

降低 `--batch-size` 后重新执行。

### `temporary JSON data exceeds ... MiB`

差异片段及外部归并的临时数据达到硬上限。本轮工作目录会自动清理。先检查差异规模和文件系统可用空间；确需完整输出时，再显式提高 `--max-temporary-disk-mib`。

### `JSON report exceeds ... MiB`

最终报告达到硬上限。指定 `--output` 时，既有报告不会被半成品覆盖。先评估差异数量和输出目录空间，再显式提高 `--max-output-mib`。

### 没有生成 JSON 文件

参数、网络、TLS、API、内存不足、成员变化或 JSON 输出错误会在报告写入前退出，仅在 stderr 打印 `error: ...`。使用 `--output` 时，目标文件只在完整报告生成后替换；使用 `>file` 重定向时，shell 可能已经留下空文件或部分 JSON。检查命令退出码和 stderr，不要把旧文件或不完整文件误认为本轮结果。

## 运行测试

脚本测试使用本地模拟 PD HTTP Server，覆盖一致、缺失 Region、Key Range、Epoch、Peers、Region Leader Peer、Peer 顺序、瞬时差异、数量变化、非有限限速参数和 uint64 边界等场景：

```bash
python3 tools/pd-region-meta-checker/test_pd_region_meta_checker.py -v
```

百万级流式路径使用前文的 `benchmark_streaming.py` 单独复现，避免把长时间资源测试放进日常单元测试。
