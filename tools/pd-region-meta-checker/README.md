# PD region meta consistency checker

`pd_region_meta_checker.py` 用于检查同一 PD 集群中 Leader 与各 Follower 本地缓存的 region meta 是否一致。脚本直接访问每个 PD 实例，以小批量、全局串行和限速方式扫描 Region，输出单个 JSON 报告。

该脚本以 [`v8.5.4-20260625-8b1b130`](https://github.com/tikv/pd/releases/tag/v8.5.4-20260625-8b1b130)（commit `8b1b1307f98e2286429d25e44133cd65541b1c63`）的 HTTP API 行为为基线。用于其他版本前，应确认成员发现、Region 扫描和 Follower 本地读取能力兼容。

> 全量检查一定会增加 PD 的 HTTP 处理、JSON 序列化、网络流量和短时 Region tree 读锁开销。默认参数用于限制而不是消除这些开销；如果生产要求严格零扰动，不应执行本工具。

## 快速执行

脚本无需编译，也不需要安装 pip 依赖。本文验证和基准使用 commit `517fdc0c07bb6b4781be1682a83c52133e6ff709` 中的脚本，其 SHA-256 为：

```text
c74cd62865e0c46880f5df50d0684439573ff99f30b0a7d79df77b5e4f95cd07
```

Linux 上执行以下命令验证脚本，只有输出 `OK` 时才继续：

```bash
script=./tools/pd-region-meta-checker/pd_region_meta_checker.py
expected_sha256=c74cd62865e0c46880f5df50d0684439573ff99f30b0a7d79df77b5e4f95cd07
printf '%s  %s\n' "$expected_sha256" "$script" | sha256sum -c -
```

macOS 使用 `shasum -a 256 "$script"`，并确认输出值相同。校验失败时不要执行，应重新获取与上述 commit 对应的脚本。

建议从独立运维机执行，并为每轮检查使用新的输出文件：

```bash
report="region-meta-report-$(date -u +%Y%m%dT%H%M%SZ).json"
set +e
./tools/pd-region-meta-checker/pd_region_meta_checker.py \
  http://10.0.0.1:2379 \
  --batch-size 128 \
  --interval 0.05 \
  --timeout 10 \
  --max-runtime 14400 \
  --retries 0 \
  --scan-retries 0 \
  --output "$report"
rc=$?
set -e

printf 'exit_code=%s report=%s\n' "$rc" "$report"
test ! -s "$report" || jq '{status, summary, confirmation, nodes}' "$report"
```

一个 URL 仅作为 seed。脚本会通过 `/pd/api/v1/members` 发现所有成员，然后直连每个成员公布的第一个 `client_urls` 地址。也可以显式传入全部地址，但必须和成员公布的地址一一匹配：

```bash
./tools/pd-region-meta-checker/pd_region_meta_checker.py \
  http://10.0.0.1:2379 \
  http://10.0.0.2:2379 \
  http://10.0.0.3:2379 \
  --output region-meta-report.json
```

| 退出码 | 状态 | 处理建议 |
| ---: | --- | --- |
| `0` | `consistent` | 本轮没有保留差异。 |
| `1` | `inconsistent` | 检查成功，并确认存在至少一个稳定差异；按 Region ID 继续排查。 |
| `2` | `incomplete` 或无新报告 | 结果不足以判断一致性；先处理 stderr 中的错误，不要把旧报告当成本轮结果。 |
| `130` | 无新报告 | 用户通过 Ctrl-C 中断。 |

退出码 `1` 表示发现不一致，不是脚本执行失败。

## 执行前检查

开始前确认以下条件：

- 使用 Python 3.8 或更高版本，并运行在支持 POSIX interval timer 的 Linux 或 macOS 上。
- 集群至少有两个 PD 成员；执行机能直连每个成员公布的 `client_urls`，没有经过负载均衡地址。
- Follower Region Syncer 正常，能够处理携带 `PD-Allow-Follower-Handle: true` 的本地读取请求。
- 当前处于业务低峰，PD/TiDB SLO、告警、CPU、内存和业务延迟均有明确余量。
- 扫描窗口内不计划进行 PD 重启、扩缩容或主动 Leader 切换。
- `--work-dir` 和输出目录已存在并具有足够空间；报告按内部诊断数据保护。

推荐独立检查机至少为 `1 vCPU / 512 MiB RAM / 3 GiB 可用磁盘 / 100 Mbps`；更稳妥的配置为 `2 vCPU / 1 GiB RAM / 5 GiB 可用磁盘`。详细容量依据见[资源与容量规划](#资源与容量规划)。

## 检查内容

| 类别 | 比较字段 |
| --- | --- |
| Region | Region ID |
| Key Range | `start_key`、`end_key` |
| Epoch | `conf_ver`、`version` |
| Peers | `id`、`store_id`、`role`、`is_witness` |
| Region Leader Peer | `id`、`store_id`、`role`、`is_witness` |

流量、大小、`pending_peers`、`down_peers`、Buckets 等心跳统计字段不参与比较。Peer 数组顺序也不参与比较，脚本会先按 Peer 内容排序。

报告中的 `reference` 只是扫描开始时的 PD Leader 身份，不代表该节点的数据必然正确，也不会作为多数派基准。`leader_peer` 表示 Region Leader Peer，与 PD Leader 是两个概念。

## 工作原理与结果边界

1. 发现成员、PD Leader 和 cluster ID，创建指向每个 PD 实例的直接 HTTP 连接。
2. 扫描前后分别读取每个实例的 Region 数，通过 `/pd/api/v1/regions/key` 按 Key Range 分页。
3. 所有实例共享一个限速器，并发固定为 `1`，通过 round-robin 交错扫描；请求固定携带 `PD-Allow-Follower-Handle: true`、`PD-Redirector: pd-region-meta-checker` 和 `X-Caller-ID: pd-region-meta-checker`，直接读取目标实例的本地 Region cache，不产生 GetRegion 类 gRPC 请求。
4. 按 Key Range 结束边界对齐各节点数据流。相同片段立即释放；只有差异片段进入有界临时 JSONL，按 Region ID 外部归并排序。
5. 对 Region ID 最小的前 `--confirm-limit` 个差异等待 1 秒后再次读取，并在结束前确认 PD Leader、cluster ID 和成员身份没有变化。

只有某节点的“扫描前数量、实际扫描数量、扫描后数量”相等，本轮扫描才会被接受。默认 `--scan-retries=0`，数量不稳定时立即失败，避免自动重扫放大负载。

本工具提供的是受限交错扫描和差异二次确认，不是原子快照：

- `consistent` 表示本轮观察没有留下差异，不等价于线性一致性证明。
- `inconsistent` 表示至少一个已确认差异在两次读取中保持相同，是较强的不一致证据。
- `incomplete` 表示证据不足，不能解释为一致。

开始和结束时的成员签名检查无法发现期间发生后又恢复的拓扑变化，因此应选择稳定窗口执行。

## 参数参考

完整参数以 `--help` 为准：

```bash
./tools/pd-region-meta-checker/pd_region_meta_checker.py --help
```

| 参数 | 默认值 | 说明 |
| --- | ---: | --- |
| `PD_URL [PD_URL ...]` | 必填 | 一个或多个 `http://`、`https://` 根 URL；不能包含用户名、密码、Path、Query 或 Fragment。 |
| `--batch-size` | `128` | 每次最多读取的 Region 数，范围 `1..1024`。减小可降低单次峰值，但会增加请求数和运行时间。 |
| `--interval` | `0.05` | 所有节点共享的请求起始间隔，必须为非负有限值；默认约束为全局最多 20 requests/s。 |
| `--timeout` | `10` | 单个 HTTP 请求的 socket timeout，单位秒。 |
| `--max-runtime` | `14400` | 整轮检查的 wall-clock 硬上限，单位秒；到期清理临时文件并退出 `2`。 |
| `--retries` | `0` | 单请求额外重试次数，范围 `0..10`。默认不重试，避免在 PD 已有压力时放大请求。 |
| `--scan-retries` | `0` | Region 数变化时的整集群重扫次数，范围 `0..3`。每次重试都会重新扫描全部节点。 |
| `--confirm-limit` | `128` | 二次确认的差异 Region 上限，范围 `0..1024`；`0` 表示禁用确认。 |
| `--work-dir` | 系统临时目录 | 临时差异文件和 stdout 临时报告所在目录，必须已经存在。 |
| `--max-temporary-disk-mib` | `1024` | 差异排序与归并临时数据的硬上限。 |
| `--max-output-mib` | `1024` | 最终 JSON 报告的硬上限。 |
| `--output` | `-` | `-` 表示 stdout；指定路径时完整写入并 `fsync` 后原子替换目标文件。 |
| `--cacert` | 系统 CA | HTTPS CA bundle。 |
| `--cert`、`--key` | 无 | mTLS 客户端证书和私钥，必须同时提供。 |
| `--authorization-file` | 无 | 包含一行完整 Authorization Header 值的文件；只能和 HTTPS 一起使用。 |

`--interval 0` 只用于隔离测试，不能用于生产。不要为了缩短时间而提高 `--batch-size`、取消限速或首先启用 `--scan-retries`；这些操作会增大 PD 瞬时或累计负载。

集群负载较高但仍允许受控诊断开销时，可使用更保守的组合：

```bash
--batch-size 64 --interval 0.1 --retries 0 --scan-retries 0
```

### HTTPS、mTLS 和鉴权

mTLS 示例：

```bash
./tools/pd-region-meta-checker/pd_region_meta_checker.py \
  https://pd-1.example.com:2379 \
  --cacert /secure/ca.pem \
  --cert /secure/client.pem \
  --key /secure/client-key.pem \
  --output region-meta-report.json
```

Authorization 文件必须包含完整 Header 值，而不只是 token：

```bash
printf '%s\n' 'Bearer REPLACE_WITH_TOKEN' > /secure/pd-authorization
chmod 600 /secure/pd-authorization

./tools/pd-region-meta-checker/pd_region_meta_checker.py \
  https://pd-1.example.com:2379 \
  --authorization-file /secure/pd-authorization \
  --output region-meta-report.json
```

脚本不允许 URL 内嵌用户名或密码，也不提供跳过 TLS 证书校验的选项。Authorization Header 只会在所有传入 URL 和成员 URL 都使用 HTTPS 时发送。

## 结果判读

报告是单行紧凑 JSON，主要字段如下：

| 字段 | 含义 |
| --- | --- |
| `status` | `consistent`、`inconsistent` 或 `incomplete`。 |
| `reference` | 扫描开始时的 PD Leader 身份，仅用于定位实例。 |
| `settings` | 实际限速、容量上限、HTTP 请求数、Response Body 字节数和临时磁盘峰值；Body 字节数不含 HTTP Header、TCP 和 TLS 开销。 |
| `nodes` | 各 PD 的名称、地址、角色、扫描 Region 数、批次数和时间。 |
| `confirmation` | 差异确认范围、稳定/消失/变化的 Region ID 及未确认数量。 |
| `summary` | 不同 Region 总数和各差异字段的计数。 |
| `differences` | 按 Region ID 排序的全部保留差异。 |

每个差异元素包含 `region_id`，并且只输出真正不同的字段：

| 字段 | 含义 |
| --- | --- |
| `missing_on` | 缺少该 Region 的实例，例如 `pd-2@10.0.0.3:2379`。 |
| `key_range` | 各实例的 `start_key`、`end_key`，使用大写十六进制；空字符串表示无界。 |
| `epoch` | 各实例的 `conf_ver`、`version`。 |
| `peers` | 各实例完整 Peer 数组，包括 Peer ID、Store ID、数值 Role 和 Witness 状态。 |
| `leader_peer` | 各实例的 Region Leader Peer，可能为 `null`。 |

Peer `role` 数值为：`0=Voter`、`1=Learner`、`2=IncomingVoter`、`3=DemotingVoter`。实例键固定使用 `<PD member name>@<client URL host:port>`，不会只显示模糊的 leader/follower 代号。

### 确认状态

| `confirmation.result` | 含义 |
| --- | --- |
| `not_needed` | 初扫没有差异。 |
| `stable` | 至少一个已检查差异在二次读取后保持相同。 |
| `resolved` | 所有初始差异均已确认并且已经消失。 |
| `confirmation_disabled` | 禁用了确认，存在差异但证据不足，最终状态为 `incomplete`。 |
| `changed_during_recheck` | 已检查差异发生变化且没有足够稳定证据，最终状态为 `incomplete`。 |

`--confirm-limit` 只限制二次确认请求，不截断 `differences`。默认报告全量初扫差异，但只确认 Region ID 最小的前 128 个；应同时查看 `checked_regions`、`unconfirmed_regions`、`stable_regions` 和 `changed_regions`。

### 精简样例

一致结果的关键字段：

```json
{
  "status": "consistent",
  "summary": {"different_regions": 0, "by_field": {}},
  "confirmation": {"result": "not_needed", "final_differences": 0},
  "differences": []
}
```

稳定不一致结果的关键字段：

```json
{
  "status": "inconsistent",
  "confirmation": {
    "result": "stable",
    "checked_regions": 1,
    "unconfirmed_regions": 1,
    "stable_regions": [42],
    "final_differences": 2
  },
  "summary": {
    "different_regions": 2,
    "by_field": {"missing_on": 1, "epoch": 1}
  },
  "differences": [
    {
      "region_id": 42,
      "epoch": {
        "pd-0@10.0.0.1:2379": {"conf_ver": 3, "version": 8},
        "pd-1@10.0.0.2:2379": {"conf_ver": 3, "version": 7}
      }
    },
    {
      "region_id": 43,
      "missing_on": ["pd-1@10.0.0.2:2379"]
    }
  ]
}
```

样例只展示关键字段；实际报告还包含集群、节点、参数和扫描统计。

### 常用查询

```bash
# 状态、汇总和确认结果
jq '{status, summary, confirmation}' region-meta-report.json

# 全部不一致 Region ID
jq -r '.differences[].region_id' region-meta-report.json

# 按差异类型筛选
jq '.differences[] | select(has("missing_on") or has("key_range") or has("epoch"))' region-meta-report.json
jq '.differences[] | select(has("peers") or has("leader_peer"))' region-meta-report.json

# 未二次确认的差异数量
jq '.confirmation.unconfirmed_regions // 0' region-meta-report.json
```

## 资源与容量规划

### 检查器资源

| 资源 | 最低配置 | 建议配置 | 说明 |
| --- | ---: | ---: | --- |
| CPU | 1 vCPU | 2 vCPU | 全局并发固定为 `1`；额外 CPU 留给操作系统、TLS 和报告处理。 |
| 内存 | 512 MiB | 1 GiB | 一致路径最大 RSS 为 23.13 MiB，113,627 个差异的样本为 43.45 MiB。 |
| 可用磁盘 | 3 GiB | 5 GiB | 覆盖默认 1 GiB 临时数据、1 GiB 报告、既有报告和文件系统余量。 |
| 网络 | 100 Mbps | 与 PD 同 VPC 或同可用区 | 必须直连所有 PD 地址；8,000,000 Region 的默认平均 Body 流量估算为 1.095 MiB/s。 |
| 任务窗口 | 4 h | 大于 4 h | 默认硬上限为 4 h；8,000,000 Region 的默认限速时间下界约为 2 h 36 min。 |

磁盘空间按以下关系规划。即使 `--output=-`，脚本也会先生成完整临时报告，再复制到 stdout：

```text
可用空间 >= max-temporary-disk + max-output + 既有输出文件 + 文件系统余量
```

### 与 PD 同机运行

优先使用独立运维机。同机运行不会减少其他 PD 的 API 工作量，只会让检查器 CPU、网络和报告 I/O 与其中一台 PD 竞争资源。

必须同机时，除 PD 原有容量外，至少预留一个空闲逻辑 CPU、512 MiB `MemAvailable` 和 3 GiB 可用磁盘，并尽量让 `--work-dir` 和输出目录避开 PD 数据盘。

在本次 8,000,000 Region、每 Region 3 Peer 的模型中，单个 PD RSS 约为 9.4–14.0 GiB。24 GiB 总内存且启动前至少 4 GiB `MemAvailable` 是同机运行的最低参考线，32 GiB 或更多更稳妥。Key 长度、Peer 数量、后台任务和业务负载都会改变该数字，不能用它代替等价环境容量验证。

### 百万 Region 实测基线

测试使用指定 PD commit、三个 PD、三个 Store、每 Region 三个 Peer；每个规模使用全新数据目录。执行环境为 AWS EC2 `r7i.4xlarge`（16 vCPU、123 GiB 可见内存、无 swap）、Ubuntu 22.04.5、Python 3.10.12 和 TiUP 1.17.0，脚本通过 loopback 访问 PD，测试期间没有业务负载。`--interval 0` 用于隔离环境压力基线，不代表生产参数。

| 每个 PD 的 Region 数 | HTTP 请求数 | `interval=0` Wall time | 脚本最大 RSS | Response Body | 默认限速时间下界 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | 23,447 | 44.59 s | 22.63 MiB | 1.24 GiB | 19 min 32 s |
| 2,000,000 | 46,883 | 89.44 s | 23.13 MiB | 2.48 GiB | 39 min 04 s |
| 4,000,000 | 93,758 | 179.21 s | 23.00 MiB | 4.99 GiB | 1 h 18 min 08 s |
| 8,000,000 | 187,508 | 359.54 s | 23.00 MiB | 10.02 GiB | 2 h 36 min 15 s |

8,000,000 Region 的 `interval=0` 压力基线约为 522 requests/s、28.55 MiB/s Response Body、三个 PD 合计 0.71 CPU core，并观察到三个 PD 的 Go runtime `sys` 合计增加 3.807 GiB。该模式不能用于生产，也不能用来推断默认限速下的内存变化。

将同一批 HTTP 和 CPU 工作量均匀摊入默认限速时间，估算三个 PD 合计平均 Body 流量为 1.095 MiB/s，PD CPU 合计约占一个 core 的 2.74%，检查器约占一个 core 的 2.67%。这些是容量规划参考，不是生产 SLA；单次扫描、JSON 序列化和短时读锁峰值不会因请求间休眠而消失。

一致结果的报告约为 1.5 KiB，临时差异磁盘为 `0`。在 113,627 个 `missing_on` 差异样本中，报告为 9.11 MiB、检查器最大 RSS 为 43.45 MiB；实际报告大小取决于实例标识和差异字段内容。

### 为什么 RSS 不随 Region 数增长

`http_response_bytes` 是整轮所有 HTTP Response Body 的累计值，并不是同时驻留内存的数据量。脚本每次只读取一个受 8 MiB 硬上限保护的响应，每个 PD 最多保留当前一页 Region；相同片段比较后立即释放。

差异路径只增加固定 8 MiB 排序缓冲，缓冲满后写入临时 JSONL，并通过 fan-in 为 8 的外部归并处理。二次确认最多保留 `--confirm-limit` 个候选，最终报告也逐条写入文件。因此内存复杂度近似为：

```text
O(PD 节点数 × batch-size + 单个响应 + 固定差异缓冲 + confirm-limit)
```

而不是 `O(Region 总数)`。Region 数增加主要表现为请求数、累计网络流量和运行时间增加。

## 生产运行守则

- 仅在业务低峰使用生产默认参数；如果 PD 没有明确资源余量，不要执行。
- 监控现有生产阈值下的 `process_cpu_seconds_total`、`go_memstats_heap_alloc_bytes`、`go_memstats_heap_inuse_bytes`、GC、业务请求延迟以及 `pd_region_syncer_status{type="sync_index"}` / `{type="last_index"}`。
- 任一现有 SLO、告警或资源阈值被触发时立即发送 Ctrl-C；不要为了跑完而临时放宽阈值、增加批量、取消限速或启用整轮重扫。
- 运行期间避免 PD 重启、扩缩容或主动 Leader 切换。成员签名变化会使本轮结果失效。
- 不要使用负载均衡地址代替成员直连地址。手工调用 API 时缺少 Follower 本地读取 Header，可能得到由 PD Leader 处理的假一致结果。
- 报告包含 PD 地址、Region ID、Key Range、Peer ID 和 Store ID 等拓扑信息，不要直接发布到公开 Issue、公共聊天或无访问控制的日志系统。
- Authorization 文件应限制为当前用户可读，只通过 HTTPS 发送，并按组织安全规范保管或删除。

## 常见错误

| 错误或现象 | 原因与处理 |
| --- | --- |
| `direct endpoint ... missing or ambiguous` | 传入地址没有和成员 `client_urls` 一一匹配。使用一个可访问 seed 自动发现，或传入 `/members` 返回的精确地址。 |
| `unstable Region set ...` | 扫描期间 Region 数持续变化。等待集群稳定并在低峰重跑；不要通过增大批量、减小间隔或增加整轮重试强行完成。 |
| `PD membership or leader changed ...` | 开始和结束时的 Leader 或成员签名不同。等待拓扑稳定后重跑。 |
| HTTP `401/403` | 检查完整 Authorization Header、权限和 HTTPS 配置。 |
| HTTP `500/503` | 检查目标 Follower 和 Region Syncer；确认携带本地读取 Header 时对应 API 可用。默认不自动重试。 |
| `response exceeds 8 MiB` | 降低 `--batch-size`。 |
| `temporary JSON data exceeds ...` | 差异临时数据达到上限；检查文件系统和差异规模后，再决定是否提高 `--max-temporary-disk-mib`。 |
| `JSON report exceeds ...` | 报告达到上限；评估差异规模和输出磁盘后，再决定是否提高 `--max-output-mib`。 |
| 没有生成新报告 | 参数、网络、TLS、成员变化或资源错误发生在报告写入前。检查 stderr；指定 `--output` 时旧文件不会被半成品覆盖。 |

每 100 个扫描批次会向 stderr 输出一次进度，不会污染 JSON stdout。

## 测试

本地模拟 PD HTTP Server 的测试覆盖一致、Region 缺失、Key Range、Epoch、Peers、Region Leader Peer、Peer 顺序、瞬时差异、数量变化、Role、Witness、限速参数和 uint64 边界：

```bash
python3 tools/pd-region-meta-checker/test_pd_region_meta_checker.py -v
```

流式资源路径可以在隔离测试机复现：

```bash
python3 tools/pd-region-meta-checker/benchmark_streaming.py 8000000
```

该基准会产生显著本机 CPU 和 loopback 流量，不应在 PD 主机上运行。
