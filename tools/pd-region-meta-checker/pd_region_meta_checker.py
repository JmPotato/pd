#!/usr/bin/env python3
"""Compare region meta held by every PD member through bounded local HTTP scans."""

import argparse
import collections
import datetime
import heapq
import http.client
import json
import math
import os
import shutil
import ssl
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode, urlsplit


CALLER_ID = "pd-region-meta-checker"
LOCAL_HEADERS = {
    "PD-Allow-Follower-Handle": "true",
    "PD-Redirector": CALLER_ID,
    "X-Caller-ID": CALLER_ID,
}
REPORT_FIELDS = {
    "missing": "missing_on",
    "key_range": "key_range",
    "epoch": "epoch",
    "peers": "peers",
    "leader": "leader_peer",
}
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
UINT64_MAX = (1 << 64) - 1
CONFIRM_DELAY_SECONDS = 1.0
SORT_BUFFER_BYTES = 8 * 1024 * 1024
MERGE_FAN_IN = 8
RegionFields = collections.namedtuple(
    "RegionFields", "start_key end_key conf_ver version peers leader"
)


class CheckerError(Exception):
    pass


@dataclass
class Node:
    index: int
    member_id: int
    name: str
    url: str
    role: str

    @property
    def instance(self):
        return f"{self.name}@{urlsplit(self.url).netloc}"


@dataclass
class ScanState:
    node: Node
    cursor: str = ""
    done: bool = False
    pages: int = 0
    inserted: int = 0
    started_at: str = ""
    finished_at: str = ""
    count_before: int = 0
    count_after: int = 0
    attempts: int = 1

    @property
    def stable(self):
        return self.count_before == self.inserted == self.count_after


class RateLimiter:
    def __init__(self, interval):
        self.interval = interval
        self.next_request = 0.0

    def wait(self):
        now = time.monotonic()
        if now < self.next_request:
            time.sleep(self.next_request - now)
        self.next_request = time.monotonic() + self.interval


class TemporaryJSONBudget:
    def __init__(self, limit_bytes, limit_mib):
        self.limit_bytes = limit_bytes
        self.limit_mib = limit_mib
        self.current_bytes = 0
        self.peak_bytes = 0
        self.file_sizes = {}

    def write(self, output, payload):
        new_size = self.current_bytes + len(payload)
        if new_size > self.limit_bytes:
            raise CheckerError(
                f"temporary JSON data exceeds {self.limit_mib} MiB; "
                "increase --max-temporary-disk-mib only after checking free space"
            )
        output.write(payload)
        path = Path(output.name)
        self.file_sizes[path] = self.file_sizes.get(path, 0) + len(payload)
        self.current_bytes = new_size
        self.peak_bytes = max(self.peak_bytes, new_size)

    def remove(self, path):
        path = Path(path)
        self.current_bytes -= self.file_sizes.pop(path, 0)
        path.unlink(missing_ok=True)


class SortedDifferences:
    def __init__(self, directory, budget):
        self.directory = Path(directory)
        self.budget = budget
        self.buffer = []
        self.buffer_bytes = 0
        self.chunks = []

    @staticmethod
    def _encode(region_id, node_index, meta):
        row = [region_id, node_index, *meta]
        return json.dumps(row, separators=(",", ":")).encode("utf-8") + b"\n"

    @staticmethod
    def _key(payload):
        row = json.loads(payload)
        return int(row[0]), int(row[1])

    @staticmethod
    def _decode(payload):
        row = json.loads(payload)
        leader = tuple(row[7]) if row[7] is not None else None
        meta = RegionFields(
            row[2],
            row[3],
            int(row[4]),
            int(row[5]),
            tuple(tuple(peer) for peer in row[6]),
            leader,
        )
        return int(row[0]), int(row[1]), meta

    def add(self, region_id, node_index, meta):
        payload = self._encode(region_id, node_index, meta)
        self.buffer.append((region_id, node_index, payload))
        self.buffer_bytes += len(payload)
        if self.buffer_bytes >= SORT_BUFFER_BYTES:
            self._flush()

    def _new_file(self):
        return tempfile.NamedTemporaryFile(
            mode="w+b",
            dir=self.directory,
            prefix="region-meta-",
            suffix=".jsonl",
            delete=False,
        )

    def _flush(self):
        if not self.buffer:
            return
        self.buffer.sort(key=lambda item: (item[0], item[1]))
        output = self._new_file()
        path = Path(output.name)
        try:
            for _, _, payload in self.buffer:
                self.budget.write(output, payload)
            output.close()
        except BaseException:
            output.close()
            self.budget.remove(path)
            raise
        self.chunks.append(path)
        self.buffer.clear()
        self.buffer_bytes = 0

    def _iter_lines(self, paths):
        inputs = [path.open("rb") for path in paths]
        heap = []
        try:
            for index, source in enumerate(inputs):
                payload = source.readline()
                if payload:
                    heapq.heappush(heap, (*self._key(payload), index, payload))
            while heap:
                _, _, index, payload = heapq.heappop(heap)
                yield payload
                following = inputs[index].readline()
                if following:
                    heapq.heappush(heap, (*self._key(following), index, following))
        finally:
            for source in inputs:
                source.close()

    def _merge_group(self, paths):
        output = self._new_file()
        merged = Path(output.name)
        try:
            for payload in self._iter_lines(paths):
                self.budget.write(output, payload)
            output.close()
        except BaseException:
            output.close()
            self.budget.remove(merged)
            raise
        for path in paths:
            self.budget.remove(path)
        return merged

    def finish(self):
        self._flush()
        while len(self.chunks) > MERGE_FAN_IN:
            inputs = self.chunks
            merged = []
            try:
                for offset in range(0, len(inputs), MERGE_FAN_IN):
                    group = inputs[offset : offset + MERGE_FAN_IN]
                    merged.append(
                        group[0] if len(group) == 1 else self._merge_group(group)
                    )
            except BaseException:
                self.chunks = inputs + merged
                raise
            self.chunks = merged

    def rows(self):
        for payload in self._iter_lines(self.chunks):
            yield self._decode(payload)

    def cleanup(self):
        self.buffer.clear()
        self.buffer_bytes = 0
        for path in self.chunks:
            self.budget.remove(path)
        self.chunks.clear()


class HTTPClient:
    def __init__(self, endpoint, timeout, retries, limiter, ssl_context, authorization):
        parsed = urlsplit(endpoint)
        self.endpoint = endpoint
        self.scheme = parsed.scheme
        self.host = parsed.hostname
        self.port = parsed.port or (443 if self.scheme == "https" else 80)
        self.timeout = timeout
        self.retries = retries
        self.limiter = limiter
        self.ssl_context = ssl_context
        self.authorization = authorization
        self.connection = None
        self.requests = 0
        self.response_bytes = 0

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def _connect(self):
        if self.scheme == "https":
            return http.client.HTTPSConnection(
                self.host,
                self.port,
                timeout=self.timeout,
                context=self.ssl_context,
            )
        return http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)

    def get_json(self, path, params=None, local=False):
        target = path
        if params is not None:
            target += "?" + urlencode(params)
        headers = dict(LOCAL_HEADERS if local else {"X-Caller-ID": CALLER_ID})
        if self.authorization:
            headers["Authorization"] = self.authorization

        last_error = None
        for attempt in range(self.retries + 1):
            self.limiter.wait()
            try:
                if self.connection is None:
                    self.connection = self._connect()
                self.connection.request("GET", target, headers=headers)
                self.requests += 1
                response = self.connection.getresponse()
                body = response.read(MAX_RESPONSE_BYTES + 1)
                self.response_bytes += len(body)
                status = response.status
                retry_after = response.getheader("Retry-After")
                response.close()
                if len(body) > MAX_RESPONSE_BYTES:
                    raise CheckerError(f"{self.endpoint}{path}: response exceeds 8 MiB")
                if status == 200:
                    try:
                        return json.loads(body)
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        raise CheckerError(f"{self.endpoint}{path}: invalid JSON: {exc}") from exc
                message = body[:512].decode("utf-8", "replace").strip()
                last_error = CheckerError(
                    f"{self.endpoint}{path}: HTTP {status}" + (f": {message}" if message else "")
                )
                if status not in RETRYABLE_STATUS:
                    raise last_error
                if retry_after and retry_after.isdigit():
                    time.sleep(min(float(retry_after), 5.0))
            except CheckerError:
                self.close()
                raise
            except (OSError, ssl.SSLError, TimeoutError, http.client.HTTPException) as exc:
                last_error = CheckerError(f"{self.endpoint}{path}: {exc}")
                self.close()
            if attempt < self.retries:
                time.sleep(min(0.1 * (2**attempt), 1.0))
        raise last_error


def now_utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def normalize_url(raw):
    try:
        parsed = urlsplit(raw.strip())
        host = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise CheckerError(f"invalid PD URL: {raw!r}: {exc}") from exc
    if parsed.scheme not in ("http", "https") or not host:
        raise CheckerError(f"invalid PD URL: {raw!r}")
    if parsed.username or parsed.password:
        raise CheckerError("credentials must not be embedded in a PD URL")
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise CheckerError(f"PD URL must not include a path, query, or fragment: {raw!r}")
    host = host.lower()
    if ":" in host:
        host = f"[{host}]"
    port = port or (443 if parsed.scheme == "https" else 80)
    return f"{parsed.scheme.lower()}://{host}:{port}"


def parse_membership(payload):
    if not isinstance(payload, dict) or not isinstance(payload.get("members"), list):
        raise CheckerError("/members response does not contain a members array")
    leader = payload.get("leader") or {}
    leader_id = int(leader.get("member_id", 0))
    if leader_id <= 0:
        raise CheckerError("/members response does not identify the PD leader")

    members = []
    for raw in payload["members"]:
        member_id = int(raw.get("member_id", 0))
        urls = [normalize_url(value) for value in raw.get("client_urls") or []]
        if member_id <= 0 or not urls:
            raise CheckerError("/members contains a member without id or client_urls")
        members.append(
            {
                "member_id": member_id,
                "name": str(raw.get("name") or f"member-{member_id}"),
                "urls": urls,
            }
        )
    if len(members) < 2:
        raise CheckerError("at least two PD members are required")
    if leader_id not in {member["member_id"] for member in members}:
        raise CheckerError("PD leader is not present in the member list")
    return members, leader_id, (payload.get("header") or {}).get("cluster_id")


def discover_nodes(seed_client, supplied_endpoints):
    payload = seed_client.get_json("/pd/api/v1/members", local=True)
    members, leader_id, cluster_id = parse_membership(payload)
    supplied = [normalize_url(value) for value in supplied_endpoints]
    if len(set(supplied)) != len(supplied):
        raise CheckerError("duplicate PD endpoints were supplied")

    chosen = {}
    if len(supplied) == 1:
        chosen = {member["member_id"]: member["urls"][0] for member in members}
    else:
        for member in members:
            matches = [url for url in supplied if url in member["urls"]]
            if len(matches) != 1:
                raise CheckerError(
                    f"direct endpoint for PD member {member['name']!r} is missing or ambiguous"
                )
            chosen[member["member_id"]] = matches[0]
        advertised = set(chosen.values())
        extras = sorted(set(supplied) - advertised)
        if extras:
            raise CheckerError(f"supplied endpoints are not PD member client_urls: {extras}")

    ordered = sorted(members, key=lambda item: (item["member_id"] != leader_id, item["member_id"]))
    names = [member["name"] for member in ordered]
    if len(set(names)) != len(names):
        raise CheckerError("PD member names must be unique")
    nodes = [
        Node(
            index=index,
            member_id=member["member_id"],
            name=member["name"],
            url=chosen[member["member_id"]],
            role="leader" if member["member_id"] == leader_id else "follower",
        )
        for index, member in enumerate(ordered)
    ]
    return nodes, leader_id, cluster_id, payload


def valid_hex(value, field, region_id):
    value = str(value or "").upper()
    if len(value) % 2 or any(char not in "0123456789ABCDEF" for char in value):
        raise CheckerError(f"region {region_id} has invalid hexadecimal {field}")
    return value


def normalize_peer(raw):
    raw = raw or {}
    role = int(raw.get("role", 1 if raw.get("is_learner") else 0))
    return (
        int(raw.get("id", 0)),
        int(raw.get("store_id", 0)),
        role,
        bool(raw.get("is_witness", False)),
    )


def normalize_region(raw):
    if not isinstance(raw, dict):
        raise CheckerError("region response contains a non-object value")
    region_id = int(raw.get("id", 0))
    if not 0 < region_id <= UINT64_MAX:
        raise CheckerError("region response contains an invalid uint64 id")
    start_key = valid_hex(raw.get("start_key", ""), "start_key", region_id)
    end_key = valid_hex(raw.get("end_key", ""), "end_key", region_id)
    epoch = raw.get("epoch") or {}
    conf_ver = int(epoch.get("conf_ver", 0))
    version = int(epoch.get("version", 0))
    if not 0 <= conf_ver <= UINT64_MAX or not 0 <= version <= UINT64_MAX:
        raise CheckerError(f"region {region_id} contains an invalid uint64 epoch")
    peers = tuple(sorted(normalize_peer(peer) for peer in (raw.get("peers") or [])))
    leader = normalize_peer(raw.get("leader"))
    if leader[0] == 0 and leader[1] == 0:
        leader = None
    meta = RegionFields(
        start_key,
        end_key,
        conf_ver,
        version,
        peers,
        leader,
    )
    return region_id, meta


def get_region_count(client):
    payload = client.get_json("/pd/api/v1/regions/count", local=True)
    try:
        count = int(payload["count"])
    except (KeyError, TypeError, ValueError) as exc:
        raise CheckerError("/regions/count response does not contain a valid count") from exc
    if count < 0:
        raise CheckerError("/regions/count returned a negative count")
    return count


def get_region_page(client, cursor, batch_size):
    payload = client.get_json(
        "/pd/api/v1/regions/key",
        params={"format": "hex", "key": cursor, "end_key": "", "limit": batch_size},
        local=True,
    )
    if not isinstance(payload, dict) or not isinstance(payload.get("regions"), list):
        raise CheckerError("/regions/key response does not contain a regions array")
    regions = payload["regions"]
    try:
        count = int(payload["count"])
    except (KeyError, TypeError, ValueError) as exc:
        raise CheckerError("/regions/key response does not contain a valid count") from exc
    if count != len(regions) or len(regions) > batch_size:
        raise CheckerError("/regions/key response count or batch limit is invalid")
    return regions


class RegionStream:
    def __init__(self, state, client, batch_size, page_counter):
        self.state = state
        self.client = client
        self.batch_size = batch_size
        self.page_counter = page_counter
        self.buffer = collections.deque()
        self.last_end_key = None
        self.terminal_page = False
        self.max_pages = (state.count_before + batch_size - 1) // batch_size + 1

    def _load_page(self):
        if self.state.pages >= self.max_pages:
            raise CheckerError(
                f"{self.state.node.name}: exceeded the bounded Region page count"
            )
        page = get_region_page(self.client, self.state.cursor, self.batch_size)
        self.state.pages += 1
        self.page_counter[0] += 1
        if self.page_counter[0] % 100 == 0:
            print(
                f"scanned {self.page_counter[0]} batches",
                file=sys.stderr,
                flush=True,
            )
        if not page:
            self.terminal_page = True
            return

        for raw in page:
            region_id, meta = normalize_region(raw)
            end_key = meta.end_key
            if self.last_end_key == "":
                raise CheckerError(
                    f"{self.state.node.name}: Region found after the unbounded key"
                )
            if (
                self.last_end_key is not None
                and end_key
                and bytes.fromhex(end_key) <= bytes.fromhex(self.last_end_key)
            ):
                raise CheckerError(
                    f"{self.state.node.name}: Region scan key did not advance"
                )
            self.last_end_key = end_key
            self.buffer.append((region_id, meta))

        if self.last_end_key == "":
            self.terminal_page = True
        else:
            if bytes.fromhex(self.last_end_key) <= bytes.fromhex(self.state.cursor):
                raise CheckerError(
                    f"{self.state.node.name}: Region scan cursor did not advance"
                )
            self.state.cursor = self.last_end_key

    def next_record(self):
        if not self.buffer and not self.terminal_page:
            self._load_page()
        if not self.buffer:
            self.state.done = True
            if not self.state.finished_at:
                self.state.finished_at = now_utc()
            return None
        record = self.buffer.popleft()
        self.state.inserted += 1
        if not self.buffer and self.terminal_page:
            self.state.done = True
            self.state.finished_at = now_utc()
        return record


def boundary_order(value):
    return (1, b"") if value == "" else (0, bytes.fromhex(value))


def scan_streams(states, clients, batch_size, differences):
    page_counter = [0]
    for state in states:
        state.started_at = now_utc()
    streams = [
        RegionStream(state, clients[state.node.index], batch_size, page_counter)
        for state in states
    ]

    while True:
        records = [stream.next_record() for stream in streams]
        if all(record is None for record in records):
            return

        boundaries = [
            record[1].end_key if record is not None else "" for record in records
        ]
        divergent = not all(record == records[0] for record in records[1:])
        if divergent:
            for node_index, record in enumerate(records):
                if record is not None:
                    differences.add(record[0], node_index, record[1])

        while len(set(boundaries)) != 1:
            boundary = min(boundaries, key=boundary_order)
            for node_index, current in enumerate(boundaries):
                if current != boundary:
                    continue
                record = streams[node_index].next_record()
                if record is None:
                    boundaries[node_index] = ""
                else:
                    boundaries[node_index] = record[1].end_key
                    differences.add(record[0], node_index, record[1])


def collect_regions(nodes, clients, batch_size, scan_retries, directory, budget):
    for attempt in range(1, scan_retries + 2):
        states = [ScanState(node=node, attempts=attempt) for node in nodes]
        for state in states:
            state.count_before = get_region_count(clients[state.node.index])
        differences = SortedDifferences(directory, budget)
        try:
            scan_streams(states, clients, batch_size, differences)
            for state in states:
                state.count_after = get_region_count(clients[state.node.index])
        except BaseException:
            differences.cleanup()
            raise
        if all(state.stable for state in states):
            try:
                differences.finish()
            except BaseException:
                differences.cleanup()
                raise
            return states, differences
        differences.cleanup()
        if attempt <= scan_retries:
            print(
                "Region count changed during scan; retrying every PD member",
                file=sys.stderr,
                flush=True,
            )

    details = "; ".join(
        f"{state.node.name}: before={state.count_before}, scanned={state.inserted}, "
        f"after={state.count_after}"
        for state in states
        if not state.stable
    )
    raise CheckerError(
        f"unstable Region set after {scan_retries + 1} cluster-wide scan attempt(s) "
        f"({details})"
    )


def peer_to_report(value):
    if value is None:
        return None
    return {
        "id": int(value[0]),
        "store_id": int(value[1]),
        "role": int(value[2]),
        "is_witness": bool(value[3]),
    }


def meta_to_report(region_id, meta):
    return {
        "id": region_id,
        "start_key": meta.start_key,
        "end_key": meta.end_key,
        "epoch": {"conf_ver": meta.conf_ver, "version": meta.version},
        "peers": [peer_to_report(peer) for peer in meta.peers],
        "leader": peer_to_report(meta.leader),
    }


def category_value(category, meta):
    if category == "missing":
        return meta is not None
    if meta is None:
        return None
    if category == "key_range":
        return {"start_key": meta["start_key"], "end_key": meta["end_key"]}
    if category == "epoch":
        return meta["epoch"]
    return meta[category]


def make_difference(region_id, rows, nodes):
    metas = [
        meta_to_report(region_id, row) if row is not None else None
        for row in rows
    ]

    values_by_category = {
        category: [category_value(category, meta) for meta in metas]
        for category in REPORT_FIELDS
    }
    categories = []
    for category, values in values_by_category.items():
        comparable = (
            values
            if category == "missing"
            else [value for value, meta in zip(values, metas) if meta is not None]
        )
        if comparable and any(value != comparable[0] for value in comparable[1:]):
            categories.append(category)
    if not categories:
        return None

    difference = {"region_id": region_id}
    if "missing" in categories:
        difference["missing_on"] = [
            node.instance for node in nodes if metas[node.index] is None
        ]
    for category in categories:
        if category == "missing":
            continue
        difference[REPORT_FIELDS[category]] = {
            node.instance: values_by_category[category][node.index]
            for node in nodes
            if metas[node.index] is not None
        }
    return difference


def iter_differences(sorted_rows, nodes):
    region_id = None
    rows = None
    for current_id, node_index, meta in sorted_rows.rows():
        if current_id != region_id:
            if rows is not None:
                difference = make_difference(region_id, rows, nodes)
                if difference is not None:
                    yield difference
            region_id = current_id
            rows = [None] * len(nodes)
        if rows[node_index] is not None:
            raise CheckerError(
                f"{nodes[node_index].name}: duplicate Region id during scan"
            )
        rows[node_index] = meta
    if rows is not None:
        difference = make_difference(region_id, rows, nodes)
        if difference is not None:
            yield difference


def summarize(sorted_rows, nodes, confirmation_limit):
    field_counts = collections.Counter()
    different_regions = 0
    confirmation_candidates = []
    for difference in iter_differences(sorted_rows, nodes):
        different_regions += 1
        field_counts.update(field for field in REPORT_FIELDS.values() if field in difference)
        if len(confirmation_candidates) < confirmation_limit:
            confirmation_candidates.append(difference)
    summary = {
        "different_regions": different_regions,
        "by_field": {
            field: field_counts[field]
            for field in REPORT_FIELDS.values()
            if field_counts[field]
        },
    }
    return summary, confirmation_candidates


def recheck_differences(initial, nodes, clients, difference_count, limit):
    confirmation = {
        "initial_differences": difference_count,
        "limit": limit,
        "delay_seconds": CONFIRM_DELAY_SECONDS,
    }
    if difference_count == 0:
        confirmation["result"] = "not_needed"
        return confirmation, {}
    if limit == 0:
        confirmation["result"] = "confirmation_disabled"
        confirmation["unconfirmed_regions"] = difference_count
        return confirmation, {}

    region_ids = [difference["region_id"] for difference in initial]
    confirmation["checked_regions"] = len(region_ids)
    confirmation["unconfirmed_regions"] = difference_count - len(region_ids)
    print(
        f"rechecking {len(region_ids)} differing Regions after "
        f"{CONFIRM_DELAY_SECONDS:g}s",
        file=sys.stderr,
        flush=True,
    )
    time.sleep(CONFIRM_DELAY_SECONDS)
    final = {}
    for region_id in region_ids:
        rows = [None] * len(nodes)
        for node in nodes:
            payload = clients[node.index].get_json(
                f"/pd/api/v1/region/id/{region_id}", local=True
            )
            if payload is None:
                continue
            if not isinstance(payload, dict):
                raise CheckerError(f"{node.name}: invalid /region/id/{region_id} response")
            returned_id = int(payload.get("id", 0))
            if returned_id == 0:
                continue
            if returned_id != region_id:
                raise CheckerError(f"{node.name}: invalid /region/id/{region_id} response")
            returned_id, meta = normalize_region(payload)
            rows[node.index] = meta
        final[region_id] = make_difference(region_id, rows, nodes)
    stable = [
        difference["region_id"]
        for difference in initial
        if final[difference["region_id"]] == difference
    ]
    resolved = [region_id for region_id, difference in final.items() if difference is None]
    changed = [
        region_id
        for region_id, difference in final.items()
        if difference is not None and region_id not in stable
    ]
    confirmation["stable_regions"] = stable
    confirmation["resolved_regions"] = resolved
    confirmation["changed_regions"] = changed
    if stable:
        confirmation["result"] = "stable"
    elif len(initial) == difference_count and len(resolved) == len(initial):
        confirmation["result"] = "resolved"
    else:
        confirmation["result"] = "changed_during_recheck"
    return confirmation, final


def adjust_summary(initial_summary, initial, final):
    field_counts = collections.Counter(initial_summary["by_field"])
    different_regions = initial_summary["different_regions"]
    for difference in initial:
        region_id = difference["region_id"]
        replacement = final.get(region_id, difference)
        field_counts.subtract(
            field for field in REPORT_FIELDS.values() if field in difference
        )
        if replacement is None:
            different_regions -= 1
        else:
            field_counts.update(
                field for field in REPORT_FIELDS.values() if field in replacement
            )
    return {
        "different_regions": different_regions,
        "by_field": {
            field: field_counts[field]
            for field in REPORT_FIELDS.values()
            if field_counts[field]
        },
    }


def iter_final_differences(sorted_rows, nodes, replacements):
    for difference in iter_differences(sorted_rows, nodes):
        replacement = replacements.get(difference["region_id"], difference)
        if replacement is not None:
            yield replacement


class LimitedTextWriter:
    def __init__(self, output, limit_bytes, limit_mib):
        self.output = output
        self.limit_bytes = limit_bytes
        self.limit_mib = limit_mib
        self.written = 0

    def write(self, value):
        size = len(value.encode("utf-8"))
        if self.written + size > self.limit_bytes:
            raise CheckerError(
                f"JSON report exceeds {self.limit_mib} MiB; "
                "increase --max-output-mib only after checking free space"
            )
        self.output.write(value)
        self.written += size
        return len(value)


def write_report(path, report, differences, directory, limit_bytes, limit_mib):
    target = None
    output_directory = directory
    if path != "-":
        target = Path(path).expanduser().resolve()
        if not target.parent.is_dir():
            raise CheckerError(f"output directory does not exist: {target.parent}")
        output_directory = target.parent
    temporary = tempfile.NamedTemporaryFile(
        mode="w+",
        encoding="utf-8",
        dir=output_directory,
        prefix="region-meta-report-",
        suffix=".json",
        delete=False,
    )
    output = LimitedTextWriter(temporary, limit_bytes, limit_mib)
    try:
        output.write("{")
        for index, (key, value) in enumerate(report.items()):
            if index:
                output.write(",")
            output.write(json.dumps(key) + ":")
            output.write(
                json.dumps(value, ensure_ascii=False, separators=(",", ":"))
            )
        output.write(',"differences":[')
        for index, difference in enumerate(differences):
            if index:
                output.write(",")
            output.write(
                json.dumps(difference, ensure_ascii=False, separators=(",", ":"))
            )
        output.write("]}\n")
        temporary.flush()
        if target is not None:
            os.fsync(temporary.fileno())
            temporary.close()
            os.replace(temporary.name, target)
        else:
            temporary.seek(0)
            shutil.copyfileobj(temporary, sys.stdout)
            sys.stdout.flush()
            temporary.close()
            Path(temporary.name).unlink(missing_ok=True)
    except BaseException:
        temporary.close()
        Path(temporary.name).unlink(missing_ok=True)
        raise
    finally:
        close = getattr(differences, "close", None)
        if close is not None:
            close()


def membership_signature(payload):
    members, leader_id, cluster_id = parse_membership(payload)
    identities = tuple(
        sorted(
            (
                member["member_id"],
                member["name"],
                tuple(sorted(member["urls"])),
            )
            for member in members
        )
    )
    return identities, leader_id, cluster_id


def build_ssl_context(args):
    context = ssl.create_default_context(cafile=args.cacert)
    if args.cert:
        context.load_cert_chain(args.cert, args.key)
    return context


def read_authorization(path):
    if not path:
        return None
    value = Path(path).expanduser().read_text(encoding="utf-8").strip()
    if not value or "\r" in value or "\n" in value:
        raise CheckerError("authorization file must contain exactly one non-empty line")
    return value


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description=(
            "Compare region meta from each PD member's local cache using bounded HTTP scans. "
            "One endpoint discovers the cluster; multiple endpoints must match member client_urls."
        )
    )
    parser.add_argument("endpoints", nargs="+", metavar="PD_URL")
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--interval", type=float, default=0.05, help="seconds between requests")
    parser.add_argument("--timeout", type=float, default=10.0, help="per-request timeout")
    parser.add_argument("--retries", type=int, default=0, help="HTTP retries per request")
    parser.add_argument(
        "--scan-retries",
        type=int,
        default=0,
        help="whole-cluster retries when any Region count changes during a scan",
    )
    parser.add_argument(
        "--confirm-limit",
        type=int,
        default=128,
        help="maximum differing Regions to recheck; 0 disables confirmation",
    )
    parser.add_argument("--cacert", help="trusted CA bundle for HTTPS")
    parser.add_argument("--cert", help="HTTPS client certificate")
    parser.add_argument("--key", help="HTTPS client private key")
    parser.add_argument(
        "--authorization-file",
        help="file containing the complete Authorization header value",
    )
    parser.add_argument(
        "--work-dir",
        help="existing directory for automatically cleaned temporary JSON files",
    )
    parser.add_argument(
        "--max-temporary-disk-mib",
        type=int,
        default=1024,
        help="hard limit for temporary JSON data",
    )
    parser.add_argument(
        "--max-output-mib",
        type=int,
        default=1024,
        help="hard limit for the final JSON report",
    )
    parser.add_argument("--output", default="-", help="JSON report path; default: stdout")
    args = parser.parse_args(argv)
    if not 1 <= args.batch_size <= 1024:
        parser.error("--batch-size must be in [1, 1024]")
    if (
        not math.isfinite(args.interval)
        or args.interval < 0
        or not math.isfinite(args.timeout)
        or args.timeout <= 0
    ):
        parser.error(
            "--interval must be finite and non-negative and "
            "--timeout must be finite and positive"
        )
    if not 0 <= args.retries <= 10 or not 0 <= args.scan_retries <= 3:
        parser.error("--retries must be in [0, 10] and --scan-retries in [0, 3]")
    if not 0 <= args.confirm_limit <= 1024:
        parser.error("--confirm-limit must be in [0, 1024]")
    if args.max_temporary_disk_mib <= 0 or args.max_output_mib <= 0:
        parser.error("--max-temporary-disk-mib and --max-output-mib must be positive")
    if bool(args.cert) != bool(args.key):
        parser.error("--cert and --key must be provided together")
    return args


def run(args):
    supplied = [normalize_url(value) for value in args.endpoints]
    authorization = read_authorization(args.authorization_file)
    if authorization and any(urlsplit(value).scheme != "https" for value in supplied):
        raise CheckerError("Authorization requires HTTPS for every supplied PD URL")
    work_root = None
    if args.work_dir:
        work_root = Path(args.work_dir).expanduser().resolve()
        if not work_root.is_dir():
            raise CheckerError(f"work directory does not exist: {work_root}")

    limiter = RateLimiter(args.interval)
    ssl_context = build_ssl_context(args)
    seed = HTTPClient(
        supplied[0], args.timeout, args.retries, limiter, ssl_context, authorization
    )
    clients = []
    try:
        nodes, leader_id, cluster_id, membership_start = discover_nodes(seed, supplied)
        if authorization and any(urlsplit(node.url).scheme != "https" for node in nodes):
            raise CheckerError("Authorization requires HTTPS for every PD member URL")
        clients = [
            HTTPClient(node.url, args.timeout, args.retries, limiter, ssl_context, authorization)
            for node in nodes
        ]
        with tempfile.TemporaryDirectory(
            prefix="pd-region-meta-checker-", dir=work_root
        ) as directory:
            budget = TemporaryJSONBudget(
                args.max_temporary_disk_mib * 1024 * 1024,
                args.max_temporary_disk_mib,
            )
            sorted_rows = None
            try:
                states, sorted_rows = collect_regions(
                    nodes,
                    clients,
                    args.batch_size,
                    args.scan_retries,
                    directory,
                    budget,
                )
                initial_summary, initial = summarize(
                    sorted_rows, nodes, args.confirm_limit
                )
                confirmation, replacements = recheck_differences(
                    initial,
                    nodes,
                    clients,
                    initial_summary["different_regions"],
                    args.confirm_limit,
                )
                membership_end = seed.get_json("/pd/api/v1/members", local=True)
                if membership_signature(membership_start) != membership_signature(
                    membership_end
                ):
                    raise CheckerError("PD membership or leader changed during the scan")
                summary = adjust_summary(initial_summary, initial, replacements)
                confirmation["final_differences"] = summary["different_regions"]
                if confirmation["result"] in (
                    "confirmation_disabled",
                    "changed_during_recheck",
                ):
                    status, exit_code = "incomplete", 2
                elif summary["different_regions"] == 0:
                    status, exit_code = "consistent", 0
                else:
                    status, exit_code = "inconsistent", 1
                reference = next(node for node in nodes if node.member_id == leader_id)
                report = {
                    "status": status,
                    "generated_at": now_utc(),
                    "cluster_id": cluster_id,
                    "reference": {
                        "name": reference.name,
                        "member_id": reference.member_id,
                        "url": reference.url,
                    },
                    "settings": {
                        "batch_size": args.batch_size,
                        "request_interval_seconds": args.interval,
                        "request_timeout_seconds": args.timeout,
                        "global_concurrency": 1,
                        "confirmation_limit": args.confirm_limit,
                        "temporary_disk_limit_mib": args.max_temporary_disk_mib,
                        "output_limit_mib": args.max_output_mib,
                        "http_requests": seed.requests
                        + sum(client.requests for client in clients),
                        "http_response_bytes": seed.response_bytes
                        + sum(client.response_bytes for client in clients),
                        "temporary_disk_peak_bytes": budget.peak_bytes,
                        "snapshot_semantics": (
                            "bounded round-robin scans; differences are rechecked, "
                            "but the result is not an atomic snapshot"
                        ),
                    },
                    "nodes": [
                        {
                            "name": node.name,
                            "member_id": node.member_id,
                            "url": node.url,
                            "role": node.role,
                            "region_count": states[node.index].inserted,
                            "batches": states[node.index].pages,
                            "scan_attempts": states[node.index].attempts,
                            "started_at": states[node.index].started_at,
                            "finished_at": states[node.index].finished_at,
                        }
                        for node in nodes
                    ],
                    "confirmation": confirmation,
                    "summary": summary,
                }
                write_report(
                    args.output,
                    report,
                    iter_final_differences(sorted_rows, nodes, replacements),
                    directory,
                    args.max_output_mib * 1024 * 1024,
                    args.max_output_mib,
                )
                return exit_code
            finally:
                if sorted_rows is not None:
                    sorted_rows.cleanup()
    finally:
        seed.close()
        for client in clients:
            client.close()


def main(argv=None):
    try:
        return run(parse_args(argv))
    except KeyboardInterrupt:
        return 130
    except MemoryError:
        print("error: insufficient memory to continue the check", file=sys.stderr)
        return 2
    except (
        CheckerError,
        OSError,
        ssl.SSLError,
        TypeError,
        ValueError,
        AttributeError,
        OverflowError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
