#!/usr/bin/env python3
"""Compare RegionMeta held by every PD member through bounded local HTTP scans."""

import argparse
import collections
import datetime
import http.client
import itertools
import json
import os
import sqlite3
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
MAX_RESPONSE_BYTES = 64 * 1024 * 1024
UINT64_MAX = (1 << 64) - 1
CONFIRM_DELAY_SECONDS = 1.0
REGION_SELECT = """
    SELECT region_id, node_index, start_key, end_key, epoch_conf_ver,
           epoch_version, peers, leader
    FROM regions
"""


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
                response = self.connection.getresponse()
                body = response.read(MAX_RESPONSE_BYTES + 1)
                status = response.status
                retry_after = response.getheader("Retry-After")
                response.close()
                if len(body) > MAX_RESPONSE_BYTES:
                    raise CheckerError(f"{self.endpoint}{path}: response exceeds 64 MiB")
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
            except CheckerError as exc:
                last_error = exc
                if "HTTP " in str(exc) and not any(
                    f"HTTP {status}" in str(exc) for status in RETRYABLE_STATUS
                ):
                    raise
                self.close()
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


def normalize_region(raw, node_index):
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
    peers = sorted(normalize_peer(peer) for peer in (raw.get("peers") or []))
    leader = normalize_peer(raw.get("leader"))
    if leader[0] == 0 and leader[1] == 0:
        leader_json = "null"
    else:
        leader_json = json.dumps(leader, separators=(",", ":"))
    return (
        f"{region_id:020d}",
        node_index,
        start_key,
        end_key,
        str(conf_ver),
        str(version),
        json.dumps(peers, separators=(",", ":")),
        leader_json,
    )


def create_database(path):
    db = sqlite3.connect(path)
    # ponytail: scan state is disposable; durability would only slow the diagnostic.
    db.execute("PRAGMA journal_mode=OFF")
    db.execute("PRAGMA synchronous=OFF")
    db.execute(
        """
        CREATE TABLE regions (
            region_id TEXT NOT NULL,
            node_index INTEGER NOT NULL,
            start_key TEXT NOT NULL,
            end_key TEXT NOT NULL,
            epoch_conf_ver TEXT NOT NULL,
            epoch_version TEXT NOT NULL,
            peers TEXT NOT NULL,
            leader TEXT NOT NULL,
            PRIMARY KEY (region_id, node_index)
        ) WITHOUT ROWID
        """
    )
    return db


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


def scan_round_robin(db, states, clients, batch_size):
    total_pages = 0
    for state in states:
        state.started_at = now_utc()
    while any(not state.done for state in states):
        for state in states:
            if state.done:
                continue
            page = get_region_page(clients[state.node.index], state.cursor, batch_size)
            state.pages += 1
            total_pages += 1
            if not page:
                state.done = True
                state.finished_at = now_utc()
                continue
            rows = [normalize_region(raw, state.node.index) for raw in page]
            try:
                db.executemany(
                    "INSERT INTO regions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    rows,
                )
            except sqlite3.IntegrityError as exc:
                raise CheckerError(f"{state.node.name}: duplicate Region id during scan") from exc
            state.inserted += len(rows)
            end_key = rows[-1][3]
            if end_key == "":
                state.done = True
                state.finished_at = now_utc()
            else:
                if bytes.fromhex(end_key) <= bytes.fromhex(state.cursor):
                    raise CheckerError(f"{state.node.name}: region scan cursor did not advance")
                state.cursor = end_key
            if total_pages % 100 == 0:
                print(f"scanned {total_pages} batches", file=sys.stderr, flush=True)
    db.commit()


def collect_regions(db, nodes, clients, batch_size, scan_retries):
    for attempt in range(1, scan_retries + 2):
        states = [ScanState(node=node, attempts=attempt) for node in nodes]
        for state in states:
            state.count_before = get_region_count(clients[state.node.index])
        scan_round_robin(db, states, clients, batch_size)
        for state in states:
            state.count_after = get_region_count(clients[state.node.index])
        if all(state.stable for state in states):
            return states
        if attempt <= scan_retries:
            print(
                "Region count changed during scan; retrying every PD member",
                file=sys.stderr,
                flush=True,
            )
            db.execute("DELETE FROM regions")
            db.commit()

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


def peer_from_json(value):
    if value is None:
        return None
    return {
        "id": int(value[0]),
        "store_id": int(value[1]),
        "role": int(value[2]),
        "is_witness": bool(value[3]),
    }


def row_to_meta(region_id, row):
    peers = [peer_from_json(peer) for peer in json.loads(row[6])]
    leader = json.loads(row[7])
    return {
        "id": region_id,
        "start_key": row[2],
        "end_key": row[3],
        "epoch": {"conf_ver": int(row[4]), "version": int(row[5])},
        "peers": peers,
        "leader": peer_from_json(leader),
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
    metas = [None] * len(nodes)
    for row in rows:
        metas[row[1]] = row_to_meta(region_id, row)

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
        if len({json.dumps(value, sort_keys=True) for value in comparable}) > 1:
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


def iter_differences(db, nodes):
    rows = db.execute(REGION_SELECT + " ORDER BY region_id, node_index")
    for region_id, grouped in itertools.groupby(rows, key=lambda row: row[0]):
        difference = make_difference(int(region_id), list(grouped), nodes)
        if difference is not None:
            yield difference


def get_difference(db, nodes, region_id):
    rows = list(
        db.execute(
            REGION_SELECT + " WHERE region_id = ? ORDER BY node_index",
            (f"{region_id:020d}",),
        )
    )
    return make_difference(region_id, rows, nodes) if rows else None


def summarize(db, nodes):
    field_counts = collections.Counter()
    different_regions = 0
    for difference in iter_differences(db, nodes):
        different_regions += 1
        field_counts.update(field for field in REPORT_FIELDS.values() if field in difference)
    return {
        "different_regions": different_regions,
        "by_field": {
            field: field_counts[field]
            for field in REPORT_FIELDS.values()
            if field_counts[field]
        },
    }


def recheck_differences(db, nodes, clients, difference_count, limit):
    confirmation = {
        "initial_differences": difference_count,
        "limit": limit,
        "delay_seconds": CONFIRM_DELAY_SECONDS,
    }
    if difference_count == 0:
        confirmation["result"] = "not_needed"
        return confirmation
    if limit == 0:
        confirmation["result"] = "confirmation_disabled"
        confirmation["unconfirmed_regions"] = difference_count
        return confirmation

    initial = list(itertools.islice(iter_differences(db, nodes), limit))
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
    for region_id in region_ids:
        region_key = f"{region_id:020d}"
        db.execute("DELETE FROM regions WHERE region_id = ?", (region_key,))
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
            db.execute(
                "INSERT INTO regions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                normalize_region(payload, node.index),
            )
    db.commit()
    final = {region_id: get_difference(db, nodes, region_id) for region_id in region_ids}
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
    return confirmation


def write_report(path, report, differences):
    target = None
    temporary = None
    if path == "-":
        output = sys.stdout
    else:
        target = Path(path).expanduser().resolve()
        if not target.parent.is_dir():
            raise CheckerError(f"output directory does not exist: {target.parent}")
        temporary = tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=target.parent, delete=False
        )
        output = temporary
    try:
        output.write("{")
        for index, (key, value) in enumerate(report.items()):
            if index:
                output.write(",")
            output.write(json.dumps(key) + ":")
            json.dump(value, output, ensure_ascii=False, separators=(",", ":"))
        output.write(',"differences":[')
        for index, difference in enumerate(differences):
            if index:
                output.write(",")
            json.dump(difference, output, ensure_ascii=False, separators=(",", ":"))
        output.write("]}\n")
        output.flush()
        if temporary is not None:
            os.fsync(temporary.fileno())
            temporary.close()
            os.replace(temporary.name, target)
    except Exception:
        if temporary is not None:
            temporary.close()
            Path(temporary.name).unlink(missing_ok=True)
        raise


def membership_signature(payload):
    members, leader_id, cluster_id = parse_membership(payload)
    return ({member["member_id"] for member in members}, leader_id, cluster_id)


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
            "Compare RegionMeta from each PD member's local cache using bounded HTTP scans. "
            "One endpoint discovers the cluster; multiple endpoints must match member client_urls."
        )
    )
    parser.add_argument("endpoints", nargs="+", metavar="PD_URL")
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--interval", type=float, default=0.05, help="seconds between requests")
    parser.add_argument("--timeout", type=float, default=10.0, help="per-request timeout")
    parser.add_argument("--retries", type=int, default=2, help="HTTP retries per request")
    parser.add_argument(
        "--scan-retries",
        type=int,
        default=1,
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
    parser.add_argument("--output", default="-", help="JSON report path; default: stdout")
    args = parser.parse_args(argv)
    if not 1 <= args.batch_size <= 1024:
        parser.error("--batch-size must be in [1, 1024]")
    if args.interval < 0 or args.timeout <= 0:
        parser.error("--interval must be non-negative and --timeout must be positive")
    if not 0 <= args.retries <= 10 or not 0 <= args.scan_retries <= 3:
        parser.error("--retries must be in [0, 10] and --scan-retries in [0, 3]")
    if not 0 <= args.confirm_limit <= 1024:
        parser.error("--confirm-limit must be in [0, 1024]")
    if bool(args.cert) != bool(args.key):
        parser.error("--cert and --key must be provided together")
    return args


def run(args):
    supplied = [normalize_url(value) for value in args.endpoints]
    limiter = RateLimiter(args.interval)
    ssl_context = build_ssl_context(args)
    authorization = read_authorization(args.authorization_file)
    seed = HTTPClient(
        supplied[0], args.timeout, args.retries, limiter, ssl_context, authorization
    )
    clients = []
    try:
        nodes, leader_id, cluster_id, membership_start = discover_nodes(seed, supplied)
        clients = [
            HTTPClient(node.url, args.timeout, args.retries, limiter, ssl_context, authorization)
            for node in nodes
        ]

        with tempfile.TemporaryDirectory(prefix="pd-region-meta-checker-") as directory:
            db = create_database(str(Path(directory) / "regions.sqlite"))
            try:
                states = collect_regions(
                    db, nodes, clients, args.batch_size, args.scan_retries
                )
                initial_summary = summarize(db, nodes)
                confirmation = recheck_differences(
                    db,
                    nodes,
                    clients,
                    initial_summary["different_regions"],
                    args.confirm_limit,
                )
                membership_end = seed.get_json("/pd/api/v1/members", local=True)
                if membership_signature(membership_start) != membership_signature(membership_end):
                    raise CheckerError("PD membership or leader changed during the scan")
                summary = summarize(db, nodes)
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
                write_report(args.output, report, iter_differences(db, nodes))
                return exit_code
            finally:
                db.close()
    finally:
        seed.close()
        for client in clients:
            client.close()


def main(argv=None):
    try:
        return run(parse_args(argv))
    except KeyboardInterrupt:
        return 130
    except (
        CheckerError,
        OSError,
        ssl.SSLError,
        sqlite3.Error,
        TypeError,
        ValueError,
        AttributeError,
        OverflowError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
