#!/usr/bin/env python3
"""Measure checker streaming cost with generated HTTP data, not PD performance."""

import argparse
import json
import resource
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


SCRIPT = Path(__file__).with_name("pd_region_meta_checker.py")


def positive_int(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("REGION_COUNT must be positive")
    return parsed


def make_region(index, region_count):
    region_id = index + 1
    peers = [
        {"id": region_id * 10 + peer, "store_id": peer + 1, "role": 0}
        for peer in range(3)
    ]
    return {
        "id": region_id,
        "start_key": "" if index == 0 else f"{index:016X}",
        "end_key": "" if region_id == region_count else f"{region_id:016X}",
        "epoch": {"conf_ver": 1, "version": 1},
        "peers": peers,
        "leader": peers[0],
    }


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        parsed = urlparse(self.path)
        self.server.requests += 1
        if parsed.path == "/pd/api/v1/members":
            return self.respond(self.server.members)
        if parsed.path == "/pd/api/v1/regions/count":
            return self.respond({"count": self.server.region_count})
        if parsed.path == "/pd/api/v1/regions/key":
            query = parse_qs(parsed.query, keep_blank_values=True)
            cursor = query.get("key", [""])[0]
            start = int(cursor, 16) if cursor else 0
            limit = int(query["limit"][0])
            end = min(start + limit, self.server.region_count)
            regions = [
                make_region(index, self.server.region_count)
                for index in range(start, end)
            ]
            return self.respond({"count": len(regions), "regions": regions})
        self.send_error(404)

    def respond(self, value):
        payload = json.dumps(value, separators=(",", ":")).encode()
        self.server.response_bytes += len(payload)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args):
        pass


def start_servers(region_count):
    servers = []
    threads = []
    for _ in range(3):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        server.region_count = region_count
        server.requests = 0
        server.response_bytes = 0
        servers.append(server)

    members = [
        {
            "name": f"pd-{index}",
            "member_id": index + 1,
            "client_urls": [f"http://127.0.0.1:{server.server_port}"],
        }
        for index, server in enumerate(servers)
    ]
    membership = {
        "header": {"cluster_id": 1},
        "members": members,
        "leader": members[0],
    }
    for server in servers:
        server.members = membership
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        threads.append(thread)
    return servers, threads, members


def max_rss_bytes(usage):
    return usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Exercise the checker's streaming path with three generated HTTP fixtures. "
            "The result does not measure PD Server."
        )
    )
    parser.add_argument("region_count", type=positive_int, metavar="REGION_COUNT")
    args = parser.parse_args(argv)

    servers, threads, members = start_servers(args.region_count)
    try:
        with tempfile.TemporaryDirectory(prefix="pd-region-meta-stream-benchmark-") as directory:
            report_path = Path(directory) / "report.json"
            command = [
                sys.executable,
                str(SCRIPT),
                *(member["client_urls"][0] for member in members),
                "--batch-size",
                "128",
                "--interval",
                "0",
                "--timeout",
                "10",
                "--retries",
                "0",
                "--output",
                str(report_path),
            ]
            child_before = resource.getrusage(resource.RUSAGE_CHILDREN)
            started = time.monotonic()
            result = subprocess.run(command, check=False, capture_output=True, text=True)
            elapsed = time.monotonic() - started
            child_after = resource.getrusage(resource.RUSAGE_CHILDREN)
            if result.returncode != 0:
                sys.stderr.write(result.stderr)
                return result.returncode
            report = json.loads(report_path.read_text(encoding="utf-8"))
            metrics = {
                "region_count_per_instance": args.region_count,
                "status": report["status"],
                "elapsed_seconds": elapsed,
                "checker_user_seconds": child_after.ru_utime - child_before.ru_utime,
                "checker_system_seconds": child_after.ru_stime - child_before.ru_stime,
                "checker_max_rss_bytes": max_rss_bytes(child_after),
                "http_requests": sum(server.requests for server in servers),
                "http_response_bytes": sum(
                    server.response_bytes for server in servers
                ),
                "report_bytes": report_path.stat().st_size,
            }
            print(json.dumps(metrics, separators=(",", ":")))
            return 0
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join()


if __name__ == "__main__":
    sys.exit(main())
