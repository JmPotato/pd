#!/usr/bin/env python3

import copy
import json
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from socketserver import TCPServer
from urllib.parse import parse_qs, urlparse


SCRIPT = Path(__file__).with_name("pd_region_meta_checker.py")


def peer(peer_id, store_id):
    return {
        "id": peer_id,
        "store_id": store_id,
        "role_name": "Voter",
    }


def region(region_id, start_key, end_key, peers=None, leader=None):
    peers = peers or [peer(region_id * 10, 1)]
    return {
        "id": region_id,
        "start_key": start_key,
        "end_key": end_key,
        "epoch": {"conf_ver": 1, "version": 1},
        "peers": peers,
        "leader": leader or peers[0],
    }


class PDHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        self.server.request_paths.append(parsed.path)
        if parsed.path == self.server.failure_path:
            self.send_response(503)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if parsed.path == "/pd/api/v1/members":
            return self._json(self.server.members)
        if parsed.path.startswith("/pd/api/v1/region/id/"):
            self._record_local_request()
            region_id = int(parsed.path.rsplit("/", 1)[1])
            value = next(
                (copy.deepcopy(item) for item in self.server.regions if item["id"] == region_id),
                {},
            )
            return self._json(value)
        if parsed.path == "/pd/api/v1/regions/count":
            self._record_local_request()
            self.server.count_calls += 1
            count = len(self.server.regions)
            if self.server.count_hook:
                self.server.count_hook(self.server.count_calls)
            return self._json({"count": count})
        if parsed.path == "/pd/api/v1/regions/key":
            self._record_local_request()
            query = parse_qs(parsed.query, keep_blank_values=True)
            self.server.scan_queries.append(query)
            start = query.get("key", [""])[0].upper()
            end = query.get("end_key", [""])[0].upper()
            limit = int(query.get("limit", ["16"])[0])
            if self.server.page_limit is not None:
                limit = min(limit, self.server.page_limit)
            regions = copy.deepcopy([
                item
                for item in self.server.regions
                if (not item["end_key"] or item["end_key"] > start)
                and (not end or item["start_key"] < end)
            ][:limit])
            self.server.scan_calls += 1
            if self.server.scan_hook:
                self.server.scan_hook(self.server.scan_calls)
            return self._json({"count": len(regions), "regions": regions})
        self.send_error(404)

    def _record_local_request(self):
        self.server.local_headers.append(
            (
                self.headers.get("PD-Allow-Follower-Handle"),
                self.headers.get("X-Caller-ID"),
                self.headers.get("PD-Redirector"),
            )
        )

    def _json(self, value):
        if self.server.response_delay:
            time.sleep(self.server.response_delay)
        payload = json.dumps(value).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *_args):
        pass


class LocalHTTPServer(ThreadingHTTPServer):
    def server_bind(self):
        # Avoid HTTPServer's reverse-DNS lookup; it is unrelated to the fixture.
        TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address


class FakePD:
    def __init__(self, regions):
        self.server = LocalHTTPServer(("127.0.0.1", 0), PDHandler)
        self.server.regions = regions
        self.server.members = {}
        self.server.scan_queries = []
        self.server.local_headers = []
        self.server.count_calls = 0
        self.server.count_hook = None
        self.server.scan_calls = 0
        self.server.scan_hook = None
        self.server.page_limit = None
        self.server.request_paths = []
        self.server.failure_path = None
        self.server.response_delay = 0
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self):
        host, port = self.server.server_address
        return f"http://{host}:{port}"

    def start(self):
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


class CheckerCLITest(unittest.TestCase):
    def setUp(self):
        base = [
            region(1, "", "10"),
            region(2, "10", "20"),
            region(3, "20", "30", [peer(30, 1), peer(31, 2)]),
            region(4, "30", "40"),
            region(5, "40", ""),
        ]
        self.leader = FakePD(copy.deepcopy(base))
        self.follower = FakePD(copy.deepcopy(base))
        self.follower_2 = FakePD(copy.deepcopy(base))
        self.leader.start()
        self.follower.start()
        self.follower_2.start()
        self.leader_instance = f"pd-leader@{urlparse(self.leader.url).netloc}"
        self.follower_instance = f"pd-follower@{urlparse(self.follower.url).netloc}"
        self.follower_2_instance = f"pd-follower-2@{urlparse(self.follower_2.url).netloc}"
        members = [
            {"name": "pd-leader", "member_id": 1, "client_urls": [self.leader.url]},
            {"name": "pd-follower", "member_id": 2, "client_urls": [self.follower.url]},
            {"name": "pd-follower-2", "member_id": 3, "client_urls": [self.follower_2.url]},
        ]
        membership = {"members": members, "leader": members[0]}
        self.leader.server.members = membership
        self.follower.server.members = membership
        self.follower_2.server.members = membership

    def tearDown(self):
        self.leader.close()
        self.follower.close()
        self.follower_2.close()

    def run_checker(self, *extra):
        return subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                self.leader.url,
                self.follower.url,
                self.follower_2.url,
                "--batch-size",
                "2",
                "--interval",
                "0",
                "--timeout",
                "2",
                *extra,
            ],
            check=False,
            capture_output=True,
            text=True,
        )

    def test_consistent_cluster_uses_small_local_batches(self):
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "consistent")
        self.assertEqual(report["summary"]["different_regions"], 0)
        self.assertEqual(report["reference"]["name"], "pd-leader")
        self.assertEqual(report["settings"]["http_requests"], 17)
        self.assertEqual(report["settings"]["max_runtime_seconds"], 14400)
        self.assertGreater(report["settings"]["http_response_bytes"], 0)
        self.assertEqual(report["settings"]["temporary_disk_peak_bytes"], 0)

        for server in (self.leader.server, self.follower.server, self.follower_2.server):
            self.assertTrue(server.scan_queries)
            self.assertTrue(all(q["format"] == ["hex"] for q in server.scan_queries))
            self.assertTrue(all(int(q["limit"][0]) <= 2 for q in server.scan_queries))
            self.assertTrue(server.local_headers)
            self.assertTrue(all(value == "true" for value, _, _ in server.local_headers))
            self.assertTrue(
                all(caller == "pd-region-meta-checker" for _, caller, _ in server.local_headers)
            )
            self.assertTrue(
                all(
                    redirector == "pd-region-meta-checker"
                    for _, _, redirector in server.local_headers
                )
            )

    def test_writes_json_report_file(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.json"
            work_dir = Path(directory) / "work"
            work_dir.mkdir()
            result = self.run_checker(
                "--work-dir",
                str(work_dir),
                "--output",
                str(output),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, "")
            report = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "consistent")
            self.assertEqual(list(work_dir.iterdir()), [])

    def test_rejects_authorization_over_plain_http(self):
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as auth:
            auth.write("Bearer secret\n")
            auth.flush()
            result = self.run_checker("--authorization-file", auth.name)

        self.assertEqual(result.returncode, 2)
        self.assertIn("Authorization requires HTTPS", result.stderr)
        self.assertEqual(self.leader.server.local_headers, [])

    def test_identifies_instances_by_member_name_and_endpoint(self):
        self.follower.server.regions[0]["epoch"]["version"] = 2

        result = self.run_checker()
        self.assertEqual(result.returncode, 1, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(
            set(report["differences"][0]["epoch"]),
            {
                self.leader_instance,
                self.follower_instance,
                self.follower_2_instance,
            },
        )

    def test_reports_each_region_meta_difference_category(self):
        self.follower.server.regions[0]["epoch"]["version"] = 2
        self.follower.server.regions[1]["start_key"] = "11"
        self.follower.server.regions[2]["leader"] = peer(31, 2)
        self.follower.server.regions[3]["peers"].append(peer(41, 2))
        self.follower.server.regions.pop()

        result = self.run_checker()
        self.assertEqual(result.returncode, 1, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "inconsistent")
        self.assertEqual(report["summary"]["different_regions"], 5)
        self.assertEqual(
            report["summary"]["by_field"],
            {
                "missing_on": 1,
                "key_range": 1,
                "epoch": 1,
                "peers": 1,
                "leader_peer": 1,
            },
        )
        differences = {item["region_id"]: item for item in report["differences"]}
        self.assertEqual(set(differences[1]), {"region_id", "epoch"})
        self.assertEqual(differences[1]["epoch"][self.follower_instance]["version"], 2)
        self.assertEqual(set(differences[2]), {"region_id", "key_range"})
        self.assertEqual(
            differences[2]["key_range"][self.follower_instance]["start_key"], "11"
        )
        self.assertEqual(set(differences[3]), {"region_id", "leader_peer"})
        self.assertEqual(differences[3]["leader_peer"][self.follower_instance]["id"], 31)
        self.assertEqual(set(differences[4]), {"region_id", "peers"})
        self.assertEqual(differences[4]["peers"][self.follower_instance][-1]["id"], 41)
        self.assertEqual(
            differences[5], {"region_id": 5, "missing_on": [self.follower_instance]}
        )

    def test_reports_pd_leader_peer_values_directly(self):
        self.leader.server.regions[2]["leader"] = peer(31, 2)

        result = self.run_checker()
        self.assertEqual(result.returncode, 1, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(
            report["differences"],
            [
                {
                    "region_id": 3,
                    "leader_peer": {
                        self.leader_instance: {
                            "id": 31,
                            "store_id": 2,
                            "role": 0,
                            "is_witness": False,
                        },
                        self.follower_instance: {
                            "id": 30,
                            "store_id": 1,
                            "role": 0,
                            "is_witness": False,
                        },
                        self.follower_2_instance: {
                            "id": 30,
                            "store_id": 1,
                            "role": 0,
                            "is_witness": False,
                        },
                    },
                }
            ],
        )
        self.assertEqual(
            report["summary"],
            {"different_regions": 1, "by_field": {"leader_peer": 1}},
        )

    def test_reports_leader_peer_only_and_multi_axis_no_consensus(self):
        # Region 3 only disagrees on the elected Region Leader peer.
        self.follower.server.regions[2]["leader"] = peer(31, 2)

        # Region 2 has three different ranges and Epoch pairs across the PD members.
        self.follower.server.regions[1]["start_key"] = "11"
        self.follower.server.regions[1]["epoch"]["version"] = 2
        self.follower_2.server.regions[1]["end_key"] = "21"
        self.follower_2.server.regions[1]["epoch"]["conf_ver"] = 2

        result = self.run_checker()
        self.assertEqual(result.returncode, 1, result.stderr)
        report = json.loads(result.stdout)
        differences = {item["region_id"]: item for item in report["differences"]}

        leader_only = differences[3]
        self.assertEqual(set(leader_only), {"region_id", "leader_peer"})
        self.assertEqual(
            leader_only["leader_peer"][self.follower_instance]["id"],
            31,
        )

        multi_axis = differences[2]
        self.assertEqual(set(multi_axis), {"region_id", "key_range", "epoch"})
        self.assertEqual(
            multi_axis["epoch"][self.follower_instance],
            {"conf_ver": 1, "version": 2},
        )
        self.assertEqual(
            multi_axis["epoch"][self.follower_2_instance],
            {"conf_ver": 2, "version": 1},
        )
        self.assertEqual(report["summary"]["different_regions"], 2)
        self.assertEqual(
            report["summary"]["by_field"],
            {"key_range": 1, "epoch": 1, "leader_peer": 1},
        )

    def test_matches_region_ids_across_divergent_key_segments(self):
        moved = copy.deepcopy(self.leader.server.regions[0])
        moved["start_key"] = "10"
        moved["end_key"] = "20"
        replacement = region(9, "", "10")
        self.follower.server.regions[0:2] = [replacement, moved]

        result = self.run_checker()
        self.assertEqual(result.returncode, 1, result.stderr)
        report = json.loads(result.stdout)
        differences = {item["region_id"]: item for item in report["differences"]}
        self.assertEqual(set(differences), {1, 2, 9})
        self.assertEqual(set(differences[1]), {"region_id", "key_range"})
        self.assertEqual(
            differences[1]["key_range"][self.follower_instance],
            {"start_key": "10", "end_key": "20"},
        )
        self.assertEqual(differences[2]["missing_on"], [self.follower_instance])
        self.assertEqual(
            differences[9]["missing_on"],
            [self.leader_instance, self.follower_2_instance],
        )

    def test_ignores_peer_order_and_non_meta_heartbeat_fields(self):
        for server in (self.follower.server, self.follower_2.server):
            server.regions[2]["peers"].reverse()
            server.regions[0]["written_bytes"] = 999
            server.regions[0]["pending_peers"] = [peer(999, 9)]
            server.regions[0]["buckets"] = ["10"]

        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["status"], "consistent")

    def test_reports_peer_role_and_witness_differences(self):
        self.follower.server.regions[2]["peers"][0]["role"] = 1
        self.follower_2.server.regions[2]["peers"][0]["is_witness"] = True
        for server in (self.follower.server, self.follower_2.server):
            server.regions[2]["leader"] = peer(30, 1)

        result = self.run_checker()
        self.assertEqual(result.returncode, 1, result.stderr)
        report = json.loads(result.stdout)
        difference = report["differences"][0]
        self.assertEqual(set(difference), {"region_id", "peers"})
        self.assertEqual(
            difference["peers"][self.follower_instance][0]["role"], 1
        )
        self.assertTrue(
            difference["peers"][self.follower_2_instance][0]["is_witness"]
        )

    def test_rejects_unbounded_batch_size(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), self.leader.url, "--batch-size", "0"],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--batch-size must be in [1, 1024]", result.stderr)

    def test_rejects_non_finite_request_timing(self):
        for flag, value in (
            ("--interval", "nan"),
            ("--timeout", "inf"),
            ("--max-runtime", "nan"),
        ):
            with self.subTest(flag=flag, value=value):
                result = subprocess.run(
                    [sys.executable, str(SCRIPT), self.leader.url, flag, value],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 2)
                self.assertIn(
                    "--interval must be finite and non-negative", result.stderr
                )

    def test_hard_runtime_limit_interrupts_a_slow_response(self):
        self.leader.server.response_delay = 0.5
        started = time.monotonic()

        result = self.run_checker("--max-runtime", "0.1")

        self.assertEqual(result.returncode, 2)
        self.assertIn("maximum runtime of 0.1s exceeded", result.stderr)
        self.assertLess(time.monotonic() - started, 1.0)

    def test_bounds_page_requests_when_api_returns_short_pages(self):
        self.leader.server.page_limit = 1
        result = self.run_checker()
        self.assertEqual(result.returncode, 2)
        self.assertIn("exceeded the bounded Region page count", result.stderr)

    def test_rejects_invalid_endpoint_without_traceback(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "http://127.0.0.1:not-a-port"],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("invalid PD URL", result.stderr)
        self.assertNotIn("Traceback", result.stderr)

    def test_does_not_retry_an_oversized_response(self):
        self.leader.server.members["padding"] = "x" * (8 * 1024 * 1024)

        result = self.run_checker("--retries", "2")

        self.assertEqual(result.returncode, 2)
        self.assertIn("response exceeds 8 MiB", result.stderr)
        self.assertEqual(
            self.leader.server.request_paths.count("/pd/api/v1/members"), 1
        )

    def test_default_does_not_retry_a_retryable_http_error(self):
        self.leader.server.failure_path = "/pd/api/v1/regions/count"

        result = self.run_checker()

        self.assertEqual(result.returncode, 2)
        self.assertIn("HTTP 503", result.stderr)
        self.assertEqual(
            self.leader.server.request_paths.count("/pd/api/v1/regions/count"), 1
        )

    def test_accepts_uint64_region_ids_and_epoch(self):
        large_uint64 = 2**63 + 1
        for server in (self.leader.server, self.follower.server, self.follower_2.server):
            server.regions[0]["id"] = large_uint64
            server.regions[0]["epoch"] = {
                "conf_ver": large_uint64,
                "version": large_uint64,
            }

        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["status"], "consistent")

    def test_retries_all_members_when_any_region_count_changes(self):
        def split_after_leader_count(count_calls):
            if count_calls != 2:
                return
            self.leader.server.count_hook = None
            for server in (self.leader.server, self.follower.server, self.follower_2.server):
                server.regions[-1]["end_key"] = "50"
                server.regions.append(region(6, "50", ""))

        self.leader.server.count_hook = split_after_leader_count
        result = self.run_checker("--scan-retries", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "consistent")
        self.assertEqual([node["scan_attempts"] for node in report["nodes"]], [2, 2, 2])

    def test_default_does_not_repeat_a_full_cluster_scan(self):
        def split_after_leader_count(count_calls):
            if count_calls != 2:
                return
            self.leader.server.count_hook = None
            for server in (self.leader.server, self.follower.server, self.follower_2.server):
                server.regions[-1]["end_key"] = "50"
                server.regions.append(region(6, "50", ""))

        self.leader.server.count_hook = split_after_leader_count
        result = self.run_checker()
        self.assertEqual(result.returncode, 2)
        self.assertIn("unstable Region set after 1 cluster-wide scan attempt", result.stderr)
        self.assertEqual(self.leader.server.scan_calls, 3)

    def test_temporary_json_limit_is_enforced_and_cleaned(self):
        count = 6000
        base = [
            region(
                item + 1,
                "" if item == 0 else f"{item:08X}",
                "" if item == count - 1 else f"{item + 1:08X}",
            )
            for item in range(count)
        ]
        self.leader.server.regions = copy.deepcopy(base)
        self.follower.server.regions = copy.deepcopy(base)
        self.follower_2.server.regions = copy.deepcopy(base)
        for item in self.follower.server.regions:
            item["epoch"]["version"] = 2

        with tempfile.TemporaryDirectory() as directory:
            work_dir = Path(directory) / "work"
            work_dir.mkdir()
            result = self.run_checker(
                "--batch-size",
                "1024",
                "--work-dir",
                str(work_dir),
                "--max-temporary-disk-mib",
                "1",
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("temporary JSON data exceeds 1 MiB", result.stderr)
            self.assertEqual(list(work_dir.iterdir()), [])

    def test_output_limit_preserves_existing_report_and_cleans_work_files(self):
        count = 6000
        base = [
            region(
                item + 1,
                "" if item == 0 else f"{item:08X}",
                "" if item == count - 1 else f"{item + 1:08X}",
            )
            for item in range(count)
        ]
        self.leader.server.regions = copy.deepcopy(base)
        self.follower.server.regions = copy.deepcopy(base)
        self.follower_2.server.regions = copy.deepcopy(base)
        for item in self.follower.server.regions:
            item["epoch"]["version"] = 2

        with tempfile.TemporaryDirectory() as directory:
            work_dir = Path(directory) / "work"
            work_dir.mkdir()
            output = Path(directory) / "report.json"
            output.write_text("existing report\n", encoding="utf-8")
            result = self.run_checker(
                "--batch-size",
                "1024",
                "--work-dir",
                str(work_dir),
                "--max-output-mib",
                "1",
                "--output",
                str(output),
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("JSON report exceeds 1 MiB", result.stderr)
            self.assertEqual(output.read_text(encoding="utf-8"), "existing report\n")
            self.assertEqual(list(work_dir.iterdir()), [])

    def test_rechecks_transient_meta_differences(self):
        def update_after_first_leader_page(scan_calls):
            if scan_calls != 1:
                return
            self.leader.server.scan_hook = None
            for server in (self.leader.server, self.follower.server, self.follower_2.server):
                server.regions[0]["epoch"]["version"] = 2

        self.leader.server.scan_hook = update_after_first_leader_page
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "consistent")
        self.assertEqual(report["confirmation"]["initial_differences"], 1)
        self.assertEqual(report["confirmation"]["result"], "resolved")

    def test_rejects_member_identity_change_during_scan(self):
        def rename_member_after_first_page(scan_calls):
            if scan_calls != 1:
                return
            self.leader.server.scan_hook = None
            self.leader.server.members["members"][1]["name"] = "pd-renamed"

        self.leader.server.scan_hook = rename_member_after_first_page
        result = self.run_checker()

        self.assertEqual(result.returncode, 2)
        self.assertIn("PD membership or leader changed during the scan", result.stderr)

    def test_marks_unconfirmed_differences_incomplete(self):
        self.follower.server.regions[0]["epoch"]["version"] = 2
        result = self.run_checker("--confirm-limit", "0")
        self.assertEqual(result.returncode, 2, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "incomplete")
        self.assertEqual(report["confirmation"]["result"], "confirmation_disabled")
        self.assertEqual(set(report["differences"][0]), {"region_id", "epoch"})

    def test_confirms_large_static_difference_set_with_bounded_requests(self):
        for item in self.follower.server.regions:
            item["epoch"]["version"] = 2

        result = self.run_checker("--confirm-limit", "1")
        self.assertEqual(result.returncode, 1, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "inconsistent")
        self.assertEqual(report["confirmation"]["result"], "stable")
        self.assertEqual(report["confirmation"]["checked_regions"], 1)
        self.assertEqual(report["confirmation"]["unconfirmed_regions"], 4)
        self.assertEqual(report["summary"]["different_regions"], 5)


if __name__ == "__main__":
    unittest.main()
