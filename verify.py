import collections, json, math
from pathlib import Path
p = Path(__file__).parent / "random-load"
result = json.loads((p / "results.json").read_text())
buckets = collections.defaultdict(lambda: [[], []])
count = 0
for node in ["a", "b"]:
    for line in (p / f"events-{node}.jsonl").read_text().splitlines():
        row = json.loads(line)
        second = row["time_ns"] // 1_000_000_000
        buckets[second][0].append(row["rru"])
        buckets[second][1].append(row["wru"])
        count += 1
for row in result["checks"]:
    end = row["minute_end"]
    values = {s: [math.fsum(x) for x in buckets[s]] for s in range(end - 60, end)}
    peak_second = max(values, key=lambda s: math.fsum(values[s]))
    assert peak_second == row["metric_peak_second"]
    total = math.fsum(values[peak_second])
    assert math.isclose(total, row["metric_ru_per_second"], rel_tol=1e-10, abs_tol=1e-8)
    for expected, name in zip(values[peak_second], ["metric_rru", "metric_wru"]):
        assert math.isclose(expected, row[name], rel_tol=1e-10, abs_tol=1e-8)
print(f"{count} accounting events; {len(result['checks'])} complete minutes matched")
