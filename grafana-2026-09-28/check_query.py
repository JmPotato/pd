"""Compare the dashboard query with the raw per-minute samples on Prometheus 2.x and 3.x."""
import json, time, urllib.parse, urllib.request
M = 'resource_manager_resource_unit_peak_per_second{k8s_cluster="local",tidb_cluster="ru-peak-demo",keyspace_name="scale-sql"}'
def q(port, path, **params):
    url = f'http://127.0.0.1:{port}/api/v1/{path}?' + urllib.parse.urlencode(params)
    return json.load(urllib.request.urlopen(url))['data']['result']
def series(res):
    return {r['metric']['resource_group']: {int(float(t)): float(v) for t, v in r['values']} for r in res}
now = int(time.time()) // 60 * 60 - 120
start = now - 32 * 60
for port, name in [(29590, 'prometheus 3.14'), (29592, 'prometheus 2.55')]:
    raw = series(q(port, 'query', query=f'{M}[40m]', time=now))
    for step in (60, 120):
        panel = f'max_over_time({M}[{step}s] offset -1s) and (count_over_time({M}[{step}s] offset -1s) == {step // 60})'
        naive = f'max_over_time({M}[{step}s])'
        got = series(q(port, 'query_range', query=panel, start=start, end=now, step=step))
        old = series(q(port, 'query_range', query=naive, start=start, end=now, step=step))
        bad = oldbad = points = 0
        for g, samples in raw.items():
            for t in range(start, now + 1, step):
                want = [samples.get(t - k * 60) for k in range(step // 60)]
                exp = max(want) if all(v is not None for v in want) else None
                points += 1
                if got.get(g, {}).get(t) != exp: bad += 1
                if exp is not None and old.get(g, {}).get(t) != exp: oldbad += 1
        print(f'{name} step={step}s: {points} points, dashboard query mismatches={bad}, naive [{step}s] mismatches on complete intervals={oldbad}')
