"""Shaped SQL load for the RU peak dashboard demo."""
import concurrent.futures, json, random, signal, threading, time
from pathlib import Path
import pymysql

D = Path(__file__).resolve().parent
PORTS = {'a': 29401, 'b': 29402}
STOP = threading.Event()
LOG = open(D / 'load-events.jsonl', 'a', buffering=1)

def event(name, **fields):
    LOG.write(json.dumps({"time": time.time(), "event": name, **fields}) + '\n')

def connect(node):
    return pymysql.connect(host='127.0.0.1', port=PORTS[node], user='root', autocommit=True,
                           connect_timeout=5, read_timeout=30, write_timeout=30)

def run(cursor, group, kind, slot):
    hint = f'/*+ RESOURCE_GROUP({group}) */ '
    if kind == 'write':
        cursor.execute(f'UPDATE {hint}demo.t SET marker=marker+1, payload=REPEAT(%s, 512) WHERE id=%s', (chr(97 + slot % 26), slot % 1000))
    elif kind == 'scan':
        cursor.execute(f'SELECT {hint}SUM(LENGTH(payload)) FROM demo.t WHERE id BETWEEN %s AND %s', (slot % 500, slot % 500 + 400)); cursor.fetchall()
    else:
        cursor.execute(f'SELECT {hint}payload FROM demo.t WHERE id=%s', (slot % 1000,)); cursor.fetchall()

def steady(node, group, kinds, interval):
    slot = random.randrange(1000)
    while not STOP.is_set():
        try:
            with connect(node) as conn, conn.cursor() as cursor:
                while not STOP.is_set():
                    slot += 1
                    run(cursor, group, kinds[slot % len(kinds)], slot)
                    STOP.wait(interval * random.uniform(0.7, 1.3))
        except Exception as exc:
            event('steady_error', node=node, group=group, error=str(exc)); STOP.wait(3)

def burst(node, group, kind, count, at, workers=4):
    def worker(index):
        try:
            with connect(node) as conn, conn.cursor() as cursor:
                if STOP.wait(max(0, at - time.time())): return
                for slot in range(index, count, workers):
                    if STOP.is_set(): return
                    run(cursor, group, kind, slot)
        except Exception as exc:
            event('burst_error', node=node, group=group, error=str(exc))
    with concurrent.futures.ThreadPoolExecutor(workers) as pool:
        list(pool.map(worker, range(workers)))
    event('burst', node=node, group=group, kind=kind, count=count, at=at)

def periodic(period, jitter, fn):
    while not STOP.wait(period + random.uniform(-jitter, jitter)):
        threading.Thread(target=fn, daemon=True).start()

def main():
    for s in (signal.SIGTERM, signal.SIGINT):
        signal.signal(s, lambda *_: STOP.set())
    with connect('a') as conn, conn.cursor() as c:
        c.execute('CREATE DATABASE IF NOT EXISTS demo')
        c.execute('CREATE TABLE IF NOT EXISTS demo.t (id INT PRIMARY KEY, payload VARCHAR(1024), marker INT)')
        for i in range(0, 1000, 100):
            c.execute('INSERT IGNORE INTO demo.t VALUES ' + ','.join(f'({j}, REPEAT("x", 512), 0)' for j in range(i, i + 100)))
        for g in ('rg_oltp', 'rg_batch', 'rg_report', 'rg_etl'):
            c.execute(f'CREATE RESOURCE GROUP IF NOT EXISTS {g} RU_PER_SEC=1000000 BURSTABLE')
    event('started')
    threads = [threading.Thread(target=steady, args=(n, 'rg_oltp', ['point', 'point', 'write'], 0.05)) for n in PORTS]
    threads += [threading.Thread(target=steady, args=(n, 'default', ['point'], 0.5)) for n in PORTS]
    threads.append(threading.Thread(target=periodic, args=(180, 20, lambda: burst('a', 'rg_batch', 'write', 1200, time.time() + 0.2, 8))))
    def report():
        at = time.time() + 0.2
        for n in PORTS: threading.Thread(target=burst, args=(n, 'rg_report', 'scan', random.randrange(200, 500), at), daemon=True).start()
    threads.append(threading.Thread(target=periodic, args=(75, 25, report)))
    def etl():
        at = time.time() + 0.2
        threading.Thread(target=burst, args=('a', 'rg_etl', 'scan', 300, at), daemon=True).start()
        threading.Thread(target=burst, args=('b', 'rg_etl', 'scan', 300, at + random.uniform(4, 10)), daemon=True).start()
    threads.append(threading.Thread(target=periodic, args=(110, 20, etl)))
    for t in threads: t.daemon = True; t.start()
    while not STOP.wait(1): pass
    event('stopped')

if __name__ == '__main__':
    main()
