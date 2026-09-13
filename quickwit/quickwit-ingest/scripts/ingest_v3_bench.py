#!/usr/bin/env python3
"""Load generator for the ingest v3 object-store WAL benchmark.

Fires `POST /api/v1/<index>/ingest?commit=wait_for` requests from N concurrent clients, each
carrying a batch of `--batch-docs` JSON docs, for `--duration` seconds, and reports the client-side
acknowledgement latency distribution alongside the WAL metrics scraped from `/metrics` before and
after the run.

Standard library only (no aiohttp on a fresh box): one thread per client, `http.client`.

    ./ingest_v3_bench.py --url http://127.0.0.1:7280 --index bench --clients 32 --batch-docs 100 \
        --duration 60 --label "flush=250ms"
"""

import argparse
import http.client
import json
import random
import statistics
import string
import threading
import time
import urllib.parse
import urllib.request

METRICS = [
    "quickwit_ingest_wal_object_puts_total",
    "quickwit_ingest_wal_object_put_duration_secs",
    "quickwit_ingest_wal_object_flush_latency_secs",
    "quickwit_ingest_wal_object_bytes_written_total",
    "quickwit_ingest_wal_object_records_written_total",
    "quickwit_ingest_wal_object_buffered_bytes",
]


def make_doc(rng, i):
    return json.dumps({
        "ts": int(time.time() * 1000),
        "seq": i,
        "level": rng.choice(["INFO", "WARN", "ERROR", "DEBUG"]),
        "service": rng.choice(["api", "auth", "billing", "search", "ingest"]),
        "msg": "".join(rng.choices(string.ascii_lowercase + " ", k=rng.randint(60, 160))),
        "latency_ms": rng.randint(1, 5000),
    })


def scrape(url):
    with urllib.request.urlopen(f"{url}/metrics", timeout=10) as resp:
        text = resp.read().decode()
    out = {}
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        for m in METRICS:
            if line.startswith(m):
                name, _, value = line.rpartition(" ")
                out[name] = float(value)
    return out


def hist_quantiles(metrics, name):
    """Approximate quantiles from a Prometheus histogram's cumulative buckets."""
    buckets = []
    for key, value in metrics.items():
        if key.startswith(name + "_bucket{"):
            le = key.split('le="')[1].split('"')[0]
            if le == "+Inf":
                continue
            buckets.append((float(le), value))
    buckets.sort()
    total = metrics.get(name + "_count", 0)
    if not buckets or total == 0:
        return {}
    result = {}
    for q in (0.5, 0.9, 0.99):
        target = q * total
        for le, cum in buckets:
            if cum >= target:
                result[q] = le
                break
        else:
            result[q] = float("inf")
    return result


def worker(args, client_idx, deadline, latencies, errors, stop):
    rng = random.Random(client_idx)
    parsed = urllib.parse.urlparse(args.url)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=60)
    path = f"/api/v1/{args.index}/ingest?commit=wait_for" if args.wait_for else f"/api/v1/{args.index}/ingest"
    i = 0
    while time.time() < deadline and not stop.is_set():
        body = "\n".join(make_doc(rng, i + k) for k in range(args.batch_docs)).encode()
        i += args.batch_docs
        start = time.perf_counter()
        try:
            conn.request("POST", path, body=body, headers={"Content-Type": "application/json"})
            resp = conn.getresponse()
            resp.read()
            if resp.status != 200:
                errors.append(resp.status)
                continue
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc)[:60])
            conn.close()
            conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=60)
            time.sleep(0.1)
            continue
        latencies.append(time.perf_counter() - start)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:7280")
    parser.add_argument("--index", default="bench")
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--batch-docs", type=int, default=100)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--label", default="")
    parser.add_argument("--no-wait-for", dest="wait_for", action="store_false")
    parser.add_argument("--pid", type=int, default=0, help="quickwit pid, for CPU usage sampling")
    args = parser.parse_args()

    def cpu_seconds(pid):
        if not pid:
            return 0.0
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().split(") ")[1].split()
        return (int(fields[11]) + int(fields[12])) / 100.0  # utime + stime, clock ticks

    before = scrape(args.url)
    cpu_before = cpu_seconds(args.pid)
    latencies, errors = [], []
    stop = threading.Event()
    deadline = time.time() + args.duration
    threads = [
        threading.Thread(target=worker, args=(args, i, deadline, latencies, errors, stop), daemon=True)
        for i in range(args.clients)
    ]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - t0
    after = scrape(args.url)
    cpu_cores = (cpu_seconds(args.pid) - cpu_before) / elapsed if args.pid else float("nan")

    def delta(name):
        return after.get(name, 0) - before.get(name, 0)

    puts = delta('quickwit_ingest_wal_object_puts_total{result="success"}')
    records = delta("quickwit_ingest_wal_object_records_written_total")
    wal_bytes = delta("quickwit_ingest_wal_object_bytes_written_total")
    put_sum = delta("quickwit_ingest_wal_object_put_duration_secs_sum")
    put_q = hist_quantiles(after, "quickwit_ingest_wal_object_put_duration_secs")
    flush_q = hist_quantiles(after, "quickwit_ingest_wal_object_flush_latency_secs")

    latencies.sort()
    n = len(latencies)
    pct = lambda p: latencies[min(n - 1, int(p * n))] * 1000 if n else float("nan")  # noqa: E731
    docs = n * args.batch_docs
    print(f"=== {args.label} clients={args.clients} batch={args.batch_docs} docs wait_for={args.wait_for}")
    print(f"requests: {n}  errors: {len(errors)}  docs: {docs}  docs/s: {docs / elapsed:,.0f}  "
          f"MB/s(json): {sum(len(make_doc(random.Random(0), 0)) for _ in range(1)) * docs / elapsed / 1e6:,.1f}")
    if n:
        print(f"ack latency ms: p50={pct(0.5):.0f} p90={pct(0.9):.0f} p99={pct(0.99):.0f} "
              f"max={latencies[-1] * 1000:.0f} mean={statistics.fmean(latencies) * 1000:.0f}")
    print(f"quickwit CPU: {cpu_cores:.2f} cores")
    print(f"WAL: puts={puts:.0f} ({puts / elapsed:.2f}/s)  records={records:.0f}  "
          f"bytes={wal_bytes / 1e6:.1f}MB  docs/put={records / puts if puts else 0:,.0f}  "
          f"put mean={put_sum / puts * 1000 if puts else 0:.0f}ms")
    print(f"WAL put duration (cumulative histogram) p50<={put_q.get(0.5, 0) * 1000:.0f}ms "
          f"p90<={put_q.get(0.9, 0) * 1000:.0f}ms p99<={put_q.get(0.99, 0) * 1000:.0f}ms")
    print(f"WAL flush latency (cumulative histogram) p50<={flush_q.get(0.5, 0) * 1000:.0f}ms "
          f"p90<={flush_q.get(0.9, 0) * 1000:.0f}ms p99<={flush_q.get(0.99, 0) * 1000:.0f}ms")
    if errors:
        print("errors sample:", errors[:5])


if __name__ == "__main__":
    main()
