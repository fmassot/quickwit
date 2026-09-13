#!/usr/bin/env bash
# usage: ./bench_run.sh <clients> <batch_docs> <duration> "<label>"
cd ~/bench
scrape() { curl -s http://127.0.0.1:7280/metrics | grep -E '^quickwit_ingest_wal_object_(puts_total\{result="success"\}|bytes_written_total|records_written_total|put_duration_secs_sum|buffered_bytes)|^quickwit_ingest_shards\{state="open"\}|^quickwit_ingest_ingest_result_total\{result="(success|wal_full|no_shards_available|shard_rate_limited)"\}'; }
PID=$(pgrep -x quickwit)
scrape > m0.txt; cpu0=$(awk '{print $14+$15}' /proc/$PID/stat); t0=$(date +%s.%N)
~/ingest_bench/target/release/ingest_bench --clients $1 --batch-docs $2 --duration $3 --label "$4"
t1=$(date +%s.%N); cpu1=$(awk '{print $14+$15}' /proc/$PID/stat); scrape > m1.txt
T0=$t0 T1=$t1 CPU0=$cpu0 CPU1=$cpu1 python3 - <<'PY'
import os
def load(f): return {l.rsplit(" ",1)[0]: float(l.rsplit(" ",1)[1]) for l in open(f).read().splitlines()}
a,b=load("m0.txt"),load("m1.txt"); el=float(os.environ["T1"])-float(os.environ["T0"])
d=lambda k: b.get(k,0)-a.get(k,0)
puts=d('quickwit_ingest_wal_object_puts_total{result="success"}')
by=d('quickwit_ingest_wal_object_bytes_written_total')
print(f"quickwit CPU: {(float(os.environ['CPU1'])-float(os.environ['CPU0']))/100/el:.2f} cores")
print(f"WAL: puts={puts:.0f} ({puts/el:.1f}/s) records={d('quickwit_ingest_wal_object_records_written_total'):.0f} bytes={by/1e6:.0f}MB ({by/1e6/el:.1f} MB/s) obj={by/1e6/max(puts,1):.1f}MB put mean={d('quickwit_ingest_wal_object_put_duration_secs_sum')/max(puts,1)*1000:.0f}ms")
res={k.split('=')[1].strip('"}'): int(d(k)) for k in b if k.startswith("quickwit_ingest_ingest_result_total")}
print("open shards:", int(b.get('quickwit_ingest_shards{state="open"}',0)), " ingest results:", res)
PY
top -bn1 -o %CPU | grep -E "quickwit|ingest_bench" | head -2 | awk '{print $12, $9"%"}'
