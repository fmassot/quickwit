#!/usr/bin/env bash
# usage: ./sql.sh "<sql>"   (DDL for the metrics table is prepended)
DDL='CREATE OR REPLACE EXTERNAL TABLE metrics (metric_name VARCHAR NOT NULL, metric_type TINYINT, timestamp_secs BIGINT NOT NULL, value DOUBLE NOT NULL, hostname VARCHAR, region VARCHAR, datacenter VARCHAR, rack VARCHAR, os VARCHAR, arch VARCHAR, team VARCHAR, service VARCHAR, service_version VARCHAR, service_environment VARCHAR) STORED AS metrics LOCATION '"'"'otel-metrics-v0_9'"'"';'
SQL="$DDL $1"
python3 - "$SQL" <<'PY'
import json, subprocess, sys, base64, io, time
sql = sys.argv[1]
t0 = time.time()
out = subprocess.run(["grpcurl", "-plaintext", "-max-msg-sz", "268435456", "-d", json.dumps({"sql": sql}), "127.0.0.1:7281", "quickwit.datafusion.DataFusionService/ExecuteSql"], capture_output=True, text=True)
el = time.time() - t0
if out.returncode != 0:
    print("ERROR:", out.stderr.strip()[:500]); sys.exit(1)
import pyarrow.ipc as ipc
rows = 0; shown = 0
# grpcurl prints one JSON object per streamed message
dec = json.JSONDecoder(); s = out.stdout; i = 0
while i < len(s):
    while i < len(s) and s[i].isspace(): i += 1
    if i >= len(s): break
    obj, j = dec.raw_decode(s, i); i = j
    b = base64.b64decode(obj.get("arrowIpcBytes", ""))
    if not b: continue
    reader = ipc.open_stream(io.BytesIO(b))
    for batch in reader:
        rows += batch.num_rows
        if shown < 12:
            for r in batch.to_pylist()[:12 - shown]:
                print(r); shown += 1
print(f"-- {rows} rows in {el*1000:.0f} ms")
PY
