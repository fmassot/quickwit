#!/usr/bin/env bash
# usage: FLUSH_MS=250 ./run.sh   (starts quickwit in background, waits for ready)
set -e
cd ~/bench; source env.sh
if pgrep -x quickwit >/dev/null; then
  pkill -x quickwit; for i in $(seq 1 30); do pgrep -x quickwit >/dev/null || break; sleep 1; done
  pgrep -x quickwit >/dev/null && pkill -9 -x quickwit; sleep 1
fi
nohup ~/quickwit/quickwit/target/release/quickwit run --config node.yaml > qw-$(date +%H%M%S).log 2>&1 &
echo $! > qw.pid
for i in $(seq 1 120); do curl -sf http://127.0.0.1:7280/health/readyz >/dev/null 2>&1 && break; sleep 1; done
curl -sf http://127.0.0.1:7280/api/v1/indexes/bench >/dev/null 2>&1 || curl -s -XPOST -H 'content-type: application/yaml' --data-binary @index.yaml http://127.0.0.1:7280/api/v1/indexes >/dev/null
echo "quickwit up (flush=${FLUSH_MS:-250}ms), pid $(cat qw.pid)"
