#!/usr/bin/env bash
cd ~/bench
T0=1700000000
run() { echo "## $1"; ./sql.sh "$2" | tail -2; }
run "single-groupby-1-1-1: 1 host, 1 metric, max per minute over 1h" \
 "SELECT (timestamp_secs/60)*60 AS minute, MAX(value) FROM metrics WHERE metric_name='cpu.usage_user' AND hostname='host_42' AND timestamp_secs >= $T0 AND timestamp_secs < $T0+3600 GROUP BY minute ORDER BY minute"
run "single-groupby-1-8-1: 8 hosts, 1 metric, max per minute over 1h" \
 "SELECT (timestamp_secs/60)*60 AS minute, hostname, MAX(value) FROM metrics WHERE metric_name='cpu.usage_user' AND hostname IN ('host_1','host_2','host_3','host_4','host_5','host_6','host_7','host_8') AND timestamp_secs >= $T0 AND timestamp_secs < $T0+3600 GROUP BY minute, hostname ORDER BY minute, hostname"
run "single-groupby-5-8-1: 8 hosts, 5 metrics, max per minute over 1h" \
 "SELECT (timestamp_secs/60)*60 AS minute, hostname, metric_name, MAX(value) FROM metrics WHERE metric_name IN ('cpu.usage_user','cpu.usage_system','cpu.usage_idle','cpu.usage_nice','cpu.usage_iowait') AND hostname IN ('host_1','host_2','host_3','host_4','host_5','host_6','host_7','host_8') AND timestamp_secs >= $T0 AND timestamp_secs < $T0+3600 GROUP BY minute, hostname, metric_name ORDER BY minute, hostname, metric_name"
run "double-groupby-1: all hosts, 1 metric, mean per hour per host over 6h" \
 "SELECT (timestamp_secs/3600)*3600 AS hour, hostname, AVG(value) FROM metrics WHERE metric_name='cpu.usage_user' AND timestamp_secs >= $T0 AND timestamp_secs < $T0+21600 GROUP BY hour, hostname ORDER BY hour, hostname"
run "double-groupby-all: all hosts, 10 metrics, mean per hour per host over 6h" \
 "SELECT (timestamp_secs/3600)*3600 AS hour, hostname, metric_name, AVG(value) FROM metrics WHERE timestamp_secs >= $T0 AND timestamp_secs < $T0+21600 GROUP BY hour, hostname, metric_name ORDER BY hour, hostname, metric_name"
run "high-cpu-all: rows with usage_user > 90 over 6h, all hosts" \
 "SELECT hostname, timestamp_secs, value FROM metrics WHERE metric_name='cpu.usage_user' AND value > 90 AND timestamp_secs >= $T0 AND timestamp_secs < $T0+21600 ORDER BY timestamp_secs LIMIT 100000"
run "lastpoint: last usage_user per host" \
 "SELECT hostname, MAX(timestamp_secs) AS ts FROM metrics WHERE metric_name='cpu.usage_user' GROUP BY hostname ORDER BY hostname"
run "groupby-orderby-limit: top 5 minutes by max usage_user before t0+5h" \
 "SELECT (timestamp_secs/60)*60 AS minute, MAX(value) AS m FROM metrics WHERE metric_name='cpu.usage_user' AND timestamp_secs < $T0+18000 GROUP BY minute ORDER BY minute DESC LIMIT 5"
run "cpu-max-all-1: 1 host, 10 metrics, max per hour over 6h" \
 "SELECT (timestamp_secs/3600)*3600 AS hour, metric_name, MAX(value) FROM metrics WHERE hostname='host_7' AND timestamp_secs >= $T0 AND timestamp_secs < $T0+21600 GROUP BY hour, metric_name ORDER BY hour, metric_name"
