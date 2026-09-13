//! TSBS "devops / cpu-only" shaped load, sent as OTLP metrics over gRPC.
//!
//! `--hosts` hosts, each reporting the 10 TSBS cpu fields every `--interval-secs` (simulated
//! time), with the 10 TSBS host tags as attributes. Values follow a clamped random walk like
//! TSBS. Simulated time starts at `--start` (unix secs) and advances by `interval` per round;
//! rounds are sent as fast as the server accepts them (`--concurrency` in flight), or throttled
//! to real time with `--realtime`.
//!
//!   tsbs_otlp --endpoint http://127.0.0.1:7281 --hosts 1000 --rounds 8640 --concurrency 8

use std::time::{Duration, Instant};

use clap::Parser;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    metric, number_data_point, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const CPU_FIELDS: [&str; 10] = [
    "usage_user", "usage_system", "usage_idle", "usage_nice", "usage_iowait",
    "usage_irq", "usage_softirq", "usage_steal", "usage_guest", "usage_guest_nice",
];
const REGIONS: [&str; 9] = [
    "us-east-1", "us-west-1", "us-west-2", "eu-central-1", "eu-west-1",
    "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "sa-east-1",
];
const OSES: [&str; 3] = ["Ubuntu16.10", "Ubuntu16.04LTS", "Ubuntu15.10"];
const ARCHES: [&str; 2] = ["x64", "x86"];
const TEAMS: [&str; 4] = ["SF", "NYC", "LON", "CHI"];
const SERVICES: usize = 20;
const ENVS: [&str; 4] = ["production", "staging", "test", "dev"];

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:7281")]
    endpoint: String,
    #[arg(long, default_value_t = 100)]
    hosts: usize,
    /// Number of 10 s rounds (8640 = one day).
    #[arg(long, default_value_t = 360)]
    rounds: u64,
    #[arg(long, default_value_t = 10)]
    interval_secs: u64,
    #[arg(long, default_value_t = 1_700_000_000)]
    start: u64,
    /// Hosts per export request.
    #[arg(long, default_value_t = 100)]
    hosts_per_request: usize,
    #[arg(long, default_value_t = 8)]
    concurrency: usize,
    /// Throttle to real time (one round per `interval_secs`).
    #[arg(long, default_value_t = false)]
    realtime: bool,
}

struct Host {
    attrs: Vec<KeyValue>,
    values: [f64; 10],
}

fn kv(k: &str, v: String) -> KeyValue {
    KeyValue {
        key: k.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(v)),
        }),
    }
}

fn make_hosts(n: usize, rng: &mut StdRng) -> Vec<Host> {
    (0..n)
        .map(|i| {
            let region = REGIONS[i % REGIONS.len()];
            let attrs = vec![
                kv("hostname", format!("host_{i}")),
                kv("region", region.to_string()),
                kv("datacenter", format!("{region}{}", (i / REGIONS.len()) % 3 + 1)),
                kv("rack", format!("{}", i % 100)),
                kv("os", OSES[i % OSES.len()].to_string()),
                kv("arch", ARCHES[i % ARCHES.len()].to_string()),
                kv("team", TEAMS[i % TEAMS.len()].to_string()),
                kv("service", format!("{}", i % SERVICES)),
                kv("service_version", format!("{}", i % 2)),
                kv("service_environment", ENVS[i % ENVS.len()].to_string()),
            ];
            let mut values = [0f64; 10];
            for v in values.iter_mut() {
                *v = rng.gen_range(0.0..100.0f64).round();
            }
            Host { attrs, values }
        })
        .collect()
}

fn step(host: &mut Host, rng: &mut StdRng) {
    for v in host.values.iter_mut() {
        // TSBS cpu fields are integers in 0..=100 (random walk).
        *v = (*v + rng.gen_range(-1.0..=1.0)).round().clamp(0.0, 100.0);
    }
}

fn build_request(hosts: &[Host], ts_nanos: u64) -> ExportMetricsServiceRequest {
    let resource_metrics = hosts
        .iter()
        .map(|host| ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "tsbs".to_string())],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: CPU_FIELDS
                    .iter()
                    .enumerate()
                    .map(|(f, field)| Metric {
                        name: format!("cpu.{field}"),
                        description: String::new(),
                        unit: "percent".to_string(),
                        metadata: vec![],
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: host.attrs.clone(),
                                start_time_unix_nano: 0,
                                time_unix_nano: ts_nanos,
                                exemplars: vec![],
                                flags: 0,
                                value: Some(number_data_point::Value::AsDouble(host.values[f])),
                            }],
                        })),
                    })
                    .collect(),
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        })
        .collect();
    ExportMetricsServiceRequest { resource_metrics }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut rng = StdRng::seed_from_u64(1);
    let mut hosts = make_hosts(args.hosts, &mut rng);
    let points_per_round = (args.hosts * CPU_FIELDS.len()) as u64;
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(args.concurrency));
    let channel = tonic::transport::Channel::from_shared(args.endpoint.clone())
        .unwrap()
        .connect()
        .await
        .expect("connect");
    let client = MetricsServiceClient::new(channel)
        .max_encoding_message_size(64 << 20)
        .max_decoding_message_size(64 << 20);

    let start = Instant::now();
    let mut sent_points = 0u64;
    let mut errors = 0u64;
    let mut latencies: Vec<u32> = Vec::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(Result<(), String>, u32)>();
    let mut in_flight = 0usize;
    let mut last_report = Instant::now();

    for round in 0..args.rounds {
        let round_start = Instant::now();
        let ts_nanos = (args.start + round * args.interval_secs) * 1_000_000_000;
        for host in hosts.iter_mut() {
            step(host, &mut rng);
        }
        for chunk in hosts.chunks(args.hosts_per_request) {
            let request = build_request(chunk, ts_nanos);
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let mut client = client.clone();
            let tx = tx.clone();
            in_flight += 1;
            tokio::spawn(async move {
                let t0 = Instant::now();
                let result = client.export(request).await.map(|_| ()).map_err(|e| e.to_string());
                drop(permit);
                let _ = tx.send((result, t0.elapsed().as_millis() as u32));
            });
            while let Ok((result, lat)) = rx.try_recv() {
                in_flight -= 1;
                match result {
                    Ok(()) => {
                        sent_points += (args.hosts_per_request.min(args.hosts) * CPU_FIELDS.len()) as u64;
                        latencies.push(lat);
                    }
                    Err(e) => {
                        errors += 1;
                        if errors <= 5 {
                            eprintln!("export error: {e}");
                        }
                    }
                }
            }
        }
        if args.realtime {
            let elapsed = round_start.elapsed();
            let period = Duration::from_secs(args.interval_secs);
            if elapsed < period {
                tokio::time::sleep(period - elapsed).await;
            }
        }
        if last_report.elapsed() >= Duration::from_secs(10) {
            let el = start.elapsed().as_secs_f64();
            eprintln!(
                "round {round}/{}: {sent_points} points sent, {:.0} points/s, errors {errors}, in flight {in_flight}",
                args.rounds,
                sent_points as f64 / el
            );
            last_report = Instant::now();
        }
    }
    drop(tx);
    while let Some((result, lat)) = rx.recv().await {
        match result {
            Ok(()) => {
                sent_points += points_per_round.min((args.hosts_per_request * CPU_FIELDS.len()) as u64);
                latencies.push(lat);
            }
            Err(_) => errors += 1,
        }
    }
    let el = start.elapsed().as_secs_f64();
    latencies.sort_unstable();
    let pct = |p: f64| latencies.get(((latencies.len() as f64 * p) as usize).min(latencies.len().saturating_sub(1))).copied().unwrap_or(0);
    println!(
        "hosts={} rounds={} points={} elapsed={:.1}s points/s={:.0} requests={} errors={} export latency ms p50={} p99={}",
        args.hosts, args.rounds, sent_points, el, sent_points as f64 / el, latencies.len(), errors, pct(0.5), pct(0.99)
    );
}
