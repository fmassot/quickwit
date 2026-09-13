//! Load generator for the ingest v3 benchmark: pre-generated NDJSON bodies, `--clients`
//! concurrent closed-loop connections, `--duration` seconds, client-side ack latency.
//!
//!   ingest_bench --url http://127.0.0.1:7280 --index bench --clients 64 --batch-docs 500 --duration 60

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:7280")]
    url: String,
    #[arg(long, default_value = "bench")]
    index: String,
    #[arg(long, default_value_t = 64)]
    clients: usize,
    #[arg(long, default_value_t = 500)]
    batch_docs: usize,
    #[arg(long, default_value_t = 60.0)]
    duration: f64,
    /// Number of distinct pre-generated bodies (rotated per request).
    #[arg(long, default_value_t = 256)]
    bodies: usize,
    #[arg(long, default_value = "")]
    label: String,
}

fn make_body(rng: &mut StdRng, batch_docs: usize, base_seq: u64) -> Vec<u8> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    let mut out = Vec::with_capacity(batch_docs * 200);
    let levels = ["INFO", "WARN", "ERROR", "DEBUG"];
    let services = ["api", "auth", "billing", "search", "ingest"];
    for i in 0..batch_docs {
        let msg_len = rng.gen_range(60..160);
        let msg: String = (0..msg_len)
            .map(|_| {
                let c = rng.gen_range(0..27u8);
                if c == 26 { ' ' } else { (b'a' + c) as char }
            })
            .collect();
        out.extend_from_slice(
            format!(
                "{{\"ts\": {}, \"seq\": {}, \"level\": \"{}\", \"service\": \"{}\", \"msg\": \"{}\", \"latency_ms\": {}}}\n",
                now,
                base_seq + i as u64,
                levels[rng.gen_range(0..levels.len())],
                services[rng.gen_range(0..services.len())],
                msg,
                rng.gen_range(1..5000)
            )
            .as_bytes(),
        );
    }
    out
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut rng = StdRng::seed_from_u64(42);
    let bodies: Vec<Arc<Vec<u8>>> = (0..args.bodies)
        .map(|i| Arc::new(make_body(&mut rng, args.batch_docs, (i * args.batch_docs) as u64)))
        .collect();
    let body_bytes: usize = bodies.iter().map(|b| b.len()).sum::<usize>() / bodies.len();
    let url = format!("{}/api/v1/{}/ingest", args.url, args.index);
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(args.clients)
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs_f64(args.duration);
    let requests = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let latencies: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::with_capacity(1 << 20)));
    let start = Instant::now();
    let mut handles = Vec::new();
    for c in 0..args.clients {
        let client = client.clone();
        let url = url.clone();
        let bodies = bodies.clone();
        let (requests, errors, latencies) = (requests.clone(), errors.clone(), latencies.clone());
        handles.push(tokio::spawn(async move {
            let mut i = c;
            let mut local = Vec::new();
            while Instant::now() < deadline {
                let body = bodies[i % bodies.len()].clone();
                i += 1;
                let t0 = Instant::now();
                let result = client
                    .post(&url)
                    .header("content-type", "application/json")
                    .body(reqwest::Body::from((*body).clone()))
                    .send()
                    .await;
                match result {
                    Ok(resp) if resp.status().is_success() => {
                        let _ = resp.bytes().await;
                        local.push(t0.elapsed().as_micros() as u32);
                        requests.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(resp) => {
                        let _ = resp.bytes().await;
                        errors.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
            latencies.lock().unwrap().extend(local);
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let n = requests.load(Ordering::Relaxed);
    let e = errors.load(Ordering::Relaxed);
    let mut lat = latencies.lock().unwrap().clone();
    lat.sort_unstable();
    let pct = |p: f64| lat.get(((lat.len() as f64 * p) as usize).min(lat.len().saturating_sub(1))).copied().unwrap_or(0) as f64 / 1000.0;
    let docs = n as f64 * args.batch_docs as f64;
    println!(
        "=== {} clients={} batch={} bodies={} ({} KB each)",
        args.label, args.clients, args.batch_docs, args.bodies, body_bytes / 1024
    );
    println!(
        "requests: {}  errors: {}  docs/s: {:.0}  MB/s(json): {:.1}",
        n, e, docs / elapsed, docs * body_bytes as f64 / args.batch_docs as f64 / elapsed / 1e6
    );
    if !lat.is_empty() {
        println!(
            "ack latency ms: p50={:.0} p90={:.0} p99={:.0} max={:.0}",
            pct(0.5), pct(0.9), pct(0.99), *lat.last().unwrap() as f64 / 1000.0
        );
    }
}
