//! Where the CPU of one RLOG L0 -> L1 compaction goes (issue #3819).
//!
//! Builds one sealed logs bucket (one shard, one ingest hour) of synthetic L0
//! objects shaped like the logs benchmark on a `MemoryStore`, runs
//! `ravel_maintain::compact_bucket` over it once with the defaults
//! `ravel-cli compact-bucket` resolves, and reports:
//!
//! - the shape actually built, read back from the written objects;
//! - records, L0 and L1 bytes, L1 part count, the merge read mode;
//! - wall and process CPU of the compaction call, records/s per core;
//! - a phase timeline from store-operation timestamps and the compactor's own
//!   tracing events, and the request ledger per phase;
//! - under the `profiling` feature with `RAVEL_BENCH_PROFILE_SVG` set, a
//!   flamegraph of the call, the top leaf (exclusive) and inclusive frames,
//!   and the profile samples attributed to phases by stack markers.
//!
//! `--page-choice-ab` re-encodes every L1 part from its decoded records with
//! the compactor's writer settings, once per `PageChoice`, because
//! `CompactorConfig` does not expose the page choice.
//!
//! Measurement only: it changes no library behaviour. One JSON document goes
//! to stdout, a human table to stderr.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use clap::Parser;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::{
    AttrValue, LogRecord, ObjectIdentity, PageChoice, Predicate, RlogConfig, RlogReader,
    RlogWriter, stream_attrs_bytes,
};
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, MergeMemoryTracker, PhaseRequests,
    RequestLedger, ResolvedL1PartMemoryTarget, compact_bucket, detect_host_memory_total_bytes,
    host_memory_budget_bytes, merge_memory_budget_bytes, validate_rlog_zstd_level,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError,
};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantId};
use serde_json::{Value, json};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use uuid::Uuid;

#[cfg(all(feature = "compaction-jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const TENANT: &str = "rlog-compaction-profile";
const SHARD: u32 = 0;
const HOUR: u32 = 495_000;
const NS_PER_MS: i64 = 1_000_000;
const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Clock padding past the hour end: far beyond the default flush lifetime,
/// skew allowance and fold margin, so the bucket reads as sealed.
const SEAL_PADDING_NS: i64 = 3 * NS_PER_HOUR;
const OBJECT_SPAN_NS: i64 = 700 * NS_PER_MS;

/// The attribute fields every record may carry, in name order.
const FIELDS: [&str; 57] = [
    "app_version",
    "autoscaler",
    "backup_policy",
    "build_id",
    "bytes_sent",
    "cache_backend",
    "cache_status",
    "component",
    "compression",
    "content_type",
    "cost_center",
    "cpu_request",
    "criticality",
    "db_pool",
    "duration_ms",
    "env",
    "feature_set",
    "image_digest",
    "ingress_class",
    "instance_type",
    "ip",
    "kafka_topic",
    "managed_by",
    "memory_request",
    "method",
    "namespace",
    "on_call",
    "os_image",
    "part_of",
    "path",
    "priority_class",
    "protocol",
    "qos_class",
    "queue_backend",
    "referer",
    "release_channel",
    "replicas",
    "request_id",
    "retries",
    "runtime",
    "sampled",
    "sdk_version",
    "security_profile",
    "service_account",
    "service_mesh",
    "sla_tier",
    "span_id",
    "status",
    "storage_class",
    "team",
    "tenant_mode",
    "tier",
    "tls_version",
    "user_agent",
    "user_id",
    "vcs_branch",
    "zone",
];

const BODY_WORDS: [&str; 48] = [
    "request",
    "completed",
    "failed",
    "retrying",
    "connection",
    "upstream",
    "timeout",
    "handler",
    "cache",
    "miss",
    "hit",
    "user",
    "session",
    "token",
    "refresh",
    "query",
    "rows",
    "returned",
    "scheduled",
    "job",
    "worker",
    "queue",
    "message",
    "published",
    "consumed",
    "offset",
    "partition",
    "lease",
    "acquired",
    "released",
    "pool",
    "exhausted",
    "healthcheck",
    "ok",
    "degraded",
    "backend",
    "selected",
    "route",
    "matched",
    "payload",
    "validated",
    "rejected",
    "quota",
    "exceeded",
    "config",
    "reloaded",
    "shard",
    "flushed",
];

const METHODS: [&str; 6] = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"];
const STATUSES: [i64; 12] = [200, 200, 200, 200, 201, 204, 301, 304, 400, 404, 500, 503];

#[derive(Parser, Debug)]
#[command(about = "Profile one RLOG L0 -> L1 compaction (issue #3819)")]
struct Args {
    /// L0 objects in the bucket.
    #[arg(long, default_value_t = 230)]
    inputs: usize,
    /// Distinct streams in the bucket.
    #[arg(long, default_value_t = 2_477)]
    streams: usize,
    /// Fewest streams one object carries.
    #[arg(long, default_value_t = 2_200)]
    min_streams_per_object: usize,
    /// Most streams one object carries (capped at --streams).
    #[arg(long, default_value_t = 2_480)]
    max_streams_per_object: usize,
    /// Median records per object (log-normal around it, clamped).
    #[arg(long, default_value_t = 10_900)]
    median_records: usize,
    #[arg(long, default_value_t = 5_000)]
    min_records: usize,
    #[arg(long, default_value_t = 29_000)]
    max_records: usize,
    /// Log-normal sigma of records per object.
    #[arg(long, default_value_t = 0.35)]
    records_sigma: f64,
    /// RNG seed; the same seed builds the same bucket.
    #[arg(long, default_value_t = 3_819)]
    seed: u64,
    /// RLOG zstd level for the L1 parts; unset keeps the compactor default.
    #[arg(long)]
    zstd_level: Option<i32>,
    /// After the compaction, re-encode every L1 part with PageChoice
    /// Exhaustive and Estimated and report both.
    #[arg(long)]
    page_choice_ab: bool,
}

// ---------------------------------------------------------------------------
// Synthetic bucket.

enum FieldKind {
    Vocab(Vec<String>),
    RequestId,
    SpanId,
    UserId,
    Ip,
    ImageDigest,
    BuildId,
    BytesSent,
    DurationMs,
    Replicas,
    Retries,
    Status,
}

struct FieldSpec {
    name: &'static str,
    null_p: f64,
    kind: FieldKind,
}

fn token(rng: &mut StdRng, len: usize) -> String {
    const ALPHA: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    (0..len)
        .map(|_| ALPHA[rng.random_range(0..ALPHA.len())] as char)
        .collect()
}

fn hex(rng: &mut StdRng, len: usize) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    (0..len)
        .map(|_| HEX[rng.random_range(0..HEX.len())] as char)
        .collect()
}

fn normal(rng: &mut StdRng) -> f64 {
    let u1 = 1.0 - rng.random::<f64>();
    let u2 = rng.random::<f64>();
    (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
}

fn field_specs(seed: u64) -> Vec<FieldSpec> {
    let mut rng = StdRng::seed_from_u64(seed ^ 0xF1E1_D5F1_E1D5);
    FIELDS
        .iter()
        .map(|&name| {
            let null_p = rng.random_range(0.40..=0.70);
            let kind = match name {
                "request_id" => FieldKind::RequestId,
                "span_id" => FieldKind::SpanId,
                "user_id" => FieldKind::UserId,
                "ip" => FieldKind::Ip,
                "image_digest" => FieldKind::ImageDigest,
                "build_id" => FieldKind::BuildId,
                "bytes_sent" => FieldKind::BytesSent,
                "duration_ms" => FieldKind::DurationMs,
                "replicas" => FieldKind::Replicas,
                "retries" => FieldKind::Retries,
                "status" => FieldKind::Status,
                _ => {
                    // Vocabulary size log-uniform over [10, 1000].
                    let size = (10f64 * 100f64.powf(rng.random::<f64>())).round() as usize;
                    let len = rng.random_range(3..=24);
                    let vocab = (0..size)
                        .map(|i| format!("{name}-{}-{i}", token(&mut rng, len)))
                        .collect();
                    FieldKind::Vocab(vocab)
                }
            };
            FieldSpec { name, null_p, kind }
        })
        .collect()
}

fn field_value(kind: &FieldKind, rng: &mut StdRng) -> AttrValue {
    match kind {
        FieldKind::Vocab(vocab) => {
            // Skewed toward the head of the vocabulary.
            let u = rng.random::<f64>();
            let i = ((u * u) * vocab.len() as f64) as usize;
            AttrValue::Str(vocab[i.min(vocab.len() - 1)].clone())
        }
        FieldKind::RequestId => AttrValue::Str(hex(rng, 32)),
        FieldKind::SpanId => AttrValue::Str(hex(rng, 16)),
        FieldKind::UserId => AttrValue::Str(format!("user-{}", rng.random_range(0..2_000_000))),
        FieldKind::Ip => AttrValue::Str(format!(
            "10.{}.{}.{}",
            rng.random_range(0..256),
            rng.random_range(0..256),
            rng.random_range(1..255)
        )),
        FieldKind::ImageDigest => AttrValue::Str(format!("sha256:{}", hex(rng, 64))),
        FieldKind::BuildId => AttrValue::Str(hex(rng, 12)),
        FieldKind::BytesSent => AttrValue::I64((7.6 + 1.5 * normal(rng)).exp() as i64),
        FieldKind::DurationMs => AttrValue::I64((3.7 + 1.2 * normal(rng)).exp() as i64),
        FieldKind::Replicas => AttrValue::I64(rng.random_range(1..=12)),
        FieldKind::Retries => {
            let u = rng.random::<f64>();
            AttrValue::I64(if u < 0.85 { 0 } else { rng.random_range(1..=5) })
        }
        FieldKind::Status => AttrValue::I64(STATUSES[rng.random_range(0..STATUSES.len())]),
    }
}

fn body(rng: &mut StdRng) -> String {
    let target = rng.random_range(60..=200);
    let mut out = String::with_capacity(target + 40);
    let method = METHODS[rng.random_range(0..METHODS.len())];
    let _ = write!(
        out,
        "{method} /api/v{}/{}/{} ",
        rng.random_range(1..=3),
        BODY_WORDS[rng.random_range(0..BODY_WORDS.len())],
        rng.random_range(0..10_000)
    );
    while out.len() < target {
        match rng.random_range(0..4) {
            0 => {
                let _ = write!(
                    out,
                    "{}={} ",
                    BODY_WORDS[rng.random_range(0..BODY_WORDS.len())],
                    rng.random_range(0..100_000)
                );
            }
            1 => {
                let _ = write!(out, "id={} ", hex(rng, 8));
            }
            _ => {
                out.push_str(BODY_WORDS[rng.random_range(0..BODY_WORDS.len())]);
                out.push(' ');
            }
        }
    }
    out.truncate(target);
    out
}

struct Stream {
    id: ravel_types::logstream::LogStreamId,
    blob: Vec<u8>,
}

fn streams(count: usize) -> Vec<Stream> {
    (0..count)
        .map(|s| {
            let app = format!("app-{:02}", s / 31);
            let host = format!("node-{:03}", (s * 7_919) % 180);
            let pod = format!("{app}-{:05x}-{:05x}", (s * 104_729) % 0xF_FFFF, s);
            let res = vec![
                ("app".to_string(), AttrValue::Str(app)),
                ("host".to_string(), AttrValue::Str(host)),
                ("pod".to_string(), AttrValue::Str(pod)),
            ];
            Stream {
                id: log_stream_id(&res, "", "", &[]),
                blob: stream_attrs_bytes(&res, "", "", &[]),
            }
        })
        .collect()
}

struct L0Object {
    seq: u64,
    bytes: Bytes,
    records: u64,
    streams: u64,
    min_ts: i64,
    max_ts: i64,
}

fn object_records(
    args: &Args,
    index: usize,
    specs: &[FieldSpec],
    all: &[Stream],
) -> Vec<LogRecord> {
    let mut rng =
        StdRng::seed_from_u64(args.seed ^ (index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let median = args.median_records as f64;
    let n = ((median.ln() + args.records_sigma * normal(&mut rng)).exp() as usize)
        .clamp(args.min_records, args.max_records);
    let max_k = args.max_streams_per_object.min(all.len());
    let min_k = args.min_streams_per_object.min(max_k);
    let k = rng.random_range(min_k..=max_k).min(n);
    let mut order: Vec<usize> = (0..all.len()).collect();
    for i in 0..k {
        let j = rng.random_range(i..order.len());
        order.swap(i, j);
    }
    let chosen = &order[..k];
    let mut per_stream = vec![1usize; k];
    for _ in k..n {
        per_stream[rng.random_range(0..k)] += 1;
    }
    let window_start = i64::from(HOUR) * NS_PER_HOUR + index as i64 * OBJECT_SPAN_NS;
    let mut records = Vec::with_capacity(n);
    for (slot, &s) in chosen.iter().enumerate() {
        let stream = &all[s];
        for _ in 0..per_stream[slot] {
            let ts = window_start + rng.random_range(0..OBJECT_SPAN_NS);
            let sev = rng.random_range(0..100);
            let (severity_num, severity_text) = match sev {
                0..=14 => (5u8, "DEBUG"),
                15..=84 => (9, "INFO"),
                85..=95 => (13, "WARN"),
                _ => (17, "ERROR"),
            };
            let mut attrs = Vec::new();
            for spec in specs {
                if rng.random::<f64>() >= spec.null_p {
                    attrs.push((spec.name.to_string(), field_value(&spec.kind, &mut rng)));
                }
            }
            records.push(LogRecord {
                stream_id: stream.id,
                stream_attrs: stream.blob.clone(),
                ts_ns: ts,
                observed_ts_ns: ts + rng.random_range(0..50 * NS_PER_MS),
                severity_num,
                severity_text: severity_text.to_string(),
                body: body(&mut rng),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs,
            });
        }
    }
    records.sort_by(|a, b| (a.stream_id, a.ts_ns).cmp(&(b.stream_id, b.ts_ns)));
    records
}

fn build_objects(args: &Args, writer_id: Uuid, epoch: u64) -> Vec<L0Object> {
    let tenant_hash = TenantId::new(TENANT).hash();
    let specs = field_specs(args.seed);
    let all = streams(args.streams);
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(args.inputs.max(1));
    let mut out: Vec<Option<L0Object>> = (0..args.inputs).map(|_| None).collect();
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let specs = &specs;
                let all = &all;
                scope.spawn(move || {
                    let mut built = Vec::new();
                    for index in (t..args.inputs).step_by(threads) {
                        let records = object_records(args, index, specs, all);
                        let seq = index as u64 + 1;
                        let identity = ObjectIdentity {
                            tenant_hash: tenant_hash.0,
                            shard: SHARD,
                            writer_id: writer_id.into_bytes(),
                            writer_epoch: epoch,
                            writer_seq: seq,
                        };
                        let streams: HashSet<_> = records.iter().map(|r| r.stream_id).collect();
                        let min_ts = records.iter().map(|r| r.ts_ns).min().unwrap_or(0);
                        let max_ts = records.iter().map(|r| r.ts_ns).max().unwrap_or(0);
                        let count = records.len() as u64;
                        let mut w = RlogWriter::new(RlogConfig::default(), identity);
                        for r in records {
                            w.push(r).expect("push L0 record");
                        }
                        let bytes = Bytes::from(w.finish().expect("finish L0 object"));
                        built.push((
                            index,
                            L0Object {
                                seq,
                                bytes,
                                records: count,
                                streams: streams.len() as u64,
                                min_ts,
                                max_ts,
                            },
                        ));
                    }
                    built
                })
            })
            .collect();
        for h in handles {
            for (index, obj) in h.join().expect("generator thread") {
                out[index] = Some(obj);
            }
        }
    });
    out.into_iter()
        .map(|o| o.expect("every object built"))
        .collect()
}

async fn seed_objects(
    store: &dyn ObjectStoreBackend,
    objects: &[L0Object],
    writer_id: Uuid,
    epoch: u64,
) {
    let th = TenantId::new(TENANT).hash();
    for obj in objects {
        let content_hash: [u8; 32] = *blake3::hash(&obj.bytes).as_bytes();
        let data_key = keys::data_key(
            &th,
            Signal::Logs,
            SHARD,
            writer_id,
            epoch,
            obj.seq,
            &content_hash,
        )
        .expect("data key");
        store
            .put(&data_key, obj.bytes.clone(), PutOptions::default())
            .await
            .expect("put L0 object");
        let created = i64::from(HOUR) * NS_PER_HOUR + obj.seq as i64 * OBJECT_SPAN_NS;
        let rec = record::build(NewCommitRecord {
            tenant_hash: th,
            signal: Signal::Logs,
            shard: SHARD,
            writer_id,
            writer_epoch: epoch,
            writer_seq: obj.seq,
            object_size: obj.bytes.len() as u64,
            content_hash,
            sample_count: obj.records,
            series_count: obj.streams,
            min_event_ts_ns: obj.min_ts,
            max_event_ts_ns: obj.max_ts,
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            created_unix_ns: created,
            ingest_hour_bucket: HOUR,
        })
        .expect("commit record");
        let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
        store
            .put(&commit_key, record::encode(&rec), PutOptions::default())
            .await
            .expect("put commit record");
    }
}

// ---------------------------------------------------------------------------
// Shape read back from the written objects.

fn spread(values: &mut [u64]) -> Value {
    values.sort_unstable();
    if values.is_empty() {
        return json!(null);
    }
    let sum: u64 = values.iter().sum();
    json!({
        "min": values[0],
        "median": values[values.len() / 2],
        "max": values[values.len() - 1],
        "mean": sum as f64 / values.len() as f64,
        "sum": sum,
    })
}

fn verify_shape(objects: &[L0Object]) -> Value {
    let cfg = RlogConfig::default();
    let mut records = Vec::new();
    let mut streams = Vec::new();
    let mut blocks = Vec::new();
    let mut row_groups = Vec::new();
    let mut bytes = Vec::new();
    let mut span_ms = Vec::new();
    let mut per_stream = Vec::new();
    let mut union = BTreeSet::new();
    let mut contiguous = true;
    let mut prev_max: Option<i64> = None;
    for obj in objects {
        let dirs = RlogReader::<[u8]>::decode_directories(&obj.bytes[..], &cfg)
            .expect("decode directories");
        let reader = RlogReader::new(&obj.bytes[..], &cfg).expect("open L0");
        let l0 = &dirs.skip_index().l0;
        let recs: u64 = l0.iter().map(|e| u64::from(e.record_count)).sum();
        let min_ts = l0.iter().map(|e| e.min_ts).min().unwrap_or(0);
        let max_ts = l0.iter().map(|e| e.max_ts).max().unwrap_or(0);
        if let Some(p) = prev_max
            && min_ts < p
        {
            contiguous = false;
        }
        prev_max = Some(max_ts);
        let sd = dirs.stream_dir();
        for e in sd.entries() {
            union.insert(e.stream_id);
        }
        records.push(recs);
        streams.push(sd.len() as u64);
        blocks.push(l0.len() as u64);
        row_groups.push(reader.row_group_count() as u64);
        bytes.push(obj.bytes.len() as u64);
        span_ms.push(((max_ts - min_ts) / NS_PER_MS) as u64);
        per_stream.push((recs as f64 / sd.len().max(1) as f64 * 100.0).round() as u64);
    }

    // Field and body figures from the first object's decoded records.
    let sample = objects.first().map(|obj| {
        let reader = RlogReader::new(&obj.bytes[..], &cfg).expect("open L0");
        let (recs, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan L0");
        let mut present: BTreeMap<String, (u64, HashSet<String>, &'static str)> = BTreeMap::new();
        let mut body_len = Vec::new();
        for r in &recs {
            body_len.push(r.body.len() as u64);
            for (k, v) in &r.attrs {
                let (kind, text) = match v {
                    AttrValue::Str(s) => ("str", s.clone()),
                    AttrValue::I64(i) => ("i64", i.to_string()),
                    other => ("other", format!("{other:?}")),
                };
                let e = present.entry(k.clone()).or_insert((0, HashSet::new(), kind));
                e.0 += 1;
                e.1.insert(text);
            }
        }
        let n = recs.len().max(1) as f64;
        let mut null_share: Vec<u64> = present
            .values()
            .map(|(c, _, _)| ((1.0 - *c as f64 / n) * 1000.0).round() as u64)
            .collect();
        let fields: Vec<Value> = present
            .iter()
            .map(|(k, (c, d, kind))| {
                json!({"field": k, "type": kind, "null_share": 1.0 - *c as f64 / n, "distinct": d.len()})
            })
            .collect();
        let str_fields = present.values().filter(|(_, _, k)| *k == "str").count();
        let int_fields = present.values().filter(|(_, _, k)| *k == "i64").count();
        json!({
            "object_index": 0,
            "records": recs.len(),
            "fields_present": present.len(),
            "string_fields": str_fields,
            "i64_fields": int_fields,
            "null_share_permille": spread(&mut null_share),
            "body_bytes": spread(&mut body_len),
            "fields": fields,
        })
    });

    json!({
        "objects": objects.len(),
        "distinct_streams_in_bucket": union.len(),
        "records_per_object": spread(&mut records),
        "streams_per_object": spread(&mut streams),
        "records_per_stream_per_object_x100": spread(&mut per_stream),
        "blocks_per_object": spread(&mut blocks),
        "row_groups_per_object": spread(&mut row_groups),
        "bytes_per_object": spread(&mut bytes),
        "ts_span_ms_per_object": spread(&mut span_ms),
        "objects_consecutive_in_time": contiguous,
        "first_object_fields": sample,
    })
}

// ---------------------------------------------------------------------------
// Instrumentation: store-operation timing and captured tracing events.

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum OpKind {
    Get,
    Put,
    List,
    Head,
    Delete,
}

struct Op {
    kind: OpKind,
    key: String,
    start: Instant,
    end: Instant,
    bytes: u64,
}

struct TimingStore {
    inner: MemoryStore,
    armed: AtomicBool,
    ops: Mutex<Vec<Op>>,
}

impl TimingStore {
    fn new() -> Self {
        TimingStore {
            inner: MemoryStore::new(),
            armed: AtomicBool::new(false),
            ops: Mutex::new(Vec::new()),
        }
    }

    fn note(&self, kind: OpKind, key: &str, start: Instant, bytes: u64) {
        if self.armed.load(Ordering::Relaxed) {
            let end = Instant::now();
            self.ops.lock().expect("ops lock").push(Op {
                kind,
                key: key.to_string(),
                start,
                end,
                bytes,
            });
        }
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for TimingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        let start = Instant::now();
        let len = data.len() as u64;
        let out = self.inner.put(key, data, opts).await;
        self.note(OpKind::Put, key, start, len);
        out
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        let start = Instant::now();
        let out = self.inner.get(key, range).await;
        let len = out.as_ref().map(|o| o.data.len() as u64).unwrap_or(0);
        self.note(OpKind::Get, key, start, len);
        out
    }
    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        let start = Instant::now();
        let out = self.inner.get_pinned(key, range, pin).await;
        let len = out
            .as_ref()
            .map(|o| o.outcome.data.len() as u64)
            .unwrap_or(0);
        self.note(OpKind::Get, key, start, len);
        out
    }
    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        let start = Instant::now();
        let out = self.inner.get_with_pin(key, range).await;
        let len = out
            .as_ref()
            .map(|o| o.outcome.data.len() as u64)
            .unwrap_or(0);
        self.note(OpKind::Get, key, start, len);
        out
    }
    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        let start = Instant::now();
        let out = self.inner.pin_of(key).await;
        self.note(OpKind::Head, key, start, 0);
        out
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        let start = Instant::now();
        let out = self.inner.head(key).await;
        self.note(OpKind::Head, key, start, 0);
        out
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        let start = Instant::now();
        let out = self.inner.list(prefix, page).await;
        self.note(OpKind::List, prefix, start, 0);
        out
    }
    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        let start = Instant::now();
        let out = self.inner.list_after(prefix, start_after, page).await;
        self.note(OpKind::List, prefix, start, 0);
        out
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        let start = Instant::now();
        let out = self.inner.list_delimited(prefix).await;
        self.note(OpKind::List, prefix, start, 0);
        out
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        let start = Instant::now();
        let out = self.inner.delete(key).await;
        self.note(OpKind::Delete, key, start, 0);
        out
    }
    fn capabilities(&self) -> Capabilities {
        // multipart: false to match the refusing default `put_multipart` this
        // wrapper inherits, so every part PUT goes through `put` and is timed.
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
    fn observed_store_time_ns(&self) -> Option<i64> {
        self.inner.observed_store_time_ns()
    }
}

struct CapturedEvent {
    at: Instant,
    level: tracing::Level,
    message: String,
    fields: BTreeMap<String, String>,
}

#[derive(Default)]
struct FieldVisitor {
    message: String,
    fields: BTreeMap<String, String>,
}

impl tracing::field::Visit for FieldVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let text = format!("{value:?}");
        if field.name() == "message" {
            self.message = text;
        } else {
            self.fields.insert(field.name().to_string(), text);
        }
    }
}

#[derive(Clone)]
struct EventCapture {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for EventCapture {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let level = *event.metadata().level();
        if level <= tracing::Level::WARN {
            eprintln!("[{level}] {} {:?}", visitor.message, visitor.fields);
        }
        self.events
            .lock()
            .expect("events lock")
            .push(CapturedEvent {
                at: Instant::now(),
                level,
                message: visitor.message,
                fields: visitor.fields,
            });
    }
}

// ---------------------------------------------------------------------------
// CPU clocks.

fn process_cpu_s() -> f64 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    t.tv_sec as f64 + t.tv_nsec as f64 * 1e-9
}

fn thread_cpu_s() -> f64 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    t.tv_sec as f64 + t.tv_nsec as f64 * 1e-9
}

/// `(utime, stime)` in clock ticks from a `/proc/.../stat` line.
fn stat_ticks(text: &str) -> Option<(String, u64, u64)> {
    let open = text.find('(')?;
    let close = text.rfind(')')?;
    let comm = text.get(open + 1..close)?.to_string();
    let rest: Vec<&str> = text.get(close + 2..)?.split_whitespace().collect();
    // Fields after the comm start at field 3 (state); utime is 14, stime 15.
    let utime = rest.get(11)?.parse().ok()?;
    let stime = rest.get(12)?.parse().ok()?;
    Some((comm, utime, stime))
}

fn self_ticks() -> (u64, u64) {
    std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|t| stat_ticks(&t))
        .map(|(_, u, s)| (u, s))
        .unwrap_or((0, 0))
}

fn thread_ticks() -> HashMap<String, (String, u64)> {
    let mut out = HashMap::new();
    if let Ok(dir) = std::fs::read_dir("/proc/self/task") {
        for entry in dir.flatten() {
            let tid = entry.file_name().to_string_lossy().to_string();
            if let Ok(text) = std::fs::read_to_string(entry.path().join("stat"))
                && let Some((comm, u, s)) = stat_ticks(&text)
            {
                out.insert(tid, (comm, u + s));
            }
        }
    }
    out
}

/// Linux USER_HZ, the unit of `/proc/*/stat` utime and stime.
const TICKS_PER_S: f64 = 100.0;

// ---------------------------------------------------------------------------
// Profile (profiling feature only).

#[cfg(feature = "profiling")]
mod prof {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::path::PathBuf;

    use serde_json::{Value, json};

    pub const HZ: i32 = 997;

    pub struct Sampler {
        guard: pprof::ProfilerGuard<'static>,
        path: PathBuf,
    }

    pub fn start() -> Option<Sampler> {
        let path = match std::env::var_os(ravel_bench::profiling::PROFILE_ENV) {
            Some(p) if !p.is_empty() => PathBuf::from(p),
            _ => return None,
        };
        let exe = std::env::current_exe().expect("current exe");
        let count = ravel_bench::profiling::count_exe_fp_prologues(&exe).unwrap_or_else(|e| {
            eprintln!("profiling: {e}");
            std::process::exit(1);
        });
        if let Err(e) = ravel_bench::profiling::check_fp_prologue_count(count) {
            eprintln!("profiling: {e}");
            std::process::exit(1);
        }
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(HZ)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .expect("start sampler");
        eprintln!("profiling: sampling at {HZ} Hz into {}", path.display());
        Some(Sampler { guard, path })
    }

    fn clean(name: &str) -> String {
        // Strip the `::h<16 hex>` symbol hash.
        if name.len() > 19 {
            let (head, tail) = name.split_at(name.len() - 19);
            if tail.starts_with("::h") && tail[3..].bytes().all(|b| b.is_ascii_hexdigit()) {
                return head.to_string();
            }
        }
        name.to_string()
    }

    const CATALOG: &[&str] = &[
        "rlog::load_input_catalog",
        "rlog::load_catalog_from_object",
        "rlog::load_catalogs_by_key",
        "rlog::read_bloom_scope",
        "rlog::declared_columns_from_inputs",
        "rlog::fetch_section",
    ];
    const PLAN: &[&str] = &[
        "rlog::plan_merge",
        "rlog::hot_split",
        "rlog::input_cursor_pricing",
        "rlog::block_decode_ceiling_bytes",
    ];
    const ENCODE: &[&str] = &[
        "rlog::PartBuilder",
        "rlog::build_writer",
        "rlog::encode_clone_job",
        "rlog::into_encoded",
        "RlogWriter",
        "ravel_logseg::writer::",
    ];
    const PUT: &[&str] = &["rlog::finalize_part", "put_part_with_ledger"];
    const DECODE: &[&str] = &[
        "rlog::decode_next_block",
        "ravel_logseg::ranged::",
        "ravel_logseg::reader::",
        "ravel_logseg::columnar_rows::",
        "ravel_logseg::columnar_batch::",
        "ravel_logseg::page::",
        "ravel_logseg::block::",
        "ravel_logseg::rlog_codec::",
        "ravel_codec::",
    ];
    const FETCH: &[&str] = &[
        "rlog::fetch_resident_groups",
        "rlog::fetch_input_range",
        "rlog::fetch_range",
        "rlog::fetch_block",
        "rlog::next_raw_block",
    ];
    const MERGE: &[&str] = &[
        "rlog::merge_catalogs",
        "rlog::run_stream_pass",
        "rlog::cursor_merge_one_stream",
        "rlog::merge_cursor_batch",
        "rlog::reservation_batches",
        "rlog::merge_stream_into_parts",
        "rlog::PartSink",
    ];

    pub const PHASES: [&str; 10] = [
        "catalog load",
        "plan",
        "zstd compression",
        "part encode (excl. zstd)",
        "part PUT",
        "block decode (incl. zstd decompress)",
        "row group fetch",
        "merge (k-way + reservation batching)",
        "compaction other (pre-merge, publish, runtime)",
        "outside compaction stack",
    ];

    fn any(names: &[String], pats: &[&str]) -> bool {
        names.iter().any(|n| pats.iter().any(|p| n.contains(p)))
    }

    fn zstd(names: &[String], decompress: bool) -> bool {
        names.iter().any(|n| {
            let l = n.to_ascii_lowercase();
            let is_zstd = l.contains("zstd") || l.contains("huf_") || l.contains("fse_");
            let is_de = l.contains("decompress") || l.contains("decode");
            is_zstd
                && (is_de == decompress)
                && (decompress
                    || l.contains("compress")
                    || l.contains("huf_")
                    || l.contains("fse_")
                    || l.contains("zstd_"))
        })
    }

    /// The phase one sample stack belongs to; the first rule that matches wins,
    /// so a nested phase (zstd inside encode, decompress inside catalog load)
    /// is taken before its enclosing one.
    fn classify(names: &[String]) -> &'static str {
        let in_compaction = any(names, &["ravel_maintain::"]);
        if !in_compaction {
            return PHASES[9];
        }
        if any(names, CATALOG) {
            PHASES[0]
        } else if any(names, PLAN) {
            PHASES[1]
        } else if zstd(names, false) && !zstd(names, true) {
            PHASES[2]
        } else if any(names, ENCODE) {
            PHASES[3]
        } else if any(names, PUT) {
            PHASES[4]
        } else if zstd(names, true) || any(names, DECODE) {
            PHASES[5]
        } else if any(names, FETCH) {
            PHASES[6]
        } else if any(names, MERGE) {
            PHASES[7]
        } else {
            PHASES[8]
        }
    }

    pub fn finish(sampler: Sampler) -> Value {
        let report = match sampler.guard.report().build() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("profiling: could not build report: {e}");
                return json!({"error": e.to_string()});
            }
        };
        let mut total: u64 = 0;
        let mut unattributed: u64 = 0;
        let mut leaf: HashMap<String, u64> = HashMap::new();
        let mut inclusive: HashMap<String, u64> = HashMap::new();
        let mut phases: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut threads: HashMap<String, u64> = HashMap::new();
        let mut zstd_decompress: u64 = 0;
        for (frames, count) in &report.data {
            let count = u64::try_from(*count).unwrap_or(0);
            total += count;
            // frames[0] is the innermost frame; within a frame, symbol 0 is the
            // innermost inlined function.
            let names: Vec<String> = frames
                .frames
                .iter()
                .flat_map(|f| f.iter().map(|s| clean(&s.name())))
                .collect();
            let leaf_name = names.first().cloned().unwrap_or_default();
            if !ravel_bench::profiling::frame_is_attributed(&leaf_name) {
                unattributed += count;
            }
            *leaf.entry(leaf_name).or_default() += count;
            let unique: HashSet<&String> = names.iter().collect();
            for n in unique {
                *inclusive.entry(n.clone()).or_default() += count;
            }
            *phases.entry(classify(&names)).or_default() += count;
            if classify(&names) == PHASES[5] && zstd(&names, true) {
                zstd_decompress += count;
            }
            *threads.entry(frames.thread_name_or_id()).or_default() += count;
        }
        let share = match ravel_bench::profiling::check_attribution(unattributed, total) {
            Ok(share) => share,
            Err(msg) => {
                eprintln!("profiling: {msg}");
                std::process::exit(1);
            }
        };
        match std::fs::File::create(&sampler.path) {
            Ok(file) => {
                if let Err(e) = report.flamegraph(file) {
                    eprintln!("profiling: could not write flamegraph: {e}");
                }
            }
            Err(e) => eprintln!(
                "profiling: could not create {}: {e}",
                sampler.path.display()
            ),
        }
        let pct = |c: u64| {
            if total == 0 {
                0.0
            } else {
                c as f64 * 100.0 / total as f64
            }
        };
        let top = |m: &HashMap<String, u64>, n: usize, max_pct: f64| -> Vec<Value> {
            let mut v: Vec<(&String, &u64)> =
                m.iter().filter(|(_, c)| pct(**c) < max_pct).collect();
            v.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
            v.into_iter()
                .take(n)
                .map(|(name, c)| json!({"function": name, "samples": c, "share_pct": pct(*c)}))
                .collect()
        };
        let phase_rows: Vec<Value> = PHASES
            .iter()
            .map(|p| {
                let c = phases.get(p).copied().unwrap_or(0);
                json!({"phase": p, "samples": c, "share_pct": pct(c), "cpu_s_est": c as f64 / f64::from(HZ)})
            })
            .collect();
        let mut thread_rows: Vec<(String, u64)> = threads.into_iter().collect();
        thread_rows.sort_by(|a, b| b.1.cmp(&a.1));
        json!({
            "svg": sampler.path.display().to_string(),
            "frequency_hz": HZ,
            "samples": total,
            "cpu_s_est": total as f64 / f64::from(HZ),
            "unattributed_share_pct": share * 100.0,
            "leaf_exclusive_top15": top(&leaf, 15, f64::INFINITY),
            "inclusive_top10_below_95pct": top(&inclusive, 10, 95.0),
            "inclusive_top40": top(&inclusive, 40, f64::INFINITY),
            "phases": phase_rows,
            "zstd_decompress_samples_in_block_decode": zstd_decompress,
            "samples_by_thread": thread_rows.iter().take(12).map(|(t, c)| json!({"thread": t, "samples": c, "share_pct": pct(*c)})).collect::<Vec<_>>(),
        })
    }
}

// ---------------------------------------------------------------------------
// The run.

fn compactor_config(zstd_level: Option<i32>, ledger: RequestLedger) -> (CompactorConfig, Value) {
    // Mirrors ravel-cli's build_compactor_config for `compact-bucket` with no
    // knob set and one concurrent merge.
    let mut config = CompactorConfig {
        dry_run: false,
        merge_memory_tracker: Some(MergeMemoryTracker::new()),
        format_floor: Some(ravel_ingest::FormatFloorPolicy::default()),
        ..CompactorConfig::default()
    };
    let host_total = detect_host_memory_total_bytes();
    let budget = host_total.map(|total| {
        merge_memory_budget_bytes(
            host_memory_budget_bytes(total),
            config.merge_cursor_budget_bytes,
        )
    });
    let target = ResolvedL1PartMemoryTarget::resolve(
        None,
        budget,
        1,
        ravel_maintain::config::DEFAULT_CLAIM_LEASE_DURATION,
    );
    target.apply_to(&mut config);
    if let Some(level) = zstd_level {
        config.rlog_zstd_level = validate_rlog_zstd_level(level).expect("valid zstd level");
    }
    config.request_ledger = Some(ledger);
    let resolved = json!({
        "host_memory_total_bytes": host_total,
        "merge_memory_budget_bytes": budget,
        "l1_part_memory_target_bytes": target.bytes,
        "l1_part_memory_target_source": format!("{:?}", target.source),
        "rlog_l1_part_memory_target_bytes": config.rlog_l1_part_memory_target_bytes,
        "max_l1_part_bytes": config.max_l1_part_bytes,
        "rlog_max_l1_part_bytes": config.rlog_max_l1_part_bytes,
        "rlog_zstd_level": config.rlog_zstd_level,
        "merge_cursor_budget_bytes": config.merge_cursor_budget_bytes,
        "merge_resident_ceiling_bytes": config.merge_resident_ceiling_bytes,
        "input_read_concurrency": config.input_read_concurrency,
        "page_choice": "Exhaustive (RlogConfig::default(); CompactorConfig has no page_choice)",
    });
    (config, resolved)
}

fn phase_requests(p: &PhaseRequests) -> Value {
    json!({"requests": p.requests, "wire_bytes_received": p.wire_bytes_received, "wire_bytes_sent": p.wire_bytes_sent})
}

/// Total wall covered by the union of `[start, end)` intervals, in seconds.
fn union_s(mut spans: Vec<(f64, f64)>) -> f64 {
    spans.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut total = 0.0;
    let mut cur: Option<(f64, f64)> = None;
    for (s, e) in spans {
        match cur {
            Some((cs, ce)) if s <= ce => cur = Some((cs, ce.max(e))),
            Some((cs, ce)) => {
                total += ce - cs;
                cur = Some((s, e));
            }
            None => cur = Some((s, e)),
        }
    }
    if let Some((cs, ce)) = cur {
        total += ce - cs;
    }
    total
}

struct Compacted {
    report: Value,
    parts: Vec<(ravel_proto::commit::v1::CompactionPart, Bytes)>,
    input_set_hash: Vec<u8>,
    identity: ObjectIdentity,
    zstd_level: i32,
}

async fn run(
    args: &Args,
    objects: &[L0Object],
    events: Arc<Mutex<Vec<CapturedEvent>>>,
) -> Compacted {
    let store = Arc::new(TimingStore::new());
    let writer_id = Uuid::from_u128(u128::from(args.seed) << 64 | 0x3819);
    let seed_start = Instant::now();
    seed_objects(store.as_ref(), objects, writer_id, 1).await;
    let seed_s = seed_start.elapsed().as_secs_f64();

    let th = TenantId::new(TENANT).hash();
    let bucket = Bucket::new(th, Signal::Logs, SHARD, HOUR);
    let clock = FixedClock::new((i64::from(HOUR) + 1) * NS_PER_HOUR + SEAL_PADDING_NS);
    let ledger = RequestLedger::new();
    let (config, resolved) = compactor_config(args.zstd_level, ledger.clone());

    events.lock().expect("events lock").clear();
    store.armed.store(true, Ordering::Relaxed);
    #[cfg(feature = "profiling")]
    let sampler = prof::start();
    let threads0 = thread_ticks();
    let (u0, s0) = self_ticks();
    let cpu0 = process_cpu_s();
    let t0 = Instant::now();
    let outcome = compact_bucket(store.as_ref(), &clock, &config, &bucket)
        .await
        .expect("compact_bucket");
    let wall_s = t0.elapsed().as_secs_f64();
    let cpu_s = process_cpu_s() - cpu0;
    let (u1, s1) = self_ticks();
    let threads1 = thread_ticks();
    #[cfg(feature = "profiling")]
    let profile = sampler.map(prof::finish);
    #[cfg(not(feature = "profiling"))]
    let profile: Option<Value> = None;
    store.armed.store(false, Ordering::Relaxed);

    let parts_written = match outcome {
        CompactionOutcome::Compacted { parts, .. } => parts,
        other => panic!("compaction did not compact: {other:?}"),
    };

    // Output parts, read from the published compaction record.
    let listing = ravel_maintain::read::list_bucket(store.as_ref(), &bucket)
        .await
        .expect("list bucket");
    let rec_key = listing
        .compaction_record_keys
        .first()
        .expect("a compaction record exists")
        .clone();
    let got = store
        .get(&rec_key, GetRange::Full)
        .await
        .expect("get record");
    let crec = record::decode_compaction(got.data.as_ref()).expect("decode compaction record");
    let mut parts = Vec::new();
    for p in &crec.parts {
        let key = keys::reconstruct_l1_part_key(&crec, p).expect("part key");
        let data = store
            .get(&key, GetRange::Full)
            .await
            .expect("get part")
            .data;
        parts.push((p.clone(), data));
    }
    let l0_bytes: u64 = objects.iter().map(|o| o.bytes.len() as u64).sum();
    let l0_records: u64 = objects.iter().map(|o| o.records).sum();
    let l1_bytes: u64 = crec.parts.iter().map(|p| p.object_size).sum();
    let l1_records: u64 = crec.parts.iter().map(|p| p.sample_count).sum();

    // Timeline, seconds from the call.
    let rel = |t: Instant| t.saturating_duration_since(t0).as_secs_f64();
    let evs = events.lock().expect("events lock");
    let find = |needle: &str| evs.iter().find(|e| e.message.contains(needle));
    let merge_started = find("compaction merge started").map(|e| rel(e.at));
    let mode_event = find("rlog merge read mode");
    let mode_at = mode_event.map(|e| rel(e.at));
    let merge_ended = find("compaction merge ended").map(|e| rel(e.at));
    let mode = mode_event.map(|e| e.fields.clone()).unwrap_or_default();
    let warnings: Vec<String> = evs
        .iter()
        .filter(|e| e.level <= tracing::Level::WARN)
        .map(|e| format!("{} {:?}", e.message, e.fields))
        .collect();
    drop(evs);

    let ops = store.ops.lock().expect("ops lock");
    let record_prefix =
        keys::commit_shard_hour_prefix(&th, Signal::Logs, SHARD, HOUR).expect("prefix");
    let mode_t = mode_at.unwrap_or(0.0);
    let ms = merge_started.unwrap_or(0.0);
    let spans = |pred: &dyn Fn(&Op) -> bool| -> (f64, u64, u64, f64, f64) {
        let sel: Vec<&Op> = ops.iter().filter(|o| pred(o)).collect();
        let v: Vec<(f64, f64)> = sel.iter().map(|o| (rel(o.start), rel(o.end))).collect();
        let first = v.iter().map(|x| x.0).fold(f64::INFINITY, f64::min);
        let last = v.iter().map(|x| x.1).fold(0.0, f64::max);
        let bytes = sel.iter().map(|o| o.bytes).sum();
        (union_s(v), sel.len() as u64, bytes, first, last)
    };
    let span_json = |s: (f64, u64, u64, f64, f64)| {
        json!({"union_wall_s": s.0, "ops": s.1, "bytes": s.2,
               "first_start_s": if s.1 == 0 { Value::Null } else { json!(s.3) },
               "last_end_s": if s.1 == 0 { Value::Null } else { json!(s.4) }})
    };
    let pre_merge_ops = spans(&|o: &Op| rel(o.start) < ms);
    let catalog_gets =
        spans(&|o: &Op| o.kind == OpKind::Get && rel(o.start) >= ms && rel(o.start) < mode_t);
    let block_gets = spans(&|o: &Op| o.kind == OpKind::Get && rel(o.start) >= mode_t);
    let part_puts = spans(&|o: &Op| o.kind == OpKind::Put && !o.key.starts_with(&record_prefix));
    let record_puts = spans(&|o: &Op| o.kind == OpKind::Put && o.key.starts_with(&record_prefix));
    let other_ops =
        spans(&|o: &Op| rel(o.start) >= ms && !matches!(o.kind, OpKind::Get | OpKind::Put));
    drop(ops);

    // The window from the read-mode decision to the last part PUT: fetch,
    // decode, merge, encode, zstd and PUT all run inside it.
    let merge_window = (
        mode_at,
        if part_puts.1 > 0 {
            Some(part_puts.4)
        } else {
            None
        },
    );
    let timeline = json!({
        "call_start_s": 0.0,
        "merge_started_event_s": merge_started,
        "read_mode_event_s": mode_at,
        "merge_ended_event_s": merge_ended,
        "call_end_s": wall_s,
        "pre_merge_s (call start to merge-started event: listing, record reads, admission)": merge_started,
        "catalog_load_plus_plan_s (merge-started to read-mode event)": match (merge_started, mode_at) { (Some(a), Some(b)) => json!(b - a), _ => Value::Null },
        "read_mode_to_last_part_put_s": match merge_window { (Some(a), Some(b)) => json!(b - a), _ => Value::Null },
        "last_part_put_to_call_end_s": if part_puts.1 > 0 { json!(wall_s - part_puts.4) } else { Value::Null },
        "store_ops": {
            "pre_merge": span_json(pre_merge_ops),
            "catalog_gets (between merge-started and read-mode)": span_json(catalog_gets),
            "row_group_gets (after read-mode)": span_json(block_gets),
            "part_puts": span_json(part_puts),
            "record_puts": span_json(record_puts),
            "other_ops_after_merge_started": span_json(other_ops),
        },
    });

    let lr = ledger.report();
    let ledger_json = json!({
        "list": phase_requests(&lr.list),
        "record_read": phase_requests(&lr.record_read),
        "catalog_read": phase_requests(&lr.catalog_read),
        "block_read": phase_requests(&lr.block_read),
        "part_put": phase_requests(&lr.part_put),
        "publish": phase_requests(&lr.publish),
        "coordinate": phase_requests(&lr.coordinate),
        "floor_read": phase_requests(&lr.floor_read),
    });

    let mut thread_rows: Vec<(String, String, f64)> = threads1
        .iter()
        .map(|(tid, (comm, t1))| {
            let before = threads0.get(tid).map(|(_, t)| *t).unwrap_or(0);
            (
                tid.clone(),
                comm.clone(),
                t1.saturating_sub(before) as f64 / TICKS_PER_S,
            )
        })
        .filter(|r| r.2 > 0.0)
        .collect();
    thread_rows.sort_by(|a, b| b.2.total_cmp(&a.2));
    let busiest = thread_rows.first().map(|r| r.2).unwrap_or(0.0);

    let tracker = config.merge_memory_tracker.as_ref().expect("tracker");
    let peaks = tracker.phase_peaks();
    let report = json!({
        "issue": 3819,
        "build": {
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "profiling_feature": cfg!(feature = "profiling"),
            "jemalloc": cfg!(all(feature = "compaction-jemalloc", not(target_env = "msvc"))),
        },
        "args": format!("{args:?}"),
        "seed_wall_s": seed_s,
        "config": resolved,
        "records_in": l0_records,
        "records_out": l1_records,
        "l0_objects": objects.len(),
        "l0_bytes": l0_bytes,
        "l1_bytes": l1_bytes,
        "l1_l0_ratio": l1_bytes as f64 / l0_bytes.max(1) as f64,
        "l1_parts": crec.parts.len(),
        "l1_parts_reported_by_outcome": parts_written,
        "l1_part_bytes": crec.parts.iter().map(|p| p.object_size).collect::<Vec<_>>(),
        "merge_read_mode": mode.get("mode").cloned(),
        "read_mode_event_fields": mode,
        "pass_count": Value::Null,
        "pass_count_note": "not exposed: compact_bucket emits no per-pass event and the ledger has no per-pass counter",
        "memory_target_flushes": tracker.memory_target_flushes(),
        "stored_target_flushes": tracker.stored_target_flushes(),
        "probes_run": tracker.probes_run(),
        "peaks": {
            "total_bytes": tracker.peak_total_bytes(),
            "resident_bytes": tracker.peak_resident_bytes(),
            "writer_bytes": tracker.peak_writer_bytes(),
            "retained_part_bytes": tracker.peak_retained_part_bytes(),
            "catalog_directory_bytes": peaks.catalog_directory_decoded_bytes,
        },
        "wall_s": wall_s,
        "process_cpu_s": cpu_s,
        "user_s (proc ticks)": (u1 - u0) as f64 / TICKS_PER_S,
        "sys_s (proc ticks)": (s1 - s0) as f64 / TICKS_PER_S,
        "cores_busy (cpu/wall)": cpu_s / wall_s,
        "records_per_s_wall": l0_records as f64 / wall_s,
        "records_per_s_per_core (records / process cpu)": l0_records as f64 / cpu_s,
        "busiest_thread_cpu_s": busiest,
        "threads_cpu_s": thread_rows.iter().take(8).map(|r| json!({"tid": r.0, "comm": r.1, "cpu_s": r.2})).collect::<Vec<_>>(),
        "timeline": timeline,
        "request_ledger": ledger_json,
        "warnings": warnings,
        "profile": profile,
    });
    Compacted {
        report,
        parts,
        input_set_hash: crec.input_set_hash.clone(),
        identity: ObjectIdentity {
            tenant_hash: th.0,
            shard: SHARD,
            writer_id: config.compactor_writer_id.into_bytes(),
            writer_epoch: 0,
            writer_seq: 0,
        },
        zstd_level: config.rlog_zstd_level,
    }
}

/// Re-encodes every L1 part from its decoded records with the compactor's
/// writer settings and `choice`. Returns bytes, encode CPU and whether each
/// part reproduced the published bytes.
fn reencode(c: &Compacted, choice: PageChoice) -> Value {
    let read_cfg = RlogConfig::default();
    let mut bytes = 0u64;
    let mut cpu = 0.0;
    let mut wall = 0.0;
    let mut decode_cpu = 0.0;
    let mut identical = 0usize;
    for (part, data) in &c.parts {
        let d0 = thread_cpu_s();
        let reader = RlogReader::new(&data[..], &read_cfg).expect("open L1 part");
        let (records, _) = reader
            .scan(&Predicate::And(Vec::new()))
            .expect("scan L1 part");
        decode_cpu += thread_cpu_s() - d0;
        let cfg = RlogConfig {
            zstd_level: c.zstd_level,
            page_choice: choice,
            ..RlogConfig::default()
        };
        let w0 = Instant::now();
        let c0 = thread_cpu_s();
        let mut w = RlogWriter::new(cfg, c.identity);
        for r in records {
            w.push(r).expect("push L1 record");
        }
        let (out, _) = w
            .finish_compacted_with_stats(1, c.input_set_hash.clone(), part.part_index)
            .expect("re-encode L1 part");
        cpu += thread_cpu_s() - c0;
        wall += w0.elapsed().as_secs_f64();
        bytes += out.len() as u64;
        if blake3::hash(&out).as_bytes()[..] == part.content_hash[..] {
            identical += 1;
        }
    }
    json!({
        "page_choice": format!("{choice:?}"),
        "zstd_level": c.zstd_level,
        "parts": c.parts.len(),
        "bytes": bytes,
        "encode_thread_cpu_s": cpu,
        "encode_wall_s": wall,
        "decode_thread_cpu_s": decode_cpu,
        "parts_byte_identical_to_published": identical,
    })
}

fn host_info() -> Value {
    let uname = std::fs::read_to_string("/proc/sys/kernel/osrelease").unwrap_or_default();
    let mem_kb = std::fs::read_to_string("/proc/meminfo").ok().and_then(|t| {
        t.lines()
            .find(|l| l.starts_with("MemTotal:"))
            .and_then(|l| {
                l.split_whitespace()
                    .nth(1)
                    .and_then(|v| v.parse::<u64>().ok())
            })
    });
    json!({
        "arch": std::env::consts::ARCH,
        "kernel": uname.trim(),
        "available_parallelism": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
        "mem_total_kib": mem_kb,
    })
}

fn f(v: &Value) -> String {
    match v {
        Value::Number(n) => {
            let x = n.as_f64().unwrap_or(0.0);
            if x.fract() == 0.0 && x.abs() < 1e15 {
                format!("{}", x as i64)
            } else {
                format!("{x:.3}")
            }
        }
        Value::Null => "-".to_string(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn print_table(doc: &Value) {
    let r = &doc["run"];
    let mut out = String::new();
    let _ = writeln!(out, "== rlog compaction profile (issue #3819) ==");
    let _ = writeln!(out, "host: {}", doc["host"]);
    let s = &doc["shape"];
    let _ = writeln!(
        out,
        "shape: objects {} streams {} records/object {} streams/object {} blocks/object {} row groups/object {}",
        f(&s["objects"]),
        f(&s["distinct_streams_in_bucket"]),
        s["records_per_object"],
        s["streams_per_object"],
        s["blocks_per_object"],
        s["row_groups_per_object"]
    );
    let _ = writeln!(
        out,
        "records {} | L0 bytes {} | L1 bytes {} | L1/L0 {} | L1 parts {} | mode {}",
        f(&r["records_in"]),
        f(&r["l0_bytes"]),
        f(&r["l1_bytes"]),
        f(&r["l1_l0_ratio"]),
        f(&r["l1_parts"]),
        f(&r["merge_read_mode"])
    );
    let _ = writeln!(
        out,
        "wall {} s | process CPU {} s | busiest thread {} s | records/s/core {}",
        f(&r["wall_s"]),
        f(&r["process_cpu_s"]),
        f(&r["busiest_thread_cpu_s"]),
        f(&r["records_per_s_per_core (records / process cpu)"])
    );
    let t = &r["timeline"];
    let _ = writeln!(out, "timeline (s from call start):");
    for k in [
        "merge_started_event_s",
        "read_mode_event_s",
        "merge_ended_event_s",
        "call_end_s",
    ] {
        let _ = writeln!(out, "  {k:<28} {}", f(&t[k]));
    }
    if let Some(ops) = t["store_ops"].as_object() {
        for (k, v) in ops {
            let _ = writeln!(
                out,
                "  {k:<50} ops {:>6} union wall {} s",
                f(&v["ops"]),
                f(&v["union_wall_s"])
            );
        }
    }
    if let Some(p) = r["profile"].as_object() {
        let _ = writeln!(
            out,
            "profile: {} samples ({} s est.), unattributed {}%",
            f(&p["samples"]),
            f(&p["cpu_s_est"]),
            f(&p["unattributed_share_pct"])
        );
        let _ = writeln!(
            out,
            "phase (profile samples)                         share%   cpu s"
        );
        for row in p["phases"].as_array().into_iter().flatten() {
            let _ = writeln!(
                out,
                "  {:<46} {:>6} {:>7}",
                f(&row["phase"]),
                f(&row["share_pct"]),
                f(&row["cpu_s_est"])
            );
        }
        let _ = writeln!(out, "top leaf functions (exclusive):");
        for row in p["leaf_exclusive_top15"].as_array().into_iter().flatten() {
            let _ = writeln!(
                out,
                "  {:>6}%  {}",
                f(&row["share_pct"]),
                f(&row["function"])
            );
        }
        let _ = writeln!(
            out,
            "top functions (inclusive, frames under 95% of samples):"
        );
        for row in p["inclusive_top10_below_95pct"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let _ = writeln!(
                out,
                "  {:>6}%  {}",
                f(&row["share_pct"]),
                f(&row["function"])
            );
        }
    }
    for ab in doc["page_choice_ab"].as_array().into_iter().flatten() {
        let _ = writeln!(
            out,
            "re-encode {}: bytes {} encode cpu {} s identical parts {}",
            f(&ab["page_choice"]),
            f(&ab["bytes"]),
            f(&ab["encode_thread_cpu_s"]),
            f(&ab["parts_byte_identical_to_published"])
        );
    }
    eprint!("{out}");
}

fn main() {
    let args = Args::parse();
    let events = Arc::new(Mutex::new(Vec::new()));
    tracing_subscriber::registry()
        .with(EventCapture {
            events: Arc::clone(&events),
        })
        .with(tracing_subscriber::filter::LevelFilter::INFO)
        .init();

    let writer_id = Uuid::from_u128(u128::from(args.seed) << 64 | 0x3819);
    let gen_start = Instant::now();
    let objects = build_objects(&args, writer_id, 1);
    let gen_s = gen_start.elapsed().as_secs_f64();
    let shape = verify_shape(&objects);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let compacted = rt.block_on(run(&args, &objects, events));
    drop(rt);
    drop(objects);

    let page_choice_ab = if args.page_choice_ab {
        let ex = reencode(&compacted, PageChoice::Exhaustive);
        let es = reencode(&compacted, PageChoice::Estimated);
        let l0 = compacted.report["l0_bytes"].as_u64().unwrap_or(1).max(1);
        let ratio = |v: &Value| v["bytes"].as_u64().unwrap_or(0) as f64 / l0 as f64;
        let (rex, res) = (ratio(&ex), ratio(&es));
        let mut ex = ex;
        let mut es = es;
        ex["l1_l0_ratio"] = json!(rex);
        es["l1_l0_ratio"] = json!(res);
        json!([ex, es])
    } else {
        Value::Null
    };

    let doc = json!({
        "host": host_info(),
        "generate_wall_s": gen_s,
        "shape": shape,
        "run": compacted.report,
        "page_choice_ab": page_choice_ab,
    });
    print_table(&doc);
    println!("{}", serde_json::to_string_pretty(&doc).expect("json"));
}
