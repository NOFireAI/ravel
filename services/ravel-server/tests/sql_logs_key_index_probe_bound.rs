//! A logs equality on a key-indexed declared column, served over
//! `POST /api/v1/sql`, probes the KEY_IDX section of every object above the
//! fetcher's block-range threshold, whatever the projection break-even
//! (ADR-2707 decision 5, issue #3117). Under the break-even the object would
//! otherwise be read whole with no probe, either opened unplanned (ADR-2677
//! decision 2, when the scan holds at least `target_partitions` such
//! objects) or planned without its key. An object at or under the threshold
//! is still read whole and not probed.
//!
//! The fixture is 24 RLOG objects of eight 16-record blocks, every record
//! carrying a distinct `user_id`; only record 50 of object 0 holds the target.
//! Object size is set by a random pad in each body: about 274 KB for a small
//! object and about 900 KB for a big one. The expected KEY_IDX reads of each
//! object are computed from its bytes with
//! `ravel_logseg::key_index::probe_ranged`, independently of the fetcher, and
//! compared with what the store served.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::Clock;
use ravel_logseg::footer::kind;
use ravel_logseg::key_index::{KeyType, key8_i64, probe_ranged};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, LogSegError, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::{EngineConfig, LogSegmentFetcher, SegmentFetcher};
use ravel_server::sql::{SqlState, router};
use ravel_sql::{
    DeclaredColumn, DeclaredType, KeyIndexDeclaration, SqlConfig, SqlExecutor,
    StaticDeclaredColumns,
};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NOW_NS: i64 = 4 * NS_PER_HOUR;
const TOKEN: &str = "acme-token";
const RECORDS_PER_BLOCK: usize = 16;
const BLOCKS: usize = 8;
const RECORDS: usize = RECORDS_PER_BLOCK * BLOCKS;
const OBJECTS: usize = 24;
const TARGET: i64 = 435_090_932_899_640_449;
const HOLDER: usize = 0;
/// Pad bytes per record of a big object (about 900 KB) and a small one
/// (about 274 KB), hex-encoded into the body at twice the length.
const BIG: usize = 6 * 1024;
const SMALL: usize = 2 * 1024;
const KIB: u64 = 1024;
const SQL: &str = "SELECT substr(body, 1, 6) AS id FROM logs WHERE user_id = 435090932899640449";
/// The KEY_IDX reads of probing all 24 objects for `TARGET`: one prefix read
/// per object, plus one bucket read for each of the 13 objects whose bucket
/// for `TARGET` is not empty (the KEY_IDX content is the same at either pad).
const ALL_OBJECTS_KEY_READS: u64 = 24 + 13;

struct FixedClock;
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

struct RecordingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    gets: Mutex<Vec<(String, GetRange)>>,
}

#[async_trait::async_trait]
impl ObjectStoreBackend for RecordingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.gets
            .lock()
            .expect("get log")
            .push((key.to_string(), range));
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

fn tenant() -> TenantId {
    TenantId::new("acme".to_string())
}

/// A distinct i64 per (object, record), never equal to TARGET.
fn decoy(object: usize, i: usize) -> i64 {
    1_000_000 + (object * RECORDS + i) as i64
}

fn record_of(object: usize, i: usize, pad_bytes: usize) -> LogRecord {
    let user = if object == HOLDER && i == 50 {
        TARGET
    } else {
        decoy(object, i)
    };
    let mut hasher = blake3::Hasher::new();
    hasher.update(&((object * RECORDS + i) as u64).to_le_bytes());
    let mut pad = vec![0u8; pad_bytes];
    hasher.finalize_xof().fill(&mut pad);
    let body: String = pad.iter().map(|b| format!("{b:02x}")).collect();
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let ts = 1_000 + (i * 4 + object) as i64;
    LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("o{object}r{i:03}|{body}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![("user_id".to_string(), AttrValue::I64(user))],
    }
}

/// Pads with object 0 (the holder) big, objects `1..=small` small, the rest
/// big.
fn mixed(small: usize) -> Vec<usize> {
    (0..OBJECTS)
        .map(|i| if (1..=small).contains(&i) { SMALL } else { BIG })
        .collect()
}

struct Config {
    /// Per object, the random pad per record (the object size knob).
    pads: Vec<usize>,
    target_partitions: usize,
    /// `with_block_range_threshold`.
    threshold: u64,
    /// `with_projection_break_even_bytes`, which moves
    /// `plan_whole_object_bound` above the threshold.
    break_even: u64,
}

struct Object {
    bytes: Bytes,
    gets: Vec<GetRange>,
}

impl Object {
    fn size(&self) -> u64 {
        self.bytes.len() as u64
    }

    /// The GETs this object served that lie inside its KEY_IDX section.
    fn key_idx_gets(&self) -> Vec<GetRange> {
        let footer = ravel_logseg::footer::open(&self.bytes).expect("footer");
        let desc = footer.section(kind::KEY_IDX).expect("KEY_IDX");
        self.gets
            .iter()
            .filter(|g| {
                let r = span(g, self.size());
                desc.offset <= r.start && r.end <= desc.offset + desc.len
            })
            .cloned()
            .collect()
    }

    /// The KEY_IDX reads a probe for `TARGET` issues on this object,
    /// recomputed from the format crate.
    fn expected_key_reads(&self) -> Vec<GetRange> {
        let footer = ravel_logseg::footer::open(&self.bytes).expect("footer");
        let mut reads = Vec::new();
        probe_ranged(
            &footer,
            "user_id",
            KeyType::I64,
            &key8_i64(TARGET),
            |r: Range<u64>| {
                reads.push(GetRange::Range(r.start, r.end));
                Ok::<_, LogSegError>(self.bytes.slice(r.start as usize..r.end as usize))
            },
        )
        .expect("probe");
        reads
    }

    fn read_whole(&self) -> bool {
        self.gets.iter().any(|g| matches!(g, GetRange::Full))
    }
}

fn span(get: &GetRange, size: u64) -> Range<u64> {
    match *get {
        GetRange::Full => 0..size,
        GetRange::Range(start, end) => start..end,
        GetRange::Suffix(n) => size.saturating_sub(n)..size,
    }
}

struct Outcome {
    response: Value,
    objects: Vec<Object>,
}

impl Outcome {
    fn ids(&self) -> Vec<String> {
        self.response["data"]["rows"]
            .as_array()
            .unwrap_or_else(|| panic!("no rows in {}", self.response))
            .iter()
            .map(|row| row[0].as_str().expect("string cell").to_string())
            .collect()
    }

    fn pruned(&self) -> u64 {
        self.response["stats"]["pruning"]["segmentsPrunedByKeyIndex"]
            .as_u64()
            .unwrap_or_else(|| panic!("no segmentsPrunedByKeyIndex in {}", self.response))
    }

    fn key_index_gets(&self) -> u64 {
        self.response["stats"]["phases"]
            .as_array()
            .expect("phases")
            .iter()
            .find(|p| p["phase"] == "keyIndex")
            .map_or(0, |p| p["s3GetRequests"].as_u64().expect("u64"))
    }
}

async fn run(cfg: &Config) -> Outcome {
    let recorder = Arc::new(RecordingStore {
        inner: Arc::new(MemoryStore::new()),
        gets: Mutex::new(Vec::new()),
    });
    let store: Arc<dyn ObjectStoreBackend> = recorder.clone();
    let tenant_hash = tenant().hash();
    let mut published: Vec<(String, Bytes)> = Vec::new();
    for (seq, pad) in cfg.pads.iter().enumerate() {
        let writer_id = Uuid::from_u128(3_117_000 + seq as u128);
        let mut writer = RlogWriter::new(
            RlogConfig {
                block_target_records: RECORDS_PER_BLOCK,
                ..RlogConfig::default()
            },
            ObjectIdentity {
                tenant_hash: tenant_hash.0,
                shard: 0,
                writer_id: writer_id.into_bytes(),
                writer_epoch: 1,
                writer_seq: seq as u64 + 1,
            },
        )
        .with_key_index_fields(vec![("user_id".to_string(), KeyType::I64)]);
        let records: Vec<LogRecord> = (0..RECORDS).map(|i| record_of(seq, i, *pad)).collect();
        let min = records.iter().map(|r| r.ts_ns).min().expect("records");
        let max = records.iter().map(|r| r.ts_ns).max().expect("records");
        for r in records {
            writer.push(r).expect("push");
        }
        let bytes = Bytes::from(writer.finish().expect("finish"));
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: seq as u64 + 1,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: RECORDS as u64,
            series_count: 1,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            min_ingest_ts_ns: min,
            max_ingest_ts_ns: max,
            segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
            created_unix_ns: 10 + seq as i64,
            ingest_hour_bucket: 0,
        })
        .expect("record");
        let key = keys::reconstruct_data_key(&rec).expect("key");
        recorder
            .put(&key, bytes.clone(), PutOptions::default())
            .await
            .expect("put");
        publish::publish(recorder.as_ref(), &rec, &RetryPolicy::default())
            .await
            .expect("publish");
        published.push((key, bytes));
    }

    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    // A 1 KiB tail probe (KEY_IDX, right after BLOCKS, is never inside it) and
    // a request cost low enough that every wanted extent is its own GET.
    let logs = LogSegmentFetcher::new(store.clone())
        .with_suffix_len(1024)
        .with_request_cost_bytes(1)
        .with_block_range_threshold(cfg.threshold)
        .with_projection_break_even_bytes(Some(cfg.break_even));
    assert_eq!(
        logs.plan_whole_object_bound(),
        cfg.break_even.max(cfg.threshold)
    );
    let sql_config = SqlConfig {
        engine: EngineConfig {
            sql_partition_count: Some(cfg.target_partitions),
            ..EngineConfig::default()
        },
        ..SqlConfig::default()
    };
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(store.clone()),
        logs,
        ravel_sql::SpanSegmentFetcher::new(store.clone()),
        sql_config,
        1 << 30,
    )
    .with_declared_column_source(Arc::new(
        StaticDeclaredColumns::new(vec![DeclaredColumn::new("user_id", DeclaredType::I64)])
            .with_key_index(KeyIndexDeclaration {
                columns: vec!["user_id".to_string()],
                trace_id: false,
            }),
    ));
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant());
    let app: Router = router(SqlState {
        executor: Arc::new(executor),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
        store: store.clone(),
        clock: Arc::new(FixedClock),
        max_deadline: Duration::from_secs(60),
        query_accounting: Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        query_admission: ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
    });

    recorder.gets.lock().expect("log").clear();
    let payload = serde_json::json!({
        "query": SQL,
        "start": 0.0,
        "end": NOW_NS as f64 / 1_000_000_000.0,
        "timeout": 60.0,
    })
    .to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::from(payload))
        .expect("request");
    let response = app.oneshot(request).await.expect("oneshot");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let response: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(response["status"], "success", "{response}");

    let gets = recorder.gets.lock().expect("log").clone();
    let objects = published
        .into_iter()
        .map(|(key, bytes)| Object {
            gets: gets
                .iter()
                .filter(|(k, _)| *k == key)
                .map(|(_, r)| *r)
                .collect(),
            bytes,
        })
        .collect();
    Outcome { response, objects }
}

/// Every object's KEY_IDX is probed: the holder's row is returned, every
/// other object is pruned by the probe, each object's KEY_IDX reads are the
/// format's, no object but the holder is read whole, and the keyIndex phase
/// counts exactly the probe reads of all 24 objects.
fn assert_every_object_probed(o: &Outcome) {
    assert_eq!(o.ids(), ["o0r050"]);
    assert_eq!(o.pruned(), (OBJECTS - 1) as u64, "{}", o.response);
    let mut per_object = Vec::new();
    for (n, object) in o.objects.iter().enumerate() {
        let want = object.expected_key_reads();
        assert_eq!(object.key_idx_gets(), want, "object {n}");
        per_object.push(want.len() as u64);
        if n != HOLDER {
            assert!(!object.read_whole(), "object {n}: {:?}", object.gets);
        }
    }
    assert_eq!(per_object.iter().filter(|&&r| r >= 1).count(), 24);
    assert_eq!(per_object.iter().filter(|&&r| r == 2).count(), 13);
    assert_eq!(per_object.iter().sum::<u64>(), ALL_OBJECTS_KEY_READS);
    assert_eq!(o.key_index_gets(), ALL_OBJECTS_KEY_READS);
}

/// 16 small objects sit between a 256 KiB threshold and a 512 KiB
/// break-even, at least `target_partitions` of them, so the scan would open
/// them unplanned (ADR-2677 decision 2). They are probed and pruned.
///
/// FLIP: with only `plan_segment_keyed`'s gate fixed (`may_skip_plan`
/// reduced to `!ctx.fetcher.plans_segment(seg)`) the pruned count reads
/// `left: 7, right: 23`.
#[tokio::test]
async fn sub_break_even_objects_left_unplanned_are_probed() {
    let o = run(&Config {
        pads: mixed(16),
        target_partitions: 4,
        threshold: 256 * KIB,
        break_even: 512 * KIB,
    })
    .await;
    for (n, object) in o.objects.iter().enumerate() {
        let small = (1..=16).contains(&n);
        assert_eq!(
            256 * KIB < object.size() && object.size() <= 512 * KIB,
            small,
            "object {n}: {} bytes",
            object.size()
        );
    }
    assert_every_object_probed(&o);
}

/// The same objects with more partitions than small objects: the scan plans
/// every one, and the plan of a sub-break-even object probes it rather than
/// reading it whole without its key.
///
/// FLIP: with only `may_skip_plan` fixed (`plan_segment_keyed` gating on
/// `plan_whole_object_bound()`) the pruned count reads `left: 7, right: 23`.
#[tokio::test]
async fn sub_break_even_objects_the_scan_plans_are_probed() {
    let o = run(&Config {
        pads: mixed(16),
        target_partitions: 32,
        threshold: 256 * KIB,
        break_even: 512 * KIB,
    })
    .await;
    assert_every_object_probed(&o);
}

/// Every object under a 2 MiB break-even, including the holder: all are
/// probed, and only the holder, where the key is present, is read whole.
///
/// FLIP: against the dispatch ref the pruned count reads `left: 0, right: 23`.
#[tokio::test]
async fn every_object_under_a_larger_break_even_is_probed() {
    let o = run(&Config {
        pads: vec![BIG; OBJECTS],
        target_partitions: 4,
        threshold: 256 * KIB,
        break_even: 2048 * KIB,
    })
    .await;
    for object in &o.objects {
        assert!(512 * KIB < object.size() && object.size() <= 2048 * KIB);
    }
    assert_every_object_probed(&o);
    assert!(o.objects[HOLDER].read_whole());
}

/// At or under the block-range threshold an object is read whole, in one
/// GET, and never probed: one small GET is cheaper than the probe. The big
/// objects above it, under the break-even, are still probed. Both scan
/// routes: `target_partitions` 4 opens the 16 small objects unplanned, 32
/// plans them.
///
/// FLIP: dropping the size gate (`plan_segment_keyed` probing whatever the
/// size, and `may_skip_plan` refusing every keyed segment) reads
/// `left: 23, right: 7` on the pruned count.
#[tokio::test]
async fn objects_at_or_under_the_threshold_are_read_whole_unprobed() {
    for target_partitions in [4, 32] {
        let o = run(&Config {
            pads: mixed(16),
            target_partitions,
            threshold: 512 * KIB,
            break_even: 2048 * KIB,
        })
        .await;
        assert_eq!(o.ids(), ["o0r050"], "tp {target_partitions}");
        assert_eq!(o.pruned(), 7, "tp {target_partitions}: {}", o.response);
        let mut per_object = Vec::new();
        for (n, object) in o.objects.iter().enumerate() {
            if (1..=16).contains(&n) {
                assert!(object.size() <= 512 * KIB, "object {n}");
                assert_eq!(
                    object.gets,
                    [GetRange::Full],
                    "tp {target_partitions} object {n}"
                );
            } else {
                assert!(object.size() > 512 * KIB, "object {n}");
                let want = object.expected_key_reads();
                assert_eq!(
                    object.key_idx_gets(),
                    want,
                    "tp {target_partitions} object {n}"
                );
                per_object.push(want.len() as u64);
            }
        }
        // The 8 big objects: one prefix read each, and a bucket read on 6.
        let tp = target_partitions;
        assert_eq!(per_object.iter().filter(|&&r| r >= 1).count(), 8, "tp {tp}");
        assert_eq!(per_object.iter().filter(|&&r| r == 2).count(), 6, "tp {tp}");
        assert_eq!(o.key_index_gets(), 8 + 6, "tp {tp}");
    }
}
