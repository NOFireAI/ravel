//! A logs equality on an indexed declared column (`I64`, `Str`) or on
//! `trace_id`, served over `POST /api/v1/sql`, probes each object's KEY_IDX
//! section (ADR-2707 decision 5, tier 1, issue #2762): an object without the
//! key is dropped before any directory past its footer is read, and an object
//! holding it reads only the blocks KEY_IDX names.
//!
//! The fixture is three RLOG objects of eight 16-record blocks. Every block
//! carries `status_code` 400 and 600, so the per-block numeric ranges keep
//! every block for `status_code = 500`, and only object 0 holds the probed
//! values: `status_code = 500` in blocks 3 and 5, `region = 'ap-south-1'` in
//! blocks 3 and 6, and the target trace in block 3. The expected KEY_IDX reads
//! of each object are computed from its bytes with
//! `ravel_logseg::key_index::probe_ranged`, which reads the section the way the
//! format defines, independently of the fetcher under test, and compared with
//! what the store served.
//!
//! The fetcher is configured so the reads are observable one by one: a zero
//! block-range threshold (every object takes the ranged path, where the probe
//! runs), a 1 KiB tail probe (KEY_IDX, which sits right after BLOCKS, is never
//! inside it), and a request cost low enough that every wanted extent further
//! than the 64 KiB coalescing floor from the next is its own GET.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::io;
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
use ravel_logseg::key_index::{
    KEY_IDX_VERSION_1, KeyType, TRACE_ID_FIELD, key8_i64, key8_id16, key8_str, probe_ranged,
    rewrite_key_index,
};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, LogRecord, LogSegError, RlogConfig, RlogReader, RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_server::sql::{SqlState, router};
use ravel_sql::{
    DeclaredColumn, DeclaredType, KeyIndexDeclaration, SqlConfig, SqlExecutor,
    StaticDeclaredColumns,
};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use tracing_subscriber::fmt::MakeWriter;
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NOW_NS: i64 = 4 * NS_PER_HOUR;
const TOKEN: &str = "acme-token";
const RECORDS_PER_BLOCK: usize = 16;
const BLOCKS: usize = 8;
const RECORDS: usize = RECORDS_PER_BLOCK * BLOCKS;
const SUFFIX_LEN: u64 = 1024;
const REGION: &str = "ap-south-1";
/// The trace the lookups probe for.
const TARGET_TRACE: [u8; 16] = [0x11; 16];

struct FixedClock;
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

/// A backend wrapper that records every GET's key and range, including a GET
/// the wrapped backend then fails.
struct RecordingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    gets: Mutex<Vec<(String, GetRange)>>,
}

impl RecordingStore {
    /// The ranges GET'd on `key` since the last `clear`, in issue order.
    fn on(&self, key: &str) -> Vec<GetRange> {
        self.gets
            .lock()
            .expect("get log")
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, range)| *range)
            .collect()
    }

    fn clear(&self) {
        self.gets.lock().expect("get log").clear();
    }
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

/// A `tracing` writer that appends every emitted byte to a shared buffer.
#[derive(Clone)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl io::Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedLog {
    type Writer = CapturedLog;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A distinct trace per (object, record), never equal to [`TARGET_TRACE`].
fn decoy_trace(object: usize, record: usize) -> [u8; 16] {
    let seed = (object * RECORDS + record) as u64;
    let mut t = [0u8; 16];
    t.copy_from_slice(&blake3::hash(&seed.to_le_bytes()).as_bytes()[..16]);
    t[0] |= 0x80;
    t
}

/// Record `i` of object `object`. Every block carries `status_code` 400 and
/// 600 (even and odd records); object 0 overrides records 50 and 85 (blocks 3
/// and 5) to 500, records 50 and 100 (blocks 3 and 6) to region [`REGION`],
/// and record 50's trace to [`TARGET_TRACE`].
fn fixture_record(object: usize, i: usize) -> LogRecord {
    let even = i.is_multiple_of(2);
    let mut code = if even { 400 } else { 600 };
    let mut region = if even { "eu-west-1" } else { "us-east-1" };
    let mut trace = decoy_trace(object, i);
    if object == 0 {
        if i == 50 || i == 85 {
            code = 500;
        }
        if i == 50 || i == 100 {
            region = REGION;
        }
        if i == 50 {
            trace = TARGET_TRACE;
        }
    }
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    // Every object spans the same window, so `ts` prunes none of them.
    let ts = 1_000 + (i * 4 + object) as i64;
    LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: body_of(object, i),
        trace_id: Some(trace),
        span_id: None,
        flags: 0,
        attrs: vec![
            ("status_code".to_string(), AttrValue::I64(code)),
            ("region".to_string(), AttrValue::Str(region.to_string())),
        ],
    }
}

/// `o<object>r<record, three digits>|` and about 6 KiB (compressed) of hex of
/// pseudo-random bytes. The body page of one block is then wider than the
/// fetcher's 64 KiB coalescing floor, so two wanted blocks with one block
/// between them are read by separate GETs and a block the plan does not want
/// is visible as an unread body page. Queries select the six-byte id.
fn body_of(object: usize, i: usize) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&((object * RECORDS + i) as u64).to_le_bytes());
    let mut pad = vec![0u8; 6 * 1024];
    hasher.finalize_xof().fill(&mut pad);
    format!("o{object}r{i:03}|{}", hex(&pad))
}

/// One fixture object's records.
fn fixture_records(object: usize) -> Vec<LogRecord> {
    (0..RECORDS).map(|i| fixture_record(object, i)).collect()
}

/// The KEY_IDX fields every indexed fixture object is written with.
fn indexed_fields() -> Vec<(String, KeyType)> {
    vec![
        ("status_code".to_string(), KeyType::I64),
        ("region".to_string(), KeyType::Str),
        (TRACE_ID_FIELD.to_string(), KeyType::Id16),
    ]
}

fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("status_code", DeclaredType::I64),
        DeclaredColumn::new("region", DeclaredType::Str),
    ]
}

/// The tenant's KEY_IDX declaration: both declared columns and `trace_id`.
fn key_index() -> KeyIndexDeclaration {
    KeyIndexDeclaration {
        columns: vec!["status_code".to_string(), "region".to_string()],
        trace_id: true,
    }
}

/// The logs fetcher every fixture starts from: a 1 KiB tail probe and a
/// request cost low enough that every wanted extent is its own GET.
fn fixture_fetcher(store: Arc<dyn ObjectStoreBackend>) -> LogSegmentFetcher {
    LogSegmentFetcher::new(store)
        .with_suffix_len(SUFFIX_LEN)
        .with_request_cost_bytes(1)
}

#[derive(Clone)]
struct Published {
    key: String,
    bytes: Bytes,
}

/// A tenant's logs over a recording store, served by a SQL router whose logs
/// fetcher probes every object.
struct Fixture {
    recorder: Arc<RecordingStore>,
    app: Router,
    objects: Vec<Published>,
}

impl Fixture {
    fn new(
        inner: Arc<dyn ObjectStoreBackend>,
        declared: Vec<DeclaredColumn>,
        key_index: KeyIndexDeclaration,
    ) -> Self {
        Fixture::with_fetcher(inner, declared, key_index, |store| {
            fixture_fetcher(store).with_block_range_threshold(0)
        })
    }

    /// A fixture whose logs fetcher `fetcher` builds over the recording store.
    fn with_fetcher(
        inner: Arc<dyn ObjectStoreBackend>,
        declared: Vec<DeclaredColumn>,
        key_index: KeyIndexDeclaration,
        fetcher: impl FnOnce(Arc<dyn ObjectStoreBackend>) -> LogSegmentFetcher,
    ) -> Self {
        let recorder = Arc::new(RecordingStore {
            inner,
            gets: Mutex::new(Vec::new()),
        });
        let store: Arc<dyn ObjectStoreBackend> = recorder.clone();
        let catalog =
            Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
        let logs = fetcher(store.clone());
        let executor = SqlExecutor::new(
            catalog,
            SegmentFetcher::new(store.clone()),
            logs,
            ravel_sql::SpanSegmentFetcher::new(store.clone()),
            SqlConfig::default(),
            1 << 30,
        )
        .with_declared_column_source(Arc::new(
            StaticDeclaredColumns::new(declared).with_key_index(key_index),
        ));
        let mut tokens = HashMap::new();
        tokens.insert(TOKEN.to_string(), tenant());
        let app = router(SqlState {
            executor: Arc::new(executor),
            tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
            store,
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
        Fixture {
            recorder,
            app,
            objects: Vec::new(),
        }
    }

    fn memory() -> Self {
        Fixture::new(Arc::new(MemoryStore::new()), declared(), key_index())
    }

    /// Writes `records` as one RLOG object with KEY_IDX over `key_fields`
    /// (no KEY_IDX section when empty), lets `mutate` rewrite the finished
    /// bytes, and publishes the object and its commit record.
    async fn publish(
        &mut self,
        records: Vec<LogRecord>,
        key_fields: Vec<(String, KeyType)>,
        mutate: impl FnOnce(&mut Vec<u8>),
    ) {
        let seq = self.objects.len();
        let tenant_hash = tenant().hash();
        let writer_id = Uuid::from_u128(2_762_000 + seq as u128);
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
        );
        if !key_fields.is_empty() {
            writer = writer.with_key_index_fields(key_fields);
        }
        let min = records.iter().map(|r| r.ts_ns).min().expect("records");
        let max = records.iter().map(|r| r.ts_ns).max().expect("records");
        let count = records.len() as u64;
        for r in records {
            writer.push(r).expect("push log record");
        }
        let mut bytes = writer.finish().expect("finish rlog object");
        mutate(&mut bytes);
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: seq as u64 + 1,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: count,
            series_count: 1,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            min_ingest_ts_ns: min,
            max_ingest_ts_ns: max,
            segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
            created_unix_ns: 10 + seq as i64,
            ingest_hour_bucket: 0,
        })
        .expect("valid log commit record");
        let key = keys::reconstruct_data_key(&rec).expect("data key");
        let bytes = Bytes::from(bytes);
        self.recorder
            .put(&key, bytes.clone(), PutOptions::default())
            .await
            .expect("put log data object");
        publish::publish(self.recorder.as_ref(), &rec, &RetryPolicy::default())
            .await
            .expect("publish log commit");
        self.objects.push(Published { key, bytes });
    }

    /// The three fixture objects, every one indexed over [`indexed_fields`].
    async fn publish_fixture(&mut self) {
        for object in 0..3 {
            self.publish(fixture_records(object), indexed_fields(), |_| {})
                .await;
        }
    }

    /// Runs `sql` with the GET log cleared first.
    async fn post(&self, sql: &str) -> (StatusCode, Value) {
        self.recorder.clear();
        post(&self.app, sql).await
    }

    async fn run(&self, sql: &str) -> Value {
        let (status, value) = self.post(sql).await;
        assert_eq!(status, StatusCode::OK, "{sql}: {value}");
        assert_eq!(value["status"], "success", "{sql}: {value}");
        value
    }

    /// Object `n`'s GETs during the last query, in issue order.
    fn gets(&self, n: usize) -> Vec<GetRange> {
        self.recorder.on(&self.objects[n].key)
    }
}

fn tenant() -> TenantId {
    TenantId::new("acme".to_string())
}

async fn post(app: &Router, query: &str) -> (StatusCode, Value) {
    let payload = serde_json::json!({
        "query": query,
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
        .expect("build request");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot is infallible");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value: Value = serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!(
            "body is not JSON ({e}): {}",
            String::from_utf8_lossy(&bytes)
        )
    });
    (status, value)
}

/// The first column of every row, as strings, in row order.
fn first_column(value: &Value) -> Vec<String> {
    value["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("no rows in {value}"))
        .iter()
        .map(|row| row[0].as_str().expect("string cell").to_string())
        .collect()
}

/// Field `field` of the `stats.phases` entry for phase `name`.
fn phase(value: &Value, name: &str, field: &str) -> u64 {
    value["stats"]["phases"]
        .as_array()
        .expect("phases")
        .iter()
        .find(|phase| phase["phase"] == name)
        .unwrap_or_else(|| panic!("no {name} phase in {value}"))[field]
        .as_u64()
        .unwrap_or_else(|| panic!("{name}.{field} is a u64 in {value}"))
}

/// Field `field` of `stats.pruning`.
fn pruning(value: &Value, field: &str) -> u64 {
    value["stats"]["pruning"][field]
        .as_u64()
        .unwrap_or_else(|| panic!("stats.pruning.{field} is a u64 in {value}"))
}

/// The object-absolute bytes `get` covers on an object of `size` bytes.
fn span(get: &GetRange, size: u64) -> Range<u64> {
    match *get {
        GetRange::Full => 0..size,
        GetRange::Range(start, end) => start..end,
        GetRange::Suffix(n) => size.saturating_sub(n)..size,
    }
}

fn wire_bytes(gets: &[GetRange], size: u64) -> u64 {
    gets.iter()
        .map(|g| {
            let r = span(g, size);
            r.end - r.start
        })
        .sum()
}

/// Whether any GET in `gets` overlaps `range`.
fn touches(gets: &[GetRange], size: u64, range: &Range<u64>) -> bool {
    gets.iter().any(|g| {
        let r = span(g, size);
        r.start < range.end && range.start < r.end
    })
}

/// The object-absolute byte range of section `k` in `bytes`.
fn section(bytes: &Bytes, k: u32) -> Range<u64> {
    let footer = ravel_logseg::footer::open(bytes).expect("footer");
    let desc = footer.section(k).expect("section present");
    desc.offset..desc.offset + desc.len
}

/// The KEY_IDX reads a probe for (`field`, `key_type`, `key8`) issues on
/// `bytes`, recomputed from the format crate, and the blocks it names (`None`
/// when the probe declines, `Some(empty)` when the key is absent).
fn expected_key_reads(
    bytes: &Bytes,
    field: &str,
    key_type: KeyType,
    key8: &[u8; 8],
) -> (Vec<GetRange>, Option<Vec<u32>>) {
    let footer = ravel_logseg::footer::open(bytes).expect("footer");
    let mut reads = Vec::new();
    let answer = probe_ranged(&footer, field, key_type, key8, |r: Range<u64>| {
        reads.push(GetRange::Range(r.start, r.end));
        Ok::<_, LogSegError>(bytes.slice(r.start as usize..r.end as usize))
    })
    .expect("probe");
    (reads, answer)
}

/// Object `n`'s GETs during the last query that lie inside its KEY_IDX
/// section (none when it has no such section).
fn key_idx_gets(fx: &Fixture, n: usize) -> Vec<GetRange> {
    let bytes = &fx.objects[n].bytes;
    let footer = ravel_logseg::footer::open(bytes).expect("footer");
    let Some(desc) = footer.section(kind::KEY_IDX) else {
        return Vec::new();
    };
    let size = bytes.len() as u64;
    fx.gets(n)
        .into_iter()
        .filter(|g| {
            let r = span(g, size);
            desc.offset <= r.start && r.end <= desc.offset + desc.len
        })
        .collect()
}

/// Each block's body page as an object-absolute byte range, by block id: the
/// block's widest page, since the body carries the pad.
fn body_pages(bytes: &Bytes) -> Vec<Range<u64>> {
    let dirs = RlogReader::decode_directories(bytes.as_ref(), &RlogConfig::default())
        .expect("decode directories");
    let base = section(bytes, kind::BLOCKS).start;
    (0..BLOCKS as u32)
        .map(|block| {
            let page = dirs
                .page_dir()
                .block_pages(block)
                .expect("block pages")
                .into_iter()
                .max_by_key(|p| p.desc.len)
                .expect("a page");
            base + page.offset..base + page.offset + page.desc.len
        })
        .collect()
}

/// One exact-key lookup against the three-object fixture.
struct Lookup<'a> {
    sql: &'a str,
    field: &'a str,
    key_type: KeyType,
    key8: [u8; 8],
    rows: &'a [&'a str],
    /// The blocks of object 0 holding the key; objects 1 and 2 hold none.
    blocks: &'a [u32],
}

/// Asserts `lookup` returns its rows having read exactly the KEY_IDX bytes
/// the format names, no byte past KEY_IDX of objects 1 and 2, and only the
/// body pages of `lookup.blocks` in object 0, with the stats reporting each.
async fn assert_reads_only_the_indexed_blocks(fx: &Fixture, lookup: &Lookup<'_>) {
    let sql = lookup.sql;
    let v = fx.run(sql).await;
    assert_eq!(first_column(&v), lookup.rows, "{sql}");

    let mut key_requests = 0u64;
    let mut key_bytes = 0u64;
    let mut holding_key_reads = Vec::new();
    for (n, object) in fx.objects.iter().enumerate() {
        let size = object.bytes.len() as u64;
        let key_idx = section(&object.bytes, kind::KEY_IDX);
        assert!(
            key_idx.end <= size - SUFFIX_LEN,
            "object {n}: KEY_IDX {key_idx:?} is inside the {SUFFIX_LEN}-byte tail probe of {size}"
        );
        let (reads, answer) =
            expected_key_reads(&object.bytes, lookup.field, lookup.key_type, &lookup.key8);
        let want: &[u32] = if n == 0 { lookup.blocks } else { &[] };
        assert_eq!(answer.as_deref(), Some(want), "object {n}: fixture probe");
        key_requests += reads.len() as u64;
        key_bytes += wire_bytes(&reads, size);
        let got = fx.gets(n);
        let in_key_idx: Vec<GetRange> = got
            .iter()
            .copied()
            .filter(|g| {
                let r = span(g, size);
                key_idx.start <= r.start && r.end <= key_idx.end
            })
            .collect();
        assert_eq!(in_key_idx, reads, "{sql}: object {n} KEY_IDX reads");
        if n == 0 {
            holding_key_reads = reads;
        } else {
            // The tail probe, then KEY_IDX, then nothing: no directory, no page.
            let mut want_gets = vec![GetRange::Suffix(SUFFIX_LEN)];
            want_gets.extend(reads);
            assert_eq!(got, want_gets, "{sql}: object {n} read past KEY_IDX");
        }
    }
    assert_eq!(
        phase(&v, "keyIndex", "s3GetRequests"),
        key_requests,
        "{sql}"
    );
    assert_eq!(phase(&v, "keyIndex", "s3GetWireBytes"), key_bytes, "{sql}");

    // Object 0: the tail probe, its KEY_IDX reads, SKIP_IDX with PAGE_DIR and
    // the front directories (plan), then the scan.
    let holding = &fx.objects[0];
    let size = holding.bytes.len() as u64;
    let got = fx.gets(0);
    let blocks_start = section(&holding.bytes, kind::BLOCKS).start;
    let skip_idx = section(&holding.bytes, kind::SKIP_IDX);
    let page_dir = section(&holding.bytes, kind::PAGE_DIR);
    assert_eq!(skip_idx.end, page_dir.start, "PAGE_DIR follows SKIP_IDX");
    let mut plan = vec![GetRange::Suffix(SUFFIX_LEN)];
    plan.extend(holding_key_reads.iter().copied());
    plan.push(GetRange::Range(skip_idx.start, page_dir.end));
    plan.push(GetRange::Range(0, blocks_start));
    assert_eq!(
        &got[..plan.len()],
        plan.as_slice(),
        "{sql}: object 0 plan reads"
    );
    let scan = &got[plan.len()..];
    for (block, page) in body_pages(&holding.bytes).iter().enumerate() {
        assert_eq!(
            touches(scan, size, page),
            lookup.blocks.contains(&(block as u32)),
            "{sql}: block {block}'s body page {page:?} against the scan reads {scan:?}"
        );
    }
    // One GET per wanted body page, plus four that do not depend on which
    // blocks are wanted: PAGE_DIR, BLOOM and two runs of narrow column pages.
    assert_eq!(scan.len(), 4 + lookup.blocks.len(), "{sql}: {scan:?}");
    assert_eq!(
        phase(&v, "scan", "s3GetRequests"),
        scan.len() as u64,
        "{sql}"
    );
    assert_eq!(
        phase(&v, "scan", "s3GetWireBytes"),
        wire_bytes(scan, size),
        "{sql}"
    );
    assert_eq!(
        phase(&v, "plan", "s3GetRequests"),
        fx.objects.len() as u64 + 2,
        "{sql}: one tail probe per object, SKIP_IDX and the front of object 0"
    );

    assert_eq!(pruning(&v, "segments"), 3, "{sql}");
    assert_eq!(pruning(&v, "segmentsPrunedByStats"), 0, "{sql}");
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 2, "{sql}");
    assert_eq!(pruning(&v, "blocksTotal"), BLOCKS as u64, "{sql}");
    assert_eq!(
        pruning(&v, "blocksPrunedByKeyIndex"),
        (BLOCKS - lookup.blocks.len()) as u64,
        "{sql}"
    );
    assert_eq!(
        pruning(&v, "blocksScanned"),
        lookup.blocks.len() as u64,
        "{sql}"
    );
}

#[tokio::test]
async fn declared_i64_equality_reads_only_the_indexed_blocks() {
    let mut fx = Fixture::memory();
    fx.publish_fixture().await;
    for sql in [
        "SELECT substr(body, 1, 6) AS id FROM logs WHERE status_code = 500 ORDER BY id",
        "SELECT substr(body, 1, 6) AS id FROM logs WHERE 500 = status_code ORDER BY id",
    ] {
        let lookup = Lookup {
            sql,
            field: "status_code",
            key_type: KeyType::I64,
            key8: key8_i64(500),
            rows: &["o0r050", "o0r085"],
            blocks: &[3, 5],
        };
        assert_reads_only_the_indexed_blocks(&fx, &lookup).await;
    }
}

#[tokio::test]
async fn declared_str_equality_reads_only_the_indexed_blocks() {
    let mut fx = Fixture::memory();
    fx.publish_fixture().await;
    for sql in [
        "SELECT substr(body, 1, 6) AS id FROM logs WHERE region = 'ap-south-1' ORDER BY id",
        "SELECT substr(body, 1, 6) AS id FROM logs WHERE 'ap-south-1' = region ORDER BY id",
    ] {
        let lookup = Lookup {
            sql,
            field: "region",
            key_type: KeyType::Str,
            key8: key8_str(REGION),
            rows: &["o0r050", "o0r100"],
            blocks: &[3, 6],
        };
        assert_reads_only_the_indexed_blocks(&fx, &lookup).await;
    }
}

#[tokio::test]
async fn trace_id_equality_reads_only_the_indexed_blocks() {
    let mut fx = Fixture::memory();
    fx.publish_fixture().await;
    let target = hex(&TARGET_TRACE);
    for sql in [
        format!(
            "SELECT substr(body, 1, 6) AS id FROM logs WHERE trace_id = '{target}' ORDER BY id"
        ),
        format!(
            "SELECT substr(body, 1, 6) AS id FROM logs WHERE X'{target}' = trace_id ORDER BY id"
        ),
    ] {
        let lookup = Lookup {
            sql: &sql,
            field: TRACE_ID_FIELD,
            key_type: KeyType::Id16,
            key8: key8_id16(&TARGET_TRACE),
            rows: &["o0r050"],
            blocks: &[3],
        };
        assert_reads_only_the_indexed_blocks(&fx, &lookup).await;
    }
}

#[tokio::test]
async fn and_intersects_the_keys_and_or_does_not_prune() {
    let mut fx = Fixture::memory();
    fx.publish_fixture().await;

    let sql = "SELECT substr(body, 1, 6) AS id FROM logs \
               WHERE status_code = 500 AND region = 'ap-south-1' ORDER BY id";
    let v = fx.run(sql).await;
    assert_eq!(first_column(&v), ["o0r050"]);
    // Object 0 holds 500 in blocks 3 and 5 and the region in 3 and 6: only
    // block 3 holds both, and only its body page is read.
    let holding = &fx.objects[0];
    let got = fx.gets(0);
    let size = holding.bytes.len() as u64;
    for (block, page) in body_pages(&holding.bytes).iter().enumerate() {
        assert_eq!(
            touches(&got, size, page),
            block == 3,
            "block {block}: {got:?}"
        );
    }
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 2);
    assert_eq!(pruning(&v, "blocksPrunedByKeyIndex"), 7);
    assert_eq!(pruning(&v, "blocksScanned"), 1);
    // Every KEY_IDX read is one the two probes need on that object; objects 1
    // and 2 are dropped on the first key, so they read nothing past KEY_IDX.
    let mut key_requests = 0u64;
    for (n, object) in fx.objects.iter().enumerate() {
        let size = object.bytes.len() as u64;
        let key_idx = section(&object.bytes, kind::KEY_IDX);
        let (status, _) =
            expected_key_reads(&object.bytes, "status_code", KeyType::I64, &key8_i64(500));
        let (region, _) =
            expected_key_reads(&object.bytes, "region", KeyType::Str, &key8_str(REGION));
        let got = fx.gets(n);
        let in_key_idx: Vec<GetRange> = got
            .iter()
            .copied()
            .filter(|g| {
                let r = span(g, size);
                key_idx.start <= r.start && r.end <= key_idx.end
            })
            .collect();
        assert!(
            in_key_idx
                .iter()
                .all(|g| status.contains(g) || region.contains(g)),
            "object {n}: {in_key_idx:?} against {status:?} and {region:?}"
        );
        if n != 0 {
            assert_eq!(got[0], GetRange::Suffix(SUFFIX_LEN));
            assert_eq!(&got[1..], in_key_idx.as_slice(), "object {n}");
        }
        key_requests += in_key_idx.len() as u64;
    }
    assert_eq!(phase(&v, "keyIndex", "s3GetRequests"), key_requests);

    let sql = "SELECT substr(body, 1, 6) AS id FROM logs \
               WHERE status_code = 500 OR region = 'ap-south-1' ORDER BY id";
    let v = fx.run(sql).await;
    assert_eq!(first_column(&v), ["o0r050", "o0r085", "o0r100"]);
    assert_eq!(phase(&v, "keyIndex", "s3GetRequests"), 0);
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 0);
    assert_eq!(pruning(&v, "blocksPrunedByKeyIndex"), 0);
    assert_eq!(pruning(&v, "blocksScanned"), 3 * BLOCKS as u64);
}

/// Two keys both present in object 0 but in no common block: the empty
/// intersection drops the object as an absent key does, and it counts in
/// `segmentsPrunedByKeyIndex`, not `blocksPrunedByKeyIndex`.
#[tokio::test]
async fn an_empty_and_intersection_counts_as_a_pruned_segment() {
    let mut fx = Fixture::memory();
    fx.publish_fixture().await;

    // 500 sits in blocks 3 and 5 of object 0; record 100's trace in block 6.
    let trace = hex(&decoy_trace(0, 100));
    let sql = format!(
        "SELECT substr(body, 1, 6) AS id FROM logs \
         WHERE status_code = 500 AND trace_id = X'{trace}' ORDER BY id"
    );
    let v = fx.run(&sql).await;
    assert!(first_column(&v).is_empty(), "{v}");
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 3);
    assert_eq!(pruning(&v, "blocksPrunedByKeyIndex"), 0);
    assert_eq!(pruning(&v, "blocksScanned"), 0);
    let holding = &fx.objects[0];
    let size = holding.bytes.len() as u64;
    let got = fx.gets(0);
    for page in body_pages(&holding.bytes) {
        assert!(!touches(&got, size, &page), "{got:?}");
    }
}

#[tokio::test]
async fn an_unindexed_field_an_object_without_key_idx_and_an_undeclared_column_prune_nothing() {
    let sql = "SELECT substr(body, 1, 6) AS id FROM logs WHERE status_code = 500 ORDER BY id";

    // KEY_IDX present but not over `status_code`: the probe reads the
    // section's prefix once per object, finds no entry for the field, and
    // declines.
    let mut fx = Fixture::memory();
    for object in 0..3 {
        fx.publish(
            fixture_records(object),
            vec![(TRACE_ID_FIELD.to_string(), KeyType::Id16)],
            |_| {},
        )
        .await;
    }
    let v = fx.run(sql).await;
    assert_eq!(first_column(&v), ["o0r050", "o0r085"]);
    for (n, object) in fx.objects.iter().enumerate() {
        let (reads, answer) =
            expected_key_reads(&object.bytes, "status_code", KeyType::I64, &key8_i64(500));
        assert_eq!(answer, None, "object {n}");
        assert_eq!(reads.len(), 1, "object {n}");
        assert!(fx.gets(n).contains(&reads[0]), "object {n}");
    }
    assert_eq!(phase(&v, "keyIndex", "s3GetRequests"), 3);
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 0);
    assert_eq!(pruning(&v, "blocksPrunedByKeyIndex"), 0);
    assert_eq!(pruning(&v, "blocksScanned"), 3 * BLOCKS as u64);

    // Object 0 written before any field was indexed (no KEY_IDX section):
    // nothing to probe there, while objects 1 and 2 are still dropped.
    let mut fx = Fixture::memory();
    fx.publish(fixture_records(0), Vec::new(), |_| {}).await;
    for object in 1..3 {
        fx.publish(fixture_records(object), indexed_fields(), |_| {})
            .await;
    }
    let footer = ravel_logseg::footer::open(&fx.objects[0].bytes).expect("footer");
    assert!(footer.section(kind::KEY_IDX).is_none());
    let v = fx.run(sql).await;
    assert_eq!(first_column(&v), ["o0r050", "o0r085"]);
    let mut key_requests = 0;
    for object in &fx.objects[1..] {
        let (reads, answer) =
            expected_key_reads(&object.bytes, "status_code", KeyType::I64, &key8_i64(500));
        assert_eq!(answer, Some(Vec::new()));
        key_requests += reads.len() as u64;
    }
    assert_eq!(key_requests, 2);
    assert_eq!(phase(&v, "keyIndex", "s3GetRequests"), key_requests);
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 2);
    assert_eq!(pruning(&v, "blocksPrunedByKeyIndex"), 0);
    assert_eq!(pruning(&v, "blocksScanned"), BLOCKS as u64);

    // `region` not declared: an equality on the attribute map carries no
    // key, so no object is probed although every one indexes the field.
    let mut fx = Fixture::new(
        Arc::new(MemoryStore::new()),
        vec![DeclaredColumn::new("status_code", DeclaredType::I64)],
        key_index(),
    );
    fx.publish_fixture().await;
    let v = fx
        .run(
            "SELECT substr(body, 1, 6) AS id FROM logs \
             WHERE attrs['region'] = 'ap-south-1' ORDER BY id",
        )
        .await;
    assert_eq!(first_column(&v), ["o0r050", "o0r100"]);
    assert_eq!(phase(&v, "keyIndex", "s3GetRequests"), 0);
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 0);
    assert_eq!(pruning(&v, "blocksPrunedByKeyIndex"), 0);
}

/// A declared `I64` column compares only values stored as `I64`: a row that
/// stored the field as the string "500" is NULL in `status_code`, so
/// `status_code = 500` cannot match it, and KEY_IDX, which indexes only
/// values of the declared type, dropping the object loses no row.
#[tokio::test]
async fn a_value_stored_under_another_type_matches_neither_the_probe_nor_the_filter() {
    let mut fx = Fixture::memory();
    let mut records = fixture_records(1);
    records[50].attrs[0] = ("status_code".to_string(), AttrValue::Str("500".to_string()));
    fx.publish(records, indexed_fields(), |_| {}).await;
    let object = &fx.objects[0];
    let (reads, answer) =
        expected_key_reads(&object.bytes, "status_code", KeyType::I64, &key8_i64(500));
    assert_eq!(answer, Some(Vec::new()), "KEY_IDX holds no I64 500");

    // The row is there, under the attribute map's rendering of the string.
    let v = fx
        .run("SELECT substr(body, 1, 6) AS id FROM logs WHERE attrs['status_code'] = '500'")
        .await;
    assert_eq!(first_column(&v), ["o1r050"]);

    // The probe drops the object...
    let v = fx
        .run("SELECT substr(body, 1, 6) AS id FROM logs WHERE status_code = 500")
        .await;
    assert_eq!(first_column(&v), Vec::<String>::new());
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 1);
    assert_eq!(phase(&v, "keyIndex", "s3GetRequests"), reads.len() as u64);

    // ...and the exact filter, with no key to probe, agrees.
    let v = fx
        .run("SELECT substr(body, 1, 6) AS id FROM logs WHERE status_code + 0 = 500")
        .await;
    assert_eq!(first_column(&v), Vec::<String>::new());
    assert_eq!(phase(&v, "keyIndex", "s3GetRequests"), 0);
    assert_eq!(pruning(&v, "blocksScanned"), BLOCKS as u64);
}

#[tokio::test]
async fn a_corrupt_bucket_falls_back_to_the_unpruned_scan_with_identical_rows() {
    let sql = "SELECT substr(body, 1, 6) AS id FROM logs WHERE status_code = 500 ORDER BY id";
    let mut clean = Fixture::memory();
    clean.publish_fixture().await;
    let want = first_column(&clean.run(sql).await);
    assert_eq!(want, ["o0r050", "o0r085"]);

    let (reads, _) = expected_key_reads(
        &clean.objects[0].bytes,
        "status_code",
        KeyType::I64,
        &key8_i64(500),
    );
    let bucket = span(&reads[1], clean.objects[0].bytes.len() as u64);

    let mut fx = Fixture::memory();
    let flip = bucket.clone();
    fx.publish(fixture_records(0), indexed_fields(), move |bytes| {
        for b in &mut bytes[flip.start as usize..flip.end as usize] {
            *b ^= 0xff;
        }
    })
    .await;
    for object in 1..3 {
        fx.publish(fixture_records(object), indexed_fields(), |_| {})
            .await;
    }

    let log = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(CapturedLog(log.clone()))
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .finish();
    let v = {
        let _guard = tracing::subscriber::set_default(subscriber);
        fx.run(sql).await
    };
    assert_eq!(first_column(&v), want);
    assert!(
        fx.gets(0)
            .contains(&GetRange::Range(bucket.start, bucket.end)),
        "the bucket was read: {:?}",
        fx.gets(0)
    );
    // Object 0 is scanned as if it had no KEY_IDX; 1 and 2 are still dropped.
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 2);
    assert_eq!(pruning(&v, "blocksPrunedByKeyIndex"), 0);
    assert_eq!(pruning(&v, "blocksScanned"), BLOCKS as u64);
    let log = String::from_utf8(log.lock().expect("log").clone()).expect("utf-8 log");
    let warning = log
        .lines()
        .find(|line| line.contains("KEY_IDX probe failed"))
        .unwrap_or_else(|| panic!("no KEY_IDX warning in {log:?}"));
    assert!(
        warning.contains(&fx.objects[0].key),
        "the warning names the object: {warning}"
    );
}

#[tokio::test]
async fn a_failed_bucket_read_is_a_typed_error() {
    // The data keys are deterministic: learn object 0's from a plain store.
    let mut scratch = Fixture::memory();
    scratch.publish_fixture().await;
    let key = scratch.objects[0].key.clone();
    let (reads, _) = expected_key_reads(
        &scratch.objects[0].bytes,
        "status_code",
        KeyType::I64,
        &key8_i64(500),
    );
    assert_eq!(reads.len(), 2, "a prefix read and a bucket read");

    // Object 0's GETs are the tail probe, the KEY_IDX prefix, then the bucket.
    let faulty = Arc::new(FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Permanent("bucket read refused".to_string()),
            )
            .with_key_contains(key.clone())
            .with_occurrence(Occurrence::Nth(3)),
        ),
    ));
    let mut fx = Fixture::new(faulty.clone(), declared(), key_index());
    fx.publish_fixture().await;
    assert_eq!(fx.objects[0].key, key);

    let (status, body) = fx
        .post("SELECT substr(body, 1, 6) AS id FROM logs WHERE status_code = 500")
        .await;
    assert_eq!(faulty.fault_count(Op::Get, FaultKind::Permanent), 1);
    let got = fx.gets(0);
    assert_eq!(got.get(2), Some(&reads[1]), "the faulted GET is the bucket");
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["status"], "error", "{body}");
    assert_eq!(body["errorType"], "unavailable", "{body}");
}

/// Record 50 of `object` with `region` carried only by its scope, behind a
/// resource entry holding a list, which the reader drops: the row reads
/// [`REGION`].
fn scope_region_record(object: usize) -> LogRecord {
    let mut record = fixture_record(object, 50);
    let resource = vec![
        (
            "region".to_string(),
            AttrValue::List(vec![AttrValue::Str("zz".to_string())]),
        ),
        (
            "service.name".to_string(),
            AttrValue::Str("api".to_string()),
        ),
    ];
    let scope = vec![("region".to_string(), AttrValue::Str(REGION.to_string()))];
    record.stream_id = log_stream_id(&resource, "scope", "1.0", &scope);
    record.stream_attrs = stream_attrs_bytes(&resource, "scope", "1.0", &scope);
    record.attrs.retain(|(name, _)| name != "region");
    record
}

/// A version 1 KEY_IDX section was keyed by an earlier rule than the reader's,
/// so the probe declines on it: the row whose `region` only the reader's rule
/// resolves is returned with nothing pruned by KEY_IDX. A version 2 section
/// over the same records keys it and prunes.
#[tokio::test]
async fn a_version_1_key_idx_section_declines_and_the_row_is_returned() {
    let sql = "SELECT substr(body, 1, 6) AS id FROM logs WHERE region = 'ap-south-1' ORDER BY id";
    let records = || {
        let mut records = fixture_records(1);
        records[50] = scope_region_record(1);
        records
    };

    let mut fx = Fixture::memory();
    fx.publish(records(), indexed_fields(), |_| {}).await;
    let (reads, answer) = expected_key_reads(
        &fx.objects[0].bytes,
        "region",
        KeyType::Str,
        &key8_str(REGION),
    );
    assert_eq!(
        answer.map(|b| b.len()),
        Some(1),
        "version 2 keys the scope's value"
    );
    let v = fx.run(sql).await;
    assert_eq!(first_column(&v), ["o1r050"]);
    assert_eq!(key_idx_gets(&fx, 0), reads);
    assert_eq!(phase(&v, "keyIndex", "s3GetRequests"), reads.len() as u64);
    assert_eq!(pruning(&v, "blocksPrunedByKeyIndex"), BLOCKS as u64 - 1);
    assert_eq!(pruning(&v, "blocksScanned"), 1);

    // The earlier writer let the resource's list claim `region` and keyed
    // nothing for that record.
    let mut fx = Fixture::memory();
    fx.publish(records(), indexed_fields(), |bytes| {
        *bytes = rewrite_key_index(bytes, KEY_IDX_VERSION_1, |field, _, entries| {
            if field == "region" {
                entries
                    .into_iter()
                    .filter(|(key, _)| *key != key8_str(REGION))
                    .collect()
            } else {
                entries
            }
        })
        .expect("rewrite as version 1");
    })
    .await;
    let (reads, answer) = expected_key_reads(
        &fx.objects[0].bytes,
        "region",
        KeyType::Str,
        &key8_str(REGION),
    );
    assert_eq!(answer, None, "a version 1 section declines");
    assert_eq!(reads.len(), 1, "the prefix, then nothing");
    let v = fx.run(sql).await;
    assert_eq!(first_column(&v), ["o1r050"]);
    assert_eq!(key_idx_gets(&fx, 0), reads);
    assert_eq!(phase(&v, "keyIndex", "s3GetRequests"), 1);
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 0);
    assert_eq!(pruning(&v, "blocksPrunedByKeyIndex"), 0);
}

/// An arm whose field the object does not index declines, and a conjunction
/// prunes on its other arms alone, whichever side of the AND it is on.
#[tokio::test]
async fn an_and_prunes_on_its_present_key_and_ignores_a_declined_one() {
    let mut fx = Fixture::memory();
    for object in 0..3 {
        fx.publish(
            fixture_records(object),
            vec![
                ("region".to_string(), KeyType::Str),
                (TRACE_ID_FIELD.to_string(), KeyType::Id16),
            ],
            |_| {},
        )
        .await;
    }
    let mut region_requests = 0u64;
    for (n, object) in fx.objects.iter().enumerate() {
        let (_, status) =
            expected_key_reads(&object.bytes, "status_code", KeyType::I64, &key8_i64(500));
        assert_eq!(status, None, "object {n}: status_code is not indexed");
        let (reads, _) =
            expected_key_reads(&object.bytes, "region", KeyType::Str, &key8_str(REGION));
        region_requests += reads.len() as u64;
    }
    for sql in [
        "SELECT substr(body, 1, 6) AS id FROM logs \
         WHERE region = 'ap-south-1' AND status_code = 500 ORDER BY id",
        "SELECT substr(body, 1, 6) AS id FROM logs \
         WHERE status_code = 500 AND region = 'ap-south-1' ORDER BY id",
    ] {
        let v = fx.run(sql).await;
        assert_eq!(first_column(&v), ["o0r050"], "{sql}");
        // Objects 1 and 2 hold no region key; object 0 keeps the region's
        // blocks 3 and 6.
        assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 2, "{sql}");
        assert_eq!(
            pruning(&v, "blocksPrunedByKeyIndex"),
            BLOCKS as u64 - 2,
            "{sql}"
        );
        assert_eq!(pruning(&v, "blocksScanned"), 2, "{sql}");
        assert_eq!(
            phase(&v, "keyIndex", "s3GetRequests"),
            region_requests,
            "{sql}"
        );
    }
}

/// An object written while `status_code` was declared `Str` indexes it under
/// `Str`. Once the declaration is `I64`, an `I64` probe of that object
/// declines rather than reading the `Str` entries, and the rows are returned.
#[tokio::test]
async fn an_object_indexed_under_the_old_type_declines_after_a_retype() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let str_declared = vec![
        DeclaredColumn::new("status_code", DeclaredType::Str),
        DeclaredColumn::new("region", DeclaredType::Str),
    ];
    let mut before = Fixture::new(Arc::clone(&store), str_declared, key_index());
    for object in 0..3 {
        let mut records = fixture_records(object);
        records[7].attrs[0] = ("status_code".to_string(), AttrValue::Str("500".to_string()));
        before
            .publish(
                records,
                vec![
                    ("status_code".to_string(), KeyType::Str),
                    ("region".to_string(), KeyType::Str),
                ],
                |_| {},
            )
            .await;
    }
    for (n, object) in before.objects.iter().enumerate() {
        let (_, answer) =
            expected_key_reads(&object.bytes, "status_code", KeyType::Str, &key8_str("500"));
        assert_eq!(answer, Some(vec![0]), "object {n}: indexed under Str");
    }
    let v = before
        .run("SELECT substr(body, 1, 6) AS id FROM logs WHERE status_code = '500' ORDER BY id")
        .await;
    assert_eq!(first_column(&v), ["o0r007", "o1r007", "o2r007"]);
    assert_eq!(
        pruning(&v, "blocksPrunedByKeyIndex"),
        3 * (BLOCKS as u64 - 1)
    );

    let mut after = Fixture::new(store, declared(), key_index());
    after.objects = before.objects.clone();
    let v = after
        .run("SELECT substr(body, 1, 6) AS id FROM logs WHERE status_code = 500 ORDER BY id")
        .await;
    assert_eq!(first_column(&v), ["o0r050", "o0r085"]);
    for (n, object) in after.objects.iter().enumerate() {
        let (reads, answer) =
            expected_key_reads(&object.bytes, "status_code", KeyType::I64, &key8_i64(500));
        assert_eq!(answer, None, "object {n}: no I64 entry to probe");
        assert_eq!(
            key_idx_gets(&after, n),
            reads,
            "object {n}: the prefix only"
        );
    }
    assert_eq!(phase(&v, "keyIndex", "s3GetRequests"), 3);
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 0);
    assert_eq!(pruning(&v, "blocksPrunedByKeyIndex"), 0);
}

/// At or under the fetcher's block-range threshold an object is planned with
/// no KEY_IDX read; one byte above it the probe runs. The threshold defaults
/// to 512 KiB. Each case pairs the object under test with one well above the
/// threshold, so the scan plans both rather than reading a lone small object
/// unplanned.
#[tokio::test]
async fn the_probe_runs_only_above_the_block_range_threshold() {
    let sql = "SELECT substr(body, 1, 6) AS id FROM logs WHERE status_code = 500 ORDER BY id";
    // The first 64 records of object 0 (record 50 holds 500, record 85 is
    // cut), then object 1, which holds no 500.
    let publish = |mut fx: Fixture| async move {
        fx.publish(fixture_records(0)[..64].to_vec(), indexed_fields(), |_| {})
            .await;
        fx.publish(fixture_records(1), indexed_fields(), |_| {})
            .await;
        fx
    };
    let small = publish(Fixture::memory()).await;
    let size = small.objects[0].bytes.len() as u64;
    let large = small.objects[1].bytes.len() as u64;
    let (small_reads, _) = expected_key_reads(
        &small.objects[0].bytes,
        "status_code",
        KeyType::I64,
        &key8_i64(500),
    );
    let (large_reads, _) = expected_key_reads(
        &small.objects[1].bytes,
        "status_code",
        KeyType::I64,
        &key8_i64(500),
    );
    assert_eq!(small_reads.len(), 2);
    assert!(
        size < 512 * 1024 && 512 * 1024 < large,
        "{size} and {large}"
    );

    let fetcher = fixture_fetcher(Arc::new(MemoryStore::new()));
    assert_eq!(fetcher.plan_whole_object_bound(), 512 * 1024);
    for (name, bound, probed) in [
        ("at the bound", Some(size), false),
        ("one byte above it", Some(size - 1), true),
        ("under the default bound", None, false),
    ] {
        let fx = publish(Fixture::with_fetcher(
            Arc::new(MemoryStore::new()),
            declared(),
            key_index(),
            move |store| match bound {
                Some(bound) => fixture_fetcher(store).with_block_range_threshold(bound),
                None => fixture_fetcher(store),
            },
        ))
        .await;
        let v = fx.run(sql).await;
        assert_eq!(first_column(&v), ["o0r050"], "{name}");
        let want = if probed {
            small_reads.clone()
        } else {
            Vec::new()
        };
        assert_eq!(key_idx_gets(&fx, 0), want, "{name}: {size} bytes");
        assert_eq!(key_idx_gets(&fx, 1), large_reads, "{name}: {large} bytes");
        assert_eq!(
            phase(&v, "keyIndex", "s3GetRequests"),
            (want.len() + large_reads.len()) as u64,
            "{name}"
        );
        assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 1, "{name}");
    }
}

/// Runs `sql` against `object_keys` (KEY_IDX fields per object, empty for
/// none) under the tenant declaration `key_index`, and returns the response
/// and each object's GETs.
async fn run_routed(
    fetcher: fn(Arc<dyn ObjectStoreBackend>) -> LogSegmentFetcher,
    key_index: KeyIndexDeclaration,
    object_keys: &[Vec<(String, KeyType)>],
    sql: &str,
) -> (Value, Vec<Vec<GetRange>>) {
    let mut fx =
        Fixture::with_fetcher(Arc::new(MemoryStore::new()), declared(), key_index, fetcher);
    for (object, keys) in object_keys.iter().enumerate() {
        fx.publish(fixture_records(object), keys.clone(), |_| {})
            .await;
    }
    let v = fx.run(sql).await;
    let gets = (0..fx.objects.len()).map(|n| fx.gets(n)).collect();
    (v, gets)
}

/// The fixture fetcher, ranged above a zero bound.
fn ranged_fetcher(store: Arc<dyn ObjectStoreBackend>) -> LogSegmentFetcher {
    fixture_fetcher(store).with_block_range_threshold(0)
}

/// A probe that prunes nothing hands the object to the route an unkeyed query
/// takes, with the footer probe already read: the GETs are the unkeyed
/// query's, read for read, plus the KEY_IDX prefix when the object has the
/// section. An object with no KEY_IDX costs nothing extra. Per object, the
/// `region` equality takes the plan's fallback, whose read crosses over to one
/// whole-object GET carried to the scan (#835): the tail probe, SKIP_IDX with
/// PAGE_DIR, then the object. The `status_code` equality takes the
/// skip-decidable route (#761), which reads the front directories as well,
/// and its scan, keeping every block, reads the object whole.
#[tokio::test]
async fn a_probe_that_prunes_nothing_reads_what_the_unkeyed_query_reads() {
    let unkeyed = KeyIndexDeclaration::default();
    let none = vec![Vec::new(); 3];
    let trace_only = vec![vec![(TRACE_ID_FIELD.to_string(), KeyType::Id16)]; 3];
    for (sql, rows, full, total) in [
        (
            "SELECT substr(body, 1, 6) AS id FROM logs WHERE region = 'ap-south-1' ORDER BY id",
            ["o0r050", "o0r100"],
            3,
            9,
        ),
        (
            "SELECT substr(body, 1, 6) AS id FROM logs WHERE status_code = 500 ORDER BY id",
            ["o0r050", "o0r085"],
            3,
            12,
        ),
    ] {
        // No KEY_IDX section: the same GETs as with no key at all.
        let (base, base_gets) = run_routed(ranged_fetcher, unkeyed.clone(), &none, sql).await;
        let (keyed, keyed_gets) = run_routed(ranged_fetcher, key_index(), &none, sql).await;
        assert_eq!(first_column(&base), rows, "{sql}");
        assert_eq!(first_column(&keyed), rows, "{sql}");
        assert_eq!(keyed_gets, base_gets, "{sql}: no KEY_IDX");
        for p in ["plan", "scan"] {
            for field in ["s3GetRequests", "s3GetWireBytes"] {
                assert_eq!(
                    phase(&keyed, p, field),
                    phase(&base, p, field),
                    "{sql}: {p}.{field}"
                );
            }
        }
        assert_eq!(phase(&keyed, "keyIndex", "s3GetRequests"), 0, "{sql}");
        let got_total: usize = base_gets.iter().map(Vec::len).sum();
        let got_full = base_gets
            .iter()
            .flatten()
            .filter(|g| **g == GetRange::Full)
            .count();
        assert_eq!((got_full, got_total), (full, total), "{sql}: {base_gets:?}");

        // KEY_IDX over another field: the same GETs plus the section's
        // prefix, read right after the tail probe.
        let (base, base_gets) = run_routed(ranged_fetcher, unkeyed.clone(), &trace_only, sql).await;
        let (keyed, keyed_gets) = run_routed(ranged_fetcher, key_index(), &trace_only, sql).await;
        assert_eq!(first_column(&keyed), first_column(&base), "{sql}");
        for (n, (keyed, base)) in keyed_gets.iter().zip(&base_gets).enumerate() {
            assert_eq!(keyed.len(), base.len() + 1, "{sql}: object {n}");
            assert_eq!(keyed[0], base[0], "{sql}: object {n} tail probe");
            assert_eq!(&keyed[2..], &base[1..], "{sql}: object {n}");
        }
        assert_eq!(phase(&keyed, "keyIndex", "s3GetRequests"), 3, "{sql}");
        assert_eq!(pruning(&keyed, "segmentsPrunedByKeyIndex"), 0, "{sql}");
        assert_eq!(pruning(&keyed, "blocksPrunedByKeyIndex"), 0, "{sql}");
    }
}

/// A declared column the tenant's `key_index` declaration does not name is
/// never probed, even on objects that index it, and `trace_id` is probed only
/// with `index_trace_id` on: those queries read what they read with no
/// declaration at all.
#[tokio::test]
async fn an_unindexed_declared_column_issues_no_key_idx_read() {
    let all = vec![indexed_fields(); 3];
    let region_only = KeyIndexDeclaration {
        columns: vec!["region".to_string()],
        trace_id: false,
    };
    let target = hex(&TARGET_TRACE);
    let trace_sql = format!(
        "SELECT substr(body, 1, 6) AS id FROM logs WHERE trace_id = '{target}' ORDER BY id"
    );
    for (sql, rows) in [
        (
            "SELECT substr(body, 1, 6) AS id FROM logs WHERE status_code = 500 ORDER BY id",
            vec!["o0r050", "o0r085"],
        ),
        (trace_sql.as_str(), vec!["o0r050"]),
    ] {
        let (base, base_gets) =
            run_routed(ranged_fetcher, KeyIndexDeclaration::default(), &all, sql).await;
        let (v, gets) = run_routed(ranged_fetcher, region_only.clone(), &all, sql).await;
        assert_eq!(first_column(&v), rows, "{sql}");
        assert_eq!(phase(&v, "keyIndex", "s3GetRequests"), 0, "{sql}");
        assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 0, "{sql}");
        assert_eq!(gets, base_gets, "{sql}");
        assert_eq!(first_column(&base), rows, "{sql}");
    }
    // The column the declaration names is probed.
    let (v, _) = run_routed(
        ranged_fetcher,
        region_only,
        &all,
        "SELECT substr(body, 1, 6) AS id FROM logs WHERE region = 'ap-south-1' ORDER BY id",
    )
    .await;
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 2);
}
