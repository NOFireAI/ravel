//! A logs equality on a declared key-index column routes the scan through its
//! folded part's key-index leaves once, before the plan pass's per-segment
//! loop (ADR-3692 decisions 4 and 9, issue #3703): a covering part costs two
//! leaf GETs, an object the leaves prove absent is never opened whatever its
//! size, and the holder is read for the leaves' block set with no KEY_IDX
//! read. A segment no leaf covers keeps the tier-1 routes: a version 1 RLOG
//! KEY_IDX section is listed uncovered by the fold, and a leaf that cannot be
//! read, fails its checksum, or is refused its decode reservation leaves its
//! part's segments uncovered, never pruned.
//!
//! The fixture is 24 RLOG objects of eight 16-record blocks, every record
//! carrying a distinct `user_id`; record 50 of each holder holds the target.
//! Objects 1 to 16 are about 274 KB, at or under the 512 KiB block-range
//! threshold and whole-object bound; the rest are about 900 KB, above both.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use ravel_catalog::{
    Catalog, CatalogConfig, CatalogError, DeclaredColumnType, DeclaredTypedColumn,
    KeyIndexLeafLimits, KeyIndexLeafRequest, KeyIndexRebuildOptions, Snapshot, SnapshotParts, StorageLayoutWrite,
    TenantConfig, TenantLifecycleState, set_tenant_config,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::Clock;
use ravel_logseg::footer::kind;
use ravel_logseg::key_index::{KEY_IDX_VERSION_1, KeyType, key8_i64, probe_ranged, rewrite_key_index};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, LogSegError, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::key_index_route::{
    KeyIndexPartOutcome, key_index_leaf_gets_total, key_index_parts_total,
};
use ravel_query::{EngineConfig, LogSegmentFetcher, SegmentFetcher};
use ravel_server::sql::{SqlState, router};
use ravel_sql::{
    DeclaredColumn, DeclaredType, KeyIndexDeclaration, SqlConfig, SqlExecutor,
    StaticDeclaredColumns,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantId, TimeRange};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Late enough that a fold seals ingest hour 0.
const NOW_NS: i64 = 4 * NS_PER_HOUR;
const TOKEN: &str = "acme-token";
const RECORDS_PER_BLOCK: usize = 16;
const BLOCKS: usize = 8;
const RECORDS: usize = RECORDS_PER_BLOCK * BLOCKS;
const OBJECTS: usize = 24;
const TARGET: i64 = 435_090_932_899_640_449;
/// Pad bytes per record of a big object (about 900 KB) and a small one
/// (about 274 KB), hex-encoded into the body at twice the length.
const BIG: usize = 6 * 1024;
const SMALL: usize = 2 * 1024;
/// The fetcher's block-range threshold and projection break-even, so its
/// whole-object bound too.
const BOUND: u64 = 512 * 1024;
const SQL: &str = "SELECT substr(body, 1, 6) AS id FROM logs WHERE user_id = 435090932899640449 \
                   ORDER BY id";

/// The process route totals are shared by every test in this binary.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct FixedClock;
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

/// Records the key and range of each GET.
struct RecordingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    gets: Mutex<Vec<(String, GetRange)>>,
}

impl RecordingStore {
    fn new(inner: Arc<dyn ObjectStoreBackend>) -> Arc<Self> {
        Arc::new(RecordingStore {
            inner,
            gets: Mutex::new(Vec::new()),
        })
    }

    fn of(&self, key: &str) -> Vec<GetRange> {
        self.gets
            .lock()
            .expect("get log")
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, r)| *r)
            .collect()
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

fn tenant() -> TenantId {
    TenantId::new("acme".to_string())
}

/// A distinct i64 per (object, record), never equal to TARGET.
fn decoy(object: usize, i: usize) -> i64 {
    1_000_000 + (object * RECORDS + i) as i64
}

fn record_of(object: usize, i: usize, pad_bytes: usize, holds: bool) -> LogRecord {
    let user = if holds && i == 50 {
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

fn pad_of(object: usize) -> usize {
    if (1..=16).contains(&object) {
        SMALL
    } else {
        BIG
    }
}

struct Published {
    key: String,
    bytes: Bytes,
}

impl Published {
    fn size(&self) -> u64 {
        self.bytes.len() as u64
    }

    /// The tier-1 KEY_IDX reads a probe for `TARGET` issues on this object,
    /// recomputed from the format crate, and whether the section answered.
    fn expected_key_reads(&self) -> (Vec<GetRange>, bool) {
        let footer = ravel_logseg::footer::open(&self.bytes).expect("footer");
        let mut reads = Vec::new();
        let answer = probe_ranged(
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
        (reads, answer.is_some())
    }

    /// The GETs of `gets` that lie inside this object's KEY_IDX section.
    fn key_idx_gets(&self, gets: &[GetRange]) -> Vec<GetRange> {
        let footer = ravel_logseg::footer::open(&self.bytes).expect("footer");
        let desc = footer.section(kind::KEY_IDX).expect("KEY_IDX");
        gets.iter()
            .filter(|g| {
                let r = span(g, self.size());
                desc.offset <= r.start && r.end <= desc.offset + desc.len
            })
            .copied()
            .collect()
    }
}

fn span(get: &GetRange, size: u64) -> Range<u64> {
    match *get {
        GetRange::Full => 0..size,
        GetRange::Range(start, end) => start..end,
        GetRange::Suffix(n) => size.saturating_sub(n)..size,
    }
}

/// The 24 objects published to one memory store and, unless `unfolded`,
/// folded into one part with key-index leaves.
struct Fixture {
    base: Arc<dyn ObjectStoreBackend>,
    objects: Vec<Published>,
}

impl Fixture {
    /// `holders` hold `TARGET` in record 50. Each object in `version_one` has
    /// its KEY_IDX rewritten as a version 1 section, without `TARGET`'s entry:
    /// the earlier writer's rule keyed less than the reader resolves.
    async fn publish(holders: &[usize], version_one: &[usize]) -> Self {
        let base: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant_hash = tenant().hash();
        let mut objects = Vec::new();
        for seq in 0..OBJECTS {
            let writer_id = Uuid::from_u128(3_703_000 + seq as u128);
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
            let records: Vec<LogRecord> = (0..RECORDS)
                .map(|i| record_of(seq, i, pad_of(seq), holders.contains(&seq)))
                .collect();
            let min = records.iter().map(|r| r.ts_ns).min().expect("records");
            let max = records.iter().map(|r| r.ts_ns).max().expect("records");
            for r in records {
                writer.push(r).expect("push");
            }
            let mut bytes = writer.finish().expect("finish");
            if version_one.contains(&seq) {
                bytes = rewrite_key_index(&bytes, KEY_IDX_VERSION_1, |_, _, entries| {
                    entries
                        .into_iter()
                        .filter(|(key, _)| *key != key8_i64(TARGET))
                        .collect()
                })
                .expect("rewrite as version 1");
            }
            let bytes = Bytes::from(bytes);
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
            base.put(&key, bytes.clone(), PutOptions::default())
                .await
                .expect("put");
            publish::publish(base.as_ref(), &rec, &RetryPolicy::default())
                .await
                .expect("publish");
            objects.push(Published { key, bytes });
        }
        for (n, object) in objects.iter().enumerate() {
            assert_eq!(object.size() <= BOUND, (1..=16).contains(&n), "object {n}");
        }
        Fixture { base, objects }
    }

    /// Declares `user_id` an I64 key-index column, which is what makes the
    /// fold build a leaf for it, then folds.
    async fn fold(&self) {
        self.fold_with(leaf_catalog(Arc::clone(&self.base))).await;
    }

    /// Declares `user_id` as the tenant's logs key-index field and folds every
    /// object into one part through `catalog`.
    async fn fold_with(&self, catalog: Catalog) {
        let mut config = TenantConfig {
            typed_attr_columns: Some(vec![DeclaredTypedColumn {
                key: "user_id".to_string(),
                ty: DeclaredColumnType::I64,
            }]),
            ..TenantConfig::new(TenantLifecycleState::Active)
        };
        config
            .set_key_index_columns(
                vec!["user_id".to_string()],
                StorageLayoutWrite::ReadersRolledOut,
            )
            .expect("declare the key index");
        set_tenant_config(self.base.as_ref(), &tenant().hash(), &config, 1)
            .await
            .expect("write tenant config");
        let report = catalog
            .fold(
                &tenant().hash(),
                Signal::Logs,
                Uuid::from_u128(1),
                NOW_NS,
                &[],
                None,
            )
            .await
            .expect("fold");
        assert_eq!(report.entry_count, OBJECTS as u64, "{report:?}");
    }

    /// The catalog's own resolve with its parts.
    async fn resolve(&self) -> (Snapshot, SnapshotParts) {
        let (snapshot, _, parts) = leaf_catalog(Arc::clone(&self.base))
            .resolve_pruned_with_parts(
                &tenant().hash(),
                Signal::Logs,
                TimeRange {
                    start_ns: 0,
                    end_ns: NOW_NS,
                },
                &[],
                NOW_NS,
                None,
                &QueryAccounting::new(),
            )
            .await
            .expect("resolve");
        (snapshot, parts)
    }

    /// The one part's `user_id` leaf key and, per object, its entry ordinal
    /// in that part.
    async fn layout(&self) -> (String, Vec<u64>) {
        let (snapshot, parts) = self.resolve().await;
        assert_eq!(parts.parts.len(), 1, "one part");
        let leaves = &parts.parts[0].key_index;
        assert_eq!(leaves.len(), 1, "one user_id leaf: {leaves:?}");
        assert_eq!(leaves[0].field, "user_id");
        let by_key: HashMap<&str, u64> = snapshot
            .segments
            .iter()
            .zip(&parts.origin)
            .map(|(s, o)| {
                let o = o.expect("every segment is sealed into the part");
                (s.data_object_key.as_str(), u64::from(o.ordinal))
            })
            .collect();
        let ordinals = self
            .objects
            .iter()
            .map(|o| by_key[o.key.as_str()])
            .collect();
        (leaves[0].key.clone(), ordinals)
    }

    /// The entry ordinals the part's leaf lists as uncovered, read by probing
    /// it for `TARGET`.
    async fn uncovered_for_target(&self) -> Option<Vec<u64>> {
        let (_, parts) = self.resolve().await;
        let part = &parts.parts[0];
        let probe = leaf_catalog(Arc::clone(&self.base))
            .probe_key_index_leaf(
                &part.key_index[0],
                &KeyIndexLeafRequest {
                    tenant_hash: tenant().hash(),
                    signal: Signal::Logs,
                    part_blake3: &part.blake3,
                    part_entry_count: part.entry_count,
                    field: "user_id",
                    key_type: KeyType::I64,
                },
                &key8_i64(TARGET),
                &KeyIndexLeafLimits::default(),
            )
            .await
            .expect("probe the leaf");
        probe.uncovered_entry_ordinals().map(<[u64]>::to_vec)
    }
}

/// A catalog whose fold builds key-index leaves.
fn leaf_catalog(store: Arc<dyn ObjectStoreBackend>) -> Catalog {
    Catalog::new(
        store,
        CatalogConfig {
            build_key_index_leaves: true,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog")
}

#[derive(Default)]
struct Serve {
    /// The catalog's decoded-memory budget.
    catalog_budget: Option<u64>,
}

/// `POST /api/v1/sql` over `store`, with a 1 KiB tail probe, a request cost
/// low enough that every wanted extent is its own GET, and the block-range
/// threshold and break-even both at [`BOUND`].
fn serve(store: Arc<dyn ObjectStoreBackend>, cfg: &Serve) -> Router {
    let catalog = leaf_catalog(Arc::clone(&store));
    let catalog = match cfg.catalog_budget {
        Some(bytes) => catalog.with_memory_budget(Arc::new(ravel_memory::MemoryBudget::new(bytes))),
        None => catalog,
    };
    let logs = LogSegmentFetcher::new(store.clone())
        .with_suffix_len(1024)
        .with_request_cost_bytes(1)
        .with_block_range_threshold(BOUND)
        .with_projection_break_even_bytes(Some(BOUND));
    assert_eq!(logs.plan_whole_object_bound(), BOUND);
    let sql_config = SqlConfig {
        engine: EngineConfig {
            sql_partition_count: Some(4),
            ..EngineConfig::default()
        },
        ..SqlConfig::default()
    };
    let executor = SqlExecutor::new(
        Arc::new(catalog),
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
    router(SqlState {
        executor: Arc::new(executor),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
        store,
        clock: Arc::new(FixedClock),
        max_deadline: Duration::from_secs(60),
        query_accounting: Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            HashSet::new(),
        )),
        query_admission: ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
    })
}

async fn sql(app: &Router) -> Value {
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
    let response = app.clone().oneshot(request).await.expect("oneshot");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let value: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["status"], "success", "{value}");
    value
}

fn rows(value: &Value) -> Vec<String> {
    value["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("no rows in {value}"))
        .iter()
        .map(|row| row[0].as_str().expect("string cell").to_string())
        .collect()
}

fn phase(value: &Value, name: &str, field: &str) -> u64 {
    value["stats"]["phases"]
        .as_array()
        .expect("phases")
        .iter()
        .find(|p| p["phase"] == name)
        .map_or(0, |p| {
            p[field]
                .as_u64()
                .unwrap_or_else(|| panic!("{name}.{field} is a u64 in {value}"))
        })
}

fn pruning(value: &Value, field: &str) -> u64 {
    value["stats"]["pruning"][field]
        .as_u64()
        .unwrap_or_else(|| panic!("stats.pruning.{field} is a u64 in {value}"))
}

fn key_index(value: &Value, field: &str) -> u64 {
    value["stats"]["keyIndex"][field]
        .as_u64()
        .unwrap_or_else(|| panic!("stats.keyIndex.{field} is a u64 in {value}"))
}

/// The ten `stats.keyIndex` fields.
const KEY_INDEX_FIELDS: [&str; 10] = [
    "keysProbed",
    "partsCovered",
    "partsUncovered",
    "partsBeyondCeiling",
    "leavesRejected",
    "reservationsRefused",
    "leafGetRequests",
    "leafWireBytes",
    "skippedByBudget",
    "skipIdxDropped",
];

/// `stats.keyIndex` is the logs entry's `keyIndex` in `stats.signals`, the
/// statement's only signal, with exactly the ten fields.
fn assert_signal_key_index_matches(value: &Value) {
    let signals = value["stats"]["signals"].as_array().expect("signals");
    assert_eq!(signals.len(), 1, "{value}");
    assert_eq!(signals[0]["table"], "logs", "{value}");
    assert_eq!(
        signals[0]["keyIndex"], value["stats"]["keyIndex"],
        "{value}"
    );
    let fields: BTreeSet<&str> = value["stats"]["keyIndex"]
        .as_object()
        .unwrap_or_else(|| panic!("stats.keyIndex is an object in {value}"))
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(fields, KEY_INDEX_FIELDS.into_iter().collect(), "{value}");
}

/// The route totals the `/metrics` families render, for logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct Totals {
    /// Per [`KeyIndexPartOutcome::ALL`], in that order.
    parts: [u64; 6],
    leaf_gets: u64,
}

impl Totals {
    /// Parts moved under `outcome` and leaf GETs moved, every other outcome
    /// zero.
    fn only(outcome: KeyIndexPartOutcome, parts: u64, leaf_gets: u64) -> Self {
        let mut totals = Totals::default();
        for (i, o) in KeyIndexPartOutcome::ALL.into_iter().enumerate() {
            if o == outcome {
                totals.parts[i] = parts;
            }
        }
        totals.leaf_gets = leaf_gets;
        totals
    }
}

fn totals() -> Totals {
    Totals {
        parts: KeyIndexPartOutcome::ALL.map(|o| key_index_parts_total(Signal::Logs, o)),
        leaf_gets: key_index_leaf_gets_total(Signal::Logs),
    }
}

fn delta(before: Totals, after: Totals) -> Totals {
    Totals {
        parts: std::array::from_fn(|i| after.parts[i] - before.parts[i]),
        leaf_gets: after.leaf_gets - before.leaf_gets,
    }
}

/// The folded equality routes once: the part's leaf costs two GETs, the 23
/// objects it proves absent are never opened (none read at all, the 16 at or
/// under the 512 KiB threshold included), and the holder, above the
/// whole-object bound, is planned for the leaf's block set with no KEY_IDX
/// read. The rows equal the unfolded answer.
///
/// FLIP (b): letting the block-range threshold gate a pruned segment
/// (`compute_plan_counts` treating `Pruned` like `Uncovered` for objects at
/// or under it) reads the small objects whole: the "a pruned object is never
/// read" assertion fails on object 1. FLIP (c): planning a `Blocks(set)`
/// segment with `plan_segment_keyed` instead of `plan_segment_routed` reads
/// the holder's KEY_IDX: the holder's KEY_IDX assertion fails.
#[tokio::test]
async fn folded_equality_opens_only_the_holder() {
    let _serial = SERIAL.lock().await;
    let fx = Fixture::publish(&[0], &[]).await;
    let unfolded = sql(&serve(Arc::clone(&fx.base), &Serve::default())).await;
    assert_eq!(rows(&unfolded), ["o0r050"]);

    fx.fold().await;
    let (leaf, _) = fx.layout().await;
    let recorder = RecordingStore::new(Arc::clone(&fx.base));
    let app = serve(recorder.clone(), &Serve::default());
    let before = totals();
    let v = sql(&app).await;
    let moved = delta(before, totals());
    assert_eq!(rows(&v), rows(&unfolded), "rows equal the unfolded answer");

    let opened: Vec<usize> = (0..OBJECTS)
        .filter(|&n| !recorder.of(&fx.objects[n].key).is_empty())
        .collect();
    assert_eq!(opened, [0], "only the holder is opened");
    for (n, object) in fx.objects.iter().enumerate().skip(1) {
        assert_eq!(
            recorder.of(&object.key),
            Vec::new(),
            "object {n}: a pruned object is never read"
        );
    }
    let holder = &fx.objects[0];
    let holder_gets = recorder.of(&holder.key);
    assert_eq!(
        holder.key_idx_gets(&holder_gets),
        Vec::new(),
        "the holder's KEY_IDX is never read: {holder_gets:?}"
    );
    assert!(!holder_gets.contains(&GetRange::Full), "{holder_gets:?}");
    assert_eq!(recorder.of(&leaf).len(), 2, "two leaf GETs");

    assert_eq!(key_index(&v, "keysProbed"), 1, "{v}");
    assert_eq!(key_index(&v, "partsCovered"), 1, "{v}");
    assert_eq!(key_index(&v, "partsUncovered"), 0, "{v}");
    assert_eq!(key_index(&v, "partsBeyondCeiling"), 0, "{v}");
    assert_eq!(key_index(&v, "leavesRejected"), 0, "{v}");
    assert_eq!(key_index(&v, "reservationsRefused"), 0, "{v}");
    assert_eq!(key_index(&v, "leafGetRequests"), 2, "{v}");
    assert_eq!(key_index(&v, "skippedByBudget"), 0, "{v}");
    assert_eq!(key_index(&v, "skipIdxDropped"), 0, "{v}");
    assert_signal_key_index_matches(&v);
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 23, "{v}");
    // The keyIndex phase carries the two leaf GETs and nothing else.
    assert_eq!(phase(&v, "keyIndex", "s3GetRequests"), 2, "{v}");
    assert_eq!(
        phase(&v, "keyIndex", "s3GetWireBytes"),
        key_index(&v, "leafWireBytes"),
        "{v}"
    );
    // Every data-object GET is the holder's: its suffix in the plan, then the
    // planned blocks in the scan.
    assert_eq!(holder_gets.len(), 5, "{holder_gets:?}");
    assert_eq!(phase(&v, "plan", "s3GetRequests"), 1, "{v}");
    assert_eq!(phase(&v, "scan", "s3GetRequests"), 4, "{v}");
    assert_eq!(phase(&v, "probe", "s3GetRequests"), 0, "{v}");
    assert_eq!(phase(&v, "resolve", "s3GetRequests"), 2, "{v}");
    assert_eq!(moved, Totals::only(KeyIndexPartOutcome::Covered, 1, 2));
}

/// Objects 17 and 20 carry a version 1 KEY_IDX section without `TARGET`'s
/// entry, and object 20 holds `TARGET`. The fold lists both entries in the
/// leaf's `uncovered_entry_ordinals`, so the router leaves them uncovered:
/// each takes tier 1, where its own probe declines on the version 1 section,
/// and object 20's row is returned. The rows equal the unfolded answer.
///
/// FLIP (a): routing `SegmentRoute::Uncovered` as `Pruned` in
/// `compute_plan_counts` drops object 20's row: the rows assertion fails.
/// Reverting the fold's `section_resolves_like_the_reader` check makes both
/// entries covered and the leaf proves `TARGET` absent from object 20: the
/// `uncovered_entry_ordinals` assertion fails.
#[tokio::test]
async fn version_one_sections_are_uncovered_in_the_leaf() {
    let _serial = SERIAL.lock().await;
    let fx = Fixture::publish(&[0, 20], &[17, 20]).await;
    for n in [17, 20] {
        let (reads, answered) = fx.objects[n].expected_key_reads();
        assert!(!answered, "object {n}: a version 1 section declines");
        assert_eq!(reads.len(), 1, "object {n}: the prefix, then nothing");
    }
    let unfolded = sql(&serve(Arc::clone(&fx.base), &Serve::default())).await;
    assert_eq!(rows(&unfolded), ["o0r050", "o20r05"]);

    fx.fold().await;
    let (leaf, ordinals) = fx.layout().await;
    let mut want = vec![ordinals[17], ordinals[20]];
    want.sort_unstable();
    assert_eq!(fx.uncovered_for_target().await, Some(want));

    let recorder = RecordingStore::new(Arc::clone(&fx.base));
    let v = sql(&serve(recorder.clone(), &Serve::default())).await;
    assert_eq!(rows(&v), rows(&unfolded), "rows equal the unfolded answer");
    for n in [17, 20] {
        let object = &fx.objects[n];
        let (reads, _) = object.expected_key_reads();
        assert_eq!(
            object.key_idx_gets(&recorder.of(&object.key)),
            reads,
            "object {n}: tier 1 probes its own KEY_IDX and declines"
        );
    }
    let opened: Vec<usize> = (0..OBJECTS)
        .filter(|&n| !recorder.of(&fx.objects[n].key).is_empty())
        .collect();
    assert_eq!(opened, [0, 17, 20]);
    assert_eq!(recorder.of(&leaf).len(), 2, "two leaf GETs");
    assert_eq!(key_index(&v, "partsCovered"), 1, "{v}");
    assert_eq!(key_index(&v, "leafGetRequests"), 2, "{v}");
    assert_signal_key_index_matches(&v);
    // The 21 objects the leaf covers and proves absent; 17 and 20 decline.
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 21, "{v}");
    // The leaf GETs plus the two declining prefix reads.
    assert_eq!(phase(&v, "keyIndex", "s3GetRequests"), 2 + 2, "{v}");
}

/// `--rebuild-key-index` builds a leaf through the same section read as the
/// fold, so on a part folded without leaves it lists the two version 1
/// entries as uncovered too.
///
/// FLIP: reverting the `section_resolves_like_the_reader` check in
/// `read_entry_key_index_counted` makes the rebuild's `uncovered_entries` 0
/// and the leaf's uncovered list empty: the report assertion fails.
#[tokio::test]
async fn version_one_sections_are_uncovered_in_a_rebuilt_leaf() {
    let _serial = SERIAL.lock().await;
    let fx = Fixture::publish(&[0, 20], &[17, 20]).await;
    fx.fold_with(Catalog::new(Arc::clone(&fx.base), CatalogConfig::default()).expect("catalog"))
        .await;
    let (_, parts) = fx.resolve().await;
    assert_eq!(parts.parts.len(), 1);
    assert!(parts.parts[0].key_index.is_empty(), "folded without leaves");

    let report = leaf_catalog(Arc::clone(&fx.base))
        .rebuild_key_index(
            &tenant().hash(),
            Signal::Logs,
            &KeyIndexRebuildOptions::default(),
        )
        .await
        .expect("rebuild");
    assert_eq!(report.parts_rebuilt, 1, "{report:?}");
    assert_eq!(report.uncovered_entries, 2, "{report:?}");
    let (_, ordinals) = fx.layout().await;
    let mut want = vec![ordinals[17], ordinals[20]];
    want.sort_unstable();
    assert_eq!(fx.uncovered_for_target().await, Some(want));
}

/// The tier-1 answer every leaf-path failure must fall back to: the 16 small
/// objects read whole unprobed, the 8 big ones probed by their own KEY_IDX,
/// and the 7 big decoys pruned there.
fn assert_tier_one(fx: &Fixture, recorder: &RecordingStore, v: &Value) {
    assert_eq!(rows(v), ["o0r050"], "{v}");
    assert_eq!(pruning(v, "segmentsPrunedByKeyIndex"), 7, "{v}");
    let mut key_reads = 0;
    for (n, object) in fx.objects.iter().enumerate() {
        let gets = recorder.of(&object.key);
        if (1..=16).contains(&n) {
            assert_eq!(gets, [GetRange::Full], "object {n}");
        } else {
            let (reads, answered) = object.expected_key_reads();
            assert!(answered, "object {n}");
            assert_eq!(object.key_idx_gets(&gets), reads, "object {n}");
            key_reads += reads.len() as u64;
        }
    }
    let leaf_gets = key_index(v, "leafGetRequests");
    assert_eq!(
        phase(v, "keyIndex", "s3GetRequests"),
        key_reads + leaf_gets,
        "{v}"
    );
    assert_eq!(key_index(v, "partsCovered"), 0, "{v}");
    assert_signal_key_index_matches(v);
}

/// A refused charge for the parts runs the scan at tier 1 and answers
/// exactly: the router does not run (every `stats.keyIndex` field 0, no
/// route total moved). The fixture is unfolded, because on a folded tenant
/// the resolve's own decode of the part outweighs the parts' handles and no
/// budget separates the two; the same statement with no budget does run the
/// router (`keysProbed` 1), so the zeros are the refusal's.
///
/// FLIP: re-raising `CatalogError::MemoryExhausted` for the logs lane in the
/// executor's parts resolve fails the statement with a memory error.
#[tokio::test]
async fn a_refused_parts_charge_degrades_to_tier_one() {
    let _serial = SERIAL.lock().await;
    let fx = Fixture::publish(&[0], &[]).await;
    let routed = sql(&serve(Arc::clone(&fx.base), &Serve::default())).await;
    assert_eq!(key_index(&routed, "keysProbed"), 1, "{routed}");
    // A budget the resolve fits and the parts' handles do not.
    let budget = 1;
    let catalog = leaf_catalog(Arc::clone(&fx.base))
        .with_memory_budget(Arc::new(ravel_memory::MemoryBudget::new(budget)));
    let window = TimeRange {
        start_ns: 0,
        end_ns: NOW_NS,
    };
    let refused = catalog
        .resolve_pruned_with_parts(
            &tenant().hash(),
            Signal::Logs,
            window,
            &[],
            NOW_NS,
            None,
            &QueryAccounting::new(),
        )
        .await;
    assert!(
        matches!(refused, Err(CatalogError::MemoryExhausted(_))),
        "the parts' charge exceeds {budget} bytes: {:?}",
        refused.map(|(snapshot, _, parts)| (snapshot.segments.len(), parts.heap_bytes()))
    );
    let (snapshot, _) = catalog
        .resolve_pruned_with_admission(
            &tenant().hash(),
            Signal::Logs,
            window,
            &[],
            NOW_NS,
            None,
            &QueryAccounting::new(),
        )
        .await
        .expect("the resolve alone fits the budget");
    assert_eq!(snapshot.segments.len(), OBJECTS);
    let recorder = RecordingStore::new(Arc::clone(&fx.base));
    let app = serve(
        recorder.clone(),
        &Serve {
            catalog_budget: Some(budget),
            ..Serve::default()
        },
    );
    let before = totals();
    let v = sql(&app).await;
    assert_eq!(delta(before, totals()), Totals::default());
    for field in KEY_INDEX_FIELDS {
        assert_eq!(key_index(&v, field), 0, "{field}: {v}");
    }
    assert_tier_one(&fx, &recorder, &v);
}

/// Folds the fixture and runs the statement over a fault store scripted with
/// `rule` on the leaf's key.
async fn with_leaf_fault(
    fault: ScriptedFault,
) -> (Fixture, Arc<RecordingStore>, Arc<FaultStore<Arc<dyn ObjectStoreBackend>>>, Value, Totals) {
    let fx = Fixture::publish(&[0], &[]).await;
    fx.fold().await;
    let (leaf, _) = fx.layout().await;
    let faults = Arc::new(FaultStore::new(
        Arc::clone(&fx.base),
        FaultPlan::empty().with_rule(Rule::new(Op::Get, fault).with_key_contains(leaf.clone())),
    ));
    let recorder = RecordingStore::new(faults.clone());
    let app = serve(recorder.clone(), &Serve::default());
    let before = totals();
    let v = sql(&app).await;
    let moved = delta(before, totals());
    assert!(!recorder.of(&leaf).is_empty(), "the leaf was read");
    (fx, recorder, faults, v, moved)
}

/// A leaf GET that fails rejects the leaf (`leavesRejected`, the router's
/// figure for a leaf it could not use, not `partsUncovered`, which counts
/// parts with no leaf for the field), and the part's segments are left
/// uncovered for tier 1, none pruned by the leaf. The failed GET is still
/// charged: one leaf GET request, no wire bytes.
///
/// FLIP (a): routing `Uncovered` as `Pruned` returns no row here.
#[tokio::test]
async fn a_failed_leaf_get_leaves_the_part_uncovered() {
    let _serial = SERIAL.lock().await;
    let (fx, recorder, faults, v, moved) =
        with_leaf_fault(ScriptedFault::Permanent("leaf gone".into())).await;
    assert_eq!(faults.fault_count(Op::Get, FaultKind::Permanent), 1);
    assert_eq!(key_index(&v, "keysProbed"), 1, "{v}");
    assert_eq!(key_index(&v, "leavesRejected"), 1, "{v}");
    assert_eq!(key_index(&v, "partsUncovered"), 0, "{v}");
    assert_eq!(key_index(&v, "leafGetRequests"), 1, "{v}");
    assert_eq!(key_index(&v, "leafWireBytes"), 0, "{v}");
    assert_eq!(moved, Totals::only(KeyIndexPartOutcome::Rejected, 1, 1));
    assert_tier_one(&fx, &recorder, &v);
}

/// A leaf whose bytes fail their checksum is rejected the same way.
#[tokio::test]
async fn a_leaf_failing_its_checksum_leaves_the_part_uncovered() {
    let _serial = SERIAL.lock().await;
    let (fx, recorder, faults, v, moved) = with_leaf_fault(ScriptedFault::CorruptRange).await;
    assert_eq!(faults.fault_count(Op::Get, FaultKind::CorruptRange), 1);
    assert_eq!(key_index(&v, "keysProbed"), 1, "{v}");
    assert_eq!(key_index(&v, "leavesRejected"), 1, "{v}");
    assert_eq!(key_index(&v, "partsUncovered"), 0, "{v}");
    assert_eq!(moved.parts, Totals::only(KeyIndexPartOutcome::Rejected, 1, 0).parts);
    assert_tier_one(&fx, &recorder, &v);
}

// A refused frame reservation has no end-to-end case here: the leaf's decoded
// bound grows with the square root of its body, so on any fixture a statement
// pool too small for one frame is too small for the tier-1 scan the refusal
// falls back to, and the statement fails on the scan's own reservation.
// `pool_leaf_budget_tests` in the logs scan pins that the reservation is the
// statement pool's, and the router's
// `a_refused_reservation_leaves_the_part_uncovered` that a refusal leaves the
// part uncovered.
