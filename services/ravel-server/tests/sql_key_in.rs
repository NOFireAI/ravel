//! Exact-key `IN` lists on logs and spans (ADR-3692 decision 6, issue #3705):
//! a declared key-index column `IN` literals, and `trace_id IN` 16-byte
//! literals, carry one key set through the tier-2 router and the tier-1 set
//! probe. The rows of an `IN` list equal the union of the per-key equality
//! answers, `stats.keyIndex.keysProbed` is the list's distinct key count,
//! an object holding none of the keys is pruned on either tier, and a list
//! the router cannot afford reads its part from tier 1 with no further leaf
//! read.
//! A list of more than 1,024 values carries no key and is answered by the
//! residual filter; a list beside a second key arm stays on tier 1.
//!
//! The fixture is one tenant holding four RLOG objects and four RSPAN
//! objects of 1,024 records each, every record carrying a distinct
//! `user_id` (logs) or trace id (spans), folded into one part per signal
//! with key-index leaves. Cases whose part must be no dearer to route than
//! to probe at tier 1 add filler objects of their own ids, up to
//! `WIDE_OBJECTS` entries per part.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use futures::StreamExt;
use ravel_catalog::{
    AbsentPolicy, Catalog, CatalogConfig, DeclaredColumnType, DeclaredTypedColumn,
    KeyIndexLeafInput, KeyIndexLeafLimits, StorageLayoutWrite, TenantConfig, TenantLifecycleState,
    decode_head, encode_head, encode_key_index_leaf_with_bucket_bits, set_tenant_config,
    validate_or_adopt, verify_key_index_leaf,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::Clock;
use ravel_logseg::key_index::KeyType;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions};
use ravel_proto::catalog::v1::SnapshotKeyIndexLeafRef;
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::key_index_route::{KeyIndexPartOutcome, key_index_parts_total};
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
/// Late enough that a fold seals ingest hour 0.
const NOW_NS: i64 = 4 * NS_PER_HOUR;
const TOKEN: &str = "acme-token";
const OBJECTS: usize = 4;
const RECORDS: usize = 1_024;
const PER_BLOCK: usize = 16;
/// Entries of the part the leaf route must be no dearer for than tier 1 is:
/// a 1,000-key set on a 13-bit leaf touches 1 + B_run = 65 leaves, at most
/// `2 x 36 = 72`, the tier-1 probe of the part's segments.
const WIDE_OBJECTS: usize = 36;
/// Records in each filler object. Twice [`RECORDS`] keeps the 13-bit leaf's
/// bucket bytes above the part's bridged allowance of `entries x T1_BYTES`
/// (11 KiB per entry); at 1,024 a filler adds about 9.3 KiB of buckets and
/// the 1,000-key read fits.
const FILLER_RECORDS: usize = 2 * RECORDS;
/// The list sizes the acceptance runs.
const SIZES: [usize; 4] = [1, 10, 100, 1_000];

struct FixedClock;
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

fn tenant() -> TenantId {
    TenantId::new("acme".to_string())
}

/// The `j`th key of every list: record `(j / 4 * 37) % 1024` of object
/// `j % 4`, distinct for every `j` below 4,096, so the list of size `n` is
/// the first `n` keys and spreads over every object and many blocks.
fn key_record(j: usize) -> (usize, usize) {
    (j % OBJECTS, (j / OBJECTS * 37) % RECORDS)
}

/// Records in `object`: [`RECORDS`] in the four key objects, and
/// [`FILLER_RECORDS`] in a filler.
fn records(object: usize) -> usize {
    if object < OBJECTS {
        RECORDS
    } else {
        FILLER_RECORDS
    }
}

/// Record `i` of `object`'s number across the tenant, distinct for every
/// record of every object.
fn record_number(object: usize, i: usize) -> usize {
    if object < OBJECTS {
        object * RECORDS + i
    } else {
        OBJECTS * RECORDS + (object - OBJECTS) * FILLER_RECORDS + i
    }
}

fn user_id(object: usize, i: usize) -> i64 {
    1_000_000 + record_number(object, i) as i64
}

fn trace_id(object: usize, i: usize) -> [u8; 16] {
    let mut t = [0u8; 16];
    t.copy_from_slice(
        &blake3::hash(&(record_number(object, i) as u64).to_le_bytes()).as_bytes()[..16],
    );
    t
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The row a record answers with: its object and ordinal.
fn label(object: usize, i: usize) -> String {
    format!("o{object}r{i:04}")
}

/// Which signal a statement reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lane {
    Logs,
    Spans,
}

impl Lane {
    fn signal(self) -> Signal {
        match self {
            Lane::Logs => Signal::Logs,
            Lane::Spans => Signal::Spans,
        }
    }

    fn literal(self, j: usize) -> String {
        let (o, i) = key_record(j);
        match self {
            Lane::Logs => user_id(o, i).to_string(),
            Lane::Spans => format!("X'{}'", hex(&trace_id(o, i))),
        }
    }

    fn select(self, predicate: &str) -> String {
        match self {
            Lane::Logs => format!("SELECT substr(body, 1, 7) AS id FROM logs WHERE {predicate}"),
            Lane::Spans => format!("SELECT name AS id FROM spans WHERE {predicate}"),
        }
    }

    fn column(self) -> &'static str {
        match self {
            Lane::Logs => "user_id",
            Lane::Spans => "trace_id",
        }
    }

    /// `column IN` the first `n` keys.
    fn in_list(self, n: usize) -> String {
        self.in_keys(&(0..n).collect::<Vec<_>>())
    }

    /// `column IN` the keys numbered `keys`.
    fn in_keys(self, keys: &[usize]) -> String {
        let values: Vec<String> = keys.iter().map(|&j| self.literal(j)).collect();
        self.select(&format!("{} IN ({})", self.column(), values.join(", ")))
    }

    fn equality(self, j: usize) -> String {
        self.select(&format!("{} = {}", self.column(), self.literal(j)))
    }

    fn field(self) -> &'static str {
        match self {
            Lane::Logs => "user_id",
            Lane::Spans => "trace_id",
        }
    }

    fn key_type(self) -> KeyType {
        match self {
            Lane::Logs => KeyType::I64,
            Lane::Spans => KeyType::Id16,
        }
    }
}

async fn publish_logs(store: &Arc<dyn ObjectStoreBackend>, objects: usize) {
    let tenant_hash = tenant().hash();
    for object in 0..objects {
        let writer_id = Uuid::from_u128(3_705_000 + object as u128);
        let mut writer = RlogWriter::new(
            RlogConfig {
                block_target_records: PER_BLOCK,
                ..RlogConfig::default()
            },
            ObjectIdentity {
                tenant_hash: tenant_hash.0,
                shard: 0,
                writer_id: writer_id.into_bytes(),
                writer_epoch: 1,
                writer_seq: object as u64 + 1,
            },
        )
        .with_key_index_fields(vec![("user_id".to_string(), KeyType::I64)]);
        let resource = vec![(
            "service.name".to_string(),
            AttrValue::Str("api".to_string()),
        )];
        let (mut min, mut max) = (i64::MAX, i64::MIN);
        for i in 0..records(object) {
            let ts = 1_000 + (i * OBJECTS + object) as i64;
            min = min.min(ts);
            max = max.max(ts);
            writer
                .push(LogRecord {
                    stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                    stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                    ts_ns: ts,
                    observed_ts_ns: ts,
                    severity_num: 9,
                    severity_text: "INFO".into(),
                    body: format!("{}|payload", label(object, i)),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: vec![("user_id".to_string(), AttrValue::I64(user_id(object, i)))],
                })
                .expect("push");
        }
        let bytes = Bytes::from(writer.finish().expect("finish"));
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: object as u64 + 1,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: records(object) as u64,
            series_count: 1,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            min_ingest_ts_ns: min,
            max_ingest_ts_ns: max,
            segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
            created_unix_ns: 10 + object as i64,
            ingest_hour_bucket: 0,
        })
        .expect("record");
        let key = keys::reconstruct_data_key(&rec).expect("key");
        store
            .put(&key, bytes, PutOptions::default())
            .await
            .expect("put");
        publish::publish(store.as_ref(), &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }
}

async fn publish_spans(store: &Arc<dyn ObjectStoreBackend>, objects: usize) {
    let tenant_hash = tenant().hash();
    validate_or_adopt(
        store.as_ref(),
        &tenant_hash,
        Signal::Spans,
        1,
        0,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("generation 0");
    for object in 0..objects {
        let writer_id = Uuid::from_u128(3_705_100 + object as u128);
        let mut writer = ravel_rspan::RspanWriter::new(
            ravel_rspan::RspanConfig {
                block_target_records: PER_BLOCK,
                ..ravel_rspan::RspanConfig::default()
            },
            ravel_rspan::ObjectIdentity {
                tenant_hash: tenant_hash.0,
                shard: 0,
                writer_id: writer_id.into_bytes(),
                writer_epoch: 1,
                writer_seq: 1,
            },
        );
        let (mut min, mut max) = (i64::MAX, i64::MIN);
        for i in 0..records(object) {
            let start = 1_000 + (i * OBJECTS + object) as i64;
            min = min.min(start);
            max = max.max(start + 100);
            writer.push(ravel_rspan::SpanRecord {
                trace_id: trace_id(object, i),
                span_id: (i as u64 + 1).to_le_bytes(),
                parent_span_id: None,
                name: label(object, i),
                start_ts_ns: start,
                end_ts_ns: start + 100,
                status_code: ravel_rspan::StatusCode::Ok,
                status_message: None,
                attrs: Vec::new(),
            });
        }
        let bytes = Bytes::from(writer.finish().expect("finish rspan object"));
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Spans,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: records(object) as u64,
            series_count: records(object) as u64,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            min_ingest_ts_ns: min,
            max_ingest_ts_ns: max,
            segment_format_version: u32::from(ravel_ingest::SPAN_SEGMENT_FORMAT_VERSION),
            created_unix_ns: min,
            ingest_hour_bucket: 0,
        })
        .expect("record");
        let key = keys::reconstruct_data_key(&rec).expect("key");
        store
            .put(&key, bytes, PutOptions::default())
            .await
            .expect("put");
        publish::publish(store.as_ref(), &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }
}

/// A catalog whose fold builds key-index leaves.
fn leaf_catalog(store: Arc<dyn ObjectStoreBackend>) -> Catalog {
    Catalog::new(
        store,
        CatalogConfig {
            build_key_index_leaves: true,
            byte_cache_max_bytes: 0,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog")
}

/// Both signals published, `user_id` declared, and each signal folded into
/// one part with leaves.
async fn fixture() -> Arc<dyn ObjectStoreBackend> {
    fixture_of(OBJECTS).await
}

/// [`fixture`] with `objects - OBJECTS` more objects per signal beside the
/// four the keys live in, each holding its own distinct ids, so the one part
/// has `objects` entries and its tier-1 probe costs `2 x objects` GETs.
async fn fixture_of(objects: usize) -> Arc<dyn ObjectStoreBackend> {
    assert!(objects >= OBJECTS);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_logs(&store, objects).await;
    publish_spans(&store, objects).await;
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
    set_tenant_config(store.as_ref(), &tenant().hash(), &config, 1)
        .await
        .expect("write tenant config");
    for signal in [Signal::Logs, Signal::Spans] {
        let report = leaf_catalog(Arc::clone(&store))
            .fold(
                &tenant().hash(),
                signal,
                Uuid::from_u128(1),
                NOW_NS,
                &[],
                None,
            )
            .await
            .expect("fold");
        assert_eq!(report.entry_count, objects as u64, "{signal:?}: {report:?}");
    }
    store
}

/// `POST /api/v1/sql` over `store`. Every object is above the fetchers'
/// thresholds, so a segment the router leaves uncovered takes the tier-1
/// probe.
fn serve(store: Arc<dyn ObjectStoreBackend>) -> Router {
    let logs = LogSegmentFetcher::new(store.clone())
        .with_suffix_len(1024)
        .with_block_range_threshold(0)
        .with_projection_break_even_bytes(Some(0));
    let executor = SqlExecutor::new(
        Arc::new(leaf_catalog(Arc::clone(&store))),
        SegmentFetcher::new(store.clone()),
        logs,
        ravel_sql::SpanSegmentFetcher::new(store.clone()),
        SqlConfig {
            engine: EngineConfig {
                sql_partition_count: Some(4),
                ..EngineConfig::default()
            },
            ..SqlConfig::default()
        },
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
        max_deadline: Duration::from_secs(120),
        query_accounting: Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            HashSet::new(),
        )),
        query_admission: ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
    })
}

async fn sql(app: &Router, statement: &str) -> Value {
    let payload = serde_json::json!({
        "query": statement,
        "start": 0.0,
        "end": NOW_NS as f64 / 1_000_000_000.0,
        "timeout": 120.0,
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

fn rows(value: &Value) -> BTreeSet<String> {
    value["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("no rows in {value}"))
        .iter()
        .map(|row| row[0].as_str().expect("string cell").to_string())
        .collect()
}

/// `stats.keyIndex.<field>`, 0 when the object is absent (no key arm).
fn key_index(value: &Value, field: &str) -> u64 {
    value["stats"]["keyIndex"][field].as_u64().unwrap_or(0)
}

/// The expected rows of the first `n` keys.
fn oracle(n: usize) -> BTreeSet<String> {
    oracle_of(&(0..n).collect::<Vec<_>>())
}

/// The expected rows of the keys numbered `keys`.
fn oracle_of(keys: &[usize]) -> BTreeSet<String> {
    keys.iter()
        .map(|&j| {
            let (o, i) = key_record(j);
            label(o, i)
        })
        .collect()
}

/// `stats.pruning.<field>`.
fn pruning(value: &Value, field: &str) -> u64 {
    value["stats"]["pruning"][field]
        .as_u64()
        .unwrap_or_else(|| panic!("stats.pruning.{field} is a u64 in {value}"))
}

/// Re-encodes the one part's leaf for `lane` at 13 bucket bits and repoints
/// HEAD at it, so a 1,000-key set touches about a thousand of its 8,192
/// buckets and its bridged read exceeds the part's tier-1 allowance.
async fn swap_in_a_13_bit_leaf(store: &Arc<dyn ObjectStoreBackend>, lane: Lane) {
    let signal = lane.signal();
    let head_key = format!(
        "t/{}/catalog/{}/HEAD",
        tenant().hash().to_hex(),
        signal.key_prefix()
    );
    let current = store.get(&head_key, GetRange::Full).await.expect("HEAD");
    let mut head = decode_head(&current.data).expect("decode HEAD");
    assert_eq!(head.parts.len(), 1, "one part");
    let part = &mut head.parts[0];
    assert_eq!(part.key_index.len(), 1, "one leaf: {:?}", part.key_index);
    let leaf = part.key_index[0].clone();
    let bytes = store
        .get(&leaf.key, GetRange::Full)
        .await
        .expect("leaf")
        .data;
    let summary = verify_key_index_leaf(&bytes, &KeyIndexLeafLimits::default()).expect("verify");
    assert!(summary.header.bucket_bits < 13, "{:?}", summary.header);
    let wide = encode_key_index_leaf_with_bucket_bits(
        &KeyIndexLeafInput {
            tenant_hash: tenant().hash(),
            signal,
            part_blake3: part.blake3.as_slice().try_into().expect("32-byte hash"),
            part_entry_count: part.entry_count,
            field: lane.field(),
            key_type: lane.key_type(),
            key8_lo: summary.header.key8_lo,
            key8_hi: summary.header.key8_hi,
            uncovered_entry_ordinals: &summary.header.uncovered_entry_ordinals,
            entries: &summary.entries,
        },
        13,
        &KeyIndexLeafLimits::default(),
    )
    .expect("encode at 13 bits");
    let (dir, name) = leaf.key.rsplit_once('/').expect("leaf key directory");
    let (hour, _) = name.split_once('.').expect("leaf key hour");
    let key = format!("{dir}/{hour}.{}.kidx", hex(&wide.leaf_ref.blake3[..8]));
    assert_ne!(key, leaf.key);
    store
        .put(
            &key,
            Bytes::from(wide.bytes),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put the 13-bit leaf");
    part.key_index = vec![SnapshotKeyIndexLeafRef {
        key,
        ..wide.leaf_ref
    }];
    store
        .put(
            &head_key,
            Bytes::from(encode_head(&head).expect("encode HEAD")),
            PutOptions {
                mode: PutMode::CasVersion(current.version),
                checksum: None,
            },
        )
        .await
        .expect("swap the ref");
}

/// For each lane, on a part of `WIDE_OBJECTS` entries whose tier-1 cost
/// admits every list size at tier 2: the rows of an `IN` list of 1, 10, 100
/// and 1,000 keys equal the union of the per-key equality answers,
/// `keysProbed` is the list's size, no list is over the part's tier-1 cost,
/// and the part is covered by its leaves with at least one leaf GET. The one
/// exception is the spans 1,000-key list: its bridged bucket read exceeds the
/// part's byte allowance, so it counts in `skippedByBudget` after the
/// prefix GET and tier 1 answers. On a 13-bit leaf the 1,000-key list leaves
/// the part uncovered after the leaf's prefix, with no further leaf read,
/// and the rows are still exact. A 1,025-key list carries no key and returns
/// exact rows; on logs, a list beside a second key equality stays on tier 1.
///
/// Flip to watch it fail: route only the first key of the set (in
/// `route_segments` and `SpansTableProvider::scan`, build the key set from
/// `set.keys()[..1]`). The router then prunes every segment holding only
/// later keys, and the 10-key rows miss nine of the ten union rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn in_list_rows_equal_the_union_of_equalities() {
    let store = fixture_of(WIDE_OBJECTS).await;
    let app = serve(Arc::clone(&store));
    for lane in [Lane::Logs, Lane::Spans] {
        // Every per-key equality answer, once.
        let equalities: Vec<BTreeSet<String>> = futures::stream::iter(0..SIZES[3])
            .map(|j| {
                let app = app.clone();
                async move { rows(&sql(&app, &lane.equality(j)).await) }
            })
            .buffered(8)
            .collect()
            .await;
        for (j, answer) in equalities.iter().enumerate() {
            let (o, i) = key_record(j);
            assert_eq!(answer, &BTreeSet::from([label(o, i)]), "{lane:?}: key {j}");
        }
        for n in SIZES {
            let v = sql(&app, &lane.in_list(n)).await;
            let union: BTreeSet<String> = equalities[..n].iter().flatten().cloned().collect();
            assert_eq!(rows(&v), union, "{lane:?}: {n} keys");
            assert_eq!(union, oracle(n), "{lane:?}: {n} keys");
            assert_eq!(
                key_index(&v, "keysProbed"),
                n as u64,
                "{lane:?}: {n} keys: {v}"
            );
            let over_bytes = u64::from(lane == Lane::Spans && n == SIZES[3]);
            assert_eq!(
                key_index(&v, "partsCovered"),
                1 - over_bytes,
                "{lane:?}: {n} keys: {v}"
            );
            assert_eq!(
                key_index(&v, "skippedByBudget"),
                over_bytes,
                "{lane:?}: {n} keys: {v}"
            );
            assert_eq!(
                key_index(&v, "partsOverTierOne"),
                0,
                "{lane:?}: {n} keys: {v}"
            );
            assert!(
                key_index(&v, "leafGetRequests") > 0,
                "{lane:?}: {n} keys: {v}"
            );
        }

        // More than k = 1,024 values: no key set, the residual answers.
        let over = sql(&app, &lane.in_list(1_025)).await;
        assert_eq!(rows(&over), oracle(1_025), "{lane:?}");
        assert_eq!(key_index(&over, "keysProbed"), 0, "{lane:?}: {over}");
        assert_eq!(key_index(&over, "leafGetRequests"), 0, "{lane:?}: {over}");
    }

    // An `IN` list beside a second key equality stays on tier 1: no route,
    // and the rows are the intersection.
    let both = Lane::Logs.select(&format!(
        "user_id IN ({}) AND user_id = {}",
        (0..10)
            .map(|j| Lane::Logs.literal(j))
            .collect::<Vec<_>>()
            .join(", "),
        Lane::Logs.literal(3)
    ));
    let v = sql(&app, &both).await;
    let (o, i) = key_record(3);
    assert_eq!(rows(&v), BTreeSet::from([label(o, i)]), "{v}");
    assert_eq!(key_index(&v, "keysProbed"), 0, "{v}");
    assert_eq!(key_index(&v, "leafGetRequests"), 0, "{v}");
    assert_eq!(key_index(&v, "partsCovered"), 0, "{v}");

    // On a 13-bit leaf the 1,000-key set's bridged read exceeds the part's
    // allowance: the part is uncovered after GET 1, and tier 1 answers. The
    // part needs `WIDE_OBJECTS` entries for its worst case of 65 leaf GETs to
    // be admitted at all.
    for lane in [Lane::Logs, Lane::Spans] {
        swap_in_a_13_bit_leaf(&store, lane).await;
        let v = sql(&serve(Arc::clone(&store)), &lane.in_list(1_000)).await;
        assert_eq!(rows(&v), oracle(1_000), "{lane:?}");
        assert_eq!(key_index(&v, "keysProbed"), 1_000, "{lane:?}: {v}");
        assert_eq!(key_index(&v, "partsCovered"), 0, "{lane:?}: {v}");
        assert_eq!(key_index(&v, "skippedByBudget"), 1, "{lane:?}: {v}");
        assert_eq!(
            key_index(&v, "leafGetRequests"),
            1,
            "{lane:?}: the prefix and no further leaf read: {v}"
        );
    }
}

/// A list of two or three keys, which DataFusion rewrites into a disjunction
/// of equalities before the scan, carries its key set on both lanes:
/// `keysProbed` is 2 and 3, the scan opens exactly the objects holding the
/// keys and prunes the rest, and the rows equal the per-key equality union
/// and the fixture's oracle. Keys `{0, 4}` both live in object 0 (3 of 4
/// objects pruned); keys `{0, 1, 2}` live in objects 0, 1 and 2 (1 pruned).
/// The disjunction spelled out by hand answers the same.
///
/// Flip to watch it fail: drop the `Or` arm of `collect_key_in` (logs) or
/// the `trace_id_disjunction_values` branch of `handle_leaf` (spans).
/// `keysProbed` then reads 0 instead of 2, and no object is pruned.
#[tokio::test]
async fn a_two_or_three_key_list_carries_its_key_set() {
    let store = fixture().await;
    let app = serve(store);
    for lane in [Lane::Logs, Lane::Spans] {
        for (keys, pruned) in [(vec![0, 4], 3u64), (vec![0, 1, 2], 1)] {
            let equalities: BTreeSet<String> = futures::stream::iter(keys.clone())
                .then(|j| {
                    let app = app.clone();
                    async move { rows(&sql(&app, &lane.equality(j)).await) }
                })
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .flatten()
                .collect();
            let spelled = lane.select(
                &keys
                    .iter()
                    .map(|&j| format!("{} = {}", lane.column(), lane.literal(j)))
                    .collect::<Vec<_>>()
                    .join(" OR "),
            );
            for statement in [lane.in_keys(&keys), spelled] {
                let v = sql(&app, &statement).await;
                assert_eq!(rows(&v), oracle_of(&keys), "{lane:?} {keys:?}: {v}");
                assert_eq!(rows(&v), equalities, "{lane:?} {keys:?}: {v}");
                assert_eq!(
                    key_index(&v, "keysProbed"),
                    keys.len() as u64,
                    "{lane:?} {keys:?}: {v}"
                );
                assert_eq!(pruning(&v, "segments"), OBJECTS as u64, "{lane:?}: {v}");
                assert_eq!(
                    pruning(&v, "segmentsPrunedByKeyIndex"),
                    pruned,
                    "{lane:?} {keys:?}: opened {} of {OBJECTS}: {v}",
                    OBJECTS as u64 - pruning(&v, "segmentsPrunedByKeyIndex")
                );
            }
        }
    }
}

/// `trace_id IN` 32-character hex strings plans against the native trace id
/// column (the analyzer rule rewrites them before type coercion) and carries
/// the same key set as the binary literals: the same rows, `keysProbed` the
/// list's size.
///
/// Flip to watch it fail: drop `TraceIdHexInListRule` from
/// `trace_id_planner::analyzer_rules`. The statement then fails to plan
/// and `sql` panics on the error status.
#[tokio::test]
async fn a_hex_trace_id_list_plans_and_carries_the_key_set() {
    let store = fixture().await;
    let app = serve(store);
    let values: Vec<String> = (0..100)
        .map(|j| {
            let (o, i) = key_record(j);
            format!("'{}'", hex(&trace_id(o, i)))
        })
        .collect();
    let v = sql(
        &app,
        &Lane::Spans.select(&format!("trace_id IN ({})", values.join(", "))),
    )
    .await;
    assert_eq!(rows(&v), oracle(100), "{v}");
    assert_eq!(key_index(&v, "keysProbed"), 100, "{v}");
}

/// A logs key set whose every key is absent from an object prunes that
/// object, both when the part's leaves route the set and when the part is
/// uncovered and each object's own KEY_IDX answers the set. Key `j` lives in
/// object `j % 4`: ten keys of object 0 leave objects 1 to 3 and the 32
/// fillers without a key, and on a 13-bit leaf 1,000 keys of objects 0 and 1
/// leave objects 2 and 3 and the fillers without one.
///
/// Flip to watch it fail: in ravel-query's `probe_key_index` (log_fetcher), push
/// the key set's answer only when it is not `Absent`. Every object is then
/// kept and the tier-1 count reads 0 instead of 34.
#[tokio::test]
async fn a_logs_set_absent_from_an_object_prunes_it() {
    let store = fixture_of(WIDE_OBJECTS).await;
    let lane = Lane::Logs;
    let fillers = (WIDE_OBJECTS - OBJECTS) as u64;

    let one_object: Vec<usize> = (0..10).map(|n| n * OBJECTS).collect();
    let v = sql(&serve(Arc::clone(&store)), &lane.in_keys(&one_object)).await;
    assert_eq!(rows(&v), oracle_of(&one_object), "{v}");
    assert_eq!(key_index(&v, "keysProbed"), 10, "{v}");
    assert_eq!(key_index(&v, "partsCovered"), 1, "{v}");
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 3 + fillers, "{v}");

    swap_in_a_13_bit_leaf(&store, lane).await;
    let two_objects: Vec<usize> = (0..).filter(|j| j % OBJECTS < 2).take(SIZES[3]).collect();
    let v = sql(&serve(Arc::clone(&store)), &lane.in_keys(&two_objects)).await;
    assert_eq!(rows(&v), oracle_of(&two_objects), "{v}");
    assert_eq!(key_index(&v, "keysProbed"), 1_000, "{v}");
    assert_eq!(key_index(&v, "partsCovered"), 0, "{v}");
    assert_eq!(key_index(&v, "skippedByBudget"), 1, "{v}");
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 2 + fillers, "{v}");
}

/// A part whose leaf route would cost more than its tier-1 probe is left to
/// tier 1 (ADR-3692's key-set ceiling amendment): ten keys of object 0 touch
/// one slice for a worst case of 1 + 10 = 11 leaf GETs against the tier-1
/// cost of the part's 4 segments in the snapshot, 2 x 4 = 8. The part counts
/// in `partsOverTierOne`, not `partsUncovered`, and under
/// `outcome="over_tier_one"` of `ravel_query_key_index_parts_total` for logs;
/// no leaf is read, each object's own KEY_IDX prunes objects 1 to 3, and the
/// rows equal those of the same keys padded past k = 1,024 with absent ids, a
/// list that carries no key set and is not routed. No other logs statement
/// of this binary is over its tier-1 cost, so the logs total moves by
/// exactly 1.
///
/// Flip to watch it fail: in ravel-query's `route_parts`, drop the arm that
/// counts a part whose `worst_gets` exceeds its snapshot segments x
/// `gets_per_entry`. The part is then probed and covered: `partsOverTierOne`
/// reads 0 and `leafGetRequests` is non-zero. Counting the arm in
/// `parts_uncovered` instead fails on `partsUncovered` reading 1.
#[tokio::test]
async fn a_part_dearer_to_route_than_to_probe_is_left_to_tier_one() {
    let store = fixture().await;
    let app = serve(store);
    let lane = Lane::Logs;
    let one_object: Vec<usize> = (0..10).map(|n| n * OBJECTS).collect();

    let before = key_index_parts_total(Signal::Logs, KeyIndexPartOutcome::OverTierOne);
    let v = sql(&app, &lane.in_keys(&one_object)).await;
    let moved = key_index_parts_total(Signal::Logs, KeyIndexPartOutcome::OverTierOne) - before;
    assert_eq!(rows(&v), oracle_of(&one_object), "{v}");
    assert_eq!(key_index(&v, "keysProbed"), 10, "{v}");
    assert_eq!(key_index(&v, "partsOverTierOne"), 1, "{v}");
    assert_eq!(moved, 1, "{v}");
    assert_eq!(key_index(&v, "partsUncovered"), 0, "{v}");
    assert_eq!(key_index(&v, "partsCovered"), 0, "{v}");
    assert_eq!(key_index(&v, "skippedByBudget"), 0, "{v}");
    assert_eq!(key_index(&v, "leafGetRequests"), 0, "{v}");
    assert_eq!(pruning(&v, "segmentsPrunedByKeyIndex"), 3, "{v}");

    let padded: Vec<String> = one_object
        .iter()
        .map(|&j| lane.literal(j))
        .chain((1..=1_015).map(|id: i64| id.to_string()))
        .collect();
    assert_eq!(padded.len(), 1_025);
    let unrouted = sql(
        &app,
        &lane.select(&format!("user_id IN ({})", padded.join(", "))),
    )
    .await;
    assert_eq!(key_index(&unrouted, "keysProbed"), 0, "{unrouted}");
    assert_eq!(key_index(&unrouted, "leafGetRequests"), 0, "{unrouted}");
    assert_eq!(rows(&v), rows(&unrouted), "{v}\n{unrouted}");
}

/// A spans `trace_id IN` list of 10 and of 100 trace ids is covered at tier
/// 2 on a part wide enough for its leaf route to be no dearer than tier 1:
/// the 36-entry part's tier-1 cost is 2 x 36 = 72 GETs, and the lists charge
/// at most 1 + 10 and 1 + 64 per touched slice. `partsCovered` is 1,
/// `partsOverTierOne` 0, `leafGetRequests` above 0, and the rows equal the
/// fixture's oracle and those of the same ids padded past k = 1,024 with
/// absent ids, a list that carries no key set and is not routed.
///
/// Flip to watch it fail: run it on [`fixture`]'s 4-entry part, whose tier-1
/// cost of 8 is below both lists' worst case. `partsCovered` then reads 0
/// and `partsOverTierOne` 1.
#[tokio::test]
async fn a_spans_trace_id_list_is_covered_at_tier_two() {
    let store = fixture_of(WIDE_OBJECTS).await;
    let app = serve(store);
    let lane = Lane::Spans;
    for n in [10, 100] {
        let v = sql(&app, &lane.in_list(n)).await;
        assert_eq!(rows(&v), oracle(n), "{n} keys: {v}");
        assert_eq!(key_index(&v, "keysProbed"), n as u64, "{n} keys: {v}");
        assert_eq!(key_index(&v, "partsCovered"), 1, "{n} keys: {v}");
        assert_eq!(key_index(&v, "partsOverTierOne"), 0, "{n} keys: {v}");
        assert_eq!(key_index(&v, "partsUncovered"), 0, "{n} keys: {v}");
        assert!(key_index(&v, "leafGetRequests") > 0, "{n} keys: {v}");

        let padded: Vec<String> = (0..n)
            .map(|j| lane.literal(j))
            .chain((0..1_025 - n).map(|i| format!("X'{}'", hex(&trace_id(10_000, i)))))
            .collect();
        assert_eq!(padded.len(), 1_025);
        let unrouted = sql(
            &app,
            &lane.select(&format!("trace_id IN ({})", padded.join(", "))),
        )
        .await;
        assert_eq!(key_index(&unrouted, "keysProbed"), 0, "{unrouted}");
        assert_eq!(key_index(&unrouted, "leafGetRequests"), 0, "{unrouted}");
        assert_eq!(rows(&v), rows(&unrouted), "{n} keys: {v}\n{unrouted}");
    }
}
