//! A spans `trace_id` lookup routes each folded segment through its part's
//! key-index leaves before any data object is read (ADR-3692 decisions 3 and
//! 9, issue #3702): a covering part costs two leaf GETs, an object its leaves
//! prove absent costs no GET at all, and a segment no leaf covers (unfolded,
//! or in a part past the ceiling) falls back to the object's own KEY_IDX
//! behind the 8 KiB footer suffix.
//!
//! The lookups run over `GET /api/traces/<id>` and `POST /api/v1/sql` against
//! a recording store. The expected reads of each object are computed from its
//! bytes with `ravel_rspan`'s ranged reader, and the leaves a lookup may read
//! are taken from the catalog's own resolve, so neither side depends on the
//! fetcher under test.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use clap::Parser;
use ravel_catalog::{
    AbsentPolicy, Catalog, CatalogConfig, Snapshot, SnapshotParts, validate_or_adopt,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::key_index_route::{
    KeyIndexPartOutcome, key_index_leaf_gets_total, key_index_parts_total,
};
use ravel_rspan::footer::{DEFAULT_MAX_SECTION_UNCOMP, kind};
use ravel_rspan::{RspanRangeReader, SpanSegError, TraceKeyProbe};
use ravel_server::Cli;
use ravel_server::query::{build_catalog, build_sql_state};
use ravel_server::store::build_cache;
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantId, TimeRange};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
/// The second part's ingest hour. Far enough past hour 0 that a window
/// starting here, less the two-hour listing lag, no longer reaches hour 0.
const LATER_HOUR: u32 = 4;
/// Late enough that a fold seals [`LATER_HOUR`], whose seal margin is the
/// one-hour flush lifetime plus twenty minutes.
const NOW_NS: i64 = 7 * NS_PER_HOUR;
const SHARDS: u32 = 2;
/// The fold seals a tail of this many entries at the next hour boundary, so
/// three objects an hour over two hours fold into two parts.
const PART_MAX_ENTRIES: usize = 3;
const TENANT: &str = "acme";
const TOKEN: &str = "acme-token";
/// Single-span traces per object. Enough blocks that KEY_IDX and SKIP_IDX lie
/// before the 8 KiB footer suffix, so each is a GET of its own.
const DECOYS: u64 = 2_400;
const BLOCK_RECORDS: usize = 4;
/// The fetcher's footer suffix.
const SUFFIX: u64 = 8 << 10;

/// The route totals are process globals and every test here routes, so the
/// tests run one at a time to read exact deltas.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct FixedClock(i64);
impl ravel_ingest::Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

/// A `MemoryStore` that records every GET's key and range.
struct RecordingStore {
    inner: MemoryStore,
    gets: Mutex<Vec<(String, GetRange)>>,
}

impl RecordingStore {
    /// The ranges GET'd on `key` since the last call, in issue order.
    fn take(&self, key: &str) -> Vec<GetRange> {
        let mut gets = self.gets.lock().expect("get log");
        let (mine, rest): (Vec<_>, Vec<_>) = gets.drain(..).partition(|(k, _)| k == key);
        *gets = rest;
        mine.into_iter().map(|(_, range)| range).collect()
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

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The looked-up trace. Its first byte is low, so it sorts ahead of every
/// decoy and its block sits far from the object's tail.
fn trace() -> [u8; 16] {
    let mut t = [0x40; 16];
    t[0] = 0x01;
    t
}

/// `trace` with byte 7 moved by `delta`: a different key that sorts right
/// beside it, so an object holding both neighbours keeps a SKIP_IDX block
/// whose trace range covers `trace` without holding it, and its KEY_IDX is
/// probed.
fn neighbour(trace: &[u8; 16], delta: i8) -> [u8; 16] {
    let mut t = *trace;
    t[7] = t[7].wrapping_add_signed(delta);
    t
}

fn bracket(trace: &[u8; 16]) -> Vec<([u8; 16], &'static str)> {
    vec![
        (neighbour(trace, -1), "left"),
        (neighbour(trace, 1), "right"),
    ]
}

/// A uniformly spread trace id with its first byte at or above 0x80.
fn decoy(seed: u64) -> [u8; 16] {
    let mut t = [0u8; 16];
    t.copy_from_slice(&blake3::hash(&seed.to_le_bytes()).as_bytes()[..16]);
    t[0] |= 0x80;
    t
}

/// One object to publish: its shard, its ingest hour, and the named spans it
/// carries beside the decoys.
struct ObjectSpec {
    shard: u32,
    hour: u32,
    spans: Vec<([u8; 16], &'static str)>,
}

impl ObjectSpec {
    fn holds(&self, trace: &[u8; 16]) -> bool {
        self.spans.iter().any(|(t, _)| t == trace)
    }
}

struct Published {
    key: String,
    bytes: Bytes,
    holder: bool,
}

/// Writes and commits one RSPAN object; `seq` keeps writer ids and decoys
/// distinct across objects.
async fn publish_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    seq: u64,
    spec: &ObjectSpec,
) -> Published {
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(3_702_000 + u128::from(seq));
    let mut writer = ravel_rspan::RspanWriter::new(
        ravel_rspan::RspanConfig {
            block_target_records: BLOCK_RECORDS,
            ..ravel_rspan::RspanConfig::default()
        },
        ravel_rspan::ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: spec.shard,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    let base = i64::from(spec.hour) * NS_PER_HOUR + 60 * NS_PER_SEC;
    let named = spec
        .spans
        .iter()
        .map(|(trace, name)| (*trace, name.to_string()));
    let decoys = (0..DECOYS).map(|i| (decoy(seq * 1_000_000 + i), "decoy".to_string()));
    let mut count = 0u64;
    let (mut min_ts, mut max_ts) = (i64::MAX, i64::MIN);
    for (i, (trace_id, name)) in named.chain(decoys).enumerate() {
        let start = base + i as i64;
        min_ts = min_ts.min(start);
        max_ts = max_ts.max(start + 1_000);
        count += 1;
        writer.push(ravel_rspan::SpanRecord {
            trace_id,
            span_id: (i as u64 + 1).to_le_bytes(),
            parent_span_id: None,
            name,
            start_ts_ns: start,
            end_ts_ns: start + 1_000,
            status_code: ravel_rspan::StatusCode::Ok,
            status_message: None,
            attrs: vec![("service.name".to_string(), "checkout".to_string())],
        });
    }
    let bytes = writer.finish().expect("finish rspan object");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Spans,
        shard: spec.shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: count,
        series_count: count,
        min_event_ts_ns: min_ts,
        max_event_ts_ns: max_ts,
        min_ingest_ts_ns: min_ts,
        max_ingest_ts_ns: max_ts,
        segment_format_version: u32::from(ravel_ingest::SPAN_SEGMENT_FORMAT_VERSION),
        created_unix_ns: min_ts,
        ingest_hour_bucket: spec.hour,
    })
    .expect("valid span commit record");
    let key = keys::reconstruct_data_key(&rec).expect("data key");
    let bytes = Bytes::from(bytes);
    store
        .put(&key, bytes.clone(), PutOptions::default())
        .await
        .expect("put span data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish span commit");
    Published {
        key,
        bytes,
        holder: spec.holds(&trace()),
    }
}

/// The reads a lookup issues on one object it keeps, per phase.
#[derive(Debug)]
struct ExpectedReads {
    /// The footer suffix.
    probe: Vec<GetRange>,
    /// The object's own KEY_IDX reads (tier 1).
    key_index: Vec<GetRange>,
    /// SKIP_IDX, read only for an object holding the trace.
    skip: GetRange,
    /// The trace's block run, `None` when the object does not hold it.
    scan: Option<GetRange>,
    /// The lowest section offset: every block lies before it.
    blocks_end: u64,
}

impl ExpectedReads {
    /// Tier 1: the suffix, KEY_IDX, and SKIP_IDX and the run where present.
    fn tier1(&self) -> Vec<GetRange> {
        let mut all = self.probe.clone();
        all.extend(self.key_index.iter().copied());
        if let Some(run) = self.scan {
            all.push(self.skip);
            all.push(run);
        }
        all
    }

    /// Tier 2, for an object the leaves name blocks in: the suffix, SKIP_IDX
    /// and the run, no KEY_IDX.
    fn tier2_holder(&self) -> Vec<GetRange> {
        let mut all = self.probe.clone();
        all.push(self.skip);
        all.extend(self.scan);
        all
    }
}

fn len(range: &GetRange) -> u64 {
    match *range {
        GetRange::Range(start, end) => end - start,
        other => panic!("expected a bounded range, got {other:?}"),
    }
}

/// The ranged trace path over `bytes`, recomputed from the format crate.
/// Also asserts the geometry the exact counts rest on: KEY_IDX, SKIP_IDX and
/// the trace's run lie before the footer suffix, so each is a GET of its own,
/// and SKIP_IDX keeps a block for the trace, so tier 1 probes KEY_IDX.
fn expected_reads(bytes: &Bytes, trace: &[u8; 16]) -> ExpectedReads {
    let size = bytes.len() as u64;
    assert_eq!(
        ravel_query::span_fetcher::DEFAULT_TRACE_PROBE_BYTES,
        SUFFIX,
        "the fetcher's first read is the 8 KiB footer suffix"
    );
    let probe_start = size - SUFFIX.min(size);
    let footer = ravel_rspan::open(bytes).expect("footer");
    let skip = *footer.section(kind::SKIP_IDX).expect("SKIP_IDX");
    let key_idx = *footer.section(kind::KEY_IDX).expect("KEY_IDX");
    assert!(
        key_idx.offset + key_idx.len <= probe_start && skip.offset + skip.len <= probe_start,
        "fixture geometry: KEY_IDX and SKIP_IDX lie before the footer suffix at {probe_start}"
    );
    let mut reads = ExpectedReads {
        probe: vec![GetRange::Range(probe_start, size)],
        skip: GetRange::Range(skip.offset, skip.offset + skip.len),
        blocks_end: footer
            .sections
            .iter()
            .map(|s| s.offset)
            .min()
            .expect("sections"),
        key_index: Vec::new(),
        scan: None,
    };
    let skip_raw =
        ravel_rspan::read_section(bytes, &skip, DEFAULT_MAX_SECTION_UNCOMP).expect("SKIP_IDX");
    let reader = RspanRangeReader::from_sections(&footer, &skip_raw).expect("range reader");
    assert!(
        !reader
            .skip_index()
            .candidate_blocks(Some(trace), i64::MIN, i64::MAX, None, None)
            .is_empty(),
        "fixture geometry: SKIP_IDX keeps a block for the trace"
    );
    let answer = reader
        .probe_trace(trace, |r: std::ops::Range<u64>| {
            reads.key_index.push(GetRange::Range(r.start, r.end));
            Ok::<_, SpanSegError>(bytes.slice(r.start as usize..r.end as usize))
        })
        .expect("probe");
    match answer {
        TraceKeyProbe::Absent => {}
        TraceKeyProbe::Present(_) => {
            let run = reader
                .trace_block_span(trace)
                .expect("block span")
                .expect("a present trace has a block run");
            assert!(
                run.end() <= probe_start,
                "fixture geometry: run before probe"
            );
            reads.scan = Some(GetRange::Range(run.start(), run.end()));
        }
        TraceKeyProbe::Declined => panic!("KEY_IDX declined the fixture trace"),
    }
    reads
}

/// Seeds the spans shard generation and publishes `specs`.
async fn publish_all(specs: &[ObjectSpec]) -> (Arc<RecordingStore>, Vec<Published>) {
    let recorder = Arc::new(RecordingStore {
        inner: MemoryStore::new(),
        gets: Mutex::new(Vec::new()),
    });
    let store: Arc<dyn ObjectStoreBackend> = recorder.clone();
    let tenant = TenantId::new(TENANT);
    validate_or_adopt(
        store.as_ref(),
        &tenant.hash(),
        Signal::Spans,
        SHARDS,
        0,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("generation 0");
    let mut published = Vec::new();
    for (seq, spec) in specs.iter().enumerate() {
        published.push(publish_object(store.as_ref(), &tenant, seq as u64, spec).await);
    }
    (recorder, published)
}

/// A catalog whose fold builds key-index leaves and seals a part every
/// [`PART_MAX_ENTRIES`] entries.
fn leaf_catalog(store: Arc<dyn ObjectStoreBackend>) -> Catalog {
    Catalog::new(
        store,
        CatalogConfig {
            shard_count: SHARDS,
            byte_cache_max_bytes: 0,
            build_key_index_leaves: true,
            snapshot_part_max_entries: PART_MAX_ENTRIES,
            ..ravel_server::query::server_catalog_config_base()
        },
    )
    .expect("catalog")
}

/// Folds every published object into parts with leaves.
async fn fold_with_leaves(recorder: &Arc<RecordingStore>, objects: usize) {
    let report = leaf_catalog(recorder.clone())
        .fold(
            &TenantId::new(TENANT).hash(),
            Signal::Spans,
            Uuid::from_u128(1),
            NOW_NS,
            &[],
            None,
        )
        .await
        .expect("fold");
    assert_eq!(
        report.entry_count, objects as u64,
        "every object folds into a part: {report:?}"
    );
}

/// The catalog's own resolve of `range` with its parts.
async fn resolve(recorder: &Arc<RecordingStore>, range: TimeRange) -> (Snapshot, SnapshotParts) {
    let (snapshot, _, parts) = leaf_catalog(recorder.clone())
        .resolve_pruned_with_parts(
            &TenantId::new(TENANT).hash(),
            Signal::Spans,
            range,
            &[],
            NOW_NS,
            None,
            &QueryAccounting::new(),
        )
        .await
        .expect("resolve");
    (snapshot, parts)
}

/// The SQL and trace routes over the recording store, with the process read
/// cache attached and the router's part ceiling lowered to `max_parts`.
fn serve(recorder: &Arc<RecordingStore>, max_parts: Option<usize>) -> Router {
    serve_with(recorder, max_parts, None)
}

/// [`serve`], with the catalog's decoded memory charged to `catalog_budget`
/// when one is given.
fn serve_with(
    recorder: &Arc<RecordingStore>,
    max_parts: Option<usize>,
    catalog_budget: Option<Arc<ravel_memory::MemoryBudget>>,
) -> Router {
    let store: Arc<dyn ObjectStoreBackend> = recorder.clone();
    let cli = Cli::try_parse_from(["ravel-server"]).expect("default flags parse");
    let catalog = build_catalog(
        Arc::clone(&store),
        SHARDS,
        cli.cache.disable_cache,
        ravel_server::config::DEFAULT_CACHE_MAX_BYTES,
        None,
        cli.cache.catalog_resolve_concurrency,
        None,
        cli.resolve_flush_cadence()
            .expect("flush cadence resolves")
            .max_flush_delay,
    )
    .expect("catalog");
    let catalog = match catalog_budget {
        Some(budget) => match Arc::try_unwrap(catalog) {
            Ok(catalog) => Arc::new(catalog.with_memory_budget(budget)),
            Err(_) => panic!("the fresh catalog has one owner"),
        },
        None => catalog,
    };
    let cache = build_cache(&cli, ravel_server::config::DEFAULT_CACHE_MAX_BYTES)
        .expect("cache enabled by default");
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let mut sql_state = build_sql_state(
        catalog,
        store,
        Arc::new(StaticBearerTokenResolver::new(tokens)),
        Some(cache),
        ravel_query::EngineConfig::default(),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
        ravel_server::query::DEFAULT_MAX_QUERY_BYTES,
        ravel_server::query::DEFAULT_MAX_TENANT_BYTES,
        false,
        Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        None,
        Arc::new(ravel_memory::MemoryBudget::unlimited()),
    )
    .expect("sql state");
    sql_state.clock = Arc::new(FixedClock(NOW_NS));
    if max_parts.is_some() {
        let executor = match Arc::try_unwrap(sql_state.executor) {
            Ok(executor) => executor,
            Err(_) => panic!("the fresh SQL state owns its executor"),
        };
        sql_state.executor = Arc::new(executor.with_key_index_max_probed_parts(max_parts));
    }
    ravel_server::sql::router(sql_state.clone()).merge(ravel_server::traces::router(sql_state))
}

async fn send(app: &Router, request: Request<Body>) -> Value {
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot is infallible");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value: Value = serde_json::from_slice(&bytes).expect("body is JSON");
    assert_eq!(status, StatusCode::OK, "request failed: {value}");
    value
}

/// `GET /api/traces/<id>`: `SELECT *` over the whole tenant.
async fn trace_route(app: &Router, trace: &[u8; 16]) -> Value {
    let request = Request::builder()
        .method("GET")
        .uri(format!("/api/traces/{}", hex(trace)))
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .expect("build request");
    send(app, request).await
}

/// `POST /api/v1/sql` with `sql` over `[start_ns, NOW_NS]`.
async fn sql(app: &Router, sql: &str, start_ns: i64) -> Value {
    let payload = serde_json::json!({
        "query": sql,
        "start": start_ns as f64 / NS_PER_SEC as f64,
        "end": NOW_NS as f64 / NS_PER_SEC as f64,
    })
    .to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::from(payload))
        .expect("build request");
    send(app, request).await
}

fn names_where(extra: &str) -> String {
    format!(
        "SELECT name FROM spans WHERE trace_id = '{}'{extra} ORDER BY name",
        hex(&trace())
    )
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

fn pruning(value: &Value, field: &str) -> u64 {
    value["stats"]["pruning"][field]
        .as_u64()
        .unwrap_or_else(|| panic!("stats.pruning.{field} is a u64 in {value}"))
}

/// Field `field` of `stats.keyIndex`, which must be present.
fn key_index(value: &Value, field: &str) -> u64 {
    value["stats"]["keyIndex"][field]
        .as_u64()
        .unwrap_or_else(|| panic!("stats.keyIndex.{field} is a u64 in {value}"))
}

/// `stats.keyIndex` is the spans entry's `keyIndex` in `stats.signals`, the
/// statement's only signal.
fn assert_signal_key_index_matches(value: &Value) {
    let signals = value["stats"]["signals"].as_array().expect("signals");
    assert_eq!(signals.len(), 1, "{value}");
    assert_eq!(signals[0]["table"], "spans", "{value}");
    assert_eq!(
        signals[0]["keyIndex"], value["stats"]["keyIndex"],
        "{value}"
    );
}

/// The route totals the `/metrics` families render, for spans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Totals {
    covered: u64,
    uncovered: u64,
    beyond_ceiling: u64,
    leaf_gets: u64,
}

fn totals() -> Totals {
    let parts = |outcome| key_index_parts_total(Signal::Spans, outcome);
    Totals {
        covered: parts(KeyIndexPartOutcome::Covered),
        uncovered: parts(KeyIndexPartOutcome::Uncovered),
        beyond_ceiling: parts(KeyIndexPartOutcome::BeyondCeiling),
        leaf_gets: key_index_leaf_gets_total(Signal::Spans),
    }
}

fn delta(before: Totals, after: Totals) -> Totals {
    Totals {
        covered: after.covered - before.covered,
        uncovered: after.uncovered - before.uncovered,
        beyond_ceiling: after.beyond_ceiling - before.beyond_ceiling,
        leaf_gets: after.leaf_gets - before.leaf_gets,
    }
}

/// Three objects in each of hours 0 and [`LATER_HOUR`], so two parts; the trace is held
/// by one object in each part, and every other object brackets it so its
/// own KEY_IDX would be probed at tier 1.
fn two_part_specs() -> Vec<ObjectSpec> {
    let t = trace();
    vec![
        ObjectSpec {
            shard: 0,
            hour: 0,
            spans: bracket(&t),
        },
        ObjectSpec {
            shard: 1,
            hour: 0,
            spans: vec![(t, "t-0"), (t, "t-1")],
        },
        ObjectSpec {
            shard: 1,
            hour: 0,
            spans: bracket(&t),
        },
        ObjectSpec {
            shard: 0,
            hour: LATER_HOUR,
            spans: vec![(t, "t-2")],
        },
        ObjectSpec {
            shard: 0,
            hour: LATER_HOUR,
            spans: bracket(&t),
        },
        ObjectSpec {
            shard: 1,
            hour: LATER_HOUR,
            spans: bracket(&t),
        },
    ]
}

/// Per part of `parts`: its leaf keys and the data keys of its segments.
fn part_layout(
    snapshot: &Snapshot,
    parts: &SnapshotParts,
) -> Vec<(BTreeSet<String>, BTreeSet<String>)> {
    let mut layout: Vec<(BTreeSet<String>, BTreeSet<String>)> = parts
        .parts
        .iter()
        .map(|p| {
            (
                p.key_index.iter().map(|r| r.key.clone()).collect(),
                BTreeSet::new(),
            )
        })
        .collect();
    for (segment, origin) in snapshot.segments.iter().zip(&parts.origin) {
        let origin = origin.expect("every segment is sealed into a part");
        layout[origin.part as usize]
            .1
            .insert(segment.data_object_key.clone());
    }
    layout
}

/// The folded lookup routes every segment through its part's leaves: each
/// covering part costs exactly two leaf GETs, the four objects the leaves
/// prove absent are never read (the recording store logs no GET on them, and
/// the probe and keyIndex phase totals hold only the holders' and the leaves'
/// GETs), and the two holders read their suffix, SKIP_IDX and run with no
/// KEY_IDX. Both routes return the rows an unfolded lookup returns, and a
/// second run is served from the read cache with the same routing counts.
///
/// FLIP: routing `SegmentRoute::Uncovered` as `Pruned` in the spans provider
/// empties the unfolded baseline, which must hold three rows. Pushing `None`
/// for a `Blocks` survivor's set in the spans provider reads each holder's
/// KEY_IDX, which the holder's read list here rejects.
#[tokio::test]
async fn folded_lookup_reads_two_leaf_gets_and_no_pruned_object() {
    let _serial = SERIAL.lock().await;
    let specs = two_part_specs();
    let (recorder, objects) = publish_all(&specs).await;
    let t = trace();
    let want: Vec<ExpectedReads> = objects
        .iter()
        .map(|o| expected_reads(&o.bytes, &t))
        .collect();
    for (object, want) in objects.iter().zip(&want) {
        assert_eq!(want.scan.is_some(), object.holder, "{}", object.key);
    }

    // The unfolded answer, from a server that sees no part.
    let unfolded = serve(&recorder, None);
    let unfolded_rows = trace_route(&unfolded, &t).await["data"].clone();
    assert_eq!(
        unfolded_rows["rows"].as_array().map(Vec::len),
        Some(3),
        "{unfolded_rows}"
    );

    fold_with_leaves(&recorder, specs.len()).await;
    let (snapshot, parts) = resolve(
        &recorder,
        TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        },
    )
    .await;
    let layout = part_layout(&snapshot, &parts);
    assert_eq!(layout.len(), 2, "two parts: {layout:?}");
    for (leaves, segments) in &layout {
        assert!(!leaves.is_empty(), "every part has a trace_id leaf");
        assert_eq!(segments.len(), PART_MAX_ENTRIES);
        assert_eq!(
            objects
                .iter()
                .filter(|o| o.holder && segments.contains(&o.key))
                .count(),
            1,
            "one holder per part"
        );
    }
    let leaf_keys: BTreeSet<String> = layout.iter().flat_map(|(l, _)| l.clone()).collect();

    let app = serve(&recorder, None);
    recorder.clear();
    let before = totals();
    let folded = trace_route(&app, &t).await;
    let moved = delta(before, totals());
    assert_eq!(
        folded["data"], unfolded_rows,
        "rows equal the unfolded answer"
    );

    for (object, want) in objects.iter().zip(&want) {
        let got = recorder.take(&object.key);
        if object.holder {
            assert_eq!(
                got,
                want.tier2_holder(),
                "{}: the suffix, SKIP_IDX and the trace's run, no KEY_IDX",
                object.key
            );
        } else {
            assert!(
                got.is_empty(),
                "{}: a pruned object is never read: {got:?}",
                object.key
            );
        }
    }
    for (leaves, _) in &layout {
        let gets: usize = leaves.iter().map(|k| recorder.take(k).len()).sum();
        assert_eq!(gets, 2, "two leaf GETs per covering part: {leaves:?}");
    }

    let holders: Vec<&ExpectedReads> = objects
        .iter()
        .zip(&want)
        .filter(|(o, _)| o.holder)
        .map(|(_, w)| w)
        .collect();
    assert_eq!(key_index(&folded, "partsCovered"), 2, "{folded}");
    assert_eq!(key_index(&folded, "partsUncovered"), 0, "{folded}");
    assert_eq!(key_index(&folded, "partsBeyondCeiling"), 0, "{folded}");
    assert_eq!(key_index(&folded, "leafGetRequests"), 4, "{folded}");
    assert_eq!(key_index(&folded, "keysProbed"), 1, "{folded}");
    assert_eq!(key_index(&folded, "skipIdxDropped"), 0, "{folded}");
    assert_signal_key_index_matches(&folded);
    // The keyIndex phase carries the leaf GETs and nothing else: no object's
    // KEY_IDX was read.
    assert_eq!(phase(&folded, "keyIndex", "s3GetRequests"), 4, "{folded}");
    assert_eq!(
        phase(&folded, "keyIndex", "s3GetWireBytes"),
        key_index(&folded, "leafWireBytes"),
        "{folded}"
    );
    // The probe phase holds the holders' suffix and SKIP_IDX only: a probe
    // GET on a pruned object would move it.
    assert_eq!(phase(&folded, "probe", "s3GetRequests"), 4, "{folded}");
    assert_eq!(
        phase(&folded, "probe", "s3GetWireBytes"),
        holders
            .iter()
            .map(|w| len(&w.probe[0]) + len(&w.skip))
            .sum::<u64>(),
        "{folded}"
    );
    assert_eq!(phase(&folded, "probe", "segmentsOpened"), 2, "{folded}");
    assert_eq!(phase(&folded, "scan", "s3GetRequests"), 2, "{folded}");
    assert_eq!(pruning(&folded, "segments"), 6, "{folded}");
    // Covered segments (six) minus holders (two).
    assert_eq!(pruning(&folded, "segmentsPrunedByKeyIndex"), 4, "{folded}");
    assert_eq!(
        moved,
        Totals {
            covered: 2,
            uncovered: 0,
            beyond_ceiling: 0,
            leaf_gets: 4,
        }
    );

    // The SQL surface routes the same way.
    let by_sql = sql(&app, &names_where(""), 0).await;
    assert_eq!(
        by_sql["data"]["rows"],
        serde_json::json!([["t-0"], ["t-1"], ["t-2"]]),
        "{by_sql}"
    );
    assert_eq!(key_index(&by_sql, "partsCovered"), 2, "{by_sql}");
    assert_eq!(pruning(&by_sql, "segmentsPrunedByKeyIndex"), 4, "{by_sql}");
    assert_eq!(phase(&by_sql, "probe", "segmentsOpened"), 2, "{by_sql}");

    // A second run reads no data object and no leaf from the store.
    recorder.clear();
    let warm = trace_route(&app, &t).await;
    for key in objects.iter().map(|o| &o.key).chain(&leaf_keys) {
        assert_eq!(
            recorder.take(key),
            Vec::new(),
            "{key}: served from the cache"
        );
    }
    assert_eq!(warm["data"], unfolded_rows);
    assert_eq!(key_index(&warm, "partsCovered"), 2, "{warm}");
    assert_eq!(key_index(&warm, "leafGetRequests"), 0, "{warm}");
    assert_eq!(pruning(&warm, "segmentsPrunedByKeyIndex"), 4, "{warm}");
    assert_eq!(phase(&warm, "probe", "segmentsOpened"), 2, "{warm}");
    assert_eq!(phase(&warm, "probe", "s3GetRequests"), 0, "{warm}");
    assert_eq!(phase(&warm, "scan", "s3GetRequests"), 0, "{warm}");
}

/// An unfolded tenant has no part, so every segment is uncovered and probes
/// its own KEY_IDX: an object KEY_IDX proves absent costs exactly three GETs
/// (the 8 KiB footer suffix and two KEY_IDX reads), under 16 KiB in all and
/// no SKIP_IDX read, while the holder pays SKIP_IDX and its block run.
/// `stats.keyIndex` is present with no part figure moved. With a duration
/// bound no block of the holder can meet, the holder's KEY_IDX blocks lie
/// wholly outside the SKIP_IDX window and it is counted in `skipIdxDropped`
/// with no block read.
///
/// FLIP: an `Uncovered` segment routed as `Pruned` in the spans provider
/// returns no rows here, and `build_stats_json` rendering `keyIndex` only
/// once a part or leaf figure moved leaves `stats.keyIndex` absent here.
#[tokio::test]
async fn unfolded_absent_object_costs_three_gets_under_sixteen_kib_and_no_skip_idx_read() {
    let _serial = SERIAL.lock().await;
    let t = trace();
    let specs = vec![
        ObjectSpec {
            shard: 0,
            hour: 0,
            spans: bracket(&t),
        },
        ObjectSpec {
            shard: 1,
            hour: 0,
            spans: vec![(t, "t-0"), (t, "t-1")],
        },
        ObjectSpec {
            shard: 1,
            hour: 1,
            spans: bracket(&t),
        },
    ];
    let (recorder, objects) = publish_all(&specs).await;
    let want: Vec<ExpectedReads> = objects
        .iter()
        .map(|o| expected_reads(&o.bytes, &t))
        .collect();
    let app = serve(&recorder, None);

    recorder.clear();
    let before = totals();
    let value = sql(&app, &names_where(""), 0).await;
    assert_eq!(
        delta(before, totals()),
        delta(before, before),
        "no part to route"
    );
    assert_eq!(
        value["data"]["rows"],
        serde_json::json!([["t-0"], ["t-1"]]),
        "{value}"
    );
    for (object, want) in objects.iter().zip(&want) {
        let got = recorder.take(&object.key);
        assert_eq!(got, want.tier1(), "{}", object.key);
        if object.holder {
            assert!(
                got.contains(&want.skip),
                "{}: the holder reads SKIP_IDX",
                object.key
            );
            continue;
        }
        assert_eq!(want.key_index.len(), 2, "fixture: KEY_IDX is two reads");
        assert_eq!(got.len(), 3, "{}: {got:?}", object.key);
        assert!(
            len(&got[0]) <= SUFFIX,
            "{}: probe wire {}",
            object.key,
            len(&got[0])
        );
        assert!(
            got.iter().map(len).sum::<u64>() < 16 << 10,
            "{}: {got:?}",
            object.key
        );
        assert!(
            !got.contains(&want.skip),
            "{}: no SKIP_IDX read",
            object.key
        );
        assert!(
            got.iter()
                .all(|r| matches!(*r, GetRange::Range(s, _) if s >= want.blocks_end)),
            "{}: no block read",
            object.key
        );
    }
    for field in [
        "partsCovered",
        "partsUncovered",
        "partsBeyondCeiling",
        "leavesRejected",
        "reservationsRefused",
        "leafGetRequests",
        "leafWireBytes",
        "skippedByBudget",
        "skipIdxDropped",
    ] {
        assert_eq!(key_index(&value, field), 0, "{field}: {value}");
    }
    assert_eq!(key_index(&value, "keysProbed"), 1, "{value}");
    assert_signal_key_index_matches(&value);
    assert_eq!(pruning(&value, "segmentsPrunedByKeyIndex"), 2, "{value}");

    // Every fixture span lasts 1 us, so a 5 us floor keeps no block. A fresh
    // server, so no read is served from the first lookup's cache.
    let app = serve(&recorder, None);
    recorder.clear();
    let dropped = sql(&app, &names_where(" AND duration_ns > 5000"), 0).await;
    assert_eq!(dropped["data"]["rows"], serde_json::json!([]), "{dropped}");
    assert_eq!(key_index(&dropped, "skipIdxDropped"), 1, "{dropped}");
    for (object, want) in objects.iter().zip(&want) {
        let got = recorder.take(&object.key);
        let mut expect = want.probe.clone();
        expect.extend(want.key_index.iter().copied());
        if object.holder {
            expect.push(want.skip);
        }
        assert_eq!(got, expect, "{}: no block read", object.key);
    }
}

/// With the part ceiling lowered to one, the router probes one part's leaves
/// and leaves the other to tier 1: `partsBeyondCeiling` is 1, the covered
/// part's non-holder is never read, and each object of the part past the
/// ceiling reads its own suffix and KEY_IDX, its non-holder no block.
#[tokio::test]
async fn parts_past_the_ceiling_are_probed_at_tier_one() {
    let _serial = SERIAL.lock().await;
    let specs = two_part_specs();
    let (recorder, objects) = publish_all(&specs).await;
    let t = trace();
    let want: Vec<ExpectedReads> = objects
        .iter()
        .map(|o| expected_reads(&o.bytes, &t))
        .collect();
    fold_with_leaves(&recorder, specs.len()).await;
    let (snapshot, parts) = resolve(
        &recorder,
        TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        },
    )
    .await;
    let layout = part_layout(&snapshot, &parts);
    assert_eq!(layout.len(), 2);

    let app = serve(&recorder, Some(1));
    recorder.clear();
    let before = totals();
    let value = trace_route(&app, &t).await;
    let moved = delta(before, totals());
    assert_eq!(
        value["data"]["rows"].as_array().map(Vec::len),
        Some(3),
        "{value}"
    );
    assert_eq!(key_index(&value, "partsCovered"), 1, "{value}");
    assert_eq!(key_index(&value, "partsBeyondCeiling"), 1, "{value}");
    assert_eq!(key_index(&value, "leafGetRequests"), 2, "{value}");
    assert_eq!(
        moved,
        Totals {
            covered: 1,
            uncovered: 0,
            beyond_ceiling: 1,
            leaf_gets: 2,
        }
    );

    let leaf_gets: Vec<usize> = layout
        .iter()
        .map(|(leaves, _)| leaves.iter().map(|k| recorder.take(k).len()).sum())
        .collect();
    let covered = leaf_gets
        .iter()
        .position(|&n| n == 2)
        .unwrap_or_else(|| panic!("one part read two leaf GETs: {leaf_gets:?}"));
    assert_eq!(leaf_gets.iter().sum::<usize>(), 2, "{leaf_gets:?}");
    for (object, want) in objects.iter().zip(&want) {
        let got = recorder.take(&object.key);
        let in_covered = layout[covered].1.contains(&object.key);
        match (in_covered, object.holder) {
            (true, true) => assert_eq!(got, want.tier2_holder(), "{}", object.key),
            (true, false) => assert!(got.is_empty(), "{}: pruned: {got:?}", object.key),
            (false, _) => {
                assert_eq!(got, want.tier1(), "{}: tier 1", object.key);
                if !object.holder {
                    assert!(
                        got.iter()
                            .all(|r| matches!(*r, GetRange::Range(s, _) if s >= want.blocks_end)),
                        "{}: a non-holder past the ceiling reads no block",
                        object.key
                    );
                }
            }
        }
    }
    // Two pruned by the covered part's leaves, two by their own KEY_IDX.
    assert_eq!(pruning(&value, "segmentsPrunedByKeyIndex"), 4, "{value}");
}

/// The resolve hands each part's handle its own leaf refs, each bound to that
/// part's blake3, and a range-scoped resolve that drops the leading part
/// renumbers the origins into the parts it loaded: the lookup over that
/// window reads only the remaining part's leaves and finds its span.
#[tokio::test]
async fn a_range_scoped_resolve_routes_through_the_remaining_parts_own_leaves() {
    let _serial = SERIAL.lock().await;
    let specs = two_part_specs();
    let (recorder, objects) = publish_all(&specs).await;
    fold_with_leaves(&recorder, specs.len()).await;

    let (full_snapshot, full) = resolve(
        &recorder,
        TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        },
    )
    .await;
    assert_eq!(full.parts.len(), 2);
    for part in &full.parts {
        assert!(!part.key_index.is_empty());
        for leaf in &part.key_index {
            assert_eq!(
                leaf.part_blake3, part.blake3,
                "{}: bound to its own part",
                leaf.key
            );
        }
    }
    let full_layout = part_layout(&full_snapshot, &full);
    assert!(
        full_layout[0].0.is_disjoint(&full_layout[1].0),
        "siblings share no leaf: {full_layout:?}"
    );

    let window = TimeRange {
        start_ns: i64::from(LATER_HOUR) * NS_PER_HOUR,
        end_ns: NOW_NS,
    };
    let (snapshot, scoped) = resolve(&recorder, window).await;
    assert_eq!(
        scoped.parts.len(),
        1,
        "the hour-0 part is dropped: watermarks {:?}",
        full.parts
            .iter()
            .map(|p| p.watermark_hour)
            .collect::<Vec<_>>()
    );
    assert_eq!(scoped.parts[0].blake3, full.parts[1].blake3);
    assert_eq!(scoped.parts[0].key_index, full.parts[1].key_index);
    let layout = part_layout(&snapshot, &scoped);
    assert_eq!(
        layout[0].1, full_layout[1].1,
        "origins renumbered to part 0"
    );

    let app = serve(&recorder, None);
    recorder.clear();
    let value = sql(&app, &names_where(""), window.start_ns).await;
    assert_eq!(
        value["data"]["rows"],
        serde_json::json!([["t-2"]]),
        "{value}"
    );
    assert_eq!(key_index(&value, "partsCovered"), 1, "{value}");
    assert_eq!(key_index(&value, "leafGetRequests"), 2, "{value}");
    let scoped_gets: usize = full_layout[1]
        .0
        .iter()
        .map(|k| recorder.take(k).len())
        .sum();
    let dropped_gets: usize = full_layout[0]
        .0
        .iter()
        .map(|k| recorder.take(k).len())
        .sum();
    assert_eq!((scoped_gets, dropped_gets), (2, 0));
    for object in objects.iter().filter(|o| full_layout[0].1.contains(&o.key)) {
        assert_eq!(recorder.take(&object.key), Vec::new(), "{}", object.key);
    }
}

/// A statement with no trace key routes nothing: `stats.keyIndex` and the
/// spans entry's `keyIndex` are absent and the route totals do not move.
#[tokio::test]
async fn key_index_stats_are_absent_without_a_trace_key() {
    let _serial = SERIAL.lock().await;
    let specs = two_part_specs();
    let (recorder, _) = publish_all(&specs).await;
    fold_with_leaves(&recorder, specs.len()).await;
    let app = serve(&recorder, None);
    let before = totals();
    let value = sql(
        &app,
        "SELECT count(*) AS n FROM spans WHERE name = 't-2'",
        0,
    )
    .await;
    assert_eq!(value["data"]["rows"], serde_json::json!([[1]]), "{value}");
    assert_eq!(delta(before, totals()), delta(before, before));
    assert!(value["stats"].get("keyIndex").is_none(), "{value}");
    let signals = value["stats"]["signals"].as_array().expect("signals");
    assert_eq!(signals.len(), 1, "{value}");
    assert!(signals[0].get("keyIndex").is_none(), "{value}");
}

/// The columnar scan path (a `SELECT name` projection, no attrs, events or
/// links) takes the router's block sets as the row path does: on a fresh
/// server, so nothing is served from a cache, each holder reads its suffix,
/// SKIP_IDX and the trace's run with no KEY_IDX, each object the leaves prove
/// absent is never read, and the keyIndex phase holds exactly the four leaf
/// GETs.
///
/// FLIP: passing `None` for `seg.blocks.as_deref()` in `prepare_columnar`'s
/// `fetch_accounted_columnar_in_blocks` call (crates/ravel-sql/src/
/// spans_scan.rs) reads each holder's KEY_IDX, which the holder's read list
/// and the keyIndex phase count here reject.
#[tokio::test]
async fn a_cold_columnar_lookup_reads_only_the_routed_blocks() {
    let _serial = SERIAL.lock().await;
    let specs = two_part_specs();
    let (recorder, objects) = publish_all(&specs).await;
    let t = trace();
    let want: Vec<ExpectedReads> = objects
        .iter()
        .map(|o| expected_reads(&o.bytes, &t))
        .collect();
    fold_with_leaves(&recorder, specs.len()).await;
    let (snapshot, parts) = resolve(
        &recorder,
        TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        },
    )
    .await;
    let layout = part_layout(&snapshot, &parts);
    assert_eq!(layout.len(), 2, "two parts: {layout:?}");

    let app = serve(&recorder, None);
    recorder.clear();
    let before = totals();
    let value = sql(&app, &names_where(""), 0).await;
    let moved = delta(before, totals());
    assert_eq!(
        value["data"]["rows"],
        serde_json::json!([["t-0"], ["t-1"], ["t-2"]]),
        "{value}"
    );
    for (object, want) in objects.iter().zip(&want) {
        let got = recorder.take(&object.key);
        if object.holder {
            assert_eq!(
                got,
                want.tier2_holder(),
                "{}: the suffix, SKIP_IDX and the trace's run, no KEY_IDX",
                object.key
            );
        } else {
            assert!(
                got.is_empty(),
                "{}: a pruned object is never read: {got:?}",
                object.key
            );
        }
    }
    for (leaves, _) in &layout {
        let gets: usize = leaves.iter().map(|k| recorder.take(k).len()).sum();
        assert_eq!(gets, 2, "two leaf GETs per covering part: {leaves:?}");
    }
    assert_eq!(key_index(&value, "partsCovered"), 2, "{value}");
    assert_eq!(key_index(&value, "leafGetRequests"), 4, "{value}");
    assert_eq!(phase(&value, "keyIndex", "s3GetRequests"), 4, "{value}");
    assert_eq!(
        phase(&value, "keyIndex", "s3GetWireBytes"),
        key_index(&value, "leafWireBytes"),
        "{value}"
    );
    assert_eq!(phase(&value, "probe", "s3GetRequests"), 4, "{value}");
    assert_eq!(phase(&value, "probe", "segmentsOpened"), 2, "{value}");
    assert_eq!(phase(&value, "scan", "s3GetRequests"), 2, "{value}");
    assert_eq!(pruning(&value, "segmentsPrunedByKeyIndex"), 4, "{value}");
    assert_eq!(
        moved,
        Totals {
            covered: 2,
            uncovered: 0,
            beyond_ceiling: 0,
            leaf_gets: 4,
        }
    );
}

/// The `/metrics` router over `catalog`, wired as `ravel_server::start`
/// wires it in `Mode::All`.
fn metrics_router(catalog: Arc<Catalog>) -> Router {
    use ravel_ingest::{AdmissionController, AdmissionLimits, SystemClock};
    use ravel_server::metrics::{MetricsState, QueryAccountingMetrics};

    ravel_server::metrics::router(MetricsState {
        mode: ravel_server::Mode::All,
        store_metrics: Arc::new(ravel_object_store::StoreMetrics::default()),
        ingest_router: None,
        log_ingest_router: None,
        span_ingest_router: None,
        catalog,
        tenant_discovery: None,
        maintenance_safety: None,
        maintenance_ownership: None,
        merge_memory: None,
        scrub: None,
        cache_metrics: None,
        cache_disk_metrics: None,
        catalog_cache_metrics: None,
        catalog_cache_disk_metrics: None,
        admission: Arc::new(AdmissionController::new(
            Arc::new(SystemClock),
            AdmissionLimits::default(),
        )),
        reconcile_cycle: Arc::new(
            ravel_server::admission_reconcile::ReconcileCycleMetrics::default(),
        ),
        metrics_tenant_labels: false,
        query_accounting: Arc::new(QueryAccountingMetrics::new(std::collections::HashSet::new())),
        metrics_tenant_allowlist: Arc::new(std::collections::HashSet::new()),
        ingest_buffer_budget: ravel_ingest::IngestByteBudget::shared(
            ravel_ingest::IngestByteBudgetLimit::Unlimited,
        ),
        ingest_concurrency: ravel_server::ingest_concurrency::IngestConcurrencyController::shared(
            ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(1024),
        ),
        distrib: None,
        #[cfg(feature = "flight-sql")]
        sql_slice_rejects: None,
        #[cfg(feature = "flight-sql")]
        sql_slice_tls_dials: None,
        #[cfg(feature = "agents")]
        agents: None,
        durable_auth: None,
        ingest_byte_metrics: Arc::new(ravel_server::ingest_byte_metrics::IngestByteMetrics::new()),
        normalize_reject_metrics: Arc::new(
            ravel_server::normalize_reject_metrics::NormalizeRejectMetrics::new(),
        ),
        metadata_cache: None,
        cache: None,
        cache_max_bytes: 0,
        catalog_cache_max_bytes: 0,
        audit_pipeline: None,
        process_memory_budget: Arc::new(ravel_memory::MemoryBudget::unlimited()),
        process_memory_budget_is_fallback: false,
        cpu_gates: ravel_server::cpu_gates::CpuGates::new(Default::default()),
        can_fold: true,
        fold_loop: Default::default(),
        refold: Default::default(),
        heartbeat: ravel_server::health_listener::Heartbeat::new(Arc::new(SystemClock)),
        decoded_cache: None,
    })
}

async fn scrape(app: &Router) -> String {
    let request = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .expect("build request");
    let response = app.clone().oneshot(request).await.expect("oneshot");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    String::from_utf8(bytes.to_vec()).expect("utf8 metrics")
}

const PARTS_HELP: &str = "# HELP ravel_query_key_index_parts_total Catalog parts the key-index router settled for an exact-key lookup or a PromQL equality value, by signal and outcome: covered parts answered from their leaves; every other outcome sends the part's segments to the per-object probe.";
const LEAF_GETS_HELP: &str = "# HELP ravel_query_key_index_leaf_gets_total Key-index leaf GETs the router issued for exact-key lookups and PromQL equality values, by signal, including those of a routing pass dropped before it finished.";

/// The lines of `scrape` equal to `line`.
fn count_lines(scrape: &str, line: &str) -> usize {
    scrape.lines().filter(|l| *l == line).count()
}

/// The value of the one sample line of `scrape` that starts with `prefix`
/// followed by a space.
fn sample(scrape: &str, prefix: &str) -> u64 {
    let lines: Vec<&str> = scrape
        .lines()
        .filter(|l| l.strip_prefix(prefix).is_some_and(|v| v.starts_with(' ')))
        .collect();
    assert_eq!(lines.len(), 1, "{prefix}: {lines:?}");
    lines[0][prefix.len() + 1..]
        .parse()
        .unwrap_or_else(|e| panic!("{}: {e}", lines[0]))
}

/// After a routed lookup, the real `GET /metrics` renders both key-index
/// router families with the HELP text docs/guides/observability.md quotes,
/// one TYPE line each, every outcome once per signal, and the values the
/// route totals hold, the lookup's two covered parts and four leaf GETs
/// included.
///
/// FLIP: removing the `render_key_index_route_family(...)` call from
/// `render()` in services/ravel-server/src/metrics.rs drops both families,
/// and the HELP line count here reads 0.
#[tokio::test]
async fn the_metrics_page_renders_both_key_index_route_families_after_a_lookup() {
    let _serial = SERIAL.lock().await;
    let specs = two_part_specs();
    let (recorder, _) = publish_all(&specs).await;
    fold_with_leaves(&recorder, specs.len()).await;
    let app = serve(&recorder, None);
    let metrics = metrics_router(Arc::new(leaf_catalog(recorder.clone())));

    let before = totals();
    let value = trace_route(&app, &trace()).await;
    assert_eq!(key_index(&value, "partsCovered"), 2, "{value}");
    let page = scrape(&metrics).await;
    let after = totals();
    assert_eq!(
        delta(before, after),
        Totals {
            covered: 2,
            uncovered: 0,
            beyond_ceiling: 0,
            leaf_gets: 4,
        }
    );

    assert_eq!(count_lines(&page, PARTS_HELP), 1, "{page}");
    assert_eq!(
        count_lines(&page, "# TYPE ravel_query_key_index_parts_total counter"),
        1,
        "{page}"
    );
    assert_eq!(count_lines(&page, LEAF_GETS_HELP), 1, "{page}");
    assert_eq!(
        count_lines(
            &page,
            "# TYPE ravel_query_key_index_leaf_gets_total counter"
        ),
        1,
        "{page}"
    );
    let parts_samples = page
        .lines()
        .filter(|l| l.starts_with("ravel_query_key_index_parts_total{"))
        .count();
    assert_eq!(parts_samples, 3 * KeyIndexPartOutcome::ALL.len(), "{page}");
    let leaf_samples = page
        .lines()
        .filter(|l| l.starts_with("ravel_query_key_index_leaf_gets_total{"))
        .count();
    assert_eq!(leaf_samples, 3, "{page}");
    for signal in [Signal::Spans, Signal::Logs, Signal::Metrics] {
        let name = match signal {
            Signal::Spans => "spans",
            Signal::Logs => "logs",
            _ => "metrics",
        };
        for outcome in KeyIndexPartOutcome::ALL {
            let got = sample(
                &page,
                &format!(
                    "ravel_query_key_index_parts_total{{mode=\"all\",signal=\"{name}\",outcome=\"{}\"}}",
                    outcome.as_str()
                ),
            );
            assert_eq!(
                got,
                key_index_parts_total(signal, outcome),
                "{name} {outcome:?}"
            );
        }
        assert_eq!(
            sample(
                &page,
                &format!("ravel_query_key_index_leaf_gets_total{{mode=\"all\",signal=\"{name}\"}}"),
            ),
            key_index_leaf_gets_total(signal),
            "{name}"
        );
    }
    assert_eq!(
        sample(
            &page,
            "ravel_query_key_index_parts_total{mode=\"all\",signal=\"spans\",outcome=\"covered\"}",
        ),
        after.covered
    );
    assert!(after.covered >= 2 && after.leaf_gets >= 4, "{after:?}");
}

/// Three unfolded objects, one holding two of the trace's spans, under a
/// catalog budget the resolve fits and the parts' handles do not.
async fn budget_fixture() -> (Arc<RecordingStore>, Vec<Published>, Router) {
    let t = trace();
    let specs = vec![
        ObjectSpec {
            shard: 0,
            hour: 0,
            spans: bracket(&t),
        },
        ObjectSpec {
            shard: 1,
            hour: 0,
            spans: vec![(t, "t-0"), (t, "t-1")],
        },
        ObjectSpec {
            shard: 1,
            hour: 1,
            spans: bracket(&t),
        },
    ];
    let (recorder, objects) = publish_all(&specs).await;
    let budget = || Arc::new(ravel_memory::MemoryBudget::new(1));
    let window = TimeRange {
        start_ns: 0,
        end_ns: NOW_NS,
    };
    let catalog = leaf_catalog(recorder.clone()).with_memory_budget(budget());
    let tenant = TenantId::new(TENANT).hash();
    let refused = catalog
        .resolve_pruned_with_parts(
            &tenant,
            Signal::Spans,
            window,
            &[],
            NOW_NS,
            None,
            &QueryAccounting::new(),
        )
        .await;
    assert!(
        matches!(
            refused,
            Err(ravel_catalog::CatalogError::MemoryExhausted(_))
        ),
        "the parts' charge exceeds the budget: {:?}",
        refused.map(|(snapshot, _, parts)| (snapshot.segments.len(), parts.heap_bytes()))
    );
    let (snapshot, _) = catalog
        .resolve_pruned_with_admission(
            &tenant,
            Signal::Spans,
            window,
            &[],
            NOW_NS,
            None,
            &QueryAccounting::new(),
        )
        .await
        .expect("the resolve alone fits the budget");
    assert_eq!(snapshot.segments.len(), 3);
    let app = serve_with(&recorder, None, Some(budget()));
    (recorder, objects, app)
}

/// A spans statement with no trace key never needs the router's parts, and a
/// refused charge for them does not fail it.
///
/// FLIP: returning the `resolve_pruned_with_parts` error as before (the
/// `Err(CatalogError::MemoryExhausted(_)) => None` arm in
/// crates/ravel-sql/src/executor.rs re-raised) fails this statement with a
/// memory error.
#[tokio::test]
async fn a_spans_statement_without_a_trace_key_survives_a_refused_parts_charge() {
    let _serial = SERIAL.lock().await;
    let (_recorder, _, app) = budget_fixture().await;
    let value = sql(
        &app,
        "SELECT name FROM spans WHERE name LIKE 't-%' ORDER BY name",
        0,
    )
    .await;
    assert_eq!(
        value["data"]["rows"],
        serde_json::json!([["t-0"], ["t-1"]]),
        "{value}"
    );
    assert!(value["stats"].get("keyIndex").is_none(), "{value}");
}

/// A trace lookup whose parts' charge is refused runs at tier 1 and answers
/// exactly: every object reads its suffix and its own KEY_IDX, the holder its
/// SKIP_IDX and run too, the router does not run (`stats.keyIndex` reads
/// `keysProbed` 0 with every part figure 0, and no route total moves), and
/// the rows equal the routed answer.
///
/// FLIP: the same re-raised arm as above fails this lookup with a memory
/// error instead of degrading it.
#[tokio::test]
async fn a_trace_lookup_degrades_to_tier_one_on_a_refused_parts_charge() {
    let _serial = SERIAL.lock().await;
    let (recorder, objects, app) = budget_fixture().await;
    let t = trace();
    recorder.clear();
    let before = totals();
    let value = sql(&app, &names_where(""), 0).await;
    assert_eq!(delta(before, totals()), delta(before, before));
    assert_eq!(
        value["data"]["rows"],
        serde_json::json!([["t-0"], ["t-1"]]),
        "{value}"
    );
    // The router did not run: no key was probed against a leaf, and no part
    // figure moved.
    for field in [
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
    ] {
        assert_eq!(key_index(&value, field), 0, "{field}: {value}");
    }
    let mut key_idx_gets = 0;
    for object in &objects {
        let want = expected_reads(&object.bytes, &t);
        key_idx_gets += want.key_index.len() as u64;
        assert_eq!(recorder.take(&object.key), want.tier1(), "{}", object.key);
    }
    assert_eq!(
        phase(&value, "keyIndex", "s3GetRequests"),
        key_idx_gets,
        "{value}"
    );
    assert_eq!(pruning(&value, "segmentsPrunedByKeyIndex"), 2, "{value}");
}
