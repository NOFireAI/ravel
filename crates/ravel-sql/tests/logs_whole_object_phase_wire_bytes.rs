//! Issue #2130: a logs statement whose scan reads every object whole must
//! charge those GETs to the scan phase of the fetcher's per-phase wire-byte
//! counter, which is the counter `sql_latency_bench` reads for
//! `wire_bytes_by_phase`. Anything the counter misses lands in the report's
//! `wire_bytes_unattributed` residual, where 11 GB of scan reads would read as
//! catalog resolve cost.
//!
//! Every statement here goes through `SqlExecutor`, over objects small enough
//! that the whole-segment fast path reads each one whole, and each test asserts
//! `logs_whole_object_opens` first so it cannot pass on the ranged path. The
//! byte and GET figures are checked against what the store itself served, per
//! key, so the expected split does not come from the accounting under test.
//!
//! The three whole-object shapes the fetcher has are each pinned: one direct
//! GET per object, the segmented covering read an object above the fetch bound
//! takes, and a read-cache miss followed by a warm hit that moves no wire bytes.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::Int64Array;
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{
    CacheFetchError, EngineConfig, LogSegmentFetcher, PhaseWireByteCounter, PhaseWireByteCounts,
    QueryPhase, SegmentFetcher,
};
use ravel_sql::{
    DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlOutcome,
    SqlRequest, StaticDeclaredColumns,
};
use ravel_types::accounting::AccountedOp;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

/// Objects in the fixture. Twice [`PARTITIONS`], so the whole-segment fast path
/// (which needs at least one relevant segment per partition) is taken.
const OBJECTS: usize = 4;
const PARTITIONS: usize = 2;
/// The q02 shape from #2121 Stage 0: a count over a declared-column predicate
/// that the whole-segment fast path still serves.
const SQL: &str = "SELECT count(*) FROM logs WHERE dur <> 0";

fn tenant() -> TenantId {
    TenantId::new("logs-whole-object-phase-wire-bytes".to_string())
}

/// `seq + 2` records per object, so every object has a different length and a
/// wrong object's bytes cannot stand in for the right one's in a byte sum.
/// `dur` cycles 0, 1, 2, so a third of the records fail the predicate.
fn records(seq: usize) -> Vec<LogRecord> {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    (0..seq + 2)
        .map(|i| {
            let ts = 1_000 + (seq * 100 + i) as i64;
            LogRecord {
                stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
                stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: format!("object {seq} record {i}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![("dur".to_string(), AttrValue::I64((i % 3) as i64))],
            }
        })
        .collect()
}

/// Records across the whole fixture whose `dur` is nonzero: the answer `SQL`
/// must return.
fn expected_count() -> i64 {
    (0..OBJECTS)
        .flat_map(records)
        .filter(|r| !matches!(r.attrs[0].1, AttrValue::I64(0)))
        .count() as i64
}

/// One written object: its data key and its length.
struct Written {
    key: String,
    len: u64,
}

/// Write `OBJECTS` RLOG objects, each with its own commit record, so a real
/// catalog resolve finds them. Each commit record carries the object's real
/// BLAKE3: the read-cache key is `(tenant, content_hash, offset, size)`, and a
/// shared placeholder hash would let two equal-size objects collide.
async fn publish_objects(store: &dyn ObjectStoreBackend, tenant: &TenantId) -> Vec<Written> {
    let writer_id = Uuid::from_u128(21_300);
    let mut written = Vec::with_capacity(OBJECTS);
    for seq in 0..OBJECTS {
        let recs = records(seq);
        let identity = ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: 1,
            writer_seq: seq as u64 + 1,
        };
        let mut w = RlogWriter::new(RlogConfig::default(), identity);
        for r in &recs {
            w.push(r.clone()).expect("push");
        }
        let bytes = w.finish().expect("finish");
        let min = recs.iter().map(|r| r.ts_ns).min().expect("nonempty");
        let max = recs.iter().map(|r| r.ts_ns).max().expect("nonempty");
        let new_record = NewCommitRecord {
            tenant_hash: tenant.hash(),
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: seq as u64 + 1,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: recs.len() as u64,
            series_count: 1,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            min_ingest_ts_ns: min,
            max_ingest_ts_ns: max,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        };
        let rec = record::build(new_record).expect("valid logs commit record");
        let key = keys::reconstruct_data_key(&rec).expect("logs data key");
        let len = bytes.len() as u64;
        store
            .put(&key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put rlog object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish logs commit record");
        written.push(Written { key, len });
    }
    let distinct: BTreeSet<u64> = written.iter().map(|w| w.len).collect();
    assert_eq!(
        distinct.len(),
        OBJECTS,
        "fixture: every object length differs"
    );
    written
}

/// GETs and bytes the store served, per key.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Served {
    gets: u64,
    bytes: u64,
}

/// A pass-through store that tallies every GET by key, so a test can split the
/// store's own traffic into data-object reads and everything else without
/// asking the accounting it is checking.
struct KeyedGetStore {
    inner: Arc<MemoryStore>,
    served: Mutex<HashMap<String, Served>>,
}

impl KeyedGetStore {
    fn new() -> Arc<Self> {
        Arc::new(KeyedGetStore {
            inner: Arc::new(MemoryStore::new()),
            served: Mutex::new(HashMap::new()),
        })
    }

    fn snapshot(&self) -> HashMap<String, Served> {
        self.served.lock().expect("served lock").clone()
    }
}

#[async_trait]
impl ObjectStoreBackend for KeyedGetStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        // A GET that fails (a catalog probe for a pointer that does not exist
        // yet) is still a request the accounting counts.
        let got = self.inner.get(key, range).await;
        let mut served = self.served.lock().expect("served lock");
        let entry = served.entry(key.to_string()).or_default();
        entry.gets += 1;
        if let Ok(got) = &got {
            entry.bytes += got.data.len() as u64;
        }
        got
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
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// The store's traffic between two snapshots, split into reads of the
/// fixture's data objects and reads of anything else (the catalog's commit
/// records and pointers).
struct Split {
    data: Served,
    other: Served,
}

fn split(
    before: &HashMap<String, Served>,
    after: &HashMap<String, Served>,
    written: &[Written],
) -> Split {
    let data_keys: BTreeSet<&str> = written.iter().map(|w| w.key.as_str()).collect();
    let mut out = Split {
        data: Served::default(),
        other: Served::default(),
    };
    for (key, now) in after {
        let was = before.get(key).copied().unwrap_or_default();
        let side = if data_keys.contains(key.as_str()) {
            &mut out.data
        } else {
            &mut out.other
        };
        side.gets += now.gets - was.gets;
        side.bytes += now.bytes - was.bytes;
    }
    out
}

fn read_cache() -> Arc<Cache<CacheFetchError>> {
    let bytes = 64 << 20;
    Arc::new(Cache::new(CacheLimits::new(bytes, 4096, bytes)))
}

/// The executor, plus the wire-byte counter of the log fetcher inside it,
/// cloned out before the fetcher moves in, the way `sql_latency_bench`'s
/// `cold_executor` does.
fn executor(
    fetcher: LogSegmentFetcher,
    store: Arc<dyn ObjectStoreBackend>,
) -> (SqlExecutor, PhaseWireByteCounter) {
    let wire = fetcher.phase_wire_byte_counter();
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        fetcher,
        SpanSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig {
            engine: EngineConfig {
                fetch_concurrency: PARTITIONS,
                sql_partition_count: Some(PARTITIONS),
                ..EngineConfig::default()
            },
            ..SqlConfig::default()
        },
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(vec![
        DeclaredColumn::new("dur", DeclaredType::I64),
    ])));
    (executor, wire)
}

fn request() -> SqlRequest {
    SqlRequest {
        sql: SQL.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: i64::MAX,
        },
        min_tokens: Vec::new(),
        now_ns: 1_000_000,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// One execution: the outcome, the fetcher's per-phase wire bytes it added, and
/// the store traffic it caused.
struct Run {
    outcome: SqlOutcome,
    wire: PhaseWireByteCounts,
    store: Split,
}

async fn run(
    executor: &SqlExecutor,
    wire: &PhaseWireByteCounter,
    store: &KeyedGetStore,
    written: &[Written],
) -> Run {
    let wire_before = wire.snapshot();
    let store_before = store.snapshot();
    let outcome = executor
        .execute(tenant().hash(), &request())
        .await
        .expect("statement executes");
    let wire = wire.snapshot().saturating_sub(&wire_before);
    let store = split(&store_before, &store.snapshot(), written);
    let count = outcome
        .output
        .batches()
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("one result row")
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count(*) is Int64")
        .value(0);
    assert_eq!(count, expected_count(), "the statement's answer");
    Run {
        outcome,
        wire,
        store,
    }
}

/// The whole-object route was taken for every object, so the run cannot pass
/// on the ranged path.
fn assert_whole_object_route(run: &Run) {
    assert_eq!(
        run.outcome.accounting.logs_whole_object_opens, OBJECTS as u64,
        "every object is opened on the whole-object route"
    );
    assert_eq!(run.outcome.accounting.logs_ranged_opens, 0);
}

/// Resolve, plan and probe move nothing on this route: the fast path skips the
/// plan phase and a whole-object read issues no probe.
fn assert_only_scan_phase(run: &Run) {
    for phase in [QueryPhase::Resolve, QueryPhase::Plan, QueryPhase::Probe] {
        assert_eq!(run.wire.phase(phase), 0, "{} wire bytes", phase.name());
        assert_eq!(run.wire.phase_requests(phase), 0, "{} GETs", phase.name());
    }
}

/// The residual the bench derives, pooled GET bytes minus attributed bytes, is
/// exactly what the store served for keys that are not data objects.
fn assert_residual_is_catalog_only(run: &Run) {
    let pooled_bytes = run.outcome.accounting.s3_bytes(AccountedOp::Get);
    let pooled_gets = run.outcome.accounting.s3_requests(AccountedOp::Get);
    assert_eq!(
        pooled_bytes - run.wire.total(),
        run.store.other.bytes,
        "unattributed GET bytes are the non-data reads, and only those"
    );
    assert_eq!(
        pooled_gets - run.wire.total_requests(),
        run.store.other.gets,
        "unattributed GETs are the non-data reads, and only those"
    );
}

/// One direct whole-object GET per object (no cache, every object under the
/// fetch bound). The scan phase carries exactly the bytes and GETs the store
/// served for the data objects, which are the objects' lengths.
///
/// Fails if the `self.wire_bytes.record(phase, ..)` line in
/// `LogSegmentFetcher::whole_object_bytes`'s no-cache branch is removed (scan
/// reads 0 against the object bytes), or if `scan_whole_accounted_with_tenant`
/// passes `ReadPhases::SCAN.metadata` instead of `ReadPhases::SCAN.blocks`
/// (the bytes move to probe and scan reads 0; that mutation fails the other
/// two tests here as well, since every route below enters through it).
#[tokio::test]
async fn direct_whole_object_gets_are_charged_to_the_scan_phase() {
    let store = KeyedGetStore::new();
    let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
    let written = publish_objects(dyn_store.as_ref(), &tenant()).await;
    let object_bytes: u64 = written.iter().map(|w| w.len).sum();
    let (executor, wire) = executor(
        LogSegmentFetcher::new(Arc::clone(&dyn_store)),
        Arc::clone(&dyn_store),
    );

    let r = run(&executor, &wire, &store, &written).await;
    assert_whole_object_route(&r);
    assert_eq!(r.store.data.gets, OBJECTS as u64, "one GET per object");
    assert_eq!(
        r.store.data.bytes, object_bytes,
        "each object read once, whole"
    );
    assert_eq!(r.wire.phase(QueryPhase::Scan), r.store.data.bytes);
    assert_eq!(r.wire.phase_requests(QueryPhase::Scan), r.store.data.gets);
    assert_only_scan_phase(&r);
    assert_residual_is_catalog_only(&r);
    assert_eq!(
        r.outcome.phase_accounting.scan.s3_bytes(AccountedOp::Get),
        object_bytes,
        "the per-phase accounting handle agrees with the wire counter"
    );
}

/// An object above the fetch bound is read as a sequence of covering
/// sub-range GETs. Every one of them is the scan phase's, and their count is
/// `ceil(len / bound)` per object.
///
/// Fails if the `self.wire_bytes.record(phase, ..)` line in
/// `BlockRangeFetcher::store_get` is removed (scan reads 0), or if
/// `whole_object_bytes` passes `QueryPhase::Probe` to `covering_read` instead
/// of its own `phase`.
#[tokio::test]
async fn segmented_covering_read_charges_every_sub_range_to_the_scan_phase() {
    let store = KeyedGetStore::new();
    let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
    let written = publish_objects(dyn_store.as_ref(), &tenant()).await;
    let bound = written.iter().map(|w| w.len).min().expect("objects") / 3;
    let sub_ranges: u64 = written.iter().map(|w| w.len.div_ceil(bound)).sum();
    let object_bytes: u64 = written.iter().map(|w| w.len).sum();
    let fetcher = LogSegmentFetcher::new(Arc::clone(&dyn_store))
        .with_max_fetch_run_bytes(bound)
        .expect("nonzero bound");
    let (executor, wire) = executor(fetcher, Arc::clone(&dyn_store));

    let r = run(&executor, &wire, &store, &written).await;
    assert_whole_object_route(&r);
    assert!(
        sub_ranges >= 3 * OBJECTS as u64,
        "fixture: every object is split into several covering GETs"
    );
    assert_eq!(
        r.store.data.gets, sub_ranges,
        "ceil(len / bound) GETs per object"
    );
    assert_eq!(r.store.data.bytes, object_bytes);
    assert_eq!(r.wire.phase(QueryPhase::Scan), object_bytes);
    assert_eq!(r.wire.phase_requests(QueryPhase::Scan), sub_ranges);
    assert_only_scan_phase(&r);
    assert_residual_is_catalog_only(&r);
}

/// With a read cache, the cold run's misses are scan-phase GETs, and a second
/// run served from the warm cache records no scan wire bytes and no scan GETs:
/// cache bytes are not store bytes.
///
/// Fails if the `self.wire_bytes.record(phase, ..)` line inside
/// `whole_object_bytes`'s cache fetch closure is removed (the cold run's scan
/// reads 0), or if the `ReadOutcome::Hit` arm charges the served bytes to the
/// wire counter as well (the warm run's scan reads the object bytes, not 0).
#[tokio::test]
async fn cache_miss_is_charged_to_the_scan_phase_and_a_warm_hit_to_nothing() {
    let store = KeyedGetStore::new();
    let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
    let written = publish_objects(dyn_store.as_ref(), &tenant()).await;
    let object_bytes: u64 = written.iter().map(|w| w.len).sum();
    let fetcher = LogSegmentFetcher::new(Arc::clone(&dyn_store)).with_cache(read_cache());
    let (executor, wire) = executor(fetcher, Arc::clone(&dyn_store));

    let cold = run(&executor, &wire, &store, &written).await;
    assert_whole_object_route(&cold);
    // The scan phase's handle, not the pooled one: the catalog's own record
    // cache reports its hits and misses on the pooled total too.
    let cold_scan = &cold.outcome.phase_accounting.scan;
    assert_eq!(
        cold_scan.cache_misses, OBJECTS as u64,
        "one miss per object"
    );
    assert_eq!(cold_scan.cache_hits, 0);
    assert_eq!(cold.store.data.gets, OBJECTS as u64);
    assert_eq!(cold.store.data.bytes, object_bytes);
    assert_eq!(cold.wire.phase(QueryPhase::Scan), object_bytes);
    assert_eq!(cold.wire.phase_requests(QueryPhase::Scan), OBJECTS as u64);
    assert_only_scan_phase(&cold);
    assert_residual_is_catalog_only(&cold);

    let warm = run(&executor, &wire, &store, &written).await;
    assert_whole_object_route(&warm);
    let warm_scan = &warm.outcome.phase_accounting.scan;
    assert_eq!(
        warm_scan.cache_hits, OBJECTS as u64,
        "every object is served from the warm cache"
    );
    assert_eq!(warm_scan.cache_misses, 0);
    assert_eq!(warm_scan.cache_bytes, object_bytes);
    assert_eq!(
        warm.store.data,
        Served::default(),
        "no data object GET at all"
    );
    assert_eq!(
        warm.wire.phase(QueryPhase::Scan),
        0,
        "cache bytes are not wire bytes"
    );
    assert_eq!(warm.wire.phase_requests(QueryPhase::Scan), 0);
    assert_only_scan_phase(&warm);
    assert_residual_is_catalog_only(&warm);
}
