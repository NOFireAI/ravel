//! ADR-3326: the decoded-column cache changes no result.
//!
//! A two-object tenant, several blocks per object, with four declared columns
//! of distinct kinds: `n` (Int64), `p` (a distinct string per row, stored
//! plain), `d` (two values, stored as a dictionary) and `c` (only ever read
//! through `LIKE '%x%'`, a `Contains` arm, which defers its decode). Each
//! statement runs once cold and three times hot through an executor whose logs
//! fetcher carries a large decoded cache, and every run's rows must equal the
//! same statement's rows through an executor with no decoded cache (the
//! `--decoded-cache-max-bytes 0` shape). Hot runs must hit the cache for every
//! projected column. `c` is served too when the scan's decoded set fits under
//! 0.95 of the cap, and never when it does not (ADR-3326, serve-if-fits
//! amendment).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::util::pretty::pretty_format_batches;
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::decoded_cache::DecodedColumnCache;
use ravel_logseg::reader::RlogReader;
use ravel_logseg::record::{COL_TS, FieldType};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_memory::MemoryBudget;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{CacheFetchError, LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest,
    StaticDeclaredColumns,
};
use ravel_types::accounting::AccessIntent;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

const OBJECTS: u64 = 2;
const ROWS_PER_BLOCK: usize = 6;
const BLOCKS_PER_OBJECT: usize = 4;
const ROWS_PER_OBJECT: usize = ROWS_PER_BLOCK * BLOCKS_PER_OBJECT;
const HOT_RUNS: usize = 3;

fn tenant() -> TenantId {
    TenantId::new("decoded-cache-3326".to_string())
}

fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("n", DeclaredType::I64),
        DeclaredColumn::new("p", DeclaredType::Str),
        DeclaredColumn::new("d", DeclaredType::Str),
        DeclaredColumn::new("c", DeclaredType::Str),
    ]
}

/// Row `i` of an object: every value but `d` carries `ts`, so a column served
/// for the wrong block reads as different rows.
fn record(ts: i64, i: usize) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let c = if i.is_multiple_of(3) {
        format!("has-x-{ts}")
    } else {
        format!("none-{ts}")
    };
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("row at {ts}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![
            ("n".to_string(), AttrValue::I64(ts * 7)),
            ("p".to_string(), AttrValue::Str(format!("p-{ts}"))),
            (
                "d".to_string(),
                AttrValue::Str(["red", "blue"][i % 2].to_string()),
            ),
            ("c".to_string(), AttrValue::Str(c)),
        ],
    }
}

/// Write object `seq` and publish its commit record; returns the object bytes.
async fn publish_object(store: &dyn ObjectStoreBackend, tenant: &TenantId, seq: u64) -> Vec<u8> {
    let base = seq as i64 * 1000;
    let records: Vec<LogRecord> = (0..ROWS_PER_OBJECT)
        .map(|i| record(base + i as i64 + 1, i))
        .collect();
    let cfg = RlogConfig {
        block_target_records: ROWS_PER_BLOCK,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(
        cfg,
        ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: [3u8; 16],
            writer_epoch: 1,
            writer_seq: seq,
        },
    );
    for r in &records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(3_326),
        writer_epoch: 1,
        writer_seq: seq,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        min_ingest_ts_ns: min,
        max_ingest_ts_ns: max,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(
            &data_key,
            bytes::Bytes::from(bytes.clone()),
            PutOptions::default(),
        )
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
    bytes
}

fn executor(
    store: &Arc<dyn ObjectStoreBackend>,
    cache: Option<Arc<DecodedColumnCache>>,
) -> SqlExecutor {
    executor_with(store, cache, false, None)
}

/// [`executor`] whose logs fetcher also carries a read cache, so its scans
/// stripe over the default 8 partitions. With more partitions than this
/// fixture's 2 segments, a `LIKE`-only scan is not taken by the whole-segment
/// fast path and plans both segments, so its plan pass makes the
/// serve-if-fits verdict (ADR-3326). Rows do not depend on the read cache.
fn striped(
    store: &Arc<dyn ObjectStoreBackend>,
    cache: Option<Arc<DecodedColumnCache>>,
) -> SqlExecutor {
    executor_with(store, cache, true, None)
}

fn executor_with(
    store: &Arc<dyn ObjectStoreBackend>,
    cache: Option<Arc<DecodedColumnCache>>,
    read_cache: bool,
    suffix_len: Option<u64>,
) -> SqlExecutor {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(store), CatalogConfig::default()).expect("catalog"));
    let logs = LogSegmentFetcher::new(Arc::clone(store));
    let logs = match suffix_len {
        Some(n) => logs.with_suffix_len(n),
        None => logs,
    };
    let logs = if read_cache {
        let bytes = 64 << 20;
        logs.with_cache(Arc::new(Cache::<CacheFetchError>::new(CacheLimits::new(
            bytes, 16_384, bytes,
        ))))
    } else {
        logs
    };
    let logs = match cache {
        Some(cache) => logs.with_decoded_cache(cache),
        None => logs,
    };
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(store)),
        logs,
        SpanSegmentFetcher::new(Arc::clone(store)),
        SqlConfig::default(),
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(declared())))
}

fn request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: 1_000_000,
        },
        min_tokens: Vec::new(),
        now_ns: 1_000_000,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// The column id `name` was written under, the same in every object.
fn column_id(objects: &[Vec<u8>], name: &str, ty: FieldType) -> u32 {
    let ids: Vec<u32> = objects
        .iter()
        .map(|bytes| {
            let reader = RlogReader::new(bytes, &RlogConfig::default()).expect("reader");
            reader
                .directories()
                .field_dir()
                .column(name, ty)
                .expect("declared key has a column")
                .column_id
        })
        .collect();
    assert!(
        ids.windows(2).all(|w| w[0] == w[1]),
        "{name} shares one column id across objects: {ids:?}"
    );
    ids[0]
}

struct Statement {
    sql: &'static str,
    /// Columns a hot run must serve from the cache: every projected column,
    /// the Contains-deferred one included, since each statement's decoded set
    /// fits the 1 GiB cache.
    served: &'static [&'static str],
    rows: usize,
    /// The intent the executor classifies it under: a statement with no
    /// filter reads as a bulk scan (ADR-3072's predicate-free rule).
    intent: AccessIntent,
}

const CONTAINS_SQL: &str = "SELECT ts, d, c FROM logs WHERE c LIKE '%x%' ORDER BY ts";

const STATEMENTS: [Statement; 4] = [
    Statement {
        sql: "SELECT ts, n, p, d FROM logs ORDER BY ts",
        served: &["ts", "n", "p", "d"],
        rows: ROWS_PER_OBJECT * OBJECTS as usize,
        intent: AccessIntent::BulkScan,
    },
    Statement {
        sql: "SELECT ts, p FROM logs WHERE n > 7084 ORDER BY ts",
        served: &["ts", "n", "p"],
        rows: 2 * ROWS_PER_OBJECT - 12,
        intent: AccessIntent::Interactive,
    },
    Statement {
        sql: "SELECT d, COUNT(*) AS k, SUM(n) AS s FROM logs GROUP BY d ORDER BY d",
        served: &["n", "d"],
        rows: 2,
        intent: AccessIntent::BulkScan,
    },
    Statement {
        sql: CONTAINS_SQL,
        served: &["ts", "d", "c"],
        rows: (ROWS_PER_OBJECT / 3) * OBJECTS as usize,
        intent: AccessIntent::Interactive,
    },
];

/// Every statement's rows, cold and on each of three hot runs, equal its rows
/// with no decoded cache; hot runs hit for every projected column, the
/// `Contains`-tested `c` included, since the fixture's decoded set is far
/// under the 1 GiB cap (ADR-3326, serve-if-fits amendment).
/// `decoded_cache_never_serves_the_contains_column_when_the_scan_does_not_fit`
/// is the other half.
#[tokio::test]
async fn decoded_cache_serves_identical_rows_and_the_contains_column_when_the_scan_fits() {
    let t = tenant();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStoreBackend> = mem;
    let mut objects = Vec::new();
    for seq in 1..=OBJECTS {
        objects.push(publish_object(store.as_ref(), &t, seq).await);
    }
    let ids = |name: &str| match name {
        "ts" => COL_TS,
        "n" => column_id(&objects, "n", FieldType::I64),
        other => column_id(&objects, other, FieldType::Str),
    };
    let c_id = ids("c");

    let uncached = executor(&store, None);
    let cache = Arc::new(DecodedColumnCache::new(
        1 << 30,
        Some(Arc::new(MemoryBudget::unlimited())),
    ));
    let cached = striped(&store, Some(Arc::clone(&cache)));

    for st in &STATEMENTS {
        let base = uncached
            .execute(t.hash(), &request(st.sql))
            .await
            .expect("uncached statement executes");
        assert_eq!(
            base.stats.decoded_cache, None,
            "no decodedCache stats without a cache: {}",
            st.sql
        );
        assert_eq!(base.output.num_rows(), st.rows, "{}", st.sql);
        let expected = pretty_format_batches(base.output.batches())
            .expect("format")
            .to_string();

        // ADR-3326 decision 3: an interactive statement admits on its first
        // decode, so every hot run serves; a bulk scan admits on the second
        // touch, so its first hot run is the admitting one.
        let first_serving_run = match st.intent {
            AccessIntent::Interactive => 1,
            _ => 2,
        };
        let mut before = Vec::new();
        let mut hot_blocks = 0u64;
        let mut cold_misses = 0;
        for run in 0..=HOT_RUNS {
            if run == first_serving_run {
                before = st
                    .served
                    .iter()
                    .map(|n| cache.column_hits(ids(n)))
                    .collect();
            }
            let outcome = cached
                .execute(t.hash(), &request(st.sql))
                .await
                .expect("cached statement executes");
            let got = pretty_format_batches(outcome.output.batches())
                .expect("format")
                .to_string();
            assert_eq!(got, expected, "run {run} of {}", st.sql);
            assert_eq!(outcome.accounting.access_intent, st.intent, "{}", st.sql);
            let stats = outcome
                .stats
                .decoded_cache
                .expect("decodedCache stats with a cache")
                .counters;
            let contains = st.sql == CONTAINS_SQL;
            assert_eq!(stats.deferred_served, contains, "{}", st.sql);
            assert_eq!(
                stats.deferred_set_estimate_bytes.is_some(),
                contains,
                "{}",
                st.sql
            );
            assert_eq!(stats.unvalidated_refusals, 0, "{}", st.sql);
            if run == 0 {
                cold_misses = stats.misses;
                if st.intent == AccessIntent::Interactive {
                    assert_eq!(stats.admissions, stats.misses, "first decode admits");
                    assert_eq!(stats.ghost_hits, 0);
                } else {
                    assert_eq!(stats.admissions, 0, "a bulk first touch only ghosts");
                }
            }
            if run == 1 && first_serving_run == 2 && cold_misses > 0 {
                assert_eq!(stats.ghost_hits, cold_misses, "second touch: {}", st.sql);
                assert_eq!(stats.admissions, cold_misses, "second touch: {}", st.sql);
            }
            if run >= first_serving_run {
                hot_blocks += outcome.stats.blocks_scanned;
                assert!(stats.hits > 0, "hot run {run} hits: {}", st.sql);
                assert_eq!(
                    stats.misses, 0,
                    "hot run {run} serves every eligible column: {}",
                    st.sql
                );
                assert_eq!(stats.admissions, 0, "hot run {run}: {}", st.sql);
            }
        }
        assert!(hot_blocks > 0, "hot runs scanned blocks: {}", st.sql);
        for (name, before) in st.served.iter().zip(before) {
            let after = cache.column_hits(ids(name));
            assert_eq!(
                after - before,
                hot_blocks,
                "{name} hit once per scanned block per hot run: {}",
                st.sql
            );
        }
    }
    assert!(
        cache.column_hits(c_id) > 0,
        "the Contains-tested column is served from the cache"
    );
    assert!(cache.resident_bytes() > 0);
}

/// Blocks the fixture holds: every one survives [`CONTAINS_SQL`]'s plan.
const BLOCKS: u64 = OBJECTS * BLOCKS_PER_OBJECT as u64;

/// What [`CONTAINS_SQL`]'s plan pass estimates the scan's decoded set at.
async fn contains_estimate(store: &Arc<dyn ObjectStoreBackend>, t: &TenantId) -> u64 {
    let probe = Arc::new(DecodedColumnCache::new(
        1 << 30,
        Some(Arc::new(MemoryBudget::unlimited())),
    ));
    striped(store, Some(probe))
        .execute(t.hash(), &request(CONTAINS_SQL))
        .await
        .expect("statement executes")
        .stats
        .decoded_cache
        .expect("decodedCache stats with a cache")
        .counters
        .deferred_set_estimate_bytes
        .expect("a Contains scan reports its estimate")
}

/// ADR-3326, serve-if-fits amendment, decision 2: a scan whose decoded set is
/// over 0.95 of the cap looks up and offers no deferred column. The cap is
/// the scan's own estimate, so the set sits between 0.95 of the cap and the
/// cap. Each block's `ts`, `stream_ref` and `d` are looked up: 24 misses and
/// 24 admissions cold, 24 hits and no miss hot, and the cache ends holding
/// those 24 entries and no `c`. The rows equal the uncached scan's.
///
/// Seen failing against serving every deferred column (the counts assertion:
/// cold `(0, 32, 32)` where `(0, 24, 24)` is expected) and against comparing
/// the estimate with the cap instead of 0.95 of it (the `deferred_served`
/// assertion on run 0, since the estimate equals the cap).
#[tokio::test]
async fn decoded_cache_never_serves_the_contains_column_when_the_scan_does_not_fit() {
    let t = tenant();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStoreBackend> = mem;
    let mut objects = Vec::new();
    for seq in 1..=OBJECTS {
        objects.push(publish_object(store.as_ref(), &t, seq).await);
    }
    let c_id = column_id(&objects, "c", FieldType::Str);
    let estimate = contains_estimate(&store, &t).await;
    let expected = executor(&store, None)
        .execute(t.hash(), &request(CONTAINS_SQL))
        .await
        .expect("uncached statement executes");
    let expected = pretty_format_batches(expected.output.batches())
        .expect("format")
        .to_string();

    let cache = Arc::new(DecodedColumnCache::new(
        estimate,
        Some(Arc::new(MemoryBudget::unlimited())),
    ));
    let cached = striped(&store, Some(Arc::clone(&cache)));
    for run in 0..2 {
        let outcome = cached
            .execute(t.hash(), &request(CONTAINS_SQL))
            .await
            .expect("cached statement executes");
        let got = pretty_format_batches(outcome.output.batches())
            .expect("format")
            .to_string();
        assert_eq!(got, expected, "run {run}");
        assert_eq!(outcome.stats.blocks_scanned, BLOCKS);
        let s = outcome
            .stats
            .decoded_cache
            .expect("decodedCache stats with a cache")
            .counters;
        assert_eq!(s.deferred_set_estimate_bytes, Some(estimate), "run {run}");
        assert!(!s.deferred_served, "run {run}");
        let (hits, misses, admissions) = if run == 0 { (0, 24, 24) } else { (24, 0, 0) };
        assert_eq!(
            (s.hits, s.misses, s.admissions),
            (hits, misses, admissions),
            "run {run}"
        );
        assert_eq!((s.evictions, s.unvalidated_refusals), (0, 0), "run {run}");
    }
    assert_eq!(cache.column_hits(c_id), 0, "c is never served");
    assert_eq!(cache.len(), 24, "ts, stream_ref and d of 8 blocks; no c");
}

/// ADR-3326, serve-if-fits amendment: a scan that fits serves its deferred
/// column with identical rows. Cold, every projected column of the 8 blocks
/// (`ts`, `stream_ref`, `d` and the deferred `c`) misses and is admitted, 32
/// of each, `c` decoded validated so nothing is refused; hot, all 32 hit, `c`
/// once per block.
#[tokio::test]
async fn a_fitting_contains_scan_serves_its_deferred_column_with_identical_rows() {
    let t = tenant();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStoreBackend> = mem;
    let mut objects = Vec::new();
    for seq in 1..=OBJECTS {
        objects.push(publish_object(store.as_ref(), &t, seq).await);
    }
    let c_id = column_id(&objects, "c", FieldType::Str);
    let expected = executor(&store, None)
        .execute(t.hash(), &request(CONTAINS_SQL))
        .await
        .expect("uncached statement executes");
    let expected = pretty_format_batches(expected.output.batches())
        .expect("format")
        .to_string();
    let cache = Arc::new(DecodedColumnCache::new(
        1 << 30,
        Some(Arc::new(MemoryBudget::unlimited())),
    ));
    let cached = striped(&store, Some(Arc::clone(&cache)));
    for run in 0..2 {
        let outcome = cached
            .execute(t.hash(), &request(CONTAINS_SQL))
            .await
            .expect("cached statement executes");
        let got = pretty_format_batches(outcome.output.batches())
            .expect("format")
            .to_string();
        assert_eq!(got, expected, "run {run}");
        let s = outcome
            .stats
            .decoded_cache
            .expect("decodedCache stats with a cache")
            .counters;
        assert!(s.deferred_served, "run {run}");
        let (hits, misses, admissions) = if run == 0 { (0, 32, 32) } else { (32, 0, 0) };
        assert_eq!(
            (s.hits, s.misses, s.admissions),
            (hits, misses, admissions),
            "run {run}"
        );
        assert_eq!(s.unvalidated_refusals, 0, "run {run}");
    }
    assert_eq!(
        cache.column_hits(c_id),
        BLOCKS,
        "c hit once per block on run 1"
    );
}

/// A statement that fails while its scan is served from the cache fails with
/// the same error, cold and on every hot run, as it does with no cache.
#[tokio::test]
async fn decoded_cache_leaves_statement_errors_unchanged() {
    let t = tenant();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStoreBackend> = mem;
    for seq in 1..=OBJECTS {
        publish_object(store.as_ref(), &t, seq).await;
    }
    let uncached = executor(&store, None);
    let cache = Arc::new(DecodedColumnCache::new(
        1 << 30,
        Some(Arc::new(MemoryBudget::unlimited())),
    ));
    let cached = executor(&store, Some(Arc::clone(&cache)));
    // Every `p` starts with `p`, which no cast to an integer accepts; casting
    // only that prefix makes the message the same whichever partition fails
    // first.
    let sql = "SELECT ts, CAST(substr(p, 1, 1) AS BIGINT) AS bad FROM logs \
               WHERE n > 0 ORDER BY ts";
    let expected = uncached
        .execute(t.hash(), &request(sql))
        .await
        .expect_err("the cast fails without a cache")
        .to_string();
    for run in 0..=HOT_RUNS {
        let before = cache.process_counters().hits;
        let got = cached
            .execute(t.hash(), &request(sql))
            .await
            .expect_err("the cast fails with a cache")
            .to_string();
        assert_eq!(got, expected, "run {run}");
        if run > 0 {
            assert!(
                cache.process_counters().hits > before,
                "hot run {run} is served from the cache before it fails"
            );
        }
    }
}

/// ADR-3326 decision 5: a cached plain `Str` column keeps the UTF-8
/// validation its decode did. `p` is a plain page whose cells are all UTF-8,
/// one per row, 48 over the tenant; `n` is an integer and `ts` a timestamp,
/// so `p` is the only string column the statement decodes. The cold run
/// validates every `p` cell twice: once at decode, and once when the build
/// makes the entry's values memo (ADR-3326, values-memo amendment, decision
/// 6). Each hot run is served `p` from the cache, reuses the memo, and
/// validates nothing. Without a cache every run validates the 48 cells
/// again, per cell in the build.
///
/// Every row here takes the memo's fast path, so this test no longer reaches
/// `StrCursor::text_at`; its use of the decode's validation is pinned by
/// `a_plain_page_validated_at_decode_is_not_validated_again` in
/// `src/logs_scan/batch.rs`, whose block has absent rows and takes the loop.
#[tokio::test]
async fn a_cache_hit_on_a_plain_str_column_validates_nothing() {
    let t = tenant();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStoreBackend> = mem;
    for seq in 1..=OBJECTS {
        publish_object(store.as_ref(), &t, seq).await;
    }
    let cells = (ROWS_PER_OBJECT * OBJECTS as usize) as u64;
    let sql = "SELECT ts, p FROM logs WHERE n > 0 ORDER BY ts";

    let uncached = executor(&store, None);
    for run in 0..2 {
        let outcome = uncached
            .execute(t.hash(), &request(sql))
            .await
            .expect("uncached statement executes");
        assert_eq!(outcome.output.num_rows() as u64, cells);
        assert_eq!(
            outcome.stats.str_cells_validated, cells,
            "uncached run {run} decodes and validates every cell"
        );
    }

    let cache = Arc::new(DecodedColumnCache::new(
        1 << 30,
        Some(Arc::new(MemoryBudget::unlimited())),
    ));
    let cached = executor(&store, Some(Arc::clone(&cache)));
    for run in 0..=HOT_RUNS {
        let outcome = cached
            .execute(t.hash(), &request(sql))
            .await
            .expect("cached statement executes");
        assert_eq!(outcome.output.num_rows() as u64, cells);
        let counters = outcome
            .stats
            .decoded_cache
            .expect("decodedCache stats with a cache")
            .counters;
        if run == 0 {
            assert_eq!(counters.hits, 0, "the cold run decodes");
            assert_eq!(
                outcome.stats.str_cells_validated,
                2 * cells,
                "cold run: the decode and the memo build"
            );
        } else {
            assert_eq!(counters.misses, 0, "hot run {run} is fully served");
            assert_eq!(
                outcome.stats.str_cells_validated, 0,
                "hot run {run} validates nothing"
            );
        }
    }
}

/// The row path (an `attrs` projection) never offers a column to the decoded
/// cache, so its decode validates no plain `Str` page whole: `p` and `c` are
/// plain pages of 48 cells each and count nothing, cached or not, and the
/// row path's per-cell validations are not counted, as before ADR-3326
/// decision 5's decode-time validation. What is left is the reader's
/// validation of `d`'s dictionary, two entries in each of the two objects,
/// the same 4 a statement reading `d` alone counts.
///
/// Seen failing against validating every decoded plain `Str` attribute page
/// at decode whatever the path: the uncached run counts the 96 cells of `p`
/// and `c` on top of the 4.
#[tokio::test]
async fn the_row_path_validates_no_plain_page_at_decode() {
    let t = tenant();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStoreBackend> = mem;
    for seq in 1..=OBJECTS {
        publish_object(store.as_ref(), &t, seq).await;
    }
    let rows = (ROWS_PER_OBJECT * OBJECTS as usize) as u64;
    let sql = "SELECT ts, attrs FROM logs WHERE n > 0 ORDER BY ts";
    let d_sql = "SELECT ts, d FROM logs WHERE n > 0 ORDER BY ts";

    let cache = Arc::new(DecodedColumnCache::new(
        1 << 30,
        Some(Arc::new(MemoryBudget::unlimited())),
    ));
    for ex in [executor(&store, None), executor(&store, Some(cache))] {
        let outcome = ex
            .execute(t.hash(), &request(sql))
            .await
            .expect("attrs statement executes");
        assert_eq!(outcome.output.num_rows() as u64, rows);
        assert_eq!(
            outcome.stats.str_cells_validated, 4,
            "d's dictionary entries only"
        );
    }
    let outcome = executor(&store, None)
        .execute(t.hash(), &request(d_sql))
        .await
        .expect("d statement executes");
    assert_eq!(outcome.stats.str_cells_validated, 4);
}

/// On the columnar path with no decoded cache, a plain `Str` column is
/// validated per surviving cell in the build, not whole at decode. The
/// statement's window `[1004, 1010)` keeps rows 4 to 9 of the first object,
/// six rows spread over its first two blocks of six, so the six surviving `p`
/// cells are validated, not the twelve the two blocks decode.
///
/// Seen failing against validating every decoded plain `Str` attribute page
/// at decode: the run counts all twelve `p` cells.
#[tokio::test]
async fn an_uncached_selective_scan_validates_only_surviving_cells() {
    let t = tenant();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStoreBackend> = mem;
    for seq in 1..=OBJECTS {
        publish_object(store.as_ref(), &t, seq).await;
    }
    let sql = "SELECT ts, p FROM logs \
               WHERE ts >= TIMESTAMP '1970-01-01 00:00:00.000001004' \
               AND ts < TIMESTAMP '1970-01-01 00:00:00.000001010' ORDER BY ts";
    let outcome = executor(&store, None)
        .execute(t.hash(), &request(sql))
        .await
        .expect("selective statement executes");
    assert_eq!(outcome.output.num_rows(), 6);
    assert_eq!(
        outcome.stats.str_cells_validated, 6,
        "the surviving p cells only"
    );
}

/// ADR-3326, LIKE-deferred amendment, decision 4, on a fixture that fits: a
/// `LIKE` scan defers the dictionary column `d`, decodes it validated on its
/// miss and admits it whole (24 misses and 24 admissions: `ts`, `stream_ref`
/// and `d` of 8 blocks, no refusal). A later scan of `d` without the arm is
/// served `d` from that entry, 8 hits on `d`, and reads exactly the rows of a
/// cold scan with no cache.
///
/// Seen failing against leaving the deferred columns out of the validate set
/// (`d` is offered raw: `unvalidated_refusals` is 8 on the first scan where 0
/// is expected), and against that plus removing `offer`'s refusal (the raw
/// dictionary is admitted and the later scan reads every `d` as NULL, so its
/// rows differ).
#[tokio::test]
async fn a_column_cached_by_a_contains_scan_serves_a_later_scan_exactly() {
    let t = tenant();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStoreBackend> = mem;
    let mut objects = Vec::new();
    for seq in 1..=OBJECTS {
        objects.push(publish_object(store.as_ref(), &t, seq).await);
    }
    let d_id = column_id(&objects, "d", FieldType::Str);
    let like = "SELECT ts, d FROM logs WHERE d LIKE '%u%' ORDER BY ts";
    let later = "SELECT ts, d FROM logs WHERE n > 0 ORDER BY ts";
    let cold = |sql: &'static str| {
        let store = Arc::clone(&store);
        let t = t.clone();
        async move {
            let out = executor(&store, None)
                .execute(t.hash(), &request(sql))
                .await
                .expect("uncached statement executes");
            pretty_format_batches(out.output.batches())
                .expect("format")
                .to_string()
        }
    };
    let (like_rows, later_rows) = (cold(like).await, cold(later).await);

    let cache = Arc::new(DecodedColumnCache::new(
        1 << 30,
        Some(Arc::new(MemoryBudget::unlimited())),
    ));
    let cached = striped(&store, Some(Arc::clone(&cache)));
    let first = cached
        .execute(t.hash(), &request(like))
        .await
        .expect("LIKE statement executes");
    let got = pretty_format_batches(first.output.batches())
        .expect("format")
        .to_string();
    assert_eq!(got, like_rows);
    let s = first
        .stats
        .decoded_cache
        .expect("decodedCache stats with a cache")
        .counters;
    assert!(s.deferred_served);
    assert_eq!(s.unvalidated_refusals, 0, "d was decoded validated");
    assert_eq!((s.hits, s.misses, s.admissions), (0, 24, 24));

    let second = cached
        .execute(t.hash(), &request(later))
        .await
        .expect("later statement executes");
    let got = pretty_format_batches(second.output.batches())
        .expect("format")
        .to_string();
    assert_eq!(got, later_rows, "served d reads exactly as a cold scan");
    let s = second
        .stats
        .decoded_cache
        .expect("decodedCache stats with a cache")
        .counters;
    assert_eq!((s.hits, s.misses, s.unvalidated_refusals), (24, 8, 0));
    assert_eq!(cache.column_hits(d_id), BLOCKS, "d served once per block");
}

/// GET requests in one phase of an outcome's accounting.
fn gets(phase: &ravel_types::accounting::QueryAccountingSnapshot) -> u64 {
    phase.s3_requests[ravel_types::accounting::AccountedOp::Get.index()]
}

/// Requests of every kind in an outcome's accounting.
fn requests(phase: &ravel_types::accounting::QueryAccountingSnapshot) -> u64 {
    phase.s3_requests.iter().sum()
}

/// One run of [`CONTAINS_SQL`] on the whole-segment fast path: without a read
/// cache the scan runs as many partitions as segments, so the fast path takes
/// it. The fast path opens both objects whole in the scan phase (2 scan-phase
/// GETs, where the planned route reuses its plan read and issues none).
/// Returns the decoded-cache counters, the plan phase's GETs, which on the
/// fast path are the verdict's directory reads, and `segmentsOpened` as
/// `(plan, scan)`.
async fn fast_path_run(
    cached: &SqlExecutor,
    t: &TenantId,
    expected: &str,
    run: usize,
) -> (
    ravel_logseg::decoded_cache::DecodedCacheSnapshot,
    u64,
    (u64, u64),
) {
    let outcome = cached
        .execute(t.hash(), &request(CONTAINS_SQL))
        .await
        .expect("cached statement executes");
    let got = pretty_format_batches(outcome.output.batches())
        .expect("format")
        .to_string();
    assert_eq!(got, expected, "run {run}");
    assert_eq!(outcome.stats.blocks_scanned, BLOCKS, "run {run}");
    assert_eq!(
        gets(&outcome.phase_accounting.scan),
        OBJECTS,
        "run {run}: the fast path opens each object whole"
    );
    let counters = outcome
        .stats
        .decoded_cache
        .expect("decodedCache stats with a cache")
        .counters;
    let opened = (
        outcome.phase_accounting.plan.segments_opened,
        outcome.phase_accounting.scan.segments_opened,
    );
    (counters, gets(&outcome.phase_accounting.plan), opened)
}

/// ADR-3326, serve-if-fits amendment, implementation note: a fitting
/// `Contains` scan on the whole-segment fast path is served as the planned
/// route is. Its estimate equals the plan pass's ([`contains_estimate`]),
/// since both cover every block. Cold, every projected column of the 8 blocks
/// (`ts`, `stream_ref`, `d` and the deferred `c`) misses and is admitted, 32
/// of each; hot, all 32 hit, `c` once per block. Rows equal the uncached
/// scan's on both runs.
///
/// Seen failing against the previous round's fast path, which made no
/// verdict: the counts assertion reads `(0, 24, 24)` on run 0 where
/// `(0, 32, 32)` is expected, since `c` is never looked up.
///
/// `segmentsOpened` (issue #3745): the cold run's verdict decodes every
/// object's footer under plan, so each counts there and the scan's open of it
/// counts none, `(OBJECTS, 0)`. The hot run's verdict reads only the memo, so
/// the scan's opens count, `(0, OBJECTS)`. Seen failing with the verdict's
/// count removed, `(0, 2)` on run 0, and with the scan not told the verdict
/// had counted, `(2, 2)`.
#[tokio::test]
async fn a_fitting_contains_scan_on_the_whole_segment_fast_path_is_served() {
    let t = tenant();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStoreBackend> = mem;
    let mut objects = Vec::new();
    for seq in 1..=OBJECTS {
        objects.push(publish_object(store.as_ref(), &t, seq).await);
    }
    let c_id = column_id(&objects, "c", FieldType::Str);
    let estimate = contains_estimate(&store, &t).await;
    let expected = executor(&store, None)
        .execute(t.hash(), &request(CONTAINS_SQL))
        .await
        .expect("uncached statement executes");
    let expected = pretty_format_batches(expected.output.batches())
        .expect("format")
        .to_string();
    let cache = Arc::new(DecodedColumnCache::new(
        1 << 30,
        Some(Arc::new(MemoryBudget::unlimited())),
    ));
    let cached = executor(&store, Some(Arc::clone(&cache)));
    for run in 0..2 {
        let (s, plan_gets, opened) = fast_path_run(&cached, &t, &expected, run).await;
        assert_eq!(
            opened,
            if run == 0 { (OBJECTS, 0) } else { (0, OBJECTS) },
            "run {run}: segmentsOpened as (plan, scan)"
        );
        let (hits, misses, admissions) = if run == 0 { (0, 32, 32) } else { (32, 0, 0) };
        assert_eq!(
            (s.hits, s.misses, s.admissions),
            (hits, misses, admissions),
            "run {run}"
        );
        assert_eq!(s.deferred_set_estimate_bytes, Some(estimate), "run {run}");
        assert!(s.deferred_served, "run {run}");
        assert_eq!(s.unvalidated_refusals, 0, "run {run}");
        assert_eq!(
            plan_gets,
            if run == 0 { OBJECTS } else { 0 },
            "run {run}: one directory read per object cold, none once memoised"
        );
    }
    assert_eq!(
        cache.column_hits(c_id),
        BLOCKS,
        "c hit once per block on run 1"
    );
}

/// The over-cap half on the fast path: with the cap at the scan's own
/// estimate the set is over 0.95 of it, so `c` is neither looked up nor
/// offered. Each run looks up 24 columns (`ts`, `stream_ref` and `d` of 8
/// blocks), the count the fast path looked up before it made a verdict:
/// `(0, 24, 24)` cold and `(24, 0, 0)` hot, the cache ending with 24 entries
/// and no `c`. Against a fast path with no verdict the counts are the same
/// and the estimate assertion fails (`None` where the estimate is expected).
///
/// `segmentsOpened` (issue #3745): each object counts once across plan and
/// scan on every run, under plan for an object whose footer the verdict
/// decoded and under scan otherwise.
#[tokio::test]
async fn an_over_cap_contains_scan_on_the_whole_segment_fast_path_is_not_served() {
    let t = tenant();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStoreBackend> = mem;
    let mut objects = Vec::new();
    for seq in 1..=OBJECTS {
        objects.push(publish_object(store.as_ref(), &t, seq).await);
    }
    let c_id = column_id(&objects, "c", FieldType::Str);
    let estimate = contains_estimate(&store, &t).await;
    let expected = executor(&store, None)
        .execute(t.hash(), &request(CONTAINS_SQL))
        .await
        .expect("uncached statement executes");
    let expected = pretty_format_batches(expected.output.batches())
        .expect("format")
        .to_string();
    let cache = Arc::new(DecodedColumnCache::new(
        estimate,
        Some(Arc::new(MemoryBudget::unlimited())),
    ));
    let cached = executor(&store, Some(Arc::clone(&cache)));
    for run in 0..2 {
        let (s, plan_gets, (plan_opened, scan_opened)) =
            fast_path_run(&cached, &t, &expected, run).await;
        assert_eq!(
            plan_opened + scan_opened,
            OBJECTS,
            "run {run}: each object counts once across plan and scan"
        );
        assert_eq!(plan_opened, plan_gets, "run {run}: one count per read");
        let (hits, misses, admissions) = if run == 0 { (0, 24, 24) } else { (24, 0, 0) };
        assert_eq!(
            (s.hits, s.misses, s.admissions),
            (hits, misses, admissions),
            "run {run}"
        );
        assert_eq!(s.deferred_set_estimate_bytes, Some(estimate), "run {run}");
        assert!(!s.deferred_served, "run {run}");
        assert_eq!((s.evictions, s.unvalidated_refusals), (0, 0), "run {run}");
        assert_eq!(
            plan_gets,
            if run == 0 { OBJECTS } else { 0 },
            "run {run}: one directory read per object cold, none once memoised"
        );
    }
    assert_eq!(cache.column_hits(c_id), 0, "c is never served");
    assert_eq!(cache.len(), 24, "ts, stream_ref and d of 8 blocks; no c");
}

/// Objects in the request-count fixture: as many as the read-cache executor's
/// 8 partitions, so its scans take the whole-segment fast path.
const FAST_OBJECTS: u64 = 8;

/// A suffix-probe length shorter than every object in the fixture.
const SHORT_PROBE: u64 = 1024;

/// What the fast-path verdict cost on one run, against the same run with no
/// decoded cache.
#[derive(Clone, Debug, PartialEq, Eq)]
struct VerdictCost {
    /// Plan-phase requests: the verdict's directory reads, since the fast
    /// path runs no plan pass.
    plan_requests: u64,
    /// Plan-phase read-cache hits.
    plan_cache_hits: u64,
    /// Plan-phase decompressed bytes: the verdict's directory decodes.
    plan_decompressed_bytes: u64,
    /// The run's requests over the baseline run's.
    extra_requests: u64,
}

/// A cold and a hot run of [`CONTAINS_SQL`] over [`FAST_OBJECTS`] objects
/// through a read cache, so on the whole-segment fast path, each beside the
/// same run through an executor with no decoded cache, which makes no
/// verdict. `suffix_len` pins the fetcher's suffix probe. Asserts both runs
/// return the same rows, the verdict serves, and the baseline issues no
/// plan-phase request (it is on the fast path).
async fn fast_path_verdict_cost(suffix_len: Option<u64>) -> Vec<VerdictCost> {
    let t = tenant();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStoreBackend> = mem;
    for seq in 1..=FAST_OBJECTS {
        let bytes = publish_object(store.as_ref(), &t, seq).await;
        assert!(
            bytes.len() > SHORT_PROBE as usize,
            "the short probe misses the front"
        );
    }
    let baseline = executor_with(&store, None, true, suffix_len);
    let cache = Arc::new(DecodedColumnCache::new(
        1 << 30,
        Some(Arc::new(MemoryBudget::unlimited())),
    ));
    let cached = executor_with(&store, Some(cache), true, suffix_len);
    let mut costs = Vec::new();
    for run in 0..2 {
        let base = baseline
            .execute(t.hash(), &request(CONTAINS_SQL))
            .await
            .expect("baseline statement executes");
        let got = cached
            .execute(t.hash(), &request(CONTAINS_SQL))
            .await
            .expect("cached statement executes");
        assert_eq!(
            pretty_format_batches(got.output.batches())
                .expect("format")
                .to_string(),
            pretty_format_batches(base.output.batches())
                .expect("format")
                .to_string(),
            "run {run}"
        );
        let s = got
            .stats
            .decoded_cache
            .expect("decodedCache stats with a cache")
            .counters;
        assert!(s.deferred_served, "run {run}: the verdict ran and fits");
        assert_eq!(
            requests(&base.phase_accounting.plan),
            0,
            "run {run}: the baseline is on the fast path"
        );
        let plan = &got.phase_accounting.plan;
        assert_eq!(gets(plan), requests(plan), "run {run}: only GETs");
        costs.push(VerdictCost {
            plan_requests: requests(plan),
            plan_cache_hits: plan.cache_hits,
            plan_decompressed_bytes: plan.decompressed_bytes,
            extra_requests: requests(&got.accounting)
                .checked_sub(requests(&base.accounting))
                .expect("the verdict never saves a request"),
        });
    }
    costs
}

/// The fast-path verdict's cost, under the plan phase's accounting, cold and
/// hot. With the default probe, which covers the whole of an object this
/// small, the verdict issues one plan-phase GET per object cold and decodes
/// each object's directories, and the whole-object opens after it are served
/// that extent by the read cache, so the run issues no more requests than the
/// baseline. With a [`SHORT_PROBE`]-byte probe, shorter than every object, the
/// verdict still reads each object's directories in one GET, but that extent
/// is not the whole object, the opens cannot share it, and the run issues 8
/// more requests than the baseline: one directory read per object. Hot, in
/// both shapes, every segment's estimate is memoised on the decoded cache, so
/// the verdict issues no request, takes no read-cache hit and decompresses
/// nothing.
///
/// Seen failing against the unmemoised verdict, which reads every segment's
/// directories again hot: the hot run reads `plan_cache_hits: 8` and a nonzero
/// `plan_decompressed_bytes` where `0, 0` is expected.
#[tokio::test]
async fn the_fast_path_verdict_reads_directories_once_per_segment_cold_and_nothing_hot() {
    let n = FAST_OBJECTS;
    let covering = fast_path_verdict_cost(None).await;
    let short = fast_path_verdict_cost(Some(SHORT_PROBE)).await;
    for (shape, costs) in [("covering", &covering), ("short", &short)] {
        assert!(
            costs[0].plan_decompressed_bytes > 0,
            "{shape}: the cold verdict decodes directories"
        );
    }
    let hot = VerdictCost {
        plan_requests: 0,
        plan_cache_hits: 0,
        plan_decompressed_bytes: 0,
        extra_requests: 0,
    };
    assert_eq!(
        covering,
        vec![
            VerdictCost {
                plan_requests: n,
                plan_cache_hits: 0,
                plan_decompressed_bytes: covering[0].plan_decompressed_bytes,
                extra_requests: 0,
            },
            hot.clone(),
        ]
    );
    assert_eq!(
        short,
        vec![
            VerdictCost {
                plan_requests: n,
                plan_cache_hits: 0,
                plan_decompressed_bytes: short[0].plan_decompressed_bytes,
                extra_requests: n,
            },
            hot,
        ]
    );
}

/// [`CONTAINS_SQL`] with `p` projected in place of `d`: a different
/// [`ColumnSelection`](ravel_logseg::ColumnSelection) over the same segments.
const CONTAINS_P_SQL: &str = "SELECT ts, p, c FROM logs WHERE c LIKE '%x%' ORDER BY ts";

/// What one fast-path verdict reported and cost.
#[derive(Debug, PartialEq, Eq)]
struct Verdict {
    estimate: u64,
    served: bool,
    /// `deferredSetEstimatePartial`: the verdict stopped before reading
    /// every segment.
    partial: bool,
    /// Plan-phase requests plus plan-phase read-cache hits: the segments
    /// whose directories the verdict read.
    directory_reads: u64,
    plan_decompressed_bytes: u64,
}

/// Runs `sql` through `exec` and returns its verdict. Asserts the rows equal
/// the same statement's through an executor with no decoded cache.
async fn verdict_of(
    exec: &SqlExecutor,
    store: &Arc<dyn ObjectStoreBackend>,
    t: &TenantId,
    sql: &str,
) -> Verdict {
    let base = executor(store, None)
        .execute(t.hash(), &request(sql))
        .await
        .expect("uncached statement executes");
    let got = exec
        .execute(t.hash(), &request(sql))
        .await
        .expect("cached statement executes");
    assert_eq!(
        pretty_format_batches(got.output.batches())
            .expect("format")
            .to_string(),
        pretty_format_batches(base.output.batches())
            .expect("format")
            .to_string(),
        "{sql}"
    );
    let s = got
        .stats
        .decoded_cache
        .expect("decodedCache stats with a cache")
        .counters;
    let plan = &got.phase_accounting.plan;
    Verdict {
        estimate: s
            .deferred_set_estimate_bytes
            .expect("a Contains scan reports its estimate"),
        served: s.deferred_served,
        partial: s.deferred_set_estimate_partial,
        directory_reads: requests(plan) + plan.cache_hits,
        plan_decompressed_bytes: plan.decompressed_bytes,
    }
}

fn fresh_cache() -> Arc<DecodedColumnCache> {
    Arc::new(DecodedColumnCache::new(
        1 << 30,
        Some(Arc::new(MemoryBudget::unlimited())),
    ))
}

/// A store holding objects `seqs`, each published.
async fn fast_store(
    t: &TenantId,
    seqs: std::ops::RangeInclusive<u64>,
) -> Arc<dyn ObjectStoreBackend> {
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStoreBackend> = mem;
    for seq in seqs {
        publish_object(store.as_ref(), t, seq).await;
    }
    store
}

/// The memoised estimate is the estimate: the same fast-path scan run cold,
/// reading every segment's directories, and hot, reading none, reports the
/// same `deferredSetEstimateBytes` and the same `deferredServed`.
#[tokio::test]
async fn the_memoised_fast_path_estimate_equals_the_uncached_one() {
    let t = tenant();
    let store = fast_store(&t, 1..=FAST_OBJECTS).await;
    let exec = executor_with(&store, Some(fresh_cache()), true, None);
    let cold = verdict_of(&exec, &store, &t, CONTAINS_SQL).await;
    let hot = verdict_of(&exec, &store, &t, CONTAINS_SQL).await;
    assert_eq!(
        cold.directory_reads, FAST_OBJECTS,
        "cold reads every segment"
    );
    assert_eq!(
        (hot.directory_reads, hot.plan_decompressed_bytes),
        (0, 0),
        "hot reads nothing"
    );
    assert_eq!(
        (hot.estimate, hot.served),
        (cold.estimate, cold.served),
        "the memoised verdict is the uncached one"
    );
    assert!(cold.served);
}

/// The memo is keyed by projection: after a scan of [`CONTAINS_SQL`] has
/// memoised every segment, the first scan of [`CONTAINS_P_SQL`] over the
/// same segments reads every segment's directories again, and each reports
/// the estimate a cache with an empty memo reports for its projection.
///
/// Seen failing against a memo keyed on (tenant, object) alone: the second
/// projection's `directory_reads` is 0 where 8 is expected.
#[tokio::test]
async fn the_fast_path_estimate_memo_is_keyed_by_projection() {
    let t = tenant();
    let store = fast_store(&t, 1..=FAST_OBJECTS).await;
    let uncached = |sql: &'static str| {
        let store = Arc::clone(&store);
        let t = t.clone();
        async move {
            let exec = executor_with(&store, Some(fresh_cache()), true, None);
            verdict_of(&exec, &store, &t, sql).await
        }
    };
    let d_alone = uncached(CONTAINS_SQL).await;
    let p_alone = uncached(CONTAINS_P_SQL).await;
    assert_ne!(
        d_alone.estimate, p_alone.estimate,
        "the two projections estimate differently"
    );

    let exec = executor_with(&store, Some(fresh_cache()), true, None);
    let d = verdict_of(&exec, &store, &t, CONTAINS_SQL).await;
    let p = verdict_of(&exec, &store, &t, CONTAINS_P_SQL).await;
    assert_eq!(d.directory_reads, FAST_OBJECTS);
    assert_eq!(
        p.directory_reads, FAST_OBJECTS,
        "a new projection misses the memo on every segment"
    );
    assert_eq!((d.estimate, d.served), (d_alone.estimate, d_alone.served));
    assert_eq!((p.estimate, p.served), (p_alone.estimate, p_alone.served));
}

/// The memo is per segment: a segment published between two runs of the same
/// scan is the only one whose directories the second run reads, and the
/// second run's estimate is the first run's plus that segment's own, as a
/// scan of a store holding only that segment reports it.
///
/// Seen failing against a memo of the whole scan's summed estimate, keyed on
/// its segment set: the second run's `directory_reads` is 9 where 1 is
/// expected.
#[tokio::test]
async fn a_segment_added_between_runs_is_the_only_one_the_verdict_reads() {
    let t = tenant();
    let added = FAST_OBJECTS + 1;
    let store = fast_store(&t, 1..=FAST_OBJECTS).await;
    let cache = fresh_cache();
    let first = verdict_of(
        &executor_with(&store, Some(Arc::clone(&cache)), true, None),
        &store,
        &t,
        CONTAINS_SQL,
    )
    .await;
    assert_eq!(first.directory_reads, FAST_OBJECTS);

    publish_object(store.as_ref(), &t, added).await;
    let second = verdict_of(
        &executor_with(&store, Some(Arc::clone(&cache)), true, None),
        &store,
        &t,
        CONTAINS_SQL,
    )
    .await;
    assert_eq!(
        second.directory_reads, 1,
        "only the added segment's directories are read"
    );

    let alone = fast_store(&t, added..=added).await;
    let added_alone = verdict_of(
        &executor(&alone, Some(fresh_cache())),
        &alone,
        &t,
        CONTAINS_SQL,
    )
    .await;
    assert_eq!(added_alone.directory_reads, 1);
    assert_eq!(
        second.estimate,
        first.estimate + added_alone.estimate,
        "the second estimate is the first plus the added segment's own"
    );
}

/// The first object of the many-segment fixture. Every object from here on
/// carries timestamps of the same width, so every segment's estimate is the
/// same and a running sum lands on exact multiples of it.
const MANY_FIRST: u64 = 10;

/// Objects in the many-segment fixture: three times the read-cache
/// executor's 8 partitions, so a verdict that stops early leaves reads
/// unissued.
const MANY: u64 = 24;

/// Segments summed before the over-cap verdict stops: the cap is this many
/// segments' estimate, so the sum first exceeds 0.95 of the cap here, one
/// segment before it exceeds the cap itself.
const STOP_AFTER: u64 = 10;

fn many_seqs() -> std::ops::RangeInclusive<u64> {
    MANY_FIRST..=MANY_FIRST + MANY - 1
}

/// The one estimate every segment of the many-segment fixture reports, each
/// read alone through a fresh cache.
async fn many_segment_estimate(t: &TenantId) -> u64 {
    let mut estimates = Vec::new();
    for seq in many_seqs() {
        let alone = fast_store(t, seq..=seq).await;
        let v = verdict_of(
            &executor(&alone, Some(fresh_cache())),
            &alone,
            t,
            CONTAINS_SQL,
        )
        .await;
        assert_eq!(v.directory_reads, 1, "seq {seq}");
        estimates.push(v.estimate);
    }
    assert!(
        estimates.windows(2).all(|w| w[0] == w[1]),
        "every segment estimates the same: {estimates:?}"
    );
    estimates[0]
}

/// ADR-3326, serve-if-fits amendment, implementation note: once the running
/// estimate is over 0.95 of the cap the fast-path verdict stops reading. The
/// cap is [`STOP_AFTER`] segments' estimate, so the verdict sums exactly that
/// many segments, reports their sum as a partial estimate (over 0.95 of the
/// cap, a lower bound of the scan's decoded set), serves nothing, and reads
/// fewer directories than the [`MANY`] relevant segments, cold.
///
/// Seen failing against an early stop at the bare cap: the estimate reads 11
/// segments' worth where 10 are expected.
#[tokio::test]
async fn an_over_cap_fast_path_verdict_stops_reading_once_past_the_threshold() {
    let t = tenant();
    let e = many_segment_estimate(&t).await;
    let store = fast_store(&t, many_seqs()).await;
    let cap = STOP_AFTER * e;
    let cache = Arc::new(DecodedColumnCache::new(
        cap,
        Some(Arc::new(MemoryBudget::unlimited())),
    ));
    let v = verdict_of(
        &executor_with(&store, Some(cache), true, None),
        &store,
        &t,
        CONTAINS_SQL,
    )
    .await;
    assert!(
        v.directory_reads < MANY,
        "stopped before reading every segment: {v:?}"
    );
    assert!(!v.served, "{v:?}");
    assert!(v.partial, "{v:?}");
    assert!(u128::from(v.estimate) * 20 > u128::from(cap) * 19, "{v:?}");
    assert_eq!(
        v.estimate, cap,
        "the sum of the segments read before the stop"
    );
}

/// The fitting half of the early stop: a verdict whose sum never passes the
/// threshold reads every segment's directories cold, reports no partial
/// flag, and its estimate is the sum of every segment's own.
#[tokio::test]
async fn a_fitting_fast_path_verdict_reads_and_sums_every_segment() {
    let t = tenant();
    let e = many_segment_estimate(&t).await;
    let store = fast_store(&t, many_seqs()).await;
    let v = verdict_of(
        &executor_with(&store, Some(fresh_cache()), true, None),
        &store,
        &t,
        CONTAINS_SQL,
    )
    .await;
    assert_eq!(
        (v.directory_reads, v.estimate, v.served, v.partial),
        (MANY, MANY * e, true, false)
    );
}

/// The `viewMemo*` keys of `stats.decodedCache` for one call of `sql`, as the
/// executor's per-call stats render them.
async fn view_memo_keys(exec: &SqlExecutor, t: &TenantId, sql: &str) -> [u64; 4] {
    let outcome = exec
        .execute(t.hash(), &request(sql))
        .await
        .expect("statement executes");
    let stats = outcome
        .stats
        .decoded_cache
        .expect("decodedCache stats with a cache");
    let json = ravel_sql::stats_json::decoded_cache_json(&stats);
    [
        "viewMemoBytes",
        "viewMemoFills",
        "viewMemoRefusals",
        "viewMemoReleases",
    ]
    .map(|k| {
        json.get(k)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_else(|| panic!("{k} in {json}"))
    })
}

/// ADR-3326, view memo amendment, decision 7, at the executor: repeated calls
/// of one `GROUP BY` over the plain declared `Str` column `p` (48 distinct
/// values in 8 blocks of 6) report `[viewMemoBytes, viewMemoFills,
/// viewMemoRefusals, viewMemoReleases]` per call. The first call only ghosts
/// and the second admits, so neither fills; the third is the first served
/// call and fills one view memo per block, 8, leaving 48 * 16 = 768 bytes;
/// the fourth fills nothing and still reports the 768-byte level. A pressure
/// release between calls empties the views: the next call refills them and
/// reports none of the releases, which happened before it started. With a
/// view budget of 0 every key reads 0 on every call.
#[tokio::test]
async fn per_call_view_memo_stats_are_the_statements_own() {
    const SQL: &str = "SELECT p, count(*) AS c FROM logs GROUP BY p";
    let views = (ROWS_PER_OBJECT * OBJECTS as usize * 16) as u64;
    let t = tenant();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStoreBackend> = mem;
    for seq in 1..=OBJECTS {
        publish_object(store.as_ref(), &t, seq).await;
    }
    let cache_with = |view_max_bytes: u64| {
        Arc::new(
            DecodedColumnCache::new(1 << 30, Some(Arc::new(MemoryBudget::unlimited())))
                .with_view_max_bytes(view_max_bytes),
        )
    };

    let cache = cache_with(1 << 30);
    let exec = executor(&store, Some(Arc::clone(&cache)));
    let mut calls = Vec::new();
    for _ in 0..4 {
        calls.push(view_memo_keys(&exec, &t, SQL).await);
    }
    assert_eq!(
        calls,
        [
            [0, 0, 0, 0],
            [0, 0, 0, 0],
            [views, 8, 0, 0],
            [views, 0, 0, 0]
        ],
        "ghost, admit, first serve, hit"
    );
    let freed = cache.evict_for_pressure(views, 0, ravel_memory::ExhaustionCause::Accounted);
    assert_eq!(freed, views, "the release frees exactly the views");
    assert_eq!(cache.view_counters().releases, 8);
    assert_eq!(
        view_memo_keys(&exec, &t, SQL).await,
        [views, 8, 0, 0],
        "the refill after a release"
    );
    assert_eq!(view_memo_keys(&exec, &t, SQL).await, [views, 0, 0, 0]);

    let cache = cache_with(0);
    let exec = executor(&store, Some(Arc::clone(&cache)));
    for call in 0..4 {
        assert_eq!(
            view_memo_keys(&exec, &t, SQL).await,
            [0; 4],
            "view budget 0, call {call}"
        );
    }
}
