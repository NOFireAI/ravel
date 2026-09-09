//! Issue #1036: a write refused with `MixedBufferRepresentation` must leave the
//! buffer's declared-column accumulator untouched, in both directions.
//!
//! `LogTenantBuf::merge_rows`/`merge_columnar` used to fold the write's extrema
//! into `declared_stats` before the `BufContent` match that refuses the other
//! representation, so a refused write's MIN/MAX survived in the accumulator and
//! the next flush stamped them onto a commit record whose object does not hold
//! those records. ADR-0873 wave 4 answers `MIN`/`MAX` from a stamp as exact, so
//! that stamp makes a query return a value absent from the data.
//!
//! Each test below seeds one representation, attempts the other carrying an
//! eligible I64 attribute that would widen the stamp on both ends, and asserts
//! the exact post-flush extrema, not merely that a count did not move.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{
    DeclaredColumnType, DeclaredTypedColumn, TenantConfig, TenantLifecycleState, set_tenant_config,
};
use ravel_commit::declared_stats::read_commit_record;
use ravel_commit::record;
use ravel_ingest::{IngestConfig, LogIngestRouter, LogWriteError, WriteMode};
use ravel_logseg::{ColumnarLogBatch, LogRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_proto::commit::v1::CommitRecord;
use ravel_types::declared_stats::DeclaredStatValue;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{TenantHash, TenantId};

mod common;
use common::TestClock;

const COL: &str = "EventDate";
const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// The two values the accepted seed write carries: the only extrema any stamp
/// below may report.
const SEED_MIN: i64 = 100;
const SEED_MAX: i64 = 200;
/// The two values the refused write carries. Both lie strictly outside the seed
/// range, so folding either one leaves a stamp no assertion below can mistake
/// for the accepted extrema.
const REFUSED_LOW: i64 = -999;
const REFUSED_HIGH: i64 = 999_999;

fn tenant() -> TenantId {
    TenantId::new("hits")
}

fn tenant_hash() -> TenantHash {
    tenant().hash()
}

/// Never flushes on size or age: both writes meet one live buffer, and the
/// flush happens only when the test asks for it.
fn never_flush() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: usize::MAX,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        ..IngestConfig::default()
    }
}

fn record(ts_ns: i64, value: Option<i64>) -> NormalizedLogRecord {
    let res: Vec<(String, AttrValue)> = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    let stream_id = log_stream_id(&res, "scope", "", &scope_attrs);
    let stream_attrs = ravel_logseg::stream_attrs_bytes(&res, "scope", "", &scope_attrs);
    NormalizedLogRecord {
        stream_id,
        stream_attrs,
        ts_ns,
        observed_ts_ns: ts_ns,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: "row".to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: value
            .map(|v| vec![(COL.to_string(), AttrValue::I64(v))])
            .unwrap_or_default(),
    }
}

fn to_logrecord(r: &NormalizedLogRecord) -> LogRecord {
    LogRecord {
        stream_id: r.stream_id,
        stream_attrs: r.stream_attrs.clone(),
        ts_ns: r.ts_ns,
        observed_ts_ns: r.observed_ts_ns,
        severity_num: r.severity_num,
        severity_text: r.severity_text.clone(),
        body: r.body.clone(),
        trace_id: r.trace_id,
        span_id: r.span_id,
        flags: r.flags,
        attrs: r.attrs.clone(),
    }
}

/// The accepted write: `SEED_MIN` and `SEED_MAX` under the declared column,
/// plus two records that omit it.
///
/// The two NULL rows are load-bearing, not padding. They leave the object's
/// `sample_count` (4) two rows above the accumulator's non-null count (2), so
/// the refused write's two extra non-null observations still fit under
/// `sample_count` and the resulting stamp passes the reader's consistency
/// clause instead of being dropped. Without them the bug shows up as a dropped
/// stamp; with them it shows up as what ADR-0873 wave 4 hands a query: an exact
/// MIN that is not in the object.
fn seed_records() -> Vec<NormalizedLogRecord> {
    vec![
        record(BASE_NS, Some(SEED_MIN)),
        record(BASE_NS + 1, Some(SEED_MAX)),
        record(BASE_NS + 2, None),
        record(BASE_NS + 3, None),
    ]
}

/// The refused write: two records whose values sit strictly outside the seed
/// range on both ends.
fn refused_records() -> Vec<NormalizedLogRecord> {
    vec![
        record(BASE_NS + 4, Some(REFUSED_LOW)),
        record(BASE_NS + 5, Some(REFUSED_HIGH)),
    ]
}

fn columnar(records: &[NormalizedLogRecord]) -> ColumnarLogBatch {
    let rows: Vec<LogRecord> = records.iter().map(to_logrecord).collect();
    ColumnarLogBatch::from_records(&rows)
}

async fn declare_i64_column(store: &dyn ObjectStoreBackend) {
    set_tenant_config(
        store,
        &tenant_hash(),
        &TenantConfig {
            typed_attr_columns: Some(vec![DeclaredTypedColumn {
                key: COL.to_string(),
                ty: DeclaredColumnType::I64,
            }]),
            ..TenantConfig::new(TenantLifecycleState::Active)
        },
        BASE_NS,
    )
    .await
    .expect("set tenant config");
}

/// Decode the one commit record the shard published. The seeding write is
/// buffered, so no token comes back to address it directly.
async fn only_commit_record(store: &dyn ObjectStoreBackend) -> CommitRecord {
    let objects = list_all(store, "t/").await.expect("list");
    let commit_keys: Vec<String> = objects
        .iter()
        .filter(|o| o.key.contains("/c/"))
        .map(|o| o.key.clone())
        .collect();
    assert_eq!(
        commit_keys.len(),
        1,
        "one flush, so exactly one commit record: {commit_keys:?}"
    );
    let raw = store
        .get(&commit_keys[0], GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    record::decode(&raw).expect("decode commit record")
}

/// Assert the flush stamped exactly the seed write's extrema over exactly the
/// seed write's rows: the refused write contributed neither an extremum nor a
/// non-null row.
fn assert_seed_only_stamp(commit: &CommitRecord) {
    assert_eq!(
        commit.sample_count, 4,
        "only the seed write's four records are in the object"
    );
    let read = read_commit_record(commit);
    assert!(
        read.dropped().is_empty(),
        "the flush's own reader drops nothing it stamped, dropped: {:?}",
        read.dropped()
    );
    let stat = read.column(COL).expect("EventDate stamped");
    assert_eq!(
        stat.min(),
        Some(DeclaredStatValue::I64(SEED_MIN)),
        "MIN is the accepted write's minimum, not the refused write's {REFUSED_LOW}"
    );
    assert_eq!(
        stat.max(),
        Some(DeclaredStatValue::I64(SEED_MAX)),
        "MAX is the accepted write's maximum, not the refused write's {REFUSED_HIGH}"
    );
    assert_eq!(
        stat.null_count(),
        2,
        "the two seed rows that omit the column are the object's only NULLs; \
         a refused write's rows must not be counted as non-null"
    );
}

/// A columnar write refused by a row-major buffer leaves no trace in the
/// declared-stat accumulator: the flush stamps the row write's extrema exactly.
#[tokio::test]
async fn refused_columnar_write_leaves_the_row_buffers_stamp_exact() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    declare_i64_column(store.as_ref()).await;

    let router = LogIngestRouter::new(never_flush(), Arc::clone(&store), clock.clone());

    // Seed: row-major, buffered (no flush trigger fires under `never_flush`).
    router
        .write(
            tenant(),
            seed_records(),
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered row write is accepted");

    // The refused write, Strict so the shard's typed error surfaces here.
    let err = router
        .write_columnar(
            tenant(),
            columnar(&refused_records()),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("a columnar write into a row-major buffer is refused");
    assert!(
        matches!(err, LogWriteError::MixedBufferRepresentation(_)),
        "expected the typed mixed-representation error, got {err:?}"
    );

    router.flush_all().await;
    router.shutdown().await;

    assert_seed_only_stamp(&only_commit_record(store.as_ref()).await);
}

/// The mirror direction: a row-major write refused by a columnar buffer leaves
/// no trace either.
#[tokio::test]
async fn refused_row_write_leaves_the_columnar_buffers_stamp_exact() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    declare_i64_column(store.as_ref()).await;

    let router = LogIngestRouter::new(never_flush(), Arc::clone(&store), clock.clone());

    // Seed: columnar, buffered.
    router
        .write_columnar(
            tenant(),
            columnar(&seed_records()),
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered columnar write is accepted");

    let err = router
        .write(
            tenant(),
            refused_records(),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("a row-major write into a columnar buffer is refused");
    assert!(
        matches!(err, LogWriteError::MixedBufferRepresentation(_)),
        "expected the typed mixed-representation error, got {err:?}"
    );

    router.flush_all().await;
    router.shutdown().await;

    assert_seed_only_stamp(&only_commit_record(store.as_ref()).await);
}
