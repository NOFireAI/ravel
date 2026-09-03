//! ADR-0873 wave 5a end-to-end acceptance (issue #1022): a tenant loaded
//! through the real log-ingest path answers `MIN`/`MAX`/`COUNT` over a declared
//! column from the flush's own commit-record stamps, with zero data GETs and no
//! `LogsScanExec`. This replaces the hand-stamped `SegmentRef` fixture as the
//! proof the feature is real: the stamps here are the ones `LogIngestRouter`
//! actually wrote, carried onto `SegmentRef` by ordinary snapshot resolution.
//!
//! It also pins the write-side properties that do not need the query engine:
//! that every stamp a flush emits round-trips the wave-2 reader with an empty
//! `dropped()` set (a flush never emits a stamp its own reader would drop), and
//! that two flushes of one shard each stamp from their own buffer, not a
//! cumulative one.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::SessionContext;

use ravel_catalog::{
    Catalog, CatalogConfig, DeclaredColumnType, DeclaredTypedColumn, TenantConfig,
    TenantLifecycleState, set_tenant_config,
};
use ravel_commit::declared_stats::read_commit_record;
use ravel_commit::keys;
use ravel_commit::record;
use ravel_ingest::{IngestConfig, LogIngestRouter, WriteMode};
use ravel_object_store::instrument::InstrumentedStore;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_proto::commit::v1::CommitRecord;
use ravel_query::LogSegmentFetcher;
use ravel_sql::{
    DeclaredColumn, DeclaredType, LogsTableProvider, SessionTable, SpillDecision, SqlConfig,
    TenantMemoryAccountant, build_session,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{CommitToken, Signal, TenantHash, TenantId, TimeRange};

mod common;
use common::TestClock;

fn tenant() -> TenantId {
    TenantId::new("hits")
}
fn tenant_hash() -> TenantHash {
    tenant().hash()
}
const COL: &str = "EventDate";
const BOOL_COL: &str = "IsRefresh";
const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Flushes on the first write (`target_bytes: 1`) and never on age, so each
/// strict write drives exactly one complete flush inline.
fn flush_on_first() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 1,
        max_flush_delay: std::time::Duration::from_secs(3600),
        flush_tick: std::time::Duration::from_millis(20),
        ..IngestConfig::default()
    }
}

/// One log record under a fixed stream, carrying whichever declared attributes
/// `attrs` names (absent when the list omits the key: a NULL for that column).
fn record(ts_ns: i64, attrs: Vec<(&str, AttrValue)>) -> NormalizedLogRecord {
    record_on(&[], ts_ns, attrs)
}

/// The same, on a stream whose resource attributes carry `res_extra` beyond the
/// fixed `service.name`. A declared key placed there is the stream-level half of
/// the merged attribute view: it is what a record that does not set the key
/// reads.
fn record_on(
    res_extra: &[(&str, AttrValue)],
    ts_ns: i64,
    attrs: Vec<(&str, AttrValue)>,
) -> NormalizedLogRecord {
    let mut res: Vec<(String, AttrValue)> = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    res.extend(
        res_extra
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect::<Vec<_>>(),
    );
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
        attrs: attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
    }
}

/// The same as [`record_on`], but the stream's SCOPE attributes also carry
/// `scope_extra` beyond an otherwise-empty scope set, so a declared column's
/// stream-level fallback can be exercised on either half of the merged view
/// in one stream (issue #1057 finding 1: a List/Map resource occurrence must
/// not shadow a matching-typed scope occurrence behind it).
fn record_on_scoped(
    res_extra: &[(&str, AttrValue)],
    scope_extra: &[(&str, AttrValue)],
    ts_ns: i64,
    attrs: Vec<(&str, AttrValue)>,
) -> NormalizedLogRecord {
    let mut res: Vec<(String, AttrValue)> = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    res.extend(
        res_extra
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect::<Vec<_>>(),
    );
    let scope_attrs: Vec<(String, AttrValue)> = scope_extra
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
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
        attrs: attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
    }
}

/// Write the tenant's declared typed columns so the flush's config read resolves
/// them and stamps their extrema.
async fn declare_columns(store: &dyn ObjectStoreBackend, columns: Vec<DeclaredTypedColumn>) {
    set_tenant_config(
        store,
        &tenant_hash(),
        &TenantConfig {
            typed_attr_columns: Some(columns),
            ..TenantConfig::new(TenantLifecycleState::Active)
        },
        BASE_NS,
    )
    .await
    .expect("set tenant config");
}

/// Decode the single L0 commit record the shard published for `token`.
async fn commit_record_for(store: &dyn ObjectStoreBackend, token: &CommitToken) -> CommitRecord {
    let commit_key =
        keys::commit_key_for_token(&tenant_hash(), Signal::Logs, token).expect("commit key");
    let bytes = store
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    record::decode(&bytes).expect("decode commit record")
}

fn i64_col_decl() -> Vec<DeclaredTypedColumn> {
    vec![DeclaredTypedColumn {
        key: COL.to_string(),
        ty: DeclaredColumnType::I64,
    }]
}

fn single_i64_value(batches: &[RecordBatch], col: usize) -> Option<i64> {
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let arr = batch
            .column(col)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 aggregate");
        return if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0))
        };
    }
    None
}

fn logs_session(provider: LogsTableProvider) -> SessionContext {
    let config = SqlConfig::default();
    let tenant = TenantMemoryAccountant::new(1 << 30);
    let (pool, _breach) = config.query_pool(tenant, QueryAccounting::new());
    build_session(
        &config,
        pool,
        SessionTable::Logs(Arc::new(provider)),
        false,
        SpillDecision::Disabled,
    )
    .expect("build session")
}

/// The acceptance test: load `N` known records through the real ingest path,
/// then answer `MIN`/`MAX` and `COUNT` over the declared column from the
/// resolved snapshot's stamps, asserting the exact answers, zero data GETs, and
/// no `LogsScanExec`.
#[tokio::test]
async fn min_max_count_answered_from_ingest_stamps_with_zero_gets() {
    let store = Arc::new(InstrumentedStore::new(MemoryStore::new()));
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let clock = TestClock::new(BASE_NS);

    declare_columns(backend.as_ref(), i64_col_decl()).await;

    // Ten records: EventDate present in eight, absent in two. Known extrema and
    // null count.
    let values = [17_100, 19_400, -5, 0, i64::MAX, i64::MIN, 42, 100];
    let mut records: Vec<NormalizedLogRecord> = values
        .iter()
        .enumerate()
        .map(|(i, v)| record(BASE_NS + i as i64, vec![(COL, AttrValue::I64(*v))]))
        .collect();
    records.push(record(BASE_NS + 100, vec![]));
    records.push(record(BASE_NS + 101, vec![]));
    let sample_count = records.len() as u64;
    let expected_min = *values.iter().min().unwrap();
    let expected_max = *values.iter().max().unwrap();
    let expected_non_null = values.len() as i64;

    let router = LogIngestRouter::new(flush_on_first(), Arc::clone(&backend), clock.clone());
    let receipt = router
        .write(
            tenant(),
            records,
            WriteMode::Strict,
            std::time::Duration::from_secs(5),
        )
        .await
        .expect("strict write flushes");
    assert_eq!(receipt.tokens.len(), 1, "one object for one flush");
    router.shutdown().await;

    // The published commit record carries the stamp the flush computed, and its
    // own reader drops none of it.
    let commit = commit_record_for(backend.as_ref(), &receipt.tokens[0]).await;
    let read = read_commit_record(&commit);
    assert!(
        read.dropped().is_empty(),
        "the flush's own reader drops nothing it stamped"
    );
    let stamped = read.column(COL).expect("EventDate stamped");
    assert_eq!(
        stamped.min(),
        Some(ravel_types::declared_stats::DeclaredStatValue::I64(
            expected_min
        ))
    );
    assert_eq!(
        stamped.null_count(),
        sample_count - expected_non_null as u64
    );

    // Resolve the snapshot the way a query does; the GETs it costs happen before
    // the measured window below.
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog"));
    let snapshot = catalog
        .resolve(
            &tenant_hash(),
            Signal::Logs,
            TimeRange {
                start_ns: 0,
                end_ns: i64::MAX,
            },
            &[],
            BASE_NS + 1_000,
        )
        .await
        .expect("resolve logs snapshot");

    let declared = vec![DeclaredColumn::new(COL, DeclaredType::I64)];
    let accounting = QueryAccounting::new();
    let provider = LogsTableProvider::new(
        snapshot,
        tenant_hash(),
        LogSegmentFetcher::new(Arc::clone(&backend)),
        accounting.clone(),
    )
    .with_declared_columns(declared);
    let ctx = logs_session(provider);

    let gets_before = store.metrics().snapshot().get.calls;
    let sql = format!(r#"SELECT MIN("{COL}"), MAX("{COL}"), COUNT("{COL}") FROM logs"#);
    let plan = ctx
        .sql(&sql)
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    let gets_after = store.metrics().snapshot().get.calls;

    assert_eq!(
        single_i64_value(&batches, 0),
        Some(expected_min),
        "MIN answered from the stamp"
    );
    assert_eq!(
        single_i64_value(&batches, 1),
        Some(expected_max),
        "MAX answered from the stamp"
    );
    assert_eq!(
        single_i64_value(&batches, 2),
        Some(expected_non_null),
        "COUNT answered from the stamp's null count"
    );
    assert!(
        !plan_text.contains("LogsScanExec"),
        "the scan must be elided, not pruned:\n{plan_text}"
    );
    assert_eq!(
        gets_after - gets_before,
        0,
        "a statement answered from stamps reads no data object"
    );
}

/// Extrema at the type edges, over BOTH stamp-eligible types, straight off a
/// real flush's commit record: negative and `i64::MIN`/`i64::MAX` for I64, and
/// `false`/`true` for BOOL, plus an all-null declared column stamping absent
/// extrema with `null_count == sample_count`.
#[tokio::test]
async fn edge_extrema_and_all_null_stamped_from_a_real_flush() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    // Three declared columns: an I64 with edge values, a BOOL, and an I64 that
    // no record ever sets (all-null).
    declare_columns(
        store.as_ref(),
        vec![
            DeclaredTypedColumn {
                key: COL.to_string(),
                ty: DeclaredColumnType::I64,
            },
            DeclaredTypedColumn {
                key: BOOL_COL.to_string(),
                ty: DeclaredColumnType::Bool,
            },
            DeclaredTypedColumn {
                key: "NeverSet".to_string(),
                ty: DeclaredColumnType::I64,
            },
        ],
    )
    .await;

    let records = vec![
        record(
            BASE_NS,
            vec![
                (COL, AttrValue::I64(i64::MIN)),
                (BOOL_COL, AttrValue::Bool(false)),
            ],
        ),
        record(
            BASE_NS + 1,
            vec![(COL, AttrValue::I64(-1)), (BOOL_COL, AttrValue::Bool(true))],
        ),
        record(
            BASE_NS + 2,
            vec![
                (COL, AttrValue::I64(i64::MAX)),
                (BOOL_COL, AttrValue::Bool(false)),
            ],
        ),
    ];
    let sample_count = records.len() as u64;

    let router = LogIngestRouter::new(flush_on_first(), Arc::clone(&store), clock.clone());
    let receipt = router
        .write(
            tenant(),
            records,
            WriteMode::Strict,
            std::time::Duration::from_secs(5),
        )
        .await
        .expect("strict write flushes");
    router.shutdown().await;

    let commit = commit_record_for(store.as_ref(), &receipt.tokens[0]).await;
    let read = read_commit_record(&commit);
    assert!(read.dropped().is_empty(), "nothing dropped");

    use ravel_types::declared_stats::DeclaredStatValue;
    let ev = read.column(COL).expect("EventDate stamped");
    assert_eq!(ev.min(), Some(DeclaredStatValue::I64(i64::MIN)));
    assert_eq!(ev.max(), Some(DeclaredStatValue::I64(i64::MAX)));
    assert_eq!(ev.null_count(), 0);

    let refresh = read.column(BOOL_COL).expect("IsRefresh stamped");
    assert_eq!(refresh.min(), Some(DeclaredStatValue::Bool(false)));
    assert_eq!(refresh.max(), Some(DeclaredStatValue::Bool(true)));
    assert_eq!(refresh.null_count(), 0);

    // The declared column no record set: absent extrema, null_count == rows.
    let never = read
        .column("NeverSet")
        .expect("all-null column still stamped");
    assert_eq!(never.min(), None);
    assert_eq!(never.max(), None);
    assert_eq!(never.null_count(), sample_count);
}

/// Two flushes of one shard produce two commit records, each stamped from its
/// OWN buffer's figures, never a cumulative one.
#[tokio::test]
async fn two_flushes_stamp_from_their_own_buffers() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    declare_columns(store.as_ref(), i64_col_decl()).await;

    let router = LogIngestRouter::new(flush_on_first(), Arc::clone(&store), clock.clone());

    // First flush: EventDate in {10, 30}.
    let first = router
        .write(
            tenant(),
            vec![
                record(BASE_NS, vec![(COL, AttrValue::I64(10))]),
                record(BASE_NS + 1, vec![(COL, AttrValue::I64(30))]),
            ],
            WriteMode::Strict,
            std::time::Duration::from_secs(5),
        )
        .await
        .expect("first flush");
    // Second flush: EventDate in {100, 200}, disjoint from the first.
    let second = router
        .write(
            tenant(),
            vec![
                record(BASE_NS + 2, vec![(COL, AttrValue::I64(100))]),
                record(BASE_NS + 3, vec![(COL, AttrValue::I64(200))]),
            ],
            WriteMode::Strict,
            std::time::Duration::from_secs(5),
        )
        .await
        .expect("second flush");
    router.shutdown().await;

    use ravel_types::declared_stats::DeclaredStatValue;

    let c1 = commit_record_for(store.as_ref(), &first.tokens[0]).await;
    let s1 = read_commit_record(&c1);
    let ev1 = s1.column(COL).expect("first stamp");
    assert_eq!(ev1.min(), Some(DeclaredStatValue::I64(10)));
    assert_eq!(
        ev1.max(),
        Some(DeclaredStatValue::I64(30)),
        "not cumulative"
    );
    assert_eq!(c1.sample_count, 2);
    assert_eq!(ev1.null_count(), 0);

    let c2 = commit_record_for(store.as_ref(), &second.tokens[0]).await;
    let s2 = read_commit_record(&c2);
    let ev2 = s2.column(COL).expect("second stamp");
    assert_eq!(
        ev2.min(),
        Some(DeclaredStatValue::I64(100)),
        "second buffer only, the first buffer's 10 is gone"
    );
    assert_eq!(ev2.max(), Some(DeclaredStatValue::I64(200)));
    assert_eq!(c2.sample_count, 2);

    // Two distinct data objects, one per flush.
    assert_ne!(
        c1.object_key, c2.object_key,
        "each flush wrote its own data object"
    );
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        objects.iter().any(|m| m.key == c1.object_key),
        "first flush's data object is durable"
    );
    assert!(
        objects.iter().any(|m| m.key == c2.object_key),
        "second flush's data object is durable"
    );
}

/// Write `records` through the real router, then answer
/// `MIN`/`MAX`/`COUNT` over `COL` from the resolved snapshot. Returns the
/// commit record of the single flush, the three answers, the plan text, and the
/// GETs the measured query window cost.
async fn stamped_min_max_count(
    records: Vec<NormalizedLogRecord>,
) -> (
    CommitRecord,
    (Option<i64>, Option<i64>, Option<i64>),
    String,
    u64,
) {
    let store = Arc::new(InstrumentedStore::new(MemoryStore::new()));
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let clock = TestClock::new(BASE_NS);
    declare_columns(backend.as_ref(), i64_col_decl()).await;

    let router = LogIngestRouter::new(flush_on_first(), Arc::clone(&backend), clock.clone());
    let receipt = router
        .write(
            tenant(),
            records,
            WriteMode::Strict,
            std::time::Duration::from_secs(5),
        )
        .await
        .expect("strict write flushes");
    assert_eq!(receipt.tokens.len(), 1, "one object for one flush");
    router.shutdown().await;

    let commit = commit_record_for(backend.as_ref(), &receipt.tokens[0]).await;

    let catalog =
        Arc::new(Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog"));
    let snapshot = catalog
        .resolve(
            &tenant_hash(),
            Signal::Logs,
            TimeRange {
                start_ns: 0,
                end_ns: i64::MAX,
            },
            &[],
            BASE_NS + 1_000,
        )
        .await
        .expect("resolve logs snapshot");

    let provider = LogsTableProvider::new(
        snapshot,
        tenant_hash(),
        LogSegmentFetcher::new(Arc::clone(&backend)),
        QueryAccounting::new(),
    )
    .with_declared_columns(vec![DeclaredColumn::new(COL, DeclaredType::I64)]);
    let ctx = logs_session(provider);

    let gets_before = store.metrics().snapshot().get.calls;
    let sql = format!(r#"SELECT MIN("{COL}"), MAX("{COL}"), COUNT("{COL}") FROM logs"#);
    let plan = ctx
        .sql(&sql)
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    let gets = store.metrics().snapshot().get.calls - gets_before;

    let answers = (
        single_i64_value(&batches, 0),
        single_i64_value(&batches, 1),
        single_i64_value(&batches, 2),
    );
    (commit, answers, plan_text, gets)
}

/// Issue #1057, end to end: a declared column whose value lives on the stream's
/// RESOURCE attributes, set by no record, is the value every record reads. The
/// flush stamps it, and `MIN`/`MAX` answer with it off the stamp alone.
///
/// Prove-the-test: fold only the record's own attributes (drop the stream-level
/// fallback from `DeclaredStatAccum::build_stamps`) and the stamp becomes
/// `min == None, max == None, null_count == 3`, the affirmative all-NULL claim
/// issue #1057 is about. DataFusion's aggregate-statistics rule declines that
/// NULL min/max scalar, so `MIN`/`MAX` would still fall back to a scan and
/// answer correctly; only `COUNT` trusts the stamp outright and would answer
/// 0 against a true 3.
#[tokio::test]
async fn stream_level_declared_value_answers_min_max_end_to_end() {
    let records = (0..3)
        .map(|i| record_on(&[(COL, AttrValue::I64(7))], BASE_NS + i, vec![]))
        .collect::<Vec<_>>();

    let (commit, (min, max, count), plan_text, gets) = stamped_min_max_count(records).await;

    let read = read_commit_record(&commit);
    assert!(
        read.dropped().is_empty(),
        "the flush's own reader drops nothing it stamped"
    );
    assert_eq!(commit.sample_count, 3);
    let ev = read.column(COL).expect("EventDate stamped");
    assert_eq!(
        ev.min(),
        Some(ravel_types::declared_stats::DeclaredStatValue::I64(7)),
        "the resource attribute is the value all three rows read"
    );
    assert_eq!(
        ev.max(),
        Some(ravel_types::declared_stats::DeclaredStatValue::I64(7))
    );
    assert_eq!(
        ev.null_count(),
        0,
        "no row is NULL: the stream supplies the value"
    );

    assert_eq!(min, Some(7), "MIN answered from the stamp");
    assert_eq!(max, Some(7), "MAX answered from the stamp");
    assert_eq!(count, Some(3), "every row counts as non-NULL");
    assert!(
        !plan_text.contains("LogsScanExec"),
        "the scan is elided, which is what makes the answer exact:\n{plan_text}"
    );
    assert_eq!(gets, 0, "a statement answered from stamps reads no data");
}

/// The override half of the merged view, end to end: one record sets the
/// declared key itself, the other two read the stream's value. The extrema span
/// both.
///
/// Prove-the-test: count the stream-level value for every row instead of
/// `rows - overrides` and the answers stay `7`/`7`, losing the record's `2`.
#[tokio::test]
async fn record_override_of_a_stream_level_value_answers_end_to_end() {
    let stream_res = [(COL, AttrValue::I64(7))];
    let records = vec![
        record_on(&stream_res, BASE_NS, vec![]),
        record_on(&stream_res, BASE_NS + 1, vec![(COL, AttrValue::I64(2))]),
        record_on(&stream_res, BASE_NS + 2, vec![]),
    ];

    let (commit, (min, max, count), plan_text, gets) = stamped_min_max_count(records).await;

    let read = read_commit_record(&commit);
    assert!(read.dropped().is_empty(), "nothing dropped");
    assert_eq!(commit.sample_count, 3);
    let ev = read.column(COL).expect("EventDate stamped");
    assert_eq!(
        ev.min(),
        Some(ravel_types::declared_stats::DeclaredStatValue::I64(2)),
        "the record's own value wins for its row"
    );
    assert_eq!(
        ev.max(),
        Some(ravel_types::declared_stats::DeclaredStatValue::I64(7)),
        "the two rows that set nothing still read the stream's 7"
    );
    assert_eq!(ev.null_count(), 0);

    assert_eq!(min, Some(2), "MIN answered from the stamp");
    assert_eq!(max, Some(7), "MAX answered from the stamp");
    assert_eq!(count, Some(3));
    assert!(
        !plan_text.contains("LogsScanExec"),
        "the scan is elided:\n{plan_text}"
    );
    assert_eq!(gets, 0, "a statement answered from stamps reads no data");
}

/// Issue #1057 finding 1, end to end: a stream whose RESOURCE attributes carry
/// the declared column as a List (the shape `ravel_otlp::logs_normalize::
/// convert_attrs` produces from real OTLP input) and whose SCOPE attributes
/// carry it as a matching-typed I64 must answer from the scope value for the
/// rows that do not set the key themselves -- the List occurrence must not
/// shadow the scope occurrence behind it, exactly as the reader's decoder
/// (which never decodes a List entry at all) resolves it.
///
/// Prove-the-test: drop the `AttrValue::List(_) | AttrValue::Map(_)` skip from
/// `stream_state` (`crates/ravel-ingest/src/log_declared_stats.rs`) and this
/// answers `2, 2, 1` (the base's wrong answer, with a data scan needed to even
/// get COUNT right) instead of `2, 7, 3` with the scan elided and zero GETs.
#[tokio::test]
async fn list_resource_attribute_falls_back_to_scope_value_end_to_end() {
    let stream_res = [(COL, AttrValue::List(vec![AttrValue::I64(1)]))];
    let stream_scope = [(COL, AttrValue::I64(7))];
    let records = vec![
        record_on_scoped(
            &stream_res,
            &stream_scope,
            BASE_NS,
            vec![(COL, AttrValue::I64(2))],
        ),
        record_on_scoped(&stream_res, &stream_scope, BASE_NS + 1, vec![]),
        record_on_scoped(&stream_res, &stream_scope, BASE_NS + 2, vec![]),
    ];

    let (commit, (min, max, count), plan_text, gets) = stamped_min_max_count(records).await;

    let read = read_commit_record(&commit);
    assert!(
        read.dropped().is_empty(),
        "the flush's own reader drops nothing it stamped"
    );
    assert_eq!(commit.sample_count, 3);
    let ev = read.column(COL).expect("EventDate stamped");
    assert_eq!(
        ev.min(),
        Some(ravel_types::declared_stats::DeclaredStatValue::I64(2)),
        "the record's own value wins for its row"
    );
    assert_eq!(
        ev.max(),
        Some(ravel_types::declared_stats::DeclaredStatValue::I64(7)),
        "the two rows that set nothing read the scope's 7, not the List"
    );
    assert_eq!(ev.null_count(), 0);

    assert_eq!(min, Some(2), "MIN answered from the stamp");
    assert_eq!(max, Some(7), "MAX answered from the stamp");
    assert_eq!(count, Some(3), "every row counts as non-NULL");
    assert!(
        !plan_text.contains("LogsScanExec"),
        "the scan is elided:\n{plan_text}"
    );
    assert_eq!(gets, 0, "a statement answered from stamps reads no data");
}
