//! Issue #4660: `x = ''` and `x <> ''` over a string or binary `x` are
//! evaluated from value lengths (`EmptyCompareExpr`, shown as `empty_eq(..)`
//! and `empty_ne(..)`), in a `FilterExec` and in the `logs` scan's row
//! filter.
//!
//! Every statement runs through the production session (`build_session`) over
//! real RLOG objects in a `MemoryStore`. The reference for an answer is the
//! same statement on the same session with the `empty_string_compare` rule
//! removed, so arrow's comparison kernel evaluates it.
//!
//! - `the_rewrite_appears_only_for_the_empty_literal` (T1).
//! - `answers_match_the_kernel_in_every_layout` (T2).
//! - `the_rule_runs_after_every_rule_that_reads_a_predicate` (class closure).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Int32Type, TimestampNanosecondType};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::SessionContext;
use ravel_catalog::SegmentRef;
use ravel_codec::encoding::Enc;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig};
use ravel_object_store::memory::MemoryStore;
use ravel_sql::{
    COMPACT_SLICES_RULE, CeilingBreach, DeclaredColumn, DeclaredType, EMPTY_STRING_COMPARE_RULE,
    LogsTableProvider, METADATA_ONLY_AGGREGATE_RULE, SessionTable, SpillDecision, SqlConfig,
    TOPK_LATE_MATERIALIZATION_RULE, TenantDelegatingPool, TenantMemoryAccountant, build_session,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;

use crate::logs_columnar::{column_page_encs, str_column_id};
use crate::logs_str_not_equal_row_filter::{
    Stream, encode, nodes, pretty, provider, put, record, run, s,
};

fn session(provider: LogsTableProvider) -> SessionContext {
    let pool = Arc::new(TenantDelegatingPool::new(
        1 << 30,
        TenantMemoryAccountant::new(1 << 30),
        CeilingBreach::new(),
        QueryAccounting::new(),
    ));
    build_session(
        &SqlConfig::default(),
        pool,
        SessionTable::Logs(Arc::new(provider)),
        false,
        SpillDecision::Disabled,
        &ravel_sql::SessionPrincipal::without_capabilities(TenantHash([0u8; 16])),
    )
    .expect("session")
}

/// `ctx` with the `empty_string_compare` rule removed and every other rule in
/// place, over the same tables.
fn without_rule(ctx: &SessionContext) -> SessionContext {
    let state = ctx.state();
    let rules = state
        .physical_optimizers()
        .iter()
        .filter(|rule| rule.name() != EMPTY_STRING_COMPARE_RULE)
        .cloned()
        .collect();
    let state = SessionStateBuilder::new_from_existing(state)
        .with_physical_optimizer_rules(rules)
        .build();
    SessionContext::new_with_state(state)
}

fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("s", DeclaredType::Str),
        DeclaredColumn::new("t", DeclaredType::Str),
        DeclaredColumn::new("b", DeclaredType::Bytes),
        DeclaredColumn::new("n", DeclaredType::I64),
    ]
}

/// A row: `s` (absent when `None`), `b` (absent when `None`), and
/// `n = ts % 100 + 1`.
fn row(ts: i64, sv: Option<&str>, bv: Option<&[u8]>) -> LogRecord {
    let mut attrs: Vec<(&str, AttrValue)> = vec![("n", AttrValue::I64(ts % 100 + 1))];
    if let Some(v) = sv {
        attrs.push(("s", s(v)));
    }
    if let Some(v) = bv {
        attrs.push(("b", AttrValue::Bytes(v.to_vec())));
    }
    record(ts, Stream::Bare, &attrs)
}

/// Three objects, one block each, so `s` reaches the filter in every
/// dictionary shape the scan builds for a declared `Str` column:
///
/// - `plain`: a plain page (values distinct), keys the identity, values
///   holding `''`;
/// - `dict_empty`: a dictionary page whose values hold `''`;
/// - `dict_none`: a dictionary page whose values do not hold `''`.
///
/// Each object has a row with no `s` (NULL). `b` is a declared `Bytes`
/// column, which the scan builds as `Binary`, with `''`, non-empty and NULL
/// values in the plain object.
async fn fixture() -> (Arc<MemoryStore>, Vec<SegmentRef>) {
    let store = Arc::new(MemoryStore::new());
    let cfg = RlogConfig::default();
    let plain = vec![
        row(100, Some("p1"), Some(b"")),
        row(101, Some(""), Some(b"x")),
        row(102, None, None),
        row(103, Some("p2"), Some(b"")),
        row(104, Some("p3"), Some(b"y")),
    ];
    let dict_empty = vec![
        row(200, Some("d"), None),
        row(201, Some(""), None),
        row(202, Some("d"), None),
        row(203, Some(""), None),
        row(204, Some("d"), None),
        row(205, Some("e"), None),
        row(206, None, None),
        row(207, Some("d"), None),
    ];
    let dict_none = vec![
        row(300, Some("d"), None),
        row(301, Some("e"), None),
        row(302, Some("d"), None),
        row(303, Some("e"), None),
        row(304, Some("d"), None),
        row(305, None, None),
        row(306, Some("e"), None),
        row(307, Some("d"), None),
    ];
    let mut segments = Vec::new();
    for (key, records, dict) in [
        ("logs/plain.rlog", &plain, false),
        ("logs/dict_empty.rlog", &dict_empty, true),
        ("logs/dict_none.rlog", &dict_none, true),
    ] {
        let bytes = encode(records, cfg);
        let encs = column_page_encs(&bytes, cfg, str_column_id(&bytes, cfg, "s"));
        assert_eq!(encs.contains(&Enc::Dict), dict, "{key}: {encs:?}");
        segments.push(put(&store, key, bytes, records).await);
    }
    (store, segments)
}

fn timestamps(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let ts = b.column(b.schema().index_of("ts").expect("ts"));
            ts.as_primitive::<TimestampNanosecondType>()
                .values()
                .to_vec()
        })
        .collect();
    out.sort_unstable();
    out
}

fn shown(plan: &Arc<dyn ExecutionPlan>) -> String {
    displayable(plan.as_ref()).indent(true).to_string()
}

/// T2, the layouts: the scan builds `s` as a dictionary whose values hold
/// `''` in the plain and `dict_empty` blocks and do not in the `dict_none`
/// block, and `b` as `Binary`. Pinned on the scan's own batches, so the
/// statements below are known to reach the expression in each shape.
async fn assert_layouts(provider: &LogsTableProvider, ctx: &SessionContext) {
    let batches = collect(provider.plan(1).expect("scan"), ctx.task_ctx())
        .await
        .expect("scan runs");
    let mut seen = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        let ts = batch
            .column(schema.index_of("ts").expect("ts"))
            .as_primitive::<TimestampNanosecondType>();
        let s = batch.column(schema.index_of("s").expect("s"));
        let dict = s.as_dictionary::<Int32Type>();
        let values = dict.values().as_string::<i32>();
        let has_empty = values.iter().any(|v| v == Some(""));
        let block = ts.value(0) / 100;
        assert_eq!(has_empty, block != 3, "block {block}: '' among the values");
        assert_eq!(
            batch.column(schema.index_of("b").expect("b")).data_type(),
            &DataType::Binary
        );
        seen.push(block);
    }
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(seen, [1, 2, 3], "every block scanned");
}

/// T1: the rewritten expression's EXPLAIN name, per statement, or `None`
/// where nothing is rewritten. `<>` on a declared `Str` column is the scan's
/// row filter (`row_filters=[empty_ne(s)]`); `=` stays in a `FilterExec`
/// (`FilterExec: empty_eq(s@..)`).
///
/// Flipped: a rule that does not look on the literal's left side leaves
/// `'' <> s` and `'' = s` as `s != ` and fails "rewritten"; one that matches
/// any literal rewrites `s <> 'x'` and fails "not rewritten".
#[tokio::test]
async fn the_rewrite_appears_only_for_the_empty_literal() {
    let (store, segments) = fixture().await;
    let ctx = session(provider(&store, segments, &declared()));
    let rewritten = [
        (
            "SELECT ts FROM logs WHERE s <> ''",
            "row_filters=[empty_ne(s)]",
        ),
        (
            "SELECT ts FROM logs WHERE '' <> s",
            "row_filters=[empty_ne(s)]",
        ),
        (
            "SELECT ts FROM logs WHERE s = ''",
            "FilterExec: empty_eq(s@",
        ),
        (
            "SELECT ts FROM logs WHERE '' = s",
            "FilterExec: empty_eq(s@",
        ),
        (
            "SELECT ts FROM logs WHERE b <> ''",
            "FilterExec: empty_ne(b@",
        ),
    ];
    for (sql, want) in rewritten {
        let (plan, _) = run(&ctx, sql).await;
        let text = shown(&plan);
        assert!(text.contains(want), "{sql}: rewritten to {want}: {text}");
    }
    let untouched = [
        "SELECT ts FROM logs WHERE s <> 'x'",
        "SELECT ts FROM logs WHERE s = 'x'",
        "SELECT ts FROM logs WHERE s = t",
        "SELECT ts FROM logs WHERE s <> t",
        "SELECT ts FROM logs WHERE n <> 0",
        "SELECT ts FROM logs WHERE n = 1",
    ];
    for sql in untouched {
        let (plan, _) = run(&ctx, sql).await;
        let text = shown(&plan);
        assert!(
            !text.contains("empty_eq") && !text.contains("empty_ne"),
            "{sql}: not rewritten: {text}"
        );
    }
}

/// T2: each statement returns exactly the rows the same statement returns
/// with the rule removed, over the three dictionary shapes of `s`, `s` cast
/// to `Utf8` (offsets) and `Utf8View` (views), and the `Binary` column `b`,
/// for `=` and `<>` in both operand orders and under `NOT`, `OR` and
/// `IS NOT TRUE` / `IS NULL`, where a NULL operand's verdict decides the row.
/// The rewrite is in every rewritten plan and in no reference plan. Two
/// answers are pinned outright.
///
/// Flipped (in `crate::empty_compare`): dropping the operand's validity makes
/// a NULL `s` read as `''` (`empty_eq`) or not (`empty_ne`) and fails "the
/// reference answer" on the `IS NOT TRUE` and `IS NULL` statements.
#[tokio::test]
async fn answers_match_the_kernel_in_every_layout() {
    let (store, segments) = fixture().await;
    let declared = declared();
    let p = provider(&store, segments.clone(), &declared);
    let on = session(provider(&store, segments, &declared));
    assert_layouts(&p, &on).await;
    let off = without_rule(&on);

    let statements = [
        "SELECT ts, s FROM logs WHERE s <> '' ORDER BY ts",
        "SELECT ts, s FROM logs WHERE '' <> s ORDER BY ts",
        "SELECT ts, s FROM logs WHERE s = '' ORDER BY ts",
        "SELECT ts, s FROM logs WHERE '' = s ORDER BY ts",
        "SELECT ts FROM logs WHERE NOT (s = '') ORDER BY ts",
        "SELECT ts FROM logs WHERE (s <> '') IS NOT TRUE ORDER BY ts",
        "SELECT ts FROM logs WHERE (s = '') IS NULL ORDER BY ts",
        "SELECT ts FROM logs WHERE s <> '' OR n = 3 ORDER BY ts",
        "SELECT ts FROM logs WHERE s = '' OR n = 3 ORDER BY ts",
        "SELECT ts FROM logs WHERE NOT (s = '' OR n = 3) ORDER BY ts",
        "SELECT ts FROM logs WHERE arrow_cast(s, 'Utf8') <> '' ORDER BY ts",
        "SELECT ts FROM logs WHERE '' = arrow_cast(s, 'Utf8') ORDER BY ts",
        "SELECT ts FROM logs WHERE arrow_cast(s, 'Utf8View') <> '' ORDER BY ts",
        "SELECT ts FROM logs WHERE (arrow_cast(s, 'Utf8View') = '') IS NOT FALSE ORDER BY ts",
        "SELECT ts FROM logs WHERE b <> '' ORDER BY ts",
        "SELECT ts FROM logs WHERE '' = b ORDER BY ts",
        "SELECT ts FROM logs WHERE (b = '') IS NOT TRUE ORDER BY ts",
    ];
    for sql in statements {
        let (reference_plan, reference) = run(&off, sql).await;
        let (plan, got) = run(&on, sql).await;
        assert_eq!(
            pretty(&got),
            pretty(&reference),
            "{sql}: the reference answer"
        );
        let text = shown(&plan);
        assert!(
            text.contains("empty_eq(") || text.contains("empty_ne("),
            "{sql}: rewritten: {text}"
        );
        let reference_text = shown(&reference_plan);
        assert!(
            !reference_text.contains("empty_eq(") && !reference_text.contains("empty_ne("),
            "{sql}: the reference is not rewritten: {reference_text}"
        );
        assert_eq!(nodes(&plan, "LogsScanExec").len(), 1, "{sql}");
    }

    let (_, got) = run(&on, statements[0]).await;
    assert_eq!(
        timestamps(&got),
        [
            100, 103, 104, 200, 202, 204, 205, 207, 300, 301, 302, 303, 304, 306, 307
        ],
        "the rows s <> '' keeps"
    );
    let (_, got) = run(&on, statements[5]).await;
    assert_eq!(
        timestamps(&got),
        [101, 102, 201, 203, 206, 305],
        "the '' and NULL rows"
    );
}

/// Class closure: the rule is appended after every rule that matches a
/// comparison's shape or rebuilds a scan with its row filters
/// (`MetadataOnlyAggregate`'s `<>`, `TopKLateMaterialization` and the `logs`
/// TopK rules), and after DataFusion's own rules, which push filters; only
/// `CompactSlices` and the aggregate profile rule, which read no predicate,
/// follow it.
///
/// Flipped: registering the rule before `MetadataOnlyAggregate` fails
/// "after metadata_only_aggregate".
#[tokio::test]
async fn the_rule_runs_after_every_rule_that_reads_a_predicate() {
    let (store, segments) = fixture().await;
    let ctx = session(provider(&store, segments, &declared()));
    let names: Vec<String> = ctx
        .state()
        .physical_optimizers()
        .iter()
        .map(|rule| rule.name().to_string())
        .collect();
    let at = |name: &str| {
        names
            .iter()
            .position(|n| n == name)
            .unwrap_or_else(|| panic!("{name} in {names:?}"))
    };
    let ours = at(EMPTY_STRING_COMPARE_RULE);
    for earlier in [
        METADATA_ONLY_AGGREGATE_RULE,
        TOPK_LATE_MATERIALIZATION_RULE,
        "confirm_logs_topk_threshold",
        "logs_limit_hint",
        "logs_partition_topk",
        "LimitPushdown",
    ] {
        assert!(at(earlier) < ours, "after {earlier}: {names:?}");
    }
    assert_eq!(
        names[ours + 1..],
        [COMPACT_SLICES_RULE, "ravel_aggregate_profile"],
        "{names:?}"
    );
}
