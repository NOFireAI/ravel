//! ADR-4628 decision 5 (issue #4685, task T5b): no `.cstat` load when every
//! touched segment's stamp answers every figure the statement reads, and the
//! per-carrier segment counts on `MetadataOnlyExec` and `stats.pruning`.
//!
//! Every object is a real RLOG written by `RlogWriter` with stamps built from
//! its own rows (`logs_stamp_readers`' executor harness), and every GET count
//! is taken through `SqlExecutor`. On an unfolded tenant a kept load is
//! exactly one plan-phase GET, the absent catalog HEAD; a skipped load is
//! none. A forced scan is the same objects with fields 6 to 8 stripped and no
//! `.cstat`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{
    Catalog, CatalogConfig, DeclaredColumnType, DeclaredTypedColumn, TenantConfig,
    TenantLifecycleState,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_sql::stats_json::pruning_json;
use ravel_sql::{DeclaredColumn, DeclaredType, SqlRequest};
use ravel_types::declared_stats::DeclaredColumnStat;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

use crate::logs_stamp_readers::{
    BASE_HOUR, ExecRun, Fields, KeyCountingStore, NS_PER_HOUR, NS_PER_SEC, Row, assert_same_answer,
    erasure_exec, exec_request, executor, pre_rewrite_world, pretty, publish_with_stamps, request,
    row, stamp_of,
};

fn gate_tenant() -> TenantId {
    TenantId::new("stamp-load-gate-4685".to_string())
}

fn ab_cols() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("a", DeclaredType::I64),
        DeclaredColumn::new("w", DeclaredType::I64),
    ]
}

/// Three objects in `BASE_HOUR` (event ts `h + 1` to `h + 23`). Each holds
/// `a = 0` beside other values, so `a <> 0` is decided per object by its zero
/// count only; `a` never reaches 100.
fn parts() -> Vec<Vec<Row>> {
    let h = i64::from(BASE_HOUR) * NS_PER_HOUR;
    vec![
        vec![
            row(h + 1, Some(0), Some(10)),
            row(h + 2, Some(4), Some(20)),
            row(h + 3, Some(0), Some(30)),
        ],
        vec![
            row(h + 11, Some(3), Some(5)),
            row(h + 12, Some(0), Some(7)),
            row(h + 13, None, Some(9)),
        ],
        vec![
            row(h + 21, Some(0), Some(11)),
            row(h + 22, Some(6), Some(13)),
            row(h + 23, Some(2), None),
        ],
    ]
}

/// Which of fields 6 to 8 one object's stamp carries.
#[derive(Clone, Copy, Debug)]
enum Stamp {
    /// Both columns carry the sum and zero count.
    Full,
    /// `a` carries them, `w` does not.
    AOnly,
    /// Neither column does (an older fold's or writer's output).
    Stripped,
    /// `a` claims a zero count of 0 beside its minimum of 0 over non-null
    /// rows (the zero-count guard of `stamp_equal_count`), with its true sum;
    /// `w` is full.
    ZeroCountLie,
}

fn stamps_for(rows: &[Row], stamp: Stamp) -> Vec<DeclaredColumnStat> {
    let full = |col| stamp_of(col, DeclaredType::I64, rows, Fields::With).expect("stamp");
    let bare = |col| stamp_of(col, DeclaredType::I64, rows, Fields::Without).expect("stamp");
    match stamp {
        Stamp::Full => vec![full("a"), full("w")],
        Stamp::AOnly => vec![full("a"), bare("w")],
        Stamp::Stripped => vec![bare("a"), bare("w")],
        Stamp::ZeroCountLie => {
            let lie = bare("a")
                .with_sum(full("a").sum().expect("sum"))
                .expect("sum")
                .with_zero_count(0)
                .expect("decode admits a zero count of 0 beside a min of 0");
            vec![lie, full("w")]
        }
    }
}

/// Past the seal horizon of `BASE_HOUR`, so a fold at this instant seals it.
fn sealed_now() -> i64 {
    let cfg = CatalogConfig::default();
    (i64::from(BASE_HOUR) + 1) * NS_PER_HOUR
        + cfg.max_flush_lifetime_ns
        + cfg.clock_skew_allowance_ns
        + cfg.fold_safety_margin_ns
        + NS_PER_SEC
}

fn sealed_request(sql: &str) -> SqlRequest {
    let now = sealed_now();
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: i64::from(BASE_HOUR) * NS_PER_HOUR,
            end_ns: now,
        },
        min_tokens: Vec::new(),
        now_ns: now,
        deadline: Duration::from_secs(120),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// The three objects published with `stamps`, unfolded.
async fn unfolded(stamps: [Stamp; 3]) -> Arc<KeyCountingStore> {
    let store = Arc::new(KeyCountingStore::default());
    for (i, (rows, stamp)) in parts().iter().zip(stamps).enumerate() {
        publish_with_stamps(
            store.as_ref(),
            &gate_tenant(),
            i as u64 + 1,
            BASE_HOUR,
            rows,
            &stamps_for(rows, stamp),
        )
        .await;
    }
    store.take_keys();
    store
}

/// [`unfolded`], then a real fold with `a` and `w` declared: it builds a
/// reconciled `.cstat` entry for every object from the object's rows and
/// carries each record's stamp into the snapshot as written, so a stripped
/// record stands for an older fold's output.
async fn folded(stamps: [Stamp; 3]) -> Arc<KeyCountingStore> {
    let store = unfolded(stamps).await;
    let tenant = gate_tenant();
    let config = TenantConfig {
        typed_attr_columns: Some(
            ["a", "w"]
                .into_iter()
                .map(|key| DeclaredTypedColumn {
                    key: key.to_string(),
                    ty: DeclaredColumnType::I64,
                })
                .collect(),
        ),
        ..TenantConfig::new(TenantLifecycleState::Active)
    };
    ravel_catalog::set_tenant_config(store.as_ref(), &tenant.hash(), &config, 0)
        .await
        .expect("tenant config");
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let folder = Catalog::new(backend, CatalogConfig::default()).expect("catalog");
    let report = folder
        .fold(
            &tenant.hash(),
            Signal::Logs,
            Uuid::from_u128(4_685),
            sealed_now(),
            &[],
            None,
        )
        .await
        .expect("fold");
    assert_eq!(
        report.watermark_hour,
        Some(BASE_HOUR),
        "the fold seals the hour"
    );
    assert_eq!(report.entry_count, 3, "three folded entries");
    store.take_keys();
    store
}

/// `sql` over `store` at the sealed instant.
async fn run_sealed(store: &Arc<KeyCountingStore>, sql: &str) -> ExecRun {
    let ex = executor(store, ab_cols());
    exec_request(store, &ex, &gate_tenant(), sealed_request(sql)).await
}

/// `sql` over `store` inside the unsealed hour.
async fn run_unsealed(store: &Arc<KeyCountingStore>, sql: &str) -> ExecRun {
    let ex = executor(store, ab_cols());
    exec_request(store, &ex, &gate_tenant(), request(sql, Vec::new())).await
}

const Q02: &str = "SELECT COUNT(*) FROM logs WHERE a <> 0";
const SUMS: &str = "SELECT SUM(a), COUNT(*), AVG(w) FROM logs";

/// The GET keys that are neither a data object nor a commit record.
fn catalog_keys(run: &ExecRun) -> Vec<String> {
    run.get_keys
        .iter()
        .filter(|k| {
            let parts: Vec<&str> = k.split('/').collect();
            !k.ends_with(".cmt") && !(parts.len() > 3 && matches!(parts[3], "l0" | "l1" | "rw"))
        })
        .map(|k| {
            // The tenant prefix and the content-addressed name are not the
            // claim; the object kind is.
            let kind = k.rsplit('/').next().unwrap_or_default();
            match kind.rsplit_once('.') {
                Some((_, ext)) => format!(".{ext}"),
                None => kind.to_string(),
            }
        })
        .collect()
}

/// The one `MetadataOnlyExec` line of `run`'s `EXPLAIN`, trimmed.
fn marker_lines(run: &ExecRun) -> Vec<String> {
    run.plan
        .lines()
        .filter(|line| line.contains("MetadataOnlyExec"))
        .map(|line| line.trim().trim_matches('|').trim().to_string())
        .collect()
}

/// `stats.pruning`'s two carrier counts of `run`.
fn pruning_split(run: &ExecRun) -> (u64, u64) {
    let rendered = pruning_json(run.stats.as_ref().expect("the statement ran"));
    (
        rendered["segmentsAnsweredFromStamp"]
            .as_u64()
            .expect("segmentsAnsweredFromStamp"),
        rendered["segmentsAnsweredFromCstat"]
            .as_u64()
            .expect("segmentsAnsweredFromCstat"),
    )
}

fn assert_answered_with(run: &ExecRun, what: &str, stamp: u64, cstat: u64) {
    assert!(
        !run.plan.contains("LogsScanExec"),
        "{what}: answered from statistics; plan was:\n{}",
        run.plan
    );
    assert_eq!(run.data_gets, 0, "{what}: no data-object GET");
    assert_eq!(
        marker_lines(run),
        vec![format!(
            "MetadataOnlyExec: metadata_only=true, rows=1, stamp_segments={stamp}, \
             cstat_segments={cstat}"
        )],
        "{what}: the EXPLAIN line; plan was:\n{}",
        run.plan
    );
    assert_eq!(pruning_split(run), (stamp, cstat), "{what}: stats.pruning");
}

fn assert_scans(run: &ExecRun, what: &str) {
    assert!(
        run.plan.contains("LogsScanExec") && !run.plan.contains("MetadataOnlyExec"),
        "{what}: scans; plan was:\n{}",
        run.plan
    );
    assert!(run.data_gets > 0, "{what}: a scan reads data objects");
    assert_eq!(pruning_split(run), (0, 0), "{what}: a scan reads 0 and 0");
}

// ---------------------------------------------------------------------------
// Test 5 (test plan items 5 and 10): a folded tenant, some segments stamped
// with fields 6 to 8 and some without, each beside a reconciled `.cstat` entry
// ---------------------------------------------------------------------------

/// One arm of test 5: both statements answered from statistics, equal to the
/// forced scan, with the exact carrier split and plan-phase GETs.
async fn assert_folded_arm(
    stamps: [Stamp; 3],
    expected: [(&str, &str, u64, u64); 2],
    plan_gets: (u64, u64),
    plan_keys: &[&str],
) -> Vec<(String, (u64, u64), (u64, u64))> {
    let store = folded(stamps).await;
    let forced = unfolded([Stamp::Stripped; 3]).await;
    let mut costs = Vec::new();
    for (sql, answer, stamp, cstat) in expected {
        let what = format!("{stamps:?} {sql}");
        let run = run_sealed(&store, sql).await;
        assert_answered_with(&run, &what, stamp, cstat);
        assert_eq!(
            run.phases.plan, plan_gets,
            "{what}: plan-phase GETs and wire bytes"
        );
        // The resolve reads HEAD and the `.csnap` part; the plan reads what
        // follows them.
        let keys = catalog_keys(&run);
        assert_eq!(
            keys[..2],
            ["HEAD".to_string(), ".csnap".to_string()],
            "{what}: the resolve's reads"
        );
        assert_eq!(keys[2..], *plan_keys, "{what}: the plan-phase keys");
        assert!(
            pretty(&run.answer).contains(answer),
            "{what}: {}",
            pretty(&run.answer)
        );
        let scan = run_sealed(&forced, sql).await;
        assert_scans(&scan, &what);
        assert_same_answer(&run.answer, &scan.answer, &what);
        costs.push((what, run.phases.plan, run.phases.resolve));
    }
    costs
}

/// Mixed carriers. Segment 1 has both carriers and answers from its stamp:
/// adding its `.cstat` figures too would make `a <> 0` 5 and `SUM(a)` 19, and
/// counting it under both would make the split add up to 4. Segment 2 answers
/// `a` from its stamp and `w` from `.cstat`, so the sum statement counts it
/// once, as `cstat`; the not-equal count reads only `a` and counts it as a
/// stamp segment. Segment 3 has no fields and answers from `.cstat`.
///
/// `a <> 0`: 1 + 1 + 2 = 4. `SUM(a)`: 4 + 3 + 8 = 15, `COUNT(*)` 9,
/// `AVG(w)`: 105 / 8 = 13.125.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_carriers_answer_with_the_exact_split() {
    assert_folded_arm(
        [Stamp::Full, Stamp::AOnly, Stamp::Stripped],
        [(Q02, "| 4 ", 2, 1), (SUMS, "| 15 ", 1, 2)],
        (2, 718),
        &["HEAD", ".cstat"],
    )
    .await;
}

/// Every segment stamped: the load is skipped, so the plan reads nothing and
/// every segment counts as a stamp segment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_stamped_skips_the_load() {
    let costs = assert_folded_arm(
        [Stamp::Full; 3],
        [(Q02, "| 4 ", 3, 0), (SUMS, "| 15 ", 3, 0)],
        (0, 0),
        &[],
    )
    .await;
    for (what, plan, resolve) in costs {
        println!("COST all stamped {what}: plan {plan:?} resolve {resolve:?}");
    }
}

/// One segment stripped: the load runs, at the two GETs and 718 wire bytes
/// this arm paid before the skip existed (the HEAD and the `.cstat` object),
/// and only that segment counts as `.cstat`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_stripped_segment_keeps_the_load() {
    let costs = assert_folded_arm(
        [Stamp::Full, Stamp::Full, Stamp::Stripped],
        [(Q02, "| 4 ", 2, 1), (SUMS, "| 15 ", 2, 1)],
        (2, 718),
        &["HEAD", ".cstat"],
    )
    .await;
    for (what, plan, resolve) in costs {
        println!("COST one stripped {what}: plan {plan:?} resolve {resolve:?}");
    }
}

/// A q08 count reads only `.cstat`, so on a folded tenant it keeps the load
/// even with every segment stamped, and counts every touched segment as a
/// `.cstat` segment. Six groups: 0, 2, 3, 4, 6 and NULL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_q08_count_counts_every_segment_as_cstat() {
    let store = folded([Stamp::Full; 3]).await;
    let sql = "SELECT a, COUNT(*) FROM logs GROUP BY a";
    let run = run_sealed(&store, sql).await;
    assert_eq!(run.phases.plan, (2, 718), "q08 keeps the load");
    assert_eq!(run.data_gets, 0, "answered from .cstat");
    assert_eq!(
        marker_lines(&run),
        vec![
            "MetadataOnlyExec: metadata_only=true, rows=6, stamp_segments=0, cstat_segments=3"
                .to_string()
        ],
        "plan was:\n{}",
        run.plan
    );
    assert_eq!(pruning_split(&run), (0, 3));
    let forced = unfolded([Stamp::Stripped; 3]).await;
    let scan = run_sealed(&forced, &format!("{sql} ORDER BY a")).await;
    let ordered = run_sealed(&store, &format!("{sql} ORDER BY a")).await;
    assert_same_answer(&ordered.answer, &scan.answer, sql);
}

/// A statement the rewrite does not answer reads 0 and 0: DataFusion's own
/// `AggregateStatistics` answers a bare `COUNT(*)` and `MIN`, and the q08
/// count over a tenant with no `.cstat` scans.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_statement_the_rewrite_does_not_answer_reads_zero_and_zero() {
    let store = unfolded([Stamp::Full; 3]).await;
    for sql in ["SELECT COUNT(*) FROM logs", "SELECT MIN(a) FROM logs"] {
        let run = run_unsealed(&store, sql).await;
        assert!(
            !run.plan.contains("MetadataOnlyExec") && !run.plan.contains("LogsScanExec"),
            "{sql}: DataFusion's rule answers; plan was:\n{}",
            run.plan
        );
        assert_eq!(pruning_split(&run), (0, 0), "{sql}");
    }
    let run = run_unsealed(&store, "SELECT a, COUNT(*) FROM logs GROUP BY a").await;
    assert_scans(&run, "q08 with no .cstat");
}

// ---------------------------------------------------------------------------
// The skip on an unfolded tenant
// ---------------------------------------------------------------------------

/// The control and (s1): every segment's zero count answers `a <> 0` and the
/// load is skipped; with segment 3's fields stripped (0 inside its `[0, 6]`)
/// the load is kept, and with no `.cstat` the statement scans, equal to the
/// forced scan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s1_one_segment_lacking_the_figure_keeps_the_load() {
    let forced = unfolded([Stamp::Stripped; 3]).await;
    let scan = run_unsealed(&forced, Q02).await;
    assert_scans(&scan, "forced");
    assert_eq!(scan.phases.plan.0, 1, "forced: the load is kept");

    let all = unfolded([Stamp::Full; 3]).await;
    let run = run_unsealed(&all, Q02).await;
    assert_eq!(run.phases.plan.0, 0, "all stamped: no load");
    assert_answered_with(&run, "all stamped", 3, 0);
    assert_same_answer(&run.answer, &scan.answer, "all stamped");

    let one = unfolded([Stamp::Full, Stamp::Full, Stamp::Stripped]).await;
    let run = run_unsealed(&one, Q02).await;
    assert_eq!(
        run.phases.plan.0, 1,
        "s1: one stripped segment keeps the load"
    );
    assert_scans(&run, "s1");
    assert_same_answer(&run.answer, &scan.answer, "s1");
}

/// (s2), through SQL text: the stripped segment lies wholly outside the ts
/// bound, so the provider never touches it and the rewrite answers from the
/// stamps of the other two (`a <> 0` is 1 + 1). The load gate judges the
/// analyzed plan, where `TIMESTAMP '...'` is still a cast and reads no bound,
/// so it walks every segment, meets the stripped one and keeps the load: one
/// plan-phase GET, the safe side. The skip over the touched subset alone is
/// pinned on a typed bound in `executor.rs`
/// (`the_stamp_skip_judges_only_the_segments_the_ts_bound_touches`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s2_a_timestamp_text_bound_keeps_the_load() {
    let sql = "SELECT COUNT(*) FROM logs WHERE a <> 0 \
               AND ts <= TIMESTAMP '2027-01-15 08:00:00.000000015'";
    let one = unfolded([Stamp::Full, Stamp::Full, Stamp::Stripped]).await;
    let run = run_unsealed(&one, sql).await;
    assert_eq!(
        run.phases.plan.0, 1,
        "s2: the load is kept; plan was:\n{}",
        run.plan
    );
    assert_answered_with(&run, "s2", 2, 0);
    assert!(
        pretty(&run.answer).contains("| 2 "),
        "{}",
        pretty(&run.answer)
    );
    let forced = unfolded([Stamp::Stripped; 3]).await;
    let scan = run_unsealed(&forced, sql).await;
    assert_scans(&scan, "s2 forced");
    assert_same_answer(&run.answer, &scan.answer, "s2");
}

/// (s3): `a <> 100 + 100` is not a plain constant on the analyzed plan, so the
/// load is kept although every stamp decides 200 (outside every `a` range);
/// the optimizer folds the literal and the rewrite then answers from the
/// stamps.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_a_computed_literal_keeps_the_load() {
    let all = unfolded([Stamp::Full; 3]).await;
    let sql = "SELECT COUNT(*) FROM logs WHERE a <> 100 + 100";
    let run = run_unsealed(&all, sql).await;
    assert_eq!(run.phases.plan.0, 1, "s3: the load is kept");
    assert_answered_with(&run, "s3", 3, 0);
    let plain = run_unsealed(&all, "SELECT COUNT(*) FROM logs WHERE a <> 200").await;
    assert_eq!(plain.phases.plan.0, 0, "the plain constant skips it");
    assert_same_answer(&run.answer, &plain.answer, "s3");
}

/// (s4): a q08 count reads only `.cstat`, so it keeps the load, and so does a
/// q02 statement beside a q08 count in a scalar subquery. Both scan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s4_a_q08_shape_keeps_the_load() {
    let all = unfolded([Stamp::Full; 3]).await;
    let forced = unfolded([Stamp::Stripped; 3]).await;
    for sql in [
        "SELECT a, COUNT(*) FROM logs GROUP BY a ORDER BY a",
        "SELECT (SELECT COUNT(*) FROM logs WHERE a <> 0) + \
         (SELECT MAX(c) FROM (SELECT a, COUNT(*) AS c FROM logs GROUP BY a))",
    ] {
        let run = run_unsealed(&all, sql).await;
        assert_eq!(run.phases.plan.0, 1, "{sql}: the load is kept");
        assert!(run.plan.contains("LogsScanExec"), "{sql}:\n{}", run.plan);
        let scan = run_unsealed(&forced, sql).await;
        assert_same_answer(&run.answer, &scan.answer, sql);
    }
}

/// (s5): a pending erasure refuses the load first, so nothing is loaded even
/// where the stamps, stripped here, would have kept it; the statement scans.
/// The same world with no request keeps the load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s5_a_pending_erasure_loads_nothing() {
    let sql = "SELECT COUNT(*) FROM logs WHERE a <> 0";
    let pending = pre_rewrite_world(Fields::Without, true).await;
    let run = erasure_exec(&pending, &request(sql, Vec::new())).await;
    assert_eq!(run.phases.plan.0, 0, "s5: no load under a pending erasure");
    assert!(run.plan.contains("LogsScanExec"), "{}", run.plan);
    assert!(run.data_gets > 0, "s5 scans");
    let none = pre_rewrite_world(Fields::Without, false).await;
    let run = erasure_exec(&none, &request(sql, Vec::new())).await;
    assert_eq!(run.phases.plan.0, 1, "no request: the load is kept");
}

/// (s6): segment 1's stamp carries a zero count of 0 beside a minimum of 0
/// over non-null rows, which `stamp_equal_count` refuses, so the stamp gives
/// no figure for `a <> 0` although field 8 is present: the load is kept and
/// the statement scans, equal to the forced scan (which counts segment 1's
/// one non-zero row, not three).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s6_a_refused_zero_count_keeps_the_load() {
    let lie = unfolded([Stamp::ZeroCountLie, Stamp::Full, Stamp::Full]).await;
    let run = run_unsealed(&lie, Q02).await;
    assert_eq!(run.phases.plan.0, 1, "s6: the load is kept");
    assert_scans(&run, "s6");
    let forced = unfolded([Stamp::Stripped; 3]).await;
    let scan = run_unsealed(&forced, Q02).await;
    assert_same_answer(&run.answer, &scan.answer, "s6");
    assert!(
        pretty(&run.answer).contains("| 4 "),
        "{}",
        pretty(&run.answer)
    );
}

/// (s7): stamps without fields 6 to 8 still answer `a <> 100`, a literal
/// outside every segment's `[min, max]`: the load is skipped and the answer
/// is every non-null row, 8.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s7_extrema_alone_skip_the_load() {
    let stripped = unfolded([Stamp::Stripped; 3]).await;
    let sql = "SELECT COUNT(*) FROM logs WHERE a <> 100";
    let run = run_unsealed(&stripped, sql).await;
    assert_eq!(run.phases.plan.0, 0, "s7: no load");
    assert_answered_with(&run, "s7", 3, 0);
    assert!(
        pretty(&run.answer).contains("| 8 "),
        "{}",
        pretty(&run.answer)
    );
}

/// (s8): two scans of `logs` in one statement keep the load although each
/// one's figures are answered by every stamp; both subqueries then answer
/// from the stamps, and the counters sum over both nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s8_two_scans_keep_the_load() {
    let all = unfolded([Stamp::Full; 3]).await;
    let sql = "SELECT (SELECT COUNT(*) FROM logs WHERE a <> 0), (SELECT SUM(w) FROM logs)";
    let run = run_unsealed(&all, sql).await;
    assert_eq!(run.phases.plan.0, 1, "s8: the load is kept");
    assert_eq!(run.data_gets, 0, "s8: both answered from statistics");
    assert_eq!(
        marker_lines(&run),
        vec![
            "MetadataOnlyExec: metadata_only=true, rows=1, stamp_segments=3, cstat_segments=0"
                .to_string();
            2
        ],
        "plan was:\n{}",
        run.plan
    );
    assert_eq!(pruning_split(&run), (6, 0), "summed over both nodes");
    for sql in [Q02, "SELECT SUM(w) FROM logs"] {
        assert_eq!(
            run_unsealed(&all, sql).await.phases.plan.0,
            0,
            "{sql}: alone, skipped"
        );
    }
}
