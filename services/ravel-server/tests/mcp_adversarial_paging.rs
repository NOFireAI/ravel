//! Pagination under concurrent change on the MCP surface (issue #1383,
//! ADR-1374 D5 and D8).
//!
//! A walk over every page of `ravel_query_sql` and `ravel_search_logs` while
//! ingest, the catalog fold and compaction run between the pages, built from
//! the same pieces the `ravel-sim` cycle driver (`crates/ravel-sim/src/driver.rs`)
//! drives: segments published the way a flush publishes them, `Catalog::fold`,
//! and `ravel_maintain::compact_bucket` at a sealed instant. The driver's own
//! `run_cycle` owns its runtime and its query engine end to end, so it cannot
//! hand control back between two pages of an MCP walk.
//!
//! The tool bodies run against the real `ServiceBackend` with a call context
//! whose clock the test sets: page one mints its cursor four hours before the
//! later pages are asked for, which is the only way to put changes "after the
//! mint" into hours the later pages' own resolve would otherwise list. That
//! is also why this cannot go through `POST /mcp`, whose clock is the wall's.
//!
//! What a later page owes the caller is ADR-1374 D5 as amended (2026-09-12,
//! item 4): it resolves at the cursor's mint instant, so a row is never
//! repeated, a row committed into an ingest hour that instant does not list
//! never appears, and the page reports page one's snapshot id and freshness
//! watermark with `visibility.pinned` set. That instant bounds which hours are
//! listed and not what they hold, so a row committed after the mint into an
//! hour it does list can still appear; see the ignored test at the end.

#![cfg(feature = "mcp")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod mcp_adversarial_common;

use mcp_adversarial_common::*;
use ravel_mcp::tools::{ToolContext, query_sql, search_logs};
use ravel_object_store::ObjectStoreBackend;
use ravel_server::mcp::ServiceBackend;
use ravel_types::Signal;
use serde_json::{Value, json};

/// The event-time window every call reads: every row the fixture writes, early
/// or late, falls inside it, so only the pin can keep a row out.
fn window() -> Value {
    json!({"start_ns": "0", "end_ns": "1000"})
}

fn hour(ts_ns: i64) -> u32 {
    u32::try_from(ts_ns / NS_PER_HOUR).expect("the hour fits a u32")
}

/// The instants of one walk: page one mints at `mint`, four hours before the
/// later pages run at `later`. Rows committed before the mint sit ten and
/// nine hours before `later`, so a fold and a compaction at `later` find
/// their buckets sealed; rows committed between the pages go to `later`'s
/// own hour, past every hour a resolve at `mint` lists.
struct Clock {
    mint: i64,
    later: i64,
}

impl Clock {
    fn now() -> Clock {
        let later = wall_now_ns();
        Clock {
            mint: later - 4 * NS_PER_HOUR,
            later,
        }
    }

    fn early(&self) -> u32 {
        hour(self.later) - 10
    }

    fn early_next(&self) -> u32 {
        hour(self.later) - 9
    }

    fn late(&self) -> u32 {
        hour(self.later)
    }
}

/// Fold `signal` for tenant A at `now_ns` and return the buckets it folded.
async fn fold(store: &std::sync::Arc<dyn ObjectStoreBackend>, signal: Signal, now_ns: i64) -> u64 {
    let catalog = ravel_catalog::Catalog::new(
        std::sync::Arc::clone(store),
        ravel_catalog::CatalogConfig::default(),
    )
    .expect("catalog config");
    catalog
        .fold(
            &hash(TENANT_A),
            signal,
            uuid::Uuid::from_u128(9_000),
            now_ns,
            &[],
            None,
        )
        .await
        .expect("fold")
        .buckets_folded
}

/// Compact tenant A's `signal` bucket `ingest_hour` at `now_ns`.
async fn compact(
    store: &dyn ObjectStoreBackend,
    signal: Signal,
    ingest_hour: u32,
    now_ns: i64,
) -> ravel_maintain::CompactionOutcome {
    let config = ravel_maintain::CompactorConfig {
        compactor_writer_id: uuid::Uuid::from_u128(9_100),
        ..ravel_maintain::CompactorConfig::default()
    };
    ravel_maintain::compact_bucket(
        store,
        &ravel_maintain::FixedClock::new(now_ns),
        &config,
        &ravel_maintain::Bucket::new(hash(TENANT_A), signal, 0, ingest_hour),
    )
    .await
    .expect("compaction runs")
}

fn assert_compacted(outcome: &ravel_maintain::CompactionOutcome) {
    assert!(
        matches!(
            outcome,
            ravel_maintain::CompactionOutcome::Compacted { parts, .. } if *parts > 0
        ),
        "{outcome:?}"
    );
}

/// A metrics segment of tenant A: one series labelled `k=<series>`, with
/// one sample per `(ts_ns, value)`.
async fn metrics(
    store: &dyn ObjectStoreBackend,
    ingest_hour: u32,
    writer_seq: u64,
    series: &str,
    samples: &[(i64, f64)],
) {
    publish_series(
        store,
        TENANT_A,
        ingest_hour,
        writer_seq,
        "m",
        &[TestSeries {
            labels: vec![("k", series)],
            samples: samples.to_vec(),
        }],
    )
    .await;
}

/// One RLOG object of tenant A holding a line per `(ts_ns, body)`.
async fn logs(
    store: &dyn ObjectStoreBackend,
    ingest_hour: u32,
    writer_seq: u64,
    lines: &[(i64, &str)],
) {
    let records: Vec<TestLog<'_>> = lines
        .iter()
        .map(|&(ts_ns, body)| TestLog {
            ts_ns,
            severity_num: 9,
            body,
            trace_id: None,
            attrs: Vec::new(),
        })
        .collect();
    publish_logs(store, TENANT_A, ingest_hour, writer_seq, &records).await;
}

/// One page of `tool` at `now_ns`, two rows a page, redeeming `cursor`.
async fn page(
    tool: &str,
    backend: &ServiceBackend,
    arguments: &Value,
    cursor: Option<&str>,
    now_ns: i64,
) -> Value {
    let key = cursor_key();
    let ctx: ToolContext<'_> = tool_context(TENANT_A, now_ns, 2, &key);
    let mut arguments = arguments.clone();
    if let Some(cursor) = cursor {
        arguments["cursor"] = json!(cursor);
    }
    let envelope = match tool {
        "ravel_query_sql" => query_sql::run(arguments, &ctx, backend).await,
        "ravel_search_logs" => search_logs::run(arguments, &ctx, backend).await,
        other => panic!("not a paging tool: {other}"),
    };
    let envelope = serde_json::to_value(envelope).expect("an envelope serializes");
    assert_ne!(envelope["status"], json!("error"), "{tool}: {envelope}");
    envelope
}

/// What every later page of a walk must say about the snapshot it read: page
/// one's id and watermark, and that it was pinned.
fn assert_pinned_to(first: &Value, later: &Value) {
    assert_eq!(
        later["visibility"]["snapshot_id"], first["visibility"]["snapshot_id"],
        "{later}"
    );
    assert_eq!(
        later["visibility"]["ingest_watermark_hour"], first["visibility"]["ingest_watermark_hour"],
        "{later}"
    );
    assert_eq!(later["visibility"]["pinned"], json!(true), "{later}");
}

fn values(page: &Value) -> Vec<f64> {
    column(page, "value")
        .iter()
        .map(|cell| cell.as_f64().expect("a float value"))
        .collect()
}

fn bodies(page: &Value) -> Vec<String> {
    column(page, "body")
        .iter()
        .map(|cell| cell.as_str().expect("a body").to_string())
        .collect()
}

/// `ravel_query_sql` walks three pages of two rows under `ORDER BY ts`.
/// Between pages one and two, rows land in the later hour both before and
/// after the cursor position, the catalog folds the sealed hours, and the two
/// L0 segments of the earliest hour compact into L1; between pages two and
/// three, more rows land and the catalog folds again. The six rows committed
/// before the mint appear exactly once, in order, and none of the eleven
/// rows committed after it appears. A fresh page one at the later instant
/// does see them, so it is the pin that keeps them out. Every later page
/// reports page one's snapshot id and watermark, pinned.
///
/// FLIP: `later_page` in `crates/ravel-mcp/src/tools/query_sql.rs` passing
/// `ctx.now_ns` to `sql_execute` instead of `pending.resolve_at_ns()`
/// resolves page two at the later instant, which lists the later hour, and
/// page two answers 2.5 (a late row sorting after the cursor) in place of 3.
/// FLIP: `keyset_predicate` in `crates/ravel-sql/src/page_plan.rs` resuming
/// at `>=` instead of `>` the last tuple repeats page one's last row, 2.0, on
/// page two.
#[tokio::test]
async fn query_sql_pages_exactly_once_through_ingest_fold_and_compaction() {
    let clock = Clock::now();
    let store = memory_store();
    metrics(
        store.as_ref(),
        clock.early(),
        1,
        "a",
        &[(10, 1.0), (30, 3.0), (50, 5.0)],
    )
    .await;
    metrics(
        store.as_ref(),
        clock.early(),
        2,
        "b",
        &[(20, 2.0), (40, 4.0)],
    )
    .await;
    metrics(store.as_ref(), clock.early_next(), 3, "c", &[(60, 6.0)]).await;
    let running = start(server_config(), store.clone()).await;
    let backend = backend(&running);
    let arguments = json!({"query": "SELECT * FROM samples ORDER BY ts", "time_range": window()});

    let one = page("ravel_query_sql", &backend, &arguments, None, clock.mint).await;
    assert_eq!(one["status"], json!("ok_page"), "{one}");
    assert_eq!(values(&one), [1.0, 2.0]);
    assert_eq!(
        one["visibility"]["ingest_watermark_hour"],
        json!(clock.early_next().to_string())
    );

    // Late rows on both sides of the cursor, then fold and compaction.
    metrics(
        store.as_ref(),
        clock.late(),
        4,
        "late",
        &[(5, 0.5), (25, 2.5), (70, 7.0)],
    )
    .await;
    metrics(
        store.as_ref(),
        clock.late(),
        5,
        "later",
        &[(15, 1.5), (35, 3.5), (65, 6.5)],
    )
    .await;
    assert_eq!(fold(&store, Signal::Metrics, clock.later).await, 2);
    assert_compacted(&compact(store.as_ref(), Signal::Metrics, clock.early(), clock.later).await);

    let cursor = cursor_of(&one).expect("page one cursor");
    let two = page(
        "ravel_query_sql",
        &backend,
        &arguments,
        Some(&cursor),
        clock.later,
    )
    .await;
    assert_eq!(values(&two), [3.0, 4.0], "{two}");
    assert_pinned_to(&one, &two);

    metrics(
        store.as_ref(),
        clock.late(),
        6,
        "latest",
        &[(45, 4.5), (55, 5.5), (80, 8.0)],
    )
    .await;
    metrics(
        store.as_ref(),
        clock.late(),
        7,
        "last",
        &[(1, 0.1), (90, 9.0)],
    )
    .await;
    fold(&store, Signal::Metrics, clock.later).await;

    let cursor = cursor_of(&two).expect("page two cursor");
    let three = page(
        "ravel_query_sql",
        &backend,
        &arguments,
        Some(&cursor),
        clock.later,
    )
    .await;
    assert_eq!(three["status"], json!("ok"), "{three}");
    assert_eq!(values(&three), [5.0, 6.0], "{three}");
    assert_eq!(cursor_of(&three), None);
    assert_pinned_to(&one, &three);

    let mut fresh = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let next = page(
            "ravel_query_sql",
            &backend,
            &arguments,
            cursor.as_deref(),
            clock.later,
        )
        .await;
        fresh.extend(values(&next));
        cursor = cursor_of(&next);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(
        fresh,
        [
            0.1, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 8.0, 9.0
        ]
    );

    running.shutdown().await.expect("server stops");
}

/// `ravel_search_logs` walks three pages of two lines with the same changes
/// between them as the SQL walk: late lines in the later hour on both sides
/// of the cursor, a fold, and the compaction of the earliest hour's two RLOG
/// objects. The six lines committed before the mint appear exactly once, in
/// `(ts, observed_ts)` order; none of the late ones does, though a fresh walk
/// sees all of them.
///
/// FLIP: `later_page` in `crates/ravel-mcp/src/tools/search_logs.rs` passing
/// `ctx.now_ns` instead of the pending redemption's resolve instant serves
/// `late-25` on page two.
/// FLIP: `keyset_predicate` in `crates/ravel-sql/src/page_plan.rs` resuming
/// at `>=` repeats `e-20` on page two.
#[tokio::test]
async fn search_logs_pages_exactly_once_through_ingest_fold_and_compaction() {
    let clock = Clock::now();
    let store = memory_store();
    logs(
        store.as_ref(),
        clock.early(),
        1,
        &[(10, "e-10"), (30, "e-30"), (50, "e-50")],
    )
    .await;
    logs(
        store.as_ref(),
        clock.early(),
        2,
        &[(20, "e-20"), (40, "e-40")],
    )
    .await;
    logs(store.as_ref(), clock.early_next(), 3, &[(60, "e-60")]).await;
    let running = start(server_config(), store.clone()).await;
    let backend = backend(&running);
    let arguments = json!({"time_range": window()});

    let one = page("ravel_search_logs", &backend, &arguments, None, clock.mint).await;
    assert_eq!(one["status"], json!("ok_page"), "{one}");
    assert_eq!(bodies(&one), ["e-10", "e-20"]);

    logs(
        store.as_ref(),
        clock.late(),
        4,
        &[(5, "late-5"), (25, "late-25"), (70, "late-70")],
    )
    .await;
    assert_eq!(fold(&store, Signal::Logs, clock.later).await, 2);
    assert_compacted(&compact(store.as_ref(), Signal::Logs, clock.early(), clock.later).await);

    let cursor = cursor_of(&one).expect("page one cursor");
    let two = page(
        "ravel_search_logs",
        &backend,
        &arguments,
        Some(&cursor),
        clock.later,
    )
    .await;
    assert_eq!(bodies(&two), ["e-30", "e-40"], "{two}");
    assert_pinned_to(&one, &two);

    logs(
        store.as_ref(),
        clock.late(),
        5,
        &[(45, "late-45"), (90, "late-90")],
    )
    .await;
    fold(&store, Signal::Logs, clock.later).await;

    let cursor = cursor_of(&two).expect("page two cursor");
    let three = page(
        "ravel_search_logs",
        &backend,
        &arguments,
        Some(&cursor),
        clock.later,
    )
    .await;
    assert_eq!(three["status"], json!("ok"), "{three}");
    assert_eq!(bodies(&three), ["e-50", "e-60"], "{three}");
    assert_pinned_to(&one, &three);

    let mut fresh = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let next = page(
            "ravel_search_logs",
            &backend,
            &arguments,
            cursor.as_deref(),
            clock.later,
        )
        .await;
        fresh.extend(bodies(&next));
        cursor = cursor_of(&next);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(
        fresh,
        [
            "late-5", "e-10", "e-20", "late-25", "e-30", "e-40", "late-45", "e-50", "e-60",
            "late-70", "late-90"
        ]
    );

    running.shutdown().await.expect("server stops");
}

/// The issue's stronger reading of "the later pages resolve at the mint
/// instant": a row committed after page one's mint, into the ingest hour the
/// mint instant itself falls in, appears on no later page. A later page
/// lists every bucket up to the mint instant plus the clock-skew allowance,
/// and a bucket's listing includes whatever was committed into it since, so
/// the late row (sorting after the cursor) is served on page two, from an
/// hour past the `visibility.ingest_watermark_hour` that page reports as
/// page one's, with nothing in the envelope saying so. ADR-1374's
/// 2026-09-12 amendment, item 4, documents the row appearing; the issue's
/// acceptance text says it must not, and that conflict is reported rather
/// than resolved here.
///
/// FLIP: none needed while ignored: today's code fails it, page two
/// answering `[3.0, 3.5]` where `[3.0, 4.0]` is asserted.
#[tokio::test]
#[ignore = "a row committed after the mint into the mint's own ingest hour appears on a later page with no disclosure"]
async fn a_row_committed_into_the_mint_hour_after_the_mint_appears_on_no_later_page() {
    let clock = Clock::now();
    let store = memory_store();
    metrics(
        store.as_ref(),
        clock.early(),
        1,
        "a",
        &[(10, 1.0), (30, 3.0), (50, 5.0)],
    )
    .await;
    metrics(
        store.as_ref(),
        clock.early(),
        2,
        "b",
        &[(20, 2.0), (40, 4.0)],
    )
    .await;
    let running = start(server_config(), store.clone()).await;
    let backend = backend(&running);
    let arguments = json!({"query": "SELECT * FROM samples ORDER BY ts", "time_range": window()});

    let one = page("ravel_query_sql", &backend, &arguments, None, clock.mint).await;
    assert_eq!(values(&one), [1.0, 2.0]);

    metrics(store.as_ref(), hour(clock.mint), 3, "late", &[(35, 3.5)]).await;

    let cursor = cursor_of(&one).expect("page one cursor");
    let two = page(
        "ravel_query_sql",
        &backend,
        &arguments,
        Some(&cursor),
        clock.later,
    )
    .await;
    assert_eq!(values(&two), [3.0, 4.0], "{two}");

    running.shutdown().await.expect("server stops");
}
