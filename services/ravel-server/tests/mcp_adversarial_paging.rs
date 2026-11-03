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
//! hour it does list can still appear on a later page if it sorts after the
//! cursor position, and is silently omitted from every page if it sorts
//! before it; three tests pin both, and the own-column walks pin that a page
//! which saw the segment set move says so in a warning.
//!
//! A row committed that way can also change a value computed from other
//! rows, so for an order on a `count(...)` the guarantee is tightened (the
//! pinned segment set amendment, issue #3774): the cursor pins a digest of
//! page one's segment set and a later page is served only over the same
//! segments, else it is `cursor_expired` with `rerun_page_one`. Three tests
//! pin that a late row and a compaction each end such a walk and that an
//! unchanged segment set walks every group exactly once.

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
    let envelope = any_page(tool, backend, arguments, cursor, now_ns).await;
    assert_ne!(envelope["status"], json!("error"), "{tool}: {envelope}");
    envelope
}

/// As [`page`], for a call that may be refused.
async fn any_page(
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
    serde_json::to_value(envelope).expect("an envelope serializes")
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
    assert!(
        two["warnings"]
            .as_array()
            .expect("warnings")
            .iter()
            .any(|warning| warning
                .as_str()
                .is_some_and(|text| text.starts_with("segment set changed since page one"))),
        "{two}"
    );

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

/// ADR-1374's 2026-09-12 amendment, item 4: a row committed after page one's
/// mint, into an ingest hour the mint instant lists, may appear on a later
/// page. A later page lists every bucket up to the mint instant plus the
/// clock-skew allowance, and a bucket's listing includes whatever was
/// committed into it since, so the late row, sorting after the cursor, is
/// served on page two. The page still reports page one's
/// `visibility.ingest_watermark_hour`, and nothing in the envelope names the
/// late row.
///
/// FLIP: a later page that pinned the bucket contents as well as the hour
/// list would answer `[3.0, 4.0]` here; this test then fails, and the
/// amendment's "a row may appear on a later page" would need revisiting.
#[tokio::test]
async fn a_row_committed_into_the_mint_hour_after_the_mint_may_appear_on_a_later_page() {
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
    assert_eq!(values(&two), [3.0, 3.5], "{two}");

    running.shutdown().await.expect("server stops");
}

/// Each row of a `SELECT ts, count(*) AS c` page as its `(ts, c)` cells.
fn counts(page: &Value) -> Vec<(String, String)> {
    rows(page)
        .iter()
        .map(|row| {
            let cell = |at: usize| row[at].as_str().expect("an integer cell").to_string();
            (cell(0), cell(1))
        })
        .collect()
}

/// The statement of the count walks: a group per `ts`, ordered by its count.
const COUNT_ORDER: &str = "SELECT ts, count(*) AS c FROM samples GROUP BY ts ORDER BY c, ts";

/// The count walks' fixture, all in `clock.early()`: ts 10 holds one row, ts
/// 20 two and ts 30 three, from three writers.
async fn count_fixture(store: &dyn ObjectStoreBackend, clock: &Clock) {
    metrics(store, clock.early(), 1, "a", &[(10, 1.0), (20, 2.0), (30, 3.0)]).await;
    metrics(store, clock.early(), 2, "b", &[(20, 2.0), (30, 3.0)]).await;
    metrics(store, clock.early(), 3, "c", &[(30, 3.0)]).await;
}

/// Two more rows at ts 10 into `clock.early()`, an hour every resolve of the
/// walk lists, so ts 10's count goes from 1 to 3 and the watermark stays.
async fn late_tens(store: &dyn ObjectStoreBackend, clock: &Clock) {
    metrics(store, clock.early(), 4, "late", &[(10, 1.5)]).await;
    metrics(store, clock.early(), 5, "later", &[(10, 1.5)]).await;
}

fn group(ts: &str, c: &str) -> (String, String) {
    (ts.to_string(), c.to_string())
}

/// A later page of a count walk refused because the segment set moved:
/// `cursor_expired`, one `rerun_page_one` next step, and no rows.
fn assert_rerun_page_one(page: &Value) {
    assert_eq!(page["status"], json!("error"), "{page}");
    assert_eq!(page["failure"]["class"], json!("cursor_expired"), "{page}");
    let actions: Vec<&Value> = page["next_steps"]
        .as_array()
        .expect("next steps")
        .iter()
        .map(|step| &step["action"])
        .collect();
    assert_eq!(actions, [&json!("rerun_page_one")], "{page}");
    assert_eq!(page["data"]["rows"], json!([]), "{page}");
}

/// Every `ts` a walk served, failing if one was served twice.
fn assert_each_group_once(pages: &[Vec<(String, String)>]) {
    let walked: Vec<&String> = pages.iter().flatten().map(|(ts, _)| ts).collect();
    let mut served = walked.clone();
    served.sort();
    served.dedup();
    assert_eq!(served.len(), walked.len(), "a group served twice: {pages:?}");
}

/// ADR-1374's pinned segment set amendment, items 2 and 3 (issue #3774):
/// `ORDER BY c, ts` over `count(*) AS c` pages, and a row committed after
/// page one into an hour page one listed ends the walk instead of moving a
/// group already served. Page one serves ts 10 (one row) and ts 20 (two rows)
/// with a cursor. Two rows at ts 10 then land in the same early hour, so the
/// watermark does not move and a fresh page one serves ts 10 after ts 20 with
/// a count of 3. Redeeming page one's cursor is `cursor_expired` with a
/// `rerun_page_one` next step and no rows, and no group was served twice.
///
/// FLIP K: `later_page` in `crates/ravel-mcp/src/tools/query_sql.rs` passing
/// `SegmentSetPin::Advisory` for an `order_from_other_rows` plan serves page
/// two as `(10, 3), (30, 3)`, and ts 10 repeats.
/// FLIP L: `segment_set_digest` in `crates/ravel-mcp/src/cursor.rs` hashing
/// each key's ingest hour instead of the key: the late rows change no hour,
/// page two is served, and ts 10 repeats.
#[tokio::test]
async fn query_sql_a_count_order_pages_and_a_late_row_ends_the_walk_instead_of_repeating_a_group()
 {
    let clock = Clock::now();
    let store = memory_store();
    count_fixture(store.as_ref(), &clock).await;
    let running = start(server_config(), store.clone()).await;
    let backend = backend(&running);
    let arguments = json!({"query": COUNT_ORDER, "time_range": window()});

    let one = page("ravel_query_sql", &backend, &arguments, None, clock.mint).await;
    assert_eq!(one["status"], json!("ok_page"), "{one}");
    assert_eq!(counts(&one), [group("10", "1"), group("20", "2")], "{one}");
    let cursor = cursor_of(&one).expect("a count order mints a cursor");

    late_tens(store.as_ref(), &clock).await;

    let listed = page("ravel_query_sql", &backend, &arguments, None, clock.mint).await;
    assert_eq!(
        counts(&listed),
        [group("20", "2"), group("10", "3")],
        "{listed}"
    );
    assert_eq!(
        listed["visibility"]["ingest_watermark_hour"], one["visibility"]["ingest_watermark_hour"],
        "{listed}"
    );

    let two = any_page(
        "ravel_query_sql",
        &backend,
        &arguments,
        Some(&cursor),
        clock.later,
    )
    .await;
    let mut walk = vec![counts(&one)];
    if two["status"] != json!("error") {
        walk.push(counts(&two));
    }
    assert_each_group_once(&walk);
    assert_rerun_page_one(&two);

    running.shutdown().await.expect("server stops");
}

/// ADR-1374's pinned segment set amendment, item 3: over an unchanged segment
/// set a count walk serves every group exactly once and in order. With the
/// two ts-10 rows committed before page one, page one serves `(20, 2), (10,
/// 3)` with a cursor and page two, four hours later, serves `(30, 3)` and
/// ends the walk, reporting page one's snapshot, pinned.
///
/// FLIP M: `keyset_predicate` in `crates/ravel-sql/src/page_plan.rs` resuming
/// at `>=` instead of `>` the last tuple serves `(10, 3)` again on page two.
#[tokio::test]
async fn query_sql_a_count_order_walks_every_group_exactly_once_over_an_unchanged_segment_set() {
    let clock = Clock::now();
    let store = memory_store();
    count_fixture(store.as_ref(), &clock).await;
    late_tens(store.as_ref(), &clock).await;
    let running = start(server_config(), store.clone()).await;
    let backend = backend(&running);
    let arguments = json!({"query": COUNT_ORDER, "time_range": window()});

    let one = page("ravel_query_sql", &backend, &arguments, None, clock.mint).await;
    assert_eq!(one["status"], json!("ok_page"), "{one}");
    assert_eq!(counts(&one), [group("20", "2"), group("10", "3")], "{one}");

    let cursor = cursor_of(&one).expect("a count order mints a cursor");
    let two = page(
        "ravel_query_sql",
        &backend,
        &arguments,
        Some(&cursor),
        clock.later,
    )
    .await;
    assert_eq!(two["status"], json!("ok"), "{two}");
    assert_eq!(counts(&two), [group("30", "3")], "{two}");
    assert_eq!(cursor_of(&two), None, "{two}");
    assert_pinned_to(&one, &two);
    assert_each_group_once(&[counts(&one), counts(&two)]);

    running.shutdown().await.expect("server stops");
}

/// ADR-1374's pinned segment set amendment, item 5: compacting a bucket the
/// window covers ends a count walk, because the compaction writes new keys
/// although it conserves every row. Page one mints a cursor; the earliest
/// hour's three L0 segments then compact, and redeeming the cursor is
/// `cursor_expired` with `rerun_page_one`, not a page.
///
/// FLIP N: the `segment_set` of `crates/ravel-sql/src/executor.rs` built from
/// the resolved segments' total `sample_count` instead of their keys, a
/// figure the compaction conserves, serves page two.
#[tokio::test]
async fn query_sql_a_compaction_between_pages_ends_a_count_order_walk() {
    let clock = Clock::now();
    let store = memory_store();
    count_fixture(store.as_ref(), &clock).await;
    let running = start(server_config(), store.clone()).await;
    let backend = backend(&running);
    let arguments = json!({"query": COUNT_ORDER, "time_range": window()});

    let one = page("ravel_query_sql", &backend, &arguments, None, clock.mint).await;
    assert_eq!(counts(&one), [group("10", "1"), group("20", "2")], "{one}");
    let cursor = cursor_of(&one).expect("a count order mints a cursor");

    assert_compacted(&compact(store.as_ref(), Signal::Metrics, clock.early(), clock.later).await);

    let listed = page("ravel_query_sql", &backend, &arguments, None, clock.mint).await;
    assert_eq!(
        counts(&listed),
        [group("10", "1"), group("20", "2")],
        "the compaction conserves every row: {listed}"
    );

    let two = any_page(
        "ravel_query_sql",
        &backend,
        &arguments,
        Some(&cursor),
        clock.later,
    )
    .await;
    assert_rerun_page_one(&two);

    running.shutdown().await.expect("server stops");
}

/// Walk `tool` from `cursor` at `now_ns` to its last page, or to the fourth
/// page if a walk never ends, and return each page's rows as `rows` reads
/// them.
async fn rest_of_walk<T>(
    tool: &str,
    backend: &ServiceBackend,
    arguments: &Value,
    cursor: String,
    now_ns: i64,
    rows: fn(&Value) -> Vec<T>,
) -> Vec<Vec<T>> {
    let mut pages = Vec::new();
    let mut cursor = Some(cursor);
    while let Some(current) = cursor.filter(|_| pages.len() < 4) {
        let next = page(tool, backend, arguments, Some(&current), now_ns).await;
        pages.push(rows(&next));
        cursor = cursor_of(&next);
    }
    pages
}

/// ADR-1374's 2026-09-12 amendment, item 4, through `ravel_query_sql`: a
/// late-arriving row that sorts before the cursor position is silently
/// omitted from every page. After page one mints its cursor after 2.0, a row
/// at ts 15 (1.5) is committed into the mint instant's own ingest hour, which
/// the later pages list, as a fresh page one at the mint instant shows by
/// serving it. The remaining pages still serve 3.0, 4.0 and 5.0 exactly once
/// and in order, and 1.5 appears on none of them.
///
/// FLIP: `plan_page` in `crates/ravel-sql/src/page_plan.rs` building the
/// resumed filter as `String::new()` in place of the `keyset_predicate` WHERE
/// (a later page that resumes from the start) serves 1.0, 1.5 on page two.
#[tokio::test]
async fn query_sql_a_late_arriving_row_that_sorts_before_the_cursor_position_is_silently_omitted_from_every_page()
 {
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

    metrics(store.as_ref(), hour(clock.mint), 3, "late", &[(15, 1.5)]).await;

    let listed = page("ravel_query_sql", &backend, &arguments, None, clock.mint).await;
    assert_eq!(values(&listed), [1.0, 1.5], "{listed}");

    let cursor = cursor_of(&one).expect("page one cursor");
    let rest = rest_of_walk(
        "ravel_query_sql",
        &backend,
        &arguments,
        cursor,
        clock.later,
        values,
    )
    .await;
    assert_eq!(rest, [vec![3.0, 4.0], vec![5.0]]);
    assert!(
        rest.iter()
            .flatten()
            .all(|value| value.to_bits() != 1.5f64.to_bits()),
        "{rest:?}"
    );

    running.shutdown().await.expect("server stops");
}

/// ADR-1374's 2026-09-12 amendment, item 4, through `ravel_search_logs`: a
/// late-arriving row that sorts before the cursor position is silently
/// omitted from every page. After page one mints its cursor after `e-20`,
/// `late-15` is committed into the mint instant's own ingest hour, which a
/// fresh page one at the mint instant serves. The remaining pages serve
/// `e-30`, `e-40` and `e-50` exactly once and in order, and `late-15` on none
/// of them.
///
/// FLIP: `plan_page` in `crates/ravel-sql/src/page_plan.rs` building the
/// resumed filter as `String::new()` in place of the `keyset_predicate` WHERE
/// serves `e-10`, `late-15` on page two.
#[tokio::test]
async fn search_logs_a_late_arriving_row_that_sorts_before_the_cursor_position_is_silently_omitted_from_every_page()
 {
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
    let running = start(server_config(), store.clone()).await;
    let backend = backend(&running);
    let arguments = json!({"time_range": window()});

    let one = page("ravel_search_logs", &backend, &arguments, None, clock.mint).await;
    assert_eq!(bodies(&one), ["e-10", "e-20"]);

    logs(store.as_ref(), hour(clock.mint), 3, &[(15, "late-15")]).await;

    let listed = page("ravel_search_logs", &backend, &arguments, None, clock.mint).await;
    assert_eq!(bodies(&listed), ["e-10", "late-15"], "{listed}");

    let cursor = cursor_of(&one).expect("page one cursor");
    let rest = rest_of_walk(
        "ravel_search_logs",
        &backend,
        &arguments,
        cursor,
        clock.later,
        bodies,
    )
    .await;
    assert_eq!(
        rest,
        [
            vec!["e-30".to_string(), "e-40".to_string()],
            vec!["e-50".to_string()]
        ]
    );
    assert!(
        rest.iter().flatten().all(|body| body != "late-15"),
        "{rest:?}"
    );

    running.shutdown().await.expect("server stops");
}
