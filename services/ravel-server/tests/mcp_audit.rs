//! Audit rows for MCP tool calls on the shipping start path (issue #1384,
//! ADR-1374 decisions 3 and 7), with the audit pipeline in `required` mode,
//! so a call's record is durable before the call answers.
//!
//! A call cannot be keyed by its query id: the server reports none, either in
//! the envelope (`ids.query_id` is empty on every tool) or on the audit
//! record (`ravel_maintain::query_audit` writes no id attribute). Each call is
//! keyed instead by the records it added to the audit shard, compared as a
//! multiset against the shard before the call, and by those records'
//! `query.language` and `query.text`. The languages are the engine's own
//! (`sql`, `promql`, ...), not ADR-1374 decision 3 item 6's `mcp:<tool>`, so
//! an MCP call's record cannot be told from the same query over HTTP.
//!
//! Already pinned elsewhere and not repeated here: `ravel_get_trace`'s record
//! carrying the caller's configured subject
//! (`trace_by_id.rs::get_trace_over_mcp_audits_the_callers_configured_subject`),
//! and a dropped call's usage record with its exact spend
//! (`mcp_e2e.rs::dropped_stream_releases_admission_permit`).

#![cfg(feature = "mcp")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "support/mcp.rs"]
mod support;

use std::sync::Arc;
use std::time::Duration;

use ravel_logseg::LogRecord;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_server::metrics::{QueryCostCounters, QueryOutcomeStatus};
use serde_json::json;
use support::*;

/// The records in `after` that `before` does not hold, as a multiset: the
/// audit shard lists in key order, so a new record is not necessarily last.
fn new_records(before: &[LogRecord], after: Vec<LogRecord>) -> Vec<LogRecord> {
    let mut seen: Vec<String> = before.iter().map(|row| format!("{row:?}")).collect();
    after
        .into_iter()
        .filter(|row| {
            let key = format!("{row:?}");
            match seen.iter().position(|old| *old == key) {
                Some(at) => {
                    seen.swap_remove(at);
                    false
                }
                None => true,
            }
        })
        .collect()
}

/// The `query.language` and `query.text` each tool's fixture call is audited
/// under, or `None` for a tool that writes no audit record.
fn expected_record(tool: &str) -> Option<(&'static str, String)> {
    let text = match tool {
        // Answers from compile-time facts and the resolved caller, reads no
        // object storage, and so submits no audit event.
        "ravel_capabilities" => return None,
        "ravel_describe_data" => (
            "describe_data",
            "describe_data signal=metrics after_name=None limit=100".to_string(),
        ),
        // An empty text is the defect #3734 tracks, pinned until it lands.
        "ravel_find_labels" => ("label_values", String::new()),
        "ravel_explain_query" | "ravel_query_sql" => {
            ("sql", "SELECT value FROM samples".to_string())
        }
        "ravel_query_promql" => ("promql", "m".to_string()),
        "ravel_analyze_timeseries" => ("analytics", r#"m{k="quad"}"#.to_string()),
        "ravel_search_logs" => (
            "sql",
            "SELECT * FROM (SELECT * FROM logs WHERE has_word(\"body\", 'timeout')) \
             AS ravel_page ORDER BY \"ts\" ASC, \"observed_ts\" ASC"
                .to_string(),
        ),
        "ravel_get_trace" => ("sql", ravel_server::traces::trace_statement(&[0x7a; 16])),
        other => panic!("no expected audit record for {other}"),
    };
    Some(text)
}

/// Each of the nine read tools, called once over the fixture, adds exactly
/// one `kind=query` record to the tenant's audit shard (none for
/// `ravel_capabilities`, which reads nothing), with `query.status=ok`, the
/// caller's tenant and the call's own language and statement.
///
/// FLIP: replacing `self.audit_sink.submit(event).await...?` in
/// `QueryControls::audit` (`crates/ravel-query/src/http/service.rs`) with a
/// dropped `event` leaves every audited call with zero records; submitting
/// it twice leaves two.
#[tokio::test]
async fn every_tool_call_writes_exactly_one_audit_record() {
    let (store, metrics) = instrumented_memory();
    seed_fixture(store.as_ref()).await;
    let running = start_with(server_config(), Arc::clone(&store), metrics).await;
    let client = connect(&running).await;

    let mut audited = 0;
    for (name, arguments) in nine_tool_calls() {
        let before = query_audit_records(store.as_ref()).await;
        let (is_error, envelope) = call(&client, name, arguments).await;
        assert_eq!(is_error, Some(false), "{name}: {envelope}");
        assert_eq!(envelope["ids"]["query_id"], json!(""), "{name}");
        let new = new_records(&before, query_audit_records(store.as_ref()).await);

        let Some((language, text)) = expected_record(name) else {
            assert!(new.is_empty(), "{name} wrote {new:?}");
            continue;
        };
        audited += 1;
        assert_eq!(new.len(), 1, "{name} wrote {new:?}");
        let record = &new[0];
        assert_eq!(
            audit_attr(record, "query.language"),
            Some(language),
            "{name}"
        );
        assert_eq!(
            audit_attr(record, "query.text"),
            Some(text.as_str()),
            "{name}"
        );
        assert_eq!(audit_attr(record, "query.status"), Some("ok"), "{name}");
        assert_eq!(
            audit_attr(record, "query.tenant"),
            Some(expected_tenant().to_hex().as_str()),
            "{name}"
        );
    }
    assert_eq!(audited, 8);

    client.cancel().await.expect("client stops");
    running.shutdown().await.expect("server stops");
}

/// A call cancelled by its own `deadline_ms`, while its first catalog LIST is
/// held in the store, writes one audit record with `query.status=error`, and
/// the spend it reached is recorded in the same call's usage record, with
/// the `Timeout` outcome.
///
/// The deadline is the cancellation mode chosen because the server observes
/// it: the service layer ends the engine call and still runs the audit step.
/// A client drop is not observable there; the service layer documents that a
/// dropped future never reaches the audit step and leaves only its usage
/// record, which `mcp_e2e.rs::dropped_stream_releases_admission_permit`
/// pins. The audit record carries no spend attribute at all
/// (`ravel_maintain::query_audit` has none), so the spend is asserted on the
/// usage record, the one place the server records it.
///
/// FLIP: `UsageGuard::finish_failed` (`crates/ravel-query/src/http/service.rs`)
/// recording `QueryAccountingSnapshot::default()` instead of
/// `self.live.snapshot()` leaves the Timeout row at `s3_requests: 0` and
/// `cache_misses: 0`.
#[tokio::test]
async fn a_call_ended_by_its_deadline_audits_and_records_its_spend() {
    let faults = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = faults.clone();
    seed_fixture(store.as_ref()).await;
    let mut config = server_config();
    // One catalog LIST at a time, so the held LIST parks the whole resolve
    // and the spend reached before it is one exact figure.
    config.catalog_resolve_concurrency = Some(1);
    let running = start_with(
        config,
        Arc::clone(&store),
        Arc::new(ravel_object_store::StoreMetrics::default()),
    )
    .await;
    let client = connect(&running).await;
    let before = query_audit_records(store.as_ref()).await;

    let gate = faults.hold(Op::List, None, Occurrence::Nth(1));
    let (is_error, late) = tokio::time::timeout(
        Duration::from_secs(30),
        call(
            &client,
            "ravel_query_promql",
            json!({
                "query": "m",
                "evaluation_time": (100 * NS_PER_SEC).to_string(),
                // Long enough that a loaded machine still reaches the held
                // LIST before it fires; the hold, not this figure, decides
                // that the deadline ends the call.
                "deadline_ms": 1000,
            }),
        ),
    )
    .await
    .expect("the deadline ends the call");
    assert_eq!(is_error, Some(true), "{late}");
    assert_eq!(late["failure"]["class"], json!("deadline"), "{late}");
    assert_eq!(late["budget"]["effective"]["deadline_ms"], json!(1000));
    // The call reached the store and was parked there when it ended, so the
    // deadline, not the route, ended it.
    let held = gate.held();
    assert_eq!(held.len(), 1);
    for id in held {
        gate.release(id);
    }

    let new = new_records(&before, query_audit_records(store.as_ref()).await);
    assert_eq!(new.len(), 1, "{new:?}");
    assert_eq!(audit_attr(&new[0], "query.language"), Some("promql"));
    assert_eq!(audit_attr(&new[0], "query.text"), Some("m"));
    assert_eq!(audit_attr(&new[0], "query.status"), Some("error"));

    let outcomes = running.query_accounting.outcome_snapshot();
    let timed_out: Vec<_> = outcomes
        .iter()
        .filter(|row| row.status == QueryOutcomeStatus::Timeout)
        .collect();
    assert_eq!(timed_out.len(), 1, "{outcomes:?}");
    // Three store requests completed before the fourth, the held LIST; no
    // bytes had arrived, and the HEAD lookup was the one cache miss. No plan
    // completed, so there is no estimate.
    assert_eq!(
        timed_out[0].counters,
        QueryCostCounters {
            queries: 1,
            s3_requests: 3,
            s3_bytes: 0,
            cache_hits: 0,
            cache_misses: 1,
            decompressed_bytes: 0,
            estimated_requests: 0,
            estimated_store_bytes: 0,
            estimated_decompressed_bytes: 0,
        }
    );

    client.cancel().await.expect("client stops");
    running.shutdown().await.expect("server stops");
}
