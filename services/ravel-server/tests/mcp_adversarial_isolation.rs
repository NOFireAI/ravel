//! Tenant isolation on the MCP surface (issue #1383, ADR-1374 D8): two
//! tenants with distinct hashes and overlapping data behind one real
//! `ravel_server::start` server, each reached over `POST /mcp` with its own
//! bearer token. Both hold metric `m` with families of the same names, log
//! lines with the same body and trace id, and spans of that same trace id;
//! only an `owner` label or attribute, a help text and a span name say whose
//! they are, and the two tenants hold different numbers of each.
//!
//! This lives beside the server rather than in `crates/ravel-mcp/tests/`
//! because the property is the real adapter's: the tenant comes from the
//! bearer token through `auth` and the service layer scopes every read to
//! it, and a `QueryBackend` stub has neither.

#![cfg(feature = "mcp")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod mcp_adversarial_common;

use mcp_adversarial_common::*;
use ravel_object_store::ObjectStoreBackend;
use serde_json::{Value, json};

const SHARED_TRACE: [u8; 16] = [0x5a; 16];
const SQL_NOW_NS: i64 = 4 * NS_PER_HOUR;

/// Both tenants' overlapping fixture: tenant A (`acme`) holds one segment of
/// one series, 120 families, three log lines and two spans; tenant B
/// (`globex`) two segments of two series, 130 families, four log lines and
/// three spans.
async fn seed(store: &dyn ObjectStoreBackend) {
    publish_series(
        store,
        TENANT_A,
        0,
        1,
        "m",
        &[TestSeries {
            labels: vec![("owner", "acme")],
            samples: vec![
                (10 * NS_PER_SEC, 1.0),
                (20 * NS_PER_SEC, 2.0),
                (30 * NS_PER_SEC, 3.0),
            ],
        }],
    )
    .await;
    publish_series(
        store,
        TENANT_B,
        0,
        1,
        "m",
        &[TestSeries {
            labels: vec![("owner", "globex")],
            samples: vec![
                (10 * NS_PER_SEC, 100.0),
                (20 * NS_PER_SEC, 200.0),
                (30 * NS_PER_SEC, 300.0),
                (40 * NS_PER_SEC, 400.0),
            ],
        }],
    )
    .await;
    publish_series(
        store,
        TENANT_B,
        1,
        2,
        "m",
        &[TestSeries {
            labels: vec![("owner", "globex"), ("extra", "x")],
            samples: vec![(50 * NS_PER_SEC, 500.0)],
        }],
    )
    .await;
    let families = |n: usize| -> Vec<String> { (0..n).map(|i| format!("fam_{i:03}")).collect() };
    write_families(store, TENANT_A, &families(120), "acme family").await;
    write_families(store, TENANT_B, &families(130), "globex family").await;
    let logs = |owner: &'static str, n: i64| -> Vec<TestLog<'static>> {
        (1..=n)
            .map(|i| TestLog {
                ts_ns: i * 10 * NS_PER_SEC,
                severity_num: 9,
                body: "shared body",
                trace_id: Some(SHARED_TRACE),
                attrs: vec![("owner", owner)],
            })
            .collect()
    };
    publish_logs(store, TENANT_A, 0, 1, &logs("acme", 3)).await;
    publish_logs(store, TENANT_B, 0, 1, &logs("globex", 4)).await;
    let spans = |name: &'static str, n: u8| -> Vec<TestSpan<'static>> {
        (1..=n)
            .map(|i| TestSpan {
                span_id: [i; 8],
                parent_span_id: (i > 1).then_some([1; 8]),
                name,
                start_ts_ns: i64::from(i) * 10 * NS_PER_SEC,
                attrs: Vec::new(),
            })
            .collect()
    };
    publish_trace(store, TENANT_A, SHARED_TRACE, &spans("acme-op", 2)).await;
    publish_trace(store, TENANT_B, SHARED_TRACE, &spans("globex-op", 3)).await;
}

/// What one tenant must see of the fixture, and the marker that must never
/// appear in anything it is answered.
struct Expect {
    tenant: &'static str,
    token: &'static str,
    marker: &'static str,
    foreign: &'static str,
    families_total: u64,
    committed_rows: &'static str,
    segments: u64,
    series: u64,
    sql_values: Vec<f64>,
    promql_values: Vec<f64>,
    log_lines: usize,
    span_names: Vec<&'static str>,
    analyzed_points: Vec<&'static str>,
}

fn acme() -> Expect {
    Expect {
        tenant: TENANT_A,
        token: TOKEN_A,
        marker: "acme",
        foreign: "globex",
        families_total: 120,
        committed_rows: "3",
        segments: 1,
        series: 1,
        sql_values: vec![1.0, 2.0, 3.0],
        promql_values: vec![3.0],
        log_lines: 3,
        span_names: vec!["acme-op"; 2],
        analyzed_points: vec!["5"],
    }
}

fn globex() -> Expect {
    Expect {
        tenant: TENANT_B,
        token: TOKEN_B,
        marker: "globex",
        foreign: "acme",
        families_total: 130,
        committed_rows: "5",
        segments: 2,
        series: 2,
        sql_values: vec![100.0, 200.0, 300.0, 400.0, 500.0],
        promql_values: vec![400.0, 500.0],
        log_lines: 4,
        span_names: vec!["globex-op"; 3],
        analyzed_points: vec!["1", "5"],
    }
}

/// Every answer one tenant gets: no trace of the other tenant's marker
/// anywhere in the envelope or its text rendering.
fn assert_clean(expect: &Expect, what: &str, envelope: &Value) {
    let text = envelope.to_string();
    assert!(
        !text.contains(expect.foreign),
        "{} {what} carries {}'s data: {text}",
        expect.tenant,
        expect.foreign
    );
}

/// Walk every page of `name` from `arguments`, asserting each page clean, and
/// return the pages.
async fn walk(client: &Client, expect: &Expect, name: &str, arguments: Value) -> Vec<Value> {
    let mut pages = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut arguments = arguments.clone();
        if let Some(token) = &cursor {
            arguments["cursor"] = json!(token);
        }
        let (is_error, page) = call(client, name, arguments).await;
        assert_eq!(is_error, Some(false), "{} {name}: {page}", expect.tenant);
        assert_clean(expect, name, &page);
        cursor = cursor_of(&page);
        pages.push(page);
        if cursor.is_none() {
            return pages;
        }
        assert!(pages.len() < 10, "{name} never ends: {pages:?}");
    }
}

fn floats(cells: &[Value]) -> Vec<f64> {
    cells
        .iter()
        .map(|cell| cell.as_f64().unwrap_or_else(|| panic!("a float: {cell}")))
        .collect()
}

/// A refusal is clean, carries no rows, and counts none.
fn assert_refused(expect: &Expect, what: &str, is_error: Option<bool>, envelope: &Value) {
    assert_eq!(is_error, Some(true), "{what}: {envelope}");
    assert_eq!(envelope["status"], json!("error"), "{what}: {envelope}");
    assert_eq!(rows(envelope).len(), 0, "{what}: {envelope}");
    assert_eq!(
        envelope["data"]["row_count"],
        json!(0),
        "{what}: {envelope}"
    );
    assert_clean(expect, what, envelope);
}

/// Every tool `expect.tenant` calls, on its first page, its later pages and a
/// refusal, answered from that tenant's own data. Returns how many of the
/// calls run a query and so must each leave one audit record.
async fn every_tool(running: &ravel_server::Running, expect: &Expect) -> usize {
    let client = connect(running, expect.token).await;
    let window = json!({"start_ns": "0", "end_ns": SQL_NOW_NS.to_string()});
    let trace: String = SHARED_TRACE.iter().map(|b| format!("{b:02x}")).collect();
    let mut audited = 0;

    let (_, capabilities) = call(&client, "ravel_capabilities", json!({})).await;
    assert_clean(expect, "capabilities", &capabilities);
    assert_eq!(
        rows(&capabilities)[3],
        json!(["tenant", {"hash": hash(expect.tenant).to_hex()}]),
        "{capabilities}"
    );

    let pages = walk(
        &client,
        expect,
        "ravel_describe_data",
        json!({"signal": "metrics"}),
    )
    .await;
    audited += pages.len();
    assert_eq!(pages.len(), 2, "{pages:?}");
    let table = &rows(&pages[0])[0];
    assert_eq!(table[2]["families_total"], json!(expect.families_total));
    assert_eq!(table[2]["committed_rows"], json!(expect.committed_rows));
    assert_eq!(table[2]["segments"], json!(expect.segments));
    let helps: Vec<&Value> = pages
        .iter()
        .flat_map(|page| rows(page).iter())
        .filter(|row| row[0] == json!("family"))
        .map(|row| &row[2]["help"])
        .collect();
    assert_eq!(helps.len() as u64, expect.families_total);
    assert!(
        helps
            .iter()
            .all(|help| **help == json!(format!("{} family", expect.marker)))
    );

    let (is_error, labels) = call(
        &client,
        "ravel_find_labels",
        json!({"label_name": "owner", "time_range": window}),
    )
    .await;
    audited += 1;
    assert_eq!(is_error, Some(false), "{labels}");
    assert_clean(expect, "find_labels", &labels);
    assert_eq!(labels["data"]["rows"], json!([[expect.marker]]), "{labels}");

    let (is_error, explain) = call(
        &client,
        "ravel_explain_query",
        json!({"query": "SELECT * FROM samples", "time_range": window}),
    )
    .await;
    audited += 1;
    assert_eq!(is_error, Some(false), "{explain}");
    assert_clean(expect, "explain_query", &explain);
    let estimate = &explain["budget"]["estimate"];
    assert_eq!(
        estimate["segments_resolved"],
        json!(expect.segments),
        "{explain}"
    );
    assert_eq!(estimate["series"], json!(expect.series), "{explain}");

    let pages = walk(
        &client,
        expect,
        "ravel_query_sql",
        json!({"query": "SELECT * FROM samples ORDER BY ts", "time_range": window, "max_rows": 2}),
    )
    .await;
    audited += pages.len();
    let values: Vec<f64> = pages
        .iter()
        .flat_map(|page| floats(&column(page, "value")))
        .collect();
    assert_eq!(values, expect.sql_values);
    assert_eq!(pages.len(), expect.sql_values.len().div_ceil(2));
    assert_eq!(
        pages[0]["budget"]["actual"]["segments_read"],
        json!(expect.segments),
        "{}",
        pages[0]
    );

    let (_, instant) = call(
        &client,
        "ravel_query_promql",
        json!({"query": "m", "evaluation_time": (60 * NS_PER_SEC).to_string()}),
    )
    .await;
    audited += 1;
    assert_clean(expect, "query_promql", &instant);
    let mut values: Vec<f64> = rows(&instant)
        .iter()
        .flat_map(|row| floats(row[1]["value"].as_array().expect("values")))
        .collect();
    values.sort_by(f64::total_cmp);
    assert_eq!(values, expect.promql_values, "{instant}");

    let pages = walk(
        &client,
        expect,
        "ravel_search_logs",
        json!({"time_range": window, "trace_id": trace, "max_rows": 2}),
    )
    .await;
    audited += pages.len();
    let lines: usize = pages.iter().map(|page| rows(page).len()).sum();
    assert_eq!(lines, expect.log_lines, "{pages:?}");
    assert_eq!(pages.len(), expect.log_lines.div_ceil(2));
    assert_eq!(
        rows(&pages[0])[0][1]["owner"],
        json!(expect.marker),
        "{}",
        pages[0]
    );

    let (_, trace_rows) = call(&client, "ravel_get_trace", json!({"trace_id": trace})).await;
    audited += 1;
    assert_clean(expect, "get_trace", &trace_rows);
    assert_eq!(
        column(&trace_rows, "name"),
        expect
            .span_names
            .iter()
            .map(|name| json!(name))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        trace_rows["data"]["row_count"],
        json!(expect.span_names.len())
    );

    let (_, analysis) = call(
        &client,
        "ravel_analyze_timeseries",
        json!({
            "query": "m",
            "time_range": {"start_ns": "0", "end_ns": (60 * NS_PER_SEC).to_string()},
            "step": "10s",
            "op": "summary",
        }),
    )
    .await;
    audited += 1;
    assert_clean(expect, "analyze_timeseries", &analysis);
    let mut points: Vec<Value> = column(&analysis, "points");
    points.sort_by_key(Value::to_string);
    assert_eq!(
        points,
        expect
            .analyzed_points
            .iter()
            .map(|n| json!(n))
            .collect::<Vec<_>>(),
        "{analysis}"
    );

    // One refusal per tool, each for an argument the tool checks before it
    // reads: none answers with a row or a count, and none with the other
    // tenant's data.
    let bad_window = json!({"start_ns": "10", "end_ns": "5"});
    let refusals = [
        ("ravel_describe_data", json!({"signal": "nope"})),
        (
            "ravel_find_labels",
            json!({"label_name": "owner", "time_range": bad_window}),
        ),
        (
            "ravel_explain_query",
            json!({"query": "SELECT * FROM samples", "time_range": bad_window}),
        ),
        (
            "ravel_query_sql",
            json!({"query": "SELECT * FROM samples ORDER BY ts", "time_range": bad_window}),
        ),
        ("ravel_query_promql", json!({"query": "m"})),
        ("ravel_search_logs", json!({"time_range": bad_window})),
        ("ravel_get_trace", json!({"trace_id": "not-hex"})),
        (
            "ravel_analyze_timeseries",
            json!({"query": "m", "time_range": bad_window, "step": "10s", "op": "summary"}),
        ),
    ];
    for (name, arguments) in refusals {
        let (is_error, refused) = call(&client, name, arguments).await;
        assert_refused(expect, name, is_error, &refused);
    }

    client.cancel().await.expect("client stops");
    audited
}

/// Two tenants with distinct hashes and overlapping data, behind one server
/// and one process cursor key: every tool, on its first page, every later
/// page it mints, and a refusal, answers each tenant with its own rows and
/// its own counts (families, committed rows, segments, series, log lines,
/// spans, analysed points), and never with a byte of the other's. Every
/// query either tenant ran left exactly one audit record, on that tenant's
/// own audit shard, naming that tenant's hash.
///
/// FLIP: `RavelMcp::call_tool` in `services/ravel-server/src/mcp/adapter.rs`
/// taking `tenant_hash` as `TenantId::new("acme").hash()` instead of the
/// authenticated principal's answers globex with acme's data, and the first
/// globex assertion fails (capabilities reports acme's hash).
#[tokio::test]
async fn every_tool_answers_each_tenant_from_its_own_data_only() {
    let store = memory_store();
    seed(store.as_ref()).await;
    let running = start(audited_config(), store.clone()).await;
    assert_ne!(hash(TENANT_A), hash(TENANT_B));

    let mut audited = Vec::new();
    for expect in [acme(), globex()] {
        audited.push((every_tool(&running, &expect).await, expect));
    }
    // Read after both tenants ran, so a record either call routed to the
    // wrong shard is counted on the shard it landed on.
    for (audited, expect) in audited {
        let records = query_audit_records(store.as_ref(), expect.tenant).await;
        assert_eq!(records.len(), audited, "{}: {records:?}", expect.tenant);
        let own = hash(expect.tenant).to_hex();
        for record in &records {
            assert_eq!(
                audit_attr(record, "query.tenant"),
                Some(own.as_str()),
                "{record:?}"
            );
        }
    }

    running.shutdown().await.expect("server stops");
}
