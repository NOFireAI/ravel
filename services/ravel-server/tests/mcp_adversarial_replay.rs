//! Cursor and evidence-reference replay across tenants on the MCP surface
//! (issue #1383, ADR-1374 D5 and D8).
//!
//! Two tenants behind one real server share one process cursor key, which is
//! exactly the setting where a cursor one tenant holds could be redeemed by
//! the other: the MAC verifies either way, so only the tenant binding stands
//! between them. The three cursor-minting tools are `ravel_describe_data`,
//! `ravel_query_sql` and `ravel_search_logs` (the only bodies that call
//! `Cursor::redeem`). Driven through `POST /mcp` with two bearer tokens, so
//! this lives beside the server: a `QueryBackend` stub could mint and redeem
//! a token, but not show that the tenant it is checked against is the one the
//! transport authenticated.

#![cfg(feature = "mcp")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod mcp_adversarial_common;

use mcp_adversarial_common::*;
use ravel_object_store::ObjectStoreBackend;
use serde_json::{Value, json};

const SQL_NOW_NS: i64 = 4 * NS_PER_HOUR;

/// Each tenant: three samples of `m`, 150 families and three log lines, all
/// carrying the tenant's own name, so a page of any cursor tool at two rows
/// (or 100 families) leaves a cursor.
async fn seed(store: &dyn ObjectStoreBackend) {
    for (tenant, base) in [(TENANT_A, 1.0), (TENANT_B, 100.0)] {
        publish_series(
            store,
            tenant,
            0,
            1,
            "m",
            &[TestSeries {
                labels: vec![("owner", tenant)],
                samples: (1..=3)
                    .map(|i| (i * 10 * NS_PER_SEC, base * i as f64))
                    .collect(),
            }],
        )
        .await;
        let names: Vec<String> = (0..150).map(|i| format!("fam_{i:03}")).collect();
        write_families(store, tenant, &names, &format!("{tenant} family")).await;
        let lines: Vec<TestLog<'_>> = (1..=3)
            .map(|i| TestLog {
                ts_ns: i * 10 * NS_PER_SEC,
                severity_num: 9,
                body: "shared body",
                trace_id: None,
                attrs: vec![("owner", tenant)],
            })
            .collect();
        publish_logs(store, tenant, 0, 1, &lines).await;
    }
}

/// The three cursor-minting tools, each with arguments whose first page
/// mints a cursor.
fn cursor_tools() -> Vec<(&'static str, Value)> {
    let window = json!({"start_ns": "0", "end_ns": SQL_NOW_NS.to_string()});
    vec![
        ("ravel_describe_data", json!({"signal": "metrics"})),
        (
            "ravel_query_sql",
            json!({"query": "SELECT * FROM samples ORDER BY ts", "time_range": window, "max_rows": 2}),
        ),
        (
            "ravel_search_logs",
            json!({"time_range": window, "max_rows": 2}),
        ),
    ]
}

fn with_cursor(arguments: &Value, cursor: &str) -> Value {
    let mut arguments = arguments.clone();
    arguments["cursor"] = json!(cursor);
    arguments
}

/// The same token with one character of its MAC'd payload changed.
fn tampered(token: &str) -> String {
    let mut bytes = token.as_bytes().to_vec();
    let at = bytes.len() / 2;
    bytes[at] = if bytes[at] == b'A' { b'B' } else { b'A' };
    String::from_utf8(bytes).expect("base64url stays ASCII")
}

/// For every cursor-minting tool, a cursor tenant A's page one minted and
/// tenant B redeems with the same arguments is refused as `cursor_invalid`,
/// before the call reads the store at all (zero store calls over it), with
/// no row, and with exactly the failure a tampered token gets, so B
/// cannot tell "minted for someone else" from "corrupt". The same holds the
/// other way round, and each tenant redeeming its own cursor is served its
/// own page two, so the refused tokens were good ones.
///
/// FLIP: deleting the `cursor.tenant != caller_tenant` check in
/// `Cursor::redeem` (`crates/ravel-mcp/src/cursor.rs`) serves globex's page
/// two at acme's position and the first redemption answers `ok`.
#[tokio::test]
async fn a_cursor_redeemed_by_another_tenant_is_cursor_invalid_for_every_tool() {
    let store = memory_store();
    seed(store.as_ref()).await;
    let (running, metrics) = start_counted(server_config(), store).await;
    let acme = connect(&running, TOKEN_A).await;
    let globex = connect(&running, TOKEN_B).await;

    for (name, arguments) in cursor_tools() {
        for (minter, minter_name, redeemer, redeemer_name) in [
            (&acme, TENANT_A, &globex, TENANT_B),
            (&globex, TENANT_B, &acme, TENANT_A),
        ] {
            let (is_error, one) = call(minter, name, arguments.clone()).await;
            assert_eq!(is_error, Some(false), "{name}: {one}");
            assert_eq!(one["status"], json!("ok_page"), "{name}: {one}");
            let token = cursor_of(&one).expect("page one mints a cursor");

            let before = store_calls(&metrics);
            let (is_error, replayed) = call(redeemer, name, with_cursor(&arguments, &token)).await;
            assert_eq!(
                store_calls(&metrics) - before,
                0,
                "{name}: the refused redemption read the store"
            );
            assert_eq!(is_error, Some(true), "{name} {redeemer_name}: {replayed}");
            assert_eq!(replayed["status"], json!("error"), "{replayed}");
            assert_eq!(
                replayed["failure"]["class"],
                json!("cursor_invalid"),
                "{name}: {replayed}"
            );
            assert_eq!(rows(&replayed).len(), 0, "{replayed}");
            assert_eq!(cursor_of(&replayed), None, "{replayed}");
            assert!(
                !replayed.to_string().contains(minter_name),
                "{name}: the refusal carries {minter_name}'s data: {replayed}"
            );

            let (_, corrupt) =
                call(redeemer, name, with_cursor(&arguments, &tampered(&token))).await;
            assert_eq!(
                replayed["failure"], corrupt["failure"],
                "{name}: a wrong-tenant cursor must fail exactly as a corrupt one"
            );

            let (is_error, own) = call(minter, name, with_cursor(&arguments, &token)).await;
            assert_eq!(is_error, Some(false), "{name} {minter_name}: {own}");
            assert!(!rows(&own).is_empty(), "{name}: {own}");
            assert!(
                own.to_string().contains(minter_name) && !own.to_string().contains(redeemer_name),
                "{name}: {own}"
            );
        }
    }

    acme.cancel().await.expect("client stops");
    globex.cancel().await.expect("client stops");
    running.shutdown().await.expect("server stops");
}

/// `evidence_ref` is not served by this build, and every tool that takes it
/// refuses a value for it as `unsupported` with no row, before reading
/// anything, rather than running the call and ignoring the reference. The
/// day one of them serves it this fails loudly, and the test should then be
/// replaced by a replay case for it. `ravel_find_labels` takes no
/// `evidence_ref` at all and refuses one as `invalid_argument`.
///
/// FLIP: deleting the `input.evidence_ref.is_some()` refusal from
/// `ravel_query_sql`'s `Call::from_args` (`crates/ravel-mcp/src/tools/query_sql.rs`)
/// runs the statement and answers `ok` with acme's rows.
#[tokio::test]
async fn evidence_ref_is_unsupported_by_every_tool_that_takes_it() {
    let store = memory_store();
    seed(store.as_ref()).await;
    let (running, metrics) = start_counted(server_config(), store).await;
    let acme = connect(&running, TOKEN_A).await;
    let window = json!({"start_ns": "0", "end_ns": SQL_NOW_NS.to_string()});
    let reference = "ZXZpZGVuY2U";

    let cases = [
        (
            "ravel_query_sql",
            json!({"query": "SELECT * FROM samples ORDER BY ts", "time_range": window}),
            "unsupported",
        ),
        (
            "ravel_query_promql",
            json!({"query": "m", "evaluation_time": (30 * NS_PER_SEC).to_string()}),
            "unsupported",
        ),
        (
            "ravel_search_logs",
            json!({"time_range": window}),
            "unsupported",
        ),
        (
            "ravel_get_trace",
            json!({"trace_id": "ab".repeat(16)}),
            "unsupported",
        ),
        (
            "ravel_analyze_timeseries",
            json!({
                "query": "m",
                "time_range": {"start_ns": "0", "end_ns": (60 * NS_PER_SEC).to_string()},
                "step": "10s",
                "op": "summary",
            }),
            "unsupported",
        ),
        (
            "ravel_find_labels",
            json!({"label_name": "owner", "time_range": window}),
            "invalid_argument",
        ),
    ];
    for (name, arguments, class) in cases {
        // Without the reference the same call is served, so the refusal
        // below is the reference's and nothing else's.
        let (is_error, served) = call(&acme, name, arguments.clone()).await;
        assert_eq!(is_error, Some(false), "{name}: {served}");

        let mut with_reference = arguments;
        with_reference["evidence_ref"] = json!(reference);
        let before = store_calls(&metrics);
        let (is_error, refused) = call(&acme, name, with_reference).await;
        assert_eq!(store_calls(&metrics) - before, 0, "{name}: read the store");
        assert_eq!(is_error, Some(true), "{name}: {refused}");
        assert_eq!(
            refused["failure"]["class"],
            json!(class),
            "{name}: {refused}"
        );
        let message = refused["failure"]["message"].as_str().expect("a message");
        assert!(message.contains("evidence_ref"), "{name}: {message}");
        if class == "unsupported" {
            assert_eq!(
                message,
                format!("evidence_ref is not served by {name} in this build")
            );
        }
        assert_eq!(rows(&refused).len(), 0, "{name}: {refused}");
        assert_eq!(refused["data"]["row_count"], json!(0), "{name}: {refused}");
        assert_eq!(refused["evidence"], json!([]), "{name}: {refused}");
    }

    acme.cancel().await.expect("client stops");
    running.shutdown().await.expect("server stops");
}
