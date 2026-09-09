//! `ravel_capabilities` (ADR-1374 D2): what this deployment serves, in one
//! call that reads no object storage.
//!
//! Everything reported here is either a compile-time fact (the protocol
//! revisions this build speaks, the server version, the tool catalog, the
//! query dialects) or already resolved on the call's own
//! [`ToolContext`](super::ToolContext) (the tenant hash, the effective budget
//! ceilings). Nothing is discovered, so the tool costs zero store requests
//! and zero scanned bytes, and its `budget.actual` block says so.

use rmcp::model::ProtocolVersion;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::catalog::{CapabilitiesInput, tool_catalog};
use crate::envelope::{
    AnyJson, Budget, Cell, Column, Envelope, Failure, FailureClass, Row, Status,
};

use super::{SERVED_TOOLS, ToolContext};

/// The signals a Ravel deployment answers agent queries over. Audit is a
/// tenant's own trail rather than a queryable telemetry signal, so it is not
/// listed.
const ENABLED_SIGNALS: [&str; 3] = ["metrics", "logs", "traces"];

/// The two column names of the capability table.
const CAPABILITY_COLUMN: &str = "capability";
const VALUE_COLUMN: &str = "value";

/// Run the tool. The only way this fails is an argument object that is not
/// the (empty) `CapabilitiesInput` shape, which is a D4 `invalid_argument`
/// envelope rather than a protocol error.
pub fn run(args: Value, ctx: &ToolContext<'_>) -> Envelope {
    if let Err(err) = CapabilitiesInput::deserialize(args) {
        return invalid_argument(format!("ravel_capabilities arguments: {err}"), ctx);
    }

    let mut envelope = Envelope {
        data: crate::envelope::Data {
            columns: vec![
                Column {
                    name: CAPABILITY_COLUMN.to_string(),
                    r#type: "string".to_string(),
                },
                Column {
                    name: VALUE_COLUMN.to_string(),
                    r#type: "json".to_string(),
                },
            ],
            rows: capability_rows(ctx),
            row_count: 0,
        },
        ..Envelope::default()
    };
    envelope.data.row_count = envelope.data.rows.len() as u64;
    envelope.scope.signal = "none".to_string();
    envelope.scope.table = "capabilities".to_string();
    envelope.coverage.complete = true;
    envelope.accuracy.exact = true;
    envelope.presentation.max_rows = ctx.budgets.max_rows;
    envelope.budget = budget_block(ctx);

    // No ordering, so no cursor: the whole table is always one page.
    envelope.fit(ctx.budgets.max_response_bytes).finish(false)
}

/// One row per capability group, each value a JSON object.
fn capability_rows(ctx: &ToolContext<'_>) -> Vec<Row> {
    vec![
        row(
            "protocol",
            json!({
                "revisions": [
                    ProtocolVersion::V_2026_07_28.as_str(),
                    ProtocolVersion::V_2025_11_25.as_str(),
                ],
                "server_version": env!("CARGO_PKG_VERSION"),
            }),
        ),
        row(
            "tools",
            json!({
                "enabled": catalog_names(true),
                "catalogued": catalog_names(false),
            }),
        ),
        row("budgets", budget_ceilings(ctx)),
        row("tenant", json!({ "hash": ctx.tenant_hash.to_hex() })),
        row("signals", json!({ "enabled": ENABLED_SIGNALS })),
        row(
            "dialects",
            json!({
                "promql": "PromQL instant and range queries over metrics, and over log \
                           streams through the log metric names",
                "sql": "Read-only SELECT over the metrics, logs, and traces tables, with \
                        an explain-only mode",
            }),
        ),
    ]
}

/// The catalog names this build serves (`served`) or merely declares.
///
/// `enabled` is what [`dispatch`](super::dispatch) will actually run today;
/// `catalogued` is present in the catalog and answers `NotShipped`. Reporting
/// one merged list would tell a caller it may call eight tools that refuse
/// every call, which is worse than telling it nothing.
fn catalog_names(served: bool) -> Vec<String> {
    tool_catalog()
        .iter()
        .map(|tool| tool.name.to_string())
        .filter(|name| SERVED_TOOLS.contains(&name.as_str()) == served)
        .collect()
}

/// A `(capability, value)` row. A value that is not a JSON object cannot be
/// built by any caller of this function, and a non-object degrades to its
/// text form rather than being dropped.
fn row(name: &str, value: Value) -> Row {
    let cell = match value {
        Value::Object(map) => Cell::Map(map),
        other => Cell::Str(other.to_string()),
    };
    vec![Cell::Str(name.to_string()), cell]
}

/// The effective ceilings this call resolved to, in the same spelling the
/// envelope's `budget.effective` block uses.
fn budget_ceilings(ctx: &ToolContext<'_>) -> Value {
    let budgets = &ctx.budgets;
    json!({
        "max_rows": budgets.max_rows,
        "max_response_bytes": budgets.max_response_bytes,
        "deadline_ms": budgets.deadline.as_millis() as u64,
        "max_bytes_scanned": byte_limit(budgets.query.max_bytes_scanned),
        "max_store_requests": request_limit(budgets.query.max_store_requests),
        "max_segments": budgets.query.max_segments,
    })
}

fn byte_limit(limit: ravel_query::ByteLimit) -> Value {
    match limit {
        ravel_query::ByteLimit::Bounded(bytes) => json!(bytes),
        ravel_query::ByteLimit::Unlimited => Value::Null,
    }
}

fn request_limit(limit: ravel_query::RequestLimit) -> Value {
    match limit {
        ravel_query::RequestLimit::Bounded(requests) => json!(requests),
        ravel_query::RequestLimit::Unlimited => Value::Null,
    }
}

/// `effective` is what the call resolved to; `actual` and `estimate` are the
/// zero this tool genuinely costs, not a placeholder. The estimate is exact,
/// so it is not an upper envelope.
fn budget_block(ctx: &ToolContext<'_>) -> Budget {
    let spent = json!({
        "bytes_scanned": 0,
        "store_requests": 0,
        "segments_read": 0,
    });
    Budget {
        effective: AnyJson(budget_ceilings(ctx)),
        actual: AnyJson(spent.clone()),
        estimate: AnyJson(spent),
        estimate_is_upper_envelope: false,
    }
}

fn invalid_argument(message: String, ctx: &ToolContext<'_>) -> Envelope {
    let envelope = Envelope {
        status: Status::Error,
        failure: Some(Failure {
            class: FailureClass::InvalidArgument,
            message,
            counter: None,
        }),
        budget: Budget {
            effective: AnyJson(budget_ceilings(ctx)),
            actual: AnyJson(Value::Object(Map::new())),
            estimate: AnyJson(Value::Object(Map::new())),
            estimate_is_upper_envelope: false,
        },
        ..Envelope::default()
    };
    envelope.fit(ctx.budgets.max_response_bytes).finish(false)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::cursor::{CURSOR_KEY_LEN, CursorKey};
    use crate::tools::tests::test_context;

    /// Every group the D2 catalog entry promises is present exactly once, and
    /// the tenant hash is the one the transport authenticated rather than a
    /// value the caller could influence.
    #[test]
    fn capabilities_reports_every_group_once() {
        let key = CursorKey::from_process_secret([3u8; CURSOR_KEY_LEN]);
        let ctx = test_context(&key);
        let envelope = run(json!({}), &ctx);

        assert_eq!(envelope.status, Status::Ok);
        assert_eq!(envelope.data.row_count, 6);

        let groups: Vec<String> = envelope
            .data
            .rows
            .iter()
            .map(|row| match &row[0] {
                Cell::Str(name) => name.clone(),
                other => panic!("capability column is not a string: {other:?}"),
            })
            .collect();
        assert_eq!(
            groups,
            vec![
                "protocol", "tools", "budgets", "tenant", "signals", "dialects"
            ]
        );

        let value = serde_json::to_value(&envelope).expect("envelope serializes");
        let rows = value["data"]["rows"]
            .as_array()
            .expect("rows is an array")
            .clone();
        assert_eq!(rows[0][1]["revisions"], json!(["2026-07-28", "2025-11-25"]));
        // Served and catalogued are separate lists, and the exact split is
        // what this build does today: one body, eight declared names that
        // refuse every call.
        assert_eq!(rows[1][1]["enabled"], json!(["ravel_capabilities"]));
        assert_eq!(
            rows[1][1]["catalogued"]
                .as_array()
                .expect("catalogued tool list")
                .len(),
            8
        );
        assert_eq!(rows[3][1]["hash"], json!(ctx.tenant_hash.to_hex()));
        assert_eq!(rows[4][1]["enabled"], json!(["metrics", "logs", "traces"]));
    }

    /// The tool reports zero spend, and reports it as an exact figure rather
    /// than an upper envelope: it reads nothing.
    #[test]
    fn capabilities_reports_zero_spend() {
        let key = CursorKey::from_process_secret([5u8; CURSOR_KEY_LEN]);
        let ctx = test_context(&key);
        let envelope = run(json!({}), &ctx);

        assert!(!envelope.budget.estimate_is_upper_envelope);
        assert_eq!(envelope.budget.actual.0["store_requests"], json!(0));
        assert_eq!(envelope.budget.actual.0["bytes_scanned"], json!(0));
        assert_eq!(
            envelope.budget.effective.0["max_rows"],
            json!(ctx.budgets.max_rows)
        );
    }

    /// `ravel_capabilities` never resolves a snapshot, a watermark, a query
    /// id, or an audit ref: it answers from compile-time facts and the
    /// call's own context, reading no object storage. So it always carries
    /// all four D4 identity fields as warnings, in the same wording and
    /// order `finish` uses everywhere else, rather than shipping them as the
    /// empty strings D4's typing would otherwise force.
    #[test]
    fn capabilities_warns_about_its_unmeasured_identity_fields() {
        let key = CursorKey::from_process_secret([11u8; CURSOR_KEY_LEN]);
        let ctx = test_context(&key);

        let envelope = run(json!({}), &ctx);

        assert_eq!(
            envelope.warnings,
            vec![
                "visibility.snapshot_id is not reported by this operation".to_string(),
                "visibility.watermark_hour is not reported by this operation".to_string(),
                "ids.query_id is not reported by this operation".to_string(),
                "ids.audit_ref is not reported by this operation".to_string(),
            ]
        );
    }

    /// A non-object argument is the caller's mistake, and it comes back as a
    /// D4 `invalid_argument` envelope rather than a protocol error, because
    /// D4 reserves protocol errors for malformed JSON-RPC and unknown tools.
    #[test]
    fn non_object_arguments_are_an_invalid_argument_envelope() {
        let key = CursorKey::from_process_secret([9u8; CURSOR_KEY_LEN]);
        let ctx = test_context(&key);
        let envelope = run(json!("capabilities"), &ctx);

        assert_eq!(envelope.status, Status::Error);
        let failure = envelope.failure.expect("an error envelope carries one");
        assert_eq!(failure.class, FailureClass::InvalidArgument);
    }
}
