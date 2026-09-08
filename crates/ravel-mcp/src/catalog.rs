//! The D2 nine-tool catalog (ADR-1374).
//!
//! Every tool advertises the same shallow envelope shape as its output
//! schema (see [`envelope_output_schema`] for why it is shallow rather than
//! the fully nested [`crate::envelope::Envelope`] derive): D4 defines one
//! envelope for every tool result, success or failure, so there is nothing
//! tool-specific to generate there. What differs per tool is its input
//! schema, built from a small per-tool argument struct via
//! [`schemars::JsonSchema`] and `rmcp`'s non-macro `Tool` builder (the
//! `#[tool]` attribute macro cannot express a separate output schema, per
//! the spike in this crate's earlier commits). Tool bodies -- the code that
//! actually calls a [`crate::service`] trait -- land in #1380 and #1382;
//! this module only advertises shape.

use std::sync::Arc;

use rmcp::model::{JsonObject, Tool, ToolAnnotations};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::envelope::{AnyJson, TimeRange};

/// `ravel_capabilities` takes no arguments.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
pub struct CapabilitiesInput {}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct DescribeDataInput {
    pub signal: String,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct FindLabelsInput {
    pub selector: Option<String>,
    pub label_name: Option<String>,
    pub filter: Option<String>,
    pub time_range: TimeRange,
    pub evidence_ref: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ExplainQueryInput {
    pub query: String,
    pub time_range: TimeRange,
}

/// Every data-returning tool declares the same six lowerable budget knobs
/// (docs/reference/mcp.md#budget-defaults-and-floors): `max_rows`,
/// `max_bytes_scanned`, `max_store_requests`, `max_segments`,
/// `max_response_bytes`, `deadline_ms`. An absent field resolves to its
/// default or to the server ceiling, and a value above its ceiling clamps
/// down; both happen in [`crate::budget::McpRequestBudgets::clamp`], never
/// here. The fields are repeated per input struct rather than shared,
/// because `tools/list` advertises each tool's input schema on its own and a
/// caller reads that one schema, not a definition it would have to resolve.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct QuerySqlInput {
    pub query: String,
    pub time_range: TimeRange,
    /// Rows returned; default 200, ceiling 5,000.
    pub max_rows: Option<u32>,
    pub max_bytes_scanned: Option<u64>,
    pub max_store_requests: Option<u32>,
    pub max_segments: Option<u32>,
    /// Bounds the whole serialized envelope; default 512 KiB, floored at
    /// 256 KiB.
    pub max_response_bytes: Option<u64>,
    pub deadline_ms: Option<u64>,
    pub cursor: Option<String>,
    pub evidence_ref: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct QueryPromqlInput {
    pub query: String,
    /// Set together with `step` for a range evaluation. Exactly one of this
    /// pair or `evaluation_time` must be present.
    pub time_range: Option<TimeRange>,
    pub step: Option<String>,
    /// Set alone for an instant evaluation. Exactly one of this or the
    /// `time_range`/`step` pair must be present.
    pub evaluation_time: Option<String>,
    pub allow_partial_coverage: bool,
    /// Series rows returned; default 200, ceiling 5,000.
    pub max_rows: Option<u32>,
    pub max_bytes_scanned: Option<u64>,
    pub max_store_requests: Option<u32>,
    pub max_segments: Option<u32>,
    /// Bounds the whole serialized envelope; default 512 KiB, floored at
    /// 256 KiB.
    pub max_response_bytes: Option<u64>,
    pub deadline_ms: Option<u64>,
    pub evidence_ref: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct SearchLogsInput {
    /// Indexed and typed-attribute predicates. Shape is per-predicate; this
    /// tool has no fixed predicate schema, so each entry is caller-supplied
    /// JSON.
    #[serde(default)]
    pub predicates: Vec<AnyJson>,
    pub has_word: Option<String>,
    pub severity: Option<String>,
    pub trace_id: Option<String>,
    pub time_range: TimeRange,
    /// Rows returned; default 200, ceiling 5,000.
    pub max_rows: Option<u32>,
    pub max_bytes_scanned: Option<u64>,
    pub max_store_requests: Option<u32>,
    pub max_segments: Option<u32>,
    /// Bounds the whole serialized envelope; default 512 KiB, floored at
    /// 256 KiB.
    pub max_response_bytes: Option<u64>,
    pub deadline_ms: Option<u64>,
    pub cursor: Option<String>,
    pub evidence_ref: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct GetTraceInput {
    pub trace_id: String,
    pub time_range: TimeRange,
    pub include_logs: Option<bool>,
    /// Spans (plus log rows when `include_logs` is set) returned; default
    /// 200, ceiling 5,000.
    pub max_rows: Option<u32>,
    pub max_bytes_scanned: Option<u64>,
    pub max_store_requests: Option<u32>,
    pub max_segments: Option<u32>,
    /// Bounds the whole serialized envelope; default 512 KiB, floored at
    /// 256 KiB.
    pub max_response_bytes: Option<u64>,
    pub deadline_ms: Option<u64>,
    pub evidence_ref: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AnalyzeOp {
    ChangePoint,
    Summary,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct AnalyzeTimeseriesInput {
    pub query: String,
    pub time_range: TimeRange,
    pub step: String,
    pub op: AnalyzeOp,
    pub evidence_ref: Option<String>,
}

fn read_only_annotations() -> ToolAnnotations {
    ToolAnnotations::new()
        .read_only(true)
        .destructive(false)
        .open_world(false)
}

/// The D4 envelope's top-level shape, deliberately shallow.
///
/// [`crate::envelope::Envelope`] already derives `JsonSchema` and a caller
/// with a use for the fully nested, per-block schema (a doc generator, a
/// client-side validator) can ask for it directly. `tools/list` cannot: the
/// derived schema serializes to 5,146 B, and D2 bounds the whole
/// `tools/list` response to under 24,576 B for nine tools plus their input
/// schemas, descriptions, and annotations, so attaching it to every one of
/// the nine tools (46 KiB before anything else) blows that budget by
/// roughly 2x on the output schemas alone. Every block's own shape is
/// already normative in `docs/reference/mcp.md#the-envelope` and in
/// `ravel_mcp::envelope`; leaving each block open here is the same
/// "unconstrained schema is the honest one" choice `envelope.rs` already
/// makes for `Cell` and `AnyJson`, extended to the tool-catalog level.
fn envelope_output_schema() -> Arc<JsonObject> {
    let schema = json!({
        "type": "object",
        "description": "The D4 result envelope. See docs/reference/mcp.md#the-envelope.",
        "properties": {
            "status": {"type": "string", "enum": ["ok", "ok_bounded", "ok_page", "error"]},
            "failure": {},
            "data": {},
            "plan": {},
            "scope": {},
            "ids": {},
            "visibility": {},
            "coverage": {},
            "accuracy": {},
            "presentation": {},
            "budget": {},
            "evidence": {},
            "warnings": {},
            "next_steps": {}
        },
        "required": [
            "status", "failure", "data", "plan", "scope", "ids", "visibility",
            "coverage", "accuracy", "presentation", "budget", "evidence",
            "warnings", "next_steps"
        ]
    });
    let object = match schema {
        Value::Object(map) => map,
        _ => unreachable!("the literal above is always a JSON object"),
    };
    Arc::new(object)
}

fn tool<T: JsonSchema + 'static>(
    name: &'static str,
    title: &'static str,
    description: &'static str,
) -> Tool {
    Tool::new_with_raw(name, Some(description.into()), JsonObject::default())
        .with_input_schema::<T>()
        .with_raw_output_schema(envelope_output_schema())
        .with_title(title)
        .with_annotations(read_only_annotations())
}

/// The nine D2 tools, in the fixed order the reference doc
/// (docs/reference/mcp.md) lists them.
pub fn tool_catalog() -> Vec<Tool> {
    vec![
        tool::<CapabilitiesInput>(
            "ravel_capabilities",
            "Capabilities",
            "Protocol and server version, enabled tools, effective budget \
             ceilings, dialect summaries, tenant hash, enabled signals",
        ),
        tool::<DescribeDataInput>(
            "ravel_describe_data",
            "Describe data",
            "Effective schema, indexed keys, metric families, freshness \
             watermark, coverage window, exact row counts where available",
        ),
        tool::<FindLabelsInput>(
            "ravel_find_labels",
            "Find labels",
            "Metric names, label names, or label values for a selector",
        ),
        tool::<ExplainQueryInput>(
            "ravel_explain_query",
            "Explain query",
            "Validate a SQL or PromQL statement, estimate its cost, and \
             return the plan shape. No scan runs.",
        ),
        tool::<QuerySqlInput>("ravel_query_sql", "Query SQL", "One SELECT over one table"),
        tool::<QueryPromqlInput>(
            "ravel_query_promql",
            "Query PromQL",
            "Instant or range PromQL evaluation",
        ),
        tool::<SearchLogsInput>(
            "ravel_search_logs",
            "Search logs",
            "Typed log search compiled to SQL",
        ),
        tool::<GetTraceInput>(
            "ravel_get_trace",
            "Get trace",
            "Spans of one trace id, the span tree, missing parents, orphans",
        ),
        tool::<AnalyzeTimeseriesInput>(
            "ravel_analyze_timeseries",
            "Analyze timeseries",
            "change_point or summary over a PromQL range result",
        ),
    ]
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use rmcp::model::ListToolsResult;

    use super::*;

    use crate::envelope::{Envelope, Status};

    /// The exact serialized size of the nine-tool `tools/list` response.
    ///
    /// Pinned rather than bounded: the D2 budget is what the figure must
    /// stay under, but a schema change that moves it is a change to what
    /// every client parses on every reconnect, and it should be read in a
    /// diff rather than absorbed silently anywhere under the bound. Update
    /// it in the same commit as the schema change that moves it.
    const TOOLS_LIST_SERIALIZED_LEN: usize = 14_206;

    /// D2 bounds `tools/list` so the catalog itself never competes with a
    /// data response for the response-size budget.
    const _: () = assert!(
        TOOLS_LIST_SERIALIZED_LEN < 24_576,
        "tools/list must serialize to under 24576 bytes"
    );

    /// `tools/list` is served once per session and re-parsed by every
    /// client on every reconnect.
    #[test]
    fn tools_list_serializes_inside_band() {
        let tools = tool_catalog();
        assert_eq!(tools.len(), 9, "D2 names exactly nine tools");
        let result = ListToolsResult::with_all_items(tools);
        let bytes = serde_json::to_vec(&result).expect("tools/list serializes");
        assert_eq!(bytes.len(), TOOLS_LIST_SERIALIZED_LEN);
    }

    /// The advertised output schema is hand-written, so nothing but a test
    /// keeps it in step with the struct it describes. A block added to
    /// [`Envelope`] and not to the schema would make every tool advertise a
    /// shape its own results violate.
    #[test]
    fn schema_required_list_matches_envelope_keys() {
        let schema = envelope_output_schema();

        let mut required: Vec<&str> = schema
            .get("required")
            .and_then(Value::as_array)
            .expect("the schema has a required list")
            .iter()
            .map(|entry| entry.as_str().expect("every required entry is a string"))
            .collect();
        required.sort_unstable();

        let mut advertised: Vec<&str> = schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("the schema has a properties object")
            .keys()
            .map(String::as_str)
            .collect();
        advertised.sort_unstable();

        let envelope = serde_json::to_value(Envelope::default()).expect("envelope serializes");
        let mut actual: Vec<&str> = envelope
            .as_object()
            .expect("an envelope is a JSON object")
            .keys()
            .map(String::as_str)
            .collect();
        actual.sort_unstable();

        assert_eq!(actual.len(), 14, "D4 defines exactly 14 envelope blocks");
        assert_eq!(required, actual);
        assert_eq!(advertised, actual);
    }

    /// The `status` property enumerates the four D4 values, in the wire
    /// spelling [`Status`] serializes to.
    #[test]
    fn schema_status_enum_matches_the_status_wire_values() {
        let schema = envelope_output_schema();
        let advertised = schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("status"))
            .cloned()
            .expect("the schema advertises status");

        let wire: Vec<Value> = [Status::Ok, Status::OkBounded, Status::OkPage, Status::Error]
            .iter()
            .map(|status| serde_json::to_value(status).expect("a status serializes"))
            .collect();

        assert_eq!(wire.len(), 4);
        assert_eq!(
            advertised,
            json!({"type": "string", "enum": Value::Array(wire)})
        );
    }

    /// Every data-returning tool takes the same six lowerable budget knobs
    /// (docs/reference/mcp.md#budget-defaults-and-floors). A tool missing
    /// one advertises no way to lower it, so a caller that cannot afford
    /// the default has only the server ceiling to fall back on.
    #[test]
    fn every_data_tool_advertises_the_lowerable_budgets() {
        const BUDGET_KEYS: [&str; 6] = [
            "deadline_ms",
            "max_bytes_scanned",
            "max_response_bytes",
            "max_rows",
            "max_segments",
            "max_store_requests",
        ];
        const DATA_TOOLS: [&str; 4] = [
            "ravel_query_sql",
            "ravel_query_promql",
            "ravel_search_logs",
            "ravel_get_trace",
        ];

        let catalog = tool_catalog();
        for name in DATA_TOOLS {
            let tool = catalog
                .iter()
                .find(|tool| tool.name == name)
                .unwrap_or_else(|| panic!("{name} is in the catalog"));
            let properties = tool
                .input_schema
                .get("properties")
                .and_then(Value::as_object)
                .unwrap_or_else(|| panic!("{name} advertises input properties"));

            let mut present: Vec<&str> = BUDGET_KEYS
                .into_iter()
                .filter(|key| properties.contains_key(*key))
                .collect();
            present.sort_unstable();
            assert_eq!(present, BUDGET_KEYS, "{name} is missing a budget knob");
        }
    }
}
