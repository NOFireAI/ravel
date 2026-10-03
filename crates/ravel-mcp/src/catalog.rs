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
    /// Bounds the whole serialized envelope; default 512 KiB, floored at
    /// 256 KiB. A label list is bounded by its own page, but the page is
    /// still what a caller may need to make smaller.
    pub max_response_bytes: Option<u64>,
    pub deadline_ms: Option<u64>,
    pub evidence_ref: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ExplainQueryInput {
    pub query: String,
    pub time_range: TimeRange,
    /// Bounds the whole serialized envelope; default 512 KiB, floored at
    /// 256 KiB. An explain returns a plan and an effective schema, both of
    /// which grow with the statement.
    pub max_response_bytes: Option<u64>,
    pub deadline_ms: Option<u64>,
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
///
/// `ravel_find_labels`, `ravel_explain_query`, and `ravel_analyze_timeseries`
/// return no caller-sized row set, so they take the two knobs that still
/// apply to them: `max_response_bytes` and `deadline_ms`.
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
    /// Absent means no consent: a partial-coverage result is refused rather
    /// than the whole call failing to deserialize.
    #[serde(default)]
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
    /// Bounds the whole serialized envelope; default 512 KiB, floored at
    /// 256 KiB. An analysis runs a PromQL range evaluation underneath, so it
    /// takes the same deadline and response bound the evaluation would.
    pub max_response_bytes: Option<u64>,
    pub deadline_ms: Option<u64>,
    pub evidence_ref: Option<String>,
}

fn read_only_annotations() -> ToolAnnotations {
    ToolAnnotations::new()
        .read_only(true)
        .destructive(false)
        .open_world(false)
}

/// The 14 D4 envelope blocks, in the order [`crate::envelope::Envelope`]
/// serializes them. Both halves of the output schema are built from this one
/// list, so a block cannot appear in `properties` and be missing from
/// `required`.
const ENVELOPE_BLOCKS: [&str; 14] = [
    "status",
    "failure",
    "data",
    "plan",
    "scope",
    "ids",
    "visibility",
    "coverage",
    "accuracy",
    "presentation",
    "budget",
    "evidence",
    "warnings",
    "next_steps",
];

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
    let mut properties = JsonObject::new();
    for block in ENVELOPE_BLOCKS {
        let shape = if block == "status" {
            json!({"type": "string", "enum": ["ok", "ok_bounded", "ok_page", "error"]})
        } else {
            json!({})
        };
        properties.insert(block.to_string(), shape);
    }
    let required: Vec<Value> = ENVELOPE_BLOCKS
        .iter()
        .map(|block| Value::String((*block).to_string()))
        .collect();

    let mut schema = JsonObject::new();
    schema.insert("type".to_string(), Value::String("object".to_string()));
    schema.insert(
        "description".to_string(),
        Value::String(
            "The D4 result envelope. See docs/reference/mcp.md#the-envelope.".to_string(),
        ),
    );
    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert("required".to_string(), Value::Array(required));
    Arc::new(schema)
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
    const TOOLS_LIST_SERIALIZED_LEN: usize = 15_378;

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

    /// Every row-returning tool takes the same six lowerable budget knobs
    /// (docs/reference/mcp.md#budget-defaults-and-floors), and every other
    /// tool that spends a budget takes the two that apply to it. A tool
    /// missing one advertises no way to lower it, so a caller that cannot
    /// afford the default has only the server ceiling to fall back on.
    ///
    /// `ravel_capabilities` and `ravel_describe_data` are the two tools with
    /// no budget knob at all: the first reads nothing from the store, and
    /// the second answers from the metadata cache and one resolve per
    /// signal.
    #[test]
    fn every_data_tool_advertises_the_lowerable_budgets() {
        const ROW_BUDGET_KEYS: [&str; 6] = [
            "deadline_ms",
            "max_bytes_scanned",
            "max_response_bytes",
            "max_rows",
            "max_segments",
            "max_store_requests",
        ];
        /// The subset that applies to a tool returning no caller-sized row
        /// set: it still occupies a response and still spends wall clock.
        const SHARED_BUDGET_KEYS: [&str; 2] = ["deadline_ms", "max_response_bytes"];

        let expected: [(&str, &[&str]); 7] = [
            ("ravel_query_sql", &ROW_BUDGET_KEYS),
            ("ravel_query_promql", &ROW_BUDGET_KEYS),
            ("ravel_search_logs", &ROW_BUDGET_KEYS),
            ("ravel_get_trace", &ROW_BUDGET_KEYS),
            ("ravel_find_labels", &SHARED_BUDGET_KEYS),
            ("ravel_explain_query", &SHARED_BUDGET_KEYS),
            ("ravel_analyze_timeseries", &SHARED_BUDGET_KEYS),
        ];

        let catalog = tool_catalog();
        for (name, keys) in expected {
            let tool = catalog
                .iter()
                .find(|tool| tool.name == name)
                .unwrap_or_else(|| panic!("{name} is in the catalog"));
            let properties = tool
                .input_schema
                .get("properties")
                .and_then(Value::as_object)
                .unwrap_or_else(|| panic!("{name} advertises input properties"));

            let mut present: Vec<&str> = ROW_BUDGET_KEYS
                .into_iter()
                .filter(|key| properties.contains_key(*key))
                .collect();
            present.sort_unstable();
            assert_eq!(present, keys, "{name} advertises the wrong budget knobs");
        }

        for name in ["ravel_capabilities", "ravel_describe_data"] {
            let tool = catalog
                .iter()
                .find(|tool| tool.name == name)
                .unwrap_or_else(|| panic!("{name} is in the catalog"));
            let properties = tool
                .input_schema
                .get("properties")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            let present: Vec<&str> = ROW_BUDGET_KEYS
                .into_iter()
                .filter(|key| properties.contains_key(*key))
                .collect();
            assert!(present.is_empty(), "{name} spends no budget");
        }
    }

    /// D2's three flat properties of the whole catalog, asserted for all
    /// nine tools at once: the `readOnlyHint: true`, `destructiveHint:
    /// false`, `openWorldHint: false` annotations, the `ravel_` name prefix,
    /// and the order the reference doc lists the tools in (a client renders
    /// `tools/list` in the order it arrives, so the order is part of what is
    /// advertised).
    ///
    /// `idempotent_hint` stays unset on purpose. ADR-1374 D2 names three
    /// annotations, and rmcp documents that hint as meaningful only when
    /// `readOnlyHint == false`, so setting it here would advertise something
    /// the ADR does not.
    #[test]
    fn every_tool_is_read_only_idempotent_and_ravel_prefixed_in_catalog_order() {
        const CATALOG_ORDER: [&str; 9] = [
            "ravel_capabilities",
            "ravel_describe_data",
            "ravel_find_labels",
            "ravel_explain_query",
            "ravel_query_sql",
            "ravel_query_promql",
            "ravel_search_logs",
            "ravel_get_trace",
            "ravel_analyze_timeseries",
        ];

        let catalog = tool_catalog();
        let names: Vec<&str> = catalog.iter().map(|tool| tool.name.as_ref()).collect();
        assert_eq!(names, CATALOG_ORDER);

        for tool in &catalog {
            assert!(
                tool.name.starts_with("ravel_"),
                "{} is missing the D2 prefix",
                tool.name
            );
            let annotations = tool
                .annotations
                .as_ref()
                .unwrap_or_else(|| panic!("{} carries annotations", tool.name));
            assert_eq!(annotations.read_only_hint, Some(true), "{}", tool.name);
            assert_eq!(annotations.destructive_hint, Some(false), "{}", tool.name);
            assert_eq!(annotations.open_world_hint, Some(false), "{}", tool.name);
            assert_eq!(annotations.idempotent_hint, None, "{}", tool.name);
        }
    }

    /// `allow_partial_coverage` absent means no consent, not a
    /// deserialization failure: a caller that never asked for a partial
    /// result should not have to say so to make a whole-coverage call.
    #[test]
    fn absent_allow_partial_coverage_deserializes_as_no_consent() {
        let input: QueryPromqlInput = serde_json::from_value(
            json!({"query": "up", "evaluation_time": "2026-09-08T00:00:00Z"}),
        )
        .expect("a call without the consent flag deserializes");
        assert!(!input.allow_partial_coverage);

        let schema = tool_catalog()
            .into_iter()
            .find(|tool| tool.name == "ravel_query_promql")
            .expect("ravel_query_promql is in the catalog")
            .input_schema
            .get("required")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        assert_eq!(schema, vec![Value::String("query".to_string())]);
    }
}
