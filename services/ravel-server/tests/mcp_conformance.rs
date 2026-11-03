//! Protocol conformance of the MCP surface the shipping start path mounts
//! (issue #1384, ADR-1374 decisions 2 and 8): every revision the server
//! advertises completes a full client lifecycle, a revision it does not
//! support is refused, and every read tool's result satisfies the
//! `outputSchema` that `tools/list` declares for it.
//!
//! Already pinned in `mcp_e2e.rs` and not repeated here:
//! - a build without the `mcp` feature serves 404 on `/mcp`:
//!   `feature_off_build_serves_no_mcp_route`;
//! - on `2026-07-28`, an `Mcp-Method` or `Mcp-Name` header that is missing
//!   or disagrees with the body is refused by the header check:
//!   `current_revision_tools_call_requires_mcp_method_and_name`, with
//!   `legacy_initialize_is_accepted_without_mcp_method_header` for the
//!   `initialize` exemption.
//!
//! The agents tools are not covered by the schema test: they are served only
//! with `--features agents` and an agents deployment (`AgentsConfig` and a
//! capability principal), a second fixture this file does not build. They
//! share the envelope schema (`envelope_output_schema` in ravel-mcp's
//! catalog), so the checker below applies to them unchanged.

#![cfg(feature = "mcp")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "support/mcp.rs"]
mod support;

use std::collections::BTreeSet;
use std::sync::Arc;

use ravel_mcp::envelope::{
    Column, Envelope, EvidenceEntry, Failure, FailureClass, NextStep, TimeRange,
};
use rmcp::model::ProtocolVersion;
use serde_json::{Value, json};
use support::*;

/// Every revision `ravel_capabilities` advertises completes its lifecycle
/// (`initialize` on `2025-11-25`, `server/discover` on `2026-07-28`), lists
/// the nine read tools, and answers a `tools/call` with an `ok` envelope over
/// object storage. The revision list is read from the server, not copied, so
/// a revision the server advertises and then refuses fails here.
///
/// FLIP: dropping `ProtocolVersion::V_2025_11_25` from `SUPPORTED_REVISIONS`
/// in `src/mcp/auth.rs` refuses the legacy client's first post-handshake
/// request (it carries `MCP-Protocol-Version: 2025-11-25`), so its
/// `tools/list` fails.
#[tokio::test]
async fn every_advertised_revision_completes_initialize_list_and_call() {
    let (store, metrics) = instrumented_memory();
    seed_fixture(store.as_ref()).await;
    let running = start_with(server_config(), store, metrics).await;

    let client = connect(&running).await;
    let (_, capabilities) = call(&client, "ravel_capabilities", json!({})).await;
    client.cancel().await.expect("client stops");
    let advertised: Vec<ProtocolVersion> = capabilities["data"]["rows"][0][1]["revisions"]
        .as_array()
        .expect("capabilities lists the served revisions")
        .iter()
        .map(|revision| serde_json::from_value(revision.clone()).expect("a revision string"))
        .collect();
    assert_eq!(
        advertised,
        vec![ProtocolVersion::V_2026_07_28, ProtocolVersion::V_2025_11_25]
    );

    let mut served_data = None;
    for revision in &advertised {
        let client = connect_on(&running, revision).await;
        let info = client
            .peer_info()
            .expect("the server answered the lifecycle's first request");
        assert_eq!(&info.protocol_version, revision);

        let tools = client
            .list_all_tools()
            .await
            .unwrap_or_else(|err| panic!("tools/list on {revision:?}: {err}"));
        let names: BTreeSet<String> = tools.iter().map(|tool| tool.name.to_string()).collect();
        let expected: BTreeSet<String> = nine_tool_calls()
            .iter()
            .map(|(name, _)| (*name).to_string())
            .collect();
        assert_eq!(names, expected, "on {revision:?}");

        let (is_error, envelope) =
            call(&client, "ravel_describe_data", json!({"signal": "metrics"})).await;
        assert_eq!(is_error, Some(false), "on {revision:?}: {envelope}");
        assert_eq!(envelope["status"], json!("ok"), "on {revision:?}");
        assert!(
            envelope["data"]["row_count"].as_u64() > Some(0),
            "on {revision:?}: {envelope}"
        );
        // Both revisions read the same tenant through the same tool.
        let data = served_data.get_or_insert_with(|| envelope["data"].clone());
        assert_eq!(&envelope["data"], data, "on {revision:?}");

        client.cancel().await.expect("client stops");
    }

    running.shutdown().await.expect("server stops");
}

/// A request naming a revision the server does not serve is refused by the
/// header check before the protocol layer sees it, on `initialize` (which is
/// otherwise exempt from the header rules) and on a `tools/call` whose
/// method and name headers are correct. Each refusal sits beside the same
/// request on `2026-07-28` answering 200, so it is the revision alone that is
/// refused. The refused requests touch no object storage.
///
/// FLIP: deleting the `SUPPORTED_REVISIONS` check in `check_standard_headers`
/// (`src/mcp/auth.rs`) hands `2025-06-18` to the `!= 2026-07-28` early
/// return, and the request reaches rmcp, which does not answer the
/// `errorType`-tagged refusal.
#[tokio::test]
async fn an_unsupported_revision_header_is_refused() {
    let (store, metrics) = instrumented_memory();
    let running = start_with(server_config(), store, Arc::clone(&metrics)).await;
    let call_headers = |revision: &'static str| {
        [
            ("mcp-protocol-version", revision),
            ("mcp-method", "tools/call"),
            ("mcp-name", "ravel_capabilities"),
        ]
    };

    let served = post_raw(
        &running,
        &call_headers("2026-07-28"),
        tools_call_body("2026-07-28").to_string(),
    )
    .await;
    assert_eq!(served.status(), reqwest::StatusCode::OK);
    let served = post_raw(
        &running,
        &[("mcp-protocol-version", "2026-07-28")],
        initialize_body("2026-07-28").to_string(),
    )
    .await;
    assert_eq!(served.status(), reqwest::StatusCode::OK);

    let store_calls_before = metrics.snapshot();
    for unsupported in ["2025-06-18", "2024-11-05", "1999-01-01"] {
        assert_refused_by_the_header_check(
            post_raw(
                &running,
                &call_headers(unsupported),
                tools_call_body(unsupported).to_string(),
            )
            .await,
        )
        .await;
        assert_refused_by_the_header_check(
            post_raw(
                &running,
                &[("mcp-protocol-version", unsupported)],
                initialize_body(unsupported).to_string(),
            )
            .await,
        )
        .await;
    }
    let after = metrics.snapshot();
    assert_eq!(after.get.calls, store_calls_before.get.calls);
    assert_eq!(after.list.calls, store_calls_before.list.calls);
    assert_eq!(after.put.calls, store_calls_before.put.calls);

    running.shutdown().await.expect("server stops");
}

/// Every read tool's fixture result satisfies the `outputSchema` its
/// `tools/list` entry declares, read from the server rather than copied, and
/// has the shape of `ravel_mcp::envelope::Envelope`. Every tool `tools/list`
/// names has a fixture call, so a tenth tool fails here until it has one.
/// `ravel_get_trace` is checked for envelope shape and status only; its
/// columns are not pinned.
///
/// FLIP: removing one block from the result in this test's own pipeline
/// (`envelope.as_object_mut().remove("coverage")` before the two checks)
/// fails both; `the_checks_reject_a_result_missing_any_required_block` keeps
/// that wrong implementation as a standing test, one block at a time.
#[tokio::test]
async fn every_tool_result_satisfies_its_declared_output_schema() {
    let (store, metrics) = instrumented_memory();
    seed_fixture(store.as_ref()).await;
    let running = start_with(server_config(), store, metrics).await;
    let client = connect(&running).await;

    let tools = client.list_all_tools().await.expect("tools/list answers");
    let calls = nine_tool_calls();
    let called: BTreeSet<&str> = calls.iter().map(|(name, _)| *name).collect();
    let listed: BTreeSet<&str> = tools.iter().map(|tool| tool.name.as_ref()).collect();
    assert_eq!(listed, called, "every listed tool has a fixture call");

    for (name, arguments) in calls {
        let tool = tools
            .iter()
            .find(|tool| tool.name == name)
            .expect("the tool is listed");
        let schema = Value::Object(
            tool.output_schema
                .as_deref()
                .cloned()
                .unwrap_or_else(|| panic!("{name} declares an outputSchema")),
        );
        let (is_error, envelope) = call(&client, name, arguments).await;
        assert_eq!(is_error, Some(false), "{name}: {envelope}");
        assert_eq!(envelope["status"], json!("ok"), "{name}: {envelope}");

        let errors = schema_errors(&schema, &envelope, name);
        assert!(
            errors.is_empty(),
            "{name} against its outputSchema: {errors:#?}"
        );
        let errors = envelope_shape_errors(&envelope);
        assert!(errors.is_empty(), "{name} against Envelope: {errors:#?}");
    }

    client.cancel().await.expect("client stops");
    running.shutdown().await.expect("server stops");
}

/// The two checks above fail against a result missing any one required
/// block, and against a `status` outside its enum, so a pass from them is
/// evidence: the wrong implementation is a real `ravel_describe_data` result
/// with one block removed, each block in turn.
///
/// FLIP: a `schema_errors` that skips the `required` keyword passes every
/// removal, and so does an `envelope_shape_errors` that compares only the
/// keys present in the result.
#[tokio::test]
async fn the_checks_reject_a_result_missing_any_required_block() {
    let (store, metrics) = instrumented_memory();
    seed_fixture(store.as_ref()).await;
    let running = start_with(server_config(), store, metrics).await;
    let client = connect(&running).await;

    let tools = client.list_all_tools().await.expect("tools/list answers");
    let tool = tools
        .iter()
        .find(|tool| tool.name == "ravel_describe_data")
        .expect("listed");
    let schema = Value::Object(tool.output_schema.as_deref().cloned().expect("schema"));
    let (_, envelope) = call(&client, "ravel_describe_data", json!({"signal": "metrics"})).await;
    assert!(schema_errors(&schema, &envelope, "describe").is_empty());
    assert!(envelope_shape_errors(&envelope).is_empty());

    let required: Vec<String> = schema["required"]
        .as_array()
        .expect("the schema lists its required blocks")
        .iter()
        .map(|block| block.as_str().expect("a block name").to_string())
        .collect();
    assert_eq!(required.len(), 14, "{required:?}");
    for block in &required {
        let mut wrong = envelope.clone();
        wrong
            .as_object_mut()
            .expect("an object")
            .remove(block)
            .expect("the block is present in the real result");
        assert_eq!(
            schema_errors(&schema, &wrong, "describe"),
            vec![format!("describe: required property {block:?} is missing")],
        );
        assert!(
            !envelope_shape_errors(&wrong).is_empty(),
            "the Envelope shape check accepts a result without {block:?}"
        );
    }

    let mut wrong = envelope.clone();
    wrong["status"] = json!("partial");
    assert_eq!(schema_errors(&schema, &wrong, "describe").len(), 1);
    assert!(!envelope_shape_errors(&wrong).is_empty());

    client.cancel().await.expect("client stops");
    running.shutdown().await.expect("server stops");
}

/// The JSON Schema subset the envelope's `outputSchema` uses: `type`,
/// `properties`, `required` and `enum`, plus `description`, which constrains
/// nothing. Any other keyword is reported rather than ignored, so a schema
/// that grows past this subset fails here instead of passing unchecked.
fn schema_errors(schema: &Value, value: &Value, at: &str) -> Vec<String> {
    let Some(schema) = schema.as_object() else {
        return vec![format!("{at}: the schema is not an object")];
    };
    let mut errors = Vec::new();
    for keyword in schema.keys() {
        if !matches!(
            keyword.as_str(),
            "type" | "properties" | "required" | "enum" | "description"
        ) {
            errors.push(format!(
                "{at}: keyword {keyword:?} is outside the checked subset"
            ));
        }
    }
    if let Some(expected) = schema.get("type") {
        let expected = expected.as_str().unwrap_or("<not a string>");
        let matches = match expected {
            "object" => value.is_object(),
            "array" => value.is_array(),
            "string" => value.is_string(),
            "boolean" => value.is_boolean(),
            "number" => value.is_number(),
            "integer" => value.is_i64() || value.is_u64(),
            "null" => value.is_null(),
            _ => false,
        };
        if !matches {
            errors.push(format!("{at}: expected type {expected}, found {value}"));
            return errors;
        }
    }
    if let Some(allowed) = schema.get("enum") {
        let allowed = allowed.as_array().map(Vec::as_slice).unwrap_or_default();
        if !allowed.contains(value) {
            errors.push(format!("{at}: {value} is not one of {allowed:?}"));
        }
    }
    let object = value.as_object();
    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        for name in required.iter().filter_map(Value::as_str) {
            if !object.is_some_and(|object| object.contains_key(name)) {
                errors.push(format!("{at}: required property {name:?} is missing"));
            }
        }
    }
    if let (Some(properties), Some(object)) =
        (schema.get("properties").and_then(Value::as_object), object)
    {
        for (name, property) in properties {
            if let Some(field) = object.get(name) {
                errors.extend(schema_errors(property, field, &format!("{at}.{name}")));
            }
        }
    }
    errors
}

/// Where `ravel_mcp::envelope::Envelope` cannot be deserialized into (it
/// derives `Serialize` only), the result is compared against the type's own
/// serialization instead: every struct carries exactly the keys
/// `Envelope::default()` serializes to, every scalar has the JSON type of the
/// default's, and the fields the default leaves null or empty are compared
/// against their own types' serializations.
fn envelope_shape_errors(result: &Value) -> Vec<String> {
    let template = serde_json::to_value(Envelope::default()).expect("Envelope serializes");
    let mut errors = Vec::new();
    shape_errors(&template, result, "", &mut errors);
    errors
}

fn shape_errors(template: &Value, value: &Value, at: &str, errors: &mut Vec<String>) {
    match template {
        Value::Object(template) => {
            let Some(object) = value.as_object() else {
                errors.push(format!("{at}: expected an object, found {value}"));
                return;
            };
            let expected: BTreeSet<&String> = template.keys().collect();
            let found: BTreeSet<&String> = object.keys().collect();
            if expected != found {
                errors.push(format!("{at}: keys {found:?}, expected {expected:?}"));
            }
            for (name, field_template) in template {
                if let Some(field) = object.get(name) {
                    shape_errors(field_template, field, &format!("{at}.{name}"), errors);
                }
            }
        }
        Value::Array(_) => {
            let Some(items) = value.as_array() else {
                errors.push(format!("{at}: expected an array, found {value}"));
                return;
            };
            // A row's cells take each column's type, which the envelope
            // type does not fix, so a row is checked only for being an array.
            if at == ".data.rows" {
                for (index, row) in items.iter().enumerate() {
                    if !row.is_array() {
                        errors.push(format!("{at}[{index}]: a row is not an array"));
                    }
                }
                return;
            }
            match element_template(at) {
                Some(element) => {
                    for (index, item) in items.iter().enumerate() {
                        shape_errors(&element, item, &format!("{at}[{index}]"), errors);
                    }
                }
                None => errors.push(format!("{at}: no element shape known for this array")),
            }
        }
        Value::Null => {
            if !value.is_null() {
                match nullable_template(at) {
                    Some(Value::Null) => {}
                    Some(inner) => shape_errors(&inner, value, at, errors),
                    None => errors.push(format!("{at}: expected null, found {value}")),
                }
            }
        }
        Value::String(_) if !value.is_string() => {
            errors.push(format!("{at}: expected a string, found {value}"));
        }
        Value::Bool(_) if !value.is_boolean() => {
            errors.push(format!("{at}: expected a boolean, found {value}"));
        }
        Value::Number(_) if !value.is_u64() => {
            errors.push(format!("{at}: expected an unsigned integer, found {value}"));
        }
        _ => {}
    }
    if at == ".status" && !STATUSES.contains(&value.as_str().unwrap_or_default()) {
        errors.push(format!("{at}: {value} is not a Status"));
    }
    if at == ".failure.class" {
        let classes: Vec<Value> = FAILURE_CLASSES
            .iter()
            .map(|class| serde_json::to_value(class).expect("FailureClass serializes"))
            .collect();
        if !classes.contains(value) {
            errors.push(format!("{at}: {value} is not a FailureClass"));
        }
    }
}

const STATUSES: [&str; 4] = ["ok", "ok_bounded", "ok_page", "error"];

const FAILURE_CLASSES: [FailureClass; 13] = [
    FailureClass::Unauthorized,
    FailureClass::InvalidArgument,
    FailureClass::MissingArgument,
    FailureClass::Validation,
    FailureClass::Unsupported,
    FailureClass::BudgetEstimateExceedsCeiling,
    FailureClass::BudgetExceeded,
    FailureClass::Deadline,
    FailureClass::Unavailable,
    FailureClass::SnapshotInvalidated,
    FailureClass::CursorExpired,
    FailureClass::CursorInvalid,
    FailureClass::Internal,
];

/// The serialized shape of one element of the array at `at`.
fn element_template(at: &str) -> Option<Value> {
    let shape = match at {
        ".data.columns" => serde_json::to_value(Column::default()),
        ".evidence" => serde_json::to_value(EvidenceEntry::default()),
        ".next_steps" => serde_json::to_value(NextStep::default()),
        ".scope.predicates_applied"
        | ".scope.order_by"
        | ".visibility.min_commit_tokens_applied"
        | ".coverage.fragments"
        | ".coverage.unindexed_predicates"
        | ".warnings" => Ok(json!("")),
        _ => return None,
    };
    Some(shape.expect("the element type serializes"))
}

/// The shape a field the default serializes as null takes when it is set:
/// `Some(Value::Null)` admits any value.
fn nullable_template(at: &str) -> Option<Value> {
    let shape = match at {
        ".failure" => serde_json::to_value(Failure {
            class: FailureClass::Internal,
            message: String::new(),
            counter: None,
        }),
        ".scope.time_range" => serde_json::to_value(TimeRange::default()),
        ".plan" | ".accuracy.approximation" | ".presentation.cursor" | ".failure.counter" => {
            Ok(json!(""))
        }
        ".budget.effective" | ".budget.actual" | ".budget.estimate" => Ok(Value::Null),
        _ => return None,
    };
    Some(shape.expect("the field type serializes"))
}
