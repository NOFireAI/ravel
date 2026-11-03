//! Drift check for the tool catalog in `docs/reference/mcp.md`. Two blocks are
//! rendered from what `tools/list` advertises, one for the nine query tools and
//! one for the six Ravel Agents tools, and compared to the committed page;
//! `RAVEL_UPDATE_MCP_REFERENCE=1` rewrites the page instead of asserting.
//!
//! Both catalogs are compiled into every build of this crate, so both blocks
//! are checked in every build; whether a server serves the agents tools is a
//! build and run-time choice the page states in prose.
#![allow(clippy::expect_used)]

use std::env;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use ravel_mcp::catalog::{agents_tool_catalog, tool_catalog};
use ravel_mcp::envelope::{Envelope, EvidenceEntry, NextStep};
use rmcp::model::{JsonObject, Tool};
use serde_json::Value;

/// Environment variable that switches the check into a rewrite.
const UPDATE_ENV: &str = "RAVEL_UPDATE_MCP_REFERENCE";

/// The exact command a failing run prints so the fix is copy-pasteable.
const REGEN: &str = "RAVEL_UPDATE_MCP_REFERENCE=1 cargo test -p ravel-mcp --test mcp_reference";

const QUERY_BEGIN: &str = "<!-- BEGIN GENERATED MCP TOOLS -->";
const QUERY_END: &str = "<!-- END GENERATED MCP TOOLS -->";
const AGENTS_BEGIN: &str = "<!-- BEGIN GENERATED MCP AGENTS TOOLS -->";
const AGENTS_END: &str = "<!-- END GENERATED MCP AGENTS TOOLS -->";

/// `docs/reference/mcp.md`, resolved from this crate's manifest directory
/// rather than the process working directory.
fn reference_doc() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("docs")
        .join("reference")
        .join("mcp.md")
}

/// Join a schema description's wrapped lines and escape it for a table cell.
fn cell_text(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace('|', "\\|")
}

/// The `$defs` entry a `$ref` names.
fn resolve<'a>(reference: &str, root: &'a JsonObject) -> &'a JsonObject {
    let name = reference
        .strip_prefix("#/$defs/")
        .unwrap_or_else(|| panic!("unexpected $ref {reference}"));
    root.get("$defs")
        .and_then(|defs| defs.get(name))
        .and_then(Value::as_object)
        .unwrap_or_else(|| panic!("$ref {reference} does not resolve"))
}

/// The JSON type of one schema, in words. Every shape the catalog uses is
/// handled; any other shape fails the test rather than rendering as blank.
fn json_type(schema: &JsonObject, root: &JsonObject) -> String {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        return json_type(resolve(reference, root), root);
    }
    if let Some(choices) = schema.get("anyOf").and_then(Value::as_array) {
        let alternatives: Vec<String> = choices
            .iter()
            .map(|choice| choice.as_object().expect("an anyOf entry is a schema"))
            .filter(|choice| choice.get("type").and_then(Value::as_str) != Some("null"))
            .map(|choice| json_type(choice, root))
            .collect();
        return alternatives.join(" or ");
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        let mut names: Vec<String> = values
            .iter()
            .map(|value| format!("`{}`", value.as_str().expect("an enum value is a string")))
            .collect();
        let last = names.pop().expect("an enum lists at least one value");
        let listed = if names.is_empty() {
            last
        } else {
            format!("{} or {last}", names.join(", "))
        };
        return format!("string ({listed})");
    }
    let types: Vec<&str> = match schema.get("type") {
        None => {
            assert!(
                schema.keys().all(|key| key == "description" || key == "default"),
                "unrendered schema shape {schema:?}"
            );
            return "any JSON".to_string();
        }
        Some(Value::String(one)) => vec![one.as_str()],
        Some(Value::Array(many)) => many
            .iter()
            .map(|entry| entry.as_str().expect("a type entry is a string"))
            .filter(|entry| *entry != "null")
            .collect(),
        Some(other) => panic!("unexpected type {other}"),
    };
    let [kind] = types.as_slice() else {
        panic!("unexpected type list {types:?}");
    };
    match *kind {
        "array" => {
            let items = schema
                .get("items")
                .and_then(Value::as_object)
                .expect("an array schema names its items");
            format!("array of {}", json_type(items, root))
        }
        "object" => {
            let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
                return "object".to_string();
            };
            let required = required_names(schema);
            let mut names: Vec<&String> = properties.keys().collect();
            names.sort();
            let fields: Vec<String> = names
                .into_iter()
                .map(|name| {
                    let field = properties
                        .get(name)
                        .and_then(Value::as_object)
                        .expect("a property is a schema");
                    let mark = if required.contains(name) { "" } else { "?" };
                    format!("`{name}`{mark}: {}", json_type(field, root))
                })
                .collect();
            format!("object {{{}}}", fields.join(", "))
        }
        other => other.to_string(),
    }
}

fn required_names(schema: &JsonObject) -> Vec<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .map(|names| {
            names
                .iter()
                .map(|name| name.as_str().expect("a required name is a string").to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// The annotations a tool sets, in a fixed order; an unset hint is omitted.
fn annotations(tool: &Tool) -> String {
    let annotations = tool
        .annotations
        .as_ref()
        .unwrap_or_else(|| panic!("{} carries annotations", tool.name));
    [
        ("readOnlyHint", annotations.read_only_hint),
        ("destructiveHint", annotations.destructive_hint),
        ("idempotentHint", annotations.idempotent_hint),
        ("openWorldHint", annotations.open_world_hint),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.map(|value| format!("`{name}: {value}`")))
    .collect::<Vec<_>>()
    .join(", ")
}

/// One subsection per tool, in catalog order (the order `tools/list`
/// advertises). Arguments are listed required first, then optional, each
/// group sorted by name, so the block does not depend on how the schema
/// map orders its keys.
fn render_block(tools: &[Tool]) -> String {
    let mut out = String::new();
    for tool in tools {
        let title = tool
            .title
            .as_deref()
            .unwrap_or_else(|| panic!("{} has a title", tool.name));
        let description = tool
            .description
            .as_deref()
            .unwrap_or_else(|| panic!("{} has a description", tool.name));
        let _ = writeln!(out, "#### `{}`\n", tool.name);
        let _ = writeln!(out, "- Title: {}", cell_text(title));
        let _ = writeln!(out, "- Description: {}", cell_text(description));
        let _ = writeln!(out, "- Annotations: {}\n", annotations(tool));

        let schema = tool.input_schema.as_ref();
        let properties = schema
            .get("properties")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        if properties.is_empty() {
            out.push_str("No arguments.\n\n");
            continue;
        }
        let required = required_names(schema);
        let mut names: Vec<&String> = properties.keys().collect();
        names.sort_by_key(|name| (!required.contains(name), name.as_str()));

        out.push_str("| Argument | Type | Required | Description |\n");
        out.push_str("| --- | --- | --- | --- |\n");
        for name in names {
            let property = properties
                .get(name)
                .and_then(Value::as_object)
                .expect("a property is a schema");
            let description = property
                .get("description")
                .and_then(Value::as_str)
                .map(cell_text)
                .unwrap_or_default();
            let _ = writeln!(
                out,
                "| `{name}` | {} | {} | {description} |",
                cell_text(&json_type(property, schema)),
                if required.contains(name) { "yes" } else { "no" },
            );
        }
        out.push('\n');
    }
    out
}

/// Replace the content between `begin` and `end` in `doc` with `block`,
/// leaving everything outside the markers untouched. `None` when either
/// marker is absent.
fn splice(doc: &str, begin: &str, end: &str, block: &str) -> Option<String> {
    let start = doc.find(begin)?;
    let after_begin = start + begin.len();
    let stop = doc[after_begin..].find(end)? + after_begin;

    let mut out = String::with_capacity(doc.len() + block.len());
    out.push_str(&doc[..after_begin]);
    out.push('\n');
    out.push_str(block);
    out.push_str(&doc[stop..]);
    Some(out)
}

/// The rendered blocks name every tool each catalog serves, once each, so a
/// renderer that skipped a tool fails here rather than in a doc diff.
#[test]
fn blocks_have_one_subsection_per_tool() {
    for tools in [tool_catalog(), agents_tool_catalog()] {
        let block = render_block(&tools);
        let headings = block.lines().filter(|line| line.starts_with("#### ")).count();
        assert_eq!(headings, tools.len());
        for tool in &tools {
            let heading = format!("#### `{}`\n", tool.name);
            assert_eq!(block.matches(&heading).count(), 1, "{}", tool.name);
        }
    }
}

/// A required argument and an optional one render as such, with the type
/// their schema gives, nested objects and enums included.
#[test]
fn argument_rows_carry_type_and_requiredness() {
    let block = render_block(&tool_catalog());
    for row in [
        "| `query` | string | yes |",
        "| `time_range` | object {`end_ns`: string, `start_ns`: string} | yes |",
        "| `op` | string (`change_point` or `summary`) | yes |",
        "| `predicates` | array of object {`key`: string, `op`: string (`eq`, `ne`, `in` or \
         `exists`), `value`?: string or array of string} | no |",
        "| `max_rows` | integer | no |",
        "| `allow_partial_coverage` | boolean | no |",
    ] {
        assert!(block.contains(row), "missing {row}");
    }
}

/// Every JSON object key in `text`, in the order it appears.
fn keys_in_order(text: &str) -> Vec<&str> {
    let mut keys = Vec::new();
    let mut rest = text;
    while let Some(open) = rest.find('"') {
        let after = &rest[open + 1..];
        let Some(close) = after.find('"') else { break };
        let token = &after[..close];
        rest = &after[close + 1..];
        if rest.trim_start().starts_with(':') {
            keys.push(token);
        }
    }
    keys
}

/// The envelope literal under "The envelope" names every field
/// [`Envelope`] serializes, nested blocks included, in serialization order.
/// One evidence entry and one next step are filled in, as the literal shows
/// one of each.
#[test]
fn envelope_literal_lists_every_field_in_order() {
    let path = reference_doc();
    let doc = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    let section = doc
        .split_once("## The envelope")
        .map(|(_, after)| after)
        .expect("the page has an envelope section");
    let literal = section
        .split_once("```json\n")
        .and_then(|(_, after)| after.split_once("\n```"))
        .map(|(body, _)| body)
        .expect("the envelope section opens with a json literal");

    let envelope = Envelope {
        evidence: vec![EvidenceEntry::default()],
        next_steps: vec![NextStep::default()],
        ..Envelope::default()
    };
    let serialized = serde_json::to_string(&envelope).expect("an envelope serializes");
    assert_eq!(keys_in_order(literal), keys_in_order(&serialized));
}

/// The committed reference matches what the current catalogs render. A tool,
/// argument, description or annotation changed without regenerating fails
/// here.
#[test]
fn mcp_reference_is_current() {
    let path = reference_doc();
    let doc = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));

    let mut updated = doc.clone();
    for (begin, end, tools) in [
        (QUERY_BEGIN, QUERY_END, tool_catalog()),
        (AGENTS_BEGIN, AGENTS_END, agents_tool_catalog()),
    ] {
        updated = splice(&updated, begin, end, &render_block(&tools)).unwrap_or_else(|| {
            panic!("{} is missing the markers {begin} .. {end}", path.display())
        });
    }

    if env::var(UPDATE_ENV).as_deref() == Ok("1") {
        if updated != doc {
            std::fs::write(&path, &updated)
                .unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
            eprintln!("rewrote {}", path.display());
        }
        return;
    }

    assert_eq!(
        updated, doc,
        "docs/reference/mcp.md does not match the tool catalog. Regenerate it with:\n  {REGEN}"
    );
}
