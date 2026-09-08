//! Compact text rendering of an [`Envelope`] (ADR-1374 D2).
//!
//! Every tool result carries an MCP `content` text block alongside the
//! `structuredContent` envelope, and D2 says the text block "is a compact
//! rendering of the same data, not a second copy of the rows". D4 adds
//! that it "is rendered from the truncated `data`, so the two
//! representations never differ" -- callers must render from the envelope
//! returned by [`crate::envelope::Envelope::fit`], never from data captured
//! before it ran.
//!
//! Neither section specifies the text's exact syntax, only its two
//! properties: it must not duplicate the row cost of `structuredContent`
//! (so it cannot just be the JSON re-printed), and for `ravel_search_logs`
//! it must "group rows by attribute set and hoist shared attributes once"
//! (D2). [`render`] picks one concrete syntax meeting both: a header line,
//! a tab-separated table of `data.columns`/`data.rows`, and -- per column,
//! whenever every row's cell there is a [`Cell::Map`] -- a `# shared`
//! line carrying the key/value pairs identical across every row, with each
//! row's own cell then printing only the keys that differ from that
//! shared set.

use serde_json::{Map, Value};

use crate::envelope::{Cell, Column, Data, Envelope, FailureClass, Row, Status};

fn status_label(status: Status) -> &'static str {
    match status {
        Status::Ok => "ok",
        Status::OkBounded => "ok_bounded",
        Status::OkPage => "ok_page",
        Status::Error => "error",
    }
}

fn failure_class_label(class: FailureClass) -> &'static str {
    match class {
        FailureClass::Unauthorized => "unauthorized",
        FailureClass::InvalidArgument => "invalid_argument",
        FailureClass::MissingArgument => "missing_argument",
        FailureClass::Validation => "validation",
        FailureClass::Unsupported => "unsupported",
        FailureClass::BudgetEstimateExceedsCeiling => "budget_estimate_exceeds_ceiling",
        FailureClass::BudgetExceeded => "budget_exceeded",
        FailureClass::Deadline => "deadline",
        FailureClass::Unavailable => "unavailable",
        FailureClass::SnapshotInvalidated => "snapshot_invalidated",
        FailureClass::CursorExpired => "cursor_expired",
        FailureClass::CursorInvalid => "cursor_invalid",
        FailureClass::Internal => "internal",
    }
}

fn cell_display(cell: &Cell) -> String {
    match cell {
        Cell::Null => "null".to_string(),
        Cell::Bool(b) => b.to_string(),
        Cell::Int(n) | Cell::Timestamp(n) => n.to_string(),
        Cell::Float(f) if f.is_nan() => "NaN".to_string(),
        Cell::Float(f) if *f == f64::INFINITY => "+Inf".to_string(),
        Cell::Float(f) if *f == f64::NEG_INFINITY => "-Inf".to_string(),
        Cell::Float(f) => f.to_string(),
        Cell::HexId(s) | Cell::Str(s) => s.clone(),
        Cell::Map(m) => serde_json::to_string(&Value::Object(m.clone())).unwrap_or_default(),
    }
}

/// The key/value pairs identical, by JSON equality, across every row's
/// [`Cell::Map`] at `col_idx`. `None` when there are fewer than two rows,
/// any row's cell there is not a `Map`, or no pair is shared by all of
/// them -- hoisting one column out of many is still useful even when
/// another column has nothing in common.
fn shared_map_keys(rows: &[Row], col_idx: usize) -> Option<Map<String, Value>> {
    if rows.len() < 2 {
        return None;
    }
    let mut rows_iter = rows.iter();
    let mut shared = match rows_iter.next()?.get(col_idx)? {
        Cell::Map(m) => m.clone(),
        _ => return None,
    };
    for row in rows_iter {
        let Some(Cell::Map(m)) = row.get(col_idx) else {
            return None;
        };
        shared.retain(|k, v| m.get(k) == Some(v));
        if shared.is_empty() {
            return None;
        }
    }
    Some(shared)
}

fn render_data_table(data: &Data, out: &mut String) {
    if data.columns.is_empty() {
        return;
    }
    let header: Vec<&str> = data.columns.iter().map(|c: &Column| c.name.as_str()).collect();
    out.push_str(&header.join("\t"));
    out.push('\n');

    let shared: Vec<Option<Map<String, Value>>> = (0..data.columns.len())
        .map(|i| shared_map_keys(&data.rows, i))
        .collect();
    for (i, keys) in shared.iter().enumerate() {
        if let Some(map) = keys {
            out.push_str(&format!(
                "# shared {}: {}\n",
                data.columns[i].name,
                serde_json::to_string(&Value::Object(map.clone())).unwrap_or_default()
            ));
        }
    }

    for row in &data.rows {
        let cells: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, cell)| match (shared.get(i).and_then(Option::as_ref), cell) {
                (Some(shared_map), Cell::Map(m)) => {
                    let diff: Map<String, Value> = m
                        .iter()
                        .filter(|(k, v)| shared_map.get(k.as_str()) != Some(*v))
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    serde_json::to_string(&Value::Object(diff)).unwrap_or_default()
                }
                _ => cell_display(cell),
            })
            .collect();
        out.push_str(&cells.join("\t"));
        out.push('\n');
    }

    let truncated = data.rows.len() as u64 != data.row_count;
    out.push_str(&format!(
        "row_count: {}{}\n",
        data.row_count,
        if truncated { " (truncated)" } else { "" }
    ));
}

/// Render `envelope`'s compact text block. `envelope` must already have
/// gone through [`crate::envelope::Envelope::fit`] -- this function renders
/// exactly the `data` it is given, truncated or not, per D4's "the two
/// representations never differ".
pub fn render(envelope: &Envelope) -> String {
    let mut out = String::new();
    out.push_str(&format!("status: {}\n", status_label(envelope.status)));
    if let Some(failure) = &envelope.failure {
        out.push_str(&format!(
            "failure: {} {}\n",
            failure_class_label(failure.class),
            failure.message
        ));
    }
    render_data_table(&envelope.data, &mut out);
    if !envelope.warnings.is_empty() {
        out.push_str(&format!("warnings: {}\n", envelope.warnings.join("; ")));
    }
    for step in &envelope.next_steps {
        out.push_str(&format!("next_step: {} - {}\n", step.action, step.detail));
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::envelope::Column;

    #[test]
    fn renders_header_and_rows_without_panicking() {
        let mut envelope = Envelope::default();
        envelope.data.columns = vec![Column {
            name: "n".to_string(),
            r#type: "int64".to_string(),
        }];
        envelope.data.rows = vec![vec![Cell::Int(1)], vec![Cell::Int(2)]];
        envelope.data.row_count = 2;
        let text = render(&envelope);
        assert!(text.starts_with("status: ok\n"));
        assert!(text.contains("n\n"));
        assert!(text.contains("1\n"));
        assert!(text.contains("2\n"));
        assert!(text.contains("row_count: 2\n"));
    }

    #[test]
    fn hoists_attributes_shared_by_every_row() {
        let mut envelope = Envelope::default();
        envelope.data.columns = vec![Column {
            name: "attrs".to_string(),
            r#type: "map".to_string(),
        }];
        let mut m1 = Map::new();
        m1.insert("service".to_string(), Value::String("api".to_string()));
        m1.insert("id".to_string(), Value::String("1".to_string()));
        let mut m2 = Map::new();
        m2.insert("service".to_string(), Value::String("api".to_string()));
        m2.insert("id".to_string(), Value::String("2".to_string()));
        envelope.data.rows = vec![vec![Cell::Map(m1)], vec![Cell::Map(m2)]];
        envelope.data.row_count = 2;

        let text = render(&envelope);
        assert!(text.contains("# shared attrs: {\"service\":\"api\"}"));
        assert!(text.contains("{\"id\":\"1\"}"));
        assert!(text.contains("{\"id\":\"2\"}"));
        assert!(!text.contains("\"service\":\"api\",\"id\""));
    }

    #[test]
    fn renders_failure_line_for_error_status() {
        let mut envelope = Envelope::default();
        envelope.status = Status::Error;
        envelope.failure = Some(crate::envelope::Failure {
            class: FailureClass::MissingArgument,
            message: "time_range is required".to_string(),
            counter: None,
        });
        let text = render(&envelope);
        assert!(text.contains("status: error\n"));
        assert!(text.contains("failure: missing_argument time_range is required\n"));
    }
}
