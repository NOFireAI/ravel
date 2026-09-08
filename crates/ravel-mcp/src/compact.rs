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
//!
//! # Two bounds make "not a second copy" mechanical
//!
//! Hoisting alone does not stop the text from costing what the rows cost:
//! a result of 5,000 rows with nothing in common renders 5,000 lines, and
//! one row that `fit` shortened to just under a 4 MiB response cap renders
//! one line of nearly that size. Both would double the response the D4 byte
//! cap just sized. So the text carries at most [`MAX_TEXT_ROWS`] rows and at
//! most [`MAX_TEXT_BYTES`] bytes, and says so in the text when either bound
//! bites: `# rows_not_shown: N` for the row bound, a `...[truncated]`
//! marker for the byte bound. `structuredContent` remains the complete
//! representation, which is what a caller reads programmatically anyway;
//! `data.row_count` keeps the true count either way.
//!
//! The byte bound applies to the table alone. The summary lines (status,
//! failure, warnings, next steps) are what a caller acts on when a result
//! is too big to read, so they are rendered outside the bounded region and
//! survive it.

use serde_json::{Map, Value};

use crate::envelope::{Cell, Column, Data, Envelope, FailureClass, Row, Status};

/// The most rows the text block renders, however many `data.rows` holds.
pub const MAX_TEXT_ROWS: usize = 20;

/// The most bytes the whole text block occupies. 64 KiB, an eighth of the
/// 512 KiB default response cap.
pub const MAX_TEXT_BYTES: usize = 64 * 1024;

/// Written where the byte bound cut the table. Leads with a newline so it
/// terminates whatever partial line it follows.
const TRUNCATION_MARKER: &str = "\n...[truncated]\n";

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
        Cell::HexId(id) => id.as_str().to_string(),
        Cell::Str(s) => s.clone(),
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

/// The longest prefix of `text` that is at most `max` bytes and ends on a
/// character boundary.
fn floor_char_boundary(text: &str, max: usize) -> &str {
    if max >= text.len() {
        return text;
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text.get(..end).unwrap_or("")
}

fn render_data_table(data: &Data, out: &mut String) {
    if data.columns.is_empty() {
        return;
    }
    let header: Vec<&str> = data
        .columns
        .iter()
        .map(|c: &Column| c.name.as_str())
        .collect();
    out.push_str(&header.join("\t"));
    out.push('\n');

    // Hoisting is computed over the rows actually rendered, so a key shared
    // by every rendered row is hoisted even when a row beyond the bound
    // differs there, and every printed cell's diff is against the `# shared`
    // line the reader can see.
    let rows = data.rows.get(..MAX_TEXT_ROWS).unwrap_or(&data.rows);
    let shared: Vec<Option<Map<String, Value>>> = (0..data.columns.len())
        .map(|i| shared_map_keys(rows, i))
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

    for row in rows {
        let cells: Vec<String> = row
            .iter()
            .enumerate()
            .map(
                |(i, cell)| match (shared.get(i).and_then(Option::as_ref), cell) {
                    (Some(shared_map), Cell::Map(m)) => {
                        let diff: Map<String, Value> = m
                            .iter()
                            .filter(|(k, v)| shared_map.get(k.as_str()) != Some(*v))
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        serde_json::to_string(&Value::Object(diff)).unwrap_or_default()
                    }
                    _ => cell_display(cell),
                },
            )
            .collect();
        out.push_str(&cells.join("\t"));
        out.push('\n');
    }

    let not_shown = data.rows.len().saturating_sub(rows.len());
    if not_shown > 0 {
        out.push_str(&format!("# rows_not_shown: {not_shown}\n"));
    }

    let truncated = data.rows.len() as u64 != data.row_count;
    out.push_str(&format!(
        "row_count: {}{}\n",
        data.row_count,
        if truncated { " (truncated)" } else { "" }
    ));
}

/// Render `envelope`'s compact text block, at most [`MAX_TEXT_ROWS`] rows
/// and at most [`MAX_TEXT_BYTES`] bytes. `envelope` must already have gone
/// through [`crate::envelope::Envelope::fit`] -- this function renders
/// exactly the `data` it is given, truncated or not, per D4's "the two
/// representations never differ".
pub fn render(envelope: &Envelope) -> String {
    let mut head = String::new();
    head.push_str(&format!("status: {}\n", status_label(envelope.status)));
    if let Some(failure) = &envelope.failure {
        head.push_str(&format!(
            "failure: {} {}\n",
            failure_class_label(failure.class),
            failure.message
        ));
    }

    let mut table = String::new();
    render_data_table(&envelope.data, &mut table);

    let mut tail = String::new();
    if !envelope.warnings.is_empty() {
        tail.push_str(&format!("warnings: {}\n", envelope.warnings.join("; ")));
    }
    for step in &envelope.next_steps {
        tail.push_str(&format!("next_step: {} - {}\n", step.action, step.detail));
    }

    let summary_len = head.len() + tail.len();
    if summary_len + table.len() <= MAX_TEXT_BYTES {
        return head + &table + &tail;
    }

    let available = MAX_TEXT_BYTES.saturating_sub(summary_len + TRUNCATION_MARKER.len());
    let mut out = head;
    out.push_str(floor_char_boundary(&table, available));
    out.push_str(TRUNCATION_MARKER);
    out.push_str(&tail);
    // Reached only when the summary lines alone are over the bound, which
    // the D4 metadata bounds make unlikely rather than impossible.
    if out.len() > MAX_TEXT_BYTES {
        out = floor_char_boundary(&out, MAX_TEXT_BYTES).to_string();
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::envelope::Column;

    fn int_envelope(row_count: usize) -> Envelope {
        let mut envelope = Envelope::default();
        envelope.data.columns = vec![Column {
            name: "n".to_string(),
            r#type: "int64".to_string(),
        }];
        envelope.data.rows = (0..row_count)
            .map(|i| vec![Cell::Int(i as i64 + 1)])
            .collect();
        envelope.data.row_count = row_count as u64;
        envelope
    }

    /// The whole rendering, asserted exactly. `contains` assertions cannot
    /// see a dropped row: `contains("2\n")` still holds when the row `2` is
    /// gone, because `row_count: 2\n` ends in the same two characters.
    #[test]
    fn renders_every_row_as_its_own_exact_line() {
        let text = render(&int_envelope(2));
        assert_eq!(text, "status: ok\nn\n1\n2\nrow_count: 2\n");
    }

    /// 25 rows render as exactly 20 row lines plus the count of the 5 that
    /// did not, so the text never costs what the rows cost while
    /// `row_count` still reports the true total.
    #[test]
    fn renders_at_most_twenty_rows_and_reports_the_rest() {
        let text = render(&int_envelope(25));

        let mut expected = String::from("status: ok\nn\n");
        for n in 1..=20 {
            expected.push_str(&format!("{n}\n"));
        }
        expected.push_str("# rows_not_shown: 5\nrow_count: 25\n");
        assert_eq!(text, expected);

        let row_lines = text
            .lines()
            .filter(|line| line.parse::<u32>().is_ok())
            .count();
        assert_eq!(row_lines, MAX_TEXT_ROWS);
        assert_eq!(row_lines, 20);
    }

    /// One row is enough to blow the byte bound: `fit` may keep a cell just
    /// under a 4 MiB response cap, and rendering it whole would double the
    /// response. The text is cut to exactly 64 KiB, marker included, and
    /// the summary lines outside the bounded table survive the cut.
    #[test]
    fn renders_at_most_sixty_four_kibibytes() {
        let mut envelope = int_envelope(1);
        envelope.data.rows = vec![vec![Cell::Str("x".repeat(200 * 1024))]];
        envelope.warnings = vec!["one row was shortened".to_string()];

        let text = render(&envelope);

        assert_eq!(text.len(), MAX_TEXT_BYTES);
        assert_eq!(text.len(), 65_536);
        assert!(text.starts_with("status: ok\nn\nxxx"));
        assert!(text.ends_with("\n...[truncated]\nwarnings: one row was shortened\n"));
    }

    /// The cut lands on a character boundary, never mid-`char`: a table of
    /// three-byte characters cuts to 65,534 bytes, the largest total at or
    /// under the bound that leaves no split character.
    #[test]
    fn byte_bound_cuts_on_a_character_boundary() {
        let mut envelope = int_envelope(1);
        envelope.data.rows = vec![vec![Cell::Str("\u{20ac}".repeat(30 * 1024))]];

        let text = render(&envelope);

        assert_eq!(text.len(), 65_534);
        assert!(text.len() <= MAX_TEXT_BYTES);
        assert!(text.ends_with("\u{20ac}\n...[truncated]\n"));
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
