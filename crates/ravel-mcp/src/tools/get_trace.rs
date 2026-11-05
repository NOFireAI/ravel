//! `ravel_get_trace`: every span of one trace id.
//!
//! The body validates the arguments and hands the lookup to
//! [`QueryBackend::trace_lookup`], which runs the same statement and window
//! rule as `GET /api/traces/{trace_id}`. Without a `time_range` the lookup
//! reads every span the tenant retains; with one, the window is kept exactly.
//! The rows keep the lookup's order, `(start_ts, span_id)`. No cursor is
//! minted: a trace over `max_rows` is `ok_bounded` (ADR-1374 span-tree
//! amendment, item C).
//!
//! # Output
//!
//! One data row per span. The first two columns are added by this tool and
//! describe the span tree over the rows returned:
//!
//! - `__ravel_parent`: `root` when the span has no parent id, or one of
//!   zeros only; `present` when a returned span has that id; `cycle` when following parents from the
//!   span leads back to it; otherwise `missing` when the result is complete
//!   and `not_returned` when it is not. Complete means no row cap and no byte
//!   cap fired, no row was omitted, and coverage is complete, so an absent
//!   parent in a capped result is never called missing.
//! - `__ravel_depth`: how many returned ancestors the span has: 0 for a root
//!   and for a span whose parent is absent, null for a span on a cycle or
//!   whose ancestors lead into one.
//!
//! When two rows share a span id, the first in row order is the parent its
//! children resolve to.
//!
//! The tree is computed after the row and byte caps, over the rows that
//! survive them. The byte fit runs with both columns already present and
//! holding the widest value each can take, so writing the real values
//! afterwards can only shorten the envelope the fit measured.
//!
//! # The logs leg
//!
//! With `include_logs: true` (it needs `time_range`) the call also reads the
//! log records carrying the trace id in the same window, through
//! `ravel_search_logs` with a `trace_id` argument, so the statement is that
//! tool's synthesis, ordered by `(ts, observed_ts)`. The records go in the
//! envelope's `logs` block, never in `data`, and with `include_logs` false or
//! absent that block is not serialized at all.
//!
//! - It runs after the span lookup under what is left of the call's one
//!   deadline, and is skipped with a stated reason when nothing is left.
//! - Its records have their own `max_rows` and share `max_response_bytes`
//!   with the spans, spans first: the leg's columns are in the envelope the
//!   span fit measures, and its records are then added as a prefix while the
//!   whole envelope stays under the same cap. No cursor: a capped leg reports
//!   its omitted records and a step naming `ravel_search_logs`.
//! - Its spend is `logs.budget` and the span lookup's is
//!   `logs.span_lookup_budget`; `budget.actual` is their sum. The leg's
//!   estimate is not an upper envelope, so a call with the leg never claims
//!   one.
//! - A leg that fails leaves the spans as they are and reports itself
//!   `failed` with its failure class.

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::catalog::GetTraceInput;
use crate::envelope::{
    AnyJson, Budget, CapAdvice, CapCut, Cell, Column, Envelope, Failure, FailureClass, LegSpend,
    LegStatus, LogsLeg, NextStep, Row, Status, fit_cap, serialized_len,
};
use crate::service::{QueryBackend, TraceLookupRequest};

use super::ToolContext;
use super::capabilities::refused;

const DEPTH_COLUMN: &str = "__ravel_depth";
const PARENT_COLUMN: &str = "__ravel_parent";
const WIDEN_STEP: &str = "widen_time_range";
const LOGS_STEP: &str = "ravel_search_logs";

/// What every call whose logs leg started carries.
pub const LOGS_SCAN_WARNING: &str = "include_logs ran an unindexed scan: no trace index covers \
     logs, so the leg reads the logs in time_range and keeps the records carrying trace_id \
     (a logs key index on trace_id, where the tenant declares one, can skip objects); its \
     spend is logs.budget, apart from the span lookup's logs.span_lookup_budget";

/// Why a leg did not start.
pub const LOGS_SKIPPED_NO_TIME: &str =
    "no time was left of the call's deadline after the span lookup";

/// A leg failure's message is cut to this many bytes, on a character
/// boundary.
const LEG_FAILURE_MESSAGE_BOUND: usize = 512;

/// The lead of the cap step a trace with a `not_returned` parent carries.
const NOT_RETURNED: &str = "a parent was not returned";

/// The lookup takes no projection, so only fewer spans make the trace
/// smaller.
const ADVICE: CapAdvice<'static> = CapAdvice {
    fewer_rows: "narrow time_range",
    smaller_rows_action: "narrow_time_range",
    smaller_rows: "narrow time_range",
    read_cell: None,
};

/// Run the tool. An argument the body cannot use is an `invalid_argument`
/// envelope and reaches no store.
pub async fn run<B: QueryBackend>(args: Value, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let call = match request(args) {
        Ok(call) => call,
        Err(failure) => return refused(failure, ctx),
    };
    let spans = match backend.trace_lookup(ctx.tenant_hash, &call.lookup).await {
        Ok(envelope) => envelope,
        Err(failure) => return refused(failure, ctx),
    };
    let leg = match &call.logs {
        Some(logs) => Some(logs_leg(logs, ctx, backend).await),
        None => None,
    };
    match finish(spans, &call, leg, ctx) {
        Ok(envelope) => envelope,
        Err(failure) => refused(failure, ctx),
    }
}

/// One validated call: the span lookup, and the logs leg when asked for.
#[derive(Debug)]
struct Call {
    lookup: TraceLookupRequest,
    logs: Option<LogsCall>,
}

/// The logs leg's `ravel_search_logs` arguments, and the step a capped leg
/// carries.
#[derive(Debug)]
struct LogsCall {
    args: Value,
    step: NextStep,
}

/// What the logs leg came back with.
enum Leg {
    /// It did not start, for the reason given.
    Skipped(&'static str),
    /// `ravel_search_logs`'s envelope: an error envelope when it failed.
    Ran(Box<Envelope>),
}

async fn logs_leg<B: QueryBackend>(logs: &LogsCall, ctx: &ToolContext<'_>, backend: &B) -> Leg {
    if backend.time_left().is_some_and(|left| left.is_zero()) {
        return Leg::Skipped(LOGS_SKIPPED_NO_TIME);
    }
    Leg::Ran(Box::new(
        super::search_logs::run(logs.args.clone(), ctx, backend).await,
    ))
}

/// The `logs` block for `leg`, and the records it read, held out of the
/// block until the spans are fitted.
fn leg_block(leg: Leg, span_lookup: &Budget) -> (LogsLeg, Vec<Row>) {
    let span_lookup_budget = LegSpend {
        actual: span_lookup.actual.clone(),
        estimate: span_lookup.estimate.clone(),
        estimate_is_upper_envelope: span_lookup.estimate_is_upper_envelope,
    };
    let mut block = LogsLeg {
        status: LegStatus::Skipped,
        failure: None,
        skipped_reason: None,
        columns: Vec::new(),
        rows: Vec::new(),
        row_count: 0,
        row_cap_hit: false,
        bytes_cap_hit: false,
        rows_omitted: 0,
        budget: LegSpend::default(),
        span_lookup_budget,
    };
    let envelope = match leg {
        Leg::Skipped(reason) => {
            block.skipped_reason = Some(reason.to_string());
            return (block, Vec::new());
        }
        Leg::Ran(envelope) => *envelope,
    };
    block.budget = LegSpend {
        actual: envelope.budget.actual,
        estimate: envelope.budget.estimate,
        estimate_is_upper_envelope: false,
    };
    if let Some(mut failure) = envelope.failure {
        failure.message = bounded(failure.message, LEG_FAILURE_MESSAGE_BOUND);
        block.status = LegStatus::Failed;
        block.failure = Some(failure);
        return (block, Vec::new());
    }
    let shown = &envelope.presentation;
    let capped = shown.row_cap_hit
        || shown.bytes_cap_hit
        || shown.rows_omitted > 0
        || envelope.status != Status::Ok;
    block.status = if capped {
        LegStatus::OkBounded
    } else {
        LegStatus::Ok
    };
    block.row_cap_hit = shown.row_cap_hit;
    block.bytes_cap_hit = shown.bytes_cap_hit;
    block.rows_omitted = shown.rows_omitted;
    block.row_count = envelope.data.row_count;
    block.columns = envelope.data.columns;
    (block, envelope.data.rows)
}

/// `message` cut to at most `bound` bytes on a character boundary.
fn bounded(mut message: String, bound: usize) -> String {
    if message.len() > bound {
        let mut at = bound;
        while !message.is_char_boundary(at) {
            at -= 1;
        }
        message.truncate(at);
    }
    message
}

/// The whole call's budget: the span lookup's ceilings, the two parts'
/// spend summed, and no upper-envelope claim.
fn whole_call(span_lookup: &Budget, leg: &LegSpend) -> Budget {
    Budget {
        effective: span_lookup.effective.clone(),
        actual: AnyJson(sum_figures(&span_lookup.actual.0, &leg.actual.0)),
        estimate: AnyJson(sum_figures(&span_lookup.estimate.0, &leg.estimate.0)),
        estimate_is_upper_envelope: false,
    }
}

/// Two spend objects added key by key. Numbers add; a null on either side
/// stays null (a figure nobody could bound); string lists (`unbounded`) are
/// merged; a key only one side has keeps that side's value. A side that is
/// not an object contributes nothing.
fn sum_figures(a: &Value, b: &Value) -> Value {
    let (Value::Object(a), Value::Object(b)) = (a, b) else {
        return match a {
            Value::Object(_) => a.clone(),
            _ => b.clone(),
        };
    };
    let mut sum = Map::new();
    for (key, left) in a {
        let value = match (left, b.get(key)) {
            (left, None) => left.clone(),
            (Value::Number(x), Some(Value::Number(y))) => match (x.as_u64(), y.as_u64()) {
                (Some(x), Some(y)) => json!(x.saturating_add(y)),
                _ => Value::Null,
            },
            (Value::Array(x), Some(Value::Array(y))) => {
                let mut merged = x.clone();
                for item in y {
                    if !merged.contains(item) {
                        merged.push(item.clone());
                    }
                }
                Value::Array(merged)
            }
            _ => Value::Null,
        };
        sum.insert(key.clone(), value);
    }
    for (key, right) in b {
        if !a.contains_key(key) {
            sum.insert(key.clone(), right.clone());
        }
    }
    Value::Object(sum)
}

/// The leg's cap figures before its records are placed.
#[derive(Debug, Clone, Copy)]
struct Before {
    omitted: u64,
    bytes_cut: bool,
    capped: bool,
}

/// Set every leg field that placing `offered` records can change to its
/// widest value, so the span fit and the placement measure an envelope the
/// final values can only shorten, and return what they were.
fn widen(leg: &mut LogsLeg, offered: usize) -> Before {
    let before = Before {
        omitted: leg.rows_omitted,
        bytes_cut: leg.bytes_cap_hit,
        capped: leg.status == LegStatus::OkBounded,
    };
    leg.rows_omitted = leg.rows_omitted.saturating_add(offered as u64);
    // `false` serializes one byte longer than `true`.
    leg.bytes_cap_hit = false;
    leg.status = LegStatus::OkBounded;
    before
}

/// Add the leg's records to the fitted envelope as the longest prefix that
/// keeps the whole envelope under the fit cap, then write the leg's real cap
/// figures, which can only shorten what was measured. A leg left uncapped
/// drops its step.
fn place_logs(
    envelope: &mut Envelope,
    mut rows: Vec<Row>,
    before: Before,
    max_response_bytes: u64,
) {
    let offered = rows.len();
    let Before {
        omitted: omitted_before,
        bytes_cut: cut_before,
        capped: capped_before,
    } = before;
    let cap = fit_cap(max_response_bytes);
    let mut used = serialized_len(envelope);
    let mut kept = 0;
    for row in &rows {
        let len = serde_json::to_vec(row).map_or(usize::MAX, |bytes| bytes.len());
        let add = len.saturating_add(usize::from(kept > 0));
        if used.saturating_add(add) > cap {
            break;
        }
        used += add;
        kept += 1;
    }
    rows.truncate(kept);
    let Some(leg) = envelope.logs.as_mut() else {
        return;
    };
    let cut = kept < offered;
    leg.rows = rows;
    leg.rows_omitted = omitted_before.saturating_add((offered - kept) as u64);
    leg.bytes_cap_hit = cut_before || cut;
    leg.status = if capped_before || cut {
        LegStatus::OkBounded
    } else {
        LegStatus::Ok
    };
    if leg.status == LegStatus::Ok {
        envelope.next_steps.retain(|next| next.action != LOGS_STEP);
    }
}

/// Where a span's parent stands among the rows returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParentState {
    Root,
    Present,
    Missing,
    NotReturned,
    Cycle,
}

impl ParentState {
    fn label(self) -> &'static str {
        match self {
            ParentState::Root => "root",
            ParentState::Present => "present",
            ParentState::Missing => "missing",
            ParentState::NotReturned => "not_returned",
            ParentState::Cycle => "cycle",
        }
    }
}

/// The longest label: the fit reserves its width on every row.
const WIDEST_LABEL: &str = "not_returned";

/// One span's place in the tree. `depth` is `None` on a cycle and on any
/// span whose ancestors lead into one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Placement {
    depth: Option<u64>,
    parent: ParentState,
}

#[derive(Debug, Clone, Copy)]
enum Link {
    Root,
    Absent,
    To(usize),
}

/// The span tree over `rows`: one placement per row, in row order.
///
/// `span_at` and `parent_at` are the positions of `span_id` and
/// `parent_span_id`. A null or all-zero parent id is a root. A parent id resolves to the
/// first row carrying it as `span_id`. An unresolved parent id is `missing`
/// when `complete` and `not_returned` otherwise. The walk visits each row at
/// most once on its way up, so it ends on any input, cycles included.
fn span_tree(
    rows: &[Row],
    span_at: usize,
    parent_at: usize,
    complete: bool,
) -> Result<Vec<Placement>, Failure> {
    let mut first: HashMap<&str, usize> = HashMap::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        if let Some(id) = id_at(row, span_at, "span_id")? {
            first.entry(id).or_insert(index);
        }
    }
    let links = rows
        .iter()
        .map(|row| {
            Ok(match id_at(row, parent_at, "parent_span_id")? {
                None => Link::Root,
                Some(id) if is_zero_id(id) => Link::Root,
                Some(id) => first.get(id).map_or(Link::Absent, |&at| Link::To(at)),
            })
        })
        .collect::<Result<Vec<Link>, Failure>>()?;
    let absent = if complete {
        ParentState::Missing
    } else {
        ParentState::NotReturned
    };

    let mut placed: Vec<Option<Placement>> = vec![None; rows.len()];
    let mut on_path = vec![false; rows.len()];
    let mut path: Vec<usize> = Vec::new();
    for start in 0..rows.len() {
        let mut at = start;
        let mut depth = loop {
            if let Some(done) = placed[at] {
                break done.depth;
            }
            if on_path[at] {
                // Every row on the path from `at` onwards is on the cycle.
                let entry = path.iter().position(|&row| row == at).unwrap_or(0);
                for row in path.drain(entry..) {
                    on_path[row] = false;
                    placed[row] = Some(Placement {
                        depth: None,
                        parent: ParentState::Cycle,
                    });
                }
                break None;
            }
            let parent = match links[at] {
                Link::Root => ParentState::Root,
                Link::Absent => absent,
                Link::To(parent) => {
                    on_path[at] = true;
                    path.push(at);
                    at = parent;
                    continue;
                }
            };
            placed[at] = Some(Placement {
                depth: Some(0),
                parent,
            });
            break Some(0);
        };
        while let Some(row) = path.pop() {
            on_path[row] = false;
            depth = depth.map(|depth| depth + 1);
            placed[row] = Some(Placement {
                depth,
                parent: ParentState::Present,
            });
        }
    }
    placed
        .into_iter()
        .map(|placement| placement.ok_or_else(|| internal("a span was left unplaced".to_string())))
        .collect()
}

/// An id of zeros only: OTLP's spelling of "no parent".
fn is_zero_id(id: &str) -> bool {
    !id.is_empty() && id.bytes().all(|byte| byte == b'0')
}

/// The id a cell holds: `None` for null, the text for a string or hex id.
fn id_at<'a>(row: &'a Row, at: usize, name: &str) -> Result<Option<&'a str>, Failure> {
    match row.get(at) {
        Some(Cell::Null) => Ok(None),
        Some(Cell::Str(id)) => Ok(Some(id)),
        Some(Cell::HexId(id)) => Ok(Some(id.as_str())),
        Some(other) => Err(internal(format!("{name} holds {other:?}, not a span id"))),
        None => Err(internal(format!("a row has no {name} cell"))),
    }
}

/// Cap, fit, then label: the labels are computed over the rows the caller
/// receives, so a parent the byte cap dropped reads as `not_returned`.
fn finish(
    mut envelope: Envelope,
    call: &Call,
    leg: Option<Leg>,
    ctx: &ToolContext<'_>,
) -> Result<Envelope, Failure> {
    let request = &call.lookup;
    let span_at = column_at(&envelope, "span_id")?;
    let parent_at = column_at(&envelope, "parent_span_id")?;

    let produced = envelope.data.row_count;
    let max_rows = ctx.budgets.max_rows as usize;
    let row_cap_omitted = envelope.data.rows.len().saturating_sub(max_rows) as u64;
    envelope.data.rows.truncate(max_rows);
    envelope.presentation.max_rows = ctx.budgets.max_rows;

    // A depth is at most the row count less one; 10 keeps the placeholder
    // at least as wide as `null`.
    let widest_depth = envelope.data.rows.len().max(10) as i128;
    for row in &mut envelope.data.rows {
        row.splice(
            0..0,
            [Cell::Int(widest_depth), Cell::Str(WIDEST_LABEL.to_string())],
        );
    }
    envelope.data.columns.splice(
        0..0,
        [
            Column {
                name: DEPTH_COLUMN.to_string(),
                r#type: "Int64".to_string(),
            },
            Column {
                name: PARENT_COLUMN.to_string(),
                r#type: "Utf8".to_string(),
            },
        ],
    );
    let mut log_rows = Vec::new();
    let mut before = None;
    if let (Some(leg), Some(logs)) = (leg, &call.logs) {
        let (mut block, rows) = leg_block(leg, &envelope.budget);
        if block.status != LegStatus::Skipped {
            envelope.warnings.push(LOGS_SCAN_WARNING.to_string());
        }
        if matches!(block.status, LegStatus::Ok | LegStatus::OkBounded) {
            envelope.next_steps.push(logs.step.clone());
            before = Some(widen(&mut block, rows.len()));
        }
        envelope.budget = whole_call(&envelope.budget, &block.budget);
        envelope.logs = Some(block);
        log_rows = rows;
    }
    envelope.next_steps.push(step(
        WIDEN_STEP,
        "a parent is missing and may start outside time_range; widen time_range or drop it",
    ));
    envelope
        .next_steps
        .push(ADVICE.widest(NOT_RETURNED, &ctx.budgets));
    let cap_step_at = envelope.next_steps.len() - 1;

    let cap = ctx.budgets.max_response_bytes;
    let mut fitted = envelope.clone().fit_row_capped(cap, row_cap_omitted);
    if serialized_len(&fitted) > fit_cap(cap)
        && let Some(leg) = envelope.logs.as_mut()
    {
        // The leg's columns alone leave no room: none of its records can be
        // returned, and the spans keep the room.
        leg.columns.clear();
        before = before.map(|before| Before {
            omitted: before.omitted.saturating_add(log_rows.len() as u64),
            bytes_cut: true,
            capped: true,
        });
        log_rows.clear();
        fitted = envelope.fit_row_capped(cap, row_cap_omitted);
    }
    fitted.data.row_count = produced;
    let cut = CapCut::of(&fitted.presentation, fitted.data.rows.len(), &ctx.budgets);

    let shown = &fitted.presentation;
    let complete = !shown.row_cap_hit
        && !shown.bytes_cap_hit
        && shown.rows_omitted == 0
        && fitted.coverage.complete;
    let tree = span_tree(&fitted.data.rows, span_at + 2, parent_at + 2, complete)?;
    for (row, placement) in fitted.data.rows.iter_mut().zip(&tree) {
        row[0] = placement
            .depth
            .map_or(Cell::Null, |depth| Cell::Int(i128::from(depth)));
        row[1] = Cell::Str(placement.parent.label().to_string());
    }
    let missing = tree.iter().any(|p| p.parent == ParentState::Missing);
    let not_returned = tree.iter().any(|p| p.parent == ParentState::NotReturned);
    let widen = missing && request.window.is_some();
    let cap_step = match cut {
        CapCut::Cells { .. } => ADVICE.step(cut, ""),
        _ if not_returned => ADVICE.step(cut, NOT_RETURNED),
        _ => None,
    };
    fitted.replace_step(cap_step_at, cap_step);
    if !widen {
        fitted.next_steps.retain(|next| next.action != WIDEN_STEP);
    }
    if let Some(before) = before {
        place_logs(&mut fitted, log_rows, before, cap);
    }
    Ok(fitted.finish(false))
}

fn column_at(envelope: &Envelope, name: &str) -> Result<usize, Failure> {
    envelope
        .data
        .columns
        .iter()
        .position(|column| column.name == name)
        .ok_or_else(|| internal(format!("the trace lookup returned no {name} column")))
}

fn step(action: &str, detail: &str) -> NextStep {
    NextStep {
        action: action.to_string(),
        detail: detail.to_string(),
    }
}

fn request(args: Value) -> Result<Call, Failure> {
    let input = GetTraceInput::deserialize(args)
        .map_err(|err| invalid(format!("ravel_get_trace arguments: {err}")))?;
    if input.evidence_ref.is_some() {
        return Err(Failure {
            class: FailureClass::Unsupported,
            message: "evidence_ref is not served by ravel_get_trace in this build".to_string(),
            counter: None,
        });
    }
    let trace_id = parse_trace_id(&input.trace_id)?;
    let window = input
        .time_range
        .as_ref()
        .map(crate::time::window)
        .transpose()?;
    let logs = match (input.include_logs, window) {
        (Some(true), None) => {
            return Err(Failure {
                class: FailureClass::MissingArgument,
                message: "include_logs requires time_range: the logs leg reads the logs in \
                          that window"
                    .to_string(),
                counter: None,
            });
        }
        (Some(true), Some(window)) => Some(logs_call(&trace_id, window)),
        _ => None,
    };
    Ok(Call {
        lookup: TraceLookupRequest { trace_id, window },
        logs,
    })
}

/// The leg's `ravel_search_logs` arguments: the trace id and the window,
/// nothing else, so the statement is that tool's `trace_id` synthesis.
fn logs_call(trace_id: &[u8; 16], window: ravel_types::TimeRange) -> LogsCall {
    let hex: String = trace_id.iter().map(|byte| format!("{byte:02x}")).collect();
    let (start_ns, end_ns) = (window.start_ns.to_string(), window.end_ns.to_string());
    let step = step(
        LOGS_STEP,
        &format!(
            "the trace's log records were capped and this leg has no cursor; page through \
             them with ravel_search_logs, trace_id {hex}, time_range start_ns {start_ns} \
             end_ns {end_ns}"
        ),
    );
    LogsCall {
        args: json!({
            "trace_id": hex,
            "time_range": {"start_ns": start_ns, "end_ns": end_ns},
        }),
        step,
    }
}

fn parse_trace_id(raw: &str) -> Result<[u8; 16], Failure> {
    let invalid_id = || invalid(format!("trace_id {raw:?} is not 32 hexadecimal characters"));
    let bytes = raw.as_bytes();
    if bytes.len() != 32 {
        return Err(invalid_id());
    }
    let mut id = [0u8; 16];
    for (out, pair) in id.iter_mut().zip(bytes.chunks_exact(2)) {
        let hi = char::from(pair[0]).to_digit(16).ok_or_else(invalid_id)?;
        let lo = char::from(pair[1]).to_digit(16).ok_or_else(invalid_id)?;
        *out = (hi << 4 | lo) as u8;
    }
    Ok(id)
}

fn invalid(message: String) -> Failure {
    Failure {
        class: FailureClass::InvalidArgument,
        message,
        counter: None,
    }
}

fn internal(message: String) -> Failure {
    Failure {
        class: FailureClass::Internal,
        message,
        counter: None,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::budget::MAX_RESPONSE_BYTES_FLOOR;
    use crate::cursor::{CURSOR_KEY_LEN, CursorKey};
    use crate::envelope::{Status, serialized_len};
    use crate::tools::tests::{Stub, block_on, test_context};
    use serde_json::json;

    const TRACE: &str = "00112233445566778899aabbccddeeff";

    fn key() -> CursorKey {
        CursorKey::from_process_secret([0x5bu8; CURSOR_KEY_LEN])
    }

    /// What the port returns: the `spans` columns that matter here, one row
    /// per `(span_id, parent_span_id, name)`, complete and uncapped.
    fn spans(rows: &[(&str, Option<&str>, &str)]) -> Envelope {
        let mut envelope = Envelope::default();
        envelope.data.columns = ["trace_id", "span_id", "parent_span_id", "name"]
            .iter()
            .map(|name| Column {
                name: (*name).to_string(),
                r#type: "Utf8".to_string(),
            })
            .collect();
        envelope.data.rows = rows
            .iter()
            .map(|&(span, parent, name)| {
                vec![
                    Cell::Str(TRACE.to_string()),
                    Cell::Str(span.to_string()),
                    parent.map_or(Cell::Null, |parent| Cell::Str(parent.to_string())),
                    Cell::Str(name.to_string()),
                ]
            })
            .collect();
        envelope.data.row_count = rows.len() as u64;
        envelope.coverage.complete = true;
        envelope.accuracy.exact = true;
        envelope
    }

    fn call(port: Envelope, args: Value, ctx: &ToolContext<'_>) -> Envelope {
        let stub = Stub::default();
        stub.trace.borrow_mut().push_back(Ok(port));
        block_on(run(args, ctx, &stub))
    }

    fn untimed() -> Value {
        json!({"trace_id": TRACE})
    }

    fn windowed() -> Value {
        json!({"trace_id": TRACE, "time_range": {"start_ns": "7", "end_ns": "9"}})
    }

    /// Each returned row as `(span_id, depth, parent label)`, read by column
    /// name; the depth as its JSON form, so null stays distinguishable.
    fn tree(envelope: &Envelope) -> Vec<(String, Value, String)> {
        let at = |name: &str| {
            envelope
                .data
                .columns
                .iter()
                .position(|column| column.name == name)
                .expect("column present")
        };
        let (depth, parent, span) = (at(DEPTH_COLUMN), at(PARENT_COLUMN), at("span_id"));
        envelope
            .data
            .rows
            .iter()
            .map(|row| {
                let text = |cell: &Cell| match cell {
                    Cell::Str(text) => text.clone(),
                    other => panic!("not a string: {other:?}"),
                };
                (
                    text(&row[span]),
                    serde_json::to_value(&row[depth]).expect("a cell serializes"),
                    text(&row[parent]),
                )
            })
            .collect()
    }

    fn node(span: &str, depth: Value, parent: &str) -> (String, Value, String) {
        (span.to_string(), depth, parent.to_string())
    }

    fn actions(envelope: &Envelope) -> Vec<&str> {
        envelope
            .next_steps
            .iter()
            .map(|next| next.action.as_str())
            .collect()
    }

    /// The root `r` is not returned and the result is complete: `c` is
    /// `missing` at depth 0, its child `g` is `present` at depth 1, and both
    /// added columns lead the row.
    ///
    /// FLIP: counting depth from the absent parent (an absent link at depth
    /// 1) reports `c` at 1 and `g` at 2.
    #[test]
    fn missing_parent_is_disclosed() {
        let key = key();
        let ctx = test_context(&key);
        let port = spans(&[("c", Some("r"), "child"), ("g", Some("c"), "grandchild")]);
        let envelope = call(port, untimed(), &ctx);
        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");
        assert_eq!(envelope.data.columns[0].name, DEPTH_COLUMN);
        assert_eq!(envelope.data.columns[1].name, PARENT_COLUMN);
        assert_eq!(envelope.data.columns[3].name, "span_id");
        assert_eq!(
            tree(&envelope),
            [
                node("c", json!("0"), "missing"),
                node("g", json!("1"), "present"),
            ]
        );
        assert!(envelope.next_steps.is_empty(), "{:?}", envelope.next_steps);
    }

    /// An absent parent in a row-capped result is `not_returned`, whether
    /// the executor's cap fired or this tool's `max_rows` cut the rows.
    ///
    /// FLIP: deciding completeness without `row_cap_hit` labels `c`
    /// `missing` in the executor case.
    #[test]
    fn a_row_capped_result_never_says_missing() {
        let key = key();
        let ctx = test_context(&key);
        let mut capped = spans(&[("c", Some("r"), "child"), ("g", Some("c"), "grandchild")]);
        capped.presentation.row_cap_hit = true;
        let envelope = call(capped, untimed(), &ctx);
        assert_eq!(envelope.status, Status::OkBounded, "{envelope:?}");
        assert_eq!(
            tree(&envelope),
            [
                node("c", json!("0"), "not_returned"),
                node("g", json!("1"), "present"),
            ]
        );

        let mut ctx = test_context(&key);
        ctx.budgets.max_rows = 1;
        let port = spans(&[("c", Some("p"), "child"), ("p", None, "parent")]);
        let envelope = call(port, untimed(), &ctx);
        assert!(envelope.presentation.row_cap_hit);
        assert_eq!(envelope.presentation.rows_omitted, 1);
        assert_eq!(envelope.data.row_count, 2);
        assert_eq!(tree(&envelope), [node("c", json!("0"), "not_returned")]);
    }

    /// The byte cap drops a suffix that holds `c`'s parent: `c` survives as
    /// `not_returned`, and the envelope with both columns is inside the cap.
    ///
    /// FLIP: labelling before the fit calls `c` `present`; prepending the
    /// columns after the fit puts the envelope over the cap; the fixed
    /// `raise_max_rows` step answers a cut the row cap did not make.
    #[test]
    fn a_byte_capped_result_labels_after_the_fit_and_stays_inside_the_cap() {
        let key = key();
        let mut ctx = test_context(&key);
        ctx.budgets.max_response_bytes = MAX_RESPONSE_BYTES_FLOOR;
        ctx.budgets.max_rows = crate::budget::MAX_ROWS_CEILING;
        let filler = "x".repeat(250);
        let ids: Vec<String> = (0..1_200).map(|n| format!("f{n:015}")).collect();
        let mut rows = vec![("c", Some("p"), "child")];
        rows.extend(ids.iter().map(|id| (id.as_str(), None, filler.as_str())));
        rows.push(("p", None, "parent"));
        let port = spans(&rows);
        assert!(serialized_len(&port) > MAX_RESPONSE_BYTES_FLOOR as usize);

        let envelope = call(port, untimed(), &ctx);
        assert!(
            envelope.presentation.bytes_cap_hit,
            "{:?}",
            envelope.presentation
        );
        assert!(envelope.presentation.rows_omitted > 0);
        let tree = tree(&envelope);
        assert_eq!(tree[0], node("c", json!("0"), "not_returned"));
        assert!(tree.iter().all(|(span, _, _)| span != "p"));
        assert!(
            serialized_len(&envelope) <= MAX_RESPONSE_BYTES_FLOOR as usize,
            "{} bytes",
            serialized_len(&envelope)
        );
        assert_eq!(
            steps(&envelope),
            [(
                "raise_max_response_bytes",
                "a parent was not returned; the byte cap dropped rows; narrow time_range, or \
                 raise max_response_bytes (up to 4 MiB)"
                    .to_string()
            )]
        );
    }

    /// One span whose name is wider than the byte cap: the cap shortens the
    /// cell and drops no span, so the trace is `ok` with one step to read
    /// the cell in full. Under a configured 512 KiB ceiling, a 256 KiB cap
    /// is told to raise `max_response_bytes` to that ceiling, and a cap at
    /// it is told the span cannot be returned in full. Neither names
    /// time_range: a narrower window cannot return more of the one span.
    ///
    /// FLIP: building the cells detail from `smaller_rows` names "narrow
    /// time_range" in both details.
    #[test]
    fn one_span_cut_by_the_byte_cap_has_only_the_cells_step() {
        let ceiling = 2 * MAX_RESPONSE_BYTES_FLOOR;
        let cases = [
            (
                MAX_RESPONSE_BYTES_FLOOR,
                "the result is complete but the byte cap shortened cells; raise \
                 max_response_bytes (up to 512 KiB) to read them in full",
            ),
            (
                ceiling,
                "the result is complete but the byte cap shortened cells, which cannot be \
                 returned in full under the 512 KiB max_response_bytes ceiling",
            ),
        ];
        for (cap, detail) in cases {
            let key = key();
            let mut ctx = test_context(&key);
            ctx.budgets.max_response_bytes_ceiling = ceiling;
            ctx.budgets.max_response_bytes = cap;
            let wide = "x".repeat(cap as usize + 1);
            let envelope = call(spans(&[("r", None, wide.as_str())]), untimed(), &ctx);

            assert_eq!(
                envelope.status,
                Status::Ok,
                "{cap}: {:?}",
                envelope.presentation
            );
            assert_eq!(envelope.presentation.cells_truncated, 1, "{cap}");
            assert_eq!(envelope.presentation.rows_omitted, 0, "{cap}");
            assert_eq!(
                steps(&envelope),
                [("read_full_cells", detail.to_string())],
                "{cap}"
            );
            for next in &envelope.next_steps {
                assert!(
                    !next.detail.contains("time_range"),
                    "{cap}: {}",
                    next.detail
                );
            }
            assert!(serialized_len(&envelope) <= cap as usize, "{cap}");
        }
    }

    /// The row cap firing with `max_rows` at its ceiling cannot be answered
    /// by raising `max_rows`; below the ceiling it can.
    ///
    /// FLIP: choosing `raise_max_rows` whenever the row cap fired, as the
    /// fixed step did, lists it at the ceiling too.
    #[test]
    fn the_row_cap_step_follows_max_rows_against_its_ceiling() {
        let key = key();
        let mut ctx = test_context(&key);
        let capped = || {
            let mut envelope = spans(&[("c", Some("r"), "child")]);
            envelope.presentation.row_cap_hit = true;
            envelope
        };
        ctx.budgets.max_rows = crate::budget::MAX_ROWS_CEILING;
        let envelope = call(capped(), untimed(), &ctx);
        assert_eq!(envelope.status, Status::OkBounded, "{envelope:?}");
        assert_eq!(
            steps(&envelope),
            [(
                "narrow_time_range",
                "a parent was not returned; the row cap stopped the result with max_rows at \
                 its 5,000 ceiling; narrow time_range"
                    .to_string()
            )]
        );

        ctx.budgets.max_rows = crate::budget::MAX_ROWS_CEILING - 1;
        let envelope = call(capped(), untimed(), &ctx);
        assert_eq!(
            steps(&envelope),
            [(
                "raise_max_rows",
                "a parent was not returned; the row cap stopped the result; raise max_rows \
                 (up to 5,000) or narrow time_range"
                    .to_string()
            )]
        );
    }

    fn steps(envelope: &Envelope) -> Vec<(&str, String)> {
        envelope
            .next_steps
            .iter()
            .map(|next| (next.action.as_str(), next.detail.clone()))
            .collect()
    }

    /// A row-capped trace packed right up to the byte cap stays inside it
    /// once the row cap's count, `row_cap_hit` and the `ok_bounded` status
    /// they make are written. The sweep is the widest response the byte cap
    /// leaves alone, the 16 below it, and the narrowest it cuts.
    ///
    /// FLIP: folding the row cap in after the fit does not fail here. The
    /// fit measures the `widen_time_range` step, and a row-capped trace is
    /// never complete, so that step is always removed afterward, which frees
    /// more than the 8 B status change and the count's digits add.
    #[test]
    fn a_row_capped_trace_at_its_byte_cap_finishes_inside_it() {
        let key = key();
        let mut ctx = test_context(&key);
        ctx.budgets.max_response_bytes = MAX_RESPONSE_BYTES_FLOOR;
        ctx.budgets.max_rows = 1;
        let ids: Vec<String> = (0..13).map(|n| format!("s{n:02}")).collect();
        let run_with = |len: usize| {
            let name = "x".repeat(len);
            let rows: Vec<_> = ids
                .iter()
                .map(|id| (id.as_str(), None, name.as_str()))
                .collect();
            let mut port = spans(&rows);
            port.visibility.resolved_no_segments = true;
            call(port, untimed(), &ctx)
        };
        let cap = MAX_RESPONSE_BYTES_FLOOR as usize;
        let (mut lo, mut hi) = (0usize, cap);
        assert!(!run_with(lo).presentation.bytes_cap_hit);
        assert!(run_with(hi).presentation.bytes_cap_hit);
        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            if run_with(mid).presentation.bytes_cap_hit {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        let mut widest = 0;
        for len in lo - 16..=hi {
            let envelope = run_with(len);
            assert_eq!(envelope.status, Status::OkBounded, "{len}");
            assert!(envelope.presentation.row_cap_hit, "{len}");
            assert_eq!(envelope.presentation.rows_omitted, 12, "{len}");
            assert_eq!(envelope.data.rows.len(), 1, "{len}");
            let size = serialized_len(&envelope);
            assert!(size <= cap, "len {len}: {size} B, over the {cap} B cap");
            widest = widest.max(size);
        }
        // The reserved steps, both removed from these traces, are the gap.
        assert!(widest + 512 >= cap, "the sweep never came near the cap");
    }

    /// A child that starts before its parent (clock skew) comes first in row
    /// order and still resolves to it, with the right depth.
    ///
    /// FLIP: a single forward pass that only resolves parents seen earlier
    /// labels `c` `missing` at depth 0 and `g` at depth 1.
    #[test]
    fn a_child_that_starts_before_its_parent_is_present() {
        let key = key();
        let ctx = test_context(&key);
        let port = spans(&[
            ("c", Some("p"), "skewed child"),
            ("p", None, "root"),
            ("g", Some("c"), "grandchild"),
        ]);
        let envelope = call(port, untimed(), &ctx);
        assert_eq!(
            tree(&envelope),
            [
                node("c", json!("1"), "present"),
                node("p", json!("0"), "root"),
                node("g", json!("2"), "present"),
            ]
        );
    }

    /// Parents that loop (`a` and `b` each other's parent, `s` its own) end
    /// the walk: the spans on a loop are `cycle` with a null depth, and `d`,
    /// whose parent is on one, is `present` with a null depth. The walk runs
    /// on its own thread under a bound so a hang fails rather than stalls.
    ///
    /// FLIP: following parents without marking the path never ends on `a`,
    /// and the bound fails the test.
    #[test]
    fn a_parent_cycle_ends_and_is_labelled() {
        let rows = spans(&[
            ("a", Some("b"), "a"),
            ("b", Some("a"), "b"),
            ("d", Some("a"), "into the cycle"),
            ("s", Some("s"), "self"),
            ("r", None, "root"),
        ])
        .data
        .rows;
        let (send, receive) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = send.send(span_tree(&rows, 1, 2, true));
        });
        let placed = receive
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the walk ends")
            .expect("a placement per row");
        let cycle = Placement {
            depth: None,
            parent: ParentState::Cycle,
        };
        assert_eq!(
            placed,
            [
                cycle,
                cycle,
                Placement {
                    depth: None,
                    parent: ParentState::Present,
                },
                cycle,
                Placement {
                    depth: Some(0),
                    parent: ParentState::Root,
                },
            ]
        );

        let key = key();
        let ctx = test_context(&key);
        let envelope = call(spans(&[("s", Some("s"), "self")]), untimed(), &ctx);
        assert_eq!(tree(&envelope), [node("s", Value::Null, "cycle")]);
    }

    /// Two rows share the id `x`. The first in row order is the parent: `k`
    /// sits under the first `x` at depth 2, not under the second at depth 1.
    ///
    /// FLIP: an id map where the last row wins puts `k` at depth 1.
    #[test]
    fn a_duplicated_span_id_resolves_to_its_first_row() {
        let key = key();
        let ctx = test_context(&key);
        let port = spans(&[
            ("r", None, "root"),
            ("x", Some("r"), "first x"),
            ("x", None, "second x"),
            ("k", Some("x"), "child of x"),
        ]);
        let envelope = call(port, untimed(), &ctx);
        assert_eq!(
            tree(&envelope),
            [
                node("r", json!("0"), "root"),
                node("x", json!("1"), "present"),
                node("x", json!("0"), "root"),
                node("k", json!("2"), "present"),
            ]
        );
    }

    /// The widen step only for a `missing` parent under a `time_range`, the
    /// raise step only for a `not_returned` one, and none when every parent
    /// is present.
    ///
    /// FLIP: keeping the reserved steps without pruning them lists both in
    /// every case.
    #[test]
    fn next_steps_appear_exactly_for_an_absent_parent() {
        let key = key();
        let ctx = test_context(&key);
        let orphan = || spans(&[("c", Some("r"), "child")]);
        let whole = || spans(&[("r", None, "root"), ("c", Some("r"), "child")]);
        let capped = || {
            let mut envelope = orphan();
            envelope.presentation.row_cap_hit = true;
            envelope
        };
        for (what, port, args, expected) in [
            ("missing, windowed", orphan(), windowed(), vec![WIDEN_STEP]),
            ("missing, no window", orphan(), untimed(), vec![]),
            (
                "not returned, windowed",
                capped(),
                windowed(),
                vec!["raise_max_rows"],
            ),
            (
                "not returned, no window",
                capped(),
                untimed(),
                vec!["raise_max_rows"],
            ),
            ("all present, windowed", whole(), windowed(), vec![]),
        ] {
            let envelope = call(port, args, &ctx);
            assert_eq!(actions(&envelope), expected, "{what}");
        }
    }

    /// A lookup result without `span_id` or `parent_span_id` is an internal
    /// failure, not a tree computed over the wrong column.
    #[test]
    fn a_result_without_the_id_columns_is_internal() {
        let key = key();
        let ctx = test_context(&key);
        for name in ["span_id", "parent_span_id"] {
            let mut port = spans(&[("r", None, "root")]);
            let at = port
                .data
                .columns
                .iter()
                .position(|column| column.name == name)
                .expect("fixture column");
            port.data.columns.remove(at);
            for row in &mut port.data.rows {
                row.remove(at);
            }
            let envelope = call(port, untimed(), &ctx);
            assert_eq!(envelope.status, Status::Error, "{name}");
            let failure = envelope.failure.expect("a failure");
            assert_eq!(failure.class, FailureClass::Internal, "{name}");
            assert!(failure.message.contains(name), "{}", failure.message);
        }
    }

    /// The leg reads the logs in a window, so asking for it without one is
    /// refused before any store is reached.
    #[test]
    fn include_logs_needs_a_time_range() {
        let failure =
            request(json!({"trace_id": TRACE, "include_logs": true})).expect_err("refused");
        assert_eq!(failure.class, FailureClass::MissingArgument);
        assert_eq!(
            failure.message,
            "include_logs requires time_range: the logs leg reads the logs in that window"
        );
    }

    /// The span lookup's spend in the fixtures below.
    fn span_spend() -> (Value, Value) {
        (
            json!({"bytes_scanned": 100, "store_requests": 1, "segments_read": 1,
                   "decompressed_bytes": 200}),
            json!({"bytes_scanned": 150, "store_requests": 2, "segments_read": 1,
                   "decompressed_bytes": 300, "unbounded": []}),
        )
    }

    /// The leg's spend in the fixtures below.
    fn leg_spend() -> (Value, Value) {
        (
            json!({"bytes_scanned": 5000, "store_requests": 7, "segments_read": 3,
                   "decompressed_bytes": 9000}),
            json!({"bytes_scanned": 6000, "store_requests": 9, "segments_read": 3,
                   "decompressed_bytes": null, "unbounded": ["decompressed_bytes"]}),
        )
    }

    /// Three spans, complete, with [`span_spend`] and an upper-envelope
    /// estimate.
    fn costed_spans() -> Envelope {
        let mut port = spans(&[
            ("r", None, "root"),
            ("c", Some("r"), "child"),
            ("g", Some("c"), "grandchild"),
        ]);
        let (actual, estimate) = span_spend();
        port.budget = Budget {
            effective: AnyJson(json!({"max_rows": 200})),
            actual: AnyJson(actual),
            estimate: AnyJson(estimate),
            estimate_is_upper_envelope: true,
        };
        port
    }

    /// What the executor answers for the leg: one record per `(id, ts)`, in
    /// the order given, each `body` `body_len` bytes long, with
    /// [`leg_spend`].
    fn records(rows: &[(&str, u8)], body_len: usize) -> crate::draft::Draft {
        use ravel_sql::ResumeValue;
        let mut envelope = Envelope::default();
        envelope.ids.query_id = "q".to_owned();
        envelope.ids.audit_ref = "a".to_owned();
        envelope.data.columns = ["id", "trace_id", "ts", "observed_ts", "body", "attrs"]
            .iter()
            .map(|name| Column {
                name: (*name).to_owned(),
                r#type: "test".to_owned(),
            })
            .collect();
        envelope.data.rows = rows
            .iter()
            .map(|(id, ts)| {
                vec![
                    Cell::Str((*id).to_owned()),
                    Cell::Str(TRACE.to_owned()),
                    Cell::Int(i128::from(*ts)),
                    Cell::Int(i128::from(*ts)),
                    Cell::Str("b".repeat(body_len)),
                    Cell::Map(serde_json::Map::new()),
                ]
            })
            .collect();
        envelope.data.row_count = rows.len() as u64;
        envelope.coverage.complete = true;
        let (actual, estimate) = leg_spend();
        envelope.budget = Budget {
            effective: AnyJson(json!({"max_rows": 200})),
            actual: AnyJson(actual),
            estimate: AnyJson(estimate),
            estimate_is_upper_envelope: false,
        };
        let tuples = rows
            .iter()
            .map(|(_, ts)| {
                Some(vec![
                    ResumeValue::TimestampNanos(i64::from(*ts)),
                    ResumeValue::TimestampNanos(i64::from(*ts)),
                ])
            })
            .collect();
        crate::draft::Draft::new(
            envelope,
            Ok(ravel_types::Signal::Logs),
            Vec::new(),
            Vec::new(),
            Some(2),
            None,
            9,
        )
        .with_order_tuples(tuples)
    }

    fn with_logs() -> Value {
        json!({
            "trace_id": TRACE,
            "time_range": {"start_ns": "7", "end_ns": "9"},
            "include_logs": true,
        })
    }

    /// Run the tool against `stub`, its trace queue holding `port`.
    fn call_with(stub: &Stub, port: Envelope, args: Value, ctx: &ToolContext<'_>) -> Envelope {
        stub.trace.borrow_mut().push_back(Ok(port));
        block_on(run(args, ctx, stub))
    }

    fn leg(envelope: &Envelope) -> &LogsLeg {
        envelope.logs.as_ref().expect("a logs block")
    }

    /// The `id` of each record the leg returned, in order.
    fn record_ids(envelope: &Envelope) -> Vec<String> {
        let leg = leg(envelope);
        let at = leg
            .columns
            .iter()
            .position(|column| column.name == "id")
            .expect("an id column");
        leg.rows
            .iter()
            .map(|row| match &row[at] {
                Cell::Str(id) => id.clone(),
                other => panic!("id cell {other:?}"),
            })
            .collect()
    }

    fn json_of<T: serde::Serialize>(value: &T) -> Value {
        serde_json::to_value(value).expect("serializes")
    }

    /// The leg runs `ravel_search_logs`'s `trace_id` statement over the
    /// call's window, returns the records the executor matched in their
    /// order, warns that it is an unindexed scan, and reports its own spend
    /// apart from the span lookup's, with `budget.actual` their sum.
    ///
    /// FLIP: reporting the summed figures as the span lookup's own fails the
    /// `span_lookup_budget` assertion.
    #[test]
    fn logs_leg_reports_unindexed_scan_cost() {
        let key = key();
        let ctx = test_context(&key);
        let stub = Stub::default();
        stub.sql
            .borrow_mut()
            .push_back(records(&[("l1", 7), ("l2", 8), ("l3", 8)], 4));
        let envelope = call_with(&stub, costed_spans(), with_logs(), &ctx);

        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");
        assert_eq!(tree(&envelope).len(), 3);
        let executed = stub.executed.borrow();
        assert_eq!(executed.len(), 1);
        assert_eq!(
            executed[0].request.sql,
            "SELECT * FROM (SELECT * FROM logs WHERE \"trace_id\" = \
             X'00112233445566778899aabbccddeeff') AS ravel_page \
             ORDER BY \"ts\" ASC, \"observed_ts\" ASC"
        );
        assert_eq!(
            executed[0].request.window,
            ravel_types::TimeRange {
                start_ns: 7,
                end_ns: 9
            }
        );
        assert!(executed[0].request.row_window);
        assert_eq!(record_ids(&envelope), ["l1", "l2", "l3"]);

        let leg = leg(&envelope);
        assert_eq!(leg.status, LegStatus::Ok);
        assert_eq!(leg.row_count, 3);
        assert_eq!(leg.rows_omitted, 0);
        assert!(!leg.row_cap_hit && !leg.bytes_cap_hit);
        assert!(envelope.warnings.contains(&LOGS_SCAN_WARNING.to_string()));
        assert!(!actions(&envelope).contains(&LOGS_STEP));

        let (span_actual, span_estimate) = span_spend();
        let (leg_actual, leg_estimate) = leg_spend();
        assert_eq!(leg.budget.actual.0, leg_actual);
        assert_eq!(leg.budget.estimate.0, leg_estimate);
        assert!(!leg.budget.estimate_is_upper_envelope);
        assert_eq!(leg.span_lookup_budget.actual.0, span_actual);
        assert_eq!(leg.span_lookup_budget.estimate.0, span_estimate);
        assert!(leg.span_lookup_budget.estimate_is_upper_envelope);
        assert_eq!(
            envelope.budget.actual.0,
            json!({"bytes_scanned": 5100, "store_requests": 8, "segments_read": 4,
                   "decompressed_bytes": 9200})
        );
        assert_eq!(
            envelope.budget.estimate.0,
            json!({"bytes_scanned": 6150, "store_requests": 11, "segments_read": 4,
                   "decompressed_bytes": null, "unbounded": ["decompressed_bytes"]})
        );
        assert_eq!(envelope.budget.effective.0, json!({"max_rows": 200}));
    }

    /// `include_logs: false` serializes to exactly the bytes of a call
    /// without the argument: no `logs` key, no warning, no leg statement.
    ///
    /// FLIP: running the leg whatever `include_logs` says reaches the stub's
    /// empty `sql` queue, which panics.
    #[test]
    fn include_logs_false_leaves_the_envelope_unchanged() {
        let key = key();
        let ctx = test_context(&key);
        let without = call_with(&Stub::default(), costed_spans(), windowed(), &ctx);
        let stub = Stub::default();
        let mut args = windowed();
        args["include_logs"] = json!(false);
        let with_false = call_with(&stub, costed_spans(), args, &ctx);
        let bytes = |envelope: &Envelope| serde_json::to_vec(envelope).expect("serializes");
        assert_eq!(bytes(&with_false), bytes(&without));
        assert!(stub.executed.borrow().is_empty());
        assert!(with_false.logs.is_none());
        assert!(json_of(&with_false).get("logs").is_none());
        assert!(with_false.budget.estimate_is_upper_envelope);
        let (span_actual, _) = span_spend();
        assert_eq!(with_false.budget.actual.0, span_actual);
    }

    /// A call with the leg claims no upper envelope, and neither does the
    /// leg; the span lookup's own claim is kept beside them. The same spans
    /// without the leg keep the claim.
    #[test]
    fn a_call_with_the_leg_claims_no_upper_envelope() {
        let key = key();
        let ctx = test_context(&key);
        let stub = Stub::default();
        stub.sql.borrow_mut().push_back(records(&[("l1", 7)], 4));
        let with = call_with(&stub, costed_spans(), with_logs(), &ctx);
        assert!(!with.budget.estimate_is_upper_envelope);
        assert!(!leg(&with).budget.estimate_is_upper_envelope);
        assert!(leg(&with).span_lookup_budget.estimate_is_upper_envelope);
        let without = call_with(&Stub::default(), costed_spans(), windowed(), &ctx);
        assert!(without.budget.estimate_is_upper_envelope);
    }

    /// With nothing left of the call's deadline the leg does not start: it
    /// is `skipped` with its reason, the spans are returned as usual, and
    /// the call's spend is the span lookup's.
    #[test]
    fn a_leg_with_no_time_left_is_skipped() {
        let key = key();
        let ctx = test_context(&key);
        let stub = Stub {
            time_left: Some(std::time::Duration::ZERO),
            ..Stub::default()
        };
        let envelope = call_with(&stub, costed_spans(), with_logs(), &ctx);
        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");
        assert_eq!(tree(&envelope).len(), 3);
        assert!(stub.executed.borrow().is_empty());
        let leg = leg(&envelope);
        assert_eq!(leg.status, LegStatus::Skipped);
        assert_eq!(leg.skipped_reason.as_deref(), Some(LOGS_SKIPPED_NO_TIME));
        assert!(leg.failure.is_none());
        assert!(leg.rows.is_empty() && leg.columns.is_empty());
        assert!(!envelope.warnings.contains(&LOGS_SCAN_WARNING.to_string()));
        assert!(!actions(&envelope).contains(&LOGS_STEP));
        let (span_actual, _) = span_spend();
        assert_eq!(envelope.budget.actual.0, span_actual);
    }

    /// A leg the backend fails leaves the spans as they are: the call is
    /// `ok`, and the leg is `failed` and names its failure class.
    ///
    /// FLIP: refusing the call on a leg failure makes it `error`.
    #[test]
    fn a_failed_leg_still_returns_the_spans() {
        let key = key();
        let ctx = test_context(&key);
        let stub = Stub::default();
        *stub.sql_failure.borrow_mut() = Some(Failure {
            class: FailureClass::BudgetExceeded,
            message: "query exceeded max_bytes_scanned".to_string(),
            counter: None,
        });
        let envelope = call_with(&stub, costed_spans(), with_logs(), &ctx);
        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");
        assert!(envelope.failure.is_none());
        assert_eq!(
            tree(&envelope),
            [
                ("r".to_string(), json!("0"), "root".to_string()),
                ("c".to_string(), json!("1"), "present".to_string()),
                ("g".to_string(), json!("2"), "present".to_string()),
            ]
        );
        assert_eq!(stub.executed.borrow().len(), 1);
        let leg = leg(&envelope);
        assert_eq!(leg.status, LegStatus::Failed);
        let failure = leg.failure.as_ref().expect("a failure");
        assert_eq!(failure.class, FailureClass::BudgetExceeded);
        assert_eq!(failure.message, "query exceeded max_bytes_scanned");
        assert_eq!(
            json_of(leg)["failure"]["class"],
            json!("budget_exceeded"),
            "the class is named in the serialized block"
        );
        assert!(leg.rows.is_empty());
        assert!(envelope.warnings.contains(&LOGS_SCAN_WARNING.to_string()));
        assert!(!actions(&envelope).contains(&LOGS_STEP));
    }

    /// A leg over `max_rows` returns the first records, counts the rest in
    /// `rows_omitted`, mints no cursor, and points at `ravel_search_logs`
    /// with the trace id and the window.
    #[test]
    fn a_row_capped_leg_points_at_search_logs() {
        let key = key();
        let mut ctx = test_context(&key);
        ctx.budgets.max_rows = 3;
        let stub = Stub::default();
        stub.sql.borrow_mut().push_back(records(
            &[("l1", 7), ("l2", 8), ("l3", 9), ("l4", 10), ("l5", 11)],
            4,
        ));
        let envelope = call_with(&stub, costed_spans(), with_logs(), &ctx);
        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");
        assert_eq!(tree(&envelope).len(), 3);
        assert!(envelope.presentation.cursor.is_none());
        let leg = leg(&envelope);
        assert_eq!(leg.status, LegStatus::OkBounded);
        assert_eq!(record_ids(&envelope), ["l1", "l2", "l3"]);
        assert!(leg.row_cap_hit);
        assert_eq!(leg.rows_omitted, 2);
        assert_eq!(leg.row_count, 5);
        assert!(json_of(leg).get("cursor").is_none());
        assert_eq!(
            steps(&envelope),
            [(
                LOGS_STEP,
                "the trace's log records were capped and this leg has no cursor; page \
                 through them with ravel_search_logs, trace_id \
                 00112233445566778899aabbccddeeff, time_range start_ns 7 end_ns 9"
                    .to_string()
            )]
        );
    }

    /// Records that do not fit beside the spans are dropped from the end:
    /// every span is kept, the leg reports the records it lost, and the
    /// final envelope is inside `max_response_bytes`.
    #[test]
    fn a_byte_capped_leg_keeps_the_envelope_inside_the_cap() {
        let key = key();
        let mut ctx = test_context(&key);
        ctx.budgets.max_response_bytes = MAX_RESPONSE_BYTES_FLOOR;
        let stub = Stub::default();
        let ids: Vec<String> = (0..20).map(|i| format!("l{i:02}")).collect();
        let rows: Vec<(&str, u8)> = ids.iter().map(|id| (id.as_str(), 8)).collect();
        stub.sql.borrow_mut().push_back(records(&rows, 10_000));
        let mut port = costed_spans();
        for row in &mut port.data.rows {
            row[3] = Cell::Str("n".repeat(40_000));
        }
        let envelope = call_with(&stub, port, with_logs(), &ctx);

        let len = serialized_len(&envelope);
        assert!(len <= MAX_RESPONSE_BYTES_FLOOR as usize, "{len}");
        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");
        assert_eq!(tree(&envelope).len(), 3, "every span is kept");
        assert!(!envelope.presentation.bytes_cap_hit);
        let leg = leg(&envelope);
        let kept = leg.rows.len();
        assert_eq!(kept, 13, "the records that fit beside the spans");
        let expected: Vec<String> = ids[..kept].to_vec();
        assert_eq!(record_ids(&envelope), expected);
        assert_eq!(leg.status, LegStatus::OkBounded);
        assert!(leg.bytes_cap_hit);
        assert_eq!(leg.rows_omitted, (20 - kept) as u64);
        assert_eq!(actions(&envelope), [LOGS_STEP]);
    }

    /// An all-zero `parent_span_id`, OTLP's spelling of no parent, is a root
    /// like a null one.
    ///
    /// FLIP: resolving it as a real id leaves `a` with an absent parent,
    /// `missing` in this complete result.
    #[test]
    fn an_all_zero_parent_is_a_root() {
        let key = key();
        let ctx = test_context(&key);
        let port = spans(&[
            ("a", Some("0000000000000000"), "root"),
            ("b", Some("a"), "child"),
        ]);
        let envelope = call(port, untimed(), &ctx);
        assert_eq!(
            tree(&envelope),
            [
                ("a".to_string(), json!("0"), "root".to_string()),
                ("b".to_string(), json!("1"), "present".to_string()),
            ]
        );
        assert!(!is_zero_id(""));
        assert!(!is_zero_id("0000000000000001"));
        assert!(is_zero_id("00000000000000000000000000000000"));
    }

    #[test]
    fn an_omitted_time_range_is_the_no_window_lookup() {
        let request = request(json!({"trace_id": "00112233445566778899AABBCCDDEEFF"}))
            .expect("valid arguments");
        assert_eq!(request.lookup.window, None);
        assert_eq!(request.lookup.trace_id[0], 0x00);
        assert_eq!(request.lookup.trace_id[10], 0xaa);
        assert_eq!(request.lookup.trace_id[15], 0xff);
        assert!(request.logs.is_none());
    }

    #[test]
    fn a_given_time_range_is_kept_exactly() {
        let request = request(json!({
            "trace_id": "00112233445566778899aabbccddeeff",
            "time_range": {"start_ns": "7", "end_ns": "9"},
        }))
        .expect("valid arguments");
        assert_eq!(
            request.lookup.window,
            Some(ravel_types::TimeRange {
                start_ns: 7,
                end_ns: 9
            })
        );
    }

    /// FLIP: a get_trace that kept its own decimal-only parser instead of
    /// `crate::time` refuses this RFC 3339 window as `invalid_argument`.
    #[test]
    fn an_rfc3339_time_range_is_the_same_window_as_its_decimal_form() {
        let request = request(json!({
            "trace_id": "00112233445566778899aabbccddeeff",
            "time_range": {
                "start_ns": "2026-09-12T10:00:00Z",
                "end_ns": "2026-09-12T10:00:00.5Z",
            },
        }))
        .expect("valid arguments");
        assert_eq!(
            request.lookup.window,
            Some(ravel_types::TimeRange {
                start_ns: 1_789_207_200_000_000_000,
                end_ns: 1_789_207_200_500_000_000,
            })
        );
    }

    #[test]
    fn unusable_arguments_are_refused_with_their_class() {
        for (args, class) in [
            (json!({}), FailureClass::InvalidArgument),
            (json!({"trace_id": "abc"}), FailureClass::InvalidArgument),
            (
                json!({"trace_id": "00112233445566778899aabbccddeefg"}),
                FailureClass::InvalidArgument,
            ),
            (
                json!({"trace_id": "+0112233445566778899aabbccddeeff"}),
                FailureClass::InvalidArgument,
            ),
            (
                json!({
                    "trace_id": "00112233445566778899aabbccddeeff",
                    "time_range": {"start_ns": "9", "end_ns": "7"},
                }),
                FailureClass::InvalidArgument,
            ),
            (
                json!({
                    "trace_id": "00112233445566778899aabbccddeeff",
                    "time_range": {"start_ns": "soon", "end_ns": "7"},
                }),
                FailureClass::InvalidArgument,
            ),
            (
                json!({
                    "trace_id": "00112233445566778899aabbccddeeff",
                    "include_logs": true,
                }),
                FailureClass::MissingArgument,
            ),
            (
                json!({
                    "trace_id": "00112233445566778899aabbccddeeff",
                    "evidence_ref": "ev1",
                }),
                FailureClass::Unsupported,
            ),
        ] {
            let failure = request(args.clone()).expect_err("refused");
            assert_eq!(failure.class, class, "{args}");
        }
    }
}
