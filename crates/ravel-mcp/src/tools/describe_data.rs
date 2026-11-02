//! `ravel_describe_data`: one signal's effective schema, indexed keys,
//! committed row count, coverage window, freshness watermark, and its metric
//! families, a page of 100 at a time.
//!
//! The family list pages with a D5 keyset cursor. The position is the last
//! served family name's bytes under `order_by: ["name"]`, and a later page
//! asks [`QueryBackend::describe_data`] for the families whose names sort
//! after it in byte order, the order the service sorts the record in. The
//! cursor pins the resolve inputs of page one, so a later page resolves at
//! page one's instant and reports page one's watermark and snapshot id; the
//! redemption is refused when a newer erasure on the signal or a changed
//! declared column set has moved what that pin describes.
//!
//! The family list is not pinned: the metadata record is read fresh for each
//! page. A family added that sorts after the position appears on a later
//! page, one added that sorts before it does not, and `families_total` is the
//! count of the read that served the page, so it can differ between pages.
//!
//! Page one carries a `table` row (signal, segments, committed rows, coverage
//! window, family total), one `column` row per fixed and declared column, one
//! `indexed_key` row per key, then the `family` rows. A later page carries
//! `family` rows only.
//!
//! `budget.actual` counts the catalog resolve only. The metadata record read
//! and the tenant configuration read are not in the service's accounting,
//! and every page says so in `warnings`.

use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::catalog::DescribeDataInput;
use crate::cursor::{Cursor, CursorError, CursorPosition};
use crate::draft::{Draft, PageOrder, PinScope};
use crate::envelope::{
    AnyJson, Budget, Cell, Column, Envelope, Failure, FailureClass, Row, TimeRange,
};
use crate::service::{DescribeOutcome, DescribeRequest, QueryBackend};
use ravel_types::Signal;

use super::ToolContext;
use super::capabilities::refused;

/// The catalog name, which a cursor is bound to.
const TOOL: &str = "ravel_describe_data";

/// Families per page: the service's own page bound.
pub const FAMILIES_PER_PAGE: usize = 100;

/// The keyset order the position is taken under.
const ORDER_BY: &str = "name";

/// The BLAKE3 derive-key context of the cursor's argument hash.
const ARGUMENT_HASH_CONTEXT: &str = "ravel-mcp ravel_describe_data arguments v1";

/// The warning every page carries about what `budget.actual` leaves out.
pub const UNCOUNTED_READS_WARNING: &str = "budget.actual counts the catalog resolve only; \
     the metric metadata record and tenant configuration reads are not counted";

/// Run the tool. An argument the body cannot use, an unknown signal, or a
/// cursor that does not redeem is a failure envelope that reaches no store.
pub async fn run<B: QueryBackend>(args: Value, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let input = match DescribeDataInput::deserialize(args) {
        Ok(input) => input,
        Err(err) => return refused(invalid(format!("{TOOL} arguments: {err}")), ctx),
    };
    let signal = match parse_signal(&input.signal) {
        Ok(signal) => signal,
        Err(failure) => return refused(failure, ctx),
    };
    let argument_hash = argument_hash(&input.signal);
    match input.cursor {
        None => first_page(signal, &input.signal, argument_hash, ctx, backend).await,
        Some(token) => {
            later_page(signal, &input.signal, &token, argument_hash, ctx, backend).await
        }
    }
}

async fn first_page<B: QueryBackend>(
    signal: Signal,
    name: &str,
    argument_hash: [u8; 32],
    ctx: &ToolContext<'_>,
    backend: &B,
) -> Envelope {
    let request = DescribeRequest {
        signal,
        after_name: None,
        limit: FAMILIES_PER_PAGE,
        now_ns: ctx.now_ns,
    };
    let outcome = match backend.describe_data(ctx.tenant_hash, &request).await {
        Ok(outcome) => outcome,
        Err(failure) => return refused(failure, ctx),
    };
    let mut rows = vec![table_row(name, &outcome)];
    rows.extend(column_rows(&outcome));
    rows.extend(indexed_key_rows(&outcome));
    rows.extend(family_rows(&outcome));
    let draft = Draft::new(
        base(name, &outcome, rows, ctx),
        Ok(signal),
        outcome.pending_erasure.clone(),
        outcome.declared_column_set.clone(),
        outcome.ingest_watermark_hour,
        outcome.resolved_at_ns,
    );
    let scope = PinScope {
        tenant: ctx.tenant_hash,
        range_start_ns: 0,
        range_end_ns: i64::MAX,
        min_commit_watermark: Vec::new(),
    };
    finish(draft, &scope, argument_hash, ctx)
}

async fn later_page<B: QueryBackend>(
    signal: Signal,
    name: &str,
    token: &str,
    argument_hash: [u8; 32],
    ctx: &ToolContext<'_>,
    backend: &B,
) -> Envelope {
    let pending = match Cursor::redeem(
        token,
        ctx.cursor_key,
        ctx.tenant_hash,
        TOOL,
        &argument_hash,
        ctx.now_ns,
        ctx.protection_horizon_ns,
    ) {
        Ok(pending) => pending,
        Err(error) => return refused(error.failure(), ctx),
    };
    let after_name = match after_name(pending.position()) {
        Ok(after_name) => after_name,
        Err(error) => return refused(error.failure(), ctx),
    };
    let request = DescribeRequest {
        signal,
        after_name,
        limit: FAMILIES_PER_PAGE,
        now_ns: pending.resolve_at_ns(),
    };
    let outcome = match backend.describe_data(ctx.tenant_hash, &request).await {
        Ok(outcome) => outcome,
        Err(failure) => return refused(failure, ctx),
    };
    let observed_erasure: Vec<(Signal, _)> = outcome
        .pending_erasure
        .iter()
        .map(|predicate| (signal, predicate.clone()))
        .collect();
    let confirmed = match pending.confirm(
        ctx.now_ns,
        &observed_erasure,
        &outcome.declared_column_set,
    ) {
        Ok(confirmed) => confirmed,
        Err(error) => return refused(error.failure(), ctx),
    };

    let mut envelope = base(name, &outcome, family_rows(&outcome), ctx);
    envelope.warnings.push(format!(
        "families_total: {} in the metadata record read for this page; the count \
         can differ between pages of one sequence",
        outcome.families_total
    ));
    let draft = Draft::new(
        envelope,
        Ok(signal),
        outcome.pending_erasure.clone(),
        outcome.declared_column_set.clone(),
        outcome.ingest_watermark_hour,
        outcome.resolved_at_ns,
    )
    .pinned_to(&confirmed);
    finish(draft, &PinScope::of(&confirmed), argument_hash, ctx)
}

/// The envelope every page shares before its rows are capped.
fn base(name: &str, outcome: &DescribeOutcome, rows: Vec<Row>, ctx: &ToolContext<'_>) -> Envelope {
    let mut envelope = Envelope {
        data: crate::envelope::Data {
            columns: vec![
                column("kind", "string"),
                column("name", "string"),
                column("value", "json"),
            ],
            row_count: rows.len() as u64,
            rows,
        },
        ..Envelope::default()
    };
    envelope.scope.signal = name.to_string();
    envelope.scope.table = outcome.table.clone();
    envelope.scope.time_range = Some(TimeRange {
        start_ns: "0".to_string(),
        end_ns: i64::MAX.to_string(),
    });
    envelope.scope.order_by = vec![ORDER_BY.to_string()];
    envelope.coverage.complete = true;
    envelope.accuracy.exact = true;
    envelope.presentation.max_rows = FAMILIES_PER_PAGE as u32;
    envelope.presentation.row_cap_hit = outcome.more_families;
    envelope.budget = Budget {
        effective: AnyJson(json!({
            "max_response_bytes": ctx.budgets.max_response_bytes,
            "deadline_ms": ctx.budgets.deadline.as_millis() as u64,
        })),
        actual: AnyJson(json!({
            "bytes_scanned": outcome.accounting.total_s3_bytes(),
            "store_requests": outcome.accounting.total_s3_requests(),
            "segments_read": outcome.accounting.segments_opened,
            "decompressed_bytes": outcome.accounting.decompressed_bytes,
        })),
        estimate: AnyJson(Value::Object(Map::new())),
        estimate_is_upper_envelope: false,
    };
    if let Some(reason) = &outcome.families_absent_reason {
        envelope.warnings.push(format!("families: {reason}"));
    }
    envelope.warnings.push(UNCOUNTED_READS_WARNING.to_string());
    envelope
}

/// Cap the page and mint its cursor. The family page is already bounded by
/// the service, so the row cap here is the page itself: only the byte cap can
/// cut it, and a cursor is minted when either cut it or the record held more
/// families than the page.
fn finish(
    draft: Draft,
    scope: &PinScope,
    argument_hash: [u8; 32],
    ctx: &ToolContext<'_>,
) -> Envelope {
    let mut budgets = ctx.budgets;
    budgets.max_rows = u32::try_from(draft.envelope.data.rows.len()).unwrap_or(u32::MAX);
    draft.finish_page(&budgets, scope, PageOrder::Total, |pins, last| {
        let position = CursorPosition::Keyset {
            tuple: family_name(last).map(|name| name.as_bytes().to_vec()).unwrap_or_default(),
            order_by: vec![ORDER_BY.to_string()],
        };
        pins.cursor(TOOL, argument_hash, position, i64::MAX).mint(
            ctx.cursor_key,
            ctx.now_ns,
            ctx.protection_horizon_ns,
        )
    })
}

/// The family name a row carries, when it is a `family` row. A page cut
/// before its first family row resumes at the first family: no family name
/// is empty, so the empty tuple names that position unambiguously.
fn family_name(row: &Row) -> Option<&str> {
    match (row.first(), row.get(1)) {
        (Some(Cell::Str(kind)), Some(Cell::Str(name))) if kind == "family" => Some(name),
        _ => None,
    }
}

/// The keyset lower bound a cursor's position names: `None` before the
/// first family. Anything this tool does not mint is
/// [`CursorError::Invalid`], since the MAC already vouched for the token.
fn after_name(position: &CursorPosition) -> Result<Option<String>, CursorError> {
    match position {
        CursorPosition::Keyset { tuple, order_by } if order_by == &[ORDER_BY] => {
            if tuple.is_empty() {
                Ok(None)
            } else {
                String::from_utf8(tuple.clone())
                    .map(Some)
                    .map_err(|_| CursorError::Invalid)
            }
        }
        _ => Err(CursorError::Invalid),
    }
}

fn table_row(name: &str, outcome: &DescribeOutcome) -> Row {
    let coverage = match outcome.coverage {
        Some(window) => json!({
            "min_event_ts_ns": window.min_event_ts_ns.to_string(),
            "max_event_ts_ns": window.max_event_ts_ns.to_string(),
        }),
        None => Value::Null,
    };
    row(
        "table",
        &outcome.table,
        json!({
            "signal": name,
            "segments": outcome.segments,
            "committed_rows": outcome.committed_rows.to_string(),
            "committed_rows_note": "summed from commit records: counted before duplicate \
                                    collapse and before pending erasure",
            "coverage": coverage,
            "families_total": outcome.families_total,
            "families_absent_reason": outcome.families_absent_reason,
        }),
    )
}

fn column_rows(outcome: &DescribeOutcome) -> impl Iterator<Item = Row> + '_ {
    let fixed = outcome.fixed_columns.iter().map(|column| (column, false));
    let declared = outcome.declared_columns.iter().map(|column| (column, true));
    fixed.chain(declared).map(|(column, declared)| {
        row(
            "column",
            &column.name,
            json!({
                "type": column.data_type,
                "nullable": column.nullable,
                "declared": declared,
            }),
        )
    })
}

fn indexed_key_rows(outcome: &DescribeOutcome) -> impl Iterator<Item = Row> + '_ {
    outcome.indexed_keys.iter().map(|key| {
        row(
            "indexed_key",
            key,
            json!({ "source": outcome.indexed_keys_source }),
        )
    })
}

fn family_rows(outcome: &DescribeOutcome) -> Vec<Row> {
    outcome
        .families
        .iter()
        .map(|family| {
            row(
                "family",
                &family.name,
                json!({
                    "type": family.kind,
                    "unit": family.unit,
                    "help": family.help,
                }),
            )
        })
        .collect()
}

fn row(kind: &str, name: &str, value: Value) -> Row {
    let value = match value {
        Value::Object(map) => Cell::Map(map),
        other => Cell::Str(other.to_string()),
    };
    vec![Cell::Str(kind.to_string()), Cell::Str(name.to_string()), value]
}

fn column(name: &str, r#type: &str) -> Column {
    Column {
        name: name.to_string(),
        r#type: r#type.to_string(),
    }
}

fn parse_signal(name: &str) -> Result<Signal, Failure> {
    match name {
        "metrics" => Ok(Signal::Metrics),
        "logs" => Ok(Signal::Logs),
        "traces" => Ok(Signal::Spans),
        other => Err(invalid(format!(
            "unknown signal {other:?}: expected \"metrics\", \"logs\" or \"traces\""
        ))),
    }
}

/// The hash a cursor binds its arguments to: the canonical signal name, the
/// only argument besides the cursor itself.
fn argument_hash(signal: &str) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(ARGUMENT_HASH_CONTEXT);
    hasher.update(signal.as_bytes());
    *hasher.finalize().as_bytes()
}

fn invalid(message: String) -> Failure {
    Failure {
        class: FailureClass::InvalidArgument,
        message,
        counter: None,
    }
}
