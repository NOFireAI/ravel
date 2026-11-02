//! `ravel_query_sql`: run one read-only SQL statement over one table, scoped
//! to a required `time_range`, and page its rows with a D5 keyset cursor
//! (ADR-1374 D2, D3, D4, D5).
//!
//! The statement is checked from its text alone before any backend call, in
//! this order: the one-table rule (`ravel_sql::refuse_several_tables`), the
//! read-only gate (`ravel_sql::validate_query`), and the page planner
//! (`ravel_sql::plan_page`). A planner refusal is `invalid_argument` naming
//! the reason, with one exception: a statement with no `ORDER BY` over a
//! table with no row identity runs unpaged, and a capped result is
//! `ok_bounded` with no cursor.
//!
//! `time_range` is applied as a half-open row filter (`row_window`), not
//! only as the segment-listing bound, so a row outside it is excluded even
//! when its segment overlaps the window.
//!
//! A page runs the planned statement with the executor's row cap at
//! `max_rows`, so the executor returns at most `max_rows + 1` rows and the
//! extra row tells the page whether its last kept row ends a group. Under a
//! total order (the statement's `ORDER BY` plus the tiebreak the planner
//! appends) the cursor resumes after the page's last row. Under an order
//! that is not total, the trailing group of rows equal under the order is
//! held back whole, and the cursor resumes after the last complete group; a
//! page on which no complete group fits carries no cursor.
//!
//! The cursor position is the last kept row's typed order values, read from
//! the result's arrays (never from rendered cells) and encoded with
//! [`crate::resume_tuple`], together with the effective `ORDER BY`. A later
//! page resolves at page one's instant, audits at its own clock, and is
//! refused as `cursor_expired` when a newer erasure or a changed declared
//! column set moved what page one's pin describes.
//!
//! Three boundaries have no cursor position, and each ends the page
//! `ok_bounded` with no cursor and a `next_steps` entry rather than a
//! failure: a last row whose order values could not be read, an order value
//! that is NaN or infinite, and a cursor longer than the envelope's cursor
//! bound.

use ravel_query::RequestBudgets;
use ravel_sql::{
    PagePlan, PagePlanError, ResumePosition, ResumeValue, SqlRequest, ValidationError,
};
use serde::Deserialize;
use serde_json::Value;

use crate::catalog::QuerySqlInput;
use crate::cursor::{Cursor, CursorError, CursorPosition};
use crate::draft::{Draft, NoCursor, PageOrder, PinScope};
use crate::envelope::{
    CURSOR_BOUND, Envelope, Failure, FailureClass, NextStep, Status, serialized_str_len,
};
use crate::resume_tuple::{decode_resume_tuple, encode_resume_tuple};
use crate::service::QueryBackend;
use ravel_types::Signal;

use super::ToolContext;
use super::capabilities::refused_with;
use super::explain_query::sql_failure;

/// The catalog name, which a cursor is bound to.
const TOOL: &str = "ravel_query_sql";

/// The BLAKE3 derive-key context of the cursor's argument hash.
const ARGUMENT_HASH_CONTEXT: &str = "ravel-mcp ravel_query_sql arguments v1";

type Refusal = (Failure, Vec<NextStep>);

/// Run the tool. Every refusal is an envelope; only a statement that passes
/// the one-table rule, validation, and the page planner reaches `backend`.
pub async fn run<B: QueryBackend>(args: Value, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let call = match Call::from_args(args, ctx) {
        Ok(call) => call,
        Err((failure, next_steps)) => return refused_with(failure, next_steps, ctx),
    };
    match &call.cursor {
        None => first_page(&call, ctx, backend).await,
        Some(token) => later_page(&call, token, ctx, backend).await,
    }
}

/// One validated call.
struct Call {
    query: String,
    window: ravel_types::TimeRange,
    cursor: Option<String>,
    /// Page one's plan, or `None` for a statement with no ordering to page
    /// by, which runs unpaged.
    plan: Option<PagePlan>,
    argument_hash: [u8; 32],
}

impl Call {
    fn from_args(args: Value, ctx: &ToolContext<'_>) -> Result<Call, Refusal> {
        for name in ["query", "time_range"] {
            if args.get(name).is_none_or(Value::is_null) {
                return Err((
                    failure(
                        FailureClass::MissingArgument,
                        format!("{TOOL} requires {name}"),
                    ),
                    Vec::new(),
                ));
            }
        }
        let input = QuerySqlInput::deserialize(args).map_err(|err| {
            (
                failure(
                    FailureClass::InvalidArgument,
                    format!("{TOOL} arguments: {err}"),
                ),
                Vec::new(),
            )
        })?;
        if input.evidence_ref.is_some() {
            return Err((
                failure(
                    FailureClass::Unsupported,
                    format!("evidence_ref is not served by {TOOL} in this build"),
                ),
                Vec::new(),
            ));
        }
        let window = crate::time::window(&input.time_range).map_err(|f| (f, Vec::new()))?;
        ravel_sql::refuse_several_tables(&input.query).map_err(|error| {
            (
                sql_failure(&error),
                vec![step(
                    "split_the_statement",
                    "query one table per statement; a cursor pins one table's snapshot",
                )],
            )
        })?;
        validate(&input.query)?;
        let plan = match ravel_sql::plan_page(&input.query, None) {
            Ok(plan) => Some(plan),
            Err(PagePlanError::NoOrdering { .. }) => None,
            Err(error) => return Err(plan_refusal(&error)),
        };
        let argument_hash = argument_hash(&input.query, &window, ctx.budgets.max_rows);
        Ok(Call {
            query: input.query,
            window,
            cursor: input.cursor,
            plan,
            argument_hash,
        })
    }

    /// The request one page executes: `sql` under the call's window as a row
    /// filter, its row cap, and its lowered budgets.
    fn request(&self, sql: String, ctx: &ToolContext<'_>) -> SqlRequest {
        let query = &ctx.budgets.query;
        SqlRequest {
            sql,
            window: self.window,
            min_tokens: Vec::new(),
            now_ns: ctx.now_ns,
            deadline: ctx.budgets.deadline,
            row_window: true,
            max_rows: Some(ctx.budgets.max_rows as usize),
            budgets: Some(RequestBudgets {
                max_bytes_scanned: Some(query.max_bytes_scanned),
                max_store_requests: Some(query.max_store_requests),
                max_segments: Some(query.max_segments),
            }),
        }
    }

    fn scope(&self, ctx: &ToolContext<'_>) -> PinScope {
        PinScope {
            tenant: ctx.tenant_hash,
            range_start_ns: self.window.start_ns,
            range_end_ns: self.window.end_ns,
            min_commit_watermark: Vec::new(),
        }
    }
}

async fn first_page<B: QueryBackend>(call: &Call, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let Some(plan) = &call.plan else {
        return unordered(call, ctx, backend).await;
    };
    let request = call.request(plan.statement.clone(), ctx);
    let columns = order_columns(plan);
    let draft = match backend
        .sql_execute(ctx.tenant_hash, &request, ctx.now_ns, &columns)
        .await
    {
        Ok(draft) => draft,
        Err(failure) => return refused_with(failure, Vec::new(), ctx),
    };
    finish(draft, plan, &call.scope(ctx), call.argument_hash, ctx)
}

async fn later_page<B: QueryBackend>(
    call: &Call,
    token: &str,
    ctx: &ToolContext<'_>,
    backend: &B,
) -> Envelope {
    let pending = match Cursor::redeem(
        token,
        ctx.cursor_key,
        ctx.tenant_hash,
        TOOL,
        &call.argument_hash,
        ctx.now_ns,
        ctx.protection_horizon_ns,
    ) {
        Ok(pending) => pending,
        Err(error) => return refused_with(error.failure(), Vec::new(), ctx),
    };
    let plan = match resume_plan(call, pending.position()) {
        Ok(plan) => plan,
        Err(failure) => return refused_with(failure, Vec::new(), ctx),
    };
    let request = call.request(plan.statement.clone(), ctx);
    let columns = order_columns(&plan);
    let draft = match backend
        .sql_execute(ctx.tenant_hash, &request, pending.resolve_at_ns(), &columns)
        .await
    {
        Ok(draft) => draft,
        Err(failure) => return refused_with(failure, Vec::new(), ctx),
    };
    let signal = pending.signal();
    let observed_erasure: Vec<(Signal, _)> = draft
        .pending_erasure
        .iter()
        .map(|predicate| (signal, predicate.clone()))
        .collect();
    let declared = draft.declared_columns.clone();
    let confirmed = match pending.confirm(ctx.now_ns, &observed_erasure, &declared) {
        Ok(confirmed) => confirmed,
        Err(error) => return refused_with(error.failure(), Vec::new(), ctx),
    };
    let draft = draft.pinned_to(&confirmed, declared);
    finish(
        draft,
        &plan,
        &PinScope::of(&confirmed),
        call.argument_hash,
        ctx,
    )
}

/// The plan of a later page: the cursor's position, decoded, under the
/// statement's own effective order. A position this tool could not have
/// minted for these arguments is `cursor_invalid`.
fn resume_plan(call: &Call, position: &CursorPosition) -> Result<PagePlan, Failure> {
    let (Some(first), CursorPosition::Keyset { tuple, order_by }) = (&call.plan, position) else {
        return Err(CursorError::Invalid.failure());
    };
    if *order_by != rendered_order(first) {
        return Err(CursorError::Invalid.failure());
    }
    let tuple = decode_resume_tuple(tuple).map_err(|_| CursorError::Invalid.failure())?;
    match ravel_sql::plan_page(&call.query, Some(&ResumePosition::new(tuple))) {
        Ok(plan) => Ok(plan),
        Err(PagePlanError::ResumeArity { .. } | PagePlanError::NonFiniteResumeValue) => {
            Err(CursorError::Invalid.failure())
        }
        Err(error) => Err(plan_refusal(&error).0),
    }
}

/// A statement with no `ORDER BY` over a table with no row identity: run as
/// written, with no position to resume from, so a capped result is
/// `ok_bounded` with no cursor.
async fn unordered<B: QueryBackend>(call: &Call, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    if call.cursor.is_some() {
        return refused_with(CursorError::Invalid.failure(), Vec::new(), ctx);
    }
    let request = call.request(call.query.clone(), ctx);
    let draft = match backend
        .sql_execute(ctx.tenant_hash, &request, ctx.now_ns, &[])
        .await
    {
        Ok(draft) => draft,
        Err(failure) => return refused_with(failure, Vec::new(), ctx),
    };
    let mut envelope = draft.finish_page(
        &ctx.budgets,
        &call.scope(ctx),
        PageOrder::Partial,
        |_, _, _| Err(NoCursor::because("the result has no order".to_string())),
    );
    if envelope.status == Status::OkBounded {
        envelope.next_steps.push(step(
            "add_an_order_by",
            "the statement has no ORDER BY and its table has no row identity to page by, \
             so the rest of the result has no cursor; order by non-null columns to page \
             through it, or narrow time_range",
        ));
    }
    envelope
}

/// Cap the page and mint its cursor under the plan's order.
fn finish(
    mut draft: Draft,
    plan: &PagePlan,
    scope: &PinScope,
    argument_hash: [u8; 32],
    ctx: &ToolContext<'_>,
) -> Envelope {
    let order_by = rendered_order(plan);
    draft.envelope.scope.order_by = order_by.clone();
    let narrow_groups = step(
        "narrow_time_range",
        "no group of rows equal under the ORDER BY fits one page; shorten time_range, \
         raise max_rows, or order by more columns so a whole group fits",
    );
    let order = match &plan.not_total {
        None => PageOrder::Total,
        Some(reason) => {
            draft.envelope.warnings.push(format!(
                "the order is not total ({reason}); pages break between groups of rows \
                 equal under the ORDER BY, never inside one"
            ));
            PageOrder::Grouped {
                same_group: &same_tuple,
                no_complete_group: &narrow_groups,
            }
        }
    };
    draft.finish_page(&ctx.budgets, scope, order, |pins, _, tuple| {
        let tuple = tuple.ok_or_else(|| {
            unmintable("the last row's ORDER BY values could not be read".to_string())
        })?;
        let encoded = encode_resume_tuple(tuple).map_err(|error| {
            unmintable(format!(
                "the last row's ORDER BY values have no cursor position: {error}"
            ))
        })?;
        let position = CursorPosition::Keyset {
            tuple: encoded,
            order_by,
        };
        let token = pins.cursor(TOOL, argument_hash, position, i64::MAX).mint(
            ctx.cursor_key,
            ctx.now_ns,
            ctx.protection_horizon_ns,
        )?;
        let len = serialized_str_len(&token);
        if len > CURSOR_BOUND {
            return Err(unmintable(format!(
                "the cursor after the last row is {len} bytes, over the {CURSOR_BOUND}-byte bound"
            )));
        }
        Ok(token)
    })
}

/// A capped page whose last row has no cursor position.
fn unmintable(reason: String) -> NoCursor {
    NoCursor {
        reason,
        next_step: Some(step(
            "narrow_time_range",
            "the rest of the result has no cursor; shorten time_range or add predicates \
             so the result fits one page, or order by shorter, finite columns",
        )),
    }
}

/// Whether two rows' order tuples are equal, floats by bit pattern.
fn same_tuple(a: &[ResumeValue], b: &[ResumeValue]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|pair| match pair {
            (ResumeValue::Float(a), ResumeValue::Float(b)) => a.to_bits() == b.to_bits(),
            (a, b) => a == b,
        })
}

fn order_columns(plan: &PagePlan) -> Vec<String> {
    plan.order_by.iter().map(|term| term.column.clone()).collect()
}

fn rendered_order(plan: &PagePlan) -> Vec<String> {
    plan.order_by.iter().map(|term| term.render()).collect()
}

/// A write or any other non-query statement is `unsupported`; any other
/// validation failure is `validation` with the engine's text.
fn validate(sql: &str) -> Result<(), Refusal> {
    match ravel_sql::validate_query(sql) {
        Ok(()) => Ok(()),
        Err(error) => Err(validation_refusal(&error)),
    }
}

fn validation_refusal(error: &ValidationError) -> Refusal {
    match error {
        ValidationError::NotReadOnly { .. } | ValidationError::WriteInQuery { .. } => (
            failure(
                FailureClass::Unsupported,
                format!("{TOOL} runs a read-only SELECT only: {error}"),
            ),
            Vec::new(),
        ),
        _ => (
            failure(FailureClass::Validation, error.to_string()),
            vec![step(
                "ravel_query_promql",
                "ravel_query_sql takes SQL only; a PromQL expression runs through \
                 ravel_query_promql",
            )],
        ),
    }
}

/// A statement the page planner refuses is the caller's to rewrite:
/// `invalid_argument` naming the planner's reason.
fn plan_refusal(error: &PagePlanError) -> Refusal {
    if let PagePlanError::Invalid(error) = error {
        return validation_refusal(error);
    }
    (
        failure(
            FailureClass::InvalidArgument,
            format!("{TOOL} cannot page this statement: {error}"),
        ),
        vec![step(
            "rewrite_the_statement",
            "order by non-null, non-float columns the statement projects, and leave the \
             row limit to max_rows",
        )],
    )
}

/// The hash a cursor binds its arguments to: the statement, the window and
/// the effective `max_rows`.
fn argument_hash(query: &str, window: &ravel_types::TimeRange, max_rows: u32) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(ARGUMENT_HASH_CONTEXT);
    hasher.update(&(query.len() as u64).to_le_bytes());
    hasher.update(query.as_bytes());
    hasher.update(&window.start_ns.to_le_bytes());
    hasher.update(&window.end_ns.to_le_bytes());
    hasher.update(&max_rows.to_le_bytes());
    *hasher.finalize().as_bytes()
}

fn failure(class: FailureClass, message: String) -> Failure {
    Failure {
        class,
        message,
        counter: None,
    }
}

fn step(action: &str, detail: &str) -> NextStep {
    NextStep {
        action: action.to_string(),
        detail: detail.to_string(),
    }
}
