//! `ravel_query_sql`: run one read-only SQL statement over one table, scoped
//! to a required `time_range`, and page its rows with a D5 keyset cursor
//! (ADR-1374 D2, D3, D4, D5).
//!
//! The statement is checked from its text alone before any backend call, in
//! this order: the one-table rule (`ravel_sql::refuse_several_tables`), the
//! read-only gate (`ravel_sql::validate_query`), and the page planner
//! (`ravel_sql::plan_page`). A planner refusal is `invalid_argument` naming
//! the reason, with two exceptions, which run unpaged so that a capped result
//! is `ok_bounded` with no cursor: a statement with no `ORDER BY` over a table
//! with no row identity, and a statement whose `ORDER BY` uses a value
//! computed from other rows that the planner does not prove non-null (`sum`,
//! `min`, `max`, `avg`, a window function other than `count`, a subquery).
//!
//! An order on `count(...)` pages (`PagePlan::order_from_other_rows`). A row
//! committed after a cursor was minted can change a count for a row already
//! served and move that row after the cursor, so such a walk pins the segment
//! set page one read ([`SegmentSetPin::Required`]): a later page is served
//! only over the same segments, and is otherwise `cursor_expired` with the
//! single next step `rerun_page_one`. A walk ordered by the row's own columns
//! is served over whatever segments its resolve returns
//! ([`SegmentSetPin::Advisory`]) and warns when they moved.
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
//! column set moved what page one's pin describes, or, for a `count` order,
//! when its segment set moved.
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
use crate::cursor::{
    Confirmed, Cursor, CursorError, CursorPosition, SEGMENT_SET_MOVED_WARNING, SegmentSetPin,
};
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
    /// Page one's plan, or why the statement runs unpaged.
    plan: Result<PagePlan, Unpaged>,
    argument_hash: [u8; 32],
}

/// Why a statement the planner gives no plan still runs, unpaged.
#[derive(Clone, Copy)]
enum Unpaged {
    /// No `ORDER BY`, over a table with no row identity
    /// (`PagePlanError::NoOrdering`).
    NoOrder,
    /// An `ORDER BY` term computed from other rows
    /// (`PagePlanError::OrderTermFromOtherRows`).
    OrderFromOtherRows,
}

impl Unpaged {
    fn of(error: &PagePlanError) -> Option<Unpaged> {
        match error {
            PagePlanError::NoOrdering { .. } => Some(Unpaged::NoOrder),
            PagePlanError::OrderTermFromOtherRows { .. } => Some(Unpaged::OrderFromOtherRows),
            _ => None,
        }
    }

    fn no_cursor(self) -> &'static str {
        match self {
            Unpaged::NoOrder => "the result has no order",
            Unpaged::OrderFromOtherRows => {
                "the result is ordered by a value computed from other rows"
            }
        }
    }

    fn next_step(self) -> NextStep {
        match self {
            Unpaged::NoOrder => step(
                "add_an_order_by",
                "the statement has no ORDER BY and its table has no row identity to page by, \
                 so the rest of the result has no cursor; order by non-null columns to page \
                 through it, or narrow time_range",
            ),
            Unpaged::OrderFromOtherRows => step(
                "order_by_the_rows_own_columns",
                "the ORDER BY uses a value computed from other rows that the statement does \
                 not prove non-null (sum, min, max, avg, a window function other than count, \
                 a subquery), so the rest of the result has no cursor; order by count(...) or \
                 by the row's own non-null columns to page, or narrow time_range",
            ),
        }
    }
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
            Ok(plan) => Ok(plan),
            Err(error) => match Unpaged::of(&error) {
                Some(unpaged) => Err(unpaged),
                None => return Err(plan_refusal(&error)),
            },
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
    let plan = match &call.plan {
        Ok(plan) => plan,
        Err(unpaged) => return unordered(call, *unpaged, ctx, backend).await,
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
    let pin = if plan.order_from_other_rows {
        SegmentSetPin::Required
    } else {
        SegmentSetPin::Advisory
    };
    let observed_segment_set = draft.segment_set_digest;
    let Confirmed {
        cursor: confirmed,
        segment_set_moved,
    } = match pending.confirm(
        ctx.now_ns,
        &observed_erasure,
        &declared,
        observed_segment_set,
        pin,
    ) {
        Ok(confirmed) => confirmed,
        Err(error @ CursorError::SegmentSetMoved) => {
            return refused_with(error.failure(), vec![rerun_page_one()], ctx);
        }
        Err(error) => return refused_with(error.failure(), Vec::new(), ctx),
    };
    let mut draft = draft.pinned_to(&confirmed, declared);
    if segment_set_moved {
        draft
            .envelope
            .warnings
            .push(SEGMENT_SET_MOVED_WARNING.to_string());
    }
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
    let (Ok(first), CursorPosition::Keyset { tuple, order_by }) = (&call.plan, position) else {
        return Err(CursorError::Invalid.failure());
    };
    if *order_by != rendered_order(first) {
        return Err(CursorError::Invalid.failure());
    }
    let tuple = decode_resume_tuple(tuple).map_err(|_| CursorError::Invalid.failure())?;
    match ravel_sql::plan_page(&call.query, Some(&ResumePosition::new(tuple))) {
        Ok(plan) => Ok(plan),
        Err(
            PagePlanError::ResumeArity { .. }
            | PagePlanError::ResumeValueTypeMismatch { .. }
            | PagePlanError::NonFiniteResumeValue,
        ) => Err(CursorError::Invalid.failure()),
        Err(error) => Err(plan_refusal(&error).0),
    }
}

/// A statement with no position to resume from, run as written, so a capped
/// result is `ok_bounded` with no cursor and the step `unpaged` names.
async fn unordered<B: QueryBackend>(
    call: &Call,
    unpaged: Unpaged,
    ctx: &ToolContext<'_>,
    backend: &B,
) -> Envelope {
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
        |_, _, _| Err(NoCursor::because(unpaged.no_cursor().to_string())),
    );
    if envelope.status == Status::OkBounded {
        envelope.next_steps.push(unpaged.next_step());
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
    let order_from_other_rows = plan.order_from_other_rows;
    draft.finish_page(&ctx.budgets, scope, order, |pins, _, tuple| {
        if order_from_other_rows && pins.segment_set_digest.is_none() {
            return Err(NoCursor::because(
                "the order depends on values computed from other rows and the backend \
                 reported no segment set to pin"
                    .to_string(),
            ));
        }
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

/// What a walk ordered by a value computed from other rows does once its
/// segment set moved: start over.
fn rerun_page_one() -> NextStep {
    step(
        "rerun_page_one",
        "a row, a compaction or an erasure changed the segments this walk reads since page \
         one, and its order depends on values computed from other rows, so no later page can \
         be exact; run page one again (a time_range that ends before the most recent ingest \
         stays stable), or order by the row's own columns to page through a moving window",
    )
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
    plan.order_by
        .iter()
        .map(|term| term.column.clone())
        .collect()
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_query::erasure::ErasurePredicate;
    use serde_json::json;

    use super::*;
    use crate::cursor::{CURSOR_KEY_LEN, CursorKey};
    use crate::envelope::{Cell, Column};
    use crate::tools::tests::{Stub, block_on, test_context};

    const TOTAL: &str = "SELECT * FROM samples ORDER BY ts";
    const GROUPED: &str = "SELECT * FROM logs ORDER BY ts";
    const START_NS: i64 = 3_600_000_000_000;
    const END_NS: i64 = 3 * 3_600_000_000_000;
    /// The later pages' clock, a minute after page one's.
    const LATER_NS: i64 = END_NS + 60_000_000_000;
    /// The segment set digest every draft reports unless a test moves it.
    const SEGMENTS: [u8; 32] = [0x5Eu8; 32];
    /// A digest other than [`SEGMENTS`]: the segment set after a move.
    const MOVED: [u8; 32] = [0x77u8; 32];
    const COUNT_ORDER: &str = "SELECT ts, count(*) AS c FROM samples GROUP BY ts ORDER BY c, ts";

    fn key() -> CursorKey {
        CursorKey::from_process_secret([0x51u8; CURSOR_KEY_LEN])
    }

    fn args(query: &str, cursor: Option<&str>) -> Value {
        let mut args = json!({
            "query": query,
            "time_range": {"start_ns": START_NS.to_string(), "end_ns": END_NS.to_string()},
        });
        if let Some(cursor) = cursor {
            args["cursor"] = json!(cursor);
        }
        args
    }

    /// The `samples` order tuple of row `i`: its `ts` and its `series_id`.
    fn sample(i: u8) -> Vec<ResumeValue> {
        vec![
            ResumeValue::TimestampNanos(START_NS + i64::from(i) * 1_000),
            ResumeValue::FixedSizeBinary(vec![i; 16]),
        ]
    }

    /// The `logs` order tuple of a row at `ts` offset `i`.
    fn log_at(i: u8) -> Vec<ResumeValue> {
        vec![ResumeValue::TimestampNanos(START_NS + i64::from(i) * 1_000)]
    }

    /// What the executor answers for one page: one row per tuple, under
    /// `signal`'s snapshot with `erasure` pending, resolved at `resolve_at_ns`.
    fn draft_of(
        signal: Signal,
        tuples: Vec<Option<Vec<ResumeValue>>>,
        erasure: Vec<ErasurePredicate>,
        resolve_at_ns: i64,
    ) -> Draft {
        let mut envelope = Envelope::default();
        envelope.ids.query_id = "q".to_owned();
        envelope.ids.audit_ref = "a".to_owned();
        envelope.data.columns = vec![Column {
            name: "row".to_owned(),
            r#type: "string".to_owned(),
        }];
        envelope.data.rows = tuples
            .iter()
            .map(|tuple| vec![Cell::Str(format!("{tuple:?}"))])
            .collect();
        envelope.data.row_count = tuples.len() as u64;
        Draft::new(
            envelope,
            Ok(signal),
            erasure,
            Vec::new(),
            Some(2),
            Some(SEGMENTS),
            resolve_at_ns,
        )
        .with_order_tuples(tuples)
    }

    fn samples(rows: &[u8]) -> Draft {
        draft_of(
            Signal::Metrics,
            rows.iter().map(|&i| Some(sample(i))).collect(),
            Vec::new(),
            END_NS,
        )
    }

    fn logs(rows: &[u8]) -> Draft {
        draft_of(
            Signal::Logs,
            rows.iter().map(|&i| Some(log_at(i))).collect(),
            Vec::new(),
            END_NS,
        )
    }

    /// A context at page one's clock (`END_NS`) with a two-row cap.
    fn context(key: &CursorKey) -> ToolContext<'_> {
        let mut ctx = test_context(key);
        ctx.now_ns = END_NS;
        ctx.budgets.max_rows = 2;
        ctx
    }

    fn later(key: &CursorKey) -> ToolContext<'_> {
        let mut ctx = context(key);
        ctx.now_ns = LATER_NS;
        ctx
    }

    fn statement(query: &str, resume: Option<Vec<ResumeValue>>) -> String {
        ravel_sql::plan_page(query, resume.map(ResumePosition::new).as_ref())
            .expect("the statement pages")
            .statement
    }

    fn actions(envelope: &Envelope) -> Vec<&str> {
        envelope
            .next_steps
            .iter()
            .map(|step| step.action.as_str())
            .collect()
    }

    fn failure_class(envelope: &Envelope) -> FailureClass {
        assert_eq!(envelope.status, Status::Error, "{envelope:?}");
        envelope
            .failure
            .as_ref()
            .expect("an error carries one")
            .class
    }

    /// A three-page walk under a total order: each page runs the planned
    /// statement with the executor's cap at `max_rows` (so it returns the
    /// k+1 row), keeps two rows and resumes after the second, and the last
    /// page is `ok` with no cursor. Every page reports one snapshot id.
    ///
    /// FLIP: resuming from the k+1 row instead of the last kept row makes
    /// page two's statement start after row 3 and skip it.
    /// FLIP: dropping the planner's `series_id` tiebreak from the order
    /// columns encodes a one-value tuple the second page's plan refuses.
    #[test]
    fn a_total_order_walks_three_pages_by_keyset() {
        let key = key();
        let stub = Stub::default();
        stub.sql
            .borrow_mut()
            .extend([samples(&[1, 2, 3]), samples(&[3, 4, 5]), samples(&[5])]);

        let one = block_on(run(args(TOTAL, None), &context(&key), &stub));
        assert_eq!(one.status, Status::OkPage, "{one:?}");
        assert_eq!(one.data.rows.len(), 2);
        assert_eq!(one.scope.order_by, ["\"ts\" ASC", "\"series_id\" ASC"]);
        let cursor = one.presentation.cursor.clone().expect("page one's cursor");

        let two = block_on(run(args(TOTAL, Some(&cursor)), &later(&key), &stub));
        assert_eq!(two.status, Status::OkPage, "{two:?}");
        assert_eq!(two.data.rows.len(), 2);
        let cursor = two.presentation.cursor.clone().expect("page two's cursor");

        let three = block_on(run(args(TOTAL, Some(&cursor)), &later(&key), &stub));
        assert_eq!(three.status, Status::Ok, "{three:?}");
        assert_eq!(three.data.rows.len(), 1);
        assert_eq!(three.presentation.cursor, None);

        let executed = stub.executed.borrow();
        let statements: Vec<&str> = executed.iter().map(|c| c.request.sql.as_str()).collect();
        assert_eq!(
            statements,
            [
                statement(TOTAL, None),
                statement(TOTAL, Some(sample(2))),
                statement(TOTAL, Some(sample(4))),
            ]
        );
        for call in executed.iter() {
            assert_eq!(call.order_columns, ["ts", "series_id"]);
            assert_eq!(call.request.max_rows, Some(2));
            assert!(call.request.row_window);
            assert_eq!(
                (call.request.window.start_ns, call.request.window.end_ns),
                (START_NS, END_NS)
            );
        }
        assert_eq!(one.visibility.snapshot_id, two.visibility.snapshot_id);
        assert_eq!(one.visibility.snapshot_id, three.visibility.snapshot_id);
        assert!(two.visibility.pinned && three.visibility.pinned);
    }

    /// A later page resolves at the instant page one minted its cursor at and
    /// carries its own clock as the request's `now_ns`, which is what the
    /// audit record and the deadline read.
    ///
    /// FLIP: resolving a later page at its own clock reads a snapshot page
    /// one never saw (`resolve_at_ns` would be `LATER_NS`).
    /// FLIP: sending page one's instant as `now_ns` audits the later call at
    /// a time it did not run.
    #[test]
    fn a_later_page_resolves_at_the_mint_instant_and_runs_at_its_own_clock() {
        let key = key();
        let stub = Stub::default();
        stub.sql
            .borrow_mut()
            .extend([samples(&[1, 2, 3]), samples(&[3])]);

        let one = block_on(run(args(TOTAL, None), &context(&key), &stub));
        let cursor = one.presentation.cursor.expect("page one's cursor");
        let two = block_on(run(args(TOTAL, Some(&cursor)), &later(&key), &stub));
        assert_eq!(two.status, Status::Ok, "{two:?}");

        let executed = stub.executed.borrow();
        assert_eq!(
            executed
                .iter()
                .map(|c| (c.resolve_at_ns, c.request.now_ns))
                .collect::<Vec<_>>(),
            [(END_NS, END_NS), (END_NS, LATER_NS)]
        );
    }

    /// Under an order that is not total, the k+1 row decides where the page
    /// ends: rows 2 and 3 share a `ts`, so the group they open is held back
    /// whole and the cursor resumes after row 1, the last complete group.
    ///
    /// FLIP: resuming after the page's last kept row (row 2) puts row 3,
    /// equal to it under the order, on no page.
    /// FLIP: comparing tuples by position only (`same_group` always false)
    /// keeps both rows and mints after row 2.
    #[test]
    fn a_grouped_page_ends_before_an_open_group() {
        let key = key();
        let stub = Stub::default();
        stub.sql
            .borrow_mut()
            .extend([logs(&[1, 2, 2]), logs(&[2, 2])]);

        let one = block_on(run(args(GROUPED, None), &context(&key), &stub));
        assert_eq!(one.status, Status::OkPage, "{one:?}");
        assert_eq!(one.data.rows.len(), 1);
        assert_eq!(one.presentation.rows_omitted, 2);
        assert!(
            one.warnings
                .iter()
                .any(|w| w.contains("the order is not total")),
            "{:?}",
            one.warnings
        );
        let cursor = one.presentation.cursor.clone().expect("page one's cursor");

        let two = block_on(run(args(GROUPED, Some(&cursor)), &later(&key), &stub));
        assert_eq!(two.status, Status::Ok, "{two:?}");
        assert_eq!(two.data.rows.len(), 2);
        assert_eq!(
            stub.executed.borrow()[1].request.sql,
            statement(GROUPED, Some(log_at(1)))
        );
    }

    /// When the k+1 row continues the only group on the page, no complete
    /// group fits: the page is `ok_bounded` with no cursor and says to narrow
    /// the range.
    ///
    /// FLIP: minting after the open group's last kept row resumes inside the
    /// group and loses its third row.
    #[test]
    fn no_complete_group_fits_is_bounded_with_no_cursor() {
        let key = key();
        let stub = Stub::default();
        stub.sql.borrow_mut().push_back(logs(&[4, 4, 4]));

        let page = block_on(run(args(GROUPED, None), &context(&key), &stub));

        assert_eq!(page.status, Status::OkBounded, "{page:?}");
        assert_eq!(page.presentation.cursor, None);
        assert_eq!(actions(&page), ["narrow_time_range"]);
    }

    /// A NaN order value, a missing order tuple, and a `Str` key long enough
    /// to put the cursor over its 4096-byte bound each end the page
    /// `ok_bounded` with no cursor and a next step, never a failure.
    ///
    /// The planner refuses every float order term, so a NaN reaches this
    /// tool only through a tuple the backend read; the boundary is held
    /// here so that a backend type the planner did not foresee degrades to
    /// a bounded page rather than an `internal` error.
    ///
    /// FLIP: propagating the encode error as a failure turns a page of good
    /// rows into an error envelope.
    /// FLIP: handing the over-bound token to `finish_page` makes it an
    /// `internal` failure (`Envelope::fit` refuses a cursor over the bound).
    #[test]
    fn a_boundary_with_no_cursor_position_is_bounded_with_a_next_step() {
        let long = "k".repeat(CURSOR_BOUND);
        let cases: [(&str, Option<Vec<ResumeValue>>, &str); 3] = [
            (
                "nan",
                Some(vec![
                    ResumeValue::Float(f64::NAN),
                    ResumeValue::FixedSizeBinary(vec![2; 16]),
                ]),
                "have no cursor position",
            ),
            ("unread", None, "could not be read"),
            (
                "over-bound",
                Some(vec![
                    ResumeValue::Str(long),
                    ResumeValue::FixedSizeBinary(vec![2; 16]),
                ]),
                "-byte bound",
            ),
        ];
        for (name, last, reason) in cases {
            let key = key();
            let stub = Stub::default();
            stub.sql.borrow_mut().push_back(draft_of(
                Signal::Metrics,
                vec![Some(sample(1)), last, Some(sample(3))],
                Vec::new(),
                END_NS,
            ));

            let page = block_on(run(args(TOTAL, None), &context(&key), &stub));

            assert_eq!(page.status, Status::OkBounded, "{name}: {page:?}");
            assert_eq!(page.presentation.cursor, None, "{name}");
            assert_eq!(page.data.rows.len(), 2, "{name}");
            assert_eq!(actions(&page), ["narrow_time_range"], "{name}");
            assert!(
                page.warnings
                    .first()
                    .is_some_and(|w| w.starts_with("no cursor:") && w.contains(reason)),
                "{name}: {:?}",
                page.warnings
            );
        }
    }

    /// A statement with no `ORDER BY` over a table with no row identity runs
    /// as written, unpaged: capped, it is `ok_bounded` with no cursor and a
    /// next step to add an ordering.
    ///
    /// FLIP: treating `NoOrdering` as a refusal answers `invalid_argument`
    /// for a statement the executor can run.
    #[test]
    fn a_capped_statement_with_no_order_is_bounded() {
        let key = key();
        let stub = Stub::default();
        stub.sql
            .borrow_mut()
            .push_back(draft_of(Signal::Logs, Vec::new(), Vec::new(), END_NS));
        stub.sql.borrow_mut()[0].envelope.data.rows = vec![vec![Cell::Int(1)]; 3];
        stub.sql.borrow_mut()[0].envelope.data.row_count = 3;

        let page = block_on(run(args("SELECT * FROM logs", None), &context(&key), &stub));

        assert_eq!(page.status, Status::OkBounded, "{page:?}");
        assert_eq!(page.presentation.cursor, None);
        assert_eq!(page.data.rows.len(), 2);
        assert_eq!(actions(&page), ["add_an_order_by"]);
        let executed = stub.executed.borrow();
        assert_eq!(executed[0].request.sql, "SELECT * FROM logs");
        assert!(executed[0].order_columns.is_empty());
    }

    /// A statement ordered by a value computed from other rows that the
    /// planner does not prove non-null runs as written, unpaged: capped, it
    /// is `ok_bounded` with no cursor and a next step naming the orders that
    /// page. Uncapped, it is `ok` with every row. A `count` written in the
    /// `ORDER BY` with no projected column defined as it is one of these.
    ///
    /// FLIP: sending `OrderTermFromOtherRows` to `plan_refusal` answers
    /// `invalid_argument` with `rewrite_the_statement`, and the backend is
    /// never called.
    #[test]
    fn a_statement_ordered_by_an_aggregate_runs_unpaged() {
        let queries = [
            "SELECT series_id, sum(value) AS s FROM samples GROUP BY series_id ORDER BY s",
            "SELECT ts, min(series_id) AS m FROM samples GROUP BY ts ORDER BY m, ts",
            "SELECT ts, series_id, row_number() OVER () AS r FROM samples ORDER BY r, ts, series_id",
            "SELECT ts, count(*) FROM samples GROUP BY ts ORDER BY count(*), ts",
        ];
        for query in queries {
            for (rows, status) in [(3, Status::OkBounded), (2, Status::Ok)] {
                let key = key();
                let stub = Stub::default();
                stub.sql.borrow_mut().push_back(draft_of(
                    Signal::Metrics,
                    Vec::new(),
                    Vec::new(),
                    END_NS,
                ));
                stub.sql.borrow_mut()[0].envelope.data.rows = vec![vec![Cell::Int(1)]; rows];
                stub.sql.borrow_mut()[0].envelope.data.row_count = rows as u64;

                let page = block_on(run(args(query, None), &context(&key), &stub));

                assert_eq!(page.status, status, "{query}: {page:?}");
                assert_eq!(page.presentation.cursor, None, "{query}");
                assert_eq!(page.data.rows.len(), 2, "{query}");
                let executed = stub.executed.borrow();
                assert_eq!(executed[0].request.sql, query);
                assert!(executed[0].order_columns.is_empty(), "{query}");
                if status == Status::OkBounded {
                    assert_eq!(actions(&page), ["order_by_the_rows_own_columns"], "{query}");
                    let detail = &page.next_steps[0].detail;
                    assert!(
                        detail.contains("computed from other rows")
                            && detail.contains("order by count(...)")
                            && detail.contains("the row's own non-null columns"),
                        "{query}: {detail}"
                    );
                } else {
                    assert!(actions(&page).is_empty(), "{query}");
                }
            }
        }
    }

    /// The `COUNT_ORDER` tuple of a group at `ts` offset `i` counting `c`.
    fn counted(c: i64, i: u8) -> Vec<ResumeValue> {
        vec![
            ResumeValue::Int(c),
            ResumeValue::TimestampNanos(START_NS + i64::from(i) * 1_000),
        ]
    }

    fn count_page(groups: &[(i64, u8)], digest: Option<[u8; 32]>) -> Draft {
        let mut draft = draft_of(
            Signal::Metrics,
            groups.iter().map(|&(c, i)| Some(counted(c, i))).collect(),
            Vec::new(),
            END_NS,
        );
        draft.segment_set_digest = digest;
        draft
    }

    fn decoded(envelope: &Envelope, key: &CursorKey) -> Cursor {
        let token = envelope.presentation.cursor.as_deref().expect("a cursor");
        Cursor::decode(token, key).expect("decodes")
    }

    /// An order on `count(...)` pages, and its cursor pins page one's segment
    /// set as required: a later page whose resolve reports another digest is
    /// `cursor_expired` with `rerun_page_one` as its one next step and no
    /// rows, and one reporting page one's digest is served, pinned, and mints
    /// a cursor carrying page one's digest. A page one whose backend reported
    /// no segment set mints no cursor.
    ///
    /// FLIP H: passing `SegmentSetPin::Advisory` for every walk serves the
    /// moved page.
    /// FLIP I: a `pinned_to` that keeps the draft's own digest is shown in
    /// `an_own_column_walk_warns_on_every_page_after_a_move`.
    #[test]
    fn a_count_order_pages_only_over_page_ones_segment_set() {
        let plan = ravel_sql::plan_page(COUNT_ORDER, None).expect("a count order plans");
        assert!(plan.order_from_other_rows);
        assert_eq!(order_columns(&plan), ["c", "ts"]);

        let key = key();
        let stub = Stub::default();
        stub.sql.borrow_mut().extend([
            count_page(&[(1, 1), (2, 2), (3, 3)], Some(SEGMENTS)),
            count_page(&[(3, 3), (4, 4), (5, 5)], Some(MOVED)),
            count_page(&[(3, 3), (4, 4), (5, 5)], Some(SEGMENTS)),
        ]);

        let one = block_on(run(args(COUNT_ORDER, None), &context(&key), &stub));
        assert_eq!(one.status, Status::OkPage, "{one:?}");
        assert_eq!(one.data.rows.len(), 2);
        assert_eq!(decoded(&one, &key).segment_set_digest, Some(SEGMENTS));
        let cursor = one.presentation.cursor.clone().expect("page one's cursor");

        let moved = block_on(run(args(COUNT_ORDER, Some(&cursor)), &later(&key), &stub));
        assert_eq!(failure_class(&moved), FailureClass::CursorExpired);
        assert_eq!(actions(&moved), ["rerun_page_one"]);
        assert_eq!(
            moved.next_steps[0].detail,
            "a row, a compaction or an erasure changed the segments this walk reads since \
             page one, and its order depends on values computed from other rows, so no later \
             page can be exact; run page one again (a time_range that ends before the most \
             recent ingest stays stable), or order by the row's own columns to page through \
             a moving window"
        );
        assert!(moved.data.rows.is_empty());

        let same = block_on(run(args(COUNT_ORDER, Some(&cursor)), &later(&key), &stub));
        assert_eq!(same.status, Status::OkPage, "{same:?}");
        assert!(same.visibility.pinned);
        assert_eq!(same.visibility.snapshot_id, one.visibility.snapshot_id);
        assert!(
            !same.warnings.contains(&SEGMENT_SET_MOVED_WARNING.to_owned()),
            "{:?}",
            same.warnings
        );
        assert_eq!(decoded(&same, &key).segment_set_digest, Some(SEGMENTS));
        assert_eq!(
            stub.executed.borrow()[2].request.sql,
            statement(COUNT_ORDER, Some(counted(2, 2)))
        );

        let unpinned = Stub::default();
        unpinned
            .sql
            .borrow_mut()
            .push_back(count_page(&[(1, 1), (2, 2), (3, 3)], None));
        let page = block_on(run(args(COUNT_ORDER, None), &context(&key), &unpinned));
        assert_eq!(page.status, Status::OkBounded, "{page:?}");
        assert_eq!(page.presentation.cursor, None);
        assert!(
            page.warnings.first().is_some_and(|w| w
                == "no cursor: the order depends on values computed from other rows and the \
                    backend reported no segment set to pin"),
            "{:?}",
            page.warnings
        );
    }

    /// A walk ordered by the row's own columns is served over a moved segment
    /// set with a warning, on page two and again on page three, because each
    /// page compares against page one's digest, not the previous page's.
    /// Over an unchanged set it carries no such warning.
    ///
    /// FLIP I: a `pinned_to` that keeps the draft's own digest mints page
    /// two's cursor over `MOVED`, so page three, also over `MOVED`, compares
    /// equal and does not warn.
    /// FLIP J: dropping the warning serves page two silently.
    #[test]
    fn an_own_column_walk_warns_on_every_page_after_a_move() {
        let key = key();
        let stub = Stub::default();
        let mut two = samples(&[3, 4, 5]);
        two.segment_set_digest = Some(MOVED);
        let mut three = samples(&[5, 6, 7]);
        three.segment_set_digest = Some(MOVED);
        stub.sql
            .borrow_mut()
            .extend([samples(&[1, 2, 3]), two, three]);

        let one = block_on(run(args(TOTAL, None), &context(&key), &stub));
        assert!(!one.warnings.contains(&SEGMENT_SET_MOVED_WARNING.to_owned()));
        let cursor = one.presentation.cursor.clone().expect("page one's cursor");

        let two = block_on(run(args(TOTAL, Some(&cursor)), &later(&key), &stub));
        assert_eq!(two.status, Status::OkPage, "{two:?}");
        assert_eq!(two.data.rows.len(), 2);
        assert!(
            two.warnings.contains(&SEGMENT_SET_MOVED_WARNING.to_owned()),
            "{:?}",
            two.warnings
        );
        let cursor = two.presentation.cursor.clone().expect("page two's cursor");

        let three = block_on(run(args(TOTAL, Some(&cursor)), &later(&key), &stub));
        assert_eq!(three.status, Status::OkPage, "{three:?}");
        assert!(
            three
                .warnings
                .contains(&SEGMENT_SET_MOVED_WARNING.to_owned()),
            "{:?}",
            three.warnings
        );
        assert_eq!(decoded(&three, &key).segment_set_digest, Some(SEGMENTS));

        let quiet = Stub::default();
        quiet
            .sql
            .borrow_mut()
            .extend([samples(&[1, 2, 3]), samples(&[3])]);
        let one = block_on(run(args(TOTAL, None), &context(&key), &quiet));
        let cursor = one.presentation.cursor.clone().expect("page one's cursor");
        let two = block_on(run(args(TOTAL, Some(&cursor)), &later(&key), &quiet));
        assert_eq!(two.status, Status::Ok, "{two:?}");
        assert!(!two.warnings.contains(&SEGMENT_SET_MOVED_WARNING.to_owned()));
    }

    /// Every statement-level refusal comes back before the backend is
    /// called: the stub has no answer queued and panics if reached.
    ///
    /// A float order term is `invalid_argument` rather than `unsupported`:
    /// the planner refuses it because a NaN or infinite value has no cursor
    /// position, so the statement as written cannot be paged without losing
    /// rows, and the caller can page the same rows by ordering on a
    /// representable column. That is a rewrite of the caller's argument, the
    /// D4 meaning of `invalid_argument`.
    ///
    /// FLIP: dropping the `refuse_several_tables` check leaves the planner's
    /// cross-signal refusal, with `rewrite_the_statement` in place of
    /// `split_the_statement`.
    /// FLIP: mapping `OrderTermNotRepresentable` through `validation_refusal`
    /// would send a PromQL hint for a SQL statement.
    #[test]
    fn statement_refusals_are_answered_before_the_backend() {
        let cases: [(&str, FailureClass, &[&str], &str); 4] = [
            (
                "SELECT * FROM samples JOIN logs ON samples.ts = logs.ts ORDER BY samples.ts",
                FailureClass::InvalidArgument,
                &["split_the_statement"],
                "",
            ),
            (
                "DELETE FROM samples",
                FailureClass::Unsupported,
                &[],
                "read-only SELECT",
            ),
            (
                "SELECT * FROM samples ORDER BY ts LIMIT 5",
                FailureClass::InvalidArgument,
                &["rewrite_the_statement"],
                "cannot page this statement",
            ),
            (
                "SELECT * FROM samples ORDER BY value",
                FailureClass::InvalidArgument,
                &["rewrite_the_statement"],
                "`value`",
            ),
        ];
        for (query, class, steps, message) in cases {
            let key = key();
            let stub = Stub::default();
            let envelope = block_on(run(args(query, None), &context(&key), &stub));
            assert_eq!(failure_class(&envelope), class, "{query}: {envelope:?}");
            assert_eq!(actions(&envelope), steps, "{query}");
            let text = &envelope.failure.as_ref().expect("failure").message;
            assert!(text.contains(message), "{query}: {text}");
            assert!(stub.executed.borrow().is_empty(), "{query}");
        }
    }

    /// `evidence_ref` is not served, and a value for it is `unsupported`
    /// before the backend.
    ///
    /// FLIP: ignoring the field answers rows a caller believes are scoped by
    /// evidence they are not scoped by.
    #[test]
    fn an_evidence_ref_is_unsupported() {
        let key = key();
        let stub = Stub::default();
        let mut call = args(TOTAL, None);
        call["evidence_ref"] = json!("e");

        let envelope = block_on(run(call, &context(&key), &stub));

        assert_eq!(failure_class(&envelope), FailureClass::Unsupported);
        assert!(stub.executed.borrow().is_empty());
    }

    /// A newer erasure observed by a later page's resolve expires the cursor
    /// rather than serving rows the pin no longer describes.
    ///
    /// FLIP: skipping `confirm` serves page two from a snapshot whose
    /// erasure set moved.
    #[test]
    fn a_newer_erasure_expires_the_cursor() {
        let key = key();
        let stub = Stub::default();
        let erased = draft_of(
            Signal::Metrics,
            vec![Some(sample(3))],
            vec![ErasurePredicate::windowless(vec![(
                "host".to_owned(),
                "a".to_owned(),
            )])],
            LATER_NS,
        );
        stub.sql.borrow_mut().extend([samples(&[1, 2, 3]), erased]);

        let one = block_on(run(args(TOTAL, None), &context(&key), &stub));
        let cursor = one.presentation.cursor.expect("page one's cursor");
        let two = block_on(run(args(TOTAL, Some(&cursor)), &later(&key), &stub));

        assert_eq!(failure_class(&two), FailureClass::CursorExpired);
        assert!(two.data.rows.is_empty());
    }

    /// A cursor is bound to its statement and its `time_range`: presented
    /// with another of either it is `cursor_invalid`, before the backend.
    ///
    /// FLIP: leaving the window out of the argument hash resumes page one's
    /// keyset under another range.
    /// FLIP: leaving the statement out resumes one statement's position in
    /// another's order.
    #[test]
    fn a_cursor_for_another_query_or_time_range_is_refused() {
        let key = key();
        let stub = Stub::default();
        stub.sql.borrow_mut().push_back(samples(&[1, 2, 3]));
        let one = block_on(run(args(TOTAL, None), &context(&key), &stub));
        let cursor = one.presentation.cursor.expect("page one's cursor");

        let other_query = args("SELECT * FROM samples ORDER BY ts DESC", Some(&cursor));
        let mut other_window = args(TOTAL, Some(&cursor));
        other_window["time_range"]["start_ns"] = json!((START_NS + 1).to_string());
        for (name, call) in [("query", other_query), ("time_range", other_window)] {
            let envelope = block_on(run(call, &later(&key), &stub));
            assert_eq!(
                failure_class(&envelope),
                FailureClass::CursorInvalid,
                "{name}"
            );
        }
        assert_eq!(stub.executed.borrow().len(), 1);
    }

    /// A cursor whose tuple does not have the order terms' types is
    /// `cursor_invalid`, the class of a cursor bound to other inputs, before
    /// the backend. Page one mints it from a `series_id` eight bytes wide
    /// where the column is sixteen.
    ///
    /// FLIP: dropping `ResumeValueTypeMismatch` from `resume_plan`'s cursor
    /// arm sends it to `plan_refusal`, which answers `invalid_argument`.
    #[test]
    fn a_cursor_whose_tuple_has_another_type_is_invalid() {
        let key = key();
        let stub = Stub::default();
        let narrow = |i: u8| {
            Some(vec![
                ResumeValue::TimestampNanos(START_NS + i64::from(i) * 1_000),
                ResumeValue::FixedSizeBinary(vec![i; 8]),
            ])
        };
        stub.sql.borrow_mut().push_back(draft_of(
            Signal::Metrics,
            vec![narrow(1), narrow(2), narrow(3)],
            Vec::new(),
            END_NS,
        ));
        let one = block_on(run(args(TOTAL, None), &context(&key), &stub));
        let cursor = one.presentation.cursor.expect("page one's cursor");

        let two = block_on(run(args(TOTAL, Some(&cursor)), &later(&key), &stub));
        assert_eq!(failure_class(&two), FailureClass::CursorInvalid);
        assert_eq!(stub.executed.borrow().len(), 1);
    }
}
