//! `ravel_search_logs`: typed log search compiled to one SQL `SELECT` over
//! the `logs` table, scoped to a required `time_range`, ordered by
//! `(ts, observed_ts)` and paged with a D5 keyset cursor (ADR-1374 D2, D5,
//! and its search-logs amendment).
//!
//! Every argument is checked before any backend call: `time_range` is
//! required, `severity` must be a listed level, `trace_id` must be 32 hex
//! characters, and each predicate's op must take the value it was given. A
//! call with predicates then reads the tenant's declared columns and indexed
//! keys through `describe_data`, at the call's own clock, and refuses a key
//! that is neither a declared column nor `trace_id` with `trace_id` indexed
//! as `invalid_argument` before any statement runs. An indexed key other
//! than `trace_id` is accepted only as the declared column it names. The
//! lookup and the statement are two backend calls under the call's one
//! deadline, and the page's `budget.actual` counts the statement only, which
//! [`UNCOUNTED_LOOKUP_WARNING`] says on every page of such a call.
//!
//! The filters compile to `SELECT * FROM logs WHERE ... ORDER BY "ts",
//! "observed_ts"`, which must pass `ravel_sql::refuse_several_tables`,
//! `ravel_sql::validate_query` and `ravel_sql::plan_page` like any caller
//! statement. Every caller value reaches the statement text through
//! [`render_literal`] and every key through [`render_identifier`]:
//!
//! - a predicate on a declared column compares the column with a literal of
//!   its declared type; on `trace_id` (an indexed key) with a 16-byte
//!   literal;
//! - `has_word` is `has_word("body", '<term>')`;
//! - `severity` is `"severity_num" >= n`, `n` the level's lowest OTLP
//!   severity number;
//! - `trace_id` is `"trace_id" = X'<hex>'`.
//!
//! `time_range` is applied as a half-open row filter (`row_window`).
//!
//! The order `(ts, observed_ts)` is not total, so a page never ends inside a
//! group of rows equal on that pair: the trailing open group is held back
//! whole, and a page on which no complete group fits returns the rows it
//! has, `ok_bounded`, with no cursor and a next step to narrow `time_range`.
//!
//! # Output
//!
//! One data row per log record, in page order. The first two columns are
//! added by this tool:
//!
//! - `__ravel_group`: the row's attribute group, numbered from 0 in order of
//!   first appearance on the page. A group is the rows whose `attrs` maps
//!   have the same set of keys.
//! - `__ravel_group_attrs`: on the first row of each group, the attributes
//!   (key and value) every row of the group on this page carries, listed
//!   once; null on the group's other rows.
//!
//! Both names carry the `__ravel_` prefix of the `logs` schema's synthetic
//! names, so a declared attribute column named `group` or `group_attrs`
//! keeps its own column beside them.
//!
//! The `logs` columns follow in table order, with `attrs` holding only the
//! row's attributes not in its group's `__ravel_group_attrs`. Groups and
//! hoisting are computed per page over the rows under `max_rows`; when the
//! byte cap or a held-back group drops rows from the end, the shared set is
//! still shared by every kept row, but it may be smaller than what the kept
//! rows alone share.

use std::collections::{BTreeMap, BTreeSet};

use ravel_query::RequestBudgets;
use ravel_sql::{DeclaredType, PagePlan, PagePlanError, ResumePosition, ResumeValue, SqlRequest};
use serde::Deserialize;
use serde_json::Value;

use crate::catalog::{Predicate, PredicateOp, PredicateValue, SearchLogsInput};
use crate::cursor::{
    Confirmed, Cursor, CursorError, CursorPosition, SEGMENT_SET_MOVED_WARNING, SegmentSetPin,
};
use crate::draft::{Draft, NoCursor, PageOrder, PinScope};
use crate::envelope::{
    CURSOR_BOUND, Cell, Column, Data, Envelope, Failure, FailureClass, NextStep, serialized_str_len,
};
use crate::resume_tuple::{decode_resume_tuple, encode_resume_tuple};
use crate::service::{DescribeRequest, QueryBackend};
use ravel_types::Signal;

use super::ToolContext;
use super::capabilities::refused_with;
use super::explain_query::sql_failure;

/// The catalog name, which a cursor is bound to.
const TOOL: &str = "ravel_search_logs";

/// The BLAKE3 derive-key context of the cursor's argument hash.
const ARGUMENT_HASH_CONTEXT: &str = "ravel-mcp ravel_search_logs arguments v1";

/// The severity levels `severity` accepts, in either case, and the lowest
/// OTLP severity number of each.
pub const SEVERITY_LEVELS: [(&str, u8); 6] = [
    ("trace", 1),
    ("debug", 5),
    ("info", 9),
    ("warn", 13),
    ("error", 17),
    ("fatal", 21),
];

/// The indexed key a predicate may name besides the declared columns.
const TRACE_ID: &str = "trace_id";

/// The output column numbering each row's attribute group.
const GROUP_COLUMN: &str = "__ravel_group";

/// The output column listing each group's shared attributes once.
const GROUP_ATTRS_COLUMN: &str = "__ravel_group_attrs";

/// The warning a page of a call with predicates carries: the declared-column
/// lookup those predicates need is a service call of its own, and its reads
/// are not in the page's `budget.actual`.
pub const UNCOUNTED_LOOKUP_WARNING: &str = "budget.actual counts the search statement only; \
     the declared-column lookup the predicates needed ran under the same deadline and its \
     reads are not counted";

type Refusal = (Failure, Vec<NextStep>);

/// Run the tool. Every refusal is an envelope; only `describe_data` (for a
/// call with predicates) runs before the statement is compiled and checked.
pub async fn run<B: QueryBackend>(args: Value, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let input = match Input::from_args(args) {
        Ok(input) => input,
        Err((failure, next_steps)) => return refused_with(failure, next_steps, ctx),
    };
    let keys = if input.predicates.is_empty() {
        Keys::default()
    } else {
        match Keys::described(ctx, backend).await {
            Ok(keys) => keys,
            Err(failure) => return refused_with(failure, Vec::new(), ctx),
        }
    };
    let call = match Call::compile(input, &keys, ctx) {
        Ok(call) => call,
        Err((failure, next_steps)) => return refused_with(failure, next_steps, ctx),
    };
    match &call.cursor {
        None => first_page(&call, ctx, backend).await,
        Some(token) => later_page(&call, token, ctx, backend).await,
    }
}

/// A SQL literal a caller value becomes.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Literal {
    Text(String),
    Int(i64),
    Bool(bool),
    Bytes(Vec<u8>),
}

/// The one function that turns a caller value into statement text. A string
/// is single-quoted with every quote doubled, which is the whole escape
/// grammar of a bare `'...'` literal in the dialect `ravel_sql` parses (no
/// backslash escapes); bytes are an `X'...'` literal of hex digits only.
fn render_literal(literal: &Literal) -> String {
    match literal {
        Literal::Text(text) => format!("'{}'", text.replace('\'', "''")),
        Literal::Int(n) => n.to_string(),
        Literal::Bool(true) => "TRUE".to_string(),
        Literal::Bool(false) => "FALSE".to_string(),
        Literal::Bytes(bytes) => format!("X'{}'", hex(bytes)),
    }
}

/// A column name as a double-quoted identifier, every quote doubled.
fn render_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The arguments, checked without any backend call.
struct Input {
    predicates: Vec<Predicate>,
    has_word: Option<String>,
    severity: Option<u8>,
    trace_id: Option<[u8; 16]>,
    window: ravel_types::TimeRange,
    cursor: Option<String>,
}

impl Input {
    fn from_args(args: Value) -> Result<Input, Refusal> {
        if args.get("time_range").is_none_or(Value::is_null) {
            return Err(refusal(
                FailureClass::MissingArgument,
                format!("{TOOL} requires time_range"),
            ));
        }
        let input = SearchLogsInput::deserialize(args).map_err(|err| {
            refusal(
                FailureClass::InvalidArgument,
                format!("{TOOL} arguments: {err}"),
            )
        })?;
        if input.evidence_ref.is_some() {
            return Err(refusal(
                FailureClass::Unsupported,
                format!("evidence_ref is not served by {TOOL} in this build"),
            ));
        }
        let window = crate::time::window(&input.time_range).map_err(|f| (f, Vec::new()))?;
        let severity = input.severity.as_deref().map(severity_number).transpose()?;
        let trace_id = input
            .trace_id
            .as_deref()
            .map(|raw| trace_id_bytes("trace_id", raw))
            .transpose()?;
        // A term with no letter or digit tokenizes to no word, which
        // `has_word` matches on every row.
        if input
            .has_word
            .as_deref()
            .is_some_and(|term| !term.chars().any(char::is_alphanumeric))
        {
            return Err(invalid(
                "has_word must hold at least one letter or digit".to_string(),
            ));
        }
        for predicate in &input.predicates {
            check_arity(predicate)?;
        }
        Ok(Input {
            predicates: input.predicates,
            has_word: input.has_word,
            severity,
            trace_id,
            window,
            cursor: input.cursor,
        })
    }
}

fn severity_number(level: &str) -> Result<u8, Refusal> {
    SEVERITY_LEVELS
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(level))
        .map(|&(_, number)| number)
        .ok_or_else(|| {
            invalid(format!(
                "severity {level:?} is not one of trace, debug, info, warn, error, fatal"
            ))
        })
}

fn trace_id_bytes(field: &str, raw: &str) -> Result<[u8; 16], Refusal> {
    decode_hex(raw)
        .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
        .ok_or_else(|| invalid(format!("{field} {raw:?} is not 32 hexadecimal characters")))
}

/// `eq` and `ne` take one string, `in` a non-empty list, `exists` nothing.
fn check_arity(predicate: &Predicate) -> Result<(), Refusal> {
    let fits = match (predicate.op, &predicate.value) {
        (PredicateOp::Eq | PredicateOp::Ne, Some(PredicateValue::One(_))) => true,
        (PredicateOp::In, Some(PredicateValue::Many(values))) => !values.is_empty(),
        (PredicateOp::Exists, None) => true,
        _ => false,
    };
    if fits {
        return Ok(());
    }
    Err(arity_refusal(predicate))
}

fn arity_refusal(predicate: &Predicate) -> Refusal {
    invalid(format!(
        "predicate on {:?}: eq and ne take one string value, in a non-empty list of \
         strings, exists no value",
        predicate.key
    ))
}

/// How a predicate key's values parse.
#[derive(Clone, Copy)]
enum KeyKind {
    Declared(DeclaredType),
    TraceId,
}

impl KeyKind {
    fn literal(self, key: &str, raw: &str) -> Result<Literal, Refusal> {
        match self {
            KeyKind::Declared(ty) => declared_literal(key, ty, raw),
            KeyKind::TraceId => trace_id_bytes(key, raw).map(|id| Literal::Bytes(id.to_vec())),
        }
    }
}

/// The keys a predicate may name: the tenant's declared columns, by type,
/// and whether `trace_id` is an indexed key.
#[derive(Default)]
struct Keys {
    declared: BTreeMap<String, DeclaredType>,
    trace_id_indexed: bool,
}

impl Keys {
    async fn described<B: QueryBackend>(
        ctx: &ToolContext<'_>,
        backend: &B,
    ) -> Result<Keys, Failure> {
        let request = DescribeRequest {
            signal: Signal::Logs,
            after_name: None,
            limit: 1,
            resolve_at_ns: ctx.now_ns,
            caller_now_ns: ctx.now_ns,
        };
        let outcome = backend.describe_data(ctx.tenant_hash, &request).await?;
        Ok(Keys {
            declared: outcome
                .declared_column_set
                .into_iter()
                .map(|column| (column.key, column.ty))
                .collect(),
            trace_id_indexed: outcome.indexed_keys.iter().any(|key| key == TRACE_ID),
        })
    }

    /// The condition `predicate` compiles to.
    fn condition(&self, predicate: &Predicate) -> Result<String, Refusal> {
        let key = &predicate.key;
        let kind = match self.declared.get(key) {
            Some(ty) => KeyKind::Declared(*ty),
            None if key == TRACE_ID && self.trace_id_indexed => KeyKind::TraceId,
            None => {
                return Err((
                    failure(
                        FailureClass::InvalidArgument,
                        format!(
                            "predicate key {key:?} is neither a declared column of the logs \
                             table nor trace_id with trace_id indexed"
                        ),
                    ),
                    vec![step(
                        "ravel_describe_data",
                        "call ravel_describe_data with signal logs: a predicate may name a \
                         column row marked declared, or trace_id when it is an indexed_key row",
                    )],
                ));
            }
        };
        let column = render_identifier(key);
        let rendered = |raw: &str| {
            kind.literal(key, raw)
                .map(|literal| render_literal(&literal))
        };
        Ok(match (predicate.op, &predicate.value) {
            (PredicateOp::Eq, Some(PredicateValue::One(raw))) => {
                format!("{column} = {}", rendered(raw)?)
            }
            (PredicateOp::Ne, Some(PredicateValue::One(raw))) => {
                format!("{column} <> {}", rendered(raw)?)
            }
            (PredicateOp::In, Some(PredicateValue::Many(raws))) if !raws.is_empty() => {
                let literals = raws
                    .iter()
                    .map(|raw| rendered(raw))
                    .collect::<Result<Vec<_>, _>>()?;
                format!("{column} IN ({})", literals.join(", "))
            }
            (PredicateOp::Exists, None) => format!("{column} IS NOT NULL"),
            _ => return Err(arity_refusal(predicate)),
        })
    }
}

/// A predicate value as a literal of its declared column's type.
fn declared_literal(key: &str, ty: DeclaredType, raw: &str) -> Result<Literal, Refusal> {
    let mismatch = |expected: &str| {
        invalid(format!(
            "predicate on {key:?}: {raw:?} is not {expected}, the column's declared type"
        ))
    };
    match ty {
        DeclaredType::Str => Ok(Literal::Text(raw.to_string())),
        DeclaredType::I64 => raw
            .parse::<i64>()
            .map(Literal::Int)
            .map_err(|_| mismatch("a 64-bit integer")),
        DeclaredType::Bool => match raw {
            "true" => Ok(Literal::Bool(true)),
            "false" => Ok(Literal::Bool(false)),
            _ => Err(mismatch("true or false")),
        },
        DeclaredType::Bytes => decode_hex(raw)
            .map(Literal::Bytes)
            .ok_or_else(|| mismatch("bytes as hexadecimal")),
    }
}

/// One compiled, checked call.
struct Call {
    statement: String,
    conditions: Vec<String>,
    window: ravel_types::TimeRange,
    cursor: Option<String>,
    plan: PagePlan,
    argument_hash: [u8; 32],
    /// The call read the declared columns before its statement.
    described: bool,
}

impl Call {
    fn compile(input: Input, keys: &Keys, ctx: &ToolContext<'_>) -> Result<Call, Refusal> {
        let mut conditions = input
            .predicates
            .iter()
            .map(|predicate| keys.condition(predicate))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(term) = &input.has_word {
            conditions.push(format!(
                "{}({}, {})",
                ravel_sql::HAS_WORD_UDF,
                render_identifier("body"),
                render_literal(&Literal::Text(term.clone()))
            ));
        }
        if let Some(number) = input.severity {
            conditions.push(format!(
                "{} >= {}",
                render_identifier("severity_num"),
                render_literal(&Literal::Int(i64::from(number)))
            ));
        }
        if let Some(id) = input.trace_id {
            conditions.push(format!(
                "{} = {}",
                render_identifier(TRACE_ID),
                render_literal(&Literal::Bytes(id.to_vec()))
            ));
        }
        let statement = statement(&conditions);
        ravel_sql::refuse_several_tables(&statement)
            .map_err(|error| (sql_failure(&error), Vec::new()))?;
        ravel_sql::validate_query(&statement).map_err(|error| {
            refusal(
                FailureClass::Validation,
                format!("{TOOL} compiled a statement the engine refuses: {error}"),
            )
        })?;
        let plan = ravel_sql::plan_page(&statement, None).map_err(|error| plan_refusal(&error))?;
        let argument_hash = argument_hash(&statement, &input.window, ctx.budgets.max_rows);
        Ok(Call {
            described: !input.predicates.is_empty(),
            statement,
            conditions,
            window: input.window,
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

/// The search statement: every row of `logs` matching every condition, in
/// `(ts, observed_ts)` order.
fn statement(conditions: &[String]) -> String {
    let order = format!(
        " ORDER BY {}, {}",
        render_identifier("ts"),
        render_identifier("observed_ts")
    );
    if conditions.is_empty() {
        return format!("SELECT * FROM logs{order}");
    }
    format!(
        "SELECT * FROM logs WHERE {}{order}",
        conditions.join(" AND ")
    )
}

async fn first_page<B: QueryBackend>(call: &Call, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let request = call.request(call.plan.statement.clone(), ctx);
    let columns = order_columns(&call.plan);
    let draft = match backend
        .sql_execute(ctx.tenant_hash, &request, ctx.now_ns, &columns)
        .await
    {
        Ok(draft) => draft,
        Err(failure) => return refused_with(failure, Vec::new(), ctx),
    };
    finish(draft, call, &call.plan, &call.scope(ctx), ctx)
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
    let observed_segment_set = draft.segment_set_digest;
    let Confirmed {
        cursor: confirmed,
        segment_set_moved,
    } = match pending.confirm(
        ctx.now_ns,
        &observed_erasure,
        &declared,
        observed_segment_set,
        SegmentSetPin::Advisory,
    ) {
        Ok(confirmed) => confirmed,
        Err(error) => return refused_with(error.failure(), Vec::new(), ctx),
    };
    let mut draft = draft.pinned_to(&confirmed, declared);
    if segment_set_moved {
        draft
            .envelope
            .warnings
            .push(SEGMENT_SET_MOVED_WARNING.to_string());
    }
    finish(draft, call, &plan, &PinScope::of(&confirmed), ctx)
}

/// The plan of a later page: the cursor's position, decoded, under the
/// search's order. A position this tool could not have minted for these
/// arguments is `cursor_invalid`.
fn resume_plan(call: &Call, position: &CursorPosition) -> Result<PagePlan, Failure> {
    let CursorPosition::Keyset { tuple, order_by } = position else {
        return Err(CursorError::Invalid.failure());
    };
    if *order_by != rendered_order(&call.plan) {
        return Err(CursorError::Invalid.failure());
    }
    let tuple = decode_resume_tuple(tuple).map_err(|_| CursorError::Invalid.failure())?;
    match ravel_sql::plan_page(&call.statement, Some(&ResumePosition::new(tuple))) {
        Ok(plan) => Ok(plan),
        Err(
            PagePlanError::ResumeArity { .. }
            | PagePlanError::ResumeValueTypeMismatch { .. }
            | PagePlanError::NonFiniteResumeValue,
        ) => Err(CursorError::Invalid.failure()),
        Err(error) => Err(plan_refusal(&error).0),
    }
}

/// Group the rows, cap the page and mint its cursor after the last complete
/// group of rows equal on `(ts, observed_ts)`.
fn finish(
    mut draft: Draft,
    call: &Call,
    plan: &PagePlan,
    scope: &PinScope,
    ctx: &ToolContext<'_>,
) -> Envelope {
    let order_by = rendered_order(plan);
    group_by_attribute_set(&mut draft.envelope.data, ctx.budgets.max_rows as usize);
    draft.envelope.scope.signal = "logs".to_string();
    draft.envelope.scope.table = "logs".to_string();
    draft.envelope.scope.order_by = order_by.clone();
    draft.envelope.scope.predicates_applied = call.conditions.clone();
    if call.described {
        draft
            .envelope
            .warnings
            .push(UNCOUNTED_LOOKUP_WARNING.to_string());
    }
    let narrow_groups = step(
        "narrow_time_range",
        "more rows share one (ts, observed_ts) pair than fit one page; shorten time_range \
         or add predicates so the group fits, or raise max_rows",
    );
    let order = match &plan.not_total {
        None => PageOrder::Total,
        Some(_) => PageOrder::Grouped {
            same_group: &same_tuple,
            no_complete_group: &narrow_groups,
        },
    };
    let argument_hash = call.argument_hash;
    draft.finish_page(&ctx.budgets, scope, order, |pins, _, tuple| {
        let tuple = tuple.ok_or_else(|| {
            unmintable("the last row's (ts, observed_ts) could not be read".to_string())
        })?;
        let encoded = encode_resume_tuple(tuple).map_err(|error| {
            unmintable(format!(
                "the last row's (ts, observed_ts) has no cursor position: {error}"
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

/// Prepend the [`GROUP_COLUMN`] and [`GROUP_ATTRS_COLUMN`] columns and hoist
/// each group's shared attributes out of `attrs`, over the first `considered`
/// rows. A row past them is one the row cap drops, and keeps null in both new
/// columns.
fn group_by_attribute_set(data: &mut Data, considered: usize) {
    let attrs_at = data
        .columns
        .iter()
        .position(|column| column.name == "attrs");
    let attrs_type = attrs_at
        .and_then(|at| data.columns.get(at))
        .map_or_else(|| "json".to_string(), |column| column.r#type.clone());
    let considered = considered.min(data.rows.len());
    let attributes = |row: &[Cell]| match attrs_at.and_then(|at| row.get(at)) {
        Some(Cell::Map(map)) => map.clone(),
        _ => serde_json::Map::new(),
    };

    let mut key_sets: Vec<BTreeSet<String>> = Vec::new();
    let mut shared: Vec<serde_json::Map<String, Value>> = Vec::new();
    let mut group_of = Vec::with_capacity(considered);
    for row in data.rows.iter().take(considered) {
        let map = attributes(row);
        let keys: BTreeSet<String> = map.keys().cloned().collect();
        let group = match key_sets.iter().position(|set| *set == keys) {
            Some(group) => {
                if let Some(common) = shared.get_mut(group) {
                    common.retain(|key, value| map.get(key) == Some(value));
                }
                group
            }
            None => {
                key_sets.push(keys);
                shared.push(map);
                key_sets.len() - 1
            }
        };
        group_of.push(group);
    }

    let mut listed = vec![false; key_sets.len()];
    for (index, row) in data.rows.iter_mut().enumerate() {
        let Some(&group) = group_of.get(index) else {
            row.splice(0..0, [Cell::Null, Cell::Null]);
            continue;
        };
        let common = shared.get(group).cloned().unwrap_or_default();
        if let Some(Cell::Map(map)) = attrs_at.and_then(|at| row.get_mut(at)) {
            map.retain(|key, _| !common.contains_key(key));
        }
        let first = listed
            .get_mut(group)
            .is_some_and(|seen| !std::mem::replace(seen, true));
        let hoisted = if first { Cell::Map(common) } else { Cell::Null };
        row.splice(0..0, [Cell::Int(group as i128), hoisted]);
    }
    data.columns.splice(
        0..0,
        [
            Column {
                name: GROUP_COLUMN.to_string(),
                r#type: "Int64".to_string(),
            },
            Column {
                name: GROUP_ATTRS_COLUMN.to_string(),
                r#type: attrs_type,
            },
        ],
    );
}

/// A capped page whose last row has no cursor position.
fn unmintable(reason: String) -> NoCursor {
    NoCursor {
        reason,
        next_step: Some(step(
            "narrow_time_range",
            "the rest of the result has no cursor; shorten time_range or add predicates \
             so the result fits one page",
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

/// The planner refusing a compiled statement is reported with its reason;
/// a later page reaches it only through a cursor this tool did not mint.
fn plan_refusal(error: &PagePlanError) -> Refusal {
    refusal(
        FailureClass::InvalidArgument,
        format!("{TOOL} cannot page the compiled statement: {error}"),
    )
}

/// The hash a cursor binds its arguments to: the compiled statement (every
/// predicate, `has_word`, `severity` and `trace_id`), the window and the
/// effective `max_rows`.
fn argument_hash(statement: &str, window: &ravel_types::TimeRange, max_rows: u32) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(ARGUMENT_HASH_CONTEXT);
    hasher.update(&(statement.len() as u64).to_le_bytes());
    hasher.update(statement.as_bytes());
    hasher.update(&window.start_ns.to_le_bytes());
    hasher.update(&window.end_ns.to_le_bytes());
    hasher.update(&max_rows.to_le_bytes());
    *hasher.finalize().as_bytes()
}

fn decode_hex(raw: &str) -> Option<Vec<u8>> {
    let digits = raw.as_bytes();
    if digits.is_empty() || !digits.len().is_multiple_of(2) {
        return None;
    }
    digits
        .chunks_exact(2)
        .map(|pair| {
            let hi = char::from(pair[0]).to_digit(16)?;
            let lo = char::from(pair[1]).to_digit(16)?;
            u8::try_from(hi << 4 | lo).ok()
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn refusal(class: FailureClass, message: String) -> Refusal {
    (failure(class, message), Vec::new())
}

fn invalid(message: String) -> Refusal {
    refusal(FailureClass::InvalidArgument, message)
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
    use ravel_sql::DeclaredColumn;
    use serde_json::json;

    use super::*;
    use crate::cursor::{CURSOR_KEY_LEN, CursorKey};
    use crate::envelope::Status;
    use crate::service::DescribeOutcome;
    use crate::tools::tests::{Stub, block_on, test_context};

    const START_NS: i64 = 3_600_000_000_000;
    const END_NS: i64 = 3 * 3_600_000_000_000;
    /// The later pages' clock, a minute after page one's.
    const LATER_NS: i64 = END_NS + 60_000_000_000;
    const HOSTILE: &str = "x' OR 1=1; -- /*";

    fn key() -> CursorKey {
        CursorKey::from_process_secret([0x52u8; CURSOR_KEY_LEN])
    }

    fn args(extra: Value) -> Value {
        let mut args = json!({
            "time_range": {"start_ns": START_NS.to_string(), "end_ns": END_NS.to_string()},
        });
        if let (Some(args), Value::Object(extra)) = (args.as_object_mut(), extra) {
            args.extend(extra);
        }
        args
    }

    /// The logs table as `describe_data` reports it: `service.name` (Str)
    /// and `http.status` (I64) declared, `trace_id` indexed.
    fn described() -> DescribeOutcome {
        DescribeOutcome {
            table: "logs".to_string(),
            fixed_columns: Vec::new(),
            declared_columns: Vec::new(),
            declared_column_set: vec![
                DeclaredColumn::new("service.name", DeclaredType::Str),
                DeclaredColumn::new("http.status", DeclaredType::I64),
            ],
            indexed_keys: vec!["service.name".to_string(), "trace_id".to_string()],
            indexed_keys_source: "key_index".to_string(),
            families: Vec::new(),
            families_total: 0,
            more_families: false,
            families_absent_reason: None,
            ingest_watermark_hour: None,
            coverage: None,
            segments: 1,
            committed_rows: 3,
            pending_erasure: Vec::new(),
            resolved_at_ns: END_NS,
            accounting: Default::default(),
        }
    }

    /// One log row: its id, its `(ts, observed_ts)` offsets and its
    /// attributes.
    type Log = (&'static str, u8, u8, Value);

    /// What the executor answers for one page of `rows`.
    fn page(rows: &[Log]) -> Draft {
        let mut envelope = Envelope::default();
        envelope.ids.query_id = "q".to_owned();
        envelope.ids.audit_ref = "a".to_owned();
        envelope.data.columns = ["id", "ts", "observed_ts", "attrs"]
            .iter()
            .map(|name| Column {
                name: (*name).to_owned(),
                r#type: "test".to_owned(),
            })
            .collect();
        let at = |offset: u8| START_NS + i64::from(offset) * 1_000;
        envelope.data.rows = rows
            .iter()
            .map(|(id, ts, observed, attrs)| {
                let Value::Object(attrs) = attrs.clone() else {
                    panic!("attrs is an object");
                };
                vec![
                    Cell::Str((*id).to_owned()),
                    Cell::Int(i128::from(at(*ts))),
                    Cell::Int(i128::from(at(*observed))),
                    Cell::Map(attrs),
                ]
            })
            .collect();
        envelope.data.row_count = rows.len() as u64;
        let tuples = rows
            .iter()
            .map(|(_, ts, observed, _)| {
                Some(vec![
                    ResumeValue::TimestampNanos(at(*ts)),
                    ResumeValue::TimestampNanos(at(*observed)),
                ])
            })
            .collect();
        Draft::new(
            envelope,
            Ok(Signal::Logs),
            Vec::new(),
            Vec::new(),
            Some(2),
            None,
            END_NS,
        )
        .with_order_tuples(tuples)
    }

    fn plain(rows: &[(&'static str, u8, u8)]) -> Draft {
        let rows: Vec<Log> = rows
            .iter()
            .map(|&(id, ts, observed)| (id, ts, observed, json!({})))
            .collect();
        page(&rows)
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

    fn ids(envelope: &Envelope) -> Vec<String> {
        let at = envelope
            .data
            .columns
            .iter()
            .position(|column| column.name == "id")
            .expect("an id column");
        envelope
            .data
            .rows
            .iter()
            .map(|row| match &row[at] {
                Cell::Str(id) => id.clone(),
                other => panic!("id cell {other:?}"),
            })
            .collect()
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

    fn executed_sql(stub: &Stub) -> Vec<String> {
        stub.executed
            .borrow()
            .iter()
            .map(|call| call.request.sql.clone())
            .collect()
    }

    /// A predicate value and a `has_word` term carrying a quote, a semicolon
    /// and both comment markers reach the statement as one string literal
    /// each, quotes doubled: the statement is still one read-only SELECT over
    /// `logs`, and its conditions are exactly the typed ones. A `trace_id`
    /// carrying them is refused before any backend call.
    ///
    /// FLIP: a `format!` of the raw value closes the literal at its quote,
    /// leaving `OR 1=1` as a condition and the rest as a comment.
    #[test]
    fn caller_values_reach_the_statement_only_as_literals() {
        let key = key();
        let stub = Stub::default();
        stub.describe.borrow_mut().push_back(described());
        stub.sql.borrow_mut().push_back(plain(&[("a", 1, 1)]));

        let envelope = block_on(run(
            args(json!({
                "predicates": [{"key": "service.name", "op": "eq", "value": HOSTILE}],
                "has_word": HOSTILE,
            })),
            &context(&key),
            &stub,
        ));
        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");

        let sql = executed_sql(&stub);
        let [sql] = sql.as_slice() else {
            panic!("one statement ran: {sql:?}");
        };
        let quoted = "'x'' OR 1=1; -- /*'";
        assert_eq!(
            envelope.scope.predicates_applied,
            [
                format!("\"service.name\" = {quoted}"),
                format!("has_word(\"body\", {quoted})"),
            ]
        );
        let planned = ravel_sql::plan_page(
            &format!(
                "SELECT * FROM logs WHERE \"service.name\" = {quoted} AND \
                 has_word(\"body\", {quoted}) ORDER BY \"ts\", \"observed_ts\""
            ),
            None,
        )
        .expect("the statement pages")
        .statement;
        assert_eq!(*sql, planned);
        ravel_sql::refuse_several_tables(sql).expect("one table");
        ravel_sql::validate_query(sql).expect("one read-only statement");

        let refused = block_on(run(
            args(json!({"trace_id": HOSTILE})),
            &context(&key),
            &Stub::default(),
        ));
        assert_eq!(failure_class(&refused), FailureClass::InvalidArgument);
        let refused = block_on(run(
            args(json!({
                "predicates": [{"key": "trace_id", "op": "eq", "value": HOSTILE}],
            })),
            &context(&key),
            &{
                let stub = Stub::default();
                stub.describe.borrow_mut().push_back(described());
                stub
            },
        ));
        assert_eq!(failure_class(&refused), FailureClass::InvalidArgument);
    }

    /// A predicate key that is neither declared nor indexed is refused after
    /// `describe_data` (read at the call's clock) and before any statement
    /// runs, with a next step naming `ravel_describe_data`.
    ///
    /// FLIP: passing the key through as a column name reaches `sql_execute`,
    /// which the stub refuses by panicking.
    #[test]
    fn an_unknown_key_is_refused_before_the_statement_runs() {
        let key = key();
        for unknown in ["k8s.pod.name", "body"] {
            let stub = Stub::default();
            stub.describe.borrow_mut().push_back(described());
            let envelope = block_on(run(
                args(json!({
                    "predicates": [{"key": unknown, "op": "exists"}],
                })),
                &context(&key),
                &stub,
            ));
            assert_eq!(
                failure_class(&envelope),
                FailureClass::InvalidArgument,
                "{unknown}"
            );
            assert_eq!(actions(&envelope), ["ravel_describe_data"]);
            let described = stub.described.borrow();
            assert_eq!(described.len(), 1);
            assert_eq!(described[0].signal, Signal::Logs);
            assert_eq!(
                (described[0].resolve_at_ns, described[0].caller_now_ns),
                (END_NS, END_NS)
            );
        }
    }

    /// The key rule as the guide states it: a predicate may name a declared
    /// column, or `trace_id` when it is indexed. An indexed key that is not
    /// declared and is not `trace_id`, and `trace_id` on a tenant that does
    /// not index it, are each refused before any statement runs, with the
    /// `ravel_describe_data` next step saying which rows name a usable key.
    ///
    /// FLIP: accepting every key `describe_data` lists as indexed reaches
    /// `sql_execute` for `k8s.namespace`, which the stub refuses by
    /// panicking.
    #[test]
    fn an_indexed_key_other_than_trace_id_is_refused_unless_declared() {
        let key = key();
        let mut indexes_more = described();
        indexes_more.indexed_keys.push("k8s.namespace".to_string());
        let mut no_trace_index = described();
        no_trace_index.indexed_keys.retain(|key| key != TRACE_ID);
        for (name, outcome, refused) in [
            ("undeclared indexed key", indexes_more, "k8s.namespace"),
            ("trace_id not indexed", no_trace_index, "trace_id"),
        ] {
            let stub = Stub::default();
            stub.describe.borrow_mut().push_back(outcome);
            let envelope = block_on(run(
                args(json!({
                    "predicates": [{"key": refused, "op": "exists"}],
                })),
                &context(&key),
                &stub,
            ));
            assert_eq!(
                failure_class(&envelope),
                FailureClass::InvalidArgument,
                "{name}"
            );
            assert_eq!(actions(&envelope), ["ravel_describe_data"], "{name}");
            assert_eq!(
                envelope.next_steps[0].detail,
                "call ravel_describe_data with signal logs: a predicate may name a column row \
                 marked declared, or trace_id when it is an indexed_key row",
                "{name}"
            );
        }
    }

    /// Every argument refusal is answered before any backend call: the stub
    /// has no describe outcome and no page queued, so reaching either
    /// panics.
    ///
    /// FLIP: reading `describe_data` before checking a predicate's arity
    /// panics the stub on the `eq`-with-a-list case.
    #[test]
    fn argument_refusals_reach_no_backend() {
        let key = key();
        let pred = |op: &str, value: Value| json!({"predicates": [{"key": "service.name", "op": op, "value": value}]});
        let cases = [
            ("no time_range", json!({}), FailureClass::MissingArgument),
            (
                "unknown op",
                pred("like", json!("a")),
                FailureClass::InvalidArgument,
            ),
            (
                "eq with a list",
                pred("eq", json!(["a"])),
                FailureClass::InvalidArgument,
            ),
            (
                "in with one",
                pred("in", json!("a")),
                FailureClass::InvalidArgument,
            ),
            (
                "in empty",
                pred("in", json!([])),
                FailureClass::InvalidArgument,
            ),
            (
                "exists with a value",
                pred("exists", json!("a")),
                FailureClass::InvalidArgument,
            ),
            (
                "unknown field",
                json!({"predicates": [{"key": "a", "op": "exists", "not": 1}]}),
                FailureClass::InvalidArgument,
            ),
            (
                "severity",
                json!({"severity": "loud"}),
                FailureClass::InvalidArgument,
            ),
            (
                "short trace_id",
                json!({"trace_id": "abc"}),
                FailureClass::InvalidArgument,
            ),
            (
                "non-hex trace_id",
                json!({"trace_id": "g".repeat(32)}),
                FailureClass::InvalidArgument,
            ),
            (
                "blank has_word",
                json!({"has_word": "  "}),
                FailureClass::InvalidArgument,
            ),
            (
                "wordless has_word",
                json!({"has_word": "-- ;"}),
                FailureClass::InvalidArgument,
            ),
            (
                "evidence_ref",
                json!({"evidence_ref": "e"}),
                FailureClass::Unsupported,
            ),
        ];
        for (name, extra, class) in cases {
            let call = if name == "no time_range" {
                extra
            } else {
                args(extra)
            };
            let envelope = block_on(run(call, &context(&key), &Stub::default()));
            assert_eq!(failure_class(&envelope), class, "{name}");
        }
    }

    /// `severity` (either case) is the level's lowest severity number or
    /// above, `trace_id` (either case) a 16-byte literal, and each predicate
    /// a literal of its key's declared type: an `I64` column compares with an
    /// integer, and a value that is not one is refused before the statement
    /// runs.
    ///
    /// FLIP: rendering every declared value as a string compares an `I64`
    /// column with `'404'`.
    #[test]
    fn filters_compile_to_typed_conditions() {
        let key = key();
        let stub = Stub::default();
        stub.describe.borrow_mut().push_back(described());
        stub.sql.borrow_mut().push_back(plain(&[("a", 1, 1)]));
        let trace = "0123456789ABCDEF0123456789abcdef";

        let envelope = block_on(run(
            args(json!({
                "predicates": [
                    {"key": "http.status", "op": "in", "value": ["404", "-1"]},
                    {"key": "service.name", "op": "ne", "value": "api"},
                    {"key": "trace_id", "op": "exists"},
                ],
                "severity": "WARN",
                "trace_id": trace,
            })),
            &context(&key),
            &stub,
        ));
        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");
        assert_eq!(
            envelope.scope.predicates_applied,
            [
                "\"http.status\" IN (404, -1)",
                "\"service.name\" <> 'api'",
                "\"trace_id\" IS NOT NULL",
                "\"severity_num\" >= 13",
                "\"trace_id\" = X'0123456789abcdef0123456789abcdef'",
            ]
        );
        assert_eq!(
            envelope.scope.order_by,
            ["\"ts\" ASC", "\"observed_ts\" ASC"]
        );
        assert_eq!(
            (
                envelope.scope.signal.as_str(),
                envelope.scope.table.as_str()
            ),
            ("logs", "logs")
        );
        let executed = stub.executed.borrow();
        assert_eq!(executed[0].order_columns, ["ts", "observed_ts"]);
        assert!(executed[0].request.row_window);
        assert_eq!(executed[0].request.max_rows, Some(2));

        let stub = Stub::default();
        stub.describe.borrow_mut().push_back(described());
        let envelope = block_on(run(
            args(json!({
                "predicates": [{"key": "http.status", "op": "eq", "value": "4o4"}],
            })),
            &context(&key),
            &stub,
        ));
        assert_eq!(failure_class(&envelope), FailureClass::InvalidArgument);
    }

    /// Rows b and c share `(ts, observed_ts)` and straddle page one's cap:
    /// page one holds both back, the cursor resumes after a, and page two
    /// carries b and c, so every row is served exactly once.
    ///
    /// FLIP: a plain keyset on the pair keeps a and b, resumes after b, and
    /// a strict `>` on page two never serves c.
    #[test]
    fn a_group_straddling_the_cap_is_not_split() {
        let key = key();
        let stub = Stub::default();
        stub.sql.borrow_mut().extend([
            plain(&[("a", 1, 1), ("b", 2, 5), ("c", 2, 5)]),
            plain(&[("b", 2, 5), ("c", 2, 5)]),
        ]);

        let one = block_on(run(args(json!({})), &context(&key), &stub));
        assert_eq!(one.status, Status::OkPage, "{one:?}");
        assert_eq!(ids(&one), ["a"]);
        let cursor = one.presentation.cursor.clone().expect("page one's cursor");

        let two = block_on(run(args(json!({"cursor": cursor})), &later(&key), &stub));
        assert_eq!(two.status, Status::Ok, "{two:?}");
        assert_eq!(ids(&two), ["b", "c"]);

        let base = statement(&[]);
        let resume = ResumePosition::new(vec![
            ResumeValue::TimestampNanos(START_NS + 1_000),
            ResumeValue::TimestampNanos(START_NS + 1_000),
        ]);
        let resumed = ravel_sql::plan_page(&base, Some(&resume))
            .expect("the statement resumes")
            .statement;
        assert_eq!(executed_sql(&stub)[1], resumed);
    }

    /// Three rows equal on `(ts, observed_ts)` with a two-row cap: the page
    /// returns the two it has, `ok_bounded`, with no cursor and a next step
    /// to narrow `time_range`.
    ///
    /// FLIP: answering an empty page, or minting a cursor after the second
    /// row, which resumes past the third.
    #[test]
    fn a_group_larger_than_max_rows_is_bounded_with_no_cursor() {
        let key = key();
        let stub = Stub::default();
        stub.sql
            .borrow_mut()
            .push_back(plain(&[("a", 4, 4), ("b", 4, 4), ("c", 4, 4)]));

        let envelope = block_on(run(args(json!({})), &context(&key), &stub));

        assert_eq!(envelope.status, Status::OkBounded, "{envelope:?}");
        assert_eq!(ids(&envelope), ["a", "b"]);
        assert_eq!(envelope.presentation.cursor, None);
        assert_eq!(actions(&envelope), ["narrow_time_range"]);
    }

    /// Rows with the same attribute key set form one group; an attribute
    /// equal on every row of the group is listed once in
    /// `__ravel_group_attrs` on the group's first row, and one that differs
    /// stays on each row.
    ///
    /// FLIP: hoisting every attribute of the group's first row lists
    /// `pod=p1` as shared and drops `pod=p2` from row b.
    #[test]
    fn shared_attributes_are_hoisted_once_per_group() {
        let key = key();
        let stub = Stub::default();
        stub.sql.borrow_mut().push_back(page(&[
            ("a", 1, 1, json!({"svc": "api", "pod": "p1"})),
            ("b", 2, 2, json!({"svc": "api", "pod": "p2"})),
            ("c", 3, 3, json!({"code": "7"})),
        ]));
        let mut ctx = context(&key);
        ctx.budgets.max_rows = 4;

        let envelope = block_on(run(args(json!({})), &ctx, &stub));
        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");

        let names: Vec<&str> = envelope
            .data
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            [
                "__ravel_group",
                "__ravel_group_attrs",
                "id",
                "ts",
                "observed_ts",
                "attrs"
            ]
        );
        let map = |value: Value| match value {
            Value::Object(map) => Cell::Map(map),
            _ => panic!("an object"),
        };
        let shape: Vec<(Cell, Cell, Cell)> = envelope
            .data
            .rows
            .iter()
            .map(|row| (row[0].clone(), row[1].clone(), row[5].clone()))
            .collect();
        assert_eq!(
            shape,
            [
                (
                    Cell::Int(0),
                    map(json!({"svc": "api"})),
                    map(json!({"pod": "p1"}))
                ),
                (Cell::Int(0), Cell::Null, map(json!({"pod": "p2"}))),
                (Cell::Int(1), map(json!({"code": "7"})), map(json!({}))),
            ]
        );
    }

    /// A later page resolves at page one's mint instant and runs at its own
    /// clock, and a cursor presented with any other result-changing input is
    /// `cursor_invalid` before the backend.
    ///
    /// FLIP: resolving at the later clock reads a snapshot page one never
    /// saw.
    /// FLIP: leaving the compiled filters out of the argument hash resumes
    /// one search's position in another's result.
    #[test]
    fn a_later_page_resolves_at_the_mint_instant_and_binds_its_inputs() {
        let key = key();
        let stub = Stub::default();
        stub.sql.borrow_mut().extend([
            plain(&[("a", 1, 1), ("b", 2, 2), ("c", 3, 3)]),
            plain(&[("c", 3, 3)]),
        ]);
        let filters = json!({"has_word": "boom", "severity": "error"});

        let one = block_on(run(args(filters.clone()), &context(&key), &stub));
        let cursor = one.presentation.cursor.clone().expect("page one's cursor");
        let mut resumed = args(filters.clone());
        resumed["cursor"] = json!(cursor);
        let two = block_on(run(resumed.clone(), &later(&key), &stub));
        assert_eq!(two.status, Status::Ok, "{two:?}");
        assert_eq!(ids(&two), ["c"]);
        assert_eq!(
            stub.executed
                .borrow()
                .iter()
                .map(|call| (call.resolve_at_ns, call.request.now_ns))
                .collect::<Vec<_>>(),
            [(END_NS, END_NS), (END_NS, LATER_NS)]
        );

        let mut other_word = resumed.clone();
        other_word["has_word"] = json!("bang");
        let mut other_severity = resumed.clone();
        other_severity["severity"] = json!("warn");
        let mut other_trace = resumed.clone();
        other_trace["trace_id"] = json!("ab".repeat(16));
        let mut other_window = resumed.clone();
        other_window["time_range"]["start_ns"] = json!((START_NS + 1).to_string());
        let mut other_cap = later(&key);
        other_cap.budgets.max_rows = 3;
        for (name, call, ctx) in [
            ("has_word", other_word, later(&key)),
            ("severity", other_severity, later(&key)),
            ("trace_id", other_trace, later(&key)),
            ("time_range", other_window, later(&key)),
            ("max_rows", resumed, other_cap),
        ] {
            let envelope = block_on(run(call, &ctx, &stub));
            assert_eq!(
                failure_class(&envelope),
                FailureClass::CursorInvalid,
                "{name}"
            );
        }
        assert_eq!(stub.executed.borrow().len(), 2);
    }

    /// A cursor presented under the same filters and `start_ns` but another
    /// `end_ns` is `cursor_invalid`, before the backend.
    ///
    /// FLIP: leaving `end_ns` out of the argument hash resumes page one's
    /// position under a window that ends elsewhere.
    #[test]
    fn a_cursor_for_another_end_ns_is_refused() {
        let key = key();
        let stub = Stub::default();
        stub.sql
            .borrow_mut()
            .push_back(plain(&[("a", 1, 1), ("b", 2, 2), ("c", 3, 3)]));
        let filters = json!({"has_word": "boom"});
        let one = block_on(run(args(filters.clone()), &context(&key), &stub));
        let cursor = one.presentation.cursor.clone().expect("page one's cursor");

        let mut other_end = args(filters);
        other_end["cursor"] = json!(cursor);
        other_end["time_range"]["end_ns"] = json!((END_NS - 1).to_string());
        let envelope = block_on(run(other_end, &later(&key), &stub));

        assert_eq!(failure_class(&envelope), FailureClass::CursorInvalid);
        let message = &envelope.failure.as_ref().expect("failure").message;
        assert_eq!(message, "cursor is invalid");
        assert!(envelope.data.rows.is_empty());
        assert_eq!(stub.executed.borrow().len(), 1);
    }

    /// A cursor whose tuple does not have the order terms' types is
    /// `cursor_invalid` before the backend. Page one mints it from `Int`
    /// values where `ts` and `observed_ts` take `TimestampNanos`.
    ///
    /// FLIP: dropping `ResumeValueTypeMismatch` from `resume_plan`'s cursor
    /// arm sends it to `plan_refusal`, which answers `invalid_argument`.
    #[test]
    fn a_cursor_whose_tuple_has_another_type_is_invalid() {
        let key = key();
        let stub = Stub::default();
        let ints = |i: i64| Some(vec![ResumeValue::Int(i), ResumeValue::Int(i)]);
        stub.sql.borrow_mut().push_back(
            plain(&[("a", 1, 1), ("b", 2, 2), ("c", 3, 3)]).with_order_tuples(vec![
                ints(1),
                ints(2),
                ints(3),
            ]),
        );
        let one = block_on(run(args(json!({})), &context(&key), &stub));
        let cursor = one.presentation.cursor.clone().expect("page one's cursor");

        let mut resumed = args(json!({}));
        resumed["cursor"] = json!(cursor);
        let two = block_on(run(resumed, &later(&key), &stub));
        assert_eq!(failure_class(&two), FailureClass::CursorInvalid);
        assert_eq!(stub.executed.borrow().len(), 1);
    }

    /// A page of a call with predicates says its `budget.actual` leaves out
    /// the declared-column lookup, on page one and on a later page (each
    /// reads the declared columns again); a call without predicates makes no
    /// lookup and carries no such warning.
    ///
    /// FLIP: dropping the warning push in `finish` fails the first two
    /// assertions; pushing it on every page fails the third.
    #[test]
    fn a_page_says_its_spend_leaves_out_the_declared_column_lookup() {
        let key = key();
        let warned = |envelope: &Envelope| {
            envelope
                .warnings
                .iter()
                .filter(|warning| *warning == UNCOUNTED_LOOKUP_WARNING)
                .count()
        };
        let stub = Stub::default();
        stub.describe
            .borrow_mut()
            .extend([described(), described()]);
        stub.sql.borrow_mut().extend([
            plain(&[("a", 1, 1), ("b", 2, 2), ("c", 3, 3)]),
            plain(&[("c", 3, 3)]),
        ]);
        let filters = json!({
            "predicates": [{"key": "service.name", "op": "eq", "value": "api"}],
        });

        let one = block_on(run(args(filters.clone()), &context(&key), &stub));
        assert_eq!(one.status, Status::OkPage, "{one:?}");
        assert_eq!(warned(&one), 1, "{:?}", one.warnings);
        let mut resumed = args(filters);
        resumed["cursor"] = json!(one.presentation.cursor.clone().expect("a cursor"));
        let two = block_on(run(resumed, &later(&key), &stub));
        assert_eq!(two.status, Status::Ok, "{two:?}");
        assert_eq!(warned(&two), 1, "{:?}", two.warnings);
        assert_eq!(stub.described.borrow().len(), 2);

        let stub = Stub::default();
        stub.sql.borrow_mut().push_back(plain(&[("a", 1, 1)]));
        let bare = block_on(run(
            args(json!({"has_word": "boom"})),
            &context(&key),
            &stub,
        ));
        assert_eq!(bare.status, Status::Ok, "{bare:?}");
        assert_eq!(warned(&bare), 0, "{:?}", bare.warnings);
        assert!(stub.described.borrow().is_empty());
    }

    /// A cursor outlives the call that minted it: it lives until the mint
    /// instant plus the protection horizon less [`GRACE_NS`], not until the
    /// minting call's deadline. Page one runs at 3 h with its deadline at
    /// 4 h and a 27 h horizon, so its cursor redeems at 5 h, past that
    /// deadline, and is `cursor_expired` at 6 h, `3 h + 27 h - 24 h`.
    ///
    /// FLIP: minting with `ctx.deadline_ns` in place of `i64::MAX` makes the
    /// 5 h redemption `cursor_expired`; dropping the bound from
    /// `Cursor::mint` lets the 6 h one run its page (the redeeming call's own
    /// clamp alone allows 9 h).
    ///
    /// [`GRACE_NS`]: crate::cursor::GRACE_NS
    #[test]
    fn a_cursor_lives_until_the_horizon_less_grace_not_the_calls_deadline() {
        const HOUR_NS: i64 = 3_600_000_000_000;
        let key = key();
        let first = context(&key);
        assert_eq!(
            (first.now_ns, first.deadline_ns, first.protection_horizon_ns),
            (3 * HOUR_NS, 4 * HOUR_NS, 30 * HOUR_NS)
        );
        let at = |now_ns: i64| {
            let mut ctx = context(&key);
            ctx.now_ns = now_ns;
            ctx.protection_horizon_ns = now_ns + 27 * HOUR_NS;
            ctx
        };
        let stub = Stub::default();
        stub.sql.borrow_mut().extend([
            plain(&[("a", 1, 1), ("b", 2, 2), ("c", 3, 3)]),
            plain(&[("c", 3, 3)]),
        ]);
        let one = block_on(run(args(json!({})), &first, &stub));
        let mut resumed = args(json!({}));
        resumed["cursor"] = json!(one.presentation.cursor.clone().expect("a cursor"));

        let past_deadline = block_on(run(resumed.clone(), &at(5 * HOUR_NS), &stub));
        assert_eq!(past_deadline.status, Status::Ok, "{past_deadline:?}");
        assert_eq!(ids(&past_deadline), ["c"]);

        let at_bound = block_on(run(resumed, &at(6 * HOUR_NS), &stub));
        assert_eq!(failure_class(&at_bound), FailureClass::CursorExpired);
        assert_eq!(stub.executed.borrow().len(), 2);
    }
}
