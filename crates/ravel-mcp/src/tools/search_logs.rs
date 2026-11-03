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
//! that is neither as `invalid_argument` before any statement runs.
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
//! - `group`: the row's attribute group, numbered from 0 in order of first
//!   appearance on the page. A group is the rows whose `attrs` maps have the
//!   same set of keys.
//! - `group_attrs`: on the first row of each group, the attributes (key and
//!   value) every row of the group on this page carries, listed once; null on
//!   the group's other rows.
//!
//! The `logs` columns follow in table order, with `attrs` holding only the
//! row's attributes not in its group's `group_attrs`. Groups and hoisting are
//! computed per page over the rows under `max_rows`; when the byte cap or a
//! held-back group drops rows from the end, the shared set is still shared by
//! every kept row, but it may be smaller than what the kept rows alone share.

use std::collections::{BTreeMap, BTreeSet};

use ravel_query::RequestBudgets;
use ravel_sql::{DeclaredType, PagePlan, PagePlanError, ResumePosition, ResumeValue, SqlRequest};
use serde::Deserialize;
use serde_json::Value;

use crate::catalog::{Predicate, PredicateOp, PredicateValue, SearchLogsInput};
use crate::cursor::{Cursor, CursorError, CursorPosition};
use crate::draft::{Draft, NoCursor, PageOrder, PinScope};
use crate::envelope::{
    CURSOR_BOUND, Cell, Column, Data, Envelope, Failure, FailureClass, NextStep,
    serialized_str_len,
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
        if input
            .has_word
            .as_deref()
            .is_some_and(|term| term.trim().is_empty())
        {
            return Err(invalid("has_word must name at least one word".to_string()));
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
        .ok_or_else(|| {
            invalid(format!(
                "{field} {raw:?} is not 32 hexadecimal characters"
            ))
        })
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
                            "predicate key {key:?} is neither a declared column nor an indexed \
                             key of the logs table"
                        ),
                    ),
                    vec![step(
                        "ravel_describe_data",
                        "call ravel_describe_data with signal logs for the declared columns \
                         and indexed keys a predicate may name",
                    )],
                ));
            }
        };
        let column = render_identifier(key);
        let rendered = |raw: &str| kind.literal(key, raw).map(|literal| render_literal(&literal));
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
        ravel_sql::refuse_several_tables(&statement).map_err(|error| (sql_failure(&error), Vec::new()))?;
        ravel_sql::validate_query(&statement).map_err(|error| {
            refusal(
                FailureClass::Validation,
                format!("{TOOL} compiled a statement the engine refuses: {error}"),
            )
        })?;
        let plan = ravel_sql::plan_page(&statement, None).map_err(|error| plan_refusal(&error))?;
        let argument_hash = argument_hash(&statement, &input.window, ctx.budgets.max_rows);
        Ok(Call {
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
    format!("SELECT * FROM logs WHERE {}{order}", conditions.join(" AND "))
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
    let confirmed = match pending.confirm(ctx.now_ns, &observed_erasure, &declared) {
        Ok(confirmed) => confirmed,
        Err(error) => return refused_with(error.failure(), Vec::new(), ctx),
    };
    let draft = draft.pinned_to(&confirmed, declared);
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
        Err(PagePlanError::ResumeArity { .. } | PagePlanError::NonFiniteResumeValue) => {
            Err(CursorError::Invalid.failure())
        }
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
    draft.envelope.scope.order_by = order_by.clone();
    draft.envelope.scope.predicates_applied = call.conditions.clone();
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

/// Prepend the `group` and `group_attrs` columns and hoist each group's
/// shared attributes out of `attrs`, over the first `considered` rows. A row
/// past them is one the row cap drops, and keeps null in both new columns.
fn group_by_attribute_set(data: &mut Data, considered: usize) {
    let attrs_at = data.columns.iter().position(|column| column.name == "attrs");
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
        let first = listed.get_mut(group).is_some_and(|seen| !std::mem::replace(seen, true));
        let hoisted = if first { Cell::Map(common) } else { Cell::Null };
        row.splice(0..0, [Cell::Int(group as i128), hoisted]);
    }
    data.columns.splice(
        0..0,
        [
            Column {
                name: "group".to_string(),
                r#type: "Int64".to_string(),
            },
            Column {
                name: "group_attrs".to_string(),
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
    if digits.is_empty() || digits.len() % 2 != 0 {
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
