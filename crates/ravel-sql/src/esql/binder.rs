//! A parsed ES|QL query to a bound [`Pipeline`] (ADR-2887 D9, D10).
//!
//! Each stage binds against the scope the stage before it left: the visible
//! columns in order, each with its ES|QL name, internal column name and type.
//! Until a `KEEP` or `STATS`, a name no column or reserved name claims is an
//! attribute key read from the hidden `attrs` column, unless a `DROP` removed
//! it or a `RENAME` renamed it away. After a `KEEP` or `STATS` no name is an
//! attribute key.
//!
//! The reserved names are the native columns' ES|QL names (`@timestamp`,
//! `observed_ts`, `severity_num`, `severity_text`, `message`, `trace_id`,
//! `span_id`, `flags`), their table names (`ts`, `body`) and `attrs`. A
//! declared column whose key is one of them is not bound: the name reads the
//! native column (or, for `attrs`, nothing), so the declared column is not
//! reachable from ES|QL. That is the precedence of ADR-2887 D10, native
//! before declared, applied at `FROM` rather than per reference, so no
//! reference is ambiguous.

use std::collections::{BTreeSet, HashMap};

use datafusion::arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNano, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use ravel_esql::Span;
use ravel_esql::ast::{
    self, BinaryOp, CallArgs, Command, Expr, IntervalKind, LiteralValue, NullsOrder, QualifiedName,
    SortOrder, UnaryOp,
};

use super::error::EsqlError;
use super::ir::{BAggregate, BExpr, BSortKey, Pipeline, Source, Stage};
use super::types::{EsqlColumn, EsqlType};
use super::window::WindowBounds;
use crate::declared::{DeclaredColumn, DeclaredType};
use crate::session::LOGS_TABLE;

/// The rows a query returns when it does not end with its own `LIMIT`.
pub const IMPLICIT_LIMIT: u64 = 1000;

/// The most rows a query returns: a final `LIMIT` above it is lowered to it.
/// This mirrors the default result cap of Elasticsearch 9.5.5, the target
/// release (`esql.query.result_truncation_max_size`, 10,000 rows).
pub const MAX_LIMIT: u64 = 10_000;

/// The most expression nodes a statement may bind to, counting every
/// stage's expressions and the column references each projection carries; a
/// statement past it is refused with a verification error.
///
/// The densest shape measured is a long division per two tokens, whose
/// guards bind to 17 nodes: a statement of such divisions binds to under 9
/// nodes per token, so one at the default 65,536-token limit binds to under
/// 589,824 nodes. The budget is the next power of two. A statement of many
/// fields over many columns can still pass it, since each field's projection
/// carries every visible column, and is refused.
pub const MAX_BOUND_NODES: usize = 1 << 20;

/// An expression the bound output repeats is copied only up to this many
/// nodes. A larger one is computed once under a fresh internal name ahead of
/// the stage that reads it, and the copies read that column, so an operand
/// larger than this appears once in the bound statement however often it is
/// read.
const INLINE_NODES: usize = 4;

/// What binding produced.
pub(crate) struct Bound {
    pub(crate) pipeline: Pipeline,
    pub(crate) columns: Vec<EsqlColumn>,
    /// `None` for `ROW`.
    pub(crate) window: Option<WindowBounds>,
    /// The statement's own final `LIMIT`, when [`MAX_LIMIT`] lowered it.
    pub(crate) clamped_limit: Option<u64>,
}

/// The request inputs binding reads.
pub(crate) struct BindInput<'a> {
    pub(crate) source: &'a str,
    pub(crate) declared: &'a [DeclaredColumn],
    pub(crate) now_ns: i64,
    pub(crate) start: Option<i64>,
    pub(crate) end: Option<i64>,
}

pub(crate) fn bind(query: &ast::Query, input: &BindInput<'_>) -> Result<Bound, EsqlError> {
    let mut binder = Binder {
        source: input.source,
        now_ns: input.now_ns,
        next: 0,
        stages: Vec::new(),
        scope: Scope::default(),
        window: None,
        ts: None,
        nodes: 0,
        at: query.source.span(),
        pre: Vec::new(),
        post: Vec::new(),
    };
    match &query.source {
        Command::From(from) => binder.from(from, input)?,
        Command::Row(row) => {
            for field in &row.fields {
                let (value, ty) = binder.value(&field.expr, &mut Cx::Row)?;
                let name = binder.field_name(field);
                binder.define(name, value, ty)?;
            }
        }
        other => return Err(binder.unsupported(other.span(), "this source command")),
    }
    for command in &query.processing {
        binder.at = command.span();
        let before = binder.timestamp_internal();
        binder.command(command)?;
        if binder.timestamp_internal() != before
            && let Some(window) = &mut binder.window
        {
            window.close();
        }
    }
    let mut clamped_limit = None;
    match (query.processing.last(), binder.stages.last_mut()) {
        (Some(Command::Limit(_)), Some(Stage::Limit(count))) => {
            if *count > MAX_LIMIT {
                clamped_limit = Some(*count);
                *count = MAX_LIMIT;
            }
        }
        _ => binder.push(Stage::Limit(IMPLICIT_LIMIT))?,
    }
    let output = binder
        .scope
        .cols
        .iter()
        .map(|col| (col.internal.clone(), col.name.clone()))
        .collect();
    let columns = binder
        .scope
        .cols
        .iter()
        .map(|col| EsqlColumn {
            name: col.name.clone(),
            ty: col.ty,
        })
        .collect();
    let source = if binder.window.is_some() {
        Source::Logs
    } else {
        Source::Row
    };
    Ok(Bound {
        pipeline: Pipeline {
            source,
            stages: binder.stages,
            output,
        },
        columns,
        window: binder.window,
        clamped_limit,
    })
}

#[derive(Debug, Clone)]
struct Col {
    name: String,
    internal: String,
    ty: EsqlType,
}

#[derive(Debug, Default)]
struct Scope {
    cols: Vec<Col>,
    /// The hidden `attrs` column's internal name while attribute keys resolve.
    attrs: Option<String>,
    /// `ts` and `body`: other names for the source's `@timestamp` and
    /// `message` columns, valid while those columns are visible.
    aliases: Vec<(&'static str, String)>,
    /// Names that never fall through to an attribute key.
    reserved: BTreeSet<String>,
    /// Names a `DROP` removed or a `RENAME` renamed away, which no longer
    /// fall through to an attribute key.
    blocked: Vec<Pattern>,
}

enum Lookup {
    Visible(usize),
    Attribute,
    Unknown,
}

impl Scope {
    fn lookup(&self, name: &str) -> Lookup {
        if let Some(i) = self.cols.iter().position(|col| col.name == name) {
            return Lookup::Visible(i);
        }
        if let Some((_, internal)) = self.aliases.iter().find(|(alias, _)| *alias == name)
            && let Some(i) = self.cols.iter().position(|col| &col.internal == internal)
        {
            return Lookup::Visible(i);
        }
        if self.reserved.contains(name) {
            return Lookup::Unknown;
        }
        if self.attrs.is_some() && !self.blocked.iter().any(|p| p.matches(name)) {
            return Lookup::Attribute;
        }
        Lookup::Unknown
    }

    /// Stop names `pattern` matches from resolving as attribute keys, while
    /// attribute keys resolve at all.
    fn block(&mut self, pattern: Pattern) {
        if self.attrs.is_some() {
            self.blocked.push(pattern);
        }
    }
}

/// Where an expression is bound.
enum Cx<'c> {
    /// Over one row of the current scope.
    Row,
    /// Inside an aggregate's argument.
    InAggregate,
    /// A `STATS` aggregate field: aggregate calls are collected, and any other
    /// column reference is refused.
    Stats {
        calls: &'c mut Vec<(BAggregate, String)>,
        filter: Option<&'c BExpr>,
    },
}

/// A bound expression, or a time duration in nanoseconds, which is only an
/// operand of date arithmetic or `BUCKET`. A duration with a date period
/// (`day` or `week`) among its terms carries the first such term, which
/// `BUCKET` refuses.
enum Value {
    Expr(BExpr, EsqlType),
    Duration(i64, Option<Period>),
}

/// A date-period term of a duration: its span and its unit as written.
#[derive(Clone)]
struct Period {
    span: Span,
    unit: String,
}

struct Binder<'a> {
    source: &'a str,
    now_ns: i64,
    next: usize,
    stages: Vec<Stage>,
    scope: Scope,
    /// `Some` for a `FROM` source.
    window: Option<WindowBounds>,
    /// The internal name of the source's timestamp column.
    ts: Option<String>,
    /// The expression nodes bound so far, against [`MAX_BOUND_NODES`].
    nodes: usize,
    /// The command being bound, where a budget refusal points.
    at: Span,
    /// Shared operands the next stage reads, over the current scope.
    pre: Vec<(String, BExpr)>,
    /// Shared operands of a `STATS` field, over the aggregate's output.
    post: Vec<(String, BExpr)>,
}

impl Binder<'_> {
    fn fresh(&mut self) -> String {
        self.next += 1;
        format!("_e{}", self.next)
    }

    fn text(&self, span: Span) -> String {
        span.slice(self.source).unwrap_or_default().to_string()
    }

    fn verification(&self, span: Span, message: impl Into<String>) -> EsqlError {
        EsqlError::verification(self.source, span, message)
    }

    fn unsupported(&self, span: Span, name: impl Into<String>) -> EsqlError {
        EsqlError::unsupported(self.source, span, name)
    }

    fn bucket_period(&self, period: &Period) -> EsqlError {
        self.unsupported(
            period.span,
            format!(
                "BUCKET over the date period [{}] (unit [{}]); BUCKET takes a fixed time duration in milliseconds, seconds, minutes or hours",
                self.text(period.span),
                period.unit
            ),
        )
    }

    fn field_name(&self, field: &ast::Field) -> String {
        match &field.name {
            Some(name) => name_text(name),
            None => self.text(field.expr.span()),
        }
    }

    fn timestamp_internal(&self) -> Option<String> {
        match self.scope.lookup("@timestamp") {
            Lookup::Visible(i) => Some(self.scope.cols[i].internal.clone()),
            _ => None,
        }
    }

    /// The internal names of every visible column and the hidden `attrs`
    /// column.
    fn carry_names(&self) -> Vec<String> {
        self.scope
            .cols
            .iter()
            .map(|col| &col.internal)
            .chain(self.scope.attrs.as_ref())
            .cloned()
            .collect()
    }

    /// Every visible column and the hidden `attrs` column, unchanged.
    fn carry(&self) -> Vec<(BExpr, String)> {
        self.carry_names()
            .into_iter()
            .map(|internal| (BExpr::Column(internal.clone()), internal))
            .collect()
    }

    /// Refuse a statement that has bound more than [`MAX_BOUND_NODES`]
    /// expression nodes, pointing at `span`.
    fn within_budget(&self, nodes: usize, span: Span) -> Result<(), EsqlError> {
        if nodes > MAX_BOUND_NODES {
            return Err(self.verification(
                span,
                format!("the statement binds to more than {MAX_BOUND_NODES} expression nodes"),
            ));
        }
        Ok(())
    }

    fn push(&mut self, stage: Stage) -> Result<(), EsqlError> {
        self.nodes = self.nodes.saturating_add(stage.nodes());
        self.within_budget(self.nodes, self.at)?;
        self.stages.push(stage);
        Ok(())
    }

    /// `value` if it is small enough to copy, else a column reading it,
    /// computed ahead of the stage that reads the result: over the
    /// aggregate's output for a `STATS` field (`post`), else over the current
    /// scope.
    fn share(&mut self, value: BExpr, post: bool) -> BExpr {
        if value.nodes() <= INLINE_NODES {
            return value;
        }
        let name = self.fresh();
        let lets = if post { &mut self.post } else { &mut self.pre };
        lets.push((name.clone(), value));
        BExpr::Column(name)
    }

    /// Compute the shared operands `reads` reaches ahead of the stage that
    /// reads them, from the current scope.
    fn flush_pre(&mut self, reads: &[&BExpr]) -> Result<(), EsqlError> {
        let lets = std::mem::take(&mut self.pre);
        let carry = self.carry_names();
        self.flush(lets, &carry, reads)
    }

    /// Compute the shared operands `lets` that `reads` reaches, one projection
    /// per level of nesting, deepest first. Each projection carries `carry`,
    /// computes the operands at its level and passes on the ones a shallower
    /// level still reads, so the stage after the last projection reads
    /// `carry` and every operand `reads` names. A shared operand reads only
    /// operands shared before it, so `lets` is in dependency order.
    fn flush(
        &mut self,
        lets: Vec<(String, BExpr)>,
        carry: &[String],
        reads: &[&BExpr],
    ) -> Result<(), EsqlError> {
        if lets.is_empty() {
            return Ok(());
        }
        let (names, mut values): (Vec<String>, Vec<Option<BExpr>>) = lets
            .into_iter()
            .map(|(name, value)| (name, Some(value)))
            .unzip();
        let index: HashMap<&str, usize> = names
            .iter()
            .enumerate()
            .map(|(i, name)| (name.as_str(), i))
            .collect();
        // depth: 1 for an operand the stage reads, one more than its deepest
        // reader otherwise, 0 when nothing reads it. reader: the depth of its
        // shallowest reader, the stage being depth 0.
        let mut depth = vec![0usize; names.len()];
        let mut reader = vec![usize::MAX; names.len()];
        let mut read = Vec::new();
        reads.iter().for_each(|e| e.columns(&mut read));
        for name in read {
            if let Some(&i) = index.get(name) {
                depth[i] = 1;
                reader[i] = 0;
            }
        }
        for i in (0..names.len()).rev() {
            if depth[i] == 0 {
                continue;
            }
            let mut read = Vec::new();
            if let Some(value) = &values[i] {
                value.columns(&mut read);
            }
            for name in read {
                if let Some(&child) = index.get(name) {
                    depth[child] = depth[child].max(depth[i] + 1);
                    reader[child] = reader[child].min(depth[i]);
                }
            }
        }
        let deepest = depth.iter().copied().max().unwrap_or(0);
        for level in (1..=deepest).rev() {
            let mut columns: Vec<(BExpr, String)> = carry
                .iter()
                .map(|name| (BExpr::Column(name.clone()), name.clone()))
                .collect();
            for (i, name) in names.iter().enumerate() {
                if depth[i] == level {
                    if let Some(value) = values[i].take() {
                        columns.push((value, name.clone()));
                    }
                } else if depth[i] > level && reader[i] < level {
                    columns.push((BExpr::Column(name.clone()), name.clone()));
                }
            }
            self.push(Stage::Project(columns))?;
        }
        Ok(())
    }

    /// Add `name` as the last column, replacing any column of that name.
    fn define(&mut self, name: String, value: BExpr, ty: EsqlType) -> Result<(), EsqlError> {
        self.flush_pre(&[&value])?;
        let internal = self.fresh();
        self.scope.cols.retain(|col| col.name != name);
        let mut columns = self.carry();
        columns.push((value, internal.clone()));
        self.scope.cols.push(Col { name, internal, ty });
        self.push(Stage::Project(columns))
    }

    fn from(&mut self, from: &ast::FromCommand, input: &BindInput<'_>) -> Result<(), EsqlError> {
        let first = from
            .sources
            .first()
            .ok_or_else(|| self.verification(from.span, "FROM names no source"))?;
        if from.sources.len() > 1 {
            let names: Vec<&str> = from.sources.iter().map(|s| s.text.as_str()).collect();
            return Err(self.unsupported(
                first.span.to(from.sources[from.sources.len() - 1].span),
                format!("FROM over more than one source [{}]", names.join(", ")),
            ));
        }
        if first.text.contains(':') {
            return Err(self.unsupported(first.span, format!("remote source [{}]", first.text)));
        }
        if first.text.contains('*') {
            return Err(self.unsupported(first.span, format!("source pattern [{}]", first.text)));
        }
        if first.text != LOGS_TABLE {
            return Err(self.unsupported(first.span, format!("source [{}]", first.text)));
        }
        if let Some(metadata) = &from.metadata {
            let names: Vec<String> = metadata.fields.iter().map(name_text).collect();
            return Err(self.unsupported(metadata.span, format!("METADATA [{}]", names.join(", "))));
        }

        let int64 = |column: &str| BExpr::Column(column.to_string()).cast(DataType::Int64);
        let natives = [
            ("@timestamp", "ts", EsqlType::DateNanos, column("ts")),
            (
                "observed_ts",
                "observed_ts",
                EsqlType::DateNanos,
                column("observed_ts"),
            ),
            (
                "severity_num",
                "severity_num",
                EsqlType::Long,
                int64("severity_num"),
            ),
            (
                "severity_text",
                "severity_text",
                EsqlType::Keyword,
                column("severity_text"),
            ),
            ("message", "body", EsqlType::Keyword, column("body")),
            (
                "trace_id",
                "trace_id",
                EsqlType::Keyword,
                hex(column("trace_id")),
            ),
            (
                "span_id",
                "span_id",
                EsqlType::Keyword,
                hex(column("span_id")),
            ),
            ("flags", "flags", EsqlType::Long, int64("flags")),
        ];
        let mut columns = Vec::new();
        for (name, native, ty, value) in natives {
            let internal = self.fresh();
            self.scope.reserved.insert(name.to_string());
            self.scope.reserved.insert(native.to_string());
            columns.push((value, internal.clone()));
            self.scope.cols.push(Col {
                name: name.to_string(),
                internal,
                ty,
            });
        }
        self.scope.reserved.insert("attrs".to_string());
        for declared in input.declared {
            if self.scope.reserved.contains(&declared.key) {
                continue;
            }
            let (value, ty) = match declared.ty {
                DeclaredType::Str => (
                    column(&declared.key).cast(DataType::Utf8),
                    EsqlType::Keyword,
                ),
                DeclaredType::I64 => (column(&declared.key), EsqlType::Long),
                DeclaredType::Bool => (column(&declared.key), EsqlType::Boolean),
                DeclaredType::Bytes => (hex(column(&declared.key)), EsqlType::Keyword),
            };
            let internal = self.fresh();
            self.scope.reserved.insert(declared.key.clone());
            columns.push((value, internal.clone()));
            self.scope.cols.push(Col {
                name: declared.key.clone(),
                internal,
                ty,
            });
        }
        let attrs = self.fresh();
        columns.push((column("attrs"), attrs.clone()));
        self.scope.attrs = Some(attrs);
        let ts = self.scope.cols[0].internal.clone();
        let body = self.scope.cols[4].internal.clone();
        self.scope.aliases = vec![("ts", ts.clone()), ("body", body)];
        self.push(Stage::Project(columns))?;

        let mut range = Vec::new();
        if let Some(start) = input.start {
            range.push(BExpr::binary(
                BExpr::Column(ts.clone()),
                Operator::GtEq,
                timestamp_literal(Some(start)),
            ));
        }
        if let Some(end) = input.end {
            range.push(BExpr::binary(
                BExpr::Column(ts.clone()),
                Operator::Lt,
                timestamp_literal(Some(end)),
            ));
        }
        if let Some(condition) = range
            .into_iter()
            .reduce(|left, right| BExpr::binary(left, Operator::And, right))
        {
            self.push(Stage::Filter(condition))?;
        }
        self.ts = Some(ts);
        self.window = Some(WindowBounds::default());
        Ok(())
    }

    fn command(&mut self, command: &Command) -> Result<(), EsqlError> {
        match command {
            Command::Where(w) => {
                let (condition, ty) = self.value(&w.condition, &mut Cx::Row)?;
                let condition = self.condition(condition, ty, w.condition.span())?;
                if let (Some(window), Some(ts)) = (&mut self.window, &self.ts) {
                    window.observe(&condition, ts);
                }
                self.flush_pre(&[&condition])?;
                self.push(Stage::Filter(condition))?;
            }
            Command::Eval(eval) => {
                for field in &eval.fields {
                    let (value, ty) = self.value(&field.expr, &mut Cx::Row)?;
                    let name = self.field_name(field);
                    self.define(name, value, ty)?;
                }
            }
            Command::Keep(keep) => self.keep(&keep.patterns)?,
            Command::Drop(drop) => self.drop(&drop.patterns, drop.span)?,
            Command::Rename(rename) => {
                for pair in &rename.renames {
                    self.rename(pair)?;
                }
            }
            Command::Stats(stats) => {
                if let Some(window) = &mut self.window {
                    window.close();
                }
                self.stats(stats)?;
            }
            Command::Sort(sort) => {
                let mut keys = Vec::new();
                for key in &sort.keys {
                    let (expr, _) = self.value(&key.expr, &mut Cx::Row)?;
                    let asc = !matches!(key.order, Some(SortOrder::Desc));
                    let nulls_first = match key.nulls {
                        Some(NullsOrder::First) => true,
                        Some(NullsOrder::Last) => false,
                        None => !asc,
                    };
                    keys.push(BSortKey {
                        expr,
                        asc,
                        nulls_first,
                    });
                }
                let reads: Vec<&BExpr> = keys.iter().map(|key| &key.expr).collect();
                self.flush_pre(&reads)?;
                self.push(Stage::Sort(keys))?;
            }
            Command::Limit(limit) => {
                if let Some(window) = &mut self.window {
                    window.close();
                }
                self.push(Stage::Limit(limit.count))?;
            }
            other => {
                let keyword = self.text(other.span());
                let keyword = keyword.split_whitespace().next().unwrap_or_default();
                return Err(self.unsupported(
                    other.span(),
                    format!("command [{}]", keyword.to_ascii_uppercase()),
                ));
            }
        }
        Ok(())
    }

    fn condition(&self, condition: BExpr, ty: EsqlType, span: Span) -> Result<BExpr, EsqlError> {
        match ty {
            EsqlType::Boolean => Ok(condition),
            EsqlType::Null => Ok(BExpr::Literal(ScalarValue::Boolean(None))),
            other => Err(self.verification(
                span,
                format!(
                    "condition [{}] must be [boolean], found [{other}]",
                    self.text(span)
                ),
            )),
        }
    }

    fn attribute(&self, name: &str) -> Option<BExpr> {
        let attrs = self.scope.attrs.as_ref()?;
        Some(BExpr::Scalar(
            "get_field",
            vec![
                BExpr::Column(attrs.clone()),
                BExpr::Literal(ScalarValue::Utf8(Some(name.to_string()))),
            ],
        ))
    }

    fn unknown(&self, name: &str, span: Span) -> EsqlError {
        self.verification(span, format!("Unknown column [{name}]"))
    }

    fn keep(&mut self, patterns: &[QualifiedName]) -> Result<(), EsqlError> {
        // For each visible column, the pattern that places it: the most
        // specific, and of equally specific ones the rightmost.
        let mut best: Vec<Option<(usize, u8)>> = vec![None; self.scope.cols.len()];
        let place = |best: &mut Vec<Option<(usize, u8)>>, col: usize, at: usize, rank: u8| {
            if best[col].is_none_or(|(i, r)| rank > r || (rank == r && at > i)) {
                best[col] = Some((at, rank));
            }
        };
        let mut attributes: Vec<(usize, String)> = Vec::new();
        for (at, name) in patterns.iter().enumerate() {
            let pattern = Pattern::of(name);
            if !pattern.is_wildcard() {
                let text = name_text(name);
                match self.scope.lookup(&text) {
                    Lookup::Visible(col) => place(&mut best, col, at, 2),
                    Lookup::Attribute => {
                        attributes.retain(|(_, key)| *key != text);
                        attributes.push((at, text));
                    }
                    Lookup::Unknown => return Err(self.unknown(&text, name.span)),
                }
                continue;
            }
            let rank = if pattern.is_star_only() { 0 } else { 1 };
            let mut matched = false;
            for (col, c) in self.scope.cols.iter().enumerate() {
                if pattern.matches(&c.name) {
                    place(&mut best, col, at, rank);
                    matched = true;
                }
            }
            if !matched {
                return Err(self.verification(
                    name.span,
                    format!("No matches found for pattern [{}]", name_text(name)),
                ));
            }
        }
        let mut cols = Vec::new();
        let mut columns = Vec::new();
        for at in 0..patterns.len() {
            for (col, placed) in best.iter().enumerate() {
                if placed.is_some_and(|(i, _)| i == at) {
                    let col = self.scope.cols[col].clone();
                    columns.push((BExpr::Column(col.internal.clone()), col.internal.clone()));
                    cols.push(col);
                }
            }
            for (_, key) in attributes.iter().filter(|(i, _)| *i == at) {
                let value = self
                    .attribute(key)
                    .ok_or_else(|| self.unknown(key, Span::new(0, 0)))?;
                let internal = self.fresh();
                columns.push((value, internal.clone()));
                cols.push(Col {
                    name: key.clone(),
                    internal,
                    ty: EsqlType::Keyword,
                });
            }
        }
        self.scope.cols = cols;
        self.scope.attrs = None;
        self.scope.blocked.clear();
        self.push(Stage::Project(columns))
    }

    fn drop(&mut self, patterns: &[QualifiedName], span: Span) -> Result<(), EsqlError> {
        for name in patterns {
            let pattern = Pattern::of(name);
            if !pattern.is_wildcard() {
                let text = name_text(name);
                match self.scope.lookup(&text) {
                    Lookup::Visible(col) => {
                        self.scope.cols.remove(col);
                        self.scope.block(pattern);
                    }
                    Lookup::Attribute => self.scope.blocked.push(pattern),
                    Lookup::Unknown => return Err(self.unknown(&text, name.span)),
                }
                continue;
            }
            let before = self.scope.cols.len();
            self.scope.cols.retain(|col| !pattern.matches(&col.name));
            if self.scope.attrs.is_some() {
                self.scope.blocked.push(pattern);
            } else if self.scope.cols.len() == before {
                return Err(self.verification(
                    name.span,
                    format!("No matches found for pattern [{}]", name_text(name)),
                ));
            }
        }
        if self.scope.cols.is_empty() {
            return Err(self.verification(
                span,
                format!("Removing all fields is not allowed [{}]", self.text(span)),
            ));
        }
        self.push(Stage::Project(self.carry()))
    }

    fn rename(&mut self, pair: &ast::RenamePair) -> Result<(), EsqlError> {
        let old = name_text(&pair.old);
        let new = name_text(&pair.new);
        match self.scope.lookup(&old) {
            Lookup::Visible(col) => {
                let internal = self.scope.cols[col].internal.clone();
                self.scope
                    .cols
                    .retain(|col| col.name != new || col.internal == internal);
                if let Some(col) = self.scope.cols.iter_mut().find(|c| c.internal == internal) {
                    col.name = new;
                }
                self.scope.block(Pattern::of(&pair.old));
                Ok(())
            }
            Lookup::Attribute => {
                let value = self
                    .attribute(&old)
                    .ok_or_else(|| self.unknown(&old, pair.old.span))?;
                self.define(new, value, EsqlType::Keyword)?;
                self.scope.block(Pattern::of(&pair.old));
                Ok(())
            }
            Lookup::Unknown => Err(self.unknown(&old, pair.old.span)),
        }
    }

    fn stats(&mut self, stats: &ast::StatsCommand) -> Result<(), EsqlError> {
        let mut aggregates = Vec::new();
        let mut outputs: Vec<(String, BExpr, EsqlType)> = Vec::new();
        for aggregate in &stats.aggregates {
            let filter = match &aggregate.filter {
                Some(filter) => {
                    let (condition, ty) = self.value(filter, &mut Cx::Row)?;
                    let condition = self.condition(condition, ty, filter.span())?;
                    // Every aggregate call in the field repeats the filter.
                    Some(self.share(condition, false))
                }
                None => None,
            };
            let (value, ty) = self.value(
                &aggregate.field.expr,
                &mut Cx::Stats {
                    calls: &mut aggregates,
                    filter: filter.as_ref(),
                },
            )?;
            outputs.push((self.field_name(&aggregate.field), value, ty));
        }
        let mut groups = Vec::new();
        for grouping in &stats.groupings {
            let (value, ty) = self.value(&grouping.expr, &mut Cx::Row)?;
            let internal = self.fresh();
            groups.push((value, internal.clone()));
            outputs.push((self.field_name(grouping), BExpr::Column(internal), ty));
        }
        let mut reads: Vec<&BExpr> = groups.iter().map(|(e, _)| e).collect();
        for (call, _) in &aggregates {
            reads.extend(&call.args);
            reads.extend(&call.filter);
        }
        self.flush_pre(&reads)?;
        let carry: Vec<String> = groups
            .iter()
            .map(|(_, name)| name)
            .chain(aggregates.iter().map(|(_, name)| name))
            .cloned()
            .collect();
        self.push(Stage::Aggregate { groups, aggregates })?;
        let post = std::mem::take(&mut self.post);
        let reads: Vec<&BExpr> = outputs.iter().map(|(_, e, _)| e).collect();
        self.flush(post, &carry, &reads)?;

        let mut cols: Vec<Col> = Vec::new();
        let mut columns = Vec::new();
        for (name, value, ty) in outputs {
            let internal = self.fresh();
            if let Some(i) = cols.iter().position(|col| col.name == name) {
                cols.remove(i);
                columns.remove(i);
            }
            columns.push((value, internal.clone()));
            cols.push(Col { name, internal, ty });
        }
        self.push(Stage::Project(columns))?;
        self.scope.cols = cols;
        self.scope.attrs = None;
        self.scope.aliases.clear();
        self.scope.blocked.clear();
        Ok(())
    }

    fn value(&mut self, e: &Expr, cx: &mut Cx<'_>) -> Result<(BExpr, EsqlType), EsqlError> {
        match self.bind(e, cx)? {
            Value::Expr(value, ty) => Ok((value, ty)),
            Value::Duration(..) => Err(self.verification(
                e.span(),
                format!(
                    "[{}] is a time duration, which is not a value outside date arithmetic and BUCKET",
                    self.text(e.span())
                ),
            )),
        }
    }

    /// Bind `e`, refusing it when the statement would pass
    /// [`MAX_BOUND_NODES`] with it.
    fn bind(&mut self, e: &Expr, cx: &mut Cx<'_>) -> Result<Value, EsqlError> {
        let value = self.bind_node(e, cx)?;
        if let Value::Expr(expr, _) = &value {
            self.within_budget(self.nodes.saturating_add(expr.nodes()), e.span())?;
        }
        Ok(value)
    }

    fn bind_node(&mut self, e: &Expr, cx: &mut Cx<'_>) -> Result<Value, EsqlError> {
        match e {
            Expr::Literal(literal) => self.literal(&literal.value, literal.span),
            Expr::Interval(interval) => {
                let period =
                    matches!(interval.kind(), Some(IntervalKind::DatePeriod)).then(|| Period {
                        span: interval.span,
                        unit: interval.unit.clone(),
                    });
                self.interval(interval)
                    .map(|nanos| Value::Duration(nanos, period))
            }
            Expr::Field(name) => {
                let text = name_text(name);
                if matches!(cx, Cx::Stats { .. }) {
                    return Err(self.verification(
                        name.span,
                        format!(
                            "column [{text}] must appear in the STATS BY clause or be used in an aggregate function"
                        ),
                    ));
                }
                match self.scope.lookup(&text) {
                    Lookup::Visible(col) => {
                        let col = &self.scope.cols[col];
                        Ok(Value::Expr(BExpr::Column(col.internal.clone()), col.ty))
                    }
                    Lookup::Attribute => match self.attribute(&text) {
                        Some(value) => Ok(Value::Expr(value, EsqlType::Keyword)),
                        None => Err(self.unknown(&text, name.span)),
                    },
                    Lookup::Unknown => Err(self.unknown(&text, name.span)),
                }
            }
            Expr::Param(param) => Err(self.unsupported(param.span, "query parameter")),
            Expr::Call(call) => self.call(call, cx),
            Expr::Unary(unary) => self.unary(unary, cx),
            Expr::Binary(binary) => self.binary(binary, cx),
            Expr::In(list) => self.in_list(list, cx),
            Expr::Like(like) => Err(self.unsupported(
                like.span,
                match like.kind {
                    ast::LikeKind::Like => "LIKE",
                    ast::LikeKind::RLike => "RLIKE",
                },
            )),
            Expr::IsNull(test) => {
                let (value, _) = self.value(&test.expr, cx)?;
                Ok(Value::Expr(
                    BExpr::IsNull(Box::new(value), test.negated),
                    EsqlType::Boolean,
                ))
            }
            Expr::Cast(cast) => {
                Err(self.unsupported(cast.span, format!("cast to [{}]", cast.type_name)))
            }
            Expr::Array(array) => Err(self.unsupported(array.span, "array literal")),
            other => Err(self.unsupported(other.span(), "this expression")),
        }
    }

    fn literal(&self, value: &LiteralValue, span: Span) -> Result<Value, EsqlError> {
        let (value, ty) = match value {
            LiteralValue::Null => (ScalarValue::Null, EsqlType::Null),
            LiteralValue::Boolean(b) => (ScalarValue::Boolean(Some(*b)), EsqlType::Boolean),
            LiteralValue::Integer(text) => match text.parse::<i64>() {
                Ok(v) => (ScalarValue::Int64(Some(v)), EsqlType::Long),
                Err(_) => {
                    return Err(self.unsupported(
                        span,
                        format!("integer literal [{text}] outside the long range"),
                    ));
                }
            },
            LiteralValue::Decimal(text) => match text.parse::<f64>() {
                Ok(v) if v.is_finite() => (ScalarValue::Float64(Some(v)), EsqlType::Double),
                _ => {
                    return Err(self.verification(
                        span,
                        format!("decimal literal [{text}] is not a finite double"),
                    ));
                }
            },
            LiteralValue::String(s) => (ScalarValue::Utf8(Some(s.clone())), EsqlType::Keyword),
        };
        Ok(Value::Expr(BExpr::Literal(value), ty))
    }

    fn interval(&self, interval: &ast::Interval) -> Result<i64, EsqlError> {
        let unit = interval.unit.to_ascii_lowercase();
        let nanos: i64 = match unit.as_str() {
            "millisecond" | "milliseconds" | "ms" => 1_000_000,
            "second" | "seconds" | "sec" | "s" => 1_000_000_000,
            "minute" | "minutes" | "min" => 60_000_000_000,
            "hour" | "hours" | "h" => 3_600_000_000_000,
            "day" | "days" | "d" => 86_400_000_000_000,
            "week" | "weeks" | "w" => 604_800_000_000_000,
            _ => {
                return Err(self.unsupported(
                    interval.span,
                    format!("calendar interval [{}]", self.text(interval.span)),
                ));
            }
        };
        interval
            .value
            .parse::<i64>()
            .ok()
            .and_then(|count| count.checked_mul(nanos))
            .ok_or_else(|| {
                self.verification(
                    interval.span,
                    format!(
                        "interval [{}] does not fit in a signed 64-bit count of nanoseconds",
                        self.text(interval.span)
                    ),
                )
            })
    }

    fn call(&mut self, call: &ast::Call, cx: &mut Cx<'_>) -> Result<Value, EsqlError> {
        let name = call.name.to_ascii_lowercase();
        let args: &[Expr] = match &call.args {
            CallArgs::List(args) => args,
            CallArgs::Star => &[],
        };
        let star = matches!(call.args, CallArgs::Star);
        match name.as_str() {
            "count" | "sum" | "min" | "max" | "avg" => self.aggregate(call, &name, cx),
            _ if star => Err(self.verification(
                call.span,
                format!(
                    "[*] is only an argument of COUNT, found [{}]",
                    self.text(call.span)
                ),
            )),
            "now" if args.is_empty() => Ok(Value::Expr(
                timestamp_literal(Some(self.now_ns)),
                EsqlType::DateNanos,
            )),
            "now" => Err(self.verification(call.span, "NOW takes no arguments")),
            "bucket" => {
                if args.len() != 2 {
                    return Err(self
                        .unsupported(call.span, format!("BUCKET with {} arguments", args.len())));
                }
                let (value, ty) = self.value(&args[0], cx)?;
                match ty {
                    EsqlType::DateNanos | EsqlType::Null => {}
                    EsqlType::Long | EsqlType::Double => {
                        return Err(self.unsupported(call.span, "BUCKET over a numeric value"));
                    }
                    other => {
                        return Err(self.verification(
                            args[0].span(),
                            format!("first argument of BUCKET must be a date, found [{other}]"),
                        ));
                    }
                }
                // date_bin bins from the epoch in fixed nanoseconds, so it
                // matches ES|QL's calendar-aligned buckets only for a fixed
                // duration (ADR-2887 D9). A month, quarter or year is refused
                // here, before `interval` refuses it as a calendar interval.
                if let Expr::Interval(interval) = &args[1]
                    && matches!(interval.kind(), Some(IntervalKind::DatePeriod))
                {
                    return Err(self.bucket_period(&Period {
                        span: interval.span,
                        unit: interval.unit.clone(),
                    }));
                }
                let width = match self.bind(&args[1], cx)? {
                    Value::Duration(_, Some(period)) => return Err(self.bucket_period(&period)),
                    Value::Duration(width, None) if width > 0 => width,
                    _ => {
                        return Err(self.verification(
                            args[1].span(),
                            format!(
                                "second argument of BUCKET must be a positive time duration, found [{}]",
                                self.text(args[1].span())
                            ),
                        ));
                    }
                };
                Ok(Value::Expr(
                    BExpr::Scalar(
                        "date_bin",
                        vec![
                            BExpr::Literal(ScalarValue::IntervalMonthDayNano(Some(
                                IntervalMonthDayNano::new(0, 0, width),
                            ))),
                            value,
                        ],
                    ),
                    EsqlType::DateNanos,
                ))
            }
            _ => Err(self.unsupported(
                call.span,
                format!("function [{}]", call.name.to_ascii_uppercase()),
            )),
        }
    }

    fn aggregate(
        &mut self,
        call: &ast::Call,
        name: &str,
        cx: &mut Cx<'_>,
    ) -> Result<Value, EsqlError> {
        let text = self.text(call.span);
        let Cx::Stats { calls, filter } = cx else {
            let message = match cx {
                Cx::InAggregate => format!("nested aggregate [{text}] is not allowed"),
                _ => format!("aggregate function [{text}] not allowed outside STATS command"),
            };
            return Err(self.verification(call.span, message));
        };
        let (arg, ty) = match &call.args {
            CallArgs::Star if name == "count" => {
                (BExpr::Literal(ScalarValue::Int64(Some(1))), EsqlType::Long)
            }
            CallArgs::Star => {
                return Err(self.verification(
                    call.span,
                    format!("[*] is only an argument of COUNT, found [{text}]"),
                ));
            }
            CallArgs::List(args) if args.len() == 1 => {
                self.value(&args[0], &mut Cx::InAggregate)?
            }
            CallArgs::List(_) => {
                return Err(self.verification(
                    call.span,
                    format!("[{}] takes exactly one argument", name.to_ascii_uppercase()),
                ));
            }
        };
        let internal = self.fresh();
        let column = BExpr::Column(internal.clone());
        let (function, arg, value, result) = match (name, ty) {
            ("count", _) => ("count", arg, column, EsqlType::Long),
            (_, EsqlType::Null) => {
                return Ok(Value::Expr(
                    BExpr::Literal(ScalarValue::Null),
                    EsqlType::Null,
                ));
            }
            ("sum", EsqlType::Long) => (
                "sum",
                arg.cast(DECIMAL),
                column.try_cast(DataType::Int64),
                EsqlType::Long,
            ),
            ("sum", EsqlType::Double) => ("sum", arg, self.finite(column, true), EsqlType::Double),
            ("avg", EsqlType::Long | EsqlType::Double) => ("avg", arg, column, EsqlType::Double),
            ("min", ty) if ty != EsqlType::Null => ("min", arg, column, ty),
            ("max", ty) if ty != EsqlType::Null => ("max", arg, column, ty),
            (_, other) => {
                return Err(self.verification(
                    call.span,
                    format!("argument of [{text}] must be [numeric], found [{other}]"),
                ));
            }
        };
        calls.push((
            BAggregate {
                function,
                args: vec![arg],
                filter: filter.cloned(),
            },
            internal,
        ));
        Ok(Value::Expr(value, result))
    }

    fn unary(&mut self, unary: &ast::Unary, cx: &mut Cx<'_>) -> Result<Value, EsqlError> {
        if unary.op == UnaryOp::Minus
            && let Expr::Literal(ast::Literal {
                value: LiteralValue::Integer(digits),
                ..
            }) = unary.operand.as_ref()
        {
            return self.literal(&LiteralValue::Integer(format!("-{digits}")), unary.span);
        }
        let operand = self.bind(&unary.operand, cx)?;
        let (value, ty) = match operand {
            Value::Duration(d, period) if unary.op == UnaryOp::Minus => {
                return d
                    .checked_neg()
                    .map(|d| Value::Duration(d, period))
                    .ok_or_else(|| {
                        self.verification(unary.span, "interval does not fit in 64 bits")
                    });
            }
            Value::Duration(d, period) if unary.op == UnaryOp::Plus => {
                return Ok(Value::Duration(d, period));
            }
            Value::Duration(..) => {
                return Err(self.mismatch(unary.span, "boolean", "time_duration"));
            }
            Value::Expr(value, ty) => (value, ty),
        };
        Ok(match (unary.op, ty) {
            (_, EsqlType::Null) => Value::Expr(BExpr::Literal(ScalarValue::Null), EsqlType::Null),
            (UnaryOp::Not, EsqlType::Boolean) => {
                Value::Expr(BExpr::Not(Box::new(value)), EsqlType::Boolean)
            }
            (UnaryOp::Plus, EsqlType::Long | EsqlType::Double) => Value::Expr(value, ty),
            (UnaryOp::Minus, EsqlType::Long) => Value::Expr(
                checked_long(
                    BExpr::Literal(ScalarValue::Int64(Some(0))),
                    Operator::Minus,
                    value,
                ),
                EsqlType::Long,
            ),
            (UnaryOp::Minus, EsqlType::Double) => {
                Value::Expr(BExpr::Negative(Box::new(value)), EsqlType::Double)
            }
            (UnaryOp::Not, other) => {
                return Err(self.mismatch(unary.span, "boolean", other.name()));
            }
            (_, other) => return Err(self.mismatch(unary.span, "numeric", other.name())),
        })
    }

    fn mismatch(&self, span: Span, expected: &str, found: &str) -> EsqlError {
        self.verification(
            span,
            format!(
                "argument of [{}] must be [{expected}], found [{found}]",
                self.text(span)
            ),
        )
    }

    fn binary(&mut self, binary: &ast::Binary, cx: &mut Cx<'_>) -> Result<Value, EsqlError> {
        let op = match binary.op {
            BinaryOp::Or => Operator::Or,
            BinaryOp::And => Operator::And,
            BinaryOp::Eq => Operator::Eq,
            BinaryOp::NotEq => Operator::NotEq,
            BinaryOp::Lt => Operator::Lt,
            BinaryOp::LtEq => Operator::LtEq,
            BinaryOp::Gt => Operator::Gt,
            BinaryOp::GtEq => Operator::GtEq,
            BinaryOp::Add => Operator::Plus,
            BinaryOp::Sub => Operator::Minus,
            BinaryOp::Mul => Operator::Multiply,
            BinaryOp::Div => Operator::Divide,
            BinaryOp::Mod => Operator::Modulo,
            BinaryOp::Match => return Err(self.unsupported(binary.span, "match operator [:]")),
        };
        let left = self.bind(&binary.left, cx)?;
        let right = self.bind(&binary.right, cx)?;
        match op {
            Operator::Or | Operator::And => self.logical(op, left, right, binary),
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => self.compare(op, left, right, binary),
            _ => self.arithmetic(op, left, right, binary, matches!(cx, Cx::Stats { .. })),
        }
    }

    fn operand_type(value: &Value) -> &'static str {
        match value {
            Value::Expr(_, ty) => ty.name(),
            Value::Duration(..) => "time_duration",
        }
    }

    fn logical(
        &self,
        op: Operator,
        left: Value,
        right: Value,
        b: &ast::Binary,
    ) -> Result<Value, EsqlError> {
        let mut sides = Vec::new();
        for (value, span) in [(left, b.left.span()), (right, b.right.span())] {
            match value {
                Value::Expr(value, EsqlType::Boolean) => sides.push(value),
                Value::Expr(_, EsqlType::Null) => {
                    sides.push(BExpr::Literal(ScalarValue::Boolean(None)))
                }
                other => {
                    return Err(self.mismatch(span, "boolean", Self::operand_type(&other)));
                }
            }
        }
        let right = sides
            .pop()
            .unwrap_or(BExpr::Literal(ScalarValue::Boolean(None)));
        let left = sides
            .pop()
            .unwrap_or(BExpr::Literal(ScalarValue::Boolean(None)));
        Ok(Value::Expr(
            BExpr::binary(left, op, right),
            EsqlType::Boolean,
        ))
    }

    fn compare(
        &self,
        op: Operator,
        left: Value,
        right: Value,
        b: &ast::Binary,
    ) -> Result<Value, EsqlError> {
        let (Value::Expr(mut l, lt), Value::Expr(mut r, rt)) = (left, right) else {
            return Err(self.verification(
                b.span,
                format!(
                    "[{}] compares a time duration, which is not a value",
                    self.text(b.span)
                ),
            ));
        };
        if lt == EsqlType::Null || rt == EsqlType::Null {
            return Ok(Value::Expr(
                BExpr::Literal(ScalarValue::Boolean(None)),
                EsqlType::Boolean,
            ));
        }
        if lt == EsqlType::DateNanos && rt == EsqlType::Keyword {
            r = self.date(r, b.right.span(), lt, rt, b)?;
        } else if lt == EsqlType::Keyword && rt == EsqlType::DateNanos {
            l = self.date(l, b.left.span(), rt, lt, b)?;
        } else if !(lt == rt || (lt.is_numeric() && rt.is_numeric())) {
            return Err(self.compare_mismatch(b.span, lt, rt));
        }
        Ok(Value::Expr(BExpr::binary(l, op, r), EsqlType::Boolean))
    }

    fn compare_mismatch(&self, span: Span, lt: EsqlType, rt: EsqlType) -> EsqlError {
        self.verification(
            span,
            format!(
                "first argument of [{}] is [{lt}] so second argument must also be [{lt}] but was [{rt}]",
                self.text(span)
            ),
        )
    }

    /// A string literal compared with a date, as the date it names.
    fn date(
        &self,
        value: BExpr,
        span: Span,
        lt: EsqlType,
        rt: EsqlType,
        b: &ast::Binary,
    ) -> Result<BExpr, EsqlError> {
        let BExpr::Literal(ScalarValue::Utf8(Some(text))) = &value else {
            return Err(self.compare_mismatch(b.span, lt, rt));
        };
        string_to_timestamp_nanos(text)
            .map(|nanos| timestamp_literal(Some(nanos)))
            .map_err(|_| self.verification(span, format!("cannot parse [{text}] as a date")))
    }

    /// `post` places shared operands over the aggregate's output, for a
    /// `STATS` field.
    fn arithmetic(
        &mut self,
        op: Operator,
        left: Value,
        right: Value,
        b: &ast::Binary,
        post: bool,
    ) -> Result<Value, EsqlError> {
        let additive = matches!(op, Operator::Plus | Operator::Minus);
        match (&left, &right) {
            (Value::Expr(_, EsqlType::Null), _) | (_, Value::Expr(_, EsqlType::Null)) => {
                return Ok(Value::Expr(
                    BExpr::Literal(ScalarValue::Null),
                    EsqlType::Null,
                ));
            }
            (Value::Duration(a, pa), Value::Duration(b_, pb)) if additive => {
                let sum = if op == Operator::Plus {
                    a.checked_add(*b_)
                } else {
                    a.checked_sub(*b_)
                };
                let period = pa.clone().or_else(|| pb.clone());
                return sum
                    .map(|sum| Value::Duration(sum, period))
                    .ok_or_else(|| self.verification(b.span, "interval does not fit in 64 bits"));
            }
            (Value::Expr(date, EsqlType::DateNanos), Value::Duration(d, _)) if additive => {
                return Ok(date_shift(date.clone(), op, *d));
            }
            (Value::Duration(d, _), Value::Expr(date, EsqlType::DateNanos))
                if op == Operator::Plus =>
            {
                return Ok(date_shift(date.clone(), op, *d));
            }
            _ => {}
        }
        match (left, right) {
            (Value::Expr(l, EsqlType::Long), Value::Expr(r, EsqlType::Long)) => Ok(Value::Expr(
                self.long_arithmetic(l, op, r, post),
                EsqlType::Long,
            )),
            (Value::Expr(l, lt), Value::Expr(r, rt)) if lt.is_numeric() && rt.is_numeric() => {
                let raw = BExpr::binary(as_double(l, lt), op, as_double(r, rt));
                Ok(Value::Expr(self.finite(raw, post), EsqlType::Double))
            }
            (left, right) => Err(self.verification(
                b.span,
                format!(
                    "[{}] has arguments with incompatible types [{}] and [{}]",
                    self.text(b.span),
                    Self::operand_type(&left),
                    Self::operand_type(&right)
                ),
            )),
        }
    }

    /// Long arithmetic with ES|QL's null results: overflow, a zero divisor,
    /// and `i64::MIN / -1` give null; `x % -1` is `0`. No operand reaches an
    /// Arrow kernel that can fail, whichever branches the engine evaluates.
    /// Each operand of `/` and `%` is read twice, so it is shared.
    fn long_arithmetic(&mut self, left: BExpr, op: Operator, right: BExpr, post: bool) -> BExpr {
        if !matches!(op, Operator::Divide | Operator::Modulo) {
            return checked_long(left, op, right);
        }
        let left = self.share(left, post);
        let right = self.share(right, post);
        let divisor = BExpr::Scalar(
            "nullif",
            vec![
                BExpr::Scalar("nullif", vec![right.clone(), long_literal(0)]),
                long_literal(-1),
            ],
        );
        let minus_one = BExpr::binary(right, Operator::Eq, long_literal(-1));
        let on_minus_one = if op == Operator::Divide {
            checked_long(long_literal(0), Operator::Minus, left.clone())
        } else {
            BExpr::binary(left.clone(), Operator::Multiply, long_literal(0))
        };
        BExpr::Case(
            vec![(minus_one, on_minus_one)],
            Some(Box::new(BExpr::binary(left, op, divisor))),
        )
    }

    /// `value` when it is a finite double, else null: ES|QL reports an
    /// infinite or NaN result as null. `value` is read three times, so it is
    /// shared.
    fn finite(&mut self, value: BExpr, post: bool) -> BExpr {
        let value = self.share(value, post);
        let within = BExpr::binary(
            BExpr::binary(
                value.clone(),
                Operator::GtEq,
                BExpr::Literal(ScalarValue::Float64(Some(f64::MIN))),
            ),
            Operator::And,
            BExpr::binary(
                value.clone(),
                Operator::LtEq,
                BExpr::Literal(ScalarValue::Float64(Some(f64::MAX))),
            ),
        );
        BExpr::Case(vec![(within, value)], None)
    }

    fn in_list(&mut self, list: &ast::InList, cx: &mut Cx<'_>) -> Result<Value, EsqlError> {
        let (value, ty) = self.value(&list.expr, cx)?;
        let mut items = Vec::new();
        for item in &list.list {
            let (bound, item_ty) = self.value(item, cx)?;
            if !matches!(bound, BExpr::Literal(_)) {
                return Err(self.unsupported(item.span(), "IN over a value that is not a literal"));
            }
            let bound = if ty == EsqlType::DateNanos && item_ty == EsqlType::Keyword {
                let BExpr::Literal(ScalarValue::Utf8(Some(text))) = &bound else {
                    return Err(
                        self.unsupported(item.span(), "IN over a value that is not a literal")
                    );
                };
                string_to_timestamp_nanos(text)
                    .map(|nanos| timestamp_literal(Some(nanos)))
                    .map_err(|_| {
                        self.verification(item.span(), format!("cannot parse [{text}] as a date"))
                    })?
            } else if item_ty == EsqlType::Null
                || ty == EsqlType::Null
                || item_ty == ty
                || (ty.is_numeric() && item_ty.is_numeric())
            {
                bound
            } else {
                return Err(self.verification(
                    item.span(),
                    format!(
                        "[{}] has an element of type [{item_ty}] in a list over [{ty}]",
                        self.text(list.span)
                    ),
                ));
            };
            items.push(bound);
        }
        if ty == EsqlType::Null {
            return Ok(Value::Expr(
                BExpr::Literal(ScalarValue::Boolean(None)),
                EsqlType::Boolean,
            ));
        }
        Ok(Value::Expr(
            BExpr::InList(Box::new(value), items, list.negated),
            EsqlType::Boolean,
        ))
    }
}

/// The exact intermediate type for long arithmetic: any sum, difference or
/// product of two longs fits, and a result outside the long range reads null
/// on the cast back.
const DECIMAL: DataType = DataType::Decimal128(38, 0);

fn column(name: &str) -> BExpr {
    BExpr::Column(name.to_string())
}

fn hex(value: BExpr) -> BExpr {
    BExpr::Scalar(
        "encode",
        vec![
            value.cast(DataType::Binary),
            BExpr::Literal(ScalarValue::Utf8(Some("hex".to_string()))),
        ],
    )
}

fn timestamp_literal(nanos: Option<i64>) -> BExpr {
    BExpr::Literal(ScalarValue::TimestampNanosecond(nanos, None))
}

fn long_literal(value: i64) -> BExpr {
    BExpr::Literal(ScalarValue::Int64(Some(value)))
}

fn as_double(value: BExpr, ty: EsqlType) -> BExpr {
    match ty {
        EsqlType::Long => value.cast(DataType::Float64),
        _ => value,
    }
}

/// `left op right` over longs for `+ - *`, null when the result leaves the
/// long range.
fn checked_long(left: BExpr, op: Operator, right: BExpr) -> BExpr {
    BExpr::binary(left.cast(DECIMAL), op, right.cast(DECIMAL)).try_cast(DataType::Int64)
}

/// `date op duration`, null when the result leaves the representable range. A
/// constant date folds to a literal, so a bound such as `NOW() - 1 hour`
/// reaches the window as one.
fn date_shift(date: BExpr, op: Operator, duration: i64) -> Value {
    let shifted = match date {
        BExpr::Literal(ScalarValue::TimestampNanosecond(nanos, _)) => {
            timestamp_literal(nanos.and_then(|nanos| {
                if op == Operator::Plus {
                    nanos.checked_add(duration)
                } else {
                    nanos.checked_sub(duration)
                }
            }))
        }
        other => checked_long(other.cast(DataType::Int64), op, long_literal(duration))
            .cast(DataType::Timestamp(TimeUnit::Nanosecond, None)),
    };
    Value::Expr(shifted, EsqlType::DateNanos)
}

fn name_text(name: &QualifiedName) -> String {
    name.parts
        .iter()
        .map(|part| part.text.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

/// A `KEEP` or `DROP` name: literal characters, and `None` for each `*` of an
/// unquoted part.
#[derive(Debug, Clone)]
struct Pattern {
    chars: Vec<Option<char>>,
}

impl Pattern {
    fn of(name: &QualifiedName) -> Self {
        let mut chars = Vec::new();
        for (i, part) in name.parts.iter().enumerate() {
            if i > 0 {
                chars.push(Some('.'));
            }
            chars.extend(
                part.text
                    .chars()
                    .map(|c| (part.quoted || c != '*').then_some(c)),
            );
        }
        Pattern { chars }
    }

    fn is_wildcard(&self) -> bool {
        self.chars.iter().any(Option::is_none)
    }

    fn is_star_only(&self) -> bool {
        self.chars.iter().all(Option::is_none)
    }

    /// Glob match in O(pattern * name).
    fn matches(&self, name: &str) -> bool {
        let text: Vec<char> = name.chars().collect();
        let p = &self.chars;
        let (mut pi, mut ti) = (0, 0);
        let mut star: Option<(usize, usize)> = None;
        while ti < text.len() {
            if pi < p.len() && p[pi] == Some(text[ti]) {
                pi += 1;
                ti += 1;
            } else if pi < p.len() && p[pi].is_none() {
                star = Some((pi, ti));
                pi += 1;
            } else if let Some((sp, st)) = star {
                pi = sp + 1;
                ti = st + 1;
                star = Some((sp, st + 1));
            } else {
                return false;
            }
        }
        while pi < p.len() && p[pi].is_none() {
            pi += 1;
        }
        pi == p.len()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn bound(source: &str) -> Result<Bound, EsqlError> {
        let query = ravel_esql::parse(source, &ravel_esql::Limits::default()).expect("parses");
        bind(
            &query,
            &BindInput {
                source,
                declared: &[],
                now_ns: 0,
                start: None,
                end: None,
            },
        )
    }

    /// The bound nodes of `ROW x = first op 1 op 1 ...` with `n` operators.
    fn chain(first: &str, op: &str, n: usize) -> usize {
        let source = format!("ROW x = {first}{}", format!(" {op} 1").repeat(n));
        bound(&source).expect("binds").pipeline.nodes()
    }

    /// The node counts at 10, 20 and 40 operators, asserting the first is
    /// `at_ten` before binding the longer chains.
    fn growth(first: &str, op: &str, at_ten: usize) -> [usize; 3] {
        let ten = chain(first, op, 10);
        assert_eq!(ten, at_ten, "{first} {op} x10");
        [ten, chain(first, op, 20), chain(first, op, 40)]
    }

    /// The densest shapes measured: a long division per two tokens, in a
    /// row, over the scope's columns, and over aggregate results, each
    /// within the default token limit and its nesting limit.
    #[test]
    fn the_largest_dense_statements_bind_within_the_budget() {
        let row = vec![format!("(9{})", " / 1".repeat(60)); 50].join(" + ");
        let row = vec![format!("x = {row}"); 10].join(", ");
        let stats = vec![format!("(SUM(severity_num){})", " / 1".repeat(60)); 50].join(" + ");
        let stats: Vec<String> = (0..10).map(|i| format!("x{i} = {stats}")).collect();
        for source in [
            format!("ROW {row}"),
            format!("FROM logs | EVAL {row}"),
            format!("FROM logs | STATS {}", stats.join(", ")),
        ] {
            let tokens = ravel_esql::lexer::tokenize(&source, &ravel_esql::Limits::default())
                .expect("lexes")
                .len();
            assert!(tokens > 60_000, "{tokens}");
            let nodes = bound(&source).expect("binds").pipeline.nodes();
            assert!(nodes < 9 * tokens, "{nodes} nodes for {tokens} tokens");
        }
        assert!(9 * ravel_esql::Limits::default().max_tokens < MAX_BOUND_NODES);
    }

    #[test]
    fn a_chain_of_double_additions_binds_to_linearly_many_nodes() {
        let [ten, twenty, forty] = growth("1.0", "+", 126);
        let step = (twenty - ten) / 10;
        assert_eq!(step, 12);
        assert_eq!(twenty - ten, 10 * step);
        assert_eq!(forty - twenty, 20 * step);
    }

    #[test]
    fn a_chain_of_long_divisions_binds_to_linearly_many_nodes() {
        let [ten, twenty, forty] = growth("9", "/", 170);
        let step = (twenty - ten) / 10;
        assert_eq!(step, 17);
        assert_eq!(twenty - ten, 10 * step);
        assert_eq!(forty - twenty, 20 * step);
    }
}
