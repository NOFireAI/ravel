//! Syntax tree for ES|QL statements. Every node carries the [`Span`] of the
//! text it was parsed from; a parenthesised expression's span covers its
//! parentheses.

mod eq;
mod print;

pub use print::print;

use crate::span::Span;

/// A query: optional `SET` directives, a source command, then processing
/// commands. A nested pipeline (a `FORK` branch, a subquery in `FROM` or in
/// `IN`) is a [`Query`] or a list of [`Command`]s inside its parent's tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    /// The source command, such as `FROM` or `ROW`.
    pub source: Command,
    /// The commands after each `|`, in order.
    pub processing: Vec<Command>,
    /// From the source command's keyword to the end of the last command;
    /// `SET` directives before the source are outside it.
    pub span: Span,
    /// The `SET name = value;` directives before the source command, in
    /// order. Always empty in a nested query.
    pub settings: Vec<Setting>,
}

/// `SET name = value;`, a query setting written before the source command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Setting {
    /// The setting's name.
    pub name: NamePart,
    /// A constant (a literal, an optionally signed number or a parameter) or
    /// a [`MapLiteral`].
    pub value: Expr,
    /// From `SET` to the semicolon.
    pub span: Span,
}

/// One pipeline stage. A command larger than [`FromCommand`] is boxed, so a
/// `Command`, and every parser frame holding one, stays that size.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Command {
    /// `ROW a = 1, ...`
    Row(RowCommand),
    /// `FROM sources [METADATA fields]`
    From(FromCommand),
    /// `WHERE condition`
    Where(WhereCommand),
    /// `EVAL a = expr, ...`
    Eval(EvalCommand),
    /// `KEEP patterns`
    Keep(KeepCommand),
    /// `DROP patterns`
    Drop(DropCommand),
    /// `RENAME old AS new, ...`
    Rename(RenameCommand),
    /// `STATS aggregates [BY groupings]`
    Stats(StatsCommand),
    /// `SORT keys`
    Sort(SortCommand),
    /// `LIMIT n`
    Limit(LimitCommand),
    /// `SHOW INFO`
    Show(ShowCommand),
    /// `TS sources [METADATA fields]`: a time series source, shaped like
    /// `FROM`.
    Ts(FromCommand),
    /// `FROM` with at least one parenthesised subquery among its sources.
    FromQueries(FromQueriesCommand),
    /// `PROMQL [name = value ...] (query)`
    Promql(Box<PromqlCommand>),
    /// `DISSECT input "pattern" [APPEND_SEPARATOR = "separator"]`
    Dissect(Box<DissectCommand>),
    /// `GROK input "pattern"`
    Grok(Box<GrokCommand>),
    /// `MV_EXPAND field`
    MvExpand(MvExpandCommand),
    /// `LOOKUP JOIN index ON field, ...`
    LookupJoin(Box<LookupJoinCommand>),
    /// `ENRICH policy [ON field] [WITH [name =] field, ...]`
    Enrich(Box<EnrichCommand>),
    /// `INLINE STATS aggregates [BY groupings]`, also spelled `INLINESTATS`.
    InlineStats(StatsCommand),
    /// `FORK (commands) (commands) ...`
    Fork(ForkCommand),
    /// `FUSE [method] [SCORE BY field] [KEY BY fields] [GROUP BY field]
    /// [WITH options]`
    Fuse(Box<FuseCommand>),
    /// `CHANGE_POINT value [ON key] [AS type_name, pvalue_name]`
    ChangePoint(Box<ChangePointCommand>),
    /// `SAMPLE probability`
    Sample(SampleCommand),
    /// `COMPLETION [target =] prompt WITH options`
    Completion(Box<CompletionCommand>),
    /// `RERANK [target =] query ON fields WITH options`
    Rerank(Box<RerankCommand>),
    /// `LIMIT n BY fields`
    LimitBy(LimitByCommand),
}

impl Command {
    /// From the command keyword to the end of the command's last token.
    pub fn span(&self) -> Span {
        match self {
            Command::Row(c) => c.span,
            Command::From(c) | Command::Ts(c) => c.span,
            Command::Where(c) => c.span,
            Command::Eval(c) => c.span,
            Command::Keep(c) => c.span,
            Command::Drop(c) => c.span,
            Command::Rename(c) => c.span,
            Command::Stats(c) | Command::InlineStats(c) => c.span,
            Command::Sort(c) => c.span,
            Command::Limit(c) => c.span,
            Command::Show(c) => c.span,
            Command::FromQueries(c) => c.span,
            Command::Promql(c) => c.span,
            Command::Dissect(c) => c.span,
            Command::Grok(c) => c.span,
            Command::MvExpand(c) => c.span,
            Command::LookupJoin(c) => c.span,
            Command::Enrich(c) => c.span,
            Command::Fork(c) => c.span,
            Command::Fuse(c) => c.span,
            Command::ChangePoint(c) => c.span,
            Command::Sample(c) => c.span,
            Command::Completion(c) => c.span,
            Command::Rerank(c) => c.span,
            Command::LimitBy(c) => c.span,
        }
    }
}

/// `SHOW INFO`: the deployment's build information.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowCommand {
    /// From `SHOW` to `INFO`.
    pub span: Span,
}

/// `FROM` whose sources include a subquery, such as
/// `FROM logs, (FROM archive | WHERE level == "error")`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromQueriesCommand {
    /// The sources in order; at least one index pattern comes first and at
    /// least one subquery follows somewhere.
    pub sources: Vec<FromSource>,
    /// The whole command.
    pub span: Span,
}

/// One source of a [`FromQueriesCommand`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FromSource {
    /// An index pattern.
    Pattern(SourcePattern),
    /// A parenthesised pipeline that starts with `FROM`.
    Query(Subquery),
}

/// A parenthesised nested query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subquery {
    /// The nested query; its `settings` are empty.
    pub query: Box<Query>,
    /// From `(` to `)`.
    pub span: Span,
}

/// `PROMQL [name = value ...] (query)`: a PromQL query embedded as a source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromqlCommand {
    /// The `name = value` parameters before the query, in order.
    pub params: Vec<PromqlParam>,
    /// The parenthesised PromQL text.
    pub query: PromqlQuery,
    /// The whole command.
    pub span: Span,
}

/// One `name = value` parameter of a [`PromqlCommand`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromqlParam {
    /// The parameter's name as written.
    pub name: String,
    /// Its value.
    pub value: PromqlValue,
    /// From the name to the value.
    pub span: Span,
}

/// The value of a [`PromqlParam`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromqlValue {
    /// An unquoted word or pattern such as `metrics-*` or `5m`, or a string.
    Text(SourcePattern),
    /// A query parameter.
    Param(Parameter),
}

/// The embedded PromQL text and the outcome of parsing it. The ES|QL parser
/// never splits this region on `|`; it hands the text to the guarded PromQL
/// parser and keeps only whether that succeeded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromqlQuery {
    /// The text between the parentheses, verbatim.
    pub text: String,
    /// The text between the parentheses.
    pub span: Span,
    /// `None` when the PromQL parser accepted the text, else its refusal.
    pub error: Option<PromqlError>,
}

/// Why the guarded PromQL parser refused an embedded query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromqlError {
    /// The PromQL parser's or complexity guard's message.
    pub message: String,
    /// The embedded PromQL text the message is about.
    pub span: Span,
}

/// `DISSECT input "pattern" [APPEND_SEPARATOR = "separator"]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DissectCommand {
    /// The text to split: a field, call, constant, parameter, cast or
    /// parenthesised expression.
    pub input: Expr,
    /// The decoded dissect pattern.
    pub pattern: String,
    /// The decoded `APPEND_SEPARATOR` value, when written.
    pub append_separator: Option<String>,
    /// The whole command.
    pub span: Span,
}

/// `GROK input "pattern"`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrokCommand {
    /// The text to match, as for [`DissectCommand::input`].
    pub input: Expr,
    /// The decoded grok pattern.
    pub pattern: String,
    /// The whole command.
    pub span: Span,
}

/// `MV_EXPAND field`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvExpandCommand {
    /// The multivalued column to expand into rows.
    pub field: QualifiedName,
    /// The whole command.
    pub span: Span,
}

/// `LOOKUP JOIN index ON field, ...`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupJoinCommand {
    /// The lookup index.
    pub index: SourcePattern,
    /// The join keys, at least one.
    pub on: Vec<QualifiedName>,
    /// The whole command.
    pub span: Span,
}

/// `ENRICH policy [ON field] [WITH [name =] field, ...]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrichCommand {
    /// The policy name, with any `_mode:` prefix, as written.
    pub policy: SourcePattern,
    /// The match field, when written.
    pub on: Option<QualifiedName>,
    /// The enrich fields to add; empty when there is no `WITH`.
    pub with: Vec<EnrichField>,
    /// The whole command.
    pub span: Span,
}

/// One `[name =] field` of an `ENRICH ... WITH`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrichField {
    /// The output column name, when the field is renamed.
    pub name: Option<QualifiedName>,
    /// The policy's enrich field.
    pub field: QualifiedName,
    /// From the name (or field) to the field.
    pub span: Span,
}

/// `FORK (commands) (commands) ...`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkCommand {
    /// The branches, at least two.
    pub branches: Vec<ForkBranch>,
    /// The whole command.
    pub span: Span,
}

/// One parenthesised branch of a `FORK`: processing commands separated by
/// `|`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkBranch {
    /// At least one processing command.
    pub commands: Vec<Command>,
    /// From `(` to `)`.
    pub span: Span,
}

/// `FUSE [method] [SCORE BY field] [KEY BY fields] [GROUP BY field]
/// [WITH options]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseCommand {
    /// `RRF` or `LINEAR`, when written.
    pub method: Option<FuseMethod>,
    /// The `SCORE BY` column, when written.
    pub score_by: Option<QualifiedName>,
    /// The `KEY BY` columns; empty when there is no `KEY BY`.
    pub key_by: Vec<QualifiedName>,
    /// The `GROUP BY` column, when written.
    pub group_by: Option<QualifiedName>,
    /// The `WITH` options, when written.
    pub options: Option<MapLiteral>,
    /// The whole command.
    pub span: Span,
}

/// How `FUSE` combines scores.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FuseMethod {
    /// `RRF`, reciprocal rank fusion.
    Rrf,
    /// `LINEAR`, a weighted sum of scores.
    Linear,
}

/// `CHANGE_POINT value [ON key] [AS type_name, pvalue_name]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangePointCommand {
    /// The metric column.
    pub value: QualifiedName,
    /// The ordering column, when written.
    pub key: Option<QualifiedName>,
    /// The two output column names, when written.
    pub output: Option<ChangePointOutput>,
    /// The whole command.
    pub span: Span,
}

/// `AS type_name, pvalue_name` on a `CHANGE_POINT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangePointOutput {
    /// The column for the change point's type.
    pub type_name: QualifiedName,
    /// The column for its p-value.
    pub pvalue_name: QualifiedName,
    /// From `AS` to the second name.
    pub span: Span,
}

/// `SAMPLE probability`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SampleCommand {
    /// The probability's digits as written, such as `0.25`.
    pub probability: String,
    /// The whole command.
    pub span: Span,
}

/// `COMPLETION [target =] prompt WITH options`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionCommand {
    /// The output column, when named.
    pub target: Option<QualifiedName>,
    /// The prompt, as for [`DissectCommand::input`].
    pub prompt: Expr,
    /// The `WITH` options, such as the inference endpoint.
    pub options: MapLiteral,
    /// The whole command.
    pub span: Span,
}

/// `RERANK [target =] query ON fields WITH options`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RerankCommand {
    /// The output score column, when named.
    pub target: Option<QualifiedName>,
    /// The query text: a string or a parameter.
    pub query: Expr,
    /// The columns to rerank on, at least one.
    pub on: Vec<QualifiedName>,
    /// The `WITH` options, such as the inference endpoint.
    pub options: MapLiteral,
    /// The whole command.
    pub span: Span,
}

/// `LIMIT n BY fields`: at most `n` rows for each distinct key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitByCommand {
    /// The row count per key.
    pub count: u64,
    /// The key columns, at least one.
    pub groupings: Vec<QualifiedName>,
    /// The whole command.
    pub span: Span,
}

/// `ROW`: one row of computed fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowCommand {
    /// At least one field.
    pub fields: Vec<Field>,
    /// The whole command.
    pub span: Span,
}

/// `FROM`: the sources to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromCommand {
    /// At least one source pattern.
    pub sources: Vec<SourcePattern>,
    /// The `METADATA` clause, when present.
    pub metadata: Option<Metadata>,
    /// The whole command.
    pub span: Span,
}

/// One source pattern of a `FROM`, such as `logs-prod-*` or `cluster:index`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourcePattern {
    /// The pattern as written when unquoted, or the decoded string when
    /// quoted.
    pub text: String,
    /// Whether the pattern was written as a string literal.
    pub quoted: bool,
    /// The pattern, quotes included.
    pub span: Span,
}

/// `METADATA field, ...` on a `FROM`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    /// At least one metadata field.
    pub fields: Vec<QualifiedName>,
    /// From the `METADATA` keyword to the last field.
    pub span: Span,
}

/// `WHERE`: a row filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WhereCommand {
    /// The filter condition.
    pub condition: Expr,
    /// The whole command.
    pub span: Span,
}

/// `EVAL`: computed columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvalCommand {
    /// At least one field.
    pub fields: Vec<Field>,
    /// The whole command.
    pub span: Span,
}

/// `KEEP`: the columns to keep, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeepCommand {
    /// At least one name; unquoted parts may contain `*`.
    pub patterns: Vec<QualifiedName>,
    /// The whole command.
    pub span: Span,
}

/// `DROP`: the columns to remove.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropCommand {
    /// At least one name; unquoted parts may contain `*`.
    pub patterns: Vec<QualifiedName>,
    /// The whole command.
    pub span: Span,
}

/// `RENAME old AS new, ...`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenameCommand {
    /// At least one rename.
    pub renames: Vec<RenamePair>,
    /// The whole command.
    pub span: Span,
}

/// One `old AS new` of a `RENAME`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenamePair {
    /// The existing column.
    pub old: QualifiedName,
    /// Its new name.
    pub new: QualifiedName,
    /// From `old` to `new`.
    pub span: Span,
}

/// `STATS`: aggregation, optionally grouped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatsCommand {
    /// The aggregates; empty only when `groupings` is not.
    pub aggregates: Vec<Aggregate>,
    /// The `BY` groupings; empty when there is no `BY`.
    pub groupings: Vec<Field>,
    /// The whole command.
    pub span: Span,
}

/// One aggregate of a `STATS`, with its own optional `WHERE` filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggregate {
    /// The optionally named aggregate expression.
    pub field: Field,
    /// The filter written after this aggregate, applying to it alone.
    pub filter: Option<Expr>,
    /// From the field to the end of the filter.
    pub span: Span,
}

/// `SORT key, ...`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortCommand {
    /// At least one key.
    pub keys: Vec<SortKey>,
    /// The whole command.
    pub span: Span,
}

/// One `SORT` key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
    /// The sort expression.
    pub expr: Expr,
    /// `ASC` or `DESC` when written.
    pub order: Option<SortOrder>,
    /// `NULLS FIRST` or `NULLS LAST` when written.
    pub nulls: Option<NullsOrder>,
    /// The key and its modifiers.
    pub span: Span,
}

/// `ASC` or `DESC`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SortOrder {
    /// `ASC`
    Asc,
    /// `DESC`
    Desc,
}

/// `NULLS FIRST` or `NULLS LAST`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NullsOrder {
    /// `NULLS FIRST`
    First,
    /// `NULLS LAST`
    Last,
}

/// `LIMIT n`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitCommand {
    /// The row count.
    pub count: u64,
    /// The whole command.
    pub span: Span,
}

/// `[name =] expr`, as in `ROW`, `EVAL`, `STATS` aggregates and groupings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    /// The assigned name, when written.
    pub name: Option<QualifiedName>,
    /// The value.
    pub expr: Expr,
    /// From the name (or expression) to the end of the expression.
    pub span: Span,
}

/// A dotted name such as `service.name` or `` `odd name`.x ``.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedName {
    /// At least one part.
    pub parts: Vec<NamePart>,
    /// From the first part to the last. When the name is a parenthesised
    /// expression such as `(a.b)`, the span covers the parentheses.
    pub span: Span,
}

/// One part of a [`QualifiedName`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamePart {
    /// The part as written when unquoted (spelling kept), or the decoded
    /// identifier when backtick-quoted. A parameter part keeps its text as
    /// written, such as `?name` or `??name`.
    pub text: String,
    /// Whether the part was backtick-quoted.
    pub quoted: bool,
    /// The part, backticks included.
    pub span: Span,
    /// The parameter standing for this part, when it is `?name`, `?1` or
    /// `??name` rather than a name: an identifier parameter, or a pattern
    /// parameter where the command takes name patterns.
    pub param: Option<Parameter>,
}

/// A query parameter's name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ParamName {
    /// `?`
    Positional,
    /// `?1`; the digits as written.
    Numbered(String),
    /// `?name`
    Named(String),
}

/// An expression.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Expr {
    /// `null`, `true`, `42`, `1.5`, `"text"`.
    Literal(Literal),
    /// A time duration or date period: `1 hour`, `5 minutes`, `1 day`.
    Interval(Interval),
    /// A column reference.
    Field(QualifiedName),
    /// `?`, `?1`, `?name`, or an identifier parameter `??name`.
    Param(Parameter),
    /// `name(args)` or `name(*)`.
    Call(Call),
    /// `NOT x`, `-x`, `+x`.
    Unary(Unary),
    /// `a OP b`, including the `:` match operator.
    Binary(Binary),
    /// `x [NOT] IN (a, b, ...)`
    In(InList),
    /// `x [NOT] LIKE "pattern"` and `x [NOT] RLIKE "pattern"`.
    Like(Like),
    /// `x IS [NOT] NULL`
    IsNull(IsNull),
    /// `x::type`
    Cast(Cast),
    /// `[1, 2, 3]`
    Array(ArrayLiteral),
    /// `x [NOT] IN (FROM ... | ...)`
    InSubquery(InSubquery),
    /// `{"key": value, ...}`, an option map: the last argument of a call, or
    /// a `SET` or `WITH` value.
    Map(MapLiteral),
}

/// A literal value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Literal {
    /// The value.
    pub value: LiteralValue,
    /// The literal as written; for a parenthesised literal such as `(1)`,
    /// the parentheses too.
    pub span: Span,
}

/// `x [NOT] IN (subquery)`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InSubquery {
    /// The tested value.
    pub expr: Box<Expr>,
    /// The parenthesised query; its `settings` are empty.
    pub query: Subquery,
    /// `NOT IN`
    pub negated: bool,
    /// From the tested value to the closing parenthesis.
    pub span: Span,
}

/// `{"key": value, ...}`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapLiteral {
    /// At least one entry, keys distinct, in written order.
    pub entries: Vec<MapEntry>,
    /// From `{` to `}`.
    pub span: Span,
}

/// One `"key": value` of a [`MapLiteral`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapEntry {
    /// The decoded key.
    pub key: String,
    /// The key, quotes included.
    pub key_span: Span,
    /// A constant, an array of constants or a nested [`MapLiteral`].
    pub value: Expr,
    /// From the key to the value.
    pub span: Span,
}

/// A literal's value. Numbers keep their text, so no precision is lost
/// before binding decides the type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiteralValue {
    /// `null`
    Null,
    /// `true` or `false`
    Boolean(bool),
    /// Decimal digits.
    Integer(String),
    /// A number with a fraction or exponent, as written.
    Decimal(String),
    /// A decoded string.
    String(String),
}

/// A quantity and a time unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interval {
    /// The integer quantity's digits.
    pub value: String,
    /// The unit as written, such as `hour` or `MINUTES`.
    pub unit: String,
    /// From the quantity to the unit.
    pub span: Span,
}

/// Whether an interval's unit makes it a time duration or a date period.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntervalKind {
    /// Milliseconds, seconds, minutes, hours.
    TimeDuration,
    /// Days, weeks, months, quarters, years.
    DatePeriod,
}

impl Interval {
    /// The interval's kind, or `None` for a unit the parser does not accept.
    pub fn kind(&self) -> Option<IntervalKind> {
        interval_unit_kind(&self.unit)
    }
}

const TIME_DURATION_UNITS: &[&str] = &[
    "millisecond",
    "milliseconds",
    "ms",
    "second",
    "seconds",
    "sec",
    "s",
    "minute",
    "minutes",
    "min",
    "hour",
    "hours",
    "h",
];

const DATE_PERIOD_UNITS: &[&str] = &[
    "day", "days", "d", "week", "weeks", "w", "month", "months", "mo", "quarter", "quarters", "q",
    "year", "years", "yr", "y",
];

pub(crate) fn interval_unit_kind(unit: &str) -> Option<IntervalKind> {
    if TIME_DURATION_UNITS
        .iter()
        .any(|u| u.eq_ignore_ascii_case(unit))
    {
        Some(IntervalKind::TimeDuration)
    } else if DATE_PERIOD_UNITS
        .iter()
        .any(|u| u.eq_ignore_ascii_case(unit))
    {
        Some(IntervalKind::DatePeriod)
    } else {
        None
    }
}

/// A query parameter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parameter {
    /// Its name.
    pub name: ParamName,
    /// `true` for the `??` identifier forms.
    pub identifier: bool,
    /// The parameter as written.
    pub span: Span,
}

/// A function call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    /// The function name as written.
    pub name: String,
    /// The arguments.
    pub args: CallArgs,
    /// From the name to the closing parenthesis.
    pub span: Span,
}

/// A call's arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallArgs {
    /// `(*)`, as in `COUNT(*)`.
    Star,
    /// A possibly empty argument list.
    List(Vec<Expr>),
}

/// A prefix operator application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unary {
    /// The operator.
    pub op: UnaryOp,
    /// The operand.
    pub operand: Box<Expr>,
    /// From the operator to the end of the operand.
    pub span: Span,
}

/// Prefix operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOp {
    /// `NOT`
    Not,
    /// `-`
    Minus,
    /// `+`
    Plus,
}

/// A binary operator application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binary {
    /// The operator.
    pub op: BinaryOp,
    /// Left operand.
    pub left: Box<Expr>,
    /// Right operand.
    pub right: Box<Expr>,
    /// From the left operand to the right.
    pub span: Span,
}

/// Binary operators. Lowest to highest precedence: `OR`; `AND`; the
/// comparisons and `:`, which do not chain; `+ -`; `* / %`. All chaining
/// operators associate to the left.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    /// `OR`
    Or,
    /// `AND`
    And,
    /// `==`
    Eq,
    /// `!=`
    NotEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
    /// `:`, the full-text match operator; its left side is a field (or a
    /// cast field) and its right side a constant.
    Match,
    /// `+`
    Add,
    /// `-`
    Sub,
    /// `*`
    Mul,
    /// `/`
    Div,
    /// `%`
    Mod,
}

/// Binding powers shared by the parser and the printer. Each level is odd so
/// that `level + 1` is a right-operand power no operator binds at.
pub(crate) mod prec {
    pub(crate) const OR: u8 = 1;
    pub(crate) const AND: u8 = 3;
    pub(crate) const NOT: u8 = 5;
    pub(crate) const COMPARISON: u8 = 7;
    pub(crate) const ADDITIVE: u8 = 9;
    pub(crate) const MULTIPLICATIVE: u8 = 11;
    pub(crate) const UNARY: u8 = 13;
    pub(crate) const CAST: u8 = 15;
    pub(crate) const PRIMARY: u8 = 17;
}

impl BinaryOp {
    /// The operator as written in ES|QL.
    pub fn symbol(self) -> &'static str {
        match self {
            BinaryOp::Or => "OR",
            BinaryOp::And => "AND",
            BinaryOp::Eq => "==",
            BinaryOp::NotEq => "!=",
            BinaryOp::Lt => "<",
            BinaryOp::LtEq => "<=",
            BinaryOp::Gt => ">",
            BinaryOp::GtEq => ">=",
            BinaryOp::Match => ":",
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            BinaryOp::Mod => "%",
        }
    }

    pub(crate) fn precedence(self) -> u8 {
        match self {
            BinaryOp::Or => prec::OR,
            BinaryOp::And => prec::AND,
            BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
            | BinaryOp::Match => prec::COMPARISON,
            BinaryOp::Add | BinaryOp::Sub => prec::ADDITIVE,
            BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => prec::MULTIPLICATIVE,
        }
    }

    pub(crate) fn is_comparison(self) -> bool {
        self.precedence() == prec::COMPARISON
    }
}

/// `x [NOT] IN (list)`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InList {
    /// The tested value.
    pub expr: Box<Expr>,
    /// At least one candidate.
    pub list: Vec<Expr>,
    /// `NOT IN`
    pub negated: bool,
    /// From the tested value to the closing parenthesis.
    pub span: Span,
}

/// `x [NOT] LIKE "pattern"` or `x [NOT] RLIKE "pattern"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Like {
    /// The tested value.
    pub expr: Box<Expr>,
    /// `LIKE` (wildcards) or `RLIKE` (regular expression).
    pub kind: LikeKind,
    /// The decoded pattern string.
    pub pattern: String,
    /// `NOT LIKE` / `NOT RLIKE`
    pub negated: bool,
    /// From the tested value to the pattern.
    pub span: Span,
}

/// Which pattern language a [`Like`] uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LikeKind {
    /// `LIKE`
    Like,
    /// `RLIKE`
    RLike,
}

/// `x IS [NOT] NULL`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IsNull {
    /// The tested value.
    pub expr: Box<Expr>,
    /// `IS NOT NULL`
    pub negated: bool,
    /// From the tested value to `NULL`.
    pub span: Span,
}

/// `x::type`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cast {
    /// The value.
    pub expr: Box<Expr>,
    /// The target type name as written.
    pub type_name: String,
    /// From the value to the type name.
    pub span: Span,
}

/// `[a, b, ...]`: a non-empty list of constants of one kind (numbers,
/// booleans or strings).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrayLiteral {
    /// The elements; a numeric element may carry a sign.
    pub elements: Vec<Expr>,
    /// From `[` to `]`.
    pub span: Span,
}

impl Expr {
    /// The text the expression was parsed from.
    pub fn span(&self) -> Span {
        match self {
            Expr::Literal(e) => e.span,
            Expr::Interval(e) => e.span,
            Expr::Field(e) => e.span,
            Expr::Param(e) => e.span,
            Expr::Call(e) => e.span,
            Expr::Unary(e) => e.span,
            Expr::Binary(e) => e.span,
            Expr::In(e) => e.span,
            Expr::Like(e) => e.span,
            Expr::IsNull(e) => e.span,
            Expr::Cast(e) => e.span,
            Expr::Array(e) => e.span,
            Expr::InSubquery(e) => e.span,
            Expr::Map(e) => e.span,
        }
    }

    pub(crate) fn span_mut(&mut self) -> &mut Span {
        match self {
            Expr::Literal(e) => &mut e.span,
            Expr::Interval(e) => &mut e.span,
            Expr::Field(e) => &mut e.span,
            Expr::Param(e) => &mut e.span,
            Expr::Call(e) => &mut e.span,
            Expr::Unary(e) => &mut e.span,
            Expr::Binary(e) => &mut e.span,
            Expr::In(e) => &mut e.span,
            Expr::Like(e) => &mut e.span,
            Expr::IsNull(e) => &mut e.span,
            Expr::Cast(e) => &mut e.span,
            Expr::Array(e) => &mut e.span,
            Expr::InSubquery(e) => &mut e.span,
            Expr::Map(e) => &mut e.span,
        }
    }

    /// The binding power of the expression's outermost operator.
    pub(crate) fn precedence(&self) -> u8 {
        match self {
            Expr::Binary(b) => b.op.precedence(),
            Expr::Unary(u) => match u.op {
                UnaryOp::Not => prec::NOT,
                UnaryOp::Minus | UnaryOp::Plus => prec::UNARY,
            },
            Expr::In(_) | Expr::InSubquery(_) | Expr::Like(_) | Expr::IsNull(_) => prec::COMPARISON,
            Expr::Cast(_) => prec::CAST,
            Expr::Literal(_)
            | Expr::Interval(_)
            | Expr::Field(_)
            | Expr::Param(_)
            | Expr::Call(_)
            | Expr::Array(_)
            | Expr::Map(_) => prec::PRIMARY,
        }
    }

    /// Structural equality that ignores every span. Iterative, so it does not
    /// recurse on the tree's depth.
    pub fn eq_ignoring_spans(&self, other: &Expr) -> bool {
        eq::exprs_equal(self, other)
    }
}

impl Query {
    /// Structural equality that ignores every span: the equality the printer
    /// round trip preserves. Iterative over expressions and nested queries.
    pub fn eq_ignoring_spans(&self, other: &Query) -> bool {
        eq::queries_equal(self, other)
    }
}
