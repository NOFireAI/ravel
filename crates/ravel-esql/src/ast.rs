//! Syntax tree for the core ES|QL pipeline. Every node carries the [`Span`]
//! of the text it was parsed from; a parenthesised expression's span covers
//! its parentheses.

mod print;

pub use print::print;

use crate::span::Span;

/// A query: a source command followed by processing commands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    /// `FROM` or `ROW`.
    pub source: Command,
    /// The commands after each `|`, in order.
    pub processing: Vec<Command>,
    /// From the source command's keyword to the end of the last command.
    pub span: Span,
}

/// One pipeline stage.
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
}

impl Command {
    /// From the command keyword to the end of the command's last token.
    pub fn span(&self) -> Span {
        match self {
            Command::Row(c) => c.span,
            Command::From(c) => c.span,
            Command::Where(c) => c.span,
            Command::Eval(c) => c.span,
            Command::Keep(c) => c.span,
            Command::Drop(c) => c.span,
            Command::Rename(c) => c.span,
            Command::Stats(c) => c.span,
            Command::Sort(c) => c.span,
            Command::Limit(c) => c.span,
        }
    }
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
    /// From the first part to the last.
    pub span: Span,
}

/// One part of a [`QualifiedName`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamePart {
    /// The part as written when unquoted (spelling kept), or the decoded
    /// identifier when backtick-quoted.
    pub text: String,
    /// Whether the part was backtick-quoted.
    pub quoted: bool,
    /// The part, backticks included.
    pub span: Span,
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
}

/// A literal value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Literal {
    /// The value.
    pub value: LiteralValue,
    /// The literal as written.
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
            Expr::In(_) | Expr::Like(_) | Expr::IsNull(_) => prec::COMPARISON,
            Expr::Cast(_) => prec::CAST,
            Expr::Literal(_)
            | Expr::Interval(_)
            | Expr::Field(_)
            | Expr::Param(_)
            | Expr::Call(_)
            | Expr::Array(_) => prec::PRIMARY,
        }
    }

    /// Structural equality that ignores every span. Iterative, so it does not
    /// recurse on the tree's depth.
    pub fn eq_ignoring_spans(&self, other: &Expr) -> bool {
        exprs_eq(vec![(self, other)])
    }
}

impl Query {
    /// Structural equality that ignores every span: the equality the printer
    /// round trip preserves.
    pub fn eq_ignoring_spans(&self, other: &Query) -> bool {
        if self.processing.len() != other.processing.len() {
            return false;
        }
        let mut pending = Vec::new();
        command_eq(&self.source, &other.source, &mut pending)
            && self
                .processing
                .iter()
                .zip(&other.processing)
                .all(|(a, b)| command_eq(a, b, &mut pending))
            && exprs_eq(pending)
    }
}

type Pending<'a> = Vec<(&'a Expr, &'a Expr)>;

fn name_eq(a: &QualifiedName, b: &QualifiedName) -> bool {
    a.parts.len() == b.parts.len()
        && a.parts
            .iter()
            .zip(&b.parts)
            .all(|(x, y)| x.text == y.text && x.quoted == y.quoted)
}

fn names_eq(a: &[QualifiedName], b: &[QualifiedName]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| name_eq(x, y))
}

fn field_eq<'a>(a: &'a Field, b: &'a Field, pending: &mut Pending<'a>) -> bool {
    let names = match (&a.name, &b.name) {
        (None, None) => true,
        (Some(x), Some(y)) => name_eq(x, y),
        _ => false,
    };
    pending.push((&a.expr, &b.expr));
    names
}

fn fields_eq<'a>(a: &'a [Field], b: &'a [Field], pending: &mut Pending<'a>) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| field_eq(x, y, pending))
}

fn opt_expr_eq<'a>(a: &'a Option<Expr>, b: &'a Option<Expr>, pending: &mut Pending<'a>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            pending.push((x, y));
            true
        }
        _ => false,
    }
}

fn list_eq<'a>(a: &'a [Expr], b: &'a [Expr], pending: &mut Pending<'a>) -> bool {
    if a.len() != b.len() {
        return false;
    }
    pending.extend(a.iter().zip(b));
    true
}

fn command_eq<'a>(a: &'a Command, b: &'a Command, pending: &mut Pending<'a>) -> bool {
    match (a, b) {
        (Command::Row(x), Command::Row(y)) => fields_eq(&x.fields, &y.fields, pending),
        (Command::From(x), Command::From(y)) => {
            let sources = x.sources.len() == y.sources.len()
                && x.sources
                    .iter()
                    .zip(&y.sources)
                    .all(|(s, t)| s.text == t.text && s.quoted == t.quoted);
            let metadata = match (&x.metadata, &y.metadata) {
                (None, None) => true,
                (Some(m), Some(n)) => names_eq(&m.fields, &n.fields),
                _ => false,
            };
            sources && metadata
        }
        (Command::Where(x), Command::Where(y)) => {
            pending.push((&x.condition, &y.condition));
            true
        }
        (Command::Eval(x), Command::Eval(y)) => fields_eq(&x.fields, &y.fields, pending),
        (Command::Keep(x), Command::Keep(y)) => names_eq(&x.patterns, &y.patterns),
        (Command::Drop(x), Command::Drop(y)) => names_eq(&x.patterns, &y.patterns),
        (Command::Rename(x), Command::Rename(y)) => {
            x.renames.len() == y.renames.len()
                && x.renames
                    .iter()
                    .zip(&y.renames)
                    .all(|(r, s)| name_eq(&r.old, &s.old) && name_eq(&r.new, &s.new))
        }
        (Command::Stats(x), Command::Stats(y)) => {
            x.aggregates.len() == y.aggregates.len()
                && x.aggregates.iter().zip(&y.aggregates).all(|(p, q)| {
                    field_eq(&p.field, &q.field, pending)
                        && opt_expr_eq(&p.filter, &q.filter, pending)
                })
                && fields_eq(&x.groupings, &y.groupings, pending)
        }
        (Command::Sort(x), Command::Sort(y)) => {
            x.keys.len() == y.keys.len()
                && x.keys.iter().zip(&y.keys).all(|(p, q)| {
                    pending.push((&p.expr, &q.expr));
                    p.order == q.order && p.nulls == q.nulls
                })
        }
        (Command::Limit(x), Command::Limit(y)) => x.count == y.count,
        _ => false,
    }
}

fn exprs_eq(mut pending: Pending<'_>) -> bool {
    while let Some((a, b)) = pending.pop() {
        let same = match (a, b) {
            (Expr::Literal(x), Expr::Literal(y)) => x.value == y.value,
            (Expr::Interval(x), Expr::Interval(y)) => x.value == y.value && x.unit == y.unit,
            (Expr::Field(x), Expr::Field(y)) => name_eq(x, y),
            (Expr::Param(x), Expr::Param(y)) => x.name == y.name && x.identifier == y.identifier,
            (Expr::Call(x), Expr::Call(y)) => {
                x.name == y.name
                    && match (&x.args, &y.args) {
                        (CallArgs::Star, CallArgs::Star) => true,
                        (CallArgs::List(p), CallArgs::List(q)) => list_eq(p, q, &mut pending),
                        _ => false,
                    }
            }
            (Expr::Unary(x), Expr::Unary(y)) => {
                pending.push((&x.operand, &y.operand));
                x.op == y.op
            }
            (Expr::Binary(x), Expr::Binary(y)) => {
                pending.push((&x.left, &y.left));
                pending.push((&x.right, &y.right));
                x.op == y.op
            }
            (Expr::In(x), Expr::In(y)) => {
                pending.push((&x.expr, &y.expr));
                x.negated == y.negated && list_eq(&x.list, &y.list, &mut pending)
            }
            (Expr::Like(x), Expr::Like(y)) => {
                pending.push((&x.expr, &y.expr));
                x.kind == y.kind && x.pattern == y.pattern && x.negated == y.negated
            }
            (Expr::IsNull(x), Expr::IsNull(y)) => {
                pending.push((&x.expr, &y.expr));
                x.negated == y.negated
            }
            (Expr::Cast(x), Expr::Cast(y)) => {
                pending.push((&x.expr, &y.expr));
                x.type_name == y.type_name
            }
            (Expr::Array(x), Expr::Array(y)) => list_eq(&x.elements, &y.elements, &mut pending),
            _ => false,
        };
        if !same {
            return false;
        }
    }
    true
}
