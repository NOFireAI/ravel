//! The ES|QL release manifest (ADR-2887 D1, D2): one record per released
//! language form, with the state of each of Ravel's layers for it, and the
//! conformance report generated from it.
//!
//! The manifest is `manifest/esql-9.5.toml` in this crate. [`load`] reads it
//! and validates it: every name is unique within its kind, every alias
//! resolves to exactly one entry that lists it back, every state is
//! `supported`, `refused` or `absent`, no layer claims more than the one
//! before it, and no expected answer names a column twice.
//! [`Manifest::render_report`] produces `docs/esql-conformance.md`.
//!
//! Nothing at runtime reads the manifest yet. Its only readers are the tests
//! that prove each entry's states against the parser, the binder and the
//! executor, and the test that keeps the generated report current.
//!
//! The file is read by a small reader for the subset of TOML it uses: `[table]`
//! and `[[array-of-tables]]` headers, `key = value` lines whose value is a
//! basic or literal string, a boolean, or an array of strings that may span
//! lines, and `#` comments. Anything else, an unknown table or key, a repeated key and a
//! missing required key are errors, so a typo cannot drop a field silently.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write as _};

/// What kind of language form an entry records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    /// A command that starts a statement, such as `FROM`.
    SourceCommand,
    /// A command after a pipe, such as `WHERE`.
    ProcessingCommand,
    /// A statement prefix, such as `SET`.
    Directive,
    /// A variant of a command's or literal's syntax.
    Syntax,
    /// A function.
    Function,
    /// Another name of a function or command.
    Alias,
    /// An operator.
    Operator,
    /// A data type.
    Type,
    /// A query parameter form.
    Parameter,
}

impl Kind {
    /// Every kind, in report order.
    pub const ALL: [Kind; 9] = [
        Kind::SourceCommand,
        Kind::ProcessingCommand,
        Kind::Directive,
        Kind::Syntax,
        Kind::Function,
        Kind::Alias,
        Kind::Operator,
        Kind::Type,
        Kind::Parameter,
    ];

    /// The name the manifest spells this kind with.
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::SourceCommand => "source_command",
            Kind::ProcessingCommand => "processing_command",
            Kind::Directive => "directive",
            Kind::Syntax => "syntax",
            Kind::Function => "function",
            Kind::Alias => "alias",
            Kind::Operator => "operator",
            Kind::Type => "type",
            Kind::Parameter => "parameter",
        }
    }

    fn parse(text: &str) -> Option<Kind> {
        Kind::ALL.into_iter().find(|kind| kind.as_str() == text)
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The family of a function entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Family {
    /// Conditional and other scalar functions.
    Scalar,
    /// Aggregate functions.
    Aggregate,
    /// Grouping functions.
    Grouping,
    /// Type conversion functions.
    Conversion,
    /// Multivalue functions.
    Multivalue,
    /// Date and time functions.
    Date,
    /// String functions.
    String,
    /// Mathematical functions.
    Math,
    /// IP functions.
    Ip,
    /// Spatial functions.
    Spatial,
    /// Full-text search functions.
    Search,
    /// Time series aggregate functions.
    TimeSeries,
}

impl Family {
    /// Every family, in report order.
    pub const ALL: [Family; 12] = [
        Family::Scalar,
        Family::Aggregate,
        Family::Grouping,
        Family::Conversion,
        Family::Multivalue,
        Family::Date,
        Family::String,
        Family::Math,
        Family::Ip,
        Family::Spatial,
        Family::Search,
        Family::TimeSeries,
    ];

    /// The name the manifest spells this family with.
    pub fn as_str(self) -> &'static str {
        match self {
            Family::Scalar => "scalar",
            Family::Aggregate => "aggregate",
            Family::Grouping => "grouping",
            Family::Conversion => "conversion",
            Family::Multivalue => "multivalue",
            Family::Date => "date",
            Family::String => "string",
            Family::Math => "math",
            Family::Ip => "ip",
            Family::Spatial => "spatial",
            Family::Search => "search",
            Family::TimeSeries => "time_series",
        }
    }

    fn parse(text: &str) -> Option<Family> {
        Family::ALL
            .into_iter()
            .find(|family| family.as_str() == text)
    }
}

impl fmt::Display for Family {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The state of one layer for one entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum State {
    /// The layer accepts the entry's example.
    Supported,
    /// The layer recognises the form and refuses it with a typed error.
    Refused,
    /// The layer does not know the form.
    Absent,
}

impl State {
    /// The name the manifest spells this state with.
    pub fn as_str(self) -> &'static str {
        match self {
            State::Supported => "supported",
            State::Refused => "refused",
            State::Absent => "absent",
        }
    }

    fn parse(text: &str) -> Option<State> {
        [State::Supported, State::Refused, State::Absent]
            .into_iter()
            .find(|state| state.as_str() == text)
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How the parser accepts an entry's example.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Grammar {
    /// A rule of the parser's own for the form: a command, clause,
    /// operator, literal or parameter.
    Own,
    /// Only the generic `name(args)` rule, which takes any identifier as a
    /// function name.
    Call,
    /// Only the generic `value::type` rule, which takes any identifier as a
    /// type name.
    Cast,
    /// Only a field reference: the type has no syntax, and the parser reads
    /// the field's name without knowing its type.
    Field,
}

impl Grammar {
    /// The name the manifest spells this grammar with.
    pub fn as_str(self) -> &'static str {
        match self {
            Grammar::Own => "own",
            Grammar::Call => "call",
            Grammar::Cast => "cast",
            Grammar::Field => "field",
        }
    }

    fn parse(text: &str) -> Option<Grammar> {
        [Grammar::Own, Grammar::Call, Grammar::Cast, Grammar::Field]
            .into_iter()
            .find(|grammar| grammar.as_str() == text)
    }
}

impl fmt::Display for Grammar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One value of an answer.
#[derive(Debug, Clone)]
pub enum Cell {
    /// A null.
    Null,
    /// A boolean.
    Bool(bool),
    /// A whole number, written without a fraction or an exponent.
    Long(i64),
    /// A number written with a fraction or an exponent. Compared by its bit
    /// pattern.
    Double(f64),
    /// A string: a keyword, or a date rendered as the release renders it.
    Text(String),
}

impl PartialEq for Cell {
    fn eq(&self, other: &Cell) -> bool {
        match (self, other) {
            (Cell::Null, Cell::Null) => true,
            (Cell::Bool(a), Cell::Bool(b)) => a == b,
            (Cell::Long(a), Cell::Long(b)) => a == b,
            (Cell::Double(a), Cell::Double(b)) => a.to_bits() == b.to_bits(),
            (Cell::Text(a), Cell::Text(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Cell {}

/// A result column: its name and its ES|QL type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// The column's name.
    pub name: String,
    /// The ES|QL type name, such as `long` or `date_nanos`.
    pub ty: String,
}

/// The answer to a statement that runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    /// The result columns, in order.
    pub columns: Vec<Column>,
    /// The rows, each with one cell per column.
    pub rows: Vec<Vec<Cell>>,
    /// The response's warnings.
    pub warnings: Vec<String>,
}

/// What an answer's row order is compared on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Order {
    /// The example fixes the whole order: rows compare in sequence.
    Exact,
    /// The example fixes no order: rows compare as a multiset.
    Multiset,
    /// The example sorts on these columns and leaves ties unordered: the
    /// sequence of their values compares exactly and the rows as a multiset.
    Key(Vec<String>),
    /// The example takes this many rows without fixing which: the answer
    /// holds that many, each one of the listed rows.
    Any(usize),
}

impl Order {
    fn parse(text: &str) -> Option<Order> {
        match text {
            "exact" => Some(Order::Exact),
            "multiset" => Some(Order::Multiset),
            _ => {
                if let Some(columns) = text.strip_prefix("key ") {
                    let columns: Vec<String> =
                        columns.split(',').map(|c| c.trim().to_owned()).collect();
                    if columns.iter().any(String::is_empty) {
                        return None;
                    }
                    Some(Order::Key(columns))
                } else {
                    text.strip_prefix("any ")
                        .and_then(|n| n.parse().ok())
                        .map(Order::Any)
                }
            }
        }
    }
}

impl fmt::Display for Order {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Order::Exact => f.write_str("exact"),
            Order::Multiset => f.write_str("multiset"),
            Order::Key(columns) => write!(f, "key {}", columns.join(", ")),
            Order::Any(n) => write!(f, "any {n}"),
        }
    }
}

/// What the release returns for an entry's example: an answer, with the
/// order its rows are compared on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expected {
    /// The answer.
    pub answer: Answer,
    /// How its rows compare.
    pub order: Order,
}

/// The divergences an entry can name: each has a normalisation that undoes
/// exactly what it changes in Ravel's answer.
pub const NORMALISED_DIVERGENCES: [&str; 3] = ["from-columns", "integer-literal", "now-type"];

impl Expected {
    /// Whether `actual` is the release's answer: the same columns, names and
    /// types in order, the same warnings, and the same rows under the
    /// expected order. The error names the first difference found.
    pub fn compare(&self, actual: &Answer) -> Result<(), String> {
        let Expected {
            answer: expected,
            order,
        } = self;
        if actual.columns != expected.columns {
            return Err(format!(
                "columns {} where the release has {}",
                show_columns(&actual.columns),
                show_columns(&expected.columns)
            ));
        }
        if actual.warnings != expected.warnings {
            return Err(format!(
                "warnings {:?} where the release has {:?}",
                actual.warnings, expected.warnings
            ));
        }
        let rows_differ = || {
            format!(
                "rows {:?} where the release has {:?} (order: {order})",
                actual.rows, expected.rows
            )
        };
        let holds = match order {
            Order::Exact => actual.rows == expected.rows,
            Order::Multiset => {
                actual.rows.len() == expected.rows.len()
                    && is_sub_multiset(&actual.rows, &expected.rows)
            }
            Order::Key(keys) => {
                let indexes: Vec<usize> = keys
                    .iter()
                    .filter_map(|key| expected.columns.iter().position(|c| &c.name == key))
                    .collect();
                let project = |rows: &[Vec<Cell>]| -> Vec<Vec<Cell>> {
                    rows.iter()
                        .map(|row| indexes.iter().map(|&i| row[i].clone()).collect())
                        .collect()
                };
                actual.rows.len() == expected.rows.len()
                    && project(&actual.rows) == project(&expected.rows)
                    && is_sub_multiset(&actual.rows, &expected.rows)
            }
            Order::Any(n) => {
                actual.rows.len() == *n && is_sub_multiset(&actual.rows, &expected.rows)
            }
        };
        if holds { Ok(()) } else { Err(rows_differ()) }
    }

    /// Whether `actual`, Ravel's answer for an entry that names divergence
    /// `id`, is the release's answer once exactly that divergence is undone.
    /// An answer that already matches fails, since the entry then names a
    /// divergence it does not show; so does one the normalisation does not
    /// apply to, and one that still differs afterwards, naming the first
    /// difference.
    // guarded-esql-parse-allow: takes a divergence id, not ES|QL source text
    pub fn compare_divergent(&self, id: &str, actual: &Answer) -> Result<(), String> {
        if self.compare(actual).is_ok() {
            return Err(format!(
                "Ravel gives the release's answer without undoing {id}, so the entry \
                 does not show it"
            ));
        }
        let normalised = undo_divergence(id, &self.answer, actual)
            .map_err(|message| format!("cannot undo {id}: {message}"))?;
        self.compare(&normalised)
            .map_err(|message| format!("after undoing {id}: {message}"))
    }
}

/// Ravel's answer with divergence `id` undone against the release's
/// `expected` columns. Only what the divergence changes moves: a type it
/// does not explain, a value, a row or a warning is left as Ravel gave it.
fn undo_divergence(id: &str, expected: &Answer, actual: &Answer) -> Result<Answer, String> {
    if let Some(row) = actual.rows.iter().find(|r| r.len() != actual.columns.len()) {
        return Err(format!(
            "a row of {} cells for {} columns",
            row.len(),
            actual.columns.len()
        ));
    }
    match id {
        "from-columns" => undo_from_columns(expected, actual),
        "integer-literal" => retype(expected, actual, "long", "integer", |cell| match cell {
            Cell::Long(value) if i32::try_from(*value).is_err() => {
                Err(format!("{value} does not fit an integer"))
            }
            Cell::Long(_) | Cell::Null => Ok(cell.clone()),
            other => Err(format!("{other:?} in a long column")),
        }),
        "now-type" => retype(expected, actual, "date_nanos", "date", |cell| match cell {
            Cell::Null => Ok(Cell::Null),
            Cell::Text(text) => millisecond_date(text).map(Cell::Text),
            other => Err(format!("{other:?} in a date_nanos column")),
        }),
        other => Err(format!("divergence {other:?} has no normalisation")),
    }
}

/// The columns `FROM logs` gives on the conformance fixture, in Ravel's
/// fixed order: the native columns, then the fixture's declared `dur` and
/// `region`. The from-columns divergence is this order where the release
/// sorts by name, and nothing else.
pub const FROM_COLUMNS: [&str; 10] = [
    "@timestamp",
    "observed_ts",
    "severity_num",
    "severity_text",
    "message",
    "trace_id",
    "span_id",
    "flags",
    "dur",
    "region",
];

/// Ravel's columns reordered by name into the release's order, each row's
/// cells moving with their column. The names must be the release's, every
/// one of them in [`FROM_COLUMNS`], the release's order must be sorted by
/// name, and Ravel's order must be that fixed order limited to the names
/// present, so no other reordering is undone.
fn undo_from_columns(expected: &Answer, actual: &Answer) -> Result<Answer, String> {
    if let Some(column) = expected
        .columns
        .iter()
        .chain(&actual.columns)
        .find(|c| !FROM_COLUMNS.contains(&c.name.as_str()))
    {
        return Err(format!(
            "column {:?} is not one FROM gives in a fixed order",
            column.name
        ));
    }
    let names = |columns: &[Column]| -> Vec<String> {
        let mut names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        names.sort();
        names
    };
    if names(&actual.columns) != names(&expected.columns) {
        return Err(format!(
            "columns {} are not the release's {} in another order",
            show_columns(&actual.columns),
            show_columns(&expected.columns)
        ));
    }
    let release: Vec<String> = expected.columns.iter().map(|c| c.name.clone()).collect();
    if release != names(&expected.columns) {
        return Err(format!(
            "the release's order [{}] is not sorted by name",
            release.join(", ")
        ));
    }
    let fixed: Vec<&str> = FROM_COLUMNS
        .into_iter()
        .filter(|name| actual.columns.iter().any(|c| c.name == *name))
        .collect();
    let order: Vec<&str> = actual.columns.iter().map(|c| c.name.as_str()).collect();
    if order != fixed {
        return Err(format!(
            "Ravel's order [{}] is not FROM's fixed order [{}]",
            order.join(", "),
            fixed.join(", ")
        ));
    }
    let positions: Vec<usize> = expected
        .columns
        .iter()
        .filter_map(|e| actual.columns.iter().position(|a| a.name == e.name))
        .collect();
    if positions.iter().enumerate().all(|(i, &p)| i == p) {
        return Err("Ravel's columns are already in the release's order".to_owned());
    }
    Ok(Answer {
        columns: positions
            .iter()
            .map(|&p| actual.columns[p].clone())
            .collect(),
        rows: actual
            .rows
            .iter()
            .map(|row| positions.iter().map(|&p| row[p].clone()).collect())
            .collect(),
        warnings: actual.warnings.clone(),
    })
}

/// Ravel's answer with each column of type `from`, where the release's
/// column at the same position and of the same name is of type `to`, given
/// type `to` and its cells passed through `cell`. At least one column must
/// change.
fn retype(
    expected: &Answer,
    actual: &Answer,
    from: &str,
    to: &str,
    cell: impl Fn(&Cell) -> Result<Cell, String>,
) -> Result<Answer, String> {
    let retyped: Vec<usize> = actual
        .columns
        .iter()
        .zip(&expected.columns)
        .enumerate()
        .filter(|(_, (a, e))| a.name == e.name && a.ty == from && e.ty == to)
        .map(|(i, _)| i)
        .collect();
    if retyped.is_empty() {
        return Err(format!(
            "no {from} column where the release has {to}: columns {} against {}",
            show_columns(&actual.columns),
            show_columns(&expected.columns)
        ));
    }
    let mut columns = actual.columns.clone();
    for &i in &retyped {
        columns[i].ty = to.to_owned();
    }
    let rows = actual
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(i, c)| {
                    if retyped.contains(&i) {
                        cell(c)
                    } else {
                        Ok(c.clone())
                    }
                })
                .collect::<Result<Vec<Cell>, String>>()
        })
        .collect::<Result<Vec<Vec<Cell>>, String>>()?;
    Ok(Answer {
        columns,
        rows,
        warnings: actual.warnings.clone(),
    })
}

/// A nanosecond date rendering (`…:SS.fffffffffZ`) as the release renders a
/// date: three fraction digits. The six dropped digits must be zeros.
fn millisecond_date(text: &str) -> Result<String, String> {
    let shape = || format!("{text:?} is not a date rendered with nine fraction digits");
    let body = text.strip_suffix('Z').ok_or_else(shape)?;
    let (head, fraction) = body.rsplit_once('.').ok_or_else(shape)?;
    if fraction.len() != 9 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return Err(shape());
    }
    let (millis, rest) = fraction.split_at(3);
    if rest != "000000" {
        return Err(format!("{text:?} has precision below a millisecond"));
    }
    Ok(format!("{head}.{millis}Z"))
}

/// Whether every row of `part` can be matched to a distinct row of `whole`.
fn is_sub_multiset(part: &[Vec<Cell>], whole: &[Vec<Cell>]) -> bool {
    let mut left: Vec<&Vec<Cell>> = whole.iter().collect();
    part.iter()
        .all(|row| match left.iter().position(|r| *r == row) {
            Some(index) => {
                left.swap_remove(index);
                true
            }
            None => false,
        })
}

fn show_columns(columns: &[Column]) -> String {
    let shown: Vec<String> = columns
        .iter()
        .map(|c| format!("{}:{}", c.name, c.ty))
        .collect();
    format!("[{}]", shown.join(", "))
}

/// Which part of the manifest an entry is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Section {
    /// Released in the target line; counted in every denominator.
    Released,
    /// Present only in development builds; excluded from every denominator.
    Development,
}

/// One language form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Released or development-only.
    pub section: Section,
    /// The kind of form.
    pub kind: Kind,
    /// The form's name, unique within its kind.
    pub name: String,
    /// The family, for a function.
    pub family: Option<Family>,
    /// The alias entries that resolve to this one.
    pub aliases: Vec<String>,
    /// The entry an alias resolves to.
    pub alias_of: Option<String>,
    /// Whether the release marks the form technical preview.
    pub preview: bool,
    /// One statement that uses the form.
    pub example: String,
    /// How the parser accepts the example.
    pub grammar: Grammar,
    /// What the release returns for the example, for a released entry
    /// Ravel runs.
    pub expected: Option<Expected>,
    /// The id of the divergence that makes Ravel's answer differ from
    /// `expected`.
    pub divergence: Option<String>,
    /// The text a binder refusal of the example names the form by, when it
    /// is not the entry's name in brackets.
    pub refusal: Option<String>,
    /// The released entry the binder refuses first, when the example cannot
    /// reach this form without it.
    pub refused_by: Option<String>,
    /// The parser's state.
    pub parser: State,
    /// The binder's state.
    pub binder: State,
    /// The executor's state.
    pub executor: State,
    /// A backend or data resource the form needs.
    pub needs: Option<String>,
    /// The test that proves the states, as `path::test`.
    pub evidence: String,
    /// Whether the form's release status is in doubt.
    pub unverified: bool,
    /// The line of the entry's table header in the manifest.
    pub line: usize,
}

/// An invalid statement every layer must refuse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Negative {
    /// What makes it invalid.
    pub name: String,
    /// The statement.
    pub example: String,
}

/// An option of the Elasticsearch query endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportOption {
    /// The option's name.
    pub name: String,
    /// What it does.
    pub description: String,
    /// Whether Ravel serves it.
    pub state: State,
}

impl Entry {
    /// The text a binder refusal must name: `refusal`, or the entry's name in
    /// brackets.
    pub fn refusal_marker(&self) -> String {
        self.refusal
            .clone()
            .unwrap_or_else(|| format!("[{}]", self.name))
    }
}

/// A valid statement Ravel answers differently from the release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Divergence {
    /// The id entries name it by.
    pub id: String,
    /// A short name.
    pub name: String,
    /// What differs.
    pub description: String,
    /// The test that pins Ravel's answer, as `path::test`.
    pub evidence: String,
    /// Why no released entry's example can show the divergence, when none
    /// names it.
    pub no_entry: Option<String>,
}

/// A figure of the report: how many of `denominator` items count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Figure {
    /// Items that count.
    pub numerator: usize,
    /// Items scored.
    pub denominator: usize,
}

impl fmt::Display for Figure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.numerator, self.denominator)
    }
}

/// A loaded and validated manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// The release line the manifest records, such as `9.5`.
    pub release_line: String,
    /// The release of that line the expected answers describe, such as
    /// `9.5.5`.
    pub release_binary: String,
    /// Whether the manifest has been reconciled with the released binary.
    pub reconciled: bool,
    /// Released and development-only entries, in file order.
    pub entries: Vec<Entry>,
    /// Invalid statements.
    pub negatives: Vec<Negative>,
    /// Query endpoint options.
    pub transport: Vec<TransportOption>,
    /// Divergences from the release.
    pub divergences: Vec<Divergence>,
}

/// Why a manifest was refused. `line` is 1-based.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ManifestError {
    /// The text is not in the TOML subset the reader accepts.
    #[error("line {line}: {message}")]
    Syntax {
        /// Where.
        line: usize,
        /// What.
        message: String,
    },
    /// A table, key or value the manifest does not allow, or a missing key.
    #[error("line {line}: {message}")]
    Field {
        /// Where.
        line: usize,
        /// What.
        message: String,
    },
    /// A state other than `supported`, `refused` or `absent`.
    #[error("line {line}: {key} = {value:?} is not supported, refused or absent")]
    InvalidState {
        /// Where.
        line: usize,
        /// Which key.
        key: String,
        /// The value found.
        value: String,
    },
    /// Two entries of one kind, or two negatives, transport options or
    /// divergences, share a name.
    #[error("line {line}: duplicate {kind} {name:?}")]
    DuplicateName {
        /// Where the second one is.
        line: usize,
        /// The kind, or the table name.
        kind: String,
        /// The name.
        name: String,
    },
    /// An alias that does not resolve to exactly one entry listing it back.
    #[error("line {line}: alias {name:?} does not resolve: {message}")]
    UnresolvedAlias {
        /// Where.
        line: usize,
        /// The alias.
        name: String,
        /// Why.
        message: String,
    },
    /// States that contradict each other.
    #[error("line {line}: {name:?}: {message}")]
    InconsistentStates {
        /// Where.
        line: usize,
        /// The entry.
        name: String,
        /// Which rule.
        message: String,
    },
}

/// Read and validate a manifest from its text.
// guarded-esql-parse-allow: reads the release manifest's TOML, not ES|QL source text
pub fn load(text: &str) -> Result<Manifest, ManifestError> {
    let tables = read_tables(text)?;
    let mut release: Option<(String, String, bool)> = None;
    let mut entries = Vec::new();
    let mut negatives = Vec::new();
    let mut transport = Vec::new();
    let mut divergences = Vec::new();
    for mut table in tables {
        match (table.name.as_str(), table.array) {
            ("release", false) => {
                if release.is_some() {
                    return Err(field(table.line, "[release] appears twice"));
                }
                let line = table.take_string("line")?;
                let binary = table.take_string("binary")?;
                let reconciled = table.take_bool("reconciled")?;
                table.finish()?;
                release = Some((line, binary, reconciled));
            }
            ("entry", true) => entries.push(read_entry(table, Section::Released)?),
            ("development", true) => entries.push(read_entry(table, Section::Development)?),
            ("negative", true) => {
                let negative = Negative {
                    name: table.take_string("name")?,
                    example: table.take_string("example")?,
                };
                table.finish()?;
                negatives.push((table.line, negative));
            }
            ("transport", true) => {
                let name = table.take_string("name")?;
                let description = table.take_string("description")?;
                let state = table.take_state("state")?;
                table.finish()?;
                transport.push((
                    table.line,
                    TransportOption {
                        name,
                        description,
                        state,
                    },
                ));
            }
            ("divergence", true) => {
                let divergence = Divergence {
                    id: table.take_string("id")?,
                    name: table.take_string("name")?,
                    description: table.take_string("description")?,
                    evidence: table.take_string("evidence")?,
                    no_entry: table.take_optional_string("no_entry")?,
                };
                check_evidence(table.line, &divergence.evidence)?;
                table.finish()?;
                divergences.push((table.line, divergence));
            }
            (name, array) => {
                let shown = if array {
                    format!("[[{name}]]")
                } else {
                    format!("[{name}]")
                };
                return Err(field(table.line, &format!("unknown table {shown}")));
            }
        }
    }
    let Some((release_line, release_binary, reconciled)) = release else {
        return Err(field(1, "missing [release]"));
    };
    if release_line.is_empty() || !release_binary.starts_with(&format!("{release_line}.")) {
        return Err(field(
            1,
            "[release] line is empty or binary is not a release of that line",
        ));
    }
    validate_entries(&entries)?;
    unique_names("negative", negatives.iter().map(|(l, n)| (*l, &n.name)))?;
    unique_names("transport", transport.iter().map(|(l, t)| (*l, &t.name)))?;
    unique_names("divergence", divergences.iter().map(|(l, d)| (*l, &d.name)))?;
    unique_names(
        "divergence id",
        divergences.iter().map(|(l, d)| (*l, &d.id)),
    )?;
    for entry in &entries {
        if let Some(id) = &entry.divergence
            && !divergences.iter().any(|(_, d)| &d.id == id)
        {
            return Err(field(
                entry.line,
                &format!("{:?}: no divergence has id {id:?}", entry.name),
            ));
        }
        if let Some(id) = &entry.divergence
            && !NORMALISED_DIVERGENCES.contains(&id.as_str())
        {
            return Err(field(
                entry.line,
                &format!(
                    "{:?}: divergence {id:?} has no normalisation, so the entry's answer \
                     could not be compared",
                    entry.name
                ),
            ));
        }
        if entry.divergence.as_deref() == Some("from-columns")
            && let Some(column) = entry
                .expected
                .iter()
                .flat_map(|e| &e.answer.columns)
                .find(|c| !FROM_COLUMNS.contains(&c.name.as_str()))
        {
            return Err(field(
                entry.line,
                &format!(
                    "{:?}: from-columns undoes FROM's fixed column order only, and column \
                     {:?} is not one FROM gives",
                    entry.name, column.name
                ),
            ));
        }
    }
    for (line, divergence) in &divergences {
        let named = entries
            .iter()
            .any(|e| e.divergence.as_deref() == Some(divergence.id.as_str()));
        if named == divergence.no_entry.is_some() {
            return Err(field(
                *line,
                &format!(
                    "divergence {:?} must be named by a released entry or carry no_entry, \
                     not both",
                    divergence.id
                ),
            ));
        }
    }
    Ok(Manifest {
        release_line,
        release_binary,
        reconciled,
        entries,
        negatives: negatives.into_iter().map(|(_, n)| n).collect(),
        transport: transport.into_iter().map(|(_, t)| t).collect(),
        divergences: divergences.into_iter().map(|(_, d)| d).collect(),
    })
}

fn read_entry(mut table: RawTable, section: Section) -> Result<Entry, ManifestError> {
    let line = table.line;
    let kind_text = table.take_string("kind")?;
    let kind = Kind::parse(&kind_text)
        .ok_or_else(|| field(line, &format!("unknown kind {kind_text:?}")))?;
    let name = table.take_string("name")?;
    let family = match table.take_optional_string("family")? {
        Some(text) => Some(
            Family::parse(&text).ok_or_else(|| field(line, &format!("unknown family {text:?}")))?,
        ),
        None => None,
    };
    let aliases = table.take_optional_strings("aliases")?.unwrap_or_default();
    let alias_of = table.take_optional_string("alias_of")?;
    let preview = table.take_bool("preview")?;
    let example = table.take_string("example")?;
    let grammar_text = table.take_string("grammar")?;
    let grammar = Grammar::parse(&grammar_text)
        .ok_or_else(|| field(line, &format!("unknown grammar {grammar_text:?}")))?;
    let parser = table.take_state("parser")?;
    let binder = table.take_state("binder")?;
    let executor = table.take_state("executor")?;
    let expected = read_expected(&mut table, &name)?;
    let divergence = table.take_optional_string("divergence")?;
    let refusal = table.take_optional_string("refusal")?;
    let refused_by = table.take_optional_string("refused_by")?;
    let needs = table.take_optional_string("needs")?;
    let evidence = table.take_string("evidence")?;
    let unverified = table.take_optional_bool("unverified")?.unwrap_or(false);
    table.finish()?;

    if name.is_empty() {
        return Err(field(line, "name is empty"));
    }
    if example.is_empty() {
        return Err(field(line, &format!("{name:?}: example is empty")));
    }
    check_evidence(line, &evidence)?;
    match (kind, family) {
        (Kind::Function, None) => {
            return Err(field(line, &format!("function {name:?} has no family")));
        }
        (Kind::Function, Some(_)) | (_, None) => {}
        (_, Some(_)) => {
            return Err(field(
                line,
                &format!("{kind} {name:?} cannot have a family"),
            ));
        }
    }
    match (kind, &alias_of) {
        (Kind::Alias, None) => {
            return Err(field(line, &format!("alias {name:?} has no alias_of")));
        }
        (Kind::Alias, Some(_)) | (_, None) => {}
        (_, Some(_)) => {
            return Err(field(
                line,
                &format!("{kind} {name:?} cannot have alias_of"),
            ));
        }
    }
    if kind == Kind::Alias && !aliases.is_empty() {
        return Err(field(line, &format!("alias {name:?} cannot have aliases")));
    }
    let inconsistent = |message: &str| ManifestError::InconsistentStates {
        line,
        name: name.clone(),
        message: message.to_owned(),
    };
    // The binder sees exactly the statements the parser accepts.
    if (parser == State::Supported) != (binder != State::Absent) {
        return Err(inconsistent(
            "binder must be absent exactly when the parser does not support the example",
        ));
    }
    if executor == State::Supported && binder != State::Supported {
        return Err(inconsistent(
            "executor cannot be supported unless the binder is",
        ));
    }
    if binder == State::Refused && executor != State::Refused {
        return Err(inconsistent(
            "a statement the binder refuses is refused by the executor",
        ));
    }
    if parser == State::Refused && executor != State::Refused {
        return Err(inconsistent(
            "a statement the parser refuses is refused by the executor",
        ));
    }
    if unverified && section == Section::Released {
        return Err(field(
            line,
            &format!("{name:?}: unverified entries belong to [[development]]"),
        ));
    }
    let runs = section == Section::Released && executor == State::Supported;
    if runs != expected.is_some() {
        return Err(field(
            line,
            &format!(
                "{name:?}: a released, executor-supported entry, and no other, records the \
                 release's answer"
            ),
        ));
    }
    if divergence.is_some() && !runs {
        return Err(field(
            line,
            &format!("{name:?}: only an entry Ravel runs can name a divergence"),
        ));
    }
    let binder_refused = parser == State::Supported && binder == State::Refused;
    if (refusal.is_some() || refused_by.is_some()) && !binder_refused {
        return Err(field(
            line,
            &format!("{name:?}: refusal and refused_by belong to binder-refused entries"),
        ));
    }
    if refusal.is_some() && refused_by.is_some() {
        return Err(field(
            line,
            &format!("{name:?}: an entry refused through another names that entry's refusal"),
        ));
    }
    Ok(Entry {
        section,
        kind,
        name,
        family,
        aliases,
        alias_of,
        preview,
        example,
        grammar,
        expected,
        divergence,
        refusal,
        refused_by,
        parser,
        binder,
        executor,
        needs,
        evidence,
        unverified,
        line,
    })
}

/// Read an entry's expected answer from its `columns`, `rows`, `order` and
/// `warnings` keys.
fn read_expected(table: &mut RawTable, name: &str) -> Result<Option<Expected>, ManifestError> {
    let line = table.line;
    let columns = table.take_optional_strings("columns")?;
    let rows = table.take_optional_strings("rows")?;
    let order = table.take_optional_string("order")?;
    let warnings = table.take_optional_strings("warnings")?;
    let wrong = |message: &str| field(line, &format!("{name:?}: {message}"));
    match (columns, rows, order, warnings) {
        (None, None, None, None) => Ok(None),
        (Some(columns), Some(rows), Some(order), Some(warnings)) => {
            let columns = columns
                .iter()
                .map(|text| match text.rsplit_once(':') {
                    Some((name, ty)) if !name.is_empty() && !ty.is_empty() => Ok(Column {
                        name: name.to_owned(),
                        ty: ty.to_owned(),
                    }),
                    _ => Err(wrong(&format!("column {text:?} is not name:type"))),
                })
                .collect::<Result<Vec<Column>, ManifestError>>()?;
            if columns.is_empty() {
                return Err(wrong("columns is empty"));
            }
            if let Some((i, column)) = columns
                .iter()
                .enumerate()
                .find(|(i, c)| columns[..*i].iter().any(|d| d.name == c.name))
            {
                return Err(wrong(&format!(
                    "column {:?} at position {} repeats an earlier column's name",
                    column.name,
                    i + 1
                )));
            }
            let rows = rows
                .iter()
                .map(|text| {
                    let row = read_row(text).map_err(|m| wrong(&format!("row {text:?}: {m}")))?;
                    if row.len() != columns.len() {
                        return Err(wrong(&format!(
                            "row {text:?} has {} cells for {} columns",
                            row.len(),
                            columns.len()
                        )));
                    }
                    Ok(row)
                })
                .collect::<Result<Vec<Vec<Cell>>, ManifestError>>()?;
            let order = Order::parse(&order).ok_or_else(|| {
                wrong(&format!(
                    "order {order:?} is not exact, multiset, key COLUMNS or any N"
                ))
            })?;
            match &order {
                Order::Key(keys) => {
                    if let Some(key) = keys.iter().find(|k| !columns.iter().any(|c| &c.name == *k))
                    {
                        return Err(wrong(&format!("order key {key:?} is not a column")));
                    }
                }
                Order::Any(n) if *n > rows.len() => {
                    return Err(wrong(&format!(
                        "order any {n} exceeds the {} rows listed",
                        rows.len()
                    )));
                }
                _ => {}
            }
            Ok(Some(Expected {
                answer: Answer {
                    columns,
                    rows,
                    warnings,
                },
                order,
            }))
        }
        _ => Err(wrong(
            "an answer needs columns, rows, order and warnings together",
        )),
    }
}

/// Read one row: a bracketed, comma-separated list of `null`, `true`,
/// `false`, numbers and double-quoted strings.
fn read_row(text: &str) -> Result<Vec<Cell>, String> {
    let Some(mut rest) = text.trim().strip_prefix('[') else {
        return Err("a row starts with [".to_owned());
    };
    let mut cells = Vec::new();
    loop {
        rest = rest.trim_start();
        if cells.is_empty()
            && let Some(after) = rest.strip_prefix(']')
        {
            return finish_row(cells, after);
        }
        let (cell, after) = read_cell(rest)?;
        cells.push(cell);
        rest = after.trim_start();
        if let Some(after) = rest.strip_prefix(',') {
            rest = after;
        } else if let Some(after) = rest.strip_prefix(']') {
            return finish_row(cells, after);
        } else {
            return Err("expected , or ] in a row".to_owned());
        }
    }
}

fn finish_row(cells: Vec<Cell>, after: &str) -> Result<Vec<Cell>, String> {
    if after.trim().is_empty() {
        Ok(cells)
    } else {
        Err("text after a row".to_owned())
    }
}

fn read_cell(text: &str) -> Result<(Cell, &str), String> {
    for (word, cell) in [
        ("null", Cell::Null),
        ("true", Cell::Bool(true)),
        ("false", Cell::Bool(false)),
    ] {
        if let Some(rest) = text.strip_prefix(word) {
            return Ok((cell, rest));
        }
    }
    if text.starts_with('"') {
        let (value, rest) = read_string(text)?;
        return Ok((Cell::Text(value), rest));
    }
    let end = text
        .find(|c: char| c == ',' || c == ']' || c.is_whitespace())
        .unwrap_or(text.len());
    let (number, rest) = text.split_at(end);
    let cell = if number.contains(['.', 'e', 'E']) {
        number.parse().map(Cell::Double).ok()
    } else {
        number.parse().map(Cell::Long).ok()
    };
    cell.map(|cell| (cell, rest))
        .ok_or_else(|| format!("{number:?} is not null, a boolean, a number or a string"))
}

fn check_evidence(line: usize, evidence: &str) -> Result<(), ManifestError> {
    match evidence.split_once("::") {
        Some((path, test)) if path.ends_with(".rs") && !test.is_empty() => Ok(()),
        _ => Err(field(
            line,
            &format!("evidence {evidence:?} is not path.rs::test"),
        )),
    }
}

fn validate_entries(entries: &[Entry]) -> Result<(), ManifestError> {
    let mut seen = BTreeSet::new();
    for entry in entries {
        if !seen.insert((entry.kind, entry.name.as_str())) {
            return Err(ManifestError::DuplicateName {
                line: entry.line,
                kind: entry.kind.to_string(),
                name: entry.name.clone(),
            });
        }
    }
    for entry in entries {
        if let Some(target) = &entry.alias_of {
            let unresolved = |message: String| ManifestError::UnresolvedAlias {
                line: entry.line,
                name: entry.name.clone(),
                message,
            };
            let targets: Vec<&Entry> = entries
                .iter()
                .filter(|e| e.kind != Kind::Alias && &e.name == target)
                .collect();
            let [resolved] = targets.as_slice() else {
                return Err(unresolved(format!(
                    "{} entries are named {target:?}",
                    targets.len()
                )));
            };
            if resolved.section != entry.section {
                return Err(unresolved(format!("{target:?} is in the other section")));
            }
            if !resolved.aliases.contains(&entry.name) {
                return Err(unresolved(format!(
                    "{target:?} does not list it in aliases"
                )));
            }
        }
        if let Some(target) = &entry.refused_by {
            let targets: Vec<&Entry> = entries
                .iter()
                .filter(|e| e.section == Section::Released && &e.name == target)
                .collect();
            let wrong =
                |message: String| field(entry.line, &format!("{:?}: {message}", entry.name));
            let [resolved] = targets.as_slice() else {
                return Err(wrong(format!(
                    "refused_by {target:?} names {} released entries, not one",
                    targets.len()
                )));
            };
            if resolved.binder != State::Refused || resolved.refused_by.is_some() {
                return Err(wrong(format!(
                    "refused_by {target:?} is not an entry the binder refuses itself"
                )));
            }
        }
        let mut listed = BTreeSet::new();
        for alias in &entry.aliases {
            if !listed.insert(alias.as_str()) {
                return Err(ManifestError::DuplicateName {
                    line: entry.line,
                    kind: "alias".to_owned(),
                    name: alias.clone(),
                });
            }
            let resolves = entries.iter().any(|e| {
                e.kind == Kind::Alias
                    && &e.name == alias
                    && e.alias_of.as_deref() == Some(entry.name.as_str())
            });
            if !resolves {
                return Err(ManifestError::UnresolvedAlias {
                    line: entry.line,
                    name: alias.clone(),
                    message: format!(
                        "no alias entry of that name has alias_of = {:?}",
                        entry.name
                    ),
                });
            }
        }
    }
    Ok(())
}

fn unique_names<'a>(
    kind: &str,
    names: impl Iterator<Item = (usize, &'a String)>,
) -> Result<(), ManifestError> {
    let mut seen = BTreeSet::new();
    for (line, name) in names {
        if !seen.insert(name.as_str()) {
            return Err(ManifestError::DuplicateName {
                line,
                kind: kind.to_owned(),
                name: name.clone(),
            });
        }
    }
    Ok(())
}

fn field(line: usize, message: &str) -> ManifestError {
    ManifestError::Field {
        line,
        message: message.to_owned(),
    }
}

impl Manifest {
    /// The entries counted in every denominator.
    pub fn released(&self) -> impl Iterator<Item = &Entry> {
        self.entries
            .iter()
            .filter(|entry| entry.section == Section::Released)
    }

    /// The development-only entries, excluded from every denominator.
    pub fn development(&self) -> impl Iterator<Item = &Entry> {
        self.entries
            .iter()
            .filter(|entry| entry.section == Section::Development)
    }

    /// Released entries the parser has a rule of its own for, and of those,
    /// the ones whose example it accepts.
    pub fn syntax_coverage(&self) -> Figure {
        count(
            self.released()
                .filter(|entry| entry.grammar == Grammar::Own),
            |entry| entry.parser == State::Supported,
        )
    }

    /// Released entries accepted only through the generic call or cast rule,
    /// or as a plain field, which do not check the entry's name, and of
    /// those, the ones whose example the parser accepts.
    pub fn generic_rule_coverage(&self) -> Figure {
        count(
            self.released()
                .filter(|entry| entry.grammar != Grammar::Own),
            |entry| entry.parser == State::Supported,
        )
    }

    /// Released entries whose example runs and gives the release's answer.
    /// A refused entry and a divergent one are each a gap here.
    pub fn execution_compatibility(&self) -> Figure {
        count(self.released(), |entry| {
            entry.executor == State::Supported && entry.divergence.is_none()
        })
    }

    /// Released entries whose example runs and gives an answer that differs
    /// from the release's by a named divergence.
    pub fn divergent(&self) -> Figure {
        count(self.released(), |entry| {
            entry.executor == State::Supported && entry.divergence.is_some()
        })
    }

    /// Released entries whose example runs, matching or not.
    pub fn ran(&self) -> Figure {
        count(self.released(), |entry| entry.executor == State::Supported)
    }

    /// Query endpoint options Ravel serves.
    pub fn transport_compatibility(&self) -> Figure {
        let served = self
            .transport
            .iter()
            .filter(|option| option.state == State::Supported)
            .count();
        Figure {
            numerator: served,
            denominator: self.transport.len(),
        }
    }

    /// The conformance report, `docs/esql-conformance.md`.
    pub fn render_report(&self) -> String {
        let mut out = String::new();
        // Writing to a String cannot fail.
        let _ = self.write_report(&mut out);
        out
    }

    fn write_report(&self, out: &mut String) -> fmt::Result {
        let line = &self.release_line;
        writeln!(out, "# ES|QL conformance")?;
        writeln!(out)?;
        writeln!(out, "<!--")?;
        writeln!(out, "  GENERATED FILE -- do not edit by hand.")?;
        writeln!(
            out,
            "  Generated from crates/ravel-esql/manifest/esql-{line}.toml by"
        )?;
        writeln!(
            out,
            "  crates/ravel-esql/tests/conformance.rs. Regenerate with:"
        )?;
        writeln!(
            out,
            "      REGEN_ESQL_CONFORMANCE=1 cargo test -p ravel-esql --test conformance"
        )?;
        writeln!(out, "-->")?;
        writeln!(out)?;
        writeln!(
            out,
            "How much of the released ES|QL language Ravel's ES|QL front end \
             accepts and runs (ADR-2887 D2). Target release line: {line}."
        )?;
        writeln!(out)?;
        writeln!(
            out,
            "The manifest this report is generated from was written from the \
             published ES|QL language reference for the {line} line, without \
             consulting the Elasticsearch source tree."
        )?;
        let binary = &self.release_binary;
        if self.reconciled {
            writeln!(
                out,
                "It has been reconciled with the released {binary} binary \
                 (ADR-2887 D3)."
            )?;
        } else {
            writeln!(
                out,
                "It has not been reconciled with the released {binary} binary \
                 (ADR-2887 D3), so an entry can be missing, misnamed or carry \
                 the wrong preview flag, and an expected answer can be wrong."
            )?;
        }
        writeln!(out)?;

        writeln!(out, "## Figures")?;
        writeln!(out)?;
        writeln!(
            out,
            "Each figure counts released entries only; development-only \
             commands are listed below and are in no denominator."
        )?;
        writeln!(out)?;
        writeln!(out, "| Figure | Score | What counts |")?;
        writeln!(out, "|---|---|---|")?;
        writeln!(
            out,
            "| Syntax coverage | {} | released entries with a parser rule of \
             their own whose example the parser accepts |",
            self.syntax_coverage()
        )?;
        writeln!(
            out,
            "| Execution compatibility | {} | released entries whose example \
             runs and gives the release's answer, columns, types, rows and \
             warnings; a valid entry Ravel refuses or answers differently is a \
             gap |",
            self.execution_compatibility()
        )?;
        writeln!(
            out,
            "| Transport compatibility | {} | options of the Elasticsearch \
             query endpoint that Ravel serves |",
            self.transport_compatibility()
        )?;
        writeln!(out)?;
        writeln!(
            out,
            "Ran and diverged: {}, released entries whose example runs and \
             gives an answer that differs from the release's by exactly one \
             known divergence: the answer differs as given, and undoing that \
             divergence in it gives the release's columns, types, rows and \
             warnings.",
            self.divergent()
        )?;
        writeln!(out)?;
        writeln!(
            out,
            "Ran: {}, released entries whose example runs, whether or not the \
             answer matches.",
            self.ran()
        )?;
        writeln!(out)?;
        writeln!(
            out,
            "Accepted through a generic rule: {}, released functions, aliases and types \
             the parser reads only through the generic call rule `name(args)`, \
             the generic cast rule `value::type`, or as a plain field. The \
             parser does not check the name in any of these rules, so a \
             made-up name parses the same way; they are not in syntax \
             coverage.",
            self.generic_rule_coverage()
        )?;
        writeln!(out)?;
        writeln!(
            out,
            "Negative statements: {}, each refused by the parser. They are \
             counted on this line only and never raise a figure above.",
            self.negatives.len()
        )?;
        writeln!(out)?;

        writeln!(out, "## By kind")?;
        writeln!(out)?;
        writeln!(out, "| Kind | Entries | Parsed | Ran | Compatible |")?;
        writeln!(out, "|---|---|---|---|---|")?;
        for kind in Kind::ALL {
            let of_kind: Vec<&Entry> = self.released().filter(|e| e.kind == kind).collect();
            write_breakdown_row(out, kind.as_str(), &of_kind)?;
        }
        writeln!(out)?;
        writeln!(out, "## Functions by family")?;
        writeln!(out)?;
        writeln!(out, "| Family | Entries | Parsed | Ran | Compatible |")?;
        writeln!(out, "|---|---|---|---|---|")?;
        for family in Family::ALL {
            let of_family: Vec<&Entry> = self
                .released()
                .filter(|e| e.family == Some(family))
                .collect();
            write_breakdown_row(out, family.as_str(), &of_family)?;
        }
        writeln!(out)?;

        writeln!(out, "## Unverified entries")?;
        writeln!(out)?;
        writeln!(
            out,
            "The manifest's author classified each of these as development-only \
             from recollection of the {line} reference, and has not confirmed \
             that classification against the release. Each is kept out of the \
             denominators; if the release ships one, it adds one as a gap to \
             the execution compatibility denominator and to the syntax coverage \
             or generic-rule denominator its grammar selects."
        )?;
        writeln!(out)?;
        for entry in self.entries.iter().filter(|e| e.unverified) {
            writeln!(out, "- {} ({})", code(&entry.name), entry.kind)?;
        }
        writeln!(out)?;

        writeln!(out, "## Development-only commands")?;
        writeln!(out)?;
        let development: Vec<&Entry> = self.development().collect();
        writeln!(
            out,
            "{} None is in a denominator.",
            parser_state_sentence(&development)
        )?;
        writeln!(out)?;
        let names: Vec<String> = development.iter().map(|e| code(&e.name)).collect();
        writeln!(out, "{}", names.join(", "))?;
        writeln!(out)?;

        writeln!(out, "## Refused through an earlier construct")?;
        writeln!(out)?;
        writeln!(
            out,
            "Released entries whose example cannot reach the form without an \
             earlier construct the binder refuses. The refusal names that \
             construct, so these entries do not show that the binder knows \
             their own form."
        )?;
        writeln!(out)?;
        for entry in self.released().filter(|e| e.refused_by.is_some()) {
            writeln!(
                out,
                "- {} ({}): refused at {}",
                code(&entry.name),
                entry.kind,
                code(entry.refused_by.as_deref().unwrap_or_default())
            )?;
        }
        writeln!(out)?;

        writeln!(out, "## Known divergences")?;
        writeln!(out)?;
        writeln!(
            out,
            "Valid statements Ravel answers differently from the release, \
             each pinned by a test. The list is not shown to be complete: no \
             test compares a Ravel answer with the released {binary} binary, \
             only with answers written by hand in the manifest, so a \
             divergence nobody wrote down is not found."
        )?;
        writeln!(out)?;
        for divergence in &self.divergences {
            let shown_by: Vec<String> = self
                .released()
                .filter(|e| e.divergence.as_deref() == Some(divergence.id.as_str()))
                .map(|e| code(&e.name))
                .collect();
            let shown = match &divergence.no_entry {
                Some(reason) => format!("No entry shows it: {reason}"),
                None => format!("Shown by: {}.", shown_by.join(", ")),
            };
            writeln!(
                out,
                "- **{}** ({}). {} Test: {}. {shown}",
                capitalize(&divergence.name),
                code(&divergence.id),
                divergence.description,
                code(&divergence.evidence)
            )?;
        }
        writeln!(out)?;

        writeln!(out, "## Transport")?;
        writeln!(out)?;
        writeln!(
            out,
            "These are the options of the Elasticsearch query endpoint, \
             `POST /_query`, which Ravel does not serve. Ravel's own \
             `POST /api/v1/esql` takes a statement in a request body of its \
             own shape and is not scored here."
        )?;
        writeln!(out)?;
        writeln!(out, "| Option | Description | State |")?;
        writeln!(out, "|---|---|---|")?;
        for option in &self.transport {
            writeln!(
                out,
                "| {} | {} | {} |",
                code(&option.name),
                cell(&option.description),
                option.state
            )?;
        }
        writeln!(out)?;

        writeln!(out, "## Entries")?;
        writeln!(out)?;
        writeln!(
            out,
            "Every released entry. Parser, binder and executor give each \
             layer's state: supported, refused with a typed error, or absent \
             (the layer never sees or does not know the form)."
        )?;
        writeln!(out)?;
        writeln!(
            out,
            "Grammar is `own` for a form the parser has a rule for, and `call`, \
             `cast` or `field` for one it reads only through a generic rule. \
             Divergence names the known divergence the entry's answer shows."
        )?;
        writeln!(out)?;
        writeln!(
            out,
            "| Kind | Name | Family | Preview | Grammar | Parser | Binder | Executor | Divergence | Needs | Example |"
        )?;
        writeln!(out, "|---|---|---|---|---|---|---|---|---|---|---|")?;
        let mut by_kind: BTreeMap<Kind, Vec<&Entry>> = BTreeMap::new();
        for entry in self.released() {
            by_kind.entry(entry.kind).or_default().push(entry);
        }
        for entries in by_kind.values() {
            for entry in entries {
                let name = match &entry.alias_of {
                    Some(target) => format!("{} (of {})", code(&entry.name), code(target)),
                    None => code(&entry.name),
                };
                writeln!(
                    out,
                    "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                    entry.kind,
                    name,
                    entry.family.map(Family::as_str).unwrap_or(""),
                    if entry.preview { "yes" } else { "" },
                    entry.grammar,
                    entry.parser,
                    entry.binder,
                    entry.executor,
                    entry.divergence.as_deref().map(code).unwrap_or_default(),
                    entry.needs.as_deref().map(cell).unwrap_or_default(),
                    code(&entry.example)
                )?;
            }
        }
        Ok(())
    }
}

fn count<'a>(entries: impl Iterator<Item = &'a Entry>, counts: impl Fn(&Entry) -> bool) -> Figure {
    let mut figure = Figure {
        numerator: 0,
        denominator: 0,
    };
    for entry in entries {
        figure.denominator += 1;
        if counts(entry) {
            figure.numerator += 1;
        }
    }
    figure
}

fn write_breakdown_row(out: &mut String, label: &str, entries: &[&Entry]) -> fmt::Result {
    let parsed = entries
        .iter()
        .filter(|e| e.parser == State::Supported)
        .count();
    let ran = entries
        .iter()
        .filter(|e| e.executor == State::Supported)
        .count();
    let compatible = entries
        .iter()
        .filter(|e| e.executor == State::Supported && e.divergence.is_none())
        .count();
    let total = entries.len();
    writeln!(
        out,
        "| {label} | {total} | {parsed}/{total} | {ran}/{total} | {compatible}/{total} |"
    )
}

/// One sentence saying how the parser treats `entries`, grouped by its state
/// for each.
fn parser_state_sentence(entries: &[&Entry]) -> String {
    let phrase = |state: State| match state {
        State::Supported => "accepts",
        State::Refused => "recognises and refuses with a typed error",
        State::Absent => "does not know, and fails on as on any invalid text",
    };
    let groups: Vec<(State, Vec<&str>)> = [State::Supported, State::Refused, State::Absent]
        .into_iter()
        .map(|state| {
            let names = entries
                .iter()
                .filter(|e| e.parser == state)
                .map(|e| e.name.as_str())
                .collect::<Vec<_>>();
            (state, names)
        })
        .filter(|(_, names)| !names.is_empty())
        .collect();
    match groups.as_slice() {
        [] => "There are none.".to_owned(),
        [(state, _)] => format!("The parser {} each of these.", phrase(*state)),
        _ => {
            let parts: Vec<String> = groups
                .iter()
                .map(|(state, names)| format!("{} {}", phrase(*state), names.join(", ")))
                .collect();
            format!("The parser {}.", parts.join("; "))
        }
    }
}

/// `text` as a markdown code span that is safe inside a table cell.
fn code(text: &str) -> String {
    let longest_run = text.split(|c| c != '`').map(str::len).max().unwrap_or(0);
    let fence = "`".repeat(longest_run + 1);
    let pad = if text.starts_with('`') || text.ends_with('`') {
        " "
    } else {
        ""
    };
    format!("{fence}{pad}{}{pad}{fence}", text.replace('|', "\\|"))
}

fn cell(text: &str) -> String {
    text.replace('|', "\\|")
}

fn capitalize(text: &str) -> String {
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Value {
    String(String),
    Bool(bool),
    Strings(Vec<String>),
}

struct RawTable {
    name: String,
    array: bool,
    line: usize,
    fields: Vec<(String, Value, usize)>,
}

impl RawTable {
    fn take(&mut self, key: &str) -> Option<(Value, usize)> {
        let index = self.fields.iter().position(|(k, _, _)| k == key)?;
        let (_, value, line) = self.fields.remove(index);
        Some((value, line))
    }

    fn missing(&self, key: &str) -> ManifestError {
        field(
            self.line,
            &format!("[{}] is missing key {key:?}", self.name),
        )
    }

    fn wrong_type(line: usize, key: &str, expected: &str) -> ManifestError {
        field(line, &format!("{key} must be {expected}"))
    }

    fn take_optional_string(&mut self, key: &str) -> Result<Option<String>, ManifestError> {
        match self.take(key) {
            None => Ok(None),
            Some((Value::String(text), _)) => Ok(Some(text)),
            Some((_, line)) => Err(Self::wrong_type(line, key, "a string")),
        }
    }

    fn take_string(&mut self, key: &str) -> Result<String, ManifestError> {
        self.take_optional_string(key)?
            .ok_or_else(|| self.missing(key))
    }

    fn take_optional_bool(&mut self, key: &str) -> Result<Option<bool>, ManifestError> {
        match self.take(key) {
            None => Ok(None),
            Some((Value::Bool(value), _)) => Ok(Some(value)),
            Some((_, line)) => Err(Self::wrong_type(line, key, "true or false")),
        }
    }

    fn take_bool(&mut self, key: &str) -> Result<bool, ManifestError> {
        self.take_optional_bool(key)?
            .ok_or_else(|| self.missing(key))
    }

    fn take_optional_strings(&mut self, key: &str) -> Result<Option<Vec<String>>, ManifestError> {
        match self.take(key) {
            None => Ok(None),
            Some((Value::Strings(values), _)) => Ok(Some(values)),
            Some((_, line)) => Err(Self::wrong_type(line, key, "an array of strings")),
        }
    }

    fn take_state(&mut self, key: &str) -> Result<State, ManifestError> {
        match self.take(key) {
            None => Err(self.missing(key)),
            Some((Value::String(text), line)) => {
                State::parse(&text).ok_or_else(|| ManifestError::InvalidState {
                    line,
                    key: key.to_owned(),
                    value: text,
                })
            }
            Some((_, line)) => Err(Self::wrong_type(line, key, "a string")),
        }
    }

    /// Refuse any key no reader took.
    fn finish(&self) -> Result<(), ManifestError> {
        match self.fields.first() {
            None => Ok(()),
            Some((key, _, line)) => Err(field(
                *line,
                &format!("unknown key {key:?} in [{}]", self.name),
            )),
        }
    }
}

fn read_tables(text: &str) -> Result<Vec<RawTable>, ManifestError> {
    let mut tables: Vec<RawTable> = Vec::new();
    let mut lines = text.lines().enumerate();
    while let Some((index, raw)) = lines.next() {
        let line = index + 1;
        let syntax = |message: &str| ManifestError::Syntax {
            line,
            message: message.to_owned(),
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix('[') {
            let (array, rest) = match rest.strip_prefix('[') {
                Some(rest) => (true, rest),
                None => (false, rest),
            };
            let close = if array { "]]" } else { "]" };
            let Some(end) = rest.find(close) else {
                return Err(syntax("unclosed table header"));
            };
            let (name, after) = rest.split_at(end);
            let after = &after[close.len()..];
            if !is_bare_key(name) {
                return Err(syntax("table name is not a bare key"));
            }
            if !is_comment_or_empty(after) {
                return Err(syntax("text after a table header"));
            }
            tables.push(RawTable {
                name: name.to_owned(),
                array,
                line,
                fields: Vec::new(),
            });
            continue;
        }
        let Some((key, rest)) = trimmed.split_once('=') else {
            return Err(syntax("expected key = value"));
        };
        let key = key.trim();
        if !is_bare_key(key) {
            return Err(syntax("key is not a bare key"));
        }
        // An array may continue over the following lines until its `]`.
        let mut joined = rest.trim_start().to_owned();
        let (value, after) = loop {
            match read_value(&joined) {
                Err(message) if message == UNTERMINATED_ARRAY => match lines.next() {
                    Some((_, more)) => {
                        joined.push('\n');
                        joined.push_str(more);
                    }
                    None => return Err(syntax(UNTERMINATED_ARRAY)),
                },
                result => break result.map_err(|m| syntax(&m))?,
            }
        };
        if !is_comment_or_empty(after) {
            return Err(syntax("text after a value"));
        }
        let Some(table) = tables.last_mut() else {
            return Err(syntax("key before any table header"));
        };
        if table.fields.iter().any(|(k, _, _)| k == key) {
            return Err(syntax(&format!("key {key:?} repeated in one table")));
        }
        table.fields.push((key.to_owned(), value, line));
    }
    Ok(tables)
}

fn is_bare_key(text: &str) -> bool {
    !text.is_empty()
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn is_comment_or_empty(text: &str) -> bool {
    let text = text.trim_start();
    text.is_empty() || text.starts_with('#')
}

const UNTERMINATED_ARRAY: &str = "unterminated array";

/// Read one value from the start of `text`, returning it and the rest. An
/// array may hold newlines and `#` comments between its values.
fn read_value(text: &str) -> Result<(Value, &str), String> {
    if let Some(rest) = text.strip_prefix("true") {
        return Ok((Value::Bool(true), rest));
    }
    if let Some(rest) = text.strip_prefix("false") {
        return Ok((Value::Bool(false), rest));
    }
    if let Some(mut rest) = text.strip_prefix('[') {
        let mut values = Vec::new();
        loop {
            rest = rest.trim_start();
            while let Some(comment) = rest.strip_prefix('#') {
                rest = comment
                    .split_once('\n')
                    .map_or("", |(_, after)| after)
                    .trim_start();
            }
            if rest.is_empty() {
                return Err(UNTERMINATED_ARRAY.to_owned());
            }
            if let Some(after) = rest.strip_prefix(']') {
                return Ok((Value::Strings(values), after));
            }
            let (value, after) = read_string(rest)?;
            values.push(value);
            rest = after.trim_start();
            while let Some(comment) = rest.strip_prefix('#') {
                rest = comment
                    .split_once('\n')
                    .map_or("", |(_, after)| after)
                    .trim_start();
            }
            if let Some(after) = rest.strip_prefix(',') {
                rest = after;
            } else if rest.is_empty() {
                return Err(UNTERMINATED_ARRAY.to_owned());
            } else if !rest.starts_with(']') {
                return Err("expected , or ] in an array".to_owned());
            }
        }
    }
    let (value, rest) = read_string(text)?;
    Ok((Value::String(value), rest))
}

fn read_string(text: &str) -> Result<(String, &str), String> {
    if text.starts_with("'''") || text.starts_with("\"\"\"") {
        return Err("multi-line strings are not supported".to_owned());
    }
    if let Some(rest) = text.strip_prefix('\'') {
        let end = rest
            .find('\'')
            .ok_or_else(|| "unterminated literal string".to_owned())?;
        return Ok((rest[..end].to_owned(), &rest[end + 1..]));
    }
    let Some(rest) = text.strip_prefix('"') else {
        return Err("expected a string, a boolean or an array".to_owned());
    };
    let mut value = String::new();
    let mut chars = rest.char_indices();
    while let Some((index, c)) = chars.next() {
        match c {
            '"' => return Ok((value, &rest[index + 1..])),
            '\\' => match chars.next() {
                Some((_, '"')) => value.push('"'),
                Some((_, '\\')) => value.push('\\'),
                Some((_, 'n')) => value.push('\n'),
                Some((_, 't')) => value.push('\t'),
                _ => return Err("unsupported escape in a basic string".to_owned()),
            },
            c => value.push(c),
        }
    }
    Err("unterminated basic string".to_owned())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    const ENTRY: &str = r#"
[release]
line = "9.5"
binary = "9.5.5"
reconciled = false

[[entry]]
kind = "function"
grammar = "call"
name = "TO_STRING"
family = "conversion"
aliases = ["TO_STR"]
preview = false
example = 'ROW a = TO_STRING(1)'
parser = "supported"
binder = "refused"
executor = "refused"
evidence = "a.rs::t"

[[entry]]
kind = "alias"
grammar = "call"
name = "TO_STR"
alias_of = "TO_STRING"
preview = false
example = 'ROW a = TO_STR(1)'
parser = "supported"
binder = "refused"
executor = "refused"
evidence = "a.rs::t"
"#;

    const RUNS: &str = r#"
[[entry]]
kind = "syntax"
grammar = "own"
name = "ROW"
preview = false
example = 'ROW a = 1, b = "x"'
parser = "supported"
binder = "supported"
executor = "supported"
columns = ["a:integer", "b:keyword"]
rows = [
  '[1, "x"]',
]
order = "exact"
warnings = []
divergence = "integer-literal"
evidence = "a.rs::t"

[[divergence]]
id = "integer-literal"
name = "Integer literals"
description = "d"
evidence = "a.rs::t"
"#;

    fn answer(columns: &[(&str, &str)], rows: Vec<Vec<Cell>>) -> Answer {
        Answer {
            columns: columns
                .iter()
                .map(|(name, ty)| Column {
                    name: (*name).to_owned(),
                    ty: (*ty).to_owned(),
                })
                .collect(),
            rows,
            warnings: Vec::new(),
        }
    }

    #[test]
    fn a_valid_manifest_loads() {
        let manifest = load(ENTRY).expect("loads");
        assert_eq!(manifest.release_line, "9.5");
        assert_eq!(manifest.release_binary, "9.5.5");
        assert_eq!(manifest.entries.len(), 2);
        assert_eq!(manifest.entries[0].aliases, vec!["TO_STR".to_owned()]);
        assert_eq!(manifest.syntax_coverage().to_string(), "0/0");
        assert_eq!(manifest.generic_rule_coverage().to_string(), "2/2");
        assert_eq!(manifest.execution_compatibility().to_string(), "0/2");
        assert_eq!(manifest.ran().to_string(), "0/2");
    }

    #[test]
    fn a_running_entry_carries_the_release_answer() {
        let manifest = load(&format!("{ENTRY}{RUNS}")).expect("loads");
        let entry = &manifest.entries[2];
        let Some(Expected {
            answer: expected,
            order,
        }) = &entry.expected
        else {
            panic!("no answer: {:?}", entry.expected);
        };
        assert_eq!(order, &Order::Exact);
        assert_eq!(
            expected.rows,
            vec![vec![Cell::Long(1), Cell::Text("x".to_owned())]]
        );
        assert_eq!(manifest.syntax_coverage().to_string(), "1/1");
        assert_eq!(manifest.ran().to_string(), "1/3");
        assert_eq!(manifest.divergent().to_string(), "1/3");
        assert_eq!(manifest.execution_compatibility().to_string(), "0/3");
        let compatible = RUNS
            .replacen("divergence = \"integer-literal\"\n", "", 1)
            .replacen(
                "evidence = \"a.rs::t\"\n\n[[divergence]]",
                "evidence = \"a.rs::t\"\n\n[[divergence]]\nno_entry = \"r\"",
                1,
            );
        let manifest = load(&format!("{ENTRY}{compatible}")).expect("loads");
        assert_eq!(manifest.execution_compatibility().to_string(), "1/3");
        assert_eq!(manifest.divergent().to_string(), "0/3");
    }

    #[test]
    fn an_answer_is_recorded_exactly_where_the_entry_runs() {
        let text = format!("{ENTRY}{RUNS}").replacen("warnings = []\n", "", 1);
        assert!(matches!(load(&text), Err(ManifestError::Field { .. })));
        let text = ENTRY.replacen(
            "evidence = \"a.rs::t\"",
            "order = \"exact\"\nevidence = \"a.rs::t\"",
            1,
        );
        assert!(matches!(load(&text), Err(ManifestError::Field { .. })));
        let text = format!("{ENTRY}{RUNS}").replacen("'[1, \"x\"]'", "'[1]'", 1);
        assert!(matches!(load(&text), Err(ManifestError::Field { .. })));
        let text = format!("{ENTRY}{RUNS}").replacen("order = \"exact\"", "order = \"key c\"", 1);
        assert!(matches!(load(&text), Err(ManifestError::Field { .. })));
    }

    /// An expected answer naming one column twice does not load, even with
    /// a different type each time: from-columns finds a column by name, and
    /// a repeated name would leave it two to choose from.
    #[test]
    fn an_answer_with_a_repeated_column_name_is_refused() {
        let text = format!("{ENTRY}{RUNS}").replacen("\"b:keyword\"", "\"a:keyword\"", 1);
        let error = load(&text).expect_err("a repeated column name");
        assert_eq!(
            error.to_string(),
            "line 32: \"ROW\": column \"a\" at position 2 repeats an earlier column's name"
        );
    }

    #[test]
    fn an_entry_names_only_a_divergence_with_a_normalisation() {
        let renamed = format!("{ENTRY}{RUNS}").replace("\"integer-literal\"", "\"widening\"");
        let error = load(&renamed).expect_err("no normalisation");
        assert!(
            error.to_string().contains("has no normalisation"),
            "{error}"
        );
        let release_error = format!("{ENTRY}{RUNS}").replacen(
            "columns = [\"a:integer\", \"b:keyword\"]\nrows = [\n  '[1, \"x\"]',\n]\n\
                 order = \"exact\"\nwarnings = []\n",
            "release_error = \"refused\"\n",
            1,
        );
        assert!(matches!(
            load(&release_error),
            Err(ManifestError::Field { .. })
        ));
    }

    fn divergent(expected: &Answer, order: Order) -> Expected {
        Expected {
            answer: expected.clone(),
            order,
        }
    }

    #[test]
    fn a_divergent_answer_compares_in_full_once_the_divergence_is_undone() {
        let text = |t: &str| Cell::Text(t.to_owned());
        let release = answer(
            &[("dur", "long"), ("message", "keyword")],
            vec![
                vec![Cell::Long(1), text("x")],
                vec![Cell::Long(2), text("y")],
            ],
        );
        let expected = divergent(&release, Order::Exact);
        let ravel = answer(
            &[("message", "keyword"), ("dur", "long")],
            vec![
                vec![text("x"), Cell::Long(1)],
                vec![text("y"), Cell::Long(2)],
            ],
        );
        assert_eq!(expected.compare_divergent("from-columns", &ravel), Ok(()));
        let mut changed = ravel.clone();
        changed.rows[1][1] = Cell::Long(3);
        let error = expected
            .compare_divergent("from-columns", &changed)
            .expect_err("a changed value");
        assert!(
            error.starts_with("after undoing from-columns: rows"),
            "{error}"
        );
        let mut retyped = ravel.clone();
        retyped.columns[0].ty = "integer".to_owned();
        let error = expected
            .compare_divergent("from-columns", &retyped)
            .expect_err("a changed type");
        assert!(
            error.starts_with("after undoing from-columns: columns"),
            "{error}"
        );
        let warned = Answer {
            warnings: vec!["w".to_owned()],
            ..ravel.clone()
        };
        let error = expected
            .compare_divergent("from-columns", &warned)
            .expect_err("a warning");
        assert!(
            error.starts_with("after undoing from-columns: warnings"),
            "{error}"
        );
        let error = expected
            .compare_divergent("from-columns", &release)
            .expect_err("no divergence shown");
        assert!(error.contains("without undoing"), "{error}");
        let sorted = answer(
            &[("@timestamp", "date_nanos"), ("message", "keyword")],
            vec![vec![text("t1"), text("x")], vec![text("t2"), text("y")]],
        );
        let swapped_rows = Answer {
            rows: vec![sorted.rows[1].clone(), sorted.rows[0].clone()],
            ..sorted.clone()
        };
        let error = divergent(&sorted, Order::Exact)
            .compare_divergent("from-columns", &swapped_rows)
            .expect_err("columns already in order");
        assert!(error.contains("already in the release's order"), "{error}");
        let renamed = answer(&[("c", "long"), ("message", "keyword")], ravel.rows.clone());
        let error = expected
            .compare_divergent("from-columns", &renamed)
            .expect_err("another column");
        assert!(error.starts_with("cannot undo from-columns"), "{error}");
        let error = expected
            .compare_divergent("integer-literal", &ravel)
            .expect_err("a divergence the answer does not show");
        assert!(error.starts_with("cannot undo integer-literal"), "{error}");
        let error = expected
            .compare_divergent("attribute-fallback", &ravel)
            .expect_err("no normalisation");
        assert!(error.contains("has no normalisation"), "{error}");
    }

    /// from-columns undoes FROM's fixed order and no other: a column FROM
    /// does not give, such as one EVAL adds, is refused even when the names
    /// match, and so is a FROM subset in an order that is not the fixed one,
    /// or a release answer whose columns are not sorted by name.
    #[test]
    fn from_columns_undoes_only_the_fixed_order() {
        let long = |n: i64| Cell::Long(n);
        let release = answer(
            &[("a", "long"), ("b", "long"), ("d2", "long")],
            vec![vec![long(1), long(2), long(3)]],
        );
        let ravel = answer(
            &[("d2", "long"), ("b", "long"), ("a", "long")],
            vec![vec![long(3), long(2), long(1)]],
        );
        let error = divergent(&release, Order::Exact)
            .compare_divergent("from-columns", &ravel)
            .expect_err("columns FROM does not give");
        assert_eq!(
            error,
            "cannot undo from-columns: column \"a\" is not one FROM gives in a fixed order"
        );

        let release = answer(
            &[("dur", "long"), ("flags", "long"), ("message", "keyword")],
            vec![vec![long(1), long(0), Cell::Text("x".to_owned())]],
        );
        let expected = divergent(&release, Order::Exact);
        let shuffled = answer(
            &[("flags", "long"), ("message", "keyword"), ("dur", "long")],
            vec![vec![long(0), Cell::Text("x".to_owned()), long(1)]],
        );
        let error = expected
            .compare_divergent("from-columns", &shuffled)
            .expect_err("a FROM subset out of the fixed order");
        assert_eq!(
            error,
            "cannot undo from-columns: Ravel's order [flags, message, dur] is not FROM's \
             fixed order [message, flags, dur]"
        );
        let fixed = answer(
            &[("message", "keyword"), ("flags", "long"), ("dur", "long")],
            vec![vec![Cell::Text("x".to_owned()), long(0), long(1)]],
        );
        assert_eq!(expected.compare_divergent("from-columns", &fixed), Ok(()));

        let unsorted = answer(
            &[("flags", "long"), ("dur", "long"), ("message", "keyword")],
            vec![vec![long(0), long(1), Cell::Text("x".to_owned())]],
        );
        let error = divergent(&unsorted, Order::Exact)
            .compare_divergent("from-columns", &fixed)
            .expect_err("a release order not sorted by name");
        assert_eq!(
            error,
            "cannot undo from-columns: the release's order [flags, dur, message] is not \
             sorted by name"
        );
    }

    /// The loader refuses a from-columns entry whose answer has a column
    /// FROM does not give, since the normalisation could never apply to it.
    #[test]
    fn a_from_columns_entry_needs_columns_from_gives() {
        let runs = |columns: &str| {
            RUNS.replacen(
                "columns = [\"a:integer\", \"b:keyword\"]",
                &format!("columns = {columns}"),
                1,
            )
            .replace("integer-literal", "from-columns")
        };
        let native = runs("[\"dur:integer\", \"message:keyword\"]");
        assert!(load(&format!("{ENTRY}{native}")).is_ok());
        let error = load(&format!(
            "{ENTRY}{}",
            runs("[\"d2:integer\", \"message:keyword\"]")
        ))
        .expect_err("a column FROM does not give");
        assert!(
            error
                .to_string()
                .contains("column \"d2\" is not one FROM gives"),
            "{error}"
        );
    }

    #[test]
    fn an_integer_literal_is_read_as_an_integer_only_where_the_release_has_one() {
        let release = answer(
            &[("a", "integer"), ("b", "long")],
            vec![vec![Cell::Long(3), Cell::Long(4)]],
        );
        let expected = divergent(&release, Order::Exact);
        let ravel = answer(
            &[("a", "long"), ("b", "long")],
            vec![vec![Cell::Long(3), Cell::Long(4)]],
        );
        assert_eq!(
            expected.compare_divergent("integer-literal", &ravel),
            Ok(())
        );
        let mut changed = ravel.clone();
        changed.rows[0][0] = Cell::Long(4);
        let error = expected
            .compare_divergent("integer-literal", &changed)
            .expect_err("a changed value");
        assert!(
            error.starts_with("after undoing integer-literal: rows"),
            "{error}"
        );
        let wide = answer(
            &[("a", "long"), ("b", "long")],
            vec![vec![Cell::Long(1 << 40), Cell::Long(4)]],
        );
        let error = expected
            .compare_divergent("integer-literal", &wide)
            .expect_err("a value past 32 bits");
        assert!(error.contains("does not fit an integer"), "{error}");
        let both = divergent(
            &answer(&[("a", "integer"), ("b", "integer")], release.rows.clone()),
            Order::Exact,
        );
        assert_eq!(both.compare_divergent("integer-literal", &ravel), Ok(()));
        let double = answer(
            &[("a", "double"), ("b", "long")],
            vec![vec![Cell::Double(3.0), Cell::Long(4)]],
        );
        let error = expected
            .compare_divergent("integer-literal", &double)
            .expect_err("a double is not a widened integer");
        assert!(error.starts_with("cannot undo integer-literal"), "{error}");
    }

    #[test]
    fn now_is_read_as_a_millisecond_date_only_where_the_release_has_one() {
        let release = answer(
            &[("t", "date")],
            vec![vec![Cell::Text("2023-11-14T22:23:00.000Z".to_owned())]],
        );
        let expected = divergent(&release, Order::Exact);
        let ravel = |rendered: &str| {
            answer(
                &[("t", "date_nanos")],
                vec![vec![Cell::Text(rendered.to_owned())]],
            )
        };
        assert_eq!(
            expected.compare_divergent("now-type", &ravel("2023-11-14T22:23:00.000000000Z")),
            Ok(())
        );
        let error = expected
            .compare_divergent("now-type", &ravel("2023-11-14T22:23:01.000000000Z"))
            .expect_err("a changed value");
        assert!(error.starts_with("after undoing now-type: rows"), "{error}");
        let error = expected
            .compare_divergent("now-type", &ravel("2023-11-14T22:23:00.000000500Z"))
            .expect_err("sub-millisecond precision");
        assert!(error.contains("below a millisecond"), "{error}");
        let error = expected
            .compare_divergent("now-type", &ravel("2023-11-14T22:23:00Z"))
            .expect_err("another rendering");
        assert!(error.contains("nine fraction digits"), "{error}");
        let error = expected
            .compare_divergent(
                "now-type",
                &answer(
                    &[("t", "keyword")],
                    vec![vec![Cell::Text(
                        "2023-11-14T22:23:00.000000000Z".to_owned(),
                    )]],
                ),
            )
            .expect_err("a keyword is not a date_nanos");
        assert!(error.starts_with("cannot undo now-type"), "{error}");
    }

    #[test]
    fn every_divergence_is_named_by_an_entry_or_says_why_not() {
        let text = format!("{ENTRY}{RUNS}").replacen("divergence = \"integer-literal\"\n", "", 1);
        assert!(matches!(load(&text), Err(ManifestError::Field { .. })));
        let text = format!("{ENTRY}{RUNS}").replacen(
            "\n[[divergence]]\n",
            "\n[[divergence]]\nno_entry = \"r\"\n",
            1,
        );
        assert!(matches!(load(&text), Err(ManifestError::Field { .. })));
        let text = format!("{ENTRY}{RUNS}").replacen(
            "divergence = \"integer-literal\"",
            "divergence = \"other\"",
            1,
        );
        assert!(matches!(load(&text), Err(ManifestError::Field { .. })));
    }

    #[test]
    fn refused_by_names_an_entry_the_binder_refuses_itself() {
        let refused_by = |target: &str| {
            ENTRY.replacen(
                "example = 'ROW a = TO_STR(1)'\nparser = \"supported\"\nbinder = \"refused\"\n\
                 executor = \"refused\"\n",
                &format!(
                    "example = 'ROW a = TO_STR(1)'\nparser = \"supported\"\nbinder = \"refused\"\n\
                     executor = \"refused\"\nrefused_by = \"{target}\"\n"
                ),
                1,
            )
        };
        let manifest = load(&refused_by("TO_STRING")).expect("loads");
        assert_eq!(manifest.entries[1].refusal_marker(), "[TO_STR]");
        assert_eq!(manifest.entries[0].refusal_marker(), "[TO_STRING]");
        assert!(matches!(
            load(&refused_by("TO_TEXT")),
            Err(ManifestError::Field { .. })
        ));
        let text = format!("{}{RUNS}", refused_by("ROW"));
        assert!(matches!(load(&text), Err(ManifestError::Field { .. })));
        let text = refused_by("TO_STRING").replacen(
            "refused_by = \"TO_STRING\"",
            "refused_by = \"TO_STRING\"\nrefusal = \"x\"",
            1,
        );
        assert!(matches!(load(&text), Err(ManifestError::Field { .. })));
        let text = format!("{ENTRY}{RUNS}").replacen(
            "warnings = []\n",
            "warnings = []\nrefusal = \"ROW\"\n",
            1,
        );
        assert!(matches!(load(&text), Err(ManifestError::Field { .. })));
    }

    #[test]
    fn the_comparison_names_the_first_difference() {
        let expected = Expected {
            answer: answer(
                &[("a", "long")],
                vec![vec![Cell::Long(1)], vec![Cell::Long(2)]],
            ),
            order: Order::Exact,
        };
        let same = answer(
            &[("a", "long")],
            vec![vec![Cell::Long(1)], vec![Cell::Long(2)]],
        );
        assert_eq!(expected.compare(&same), Ok(()));
        let swapped = answer(
            &[("a", "long")],
            vec![vec![Cell::Long(2)], vec![Cell::Long(1)]],
        );
        assert!(
            expected
                .compare(&swapped)
                .expect_err("order")
                .starts_with("rows")
        );
        let retyped = answer(&[("a", "integer")], same.rows.clone());
        assert!(
            expected
                .compare(&retyped)
                .expect_err("type")
                .starts_with("columns")
        );
        let warned = Answer {
            warnings: vec!["w".to_owned()],
            ..same.clone()
        };
        assert!(
            expected
                .compare(&warned)
                .expect_err("warning")
                .starts_with("warnings")
        );
        let multiset = Expected {
            answer: answer(&[("a", "long")], same.rows.clone()),
            order: Order::Multiset,
        };
        assert_eq!(multiset.compare(&swapped), Ok(()));
        let doubled = answer(
            &[("a", "long")],
            vec![vec![Cell::Long(1)], vec![Cell::Long(1)]],
        );
        assert!(multiset.compare(&doubled).is_err());
        let any = Expected {
            answer: answer(&[("a", "long")], same.rows.clone()),
            order: Order::Any(1),
        };
        assert_eq!(
            any.compare(&answer(&[("a", "long")], vec![vec![Cell::Long(2)]])),
            Ok(())
        );
        assert!(any.compare(&same).is_err());
    }

    #[test]
    fn a_key_order_compares_the_key_in_sequence_and_ties_as_a_multiset() {
        let rows = |cells: [(i64, &str); 3]| {
            cells
                .iter()
                .map(|(k, v)| vec![Cell::Long(*k), Cell::Text((*v).to_owned())])
                .collect::<Vec<_>>()
        };
        let columns = [("k", "long"), ("v", "keyword")];
        let expected = Expected {
            answer: answer(&columns, rows([(1, "a"), (1, "b"), (2, "c")])),
            order: Order::Key(vec!["k".to_owned()]),
        };
        assert_eq!(
            expected.compare(&answer(&columns, rows([(1, "b"), (1, "a"), (2, "c")]))),
            Ok(())
        );
        assert!(
            expected
                .compare(&answer(&columns, rows([(2, "c"), (1, "a"), (1, "b")])))
                .is_err()
        );
    }

    #[test]
    fn a_row_reads_each_cell_type() {
        assert_eq!(
            read_row(r#"[null, true, false, -3, 1.5, 2e3, "a\"b", "x, y"]"#),
            Ok(vec![
                Cell::Null,
                Cell::Bool(true),
                Cell::Bool(false),
                Cell::Long(-3),
                Cell::Double(1.5),
                Cell::Double(2000.0),
                Cell::Text("a\"b".to_owned()),
                Cell::Text("x, y".to_owned()),
            ])
        );
        assert_eq!(read_row("[]"), Ok(Vec::new()));
        assert!(read_row("[1, two]").is_err());
        assert!(read_row("[1] 2").is_err());
        assert!(read_row("1").is_err());
        assert_ne!(Cell::Double(0.0), Cell::Double(-0.0));
        assert_ne!(Cell::Long(1), Cell::Double(1.0));
    }

    #[test]
    fn the_parser_sentence_follows_the_parser_states() {
        let manifest = load(ENTRY).expect("loads");
        let entries: Vec<&Entry> = manifest.entries.iter().collect();
        assert_eq!(
            parser_state_sentence(&entries),
            "The parser accepts each of these."
        );
        let mut refused = manifest.entries[1].clone();
        refused.parser = State::Refused;
        let entries = vec![&manifest.entries[0], &refused];
        assert_eq!(
            parser_state_sentence(&entries),
            "The parser accepts TO_STRING; recognises and refuses with a typed error TO_STR."
        );
        assert_eq!(parser_state_sentence(&[]), "There are none.");
    }

    #[test]
    fn a_duplicate_name_is_refused() {
        let text = format!(
            "{ENTRY}\n[[entry]]\nkind = \"alias\"\ngrammar = \"call\"\nname = \"TO_STR\"\nalias_of = \"TO_STRING\"\n\
             preview = false\nexample = 'x'\nparser = \"supported\"\nbinder = \"refused\"\n\
             executor = \"refused\"\nevidence = \"a.rs::t\"\n"
        );
        assert!(matches!(
            load(&text),
            Err(ManifestError::DuplicateName { ref name, .. }) if name == "TO_STR"
        ));
    }

    #[test]
    fn an_alias_that_does_not_resolve_is_refused() {
        let text = ENTRY.replace("alias_of = \"TO_STRING\"", "alias_of = \"TO_TEXT\"");
        assert!(matches!(
            load(&text),
            Err(ManifestError::UnresolvedAlias { ref name, .. }) if name == "TO_STR"
        ));
        let text = ENTRY.replace("aliases = [\"TO_STR\"]", "aliases = [\"TO_STR\", \"TO_S\"]");
        assert!(matches!(
            load(&text),
            Err(ManifestError::UnresolvedAlias { ref name, .. }) if name == "TO_S"
        ));
        let text = ENTRY.replace("aliases = [\"TO_STR\"]\n", "");
        assert!(matches!(
            load(&text),
            Err(ManifestError::UnresolvedAlias { ref name, .. }) if name == "TO_STR"
        ));
    }

    #[test]
    fn a_state_outside_the_three_is_refused() {
        let text = ENTRY.replacen("binder = \"refused\"", "binder = \"partial\"", 1);
        assert!(matches!(
            load(&text),
            Err(ManifestError::InvalidState { ref value, .. }) if value == "partial"
        ));
    }

    #[test]
    fn contradicting_states_are_refused() {
        let text = ENTRY.replacen("executor = \"refused\"", "executor = \"supported\"", 1);
        assert!(matches!(
            load(&text),
            Err(ManifestError::InconsistentStates { .. })
        ));
        let text = ENTRY.replacen("parser = \"supported\"", "parser = \"absent\"", 1);
        assert!(matches!(
            load(&text),
            Err(ManifestError::InconsistentStates { .. })
        ));
    }

    #[test]
    fn unknown_and_missing_keys_are_refused() {
        let text = ENTRY.replacen("preview = false", "preview = false\nstatus = \"x\"", 1);
        assert!(matches!(load(&text), Err(ManifestError::Field { .. })));
        let text = ENTRY.replacen("evidence = \"a.rs::t\"\n", "", 1);
        assert!(matches!(load(&text), Err(ManifestError::Field { .. })));
        let text = ENTRY.replacen("family = \"conversion\"\n", "", 1);
        assert!(matches!(load(&text), Err(ManifestError::Field { .. })));
    }

    #[test]
    fn a_code_span_survives_pipes_and_backticks() {
        assert_eq!(code("FROM a | KEEP b"), "`FROM a \\| KEEP b`");
        assert_eq!(code("KEEP `m`"), "`` KEEP `m` ``");
    }
}
