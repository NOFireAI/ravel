//! The ES|QL release manifest (ADR-2887 D1, D2): one record per released
//! language form, with the state of each of Ravel's layers for it, and the
//! conformance report generated from it.
//!
//! The manifest is `manifest/esql-9.5.toml` in this crate. [`load`] reads it
//! and validates it: every name is unique within its kind, every alias
//! resolves to exactly one entry that lists it back, every state is
//! `supported`, `refused` or `absent`, and no layer claims more than the one
//! before it. [`Manifest::render_report`] produces `docs/esql-conformance.md`.
//!
//! Nothing at runtime reads the manifest yet. Its only readers are the tests
//! that prove each entry's states against the parser, the binder and the
//! executor, and the test that keeps the generated report current.
//!
//! The file is read by a small reader for the subset of TOML it uses: `[table]`
//! and `[[array-of-tables]]` headers, `key = value` lines whose value is a
//! basic or literal string, a boolean, or a one-line array of strings, and `#`
//! comments. Anything else, an unknown table or key, a repeated key and a
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
        Family::ALL.into_iter().find(|family| family.as_str() == text)
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

/// A valid statement Ravel answers differently from the release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Divergence {
    /// A short name.
    pub name: String,
    /// What differs.
    pub description: String,
    /// The test that pins Ravel's answer, as `path::test`.
    pub evidence: String,
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
    let mut release: Option<(String, bool)> = None;
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
                let reconciled = table.take_bool("reconciled")?;
                table.finish()?;
                release = Some((line, reconciled));
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
                    name: table.take_string("name")?,
                    description: table.take_string("description")?,
                    evidence: table.take_string("evidence")?,
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
    let Some((release_line, reconciled)) = release else {
        return Err(field(1, "missing [release]"));
    };
    if release_line.is_empty() {
        return Err(field(1, "[release] line is empty"));
    }
    validate_entries(&entries)?;
    unique_names("negative", negatives.iter().map(|(l, n)| (*l, &n.name)))?;
    unique_names("transport", transport.iter().map(|(l, t)| (*l, &t.name)))?;
    unique_names("divergence", divergences.iter().map(|(l, d)| (*l, &d.name)))?;
    Ok(Manifest {
        release_line,
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
            Family::parse(&text)
                .ok_or_else(|| field(line, &format!("unknown family {text:?}")))?,
        ),
        None => None,
    };
    let aliases = table.take_optional_strings("aliases")?.unwrap_or_default();
    let alias_of = table.take_optional_string("alias_of")?;
    let preview = table.take_bool("preview")?;
    let example = table.take_string("example")?;
    let parser = table.take_state("parser")?;
    let binder = table.take_state("binder")?;
    let executor = table.take_state("executor")?;
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
            return Err(field(line, &format!("{kind} {name:?} cannot have a family")));
        }
    }
    match (kind, &alias_of) {
        (Kind::Alias, None) => {
            return Err(field(line, &format!("alias {name:?} has no alias_of")));
        }
        (Kind::Alias, Some(_)) | (_, None) => {}
        (_, Some(_)) => {
            return Err(field(line, &format!("{kind} {name:?} cannot have alias_of")));
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
    Ok(Entry {
        section,
        kind,
        name,
        family,
        aliases,
        alias_of,
        preview,
        example,
        parser,
        binder,
        executor,
        needs,
        evidence,
        unverified,
        line,
    })
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
                return Err(unresolved(format!(
                    "{target:?} is in the other section"
                )));
            }
            if !resolved.aliases.contains(&entry.name) {
                return Err(unresolved(format!(
                    "{target:?} does not list it in aliases"
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

    /// Released entries whose example the parser accepts.
    pub fn syntax_coverage(&self) -> Figure {
        count(self.released(), |entry| entry.parser == State::Supported)
    }

    /// Released entries whose example prepares and runs. A refused entry is a
    /// gap here even when the refusal is typed.
    pub fn execution_compatibility(&self) -> Figure {
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
        writeln!(out, "  crates/ravel-esql/tests/conformance.rs. Regenerate with:")?;
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
        if self.reconciled {
            writeln!(
                out,
                "It has been reconciled with the released {line} binary \
                 (ADR-2887 D3)."
            )?;
        } else {
            writeln!(
                out,
                "It has not been reconciled with the released {line} binary \
                 (ADR-2887 D3), so an entry can be missing, misnamed or carry \
                 the wrong preview flag."
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
            "| Syntax coverage | {} | released entries whose example the parser accepts |",
            self.syntax_coverage()
        )?;
        writeln!(
            out,
            "| Execution compatibility | {} | released entries whose example \
             prepares and runs; a valid entry Ravel refuses is a gap |",
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
            "Negative statements: {}, each refused by the parser. They are \
             counted on this line only and never raise a figure above.",
            self.negatives.len()
        )?;
        writeln!(out)?;

        writeln!(out, "## By kind")?;
        writeln!(out)?;
        writeln!(out, "| Kind | Entries | Parsed | Executed |")?;
        writeln!(out, "|---|---|---|---|")?;
        for kind in Kind::ALL {
            let of_kind: Vec<&Entry> = self.released().filter(|e| e.kind == kind).collect();
            write_breakdown_row(out, kind.as_str(), &of_kind)?;
        }
        writeln!(out)?;
        writeln!(out, "## Functions by family")?;
        writeln!(out)?;
        writeln!(out, "| Family | Entries | Parsed | Executed |")?;
        writeln!(out, "|---|---|---|---|")?;
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
            "Recalled as development-only, but not confirmed against the {line} \
             reference. Each is kept out of the denominators; if the release \
             ships one, it adds one to every denominator as a gap."
        )?;
        writeln!(out)?;
        for entry in self.entries.iter().filter(|e| e.unverified) {
            writeln!(out, "- {} ({})", code(&entry.name), entry.kind)?;
        }
        writeln!(out)?;

        writeln!(out, "## Development-only commands")?;
        writeln!(out)?;
        writeln!(
            out,
            "The parser recognises each of these and refuses it with a typed \
             error. None is in a denominator."
        )?;
        writeln!(out)?;
        let development: Vec<String> = self.development().map(|e| code(&e.name)).collect();
        writeln!(out, "{}", development.join(", "))?;
        writeln!(out)?;

        writeln!(out, "## Divergences")?;
        writeln!(out)?;
        writeln!(
            out,
            "Valid statements Ravel answers differently from the release, \
             each pinned by a test."
        )?;
        writeln!(out)?;
        for divergence in &self.divergences {
            writeln!(
                out,
                "- **{}.** {} Test: {}.",
                capitalize(&divergence.name),
                divergence.description,
                code(&divergence.evidence)
            )?;
        }
        writeln!(out)?;

        writeln!(out, "## Transport")?;
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
            "| Kind | Name | Family | Preview | Parser | Binder | Executor | Needs | Example |"
        )?;
        writeln!(out, "|---|---|---|---|---|---|---|---|---|")?;
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
                    "| {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                    entry.kind,
                    name,
                    entry.family.map(Family::as_str).unwrap_or(""),
                    if entry.preview { "yes" } else { "" },
                    entry.parser,
                    entry.binder,
                    entry.executor,
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
    let executed = entries
        .iter()
        .filter(|e| e.executor == State::Supported)
        .count();
    writeln!(
        out,
        "| {label} | {} | {parsed}/{} | {executed}/{} |",
        entries.len(),
        entries.len(),
        entries.len()
    )
}

/// `text` as a markdown code span that is safe inside a table cell.
fn code(text: &str) -> String {
    let longest_run = text
        .split(|c| c != '`')
        .map(str::len)
        .max()
        .unwrap_or(0);
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
    for (index, raw) in text.lines().enumerate() {
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
        let (value, after) = read_value(rest.trim_start()).map_err(|m| syntax(&m))?;
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

/// Read one value from the start of `text`, returning it and the rest.
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
            if let Some(after) = rest.strip_prefix(']') {
                return Ok((Value::Strings(values), after));
            }
            let (value, after) = read_string(rest)?;
            values.push(value);
            rest = after.trim_start();
            if let Some(after) = rest.strip_prefix(',') {
                rest = after;
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
reconciled = false

[[entry]]
kind = "function"
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
name = "TO_STR"
alias_of = "TO_STRING"
preview = false
example = 'ROW a = TO_STR(1)'
parser = "supported"
binder = "refused"
executor = "refused"
evidence = "a.rs::t"
"#;

    #[test]
    fn a_valid_manifest_loads() {
        let manifest = load(ENTRY).expect("loads");
        assert_eq!(manifest.release_line, "9.5");
        assert_eq!(manifest.entries.len(), 2);
        assert_eq!(manifest.entries[0].aliases, vec!["TO_STR".to_owned()]);
        assert_eq!(manifest.syntax_coverage().to_string(), "2/2");
        assert_eq!(manifest.execution_compatibility().to_string(), "0/2");
    }

    #[test]
    fn a_duplicate_name_is_refused() {
        let text = format!(
            "{ENTRY}\n[[entry]]\nkind = \"alias\"\nname = \"TO_STR\"\nalias_of = \"TO_STRING\"\n\
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
