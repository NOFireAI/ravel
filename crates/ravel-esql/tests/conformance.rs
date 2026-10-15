//! The release manifest against the parser, and the conformance report
//! generated from it (ADR-2887 D1, D2).
//!
//! The binder and executor states are proved in ravel-sql's
//! `tests/esql/manifest.rs`, which reads the same file.

#![allow(clippy::expect_used)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use ravel_esql::manifest::{self, Entry, Expected, Grammar, Kind, Manifest, Section, State};
use ravel_esql::{Error, Limits, parse};

fn crate_dir() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn workspace_root() -> PathBuf {
    crate_dir().join("../..")
}

fn manifest_text() -> String {
    std::fs::read_to_string(crate_dir().join("manifest/esql-9.5.toml")).expect("read manifest")
}

fn load() -> Manifest {
    manifest::load(&manifest_text()).expect("the manifest loads and validates")
}

fn doc_path() -> PathBuf {
    workspace_root().join("docs/esql-conformance.md")
}

#[test]
fn the_manifest_loads_and_validates() {
    let manifest = load();
    assert_eq!(manifest.release_line, "9.5");
    assert!(!manifest.reconciled);
    assert!(manifest.released().count() > 0);
    assert!(manifest.development().count() > 0);
}

/// The in-crate reader and a full TOML parser read the same file to the same
/// tables, so a value the subset reader misreads cannot pass unnoticed.
#[test]
fn a_full_toml_parser_reads_the_same_entries() {
    let manifest = load();
    let table: toml::Table = manifest_text().parse().expect("valid TOML");
    let keys: BTreeSet<&str> = table.keys().map(String::as_str).collect();
    assert_eq!(
        keys,
        BTreeSet::from([
            "release",
            "entry",
            "development",
            "negative",
            "transport",
            "divergence"
        ])
    );
    for (section, key) in [
        (Section::Released, "entry"),
        (Section::Development, "development"),
    ] {
        let tables = table[key].as_array().expect("array of tables");
        let entries: Vec<_> = manifest
            .entries
            .iter()
            .filter(|e| e.section == section)
            .collect();
        assert_eq!(tables.len(), entries.len(), "[[{key}]] count");
        for (raw, entry) in tables.iter().zip(entries) {
            let field = |name: &str| raw.get(name).and_then(toml::Value::as_str);
            assert_eq!(field("kind"), Some(entry.kind.as_str()));
            assert_eq!(field("name"), Some(entry.name.as_str()));
            assert_eq!(field("example"), Some(entry.example.as_str()));
            assert_eq!(field("parser"), Some(entry.parser.as_str()));
            assert_eq!(field("binder"), Some(entry.binder.as_str()));
            assert_eq!(field("executor"), Some(entry.executor.as_str()));
            assert_eq!(field("needs"), entry.needs.as_deref());
            assert_eq!(field("evidence"), Some(entry.evidence.as_str()));
            assert_eq!(field("family"), entry.family.map(|f| f.as_str()));
            assert_eq!(field("alias_of"), entry.alias_of.as_deref());
            assert_eq!(field("grammar"), Some(entry.grammar.as_str()));
            assert_eq!(field("divergence"), entry.divergence.as_deref());
            assert_eq!(field("refusal"), entry.refusal.as_deref());
            assert_eq!(field("refused_by"), entry.refused_by.as_deref());
            match &entry.expected {
                None => {
                    for key in ["columns", "rows", "order", "warnings", "release_error"] {
                        assert!(raw.get(key).is_none(), "{} {key}", entry.name);
                    }
                }
                Some(Expected::Error(message)) => {
                    assert_eq!(field("release_error"), Some(message.as_str()));
                }
                Some(Expected::Answer { answer, order }) => {
                    let strings = |key: &str| -> Vec<&str> {
                        raw.get(key)
                            .and_then(toml::Value::as_array)
                            .map(|a| a.iter().filter_map(toml::Value::as_str).collect())
                            .unwrap_or_else(|| panic!("{} {key}", entry.name))
                    };
                    let columns: Vec<String> = answer
                        .columns
                        .iter()
                        .map(|c| format!("{}:{}", c.name, c.ty))
                        .collect();
                    assert_eq!(strings("columns"), columns, "{}", entry.name);
                    assert_eq!(strings("rows").len(), answer.rows.len(), "{}", entry.name);
                    assert_eq!(strings("warnings"), answer.warnings, "{}", entry.name);
                    let order_text = order.to_string();
                    assert_eq!(field("order"), Some(order_text.as_str()));
                }
            }
            assert_eq!(
                raw.get("preview").and_then(toml::Value::as_bool),
                Some(entry.preview)
            );
            assert_eq!(
                raw.get("unverified")
                    .and_then(toml::Value::as_bool)
                    .unwrap_or(false),
                entry.unverified
            );
            let aliases: Vec<&str> = raw
                .get("aliases")
                .and_then(toml::Value::as_array)
                .map(|a| a.iter().filter_map(toml::Value::as_str).collect())
                .unwrap_or_default();
            assert_eq!(aliases, entry.aliases, "{}", entry.name);
        }
    }
    for (key, len) in [
        ("negative", manifest.negatives.len()),
        ("transport", manifest.transport.len()),
        ("divergence", manifest.divergences.len()),
    ] {
        assert_eq!(table[key].as_array().map(Vec::len), Some(len), "{key}");
    }
    let raw = table["divergence"].as_array().expect("array of tables");
    for (raw, divergence) in raw.iter().zip(&manifest.divergences) {
        let field = |name: &str| raw.get(name).and_then(toml::Value::as_str);
        assert_eq!(field("id"), Some(divergence.id.as_str()));
        assert_eq!(field("name"), Some(divergence.name.as_str()));
        assert_eq!(field("no_entry"), divergence.no_entry.as_deref());
    }
    assert_eq!(
        table["release"].get("binary").and_then(toml::Value::as_str),
        Some(manifest.release_binary.as_str())
    );
}

/// Every `path::test` the manifest cites is a test function in that file.
#[test]
fn every_evidence_names_a_test_that_exists() {
    let manifest = load();
    let cited = manifest
        .entries
        .iter()
        .map(|e| e.evidence.as_str())
        .chain(manifest.divergences.iter().map(|d| d.evidence.as_str()));
    let mut missing = Vec::new();
    for evidence in cited.collect::<BTreeSet<_>>() {
        let (path, test) = evidence.split_once("::").expect("validated");
        let found = std::fs::read_to_string(workspace_root().join(path))
            .map(|source| source.contains(&format!("fn {test}(")))
            .unwrap_or(false);
        if !found {
            missing.push(evidence);
        }
    }
    assert!(missing.is_empty(), "evidence without a test: {missing:#?}");
}

/// The parser state of every entry, released and development-only: a
/// supported example parses, a refused one fails with
/// `Error::UnsupportedSyntax`, and an absent one fails with another error.
#[test]
fn every_manifest_entry_marked_parsed_has_an_example_that_parses() {
    let manifest = load();
    let limits = Limits::default();
    let mut wrong = Vec::new();
    for entry in &manifest.entries {
        let outcome = parse(&entry.example, &limits);
        let holds = match (entry.parser, &outcome) {
            (State::Supported, Ok(_)) => true,
            (State::Refused, Err(Error::UnsupportedSyntax { .. })) => true,
            (State::Absent, Err(error)) => !matches!(error, Error::UnsupportedSyntax { .. }),
            _ => false,
        };
        if !holds {
            wrong.push(format!(
                "{} {:?} is marked parser = {} but {:?} gives {:?}",
                entry.kind,
                entry.name,
                entry.parser,
                entry.example,
                outcome.map(|_| "a tree")
            ));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// Why the parsed tree of `example` does not fit `grammar` for an entry
/// named `name`, or `None` when it does. An own-grammar entry must not reach
/// the tree as a call or a cast of its own name, which is what the generic
/// rules make of any name; a call or cast entry must reach it that way; a
/// field entry as neither.
fn shape_error(name: &str, kind: Kind, grammar: Grammar, example: &str) -> Option<String> {
    let tree = match parse(example, &Limits::default()) {
        Ok(tree) => format!("{tree:?}").to_ascii_lowercase(),
        Err(error) => return Some(format!("does not parse: {error:?}")),
    };
    let lower = name.to_ascii_lowercase();
    let call_of_name = tree.contains(&format!("call(call {{ name: \"{lower}\""));
    let cast_to_name = tree.contains(&format!("type_name: \"{lower}\""));
    let any_call = tree.contains("call(call {");
    let any_cast = tree.contains("cast(cast {");
    let fits = match grammar {
        Grammar::Own => !call_of_name && !cast_to_name,
        Grammar::Call if matches!(kind, Kind::Function | Kind::Alias) => call_of_name,
        Grammar::Call => any_call,
        Grammar::Cast => any_cast,
        Grammar::Field => !any_call && !any_cast,
    };
    (!fits).then(|| format!("is classified {grammar} but parses as {tree}"))
}

/// Every released entry whose example parses has the parsed shape its
/// grammar classification claims.
#[test]
fn every_parsed_entry_has_the_shape_its_grammar_claims() {
    let manifest = load();
    let mut wrong = Vec::new();
    let mut checked = 0;
    for entry in manifest.released().filter(|e| e.parser == State::Supported) {
        checked += 1;
        if let Some(error) = shape_error(&entry.name, entry.kind, entry.grammar, &entry.example) {
            wrong.push(format!("{} {:?} {error}", entry.kind, entry.name));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    assert_eq!(
        checked,
        manifest.syntax_coverage().numerator + manifest.generic_rule_coverage().numerator
    );
}

/// The generic rules accept names no release defines, which is why an entry
/// they accept is not counted as syntax coverage, and the shape check tells
/// such an entry apart from one with a rule of its own.
#[test]
fn a_made_up_name_parses_through_the_generic_rules() {
    let call = "ROW a = NOT_A_FUNCTION(1)";
    let cast = "ROW a = 1::not_a_type";
    assert_eq!(
        shape_error("NOT_A_FUNCTION", Kind::Function, Grammar::Call, call),
        None
    );
    assert_eq!(
        shape_error("not_a_type", Kind::Type, Grammar::Cast, cast),
        None
    );
    assert!(shape_error("NOT_A_FUNCTION", Kind::Function, Grammar::Own, call).is_some());
    assert!(shape_error("not_a_type", Kind::Type, Grammar::Own, cast).is_some());
    assert!(shape_error("not_a_type", Kind::Type, Grammar::Field, cast).is_some());
    let field = "FROM logs | KEEP not_a_field";
    assert_eq!(
        shape_error("not_a_field", Kind::Type, Grammar::Field, field),
        None
    );
    assert!(shape_error("not_a_field", Kind::Type, Grammar::Cast, field).is_some());
}

/// Entries whose example cannot reach their form without an earlier refused
/// construct are listed in the report under their own heading.
#[test]
fn the_report_lists_the_entries_refused_through_an_earlier_construct() {
    let manifest = load();
    let rendered = manifest.render_report();
    let (_, section) = rendered
        .split_once("\n## Refused through an earlier construct\n")
        .expect("the section");
    let section = section.split("\n## ").next().unwrap_or_default();
    let listed: Vec<&Entry> = manifest
        .released()
        .filter(|e| e.refused_by.is_some())
        .collect();
    assert!(!listed.is_empty());
    for entry in listed {
        let by = entry.refused_by.as_deref().unwrap_or_default();
        assert!(
            section.contains(&format!(
                "- `{}` ({}): refused at `{by}`\n",
                entry.name, entry.kind
            )),
            "{} refused by {by} is not listed",
            entry.name
        );
    }
}

#[test]
fn every_negative_statement_is_refused_by_the_parser() {
    let manifest = load();
    assert!(!manifest.negatives.is_empty());
    let parsed: Vec<&str> = manifest
        .negatives
        .iter()
        .filter(|n| parse(&n.example, &Limits::default()).is_ok())
        .map(|n| n.example.as_str())
        .collect();
    assert!(parsed.is_empty(), "negatives that parse: {parsed:#?}");
}

#[test]
fn the_figures_count_released_entries_only() {
    let manifest = load();
    let released = manifest.released().count();
    let own = manifest
        .released()
        .filter(|e| e.grammar == Grammar::Own)
        .count();
    assert_eq!(manifest.syntax_coverage().denominator, own);
    assert_eq!(manifest.generic_rule_coverage().denominator, released - own);
    assert_eq!(manifest.execution_compatibility().denominator, released);
    assert_eq!(manifest.divergent().denominator, released);
    assert_eq!(manifest.ran().denominator, released);
    let parsed = |grammar_is_own: bool| {
        manifest
            .released()
            .filter(|e| (e.grammar == Grammar::Own) == grammar_is_own)
            .filter(|e| e.parser == State::Supported)
            .count()
    };
    assert_eq!(manifest.syntax_coverage().numerator, parsed(true));
    assert_eq!(manifest.generic_rule_coverage().numerator, parsed(false));
    let ran = manifest
        .released()
        .filter(|e| e.executor == State::Supported)
        .count();
    let divergent = manifest
        .released()
        .filter(|e| e.executor == State::Supported && e.divergence.is_some())
        .count();
    assert_eq!(manifest.ran().numerator, ran);
    assert_eq!(manifest.divergent().numerator, divergent);
    assert_eq!(
        manifest.execution_compatibility().numerator,
        ran - divergent
    );
    assert!(divergent > 0 && divergent < ran);
    let rendered = manifest.render_report();
    for line in [
        format!(
            "| Execution compatibility | {}/{released} |",
            ran - divergent
        ),
        format!("Ran and diverged: {divergent}/{released},"),
        format!("Ran: {ran}/{released},"),
        format!(
            "Accepted through a generic rule: {}/{},",
            parsed(false),
            released - own
        ),
    ] {
        assert!(rendered.contains(&line), "the report lacks {line:?}");
    }
    let transport = manifest.transport_compatibility();
    assert_eq!(transport.numerator, 0);
    assert_eq!(transport.denominator, manifest.transport.len());
    assert!(
        manifest
            .transport
            .iter()
            .all(|option| option.state == State::Absent)
    );

    // A development-only entry moved into the release adds one to each
    // denominator and nothing to the numerators, since none runs.
    let mut moved = manifest.clone();
    let dev = moved
        .entries
        .iter_mut()
        .find(|e| e.section == Section::Development)
        .expect("a development entry");
    dev.section = Section::Released;
    assert_eq!(
        moved.syntax_coverage().denominator,
        manifest.syntax_coverage().denominator + 1
    );
    assert_eq!(
        moved.syntax_coverage().numerator,
        manifest.syntax_coverage().numerator
    );
    assert_eq!(
        moved.execution_compatibility().numerator,
        manifest.execution_compatibility().numerator
    );
}

/// A valid entry that parses but that Ravel refuses to run is a gap in
/// execution compatibility, not in syntax coverage.
#[test]
fn an_unimplemented_valid_entry_lowers_execution_compatibility() {
    let manifest = load();
    let mut flipped = manifest.clone();
    let entry = flipped
        .entries
        .iter_mut()
        .find(|e| {
            e.section == Section::Released
                && e.executor == State::Supported
                && e.divergence.is_none()
        })
        .expect("a compatible entry");
    entry.binder = State::Refused;
    entry.executor = State::Refused;
    assert_eq!(
        flipped.execution_compatibility().numerator + 1,
        manifest.execution_compatibility().numerator
    );
    assert_eq!(flipped.syntax_coverage(), manifest.syntax_coverage());
}

#[test]
fn the_unverified_entries_are_the_seven_named_and_not_released() {
    let manifest = load();
    let unverified: BTreeSet<&str> = manifest
        .entries
        .iter()
        .filter(|e| e.unverified)
        .map(|e| e.name.as_str())
        .collect();
    assert_eq!(
        unverified,
        BTreeSet::from([
            "IP_LOCATION",
            "METRICS_INFO",
            "REGISTERED_DOMAIN",
            "TS_COLLAPSE",
            "TS_INFO",
            "URI_PARTS",
            "USER_AGENT",
        ])
    );
    assert!(
        manifest
            .entries
            .iter()
            .filter(|e| e.unverified)
            .all(|e| e.section == Section::Development)
    );
}

/// Every function family the issue names has at least one entry.
#[test]
fn every_function_family_has_entries() {
    let manifest = load();
    for family in ravel_esql::manifest::Family::ALL {
        assert!(
            manifest
                .released()
                .any(|e| e.kind == Kind::Function && e.family == Some(family)),
            "no {family} function"
        );
    }
}

#[test]
fn generate_and_check_esql_conformance_doc() {
    let rendered = load().render_report();
    let path = doc_path();
    if std::env::var_os("REGEN_ESQL_CONFORMANCE").is_some() {
        std::fs::write(&path, &rendered).expect("write docs/esql-conformance.md");
        return;
    }
    let committed = std::fs::read_to_string(&path).expect("read docs/esql-conformance.md");
    assert!(
        committed == rendered,
        "docs/esql-conformance.md is out of date. Regenerate with \
         REGEN_ESQL_CONFORMANCE=1 cargo test -p ravel-esql --test conformance"
    );
}

/// The report names its target line and carries no calendar date: a release
/// line is the target, not a day. The per-entry table is left out of the
/// check because examples of the date functions hold date literals.
#[test]
fn the_report_names_the_release_line_and_no_date() {
    let rendered = load().render_report();
    assert!(rendered.contains("Target release line: 9.5."));
    let (report, _) = rendered
        .split_once("\n## Entries\n")
        .expect("an Entries section");
    let bytes = report.as_bytes();
    let has_iso_date = bytes.windows(10).any(|w| {
        w[0..4].iter().all(u8::is_ascii_digit)
            && w[4] == b'-'
            && w[5..7].iter().all(u8::is_ascii_digit)
            && w[7] == b'-'
            && w[8..10].iter().all(u8::is_ascii_digit)
    });
    assert!(!has_iso_date, "the report carries an ISO date");
    for month in [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ] {
        assert!(!report.contains(month), "the report names {month}");
    }
}
