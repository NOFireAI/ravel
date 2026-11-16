//! Query settings and parameters in name positions: the parser accepts both,
//! and the binder refuses both by name, since it can honour neither.

use ravel_sql::esql::EsqlError;

use crate::support::prepare;

fn unsupported(source: &str) -> (String, usize, usize) {
    match prepare(source) {
        Err(EsqlError::Unsupported { name, line, column }) => (name, line, column),
        other => panic!("{source}: expected an unsupported error, got {other:?}"),
    }
}

/// The 1-based column of `needle`'s first occurrence on a one-line source.
fn column_of(source: &str, needle: &str) -> usize {
    source
        .find(needle)
        .unwrap_or_else(|| panic!("{needle} not in {source}"))
        + 1
}

#[test]
fn a_setting_is_refused_by_name_at_its_position() {
    assert_eq!(
        unsupported("SET a = 1; FROM logs"),
        ("query setting [a]".to_string(), 1, 1)
    );
    assert_eq!(
        unsupported(r#"SET project_routing = "x"; FROM logs | KEEP message"#),
        ("query setting [project_routing]".to_string(), 1, 1)
    );
}

#[test]
fn the_first_of_several_settings_is_the_one_named() {
    assert_eq!(
        unsupported("SET first = 1; SET second = 2; FROM logs"),
        ("query setting [first]".to_string(), 1, 1)
    );
    assert_eq!(
        unsupported("\n  SET b = {\"k\": 1};\nROW x = 1"),
        ("query setting [b]".to_string(), 2, 3)
    );
}

/// Every name position the parser fills from a parameter. Each case is a
/// statement that binds when the parameter part is ignored.
#[test]
fn a_parameter_in_a_name_position_is_refused_where_it_stands() {
    let cases = [
        ("ROW ?x = 1", "?x"),
        ("ROW a = 1, ?n = 2", "?n"),
        ("FROM logs | EVAL ?x = 1", "?x"),
        ("FROM logs | EVAL ??x = 1", "??x"),
        ("FROM logs | STATS ?c = COUNT(*)", "?c"),
        ("FROM logs | STATS c = COUNT(*) BY ?g = service", "?g"),
        ("FROM logs | STATS c = COUNT(*) BY ?g", "?g"),
        ("FROM logs | KEEP ?k", "?k"),
        ("FROM logs | KEEP message, ??k", "??k"),
        ("FROM logs | DROP ?k", "?k"),
        ("FROM logs | RENAME message AS ?b", "?b"),
        ("FROM logs | RENAME ?a AS b", "?a"),
        ("FROM logs | RENAME ?b = message", "?b"),
        ("FROM logs | RENAME b = ?a", "?a"),
    ];
    let wrong: Vec<String> = cases
        .iter()
        .filter_map(|(source, param)| {
            let want = ("query parameter".to_string(), 1, column_of(source, param));
            match prepare(source) {
                Err(EsqlError::Unsupported { name, line, column })
                    if (name.clone(), line, column) == want =>
                {
                    None
                }
                other => Some(format!("{source}: wanted {want:?}, got {other:?}")),
            }
        })
        .collect();
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// A parameter in a sort key or a field reference reaches the binder as an
/// expression, which was refused before name positions were.
#[test]
fn a_parameter_in_an_expression_position_stays_refused() {
    for (source, param) in [
        ("FROM logs | SORT ?x", "?x"),
        ("FROM logs | WHERE ?x == 1", "?x"),
        ("FROM logs | EVAL y = ?x", "?x"),
    ] {
        assert_eq!(
            unsupported(source),
            ("query parameter".to_string(), 1, column_of(source, param)),
            "{source}"
        );
    }
}
