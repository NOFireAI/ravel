//! Table name validation (ADR-2040 decisions D1 and D2).
//!
//! A table name matches `[a-z_][a-z0-9_]{0,62}` and is neither a built-in
//! table, a signal name, nor a key segment a shipped IAM template grants
//! after a wildcard. It is checked before any key is built from it.
//! Locations are not names: they are URLs, canonicalised in [`crate::grants`].

/// Built-in table and signal names a Parquet table may not take.
pub const RESERVED_TABLE_NAMES: [&str; 6] =
    ["samples", "logs", "spans", "alerts", "audit", "profiles"];

/// Key segments the shipped IAM templates (`deploy/iam/*.json`) grant after a
/// wildcard, which a Parquet table may not take either. IAM's `*` matches
/// across `/`, so a pattern such as `t/*/*/l0/*` also matches the manifests
/// `t/<tenant_hash>/pq/t/l0/v/...` of a table named `l0`, handing them to a
/// role that was never meant to read or write table definitions.
pub const IAM_GRANT_SEGMENTS: [&str; 10] = [
    "l0",
    "c",
    "l1",
    "idem",
    "maint",
    "admission",
    "u",
    "catalog",
    "del",
    "a",
];

/// Longest table name, in bytes (one leading character plus up to 62 more).
pub const MAX_TABLE_NAME_LEN: usize = 63;

/// Why a table name was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableDefect {
    Empty,
    TooLong {
        len: usize,
    },
    /// The first character is not `[a-z_]`.
    InvalidFirstChar(char),
    Uppercase,
    /// A later character outside `[a-z0-9_]`.
    InvalidChar(char),
    /// A built-in table or signal name, or an IAM-granted key segment: the
    /// reserved word the name equals.
    Reserved(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NameError {
    #[error(
        "invalid table name {table:?}: {defect:?} (expected [a-z_][a-z0-9_]{{0,62}}, not a reserved name)"
    )]
    InvalidTable { table: String, defect: TableDefect },
}

/// Characters a location URL may not contain: a listing that expanded them
/// would not be the set of files the statement named.
pub(crate) const GLOB_CHARS: [char; 6] = ['*', '?', '[', ']', '{', '}'];

/// Accept `table` only if it matches `[a-z_][a-z0-9_]{0,62}` and is in neither
/// [`RESERVED_TABLE_NAMES`] nor [`IAM_GRANT_SEGMENTS`].
pub fn validate_table(table: &str) -> Result<(), NameError> {
    let refuse = |defect| {
        Err(NameError::InvalidTable {
            table: table.to_string(),
            defect,
        })
    };
    let mut chars = table.chars();
    let Some(first) = chars.next() else {
        return refuse(TableDefect::Empty);
    };
    if table.len() > MAX_TABLE_NAME_LEN {
        return refuse(TableDefect::TooLong { len: table.len() });
    }
    match first {
        'a'..='z' | '_' => {}
        c if c.is_uppercase() => return refuse(TableDefect::Uppercase),
        c => return refuse(TableDefect::InvalidFirstChar(c)),
    }
    for c in chars {
        match c {
            'a'..='z' | '0'..='9' | '_' => {}
            c if c.is_uppercase() => return refuse(TableDefect::Uppercase),
            c => return refuse(TableDefect::InvalidChar(c)),
        }
    }
    if let Some(reserved) = RESERVED_TABLE_NAMES
        .iter()
        .chain(IAM_GRANT_SEGMENTS.iter())
        .find(|reserved| **reserved == table)
    {
        return refuse(TableDefect::Reserved(reserved));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn table_defect(table: &str) -> Option<TableDefect> {
        match validate_table(table) {
            Ok(()) => None,
            Err(NameError::InvalidTable { defect, .. }) => Some(defect),
        }
    }

    #[test]
    fn valid_tables_are_accepted() {
        let longest = format!("t{}", "x".repeat(62));
        assert_eq!(longest.len(), MAX_TABLE_NAME_LEN);
        for ok in [
            "hits",
            "_",
            "_hits",
            "a0",
            "sample",
            "log",
            longest.as_str(),
        ] {
            assert_eq!(table_defect(ok), None, "{ok:?}");
        }
    }

    #[test]
    fn every_refused_table_form_has_its_typed_defect() {
        let too_long = format!("t{}", "x".repeat(63));
        assert_eq!(table_defect(""), Some(TableDefect::Empty));
        assert_eq!(
            table_defect(&too_long),
            Some(TableDefect::TooLong { len: 64 })
        );
        assert_eq!(
            table_defect("0hits"),
            Some(TableDefect::InvalidFirstChar('0'))
        );
        assert_eq!(table_defect("Hits"), Some(TableDefect::Uppercase));
        assert_eq!(table_defect("hIts"), Some(TableDefect::Uppercase));
        assert_eq!(table_defect("hits-2"), Some(TableDefect::InvalidChar('-')));
        assert_eq!(table_defect("a.b"), Some(TableDefect::InvalidChar('.')));
        assert_eq!(table_defect("a/b"), Some(TableDefect::InvalidChar('/')));
        for reserved in RESERVED_TABLE_NAMES.into_iter().chain(IAM_GRANT_SEGMENTS) {
            assert_eq!(
                table_defect(reserved),
                Some(TableDefect::Reserved(reserved)),
                "{reserved}"
            );
        }
    }

    /// IAM `StringLike` / resource matching: `*` is any run of characters,
    /// `/` included, and `?` is any one character.
    fn iam_matches(pattern: &[u8], text: &[u8]) -> bool {
        match pattern.split_first() {
            None => text.is_empty(),
            Some((b'*', rest)) => (0..=text.len()).any(|skip| iam_matches(rest, &text[skip..])),
            Some((b'?', rest)) => !text.is_empty() && iam_matches(rest, &text[1..]),
            Some((c, rest)) => text.first() == Some(c) && iam_matches(rest, &text[1..]),
        }
    }

    /// Every quoted string in a template that is a `t/` object pattern: an S3
    /// resource ARN with its bucket stripped, or an `s3:prefix` value.
    fn t_patterns(template: &str) -> Vec<String> {
        template
            .split('"')
            .skip(1)
            .step_by(2)
            .filter_map(|literal| match literal.strip_prefix("arn:aws:s3:::") {
                Some(arn) => arn.split_once('/').map(|(_, key)| key),
                None => Some(literal),
            })
            .filter(|key| key.starts_with("t/"))
            .map(str::to_string)
            .collect()
    }

    /// True if `pattern` matches table `table`'s manifest key or a listing
    /// prefix that selects only that table's manifests.
    fn reaches_table(pattern: &str, table: &str) -> bool {
        let key = format!("t/{}/pq/t/{table}/v/{:020}.pqm", "ab".repeat(16), 1);
        let table_prefix_len = key.len() - "v/00000000000000000001.pqm".len();
        (table_prefix_len..=key.len())
            .any(|end| iam_matches(pattern.as_bytes(), &key.as_bytes()[..end]))
    }

    #[test]
    fn every_segment_a_template_grants_after_a_wildcard_is_reserved() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/iam");
        let mut templates = 0;
        let mut patterns = Vec::new();
        for entry in std::fs::read_dir(&dir).expect("read deploy/iam") {
            let path = entry.expect("deploy/iam entry").path();
            if path.extension().is_some_and(|ext| ext == "json") {
                templates += 1;
                patterns.extend(t_patterns(
                    &std::fs::read_to_string(&path).expect("read template"),
                ));
            }
        }
        assert!(templates >= 4, "found {templates} templates in {dir:?}");

        // A pattern that reaches an ordinary table's manifests grants the
        // tenant's whole table space on purpose (`t/*/pq/t/*`); a name
        // cannot step out of it, so only the other patterns are examined.
        let named: Vec<&String> = patterns
            .iter()
            .filter(|pattern| !reaches_table(pattern, "hits"))
            .collect();
        let mut reached = std::collections::BTreeSet::new();
        for pattern in &named {
            for (i, _) in pattern.match_indices("*/") {
                let segment = pattern[i + 2..].split('/').next().unwrap_or_default();
                if !segment.is_empty() && reaches_table(pattern, segment) {
                    reached.insert(segment.to_string());
                }
            }
        }

        let reserved: std::collections::BTreeSet<String> =
            IAM_GRANT_SEGMENTS.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            reached, reserved,
            "IAM_GRANT_SEGMENTS must be exactly the segments a template grant reaches a manifest through"
        );
        for segment in &reached {
            assert_eq!(
                table_defect(segment),
                Some(TableDefect::Reserved(
                    IAM_GRANT_SEGMENTS
                        .into_iter()
                        .find(|s| s == segment)
                        .unwrap_or_default()
                )),
                "{segment}"
            );
        }
    }

    #[test]
    fn the_iam_matcher_crosses_slashes_and_reaches_only_the_named_table() {
        assert!(iam_matches(b"t/*/*/l0/*", b"t/x/pq/t/l0/v/1.pqm"));
        assert!(iam_matches(b"t/??/a", b"t/xy/a"));
        assert!(!iam_matches(b"t/??/a", b"t/xyz/a"));
        assert!(reaches_table("t/*/*/l0/*", "l0"));
        assert!(!reaches_table("t/*/*/l0/*", "l1"));
        assert!(reaches_table("t/*/a/", "a"));
        assert!(!reaches_table("t/*/*/prov", "prov"));
        assert!(!reaches_table("t/*/catalog/*/snap/*", "snap"));
    }
}
