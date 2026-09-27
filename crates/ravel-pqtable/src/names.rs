//! Dataset path and table name validation (ADR-2040 decisions D1 and D2).
//!
//! A dataset is a tenant-relative path matching `[a-z0-9_]+(/[a-z0-9_]+)*`. A
//! table name matches `[a-z_][a-z0-9_]{0,62}` and is neither a built-in table
//! nor a signal name. Both are checked before any key is built from them.

/// Built-in table and signal names a Parquet table may not take.
pub const RESERVED_TABLE_NAMES: [&str; 6] =
    ["samples", "logs", "spans", "alerts", "audit", "profiles"];

/// Longest table name, in bytes (one leading character plus up to 62 more).
pub const MAX_TABLE_NAME_LEN: usize = 63;

/// Why a dataset path was refused. Each defect is reported on its own so a
/// caller can say what was wrong with a `LOCATION` rather than only that it
/// did not match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetDefect {
    Empty,
    /// Contains `://`, or a `:` in the first segment (`s3:bucket/x`).
    Scheme,
    LeadingSlash,
    TrailingSlash,
    /// `//`: an empty path segment.
    EmptySegment,
    /// A `..` sequence anywhere in the path.
    DotDot,
    /// One of `*`, `?`, `[`, `]`, `{`, `}`.
    Glob,
    /// A `%`, which could hide any of the other defects behind an escape.
    PercentEscape,
    Uppercase,
    /// Any other character outside `[a-z0-9_/]`.
    InvalidChar(char),
}

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
    /// A built-in table or signal name.
    Reserved,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NameError {
    #[error("invalid dataset path {dataset:?}: {defect:?} (expected [a-z0-9_]+(/[a-z0-9_]+)*)")]
    InvalidDataset {
        dataset: String,
        defect: DatasetDefect,
    },
    #[error(
        "invalid table name {table:?}: {defect:?} (expected [a-z_][a-z0-9_]{{0,62}}, not a built-in name)"
    )]
    InvalidTable { table: String, defect: TableDefect },
}

const GLOB_CHARS: [char; 6] = ['*', '?', '[', ']', '{', '}'];

/// Accept `dataset` only if it matches `[a-z0-9_]+(/[a-z0-9_]+)*`.
pub fn validate_dataset(dataset: &str) -> Result<(), NameError> {
    let refuse = |defect| {
        Err(NameError::InvalidDataset {
            dataset: dataset.to_string(),
            defect,
        })
    };
    if dataset.is_empty() {
        return refuse(DatasetDefect::Empty);
    }
    let first_segment = dataset.split('/').next().unwrap_or_default();
    if dataset.contains("://") || first_segment.contains(':') {
        return refuse(DatasetDefect::Scheme);
    }
    if dataset.starts_with('/') {
        return refuse(DatasetDefect::LeadingSlash);
    }
    if dataset.ends_with('/') {
        return refuse(DatasetDefect::TrailingSlash);
    }
    if dataset.contains("//") {
        return refuse(DatasetDefect::EmptySegment);
    }
    if dataset.contains("..") {
        return refuse(DatasetDefect::DotDot);
    }
    for c in dataset.chars() {
        match c {
            'a'..='z' | '0'..='9' | '_' | '/' => {}
            c if GLOB_CHARS.contains(&c) => return refuse(DatasetDefect::Glob),
            '%' => return refuse(DatasetDefect::PercentEscape),
            c if c.is_uppercase() => return refuse(DatasetDefect::Uppercase),
            c => return refuse(DatasetDefect::InvalidChar(c)),
        }
    }
    Ok(())
}

/// Accept `table` only if it matches `[a-z_][a-z0-9_]{0,62}` and is not in
/// [`RESERVED_TABLE_NAMES`].
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
    if RESERVED_TABLE_NAMES.contains(&table) {
        return refuse(TableDefect::Reserved);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dataset_defect(dataset: &str) -> Option<DatasetDefect> {
        match validate_dataset(dataset) {
            Ok(()) => None,
            Err(NameError::InvalidDataset { defect, .. }) => Some(defect),
            Err(other) => panic!("unexpected error {other:?}"),
        }
    }

    fn table_defect(table: &str) -> Option<TableDefect> {
        match validate_table(table) {
            Ok(()) => None,
            Err(NameError::InvalidTable { defect, .. }) => Some(defect),
            Err(other) => panic!("unexpected error {other:?}"),
        }
    }

    #[test]
    fn valid_datasets_are_accepted() {
        for ok in [
            "hits",
            "a",
            "0",
            "_",
            "clickbench/hits",
            "a/b_c/d0",
            "2026_09/day_27",
        ] {
            assert_eq!(dataset_defect(ok), None, "{ok:?}");
        }
    }

    #[test]
    fn every_refused_dataset_form_has_its_typed_defect() {
        let cases: [(&str, DatasetDefect); 20] = [
            ("", DatasetDefect::Empty),
            ("s3://bucket/hits", DatasetDefect::Scheme),
            ("file:///etc", DatasetDefect::Scheme),
            ("s3:bucket/hits", DatasetDefect::Scheme),
            ("/hits", DatasetDefect::LeadingSlash),
            ("/", DatasetDefect::LeadingSlash),
            ("hits/", DatasetDefect::TrailingSlash),
            ("a//b", DatasetDefect::EmptySegment),
            ("..", DatasetDefect::DotDot),
            ("a/../b", DatasetDefect::DotDot),
            ("a/..", DatasetDefect::DotDot),
            ("hits*", DatasetDefect::Glob),
            ("hit?", DatasetDefect::Glob),
            ("h[it]s", DatasetDefect::Glob),
            ("h{a,b}", DatasetDefect::Glob),
            ("a%2fb", DatasetDefect::PercentEscape),
            ("%2e%2e", DatasetDefect::PercentEscape),
            ("Hits", DatasetDefect::Uppercase),
            ("hits.parquet", DatasetDefect::InvalidChar('.')),
            ("a b", DatasetDefect::InvalidChar(' ')),
        ];
        for (input, defect) in cases {
            assert_eq!(dataset_defect(input), Some(defect), "{input:?}");
        }
        assert_eq!(dataset_defect("a-b"), Some(DatasetDefect::InvalidChar('-')));
        assert_eq!(
            dataset_defect("a\\b"),
            Some(DatasetDefect::InvalidChar('\\'))
        );
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
        for reserved in RESERVED_TABLE_NAMES {
            assert_eq!(
                table_defect(reserved),
                Some(TableDefect::Reserved),
                "{reserved}"
            );
        }
    }
}
