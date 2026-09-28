//! Table name validation (ADR-2040 decisions D1 and D2).
//!
//! A table name matches `[a-z_][a-z0-9_]{0,62}` and is neither a built-in
//! table nor a signal name. It is checked before any key is built from it.
//! Locations are not names: they are URLs, canonicalised in [`crate::grants`].

/// Built-in table and signal names a Parquet table may not take.
pub const RESERVED_TABLE_NAMES: [&str; 6] =
    ["samples", "logs", "spans", "alerts", "audit", "profiles"];

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
    /// A built-in table or signal name.
    Reserved,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NameError {
    #[error(
        "invalid table name {table:?}: {defect:?} (expected [a-z_][a-z0-9_]{{0,62}}, not a built-in name)"
    )]
    InvalidTable { table: String, defect: TableDefect },
}

/// Characters a location URL may not contain: a listing that expanded them
/// would not be the set of files the statement named.
pub(crate) const GLOB_CHARS: [char; 6] = ['*', '?', '[', ']', '{', '}'];

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
        for reserved in RESERVED_TABLE_NAMES {
            assert_eq!(
                table_defect(reserved),
                Some(TableDefect::Reserved),
                "{reserved}"
            );
        }
    }
}
