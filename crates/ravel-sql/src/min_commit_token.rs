//! A `min_commit_token` as the HTTP SQL statement path accepts it: a
//! [`CommitToken`], optionally qualified by the table it bounds
//! (`logs:<token>`), so a statement joining two signal tables can carry one
//! token per table (ADR-2904 decision 3).
//!
//! The token's own encoding is unchanged; the qualifier is a prefix outside
//! it. An encoded token is URL-safe base64 and holds no `:`, so the raw form
//! is split once, at its first colon.

use ravel_types::CommitToken;

/// One `min_commit_token`: the token, and the table it is qualified with, if
/// any. The qualifier is kept as sent; the statement path matches it exactly
/// against the canonical lower-case table names (`samples`, `logs`, `spans`,
/// `alerts`, `audit`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinCommitToken {
    /// The table this token bounds, or `None` for an unqualified token.
    pub table: Option<String>,
    /// The token itself.
    pub token: CommitToken,
}

/// Why a raw `min_commit_token` did not parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MinCommitTokenParseError {
    /// The raw form starts with `:`: a qualifier was marked but is empty.
    #[error("empty table qualifier")]
    EmptyQualifier,
    /// The token part does not decode as a commit token.
    #[error("token does not decode")]
    InvalidToken,
}

impl MinCommitToken {
    /// An unqualified token.
    pub fn unqualified(token: CommitToken) -> Self {
        MinCommitToken { table: None, token }
    }

    /// A token qualified with `table`.
    pub fn qualified(table: impl Into<String>, token: CommitToken) -> Self {
        MinCommitToken {
            table: Some(table.into()),
            token,
        }
    }

    /// Parse the wire form: `<token>` or `<table>:<token>`, split once at the
    /// first colon.
    pub fn parse(raw: &str) -> Result<Self, MinCommitTokenParseError> {
        let (table, encoded) = match raw.split_once(':') {
            None => (None, raw),
            Some(("", _)) => return Err(MinCommitTokenParseError::EmptyQualifier),
            Some((table, encoded)) => (Some(table.to_owned()), encoded),
        };
        let token =
            CommitToken::decode(encoded).map_err(|_| MinCommitTokenParseError::InvalidToken)?;
        Ok(MinCommitToken { table, token })
    }

    /// The wire form [`Self::parse`] reads back.
    pub fn encode(&self) -> String {
        match &self.table {
            None => self.token.encode(),
            Some(table) => format!("{table}:{}", self.token.encode()),
        }
    }
}

impl From<CommitToken> for MinCommitToken {
    fn from(token: CommitToken) -> Self {
        MinCommitToken::unqualified(token)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn token() -> CommitToken {
        CommitToken {
            shard: 0,
            writer_id: Uuid::from_u128(7),
            epoch: 1,
            seq: 3,
            ingest_hour_bucket: 0,
        }
    }

    #[test]
    fn an_unqualified_token_parses_without_a_table() {
        let raw = token().encode();
        let parsed = MinCommitToken::parse(&raw).expect("parses");
        assert_eq!(parsed, MinCommitToken::unqualified(token()));
        assert_eq!(parsed.encode(), raw);
    }

    #[test]
    fn a_qualified_token_splits_at_the_first_colon() {
        let raw = format!("logs:{}", token().encode());
        let parsed = MinCommitToken::parse(&raw).expect("parses");
        assert_eq!(parsed, MinCommitToken::qualified("logs", token()));
        assert_eq!(parsed.encode(), raw);
    }

    #[test]
    fn the_qualifier_is_kept_as_sent() {
        let parsed = MinCommitToken::parse(&format!("Logs:{}", token().encode())).expect("parses");
        assert_eq!(parsed.table.as_deref(), Some("Logs"));
    }

    #[test]
    fn an_empty_qualifier_or_an_undecodable_token_is_refused() {
        let encoded = token().encode();
        assert_eq!(
            MinCommitToken::parse(&format!(":{encoded}")),
            Err(MinCommitTokenParseError::EmptyQualifier)
        );
        assert_eq!(
            MinCommitToken::parse("logs:not-a-token"),
            Err(MinCommitTokenParseError::InvalidToken)
        );
        assert_eq!(
            MinCommitToken::parse(&format!("a:b:{encoded}")),
            Err(MinCommitTokenParseError::InvalidToken)
        );
        assert_eq!(
            MinCommitToken::parse("logs:"),
            Err(MinCommitTokenParseError::InvalidToken)
        );
    }
}
