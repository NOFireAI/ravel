//! Why an ES|QL statement was refused or failed.

use ravel_esql::Span;

use crate::error::{ErrorClass, SqlError};

/// Why an ES|QL statement produced no result.
///
/// Line and column are 1-based, against the statement text.
#[derive(Debug, thiserror::Error)]
pub enum EsqlError {
    /// The text does not parse, or exceeds a structural limit.
    #[error("{0}")]
    Parse(ravel_esql::Error),
    /// The statement parses but does not bind: an unknown column, a type
    /// mismatch, an aggregate outside `STATS`.
    #[error("line {line}:{column}: {message}")]
    Verification {
        message: String,
        line: usize,
        column: usize,
    },
    /// The statement uses a command, source, function, operator or type this
    /// lowering does not execute.
    #[error("line {line}:{column}: {name} is not supported")]
    Unsupported {
        name: String,
        line: usize,
        column: usize,
    },
    /// DataFusion could not plan the lowered statement. The payload is
    /// DataFusion's message, kept for the server-side log only, under the
    /// same redaction rule as [`SqlError::Plan`]; the display never carries
    /// it.
    #[error("the ES|QL statement could not be planned")]
    Plan(String),
    /// The lowered plan failed in the executor.
    #[error(transparent)]
    Execution(SqlError),
}

impl EsqlError {
    /// How a transport should report the error. Everything found before
    /// execution is the caller's statement; a planning failure has the class
    /// of [`SqlError::Plan`], and an execution error keeps the class the
    /// executor gave it.
    pub fn class(&self) -> ErrorClass {
        match self {
            EsqlError::Parse(_)
            | EsqlError::Verification { .. }
            | EsqlError::Unsupported { .. } => ErrorClass::BadRequest,
            EsqlError::Plan(_) => ErrorClass::Unsupported,
            EsqlError::Execution(error) => error.class(),
        }
    }

    /// An executor error as this statement's error: a planning failure is
    /// reported as the statement's, without naming SQL.
    pub(crate) fn from_executor(error: SqlError) -> Self {
        match error {
            SqlError::Plan(detail) => EsqlError::Plan(detail),
            other => EsqlError::Execution(other),
        }
    }

    pub(crate) fn verification(source: &str, span: Span, message: impl Into<String>) -> Self {
        let at = span.line_col(source);
        EsqlError::Verification {
            message: message.into(),
            line: at.line,
            column: at.column,
        }
    }

    pub(crate) fn unsupported(source: &str, span: Span, name: impl Into<String>) -> Self {
        let at = span.line_col(source);
        EsqlError::Unsupported {
            name: name.into(),
            line: at.line,
            column: at.column,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_planning_failure_is_reported_without_naming_sql() {
        let detail = "Schema error: No field named \"_e9 + Int64(1)\"".to_string();
        let error = EsqlError::from_executor(SqlError::Plan(detail.clone()));
        assert!(matches!(&error, EsqlError::Plan(kept) if *kept == detail));
        assert_eq!(
            error.to_string(),
            "the ES|QL statement could not be planned"
        );
        assert_eq!(error.class(), SqlError::Plan(detail).class());

        let execution = EsqlError::from_executor(SqlError::Execution("x".to_string()));
        assert!(matches!(
            execution,
            EsqlError::Execution(SqlError::Execution(_))
        ));
    }
}
