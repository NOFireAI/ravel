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
    /// The lowered plan failed in the executor.
    #[error(transparent)]
    Execution(SqlError),
}

impl EsqlError {
    /// How a transport should report the error. Everything found before
    /// execution is the caller's statement; an execution error keeps the class
    /// the executor gave it.
    pub fn class(&self) -> ErrorClass {
        match self {
            EsqlError::Parse(_)
            | EsqlError::Verification { .. }
            | EsqlError::Unsupported { .. } => ErrorClass::BadRequest,
            EsqlError::Execution(error) => error.class(),
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
