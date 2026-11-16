//! The ES|QL types this lowering executes (ADR-2887 D9) and the Arrow type
//! each is carried as.

use datafusion::arrow::datatypes::{DataType, TimeUnit};

/// An ES|QL column or expression type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EsqlType {
    /// UTF-8 text, carried as `Utf8`.
    Keyword,
    /// A signed 64-bit integer, carried as `Int64`.
    Long,
    /// A 64-bit float, carried as `Float64`.
    Double,
    /// Carried as `Boolean`.
    Boolean,
    /// Nanoseconds since the Unix epoch, carried as `Timestamp(Nanosecond)`
    /// with no time zone.
    DateNanos,
    /// The type of the `null` literal, carried as `Null`.
    Null,
}

impl EsqlType {
    /// The name ES|QL reports for the type.
    pub fn name(self) -> &'static str {
        match self {
            EsqlType::Keyword => "keyword",
            EsqlType::Long => "long",
            EsqlType::Double => "double",
            EsqlType::Boolean => "boolean",
            EsqlType::DateNanos => "date_nanos",
            EsqlType::Null => "null",
        }
    }

    /// The Arrow type a value of this type is carried as.
    pub fn arrow(self) -> DataType {
        match self {
            EsqlType::Keyword => DataType::Utf8,
            EsqlType::Long => DataType::Int64,
            EsqlType::Double => DataType::Float64,
            EsqlType::Boolean => DataType::Boolean,
            EsqlType::DateNanos => DataType::Timestamp(TimeUnit::Nanosecond, None),
            EsqlType::Null => DataType::Null,
        }
    }

    pub(crate) fn is_numeric(self) -> bool {
        matches!(self, EsqlType::Long | EsqlType::Double)
    }
}

impl std::fmt::Display for EsqlType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A column of an ES|QL result: its name and its type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EsqlColumn {
    pub name: String,
    pub ty: EsqlType,
}
