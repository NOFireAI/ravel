//! `ravel-cli load --no-declared-stat-aggregates` (ADR-4628 decision 8,
//! question 6b): whether the declared-column stamps a logs load writes carry
//! the sum and the zero count.

/// Whether the commit records a logs load writes carry ADR-4628's stamp
/// fields 6 to 8, the router's
/// [`ravel_ingest::IngestConfig::emit_declared_stat_aggregates`]. Min, max
/// and null count are written either way.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DeclaredStatAggregates {
    /// Write the sum and the zero count. The default.
    #[default]
    Emit,
    /// Withhold them (`--no-declared-stat-aggregates`).
    Withhold,
}

impl DeclaredStatAggregates {
    /// The value for the clap switch, which reads `true` unless
    /// `--no-declared-stat-aggregates` is given.
    pub fn from_emit(emit: bool) -> Self {
        if emit { Self::Emit } else { Self::Withhold }
    }

    /// The router setting this value selects.
    pub fn emits(self) -> bool {
        self == Self::Emit
    }
}
