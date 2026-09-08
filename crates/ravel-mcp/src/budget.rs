//! The D6 effective-budget clamp (ADR-1374).
//!
//! Adds the two MCP-layer knobs ADR-1374 D6 defines on top of
//! [`ravel_query::RequestBudgets`] (`max_bytes_scanned`, `max_store_requests`,
//! `max_segments`): `max_rows` and `max_response_bytes`, plus a per-request
//! `deadline`. Every knob follows the same lowering-only contract
//! `RequestBudgets::clamp` already established: a caller value above its
//! ceiling resolves to the ceiling, and an absent value resolves to the
//! default (which is itself never above the ceiling). The one direction that
//! is NOT a ceiling clamp is `max_response_bytes`'s floor: a caller value
//! below 256 KiB is raised to the floor, never lowered further, because
//! [`crate::envelope::Envelope::fit`]'s first-row guarantee cannot be honored
//! under a smaller cap.

use std::time::Duration;

use ravel_query::{EffectiveBudgets, EngineConfig, RequestBudgets};

/// Default `max_rows` when a caller supplies none.
pub const DEFAULT_MAX_ROWS: u32 = 200;

/// The highest `max_rows` a caller may request; a larger request clamps down
/// to this.
pub const MAX_ROWS_CEILING: u32 = 5_000;

/// Default `max_response_bytes` when a caller supplies none. 512 KiB.
pub const DEFAULT_MAX_RESPONSE_BYTES: u64 = 512 * 1024;

/// The smallest `max_response_bytes` a caller may request; a smaller request
/// is raised to this floor. 256 KiB, matching
/// [`crate::envelope::Envelope::fit`]'s own floor.
pub const MAX_RESPONSE_BYTES_FLOOR: u64 = 256 * 1024;

/// What one MCP tool call asks its budgets to be. Every field is optional;
/// an absent field resolves to the default (`max_rows`, `max_response_bytes`,
/// `deadline`) or the server ceiling (`query`'s three fields, via
/// [`RequestBudgets::clamp`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct McpRequestBudgets {
    pub max_rows: Option<u32>,
    pub max_response_bytes: Option<u64>,
    pub deadline_ms: Option<u64>,
    pub query: RequestBudgets,
}

/// The budgets one MCP tool call actually runs under: the caller's wishes
/// resolved against the server ceiling and the D6 defaults. Every field is
/// concrete, and this is exactly what the envelope's `budget` block reports
/// (ADR-1374 D4): the caller never has to infer what was applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpEffectiveBudgets {
    pub max_rows: u32,
    pub max_response_bytes: u64,
    pub deadline: Duration,
    pub query: EffectiveBudgets,
}

impl McpRequestBudgets {
    /// Resolve these budgets against the server `ceiling`. `max_rows` and the
    /// three `query` fields are ceiling clamps in the
    /// [`ravel_query::RequestBudgets::clamp`] sense (a value above the
    /// ceiling, or an absent value, resolves to the ceiling/default, whichever
    /// is smaller); `max_response_bytes` is instead a floor (a value below
    /// 256 KiB is raised, never lowered); `deadline` is a ceiling clamp
    /// against [`EngineConfig::deadline`].
    pub fn clamp(&self, ceiling: &EngineConfig) -> McpEffectiveBudgets {
        let max_rows = self
            .max_rows
            .unwrap_or(DEFAULT_MAX_ROWS)
            .min(MAX_ROWS_CEILING);
        let max_response_bytes = self
            .max_response_bytes
            .unwrap_or(DEFAULT_MAX_RESPONSE_BYTES)
            .max(MAX_RESPONSE_BYTES_FLOOR);
        let deadline = match self.deadline_ms {
            Some(ms) => Duration::from_millis(ms).min(ceiling.deadline),
            None => ceiling.deadline,
        };
        McpEffectiveBudgets {
            max_rows,
            max_response_bytes,
            deadline,
            query: self.query.clamp(ceiling),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// A caller asking for more rows than [`MAX_ROWS_CEILING`] is clamped
    /// down to the ceiling, never granted the larger request -- the same
    /// lowering-only contract `RequestBudgets::clamp` already enforces for
    /// the three query-engine budgets.
    #[test]
    fn caller_value_above_ceiling_is_clamped_to_ceiling() {
        let ceiling = EngineConfig::default();
        let requested = McpRequestBudgets {
            max_rows: Some(MAX_ROWS_CEILING + 4_800),
            ..McpRequestBudgets::default()
        };
        let effective = requested.clamp(&ceiling);
        assert_eq!(effective.max_rows, MAX_ROWS_CEILING);
    }
}
