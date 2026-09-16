//! The D6 effective-budget clamp (ADR-1374).
//!
//! Adds the two MCP-layer knobs ADR-1374 D6 defines on top of
//! [`ravel_query::RequestBudgets`] (`max_bytes_scanned`, `max_store_requests`,
//! `max_segments`): `max_rows` and `max_response_bytes`, plus a per-request
//! `deadline`. Every knob follows the same lowering-only contract
//! `RequestBudgets::clamp` already established: a caller value above its
//! ceiling resolves to the ceiling, and an absent value resolves to the
//! default (which is itself never above the ceiling).
//!
//! # The floor is applied in one place
//!
//! `max_response_bytes` has a floor as well as a ceiling, and the floor is
//! the one direction that is not a clamp down. It is applied by
//! [`crate::envelope::Envelope::fit`] and nowhere else, which is also where
//! `presentation.floor_applied` is computed. Raising it here too would make
//! `floor_applied` unobservable: `fit` would only ever see a value already at
//! or above the floor, report `false`, and the envelope would claim the
//! caller's sub-floor request was honored. This module clamps down only.

use std::time::Duration;

use ravel_query::{EffectiveBudgets, EngineConfig, RequestBudgets};

/// Default `max_rows` when a caller supplies none.
pub const DEFAULT_MAX_ROWS: u32 = 200;

/// The highest `max_rows` a caller may request; a larger request clamps down
/// to this.
pub const MAX_ROWS_CEILING: u32 = 5_000;

/// Default `max_response_bytes` when a caller supplies none. 512 KiB.
pub const DEFAULT_MAX_RESPONSE_BYTES: u64 = 512 * 1024;

/// The smallest `max_response_bytes` in force for any call; a smaller
/// request is raised to this floor by [`crate::envelope::Envelope::fit`],
/// which is the only place the floor is applied. 256 KiB.
pub const MAX_RESPONSE_BYTES_FLOOR: u64 = 256 * 1024;

/// The largest `max_response_bytes` a caller may request by default; a
/// larger request clamps down to this. 4 MiB.
///
/// Without a ceiling, `max_response_bytes` is the one budget a caller could
/// raise without limit, and it is the one that decides how much the server
/// serializes and holds in memory per in-flight call. An operator lowers it
/// through [`McpBudgetConfig`]; nothing raises it above what the adapter
/// configures.
pub const MAX_RESPONSE_BYTES_CEILING: u64 = 4 * 1024 * 1024;

/// The MCP-layer budget ceilings an adapter configures, alongside the
/// query-engine ceilings it passes as [`EngineConfig`].
///
/// Only `max_response_bytes` is configurable here: `max_rows`'s ceiling and
/// the response-byte floor are properties of the envelope's own algorithm
/// (the per-cell budget and the first-row guarantee), not deployment knobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpBudgetConfig {
    /// Ceiling on `max_response_bytes`, defaulting to
    /// [`MAX_RESPONSE_BYTES_CEILING`].
    pub max_response_bytes_ceiling: u64,
}

impl Default for McpBudgetConfig {
    fn default() -> Self {
        McpBudgetConfig {
            max_response_bytes_ceiling: MAX_RESPONSE_BYTES_CEILING,
        }
    }
}

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
    /// Resolve these budgets against the server `ceiling` and the MCP-layer
    /// `config`. Every field is a clamp down in the
    /// [`ravel_query::RequestBudgets::clamp`] sense: a value above its
    /// ceiling, or an absent value, resolves to the ceiling or the default,
    /// whichever is smaller. `max_rows` clamps to [`MAX_ROWS_CEILING`],
    /// `max_response_bytes` to [`McpBudgetConfig::max_response_bytes_ceiling`],
    /// `deadline` to [`EngineConfig::deadline`], and the three `query` fields
    /// through `RequestBudgets::clamp` itself.
    ///
    /// Nothing here raises a value. The response-byte floor belongs to
    /// [`crate::envelope::Envelope::fit`] alone (see this module's docs).
    pub fn clamp(&self, ceiling: &EngineConfig, config: &McpBudgetConfig) -> McpEffectiveBudgets {
        let max_rows = self
            .max_rows
            .unwrap_or(DEFAULT_MAX_ROWS)
            .min(MAX_ROWS_CEILING);
        let max_response_bytes = self
            .max_response_bytes
            .unwrap_or(DEFAULT_MAX_RESPONSE_BYTES)
            .min(config.max_response_bytes_ceiling);
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
        let effective = requested.clamp(&ceiling, &McpBudgetConfig::default());
        assert_eq!(effective.max_rows, MAX_ROWS_CEILING);
    }

    /// `max_response_bytes` is the one budget that decides how much the
    /// server serializes per in-flight call, so it clamps down like every
    /// other: a caller asking for 64 MiB gets the 4 MiB ceiling, and an
    /// operator that lowers the ceiling is honored over the default.
    #[test]
    fn max_response_bytes_above_ceiling_is_clamped() {
        let ceiling = EngineConfig::default();
        let requested = McpRequestBudgets {
            max_response_bytes: Some(64 * 1024 * 1024),
            ..McpRequestBudgets::default()
        };

        let effective = requested.clamp(&ceiling, &McpBudgetConfig::default());
        assert_eq!(effective.max_response_bytes, MAX_RESPONSE_BYTES_CEILING);
        assert_eq!(effective.max_response_bytes, 4 * 1024 * 1024);

        let lowered = McpBudgetConfig {
            max_response_bytes_ceiling: 1024 * 1024,
        };
        let effective = requested.clamp(&ceiling, &lowered);
        assert_eq!(effective.max_response_bytes, 1024 * 1024);
    }

    /// A caller's deadline clamps down to the engine's, and one under it is
    /// honored as asked.
    #[test]
    fn deadline_above_ceiling_is_clamped() {
        let ceiling = EngineConfig::default();
        let engine_deadline_ms =
            u64::try_from(ceiling.deadline.as_millis()).expect("engine deadline fits u64");

        let requested = McpRequestBudgets {
            deadline_ms: Some(engine_deadline_ms + 60_000),
            ..McpRequestBudgets::default()
        };
        let effective = requested.clamp(&ceiling, &McpBudgetConfig::default());
        assert_eq!(effective.deadline, ceiling.deadline);

        let requested = McpRequestBudgets {
            deadline_ms: Some(engine_deadline_ms / 2),
            ..McpRequestBudgets::default()
        };
        let effective = requested.clamp(&ceiling, &McpBudgetConfig::default());
        assert_eq!(
            effective.deadline,
            Duration::from_millis(engine_deadline_ms / 2)
        );
    }

    /// An absent field resolves to its D6 default, not to its ceiling and not
    /// to zero. The response-byte default is deliberately the 512 KiB above
    /// the floor, and `clamp` leaves it there rather than raising or lowering
    /// it.
    #[test]
    fn none_yields_the_defaults() {
        let ceiling = EngineConfig::default();
        let effective = McpRequestBudgets::default().clamp(&ceiling, &McpBudgetConfig::default());

        assert_eq!(effective.max_rows, DEFAULT_MAX_ROWS);
        assert_eq!(effective.max_rows, 200);
        assert_eq!(effective.max_response_bytes, DEFAULT_MAX_RESPONSE_BYTES);
        assert_eq!(effective.max_response_bytes, 512 * 1024);
        assert_eq!(effective.deadline, ceiling.deadline);
        assert_eq!(effective.query, RequestBudgets::default().clamp(&ceiling));
    }

    /// The floor is not applied here: a sub-floor request passes through
    /// unchanged so that `Envelope::fit` is the one place that raises it and
    /// can report `floor_applied` truthfully.
    #[test]
    fn sub_floor_request_is_left_for_the_envelope_to_floor() {
        let ceiling = EngineConfig::default();
        let requested = McpRequestBudgets {
            max_response_bytes: Some(1_024),
            ..McpRequestBudgets::default()
        };

        let effective = requested.clamp(&ceiling, &McpBudgetConfig::default());
        assert_eq!(effective.max_response_bytes, 1_024);

        let fitted = crate::envelope::Envelope::default().fit(effective.max_response_bytes);
        assert!(fitted.presentation.floor_applied);
        assert_eq!(
            fitted.presentation.effective_max_response_bytes,
            MAX_RESPONSE_BYTES_FLOOR
        );
    }
}
