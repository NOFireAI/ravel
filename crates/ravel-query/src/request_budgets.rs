//! Per-request budget overrides a caller may attach to one query (ADR-1374
//! decision 3, prerequisite 1).
//!
//! A caller never sets a budget: it lowers one. The server's [`EngineConfig`]
//! is the ceiling, and [`RequestBudgets::clamp`] resolves a caller's wishes
//! against it into the [`EffectiveBudgets`] the query actually runs under. A
//! value above the ceiling resolves to the ceiling, an absent value resolves to
//! the ceiling, and `Unlimited` from a caller cannot lift a bounded ceiling.
//! That direction is the whole point: the agent surface ADR-1374 describes
//! hands untrusted callers a budget knob, and a knob that can only turn one way
//! needs no separate authorization check.
//!
//! `max_store_requests` is the one canonical wire name for the store-request
//! budget (ADR-1374 decision 3). It maps onto the existing
//! [`EngineConfig::max_s3_requests`] ceiling, which keeps its name: the ceiling
//! is an operator-facing knob that has shipped, and renaming it would break
//! deployed configuration to no end.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::config::{ByteLimit, EngineConfig, RequestLimit};

/// The string form of an unbounded limit on the wire, matching the
/// `max_bytes_scanned = "unlimited"` spelling operators already write in
/// `ravel-server`'s limits file.
const UNLIMITED: &str = "unlimited";

/// What one request asks its budgets to be lowered to. Every field is
/// optional; an absent field leaves the server ceiling in place.
///
/// Deserialized from an agent- or client-supplied request body, so the field
/// names here are a wire contract:
/// `request_budget_field_names_are_the_wire_contract` pins them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestBudgets {
    /// Lowers [`EngineConfig::max_bytes_scanned`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes_scanned: Option<ByteLimit>,
    /// Lowers [`EngineConfig::max_s3_requests`]. The wire name is
    /// `max_store_requests`; the ceiling it lowers keeps its shipped name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_store_requests: Option<RequestLimit>,
    /// Lowers [`EngineConfig::max_segments`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_segments: Option<usize>,
}

/// The budgets a query actually runs under: the caller's wishes resolved
/// against the server ceiling. Every field is concrete, so no enforcement site
/// has to reason about an absent value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectiveBudgets {
    pub max_bytes_scanned: ByteLimit,
    pub max_store_requests: RequestLimit,
    pub max_segments: usize,
}

impl RequestBudgets {
    /// Resolve these budgets against the server `ceiling`. Lowering only: for
    /// every field the result is the more restrictive of the caller's value and
    /// the ceiling, and an absent caller value is the ceiling.
    pub fn clamp(&self, ceiling: &EngineConfig) -> EffectiveBudgets {
        EffectiveBudgets {
            max_bytes_scanned: lower_bytes(self.max_bytes_scanned, ceiling.max_bytes_scanned),
            max_store_requests: lower_requests(self.max_store_requests, ceiling.max_s3_requests),
            max_segments: match self.max_segments {
                Some(requested) => requested.min(ceiling.max_segments),
                None => ceiling.max_segments,
            },
        }
    }

    /// [`Self::clamp`] for an optional caller: `None` is exactly the ceiling,
    /// which is what every pre-ADR-1374 call site runs under.
    pub fn clamp_optional(
        budgets: Option<&RequestBudgets>,
        ceiling: &EngineConfig,
    ) -> EffectiveBudgets {
        match budgets {
            Some(budgets) => budgets.clamp(ceiling),
            None => EffectiveBudgets::from_ceiling(ceiling),
        }
    }
}

impl EffectiveBudgets {
    /// The ceiling itself, with nothing lowered.
    pub fn from_ceiling(ceiling: &EngineConfig) -> Self {
        EffectiveBudgets {
            max_bytes_scanned: ceiling.max_bytes_scanned,
            max_store_requests: ceiling.max_s3_requests,
            max_segments: ceiling.max_segments,
        }
    }

    /// `ceiling` with these three budgets substituted in.
    ///
    /// This is how a lowered budget reaches every enforcement site at once. The
    /// sites read `config.max_bytes_scanned`, `config.max_s3_requests`, and
    /// `config.max_segments`; handing them a config whose three fields are
    /// already the effective values is what makes "the check reads the
    /// effective budget" true structurally rather than by each site
    /// remembering to consult a second value.
    pub fn applied_to(&self, ceiling: &EngineConfig) -> EngineConfig {
        EngineConfig {
            max_bytes_scanned: self.max_bytes_scanned,
            max_s3_requests: self.max_store_requests,
            max_segments: self.max_segments,
            ..*ceiling
        }
    }
}

/// The more restrictive of a caller's byte limit and the ceiling. An absent
/// caller value, and a caller asking for `Unlimited` under a bounded ceiling,
/// both resolve to the ceiling.
fn lower_bytes(requested: Option<ByteLimit>, ceiling: ByteLimit) -> ByteLimit {
    match (requested, ceiling) {
        (None, ceiling) => ceiling,
        (Some(ByteLimit::Unlimited), ceiling) => ceiling,
        (Some(ByteLimit::Bounded(v)), ByteLimit::Unlimited) => ByteLimit::Bounded(v),
        (Some(ByteLimit::Bounded(v)), ByteLimit::Bounded(c)) => ByteLimit::Bounded(v.min(c)),
    }
}

/// [`lower_bytes`] for the store-request budget.
fn lower_requests(requested: Option<RequestLimit>, ceiling: RequestLimit) -> RequestLimit {
    match (requested, ceiling) {
        (None, ceiling) => ceiling,
        (Some(RequestLimit::Unlimited), ceiling) => ceiling,
        (Some(RequestLimit::Bounded(v)), RequestLimit::Unlimited) => RequestLimit::Bounded(v),
        (Some(RequestLimit::Bounded(v)), RequestLimit::Bounded(c)) => {
            RequestLimit::Bounded(v.min(c))
        }
    }
}

/// A limit on the wire: a plain number for a bound, the string `"unlimited"`
/// for none. Shared by [`ByteLimit`] and [`RequestLimit`] so both spell an
/// unbounded budget the same way an operator already spells it in the limits
/// file.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum LimitWire {
    Bounded(u64),
    Unlimited(String),
}

impl Serialize for ByteLimit {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            ByteLimit::Bounded(v) => serializer.serialize_u64(*v),
            ByteLimit::Unlimited => serializer.serialize_str(UNLIMITED),
        }
    }
}

impl<'de> Deserialize<'de> for ByteLimit {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match LimitWire::deserialize(deserializer)? {
            LimitWire::Bounded(v) => Ok(ByteLimit::Bounded(v)),
            LimitWire::Unlimited(s) if s == UNLIMITED => Ok(ByteLimit::Unlimited),
            LimitWire::Unlimited(s) => Err(serde::de::Error::custom(format!(
                "expected a byte count or {UNLIMITED:?}, got {s:?}"
            ))),
        }
    }
}

impl Serialize for RequestLimit {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            RequestLimit::Bounded(v) => serializer.serialize_u64(*v),
            RequestLimit::Unlimited => serializer.serialize_str(UNLIMITED),
        }
    }
}

impl<'de> Deserialize<'de> for RequestLimit {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match LimitWire::deserialize(deserializer)? {
            LimitWire::Bounded(v) => Ok(RequestLimit::Bounded(v)),
            LimitWire::Unlimited(s) if s == UNLIMITED => Ok(RequestLimit::Unlimited),
            LimitWire::Unlimited(s) => Err(serde::de::Error::custom(format!(
                "expected a request count or {UNLIMITED:?}, got {s:?}"
            ))),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// ADR-1374 decision 3 prerequisite 1 names `max_store_requests` as the one
    /// canonical wire name for the store-request budget. The ceiling it lowers
    /// is still spelled `max_s3_requests`, so nothing but this assertion stops
    /// the wire name from drifting back to the internal one.
    #[test]
    fn request_budget_field_names_are_the_wire_contract() {
        let budgets = RequestBudgets {
            max_bytes_scanned: Some(ByteLimit::Bounded(4096)),
            max_store_requests: Some(RequestLimit::Bounded(12)),
            max_segments: Some(7),
        };
        let json = serde_json::to_string(&budgets).expect("serialize");
        assert_eq!(
            json,
            r#"{"max_bytes_scanned":4096,"max_store_requests":12,"max_segments":7}"#
        );

        let parsed: RequestBudgets = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed, budgets);
    }

    /// An unbounded budget is the string form on both sides, so a caller can
    /// spell "no bound of my own" the same way the limits file does. It still
    /// cannot lift a bounded ceiling; that is
    /// `request_budget_cannot_be_raised_above_server_ceiling`.
    #[test]
    fn unlimited_round_trips_as_the_operator_spelling() {
        let budgets = RequestBudgets {
            max_bytes_scanned: Some(ByteLimit::Unlimited),
            max_store_requests: Some(RequestLimit::Unlimited),
            max_segments: None,
        };
        let json = serde_json::to_string(&budgets).expect("serialize");
        assert_eq!(
            json,
            r#"{"max_bytes_scanned":"unlimited","max_store_requests":"unlimited"}"#
        );
        let parsed: RequestBudgets = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed, budgets);
    }

    /// A limit string that is not `"unlimited"` is a typed deserialization
    /// error, never a silently-unbounded budget.
    #[test]
    fn an_unknown_limit_string_is_rejected() {
        let err = serde_json::from_str::<RequestBudgets>(r#"{"max_bytes_scanned":"none"}"#)
            .expect_err("must reject");
        assert!(
            err.to_string().contains("unlimited"),
            "the error must name the accepted spelling, got {err}"
        );
    }

    /// `applied_to` substitutes exactly the three budget fields and leaves the
    /// rest of the ceiling alone: a lowered budget must not silently reset an
    /// unrelated knob.
    #[test]
    fn applied_to_changes_only_the_three_budget_fields() {
        let ceiling = EngineConfig {
            max_segments: 100,
            max_bytes_scanned: ByteLimit::Bounded(10_000),
            max_s3_requests: RequestLimit::Bounded(500),
            ..EngineConfig::default()
        };
        let lowered = RequestBudgets {
            max_bytes_scanned: Some(ByteLimit::Bounded(10)),
            max_store_requests: Some(RequestLimit::Bounded(5)),
            max_segments: Some(3),
        }
        .clamp(&ceiling)
        .applied_to(&ceiling);

        assert_eq!(lowered.max_bytes_scanned, ByteLimit::Bounded(10));
        assert_eq!(lowered.max_s3_requests, RequestLimit::Bounded(5));
        assert_eq!(lowered.max_segments, 3);
        assert_eq!(
            EngineConfig {
                max_segments: ceiling.max_segments,
                max_bytes_scanned: ceiling.max_bytes_scanned,
                max_s3_requests: ceiling.max_s3_requests,
                ..lowered
            },
            ceiling,
            "every field outside the three budgets must be the ceiling's"
        );
    }
}
