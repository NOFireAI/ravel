//! Operation states and the opaque handles a client holds (ADR-3331
//! decisions 3 and 5).
//!
//! A handle is a versioned token in the style of `CommitToken`: base64url,
//! no padding, over `v1:<kind>:<agent_id>:<incarnation>:<item>`. Every handle
//! carries the incarnation it was minted under, so the kernel refuses a
//! handle from an earlier incarnation as `AgentGone`. A well-formed token of
//! any other version decodes to [`HandleError::UnsupportedVersion`], not to
//! a malformed-token error, so a client can tell an upgrade from corruption.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use crate::head::{EffectStatus, Head, ResultStatus, TurnBlock};
use crate::identity::{AgentId, Id128};

/// ADR-3331 decision 5 operation states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationState {
    Accepted,
    Running,
    Blocked,
    Completed,
    Failed,
    Cancelled,
}

impl OperationState {
    pub fn code(self) -> &'static str {
        match self {
            OperationState::Accepted => "accepted",
            OperationState::Running => "running",
            OperationState::Blocked => "blocked",
            OperationState::Completed => "completed",
            OperationState::Failed => "failed",
            OperationState::Cancelled => "cancelled",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            OperationState::Completed | OperationState::Failed | OperationState::Cancelled
        )
    }
}

/// The state of the input accepted as `accept_seq` in `head`, or `None` when
/// this incarnation never accepted it.
pub fn operation_state(head: &Head, accept_seq: u64) -> Option<OperationState> {
    if let Some(result) = head.results.iter().find(|r| r.accept_seq == accept_seq) {
        return Some(match result.status {
            ResultStatus::Completed => OperationState::Completed,
            ResultStatus::Failed => OperationState::Failed,
            ResultStatus::Cancelled => OperationState::Cancelled,
        });
    }
    if let Some(turn) = head
        .active_turn
        .as_ref()
        .filter(|t| t.input_accept_seq == accept_seq)
    {
        let in_doubt = head
            .effects
            .iter()
            .any(|e| e.turn_id == turn.turn_id && e.status == EffectStatus::InDoubt);
        return Some(if turn.block != TurnBlock::None || in_doubt {
            OperationState::Blocked
        } else {
            OperationState::Running
        });
    }
    head.inbox
        .iter()
        .any(|e| e.accept_seq == accept_seq)
        .then_some(OperationState::Accepted)
}

const HANDLE_VERSION: &str = "v1";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HandleError {
    #[error("malformed handle")]
    Malformed,
    #[error("handle version {0} is not supported by this release")]
    UnsupportedVersion(String),
    #[error("handle is a {found} handle, expected {expected}")]
    WrongKind {
        expected: &'static str,
        found: String,
    },
}

/// Which object a handle names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HandleItem {
    /// An accepted input, by `accept_seq`.
    Operation(u64),
    Step(Id128),
    Effect(Id128),
    Timer(Id128),
}

impl HandleItem {
    fn kind(&self) -> &'static str {
        match self {
            HandleItem::Operation(_) => "op",
            HandleItem::Step(_) => "step",
            HandleItem::Effect(_) => "effect",
            HandleItem::Timer(_) => "timer",
        }
    }

    fn render(&self) -> String {
        match self {
            HandleItem::Operation(seq) => seq.to_string(),
            HandleItem::Step(id) | HandleItem::Effect(id) | HandleItem::Timer(id) => id.to_hex(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Handle {
    pub agent: AgentId,
    pub incarnation: u64,
    pub item: HandleItem,
}

impl Handle {
    pub fn operation(agent: AgentId, incarnation: u64, accept_seq: u64) -> Handle {
        Handle {
            agent,
            incarnation,
            item: HandleItem::Operation(accept_seq),
        }
    }

    pub fn encode(&self) -> String {
        let raw = format!(
            "{HANDLE_VERSION}:{}:{}:{}:{}",
            self.item.kind(),
            self.agent.to_hex(),
            self.incarnation,
            self.item.render()
        );
        URL_SAFE_NO_PAD.encode(raw.as_bytes())
    }

    pub fn decode(token: &str) -> Result<Handle, HandleError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(token.as_bytes())
            .map_err(|_| HandleError::Malformed)?;
        let raw = String::from_utf8(bytes).map_err(|_| HandleError::Malformed)?;
        let mut parts = raw.split(':');
        match parts.next() {
            Some(HANDLE_VERSION) => {}
            Some(prefix) if is_version_prefix(prefix) => {
                return Err(HandleError::UnsupportedVersion(prefix.to_string()));
            }
            _ => return Err(HandleError::Malformed),
        }
        let (Some(kind), Some(agent), Some(incarnation), Some(item), None) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            return Err(HandleError::Malformed);
        };
        let agent = AgentId::from_hex(agent).ok_or(HandleError::Malformed)?;
        let incarnation = parse_decimal(incarnation)?;
        let id = || {
            hex::decode(item)
                .ok()
                .and_then(|b| Id128::from_slice(&b))
                .ok_or(HandleError::Malformed)
        };
        let item = match kind {
            "op" => HandleItem::Operation(parse_decimal(item)?),
            "step" => HandleItem::Step(id()?),
            "effect" => HandleItem::Effect(id()?),
            "timer" => HandleItem::Timer(id()?),
            _ => return Err(HandleError::Malformed),
        };
        Ok(Handle {
            agent,
            incarnation,
            item,
        })
    }

    /// Decode and require one kind, e.g. an operation handle on `/result`.
    pub fn decode_kind(token: &str, expected: &'static str) -> Result<Handle, HandleError> {
        let handle = Handle::decode(token)?;
        if handle.item.kind() != expected {
            return Err(HandleError::WrongKind {
                expected,
                found: handle.item.kind().to_string(),
            });
        }
        Ok(handle)
    }
}

// Canonical decimal only, so one handle has exactly one token.
fn parse_decimal(s: &str) -> Result<u64, HandleError> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) || (s.len() > 1 && s.starts_with('0'))
    {
        return Err(HandleError::Malformed);
    }
    s.parse().map_err(|_| HandleError::Malformed)
}

fn is_version_prefix(s: &str) -> bool {
    match s.strip_prefix('v') {
        Some(rest) => !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit()),
        None => false,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn raw(text: &str) -> String {
        URL_SAFE_NO_PAD.encode(text.as_bytes())
    }

    #[test]
    fn unknown_version_is_typed_not_malformed() {
        let agent = "00".repeat(16);
        assert_eq!(
            Handle::decode(&raw(&format!("v2:op:{agent}:1:1"))),
            Err(HandleError::UnsupportedVersion("v2".into()))
        );
        assert_eq!(
            Handle::decode(&raw(&format!("x1:op:{agent}:1:1"))),
            Err(HandleError::Malformed)
        );
        assert_eq!(Handle::decode("!!!"), Err(HandleError::Malformed));
    }

    #[test]
    fn non_canonical_numbers_and_extra_parts_are_malformed() {
        let agent = "00".repeat(16);
        for text in [
            format!("v1:op:{agent}:01:1"),
            format!("v1:op:{agent}:1:+1"),
            format!("v1:op:{agent}:1:1:extra"),
            format!("v1:op:{agent}:1"),
            format!("v1:nope:{agent}:1:1"),
        ] {
            assert_eq!(
                Handle::decode(&raw(&text)),
                Err(HandleError::Malformed),
                "{text}"
            );
        }
    }

    #[test]
    fn decode_kind_refuses_another_kind() {
        let token = Handle {
            agent: AgentId([1; 16]),
            incarnation: 2,
            item: HandleItem::Timer(Id128([3; 16])),
        }
        .encode();
        assert_eq!(
            Handle::decode_kind(&token, "op"),
            Err(HandleError::WrongKind {
                expected: "op",
                found: "timer".into()
            })
        );
    }

    #[test]
    fn uppercase_hex_is_malformed() {
        let agent = "ab".repeat(16);
        let id = "cd".repeat(16);
        for (kind, agent, item) in [
            ("effect", agent.to_uppercase(), id.clone()),
            ("effect", agent.clone(), id.to_uppercase()),
            ("step", agent.clone(), "Cd".repeat(16)),
            ("op", agent.to_uppercase(), "1".to_string()),
        ] {
            let text = format!("v1:{kind}:{agent}:1:{item}");
            assert_eq!(
                Handle::decode(&raw(&text)),
                Err(HandleError::Malformed),
                "{text}"
            );
        }
        assert!(Handle::decode(&raw(&format!("v1:effect:{agent}:1:{id}"))).is_ok());
    }

    fn item() -> impl Strategy<Value = HandleItem> {
        prop_oneof![
            any::<u64>().prop_map(HandleItem::Operation),
            any::<[u8; 16]>().prop_map(|b| HandleItem::Step(Id128(b))),
            any::<[u8; 16]>().prop_map(|b| HandleItem::Effect(Id128(b))),
            any::<[u8; 16]>().prop_map(|b| HandleItem::Timer(Id128(b))),
        ]
    }

    proptest! {
        #[test]
        fn handles_round_trip(agent in any::<[u8; 16]>(), incarnation in any::<u64>(), item in item()) {
            let handle = Handle { agent: AgentId(agent), incarnation, item };
            prop_assert_eq!(Handle::decode(&handle.encode()), Ok(handle));
        }

        // One handle, one token: any token that decodes is the one its
        // handle encodes to, whatever case its hex digits were written in.
        #[test]
        fn a_decoded_token_is_the_canonical_one(
            agent in any::<[u8; 16]>(),
            incarnation in any::<u64>(),
            item in item(),
            upper in any::<prop::sample::Index>(),
        ) {
            let handle = Handle { agent: AgentId(agent), incarnation, item };
            let canonical = handle.encode();
            let text = String::from_utf8(URL_SAFE_NO_PAD.decode(&canonical).expect("base64"))
                .expect("utf8");
            let at = upper.index(text.len());
            let mut variant = text.clone();
            variant.replace_range(at..=at, &text[at..=at].to_uppercase());
            let token = raw(&variant);
            match Handle::decode(&token) {
                Ok(decoded) => {
                    prop_assert_eq!(&decoded, &handle);
                    prop_assert_eq!(decoded.encode(), token);
                }
                Err(error) => prop_assert_eq!(error, HandleError::Malformed),
            }
        }

        #[test]
        fn arbitrary_tokens_never_panic(token in "\\PC{0,80}") {
            let _ = Handle::decode(&token);
        }
    }
}
