//! The typed errors every Ravel Agents surface reports (ADR-3331 decision 11).
//!
//! An error carries only safe identifiers: hashes and handle tokens, never a
//! raw agent key, payload or tool input. Each kind has one fixed retry hint.

/// The eleven error kinds of ADR-3331 decision 11.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ErrorKind {
    /// The agent does not exist, is tombstoned, or the handle names an
    /// incarnation that is not the current one.
    AgentGone,
    /// A request id was reused with a different request hash, or one attempt
    /// reported two different results.
    IdempotencyConflict,
    /// The command may or may not have been published; resolve the operation
    /// handle before doing anything else.
    OutcomeUnknown,
    /// The command's epoch, owner or active turn no longer matches HEAD.
    OwnershipLost,
    /// The handler version or digest does not match the one the turn is
    /// pinned to, or the worker has no handler for the input.
    IncompatibleHandler,
    /// A step key was reused with a different tool identity or input hash.
    NonDeterministicReplay,
    /// An effect under the `manual` policy had an ambiguous attempt and needs
    /// reconciliation.
    EffectInDoubt,
    /// The mailbox is at its depth cap.
    MailboxFull,
    /// A logical quota or the HEAD byte cap would be exceeded.
    BudgetExceeded,
    /// The store did not answer.
    StoreUnavailable,
    /// A record carries a format version or representation this binary does
    /// not read.
    UnsupportedFormat,
}

/// What a caller should do after an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RetryHint {
    /// Repeating the same request gives the same error.
    Never,
    /// Retry the same request, with the same request id, after a delay.
    Backoff,
    /// Resolve the operation handle; do not resubmit under a new id.
    ResolveOperation,
    /// An operator must reconcile before anything proceeds.
    Reconcile,
}

impl ErrorKind {
    pub const ALL: [ErrorKind; 11] = [
        ErrorKind::AgentGone,
        ErrorKind::IdempotencyConflict,
        ErrorKind::OutcomeUnknown,
        ErrorKind::OwnershipLost,
        ErrorKind::IncompatibleHandler,
        ErrorKind::NonDeterministicReplay,
        ErrorKind::EffectInDoubt,
        ErrorKind::MailboxFull,
        ErrorKind::BudgetExceeded,
        ErrorKind::StoreUnavailable,
        ErrorKind::UnsupportedFormat,
    ];

    /// Stable snake_case code used on the wire.
    pub fn code(self) -> &'static str {
        match self {
            ErrorKind::AgentGone => "agent_gone",
            ErrorKind::IdempotencyConflict => "idempotency_conflict",
            ErrorKind::OutcomeUnknown => "outcome_unknown",
            ErrorKind::OwnershipLost => "ownership_lost",
            ErrorKind::IncompatibleHandler => "incompatible_handler",
            ErrorKind::NonDeterministicReplay => "non_deterministic_replay",
            ErrorKind::EffectInDoubt => "effect_in_doubt",
            ErrorKind::MailboxFull => "mailbox_full",
            ErrorKind::BudgetExceeded => "budget_exceeded",
            ErrorKind::StoreUnavailable => "store_unavailable",
            ErrorKind::UnsupportedFormat => "unsupported_format",
        }
    }

    pub fn from_code(code: &str) -> Option<ErrorKind> {
        ErrorKind::ALL.into_iter().find(|kind| kind.code() == code)
    }

    pub fn retry(self) -> RetryHint {
        match self {
            ErrorKind::OutcomeUnknown => RetryHint::ResolveOperation,
            ErrorKind::MailboxFull | ErrorKind::StoreUnavailable => RetryHint::Backoff,
            ErrorKind::EffectInDoubt => RetryHint::Reconcile,
            ErrorKind::AgentGone
            | ErrorKind::IdempotencyConflict
            | ErrorKind::OwnershipLost
            | ErrorKind::IncompatibleHandler
            | ErrorKind::NonDeterministicReplay
            | ErrorKind::BudgetExceeded
            | ErrorKind::UnsupportedFormat => RetryHint::Never,
        }
    }
}

impl RetryHint {
    pub fn code(self) -> &'static str {
        match self {
            RetryHint::Never => "never",
            RetryHint::Backoff => "backoff",
            RetryHint::ResolveOperation => "resolve_operation",
            RetryHint::Reconcile => "reconcile",
        }
    }
}

/// The lifetime cap a `BudgetExceeded` or `MailboxFull` refusal hit. A
/// refusal for a per-command bound (a string, the events or intents of one
/// turn, an exhausted counter) names none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum BudgetCap {
    /// `Limits::max_receipts`.
    Receipts,
    /// `Limits::max_index_pages`, or the entries of one index page.
    IndexPages,
    /// `Limits::max_head_bytes`, or the reserve the growth guard keeps under it.
    HeadBytes,
    /// `Limits::max_mailbox_depth`.
    Mailbox,
    /// `Limits::max_timers`.
    Timers,
}

impl BudgetCap {
    pub const ALL: [BudgetCap; 5] = [
        BudgetCap::Receipts,
        BudgetCap::IndexPages,
        BudgetCap::HeadBytes,
        BudgetCap::Mailbox,
        BudgetCap::Timers,
    ];

    /// Stable snake_case code used on the wire and as a metric label.
    pub fn code(self) -> &'static str {
        match self {
            BudgetCap::Receipts => "receipts",
            BudgetCap::IndexPages => "index_pages",
            BudgetCap::HeadBytes => "head_bytes",
            BudgetCap::Mailbox => "mailbox",
            BudgetCap::Timers => "timers",
        }
    }

    pub fn from_code(code: &str) -> Option<BudgetCap> {
        BudgetCap::ALL.into_iter().find(|cap| cap.code() == code)
    }
}

/// Identifiers safe to return to any caller of the tenant: hex hashes and
/// handle tokens. Unset fields are unknown or not relevant to the error.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SafeIds {
    pub agent_id: Option<String>,
    pub incarnation: Option<u64>,
    pub operation: Option<String>,
    pub turn_id: Option<String>,
    pub step_id: Option<String>,
    pub effect_id: Option<String>,
    pub timer_id: Option<String>,
}

/// One typed error: its kind, the safe ids, and a fixed reason string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{}: {reason}", kind.code())]
pub struct AgentError {
    pub kind: ErrorKind,
    // Boxed so every `Result<_, AgentError>` stays small.
    pub ids: Box<SafeIds>,
    pub reason: &'static str,
    /// The lifetime cap the refusal hit, if it was one.
    pub cap: Option<BudgetCap>,
}

impl AgentError {
    pub fn new(kind: ErrorKind, ids: SafeIds, reason: &'static str) -> Self {
        AgentError {
            kind,
            ids: Box::new(ids),
            reason,
            cap: None,
        }
    }

    /// The same error, naming the lifetime cap it hit.
    #[must_use]
    pub fn with_cap(mut self, cap: BudgetCap) -> Self {
        self.cap = Some(cap);
        self
    }

    pub fn retry(&self) -> RetryHint {
        self.kind.retry()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_distinct_snake_case_and_round_trip() {
        let codes: std::collections::BTreeSet<&str> =
            ErrorKind::ALL.iter().map(|k| k.code()).collect();
        assert_eq!(codes.len(), ErrorKind::ALL.len());
        for kind in ErrorKind::ALL {
            let code = kind.code();
            assert!(
                code.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'),
                "{code}"
            );
            assert_eq!(ErrorKind::from_code(code), Some(kind));
        }
        assert_eq!(ErrorKind::from_code("nope"), None);
    }

    #[test]
    fn cap_codes_are_distinct_and_round_trip() {
        let codes: std::collections::BTreeSet<&str> =
            BudgetCap::ALL.iter().map(|c| c.code()).collect();
        assert_eq!(
            codes.into_iter().collect::<Vec<_>>(),
            ["head_bytes", "index_pages", "mailbox", "receipts", "timers"]
        );
        for cap in BudgetCap::ALL {
            assert_eq!(BudgetCap::from_code(cap.code()), Some(cap));
        }
        assert_eq!(BudgetCap::from_code("nope"), None);
    }

    #[test]
    fn every_kind_has_the_retry_hint_its_meaning_needs() {
        assert_eq!(
            ErrorKind::OutcomeUnknown.retry(),
            RetryHint::ResolveOperation
        );
        assert_eq!(ErrorKind::EffectInDoubt.retry(), RetryHint::Reconcile);
        assert_eq!(ErrorKind::MailboxFull.retry(), RetryHint::Backoff);
        assert_eq!(ErrorKind::StoreUnavailable.retry(), RetryHint::Backoff);
        assert_eq!(ErrorKind::IdempotencyConflict.retry(), RetryHint::Never);
    }
}
