//! Errors returned to strict-mode waiters and to [`crate::IngestRouter::write`].

/// Failure classification for a single shard's contribution to a write.
///
/// Variants carry only owned strings (not the underlying store/publish
/// errors) so a single flush outcome can be cloned out to every strict-mode
/// waiter of that flush.
#[derive(Debug, Clone, thiserror::Error)]
pub enum WriteError {
    /// The shard actor's channel is closed (actor task gone). Not retryable:
    /// the router itself is shutting down or has panicked.
    #[error("shard actor unavailable")]
    ShardUnavailable,
    /// A strict-mode ack did not arrive within the caller's `ack_deadline`.
    #[error("timed out waiting for shard ack")]
    AckTimeout,
    /// The flush was abandoned: the data-object or commit-record PUT
    /// exhausted its retry budget, or `max_flush_lifetime` elapsed first.
    /// Per docs/consistency-model.md, nothing in this flush was acknowledged
    /// so retrying the whole write is safe.
    #[error("flush abandoned: {0}")]
    Abandoned(String),
    /// Building the RSEG segment failed (a deterministic input problem, e.g.
    /// oversized batch); retrying identical input will fail again.
    #[error("segment build failed: {0}")]
    SegmentBuild(String),
}

impl WriteError {
    /// Whether a client may reasonably retry the whole write after this
    /// error. `SegmentBuild` is excluded: it reflects a problem with the
    /// input itself, not a transient condition.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            WriteError::ShardUnavailable | WriteError::AckTimeout | WriteError::Abandoned(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_build_is_not_retryable() {
        assert!(!WriteError::SegmentBuild("bad input".into()).is_retryable());
    }

    #[test]
    fn abandoned_and_timeout_and_unavailable_are_retryable() {
        assert!(WriteError::Abandoned("x".into()).is_retryable());
        assert!(WriteError::AckTimeout.is_retryable());
        assert!(WriteError::ShardUnavailable.is_retryable());
    }
}
