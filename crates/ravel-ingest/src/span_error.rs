//! Errors returned to strict-mode waiters of the span pipeline, the span-side
//! counterpart of [`crate::LogWriteError`].

/// Failure classification for a single span shard's contribution to a write.
///
/// Variants carry only owned strings (not the underlying store/publish
/// errors) so a single flush outcome can be cloned out to every strict-mode
/// waiter of that flush, exactly as [`crate::LogWriteError`] does.
///
/// There is deliberately no identity-collision variant here. The log pipeline
/// has [`crate::LogWriteError::StreamIdCollision`] because a `stream_id` is a
/// hash of resource+scope that two different inputs could collide on. A span
/// carries its `trace_id` and `span_id` verbatim from the sender (ADR-0041);
/// nothing is derived, so there is no derived identity to collide.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SpanWriteError {
    /// The span shard actor's channel is closed: the router observed either
    /// its send half or a strict-mode ack fail because the actor task is
    /// gone. Retryable at the client, but spans routed to a dead shard keep
    /// failing until the process is restarted.
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
    /// Building the RSPAN object failed (a deterministic input problem);
    /// retrying identical input will fail again.
    #[error("segment build failed: {0}")]
    SegmentBuild(String),
    /// The router's cached provisioning-record view for this tenant is older
    /// than the refresh interval `C`, so it fails the flush closed rather than
    /// route on a view that could have missed a shard-generation activation
    /// (ADR-0052 section 3). Retryable once the background refresher re-reads.
    #[error("provisioning view stale: refusing to route on a view older than the refresh interval")]
    StaleProvisioningView,
    /// The process-wide ingest buffer byte budget is at its ceiling
    /// (`--max-ingest-buffer-bytes`, ADR-0069 decision 1): charging this
    /// request's estimated buffered bytes would exceed it, so it is shed before
    /// any buffering, with no shard touched and no commit token issued.
    /// Retryable: a buffer slot frees as soon as any in-flight flush completes.
    /// The gateway maps it to HTTP 429 / gRPC `RESOURCE_EXHAUSTED`.
    #[error("ingest buffer byte budget reached")]
    BufferBudgetExceeded,
}

impl SpanWriteError {
    /// Whether a client may reasonably retry the whole write after this
    /// error. `SegmentBuild` is excluded: it reflects a problem with the input
    /// itself, not a transient condition.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            SpanWriteError::ShardUnavailable
                | SpanWriteError::AckTimeout
                | SpanWriteError::Abandoned(_)
                | SpanWriteError::StaleProvisioningView
                | SpanWriteError::BufferBudgetExceeded
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_build_is_not_retryable() {
        assert!(!SpanWriteError::SegmentBuild("bad input".into()).is_retryable());
    }

    #[test]
    fn abandoned_and_timeout_and_unavailable_are_retryable() {
        assert!(SpanWriteError::Abandoned("x".into()).is_retryable());
        assert!(SpanWriteError::AckTimeout.is_retryable());
        assert!(SpanWriteError::ShardUnavailable.is_retryable());
    }
}
