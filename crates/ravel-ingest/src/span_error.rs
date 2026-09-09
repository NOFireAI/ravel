//! Errors returned to strict-mode waiters of the span pipeline, the span-side
//! counterpart of [`crate::LogWriteError`].

use ravel_types::CommitToken;

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
    /// A multi-shard Strict write in which at least one shard failed *after*
    /// one or more sibling shards had already acked their commit durably in
    /// the same [`SpanIngestRouter::write`](crate::SpanIngestRouter::write)
    /// call (issue #1130), the span-pipeline counterpart of
    /// [`crate::LogWriteError::PartialWrite`]. `inner` is the underlying
    /// single-shard classification the write would otherwise have surfaced;
    /// `durable` carries the commit tokens the successful siblings actually
    /// acked, in shard order, so that durable data is reportable instead of
    /// being silently discarded by the error return.
    ///
    /// The router constructs this only when `durable` is non-empty: a failure
    /// that touched no durably-committed sibling still surfaces as the bare
    /// underlying variant, so an existing single-shard caller sees no change.
    /// `durable` never includes a shard whose ack failed to resolve (a
    /// panicked actor): an unresolved ack is not a durable write.
    ///
    /// This is an information-carrying wrapper, not a new failure mode: it is
    /// exactly as retryable as its `inner` cause ([`Self::is_retryable`]), and
    /// a caller that ignores [`Self::durable_tokens`] behaves exactly as it did
    /// before (the write is still reported as a failure).
    #[error("{inner}")]
    PartialWrite {
        inner: Box<SpanWriteError>,
        durable: Vec<CommitToken>,
    },
}

impl SpanWriteError {
    /// Whether a client may reasonably retry the whole write after this
    /// error. `SegmentBuild` is excluded: it reflects a problem with the input
    /// itself, not a transient condition. A [`Self::PartialWrite`] is exactly
    /// as retryable as its underlying cause: it is the same failure, carrying
    /// recovered sibling tokens.
    pub fn is_retryable(&self) -> bool {
        match self {
            SpanWriteError::ShardUnavailable
            | SpanWriteError::AckTimeout
            | SpanWriteError::Abandoned(_)
            | SpanWriteError::StaleProvisioningView
            | SpanWriteError::BufferBudgetExceeded => true,
            SpanWriteError::SegmentBuild(_) => false,
            SpanWriteError::PartialWrite { inner, .. } => inner.is_retryable(),
        }
    }

    /// The commit tokens for sibling shards that acked durable in the same
    /// multi-shard `write()` call as this failure (issue #1130). Empty for
    /// every variant except [`Self::PartialWrite`]: a failure that committed
    /// no sibling durably carries no tokens, and neither does a failure whose
    /// ack round never resolved (an [`Self::AckTimeout`], or a
    /// [`Self::ShardUnavailable`] raised at send time before any ack was
    /// awaited). A caller that ignores this loses only information, never
    /// correctness -- the write is still an error.
    pub fn durable_tokens(&self) -> &[CommitToken] {
        match self {
            SpanWriteError::PartialWrite { durable, .. } => durable,
            _ => &[],
        }
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

    #[test]
    fn non_partial_variants_carry_no_durable_tokens() {
        assert!(SpanWriteError::AckTimeout.durable_tokens().is_empty());
        assert!(
            SpanWriteError::Abandoned("x".into())
                .durable_tokens()
                .is_empty()
        );
    }

    #[test]
    fn partial_write_carries_tokens_and_delegates_retryability_and_display() {
        let token = CommitToken {
            shard: 4,
            writer_id: uuid::Uuid::nil(),
            epoch: 0,
            seq: 11,
            ingest_hour_bucket: 0,
        };
        // A retryable cause: the wrapper is retryable and surfaces the cause's
        // own Display, not a new string.
        let retryable = SpanWriteError::PartialWrite {
            inner: Box::new(SpanWriteError::Abandoned("data put exhausted".into())),
            durable: vec![token.clone()],
        };
        assert!(retryable.is_retryable());
        assert_eq!(retryable.durable_tokens(), std::slice::from_ref(&token));
        assert_eq!(retryable.to_string(), "flush abandoned: data put exhausted");

        // A non-retryable cause makes the wrapper non-retryable too.
        let not_retryable = SpanWriteError::PartialWrite {
            inner: Box::new(SpanWriteError::SegmentBuild("bad".into())),
            durable: vec![token],
        };
        assert!(!not_retryable.is_retryable());
    }
}
