//! Errors returned to strict-mode waiters of the log pipeline, the log-side
//! counterpart of [`crate::WriteError`].

/// Failure classification for a single log shard's contribution to a write.
///
/// Variants carry only owned strings (not the underlying store/publish
/// errors) so a single flush outcome can be cloned out to every strict-mode
/// waiter of that flush, exactly as [`crate::WriteError`] does.
#[derive(Debug, Clone, thiserror::Error)]
pub enum LogWriteError {
    /// The log shard actor's channel is closed: the router observed either
    /// its send half or a strict-mode ack fail because the actor task is
    /// gone. Retryable at the client, but records routed to a dead shard
    /// keep failing until the process is restarted.
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
    /// Building the RLOG object failed (a deterministic input problem, e.g.
    /// an oversized batch); retrying identical input will fail again.
    #[error("segment build failed: {0}")]
    SegmentBuild(String),
    /// Two records in one flush's batch carried the same `stream_id` but
    /// disagreed on `stream_attrs` (a stream-id hash collision, or an
    /// upstream bug computing `log_stream_id`/`stream_attrs_bytes`
    /// inconsistently). Detected by `RlogWriter::finish()`
    /// (`LogSegError::InconsistentStreamAttrs`) and mapped to
    /// this variant by the log shard actor's flush step, the same ADR-0005
    /// precedent as [`crate::WriteError::SeriesIdCollision`]: rejected
    /// fail-loud rather than silently attributing one stream's records to
    /// another's identity. Not retryable: identical input reproduces the
    /// same collision.
    #[error("stream id collision: {0}")]
    StreamIdCollision(String),
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

impl LogWriteError {
    /// Whether a client may reasonably retry the whole write after this
    /// error. `SegmentBuild` and `StreamIdCollision` are excluded: both
    /// reflect a problem with the input itself, not a transient condition.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            LogWriteError::ShardUnavailable
                | LogWriteError::AckTimeout
                | LogWriteError::Abandoned(_)
                | LogWriteError::StaleProvisioningView
                | LogWriteError::BufferBudgetExceeded
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_build_is_not_retryable() {
        assert!(!LogWriteError::SegmentBuild("bad input".into()).is_retryable());
    }

    #[test]
    fn stream_id_collision_is_not_retryable() {
        assert!(!LogWriteError::StreamIdCollision("collision".into()).is_retryable());
    }

    #[test]
    fn abandoned_and_timeout_and_unavailable_are_retryable() {
        assert!(LogWriteError::Abandoned("x".into()).is_retryable());
        assert!(LogWriteError::AckTimeout.is_retryable());
        assert!(LogWriteError::ShardUnavailable.is_retryable());
    }
}
