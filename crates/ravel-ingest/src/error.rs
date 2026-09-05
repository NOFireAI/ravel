//! Errors returned to strict-mode waiters and to [`crate::IngestRouter::write`].

use ravel_types::CommitToken;

/// Failure classification for a single shard's contribution to a write.
///
/// Variants carry only owned strings (not the underlying store/publish
/// errors) so a single flush outcome can be cloned out to every strict-mode
/// waiter of that flush.
#[derive(Debug, Clone, thiserror::Error)]
pub enum WriteError {
    /// The shard actor's channel is closed: the router observed either its
    /// send half or a strict-mode ack fail because the actor task is gone.
    /// This means the router is shutting down, or an individual shard actor
    /// task ended without shutdown (e.g. it panicked mid-flush); the latter
    /// leaves the router serving the surviving shards while every series
    /// hashing to the dead shard fails here. The router counts each distinct
    /// shard death (`IngestMetricsSnapshot::shard_deaths`) so the degraded
    /// state is observable rather than silent. Retryable at the
    /// client, but points routed to a dead shard keep failing until the
    /// process is restarted.
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
    /// Two points in one shard buffer carried the same `series_id` but
    /// distinct canonical label sets: a series-id collision (ADR-0005). The
    /// batch is rejected fail-loud rather than silently merging the losing
    /// label set into the winning one. Not retryable: identical input
    /// reproduces the same collision.
    #[error("series id collision: {0}")]
    SeriesIdCollision(String),
    /// Two points in one shard buffer carried the same `series_id` but one
    /// was scalar and the other was a histogram: a series is one value kind
    /// for its whole life in a segment (`value_kind`). The batch is rejected fail-loud rather than silently
    /// picking one kind and dropping the other's points. Not retryable:
    /// identical input reproduces the same mismatch.
    #[error("series value-kind mismatch: {0}")]
    SeriesValueKindMismatch(String),
    /// The router's cached provisioning-record view for this tenant is older
    /// than the refresh interval `C`, so it fails the flush closed rather than
    /// route on a view that could have missed a shard-generation activation
    /// (ADR-0052 section 3). Retryable: once the background refresher re-reads
    /// the record, the tenant's next write routes on a current view.
    #[error("provisioning view stale: refusing to route on a view older than the refresh interval")]
    StaleProvisioningView,
    /// The process-wide ingest buffer byte budget is at its ceiling
    /// (`--max-ingest-buffer-bytes`, ADR-0069 decision 1): charging this
    /// request's estimated buffered bytes would exceed it, so the request is
    /// shed before any buffering, with no shard touched and no commit token
    /// issued. Retryable: a buffer slot frees as soon as any in-flight flush
    /// completes. The gateway maps this to HTTP 429 with `Retry-After` / gRPC
    /// `RESOURCE_EXHAUSTED`, not the 503 the other write failures take.
    #[error("ingest buffer byte budget reached")]
    BufferBudgetExceeded,
    /// A multi-shard Strict write in which at least one shard failed *after*
    /// one or more sibling shards had already acked their commit durably in
    /// the same [`IngestRouter::write`](crate::IngestRouter::write) call
    /// (issue #1130), the metrics-pipeline counterpart of
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
    /// panicked actor): an unresolved ack is not a durable write and reporting
    /// it as one would be worse than the bug this closes.
    ///
    /// This is an information-carrying wrapper, not a new failure mode: it is
    /// exactly as retryable as its `inner` cause ([`Self::is_retryable`]), and
    /// a caller that ignores [`Self::durable_tokens`] behaves exactly as it did
    /// before (the write is still reported as a failure).
    #[error("{inner}")]
    PartialWrite {
        inner: Box<WriteError>,
        durable: Vec<CommitToken>,
    },
}

impl WriteError {
    /// Whether a client may reasonably retry the whole write after this
    /// error. `SegmentBuild` is excluded: it reflects a problem with the
    /// input itself, not a transient condition. A [`Self::PartialWrite`] is
    /// exactly as retryable as its underlying cause: it is the same failure,
    /// carrying recovered sibling tokens.
    pub fn is_retryable(&self) -> bool {
        match self {
            WriteError::ShardUnavailable
            | WriteError::AckTimeout
            | WriteError::Abandoned(_)
            | WriteError::StaleProvisioningView
            | WriteError::BufferBudgetExceeded => true,
            WriteError::SegmentBuild(_)
            | WriteError::SeriesIdCollision(_)
            | WriteError::SeriesValueKindMismatch(_) => false,
            WriteError::PartialWrite { inner, .. } => inner.is_retryable(),
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
            WriteError::PartialWrite { durable, .. } => durable,
            _ => &[],
        }
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
    fn series_id_collision_is_not_retryable() {
        assert!(!WriteError::SeriesIdCollision("collision".into()).is_retryable());
    }

    #[test]
    fn series_value_kind_mismatch_is_not_retryable() {
        assert!(!WriteError::SeriesValueKindMismatch("mismatch".into()).is_retryable());
    }

    #[test]
    fn abandoned_and_timeout_and_unavailable_are_retryable() {
        assert!(WriteError::Abandoned("x".into()).is_retryable());
        assert!(WriteError::AckTimeout.is_retryable());
        assert!(WriteError::ShardUnavailable.is_retryable());
    }

    #[test]
    fn non_partial_variants_carry_no_durable_tokens() {
        assert!(WriteError::ShardUnavailable.durable_tokens().is_empty());
        assert!(WriteError::AckTimeout.durable_tokens().is_empty());
        assert!(
            WriteError::SegmentBuild("x".into())
                .durable_tokens()
                .is_empty()
        );
    }

    #[test]
    fn partial_write_carries_tokens_and_delegates_retryability_and_display() {
        let token = CommitToken {
            shard: 2,
            writer_id: uuid::Uuid::nil(),
            epoch: 0,
            seq: 5,
            ingest_hour_bucket: 0,
        };
        // A retryable cause: the wrapper is retryable and surfaces the cause's
        // own Display, not a new string.
        let retryable = WriteError::PartialWrite {
            inner: Box::new(WriteError::ShardUnavailable),
            durable: vec![token.clone()],
        };
        assert!(retryable.is_retryable());
        assert_eq!(retryable.durable_tokens(), std::slice::from_ref(&token));
        assert_eq!(retryable.to_string(), "shard actor unavailable");

        // A non-retryable cause makes the wrapper non-retryable too.
        let not_retryable = WriteError::PartialWrite {
            inner: Box::new(WriteError::SegmentBuild("bad".into())),
            durable: vec![token],
        };
        assert!(!not_retryable.is_retryable());
    }

    /// Test debt (formal/tla/TRACEABILITY.md, ingest row 2, `AdmitFailClosed`
    /// fence / `StaleProvisioningView`): a writer whose cached provisioning
    /// view has gone stale past the refresh interval `C`, and whose re-read
    /// of the provisioning record cannot complete, and whose grace-extend
    /// horizon has already been crossed, gets exactly
    /// `WriteError::StaleProvisioningView` and durably writes nothing --
    /// no data object and no commit record appear in the store.
    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn stale_provisioning_view_fails_closed_past_the_staleness_fence() {
        use std::future::Future;
        use std::pin::Pin;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicI64, Ordering};
        use std::time::Duration;

        use ravel_object_store::fault::{
            FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
        };
        use ravel_object_store::memory::MemoryStore;
        use ravel_object_store::{ObjectStoreBackend, list_all};
        use ravel_otlp::NormalizedPoint;
        use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantId};

        use crate::clock::Clock;
        use crate::config::IngestConfig;
        use crate::router::{IngestRouter, WriteMode};

        const NS_PER_HOUR: i64 = 3_600_000_000_000;

        struct FrozenClock(AtomicI64);

        impl Clock for FrozenClock {
            fn now_ns(&self) -> i64 {
                self.0.load(Ordering::SeqCst)
            }

            fn sleep(&self, _dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
                Box::pin(async {})
            }
        }

        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Transient("simulated sustained store latency".into()),
            )
            .with_occurrence(Occurrence::Always),
        );
        let store: Arc<dyn ObjectStoreBackend> =
            Arc::new(FaultStore::new(MemoryStore::new(), plan));

        let t0 = 10 * NS_PER_HOUR;
        let clock = Arc::new(FrozenClock(AtomicI64::new(t0)));
        let router = IngestRouter::new(
            IngestConfig {
                shard_count: 4,
                ..IngestConfig::default()
            },
            Arc::clone(&store),
            Signal::Metrics,
            clock.clone(),
        );

        let tenant = TenantId::new("acme");

        // Seed a fresh cached view the ordinary way (this call never touches
        // the store), then move the clock past both the refresh interval `C`
        // and the grace-extend horizon (`min_lead_hours(C)` = 2 hours past t0
        // for the default `C`), so the upcoming write's re-read is forced and
        // its grace fallback is refused.
        router.refresh_generations(tenant.hash(), vec![], t0);
        clock.0.store(t0 + 3 * NS_PER_HOUR, Ordering::SeqCst);

        let labels = LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: "cpu_usage".to_string(),
        }])
        .expect("valid labels");
        let series_id = SeriesId::compute(&tenant, "cpu_usage", &labels).expect("series id");
        let point = NormalizedPoint {
            series_id,
            labels: Arc::new(labels),
            sample: Sample {
                ts_ns: t0,
                value: 1.0,
            },
            is_monotonic_sum: false,
        };

        let result = router
            .write(
                tenant.clone(),
                vec![point],
                WriteMode::Buffered,
                Duration::from_secs(5),
            )
            .await;

        match result {
            Err(WriteError::StaleProvisioningView) => {}
            Ok(_) => panic!("past the staleness fence, must fail closed, not route"),
            Err(other) => panic!("past the staleness fence, wrong error: {other:?}"),
        }
        assert_eq!(
            router.metrics().snapshot().stale_provisioning_flushes,
            1,
            "the fail-closed counter fires exactly once"
        );

        let objects = list_all(store.as_ref(), "t/")
            .await
            .expect("list must succeed: only Get is faulted");
        assert!(
            objects.is_empty(),
            "a write that fails closed on a stale provisioning view must not \
             durably write anything, but the store holds {objects:?}"
        );
    }
}
