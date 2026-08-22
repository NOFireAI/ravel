//! Feature-gated per-stage timing seam for the logs ingest pipeline (ADR-0104
//! decision 1). Compiled only under the `stage-timing` cargo feature, which is
//! off by default, so the shipping actor is byte-for-byte what it is today and
//! the `clock.rs` prohibition on reading time outside the injected `Clock` is
//! not weakened for any build that ships.
//!
//! Stage boundaries read `Instant::now()`, never the injected [`crate::Clock`].
//! ADR-0104 decision 1 rejects the `Clock` explicitly: `Clock::now_ns()` is
//! wall-clock, so a clock step could make a stage duration negative or jumped;
//! and a deterministic test clock returns pinned values, so every stage
//! duration measured through it would be zero under exactly the tests that
//! verify the instrumentation. `Instant` is monotonic and independent of the
//! injected clock, so a pinned test clock does not zero these measurements.
//!
//! The accumulated values are read ONLY by the bench reporter (next wave). No
//! control-flow, flush trigger, flush identity, or backpressure choice reads a
//! stage timing. That property is what keeps ADR-0104's `clock.rs` exception
//! sound; if it ever breaks, the exception stops being defensible.
//!
//! The stages here are verified against `log_router.rs` / `log_shard.rs` and
//! the RLOG flush, not transplanted from the metrics pipeline: merge and encode
//! are `RlogWriter` code in this pipeline, not `SegmentWriter`.

#[cfg(feature = "stage-timing")]
mod imp {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    /// A wired stage of the logs ingest pipeline (ADR-0104 decision 2, logs
    /// row). Ordering here fixes the natural pipeline order used by
    /// [`LogStageSnapshot`]'s `BTreeMap`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum LogStage {
        /// Router-side admission gate: the ADR-0069 process-wide ingest byte
        /// budget charge in [`crate::LogIngestRouter::write`]
        /// (`est_record_bytes` + `IngestByteBudget::try_charge`).
        ///
        /// This is NOT the per-tenant [`crate::AdmissionController`] (ADR-0089):
        /// that controller runs in the server, upstream of this router, and the
        /// bulk-load path bypasses it by construction (surfaced to operators as
        /// `ADMISSION_BYPASS_WARNING`). It is never invoked inside `write`, so
        /// no `AdmissionController` cost is folded into this figure for either
        /// OTLP ingest or `load --parquet`. The number is the router's own
        /// byte-budget admission only, which runs for both paths.
        Admit,
        /// Router generation resolution + shard grouping + dispatch: the
        /// `active_set` re-read, the `shard_for_log` grouping, and the
        /// per-shard channel sends in [`crate::LogIngestRouter::write`]. It does
        /// not include the strict-mode ack wait, which is downstream durability,
        /// not routing.
        Route,
        /// Shard-actor buffer append: `LogTenantBuf::merge` in the log shard
        /// actor's write handler. The log analogue of the metrics merge, but a
        /// plain append here (the stream-identity collision check is deferred to
        /// `RlogWriter::finish`), so it is genuinely different code.
        Merge,
        /// Flush-task RLOG serialization: the `RlogWriter::push` loop plus
        /// `finish_with_stats` in the spawned flush. `RlogWriter`, not
        /// `SegmentWriter`; it excludes the object-store PUT, which decision 5
        /// measures separately.
        Encode,
    }

    impl LogStage {
        /// Stable lowercase name for reporting.
        pub fn name(self) -> &'static str {
            match self {
                LogStage::Admit => "admit",
                LogStage::Route => "route",
                LogStage::Merge => "merge",
                LogStage::Encode => "encode",
            }
        }
    }

    /// Accumulated samples and nanoseconds for one stage.
    #[derive(Debug, Default)]
    struct StageCell {
        samples: AtomicU64,
        total_ns: AtomicU64,
    }

    /// A read-only view of one stage's accumulated totals at snapshot time.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct StageTotals {
        /// How many times the stage recorded a duration.
        pub samples: u64,
        /// Sum of every recorded duration, in nanoseconds.
        pub total_ns: u64,
    }

    /// Per-stage nanosecond accumulator for the logs pipeline, shared by `Arc`
    /// from [`crate::LogIngestRouter`] to every log shard actor and flush task.
    /// Every method is lock-free; the atomics use `Relaxed` because the only
    /// cross-task read (a caller taking a [`Self::snapshot`] after a write's ack
    /// resolves) is ordered by the oneshot ack channel, not by these atomics.
    #[derive(Debug, Default)]
    pub struct LogStageTimings {
        admit: StageCell,
        route: StageCell,
        merge: StageCell,
        encode: StageCell,
    }

    impl LogStageTimings {
        pub fn new() -> Self {
            Self::default()
        }

        fn cell(&self, stage: LogStage) -> &StageCell {
            match stage {
                LogStage::Admit => &self.admit,
                LogStage::Route => &self.route,
                LogStage::Merge => &self.merge,
                LogStage::Encode => &self.encode,
            }
        }

        /// Adds one sample of `dur` to `stage`. Called at stage boundaries with a
        /// duration measured by [`std::time::Instant`].
        pub fn record(&self, stage: LogStage, dur: Duration) {
            let cell = self.cell(stage);
            cell.samples.fetch_add(1, Ordering::Relaxed);
            let ns = u64::try_from(dur.as_nanos()).unwrap_or(u64::MAX);
            cell.total_ns.fetch_add(ns, Ordering::Relaxed);
        }

        /// A point-in-time view holding one entry per stage that recorded at
        /// least one sample. A stage that was never wired (or never reached) is
        /// absent, not present-with-zero: that is what lets a caller assert the
        /// wired stage set exactly rather than only that the map is non-empty.
        pub fn snapshot(&self) -> LogStageSnapshot {
            let mut entries = BTreeMap::new();
            for stage in [
                LogStage::Admit,
                LogStage::Route,
                LogStage::Merge,
                LogStage::Encode,
            ] {
                let cell = self.cell(stage);
                let samples = cell.samples.load(Ordering::Relaxed);
                if samples > 0 {
                    entries.insert(
                        stage,
                        StageTotals {
                            samples,
                            total_ns: cell.total_ns.load(Ordering::Relaxed),
                        },
                    );
                }
            }
            LogStageSnapshot { entries }
        }
    }

    /// An immutable snapshot of the per-stage totals, for the bench reporter.
    #[derive(Debug, Clone, Default)]
    pub struct LogStageSnapshot {
        entries: BTreeMap<LogStage, StageTotals>,
    }

    impl LogStageSnapshot {
        /// Every stage that recorded at least one sample, in pipeline order.
        pub fn stages(&self) -> impl Iterator<Item = LogStage> + '_ {
            self.entries.keys().copied()
        }

        /// Totals for `stage`, or `None` if it recorded nothing.
        pub fn get(&self, stage: LogStage) -> Option<StageTotals> {
            self.entries.get(&stage).copied()
        }

        /// Accumulated nanoseconds for `stage`, or `None` if it recorded
        /// nothing.
        pub fn total_ns(&self, stage: LogStage) -> Option<u64> {
            self.entries.get(&stage).map(|t| t.total_ns)
        }

        pub fn len(&self) -> usize {
            self.entries.len()
        }

        pub fn is_empty(&self) -> bool {
            self.entries.is_empty()
        }
    }

    /// A wired stage of the metrics ingest pipeline (ADR-0104 decision 2,
    /// metrics row). The four stages match the logs pipeline's by name, but each
    /// is verified against the metrics code (`router.rs` / `shard.rs` /
    /// `SegmentWriter`), not transplanted from [`LogStage`]: `merge` and `encode`
    /// are `SegmentWriter` code here, not `RlogWriter`, and the router's
    /// admission and routing steps differ from the log router's. Ordering fixes
    /// the natural pipeline order used by [`MetricStageSnapshot`]'s `BTreeMap`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum MetricStage {
        /// Router-side admission gate: the ADR-0069 process-wide ingest byte
        /// budget charge in [`crate::IngestRouter`]'s write path
        /// (`est_charge_bytes` fold over the request's points and exemplars, then
        /// `IngestByteBudget::try_charge`). On a budget shed the `?` returns
        /// before the `record` call, so a shed request contributes no admit
        /// sample: the stage measures the cost of admissions that succeeded.
        ///
        /// This is NOT the per-tenant [`crate::AdmissionController`] (ADR-0089):
        /// that controller runs in the server, upstream of this router, and is
        /// never invoked in the router's write path. The number is the router's
        /// own byte-budget admission only.
        Admit,
        /// Router generation resolution + shard grouping + dispatch: the
        /// `active_set` resolution, the `shard_for` grouping of points and
        /// exemplars, and the per-shard channel sends in [`crate::IngestRouter`]'s
        /// write path. It excludes the strict-mode ack wait, which is downstream
        /// durability (merge/encode/PUT happen in the shard), not routing.
        Route,
        /// Shard-actor buffer append: `TenantBuf::merge` in the metrics shard
        /// actor's write handler, the metrics analogue of the logs merge. A
        /// series-id collision (ADR-0005) rejects the batch before mutating the
        /// buffer, so a rejected batch records no merge sample; the exemplar
        /// absorb that follows is excluded, exactly as the logs merge times only
        /// the record append.
        Merge,
        /// Flush-task RSEG serialization: the `SeriesInputV3` build plus
        /// `SegmentWriter::write_histograms_with_exemplars` in the spawned flush.
        /// `SegmentWriter`, not `RlogWriter`; it excludes the flush-scoped
        /// exemplar admission and the object-store PUT (decision 5 measures the
        /// PUT separately).
        Encode,
    }

    impl MetricStage {
        /// Stable lowercase name for reporting.
        pub fn name(self) -> &'static str {
            match self {
                MetricStage::Admit => "admit",
                MetricStage::Route => "route",
                MetricStage::Merge => "merge",
                MetricStage::Encode => "encode",
            }
        }
    }

    /// Per-stage nanosecond accumulator for the metrics pipeline, shared by `Arc`
    /// from [`crate::IngestRouter`] to every metrics shard actor and flush task.
    /// The per-pipeline twin of [`LogStageTimings`] (ADR-0104 decision 2 keeps a
    /// separate table per pipeline). Every method is lock-free; the atomics use
    /// `Relaxed` because the only cross-task read (a caller taking a
    /// [`Self::snapshot`] after a write's ack resolves) is ordered by the oneshot
    /// ack channel, not by these atomics.
    #[derive(Debug, Default)]
    pub struct MetricStageTimings {
        admit: StageCell,
        route: StageCell,
        merge: StageCell,
        encode: StageCell,
    }

    impl MetricStageTimings {
        pub fn new() -> Self {
            Self::default()
        }

        fn cell(&self, stage: MetricStage) -> &StageCell {
            match stage {
                MetricStage::Admit => &self.admit,
                MetricStage::Route => &self.route,
                MetricStage::Merge => &self.merge,
                MetricStage::Encode => &self.encode,
            }
        }

        /// Adds one sample of `dur` to `stage`. Called at stage boundaries with a
        /// duration measured by [`std::time::Instant`].
        pub fn record(&self, stage: MetricStage, dur: Duration) {
            let cell = self.cell(stage);
            cell.samples.fetch_add(1, Ordering::Relaxed);
            let ns = u64::try_from(dur.as_nanos()).unwrap_or(u64::MAX);
            cell.total_ns.fetch_add(ns, Ordering::Relaxed);
        }

        /// A point-in-time view holding one entry per stage that recorded at
        /// least one sample. A stage that was never wired (or never reached) is
        /// absent, not present-with-zero: that is what lets a caller assert the
        /// wired stage set exactly rather than only that the map is non-empty.
        pub fn snapshot(&self) -> MetricStageSnapshot {
            let mut entries = BTreeMap::new();
            for stage in [
                MetricStage::Admit,
                MetricStage::Route,
                MetricStage::Merge,
                MetricStage::Encode,
            ] {
                let cell = self.cell(stage);
                let samples = cell.samples.load(Ordering::Relaxed);
                if samples > 0 {
                    entries.insert(
                        stage,
                        StageTotals {
                            samples,
                            total_ns: cell.total_ns.load(Ordering::Relaxed),
                        },
                    );
                }
            }
            MetricStageSnapshot { entries }
        }
    }

    /// An immutable snapshot of the metrics per-stage totals, for the bench
    /// reporter. The per-pipeline twin of [`LogStageSnapshot`].
    #[derive(Debug, Clone, Default)]
    pub struct MetricStageSnapshot {
        entries: BTreeMap<MetricStage, StageTotals>,
    }

    impl MetricStageSnapshot {
        /// Every stage that recorded at least one sample, in pipeline order.
        pub fn stages(&self) -> impl Iterator<Item = MetricStage> + '_ {
            self.entries.keys().copied()
        }

        /// Totals for `stage`, or `None` if it recorded nothing.
        pub fn get(&self, stage: MetricStage) -> Option<StageTotals> {
            self.entries.get(&stage).copied()
        }

        /// Accumulated nanoseconds for `stage`, or `None` if it recorded
        /// nothing.
        pub fn total_ns(&self, stage: MetricStage) -> Option<u64> {
            self.entries.get(&stage).map(|t| t.total_ns)
        }

        pub fn len(&self) -> usize {
            self.entries.len()
        }

        pub fn is_empty(&self) -> bool {
            self.entries.is_empty()
        }
    }
}

#[cfg(feature = "stage-timing")]
pub use imp::{
    LogStage, LogStageSnapshot, LogStageTimings, MetricStage, MetricStageSnapshot,
    MetricStageTimings, StageTotals,
};

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use ravel_logseg::stream_attrs_bytes;
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::memory::MemoryStore;
    use ravel_otlp::logs_normalize::NormalizedLogRecord;
    use ravel_types::TenantId;
    use ravel_types::logstream::{AttrValue, log_stream_id};

    use crate::clock::SystemClock;
    use crate::config::IngestConfig;
    use crate::log_router::LogIngestRouter;
    use crate::router::WriteMode;

    /// Flushes on the first record (`target_bytes: 1`) and never on age, so a
    /// single strict write drives one complete flush inline and the strict ack
    /// only resolves once the commit publishes (after encode).
    fn flush_on_first() -> IngestConfig {
        IngestConfig {
            shard_count: 1,
            target_bytes: 1,
            max_flush_delay: Duration::from_secs(3600),
            flush_tick: Duration::from_millis(10),
            put_retry_base_delay: Duration::from_millis(1),
            put_retry_max_delay: Duration::from_millis(5),
            ..IngestConfig::default()
        }
    }

    /// A consistently-built record: `stream_id` and `stream_attrs` derived from
    /// the same inputs, so `RlogWriter::finish`'s collision check passes.
    fn norm_record(service: &str, ts_ns: i64, body: &str) -> NormalizedLogRecord {
        let res: Vec<(String, AttrValue)> = vec![(
            "service.name".to_string(),
            AttrValue::Str(service.to_string()),
        )];
        let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
        let stream_id = log_stream_id(&res, "scope", "", &scope_attrs);
        let stream_attrs = stream_attrs_bytes(&res, "scope", "", &scope_attrs);
        NormalizedLogRecord {
            stream_id,
            stream_attrs,
            ts_ns,
            observed_ts_ns: ts_ns,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: body.to_string(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: Vec::new(),
        }
    }

    fn test_router(store: Arc<dyn ObjectStoreBackend>) -> LogIngestRouter {
        LogIngestRouter::new(flush_on_first(), store, Arc::new(SystemClock))
    }

    /// Drives a real logs write through the router to a flush and asserts the
    /// wired stage set is EXACTLY {admit, route, merge, encode} -- no missing
    /// stage, no extra one -- and every stage recorded a nonzero duration.
    ///
    /// The set is pinned exactly on purpose: a "the map is non-empty" assertion
    /// would still pass with three of the four stages silently unwired, which is
    /// the failure this test exists to catch (ADR-0104 decision 2 wires four
    /// logs stages).
    #[cfg(feature = "stage-timing")]
    #[tokio::test]
    async fn logs_pipeline_records_every_wired_stage() {
        use super::LogStage;

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = test_router(Arc::clone(&store));
        let timings = router.stage_timings();

        // A strict write blocks until the commit publishes, so on return every
        // wired stage -- admit/route on the router, merge/encode in the shard --
        // has already recorded.
        let receipt = router
            .write(
                TenantId::new("acme"),
                vec![
                    norm_record("api", 1_000, "first"),
                    norm_record("api", 2_000, "second"),
                ],
                WriteMode::Strict,
                Duration::from_secs(30),
            )
            .await
            .expect("a strict logs write commits");
        assert_eq!(receipt.tokens.len(), 1, "one shard, one flushed object");

        let snap = timings.snapshot();

        // The stage set is EXACTLY the wired set. Both directions: every wired
        // stage present, and nothing else.
        let wired = [
            LogStage::Admit,
            LogStage::Route,
            LogStage::Merge,
            LogStage::Encode,
        ];
        let recorded: Vec<LogStage> = snap.stages().collect();
        assert_eq!(
            recorded,
            wired.to_vec(),
            "recorded stage set must be exactly the four wired logs stages, got {:?}",
            recorded.iter().map(|s| s.name()).collect::<Vec<_>>(),
        );

        // Every wired stage has a NONZERO duration.
        for stage in wired {
            let ns = snap
                .total_ns(stage)
                .unwrap_or_else(|| panic!("stage {} is unwired: no sample recorded", stage.name()));
            assert!(
                ns > 0,
                "stage {} recorded a zero duration; it is not measuring real work",
                stage.name()
            );
        }

        router.shutdown().await;
    }

    /// With the feature off, the seam does not exist and the logs write path
    /// runs exactly as it does today: a strict write still drives a flush and
    /// commits. Proves feature-off behavior is unchanged (the router exposes no
    /// `stage_timings` accessor here, so this test cannot reference the seam).
    #[cfg(not(feature = "stage-timing"))]
    #[tokio::test]
    async fn feature_off_logs_write_still_commits() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = test_router(Arc::clone(&store));

        let receipt = router
            .write(
                TenantId::new("acme"),
                vec![norm_record("api", 1_000, "only")],
                WriteMode::Strict,
                Duration::from_secs(30),
            )
            .await
            .expect("a strict logs write commits with the feature off");
        assert_eq!(
            receipt.tokens.len(),
            1,
            "one shard, one flushed object, feature-off path intact"
        );

        router.shutdown().await;
    }

    /// A consistently-built metrics point: one series, one label set, a scalar
    /// sample. Two points of the same series drive a single-object flush.
    fn metric_points() -> Vec<crate::value::IngestPoint> {
        use ravel_types::{Label, LabelSet, Sample, SeriesId};

        use crate::value::{IngestPoint, IngestValue};

        let labels = Arc::new(
            LabelSet::new(vec![Label {
                name: "__name__".to_string(),
                value: "http_requests_total".to_string(),
            }])
            .expect("distinct label names"),
        );
        vec![
            IngestPoint {
                series_id: SeriesId([1u8; 16]),
                labels: Arc::clone(&labels),
                value: IngestValue::Scalar(Sample {
                    ts_ns: 1_000,
                    value: 1.0,
                }),
            },
            IngestPoint {
                series_id: SeriesId([1u8; 16]),
                labels,
                value: IngestValue::Scalar(Sample {
                    ts_ns: 2_000,
                    value: 2.0,
                }),
            },
        ]
    }

    /// Drives a real metrics write through the router to a flush and asserts the
    /// wired stage set is EXACTLY {admit, route, merge, encode} -- no missing
    /// stage, no extra one -- and every stage recorded a nonzero duration.
    ///
    /// The set is pinned exactly on purpose: a "the map is non-empty" assertion
    /// would still pass with three of the four stages silently unwired, which is
    /// the failure this test exists to catch (ADR-0104 decision 2 wires four
    /// metrics stages). The metrics analogue of
    /// [`logs_pipeline_records_every_wired_stage`].
    #[cfg(feature = "stage-timing")]
    #[tokio::test]
    async fn metrics_pipeline_records_every_wired_stage() {
        use ravel_types::Signal;

        use super::MetricStage;
        use crate::router::IngestRouter;

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = IngestRouter::new(
            flush_on_first(),
            Arc::clone(&store),
            Signal::Metrics,
            Arc::new(SystemClock),
        );
        let timings = router.stage_timings();

        // A strict write blocks until the commit publishes, so on return every
        // wired stage -- admit/route on the router, merge/encode in the shard --
        // has already recorded.
        let receipt = router
            .write_values(
                TenantId::new("acme"),
                metric_points(),
                WriteMode::Strict,
                Duration::from_secs(30),
            )
            .await
            .expect("a strict metrics write commits");
        assert_eq!(receipt.tokens.len(), 1, "one shard, one flushed object");

        let snap = timings.snapshot();

        // The stage set is EXACTLY the wired set. Both directions: every wired
        // stage present, and nothing else.
        let wired = [
            MetricStage::Admit,
            MetricStage::Route,
            MetricStage::Merge,
            MetricStage::Encode,
        ];
        let recorded: Vec<MetricStage> = snap.stages().collect();
        assert_eq!(
            recorded,
            wired.to_vec(),
            "recorded stage set must be exactly the four wired metrics stages, got {:?}",
            recorded.iter().map(|s| s.name()).collect::<Vec<_>>(),
        );

        // Every wired stage has a NONZERO duration.
        for stage in wired {
            let ns = snap
                .total_ns(stage)
                .unwrap_or_else(|| panic!("stage {} is unwired: no sample recorded", stage.name()));
            assert!(
                ns > 0,
                "stage {} recorded a zero duration; it is not measuring real work",
                stage.name()
            );
        }

        router.shutdown().await;
    }

    /// With the feature off, the seam does not exist and the metrics write path
    /// runs exactly as it does today: a strict write still drives a flush and
    /// commits. Proves feature-off behavior is unchanged (the router exposes no
    /// `stage_timings` accessor here, so this test cannot reference the seam).
    #[cfg(not(feature = "stage-timing"))]
    #[tokio::test]
    async fn feature_off_metrics_write_still_commits() {
        use ravel_types::Signal;

        use crate::router::IngestRouter;

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = IngestRouter::new(
            flush_on_first(),
            Arc::clone(&store),
            Signal::Metrics,
            Arc::new(SystemClock),
        );

        let receipt = router
            .write_values(
                TenantId::new("acme"),
                metric_points(),
                WriteMode::Strict,
                Duration::from_secs(30),
            )
            .await
            .expect("a strict metrics write commits with the feature off");
        assert_eq!(
            receipt.tokens.len(),
            1,
            "one shard, one flushed object, feature-off metrics path intact"
        );

        router.shutdown().await;
    }
}
