//! In-process Ravel side of the differential harness: the ingest crate's
//! public path (`IngestRouter::write_values`) over a `MemoryStore`, served by the
//! same `Catalog` / `QueryEngine` / HTTP router the production query
//! service uses (docs/promql-evaluator-plan.md section 5.1).
//!
//! Ingest deliberately goes through `IngestRouter`, not hand-built RSEG
//! segments (contrast `crates/ravel-query/tests/e2e.rs`): using the real
//! sharded, flushed, segment-writing path means the differential gate
//! isolates evaluator/query semantics from ingest mapping, rather than
//! also re-testing segment construction by construction.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{
    Clock, IngestConfig, IngestPoint, IngestRouter, IngestValue, WriteError, WriteMode,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::http::{AppState, StaticBearerTokenResolver, router};
use ravel_query::{EngineConfig, QueryEngine};
use ravel_segment::{HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, ResetHint};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId, TypeError};

use crate::generator::{Dataset, GeneratedHistogramPoint, GeneratedHistogramSeries};

/// Bearer token the harness authenticates its own HTTP requests with; never
/// exposed outside this process.
pub const TOKEN: &str = "difftest-token";

#[derive(Debug, thiserror::Error)]
pub enum RavelStackError {
    #[error("building label set: {0}")]
    InvalidLabels(TypeError),
    #[error("computing series id: {0}")]
    SeriesId(TypeError),
    #[error("ingest write failed: {0}")]
    Write(#[from] WriteError),
    #[error("opening catalog: {0}")]
    Catalog(#[from] ravel_catalog::CatalogError),
}

/// Clock pinned to the dataset's own `base_ts_ms`: flush/commit identity in
/// this harness never depends on wall-clock time (CLAUDE.md "Time is
/// injected").
#[derive(Debug, Clone, Copy)]
struct FixedClock(i64);

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

/// A running in-process Ravel query stack with `dataset` already ingested
/// and flushed. `app` is ready for `tower::ServiceExt::oneshot` calls
/// against `/api/v1/query` and `/api/v1/query_range`.
pub struct RavelStack {
    pub app: Router,
    pub tenant: TenantId,
    pub token: &'static str,
}

impl RavelStack {
    /// Ingests every scalar and native-histogram series in `dataset` for
    /// `tenant` through `IngestRouter::write_values`, forces a flush so every
    /// point is committed and visible, then builds the query-serving `Router`.
    ///
    /// `now_ns` anchors both the ingest clock and the flush's ingest-hour
    /// bucket; pass the dataset's own `base_ts_ms` (scaled to nanoseconds)
    /// so a query for the same instants the dataset was generated at always
    /// falls inside the catalog's ingest-lag listing window.
    pub async fn ingest(
        tenant: TenantId,
        dataset: &Dataset,
        now_ns: i64,
    ) -> Result<Self, RavelStackError> {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let clock: Arc<dyn Clock> = Arc::new(FixedClock(now_ns));
        let ingest_config = IngestConfig::default();
        let shard_count = ingest_config.shard_count;
        let ingest = IngestRouter::new(ingest_config, Arc::clone(&store), Signal::Metrics, clock);

        let points = to_points(&tenant, dataset)?;
        ingest
            .write_values(
                tenant.clone(),
                points,
                WriteMode::Buffered,
                Duration::from_secs(30),
            )
            .await?;
        ingest.flush_all().await;
        ingest.shutdown().await;

        let catalog_config = CatalogConfig {
            shard_count,
            ..CatalogConfig::default()
        };
        let catalog = Arc::new(Catalog::new(Arc::clone(&store), catalog_config)?);
        let engine = Arc::new(QueryEngine::new(catalog, store, EngineConfig::default()));

        let mut tokens = std::collections::HashMap::new();
        tokens.insert(TOKEN.to_string(), tenant.clone());
        let state = AppState {
            engine,
            tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
        };

        Ok(RavelStack {
            app: router(state),
            tenant,
            token: TOKEN,
        })
    }
}

fn to_points(tenant: &TenantId, dataset: &Dataset) -> Result<Vec<IngestPoint>, RavelStackError> {
    // Both scalar and native-histogram series flow through `IngestRouter::
    // write_values` as `IngestPoint`s. Native histograms became end-to-end
    // ingestible in #218 stage 1 (RSEG v5 histogram storage plus ravel-query's
    // histogram read path), so `dataset.histogram_series` is now ingested here
    // rather than dropped: a native-histogram corpus entry has real Ravel data
    // to compare against the pinned Prometheus side.
    let mut points = Vec::new();
    for series in &dataset.series {
        let labels = build_label_set(&series.labels)?;
        let series_id = SeriesId::compute(tenant, series.metric_name(), &labels)
            .map_err(RavelStackError::SeriesId)?;
        for (ts_ms, value) in &series.samples {
            points.push(IngestPoint {
                series_id,
                labels: labels.clone(),
                value: IngestValue::Scalar(Sample {
                    ts_ns: ts_ms * 1_000_000,
                    value: *value,
                }),
            });
        }
    }
    for series in &dataset.histogram_series {
        points.extend(histogram_series_to_points(tenant, series)?);
    }
    Ok(points)
}

fn build_label_set(labels: &[(String, String)]) -> Result<LabelSet, RavelStackError> {
    LabelSet::new(
        labels
            .iter()
            .map(|(name, value)| Label {
                name: name.clone(),
                value: value.clone(),
            })
            .collect(),
    )
    .map_err(RavelStackError::InvalidLabels)
}

/// Convert one generated native-histogram series into per-point
/// [`IngestPoint`]s carrying [`IngestValue::Histogram`]. The mapping mirrors
/// the production remote-write decode (`ravel-remote-write` normalize:
/// Prometheus `schema` becomes the RSEG `scale`, integer buckets stay
/// integer), so the Ravel side stores the same value the Prometheus side
/// received over the wire from the identical generated dataset.
fn histogram_series_to_points(
    tenant: &TenantId,
    series: &GeneratedHistogramSeries,
) -> Result<Vec<IngestPoint>, RavelStackError> {
    let labels = build_label_set(&series.labels)?;
    let series_id = SeriesId::compute(tenant, series.metric_name(), &labels)
        .map_err(RavelStackError::SeriesId)?;
    Ok(series
        .points
        .iter()
        .map(|point| IngestPoint {
            series_id,
            labels: labels.clone(),
            value: IngestValue::Histogram(to_histogram_sample(point)),
        })
        .collect())
}

fn to_histogram_sample(point: &GeneratedHistogramPoint) -> HistogramSample {
    HistogramSample {
        ts_ns: point.ts_ms * 1_000_000,
        value: HistogramValue {
            scale: point.schema,
            zero_threshold: point.zero_threshold,
            sum: Some(point.sum),
            custom_values: None,
            positive_spans: point
                .positive_spans
                .iter()
                .map(|(offset, length)| HistogramSpan {
                    offset: *offset,
                    length: *length,
                })
                .collect(),
            negative_spans: Vec::new(),
            counts: HistogramCounts::Int {
                zero_count: point.zero_count,
                count: point.count,
                positive: point.positive_buckets.clone(),
                negative: Vec::new(),
            },
            reset_hint: reset_hint_from_wire(point.reset_hint),
        },
    }
}

/// Map the generator's Prometheus `ResetHint` wire value (0 UNKNOWN, 1 YES,
/// 2 NO, 3 GAUGE) onto the RSEG [`ResetHint`]. Any other value falls back to
/// `Unknown`, matching how a decoder treats an unrecognized hint.
fn reset_hint_from_wire(wire: i32) -> ResetHint {
    match wire {
        1 => ResetHint::Yes,
        2 => ResetHint::No,
        3 => ResetHint::Gauge,
        _ => ResetHint::Unknown,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::generator::GeneratedHistogramPoint;

    fn sample_point() -> GeneratedHistogramPoint {
        GeneratedHistogramPoint {
            ts_ms: 1_700_000_000_000,
            schema: 0,
            zero_threshold: 1e-9,
            zero_count: 1,
            count: 7,
            sum: 12.5,
            positive_spans: vec![(1, 3)],
            positive_buckets: vec![2, 3, 1],
            reset_hint: 2,
        }
    }

    #[test]
    fn histogram_point_maps_field_for_field_to_the_rseg_sample() {
        let sample = to_histogram_sample(&sample_point());
        // Timestamp is scaled ms -> ns, matching the scalar path.
        assert_eq!(sample.ts_ns, 1_700_000_000_000 * 1_000_000);
        // Prometheus `schema` becomes the RSEG `scale` unchanged, mirroring the
        // production remote-write decode.
        assert_eq!(sample.value.scale, 0);
        assert_eq!(sample.value.zero_threshold.to_bits(), 1e-9_f64.to_bits());
        assert_eq!(sample.value.sum, Some(12.5));
        assert!(sample.value.custom_values.is_none());
        assert_eq!(sample.value.negative_spans, Vec::new());
        assert_eq!(
            sample.value.positive_spans,
            vec![HistogramSpan {
                offset: 1,
                length: 3
            }]
        );
        // Integer counts stay integer (never float), absolute (not delta).
        match &sample.value.counts {
            HistogramCounts::Int {
                zero_count,
                count,
                positive,
                negative,
            } => {
                assert_eq!(*zero_count, 1);
                assert_eq!(*count, 7);
                assert_eq!(*positive, vec![2, 3, 1]);
                assert!(negative.is_empty());
            }
            other => panic!("expected integer counts, got {other:?}"),
        }
        assert_eq!(sample.value.reset_hint, ResetHint::No);
    }

    #[test]
    fn reset_hint_wire_values_map_to_every_state() {
        assert_eq!(reset_hint_from_wire(0), ResetHint::Unknown);
        assert_eq!(reset_hint_from_wire(1), ResetHint::Yes);
        assert_eq!(reset_hint_from_wire(2), ResetHint::No);
        assert_eq!(reset_hint_from_wire(3), ResetHint::Gauge);
        // An unrecognized hint degrades to Unknown rather than panicking.
        assert_eq!(reset_hint_from_wire(99), ResetHint::Unknown);
    }

    #[test]
    fn dataset_histogram_series_become_ingest_points_with_histogram_values() {
        let tenant = TenantId::new("difftest");
        let dataset = crate::generator::generate(&crate::generator::DatasetConfig::default());
        let points = to_points(&tenant, &dataset).expect("build ingest points");
        // Every generated histogram sample reaches the ingest points as a
        // histogram-valued point (this is what stage 1 unblocked; before it,
        // histogram series were dropped on the Ravel side).
        let expected_histogram_points: usize = dataset
            .histogram_series
            .iter()
            .map(|s| s.points.len())
            .sum();
        let histogram_points = points
            .iter()
            .filter(|p| matches!(p.value, IngestValue::Histogram(_)))
            .count();
        assert_eq!(histogram_points, expected_histogram_points);
        assert!(histogram_points > 0, "dataset must carry histogram series");
    }
}
