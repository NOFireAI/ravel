//! In-process Ravel side of the differential harness: the ingest crate's
//! public path (`IngestRouter::write`) over a `MemoryStore`, served by the
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
use ravel_ingest::{Clock, IngestConfig, IngestRouter, WriteError, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_otlp::NormalizedPoint;
use ravel_query::http::{AppState, StaticBearerTokenResolver, router};
use ravel_query::{EngineConfig, QueryEngine};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId, TypeError};

use crate::generator::Dataset;

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
    /// Ingests every series in `dataset` for `tenant` through
    /// `IngestRouter::write`, forces a flush so every point is committed and
    /// visible, then builds the query-serving `Router`.
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
            .write(
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

fn to_points(
    tenant: &TenantId,
    dataset: &Dataset,
) -> Result<Vec<NormalizedPoint>, RavelStackError> {
    // NOTE (P11 read-path gap): `dataset.histogram_series` is intentionally
    // NOT ingested here. `NormalizedPoint` carries a scalar `ravel_types::
    // Sample` only, and the query read path (`ravel-query` fetcher/merge) is
    // f64-only, so a native histogram cannot flow ingest -> storage -> query
    // -> evaluator on the Ravel side yet. Until that lands, native-histogram
    // corpus entries have Prometheus data but no Ravel data to compare
    // against; see corpus/histogram_native.txt and the P11 report.
    let mut points = Vec::new();
    for series in &dataset.series {
        let labels = LabelSet::new(
            series
                .labels
                .iter()
                .map(|(name, value)| Label {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
        )
        .map_err(RavelStackError::InvalidLabels)?;
        let metric_name = series.metric_name();
        let series_id =
            SeriesId::compute(tenant, metric_name, &labels).map_err(RavelStackError::SeriesId)?;
        for (ts_ms, value) in &series.samples {
            points.push(NormalizedPoint {
                series_id,
                labels: labels.clone(),
                sample: Sample {
                    ts_ns: ts_ms * 1_000_000,
                    value: *value,
                },
                is_monotonic_sum: false,
            });
        }
    }
    Ok(points)
}
