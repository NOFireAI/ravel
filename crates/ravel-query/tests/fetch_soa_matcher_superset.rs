//! CHECK 1 (issue #145, ravel-query cross-area, finding rq-01 SOUND).
//!
//! Confirming test for the assumption the ravel-sql sql1-F01 tests pin from
//! the SQL side (docs/reviews/2026-07-28-ravel-sql-audit/
//! sql1-scan-pushdown-pruning.md section 6, and sql3 section 6): that
//! `SegmentFetcher::fetch_soa` applies label matchers against SERIES_TABLE as
//! a faithful PromQL superset. A series wrongly excluded here is a silent
//! under-fetch (short results) that the SQL `Inexact` residual cannot recover.
//!
//! The prune inside `fetch_soa` runs `ravel_promql::matches_series` (via
//! `decode_selected` -> `select`, fetcher.rs:404-408) over each decoded
//! `LabelSet`, i.e. the *same* `LabelMatcher::is_match` the evaluator uses.
//! These tests build a real RSEG segment with a present-label series, a
//! partial-match series, and an absent-label series, then drive `fetch_soa`
//! directly and assert:
//!
//! - negation (`job != "api"`) KEEPS the absent-label series (absent reads as
//!   `""`, and `"" != "api"` is true), so no row is lost;
//! - regex (`job =~ "api"`) anchors exactly (`^(?:api)$`), matching only the
//!   present-label `api` series, never `api-server` and never the absent one.
//!
//! Both are the exact PromQL semantics documented in
//! crates/ravel-promql/src/matchers.rs, proven end to end through the fetcher
//! against on-disk bytes (not against `is_match` in isolation).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use ravel_catalog::SegmentRef;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_promql::LabelMatcher;
use ravel_query::{FetchedSeriesSoa, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantHash, TenantId};
use uuid::Uuid;

const NS: i64 = 1_000_000_000;
const SEG_KEY: &str = "test/matcher_superset.rseg";

/// A series carrying `__name__=http_requests_total` plus the given optional
/// `job` label (absent when `job` is `None`).
fn series(tenant_id: &TenantId, job: Option<&str>) -> SeriesInput {
    let mut pairs = vec![Label {
        name: "__name__".to_string(),
        value: "http_requests_total".to_string(),
    }];
    if let Some(job) = job {
        pairs.push(Label {
            name: "job".to_string(),
            value: job.to_string(),
        });
    }
    let label_set = LabelSet::new(pairs).expect("valid labels");
    let series_id =
        SeriesId::compute(tenant_id, "http_requests_total", &label_set).expect("series id");
    SeriesInput {
        series_id,
        labels: label_set,
        samples: vec![Sample {
            ts_ns: 1_000 * NS,
            value: 1.0,
        }],
    }
}

/// Writes a real RSEG segment with three series that differ only in `job`:
/// `api` (present, exact), `api-server` (present, partial match for `api`),
/// and no `job` at all (absent). Puts the bytes on a fresh `MemoryStore`.
async fn segment_with_three_jobs() -> (SegmentFetcher, TenantHash, SegmentRef) {
    let tenant_id = TenantId::new("t".to_string());
    let tenant_hash = tenant_id.hash();
    let writer_id = Uuid::from_u128(1);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let inputs = vec![
        series(&tenant_id, Some("api")),
        series(&tenant_id, Some("api-server")),
        series(&tenant_id, None),
    ];
    let written = SegmentWriter::write(inputs, identity, bounds).expect("write segment");

    let store = Arc::new(MemoryStore::new());
    store
        .put(SEG_KEY, written.bytes.clone(), PutOptions::default())
        .await
        .expect("put segment object");

    let seg_ref = SegmentRef {
        data_object_key: SEG_KEY.to_string(),
        object_size: written.bytes.len() as u64,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        ingest_hour_bucket: 0,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        shard: 0,
        content_hash: written.summary.blake3,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 42,
    };
    let backend: Arc<dyn ObjectStoreBackend> = store;
    (SegmentFetcher::new(backend), tenant_hash, seg_ref)
}

/// The set of `job` values (absent series rendered as `<absent>`) that
/// `fetch_soa` returned for `matchers`.
fn fetched_jobs(fetched: &[FetchedSeriesSoa]) -> BTreeSet<String> {
    fetched
        .iter()
        .map(|s| {
            s.labels
                .get("job")
                .map_or_else(|| "<absent>".to_string(), str::to_string)
        })
        .collect()
}

/// Negation is the highest-risk matcher form: its correctness *requires*
/// retaining the absent-label series the operator's name suggests it drops.
/// `job != "api"` must keep `api-server` and the absent-`job` series and drop
/// only `api`. A fetcher that dropped absent-label series would silently
/// under-fetch, and the SQL Inexact residual could not recover the rows.
#[tokio::test]
async fn fetch_soa_negation_keeps_absent_label_series() {
    let (fetcher, tenant_hash, seg_ref) = segment_with_three_jobs().await;

    let matchers = vec![LabelMatcher::not_equal("job", "api")];
    let (fetched, _stats) = fetcher
        .fetch_soa(tenant_hash, &seg_ref, &matchers)
        .await
        .expect("fetch_soa");

    let jobs = fetched_jobs(&fetched);
    assert_eq!(
        jobs,
        BTreeSet::from(["api-server".to_string(), "<absent>".to_string()]),
        "job!=api must retain the absent-label series (absent-as-empty) and \
         the partial match, dropping only the exact-match series"
    );
}

/// Regex must anchor exactly as `LabelMatcher::is_match` (`^(?:api)$`,
/// absent-as-empty): `job =~ "api"` matches only the exact `api` series,
/// never the `api-server` substring and never the absent-`job` series.
#[tokio::test]
async fn fetch_soa_regex_anchors_exactly() {
    let (fetcher, tenant_hash, seg_ref) = segment_with_three_jobs().await;

    let matchers = vec![LabelMatcher::regex("job", "api").expect("compiles")];
    let (fetched, _stats) = fetcher
        .fetch_soa(tenant_hash, &seg_ref, &matchers)
        .await
        .expect("fetch_soa");

    let jobs = fetched_jobs(&fetched);
    assert_eq!(
        jobs,
        BTreeSet::from(["api".to_string()]),
        "job=~api is anchored ^(?:api)$: only the exact-match series, never \
         api-server and never the absent-label series"
    );
}

/// The complement direction: an empty selector fetches every series (the
/// baseline the two pruned fetches above are measured against), so the counts
/// above are drops, not a segment that only ever held one series.
#[tokio::test]
async fn fetch_soa_empty_matchers_fetch_all_three() {
    let (fetcher, tenant_hash, seg_ref) = segment_with_three_jobs().await;

    let (fetched, _stats) = fetcher
        .fetch_soa(tenant_hash, &seg_ref, &[])
        .await
        .expect("fetch_soa");

    assert_eq!(
        fetched_jobs(&fetched),
        BTreeSet::from([
            "api".to_string(),
            "api-server".to_string(),
            "<absent>".to_string(),
        ]),
    );
}
