//! `POST /api/v1/admin/fold`: an operator-triggered catalog fold for one
//! tenant and one signal (issue #785).
//!
//! The scheduled fold ([`crate::fold`]) runs on a fixed cadence over every
//! discovered tenant. This route is the on-demand form of the same operation:
//! one tenant, one signal, one [`Catalog::fold`] call, with the identical
//! arguments the background loop passes (a per-process `folder_id`, an
//! injected clock, and the deployment-default retention window resolved from
//! the same CLI-derived [`RetentionConfig`]). It changes nothing about the
//! scheduled fold's cadence or behavior, and it adds no second fold
//! mechanism: both call the one `Catalog::fold` entry point.
//!
//! # Authorization
//!
//! The same authorization every tenant-scoped route on `--listen-http` (and
//! the mTLS listener) requires: the configured
//! [`TenantResolver`] chain resolves the request's credential to a
//! [`TenantId`], and the fold runs for exactly that tenant. There is no
//! separate admin auth path in this crate to reuse, and inventing one is out
//! of scope, so the credential that authorizes reading a tenant's data
//! (`/api/v1/sql`, `/api/v1/query`) is the credential that authorizes folding
//! its catalog. A fold neither reveals data nor destroys any: it rewrites a
//! query-cost index the tenant already owns.
//!
//! The body may also name the tenant explicitly. When it does, the name must
//! hash to the authenticated tenant or the request is refused with 403: the
//! endpoint never folds a tenant other than the one the credential resolves
//! to, and an operator holding several tenants' credentials gets a check that
//! they used the one they meant.
//!
//! # Concurrency
//!
//! Safe to call concurrently with the scheduled fold and with itself, because
//! a fold publishes through the `HEAD` CAS protocol
//! (docs/catalog-and-mvcc.md): every folder writes its parts under
//! content-addressed keys and then compare-and-swaps `HEAD`. A losing CAS is
//! an ordinary outcome, not corruption -- the loser re-reads `HEAD` and either
//! rebases or stops, and no caller's data is lost either way. This module adds
//! no lock of its own; a lock would only convert a cheap CAS loss into a
//! queue.
//!
//! # Outcomes
//!
//! Three distinct outcomes, always 200, always named in the response body's
//! `status` field ([`FoldOutcome`]):
//!
//! - `published`: this call wrote a new snapshot, advancing `HEAD` to name
//!   content that differs from what it named before.
//! - `nothing_eligible`: no commit was eligible to fold. A fold seals an
//!   ingest hour only once `max_flush_lifetime + clock_skew_allowance +
//!   fold_safety_margin` has elapsed after that hour ends, so a fold
//!   triggered right after a load seals nothing. That is the rule working,
//!   not a failure, which is exactly why it is a separate status rather than
//!   a `published` with zero counters. The response's `head_advanced` field
//!   separates the two shapes this outcome takes: `false` when `HEAD` was
//!   left untouched, `true` when the sealing watermark moved over hours that
//!   held nothing to fold, so the new snapshot names the same entries the old
//!   one did.
//! - `lost_cas`: a concurrent fold (the scheduled loop, or another operator
//!   call) won the `HEAD` CAS. The catalog is fine and the winner's snapshot
//!   is published; this call did not publish one.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use ravel_catalog::{Catalog, CatalogError, FoldReport};
use ravel_ingest::Clock;
use ravel_maintain::RetentionConfig;
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_query::http::TenantResolver;
use ravel_types::{Signal, TenantHash, TenantId};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

/// The route an operator invokes. Named here so the wiring, the tests, and
/// the doc all refer to one string.
pub const FOLD_ROUTE: &str = "/api/v1/admin/fold";

/// Cap on the request body. The request is a signal name and an optional
/// tenant name; this mirrors the defensive bound the sibling query handlers
/// apply.
const MAX_BODY_BYTES: usize = 64 * 1024;

/// What an on-demand fold actually did. These are three different facts an
/// operator needs to tell apart, so they are three values rather than a
/// boolean plus a log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldOutcome {
    /// A new snapshot was published: `HEAD` now names content that differs
    /// from what it named before this call.
    Published,
    /// Nothing was eligible to fold: no commit entered or left the snapshot,
    /// either because no ingest hour has sealed beyond the previous watermark
    /// (`head_advanced: false`) or because the hours that did seal held
    /// nothing to fold (`head_advanced: true`, a watermark-only rewrite).
    NothingEligible,
    /// A concurrent fold won the `HEAD` CAS. This call published nothing; the
    /// winner's snapshot is what `HEAD` names.
    LostCas,
}

impl FoldOutcome {
    /// The wire spelling, and the one an operator greps for.
    pub fn as_str(self) -> &'static str {
        match self {
            FoldOutcome::Published => "published",
            FoldOutcome::NothingEligible => "nothing_eligible",
            FoldOutcome::LostCas => "lost_cas",
        }
    }

    /// One sentence for a human reading the response, because the two
    /// non-publishing outcomes are the ones that get misread as a broken
    /// feature.
    fn message(self) -> &'static str {
        match self {
            FoldOutcome::Published => "a new catalog snapshot was published",
            FoldOutcome::NothingEligible => {
                "nothing was eligible to fold: no commit entered or left the snapshot, so the \
                 catalog names exactly what it named before (see head_advanced for whether the \
                 sealing watermark still moved). An ingest hour seals only once \
                 max_flush_lifetime + clock_skew_allowance + fold_safety_margin has elapsed \
                 after that hour ends, so a fold run right after a load seals nothing"
            }
            FoldOutcome::LostCas => {
                "a concurrent fold won the HEAD compare-and-swap; this call published no \
                 snapshot and the winner's snapshot is the current HEAD"
            }
        }
    }
}

/// One on-demand fold's result: the outcome plus the underlying report when
/// there is one. The report is `None` only for a [`FoldOutcome::LostCas`]
/// reached by exhausting the catalog's own CAS retries, where no fold attempt
/// produced a report.
#[derive(Debug)]
pub struct OnDemandFold {
    pub outcome: FoldOutcome,
    pub report: Option<FoldReport>,
    /// Whether this call rewrote `HEAD`. Reported separately from the outcome
    /// because the two can differ: a fold whose sealing watermark advanced
    /// over hours holding nothing foldable rewrites `HEAD` while folding no
    /// entry at all. That is a [`FoldOutcome::NothingEligible`] -- the
    /// operator's question is whether any data was eligible -- but the
    /// watermark move is real and is never hidden.
    pub head_advanced: bool,
}

/// Shared state for the route: the same [`Catalog`] the query paths resolve
/// through and the scheduled fold folds with, the store (read directly for the
/// `HEAD` before/after comparison below), the deployment's tenant resolver,
/// an injected clock, this process's `folder_id`, and the CLI-derived
/// retention config the scheduled fold also passes into every fold.
#[derive(Clone)]
pub struct OnDemandFoldState {
    pub catalog: Arc<Catalog>,
    pub store: Arc<dyn ObjectStoreBackend>,
    pub tenant_resolver: Arc<dyn TenantResolver>,
    /// Injected clock. Library logic never calls `SystemTime::now()`; the
    /// handler reads it once per request and passes that `now_ns` into the
    /// fold, so the sealing window is deterministic under test.
    pub clock: Arc<dyn Clock>,
    /// One `folder_id` per process start, matching [`crate::fold::spawn`]'s
    /// rule (proto/ravel/catalog.proto, `SnapshotHead.folder_id`).
    pub folder_id: Uuid,
    /// The deployment-default retention window source (ADR-0078), the same
    /// `RetentionConfig` the scheduled fold and the Maintain-mode sweep use.
    pub retention: Arc<RetentionConfig>,
}

/// The `/api/v1/admin/fold` router.
pub fn router(state: OnDemandFoldState) -> Router {
    Router::new()
        .route(FOLD_ROUTE, post(handle))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct FoldBody {
    /// Which signal's catalog to fold. Required, never defaulted: a tenant can
    /// hold metrics, logs, and spans independently, and folding metrics on a
    /// logs-only tenant seals nothing while reporting a perfectly healthy
    /// `nothing_eligible`. A silent default would make that indistinguishable
    /// from a real empty window.
    signal: String,
    /// Optional explicit tenant name, checked against the authenticated
    /// tenant. Present or absent, the fold only ever runs for the tenant the
    /// credential resolves to.
    #[serde(default)]
    tenant: Option<String>,
}

/// Parses the `signal` field. Only the three signals the fold task itself
/// covers are accepted; the rest are control-plane shards with no snapshot to
/// fold.
fn parse_signal(raw: &str) -> Option<Signal> {
    match raw {
        "metrics" => Some(Signal::Metrics),
        "logs" => Some(Signal::Logs),
        "spans" => Some(Signal::Spans),
        _ => None,
    }
}

async fn handle(State(state): State<OnDemandFoldState>, req: Request<Body>) -> Response {
    match run(&state, req).await {
        Ok(response) => response,
        Err(err) => err.into_response(),
    }
}

async fn run(state: &OnDemandFoldState, req: Request<Body>) -> Result<Response, ApiError> {
    let headers = req.headers().clone();
    let tenant = authenticate(state, &headers)?;
    let tenant_hash = tenant.hash();

    let body = axum::body::to_bytes(req.into_body(), MAX_BODY_BYTES)
        .await
        .map_err(|e| ApiError::bad_request(format!("could not read request body: {e}")))?;
    let body: FoldBody = serde_json::from_slice(&body)
        .map_err(|e| ApiError::bad_request(format!("invalid JSON request body: {e}")))?;

    let signal = parse_signal(&body.signal).ok_or_else(|| {
        ApiError::bad_request(format!(
            "unknown signal {:?}; expected one of \"metrics\", \"logs\", \"spans\"",
            body.signal
        ))
    })?;

    if let Some(named) = &body.tenant
        && TenantId::new(named.clone()).hash() != tenant_hash
    {
        return Err(ApiError {
            status: StatusCode::FORBIDDEN,
            error_type: "forbidden",
            message: "the tenant named in the request body is not the authenticated tenant"
                .to_string(),
        });
    }

    let now_ns = state.clock.now_ns();
    let default_retention_ns = state.retention.window_for(&tenant_hash);

    let result = fold_once(
        state.catalog.as_ref(),
        state.store.as_ref(),
        &tenant_hash,
        signal,
        state.folder_id,
        now_ns,
        default_retention_ns,
    )
    .await
    .map_err(|err| {
        // Same redaction discipline as the query surfaces: a `CatalogError`
        // embeds the physical object key, and the tenant hash inside it, so
        // the full error is logged server-side and the caller gets a
        // class-level message.
        tracing::warn!(
            tenant = %tenant_hash.to_hex(),
            signal = ?signal,
            error = %err,
            "on-demand catalog fold failed"
        );
        ApiError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            error_type: "unavailable",
            message: "the catalog fold could not complete; retry".to_string(),
        }
    })?;

    tracing::info!(
        tenant = %tenant_hash.to_hex(),
        signal = ?signal,
        outcome = result.outcome.as_str(),
        watermark_hour = ?result.report.as_ref().and_then(|r| r.watermark_hour),
        "on-demand catalog fold complete"
    );

    let mut payload = json!({
        "status": result.outcome.as_str(),
        "published": result.outcome == FoldOutcome::Published,
        "head_advanced": result.head_advanced,
        "message": result.outcome.message(),
        "tenant_hash": tenant_hash.to_hex(),
        "signal": body.signal,
    });
    if let Some(report) = &result.report {
        // `as_object_mut` is `Some` for the literal above, which is an object;
        // the fields are merged rather than nested so a small response stays
        // one flat record an operator can read at a glance.
        if let Some(map) = payload.as_object_mut() {
            map.insert("watermark_hour".into(), json!(report.watermark_hour));
            map.insert(
                "previous_watermark_hour".into(),
                json!(report.previous_watermark_hour),
            );
            map.insert("rebuilt".into(), json!(report.rebuilt));
            map.insert("buckets_folded".into(), json!(report.buckets_folded));
            map.insert("entry_count".into(), json!(report.entry_count));
            map.insert("parts_total".into(), json!(report.parts_total));
            map.insert("list_requests".into(), json!(report.list_requests));
            map.insert("get_requests".into(), json!(report.get_requests));
            map.insert("put_requests".into(), json!(report.put_requests));
        }
    }
    Ok((StatusCode::OK, axum::Json(payload)).into_response())
}

/// One on-demand fold, with the outcome classified. Separated from the HTTP
/// layer so a test drives the three outcomes without a transport, exactly as
/// [`crate::maintain::run_tick`] is separated from its timer.
///
/// `now_ns` is the caller's clock reading: this function never reads a clock,
/// so the sealing window is deterministic under test.
///
/// Classification needs the `HEAD` object from before the fold, because
/// [`Catalog::fold`]'s own report cannot distinguish the non-publishing cases
/// on its own:
///
/// - A fold that loses the `HEAD` CAS re-reads `HEAD`, finds the winner's
///   watermark already covers its own, and returns a `no_op` report --
///   identical in shape to the report for "nothing has sealed". The `HEAD`
///   object tells them apart: unchanged means nothing happened, changed means
///   somebody else published while this call was folding.
/// - A fold whose sealing watermark advanced over hours that held no commits
///   rewrites `HEAD` and reports `no_op: false`, having folded nothing. The
///   previous `HEAD`'s entry count tells that apart from a fold that really
///   changed the snapshot's content.
pub async fn fold_once(
    catalog: &Catalog,
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    folder_id: Uuid,
    now_ns: i64,
    default_retention_ns: Option<i64>,
) -> Result<OnDemandFold, CatalogError> {
    let before = read_head(store, tenant, signal)
        .await
        .map_err(CatalogError::Store)?;
    let entries_before = head_entry_count(before.as_deref());

    match catalog
        .fold(tenant, signal, folder_id, now_ns, &[], default_retention_ns)
        .await
    {
        Ok(report) if !report.no_op => {
            // `HEAD` was rewritten. Whether anything was *eligible* is a
            // second question: an ingest hour that seals while holding no
            // commit still advances the watermark, and the resulting snapshot
            // names exactly the entries the previous one did. Comparing the
            // entry counts answers it in either direction -- a fold that
            // folds new commits in, and one that drops entries through a
            // retention tombstone, both changed the snapshot's content.
            let outcome = if report.entry_count == entries_before {
                FoldOutcome::NothingEligible
            } else {
                FoldOutcome::Published
            };
            Ok(OnDemandFold {
                outcome,
                report: Some(report),
                head_advanced: true,
            })
        }
        Ok(report) => {
            let after = read_head(store, tenant, signal)
                .await
                .map_err(CatalogError::Store)?;
            let outcome = if after == before {
                FoldOutcome::NothingEligible
            } else {
                FoldOutcome::LostCas
            };
            Ok(OnDemandFold {
                outcome,
                report: Some(report),
                head_advanced: false,
            })
        }
        // The catalog retries a losing CAS internally and only surfaces this
        // once its retry budget is spent, which takes a sustained run of
        // concurrent winners. It is still a lost race, not corruption, so it
        // is reported as one rather than as a fold failure.
        Err(CatalogError::FoldCasRetriesExhausted { attempts, .. }) => {
            tracing::info!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                attempts,
                "on-demand catalog fold exhausted its HEAD CAS retries; concurrent folders won"
            );
            Ok(OnDemandFold {
                outcome: FoldOutcome::LostCas,
                report: None,
                head_advanced: false,
            })
        }
        Err(err) => Err(err),
    }
}

/// The current `HEAD` object's bytes, or `None` when no fold has ever
/// published for this `(tenant, signal)`. Read directly rather than through
/// the catalog's cached view: this is the before/after identity the outcome
/// classification turns on, so a cached answer would be exactly wrong.
async fn read_head(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<Option<Vec<u8>>, StoreError> {
    match store
        .get(&crate::fold::head_key(tenant, signal), GetRange::Full)
        .await
    {
        Ok(got) => Ok(Some(got.data.to_vec())),
        Err(StoreError::NotFound) => Ok(None),
        Err(err) => Err(err),
    }
}

/// Entries named by a `HEAD` object, summed over its parts. `0` for an absent
/// `HEAD`, and for one this process cannot decode -- both mean "no snapshot
/// content to compare against", and a fold over either rebuilds from the
/// commit layout anyway.
fn head_entry_count(head: Option<&[u8]>) -> u64 {
    head.and_then(|bytes| ravel_catalog::decode_head(bytes).ok())
        .map(|head| head.parts.iter().map(|part| part.entry_count).sum())
        .unwrap_or(0)
}

fn authenticate(state: &OnDemandFoldState, headers: &HeaderMap) -> Result<TenantId, ApiError> {
    state
        .tenant_resolver
        .resolve(headers)
        .map_err(|_| ApiError {
            status: StatusCode::UNAUTHORIZED,
            error_type: "unauthorized",
            message: "authentication required".to_string(),
        })
}

/// The endpoint's error shape, mirroring the sibling query endpoints' typed
/// JSON body.
struct ApiError {
    status: StatusCode,
    error_type: &'static str,
    message: String,
}

impl ApiError {
    fn bad_request(message: String) -> Self {
        ApiError {
            status: StatusCode::BAD_REQUEST,
            error_type: "bad_data",
            message,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            axum::Json(json!({
                "status": "error",
                "errorType": self.error_type,
                "error": self.message,
            })),
        )
            .into_response()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    use axum::http::Request as HttpRequest;
    use bytes::Bytes;
    use ravel_catalog::CatalogConfig;
    use ravel_commit::publish::{self, RetryPolicy};
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_ingest::SystemClock;
    use ravel_logseg::{
        AttrValue, LogRecord, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes,
    };
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{FaultStore, Occurrence, Op};
    use ravel_object_store::memory::MemoryStore;
    use ravel_query::http::StaticBearerTokenResolver;
    use ravel_types::logstream::log_stream_id;
    use tower::ServiceExt;

    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const TOKEN: &str = "operator-token";

    /// A fixed "now" well past any real ingest hour used below, so the
    /// sealing arithmetic in these tests never depends on the wall clock.
    const NOW_NS: i64 = 100_000 * NS_PER_HOUR;

    fn catalog(store: Arc<dyn ObjectStoreBackend>) -> Arc<Catalog> {
        Arc::new(
            Catalog::new(
                store,
                CatalogConfig {
                    shard_count: 1,
                    ..CatalogConfig::default()
                },
            )
            .expect("build catalog"),
        )
    }

    /// Seeds one durable log flush (a real RLOG object plus its commit
    /// record) at `ingest_hour`, the same shape `tests/fold_e2e.rs` seeds.
    /// Writing the commit at a chosen hour, rather than ingesting live, is
    /// what makes the sealing window deterministic: the fold's eligibility
    /// depends on `now_ns - end(ingest_hour)`, and both are chosen here.
    async fn seed_log_commit(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantId,
        ingest_hour: u32,
        seq: u64,
    ) {
        let tenant_hash = tenant.hash();
        let shard = 0u32;
        let writer_id = Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_0007);
        let epoch = 1u64;

        let resource_attrs = vec![(
            "service.name".to_string(),
            AttrValue::Str("checkout".to_string()),
        )];
        let stream_attrs = stream_attrs_bytes(&resource_attrs, "", "", &[]);
        let stream_id = log_stream_id(&resource_attrs, "", "", &[]);
        let created_unix_ns = i64::from(ingest_hour) * NS_PER_HOUR;

        let identity = ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard,
            writer_id: writer_id.into_bytes(),
            writer_epoch: epoch,
            writer_seq: seq,
        };
        let mut writer = RlogWriter::new(RlogConfig::default(), identity);
        writer
            .push(LogRecord {
                stream_id,
                stream_attrs,
                ts_ns: created_unix_ns,
                observed_ts_ns: created_unix_ns,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: "checkout completed".to_string(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: Vec::new(),
            })
            .expect("push log record");
        let bytes = writer.finish().expect("finish RLOG object");

        let content_hash = [0x5au8; 32];
        let data_key = ravel_commit::keys::data_key(
            &tenant_hash,
            Signal::Logs,
            shard,
            writer_id,
            epoch,
            seq,
            &content_hash,
        )
        .expect("build data key");
        store
            .put(
                &data_key,
                Bytes::from(bytes),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put RLOG object");

        let commit = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Logs,
            shard,
            writer_id,
            writer_epoch: epoch,
            writer_seq: seq,
            object_size: 0,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: created_unix_ns,
            max_event_ts_ns: created_unix_ns,
            min_ingest_ts_ns: created_unix_ns,
            max_ingest_ts_ns: created_unix_ns,
            segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
            created_unix_ns,
            ingest_hour_bucket: ingest_hour,
        })
        .expect("build commit record");
        publish::publish(store, &commit, &RetryPolicy::default())
            .await
            .expect("publish commit record");
    }

    /// Happy path: a tenant with a long-sealed ingest hour folds, the outcome
    /// is `published`, and the snapshot the response claims really exists --
    /// `HEAD` is present and names a part object that is present too.
    #[tokio::test]
    async fn an_eligible_tenant_folds_and_publishes_a_snapshot() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = TenantId::new("fold-on-demand-eligible");
        let tenant_hash = tenant.hash();
        // Three hours behind `now`, well past the default sealing sum of
        // max_flush_lifetime (1h) + clock_skew_allowance (5m) +
        // fold_safety_margin (15m).
        let sealed_hour = (NOW_NS / NS_PER_HOUR - 3) as u32;
        seed_log_commit(store.as_ref(), &tenant, sealed_hour, 0).await;

        let catalog = catalog(store.clone());
        let result = fold_once(
            catalog.as_ref(),
            store.as_ref(),
            &tenant_hash,
            Signal::Logs,
            Uuid::from_u128(1),
            NOW_NS,
            None,
        )
        .await
        .expect("fold");

        assert_eq!(
            result.outcome,
            FoldOutcome::Published,
            "a sealed hour must publish"
        );
        let report = result.report.expect("a published fold carries a report");
        assert_eq!(report.watermark_hour, Some(sealed_hour));
        assert_eq!(report.entry_count, 1, "the seeded commit must be folded in");

        // The claimed snapshot exists: HEAD is readable, and every part it
        // names is present in the store.
        let head_bytes = store
            .get(
                &crate::fold::head_key(&tenant_hash, Signal::Logs),
                GetRange::Full,
            )
            .await
            .expect("HEAD present after a published fold");
        let head = ravel_catalog::decode_head(&head_bytes.data).expect("decode HEAD");
        assert_eq!(head.watermark_hour, sealed_hour);
        assert!(!head.parts.is_empty(), "a published HEAD names parts");
        for part in &head.parts {
            store
                .get(&part.key, GetRange::Full)
                .await
                .unwrap_or_else(|err| panic!("part {} named by HEAD is missing: {err}", part.key));
        }
    }

    /// Nothing-eligible path: the tenant's only write lands in the current
    /// hour, which cannot be sealed yet. The outcome is exactly
    /// `nothing_eligible` -- not a failure, and not a claimed publish -- in
    /// both shapes it takes: the first call moves the sealing watermark over
    /// hours holding nothing (`head_advanced: true`, zero entries), and the
    /// second, with the watermark already there, leaves HEAD untouched
    /// (`head_advanced: false`). Neither may claim a publish, because the
    /// operator's write is still unsealed in both.
    #[tokio::test]
    async fn a_tenant_with_unsealable_writes_reports_nothing_eligible() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = TenantId::new("fold-on-demand-unsealed");
        let tenant_hash = tenant.hash();
        // The hour `now` sits in: it has not even ended, let alone aged out
        // the sealing sum.
        let current_hour = (NOW_NS / NS_PER_HOUR) as u32;
        seed_log_commit(store.as_ref(), &tenant, current_hour, 0).await;

        let catalog = catalog(store.clone());
        let result = fold_once(
            catalog.as_ref(),
            store.as_ref(),
            &tenant_hash,
            Signal::Logs,
            Uuid::from_u128(1),
            NOW_NS,
            None,
        )
        .await
        .expect("fold");

        assert_eq!(
            result.outcome,
            FoldOutcome::NothingEligible,
            "a write in the current hour is not sealable, so nothing is eligible"
        );
        let report = result.report.expect("the fold carries a report");
        assert_eq!(
            report.entry_count, 0,
            "the unsealed write must not appear in the snapshot"
        );
        assert!(
            result.head_advanced,
            "the sealing watermark still moves over the empty hours behind it"
        );

        // Second call, same clock: the watermark is already where the sealing
        // rule allows, so this one is a true no-op and leaves HEAD alone.
        let again = fold_once(
            catalog.as_ref(),
            store.as_ref(),
            &tenant_hash,
            Signal::Logs,
            Uuid::from_u128(1),
            NOW_NS,
            None,
        )
        .await
        .expect("second fold");
        assert_eq!(
            again.outcome,
            FoldOutcome::NothingEligible,
            "still nothing sealable, so still nothing eligible"
        );
        assert!(!again.head_advanced, "HEAD must be left untouched");
        assert!(
            again.report.expect("the fold carries a report").no_op,
            "the underlying fold must be a no-op"
        );
    }

    /// Concurrent-CAS path: two folds race on the same tenant. A `FaultStore`
    /// hold gate makes the interleaving deterministic (`MemoryStore` never
    /// yields, so without it the first fold would simply run to completion
    /// before the second started): the first fold is held at its `HEAD` PUT
    /// while the second runs to completion and publishes, then the first is
    /// released into a `HEAD` that has moved under it.
    ///
    /// Exactly one publishes; the loser reports `lost_cas`.
    #[tokio::test]
    async fn two_racing_folds_publish_once_and_the_loser_reports_lost_cas() {
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), Default::default()));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let tenant = TenantId::new("fold-on-demand-race");
        let tenant_hash = tenant.hash();
        let sealed_hour = (NOW_NS / NS_PER_HOUR - 3) as u32;
        seed_log_commit(store.as_ref(), &tenant, sealed_hour, 0).await;

        let head = crate::fold::head_key(&tenant_hash, Signal::Logs);
        // Hold only the FIRST HEAD PUT. The second fold's HEAD PUT is the
        // second match, so it passes straight through and publishes.
        let gate = fault_store.hold(Op::Put, Some(head.clone()), Occurrence::Nth(1));

        // Each racer gets its own Catalog (its own cached HEAD view), the way
        // two separate processes would.
        let catalog_a = catalog(store.clone());
        let store_a = store.clone();
        let racer_a = tokio::spawn(async move {
            fold_once(
                catalog_a.as_ref(),
                store_a.as_ref(),
                &tenant_hash,
                Signal::Logs,
                Uuid::from_u128(0xa),
                NOW_NS,
                None,
            )
            .await
        });

        // Racer A is now blocked inside its HEAD PUT, before the CAS reaches
        // the backend. This assertion is the proof that the race is real: the
        // test does not proceed until a HEAD PUT is genuinely in flight.
        gate.wait_until_held(1).await;
        let held = gate.held_details();
        assert_eq!(held.len(), 1, "exactly one call is held: {held:?}");
        assert_eq!(held[0].1, Op::Put);
        assert_eq!(held[0].2, head, "the held call is the HEAD CAS");

        let catalog_b = catalog(store.clone());
        let result_b = fold_once(
            catalog_b.as_ref(),
            store.as_ref(),
            &tenant_hash,
            Signal::Logs,
            Uuid::from_u128(0xb),
            NOW_NS,
            None,
        )
        .await
        .expect("racer B fold");
        assert_eq!(
            result_b.outcome,
            FoldOutcome::Published,
            "racer B ran while A was held, so B wins the CAS"
        );
        assert_eq!(
            gate.held_count(),
            1,
            "racer A must still be held while B publishes; otherwise the race did not happen"
        );

        // Release A into a HEAD that B already moved.
        assert!(gate.release(held[0].0), "release the held HEAD CAS");
        let result_a = racer_a.await.expect("racer A task").expect("racer A fold");
        assert_eq!(
            result_a.outcome,
            FoldOutcome::LostCas,
            "racer A lost the HEAD CAS and must say so"
        );
        assert_eq!(
            gate.held_count(),
            0,
            "no call is left held once A is released"
        );

        // Exactly one snapshot generation is published, and it is B's.
        let head_bytes = store
            .get(&head, GetRange::Full)
            .await
            .expect("HEAD present after the race");
        let decoded = ravel_catalog::decode_head(&head_bytes.data).expect("decode HEAD");
        assert_eq!(decoded.watermark_hour, sealed_hour);
        assert_eq!(
            decoded.folder_id,
            Uuid::from_u128(0xb).into_bytes().to_vec(),
            "the published HEAD is racer B's, not racer A's"
        );
    }

    fn state(store: Arc<dyn ObjectStoreBackend>, catalog: Arc<Catalog>) -> OnDemandFoldState {
        let tokens = std::collections::HashMap::from([(
            TOKEN.to_string(),
            TenantId::new("fold-on-demand-http"),
        )]);
        OnDemandFoldState {
            catalog,
            store,
            tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
            clock: Arc::new(SystemClock),
            folder_id: Uuid::from_u128(1),
            retention: Arc::new(RetentionConfig::default()),
        }
    }

    async fn post(
        state: OnDemandFoldState,
        token: Option<&str>,
        body: &str,
    ) -> (StatusCode, Vec<u8>) {
        let mut builder = HttpRequest::builder()
            .method("POST")
            .uri(FOLD_ROUTE)
            .header("content-type", "application/json");
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        let request = builder
            .body(Body::from(body.to_string()))
            .expect("build request");
        let response = router(state)
            .oneshot(request)
            .await
            .expect("route the request");
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .expect("read body");
        (status, bytes.to_vec())
    }

    /// The route carries the same authorization the tenant-scoped query
    /// routes do: no credential, no fold.
    #[tokio::test]
    async fn an_unauthenticated_request_is_refused() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = catalog(store.clone());
        let (status, _) = post(state(store, catalog), None, r#"{"signal":"logs"}"#).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    /// The signal is required and never defaulted, and a tenant named in the
    /// body must be the authenticated one.
    #[tokio::test]
    async fn a_missing_signal_or_a_foreign_tenant_is_refused() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = catalog(store.clone());
        let (status, _) = post(state(store.clone(), catalog.clone()), Some(TOKEN), "{}").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "signal is required");

        let (status, _) = post(
            state(store.clone(), catalog.clone()),
            Some(TOKEN),
            r#"{"signal":"nonsense"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "unknown signal is refused");

        let (status, _) = post(
            state(store, catalog),
            Some(TOKEN),
            r#"{"signal":"logs","tenant":"some-other-tenant"}"#,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "the body may not name a tenant other than the authenticated one"
        );
    }

    /// The HTTP surface reports the outcome, not just a success: a tenant
    /// with nothing sealable gets 200 with `status: nothing_eligible` and
    /// `published: false`.
    #[tokio::test]
    async fn the_response_names_the_nothing_eligible_outcome() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = catalog(store.clone());
        let (status, body) = post(
            state(store, catalog),
            Some(TOKEN),
            r#"{"signal":"logs","tenant":"fold-on-demand-http"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(&body).expect("JSON body");
        assert_eq!(body["status"], "nothing_eligible");
        assert_eq!(body["published"], false);
    }
}
