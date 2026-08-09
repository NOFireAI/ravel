//! `GET`/`POST /api/v1/query_exemplars`: Prometheus' exemplar-query surface
//! (ADR-0047 decision 4, issue #475).
//!
//! This is the read side of exemplars: #471 through #474 wrote them into the
//! RSEG `EXEMPLARS` section (kind 10) at flush and copied them verbatim
//! through compaction, but nothing read them back until this endpoint. It is
//! what Grafana calls when a user clicks an exemplar on a metric panel, and
//! the trace/span ids it surfaces are the whole point of the feature.
//!
//! # Request
//!
//! Prometheus' `query`, `start`, and `end` parameters, form-encoded on the
//! query string (`GET`) or in the body (`POST`), exactly as the sibling
//! Prometheus-shaped endpoints in `ravel-query` accept them. `start`/`end`
//! are Unix float seconds or an RFC3339 instant; an optional `timeout`
//! (seconds, or Prometheus duration syntax) can only *lower* the server
//! deadline, and an optional repeated `min_commit_token` carries
//! read-your-write tokens, both as `/api/v1/query_range` treats them.
//!
//! # Response
//!
//! Prometheus' `/api/v1/query_exemplars` shape: `data` is an array of
//! objects, each with `seriesLabels` (the matched series' labels) and an
//! `exemplars` array whose entries carry `labels`, `value`, and `timestamp`.
//! Timestamps are float seconds, rendered exactly as every other endpoint
//! here renders them (ADR-0021 bit-exact wire text). The exemplar's trace id
//! and span id ride in `labels` under the conventional `trace_id`/`span_id`
//! keys, hex-encoded; an all-zero id means absent and its label is omitted
//! rather than emitted as zeros (W3C Trace Context reserves the all-zero id
//! as invalid, so the two cases are indistinguishable anyway).
//!
//! Alongside `data`, a `stats` object carries this request's segment counters
//! and cost accounting (ADR-0044), with the same field names
//! `/api/v1/query`'s `data.stats` uses. It sits next to `data` rather than
//! inside it only because Prometheus fixes this endpoint's `data` as an array.
//!
//! # Two data facts and how this endpoint decides them
//!
//! Both were found reviewing #474 and are called out in the issue.
//!
//! 1. **An exemplar's `ts_ns` can fall outside its object's event bounds.**
//!    The exemplar timestamp does not go through the sample path's
//!    event-time clamp, so a stored exemplar can sit outside the object's
//!    `min_event_ts_ns`/`max_event_ts_ns` and outside the commit record's
//!    bounds. This endpoint resolves the snapshot over exactly `[start, end]`
//!    (`Catalog::resolve_pruned_with_accounting`, with the same
//!    equality-`__name__` pruning a sample query uses) and never widens the
//!    window to chase a stray exemplar into an object it would not otherwise
//!    fetch: exemplars are an illustrative/sampled signal by construction
//!    (ADR-0047 decision 2), and widening would fetch more objects for a rare
//!    edge case. Within a fetched object every exemplar is considered; the
//!    returned set is then clamped to `[start, end]` by the exemplar's own
//!    `ts_ns`, matching Prometheus' time-range contract.
//!
//!    The fetch set is the fixed `[start, end]` window. It is *not* the sample
//!    path's `selector_fetch_window`: the `offset` and `@` modifiers do not
//!    move it. This matches Prometheus, which ignores `offset` and `@` on
//!    `/api/v1/query_exemplars` and reads the raw `start`/`end` window. So an
//!    earlier statement that this endpoint "never fetches a segment the
//!    equivalent sample query would not" holds only for a query with no
//!    `offset`/`@`: `query=foo offset 1h` gives the sample path a window
//!    shifted back one hour and this endpoint the raw `[start, end]`, two
//!    disjoint windows. The behaviour is deliberate and Prometheus-compatible;
//!    only that fetch-parity claim is narrowed.
//!
//! 2. **An exemplar carries no dedup priority.** During ADR-0018's overlap
//!    window a snapshot can hold both an L1 part and its inputs, so the same
//!    exemplar is readable twice; samples resolve this by dedup priority, but
//!    exemplars have none. This endpoint deduplicates at query time on the
//!    exemplar's full stored identity (series id, `ts_ns`, trace id, span id,
//!    value bit pattern, and attributes verbatim), keeping the first
//!    occurrence (see [`ExemplarDedupKey`]). That collapses the
//!    compaction-overlap double, which is byte-identical by construction and
//!    would otherwise render two identical dots in Grafana, while preserving
//!    genuinely distinct exemplars: two records that share `(series, ts,
//!    trace)` can still differ in span id, value, or attributes, and the
//!    writer preserves both verbatim and checks nothing else, so both must
//!    survive. Keying only on `(series, ts, trace)` would silently collapse
//!    them.
//!
//! # Cost
//!
//! Exemplars ride along with the segments the query already matched. A query
//! that does not ask for them pays nothing (this endpoint is separate). A
//! query that does pays, per matched segment, one whole-object `GET` plus a
//! catalog decode and one `EXEMPLARS`-section decode; segments with no
//! `EXEMPLARS` section return early after the catalog check. Every `GET`, its
//! bytes, and the decoded section bytes are recorded into `QueryAccounting`
//! like any other fetch, and the whole request runs under the same wall
//! deadline and `max_segments` budget as `/api/v1/query`.
//!
//! Those two bound the *input*. The result itself is bounded by
//! [`DEFAULT_MAX_EXEMPLARS`], this endpoint's analogue of the engine's
//! `max_series`/`max_samples`: a request that would materialize more is
//! rejected with 422, never truncated to a partial 200.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use ravel_catalog::{Catalog, SegmentLevel, SegmentRef};
use ravel_ingest::Clock;
use ravel_maintain::{QueryAuditSink, QueryStatus, query_audit_event};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_promql::{LabelMatcher, MatchOp, matches_series, plan_selectors};
use ravel_query::http::{QueryErrorResponse, TenantResolver};
use ravel_query::{QueryEngine, QueryError};
use ravel_segment::{
    ExemplarRecord, ExpectedIdentity, Footer, ReaderLimits, SeriesEntryV4, check_identity,
    decode_catalog_v5, decode_exemplars_section, open_from_full,
};
use ravel_types::accounting::{AccountedOp, QueryAccounting, QueryAccountingSnapshot};
use ravel_types::{
    CommitToken, LabelSet, METRIC_NAME_LABEL, SeriesId, Signal, TenantHash, TimeRange,
};
use serde::{Serialize, Serializer};
use serde_json::json;

/// Cap on the request body: a query plus a few scalar parameters, never large
/// in legitimate use. Mirrors the defensive bound the sibling `/api/v1/sql`
/// and `/api/v1/analytics` handlers apply.
const MAX_BODY_BYTES: usize = 1 << 20;

/// RSEG section-kind numbers this endpoint slices out of the footer. The
/// values are frozen by docs/segment-format.md; `ravel_segment::section_kind`
/// is crate-private, so they are mirrored here rather than imported. Both are
/// also validated by the ravel-segment decoders this module calls, so a drift
/// here would surface as a decode error, never as silently wrong data.
const SECTION_KIND_LABEL_DICT: u32 = 1;
const SECTION_KIND_EXEMPLARS: u32 = 10;

const NS_PER_SEC: f64 = 1_000_000_000.0;
const NS_PER_MS: i64 = 1_000_000;

/// Default cap on the exemplars one request may materialize, counted after
/// cross-segment dedup, in the same spirit as the query engine's
/// `max_series`/`max_samples` (`ravel_query::config`): a typed rejection, never
/// a silent truncation (docs/query-engine.md "never silent partial results").
///
/// Without it this endpoint is bounded only by the deadline and
/// `max_segments`: a broad selector can walk 1024 segments, each holding an
/// `EXEMPLARS` section that may decode to
/// `ReaderLimits::max_section_uncompressed_bytes` (1 GiB), and every kept
/// record costs a cloned `LabelSet` on top of the record itself. That is an
/// OOM of the whole process, which takes every co-tenant's in-flight request
/// with it.
///
/// 100_000 is chosen from both ends. From above: a retained entry here is a
/// `SeriesId`, a cloned `LabelSet`, and an `ExemplarRecord` with its
/// attributes, on the order of a few hundred bytes, so the cap bounds peak
/// accumulation at tens of megabytes -- roughly the envelope `max_samples`
/// (10_000_000 samples at 16 bytes) allows the sample path, and four orders of
/// magnitude below what one 1 GiB section alone could reach. From below: an
/// exemplar is a sampled, illustrative signal by construction (ADR-0047
/// decision 2) and the caller is a Grafana panel drawing one dot per exemplar,
/// so a legitimate request lands in the hundreds or thousands; a request over
/// this cap is a scrape-everything query, not a panel.
const DEFAULT_MAX_EXEMPLARS: usize = 100_000;

/// Shared state for the exemplars route. Holds its own `Catalog` and object
/// store handles (the same instances the PromQL engine uses, so an exemplar
/// query resolves byte-for-byte the snapshot a sample query would over the
/// same `[start, end]` window; `offset`/`@` do not apply here, see the module
/// doc data fact 1), plus the
/// query engine's own budget knobs read from its [`EngineConfig`] so this
/// endpoint honors the identical deadline and `max_segments` ceiling.
#[derive(Clone)]
pub struct ExemplarsState {
    pub catalog: Arc<Catalog>,
    pub store: Arc<dyn ObjectStoreBackend>,
    pub tenant_resolver: Arc<dyn TenantResolver>,
    pub clock: Arc<dyn Clock>,
    /// Server wall deadline (`EngineConfig::deadline`); a client `timeout` can
    /// only lower it.
    pub deadline: Duration,
    /// `EngineConfig::max_segments`: the same snapshot-size ceiling
    /// `/api/v1/query` enforces after resolve.
    pub max_segments: usize,
    /// Cap on the exemplars one request may materialize, enforced
    /// incrementally so the accumulation never grows past it. Defaults to
    /// [`DEFAULT_MAX_EXEMPLARS`]; `EngineConfig` has no exemplar knob to read
    /// it from, so it is this endpoint's own budget rather than a mirrored
    /// one.
    pub max_exemplars: usize,
    /// The evidential audit sink one event per executed exemplar query is
    /// submitted through, its durability awaited before the response is
    /// released (ADR-0062 §2a, epic EL / issue #762). Defaults to the no-op
    /// ([`from_engine`](Self::from_engine)); a deployment attaches the one
    /// shared pipeline with [`with_audit_sink`](Self::with_audit_sink).
    pub audit_sink: Arc<dyn QueryAuditSink>,
}

impl ExemplarsState {
    /// Builds the state from the same `QueryEngine` the Prometheus-shaped
    /// routes serve from, reusing its budget configuration so the two never
    /// drift, plus the catalog and store handles the engine reads from (which
    /// it does not expose, so they are passed in alongside it).
    pub fn from_engine(
        engine: &QueryEngine,
        catalog: Arc<Catalog>,
        store: Arc<dyn ObjectStoreBackend>,
        tenant_resolver: Arc<dyn TenantResolver>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let config = engine.config();
        ExemplarsState {
            catalog,
            store,
            tenant_resolver,
            clock,
            deadline: config.deadline,
            max_segments: config.max_segments,
            max_exemplars: DEFAULT_MAX_EXEMPLARS,
            audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
        }
    }

    /// Attach the shared evidential audit sink (ADR-0062 §2a). Returns `self`
    /// so it chains off [`from_engine`](Self::from_engine).
    pub fn with_audit_sink(mut self, audit_sink: Arc<dyn QueryAuditSink>) -> Self {
        self.audit_sink = audit_sink;
        self
    }
}

/// The `/api/v1/query_exemplars` router, mounted on the same listener as the
/// other Prometheus-shaped routes. Both `GET` and `POST` route to the same
/// handler, which reads parameters from the query string and the form body
/// alike.
pub fn router(state: ExemplarsState) -> Router {
    Router::new()
        .route("/api/v1/query_exemplars", get(handle).post(handle))
        .with_state(state)
}

async fn handle(State(state): State<ExemplarsState>, req: Request<Body>) -> Response {
    match run(state, req).await {
        Ok(response) => response,
        Err(err) => err.into_response(),
    }
}

async fn run(state: ExemplarsState, req: Request<Body>) -> Result<Response, ApiError> {
    let headers = req.headers().clone();
    let query_string = req.uri().query().map(str::to_owned);
    let tenant_hash = authenticate(&state, &headers)?;

    let body = axum::body::to_bytes(req.into_body(), MAX_BODY_BYTES)
        .await
        .map_err(|e| ApiError::bad_request(format!("could not read request body: {e}")))?;
    let params = Params::parse(query_string.as_deref(), Some(&body));

    let query = params.require("query")?.to_string();
    let start_ns = parse_timestamp_ns("start", params.require("start")?)?;
    let end_ns = parse_timestamp_ns("end", params.require("end")?)?;
    if start_ns > end_ns {
        return Err(ApiError::bad_request(format!(
            "start {start_ns} ns is after end {end_ns} ns"
        )));
    }
    let deadline = parse_deadline(&params, state.deadline)?;
    let min_tokens = params
        .all("min_commit_token")
        .iter()
        .map(|raw| {
            CommitToken::decode(raw)
                .map_err(|_| ApiError::bad_request(format!("invalid min_commit_token: {raw:?}")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    // The whole request runs under one wall deadline, exactly as the query
    // engine wraps its own evaluation: an elapsed timeout is the same
    // `DeadlineExceeded` (504) a sample query would surface.
    //
    // The read now runs for a resolved tenant, so it is auditable (ADR-0062
    // §2a): capture its outcome, submit one audit event, and await durability
    // before releasing the response. A request rejected earlier (auth, missing
    // or invalid parameters) never reached here and is not audited. The
    // recorded window is the request's `[start, end]`.
    let audit_now = state.clock.now_ns();
    let outcome = match tokio::time::timeout(
        deadline,
        collect_exemplars(&state, tenant_hash, &query, start_ns, end_ns, &min_tokens),
    )
    .await
    {
        Ok(inner) => inner,
        Err(_) => Err(ApiError::from_query(QueryError::DeadlineExceeded {
            deadline,
        })),
    };
    let status = if outcome.is_ok() {
        QueryStatus::Ok
    } else {
        QueryStatus::Error
    };
    submit_audit(
        &state,
        tenant_hash,
        audit_now,
        &query,
        status,
        start_ns,
        end_ns,
    )
    .await?;
    let (series, stats) = outcome?;

    Ok((StatusCode::OK, axum::Json(Envelope::success(series, stats))).into_response())
}

/// Resolves the snapshot over the query's `[start, end]` window (the fixed
/// exemplar fetch set; `offset`/`@` do not move it, matching Prometheus, see
/// the module doc data fact 1), reads exemplars from each matched segment,
/// filters and deduplicates them, and groups them by series into the
/// Prometheus response shape, alongside this request's cost counters
/// (ADR-0044).
async fn collect_exemplars(
    state: &ExemplarsState,
    tenant_hash: TenantHash,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    min_tokens: &[CommitToken],
) -> Result<(Vec<ExemplarSeriesJson>, QueryStatsJson), ApiError> {
    // Parse the query into its selectors exactly as the engine's prefetch
    // does (`plan_selectors`), so the matcher sets and the equality-`__name__`
    // pruning line up with what a sample query over the same text would use. A
    // series is offered exemplars if it matches *any* selector, mirroring the
    // union prefetch a multi-selector query performs.
    let start_ms = ns_to_ms("start", start_ns)?;
    let end_ms = ns_to_ms("end", end_ns)?;
    let plans = plan_selectors(query, start_ms, end_ms)
        .map_err(|e| ApiError::bad_request(format!("invalid query {query:?}: {e}")))?;
    let matcher_sets: Vec<Vec<LabelMatcher>> = plans.into_iter().map(|p| p.matchers).collect();
    if matcher_sets.is_empty() {
        // A query with no selectors (a bare scalar or string literal) can
        // match no series and therefore carries no exemplars. It also resolved
        // no snapshot and touched no object, so its cost is genuinely zero.
        return Ok((Vec::new(), QueryStatsJson::default()));
    }
    let name_filter = shared_equality_name_filter(&matcher_sets);

    let now_ns = state.clock.now_ns();
    let window = TimeRange { start_ns, end_ns };

    // Resolve-and-read is retried once on a store `NotFound`, exactly as the
    // sample path's `resolve_snapshot_with_retry` (ravel-query engine): a
    // pinned segment can vanish under a concurrent L0-to-L1 publish and sweep,
    // which is continuous normal compaction, not a fault. Without the retry a
    // query issued during that window returns 503 where `/api/v1/query` over
    // the same window re-resolves and succeeds. A second `NotFound` gives up
    // with `SnapshotInvalidated`, the same 503 class the engine surfaces.
    //
    // Each attempt gets a fresh `QueryAccounting` so the discarded first
    // attempt's in-flight counts never bleed into the attempt that produced
    // the result (ADR-0044 decision 1), matching the engine.
    match collect_once(
        state,
        tenant_hash,
        &matcher_sets,
        name_filter.as_deref(),
        window,
        start_ns,
        end_ns,
        min_tokens,
        now_ns,
    )
    .await
    {
        Ok(result) => Ok(result),
        Err(CollectError::Api(e)) => Err(e),
        Err(CollectError::SnapshotStale) => match collect_once(
            state,
            tenant_hash,
            &matcher_sets,
            name_filter.as_deref(),
            window,
            start_ns,
            end_ns,
            min_tokens,
            now_ns,
        )
        .await
        {
            Ok(result) => Ok(result),
            Err(CollectError::Api(e)) => Err(e),
            Err(CollectError::SnapshotStale) => {
                Err(ApiError::from_query(QueryError::SnapshotInvalidated))
            }
        },
    }
}

/// One resolve-and-read attempt: resolve the snapshot, enforce `max_segments`,
/// read every matched segment's exemplars, and build this attempt's stats.
/// Returns [`CollectError::SnapshotStale`] when a pinned object GET returns
/// `NotFound` mid-flight, which [`collect_exemplars`] retries once; every other
/// failure is a fatal [`CollectError::Api`]. A fresh [`QueryAccounting`] is
/// created here so a retried attempt starts from zero counters.
#[allow(clippy::too_many_arguments)]
async fn collect_once(
    state: &ExemplarsState,
    tenant_hash: TenantHash,
    matcher_sets: &[Vec<LabelMatcher>],
    name_filter: Option<&str>,
    window: TimeRange,
    start_ns: i64,
    end_ns: i64,
    min_tokens: &[CommitToken],
    now_ns: i64,
) -> Result<(Vec<ExemplarSeriesJson>, QueryStatsJson), CollectError> {
    let accounting = QueryAccounting::new();
    let snapshot = state
        .catalog
        .resolve_pruned_with_accounting(
            &tenant_hash,
            Signal::Metrics,
            window,
            min_tokens,
            now_ns,
            name_filter,
            &accounting,
        )
        .await
        .map_err(|e| CollectError::Api(ApiError::from_query(QueryError::from(e))))?;

    // The same snapshot-size ceiling `/api/v1/query` enforces after resolve.
    if snapshot.segments.len() > state.max_segments {
        return Err(CollectError::Api(ApiError::from_query(
            QueryError::TooManySegments {
                count: snapshot.segments.len(),
                max: state.max_segments,
            },
        )));
    }

    // Segments are read sequentially. The sample path fans this out under a
    // concurrency bound; here correctness and staying inside `ravel-server`'s
    // (non-`futures`) default dependency set win, and the wall deadline above
    // still bounds the total. Noted in the issue #475 report as a follow-up.
    //
    // Dedup runs here rather than after the walk so `max_exemplars` counts
    // distinct exemplars (an overlap-window duplicate must not consume budget)
    // and so neither `collected` nor `seen` can grow past the cap.
    let mut collected: Vec<(SeriesId, LabelSet, ExemplarRecord)> = Vec::new();
    let mut seen: HashSet<ExemplarDedupKey> = HashSet::new();
    for seg in &snapshot.segments {
        read_segment_exemplars(
            state,
            tenant_hash,
            seg,
            matcher_sets,
            start_ns,
            end_ns,
            &accounting,
            &mut seen,
            &mut collected,
        )
        .await?;
    }

    let stats = QueryStatsJson {
        segments_fetched: snapshot.segments.len() as u64,
        segments_pruned: snapshot.segments_pruned,
        accounting: QueryAccountingJson::from_snapshot(&accounting.snapshot()),
    };
    Ok((group_by_series(collected), stats))
}

/// The cross-segment exemplar dedup key. Two records collapse only when they
/// are byte-identical in every field the writer preserves: series id,
/// timestamp, trace id, span id, the value (by bit pattern, never `==`, per
/// the storage float rules), and the attributes verbatim (order and any
/// duplicate names included). The ADR-0018 overlap double is byte-identical by
/// construction, so it still collapses to one; two records that share
/// `(series, ts, trace)` but differ in span id, value, or attributes are
/// genuinely distinct (a retried write preserves both verbatim and checks
/// nothing else) and both survive. Widening this key can only ever increase
/// the kept count, so the `max_exemplars` cap still counts distinct exemplars.
type ExemplarDedupKey = (SeriesId, i64, [u8; 16], [u8; 8], u64, Vec<(String, String)>);

/// Outcome of a single [`collect_once`] attempt that is not a success.
enum CollectError {
    /// A pinned object GET returned `NotFound`: the snapshot went stale under
    /// a concurrent publish-and-sweep. The caller re-resolves and retries once.
    SnapshotStale,
    /// Any other failure, already mapped to its client-visible form.
    Api(ApiError),
}

impl From<ApiError> for CollectError {
    fn from(e: ApiError) -> Self {
        CollectError::Api(e)
    }
}

/// Reads one segment's exemplars for the matched series, appending the ones
/// not already in `seen` to `collected`. Returns early with nothing when the
/// object carries no `EXEMPLARS` section (the common case), after only the
/// whole-object `GET` and footer parse.
///
/// Appending here rather than returning a per-segment vector is what keeps
/// `state.max_exemplars` an actual memory bound: one object's decoded
/// `EXEMPLARS` section can be far larger than the cap by itself, so the
/// rejection has to fire mid-section, not after the segment is done.
#[allow(clippy::too_many_arguments)]
async fn read_segment_exemplars(
    state: &ExemplarsState,
    tenant_hash: TenantHash,
    seg: &SegmentRef,
    matcher_sets: &[Vec<LabelMatcher>],
    start_ns: i64,
    end_ns: i64,
    accounting: &QueryAccounting,
    seen: &mut HashSet<ExemplarDedupKey>,
    collected: &mut Vec<(SeriesId, LabelSet, ExemplarRecord)>,
) -> Result<(), CollectError> {
    let data_object_key = seg.data_object_key.as_str();
    let limits = ReaderLimits::default();

    // One whole-object GET per matched segment, recorded at the same funnel
    // the sample fetcher uses (ADR-0044): request count, transferred bytes,
    // and one opened segment. A `NotFound` here means a pinned object vanished
    // under a concurrent publish-and-sweep: surface it as `SnapshotStale` so
    // the caller re-resolves once, exactly as the sample path does. Any other
    // store fault is fatal.
    let got = match state.store.get(data_object_key, GetRange::Full).await {
        Ok(got) => got,
        Err(StoreError::NotFound) => return Err(CollectError::SnapshotStale),
        Err(source) => {
            return Err(CollectError::Api(fetch_store_error(
                data_object_key,
                source,
            )));
        }
    };
    accounting.record_s3_request(AccountedOp::Get);
    accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
    accounting.add_segments_opened(1);
    let object = got.data;

    let loc = open_from_full(&object, limits).map_err(|source| corrupt(data_object_key, source))?;
    let footer = &loc.footer;

    // ADR-0010 section 7 footer identity check, the same one the sample path
    // performs in `SegmentFetcher::open_segment`. Key-prefix reconstruction
    // above catches a wrong-prefix object; this catches an object whose
    // *content* belongs to another tenant sitting under this tenant's key (a
    // writer or key-reconstruction fault). Without it `/api/v1/query` returns
    // `Corrupt` on the same input while this endpoint would serve the foreign
    // exemplars, trace ids included. Level-aware, matching the fetcher.
    verify_segment_identity(footer, tenant_hash, seg)
        .map_err(|source| corrupt(data_object_key, source))?;

    // No EXEMPLARS section means the object has no exemplars (ADR-0047
    // decision 1); skip the catalog decode entirely.
    let Some((exemplars_bytes, exemplars_uncompressed)) =
        section_slice(&object, footer, SECTION_KIND_EXEMPLARS)
            .map_err(|e| corrupt(data_object_key, e))?
    else {
        return Ok(());
    };
    // LABEL_DICT is mandatory in a well-formed object; if it is somehow
    // absent, pass an empty slice and let `decode_exemplars_section` report
    // the typed `MissingSection` rather than guessing.
    let label_dict_bytes = section_slice(&object, footer, SECTION_KIND_LABEL_DICT)
        .map_err(|e| corrupt(data_object_key, e))?
        .map(|(bytes, _)| bytes)
        .unwrap_or(&[]);

    // The catalog maps each exemplar's `series_index` back to its series id
    // and labels. `decode_catalog_v5` returns entries in the object's sorted
    // SERIES_IDS order, which is exactly the index space the EXEMPLARS section
    // records reference.
    let entries: Vec<SeriesEntryV4> = decode_catalog_v5(footer, &object, limits)
        .map_err(|source| corrupt(data_object_key, source))?;

    // Precompute which series indices any selector matches, so each exemplar
    // record is a single set lookup rather than a re-match.
    let matched: Vec<bool> = entries
        .iter()
        .map(|e| {
            matcher_sets
                .iter()
                .any(|matchers| matches_series(matchers, &e.entry.labels))
        })
        .collect();
    // `series_matched` counts the series the selectors matched, not every
    // series the object happens to carry: the latter is a property of how
    // ingest packed the segment, not of what this query asked for, and
    // reporting it would inflate the ADR-0044 cost surface by the object's
    // whole cardinality on every fetch.
    accounting.add_series_matched(matched.iter().filter(|m| **m).count() as u64);

    // Whole-section decode: query_exemplars matches a *set* of series, and one
    // pass over the section filtered by `matched` is strictly fewer passes
    // than one early-exit `probe_exemplars_by_series` per matched series over
    // the same already-fetched bytes (see the issue #475 report).
    let records = decode_exemplars_section(footer, label_dict_bytes, exemplars_bytes, limits)
        .map_err(|source| corrupt(data_object_key, source))?;
    accounting.add_decompressed_bytes(exemplars_uncompressed);

    for rec in records {
        let idx = usize::try_from(rec.series_index).unwrap_or(usize::MAX);
        // A record's series_index is validated in range against the footer's
        // series_count by the decoder; entries has exactly that many rows, so
        // this lookup cannot miss on a well-formed object. Guard anyway.
        let Some(entry) = entries.get(idx) else {
            return Err(corrupt(
                data_object_key,
                ravel_segment::SegmentError::ExemplarSeriesIndexOutOfRange(rec.series_index),
            )
            .into());
        };
        if !matched[idx] {
            continue;
        }
        // Clamp to the query's time range by the exemplar's own timestamp
        // (Prometheus' [start, end] contract). This is independent of the
        // object-level pruning above: an exemplar's ts_ns can sit outside its
        // object's event bounds (#474 data fact 1).
        if rec.ts_ns < start_ns || rec.ts_ns > end_ns {
            continue;
        }
        // Dedup on the full record identity (see [`ExemplarDedupKey`]), keeping
        // the first occurrence (module doc, data fact 2). Only a byte-identical
        // record collapses; a record differing in span id, value, or attributes
        // is genuinely distinct and kept. A duplicate costs no budget and no
        // `LabelSet` clone.
        let dedup_key: ExemplarDedupKey = (
            entry.entry.series_id,
            rec.ts_ns,
            rec.trace_id,
            rec.span_id,
            rec.value.to_bits(),
            rec.attrs.clone(),
        );
        if !seen.insert(dedup_key) {
            continue;
        }
        // Reject before growing past the cap, the way the engine's
        // `max_series` does: the accumulation never exceeds the budget it is
        // being checked against.
        if collected.len() >= state.max_exemplars {
            return Err(too_many_exemplars(state.max_exemplars).into());
        }
        collected.push((entry.entry.series_id, entry.entry.labels.clone(), rec));
    }
    Ok(())
}

/// Groups collected exemplars by series. Deduplication on the exemplar's full
/// identity ([`ExemplarDedupKey`]) already happened during accumulation (see
/// the module doc, data fact 2), where it also keeps the result cap counting
/// distinct exemplars. Output is deterministic: series ordered by id,
/// exemplars within a series ordered by `(ts_ns, trace_id)`.
fn group_by_series(
    collected: Vec<(SeriesId, LabelSet, ExemplarRecord)>,
) -> Vec<ExemplarSeriesJson> {
    let mut series_order: Vec<SeriesId> = Vec::new();
    let mut by_series: HashMap<SeriesId, (LabelSet, Vec<ExemplarRecord>)> = HashMap::new();

    for (series_id, labels, rec) in collected {
        let entry = by_series.entry(series_id).or_insert_with(|| {
            series_order.push(series_id);
            (labels, Vec::new())
        });
        entry.1.push(rec);
    }

    // Deterministic series order by id bytes, independent of segment fetch
    // order and hash-map iteration order.
    series_order.sort_by_key(|s| s.0);

    // `series_order` holds exactly the keys of `by_series`, so the removal
    // cannot miss. It is expressed as a `filter_map` rather than an `expect`
    // anyway: this runs on a request path, where a panic takes the whole
    // process (and every co-tenant's in-flight query) down with it.
    series_order
        .into_iter()
        .filter_map(|series_id| {
            let (labels, mut records) = by_series.remove(&series_id)?;
            records.sort_by_key(|r| (r.ts_ns, r.trace_id));
            Some(ExemplarSeriesJson {
                series_labels: labels_to_map(&labels),
                exemplars: records.into_iter().map(exemplar_to_json).collect(),
            })
        })
        .collect()
}

/// Renders one decoded record into its Prometheus JSON entry: the exemplar's
/// own attributes become `labels`, plus `trace_id`/`span_id` under the
/// conventional keys when present (hex-encoded, all-zero omitted).
fn exemplar_to_json(rec: ExemplarRecord) -> ExemplarEntryJson {
    // The exemplar's own attributes become `labels`. A Prometheus exemplar
    // `labels` object is a unique-name label set (Prometheus models it as
    // `labels.Labels`) and a JSON object cannot carry a repeated key, so
    // collecting the writer's verbatim attribute list into a `BTreeMap`
    // resolves any duplicate name last-wins, deterministically in the writer's
    // stored order. Duplicate attribute names are discouraged by OTLP and
    // unrepresentable in this response shape either way; the query-time dedup
    // key keeps records with differing duplicates distinct even though they
    // render identically here.
    let mut labels: BTreeMap<String, String> = rec.attrs.into_iter().collect();
    // `trace_id`/`span_id` are reserved for the real ids. Strip any
    // tenant-supplied attribute of those names first, so a fake one can never
    // survive into the response: Grafana follows the `trace_id` label to a
    // trace, and an all-zero (absent) real id would otherwise leave the
    // tenant's value in place, sending the operator to a trace that is not the
    // exemplar's. The real id is then inserted only when present; an all-zero
    // id is absent (W3C Trace Context reserves it), so its label is simply
    // omitted.
    labels.remove("trace_id");
    labels.remove("span_id");
    if !rec.trace_id.iter().all(|&b| b == 0) {
        labels.insert("trace_id".to_string(), hex_encode(&rec.trace_id));
    }
    if !rec.span_id.iter().all(|&b| b == 0) {
        labels.insert("span_id".to_string(), hex_encode(&rec.span_id));
    }
    ExemplarEntryJson {
        labels,
        value: format_value(rec.value),
        timestamp: Timestamp(rec.ts_ns as f64 / NS_PER_SEC),
    }
}

fn labels_to_map(labels: &LabelSet) -> BTreeMap<String, String> {
    labels
        .iter()
        .map(|l| (l.name.clone(), l.value.clone()))
        .collect()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(char::from_digit((b >> 4) as u32, 16).unwrap_or('0'));
        out.push(char::from_digit((b & 0x0f) as u32, 16).unwrap_or('0'));
    }
    out
}

/// The literal metric name a single equality `__name__` matcher pins, or
/// `None` if postings pruning must bypass (a regex/negation/absent `__name__`,
/// or more than one `__name__` matcher). Mirrors the engine's private
/// `equality_name_filter` (docs/metric-index-plan.md P5b) for the equality
/// case; unlike the engine's filter as of ADR-0061 decision 3, this one does
/// not recognize a literal-prefix-anchored regex, so this endpoint bypasses
/// pruning on a prefix regex a sample query over the same selector would
/// prune on. Safe (strictly more conservative, never a false negative), just
/// less effective; extending this to match is tracked as a follow-up.
fn equality_name_filter(matchers: &[LabelMatcher]) -> Option<&str> {
    let mut found: Option<&str> = None;
    for m in matchers {
        if m.name == METRIC_NAME_LABEL {
            match &m.op {
                MatchOp::Eq if found.is_none() => found = Some(m.value.as_str()),
                _ => return None,
            }
        }
    }
    found
}

/// The equality `__name__` filter shared by every selector, or `None` on any
/// disagreement. Mirrors the engine's `shared_equality_name_filter`: a filter
/// narrower than some selector's own matchers would drop segments that
/// selector still needs.
fn shared_equality_name_filter(matcher_sets: &[Vec<LabelMatcher>]) -> Option<String> {
    let mut shared: Option<&str> = None;
    for matchers in matcher_sets {
        let name = equality_name_filter(matchers)?;
        match shared {
            None => shared = Some(name),
            Some(s) if s == name => {}
            Some(_) => return None,
        }
    }
    shared.map(str::to_string)
}

// ---------------------------------------------------------------------------
// Segment identity verification (ADR-0010 section 7)
// ---------------------------------------------------------------------------

/// Verifies the opened footer's identity against the commit/compaction record
/// the [`SegmentRef`] was reconstructed from, the same level-aware check the
/// sample path runs in `SegmentFetcher::open_segment`
/// (docs/compaction-retention-plan.md section 3.5). An L0 ref checks the
/// footer's writer identity via [`check_identity`]; an L1 part has no writer
/// identity of its own, so tenant/shard/ingest_hour/input_set_hash/part_index
/// and `level == 1` are checked instead. Returns
/// [`SegmentError::IdentityMismatch`](ravel_segment::SegmentError) naming the
/// first mismatching field, which the caller maps to a `Corrupt` fetch error.
fn verify_segment_identity(
    footer: &Footer,
    tenant_hash: TenantHash,
    seg: &SegmentRef,
) -> Result<(), ravel_segment::SegmentError> {
    match &seg.level {
        SegmentLevel::L0 => {
            let expected = ExpectedIdentity {
                tenant_hash: tenant_hash.0,
                shard: seg.shard,
                writer_id: seg.writer_id.to_string(),
                writer_epoch: seg.writer_epoch,
                writer_seq: seg.writer_seq,
            };
            check_identity(footer, &expected)
        }
        SegmentLevel::L1 {
            input_set_hash,
            part_index,
        } => verify_l1_identity(footer, tenant_hash, seg, input_set_hash, *part_index),
    }
}

/// L1-part footer identity check, mirroring `ravel_query`'s crate-private
/// `verify_l1_identity` (which this endpoint cannot import). A part carries no
/// writer identity, so these five fields plus `level == 1` are its identity
/// (docs/compaction-retention-plan.md section 3.5).
fn verify_l1_identity(
    footer: &Footer,
    tenant_hash: TenantHash,
    seg: &SegmentRef,
    input_set_hash: &[u8; 32],
    part_index: u32,
) -> Result<(), ravel_segment::SegmentError> {
    if footer.tenant_hash.as_slice() != tenant_hash.0.as_slice() {
        return Err(ravel_segment::SegmentError::IdentityMismatch("tenant_hash"));
    }
    if footer.shard != seg.shard {
        return Err(ravel_segment::SegmentError::IdentityMismatch("shard"));
    }
    if footer.ingest_hour_bucket != seg.ingest_hour_bucket {
        return Err(ravel_segment::SegmentError::IdentityMismatch(
            "ingest_hour_bucket",
        ));
    }
    if footer.input_set_hash.as_slice() != input_set_hash.as_slice() {
        return Err(ravel_segment::SegmentError::IdentityMismatch(
            "input_set_hash",
        ));
    }
    if footer.part_index != part_index {
        return Err(ravel_segment::SegmentError::IdentityMismatch("part_index"));
    }
    if footer.level != 1 {
        return Err(ravel_segment::SegmentError::IdentityMismatch("level"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Segment section access
// ---------------------------------------------------------------------------

/// Finds the section of `kind` in the footer and returns its stored byte
/// slice (as sliced from the whole object by the descriptor's offset/len,
/// which is exactly the form `decode_exemplars_section`/`decode_catalog_v5`'s
/// section decoders expect) together with its declared uncompressed length,
/// or `None` when the section is absent.
///
/// The `Section` proto element type is deliberately never named: iterating
/// `footer.sections` and reading the public `kind`/`offset`/`len`/
/// `uncompressed_len` fields lets rustc infer it, so this stays out of a
/// direct `ravel-proto` dependency.
fn section_slice<'a>(
    object: &'a [u8],
    footer: &ravel_segment::Footer,
    kind: u32,
) -> Result<Option<(&'a [u8], u64)>, ravel_segment::SegmentError> {
    let Some(section) = footer.sections.iter().find(|s| s.kind == kind) else {
        return Ok(None);
    };
    let offset = usize::try_from(section.offset)
        .map_err(|_| ravel_segment::SegmentError::SectionOutOfBounds)?;
    let len = usize::try_from(section.len)
        .map_err(|_| ravel_segment::SegmentError::SectionOutOfBounds)?;
    let end = offset
        .checked_add(len)
        .ok_or(ravel_segment::SegmentError::SectionOutOfBounds)?;
    let slice = object
        .get(offset..end)
        .ok_or(ravel_segment::SegmentError::SectionOutOfBounds)?;
    Ok(Some((slice, section.uncompressed_len)))
}

// ---------------------------------------------------------------------------
// Store/segment error mapping
// ---------------------------------------------------------------------------

fn fetch_store_error(key: &str, source: StoreError) -> ApiError {
    ApiError::from_query(QueryError::Fetch(ravel_query::FetchError::Store {
        key: key.to_string(),
        source,
    }))
}

fn corrupt(key: &str, source: ravel_segment::SegmentError) -> ApiError {
    ApiError::from_query(QueryError::Fetch(ravel_query::FetchError::Corrupt {
        key: key.to_string(),
        source,
    }))
}

/// The result-cap rejection: the shape `TooManySegments` and the engine's
/// `TooManySeries`/`TooManySamples` use, an error rather than a truncated
/// 200. `ravel-query`'s `QueryError` has no exemplar variant and adding one
/// belongs to that crate, so the status contract (422, `errorType`
/// `execution`) is sourced from the sibling `TooManySamples` budget class
/// through the same shared mapping every other error here goes through, and
/// only the message is exemplar-specific. `count` reads `max + 1` because the
/// walk stops at the first exemplar over the line rather than counting the
/// rest: the exact total is unknown, and materializing it is the thing the cap
/// exists to prevent.
fn too_many_exemplars(max: usize) -> ApiError {
    let mut err = ApiError::from_query(QueryError::TooManySamples {
        count: max.saturating_add(1),
        max,
    });
    err.message = format!(
        "query matched more than {max} exemplars, exceeding the limit of {max}; \
         narrow the selector or the time range"
    );
    err
}

// ---------------------------------------------------------------------------
// Parameter parsing (form-encoded, reproduced from ravel-query's private
// http::params so the semantics match without importing a private module).
// ---------------------------------------------------------------------------

struct Params {
    values: HashMap<String, Vec<String>>,
}

impl Params {
    fn parse(query_string: Option<&str>, body: Option<&[u8]>) -> Self {
        let mut values = query_string.map(parse_form_encoded).unwrap_or_default();
        if let Some(body) = body {
            let body_str = String::from_utf8_lossy(body);
            for (k, vs) in parse_form_encoded(&body_str) {
                values.entry(k).or_default().extend(vs);
            }
        }
        Params { values }
    }

    fn first(&self, key: &str) -> Option<&str> {
        self.values
            .get(key)
            .and_then(|v| v.first())
            .map(String::as_str)
    }

    fn all(&self, key: &str) -> &[String] {
        self.values.get(key).map(Vec::as_slice).unwrap_or(&[])
    }

    fn require(&self, key: &'static str) -> Result<&str, ApiError> {
        self.first(key)
            .ok_or_else(|| ApiError::bad_request(format!("missing required parameter {key:?}")))
    }
}

fn parse_form_encoded(s: &str) -> HashMap<String, Vec<String>> {
    let mut out: HashMap<String, Vec<String>> = HashMap::new();
    for pair in s.split('&') {
        if pair.is_empty() {
            continue;
        }
        let mut parts = pair.splitn(2, '=');
        let key = percent_decode(parts.next().unwrap_or(""));
        let value = percent_decode(parts.next().unwrap_or(""));
        out.entry(key).or_default().push(value);
    }
    out
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 3 <= bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    None => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Prometheus float seconds or an RFC3339 instant, yielding nanoseconds.
fn parse_timestamp_ns(name: &'static str, s: &str) -> Result<i64, ApiError> {
    let ms = if let Ok(secs) = s.parse::<f64>() {
        seconds_to_ms(name, s, secs)?
    } else {
        let system_time =
            humantime::parse_rfc3339(s).map_err(|_| ApiError::invalid_param(name, s))?;
        let dur = system_time
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| ApiError::invalid_param(name, s))?;
        i64::try_from(dur.as_millis()).map_err(|_| ApiError::invalid_param(name, s))?
    };
    ms.checked_mul(NS_PER_MS)
        .ok_or_else(|| ApiError::invalid_param(name, s))
}

fn seconds_to_ms(name: &'static str, raw: &str, secs: f64) -> Result<i64, ApiError> {
    if !secs.is_finite() {
        return Err(ApiError::invalid_param(name, raw));
    }
    let ms = secs * 1000.0;
    if ms < i64::MIN as f64 || ms > i64::MAX as f64 {
        return Err(ApiError::invalid_param(name, raw));
    }
    Ok(ms as i64)
}

fn ns_to_ms(_name: &'static str, ns: i64) -> Result<i64, ApiError> {
    // The nanosecond values came from millisecond inputs (`parse_timestamp_ns`
    // multiplies by NS_PER_MS), so this division is exact.
    Ok(ns / NS_PER_MS)
}

/// Resolves the per-request wall deadline: the client `timeout` clamped to the
/// server maximum (can only lower it), or the server maximum when absent.
fn parse_deadline(params: &Params, max: Duration) -> Result<Duration, ApiError> {
    match params.first("timeout") {
        Some(s) => {
            let ms = parse_duration_ms("timeout", s)?;
            if ms <= 0 {
                return Err(ApiError::invalid_param("timeout", s));
            }
            Ok(Duration::from_millis(ms as u64).min(max))
        }
        None => Ok(max),
    }
}

fn parse_duration_ms(name: &'static str, s: &str) -> Result<i64, ApiError> {
    if let Ok(secs) = s.parse::<f64>() {
        return seconds_to_ms(name, s, secs);
    }
    let dur = humantime::parse_duration(s).map_err(|_| ApiError::invalid_param(name, s))?;
    i64::try_from(dur.as_millis()).map_err(|_| ApiError::invalid_param(name, s))
}

// ---------------------------------------------------------------------------
// Response shape
// ---------------------------------------------------------------------------

/// The response envelope: Prometheus renders `data` as a bare array for
/// `/api/v1/query_exemplars` (unlike `/api/v1/query`, whose `data` is an
/// object with `resultType`/`result`), so this query's cost counters ride as a
/// sibling
/// `stats` object rather than inside `data` (where `/api/v1/query` puts
/// theirs, its `data` being an object). Additive: a client that reads only
/// `status` and `data`, which is every Prometheus-shaped client including
/// Grafana, is unaffected.
#[derive(Debug, Serialize)]
struct Envelope {
    status: &'static str,
    data: Vec<ExemplarSeriesJson>,
    stats: QueryStatsJson,
}

impl Envelope {
    fn success(data: Vec<ExemplarSeriesJson>, stats: QueryStatsJson) -> Self {
        Envelope {
            status: "success",
            data,
            stats,
        }
    }
}

/// This request's segment counters and cost accounting (ADR-0044 decision 1).
/// Mirrors `ravel_query::http`'s private `json::QueryStatsJson` field for
/// field, minus the two parts that do not exist on this path: `estimate`
/// (`estimate_cost` is private to `ravel-query`, and an estimate is only ever
/// computed from a real resolved snapshot, never assumed) and the
/// `rawF64Pages`/`rawF64Bytes` page counters (this endpoint reads whole
/// objects and decodes one `EXEMPLARS` section, never a value page, so they
/// would be a permanently-zero field, which `ravel-query`'s own accounting
/// JSON explicitly refuses to carry).
#[derive(Debug, Default, Serialize)]
struct QueryStatsJson {
    #[serde(rename = "segmentsFetched")]
    segments_fetched: u64,
    #[serde(rename = "segmentsPruned")]
    segments_pruned: u64,
    accounting: QueryAccountingJson,
}

/// Actual per-request store/decode counters, the ADR-0044 cost surface. Field
/// names match `/api/v1/query`'s `stats.accounting` exactly, so one collector
/// reads both endpoints.
#[derive(Debug, Default, Serialize)]
struct QueryAccountingJson {
    #[serde(rename = "s3GetRequests")]
    s3_get_requests: u64,
    #[serde(rename = "s3GetBytes")]
    s3_get_bytes: u64,
    #[serde(rename = "s3ListRequests")]
    s3_list_requests: u64,
    #[serde(rename = "s3ListBytes")]
    s3_list_bytes: u64,
    #[serde(rename = "s3HeadRequests")]
    s3_head_requests: u64,
    #[serde(rename = "s3HeadBytes")]
    s3_head_bytes: u64,
    #[serde(rename = "cacheHits")]
    cache_hits: u64,
    #[serde(rename = "cacheMisses")]
    cache_misses: u64,
    #[serde(rename = "cacheBytes")]
    cache_bytes: u64,
    #[serde(rename = "decompressedBytes")]
    decompressed_bytes: u64,
    #[serde(rename = "segmentsOpened")]
    segments_opened: u64,
    #[serde(rename = "seriesMatched")]
    series_matched: u64,
    #[serde(rename = "bytesReused")]
    bytes_reused: u64,
    #[serde(rename = "peakIntermediateBytes")]
    peak_intermediate_bytes: u64,
}

impl QueryAccountingJson {
    fn from_snapshot(snapshot: &QueryAccountingSnapshot) -> Self {
        QueryAccountingJson {
            s3_get_requests: snapshot.s3_requests(AccountedOp::Get),
            s3_get_bytes: snapshot.s3_bytes(AccountedOp::Get),
            s3_list_requests: snapshot.s3_requests(AccountedOp::List),
            s3_list_bytes: snapshot.s3_bytes(AccountedOp::List),
            s3_head_requests: snapshot.s3_requests(AccountedOp::Head),
            s3_head_bytes: snapshot.s3_bytes(AccountedOp::Head),
            cache_hits: snapshot.cache_hits,
            cache_misses: snapshot.cache_misses,
            cache_bytes: snapshot.cache_bytes,
            decompressed_bytes: snapshot.decompressed_bytes,
            segments_opened: snapshot.segments_opened,
            series_matched: snapshot.series_matched,
            bytes_reused: snapshot.bytes_reused,
            peak_intermediate_bytes: snapshot.peak_intermediate_bytes,
        }
    }
}

#[derive(Debug, Serialize)]
struct ExemplarSeriesJson {
    #[serde(rename = "seriesLabels")]
    series_labels: BTreeMap<String, String>,
    exemplars: Vec<ExemplarEntryJson>,
}

#[derive(Debug, Serialize)]
struct ExemplarEntryJson {
    labels: BTreeMap<String, String>,
    value: String,
    timestamp: Timestamp,
}

/// A result timestamp, rendered in JSON exactly as Prometheus' Go encoder
/// renders it (ADR-0021): a whole-second value is a bare integer, a
/// fractional value keeps its fractional part. Mirrors `ravel_query`'s
/// private `http::json::Timestamp`, which is not exported.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Timestamp(f64);

impl Serialize for Timestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.0.is_finite() && self.0.fract() == 0.0 && self.0.abs() < 2f64.powi(53) {
            serializer.serialize_i64(self.0 as i64)
        } else {
            serializer.serialize_f64(self.0)
        }
    }
}

/// Prometheus renders a sample value as a JSON string, so full f64 precision
/// survives round-tripping through any JSON library. Every finite bit pattern
/// (including `-0.0` and subnormals) round-trips exactly through Rust's
/// shortest-round-trippable `Display`; `NaN`/`±Inf` render as Prometheus'
/// textual forms. Mirrors `ravel_query`'s private `http::json::format_value`.
fn format_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        format!("{v}")
    }
}

// ---------------------------------------------------------------------------
// Error boundary
// ---------------------------------------------------------------------------

/// A client-visible error: a status, a stable type tag, and a message that has
/// already passed the redaction boundary (storage faults are redacted by
/// `QueryErrorResponse`, never echoed).
#[derive(Debug)]
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

    fn invalid_param(name: &str, value: &str) -> Self {
        ApiError::bad_request(format!("invalid value for parameter {name:?}: {value:?}"))
    }

    /// Maps a `QueryError` through `ravel-query`'s public HTTP mapping, so this
    /// endpoint keeps the exact status contract of `/api/v1/query`, including
    /// the redaction of storage-layer faults, from one shared source.
    fn from_query(err: QueryError) -> Self {
        let QueryErrorResponse {
            status,
            error_type,
            message,
        } = QueryErrorResponse::from_query_error(err);
        ApiError {
            status,
            error_type,
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

/// Submit one query-audit event for an executed exemplar query and await its
/// durability before the response is released (ADR-0062 §2a). `language` is
/// `exemplars` so the record shape stays one schema across surfaces. On a
/// submission failure the request fails closed with a retryable 503
/// (`audit_mode=required`); in best-effort mode the pipeline resolves it to
/// `Ok`.
async fn submit_audit(
    state: &ExemplarsState,
    tenant_hash: TenantHash,
    now_ns: i64,
    query_text: &str,
    status: QueryStatus,
    window_start_ns: i64,
    window_end_ns: i64,
) -> Result<(), ApiError> {
    let event = query_audit_event(
        &tenant_hash,
        now_ns,
        query_text,
        "exemplars",
        status,
        window_start_ns,
        window_end_ns,
    );
    state.audit_sink.submit(event).await.map_err(|_| ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        error_type: "unavailable",
        message: "query audit is temporarily unavailable; retry".to_string(),
    })
}

fn authenticate(state: &ExemplarsState, headers: &HeaderMap) -> Result<TenantHash, ApiError> {
    state
        .tenant_resolver
        .resolve(headers)
        .map(|tenant| tenant.hash())
        .map_err(|_| ApiError {
            status: StatusCode::UNAUTHORIZED,
            error_type: "unauthorized",
            message: "authentication required".to_string(),
        })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::collections::HashMap;

    use axum::body::Body;
    use axum::http::Request;
    use ravel_catalog::{Catalog, CatalogConfig};
    use ravel_commit::publish::RetryPolicy;
    use ravel_commit::record::NewCommitRecord;
    use ravel_commit::{keys, publish, record};
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_query::http::StaticBearerTokenResolver;
    use ravel_segment::{
        ExemplarInput, IngestBounds, SegmentIdentity, SegmentWriter, SeriesInputV3, SeriesValues,
        WrittenSegment,
    };
    use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};
    use serde_json::Value;
    use tower::ServiceExt;
    use uuid::Uuid;

    use super::*;

    const NS_PER_SEC: i64 = 1_000_000_000;
    const NS_PER_HOUR: i64 = 3600 * NS_PER_SEC;
    /// A fixed "now" so timestamps, hour buckets, and the injected clock all
    /// agree without touching the real clock (CLAUDE.md: time is injected).
    const NOW: i64 = 1_700_000_000 * NS_PER_SEC;
    const TOKEN: &str = "test-token";
    /// A second tenant's bearer token, registered on the same router as
    /// `TOKEN`, so isolation is tested against a resolver that *could* have
    /// returned the other tenant rather than one that only knows one.
    const TOKEN_B: &str = "test-token-b";

    struct FixedClock(i64);
    impl Clock for FixedClock {
        fn now_ns(&self) -> i64 {
            self.0
        }
    }

    fn tenant() -> TenantId {
        TenantId::new("tenant-a".to_string())
    }

    fn tenant_b() -> TenantId {
        TenantId::new("tenant-b".to_string())
    }

    fn labels(metric: &str, extra: &[(&str, &str)]) -> LabelSet {
        let mut pairs = vec![Label {
            name: "__name__".to_string(),
            value: metric.to_string(),
        }];
        for (k, v) in extra {
            pairs.push(Label {
                name: (*k).to_string(),
                value: (*v).to_string(),
            });
        }
        LabelSet::new(pairs).expect("valid labels")
    }

    fn scalar_series(
        tenant_id: &TenantId,
        metric: &str,
        extra: &[(&str, &str)],
        samples: &[(i64, f64)],
    ) -> (SeriesId, SeriesInputV3) {
        let label_set = labels(metric, extra);
        let series_id = SeriesId::compute(tenant_id, metric, &label_set).expect("series id");
        let values = SeriesValues::Scalar(
            samples
                .iter()
                .map(|(ts_ns, value)| Sample {
                    ts_ns: *ts_ns,
                    value: *value,
                })
                .collect(),
        );
        (
            series_id,
            SeriesInputV3 {
                series_id,
                labels: label_set,
                values,
            },
        )
    }

    fn exemplar(
        series_id: SeriesId,
        ts_ns: i64,
        value: f64,
        trace_id: [u8; 16],
        span_id: [u8; 8],
        attrs: &[(&str, &str)],
    ) -> ExemplarInput {
        ExemplarInput {
            series_id,
            ts_ns,
            value,
            trace_id,
            span_id,
            attrs: attrs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        }
    }

    /// Writes a real RSEG v6 segment (with an EXEMPLARS section when
    /// `exemplars` is non-empty) and publishes its commit record onto `store`,
    /// exactly the way the ingest flush path does.
    async fn publish_segment(
        store: &MemoryStore,
        writer_seq: u64,
        series: Vec<SeriesInputV3>,
        exemplars: Vec<ExemplarInput>,
    ) {
        publish_segment_for(store, &tenant(), writer_seq, series, exemplars).await
    }

    /// As `publish_segment`, but for an explicit tenant, so a test can put two
    /// tenants' exemplars into one store.
    async fn publish_segment_for(
        store: &MemoryStore,
        tenant_id: &TenantId,
        writer_seq: u64,
        series: Vec<SeriesInputV3>,
        exemplars: Vec<ExemplarInput>,
    ) {
        let tenant_hash = tenant_id.hash();
        let shard = 0u32;
        let writer_id = Uuid::new_v4();
        let hour_bucket = u32::try_from(NOW / NS_PER_HOUR).expect("hour bucket");

        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: NOW,
            max_ingest_ts_ns: NOW,
        };
        let written: WrittenSegment =
            SegmentWriter::write_histograms_with_exemplars(series, identity, bounds, exemplars)
                .expect("write segment");

        let new_record = NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard,
            writer_id,
            writer_epoch: 1,
            writer_seq,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: NOW,
            max_ingest_ts_ns: NOW,
            segment_format_version: 6,
            created_unix_ns: NOW,
            ingest_hour_bucket: hour_bucket,
        };
        let rec = record::build(new_record).expect("valid commit record");
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        publish::put_data_object(store, &data_key, written.bytes)
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }

    /// Writes a segment whose footer `SegmentIdentity` carries `footer_tenant`'s
    /// hash, but publishes it under `record_tenant`'s commit record and
    /// reconstructed data key. This is the shape of a writer or
    /// key-reconstruction fault: one tenant's content sitting under another
    /// tenant's key prefix. Every field except the footer tenant hash agrees
    /// with the record, so the ADR-0010 section 7 check fails on exactly that
    /// field (F5).
    async fn publish_segment_footer_mismatch(
        store: &MemoryStore,
        record_tenant: &TenantId,
        footer_tenant: &TenantId,
        writer_seq: u64,
        series: Vec<SeriesInputV3>,
        exemplars: Vec<ExemplarInput>,
    ) {
        let record_hash = record_tenant.hash();
        let shard = 0u32;
        let writer_id = Uuid::new_v4();
        let hour_bucket = u32::try_from(NOW / NS_PER_HOUR).expect("hour bucket");

        let identity = SegmentIdentity {
            // The foreign tenant hash: the whole point of the test.
            tenant_hash: footer_tenant.hash().0,
            shard,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: NOW,
            max_ingest_ts_ns: NOW,
        };
        let written: WrittenSegment =
            SegmentWriter::write_histograms_with_exemplars(series, identity, bounds, exemplars)
                .expect("write segment");

        let new_record = NewCommitRecord {
            tenant_hash: record_hash,
            signal: Signal::Metrics,
            shard,
            writer_id,
            writer_epoch: 1,
            writer_seq,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: NOW,
            max_ingest_ts_ns: NOW,
            segment_format_version: 6,
            created_unix_ns: NOW,
            ingest_hour_bucket: hour_bucket,
        };
        let rec = record::build(new_record).expect("valid commit record");
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        publish::put_data_object(store, &data_key, written.bytes)
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }

    /// Wraps a store and sleeps before every `get`/`list`, so a query issued
    /// under a tiny wall deadline actually yields past it. Lets the deadline
    /// test force a real elapsed timeout deterministically rather than racing
    /// the timer against an in-memory store that never yields.
    struct SlowStore {
        inner: Arc<MemoryStore>,
        delay: Duration,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for SlowStore {
        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: ravel_object_store::PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }
        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            tokio::time::sleep(self.delay).await;
            self.inner.get(key, range).await
        }
        async fn head(&self, key: &str) -> Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }
        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            tokio::time::sleep(self.delay).await;
            self.inner.list(prefix, page).await
        }
        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }
        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    fn build_router(store: Arc<MemoryStore>, deadline: Duration, max_segments: usize) -> Router {
        let backend: Arc<dyn ObjectStoreBackend> = store;
        build_router_with_store(backend, deadline, max_segments)
    }

    fn build_router_with_store(
        backend: Arc<dyn ObjectStoreBackend>,
        deadline: Duration,
        max_segments: usize,
    ) -> Router {
        build_router_full(backend, deadline, max_segments, DEFAULT_MAX_EXEMPLARS)
    }

    fn build_router_full(
        backend: Arc<dyn ObjectStoreBackend>,
        deadline: Duration,
        max_segments: usize,
        max_exemplars: usize,
    ) -> Router {
        let catalog =
            Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
        let mut tokens = HashMap::new();
        tokens.insert(TOKEN.to_string(), tenant());
        tokens.insert(TOKEN_B.to_string(), tenant_b());
        let state = ExemplarsState {
            catalog,
            store: backend,
            tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
            clock: Arc::new(FixedClock(NOW)),
            deadline,
            max_segments,
            max_exemplars,
            audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
        };
        router(state)
    }

    /// Captures every submitted audit event and reports durability success.
    struct RecordingSink {
        events: Arc<std::sync::Mutex<Vec<ravel_maintain::AuditEvent>>>,
    }

    #[async_trait::async_trait]
    impl ravel_maintain::QueryAuditSink for RecordingSink {
        async fn submit(
            &self,
            event: ravel_maintain::AuditEvent,
        ) -> Result<(), ravel_maintain::MaintainError> {
            self.events.lock().expect("lock").push(event);
            Ok(())
        }
    }

    /// A router over `store` whose exemplar surface audits through a recording
    /// sink, returning the shared event log for assertion.
    fn build_router_recording(
        store: Arc<MemoryStore>,
    ) -> (
        Router,
        Arc<std::sync::Mutex<Vec<ravel_maintain::AuditEvent>>>,
    ) {
        let backend: Arc<dyn ObjectStoreBackend> = store;
        let catalog =
            Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
        let mut tokens = HashMap::new();
        tokens.insert(TOKEN.to_string(), tenant());
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let state = ExemplarsState {
            catalog,
            store: backend,
            tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
            clock: Arc::new(FixedClock(NOW)),
            deadline: Duration::from_secs(30),
            max_segments: 1000,
            max_exemplars: DEFAULT_MAX_EXEMPLARS,
            audit_sink: Arc::new(RecordingSink {
                events: Arc::clone(&events),
            }),
        };
        (router(state), events)
    }

    /// The value of a string `attrs` entry of an audit event.
    fn audit_attr<'a>(event: &'a ravel_maintain::AuditEvent, key: &str) -> Option<&'a str> {
        event
            .attrs
            .iter()
            .find(|(k, _)| k == key)
            .and_then(|(_, v)| match v {
                ravel_logseg::AttrValue::Str(s) => Some(s.as_str()),
                _ => None,
            })
    }

    /// Percent-encodes everything except unreserved characters so a PromQL
    /// selector survives as a query-string component.
    fn encode(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
        out
    }

    async fn query(app: &Router, promql: &str) -> (StatusCode, Value) {
        query_as(app, promql, Some(TOKEN)).await
    }

    /// Issues the query as the holder of `token`, or with no `Authorization`
    /// header at all when `token` is `None`.
    async fn query_as(app: &Router, promql: &str, token: Option<&str>) -> (StatusCode, Value) {
        let start = NOW - NS_PER_HOUR;
        let uri = format!(
            "/api/v1/query_exemplars?query={}&start={}&end={}",
            encode(promql),
            start as f64 / NS_PER_SEC as f64,
            NOW as f64 / NS_PER_SEC as f64,
        );
        let mut builder = Request::builder().method("GET").uri(uri);
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        let request = builder.body(Body::empty()).expect("build request");
        let response = app.clone().oneshot(request).await.expect("oneshot");
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: Value = serde_json::from_slice(&body).expect("parse json");
        (status, json)
    }

    const TRACE_ID: [u8; 16] = [
        0x0a, 0x1b, 0x2c, 0x3d, 0x4e, 0x5f, 0x60, 0x71, 0x82, 0x93, 0xa4, 0xb5, 0xc6, 0xd7, 0xe8,
        0xf9,
    ];
    const SPAN_ID: [u8; 8] = [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];

    /// The acceptance test (issue #475, this exact path and name): a query
    /// returns the Prometheus shape, and the exemplar's trace and span ids
    /// ride in its labels under the conventional hex-encoded keys.
    #[tokio::test]
    async fn query_exemplars_returns_prometheus_shape_with_trace_labels() {
        let store = Arc::new(MemoryStore::new());
        let (sid, series) = scalar_series(
            &tenant(),
            "http_request_duration_seconds",
            &[("method", "get")],
            &[(NOW - 60 * NS_PER_SEC, 0.25)],
        );
        let ex = exemplar(
            sid,
            NOW - 30 * NS_PER_SEC,
            0.25,
            TRACE_ID,
            SPAN_ID,
            &[("region", "us-east-1")],
        );
        publish_segment(&store, 1, vec![series], vec![ex]).await;

        let app = build_router(store, Duration::from_secs(30), 1000);
        let (status, body) = query(&app, "http_request_duration_seconds{method=\"get\"}").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "success");

        let data = body["data"].as_array().expect("data array");
        assert_eq!(data.len(), 1, "one matched series");
        let series0 = &data[0];
        assert_eq!(
            series0["seriesLabels"]["__name__"],
            "http_request_duration_seconds"
        );
        assert_eq!(series0["seriesLabels"]["method"], "get");

        let exemplars = series0["exemplars"].as_array().expect("exemplars array");
        assert_eq!(exemplars.len(), 1);
        let ex0 = &exemplars[0];
        // trace_id and span_id ride in the exemplar labels, hex-encoded: this
        // is what Grafana's exemplar-to-trace link reads.
        assert_eq!(
            ex0["labels"]["trace_id"], "0a1b2c3d4e5f60718293a4b5c6d7e8f9",
            "trace id present, hex-encoded, in the exemplar labels"
        );
        assert_eq!(ex0["labels"]["span_id"], "1122334455667788");
        assert_eq!(ex0["labels"]["region"], "us-east-1");
        assert_eq!(ex0["value"], "0.25");
        // Timestamp is float seconds, whole-second values as bare integers.
        assert_eq!(ex0["timestamp"], (NOW - 30 * NS_PER_SEC) / NS_PER_SEC);
    }

    /// An all-zero trace id means absent: the label is omitted rather than
    /// emitting 32 zeros (W3C Trace Context reserves the all-zero id).
    #[tokio::test]
    async fn all_zero_trace_id_omits_the_label() {
        let store = Arc::new(MemoryStore::new());
        let (sid, series) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        // Zero trace id, but a real span id, to prove the two are independent.
        let ex = exemplar(sid, NOW - 30 * NS_PER_SEC, 1.0, [0u8; 16], SPAN_ID, &[]);
        publish_segment(&store, 1, vec![series], vec![ex]).await;

        let app = build_router(store, Duration::from_secs(30), 1000);
        let (status, body) = query(&app, "m").await;
        assert_eq!(status, StatusCode::OK);
        let ex0 = &body["data"][0]["exemplars"][0];
        assert!(
            ex0["labels"].get("trace_id").is_none(),
            "an all-zero trace id must not emit a label"
        );
        assert_eq!(ex0["labels"]["span_id"], "1122334455667788");
    }

    /// A query matching no series returns an empty array with 200, not an
    /// error.
    #[tokio::test]
    async fn no_matching_series_returns_empty_array_with_200() {
        let store = Arc::new(MemoryStore::new());
        let (sid, series) =
            scalar_series(&tenant(), "present", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        let ex = exemplar(sid, NOW - 30 * NS_PER_SEC, 1.0, TRACE_ID, SPAN_ID, &[]);
        publish_segment(&store, 1, vec![series], vec![ex]).await;

        let app = build_router(store, Duration::from_secs(30), 1000);
        let (status, body) = query(&app, "absent_metric").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "success");
        assert_eq!(
            body["data"].as_array().expect("array").len(),
            0,
            "no matched series is an empty data array, not an error"
        );
    }

    /// Value bit patterns survive the JSON round trip: a NaN and a negative
    /// zero come back with their exact bit pattern. Compared with
    /// `f64::to_bits`, never `==`.
    #[tokio::test]
    async fn value_bit_patterns_survive_json_round_trip() {
        let store = Arc::new(MemoryStore::new());
        let (sid_nan, s_nan) =
            scalar_series(&tenant(), "with_nan", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        let (sid_negzero, s_negzero) = scalar_series(
            &tenant(),
            "with_neg_zero",
            &[],
            &[(NOW - 60 * NS_PER_SEC, 1.0)],
        );
        let ex_nan = exemplar(
            sid_nan,
            NOW - 30 * NS_PER_SEC,
            f64::NAN,
            TRACE_ID,
            SPAN_ID,
            &[],
        );
        let ex_negzero = exemplar(
            sid_negzero,
            NOW - 30 * NS_PER_SEC,
            -0.0f64,
            TRACE_ID,
            SPAN_ID,
            &[],
        );
        publish_segment(&store, 1, vec![s_nan, s_negzero], vec![ex_nan, ex_negzero]).await;

        let app = build_router(store, Duration::from_secs(30), 1000);

        let (status, body) = query(&app, "with_nan").await;
        assert_eq!(status, StatusCode::OK);
        let v = body["data"][0]["exemplars"][0]["value"]
            .as_str()
            .expect("value string");
        let parsed: f64 = v.parse().expect("parse value");
        assert_eq!(
            parsed.to_bits(),
            f64::NAN.to_bits(),
            "NaN round-trips by bit pattern"
        );

        let (status, body) = query(&app, "with_neg_zero").await;
        assert_eq!(status, StatusCode::OK);
        let v = body["data"][0]["exemplars"][0]["value"]
            .as_str()
            .expect("value string");
        let parsed: f64 = v.parse().expect("parse value");
        assert_eq!(
            parsed.to_bits(),
            (-0.0f64).to_bits(),
            "negative zero round-trips by bit pattern, distinct from +0.0"
        );
    }

    /// An object with no EXEMPLARS section is normal: it contributes nothing
    /// and the query still succeeds.
    #[tokio::test]
    async fn object_without_exemplars_section_returns_nothing() {
        let store = Arc::new(MemoryStore::new());
        let (_sid, series) =
            scalar_series(&tenant(), "plain", &[], &[(NOW - 60 * NS_PER_SEC, 7.0)]);
        // No exemplars: write_histograms_with_exemplars emits no EXEMPLARS
        // section at all (ADR-0047 decision 1).
        publish_segment(&store, 1, vec![series], Vec::new()).await;

        let app = build_router(store, Duration::from_secs(30), 1000);
        let (status, body) = query(&app, "plain").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["data"].as_array().expect("array").len(),
            0,
            "a segment with no EXEMPLARS section yields no exemplars"
        );
    }

    /// The deadline path behaves as it does for `/api/v1/query`: a query that
    /// cannot finish within the wall deadline is a 504, from the same
    /// `QueryError::DeadlineExceeded` mapping. Forced deterministically with a
    /// store that sleeps past a tiny deadline rather than racing the timer.
    #[tokio::test]
    async fn exceeded_deadline_is_gateway_timeout() {
        let mem = Arc::new(MemoryStore::new());
        let (sid, series) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        let ex = exemplar(sid, NOW - 30 * NS_PER_SEC, 1.0, TRACE_ID, SPAN_ID, &[]);
        publish_segment(&mem, 1, vec![series], vec![ex]).await;

        let slow: Arc<dyn ObjectStoreBackend> = Arc::new(SlowStore {
            inner: mem,
            delay: Duration::from_millis(200),
        });
        let app = build_router_with_store(slow, Duration::from_millis(1), 1000);
        let (status, body) = query(&app, "m").await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(body["status"], "error");
    }

    /// The `timeout` parameter behaves as it does for `/api/v1/query`: a
    /// non-positive value is a 400, rejected before any query runs.
    #[tokio::test]
    async fn non_positive_timeout_param_is_rejected() {
        let store = Arc::new(MemoryStore::new());
        let (sid, series) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        let ex = exemplar(sid, NOW - 30 * NS_PER_SEC, 1.0, TRACE_ID, SPAN_ID, &[]);
        publish_segment(&store, 1, vec![series], vec![ex]).await;

        let app = build_router(store, Duration::from_secs(30), 1000);
        let start = NOW - NS_PER_HOUR;
        let uri = format!(
            "/api/v1/query_exemplars?query=m&start={}&end={}&timeout=0",
            start as f64 / NS_PER_SEC as f64,
            NOW as f64 / NS_PER_SEC as f64,
        );
        let request = Request::builder()
            .method("GET")
            .uri(uri)
            .header("authorization", format!("Bearer {TOKEN}"))
            .body(Body::empty())
            .expect("build request");
        let response = app.clone().oneshot(request).await.expect("oneshot");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// The budget path behaves as it does for `/api/v1/query`: a resolved
    /// snapshot larger than `max_segments` is a 422, the same
    /// `TooManySegments` mapping, before any exemplar is read.
    #[tokio::test]
    async fn max_segments_budget_is_enforced() {
        let store = Arc::new(MemoryStore::new());
        let (sid, series) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        let ex = exemplar(sid, NOW - 30 * NS_PER_SEC, 1.0, TRACE_ID, SPAN_ID, &[]);
        publish_segment(&store, 1, vec![series], vec![ex]).await;

        let app = build_router(store, Duration::from_secs(30), 0);
        let (status, body) = query(&app, "m").await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["status"], "error");
    }

    /// Decision, data fact 1: an exemplar whose `ts_ns` falls outside the
    /// query's `[start, end]` is excluded by the query-time timestamp clamp,
    /// even though its parent object is fetched (the object's event bounds
    /// cover the in-window sample). Pins the deliberate choice not to widen or
    /// to return out-of-range exemplars.
    #[tokio::test]
    async fn exemplar_outside_time_range_is_excluded() {
        let store = Arc::new(MemoryStore::new());
        let (sid, series) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        // In-window exemplar and one an hour past `end` (outside [start, end]).
        let in_window = exemplar(sid, NOW - 30 * NS_PER_SEC, 1.0, TRACE_ID, SPAN_ID, &[]);
        let out_of_window = exemplar(sid, NOW + NS_PER_HOUR, 2.0, [0x01; 16], [0x02; 8], &[]);
        publish_segment(&store, 1, vec![series], vec![in_window, out_of_window]).await;

        let app = build_router(store, Duration::from_secs(30), 1000);
        let (status, body) = query(&app, "m").await;
        assert_eq!(status, StatusCode::OK);
        let exemplars = body["data"][0]["exemplars"]
            .as_array()
            .expect("exemplars array");
        assert_eq!(
            exemplars.len(),
            1,
            "only the in-window exemplar is returned"
        );
        assert_eq!(exemplars[0]["value"], "1");
    }

    /// F10: the query-time `[start, end]` clamp is inclusive at both ends. An
    /// exemplar at exactly `start_ns` and one at exactly `end_ns` are present;
    /// one at `start_ns - 1` and one at `end_ns + 1` are absent. Pins the
    /// boundary so flipping the clamp's `<`/`>` to `<=`/`>=` (or the reverse)
    /// fails a test rather than passing silently.
    #[tokio::test]
    async fn time_range_clamp_is_inclusive_at_both_boundaries() {
        let store = Arc::new(MemoryStore::new());
        // The `query` helper resolves exactly this window.
        let start_ns = NOW - NS_PER_HOUR;
        let end_ns = NOW;
        let (sid, series) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        let at_start = exemplar(sid, start_ns, 1.0, [0x01; 16], SPAN_ID, &[]);
        let at_end = exemplar(sid, end_ns, 1.0, [0x02; 16], SPAN_ID, &[]);
        let before_start = exemplar(sid, start_ns - 1, 1.0, [0x03; 16], SPAN_ID, &[]);
        let after_end = exemplar(sid, end_ns + 1, 1.0, [0x04; 16], SPAN_ID, &[]);
        publish_segment(
            &store,
            1,
            vec![series],
            vec![at_start, at_end, before_start, after_end],
        )
        .await;

        let app = build_router(store, Duration::from_secs(30), 1000);
        let (status, body) = query(&app, "m").await;
        assert_eq!(status, StatusCode::OK);
        let traces: Vec<&str> = body["data"][0]["exemplars"]
            .as_array()
            .expect("exemplars array")
            .iter()
            .map(|e| e["labels"]["trace_id"].as_str().expect("trace_id"))
            .collect();
        assert!(
            traces.contains(&"01010101010101010101010101010101"),
            "an exemplar at exactly start_ns is present: {body}"
        );
        assert!(
            traces.contains(&"02020202020202020202020202020202"),
            "an exemplar at exactly end_ns is present: {body}"
        );
        assert!(
            !traces.contains(&"03030303030303030303030303030303"),
            "an exemplar at start_ns - 1 is absent: {body}"
        );
        assert!(
            !traces.contains(&"04040404040404040404040404040404"),
            "an exemplar at end_ns + 1 is absent: {body}"
        );
        assert_eq!(
            traces.len(),
            2,
            "exactly the two boundary exemplars, no more: {body}"
        );
    }

    /// F10, the positive half of data fact 1: an exemplar whose `ts_ns` falls
    /// outside its object's `min_event_ts_ns`/`max_event_ts_ns` (set by the
    /// object's samples) but inside the query `[start, end]` is returned,
    /// because the object was fetched for its samples and every in-window
    /// exemplar in it is considered. This is the counterpart to
    /// `exemplar_outside_time_range_is_excluded`: object event bounds neither
    /// widen nor narrow the returned set; only the exemplar's own `ts_ns`
    /// against `[start, end]` does.
    #[tokio::test]
    async fn exemplar_outside_object_event_bounds_but_in_window_is_returned() {
        let store = Arc::new(MemoryStore::new());
        // A single sample pins the object's event bounds to [NOW-10s, NOW-10s].
        let (sid, series) = scalar_series(&tenant(), "m", &[], &[(NOW - 10 * NS_PER_SEC, 1.0)]);
        // The exemplar is 30s ago: earlier than the object's min_event_ts, so
        // outside its event bounds, yet well inside the query window [NOW-1h,
        // NOW].
        let ex = exemplar(sid, NOW - 30 * NS_PER_SEC, 1.0, TRACE_ID, SPAN_ID, &[]);
        publish_segment(&store, 1, vec![series], vec![ex]).await;

        let app = build_router(store, Duration::from_secs(30), 1000);
        let (status, body) = query(&app, "m").await;
        assert_eq!(status, StatusCode::OK);
        let exemplars = body["data"][0]["exemplars"]
            .as_array()
            .expect("exemplars array");
        assert_eq!(
            exemplars.len(),
            1,
            "an in-window exemplar outside the object's event bounds is returned: {body}"
        );
        assert_eq!(
            exemplars[0]["labels"]["trace_id"],
            "0a1b2c3d4e5f60718293a4b5c6d7e8f9"
        );
    }

    /// Decision, data fact 2: the same exemplar readable twice during
    /// ADR-0018's overlap window (an L1 part and its input both in the
    /// snapshot) is deduplicated on `(series, ts, trace_id)` and returned
    /// once; two exemplars that share `(series, ts)` but differ in trace id
    /// (a retried write, per ADR-0047's amendment) both survive.
    #[tokio::test]
    async fn duplicate_exemplars_are_deduped_on_series_ts_trace() {
        let store = Arc::new(MemoryStore::new());
        let ts = NOW - 30 * NS_PER_SEC;

        // Two objects, each carrying the identical exemplar for the same
        // (series, ts, trace_id): the overlap-window double.
        let (sid1, series1) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        let dup_a = exemplar(sid1, ts, 1.0, TRACE_ID, SPAN_ID, &[]);
        publish_segment(&store, 1, vec![series1], vec![dup_a]).await;

        let (sid2, series2) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        let dup_b = exemplar(sid2, ts, 1.0, TRACE_ID, SPAN_ID, &[]);
        // A genuinely distinct exemplar: same series and ts, different trace.
        let distinct = exemplar(sid2, ts, 1.0, [0x77; 16], SPAN_ID, &[]);
        publish_segment(&store, 2, vec![series2], vec![dup_b, distinct]).await;

        let app = build_router(store, Duration::from_secs(30), 1000);
        let (status, body) = query(&app, "m").await;
        assert_eq!(status, StatusCode::OK);
        let exemplars = body["data"][0]["exemplars"]
            .as_array()
            .expect("exemplars array");
        // The identical (series, ts, trace) appears in both objects but is
        // returned once; the different-trace exemplar survives: 2 total.
        assert_eq!(
            exemplars.len(),
            2,
            "overlap duplicate collapses, distinct trace survives"
        );
        let traces: Vec<&str> = exemplars
            .iter()
            .map(|e| e["labels"]["trace_id"].as_str().expect("trace_id"))
            .collect();
        assert!(traces.contains(&"0a1b2c3d4e5f60718293a4b5c6d7e8f9"));
        assert!(traces.contains(&"77777777777777777777777777777777"));
    }

    /// F9: two exemplars sharing `(series, ts, trace_id)` but differing in
    /// span id, value, or attributes are genuinely distinct records the writer
    /// preserved verbatim. The widened dedup key keeps all of them, where the
    /// old `(series, ts, trace)` key would have collapsed them to one; a
    /// byte-identical copy still collapses.
    #[tokio::test]
    async fn distinct_exemplars_sharing_series_ts_trace_all_survive() {
        let store = Arc::new(MemoryStore::new());
        let ts = NOW - 30 * NS_PER_SEC;
        let (sid, series) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        // All four share (series, ts, TRACE_ID) and differ in exactly one of
        // span id, value, or attributes.
        let base = exemplar(sid, ts, 1.0, TRACE_ID, SPAN_ID, &[]);
        let diff_span = exemplar(sid, ts, 1.0, TRACE_ID, [0x99; 8], &[]);
        let diff_value = exemplar(sid, ts, 2.0, TRACE_ID, SPAN_ID, &[]);
        let diff_attrs = exemplar(sid, ts, 1.0, TRACE_ID, SPAN_ID, &[("k", "v")]);
        // A byte-identical copy of `base`, which MUST still collapse.
        let dup_base = exemplar(sid, ts, 1.0, TRACE_ID, SPAN_ID, &[]);
        publish_segment(
            &store,
            1,
            vec![series],
            vec![base, diff_span, diff_value, diff_attrs, dup_base],
        )
        .await;

        let app = build_router(store, Duration::from_secs(30), 1000);
        let (status, body) = query(&app, "m").await;
        assert_eq!(status, StatusCode::OK);
        let exemplars = body["data"][0]["exemplars"]
            .as_array()
            .expect("exemplars array");
        assert_eq!(
            exemplars.len(),
            4,
            "four distinct records survive; the byte-identical duplicate collapses: {body}"
        );
    }

    /// A tenant-supplied attribute literally named `trace_id` must never reach
    /// the response in place of the real trace id: Grafana follows the
    /// `trace_id` label to a trace, and an all-zero (absent) real id must not
    /// leave the tenant's fake value behind. The `trace_id`/`span_id` label
    /// keys are reserved for the real ids.
    #[tokio::test]
    async fn tenant_supplied_trace_id_attribute_never_masquerades() {
        let store = Arc::new(MemoryStore::new());
        let (sid, series) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        // Real trace id is all-zero (absent); the tenant tries to inject a
        // fake one, plus a fake span id, through the attribute list.
        let ex = exemplar(
            sid,
            NOW - 30 * NS_PER_SEC,
            1.0,
            [0u8; 16],
            [0u8; 8],
            &[
                ("trace_id", "deadbeef"),
                ("span_id", "cafe"),
                ("region", "eu"),
            ],
        );
        publish_segment(&store, 1, vec![series], vec![ex]).await;

        let app = build_router(store, Duration::from_secs(30), 1000);
        let (status, body) = query(&app, "m").await;
        assert_eq!(status, StatusCode::OK);
        let labels = &body["data"][0]["exemplars"][0]["labels"];
        assert!(
            labels.get("trace_id").is_none(),
            "a tenant trace_id attribute must not survive when the real id is absent: {body}"
        );
        assert!(
            labels.get("span_id").is_none(),
            "a tenant span_id attribute must not survive when the real id is absent: {body}"
        );
        // A genuine, non-reserved attribute is untouched.
        assert_eq!(labels["region"], "eu");
    }

    /// Tenant isolation, the endpoint's most important property: two tenants
    /// publish exemplars under the *same* metric name into the same store,
    /// both bearer tokens are registered on the same router, and each caller
    /// sees only its own trace ids. Cross-tenant leakage here would hand one
    /// customer another customer's trace ids, which are the whole payload of
    /// this endpoint.
    #[tokio::test]
    async fn each_tenant_sees_only_its_own_exemplars() {
        const TRACE_A: [u8; 16] = [0xaa; 16];
        const TRACE_B: [u8; 16] = [0xbb; 16];
        const TRACE_A_HEX: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        const TRACE_B_HEX: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        let store = Arc::new(MemoryStore::new());

        // Same metric name, same label set, same timestamp for both tenants:
        // only the tenant scoping can tell the two apart. (The series ids
        // differ because `SeriesId::compute` mixes in the tenant, which is
        // exactly the property under test at the storage layer; the query
        // layer must not depend on that to keep them apart.)
        let (sid_a, series_a) = scalar_series(
            &tenant(),
            "shared_metric",
            &[("route", "/x")],
            &[(NOW - 60 * NS_PER_SEC, 1.0)],
        );
        let ex_a = exemplar(sid_a, NOW - 30 * NS_PER_SEC, 1.0, TRACE_A, SPAN_ID, &[]);
        publish_segment_for(&store, &tenant(), 1, vec![series_a], vec![ex_a]).await;

        let (sid_b, series_b) = scalar_series(
            &tenant_b(),
            "shared_metric",
            &[("route", "/x")],
            &[(NOW - 60 * NS_PER_SEC, 2.0)],
        );
        let ex_b = exemplar(sid_b, NOW - 30 * NS_PER_SEC, 2.0, TRACE_B, SPAN_ID, &[]);
        publish_segment_for(&store, &tenant_b(), 1, vec![series_b], vec![ex_b]).await;

        let app = build_router(store, Duration::from_secs(30), 1000);

        for (token, own, other) in [
            (TOKEN, TRACE_A_HEX, TRACE_B_HEX),
            (TOKEN_B, TRACE_B_HEX, TRACE_A_HEX),
        ] {
            let (status, body) = query_as(&app, "shared_metric", Some(token)).await;
            assert_eq!(status, StatusCode::OK);
            let data = body["data"].as_array().expect("data array");
            assert_eq!(
                data.len(),
                1,
                "{token} must see exactly its own one series, got {body}"
            );
            let traces: Vec<&str> = data
                .iter()
                .flat_map(|s| s["exemplars"].as_array().expect("exemplars array"))
                .map(|e| e["labels"]["trace_id"].as_str().expect("trace_id"))
                .collect();
            assert_eq!(traces, vec![own], "{token} sees only its own trace id");
            assert!(
                !traces.contains(&other),
                "{token} must never see the other tenant's trace id"
            );
        }
    }

    /// F7: a store `NotFound` on the first data-object GET is retried once.
    /// A pinned segment can vanish under a concurrent L0-to-L1 publish and
    /// sweep (continuous normal compaction); the sample path re-resolves once
    /// before giving up, and this endpoint must too. Here the first `.rseg`
    /// GET returns `NotFound` and the re-resolve's GET succeeds, so the request
    /// is a 200 rather than the 503 a single attempt would return. The fault
    /// counter proves the injected fault actually fired.
    #[tokio::test]
    async fn not_found_on_first_get_re_resolves_and_succeeds() {
        let mem = MemoryStore::new();
        let (sid, series) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        let ex = exemplar(sid, NOW - 30 * NS_PER_SEC, 1.0, TRACE_ID, SPAN_ID, &[]);
        publish_segment(&mem, 1, vec![series], vec![ex]).await;

        // Fail only the first GET of the `.rseg` data object (commit records
        // end in `.cmt`, so the resolve's own reads are untouched); the retry's
        // GET falls through to the real object.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
                .with_key_contains(".rseg")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let faulted = Arc::new(FaultStore::new(mem, plan));
        let counter = faulted.clone();
        let backend: Arc<dyn ObjectStoreBackend> = faulted;

        let app = build_router_with_store(backend, Duration::from_secs(30), 1000);
        let (status, body) = query(&app, "m").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "the re-resolve after NotFound must succeed: {body}"
        );
        assert_eq!(
            body["data"][0]["exemplars"]
                .as_array()
                .expect("array")
                .len(),
            1,
            "the exemplar is served on the retried attempt: {body}"
        );
        assert_eq!(
            counter.fault_count(Op::Get, FaultKind::NotFoundBlip),
            1,
            "the injected NotFound must have fired exactly once"
        );
    }

    /// F5: the ADR-0010 section 7 footer identity check. An object whose
    /// footer `SegmentIdentity` carries a foreign tenant hash, published under
    /// this tenant's key, must error rather than serve its exemplars.
    /// Key-prefix reconstruction alone would hand the foreign trace ids to this
    /// caller; the content-level identity check stops it, exactly as
    /// `/api/v1/query` returns `Corrupt` on the same input.
    #[tokio::test]
    async fn foreign_tenant_footer_is_rejected_not_served() {
        let store = Arc::new(MemoryStore::new());
        let (sid, series) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        let ex = exemplar(sid, NOW - 30 * NS_PER_SEC, 1.0, TRACE_ID, SPAN_ID, &[]);
        // Footer says tenant-b; commit record and data key say tenant-a.
        publish_segment_footer_mismatch(&store, &tenant(), &tenant_b(), 1, vec![series], vec![ex])
            .await;

        let app = build_router(store, Duration::from_secs(30), 1000);
        let (status, body) = query(&app, "m").await;
        // A corrupt/identity fault maps to 500 internal, the same class
        // `/api/v1/query` surfaces; never a 200 serving the foreign data.
        assert_eq!(
            status,
            StatusCode::INTERNAL_SERVER_ERROR,
            "a foreign-tenant footer must be a corruption error, not served: {body}"
        );
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "internal");
        // The foreign exemplar's trace id must never reach the response body.
        assert!(
            !body
                .to_string()
                .contains("0a1b2c3d4e5f60718293a4b5c6d7e8f9"),
            "the foreign trace id must not leak: {body}"
        );
    }

    /// The accounting filled during the request reaches the client: `stats`
    /// reports the segment counters and the real store/decode cost, rather
    /// than being dropped when the handler returns (ADR-0044 decision 1).
    /// `seriesMatched` counts the series the selector matched, not every
    /// series the fetched object carries.
    #[tokio::test]
    async fn response_carries_query_accounting_stats() {
        let store = Arc::new(MemoryStore::new());
        // Two series in one object; the query matches exactly one of them, so
        // a seriesMatched of 2 would be counting the object, not the query.
        let (sid, wanted) = scalar_series(
            &tenant(),
            "m",
            &[("keep", "yes")],
            &[(NOW - 60 * NS_PER_SEC, 1.0)],
        );
        let (_other_sid, other) = scalar_series(
            &tenant(),
            "m",
            &[("keep", "no")],
            &[(NOW - 60 * NS_PER_SEC, 2.0)],
        );
        let ex = exemplar(sid, NOW - 30 * NS_PER_SEC, 1.0, TRACE_ID, SPAN_ID, &[]);
        publish_segment(&store, 1, vec![wanted, other], vec![ex]).await;

        let app = build_router(store, Duration::from_secs(30), 1000);
        let (status, body) = query(&app, "m{keep=\"yes\"}").await;
        assert_eq!(status, StatusCode::OK);

        let stats = &body["stats"];
        assert_eq!(stats["segmentsFetched"], 1);
        assert_eq!(
            stats["accounting"]["segmentsOpened"], 1,
            "the one whole-object GET is accounted: {body}"
        );
        assert!(
            stats["accounting"]["s3GetRequests"].as_u64().expect("u64") >= 1,
            "the object GET is counted"
        );
        assert!(
            stats["accounting"]["s3GetBytes"].as_u64().expect("u64") > 0,
            "the transferred bytes are counted"
        );
        assert!(
            stats["accounting"]["decompressedBytes"]
                .as_u64()
                .expect("u64")
                > 0,
            "the decoded EXEMPLARS section bytes are counted"
        );
        assert_eq!(
            stats["accounting"]["seriesMatched"], 1,
            "seriesMatched counts the matched series, not the object's two"
        );
    }

    /// The result cap is a rejection, not a truncation: a query whose matched
    /// exemplars exceed `max_exemplars` is a 422 carrying the same
    /// `errorType` the other budget classes use, and no partial `data`.
    #[tokio::test]
    async fn max_exemplars_result_cap_is_enforced() {
        let store = Arc::new(MemoryStore::new());
        let (sid, series) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        // Three distinct exemplars (distinct trace ids, so dedup keeps all
        // three) against a cap of two.
        let exemplars = (0u8..3)
            .map(|i| {
                exemplar(
                    sid,
                    NOW - (30 + i64::from(i)) * NS_PER_SEC,
                    1.0,
                    [i + 1; 16],
                    SPAN_ID,
                    &[],
                )
            })
            .collect();
        publish_segment(&store, 1, vec![series], exemplars).await;

        let backend: Arc<dyn ObjectStoreBackend> = store;
        let app = build_router_full(backend, Duration::from_secs(30), 1000, 2);
        let (status, body) = query(&app, "m").await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "execution");
        assert!(
            body["data"].is_null(),
            "a capped query returns no partial data: {body}"
        );
    }

    /// Exactly at the cap succeeds: the budget is a ceiling on what may be
    /// materialized, not on what may be attempted (mirrors the engine's
    /// `max_series_exactly_at_cap_succeeds`).
    #[tokio::test]
    async fn max_exemplars_exactly_at_cap_succeeds() {
        let store = Arc::new(MemoryStore::new());
        let (sid, series) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        let exemplars = (0u8..2)
            .map(|i| {
                exemplar(
                    sid,
                    NOW - (30 + i64::from(i)) * NS_PER_SEC,
                    1.0,
                    [i + 1; 16],
                    SPAN_ID,
                    &[],
                )
            })
            .collect();
        publish_segment(&store, 1, vec![series], exemplars).await;

        let backend: Arc<dyn ObjectStoreBackend> = store;
        let app = build_router_full(backend, Duration::from_secs(30), 1000, 2);
        let (status, body) = query(&app, "m").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["data"][0]["exemplars"]
                .as_array()
                .expect("array")
                .len(),
            2
        );
    }

    /// A duplicate must not consume result budget: the same exemplar read
    /// twice out of the ADR-0018 overlap window is one exemplar, so a cap of
    /// one still succeeds. Pins that dedup runs before the cap check.
    #[tokio::test]
    async fn overlap_duplicates_do_not_consume_the_result_cap() {
        let store = Arc::new(MemoryStore::new());
        let ts = NOW - 30 * NS_PER_SEC;
        let (sid1, series1) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        publish_segment(
            &store,
            1,
            vec![series1],
            vec![exemplar(sid1, ts, 1.0, TRACE_ID, SPAN_ID, &[])],
        )
        .await;
        let (sid2, series2) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        publish_segment(
            &store,
            2,
            vec![series2],
            vec![exemplar(sid2, ts, 1.0, TRACE_ID, SPAN_ID, &[])],
        )
        .await;

        let backend: Arc<dyn ObjectStoreBackend> = store;
        let app = build_router_full(backend, Duration::from_secs(30), 1000, 1);
        let (status, body) = query(&app, "m").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "the overlap duplicate is one exemplar, not two: {body}"
        );
        assert_eq!(
            body["data"][0]["exemplars"]
                .as_array()
                .expect("array")
                .len(),
            1
        );
    }

    /// An unauthenticated request is a 401, not an empty 200. Without this,
    /// deleting the `authenticate` call and hardcoding a tenant hash would
    /// leave every other test green.
    #[tokio::test]
    async fn missing_authorization_header_is_unauthorized() {
        let store = Arc::new(MemoryStore::new());
        let (sid, series) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        let ex = exemplar(sid, NOW - 30 * NS_PER_SEC, 1.0, TRACE_ID, SPAN_ID, &[]);
        publish_segment(&store, 1, vec![series], vec![ex]).await;

        let app = build_router(store, Duration::from_secs(30), 1000);
        let (status, body) = query_as(&app, "m", None).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "no Authorization header must be 401, not an empty 200"
        );
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "unauthorized");
        assert!(
            body["data"].is_null(),
            "an unauthorized response carries no data"
        );
    }

    /// An executed exemplar query submits exactly one audit event through the
    /// sink, with `query.language=exemplars` and `ok` status, its durability
    /// awaited before the response is released (ADR-0062 §2a, issue #762).
    #[tokio::test]
    async fn query_exemplars_submits_one_audit_event() {
        let store = Arc::new(MemoryStore::new());
        let (sid, series) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        let ex = exemplar(sid, NOW - 30 * NS_PER_SEC, 1.0, TRACE_ID, SPAN_ID, &[]);
        publish_segment(&store, 1, vec![series], vec![ex]).await;

        let (app, events) = build_router_recording(store);
        let (status, _body) = query(&app, "m").await;
        assert_eq!(status, StatusCode::OK);

        let guard = events.lock().expect("lock");
        assert_eq!(guard.len(), 1, "exactly one audit event per exemplar query");
        let event = &guard[0];
        assert_eq!(audit_attr(event, "kind"), Some("query"));
        assert_eq!(audit_attr(event, "query.language"), Some("exemplars"));
        assert_eq!(audit_attr(event, "query.status"), Some("ok"));
        assert_eq!(audit_attr(event, "query.text"), Some("m"));
        assert_eq!(
            audit_attr(event, "query.tenant"),
            Some(tenant().hash().to_hex().as_str())
        );
    }

    /// A request rejected before execution (no `Authorization`) is not audited.
    #[tokio::test]
    async fn an_unauthenticated_exemplar_request_is_not_audited() {
        let store = Arc::new(MemoryStore::new());
        let (sid, series) = scalar_series(&tenant(), "m", &[], &[(NOW - 60 * NS_PER_SEC, 1.0)]);
        let ex = exemplar(sid, NOW - 30 * NS_PER_SEC, 1.0, TRACE_ID, SPAN_ID, &[]);
        publish_segment(&store, 1, vec![series], vec![ex]).await;

        let (app, events) = build_router_recording(store);
        let (status, _body) = query_as(&app, "m", None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(
            events.lock().expect("lock").is_empty(),
            "a request rejected before execution writes no audit event"
        );
    }
}
