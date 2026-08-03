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
//! # Two data facts and how this endpoint decides them
//!
//! Both were found reviewing #474 and are called out in the issue.
//!
//! 1. **An exemplar's `ts_ns` can fall outside its object's event bounds.**
//!    The exemplar timestamp does not go through the sample path's
//!    event-time clamp, so a stored exemplar can sit outside the object's
//!    `min_event_ts_ns`/`max_event_ts_ns` and outside the commit record's
//!    bounds. This endpoint resolves *the same snapshot the equivalent sample
//!    query resolves* (`Catalog::resolve_pruned_with_accounting` over exactly
//!    `[start, end]`, with the same equality-`__name__` pruning), and never
//!    widens the window to chase a stray exemplar into an object the sample
//!    query would not have fetched. That is the deliberate choice: deliverable
//!    3 ("never fetch a segment the equivalent sample query would not") is an
//!    invariant, exemplars are an illustrative/sampled signal by construction
//!    (ADR-0047 decision 2), and widening would trade that invariant for a
//!    rare edge case. Within a fetched object every exemplar is considered;
//!    the returned set is then clamped to `[start, end]` by the exemplar's own
//!    `ts_ns`, matching Prometheus' time-range contract.
//!
//! 2. **An exemplar carries no dedup priority.** During ADR-0018's overlap
//!    window a snapshot can hold both an L1 part and its inputs, so the same
//!    exemplar is readable twice; samples resolve this by dedup priority, but
//!    exemplars have none. This endpoint deduplicates at query time on
//!    `(series_id, ts_ns, trace_id)`, keeping the first occurrence. That
//!    collapses the compaction-overlap double (identical `(series, ts,
//!    trace)`) that would otherwise render two identical dots in Grafana,
//!    while preserving genuinely distinct exemplars: a retried write can leave
//!    two exemplars at the same `(series, ts)`, but ADR-0047's 2026-08-03
//!    amendment records that they "differ only in trace id", so a different
//!    `trace_id` keeps both.
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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use ravel_catalog::Catalog;
use ravel_ingest::Clock;
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_promql::{LabelMatcher, MatchOp, matches_series, plan_selectors};
use ravel_query::http::{QueryErrorResponse, TenantResolver};
use ravel_query::{QueryEngine, QueryError};
use ravel_segment::{
    ExemplarRecord, ReaderLimits, SeriesEntryV4, decode_catalog_v5, decode_exemplars_section,
    open_from_full,
};
use ravel_types::accounting::{AccountedOp, QueryAccounting};
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

/// Shared state for the exemplars route. Holds its own `Catalog` and object
/// store handles (the same instances the PromQL engine uses, so an exemplar
/// query resolves byte-for-byte the snapshot a sample query would), plus the
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
        }
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
    let series = tokio::time::timeout(
        deadline,
        collect_exemplars(&state, tenant_hash, &query, start_ns, end_ns, &min_tokens),
    )
    .await
    .map_err(|_| ApiError::from_query(QueryError::DeadlineExceeded { deadline }))??;

    Ok((StatusCode::OK, axum::Json(Envelope::success(series))).into_response())
}

/// Resolves the snapshot the equivalent sample query would, reads exemplars
/// from each matched segment, filters and deduplicates them, and groups them
/// by series into the Prometheus response shape.
async fn collect_exemplars(
    state: &ExemplarsState,
    tenant_hash: TenantHash,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    min_tokens: &[CommitToken],
) -> Result<Vec<ExemplarSeriesJson>, ApiError> {
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
        // match no series and therefore carries no exemplars.
        return Ok(Vec::new());
    }
    let name_filter = shared_equality_name_filter(&matcher_sets);

    let now_ns = state.clock.now_ns();
    let window = TimeRange { start_ns, end_ns };
    let accounting = QueryAccounting::new();
    let snapshot = state
        .catalog
        .resolve_pruned_with_accounting(
            &tenant_hash,
            Signal::Metrics,
            window,
            min_tokens,
            now_ns,
            name_filter.as_deref(),
            &accounting,
        )
        .await
        .map_err(|e| ApiError::from_query(QueryError::from(e)))?;

    // The same snapshot-size ceiling `/api/v1/query` enforces after resolve.
    if snapshot.segments.len() > state.max_segments {
        return Err(ApiError::from_query(QueryError::TooManySegments {
            count: snapshot.segments.len(),
            max: state.max_segments,
        }));
    }

    // Segments are read sequentially. The sample path fans this out under a
    // concurrency bound; here correctness and staying inside `ravel-server`'s
    // (non-`futures`) default dependency set win, and the wall deadline above
    // still bounds the total. Noted in the issue #475 report as a follow-up.
    let mut collected: Vec<(SeriesId, LabelSet, ExemplarRecord)> = Vec::new();
    for seg in &snapshot.segments {
        let found = read_segment_exemplars(
            state,
            tenant_hash,
            &seg.data_object_key,
            &matcher_sets,
            start_ns,
            end_ns,
            &accounting,
        )
        .await
        .map_err(ApiError::from_query)?;
        collected.extend(found);
    }

    Ok(group_and_dedup(collected))
}

/// Reads one segment's exemplars for the matched series. Returns early with
/// nothing when the object carries no `EXEMPLARS` section (the common case),
/// after only the whole-object `GET` and footer parse.
async fn read_segment_exemplars(
    state: &ExemplarsState,
    tenant_hash: TenantHash,
    data_object_key: &str,
    matcher_sets: &[Vec<LabelMatcher>],
    start_ns: i64,
    end_ns: i64,
    accounting: &QueryAccounting,
) -> Result<Vec<(SeriesId, LabelSet, ExemplarRecord)>, QueryError> {
    let _ = tenant_hash; // resolution already scoped the snapshot to the tenant.
    let limits = ReaderLimits::default();

    // One whole-object GET per matched segment, recorded at the same funnel
    // the sample fetcher uses (ADR-0044): request count, transferred bytes,
    // and one opened segment.
    let got = state
        .store
        .get(data_object_key, GetRange::Full)
        .await
        .map_err(|source| fetch_store_error(data_object_key, source))?;
    accounting.record_s3_request(AccountedOp::Get);
    accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
    accounting.add_segments_opened(1);
    let object = got.data;

    let loc = open_from_full(&object, limits).map_err(|source| corrupt(data_object_key, source))?;
    let footer = &loc.footer;

    // No EXEMPLARS section means the object has no exemplars (ADR-0047
    // decision 1); skip the catalog decode entirely.
    let Some((exemplars_bytes, exemplars_uncompressed)) =
        section_slice(&object, footer, SECTION_KIND_EXEMPLARS)
            .map_err(|e| corrupt(data_object_key, e))?
    else {
        return Ok(Vec::new());
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
    accounting.add_series_matched(entries.len() as u64);

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

    // Whole-section decode: query_exemplars matches a *set* of series, and one
    // pass over the section filtered by `matched` is strictly fewer passes
    // than one early-exit `probe_exemplars_by_series` per matched series over
    // the same already-fetched bytes (see the issue #475 report).
    let records = decode_exemplars_section(footer, label_dict_bytes, exemplars_bytes, limits)
        .map_err(|source| corrupt(data_object_key, source))?;
    accounting.add_decompressed_bytes(exemplars_uncompressed);

    let mut out = Vec::new();
    for rec in records {
        let idx = usize::try_from(rec.series_index).unwrap_or(usize::MAX);
        // A record's series_index is validated in range against the footer's
        // series_count by the decoder; entries has exactly that many rows, so
        // this lookup cannot miss on a well-formed object. Guard anyway.
        let Some(entry) = entries.get(idx) else {
            return Err(corrupt(
                data_object_key,
                ravel_segment::SegmentError::ExemplarSeriesIndexOutOfRange(rec.series_index),
            ));
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
        out.push((entry.entry.series_id, entry.entry.labels.clone(), rec));
    }
    Ok(out)
}

/// Groups collected exemplars by series and deduplicates on `(series_id,
/// ts_ns, trace_id)`, keeping the first occurrence (see the module doc, data
/// fact 2). Output is deterministic: series ordered by id, exemplars within a
/// series ordered by `(ts_ns, trace_id)`.
fn group_and_dedup(
    collected: Vec<(SeriesId, LabelSet, ExemplarRecord)>,
) -> Vec<ExemplarSeriesJson> {
    // Preserve first-seen order of series, but collapse duplicates.
    let mut series_order: Vec<SeriesId> = Vec::new();
    let mut by_series: HashMap<SeriesId, (LabelSet, Vec<ExemplarRecord>)> = HashMap::new();
    let mut seen: HashSet<(SeriesId, i64, [u8; 16])> = HashSet::new();

    for (series_id, labels, rec) in collected {
        if !seen.insert((series_id, rec.ts_ns, rec.trace_id)) {
            continue;
        }
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
    let mut labels: BTreeMap<String, String> = rec.attrs.into_iter().collect();
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
/// `equality_name_filter` so this endpoint prunes exactly as a sample query
/// over the same selector would (docs/metric-index-plan.md P5b).
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

fn fetch_store_error(key: &str, source: StoreError) -> QueryError {
    QueryError::Fetch(ravel_query::FetchError::Store {
        key: key.to_string(),
        source,
    })
}

fn corrupt(key: &str, source: ravel_segment::SegmentError) -> QueryError {
    QueryError::Fetch(ravel_query::FetchError::Corrupt {
        key: key.to_string(),
        source,
    })
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
/// object with `resultType`/`result`).
#[derive(Debug, Serialize)]
struct Envelope {
    status: &'static str,
    data: Vec<ExemplarSeriesJson>,
}

impl Envelope {
    fn success(data: Vec<ExemplarSeriesJson>) -> Self {
        Envelope {
            status: "success",
            data,
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

    struct FixedClock(i64);
    impl Clock for FixedClock {
        fn now_ns(&self) -> i64 {
            self.0
        }
    }

    fn tenant() -> TenantId {
        TenantId::new("tenant-a".to_string())
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
        let tenant_id = tenant();
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
        let catalog =
            Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
        let mut tokens = HashMap::new();
        tokens.insert(TOKEN.to_string(), tenant());
        let state = ExemplarsState {
            catalog,
            store: backend,
            tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
            clock: Arc::new(FixedClock(NOW)),
            deadline,
            max_segments,
        };
        router(state)
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
        let start = NOW - NS_PER_HOUR;
        let uri = format!(
            "/api/v1/query_exemplars?query={}&start={}&end={}",
            encode(promql),
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
}
