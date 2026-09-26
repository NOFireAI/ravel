//! The ADR-0071 slice worker service and the remote client that drives it
//!.
//!
//! A coordinator dispatches one [`pb::FetchRequest`] per slice; the worker
//! ([`SeriesFetchService`]) executes the existing local fetch path over only
//! that slice's segments, applies the request's erasure predicates exactly as
//! the local path would, and streams the decoded series back, ending with
//! exactly one [`pb::Summary`] frame (slice atomicity: the slice contributes to
//! the coordinator merge only after its terminal summary arrives).
//!
//! # Scalar and native-histogram series
//!
//! As of `PROTOCOL_VERSION` 3 (ADR-0096 decision 3 step 4, #379) a Metrics slice
//! streams both scalar series (`SeriesFrame`, carrying the four packed per-sample
//! provenance columns for a run-merged L1 run) and native-histogram series
//! (`HistogramFrame`, carrying typed `HistogramRecord`s plus the same provenance
//! columns). Erasure filtering (ADR-0096 decision 3 step 3) is applied to the
//! decoded histogram series before they are encoded, exactly as it is for scalar
//! series. The version gate ([`codec::check_protocol_version`], plus the
//! intra-cluster routing filter) guarantees only a coordinator speaking the same
//! version ever receives these frames, so the new columns and records are never
//! silently dropped by an older decoder.
//!
//! # Aggregation pushdown
//!
//! A Metrics slice whose request carries a [`pb::PartialAggregateRequest`]
//! (ADR-0103 decision 2) returns one [`pb::PartialAggregate`] frame per series
//! instead of its raw series frames. The worker merges its own runs first
//! ([`merge_soa_runs`], the same total-order per-series dedup the coordinator
//! runs on the raw-fetch path) and reduces each series' deduped samples, so a
//! sample that two of this worker's segments both carry is counted once, never
//! twice. That local merge is what makes the partial exact: under ADR-0103
//! decision 1's eligibility gate the coordinator only asks for a partial when
//! no other worker and no remote cluster can hold runs of the same series, so
//! there is nothing left for the coordinator's cross-worker dedup belt to
//! reconcile. A request with no `partial_aggregate` takes the raw-frame path
//! unchanged.
//!
//! # Segment identity resolution
//!
//! A worker receives durable [`pb::SegmentIdentity`] values, not object keys or
//! trusted bytes (ADR-0071 reconstruct-don't-trust). An intra-cluster pinned
//! slice resolves them with [`ReconstructingSegmentResolver`]. For each
//! identity it rebuilds the key of the segment's own durable record with
//! `ravel_commit::keys` (the commit record for L0; the compaction record, or
//! failing that the erasure rewrite record, for an L1 part), GETs that one
//! record, and verifies it: the record decodes and validates, its own identity
//! fields reconstruct the key it was read from, an L0 record's stored
//! `object_key` passes `verify_object_key`, and the full 32-byte content hash
//! and the object size (plus, for L1, the full input-set hash and the part
//! index) equal the identity's. The ref is then built from the verified record
//! exactly as the catalog builds one, so nothing but the identity's key
//! components is taken from the wire. The worker lists nothing and resolves no
//! snapshot, so a compaction committed between the coordinator's resolve and
//! this fetch cannot change which object is read: records and the objects they
//! name are immutable.
//!
//! Each record GET holds a permit of the same GET limiter the worker's
//! data-object GETs use. Record GETs are not charged to the slice's
//! accounting, so they count toward neither `max_bytes_scanned` nor
//! `max_s3_requests` here or on the coordinator, and are not in the slice's
//! reported cost.
//!
//! A structurally malformed identity is [`ResolveIdentityError::Invalid`] and
//! fails the slice with `BAD_DATA`. A record that is missing, unreadable,
//! fails verification, or disagrees with the identity fails the slice with
//! `UNSUPPORTED`, which makes the coordinator run the whole query locally
//! through its own catalog resolve. A record GET that fails with a retryable
//! store error is [`ResolveIdentityError::RecordUnavailable`] and fails the
//! slice with `UNAVAILABLE`, which makes the coordinator re-dispatch that one
//! slice. None of them ever reads another object instead.
//!
//! Cross-cluster federation is the exception, and stays one: a resolve-scope
//! request is authoritative on the remote cluster, which resolves its OWN
//! snapshot and pins it before the fetch runs. [`SnapshotSegmentResolver`]
//! serves that path, mapping an identity to a ref of that snapshot by content
//! hash; an identity outside it is [`ResolveIdentityError::Unknown`] and maps
//! to `SNAPSHOT_INVALIDATED`, which the coordinator retries once.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use ravel_catalog::{DeclaredColumnStats, SegmentLevel, SegmentRef};
use ravel_commit::keys;
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_proto::queryfrag::v1 as pb;
use ravel_types::accounting::{AccountedOp, QueryAccounting, QueryAccountingSnapshot};
use ravel_types::{SeriesId, Signal, TenantHash};

use ravel_rspan::SpanQuery;

use crate::GetLimiter;
use crate::config::{ByteLimit, EngineConfig};
use crate::distrib::codec;
use crate::distrib::proto::series_fetch_server::{SeriesFetch, SeriesFetchServer};
use crate::engine::{bytes_scanned_exceeded, merge_soa_runs};
use crate::erasure::is_erased_span;
use crate::error::QueryError;
use crate::fetcher::{
    FetchError, FetchStats, FetchedHistogramSeries, FetchedSeriesSoa, SegmentFetcher,
};
use crate::log_fetcher::{LogFetchError, LogQuery, LogSegmentFetcher};
use crate::span_fetcher::{SpanFetchError, SpanSegmentFetcher};

/// Why a shipped [`pb::SegmentIdentity`] could not be turned into the
/// [`SegmentRef`] a worker fetches.
///
/// Each variant names its recovery through [`Self::status_code`]. None of them
/// reads a different object in place of the one pinned.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResolveIdentityError {
    /// The identity is structurally malformed: a field has the wrong length or
    /// shape, the level is unknown, or no record key can be built from it.
    /// Terminal (`BAD_DATA`): every worker reproduces it, so re-dispatch would
    /// only spread it.
    #[error("segment identity is malformed: {reason}")]
    Invalid { reason: String },
    /// The identity is well formed but names a segment outside the snapshot
    /// this worker resolved for itself. Only the federation resolver raises
    /// this; the coordinator maps it to a single re-resolve and re-dispatch.
    #[error("pinned segment not found on worker: {reason}")]
    Unknown { reason: String },
    /// No record exists at the key the identity reconstructs.
    #[error("record {key} for the pinned segment was not found")]
    RecordMissing { key: String },
    /// The record GET failed with a retryable store error (throttled, timed
    /// out, or transient). Another worker, or a later attempt, can read it.
    #[error("record {key} for the pinned segment is unavailable: {reason}")]
    RecordUnavailable { key: String, reason: String },
    /// The record at the reconstructed key could not be read, for a reason a
    /// retry would not change.
    #[error("record {key} for the pinned segment could not be read: {reason}")]
    RecordRead { key: String, reason: String },
    /// The record read back does not decode, fails its own validation, is not
    /// the record its key addresses, or names a data key its identity fields do
    /// not reconstruct.
    #[error("record {key} for the pinned segment failed verification: {reason}")]
    RecordInvalid { key: String, reason: String },
    /// The verified record disagrees with the identity on `field`.
    #[error("record {key} disagrees with the pinned identity on {field}")]
    RecordMismatch { key: String, field: &'static str },
}

impl ResolveIdentityError {
    fn invalid(reason: impl Into<String>) -> Self {
        ResolveIdentityError::Invalid {
            reason: reason.into(),
        }
    }

    /// The wire status this failure fails its slice with. A retryable record
    /// GET error is `UNAVAILABLE`, which makes the coordinator re-dispatch that
    /// one slice. Every other record failure is `UNSUPPORTED`, which makes it
    /// run the whole query locally through its own catalog resolve.
    pub fn status_code(&self) -> pb::status::Code {
        match self {
            ResolveIdentityError::Invalid { .. } => pb::status::Code::BadData,
            ResolveIdentityError::Unknown { .. } => pb::status::Code::SnapshotInvalidated,
            ResolveIdentityError::RecordUnavailable { .. } => pb::status::Code::Unavailable,
            ResolveIdentityError::RecordMissing { .. }
            | ResolveIdentityError::RecordRead { .. }
            | ResolveIdentityError::RecordInvalid { .. }
            | ResolveIdentityError::RecordMismatch { .. } => pb::status::Code::Unsupported,
        }
    }
}

/// Resolves a shipped [`pb::SegmentIdentity`] back to the [`SegmentRef`] a
/// worker fetches. See the module docs for which resolver serves which path.
#[async_trait::async_trait]
pub trait SegmentResolver: Send + Sync {
    /// The ref this identity names, or a typed refusal. A resolver never
    /// substitutes a different segment for one it cannot resolve. Any store
    /// request it issues is charged to `accounting`, which the caller picks:
    /// the slice service passes a handle apart from the slice's own (see
    /// `SeriesFetchService::resolve_pinned`).
    async fn resolve(
        &self,
        identity: &pb::SegmentIdentity,
        accounting: &QueryAccounting,
    ) -> Result<SegmentRef, ResolveIdentityError>;
}

/// How many pinned identities one slice resolves concurrently. Each costs one
/// record GET (two for an erasure-rewrite part). Every one of those GETs also
/// holds a permit of the resolver's GET limiter, so this only bounds how many
/// wait for one.
const RESOLVE_CONCURRENCY: usize = 16;

/// Resolve every identity in order, stopping at the first refusal.
async fn resolve_identities<R: SegmentResolver + ?Sized>(
    resolver: &R,
    identities: &[pb::SegmentIdentity],
    accounting: &QueryAccounting,
) -> Result<Vec<SegmentRef>, ResolveIdentityError> {
    use futures::{StreamExt as _, TryStreamExt as _};
    // Collected first so the stream holds concrete futures rather than a
    // closure over a borrowed identity, which `async fn fetch` cannot prove
    // general over every lifetime. Futures are lazy, so `buffered` still
    // bounds how many records are in flight.
    let pending: Vec<_> = identities
        .iter()
        .map(|identity| resolver.resolve(identity, accounting))
        .collect();
    futures::stream::iter(pending)
        .buffered(RESOLVE_CONCURRENCY)
        .try_collect()
        .await
}

/// The intra-cluster [`SegmentResolver`] (ADR-0071 reconstruct-don't-trust,
/// ADR-0010 §7 key discipline). For each identity it reconstructs the key of
/// the segment's own durable record, GETs that one record, verifies it, and
/// builds the ref from the verified record exactly as the catalog does. It
/// lists nothing and resolves no snapshot.
///
/// Built for one (tenant, signal) pair, which the fetch request names and the
/// gRPC handler has already re-derived from the presented credential. Those two
/// therefore never come from an identity, so a coordinator cannot point a
/// worker at another tenant's objects by rewriting the pins.
pub struct ReconstructingSegmentResolver {
    store: Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
    signal: Signal,
    /// The process-wide GET limiter (ADR-1195) the worker's data-object GETs
    /// also draw from, so record GETs never add to the per-worker bound.
    get_limiter: Arc<GetLimiter>,
}

impl ReconstructingSegmentResolver {
    pub fn new(
        store: Arc<dyn ObjectStoreBackend>,
        tenant_hash: TenantHash,
        signal: Signal,
        get_limiter: Arc<GetLimiter>,
    ) -> Self {
        ReconstructingSegmentResolver {
            store,
            tenant_hash,
            signal,
            get_limiter,
        }
    }

    /// GET one record in full, charging the request and its bytes to
    /// `accounting`.
    /// The GET holds a permit of the same limiter the data-object GETs draw
    /// from, and only around the store call.
    async fn get_record(
        &self,
        key: &str,
        accounting: &QueryAccounting,
    ) -> Result<bytes::Bytes, ResolveIdentityError> {
        let got = match self.get_limiter.acquire().await {
            Ok(_permit) => {
                accounting.record_s3_request(AccountedOp::Get);
                self.store.get(key, GetRange::Full).await
            }
            Err(closed) => Err(StoreError::Transient(closed.to_string())),
        };
        match got {
            Ok(outcome) => {
                accounting.add_s3_bytes(AccountedOp::Get, outcome.data.len() as u64);
                Ok(outcome.data)
            }
            Err(StoreError::NotFound) => Err(ResolveIdentityError::RecordMissing {
                key: key.to_string(),
            }),
            Err(err) if err.is_retryable() => Err(ResolveIdentityError::RecordUnavailable {
                key: key.to_string(),
                reason: err.to_string(),
            }),
            Err(err) => Err(ResolveIdentityError::RecordRead {
                key: key.to_string(),
                reason: err.to_string(),
            }),
        }
    }

    /// An L0 segment: its commit record, at the key the identity's
    /// (shard, ingest hour, writer, epoch, seq) reconstruct.
    async fn resolve_l0(
        &self,
        identity: &pb::SegmentIdentity,
        content_hash: &[u8; 32],
        accounting: &QueryAccounting,
    ) -> Result<SegmentRef, ResolveIdentityError> {
        if !identity.input_set_hash.is_empty() || identity.part_index != 0 {
            return Err(ResolveIdentityError::invalid(
                "an L0 identity carries L1 compaction fields (input_set_hash/part_index)",
            ));
        }
        let writer_id = uuid::Uuid::parse_str(&identity.writer_id).map_err(|_| {
            ResolveIdentityError::invalid(format!(
                "writer_id {:?} is not a uuid",
                identity.writer_id
            ))
        })?;
        let key = keys::commit_key(
            &self.tenant_hash,
            self.signal,
            identity.shard,
            identity.ingest_hour_bucket,
            writer_id,
            identity.writer_epoch,
            identity.writer_seq,
        )
        .map_err(|err| ResolveIdentityError::invalid(err.to_string()))?;
        let bytes = self.get_record(&key, accounting).await?;
        let invalid = |reason: String| ResolveIdentityError::RecordInvalid {
            key: key.clone(),
            reason,
        };
        let record =
            ravel_commit::record::decode(&bytes).map_err(|err| invalid(err.to_string()))?;
        // The body must be the commit its key addresses: this binds the
        // record's tenant, signal, shard, hour, writer, epoch and seq to the
        // identity's.
        let own_key =
            keys::commit_key_for_record(&record).map_err(|err| invalid(err.to_string()))?;
        if own_key != key {
            return Err(invalid(format!("its body addresses {own_key}")));
        }
        let data_object_key =
            keys::verify_object_key(&record).map_err(|err| invalid(err.to_string()))?;
        let record_hash: [u8; 32] = record
            .content_hash
            .as_slice()
            .try_into()
            .map_err(|_| invalid("content_hash is not 32 bytes".to_string()))?;
        let mismatch = |field| ResolveIdentityError::RecordMismatch {
            key: key.clone(),
            field,
        };
        if &record_hash != content_hash {
            return Err(mismatch("content_hash"));
        }
        if record.object_size != identity.object_size {
            return Err(mismatch("object_size"));
        }
        Ok(SegmentRef {
            data_object_key,
            object_size: record.object_size,
            min_event_ts_ns: record.min_event_ts_ns,
            max_event_ts_ns: record.max_event_ts_ns,
            ingest_hour_bucket: record.ingest_hour_bucket,
            sample_count: record.sample_count,
            series_count: record.series_count,
            shard: record.shard,
            content_hash: record_hash,
            writer_id,
            writer_epoch: record.writer_epoch,
            writer_seq: record.writer_seq,
            created_unix_ns: record.created_unix_ns,
            level: SegmentLevel::L0,
            segment_format_version: record.segment_format_version,
            declared_column_stats: DeclaredColumnStats::from_validated(
                &ravel_commit::declared_stats::read_commit_record(&record),
            ),
        })
    }

    /// An L1 part: its compaction record, or failing that the erasure rewrite
    /// record, at the key the identity's (shard, ingest hour, input-set hash)
    /// reconstruct.
    async fn resolve_l1(
        &self,
        identity: &pb::SegmentIdentity,
        content_hash: &[u8; 32],
        accounting: &QueryAccounting,
    ) -> Result<SegmentRef, ResolveIdentityError> {
        let input_set_hash: [u8; 32] =
            identity.input_set_hash.as_slice().try_into().map_err(|_| {
                ResolveIdentityError::invalid(format!(
                    "L1 input_set_hash is {} bytes, expected 32",
                    identity.input_set_hash.len()
                ))
            })?;
        let input_set_hash16 = hex::encode(&input_set_hash[..8]);
        let compaction_key = keys::compaction_record_key(
            &self.tenant_hash,
            self.signal,
            identity.shard,
            identity.ingest_hour_bucket,
            &input_set_hash16,
        )
        .map_err(|err| ResolveIdentityError::invalid(err.to_string()))?;
        match self.get_record(&compaction_key, accounting).await {
            Ok(bytes) => {
                let key = compaction_key;
                let invalid = |reason: String| ResolveIdentityError::RecordInvalid {
                    key: key.clone(),
                    reason,
                };
                let record = ravel_commit::record::decode_compaction(&bytes)
                    .map_err(|err| invalid(err.to_string()))?;
                keys::verify_compaction_record_key(&record, &key)
                    .map_err(|err| invalid(err.to_string()))?;
                let part = verified_part(
                    &key,
                    &record.input_set_hash,
                    &record.parts,
                    identity,
                    &input_set_hash,
                    content_hash,
                )?;
                let data_object_key = keys::reconstruct_l1_part_key(&record, part)
                    .map_err(|err| invalid(err.to_string()))?;
                Ok(l1_ref(
                    data_object_key,
                    part,
                    record.shard,
                    record.ingest_hour_bucket,
                    record.created_unix_ns,
                    input_set_hash,
                    DeclaredColumnStats::from_validated(
                        &ravel_commit::declared_stats::read_compaction_part(part),
                    ),
                ))
            }
            Err(ResolveIdentityError::RecordMissing { .. }) => {
                let key = keys::rewrite_record_key(
                    &self.tenant_hash,
                    self.signal,
                    identity.shard,
                    identity.ingest_hour_bucket,
                    &input_set_hash16,
                )
                .map_err(|err| ResolveIdentityError::invalid(err.to_string()))?;
                let bytes = self.get_record(&key, accounting).await?;
                let invalid = |reason: String| ResolveIdentityError::RecordInvalid {
                    key: key.clone(),
                    reason,
                };
                let record = ravel_commit::erasure::decode_rewrite(&bytes)
                    .map_err(|err| invalid(err.to_string()))?;
                keys::verify_rewrite_record_key(&record, &key)
                    .map_err(|err| invalid(err.to_string()))?;
                let part = verified_part(
                    &key,
                    &record.input_set_hash,
                    &record.parts,
                    identity,
                    &input_set_hash,
                    content_hash,
                )?;
                let data_object_key = keys::reconstruct_rewrite_part_key(&record, part)
                    .map_err(|err| invalid(err.to_string()))?;
                // A rewrite output never carries declared statistics, as in
                // the catalog's own rewrite refs (ADR-0873 decision 3).
                Ok(l1_ref(
                    data_object_key,
                    part,
                    record.shard,
                    record.ingest_hour_bucket,
                    record.created_unix_ns,
                    input_set_hash,
                    DeclaredColumnStats::default(),
                ))
            }
            Err(err) => Err(err),
        }
    }
}

/// The part of a verified compaction or rewrite record that `identity` pins,
/// after checking the record's full input-set hash and the part's full content
/// hash and object size against the identity.
fn verified_part<'a>(
    key: &str,
    record_input_set_hash: &[u8],
    parts: &'a [ravel_proto::commit::v1::CompactionPart],
    identity: &pb::SegmentIdentity,
    input_set_hash: &[u8; 32],
    content_hash: &[u8; 32],
) -> Result<&'a ravel_proto::commit::v1::CompactionPart, ResolveIdentityError> {
    let mismatch = |field| ResolveIdentityError::RecordMismatch {
        key: key.to_string(),
        field,
    };
    if record_input_set_hash != input_set_hash.as_slice() {
        return Err(mismatch("input_set_hash"));
    }
    let part = parts
        .iter()
        .find(|part| part.part_index == identity.part_index)
        .ok_or_else(|| mismatch("part_index"))?;
    if part.content_hash.as_slice() != content_hash.as_slice() {
        return Err(mismatch("content_hash"));
    }
    if part.object_size != identity.object_size {
        return Err(mismatch("object_size"));
    }
    Ok(part)
}

/// An L1 ref built from a verified record and part, field for field as the
/// catalog builds one.
fn l1_ref(
    data_object_key: String,
    part: &ravel_proto::commit::v1::CompactionPart,
    shard: u32,
    ingest_hour_bucket: u32,
    created_unix_ns: i64,
    input_set_hash: [u8; 32],
    declared_column_stats: DeclaredColumnStats,
) -> SegmentRef {
    let mut content_hash = [0u8; 32];
    content_hash.copy_from_slice(&part.content_hash);
    SegmentRef {
        data_object_key,
        object_size: part.object_size,
        min_event_ts_ns: part.min_event_ts_ns,
        max_event_ts_ns: part.max_event_ts_ns,
        ingest_hour_bucket,
        sample_count: part.sample_count,
        series_count: part.series_count,
        shard,
        content_hash,
        // A part has no writer identity of its own; never used for an L1
        // ref's identity or dedup.
        writer_id: uuid::Uuid::nil(),
        writer_epoch: 0,
        writer_seq: 0,
        created_unix_ns,
        level: SegmentLevel::L1 {
            input_set_hash,
            part_index: part.part_index,
        },
        segment_format_version: part.segment_format_version,
        declared_column_stats,
    }
}

#[async_trait::async_trait]
impl SegmentResolver for ReconstructingSegmentResolver {
    async fn resolve(
        &self,
        identity: &pb::SegmentIdentity,
        accounting: &QueryAccounting,
    ) -> Result<SegmentRef, ResolveIdentityError> {
        let content_hash = codec::identity_content_hash(identity)
            .map_err(|err| ResolveIdentityError::invalid(err.to_string()))?;
        match identity.level {
            0 => self.resolve_l0(identity, &content_hash, accounting).await,
            1 => self.resolve_l1(identity, &content_hash, accounting).await,
            other => Err(ResolveIdentityError::invalid(format!(
                "unknown segment level {other}"
            ))),
        }
    }
}

/// The federation [`SegmentResolver`]: resolves identities by content hash
/// against a fixed set of known segments, the snapshot the REMOTE cluster
/// resolved for itself before pinning the slice (see the module docs).
pub struct SnapshotSegmentResolver {
    by_content_hash: HashMap<[u8; 32], SegmentRef>,
}

impl SnapshotSegmentResolver {
    /// Builds a resolver over the given segments, keyed by content hash. A
    /// content-hash collision (never expected: blake3 over immutable objects)
    /// keeps the last segment inserted.
    pub fn new(segments: impl IntoIterator<Item = SegmentRef>) -> Self {
        let by_content_hash = segments
            .into_iter()
            .map(|seg| (seg.content_hash, seg))
            .collect();
        SnapshotSegmentResolver { by_content_hash }
    }
}

#[async_trait::async_trait]
impl SegmentResolver for SnapshotSegmentResolver {
    async fn resolve(
        &self,
        identity: &pb::SegmentIdentity,
        _accounting: &QueryAccounting,
    ) -> Result<SegmentRef, ResolveIdentityError> {
        let hash = codec::identity_content_hash(identity)
            .map_err(|err| ResolveIdentityError::invalid(err.to_string()))?;
        self.by_content_hash
            .get(&hash)
            .cloned()
            .ok_or_else(|| ResolveIdentityError::Unknown {
                reason: "identity is outside this cluster's resolved snapshot".to_string(),
            })
    }
}

/// One slice's typed failure, carrying whatever that attempt had already spent
/// when it failed (issue #1723).
///
/// A slice that fetched three segments and then took a store 503 on the fourth
/// issued three segments' worth of real GETs. Reporting that failure with a
/// zero snapshot tells the coordinator the attempt was free, and the
/// coordinator re-dispatches the same slice to another worker, so the store
/// serves the slice up to three times while the query's recorded cost is one
/// attempt's. `spent`/`stats` are what [`summary_frame`] puts on the terminal
/// frame, exactly as the byte-budget short-circuit already does for a refusal
/// it raises itself.
///
/// [`From<(pb::status::Code, String)>`] builds the zero-spend shape, so a
/// pre-fetch failure (version skew, a malformed request, an unresolved pinned
/// segment) stays a plain `Err((code, message))?` at its call site and converts
/// implicitly: those really did spend nothing.
struct SliceFailure {
    code: pb::status::Code,
    message: String,
    /// The accounting the attempt had accumulated before it failed.
    spent: QueryAccountingSnapshot,
    /// The page counters the attempt had accumulated before it failed.
    stats: FetchStats,
}

impl From<(pb::status::Code, String)> for SliceFailure {
    fn from((code, message): (pb::status::Code, String)) -> Self {
        SliceFailure {
            code,
            message,
            spent: QueryAccountingSnapshot::default(),
            stats: FetchStats::default(),
        }
    }
}

impl SliceFailure {
    /// Attach the spend this attempt had already made. Called at every failure
    /// site that sits AFTER a fetch could have issued requests.
    fn with_spend(mut self, accounting: &QueryAccounting, stats: &FetchStats) -> Self {
        self.spent = accounting.snapshot();
        self.stats = *stats;
        self
    }
}

/// The worker-side fragment service. Holds a [`SegmentFetcher`] over the same
/// object store the coordinator's snapshot pins, and a [`SegmentResolver`] to
/// turn shipped identities into refs.
pub struct SeriesFetchService<R: SegmentResolver + 'static> {
    fetcher: SegmentFetcher,
    /// The RLOG-family fetcher (Logs, Alerts, Audit), wired via
    /// [`with_log_fetcher`](Self::with_log_fetcher). `None` (the default) means
    /// this worker cannot serve a distributed log fetch: a Logs/Alerts/Audit
    /// slice returns `Unsupported` so the coordinator falls back to local
    /// execution. Kept optional so the crate-internal `new` constructor (and the
    /// out-of-crate coordinator wiring that calls it) is unchanged; a worker that
    /// wants to serve logs opts in with the builder.
    log_fetcher: Option<LogSegmentFetcher>,
    /// The RSPAN span fetcher, wired via [`with_span_fetcher`](Self::with_span_fetcher).
    /// `None` (the default) means this worker cannot serve a distributed span
    /// fetch: a Spans slice returns `Unsupported` so the coordinator falls back
    /// to local execution. Kept optional for the same reason `log_fetcher` is:
    /// the crate-internal `new` constructor and its out-of-crate callers are
    /// unchanged; a worker that wants to serve spans opts in with the builder.
    span_fetcher: Option<SpanSegmentFetcher>,
    resolver: Arc<R>,
    /// This worker's OWN limits, wired via
    /// [`with_engine_config`](Self::with_engine_config). Every wire budget is
    /// clamped to these (issue #1687 part A): a coordinator sends the query's
    /// whole budget to every slice (issue #1725), and an absent or `0` wire
    /// budget means "no cap from the coordinator", never "no cap at all". The
    /// default is [`EngineConfig::default`], whose `max_bytes_scanned` is
    /// `Unlimited`, so a caller that does not wire its config keeps the
    /// pre-#1687 behavior of honouring the wire budget verbatim.
    engine: EngineConfig,
    /// Whether this slice arrived as a cross-cluster resolve-scope request
    /// (federation), set via [`with_resolve_scope`](Self::with_resolve_scope).
    /// Such a request is rewritten to a pinned scope over the LOCAL cluster's
    /// snapshot before it reaches this service, so the scope on the request
    /// itself no longer says where it came from, and the coordinator that sent
    /// it folds this cluster's whole answer as one lump rather than seeing its
    /// series and samples slice by slice. This worker therefore enforces its
    /// own `max_series`/`max_samples` over what it is about to return.
    resolve_scope: bool,
}

impl<R: SegmentResolver + 'static> SeriesFetchService<R> {
    pub fn new(fetcher: SegmentFetcher, resolver: Arc<R>) -> Self {
        SeriesFetchService {
            fetcher,
            log_fetcher: None,
            span_fetcher: None,
            resolver,
            engine: EngineConfig::default(),
            resolve_scope: false,
        }
    }

    /// Wires this worker's own [`EngineConfig`] onto the service, so every
    /// wire budget is clamped to the limits this process's operator
    /// configured rather than taken verbatim from the coordinator (issue
    /// #1687 part A). A builder rather than a `new` parameter for the same
    /// reason [`with_log_fetcher`](Self::with_log_fetcher) is one: the
    /// out-of-crate callers of `new` stay unchanged.
    #[must_use]
    pub fn with_engine_config(mut self, engine: EngineConfig) -> Self {
        self.engine = engine;
        self
    }

    /// Marks this service as serving a cross-cluster resolve-scope
    /// (federation) request, which additionally enforces this worker's own
    /// `max_series` and `max_samples` over the slice's result. See the
    /// [`resolve_scope`](Self#structfield.resolve_scope) field.
    #[must_use]
    pub fn with_resolve_scope(mut self) -> Self {
        self.resolve_scope = true;
        self
    }

    /// Wires the RLOG-family fetch path (#284) onto this worker. Built over the
    /// same object store the metrics [`SegmentFetcher`] reads, so a Logs,
    /// Alerts, or Audit slice fetches the same pinned segments the resolver
    /// maps. Without this, those signals return `Unsupported` and the
    /// coordinator runs the query locally.
    #[must_use]
    pub fn with_log_fetcher(mut self, log_fetcher: LogSegmentFetcher) -> Self {
        self.log_fetcher = Some(log_fetcher);
        self
    }

    /// Wires the Spans fetch path (#285) onto this worker. Built over the same
    /// object store the metrics [`SegmentFetcher`] reads, so a Spans slice
    /// fetches the same pinned RSPAN segments the resolver maps. Without this, a
    /// Spans slice returns `Unsupported` and the coordinator runs the query
    /// locally.
    #[must_use]
    pub fn with_span_fetcher(mut self, span_fetcher: SpanSegmentFetcher) -> Self {
        self.span_fetcher = Some(span_fetcher);
        self
    }

    /// Wraps this service in the generated gRPC server, ready to add to a
    /// `tonic` router.
    pub fn into_server(self) -> SeriesFetchServer<Self> {
        SeriesFetchServer::new(self)
    }

    /// Runs the slice fetch, returning the full frame sequence to stream. A
    /// terminal [`pb::Summary`] is always the last element; on any typed
    /// failure the returned frames are just that one summary carrying the
    /// mapped status (slice atomicity: no partial series precede a non-OK
    /// summary).
    async fn run_slice(&self, request: pb::FetchRequest) -> Vec<pb::FetchResponse> {
        match self.run_slice_inner(request).await {
            Ok(frames) => frames,
            // A failure's summary carries what the attempt had spent when it
            // failed (issue #1723). A pre-fetch failure (version skew,
            // malformed request, a pinned segment this worker cannot resolve)
            // really did spend nothing and converts into a zero-spend
            // [`SliceFailure`]; a fetch that took a store error partway through
            // the slice's segments carries the segments it had already paid
            // for, so the coordinator folds that cost before it re-dispatches
            // the slice elsewhere. Post-fetch outcomes that spend and do not
            // fail (budget trip, histogram fallback) build their own summary
            // with real accounting inside `run_slice_inner`.
            Err(failure) => vec![summary_frame(
                &failure.spent,
                0,
                0,
                failure.code,
                failure.message,
                &failure.stats,
            )],
        }
    }

    async fn run_slice_inner(
        &self,
        request: pb::FetchRequest,
    ) -> Result<Vec<pb::FetchResponse>, SliceFailure> {
        // Version skew: the coordinator falls back to local when it sees this.
        codec::check_protocol_version(request.protocol_version)
            .map_err(|e| (pb::status::Code::Unsupported, e.to_string()))?;

        // Decode the signal discriminant, then the signal-agnostic request
        // shape (tenant, matchers, erasure), and only then dispatch on the
        // signal. An unknown discriminant is malformed (BadData);
        // `signal_from_u32` is otherwise only exercised by codec round-trip
        // tests, this is its production caller. The per-signal dispatch is a
        // real match arm reached AFTER this decode (not the former pre-decode
        // blanket rejection), so a signal's own fetch path can build on the
        // decoded request rather than re-parsing it (#283).
        let signal = codec::signal_from_u32(request.signal)
            .map_err(|e| (pb::status::Code::BadData, e.to_string()))?;

        let tenant_hash =
            decode_tenant_hash(&request.tenant_hash).map_err(|m| (pb::status::Code::BadData, m))?;

        let matchers = codec::decode_matchers(request.matchers)
            .map_err(|e| (pb::status::Code::BadData, e.to_string()))?;
        let erasure = codec::decode_erasure(request.erasure);

        // Aggregation pushdown (ADR-0103) is defined for the scalar Metrics
        // lane only: `PartialAggregate` carries f64 bounds over scalar samples,
        // and no log or span shape maps onto it. Fail closed so the coordinator
        // falls back to raw fetch, rather than silently ignoring the request and
        // streaming frames a pushdown-expecting caller did not ask for.
        if request.partial_aggregate.is_some() && !matches!(signal, Signal::Metrics) {
            return Err(SliceFailure::from((
                pb::status::Code::Unsupported,
                format!("aggregation pushdown is not defined for signal {signal:?}"),
            )));
        }

        match signal {
            // Metrics keeps its exact path: resolve the pinned scope, fetch,
            // and encode scalar series. Unchanged from before #283.
            Signal::Metrics => {
                self.run_slice_metrics(
                    request.scope,
                    request.budgets,
                    tenant_hash,
                    matchers,
                    erasure,
                    request.partial_aggregate,
                )
                .await
            }
            // The RLOG family (Logs, Alerts, Audit) fetches through the same
            // `LogSegmentFetcher`/`RlogReader` funnel the local logs/alerts/audit
            // paths use (#284). All three signals share one fetch path: they are
            // the same RLOG object family, distinguished only by the object-key
            // prefix the coordinator resolved, which the worker never re-derives
            // (it fetches the pinned segments the resolver maps). A worker with
            // no log fetcher wired returns Unsupported inside `run_slice_logs`,
            // so the coordinator falls back to local.
            Signal::Logs | Signal::Alerts | Signal::Audit => {
                self.run_slice_logs(
                    request.scope,
                    request.budgets,
                    tenant_hash,
                    matchers,
                    erasure,
                    request.window_start_ns,
                    request.window_end_ns,
                )
                .await
            }
            // Spans fetch through the RSPAN `SpanSegmentFetcher` funnel promoted
            // into this crate (#285), the same one a local `spans` SQL read uses,
            // so the per-span merged attribute view and the erasure exclusion are
            // byte-identical to a local read. A worker with no span fetcher wired
            // returns Unsupported inside `run_slice_spans`, so the coordinator
            // falls back to local.
            Signal::Spans => {
                self.run_slice_spans(
                    request.scope,
                    request.budgets,
                    tenant_hash,
                    matchers,
                    erasure,
                    request.window_start_ns,
                    request.window_end_ns,
                )
                .await
            }
            // Profiles has no distributed fetch path planned in this lane, so it
            // stays rejected exactly as every non-Metrics signal was before
            // #283: Unsupported, whole-query local fallback.
            Signal::Profiles => Err(SliceFailure::from((
                pb::status::Code::Unsupported,
                format!("signal {signal:?} is not distributed"),
            ))),
        }
    }

    /// This worker's own `max_series`/`max_samples` over the result a
    /// resolve-scope (federation) slice is about to return, as a terminal
    /// `BudgetExceeded` summary frame when either is exceeded (issue #1687
    /// part A). `None` on every other request: an intra-cluster slice is one
    /// of several the coordinator folds itself, and it already re-checks both
    /// caps over the folded totals as each slice lands.
    ///
    /// A federated slice has no such coordinator-side belt within this
    /// cluster: the requesting cluster resolved nothing here, folds this
    /// cluster's whole answer as one lump, and its caps are its own, so
    /// without this a remote request could make this cluster RETURN an
    /// unbounded number of series.
    ///
    /// This bounds what the slice returns, not what it materializes. It runs
    /// after every pinned segment has been fetched and decoded, so the series
    /// it counts are already resident in this worker's memory when it
    /// refuses; peak memory is bounded by the byte budget and the fetch
    /// layer's own memory accounting, not by this. Size a worker's memory
    /// against those, never against `max_series`.
    ///
    /// Scalar and histogram series are counted separately against
    /// `max_series`, mirroring the coordinator's two distinct-id sets. Samples
    /// are the pre-merge, pre-reduction sum over what was fetched, which is
    /// never less than what the slice returns, so this refuses a shade early
    /// on a query whose segments carry duplicate samples of one series rather
    /// than late.
    fn resolve_scope_count_refusal(
        &self,
        scalar: &[Vec<FetchedSeriesSoa>],
        histograms: &[FetchedHistogramSeries],
        accounting: &QueryAccounting,
        stats: &FetchStats,
    ) -> Option<Vec<pb::FetchResponse>> {
        if !self.resolve_scope {
            return None;
        }
        let max_series = self.engine.max_series;
        let mut distinct: HashSet<SeriesId> = HashSet::new();
        let mut distinct_hist: HashSet<SeriesId> = HashSet::new();
        let mut samples = 0usize;
        for segment_series in scalar {
            for fs in segment_series {
                distinct.insert(fs.series_id);
                samples = samples.saturating_add(fs.timestamps.len());
            }
        }
        for hs in histograms {
            distinct_hist.insert(hs.series_id);
            samples = samples.saturating_add(hs.timestamps.len());
        }
        let err = if distinct.len() > max_series {
            QueryError::TooManySeries {
                count: distinct.len(),
                max: max_series,
            }
        } else if distinct_hist.len() > max_series {
            QueryError::TooManySeries {
                count: distinct_hist.len(),
                max: max_series,
            }
        } else if samples > self.engine.max_samples {
            QueryError::TooManySamples {
                count: samples,
                max: self.engine.max_samples,
            }
        } else {
            return None;
        };
        // Carries the accounting spent so far, like the byte-budget
        // short-circuit: the coordinator folds a refusing slice's real cost
        // before failing the query, never a lost double-spend.
        Some(vec![summary_frame(
            &accounting.snapshot(),
            0,
            0,
            pb::status::Code::BudgetExceeded,
            err.to_string(),
            stats,
        )])
    }

    /// Resolves a pinned slice's shipped identities to refs. A typed refusal
    /// carries its own status (see [`ResolveIdentityError::status_code`]).
    ///
    /// The record GETs a resolver issues are charged to a handle of their own,
    /// never to the slice's. The slice's accounting is what this worker and the
    /// coordinator hold to `max_bytes_scanned` and `max_s3_requests`, and
    /// local execution never issues these GETs, so charging them would fail a
    /// query distributed that succeeds locally (ADR-0071 pinned-record
    /// amendment). The summary carries one pooled snapshot with no phase
    /// split, so they are not in the slice's reported cost either.
    async fn resolve_pinned(
        &self,
        identities: &[pb::SegmentIdentity],
    ) -> Result<Vec<SegmentRef>, SliceFailure> {
        let records = QueryAccounting::new();
        resolve_identities(self.resolver.as_ref(), identities, &records)
            .await
            .map_err(|err| SliceFailure::from((err.status_code(), err.to_string())))
    }

    /// The Metrics slice path: resolve the pinned scope to refs, fetch each
    /// segment's scalar and histogram series, enforce the per-slice
    /// bytes-scanned budget, apply erasure, and stream one `SeriesFrame` per
    /// scalar series and one `HistogramFrame` per native-histogram series,
    /// ending in a terminal summary (ADR-0096 decision 3 step 4). Extracted from
    /// `run_slice_inner` so the per-signal dispatch there can call it as one arm.
    ///
    /// When `partial_aggregate` is `Some`, the fetch loop and the erasure pass
    /// are unchanged but the encode step is replaced: the worker merges its own
    /// runs and streams one [`pb::PartialAggregate`] per series instead of every
    /// series frame (ADR-0103 decision 2). The branch is per request, not per
    /// series, so one slice returns all partials or all raw frames, never a mix.
    async fn run_slice_metrics(
        &self,
        scope: Option<pb::fetch_request::Scope>,
        budgets: Option<pb::Budgets>,
        tenant_hash: TenantHash,
        matchers: Vec<ravel_promql::LabelMatcher>,
        erasure: Vec<crate::erasure::ErasurePredicate>,
        partial_aggregate: Option<pb::PartialAggregateRequest>,
    ) -> Result<Vec<pb::FetchResponse>, SliceFailure> {
        let identities = match scope {
            Some(pb::fetch_request::Scope::Pinned(pinned)) => pinned.segments,
            // Cross-cluster resolve scope is future work; fall back to local.
            Some(pb::fetch_request::Scope::Resolve(_)) | None => {
                return Err(SliceFailure::from((
                    pb::status::Code::Unsupported,
                    "resolve-scope slices are not supported yet".to_string(),
                )));
            }
        };

        let segments = self.resolve_pinned(&identities).await?;
        // One fresh accounting handle per slice: the coordinator folds the
        // returned snapshot into the query's aggregate (ADR-0071).
        let accounting = QueryAccounting::new();

        // Per-slice bytes-scanned budget, enforced per completed segment
        // exactly as the local path does (ADR-0061 decision 1).
        let byte_limit = slice_byte_limit(budgets.as_ref(), self.engine.max_bytes_scanned);

        let mut scalar = Vec::new();
        let mut histograms: Vec<FetchedHistogramSeries> = Vec::new();
        let mut stats = FetchStats::default();
        for seg in &segments {
            let (seg_scalar, seg_stats, seg_hist) = self
                .fetcher
                .fetch_soa_and_histograms_accounted(tenant_hash, seg, &matchers, &accounting)
                .await
                // The failing segment's predecessors already cost real GETs
                // (issue #1723): the terminal summary carries them, so the
                // coordinator folds this attempt's spend before it re-dispatches
                // the slice to another worker.
                .map_err(|e| {
                    SliceFailure::from(map_fetch_error(e)).with_spend(&accounting, &stats)
                })?;
            histograms.extend(seg_hist);
            stats.raw_f64_pages += seg_stats.raw_f64_pages;
            stats.raw_f64_bytes += seg_stats.raw_f64_bytes;
            scalar.push(seg_scalar);
            // Short-circuit the moment a completed segment fetch pushes this
            // slice over budget, matching the local per-segment check. The
            // terminal summary carries the accounting spent so far so the
            // coordinator folds the slice's real cost before failing the query
            // (never a lost double-spend); the message is the same typed
            // `TooManyBytesScanned` string the local path produces.
            if let Some(err) =
                bytes_scanned_exceeded(accounting.snapshot().total_s3_bytes(), byte_limit)
            {
                return Ok(vec![summary_frame(
                    &accounting.snapshot(),
                    0,
                    0,
                    pb::status::Code::BudgetExceeded,
                    err.to_string(),
                    &stats,
                )]);
            }
        }

        // Selective-erasure exclusion on the histogram series, applied
        // post-decode exactly as the local path applies it (ADR-0064, ADR-0071,
        // ADR-0096 decision 3 step 3): the coordinator does not re-apply, so
        // worker-side application must match the local rule.
        if !erasure.is_empty() {
            crate::erasure::retain_histogram_series(&mut histograms, &erasure);
        }

        // Selective-erasure exclusion on the scalar series, applied post-decode
        // exactly as the local path applies it (ADR-0064, ADR-0071): the
        // coordinator does not re-apply, so worker-side application must match
        // the local rule.
        if !erasure.is_empty() {
            for series in &mut scalar {
                crate::erasure::retain_series_soa(series, &erasure);
            }
        }

        if let Some(frames) =
            self.resolve_scope_count_refusal(&scalar, &histograms, &accounting, &stats)
        {
            return Ok(frames);
        }

        // Aggregation pushdown (ADR-0103 decision 2): this slice returns one
        // partial per series instead of its raw runs. Everything above (fetch,
        // budget, erasure) already ran exactly as it does on the raw path; only
        // the encode step below differs.
        if let Some(want) = partial_aggregate {
            // A native-histogram series has no scalar count/min/max shape:
            // `PartialAggregate` carries f64 bounds only. Silently omitting such
            // series would answer a pushdown query over an incomplete set, so
            // fail closed to the coordinator's raw-fetch fallback instead. This
            // returns an `Unsupported` terminal summary carrying the accounting
            // already spent on the fetches above, NOT an `Err` (which
            // `run_slice`'s catch-all would rebuild from a default,
            // zero-cost snapshot): a slice that paid real fetch cost before
            // refusing must report that cost so the coordinator folds it, exactly
            // as the byte-budget short-circuit above already does (ADR-0103
            // amendment F1).
            if !histograms.is_empty() {
                return Ok(vec![summary_frame(
                    &accounting.snapshot(),
                    0,
                    0,
                    pb::status::Code::Unsupported,
                    "aggregation pushdown is not defined for native-histogram series".to_string(),
                    &stats,
                )]);
            }
            // The reduction window (ADR-0103 amendment): `reduce_start_ns`
            // exclusive, `reduce_end_ns` inclusive. Both fields are one window,
            // so a caller must send both or neither. Both absent is today's only
            // shape (no reduction restriction, byte-identical to pre-amendment
            // behavior); exactly one present is a caller bug a lone bound cannot
            // resolve into a window, so it is a typed `Internal` error rather than
            // a silent one-sided filter.
            let window = match (want.reduce_start_ns, want.reduce_end_ns) {
                (None, None) => None,
                (Some(start), Some(end)) => Some((start, end)),
                (Some(_), None) | (None, Some(_)) => {
                    // Every segment of this slice was fetched before the
                    // request shape was found bad, so the refusal carries what
                    // those fetches cost (issue #1723). A caller bug is not a
                    // free slice.
                    return Err(SliceFailure::from((
                        pb::status::Code::Internal,
                        "PartialAggregateRequest carries exactly one of \
                         reduce_start_ns/reduce_end_ns; a caller must set both or neither"
                            .to_string(),
                    ))
                    .with_spend(&accounting, &stats));
                }
            };
            // The merge runs after the whole fetch loop, so a merge failure is
            // a slice that already paid for every one of its segments
            // (issue #1723). No object a worker can decode reaches this arm
            // today: the caps passed here are `usize::MAX`, a non-monotonic run
            // is refused by the fetch path above as `Corrupt`, and the writer
            // refuses a priority column that is not parallel to its run. The
            // spend rides along anyway, so the arm cannot become a free slice
            // if a future decode path lets one of those through.
            let (partials, samples_merged) = reduce_partial_aggregates(scalar, &want, window)
                .map_err(|e| {
                    SliceFailure::from(map_merge_error(e)).with_spend(&accounting, &stats)
                })?;
            let series_returned = partials.len() as u64;
            let mut frames = Vec::with_capacity(partials.len() + 1);
            for partial in &partials {
                frames.push(pb::FetchResponse {
                    frame: Some(pb::fetch_response::Frame::PartialAggregate(
                        codec::encode_partial_aggregate(partial),
                    )),
                });
            }
            // `samples_returned` stays the count of samples this slice actually
            // reduced, not the number of frames, so the coordinator's own
            // sample-budget re-check still sees a slice's real yield even though
            // the samples themselves never cross the wire.
            frames.push(summary_frame(
                &accounting.snapshot(),
                series_returned,
                samples_merged,
                pb::status::Code::Ok,
                String::new(),
                &stats,
            ));
            return Ok(frames);
        }

        // Run-merged scalar runs and native-histogram runs now cross the wire
        // bit-exactly (ADR-0096 decision 3 step 4, #379): `encode_series_frame`
        // and `encode_histogram_frame` emit the four packed per-sample
        // provenance columns, and `HistogramFrame` also carries the typed
        // records. The version gate (the request-level `check_protocol_version`
        // above and the intra-cluster routing filter) guarantees only a
        // coordinator speaking this same version ever receives these frames, so
        // the columns and records are never silently dropped by an older
        // decoder -- which is why the #315/#348 refusals that used to sit here
        // are gone.
        let mut frames = Vec::new();
        let mut series_returned = 0u64;
        let mut samples_returned = 0u64;
        for segment_series in scalar {
            for fs in segment_series {
                series_returned += 1;
                samples_returned += fs.timestamps.len() as u64;
                frames.push(pb::FetchResponse {
                    frame: Some(pb::fetch_response::Frame::Series(
                        codec::encode_series_frame(&fs),
                    )),
                });
            }
        }
        for hs in histograms {
            series_returned += 1;
            samples_returned += hs.timestamps.len() as u64;
            frames.push(pb::FetchResponse {
                frame: Some(pb::fetch_response::Frame::Hist(
                    codec::encode_histogram_frame(&hs),
                )),
            });
        }
        frames.push(summary_frame(
            &accounting.snapshot(),
            series_returned,
            samples_returned,
            pb::status::Code::Ok,
            String::new(),
            &stats,
        ));
        Ok(frames)
    }

    /// The RLOG-family slice path (Logs, Alerts, Audit): resolve the pinned
    /// scope to refs, fetch each segment through the production
    /// [`LogSegmentFetcher`] funnel (so the per-segment merged attribute view
    /// and the erasure exclusion are byte-identical to a local read), enforce
    /// the per-slice bytes-scanned budget, and stream one `LogRecordFrame` per
    /// decoded record ending in a terminal summary.
    ///
    /// Correctness under ADR-0052 resharding rests on segment self-containment,
    /// not slice atomicity: every RLOG segment embeds the resource+scope
    /// `stream_attrs` blob for the streams it carries, so a worker reading only
    /// this slice's segments produces the exact per-record view `RlogReader`
    /// produces locally, and applies erasure identically per segment, whether or
    /// not a stream's segments straddle two slices. The coordinator (see
    /// [`crate::distrib::merge_log_records`]) re-orders under a total order but
    /// never dedups (logs have no query-time dedup); this path never assumes a
    /// stream maps to one slice.
    #[allow(clippy::too_many_arguments)]
    async fn run_slice_logs(
        &self,
        scope: Option<pb::fetch_request::Scope>,
        budgets: Option<pb::Budgets>,
        tenant_hash: TenantHash,
        matchers: Vec<ravel_promql::LabelMatcher>,
        erasure: Vec<crate::erasure::ErasurePredicate>,
        window_start_ns: i64,
        window_end_ns: i64,
    ) -> Result<Vec<pb::FetchResponse>, SliceFailure> {
        let Some(log_fetcher) = self.log_fetcher.as_ref() else {
            return Err(SliceFailure::from((
                pb::status::Code::Unsupported,
                "distributed log fetch is not configured on this worker".to_string(),
            )));
        };

        // Matcher pushdown for logs has no defined `LogQuery` mapping in this
        // lane yet (that is the SQL lane / engine log-fetch wiring, out of
        // #284's scope). Ignoring matchers would under-filter, so fail closed to
        // the coordinator's local fallback rather than return a wrong result.
        if !matchers.is_empty() {
            return Err(SliceFailure::from((
                pb::status::Code::Unsupported,
                "distributed log fetch does not support matcher pushdown yet".to_string(),
            )));
        }

        let identities = match scope {
            Some(pb::fetch_request::Scope::Pinned(pinned)) => pinned.segments,
            Some(pb::fetch_request::Scope::Resolve(_)) | None => {
                return Err(SliceFailure::from((
                    pb::status::Code::Unsupported,
                    "resolve-scope slices are not supported yet".to_string(),
                )));
            }
        };

        let segments = self.resolve_pinned(&identities).await?;
        let accounting = QueryAccounting::new();

        let byte_limit = slice_byte_limit(budgets.as_ref(), self.engine.max_bytes_scanned);

        // The slice's event-time window is the ts range for the fetch. The
        // coordinator sets it to the envelope of this slice's pinned segments, a
        // superset of every one of them, so the `TsRange` filter drops no record
        // a local read (over the whole-snapshot window) would keep. Erasure is
        // threaded through the same funnel, applied per segment after decode.
        let query = LogQuery::new(window_start_ns, window_end_ns).with_erasure(erasure);

        let mut records: Vec<ravel_logseg::LogRecord> = Vec::new();
        // Logs carry no raw-f64 page counters (a metric-path concept), so
        // `FetchStats` stays zero, exactly what a local log read reports.
        let stats = FetchStats::default();
        for seg in &segments {
            let out = log_fetcher
                .fetch_accounted_with_tenant(seg, tenant_hash, &query, &accounting)
                .await
                // Carries the spend of this slice's already-fetched segments,
                // as the metric path does (issue #1723).
                .map_err(|e| {
                    SliceFailure::from(map_log_fetch_error(e)).with_spend(&accounting, &stats)
                })?;
            if let Some(output) = out {
                records.extend(output.records);
            }
            // Per-segment bytes-scanned short-circuit, matching the metric path
            // and the local per-segment check. The terminal summary carries the
            // spend so far so the coordinator folds this slice's real cost
            // before failing the query.
            if let Some(err) =
                bytes_scanned_exceeded(accounting.snapshot().total_s3_bytes(), byte_limit)
            {
                return Ok(vec![summary_frame(
                    &accounting.snapshot(),
                    0,
                    0,
                    pb::status::Code::BudgetExceeded,
                    err.to_string(),
                    &stats,
                )]);
            }
        }

        let records_returned = records.len() as u64;
        let mut frames = Vec::with_capacity(records.len() + 1);
        for record in &records {
            frames.push(pb::FetchResponse {
                frame: Some(pb::fetch_response::Frame::LogRecord(
                    codec::encode_log_record(record),
                )),
            });
        }
        // The record count rides the summary's `series_returned` field (reused
        // per signal). No coordinator reads it back today: the client half of
        // the log fan-out was deleted with the unbounded decoder it used
        // (issue #1912), so this is the wire contract a bounded log decoder
        // would have to honour, not something a live path consumes.
        frames.push(summary_frame(
            &accounting.snapshot(),
            records_returned,
            0,
            pb::status::Code::Ok,
            String::new(),
            &stats,
        ));
        Ok(frames)
    }

    /// The Spans slice path (#285): resolve the pinned scope to refs, fetch each
    /// segment through the production [`SpanSegmentFetcher`] funnel (so the
    /// per-span merged attribute view is byte-identical to a local `spans`
    /// read), enforce the per-slice bytes-scanned budget, apply erasure per
    /// segment through the same `is_erased_span` funnel the local scan uses, and
    /// stream one `SpanFrame` per surviving span ending in a terminal summary.
    ///
    /// Correctness under ADR-0052 resharding rests on segment self-containment,
    /// not slice atomicity: RSPAN rebuilds a span's whole merged `attrs` map
    /// from one segment alone (the reader re-inserts the lifted `service.name`
    /// column), so a worker reading only this slice's segments produces the
    /// exact per-span view a local read produces, and applies erasure identically
    /// per segment, whether or not a trace's spans straddle two slices (a trace
    /// whose spans were written on both sides of a reshard activation does). The
    /// coordinator (see [`crate::distrib::merge_spans`]) re-orders under a total
    /// order but never dedups (spans have no query-time dedup); this path never
    /// assumes a trace maps to one slice.
    #[allow(clippy::too_many_arguments)]
    async fn run_slice_spans(
        &self,
        scope: Option<pb::fetch_request::Scope>,
        budgets: Option<pb::Budgets>,
        tenant_hash: TenantHash,
        matchers: Vec<ravel_promql::LabelMatcher>,
        erasure: Vec<crate::erasure::ErasurePredicate>,
        window_start_ns: i64,
        window_end_ns: i64,
    ) -> Result<Vec<pb::FetchResponse>, SliceFailure> {
        let Some(span_fetcher) = self.span_fetcher.as_ref() else {
            return Err(SliceFailure::from((
                pb::status::Code::Unsupported,
                "distributed span fetch is not configured on this worker".to_string(),
            )));
        };

        // Span matcher pushdown (service_name/name/duration/status) has no
        // defined mapping in this lane yet (that is the SQL lane / spans pushdown,
        // out of #285's scope). Ignoring matchers would under-filter, so fail
        // closed to the coordinator's local fallback rather than a wrong result,
        // exactly as the log path does.
        if !matchers.is_empty() {
            return Err(SliceFailure::from((
                pb::status::Code::Unsupported,
                "distributed span fetch does not support matcher pushdown yet".to_string(),
            )));
        }

        let identities = match scope {
            Some(pb::fetch_request::Scope::Pinned(pinned)) => pinned.segments,
            Some(pb::fetch_request::Scope::Resolve(_)) | None => {
                return Err(SliceFailure::from((
                    pb::status::Code::Unsupported,
                    "resolve-scope slices are not supported yet".to_string(),
                )));
            }
        };

        let segments = self.resolve_pinned(&identities).await?;
        let accounting = QueryAccounting::new();

        let byte_limit = slice_byte_limit(budgets.as_ref(), self.engine.max_bytes_scanned);

        // The slice's event-time window is the ts range for the scan. The
        // coordinator sets it to the envelope of this slice's pinned segments, a
        // superset of every one of them, so the interval-overlap filter drops no
        // span a local read (over the whole-snapshot window) would keep. A bare
        // time-range query with no trace_id, no duration/status prune, and no
        // bloom predicates: matcher pushdown fell back above, so nothing narrows
        // the scan beyond the window.
        let query = SpanQuery::ts_range(window_start_ns, window_end_ns);

        let mut spans: Vec<crate::span_fetcher::SpanRow> = Vec::new();
        // Spans carry no raw-f64 page counters (a metric-path concept), so
        // `FetchStats` stays zero, exactly what a local span read reports.
        let stats = FetchStats::default();
        for seg in &segments {
            let out = span_fetcher
                .fetch_accounted(seg, tenant_hash, &query, None, None, &[], &accounting)
                .await
                // Carries the spend of this slice's already-fetched segments,
                // as the metric path does (issue #1723).
                .map_err(|e| {
                    SliceFailure::from(map_span_fetch_error(e)).with_spend(&accounting, &stats)
                })?;
            if let Some(output) = out {
                spans.extend(output.records);
            }
            // Per-segment bytes-scanned short-circuit, matching the metric and
            // log paths and the local per-segment check. The terminal summary
            // carries the spend so far so the coordinator folds this slice's real
            // cost before failing the query.
            if let Some(err) =
                bytes_scanned_exceeded(accounting.snapshot().total_s3_bytes(), byte_limit)
            {
                return Ok(vec![summary_frame(
                    &accounting.snapshot(),
                    0,
                    0,
                    pb::status::Code::BudgetExceeded,
                    err.to_string(),
                    &stats,
                )]);
            }
        }

        // Selective-erasure exclusion, applied post-decode exactly as the local
        // `spans` scan applies it (ADR-0064 decision 2, `is_erased_span`): the
        // coordinator does not re-apply, so worker-side application must match
        // the local rule. Correct under a straddling trace because each segment
        // is self-contained.
        if !erasure.is_empty() {
            spans
                .retain(|row| !is_erased_span(&row.record.attrs, row.record.start_ts_ns, &erasure));
        }

        let spans_returned = spans.len() as u64;
        let mut frames = Vec::with_capacity(spans.len() + 1);
        for row in &spans {
            frames.push(pb::FetchResponse {
                frame: Some(pb::fetch_response::Frame::Span(codec::encode_span_frame(
                    row,
                ))),
            });
        }
        // The span count rides the summary's `series_returned` field (reused per
        // signal). As on the log path above, no coordinator reads it back today
        // (issue #1912).
        frames.push(summary_frame(
            &accounting.snapshot(),
            spans_returned,
            0,
            pb::status::Code::Ok,
            String::new(),
            &stats,
        ));
        Ok(frames)
    }
}

/// The stream type the generated trait requires: a boxed stream of already-
/// built frames. The slice is fetched eagerly (it does not stream
/// mid-fetch), then replayed as a stream to satisfy the server-streaming RPC.
type FrameStream =
    Pin<Box<dyn Stream<Item = Result<pb::FetchResponse, tonic::Status>> + Send + 'static>>;

#[tonic::async_trait]
impl<R: SegmentResolver + 'static> SeriesFetch for SeriesFetchService<R> {
    type FetchStream = FrameStream;

    async fn fetch(
        &self,
        request: tonic::Request<pb::FetchRequest>,
    ) -> Result<tonic::Response<Self::FetchStream>, tonic::Status> {
        let frames = self.run_slice(request.into_inner()).await;
        let stream = futures::stream::iter(frames.into_iter().map(Ok));
        Ok(tonic::Response::new(Box::pin(stream)))
    }
}

/// The bytes-scanned limit one slice runs under: the TIGHTER of the
/// coordinator's wire budget and this worker's own configured limit (issue
/// #1687 part A).
///
/// `0` and an absent `Budgets` are the wire's "no cap" sentinel (a real fetch
/// never scans zero bytes), and they mean no cap FROM THE COORDINATOR, so
/// they resolve to the worker's own limit rather than to
/// [`ByteLimit::Unlimited`]. That is what makes a worker's configuration
/// binding on a request it did not author: a coordinator in another cluster
/// (federation) or on another version can send any budget it likes, or none,
/// and still never authorize more scanning here than this process's operator
/// allowed. The result is `Unlimited` only when the coordinator asked for no
/// cap and this worker itself is configured with none.
///
/// Since #1725 a coordinator sends the query's whole `max_bytes_scanned` to
/// every slice rather than a `1/slice_count` share of it, which is why this
/// clamp is the worker's only protection against an oversized budget.
pub(super) fn slice_byte_limit(budgets: Option<&pb::Budgets>, worker: ByteLimit) -> ByteLimit {
    let wire = match budgets.map(|b| b.max_bytes_scanned) {
        Some(0) | None => return worker,
        Some(max) => max,
    };
    match worker {
        ByteLimit::Bounded(own) => ByteLimit::Bounded(wire.min(own)),
        ByteLimit::Unlimited => ByteLimit::Bounded(wire),
    }
}

fn summary_frame(
    accounting: &QueryAccountingSnapshot,
    series_returned: u64,
    samples_returned: u64,
    code: pb::status::Code,
    message: String,
    stats: &FetchStats,
) -> pb::FetchResponse {
    pb::FetchResponse {
        frame: Some(pb::fetch_response::Frame::Summary(pb::Summary {
            accounting: Some(codec::encode_accounting(accounting)),
            series_returned,
            samples_returned,
            status: Some(pb::Status {
                code: code as i32,
                message,
            }),
            raw_f64_pages: stats.raw_f64_pages,
            raw_f64_bytes: stats.raw_f64_bytes,
        })),
    }
}

/// Reduces this worker's fetched scalar runs into one exact partial aggregate
/// per series (ADR-0103 decisions 1 and 3), plus the total number of deduped
/// samples the reduction consumed (for the terminal summary's
/// `samples_returned`).
///
/// The merge comes first and is not optional. `fetched` is per-segment, so two
/// segments of this slice can both carry the same `(series_id, ts)` -- a
/// duplicate write, or a run-merged L1 segment overlapping its L0 inputs.
/// Reducing each `FetchedSeriesSoa` on its own would count such a sample twice
/// and could report a bound from the sample the dedup order drops.
/// [`merge_soa_runs`] resolves each timestamp under the full ADR-0010 total
/// order first, so the reduction below runs over exactly one winning sample per
/// timestamp -- the same samples the coordinator's raw-fetch path would have
/// merged out of these runs.
///
/// Grouping by series id here, then merging one series at a time, is what
/// preserves the identity the frame must carry: [`merge_soa_runs`] returns
/// `SeriesData` (labels plus deduped samples) and drops the series id, and a
/// worker cannot recompute it (`SeriesId::compute` hashes the `TenantId`, and a
/// worker holds only a [`TenantHash`]). One call per series id is otherwise
/// identical to one call over everything: `merge_soa_runs` already merges each
/// series id's runs independently of every other series, and the caps are
/// unbounded here because the worker's own budget is bytes-scanned only (the
/// coordinator enforces the series and sample caps, over the summary this
/// returns).
///
/// `min`/`max` fold under [`f64::total_cmp`], the same total order ADR-0023's
/// min/max UDAF uses (`crates/ravel-sql/src/minmax.rs`): a candidate replaces
/// the incumbent only on a strict `Less`/`Greater`, so NaN payloads and the sign
/// of zero behave exactly as they do in every other typed-aggregate path here,
/// and never through `PartialOrd`. A series whose samples were all erased (or
/// all filtered out below) carries `count: Some(0)` with no bounds: there is no
/// value to bound, and an absent bound stays distinct from a present `0.0`.
///
/// Two filters run over each series' merged samples, AFTER the merge and BEFORE
/// the count/min/max fold, never before the merge:
///
/// - Staleness: a sample encoded as [`STALE_NAN_BITS`] is dropped
///   unconditionally (independent of `window`), matching the evaluator, which
///   drops staleness markers from every matrix selection before a range function
///   sees them (`eval_matrix_selector`). A window whose only sample is a
///   staleness marker must contribute 0 to the count, not 1.
/// - Reduction window (`window = Some((start, end))`, ADR-0103 amendment): a
///   sample survives iff `ts_ns > start && ts_ns <= end` -- exclusive start,
///   inclusive end, the same convention `eval_matrix_selector` uses. `None`
///   means no window restriction (today's only caller shape), reducing over
///   every merged sample.
///
/// Both filters run after [`merge_soa_runs`] on purpose. The merge's own dedup
/// tie-break (`is_greater`, a bit-pattern tuple comparison, unrelated to
/// `total_cmp`) decides which candidate at a shared timestamp survives, exactly
/// as it does on the raw path. Filtering staleness (or the window) before the
/// merge could let a losing real sample win a slot the raw path resolves to the
/// marker (or vice versa), diverging from the answer the same query gets on the
/// local path.
fn reduce_partial_aggregates(
    fetched: Vec<Vec<FetchedSeriesSoa>>,
    want: &pb::PartialAggregateRequest,
    window: Option<(i64, i64)>,
) -> Result<(Vec<codec::PartialAggregate>, u64), QueryError> {
    // Insertion-ordered grouping (a positions map beside the runs), so the frame
    // order a worker emits is a deterministic function of its fetch order rather
    // than of hash iteration order.
    let mut positions: HashMap<SeriesId, usize> = HashMap::new();
    let mut grouped: Vec<(SeriesId, Vec<FetchedSeriesSoa>)> = Vec::new();
    for segment_series in fetched {
        for fs in segment_series {
            match positions.get(&fs.series_id) {
                Some(&idx) => grouped[idx].1.push(fs),
                None => {
                    positions.insert(fs.series_id, grouped.len());
                    grouped.push((fs.series_id, vec![fs]));
                }
            }
        }
    }

    let mut partials = Vec::with_capacity(grouped.len());
    let mut samples_merged = 0u64;
    for (series_id, runs) in grouped {
        // One series id in, so at most one `SeriesData` out.
        let merged = merge_soa_runs(vec![runs], usize::MAX, usize::MAX)?;
        let Some(series) = merged.into_iter().next() else {
            continue;
        };
        // Staleness filter (unconditional) and the optional reduction window,
        // both applied here -- after the merge resolved each shared timestamp,
        // before the fold. See this function's doc comment for why the order
        // matters.
        let values: Vec<f64> = series
            .samples
            .iter()
            .filter(|s| s.value.to_bits() != STALE_NAN_BITS)
            .filter(|s| match window {
                Some((start, end)) => s.ts_ns > start && s.ts_ns <= end,
                None => true,
            })
            .map(|s| s.value)
            .collect();
        let count = values.len() as u64;
        samples_merged += count;
        let min = values
            .iter()
            .copied()
            .reduce(|current, candidate| replaces(candidate, current, Ordering::Less));
        let max = values
            .iter()
            .copied()
            .reduce(|current, candidate| replaces(candidate, current, Ordering::Greater));
        partials.push(codec::PartialAggregate {
            series_id,
            labels: series.labels,
            count: want.want_count.then_some(count),
            min: if want.want_min { min } else { None },
            max: if want.want_max { max } else { None },
        });
    }
    Ok((partials, samples_merged))
}

/// The PromQL staleness marker's bit pattern (a specific quiet-NaN payload),
/// the same constant `ravel_promql`'s `eval_matrix_selector` filters on. A
/// sample carrying it marks the series absent from that point forward and must
/// not be counted; the reduction drops it after the merge, matching the
/// evaluator's own pre-range-function filter.
const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// The worker's own local min/max fold step: `candidate` replaces `current` only
/// when `candidate.total_cmp(&current)` is the wanted strict ordering (`Less` for
/// min, `Greater` for max), so a tie keeps the incumbent. Mirrors
/// `Extreme::replaces` in `crates/ravel-sql/src/minmax.rs` field for field. This
/// is only the worker's local reduction over the samples one worker holds; it
/// makes no claim about any coordinator-side combine. No caller combines
/// `min`/`max` across workers today: T3's `total_cmp` fold is not the IEEE fold
/// PromQL's `min_over_time`/`max_over_time` compute, so min/max pushdown does not
/// ship until a PromQL-semantics worker fold exists (ADR-0103 amendment).
fn replaces(candidate: f64, current: f64, want: Ordering) -> f64 {
    if candidate.total_cmp(&current) == want {
        candidate
    } else {
        current
    }
}

/// Maps a worker-side merge failure to the slice's typed status. A run that is
/// not ascending, or a per-sample dedup column that is not parallel to its run,
/// is corruption in a decoded segment, not a budget or capability problem. The
/// caps are unbounded on this path, so the two budget variants cannot fire; they
/// map to `BudgetExceeded` rather than being collapsed into a catch-all, so a
/// future capped call site gets the right status instead of `Internal`.
fn map_merge_error(err: QueryError) -> (pb::status::Code, String) {
    match err {
        QueryError::TooManySeries { .. } | QueryError::TooManySamples { .. } => {
            (pb::status::Code::BudgetExceeded, err.to_string())
        }
        QueryError::NonMonotonicSamples { .. } | QueryError::PrioritySampleCountMismatch { .. } => {
            (pb::status::Code::Corrupt, err.to_string())
        }
        other => (pb::status::Code::Internal, other.to_string()),
    }
}

fn decode_tenant_hash(bytes: &[u8]) -> Result<TenantHash, String> {
    let arr: [u8; 16] = bytes
        .try_into()
        .map_err(|_| format!("tenant hash is {} bytes, expected 16", bytes.len()))?;
    Ok(TenantHash(arr))
}

/// Maps a fetch-path error to the worker's typed status. A vanished object is
/// a snapshot invalidation (retryable); a corrupt or etag-changed object is
/// terminal.
fn map_fetch_error(err: FetchError) -> (pb::status::Code, String) {
    match err {
        FetchError::Store {
            source: ravel_object_store::StoreError::NotFound,
            ..
        } => (pb::status::Code::SnapshotInvalidated, err.to_string()),
        FetchError::Store { .. } => (pb::status::Code::Unavailable, err.to_string()),
        FetchError::Corrupt { .. } => (pb::status::Code::Corrupt, err.to_string()),
        FetchError::EtagChanged { .. } => (pb::status::Code::Corrupt, err.to_string()),
        // A memory-budget refusal is terminal for this slice, never
        // `Unavailable`. On this codebase `Unavailable` is the re-dispatch class:
        // the coordinator's `try_remote` quarantines the refusing worker and
        // re-runs the slice on the next rendezvous worker, then on the
        // coordinator itself (`services/ravel-server/src/distrib.rs`), so a
        // refusal mapped to `Unavailable` would drain healthy workers into
        // quarantine and then perform the refused allocation on the very node the
        // budget exists to protect. `BudgetExceeded` is `Attempt::Keep` (terminal,
        // no re-dispatch, no local fallback), so the refusal sheds load instead of
        // amplifying it. The message carries only byte counts.
        FetchError::FetchMemoryExhausted { .. } => {
            (pb::status::Code::BudgetExceeded, err.to_string())
        }
    }
}

/// Maps an RLOG fetch-path error to the worker's typed status, mirroring
/// [`map_fetch_error`]: a vanished object is a snapshot invalidation
/// (retryable), a transient store error is unavailable, and a corrupt object is
/// terminal. A carried whole object paired with the wrong segment is terminal
/// too, like [`map_span_fetch_error`]'s tenant mismatch: the pairing is fixed
/// by the caller, so retrying or re-dispatching it elsewhere reproduces it.
fn map_log_fetch_error(err: LogFetchError) -> (pb::status::Code, String) {
    match err {
        LogFetchError::Store {
            source: ravel_object_store::StoreError::NotFound,
            ..
        } => (pb::status::Code::SnapshotInvalidated, err.to_string()),
        LogFetchError::Store { .. } => (pb::status::Code::Unavailable, err.to_string()),
        LogFetchError::Corrupt { .. } => (pb::status::Code::Corrupt, err.to_string()),
        LogFetchError::EtagChanged { .. } => (pb::status::Code::Corrupt, err.to_string()),
        LogFetchError::CarryMismatch { .. } => (pb::status::Code::Corrupt, err.to_string()),
        // Terminal, not `Unavailable`: see the rationale on [`map_fetch_error`].
        LogFetchError::FetchMemoryExhausted { .. } => {
            (pb::status::Code::BudgetExceeded, err.to_string())
        }
    }
}

/// Maps a span fetch-path error to the worker's typed status, mirroring
/// [`map_fetch_error`]/[`map_log_fetch_error`]: a vanished object is a snapshot
/// invalidation (retryable), a transient store error is unavailable, a corrupt
/// object is terminal, and an object belonging to another tenant is terminal
/// corruption of the coordinator's contract (a pinned segment must be this
/// tenant's), never a retry or a silent skip.
fn map_span_fetch_error(err: SpanFetchError) -> (pb::status::Code, String) {
    match err {
        SpanFetchError::Store {
            source: ravel_object_store::StoreError::NotFound,
            ..
        } => (pb::status::Code::SnapshotInvalidated, err.to_string()),
        SpanFetchError::Store { .. } => (pb::status::Code::Unavailable, err.to_string()),
        SpanFetchError::Corrupt { .. } => (pb::status::Code::Corrupt, err.to_string()),
        SpanFetchError::TenantMismatch { .. } => (pb::status::Code::Corrupt, err.to_string()),
        // Terminal, not `Unavailable`: see the rationale on [`map_fetch_error`].
        SpanFetchError::FetchMemoryExhausted { .. } => {
            (pb::status::Code::BudgetExceeded, err.to_string())
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod reconstruct_tests {
    use super::*;
    use prost::Message as _;
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_proto::commit::v1 as commit_pb;

    const WRITER_ID: &str = "8d9f0b8e-6f2a-4f4a-9a1e-2c3b4d5e6f70";
    const CONTENT_HASH: [u8; 32] = [0xAB; 32];
    const INPUT_SET_HASH: [u8; 32] = [0x5C; 32];
    const PART_HASH: [u8; 32] = [0xCD; 32];
    const HOUR: u32 = 472_000;

    fn tenant() -> TenantHash {
        ravel_types::TenantId::new("reconstruct-tenant".to_string()).hash()
    }

    fn writer() -> uuid::Uuid {
        uuid::Uuid::parse_str(WRITER_ID).expect("a uuid")
    }

    fn resolver(store: Arc<dyn ObjectStoreBackend>) -> ReconstructingSegmentResolver {
        ReconstructingSegmentResolver::new(store, tenant(), Signal::Metrics, limiter(8))
    }

    fn limiter(permits: usize) -> Arc<GetLimiter> {
        Arc::new(GetLimiter::new(permits).expect("nonzero permits"))
    }

    /// A commit record whose every field differs from the identity's non-key
    /// fields, so a ref field can only equal the record's if it was read from
    /// the record.
    fn l0_record() -> commit_pb::CommitRecord {
        ravel_commit::record::build(ravel_commit::record::NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 7,
            writer_id: writer(),
            writer_epoch: 3,
            writer_seq: 11,
            object_size: 4096,
            content_hash: CONTENT_HASH,
            sample_count: 120,
            series_count: 4,
            min_event_ts_ns: 1_699_999_000_000_000_000,
            max_event_ts_ns: 1_700_000_500_000_000_000,
            min_ingest_ts_ns: 1_699_999_100_000_000_000,
            max_ingest_ts_ns: 1_700_000_600_000_000_000,
            segment_format_version: 4,
            created_unix_ns: 1_700_000_000_000_000_000,
            ingest_hour_bucket: HOUR,
        })
        .expect("valid commit record")
    }

    fn l0_key() -> String {
        keys::commit_key(&tenant(), Signal::Metrics, 7, HOUR, writer(), 3, 11).expect("commit key")
    }

    /// The identity a coordinator ships for [`l0_record`]. Its
    /// `segment_format_version` is deliberately not the record's: the ref must
    /// take the record's.
    fn l0_identity() -> pb::SegmentIdentity {
        pb::SegmentIdentity {
            level: 0,
            shard: 7,
            ingest_hour_bucket: HOUR,
            writer_id: WRITER_ID.to_string(),
            writer_epoch: 3,
            writer_seq: 11,
            input_set_hash: Vec::new(),
            part_index: 0,
            content_hash: CONTENT_HASH.to_vec(),
            object_size: 4096,
            segment_format_version: 99,
        }
    }

    fn part(part_index: u32, content_hash: [u8; 32]) -> commit_pb::CompactionPart {
        commit_pb::CompactionPart {
            part_index,
            content_hash: content_hash.to_vec(),
            object_size: 8192 + u64::from(part_index),
            sample_count: 300 + u64::from(part_index),
            series_count: 9,
            min_event_ts_ns: 1_699_998_000_000_000_000,
            max_event_ts_ns: 1_700_001_000_000_000_000,
            segment_format_version: 3,
            ..Default::default()
        }
    }

    fn compaction_record() -> commit_pb::CompactionRecord {
        commit_pb::CompactionRecord {
            format_version: ravel_commit::record::COMPACTION_FORMAT_VERSION,
            tenant_hash: tenant().0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            shard: 7,
            ingest_hour_bucket: HOUR,
            level: 1,
            inputs: Vec::new(),
            input_set_hash: INPUT_SET_HASH.to_vec(),
            parts: vec![part(0, [0x11; 32]), part(2, PART_HASH)],
            created_unix_ns: 1_700_000_900_000_000_000,
        }
    }

    fn compaction_key() -> String {
        keys::compaction_record_key(
            &tenant(),
            Signal::Metrics,
            7,
            HOUR,
            &hex::encode(&INPUT_SET_HASH[..8]),
        )
        .expect("compaction record key")
    }

    /// A rewrite record that validates: its `input_set_hash` is the canonical
    /// hash of its inputs and applied request.
    fn rewrite_record() -> commit_pb::RewriteRecord {
        let inputs = vec![commit_pb::CompactionInputIdentity {
            writer_id: WRITER_ID.to_string(),
            writer_epoch: 3,
            writer_seq: 11,
        }];
        let request_id = "0b1c2d3e-4f50-4a6b-9c7d-8e9fa0b1c2d3".to_string();
        let input_set_hash = ravel_commit::erasure::compute_rewrite_input_set_hash(
            &inputs,
            None,
            std::slice::from_ref(&request_id),
        );
        commit_pb::RewriteRecord {
            format_version: ravel_commit::erasure::FORMAT_VERSION,
            tenant_hash: tenant().0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            shard: 7,
            ingest_hour_bucket: HOUR,
            inputs,
            input_set_hash: input_set_hash.to_vec(),
            parts: vec![part(1, PART_HASH)],
            drops: vec![commit_pb::RewriteDrop {
                request_id,
                dropped_count: 2,
            }],
            created_unix_ns: 1_700_000_950_000_000_000,
            superseded_record_key: String::new(),
        }
    }

    fn rewrite_key(record: &commit_pb::RewriteRecord) -> String {
        keys::rewrite_record_key_for(record).expect("rewrite record key")
    }

    fn l1_identity(
        input_set_hash: &[u8],
        part_index: u32,
        object_size: u64,
    ) -> pb::SegmentIdentity {
        pb::SegmentIdentity {
            level: 1,
            shard: 7,
            ingest_hour_bucket: HOUR,
            writer_id: uuid::Uuid::nil().to_string(),
            writer_epoch: 0,
            writer_seq: 0,
            input_set_hash: input_set_hash.to_vec(),
            part_index,
            content_hash: PART_HASH.to_vec(),
            object_size,
            segment_format_version: 99,
        }
    }

    async fn store_with(objects: Vec<(String, Vec<u8>)>) -> Arc<dyn ObjectStoreBackend> {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        for (key, data) in objects {
            store
                .put(&key, bytes::Bytes::from(data), PutOptions::default())
                .await
                .expect("put");
        }
        store
    }

    async fn store_with_l0() -> Arc<dyn ObjectStoreBackend> {
        store_with(vec![(l0_key(), l0_record().encode_to_vec())]).await
    }

    fn gets(accounting: &QueryAccounting) -> u64 {
        accounting.snapshot().s3_requests(AccountedOp::Get)
    }

    /// Resolves `identity`, expecting a refusal; returns it with the GET count.
    async fn refusal(
        store: Arc<dyn ObjectStoreBackend>,
        identity: &pb::SegmentIdentity,
    ) -> (ResolveIdentityError, u64) {
        let accounting = QueryAccounting::new();
        let err = resolver(store)
            .resolve(identity, &accounting)
            .await
            .expect_err("the identity must be refused");
        (err, gets(&accounting))
    }

    fn assert_mismatch(err: &ResolveIdentityError, key: &str, field: &'static str) {
        assert_eq!(
            err,
            &ResolveIdentityError::RecordMismatch {
                key: key.to_string(),
                field,
            }
        );
        assert_eq!(err.status_code(), pb::status::Code::Unsupported);
    }

    fn assert_record_invalid(err: &ResolveIdentityError, key: &str, needle: &str) {
        let ResolveIdentityError::RecordInvalid { key: got, reason } = err else {
            panic!("expected RecordInvalid, got {err:?}");
        };
        assert_eq!(got, key);
        assert!(reason.contains(needle), "{reason:?} lacks {needle:?}");
        assert_eq!(err.status_code(), pb::status::Code::Unsupported);
    }

    /// Every ref field is the verified record's, and the one GET is charged.
    #[tokio::test]
    async fn l0_ref_is_built_from_the_verified_commit_record() {
        let record = l0_record();
        let encoded = record.encode_to_vec();
        let store = store_with(vec![(l0_key(), encoded.clone())]).await;
        let accounting = QueryAccounting::new();
        let seg = resolver(store)
            .resolve(&l0_identity(), &accounting)
            .await
            .expect("L0 resolves");
        let expected = SegmentRef {
            data_object_key: record.object_key.clone(),
            object_size: 4096,
            min_event_ts_ns: 1_699_999_000_000_000_000,
            max_event_ts_ns: 1_700_000_500_000_000_000,
            ingest_hour_bucket: HOUR,
            sample_count: 120,
            series_count: 4,
            shard: 7,
            content_hash: CONTENT_HASH,
            writer_id: writer(),
            writer_epoch: 3,
            writer_seq: 11,
            created_unix_ns: 1_700_000_000_000_000_000,
            level: SegmentLevel::L0,
            segment_format_version: 4,
            declared_column_stats: DeclaredColumnStats::default(),
        };
        assert_eq!(seg, expected);
        assert_eq!(
            seg.data_object_key,
            keys::data_key(
                &tenant(),
                Signal::Metrics,
                7,
                writer(),
                3,
                11,
                &CONTENT_HASH
            )
            .expect("data key")
        );
        let spend = accounting.snapshot();
        assert_eq!(spend.s3_requests(AccountedOp::Get), 1);
        assert_eq!(spend.s3_bytes(AccountedOp::Get), encoded.len() as u64);
    }

    /// The tenant comes from the resolver, never the identity: the same
    /// identity under another tenant addresses a record that does not exist.
    #[tokio::test]
    async fn the_tenant_comes_from_the_resolver_not_the_identity() {
        let store = store_with_l0().await;
        let other = ravel_types::TenantId::new("other-tenant".to_string()).hash();
        let accounting = QueryAccounting::new();
        let err = ReconstructingSegmentResolver::new(store, other, Signal::Metrics, limiter(8))
            .resolve(&l0_identity(), &accounting)
            .await
            .expect_err("another tenant's record is absent");
        let other_key =
            keys::commit_key(&other, Signal::Metrics, 7, HOUR, writer(), 3, 11).expect("key");
        assert_eq!(err, ResolveIdentityError::RecordMissing { key: other_key });
    }

    #[tokio::test]
    async fn a_missing_commit_record_is_record_missing() {
        let store = store_with(Vec::new()).await;
        let (err, gets) = refusal(store, &l0_identity()).await;
        assert_eq!(err, ResolveIdentityError::RecordMissing { key: l0_key() });
        assert_eq!(err.status_code(), pb::status::Code::Unsupported);
        assert_eq!(gets, 1, "the failed GET is still charged");
    }

    #[tokio::test]
    async fn an_unreadable_commit_record_is_record_read() {
        let inner = store_with_l0().await;
        let fault = Arc::new(FaultStore::new(
            inner,
            FaultPlan::empty().with_rule(Rule::new(
                Op::Get,
                ScriptedFault::Permanent("record GET fails".into()),
            )),
        ));
        let (err, _) = refusal(fault.clone(), &l0_identity()).await;
        assert_eq!(fault.fault_count(Op::Get, FaultKind::Permanent), 1);
        let ResolveIdentityError::RecordRead { key, .. } = &err else {
            panic!("expected RecordRead, got {err:?}");
        };
        assert_eq!(key, &l0_key());
        assert_eq!(err.status_code(), pb::status::Code::Unsupported);
    }

    /// A retryable store error on the record GET is `UNAVAILABLE`, so the
    /// coordinator re-dispatches that one slice instead of running the whole
    /// query locally. Each of the three retryable kinds is checked, and each
    /// fault is proven to have fired.
    #[tokio::test]
    async fn a_retryable_record_get_error_is_unavailable() {
        let cases = [
            (
                ScriptedFault::Transient("blip".into()),
                FaultKind::Transient,
            ),
            (ScriptedFault::Timeout, FaultKind::Timeout),
            (
                ScriptedFault::Throttled { retry_after_ms: 5 },
                FaultKind::Throttled,
            ),
        ];
        for (scripted, kind) in cases {
            let inner = store_with_l0().await;
            let fault = Arc::new(FaultStore::new(
                inner,
                FaultPlan::empty().with_rule(Rule::new(Op::Get, scripted)),
            ));
            let (err, gets) = refusal(fault.clone(), &l0_identity()).await;
            assert_eq!(fault.fault_count(Op::Get, kind), 1, "{kind:?} fired");
            assert_eq!(gets, 1, "{kind:?}: one record GET");
            let ResolveIdentityError::RecordUnavailable { key, .. } = &err else {
                panic!("{kind:?}: expected RecordUnavailable, got {err:?}");
            };
            assert_eq!(key, &l0_key());
            assert_eq!(
                err.status_code(),
                pb::status::Code::Unavailable,
                "{kind:?}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn an_undecodable_commit_record_is_record_invalid() {
        let store = store_with(vec![(l0_key(), vec![0xff, 0xff, 0xff])]).await;
        let (err, _) = refusal(store, &l0_identity()).await;
        assert_record_invalid(&err, &l0_key(), "");
    }

    /// `record::decode` validates: a record with an inverted event range at the
    /// right key is refused.
    #[tokio::test]
    async fn a_commit_record_failing_validation_is_record_invalid() {
        let mut record = l0_record();
        record.min_event_ts_ns = record.max_event_ts_ns + 1;
        let store = store_with(vec![(l0_key(), record.encode_to_vec())]).await;
        let (err, _) = refusal(store, &l0_identity()).await;
        assert_record_invalid(&err, &l0_key(), "event");
    }

    /// A self-consistent record for seq 12 stored at seq 11's key. Only the
    /// key check refuses it: its `object_key`, content hash and size all pass.
    #[tokio::test]
    async fn a_commit_record_for_another_commit_is_record_invalid() {
        let mut record = l0_record();
        record.writer_seq = 12;
        record.object_key = keys::reconstruct_data_key(&record).expect("data key");
        let store = store_with(vec![(l0_key(), record.encode_to_vec())]).await;
        let (err, _) = refusal(store, &l0_identity()).await;
        assert_record_invalid(&err, &l0_key(), "addresses");
    }

    #[tokio::test]
    async fn a_tampered_object_key_is_record_invalid() {
        let mut record = l0_record();
        record.object_key = format!("{}.moved", record.object_key);
        let store = store_with(vec![(l0_key(), record.encode_to_vec())]).await;
        let (err, _) = refusal(store, &l0_identity()).await;
        assert_record_invalid(&err, &l0_key(), "object_key mismatch");
    }

    /// The last byte differs, past the 8 bytes any key embeds.
    #[tokio::test]
    async fn an_l0_content_hash_tail_mismatch_is_refused() {
        let mut identity = l0_identity();
        identity.content_hash[31] ^= 0x01;
        let (err, _) = refusal(store_with_l0().await, &identity).await;
        assert_mismatch(&err, &l0_key(), "content_hash");
    }

    #[tokio::test]
    async fn an_l0_object_size_mismatch_is_refused() {
        let mut identity = l0_identity();
        identity.object_size = 4097;
        let (err, _) = refusal(store_with_l0().await, &identity).await;
        assert_mismatch(&err, &l0_key(), "object_size");
    }

    /// A malformed identity is refused before any record is read, even though
    /// a valid record is in the store.
    async fn assert_invalid(identity: pb::SegmentIdentity, needle: &str) {
        let store = store_with(vec![
            (l0_key(), l0_record().encode_to_vec()),
            (compaction_key(), compaction_record().encode_to_vec()),
        ])
        .await;
        let (err, gets) = refusal(store, &identity).await;
        let ResolveIdentityError::Invalid { reason } = &err else {
            panic!("expected Invalid, got {err:?}");
        };
        assert!(reason.contains(needle), "{reason:?} lacks {needle:?}");
        assert_eq!(err.status_code(), pb::status::Code::BadData);
        assert_eq!(gets, 0, "no record is read for a malformed identity");
    }

    #[tokio::test]
    async fn a_non_uuid_writer_id_is_refused() {
        let mut identity = l0_identity();
        identity.writer_id = "not-a-uuid".to_string();
        assert_invalid(identity, "not a uuid").await;
    }

    #[tokio::test]
    async fn a_short_content_hash_is_refused() {
        let mut identity = l0_identity();
        identity.content_hash = vec![0xAB; 31];
        assert_invalid(identity, "31").await;
    }

    #[tokio::test]
    async fn an_absent_content_hash_is_refused() {
        let mut identity = l0_identity();
        identity.content_hash = Vec::new();
        assert_invalid(identity, "0").await;
    }

    /// The key shape formats the shard as four digits, so a shard outside that
    /// range has no key at all; it must not be truncated into another shard's.
    #[tokio::test]
    async fn an_out_of_range_shard_is_refused() {
        let mut identity = l0_identity();
        identity.shard = 10_000;
        assert_invalid(identity, "10000").await;
    }

    #[tokio::test]
    async fn an_l0_identity_carrying_an_input_set_hash_is_refused() {
        let mut identity = l0_identity();
        identity.input_set_hash = INPUT_SET_HASH.to_vec();
        assert_invalid(identity, "L1 compaction fields").await;
    }

    #[tokio::test]
    async fn an_l0_identity_carrying_a_part_index_is_refused() {
        let mut identity = l0_identity();
        identity.part_index = 1;
        assert_invalid(identity, "L1 compaction fields").await;
    }

    #[tokio::test]
    async fn an_unknown_level_is_refused() {
        let mut identity = l0_identity();
        identity.level = 2;
        assert_invalid(identity, "unknown segment level 2").await;
    }

    #[tokio::test]
    async fn an_l1_identity_without_a_32_byte_input_set_hash_is_refused() {
        assert_invalid(l1_identity(&INPUT_SET_HASH[..16], 2, 8194), "16 bytes").await;
    }

    /// Every L1 ref field is the verified compaction record's or its part's.
    #[tokio::test]
    async fn l1_ref_is_built_from_the_verified_compaction_record() {
        let record = compaction_record();
        let store = store_with(vec![(compaction_key(), record.encode_to_vec())]).await;
        let accounting = QueryAccounting::new();
        let seg = resolver(store)
            .resolve(&l1_identity(&INPUT_SET_HASH, 2, 8194), &accounting)
            .await
            .expect("L1 resolves");
        let expected = SegmentRef {
            data_object_key: keys::reconstruct_l1_part_key(&record, &record.parts[1])
                .expect("part key"),
            object_size: 8194,
            min_event_ts_ns: 1_699_998_000_000_000_000,
            max_event_ts_ns: 1_700_001_000_000_000_000,
            ingest_hour_bucket: HOUR,
            sample_count: 302,
            series_count: 9,
            shard: 7,
            content_hash: PART_HASH,
            writer_id: uuid::Uuid::nil(),
            writer_epoch: 0,
            writer_seq: 0,
            created_unix_ns: 1_700_000_900_000_000_000,
            level: SegmentLevel::L1 {
                input_set_hash: INPUT_SET_HASH,
                part_index: 2,
            },
            segment_format_version: 3,
            declared_column_stats: DeclaredColumnStats::default(),
        };
        assert_eq!(seg, expected);
        assert_eq!(gets(&accounting), 1);
    }

    /// With no compaction record, an erasure rewrite record at the same bucket
    /// and input-set hash serves the part, at the cost of a second GET.
    #[tokio::test]
    async fn l1_ref_falls_back_to_the_verified_rewrite_record() {
        let record = rewrite_record();
        let store = store_with(vec![(rewrite_key(&record), record.encode_to_vec())]).await;
        let input_set_hash: [u8; 32] = record
            .input_set_hash
            .as_slice()
            .try_into()
            .expect("32 bytes");
        let accounting = QueryAccounting::new();
        let seg = resolver(store)
            .resolve(&l1_identity(&input_set_hash, 1, 8193), &accounting)
            .await
            .expect("rewrite part resolves");
        assert_eq!(
            seg.data_object_key,
            keys::reconstruct_rewrite_part_key(&record, &record.parts[0]).expect("part key")
        );
        assert_eq!(seg.created_unix_ns, 1_700_000_950_000_000_000);
        assert_eq!(seg.object_size, 8193);
        assert_eq!(seg.sample_count, 301);
        assert_eq!(
            seg.level,
            SegmentLevel::L1 {
                input_set_hash,
                part_index: 1,
            }
        );
        assert_eq!(gets(&accounting), 2);
    }

    #[tokio::test]
    async fn an_l1_part_with_neither_record_is_record_missing() {
        let identity = l1_identity(&INPUT_SET_HASH, 2, 8194);
        let (err, gets) = refusal(store_with(Vec::new()).await, &identity).await;
        let rewrite_key = keys::rewrite_record_key(
            &tenant(),
            Signal::Metrics,
            7,
            HOUR,
            &hex::encode(&INPUT_SET_HASH[..8]),
        )
        .expect("rewrite key");
        assert_eq!(
            err,
            ResolveIdentityError::RecordMissing { key: rewrite_key }
        );
        assert_eq!(err.status_code(), pb::status::Code::Unsupported);
        assert_eq!(gets, 2);
    }

    /// A compaction record whose own fields reconstruct another bucket's key
    /// (here shard 8) is refused where it was read.
    #[tokio::test]
    async fn a_compaction_record_for_another_bucket_is_record_invalid() {
        let mut record = compaction_record();
        record.shard = 8;
        let store = store_with(vec![(compaction_key(), record.encode_to_vec())]).await;
        let (err, _) = refusal(store, &l1_identity(&INPUT_SET_HASH, 2, 8194)).await;
        assert_record_invalid(&err, &compaction_key(), "mismatch");
    }

    #[tokio::test]
    async fn an_undecodable_compaction_record_is_record_invalid() {
        let store = store_with(vec![(compaction_key(), vec![0xff, 0xff])]).await;
        let (err, _) = refusal(store, &l1_identity(&INPUT_SET_HASH, 2, 8194)).await;
        assert_record_invalid(&err, &compaction_key(), "");
    }

    /// A rewrite record whose `input_set_hash` is not the canonical hash of its
    /// own contents fails `decode_rewrite`'s validation.
    #[tokio::test]
    async fn a_rewrite_record_failing_validation_is_record_invalid() {
        let mut record = rewrite_record();
        let key = rewrite_key(&record);
        let input_set_hash = record.input_set_hash.clone();
        record.drops[0].dropped_count = 3;
        record.drops[0].request_id = "1b1c2d3e-4f50-4a6b-9c7d-8e9fa0b1c2d3".to_string();
        let store = store_with(vec![(key.clone(), record.encode_to_vec())]).await;
        let (err, _) = refusal(store, &l1_identity(&input_set_hash, 1, 8193)).await;
        assert_record_invalid(&err, &key, "");
    }

    /// A valid rewrite record for shard 8 stored at shard 7's rewrite key: its
    /// input-set hash does not cover the shard, so only the key check refuses
    /// it.
    #[tokio::test]
    async fn a_rewrite_record_for_another_bucket_is_record_invalid() {
        let mut record = rewrite_record();
        let key = rewrite_key(&record);
        record.shard = 8;
        let input_set_hash = record.input_set_hash.clone();
        let store = store_with(vec![(key.clone(), record.encode_to_vec())]).await;
        let (err, _) = refusal(store, &l1_identity(&input_set_hash, 1, 8193)).await;
        assert_record_invalid(&err, &key, "mismatch");
    }

    async fn store_with_compaction() -> Arc<dyn ObjectStoreBackend> {
        store_with(vec![(
            compaction_key(),
            compaction_record().encode_to_vec(),
        )])
        .await
    }

    /// The last byte differs, past the 8 bytes the record key embeds, so the
    /// same record is read and the full comparison refuses it.
    #[tokio::test]
    async fn an_input_set_hash_tail_mismatch_is_refused() {
        let mut input_set_hash = INPUT_SET_HASH;
        input_set_hash[31] ^= 0x01;
        let identity = l1_identity(&input_set_hash, 2, 8194);
        let (err, _) = refusal(store_with_compaction().await, &identity).await;
        assert_mismatch(&err, &compaction_key(), "input_set_hash");
    }

    #[tokio::test]
    async fn a_part_index_absent_from_the_record_is_refused() {
        let identity = l1_identity(&INPUT_SET_HASH, 1, 8194);
        let (err, _) = refusal(store_with_compaction().await, &identity).await;
        assert_mismatch(&err, &compaction_key(), "part_index");
    }

    #[tokio::test]
    async fn an_l1_content_hash_tail_mismatch_is_refused() {
        let mut identity = l1_identity(&INPUT_SET_HASH, 2, 8194);
        identity.content_hash[31] ^= 0x01;
        let (err, _) = refusal(store_with_compaction().await, &identity).await;
        assert_mismatch(&err, &compaction_key(), "content_hash");
    }

    #[tokio::test]
    async fn an_l1_object_size_mismatch_is_refused() {
        let identity = l1_identity(&INPUT_SET_HASH, 2, 8195);
        let (err, _) = refusal(store_with_compaction().await, &identity).await;
        assert_mismatch(&err, &compaction_key(), "object_size");
    }

    /// Refs come back in identity order, and the first refusal fails the set.
    #[tokio::test]
    async fn identities_resolve_in_order_and_stop_at_the_first_refusal() {
        let store = store_with(vec![
            (l0_key(), l0_record().encode_to_vec()),
            (compaction_key(), compaction_record().encode_to_vec()),
        ])
        .await;
        let resolver = resolver(store);
        let accounting = QueryAccounting::new();
        let identities = vec![l1_identity(&INPUT_SET_HASH, 2, 8194), l0_identity()];
        let refs = resolve_identities(&resolver, &identities, &accounting)
            .await
            .expect("both resolve");
        assert_eq!(refs.len(), 2);
        assert!(matches!(refs[0].level, SegmentLevel::L1 { .. }));
        assert_eq!(refs[1].level, SegmentLevel::L0);

        let mut bad = l0_identity();
        bad.object_size = 1;
        let err = resolve_identities(&resolver, &[l0_identity(), bad], &accounting)
            .await
            .expect_err("the second identity is refused");
        assert_mismatch(&err, &l0_key(), "object_size");
    }

    /// Counts the GETs in flight at its own `get` and keeps the peak. Each GET
    /// yields before completing, so on a current-thread runtime every GET a
    /// caller lets start overlaps every other one it lets start.
    struct PeakGetStore {
        inner: MemoryStore,
        in_flight: std::sync::atomic::AtomicUsize,
        peak: std::sync::atomic::AtomicUsize,
        gets: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for PeakGetStore {
        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            use std::sync::atomic::Ordering::SeqCst;
            self.gets.fetch_add(1, SeqCst);
            let now = self.in_flight.fetch_add(1, SeqCst) + 1;
            self.peak.fetch_max(now, SeqCst);
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }
            let got = self.inner.get(key, range).await;
            self.in_flight.fetch_sub(1, SeqCst);
            got
        }

        async fn put_multipart<'a>(
            &'a self,
            key: &str,
        ) -> Result<Box<dyn ravel_object_store::MultipartUpload + 'a>, StoreError> {
            self.inner.put_multipart(key).await
        }

        async fn head(&self, key: &str) -> Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
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

    /// Record GETs draw from the GET limiter the resolver was built with, the
    /// same process-wide one the data-object GETs use (ADR-1195), so
    /// `RESOLVE_CONCURRENCY` never lets a slice put more record GETs in flight
    /// than that limiter's permits. Eight pinned identities resolve under
    /// limiters of 1 and 3 permits; the store's own peak in-flight count must
    /// equal the permit count exactly, and every identity costs one GET.
    #[tokio::test]
    async fn record_gets_are_bounded_by_the_shared_get_limiter() {
        for permits in [1usize, 3] {
            let inner = MemoryStore::new();
            inner
                .put(
                    &l0_key(),
                    bytes::Bytes::from(l0_record().encode_to_vec()),
                    PutOptions::default(),
                )
                .await
                .expect("put");
            let store = Arc::new(PeakGetStore {
                inner,
                in_flight: Default::default(),
                peak: Default::default(),
                gets: Default::default(),
            });
            let resolver = ReconstructingSegmentResolver::new(
                Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
                tenant(),
                Signal::Metrics,
                limiter(permits),
            );
            let identities = vec![l0_identity(); 8];
            let accounting = QueryAccounting::new();
            let refs = resolve_identities(&resolver, &identities, &accounting)
                .await
                .expect("every identity resolves");
            assert_eq!(refs.len(), 8);
            use std::sync::atomic::Ordering::SeqCst;
            assert_eq!(store.gets.load(SeqCst), 8, "one record GET per identity");
            assert_eq!(
                store.peak.load(SeqCst),
                permits,
                "record GETs in flight must be capped at the limiter's {permits} permits"
            );
        }
    }

    /// A snapshot resolver miss is retryable, not terminal: the coordinator
    /// re-resolves once and re-dispatches.
    #[tokio::test]
    async fn a_snapshot_resolver_miss_is_snapshot_invalidated() {
        let resolver = SnapshotSegmentResolver::new(std::iter::empty());
        let err = resolver
            .resolve(&l0_identity(), &QueryAccounting::new())
            .await
            .expect_err("an empty snapshot resolves nothing");
        assert!(matches!(err, ResolveIdentityError::Unknown { .. }));
        assert_eq!(err.status_code(), pb::status::Code::SnapshotInvalidated);
    }
}
