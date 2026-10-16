//! Shared write mechanics for `Signal::Audit` records (ADR-0040, ADR-0042).
//!
//! Every audit record - a legal-hold set/clear ([`crate::legal_hold`]) or a
//! query-audit entry ([`crate::query_audit`]) - is one immutable RLOG object
//! plus its commit record, written in the same durability order `ravel-ingest`
//! uses for any L0 log object: the data object first, then the commit record
//! that references it. This module holds that mechanics once so the two audit
//! writers do not each carry a private copy of the ~100 lines of encode,
//! content-hash, and dual-PUT logic; each supplies only the record-specific
//! parts (its shard, stream identity, severity, body, and attrs) through
//! [`AuditWrite`].

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{RngSource, SystemRng};
use ravel_logseg::{AttrValue, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter};
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError, UploadChecksum};
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

use crate::config::NS_PER_HOUR;
use crate::error::{MaintainError, Result};
use crate::rlog::OUTPUT_FORMAT_VERSION;

/// Attempts per PUT before a transient error fails the batch closed (ADR-0062
/// amendment, 2026-09-27): one attempt plus up to two retries, so a single
/// transient object-store timeout does not fail every query in the batch
/// (issue #2035). Only [`StoreError::is_retryable`] errors are retried;
/// `AlreadyExists`, `AccessDenied`, and other permanent errors are not.
/// Derived from [`RETRY_BACKOFF_BASE_MS`]'s length so the two cannot drift
/// apart: one backoff entry exists per retry, and the retry loops index that
/// array by `attempt - 1`.
const MAX_PUT_ATTEMPTS: u32 = RETRY_BACKOFF_BASE_MS.len() as u32 + 1;

/// Backoff before retry attempts 2 and 3, before jitter. With
/// [`jittered_backoff`]'s 0.75x-1.25x jitter, the worst case added wall time
/// for one PUT that exhausts all attempts is its two backoffs' jittered highs
/// added together: (50 * 1.25) + (200 * 1.25) = 62 + 250 = 312 ms. That is on
/// top of the object-store client's own request timeout for each attempt,
/// and both PUTs together are still bounded by [`AUDIT_WRITE_BUDGET`].
const RETRY_BACKOFF_BASE_MS: [u64; 2] = [50, 200];

/// Total wall-clock budget for one [`write_audit_batch`] call, covering every
/// attempt of both PUTs (data object, then commit record) and the
/// `AlreadyExists`-on-retry read-back GET of the commit key. Sized as room for
/// one full object-store request timeout (`DEFAULT_REQUEST_TIMEOUT`, 20 s in
/// `ravel-object-store`'s S3 backend) plus a retry of that same PUT, and
/// still leave the commit PUT its own attempt, while keeping the worst case
/// below the pre-change (single-attempt) worst case of two un-retried 20 s
/// timeouts back to back (40 s): a hung store now fails the batch closed by
/// 30 s instead of stretching to 120 s across the full three-attempt ladder
/// on both PUTs, and still comes in under the 40 s the batch could already
/// take before this retry ladder existed.
const AUDIT_WRITE_BUDGET: Duration = Duration::from_secs(30);

/// Which of a batch's two PUTs an attempt or retry belongs to, for logging.
#[derive(Clone, Copy, Debug)]
enum AuditPut {
    Data,
    Commit,
}

impl std::fmt::Display for AuditPut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            AuditPut::Data => "data",
            AuditPut::Commit => "commit",
        })
    }
}

/// `base_ms` scaled by a uniform random factor in `0.75..=1.25`, so concurrent
/// retries after a shared transient failure (a throttled backend, a network
/// blip affecting several batches at once) do not all retry in lockstep.
/// Draws through the injected `rng` rather than OS entropy directly (ADR-0068
/// decision 2, `ravel_commit::rng`): backoff jitter on this production path
/// must be replayable by the seeded simulation harness.
fn jittered_backoff(base_ms: u64, rng: &dyn RngSource) -> Duration {
    let low_ms = base_ms * 3 / 4;
    let range_ms = base_ms / 2;
    Duration::from_millis(low_ms + rng.jitter_ms(range_ms))
}

/// The delay before a retry: normally [`jittered_backoff`], but a `Throttled`
/// error's own `retry_after_ms` hint takes priority when it asks for longer
/// than that -- the backend is telling the caller how long it will keep
/// rejecting requests, and jitter alone might retry before the hint expires.
fn retry_delay(error: &StoreError, base_ms: u64, rng: &dyn RngSource) -> Duration {
    let jittered = jittered_backoff(base_ms, rng);
    match error {
        StoreError::Throttled { retry_after_ms } => {
            jittered.max(Duration::from_millis(*retry_after_ms))
        }
        _ => jittered,
    }
}

/// Races `fut` against the remaining budget to `deadline`, returning `None`
/// if the deadline has already passed or elapses while `fut` is still in
/// flight. Built on `tokio::time` (whose paused-clock test mode makes a PUT
/// that never returns deterministic to test) rather than an injected `Clock`:
/// this budget is a hard ceiling on one library call, not simulation-harness
/// state that needs to replay from a master seed.
async fn bound_to_deadline<F, T>(deadline: tokio::time::Instant, fut: F) -> Option<T>
where
    F: Future<Output = T>,
{
    if tokio::time::Instant::now() >= deadline {
        return None;
    }
    tokio::select! {
        result = fut => Some(result),
        () = tokio::time::sleep_until(deadline) => None,
    }
}

/// Count the retry (if `put_retries` is `Some`, i.e. this write is going
/// through [`crate::audit_pipeline::AuditPipeline`]) and log it at WARN: a
/// retry is a real signal that the object store is degraded, even though the
/// batch itself will likely still succeed.
fn note_put_retry(
    put: AuditPut,
    attempt: u32,
    error: &StoreError,
    put_retries: Option<&AtomicU64>,
) {
    if let Some(counter) = put_retries {
        counter.fetch_add(1, Ordering::Relaxed);
    }
    tracing::warn!(
        put = %put,
        attempt,
        error = %error,
        "audit batch PUT hit a transient error, retrying"
    );
}

/// One [`Signal::Audit`] record to encode and publish. The caller supplies the
/// record-specific parts; [`write_audit_object`] owns the object/commit
/// mechanics common to every audit kind.
pub(crate) struct AuditWrite {
    /// The [`Signal::Audit`] shard this record is written to. All audit kinds
    /// share the control-plane audit shard today (see
    /// [`crate::legal_hold::AUDIT_HOLD_SHARD`]).
    pub shard: u32,
    /// A caller-supplied unique object identity (a fresh `Uuid`), keeping this
    /// library function free of hidden nondeterminism.
    pub record_id: Uuid,
    /// The record timestamp; also its event/ingest-time bounds and hour bucket.
    pub now_ns: i64,
    /// The shared log stream's id and canonical resource+scope blob.
    pub stream_id: LogStreamId,
    pub stream_attrs: Vec<u8>,
    pub severity_num: u8,
    pub severity_text: String,
    pub body: String,
    pub attrs: Vec<(String, AttrValue)>,
}

/// The record-specific content of one audit log record: everything an
/// [`AuditWrite`] carries *except* the object-level identity (`shard` and
/// `record_id`), which a whole batch shares. One [`write_audit_batch`] call
/// encodes a `Vec<AuditRecord>` into exactly one RLOG object under one shared
/// `record_id`, plus one commit record whose aggregate fields span the batch.
///
/// This is also the submitter-facing event of the group-commit
/// [`crate::audit_pipeline::AuditPipeline`], re-exported there as
/// `AuditEvent`: a submitter hands over the record's content and the pipeline
/// owns the shard, the per-batch `record_id`, and the flush.
#[derive(Clone, Debug)]
pub struct AuditRecord {
    /// The tenant this record belongs to. Every object-level identity the
    /// batch write derives - the data key, the object header's tenant hash,
    /// and the commit record - comes from this field, so a record can only
    /// ever be filed under the tenant whose action it describes. A batch
    /// mixing tenants is rejected rather than filed under one of them.
    pub tenant: TenantHash,
    /// The record timestamp; contributes to the batch's event/ingest-time
    /// bounds and hour bucket.
    pub now_ns: i64,
    /// The record's log stream id and canonical resource+scope blob.
    pub stream_id: LogStreamId,
    pub stream_attrs: Vec<u8>,
    pub severity_num: u8,
    pub severity_text: String,
    pub body: String,
    pub attrs: Vec<(String, AttrValue)>,
}

impl AuditRecord {
    fn into_log_record(self) -> LogRecord {
        LogRecord {
            stream_id: self.stream_id,
            stream_attrs: self.stream_attrs,
            ts_ns: self.now_ns,
            observed_ts_ns: self.now_ns,
            severity_num: self.severity_num,
            severity_text: self.severity_text,
            body: self.body,
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: self.attrs,
        }
    }
}

/// Encode one audit RLOG record as an L0 [`Signal::Audit`] object and its
/// commit record, and PUT both (data object first, commit record last, the
/// ingest durability order). This is the minimal write path for a
/// `Signal::Audit` record: one record, one object, one commit record, exactly
/// as `ravel-ingest` writes an L0 log object.
///
/// This is the degenerate one-record case of [`write_audit_batch`]; it exists
/// unchanged for the per-record callers ([`crate::legal_hold`],
/// [`crate::provision_audit`], [`crate::query_audit`]) that mint one object per
/// record.
pub(crate) async fn write_audit_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    write: AuditWrite,
) -> Result<()> {
    let shard = write.shard;
    let record_id = write.record_id;
    let record = AuditRecord {
        tenant: *tenant,
        now_ns: write.now_ns,
        stream_id: write.stream_id,
        stream_attrs: write.stream_attrs,
        severity_num: write.severity_num,
        severity_text: write.severity_text,
        body: write.body,
        attrs: write.attrs,
    };
    write_audit_batch(store, shard, record_id, vec![record], None).await
}

/// Encode a whole batch of audit records as **one** L0 [`Signal::Audit`] object
/// and **one** commit record, and PUT both in the same data-object-first,
/// commit-record-last durability order [`write_audit_object`] uses for a single
/// record. Every record in `records` is pushed into one [`RlogWriter`] under
/// the shared `record_id`, so the batch is one immutable object; its commit
/// record's aggregate fields (`sample_count`, `series_count`, and the
/// event/ingest-time bounds) span the whole batch rather than being hardcoded
/// to one record's values.
///
/// This is the durable half of the group-commit
/// [`crate::audit_pipeline::AuditPipeline`]: the pipeline accumulates submitted
/// events into a batch, mints one `record_id`, and calls this once per flush.
/// The idempotency and conflict semantics are identical to the single-record
/// path: `AlreadyExists` on the content-addressed data object is a benign
/// idempotent republish on every attempt, while `AlreadyExists` on the commit
/// record's *first* attempt is a hard error because a reused `record_id`
/// would collide two logically distinct batches onto one commit key.
/// `AlreadyExists` on a *retry* attempt is different: the previous attempt's
/// PUT may have landed before its response was lost to the same transient
/// error that triggered the retry, so that case compares the stored bytes
/// against the ones this attempt tried to write (ADR-0062 amendment,
/// 2026-09-27) rather than assuming a collision.
///
/// Each of the batch's two PUTs attempts up to [`MAX_PUT_ATTEMPTS`] times on a
/// [`StoreError::is_retryable`] error (a timeout, a throttle, or another
/// transient classification the store layer already recognizes), with a
/// short jittered backoff between attempts (longer, when the error is a
/// `Throttled` hint asking for more than that). A non-retryable error
/// (`AccessDenied`, an invariant breach, `AlreadyExists` on the commit
/// record's first attempt) fails immediately, never retried, and so does a
/// retryable one once [`AUDIT_WRITE_BUDGET`]'s total wall-clock budget for
/// the whole call -- both PUTs, every attempt, and the `AlreadyExists`
/// read-back GET below -- runs out: the last error observed is what the
/// batch fails closed with, or, if the budget runs out while the read-back
/// GET itself is still in flight, the invariant error that GET's own
/// completion would otherwise have resolved. `put_retries`, when supplied,
/// counts only the attempts that actually retry (a retryable error observed
/// too late for another attempt to fit the remaining budget is not
/// counted); it is rendered on `/metrics` as `ravel_audit_put_retries_total`
/// (see [`crate::audit_pipeline::AuditPipeline::put_retries`]).
///
/// The tenant is taken from the records themselves, never from a parameter a
/// caller resolved once at construction time: the object identity, the data
/// key, and the commit record all derive from `records[0].tenant`, and a batch
/// whose records do not all name that same tenant is rejected as an invariant
/// breach. Filing one tenant's audit record under another's prefix would
/// disclose it to that other tenant, since the `audit` SQL table resolves per
/// tenant hash, so the mixed-tenant case fails the whole batch rather than
/// choosing a tenant for it.
pub(crate) async fn write_audit_batch(
    store: &dyn ObjectStoreBackend,
    shard: u32,
    record_id: Uuid,
    records: Vec<AuditRecord>,
    put_retries: Option<&AtomicU64>,
) -> Result<()> {
    write_audit_batch_with_rng(store, shard, record_id, records, put_retries, &SystemRng).await
}

/// [`write_audit_batch`] with the backoff-jitter source injected (ADR-0068
/// decision 2), mirroring `ravel_commit::publish`'s `publish`/
/// `publish_with_rng` split: `write_audit_batch` calls this with the
/// OS-entropy [`SystemRng`], and [`crate::audit_pipeline::AuditPipeline`]
/// holds its own `Arc<dyn RngSource>` (defaulting to [`SystemRng`], override
/// with [`crate::audit_pipeline::AuditPipeline::spawn_with_rng`]) so the
/// simulation harness can inject a seeded source and replay retry timing
/// deterministically. Behavior with the default source is identical to the
/// pre-seam code.
pub(crate) async fn write_audit_batch_with_rng(
    store: &dyn ObjectStoreBackend,
    shard: u32,
    record_id: Uuid,
    records: Vec<AuditRecord>,
    put_retries: Option<&AtomicU64>,
    rng: &dyn RngSource,
) -> Result<()> {
    let Some(first) = records.first() else {
        return Err(MaintainError::Invariant(
            "write_audit_batch called with an empty record set".to_string(),
        ));
    };
    let tenant = first.tenant;
    if let Some(other) = records.iter().find(|record| record.tenant != tenant) {
        return Err(MaintainError::Invariant(format!(
            "write_audit_batch called with a mixed-tenant batch: {} and {}",
            tenant.to_hex(),
            other.tenant.to_hex()
        )));
    }

    // Aggregate the commit record's fields across the whole batch: the count is
    // the batch length, the time bounds are the true min/max of the batch's
    // timestamps, and the series count is the number of distinct log streams
    // the batch touches. `now_ns` serves as both the event and the ingest
    // timestamp for each record, exactly as the single-record path treats it.
    let sample_count = records.len() as u64;
    let mut streams = std::collections::BTreeSet::new();
    let mut min_ts_ns = i64::MAX;
    let mut max_ts_ns = i64::MIN;
    for record in &records {
        streams.insert(record.stream_id);
        min_ts_ns = min_ts_ns.min(record.now_ns);
        max_ts_ns = max_ts_ns.max(record.now_ns);
    }
    let series_count = streams.len() as u64;

    let identity = ObjectIdentity {
        tenant_hash: tenant.0,
        shard,
        writer_id: record_id.into_bytes(),
        writer_epoch: 0,
        writer_seq: 0,
    };
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    for record in records {
        writer.push(record.into_log_record())?;
    }
    let object = Bytes::from(writer.finish()?);
    let content_hash: [u8; 32] = *blake3::hash(&object).as_bytes();

    // The batch's hour bucket and `created_unix_ns` are taken from the latest
    // record in the batch. A batch spans at most one flush window
    // (`max_age`, default 25 ms), so its records cannot straddle an hour
    // boundary except pathologically at the very edge.
    let ingest_hour_bucket = u32::try_from(max_ts_ns / NS_PER_HOUR).map_err(|_| {
        MaintainError::Invariant(format!(
            "audit timestamp {max_ts_ns} out of hour-bucket range"
        ))
    })?;
    let commit = record::build(NewCommitRecord {
        tenant_hash: tenant,
        signal: Signal::Audit,
        shard,
        writer_id: record_id,
        writer_epoch: 0,
        writer_seq: 0,
        object_size: object.len() as u64,
        content_hash,
        sample_count,
        series_count,
        min_event_ts_ns: min_ts_ns,
        max_event_ts_ns: max_ts_ns,
        min_ingest_ts_ns: min_ts_ns,
        max_ingest_ts_ns: max_ts_ns,
        segment_format_version: OUTPUT_FORMAT_VERSION,
        created_unix_ns: max_ts_ns,
        ingest_hour_bucket,
    })?;

    // One budget for the whole call: both PUTs, every attempt, and the
    // AlreadyExists-on-retry read-back GET below. See `AUDIT_WRITE_BUDGET`
    // for why 30 s keeps a hung store's worst case below the pre-retry-ladder
    // worst case of two un-retried 20 s timeouts.
    let deadline = tokio::time::Instant::now() + AUDIT_WRITE_BUDGET;

    let data_key = keys::data_key(
        &tenant,
        Signal::Audit,
        shard,
        record_id,
        0,
        0,
        &content_hash,
    )?;
    let data_checksum = UploadChecksum::Crc32c(crc32c::crc32c(&object));
    let mut attempt = 1u32;
    let mut last_error: Option<StoreError> = None;
    loop {
        let outcome = bound_to_deadline(
            deadline,
            store.put(
                &data_key,
                object.clone(),
                PutOptions::create_if_absent().with_checksum(data_checksum),
            ),
        )
        .await;
        match outcome {
            Some(Ok(_)) => break,
            // A fresh `record_id` collides only if the caller reused one; the
            // data object is content-addressed by `content_hash` in its key,
            // so an identical object already present is a genuine no-op, not
            // an error - the same idempotent-republish convergence every
            // other L0 write in this repo already relies on (ADR-0010 SS7).
            // Benign on every attempt, including a retry: a retry can only
            // ever republish this same content-addressed object.
            Some(Err(StoreError::AlreadyExists)) => break,
            Some(Err(e)) if attempt < MAX_PUT_ATTEMPTS && e.is_retryable() => {
                let delay = retry_delay(&e, RETRY_BACKOFF_BASE_MS[(attempt - 1) as usize], rng);
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if delay > remaining {
                    return Err(e.into());
                }
                note_put_retry(AuditPut::Data, attempt, &e, put_retries);
                tokio::time::sleep(delay).await;
                attempt += 1;
                last_error = Some(e);
            }
            Some(Err(e)) => return Err(e.into()),
            None => {
                return Err(last_error.map(MaintainError::from).unwrap_or_else(|| {
                    MaintainError::AuditFlush(format!(
                        "audit {} PUT exceeded its {:?} write budget with no response (attempt {attempt})",
                        AuditPut::Data,
                        AUDIT_WRITE_BUDGET
                    ))
                }));
            }
        }
    }

    let commit_key = keys::commit_key_for_record(&commit)?;
    let commit_bytes = record::encode(&commit);
    let commit_checksum = UploadChecksum::Crc32c(crc32c::crc32c(&commit_bytes));
    let mut attempt = 1u32;
    let mut last_error: Option<StoreError> = None;
    loop {
        let outcome = bound_to_deadline(
            deadline,
            store.put(
                &commit_key,
                commit_bytes.clone(),
                PutOptions::create_if_absent().with_checksum(commit_checksum),
            ),
        )
        .await;
        match outcome {
            Some(Ok(_)) => break,
            // The same `record_id` reused for a second, logically distinct
            // audit record (or batch) lands here as a REAL conflict on the
            // *first* attempt, since the two differ in content but share a
            // commit key - surfaced as an error rather than silently keeping
            // whichever one landed first.
            Some(Err(StoreError::AlreadyExists)) if attempt == 1 => {
                return Err(MaintainError::Invariant(format!(
                    "audit commit record {commit_key} already exists with different content \
                     - record_id {record_id} was reused for a different audit record"
                )));
            }
            // `AlreadyExists` on a retry is ambiguous: either a genuine
            // collision, or this same attempt's own earlier PUT landed and
            // only its response was lost to the transient error that
            // triggered the retry. Every attempt writes identical bytes
            // (same `record_id`, same encoded commit), so the stored object
            // is exactly recoverable: fetch it and compare. Equal bytes mean
            // the earlier attempt already succeeded; anything else fails
            // closed rather than assuming it.
            Some(Err(StoreError::AlreadyExists)) => {
                match bound_to_deadline(deadline, store.get(&commit_key, GetRange::Full)).await {
                    Some(Ok(existing)) if existing.data == commit_bytes => break,
                    Some(Ok(_)) => {
                        return Err(MaintainError::Invariant(format!(
                            "audit commit record {commit_key} already exists with different \
                             content after a retry - record_id {record_id} collided with an \
                             unrelated commit record"
                        )));
                    }
                    Some(Err(get_err)) => {
                        return Err(MaintainError::Invariant(format!(
                            "audit commit record {commit_key} already exists after a retry, and \
                             confirming its content failed ({get_err}) - failing closed rather \
                             than assuming the retry's own write landed"
                        )));
                    }
                    None => {
                        return Err(MaintainError::Invariant(format!(
                            "audit commit record {commit_key} already exists after a retry, and \
                             confirming its content failed (the {AUDIT_WRITE_BUDGET:?} write \
                             budget ran out before the read-back answered) - failing closed \
                             rather than assuming the retry's own write landed"
                        )));
                    }
                }
            }
            Some(Err(e)) if attempt < MAX_PUT_ATTEMPTS && e.is_retryable() => {
                let delay = retry_delay(&e, RETRY_BACKOFF_BASE_MS[(attempt - 1) as usize], rng);
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if delay > remaining {
                    return Err(e.into());
                }
                note_put_retry(AuditPut::Commit, attempt, &e, put_retries);
                tokio::time::sleep(delay).await;
                attempt += 1;
                last_error = Some(e);
            }
            Some(Err(e)) => return Err(e.into()),
            None => {
                return Err(last_error.map(MaintainError::from).unwrap_or_else(|| {
                    MaintainError::AuditFlush(format!(
                        "audit {} PUT exceeded its {:?} write budget with no response (attempt {attempt})",
                        AuditPut::Commit,
                        AUDIT_WRITE_BUDGET
                    ))
                }));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use ravel_commit::SeededRng;
    use ravel_commit::keys;
    use ravel_commit::record;
    use ravel_logseg::{LogRecord, Predicate, RlogReader, stream_attrs_bytes};
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault, Sequence,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, list_all};
    use ravel_types::logstream::log_stream_id;

    /// A stub `RngSource` returning a fixed jitter draw every time, so
    /// [`jittered_backoff`]'s formula can be pinned exactly without relying
    /// on a seeded PRNG's actual sequence.
    struct FixedRng(u64);

    impl RngSource for FixedRng {
        fn jitter_ms(&self, _max_ms: u64) -> u64 {
            self.0
        }

        fn new_uuid(&self) -> Uuid {
            Uuid::new_v4()
        }
    }

    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const AUDIT_SHARD: u32 = 1;

    fn test_stream(seed: u32) -> (LogStreamId, Vec<u8>) {
        let resource = vec![(
            "ravel.record_type".to_string(),
            AttrValue::Str(format!("audit_batch_{seed}")),
        )];
        let id = log_stream_id(&resource, "ravel.audit_batch", "1", &[]);
        let blob = stream_attrs_bytes(&resource, "ravel.audit_batch", "1", &[]);
        (id, blob)
    }

    fn test_record(tenant: TenantHash, now_ns: i64, stream_seed: u32) -> AuditRecord {
        let (stream_id, stream_attrs) = test_stream(stream_seed);
        AuditRecord {
            tenant,
            now_ns,
            stream_id,
            stream_attrs,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: format!("audit batch record {now_ns}"),
            attrs: vec![("kind".to_string(), AttrValue::Str("query".into()))],
        }
    }

    #[tokio::test]
    async fn batch_of_n_records_is_one_object_with_aggregate_commit_fields() {
        let store = MemoryStore::new();
        let tenant = TenantHash([21u8; 16]);
        let base = 3 * NS_PER_HOUR;

        // Four records across two distinct streams, with a spread of
        // timestamps, all inside one hour bucket.
        let records = vec![
            test_record(tenant, base + 500, 1),
            test_record(tenant, base + 100, 1),
            test_record(tenant, base + 900, 2),
            test_record(tenant, base + 300, 2),
        ];
        let expected_min = base + 100;
        let expected_max = base + 900;

        write_audit_batch(&store, AUDIT_SHARD, Uuid::new_v4(), records, None)
            .await
            .expect("batch write");

        // Exactly one commit record and one data object for the whole batch.
        let commit_prefix = keys::commit_shard_prefix(&tenant, Signal::Audit, AUDIT_SHARD).unwrap();
        let commits = list_all(&store, &commit_prefix).await.unwrap();
        assert_eq!(commits.len(), 1, "one commit record for the batch");

        let data_prefix = format!("t/{}/u/l0/{:04}/", tenant.to_hex(), AUDIT_SHARD);
        let data_objects = list_all(&store, &data_prefix).await.unwrap();
        assert_eq!(data_objects.len(), 1, "one data object for the batch");

        // Aggregate commit fields span the whole batch.
        let commit_bytes = store.get(&commits[0].key, GetRange::Full).await.unwrap();
        let commit = record::decode(&commit_bytes.data).unwrap();
        assert_eq!(commit.sample_count, 4, "count is the batch length");
        assert_eq!(commit.series_count, 2, "two distinct streams in the batch");
        assert_eq!(commit.min_event_ts_ns, expected_min);
        assert_eq!(commit.max_event_ts_ns, expected_max);
        assert_eq!(commit.min_ingest_ts_ns, expected_min);
        assert_eq!(commit.max_ingest_ts_ns, expected_max);

        // The single object carries all four records.
        let data_key = keys::reconstruct_data_key(&commit).unwrap();
        let object = store.get(&data_key, GetRange::Full).await.unwrap();
        let reader = RlogReader::new(object.data.as_ref(), &RlogConfig::default()).unwrap();
        let (rows, _stats): (Vec<LogRecord>, _) = reader.scan(&Predicate::And(Vec::new())).unwrap();
        assert_eq!(rows.len(), 4, "all four records in the one object");
    }

    #[tokio::test]
    async fn single_record_batch_matches_the_per_record_path() {
        let store = MemoryStore::new();
        let tenant = TenantHash([22u8; 16]);
        let now_ns = 5 * NS_PER_HOUR + 42;

        write_audit_batch(
            &store,
            AUDIT_SHARD,
            Uuid::new_v4(),
            vec![test_record(tenant, now_ns, 1)],
            None,
        )
        .await
        .expect("single-record batch");

        let commit_prefix = keys::commit_shard_prefix(&tenant, Signal::Audit, AUDIT_SHARD).unwrap();
        let commits = list_all(&store, &commit_prefix).await.unwrap();
        assert_eq!(commits.len(), 1);
        let commit_bytes = store.get(&commits[0].key, GetRange::Full).await.unwrap();
        let commit = record::decode(&commit_bytes.data).unwrap();
        assert_eq!(commit.sample_count, 1);
        assert_eq!(commit.series_count, 1);
        assert_eq!(commit.min_event_ts_ns, now_ns);
        assert_eq!(commit.max_event_ts_ns, now_ns);
    }

    #[tokio::test]
    async fn empty_batch_is_rejected() {
        let store = MemoryStore::new();
        let err = write_audit_batch(&store, AUDIT_SHARD, Uuid::new_v4(), Vec::new(), None)
            .await
            .expect_err("empty batch is an invariant breach");
        assert!(matches!(err, MaintainError::Invariant(_)));
    }

    #[tokio::test]
    async fn a_mixed_tenant_batch_is_rejected_and_writes_nothing() {
        let store = MemoryStore::new();
        let one = TenantHash([24u8; 16]);
        let two = TenantHash([25u8; 16]);
        let base = 7 * NS_PER_HOUR;

        let err = write_audit_batch(
            &store,
            AUDIT_SHARD,
            Uuid::new_v4(),
            vec![test_record(one, base, 1), test_record(two, base + 1, 1)],
            None,
        )
        .await
        .expect_err("a batch spanning two tenants is an invariant breach");
        assert!(matches!(err, MaintainError::Invariant(_)));

        // The rejection happens before any PUT, so neither tenant's prefix
        // gained an object: a mixed batch cannot half-file itself.
        for tenant in [one, two] {
            let data_prefix = format!("t/{}/", tenant.to_hex());
            let objects = list_all(&store, &data_prefix).await.unwrap();
            assert_eq!(
                objects.len(),
                0,
                "no object under {}, the batch was rejected before any PUT",
                tenant.to_hex()
            );
        }
    }

    #[test]
    fn jittered_backoff_pins_the_low_and_high_ends_of_its_range() {
        // `jitter_ms` returning 0 is the low end: `base_ms * 3 / 4` exactly.
        let low = jittered_backoff(200, &FixedRng(0));
        assert_eq!(low, Duration::from_millis(150));

        // `jitter_ms` returning `range_ms` (`base_ms / 2`) is the high end:
        // `base_ms * 3 / 4 + base_ms / 2 == base_ms * 1.25`.
        let high = jittered_backoff(200, &FixedRng(100));
        assert_eq!(high, Duration::from_millis(250));
    }

    #[test]
    fn retry_delay_honors_a_throttle_hint_longer_than_jittered_backoff() {
        let error = StoreError::Throttled {
            retry_after_ms: 1_000,
        };
        // Jitter's own high end for this base is well under the hint.
        let delay = retry_delay(&error, 50, &FixedRng(0));
        assert_eq!(delay, Duration::from_millis(1_000));
    }

    #[test]
    fn retry_delay_keeps_jittered_backoff_when_it_already_exceeds_the_hint() {
        let error = StoreError::Throttled { retry_after_ms: 10 };
        let delay = retry_delay(&error, 200, &FixedRng(0));
        assert_eq!(
            delay,
            Duration::from_millis(150),
            "jitter's low end, 150ms, beats the 10ms hint"
        );
    }

    /// ADR-0062 amendment (2026-09-27): a PUT that never returns at all (no
    /// error, no success) must still fail the batch closed once
    /// `AUDIT_WRITE_BUDGET` runs out, not hang forever. `FaultStore::hold`
    /// blocks the matching PUT indefinitely; the handle is never released.
    #[tokio::test(start_paused = true)]
    async fn a_put_that_never_returns_fails_closed_within_the_write_budget() {
        let mem = MemoryStore::new();
        let store = FaultStore::new(mem, FaultPlan::empty());
        let _held = store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Always);
        let tenant = TenantHash([61u8; 16]);

        let started = tokio::time::Instant::now();
        let err = write_audit_batch_with_rng(
            &store,
            AUDIT_SHARD,
            Uuid::new_v4(),
            vec![test_record(tenant, 9 * NS_PER_HOUR, 1)],
            None,
            &SeededRng::new(1),
        )
        .await
        .expect_err("a data PUT that never returns must fail the batch closed");

        assert_eq!(
            tokio::time::Instant::now().duration_since(started),
            AUDIT_WRITE_BUDGET,
            "the paused clock advances exactly to the budget deadline, no further"
        );
        match &err {
            MaintainError::AuditFlush(msg) => {
                assert!(msg.contains("attempt 1"), "message was: {msg}");
                assert!(msg.contains("data"), "message was: {msg}");
            }
            other => panic!("expected AuditFlush, got {other:?}"),
        }
    }

    /// Finding 6: a `Throttled` hint longer than jitter's own backoff is
    /// honored, as long as it still fits the total write budget.
    #[tokio::test(start_paused = true)]
    async fn a_throttle_hint_within_budget_is_honored_and_the_batch_succeeds() {
        let mem = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Put,
                ScriptedFault::Throttled {
                    retry_after_ms: 1_000,
                },
            )
            .with_key_contains("/l0/")
            .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(mem, plan);
        let tenant = TenantHash([62u8; 16]);
        let put_retries = AtomicU64::new(0);

        let started = tokio::time::Instant::now();
        write_audit_batch_with_rng(
            &store,
            AUDIT_SHARD,
            Uuid::new_v4(),
            vec![test_record(tenant, 10 * NS_PER_HOUR, 1)],
            Some(&put_retries),
            &SeededRng::new(1),
        )
        .await
        .expect("the retry after the throttle hint must succeed");

        assert!(
            tokio::time::Instant::now().duration_since(started) >= Duration::from_millis(1_000),
            "the retry waited at least the throttle hint, not just jitter's shorter backoff"
        );
        assert_eq!(
            store.fault_count(Op::Put, FaultKind::Throttled),
            1,
            "the injected throttle fired exactly once"
        );
        assert_eq!(put_retries.load(Ordering::Relaxed), 1);
    }

    /// Finding 6, other half: a `Throttled` hint that does not fit inside the
    /// remaining write budget stops retrying immediately rather than sleeping
    /// past the deadline, and the throttle error itself is what the batch
    /// fails closed with. Also finding 2 (review round 2): `put_retries` must
    /// stay at zero, since the abandoned retry never actually happens -
    /// counting it here would report a retry that was never attempted.
    #[tokio::test(start_paused = true)]
    async fn a_throttle_hint_that_does_not_fit_the_budget_fails_closed_without_retrying() {
        let mem = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Put,
                ScriptedFault::Throttled {
                    retry_after_ms: 60_000,
                },
            )
            .with_key_contains("/l0/")
            .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(mem, plan);
        let tenant = TenantHash([63u8; 16]);
        let put_retries = AtomicU64::new(0);

        let err = write_audit_batch_with_rng(
            &store,
            AUDIT_SHARD,
            Uuid::new_v4(),
            vec![test_record(tenant, 11 * NS_PER_HOUR, 1)],
            Some(&put_retries),
            &SeededRng::new(1),
        )
        .await
        .expect_err("a hint twice the whole budget must not be waited out");

        assert!(
            matches!(
                err,
                MaintainError::Store(StoreError::Throttled {
                    retry_after_ms: 60_000
                })
            ),
            "expected the throttle error itself surfaced, got {err:?}"
        );
        assert_eq!(
            store.fault_count(Op::Put, FaultKind::Throttled),
            1,
            "no retry was attempted, so the fault fired exactly once"
        );
        assert_eq!(
            put_retries.load(Ordering::Relaxed),
            0,
            "the abandoned retry must not be counted: no further attempt was ever made"
        );
    }

    /// Finding 1 (review round 2, issue #2035): the `AlreadyExists`-on-retry
    /// read-back GET of the commit key is the one store call in
    /// `write_audit_batch_with_rng` not raced against `AUDIT_WRITE_BUDGET`. A
    /// GET is idempotent, so the object-store client's own retry loop runs
    /// over it, and an unbounded GET can hold the whole batch open long past
    /// the write budget. A commit PUT that times out once, then reports
    /// `AlreadyExists` on its retry, whose read-back GET then never returns,
    /// must still fail the batch closed at exactly the budget rather than
    /// hanging past it.
    #[tokio::test(start_paused = true)]
    async fn a_stuck_commit_readback_get_fails_closed_within_the_write_budget() {
        let mem = MemoryStore::new();
        // Only the commit PUT (key contains "/u/c/") is scripted: the data
        // PUT (key contains "/u/l0/") must succeed normally so the batch
        // reaches the commit loop's AlreadyExists-on-retry path.
        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Put)
                .with_key_contains("/u/c/")
                .then_fault(ScriptedFault::Timeout)
                .then_fault(ScriptedFault::FailedConditionalWrite),
        );
        let store = Arc::new(FaultStore::new(mem, plan));
        let held = store.hold(Op::Get, Some("/u/c/".to_string()), Occurrence::Always);
        let tenant = TenantHash([64u8; 16]);

        let started = tokio::time::Instant::now();
        let task_store = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            write_audit_batch_with_rng(
                task_store.as_ref(),
                AUDIT_SHARD,
                Uuid::new_v4(),
                vec![test_record(tenant, 12 * NS_PER_HOUR, 1)],
                None,
                &SeededRng::new(1),
            )
            .await
        });

        // Confirm the read-back GET is the call actually stuck, before
        // letting the paused clock run the write budget out from under it.
        held.wait_until_held(1).await;
        let details = held.held_details();
        assert_eq!(details.len(), 1, "exactly one call is held");
        assert_eq!(details[0].1, Op::Get, "the held call is a GET");
        assert!(
            details[0].2.contains("/u/c/"),
            "the held GET is the commit key's read-back, key was {}",
            details[0].2
        );

        let err = handle
            .await
            .expect("write_audit_batch_with_rng task did not panic")
            .expect_err("a commit read-back GET that never returns must fail the batch closed");

        assert_eq!(
            tokio::time::Instant::now().duration_since(started),
            AUDIT_WRITE_BUDGET,
            "the paused clock advances exactly to the budget deadline, no further"
        );
        match &err {
            MaintainError::Invariant(msg) => {
                assert!(
                    msg.contains("confirming its content failed"),
                    "message was: {msg}"
                );
                assert!(msg.contains("budget ran out"), "message was: {msg}");
            }
            other => panic!("expected Invariant, got {other:?}"),
        }
    }
}
