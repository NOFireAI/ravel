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

use bytes::Bytes;
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::{AttrValue, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter};
use ravel_object_store::{ObjectStoreBackend, PutOptions, StoreError, UploadChecksum};
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

use crate::config::NS_PER_HOUR;
use crate::error::{MaintainError, Result};
use crate::rlog::OUTPUT_FORMAT_VERSION;

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
        now_ns: write.now_ns,
        stream_id: write.stream_id,
        stream_attrs: write.stream_attrs,
        severity_num: write.severity_num,
        severity_text: write.severity_text,
        body: write.body,
        attrs: write.attrs,
    };
    write_audit_batch(store, tenant, shard, record_id, vec![record]).await
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
/// idempotent republish, while `AlreadyExists` on the commit record is a hard
/// error because a reused `record_id` would collide two logically distinct
/// batches onto one commit key.
pub(crate) async fn write_audit_batch(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    shard: u32,
    record_id: Uuid,
    records: Vec<AuditRecord>,
) -> Result<()> {
    if records.is_empty() {
        return Err(MaintainError::Invariant(
            "write_audit_batch called with an empty record set".to_string(),
        ));
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
        tenant_hash: *tenant,
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

    let data_key = keys::data_key(tenant, Signal::Audit, shard, record_id, 0, 0, &content_hash)?;
    let data_checksum = UploadChecksum::Crc32c(crc32c::crc32c(&object));
    match store
        .put(
            &data_key,
            object,
            PutOptions::create_if_absent().with_checksum(data_checksum),
        )
        .await
    {
        Ok(_) => {}
        // A fresh `record_id` collides only if the caller reused one; the
        // data object is content-addressed by `content_hash` in its key, so
        // an identical object already present is a genuine no-op, not an
        // error - the same idempotent-republish convergence every other L0
        // write in this repo already relies on (ADR-0010 SS7).
        Err(StoreError::AlreadyExists) => {}
        Err(e) => return Err(e.into()),
    }
    let commit_key = keys::commit_key_for_record(&commit)?;
    let commit_bytes = record::encode(&commit);
    let commit_checksum = UploadChecksum::Crc32c(crc32c::crc32c(&commit_bytes));
    match store
        .put(
            &commit_key,
            commit_bytes,
            PutOptions::create_if_absent().with_checksum(commit_checksum),
        )
        .await
    {
        Ok(_) => {}
        // The same `record_id` reused for a second, logically distinct audit
        // record (or batch) would land here as a REAL conflict, since the two
        // differ in content but share a commit key - surfaced as an error
        // rather than silently keeping whichever one landed first.
        Err(StoreError::AlreadyExists) => {
            return Err(MaintainError::Invariant(format!(
                "audit commit record {commit_key} already exists with different content \
                 - record_id {record_id} was reused for a different audit record"
            )));
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    use ravel_commit::keys;
    use ravel_commit::record;
    use ravel_logseg::{LogRecord, Predicate, RlogReader, stream_attrs_bytes};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, list_all};
    use ravel_types::logstream::log_stream_id;

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

    fn test_record(now_ns: i64, stream_seed: u32) -> AuditRecord {
        let (stream_id, stream_attrs) = test_stream(stream_seed);
        AuditRecord {
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
            test_record(base + 500, 1),
            test_record(base + 100, 1),
            test_record(base + 900, 2),
            test_record(base + 300, 2),
        ];
        let expected_min = base + 100;
        let expected_max = base + 900;

        write_audit_batch(&store, &tenant, AUDIT_SHARD, Uuid::new_v4(), records)
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
            &tenant,
            AUDIT_SHARD,
            Uuid::new_v4(),
            vec![test_record(now_ns, 1)],
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
        let tenant = TenantHash([23u8; 16]);
        let err = write_audit_batch(&store, &tenant, AUDIT_SHARD, Uuid::new_v4(), Vec::new())
            .await
            .expect_err("empty batch is an invariant breach");
        assert!(matches!(err, MaintainError::Invariant(_)));
    }
}
