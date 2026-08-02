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

/// Encode one audit RLOG record as an L0 [`Signal::Audit`] object and its
/// commit record, and PUT both (data object first, commit record last, the
/// ingest durability order). This is the minimal write path for a
/// `Signal::Audit` record: one record, one object, one commit record, exactly
/// as `ravel-ingest` writes an L0 log object.
pub(crate) async fn write_audit_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    write: AuditWrite,
) -> Result<()> {
    let log_record = LogRecord {
        stream_id: write.stream_id,
        stream_attrs: write.stream_attrs,
        ts_ns: write.now_ns,
        observed_ts_ns: write.now_ns,
        severity_num: write.severity_num,
        severity_text: write.severity_text,
        body: write.body,
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: write.attrs,
    };

    let identity = ObjectIdentity {
        tenant_hash: tenant.0,
        shard: write.shard,
        writer_id: write.record_id.into_bytes(),
        writer_epoch: 0,
        writer_seq: 0,
    };
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    writer.push(log_record)?;
    let object = Bytes::from(writer.finish()?);
    let content_hash: [u8; 32] = *blake3::hash(&object).as_bytes();

    let ingest_hour_bucket = u32::try_from(write.now_ns / NS_PER_HOUR).map_err(|_| {
        MaintainError::Invariant(format!(
            "audit timestamp {} out of hour-bucket range",
            write.now_ns
        ))
    })?;
    let commit = record::build(NewCommitRecord {
        tenant_hash: *tenant,
        signal: Signal::Audit,
        shard: write.shard,
        writer_id: write.record_id,
        writer_epoch: 0,
        writer_seq: 0,
        object_size: object.len() as u64,
        content_hash,
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: write.now_ns,
        max_event_ts_ns: write.now_ns,
        min_ingest_ts_ns: write.now_ns,
        max_ingest_ts_ns: write.now_ns,
        segment_format_version: OUTPUT_FORMAT_VERSION,
        created_unix_ns: write.now_ns,
        ingest_hour_bucket,
    })?;

    let data_key = keys::data_key(
        tenant,
        Signal::Audit,
        write.shard,
        write.record_id,
        0,
        0,
        &content_hash,
    )?;
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
        // record would land here as a REAL conflict, since the two records
        // differ in content but share a commit key - surfaced as an error
        // rather than silently keeping whichever one landed first.
        Err(StoreError::AlreadyExists) => {
            return Err(MaintainError::Invariant(format!(
                "audit commit record {commit_key} already exists with different content \
                 - record_id {} was reused for a different audit record",
                write.record_id
            )));
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}
