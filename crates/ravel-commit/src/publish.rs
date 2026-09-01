//! Idempotent commit-record and data-object publication (docs/catalog-and-mvcc.md
//! "Commit sequence", ADR-0002, ADR-0010 §1/§7).

use std::time::Duration;

use bytes::Bytes;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError, UploadChecksum};
use ravel_proto::commit::v1::CommitRecord;
use ravel_types::CommitToken;

use crate::keys::{self, KeyError};
use crate::record::{self, RecordError};
use crate::rng::{RngSource, SystemRng};

/// Jittered exponential backoff bounds for retryable store errors
/// (`Throttled`, `Timeout`, `Transient`; see `StoreError::is_retryable`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Number of retries after the first attempt (total attempts = this + 1).
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 5,
            base_delay: Duration::from_millis(20),
            max_delay: Duration::from_secs(2),
        }
    }
}

/// Errors from [`publish`] and [`put_data_object`]. `SplitBrain` is fatal:
/// with the flush identity pinned (ADR-0010 §1), it cannot fire on a benign
/// retry, so callers should crash loudly rather than swallow it.
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error(transparent)]
    Record(#[from] RecordError),
    #[error(transparent)]
    Key(#[from] KeyError),
    #[error(
        "split-brain (fatal): commit key already holds a record with a different content_hash (this record {this}, stored {stored})"
    )]
    SplitBrain { this: String, stored: String },
    #[error("store error after {attempts} attempt(s): {source}")]
    Store {
        attempts: u32,
        #[source]
        source: StoreError,
    },
}

async fn backoff_sleep(attempt: u32, policy: &RetryPolicy, rng: &dyn RngSource) {
    let shift = attempt.min(20);
    let exp = policy.base_delay.saturating_mul(1u32 << shift);
    let capped = exp.min(policy.max_delay);
    let capped_ms = u64::try_from(capped.as_millis()).unwrap_or(u64::MAX);
    let jittered_ms = rng.jitter_ms(capped_ms);
    tokio::time::sleep(Duration::from_millis(jittered_ms)).await;
}

/// Publish a commit record: PUT it `CreateIfAbsent` at its own derived key
/// with a CRC32C upload checksum. On `AlreadyExists`, GET the existing
/// record and compare `content_hash`: equal is idempotent success (a
/// previous attempt already landed and this is a retry); different is a
/// fatal split-brain. Retryable store errors get jittered exponential
/// backoff, bounded by `retry_policy.max_attempts`.
pub async fn publish(
    store: &dyn ObjectStoreBackend,
    record: &CommitRecord,
    retry_policy: &RetryPolicy,
) -> Result<CommitToken, PublishError> {
    publish_with_rng(store, record, retry_policy, &SystemRng).await
}

/// [`publish`] with the backoff-jitter source injected (ADR-0068 decision 2).
/// `publish` calls this with the OS-entropy [`SystemRng`]; the simulation
/// harness injects a seeded [`crate::rng::SeededRng`] so retry timing is
/// reproducible from the master seed. Behavior with the default source is
/// identical to the pre-seam code.
#[tracing::instrument(skip(store, record, retry_policy, rng), fields(commit_key))]
pub async fn publish_with_rng(
    store: &dyn ObjectStoreBackend,
    record: &CommitRecord,
    retry_policy: &RetryPolicy,
    rng: &dyn RngSource,
) -> Result<CommitToken, PublishError> {
    record::validate(record)?;
    let commit_key = keys::commit_key_for_record(record)?;
    tracing::Span::current().record("commit_key", tracing::field::display(&commit_key));
    let payload = record::encode(record);
    let checksum = UploadChecksum::Crc32c(crc32c::crc32c(&payload));
    let opts = PutOptions::create_if_absent().with_checksum(checksum);

    let mut attempt: u32 = 0;
    loop {
        match store.put(&commit_key, payload.clone(), opts.clone()).await {
            Ok(_) => {
                tracing::info!(attempt, "commit record published");
                return Ok(record::token_for(record)?);
            }
            Err(StoreError::AlreadyExists) => {
                return resolve_already_exists(store, &commit_key, record).await;
            }
            Err(e) if e.is_retryable() && attempt < retry_policy.max_attempts => {
                tracing::warn!(attempt, error = %e, "retrying commit publish after store error");
                backoff_sleep(attempt, retry_policy, rng).await;
                attempt += 1;
            }
            Err(e) => {
                tracing::error!(attempt, error = %e, "commit publish failed, not retrying");
                return Err(PublishError::Store {
                    attempts: attempt + 1,
                    source: e,
                });
            }
        }
    }
}

async fn resolve_already_exists(
    store: &dyn ObjectStoreBackend,
    commit_key: &str,
    record: &CommitRecord,
) -> Result<CommitToken, PublishError> {
    let existing =
        store
            .get(commit_key, GetRange::Full)
            .await
            .map_err(|e| PublishError::Store {
                attempts: 1,
                source: e,
            })?;
    let existing_record = record::decode(&existing.data)?;
    if existing_record.content_hash == record.content_hash {
        tracing::info!("commit publish idempotent: matching record already landed");
        Ok(record::token_for(record)?)
    } else {
        tracing::error!("commit publish split-brain: content_hash mismatch on AlreadyExists");
        Err(PublishError::SplitBrain {
            this: hex::encode(&record.content_hash),
            stored: hex::encode(&existing_record.content_hash),
        })
    }
}

/// PUT a data object `CreateIfAbsent` with a CRC32C upload checksum.
/// `AlreadyExists` is treated as success: the key embeds the content hash,
/// so under `CreateIfAbsent` the stored bytes are identical to `bytes` by
/// construction (ADR-0002, ADR-0010 §7). Single attempt: the caller retries
/// the whole pinned flush step, not just this PUT, per the pinned-identity
/// rule (this crate must not re-serialize or re-derive the key on retry).
#[tracing::instrument(skip(store, bytes), fields(key = %key, len = bytes.len()))]
pub async fn put_data_object(
    store: &dyn ObjectStoreBackend,
    key: &str,
    bytes: Bytes,
) -> Result<(), PublishError> {
    let checksum = UploadChecksum::Crc32c(crc32c::crc32c(&bytes));
    match store
        .put(
            key,
            bytes,
            PutOptions::create_if_absent().with_checksum(checksum),
        )
        .await
    {
        Ok(_) => {
            tracing::info!("data object put");
            Ok(())
        }
        Err(StoreError::AlreadyExists) => {
            tracing::info!("data object already present (idempotent)");
            Ok(())
        }
        Err(e) => {
            tracing::error!(error = %e, "data object put failed");
            Err(PublishError::Store {
                attempts: 1,
                source: e,
            })
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::{Signal, TenantHash};
    use uuid::Uuid;

    use super::*;
    use crate::record::NewCommitRecord;

    fn sample_record(content_hash: [u8; 32]) -> CommitRecord {
        record::build(NewCommitRecord {
            tenant_hash: TenantHash([0x33; 16]),
            signal: Signal::Metrics,
            shard: 0,
            writer_id: Uuid::new_v4(),
            writer_epoch: 1,
            writer_seq: 1,
            object_size: 100,
            content_hash,
            sample_count: 5,
            series_count: 1,
            min_event_ts_ns: 0,
            max_event_ts_ns: 100,
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 100,
            segment_format_version: 1,
            created_unix_ns: 0,
            ingest_hour_bucket: 0,
        })
        .expect("valid record")
    }

    #[tokio::test]
    async fn publish_then_get_round_trips() {
        let store = MemoryStore::new();
        let record = sample_record([1; 32]);
        let token = publish(&store, &record, &RetryPolicy::default())
            .await
            .expect("publish");
        assert_eq!(token, record::token_for(&record).expect("token"));
        let key = keys::commit_key_for_record(&record).expect("key");
        let got = store.get(&key, GetRange::Full).await.expect("get");
        let decoded = record::decode(&got.data).expect("decode");
        assert_eq!(decoded, record);
    }

    #[tokio::test]
    async fn republishing_identical_record_is_idempotent() {
        let store = MemoryStore::new();
        let record = sample_record([2; 32]);
        let policy = RetryPolicy::default();
        let first = publish(&store, &record, &policy)
            .await
            .expect("first publish");
        let second = publish(&store, &record, &policy)
            .await
            .expect("retry publish");
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn republishing_with_different_content_hash_is_split_brain() {
        let store = MemoryStore::new();
        let mut record = sample_record([3; 32]);
        publish(&store, &record, &RetryPolicy::default())
            .await
            .expect("first publish");
        // Same identity (shard/writer/epoch/seq/hour), different bytes: can
        // only happen if the pinned-identity rule was violated upstream.
        record.content_hash = vec![4; 32];
        record.sample_count += 1;
        let err = publish(&store, &record, &RetryPolicy::default())
            .await
            .expect_err("must be split-brain");
        assert!(matches!(err, PublishError::SplitBrain { .. }));
    }

    #[tokio::test]
    async fn put_data_object_is_idempotent_on_already_exists() {
        let store = MemoryStore::new();
        let key = "t/deadbeef/m/l0/0000/w.1.1.hash.rseg";
        put_data_object(&store, key, Bytes::from_static(b"payload"))
            .await
            .expect("first put");
        put_data_object(&store, key, Bytes::from_static(b"payload"))
            .await
            .expect("second put is idempotent");
        let got = store.get(key, GetRange::Full).await.expect("get");
        assert_eq!(&got.data[..], b"payload");
    }
}
