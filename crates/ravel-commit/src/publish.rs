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
pub async fn publish_with_rng(
    store: &dyn ObjectStoreBackend,
    record: &CommitRecord,
    retry_policy: &RetryPolicy,
    rng: &dyn RngSource,
) -> Result<CommitToken, PublishError> {
    let token = put_commit_record(store, record, retry_policy, rng).await?;
    assert_commit_has_data(store, record).await;
    Ok(token)
}

/// [`publish`] without the instrumented build's data-object evidence read: a
/// caller that races the publish against a deadline or times it calls this
/// inside the race and [`assert_commit_has_data`] after the race completes,
/// so the evidence read can neither delay nor change the outcome. In the
/// default build the two are the same as [`publish`].
pub async fn publish_deferring_evidence(
    store: &dyn ObjectStoreBackend,
    record: &CommitRecord,
    retry_policy: &RetryPolicy,
) -> Result<CommitToken, PublishError> {
    put_commit_record(store, record, retry_policy, &SystemRng).await
}

#[tracing::instrument(skip(store, record, retry_policy, rng), fields(commit_key))]
async fn put_commit_record(
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
                ravel_invariant::inv_reachable!(
                    "commit: commit record put",
                    std::collections::BTreeMap::from([("commit_key", commit_key.clone())])
                );
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

/// NoCommitWithoutData, for a record that [`publish_deferring_evidence`] (or
/// [`publish`]) just reported published: the record exists at its key, so
/// its data object must already be in the store. Only a HEAD answering
/// NotFound is evidence of absence. The default build compiles the HEAD out.
pub async fn assert_commit_has_data(store: &dyn ObjectStoreBackend, record: &CommitRecord) {
    let _ = (store, record);
    ravel_invariant::inv_always!(
        match keys::reconstruct_data_key(record) {
            Ok(data_key) => !matches!(store.head(&data_key).await, Err(StoreError::NotFound)),
            Err(_) => false,
        },
        "commit: a commit record never exists without its data object",
        std::collections::BTreeMap::from([
            (
                "commit_key",
                keys::commit_key_for_record(record).unwrap_or_default()
            ),
            (
                "data_key",
                keys::reconstruct_data_key(record).unwrap_or_default()
            ),
        ])
    );
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
    // Every AlreadyExists, the split-brain arm included: the record the GET
    // returned carries this writer's content hash.
    ravel_invariant::inv_always!(
        existing_record.content_hash == record.content_hash,
        "commit: one commit identity holds one content hash",
        std::collections::BTreeMap::from([
            ("commit_key", commit_key.to_owned()),
            ("this", hex::encode(&record.content_hash)),
            ("stored", hex::encode(&existing_record.content_hash)),
        ])
    );
    if ravel_invariant::inv_mutant!("commit/mismatched-identity-idempotent")
        || existing_record.content_hash == record.content_hash
    {
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
    if ravel_invariant::inv_mutant!("commit/commit-before-data") {
        return Ok(());
    }
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
            ravel_invariant::inv_reachable!(
                "commit: data object put",
                std::collections::BTreeMap::from([("data_key", key.to_owned())])
            );
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
        record_with_writer(Uuid::new_v4(), content_hash)
    }

    fn record_with_writer(writer_id: Uuid, content_hash: [u8; 32]) -> CommitRecord {
        record::build(NewCommitRecord {
            tenant_hash: TenantHash([0x33; 16]),
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
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

    /// Byte offset, within an encoded [`sample_record`], of the last byte of
    /// the `max_event_ts_ns` varint, and the bit inside it whose flip leaves a
    /// record that still decodes and still validates. Found by flipping every
    /// bit of the encoding in turn and keeping the ones that decode to a
    /// different `max_event_ts_ns`; the assertions below re-derive both, so a
    /// change to the record shape fails loudly here instead of silently
    /// corrupting a byte that does not matter.
    const MAX_EVENT_TS_FLIP: (usize, u32) = (233, 6);

    /// ADR-1696 follow-up task 3, the acceptance test from the ticket restated
    /// for the transport mechanism that replaced the ticket's proposed record
    /// framing: publish a record, flip one byte of the *stored* object, and the
    /// read is refused.
    ///
    /// The first half of the test is what the fix prevents, asserted rather
    /// than described. Against the pre-fix tree the flipped object came back
    /// from the store as an ordinary `Ok`, `record::decode` accepted it, and
    /// `validate` accepted it too (`min_event_ts_ns` is 0, so 0 <= 36 holds):
    /// `max_event_ts_ns` read 36 where the writer wrote 100, which moves the
    /// segment out of any query whose range starts above 36 and makes the
    /// answer short and error-free. `decode` still accepts those bytes today,
    /// because the record layout did not change (ADR-1696 decision 6); what
    /// changed is that the store no longer hands them over.
    #[tokio::test]
    async fn a_corrupted_stored_commit_record_reads_as_corrupted() {
        let store = MemoryStore::new();
        let record = sample_record([7; 32]);
        publish(&store, &record, &RetryPolicy::default())
            .await
            .expect("publish");
        let key = keys::commit_key_for_record(&record).expect("key");
        let stored = store.get(&key, GetRange::Full).await.expect("get").data;

        let (offset, bit) = MAX_EVENT_TS_FLIP;
        let mut flipped = stored.to_vec();
        flipped[offset] ^= 1u8 << bit;
        let decoded = record::decode(&flipped)
            .expect("the flipped bytes are still a decodable, valid CommitRecord");
        assert_eq!(
            record.max_event_ts_ns, 100,
            "the record as written carries the writer's max event timestamp"
        );
        assert_eq!(
            decoded.max_event_ts_ns, 36,
            "the flip must land inside max_event_ts_ns, which is the whole point \
             of this offset"
        );
        assert_eq!(
            decoded.content_hash, record.content_hash,
            "content_hash is untouched, so the publish-time comparison sees a \
             matching record and never reaches SplitBrain"
        );

        store
            .corrupt_stored_byte(&key, offset, bit)
            .expect("flip one bit of the stored object");
        let err = store
            .get(&key, GetRange::Full)
            .await
            .expect_err("a corrupted stored record must not be served");
        assert!(matches!(err, StoreError::Corrupted(_)), "got {err:?}");
    }

    /// The same corruption reached through the production read that motivated
    /// the ticket: `publish`'s `AlreadyExists` path GETs the stored record whole
    /// and compares `content_hash`. Pre-fix that GET returned the corrupted
    /// record, it compared equal, and the publish reported idempotent success
    /// while the catalog kept a record with a wrong `max_event_ts_ns`. Now the
    /// GET is refused and the caller sees a store error it can retry or shed,
    /// which is also why the fatal `SplitBrain` arm can no longer fire on a
    /// corrupt stored record.
    #[tokio::test]
    async fn republishing_over_a_corrupted_stored_record_is_a_store_error() {
        let store = MemoryStore::new();
        let record = sample_record([8; 32]);
        publish(&store, &record, &RetryPolicy::default())
            .await
            .expect("first publish");
        let key = keys::commit_key_for_record(&record).expect("key");
        let (offset, bit) = MAX_EVENT_TS_FLIP;
        store
            .corrupt_stored_byte(&key, offset, bit)
            .expect("flip one bit of the stored object");

        let err = publish(&store, &record, &RetryPolicy::default())
            .await
            .expect_err("the AlreadyExists comparison must not read corrupted bytes");
        assert!(
            matches!(
                err,
                PublishError::Store {
                    source: StoreError::Corrupted(_),
                    ..
                }
            ),
            "got {err:?}"
        );
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

    const NO_COMMIT_WITHOUT_DATA: &str =
        "commit: a commit record never exists without its data object";
    const ONE_IDENTITY_ONE_CONTENT: &str = "commit: one commit identity holds one content hash";

    /// True in a build where ravel-invariant records evaluations: the
    /// invariants lane (`ravel-invariant/capture`). This crate has no feature
    /// of its own that forwards `capture`, so the gate is an assertion macro,
    /// which evaluates its condition only when `capture` (or `antithesis`) is
    /// on and expands to nothing otherwise.
    fn capturing() -> bool {
        let on = std::cell::Cell::new(false);
        ravel_invariant::inv_sometimes!(
            {
                on.set(true);
                true
            },
            "ravel-commit test: evaluations are captured",
            ""
        );
        on.get()
    }

    /// Selects mutant `name` on this thread until the returned guard drops;
    /// `None` outside the invariants lane, where no mutant is compiled in.
    fn scoped_mutant(name: &'static str) -> Option<Box<dyn std::any::Any>> {
        let guard: std::cell::RefCell<Option<Box<dyn std::any::Any>>> = Default::default();
        let _ = name;
        ravel_invariant::inv_sometimes!(
            {
                let scoped = ravel_invariant::mutant::scoped(name).expect("a registered mutant");
                *guard.borrow_mut() = Some(Box::new(scoped));
                true
            },
            "ravel-commit test: a mutant is scoped",
            ""
        );
        guard.into_inner()
    }

    /// `(condition, details)` of every captured evaluation of `message` whose
    /// details mention `needle`; empty outside the invariants lane.
    fn evaluations(message: &'static str, needle: &str) -> Vec<(bool, String)> {
        let found = std::cell::RefCell::new(Vec::new());
        let _ = (message, needle);
        ravel_invariant::inv_sometimes!(
            {
                found.borrow_mut().extend(
                    ravel_invariant::capture::evaluations_of(message)
                        .into_iter()
                        .filter(|e| e.details.contains(needle))
                        .map(|e| (e.condition, e.details)),
                );
                true
            },
            "ravel-commit test: evaluations are read",
            ""
        );
        found.into_inner()
    }

    /// Puts `record`'s data object, then publishes `record`.
    async fn put_then_publish(store: &MemoryStore, record: &CommitRecord) -> String {
        let data_key = keys::reconstruct_data_key(record).expect("data key");
        put_data_object(store, &data_key, Bytes::from_static(b"segment"))
            .await
            .expect("data put");
        publish(store, record, &RetryPolicy::default())
            .await
            .expect("publish");
        data_key
    }

    #[tokio::test]
    async fn no_commit_without_data_holds_when_the_data_put_precedes_the_commit() {
        if !capturing() {
            return;
        }
        let store = MemoryStore::new();
        let record = sample_record([0x40; 32]);
        let data_key = put_then_publish(&store, &record).await;
        // The idempotent republish reaches the same site through the
        // AlreadyExists branch.
        publish(&store, &record, &RetryPolicy::default())
            .await
            .expect("republish");

        let evaluations = evaluations(NO_COMMIT_WITHOUT_DATA, &data_key);
        assert_eq!(evaluations.len(), 2, "{evaluations:?}");
        assert!(evaluations.iter().all(|(held, _)| *held), "{evaluations:?}");
    }

    /// `commit/commit-before-data`: the data PUT reports success without
    /// storing anything, so the commit record lands with no data object. The
    /// property's HEAD of the data key answers NotFound.
    #[tokio::test]
    async fn mutant_commit_before_data_breaks_no_commit_without_data() {
        let Some(_mutant) = scoped_mutant("commit/commit-before-data") else {
            return;
        };
        let store = MemoryStore::new();
        let record = sample_record([0x41; 32]);
        let data_key = put_then_publish(&store, &record).await;

        let evaluations = evaluations(NO_COMMIT_WITHOUT_DATA, &data_key);
        assert!(
            evaluations.iter().any(|(held, _)| !held),
            "NoCommitWithoutData never evaluated false under the mutant: {evaluations:?}"
        );
        assert!(
            matches!(store.head(&data_key).await, Err(StoreError::NotFound)),
            "the mutant stored no data object"
        );
    }

    /// Two records under one identity: the writer id is the first draw of a
    /// seeded identity source, the same draw for both, so the commit keys are
    /// equal by construction and only the content hashes differ.
    fn one_identity_two_contents(seed: u64) -> (CommitRecord, CommitRecord, String) {
        use crate::rng::SeededRng;
        let first = record_with_writer(SeededRng::new(seed).new_uuid(), [0x50; 32]);
        let second = record_with_writer(SeededRng::new(seed).new_uuid(), [0x51; 32]);
        let key = keys::commit_key_for_record(&first).expect("key");
        assert_eq!(
            key,
            keys::commit_key_for_record(&second).expect("key"),
            "the pinned identity source gives both records one commit key"
        );
        assert_ne!(first.content_hash, second.content_hash);
        (first, second, key)
    }

    /// The property is evaluated on every AlreadyExists, before the hash
    /// compare decides: it holds on the identical republish and is false on
    /// the split-brain arm, whose stored hash is the first record's.
    #[tokio::test]
    async fn one_identity_one_content_is_evaluated_on_every_already_exists() {
        if !capturing() {
            return;
        }
        let store = MemoryStore::new();
        let (first, second, key) = one_identity_two_contents(0x4331_0001);
        put_then_publish(&store, &first).await;
        publish(&store, &first, &RetryPolicy::default())
            .await
            .expect("an identical republish is idempotent");
        let err = publish(&store, &second, &RetryPolicy::default())
            .await
            .expect_err("a second content under the identity is split-brain");
        assert!(matches!(err, PublishError::SplitBrain { .. }), "{err:?}");

        let evaluations = evaluations(ONE_IDENTITY_ONE_CONTENT, &key);
        assert_eq!(evaluations.len(), 2, "{evaluations:?}");
        assert!(evaluations[0].0, "{evaluations:?}");
        assert!(!evaluations[1].0, "{evaluations:?}");
        assert!(
            evaluations[1].1.contains(&format!(
                "\"stored\": \"{}\"",
                hex::encode(first.content_hash)
            )),
            "the split-brain evaluation names the stored record's hash: {evaluations:?}"
        );
    }

    /// `publish_deferring_evidence` publishes with no evidence read, and
    /// `assert_commit_has_data` is the one site that reads it: a caller can
    /// run the read after a deadline race completes.
    #[tokio::test]
    async fn the_deferred_publish_reads_no_evidence_until_asked() {
        if !capturing() {
            return;
        }
        let store = MemoryStore::new();
        let record = sample_record([0x42; 32]);
        let data_key = keys::reconstruct_data_key(&record).expect("data key");
        put_data_object(&store, &data_key, Bytes::from_static(b"segment"))
            .await
            .expect("data put");
        publish_deferring_evidence(&store, &record, &RetryPolicy::default())
            .await
            .expect("publish");
        assert!(
            evaluations(NO_COMMIT_WITHOUT_DATA, &data_key).is_empty(),
            "the deferred publish read the evidence"
        );

        assert_commit_has_data(&store, &record).await;
        let evaluations = evaluations(NO_COMMIT_WITHOUT_DATA, &data_key);
        assert_eq!(evaluations.len(), 1, "{evaluations:?}");
        assert!(evaluations[0].0, "{evaluations:?}");
    }

    /// `commit/mismatched-identity-idempotent`: the AlreadyExists branch
    /// accepts the stored record without comparing hashes, so a second
    /// content under one identity is reported published. The property
    /// compares the hash the GET returned.
    #[tokio::test]
    async fn mutant_skip_hash_compare_breaks_one_identity_one_content() {
        let Some(_mutant) = scoped_mutant("commit/mismatched-identity-idempotent") else {
            return;
        };
        let store = MemoryStore::new();
        let (first, second, key) = one_identity_two_contents(0x4331_0002);
        put_then_publish(&store, &first).await;
        let outcome = publish(&store, &second, &RetryPolicy::default()).await;

        let evaluations = evaluations(ONE_IDENTITY_ONE_CONTENT, &key);
        assert!(
            evaluations.iter().any(|(held, _)| !held),
            "OneIdentityOneContent never evaluated false under the mutant: {evaluations:?}"
        );
        let token = outcome.expect("the mutant accepts the mismatched record");
        assert_eq!(token, record::token_for(&first).expect("token"));
    }
}
