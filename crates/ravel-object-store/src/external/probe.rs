//! The two checks a bucket must pass before Ravel reads a granted table out of
//! it (ADR-2040 decision 1).
//!
//! Both are qualification, not request-path code: they run when a grant is
//! created, and their verdicts are recorded. Both fail closed. A check that
//! could not be answered is a refusal, not a pass, because the thing being
//! qualified is a bucket the operator has just pointed Ravel at and the cost of
//! being wrong is either reading bytes from a file that has since been replaced
//! or exposing Ravel's own objects through an external table.
//!
//! Nothing in a shipping binary calls either yet; the grant CLI (#2051) and
//! `CREATE EXTERNAL TABLE` (#2054) are the callers.

use bytes::Bytes;
use rand::RngExt;

use crate::{GetRange, ObjectStoreBackend, Pin, PutOptions, StoreError};

/// Prefix for the objects [`probe_not_ravel_bucket`] writes.
///
/// Objects under it are meant to be transient: the probe issues a delete for
/// its own object on every path it returns through. Two paths can still leave
/// one behind, because neither reaches that delete: a probe put that timed out
/// after the object had landed (the write is reported as failed and the key is
/// not deleted), and a cancelled probe (the future dropped before the delete is
/// issued). Nothing in Ravel reaps this prefix, so what bounds the leak is
/// whatever lifecycle rule the operator sets on `sys/pq-probe/` in the bucket
/// itself. Each leaked object is 32 bytes.
pub const PROBE_PREFIX: &str = "sys/pq-probe/";

/// The key Ravel's own buckets carry a tenancy marker under (ADR-0050).
///
/// Spelled out here rather than imported: the constant it mirrors lives in
/// `ravel-server`, which depends on this crate, so importing it would invert
/// the dependency.
const TENANCY_MARKER_KEY: &str = "sys/tenancy";

/// An ETag no store issues, used as the wrong half of the precondition probe.
///
/// Random per call, so a store cannot pass the refusing half by special-casing
/// one literal: a fixed value is a string an endpoint can learn to reject, and
/// the probe would then record "refuses a wrong ETag" for a store that refuses
/// exactly that ETag and serves every other pinned read unconditionally.
/// Quoted because S3 ETags are quoted strings and an unquoted value could be
/// rejected as malformed rather than evaluated as a precondition, which would
/// make the probe pass for the wrong reason.
fn wrong_etag() -> String {
    use std::fmt::Write;

    let bytes: [u8; 16] = rand::rng().random();
    let mut etag = String::with_capacity(34);
    etag.push('"');
    for byte in bytes {
        // Infallible: `String`'s `Write` never fails.
        let _ = write!(etag, "{byte:02x}");
    }
    etag.push('"');
    etag
}

/// Why [`probe_preconditions`] refused a store, by half.
///
/// The probe has two halves and they fail for different reasons: a store can
/// evaluate `If-Match` and get the matching case wrong (returning a refusal
/// where it should serve), or serve the wrong case (ignoring the precondition
/// entirely). Reporting which one failed is the difference between "this
/// endpoint does not implement conditional reads" and "this endpoint
/// implements them incorrectly", and an operator acts differently on each.
#[derive(Debug, thiserror::Error)]
pub enum PreconditionProbeFailure {
    /// The probe key could not be read at all, so neither half ran. The store
    /// is unqualified because the probe is inconclusive, not because it
    /// answered wrongly.
    #[error(
        "the probe key {key:?} could not be HEADed, so preconditions were never tested: {source}"
    )]
    Head { key: String, source: StoreError },
    /// Matching half: a read carrying the object's own ETag was refused.
    #[error("a ranged read of {key:?} pinned to the object's own ETag was refused: {source}")]
    MatchingPinRefused { key: String, source: StoreError },
    /// Refusing half: a read carrying a wrong ETag was served anyway, so the
    /// store ignores the precondition. A pinned read against it would return
    /// bytes from whatever version happens to be there.
    #[error(
        "a ranged read of {key:?} pinned to a wrong ETag was served: the store ignores read preconditions"
    )]
    WrongPinAccepted { key: String },
    /// Refusing half: the read was refused, but not as a precondition failure.
    /// Not a pass: the caller distinguishes `PreconditionFailed` from every
    /// other error, so a store that reports something else cannot be read
    /// through even though it did refuse.
    #[error(
        "a ranged read of {key:?} pinned to a wrong ETag failed with {source} instead of a precondition failure"
    )]
    WrongPinWrongError { key: String, source: StoreError },
}

/// What [`probe_preconditions`] learned about `key` when both halves passed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreconditionProbe {
    /// The ETag the store reported for `key`, verbatim.
    pub etag: String,
    /// The version the store reported, when it reports one distinct from the
    /// ETag.
    pub version: Option<String>,
}

/// Qualify `store` for pinned reads, using `key` as the subject.
///
/// Three requests: a HEAD to learn the object's identity, then two 1-byte
/// ranged reads, one carrying that identity and one carrying an ETag no store
/// issues, drawn afresh on every call (see [`wrong_etag`]).
/// The store qualifies only if the first is served and the second is refused
/// with [`StoreError::PreconditionFailed`]. One byte, because what is being
/// measured is the header, not the body.
///
/// `key` must already exist; the probe never writes to the store it is
/// qualifying, which is the point of it being read-only.
pub async fn probe_preconditions(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<PreconditionProbe, PreconditionProbeFailure> {
    // `pin_of` is the one path that turns store metadata into a pin, so the
    // probe asserts the same identity a caller would record. It reports a
    // version only when the store has one distinct from the ETag: sending the
    // ETag back as a `versionId` would fail for a reason that says nothing
    // about precondition support.
    let (meta, matching) =
        store
            .pin_of(key)
            .await
            .map_err(|source| PreconditionProbeFailure::Head {
                key: key.to_string(),
                source,
            })?;
    let version = matching.version.clone();

    store
        .get_pinned(key, GetRange::Range(0, 1), &matching)
        .await
        .map_err(|source| PreconditionProbeFailure::MatchingPinRefused {
            key: key.to_string(),
            source,
        })?;

    let wrong = Pin {
        etag: wrong_etag(),
        version: version.clone(),
    };
    match store.get_pinned(key, GetRange::Range(0, 1), &wrong).await {
        Ok(_) => Err(PreconditionProbeFailure::WrongPinAccepted {
            key: key.to_string(),
        }),
        Err(StoreError::PreconditionFailed) => Ok(PreconditionProbe {
            etag: meta.etag.0,
            version,
        }),
        Err(source) => Err(PreconditionProbeFailure::WrongPinWrongError {
            key: key.to_string(),
            source,
        }),
    }
}

/// Why [`probe_not_ravel_bucket`] refused a candidate.
#[derive(Debug, thiserror::Error)]
pub enum RavelBucketProbeFailure {
    /// The candidate served the probe object Ravel had just written to its own
    /// bucket: the two handles name one bucket. A grant on it would let an
    /// external table read Ravel's own objects, across tenants.
    #[error(
        "the candidate store served {key:?}, the probe object just written to Ravel's own \
         bucket: it is Ravel's bucket reached under another name"
    )]
    SameBucket { key: String },
    /// The probe object could not be written, so the question was never asked.
    #[error("the probe object {key:?} could not be written to Ravel's own bucket: {source}")]
    ProbeWriteFailed { key: String, source: StoreError },
    /// The candidate holds a Ravel tenancy marker, so it is a Ravel bucket:
    /// either Ravel's own reached under a name the probe object could not
    /// detect, or a copy or restore of one. Neither may back an external
    /// table.
    #[error("the candidate store holds {key:?}: candidate holds a Ravel tenancy marker")]
    TenancyMarkerPresent { key: String },
    /// The candidate's read neither returned the probe bytes nor reported the
    /// object absent, so the probe has no answer.
    ///
    /// This is a refusal, not a pass. A store that answers a random key with
    /// an access denial, a timeout or (stranger still) different bytes has not
    /// shown that it is a different bucket; it has shown that it cannot be
    /// asked. Passing it would qualify a grant on the strength of an error
    /// message.
    #[error("the candidate store's read of {key:?} was inconclusive ({detail}); refusing")]
    Inconclusive { key: String, detail: String },
}

/// Refuse a candidate bucket that is Ravel's own bucket, reached under another
/// name or copied.
///
/// Two reads of the candidate, and both must come back clean.
///
/// The first is the identity read. The probe writes an object with a random
/// key under [`PROBE_PREFIX`] and random contents to `ravel_store`, then reads
/// that key from `candidate_store`. The key is random so no candidate can hold
/// it by coincidence, and the contents are random so a store that answers every
/// key with the same placeholder cannot be mistaken for Ravel's own.
///
/// The second reads [`TENANCY_MARKER_KEY`] from the candidate. The identity
/// read only catches one live bucket reached twice; a copy, a restore, or a
/// replication target of a Ravel bucket is a different bucket that still holds
/// Ravel's objects, and the probe object written after the copy was taken is
/// not in it. A candidate that serves the marker at all is refused, whatever
/// the bytes are.
///
/// A read of either key that returns anything other than a clean `NotFound`
/// (or, for the identity read, the exact probe payload) is inconclusive, and
/// inconclusive refuses. One consequence is worth stating plainly: credentials
/// scoped so tightly that they cannot read `sys/` answer the marker read with
/// an access denial rather than a `NotFound`, so a grant offered under them is
/// refused. That is the intended trade. The probe cannot tell "you may not ask"
/// from "there is nothing there", and qualifying a bucket on an error message
/// is how a Ravel bucket gets granted to an external table.
///
/// The probe issues a delete for its own object before returning, on every
/// path it returns through, including the error paths. A delete that itself
/// fails is logged and does not change the verdict: the verdict is the answer
/// the caller asked for, and losing it to report a leaked probe object would be
/// the worse trade. [`PROBE_PREFIX`] describes the two paths that never reach
/// the delete at all.
pub async fn probe_not_ravel_bucket(
    ravel_store: &dyn ObjectStoreBackend,
    candidate_store: &dyn ObjectStoreBackend,
) -> Result<(), RavelBucketProbeFailure> {
    let key = probe_key();
    let payload = random_bytes();

    if let Err(source) = ravel_store
        .put(&key, payload.clone(), PutOptions::default())
        .await
    {
        // Nothing was written, so there is nothing to clean up.
        return Err(RavelBucketProbeFailure::ProbeWriteFailed { key, source });
    }

    let verdict = match candidate_store.get(&key, GetRange::Full).await {
        Ok(outcome) if outcome.data == payload => {
            Err(RavelBucketProbeFailure::SameBucket { key: key.clone() })
        }
        Ok(outcome) => Err(RavelBucketProbeFailure::Inconclusive {
            key: key.clone(),
            detail: format!(
                "it served {} bytes that are not the probe payload",
                outcome.data.len()
            ),
        }),
        // The one error that is a positive answer: the candidate does not have
        // Ravel's object. It still has to clear the tenancy marker.
        Err(StoreError::NotFound) => tenancy_marker_verdict(candidate_store).await,
        Err(other) => Err(RavelBucketProbeFailure::Inconclusive {
            key: key.clone(),
            detail: other.to_string(),
        }),
    };

    if let Err(e) = ravel_store.delete(&key).await {
        tracing::warn!(key = %key, error = %e, "the bucket probe object could not be deleted");
    }
    verdict
}

/// Refuse a candidate that holds Ravel's tenancy marker, and refuse one whose
/// answer about the marker is anything other than a clean absence.
async fn tenancy_marker_verdict(
    candidate_store: &dyn ObjectStoreBackend,
) -> Result<(), RavelBucketProbeFailure> {
    match candidate_store.get(TENANCY_MARKER_KEY, GetRange::Full).await {
        // Any bytes at all. The marker's contents are not parsed here: a
        // candidate carrying the key is a Ravel bucket whatever it holds.
        Ok(_) => Err(RavelBucketProbeFailure::TenancyMarkerPresent {
            key: TENANCY_MARKER_KEY.to_string(),
        }),
        Err(StoreError::NotFound) => Ok(()),
        Err(other) => Err(RavelBucketProbeFailure::Inconclusive {
            key: TENANCY_MARKER_KEY.to_string(),
            detail: other.to_string(),
        }),
    }
}

/// `sys/pq-probe/<32 random hex chars>`.
fn probe_key() -> String {
    use std::fmt::Write;

    let bytes: [u8; 16] = rand::rng().random();
    let mut key = String::with_capacity(PROBE_PREFIX.len() + 32);
    key.push_str(PROBE_PREFIX);
    for byte in bytes {
        // Infallible: `String`'s `Write` never fails.
        let _ = write!(key, "{byte:02x}");
    }
    key
}

fn random_bytes() -> Bytes {
    let bytes: [u8; 32] = rand::rng().random();
    Bytes::copy_from_slice(&bytes)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use crate::memory::MemoryStore;
    use crate::{
        Capabilities, DelimitedList, GetOutcome, ListPage, ObjectMeta, PageToken, PutOutcome,
    };

    const SUBJECT: &str = "granted/table/part-0.parquet";

    async fn seeded() -> Arc<MemoryStore> {
        let store = Arc::new(MemoryStore::new());
        store
            .put(
                SUBJECT,
                Bytes::from_static(b"parquet-bytes"),
                PutOptions::default(),
            )
            .await
            .expect("seed the probe subject");
        store
    }

    /// A store whose `get_pinned` drops the pin and reads unconditionally: the
    /// shape of an endpoint that accepts `If-Match` on the wire and ignores it.
    struct IgnoresPreconditions(Arc<MemoryStore>);

    /// A store that refuses every pinned read, including one carrying the
    /// object's own identity.
    struct RefusesEveryPin(Arc<MemoryStore>);

    /// A store that refuses a wrong pin, but with the wrong error.
    struct RefusesWithTheWrongError(Arc<MemoryStore>);

    /// A store that evaluates preconditions correctly and records the ETag of
    /// every pin it was handed, in order.
    struct RecordsPinEtags(Arc<MemoryStore>, std::sync::Mutex<Vec<String>>);

    macro_rules! delegate_reads {
        ($ty:ty) => {
            #[async_trait::async_trait]
            impl ObjectStoreBackend for $ty {
                async fn put(
                    &self,
                    key: &str,
                    data: Bytes,
                    opts: PutOptions,
                ) -> Result<PutOutcome, StoreError> {
                    self.0.put(key, data, opts).await
                }
                async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
                    self.0.get(key, range).await
                }
                async fn put_multipart<'a>(
                    &'a self,
                    key: &str,
                ) -> Result<Box<dyn crate::MultipartUpload + 'a>, StoreError> {
                    self.0.put_multipart(key).await
                }
                async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
                    self.0.head(key).await
                }
                async fn list(
                    &self,
                    prefix: &str,
                    page: Option<PageToken>,
                ) -> Result<ListPage, StoreError> {
                    self.0.list(prefix, page).await
                }
                async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
                    self.0.list_delimited(prefix).await
                }
                async fn delete(&self, key: &str) -> Result<(), StoreError> {
                    self.0.delete(key).await
                }
                fn capabilities(&self) -> Capabilities {
                    self.0.capabilities()
                }
                async fn get_pinned(
                    &self,
                    key: &str,
                    range: GetRange,
                    pin: &Pin,
                ) -> Result<crate::PinnedRead, StoreError> {
                    self.pinned(key, range, pin).await
                }
                async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
                    self.0.pin_of(key).await
                }
            }
        };
    }

    impl IgnoresPreconditions {
        async fn pinned(
            &self,
            key: &str,
            range: GetRange,
            _pin: &Pin,
        ) -> Result<crate::PinnedRead, StoreError> {
            self.0.get_with_pin(key, range).await
        }
    }
    impl RefusesEveryPin {
        async fn pinned(
            &self,
            _key: &str,
            _range: GetRange,
            _pin: &Pin,
        ) -> Result<crate::PinnedRead, StoreError> {
            Err(StoreError::PreconditionFailed)
        }
    }
    impl RefusesWithTheWrongError {
        async fn pinned(
            &self,
            key: &str,
            range: GetRange,
            pin: &Pin,
        ) -> Result<crate::PinnedRead, StoreError> {
            match self.0.get_pinned(key, range, pin).await {
                Err(StoreError::PreconditionFailed) => {
                    Err(StoreError::AccessDenied("no conditional reads".into()))
                }
                other => other,
            }
        }
    }

    impl RecordsPinEtags {
        async fn pinned(
            &self,
            key: &str,
            range: GetRange,
            pin: &Pin,
        ) -> Result<crate::PinnedRead, StoreError> {
            self.1
                .lock()
                .expect("the recorder lock is never poisoned")
                .push(pin.etag.clone());
            self.0.get_pinned(key, range, pin).await
        }
    }

    delegate_reads!(IgnoresPreconditions);
    delegate_reads!(RefusesEveryPin);
    delegate_reads!(RefusesWithTheWrongError);
    delegate_reads!(RecordsPinEtags);

    #[tokio::test]
    async fn a_store_with_real_preconditions_qualifies() {
        let store = seeded().await;
        let meta = store.head(SUBJECT).await.expect("head");
        let probe = probe_preconditions(store.as_ref(), SUBJECT)
            .await
            .expect("MemoryStore evaluates preconditions");
        assert_eq!(probe.etag, meta.etag.0);
        assert_eq!(probe.version.as_deref(), Some(meta.version.0.as_str()));
    }

    #[tokio::test]
    async fn a_store_that_ignores_the_pin_is_refused_as_such() {
        let store = IgnoresPreconditions(seeded().await);
        let err = probe_preconditions(&store, SUBJECT)
            .await
            .expect_err("a store that ignores preconditions must not qualify");
        assert!(
            matches!(err, PreconditionProbeFailure::WrongPinAccepted { ref key } if key == SUBJECT),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn a_store_that_refuses_its_own_etag_is_refused_on_the_matching_half() {
        let store = RefusesEveryPin(seeded().await);
        let err = probe_preconditions(&store, SUBJECT)
            .await
            .expect_err("a store that refuses its own ETag must not qualify");
        assert!(
            matches!(
                err,
                PreconditionProbeFailure::MatchingPinRefused { ref key, .. } if key == SUBJECT
            ),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn a_refusal_that_is_not_a_precondition_failure_is_its_own_verdict() {
        let store = RefusesWithTheWrongError(seeded().await);
        let err = probe_preconditions(&store, SUBJECT)
            .await
            .expect_err("a refusal reported as something else must not qualify");
        assert!(
            matches!(
                err,
                PreconditionProbeFailure::WrongPinWrongError { ref key, .. } if key == SUBJECT
            ),
            "got {err:?}"
        );
    }

    /// The refusing half must not rest on one literal ETag. A store that
    /// special-cases the value the probe always sent would pass the refusing
    /// half while serving every other pinned read unconditionally, so the
    /// value has to be drawn afresh per call.
    #[tokio::test]
    async fn the_wrong_etag_is_drawn_afresh_on_every_call() {
        let store = RecordsPinEtags(seeded().await, std::sync::Mutex::new(Vec::new()));
        probe_preconditions(&store, SUBJECT)
            .await
            .expect("the first probe qualifies");
        probe_preconditions(&store, SUBJECT)
            .await
            .expect("the second probe qualifies");

        let seen = store.1.lock().expect("the recorder lock");
        // Two calls, each sending the matching pin then the wrong one.
        assert_eq!(seen.len(), 4, "got {seen:?}");
        let (first, second) = (&seen[1], &seen[3]);
        assert_ne!(
            first, second,
            "the wrong ETag must differ between calls, not be a constant"
        );
        for wrong in [first, second] {
            assert!(
                wrong.starts_with('"') && wrong.ends_with('"'),
                "the wrong ETag must stay a quoted string: {wrong}"
            );
            assert_eq!(wrong.len(), 34, "16 random bytes in hex, quoted: {wrong}");
        }
    }

    #[tokio::test]
    async fn a_subject_that_cannot_be_headed_is_inconclusive_not_a_pass() {
        let store = MemoryStore::new();
        let err = probe_preconditions(&store, SUBJECT)
            .await
            .expect_err("a missing subject cannot qualify a store");
        assert!(
            matches!(err, PreconditionProbeFailure::Head { .. }),
            "got {err:?}"
        );
    }

    async fn probe_objects_left(store: &dyn ObjectStoreBackend) -> Vec<String> {
        store
            .list(PROBE_PREFIX, None)
            .await
            .expect("list the probe prefix")
            .objects
            .into_iter()
            .map(|o| o.key)
            .collect()
    }

    #[tokio::test]
    async fn a_candidate_that_is_ravels_own_bucket_is_detected() {
        let ravel = Arc::new(MemoryStore::new());
        // The same store reached through a second handle: exactly the "one
        // bucket under two names" case the probe exists for.
        let candidate = Arc::clone(&ravel);

        let err = probe_not_ravel_bucket(ravel.as_ref(), candidate.as_ref())
            .await
            .expect_err("Ravel's own bucket must be detected");
        assert!(
            matches!(err, RavelBucketProbeFailure::SameBucket { ref key } if key.starts_with(PROBE_PREFIX)),
            "got {err:?}"
        );
        assert!(
            probe_objects_left(ravel.as_ref()).await.is_empty(),
            "the probe object must be deleted even when the probe fails"
        );
    }

    #[tokio::test]
    async fn a_genuinely_different_bucket_passes_and_leaves_nothing_behind() {
        let ravel = MemoryStore::new();
        let candidate = MemoryStore::new();

        probe_not_ravel_bucket(&ravel, &candidate)
            .await
            .expect("a different bucket must pass");
        assert!(
            probe_objects_left(&ravel).await.is_empty(),
            "the probe object must be deleted on the passing path too"
        );
        assert!(
            probe_objects_left(&candidate).await.is_empty(),
            "the probe never writes to the candidate"
        );
    }

    /// A candidate whose read fails for any reason other than "no such object"
    /// is inconclusive, not a pass. A probe that read an error as evidence of a
    /// different bucket would qualify a grant on the strength of an error
    /// message, so this is the case that distinguishes the two.
    #[tokio::test]
    async fn a_candidate_that_cannot_be_read_is_inconclusive_not_a_pass() {
        let ravel = MemoryStore::new();
        let candidate = FaultStore::new(
            MemoryStore::new(),
            FaultPlan::empty().with_rule(Rule::new(
                Op::Get,
                ScriptedFault::Permanent("the grant does not cover this key".into()),
            )),
        );

        let err = probe_not_ravel_bucket(&ravel, &candidate)
            .await
            .expect_err("an unreadable candidate must not qualify");
        assert!(
            matches!(err, RavelBucketProbeFailure::Inconclusive { .. }),
            "got {err:?}"
        );
        assert!(
            probe_objects_left(&ravel).await.is_empty(),
            "the probe object must be deleted when the candidate read errors"
        );
    }

    /// A copy, restore or replication target of a Ravel bucket is a different
    /// bucket that still holds Ravel's objects. The probe object is written
    /// after the copy was taken, so the identity read reports it absent and
    /// only the tenancy marker is left to catch it.
    #[tokio::test]
    async fn a_candidate_that_is_a_copy_of_a_ravel_bucket_is_refused() {
        let ravel = MemoryStore::new();
        let candidate = MemoryStore::new();
        candidate
            .put(
                TENANCY_MARKER_KEY,
                Bytes::from_static(b"{\"tenant\":\"acme\"}"),
                PutOptions::default(),
            )
            .await
            .expect("seed the candidate's tenancy marker");

        let err = probe_not_ravel_bucket(&ravel, &candidate)
            .await
            .expect_err("a bucket carrying a Ravel tenancy marker must not qualify");
        assert!(
            matches!(
                err,
                RavelBucketProbeFailure::TenancyMarkerPresent { ref key }
                    if key == TENANCY_MARKER_KEY
            ),
            "got {err:?}"
        );
        assert!(
            probe_objects_left(&ravel).await.is_empty(),
            "the probe object must be deleted on the marker path too"
        );
    }

    /// The marker read is subject to the same rule as the identity read: only
    /// a clean absence is an answer. Credentials that cannot read `sys/`
    /// answer with a refusal, and a refusal is not evidence of anything.
    #[tokio::test]
    async fn a_candidate_that_refuses_the_tenancy_read_is_inconclusive() {
        let ravel = MemoryStore::new();
        let candidate = FaultStore::new(
            MemoryStore::new(),
            FaultPlan::empty().with_rule(
                Rule::new(
                    Op::Get,
                    ScriptedFault::Permanent(
                        "403 Forbidden: s3:GetObject is not granted on sys/".into(),
                    ),
                )
                .with_key_contains(TENANCY_MARKER_KEY),
            ),
        );

        let err = probe_not_ravel_bucket(&ravel, &candidate)
            .await
            .expect_err("a candidate that will not answer about the marker must not qualify");
        assert!(
            matches!(
                err,
                RavelBucketProbeFailure::Inconclusive { ref key, .. } if key == TENANCY_MARKER_KEY
            ),
            "got {err:?}"
        );
        assert!(
            probe_objects_left(&ravel).await.is_empty(),
            "the probe object must be deleted when the marker read errors"
        );
    }

    /// A store that answers every key with the same placeholder bytes, which
    /// some gateways do in place of a 404. Only `get` is reachable from the
    /// probe; the rest of the surface is unused here.
    struct ServesPlaceholderForEveryKey;

    #[async_trait::async_trait]
    impl ObjectStoreBackend for ServesPlaceholderForEveryKey {
        async fn put(
            &self,
            _key: &str,
            _data: Bytes,
            _opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            Err(StoreError::Permanent("unused".into()))
        }
        async fn get(&self, _key: &str, _range: GetRange) -> Result<GetOutcome, StoreError> {
            Ok(GetOutcome {
                data: Bytes::from_static(b"not-the-probe-payload"),
                etag: crate::Etag("placeholder".into()),
                version: crate::Version("placeholder".into()),
                total_size: 21,
            })
        }
        async fn put_multipart<'a>(
            &'a self,
            _key: &str,
        ) -> Result<Box<dyn crate::MultipartUpload + 'a>, StoreError> {
            Err(StoreError::Permanent("unused".into()))
        }
        async fn head(&self, _key: &str) -> Result<ObjectMeta, StoreError> {
            Err(StoreError::Permanent("unused".into()))
        }
        async fn list(
            &self,
            _prefix: &str,
            _page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            Err(StoreError::Permanent("unused".into()))
        }
        async fn list_delimited(&self, _prefix: &str) -> Result<DelimitedList, StoreError> {
            Err(StoreError::Permanent("unused".into()))
        }
        async fn delete(&self, _key: &str) -> Result<(), StoreError> {
            Err(StoreError::Permanent("unused".into()))
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities::mandatory()
        }
    }

    /// A candidate that answers every key with the same placeholder bytes is
    /// inconclusive too: it did not return the probe payload, but it did not
    /// report the object absent either.
    #[tokio::test]
    async fn a_candidate_that_serves_other_bytes_is_inconclusive() {
        let ravel = MemoryStore::new();
        let candidate = ServesPlaceholderForEveryKey;

        let err = probe_not_ravel_bucket(&ravel, &candidate)
            .await
            .expect_err("a candidate that served neither the payload nor a 404 is inconclusive");
        assert!(
            matches!(err, RavelBucketProbeFailure::Inconclusive { .. }),
            "got {err:?}"
        );
        assert!(probe_objects_left(&ravel).await.is_empty());
    }

    #[tokio::test]
    async fn a_probe_that_cannot_be_written_says_so_and_does_not_pass() {
        let ravel = FaultStore::new(
            MemoryStore::new(),
            FaultPlan::empty().with_rule(Rule::new(
                Op::Put,
                ScriptedFault::Transient("the bucket is unreachable".into()),
            )),
        );
        let candidate = MemoryStore::new();

        let err = probe_not_ravel_bucket(&ravel, &candidate)
            .await
            .expect_err("a probe that was never written answers nothing");
        assert!(
            matches!(err, RavelBucketProbeFailure::ProbeWriteFailed { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn probe_keys_are_random_and_under_the_probe_prefix() {
        let first = probe_key();
        let second = probe_key();
        assert_ne!(first, second, "two probes must not collide");
        for key in [&first, &second] {
            assert!(key.starts_with(PROBE_PREFIX), "{key}");
            let suffix = &key[PROBE_PREFIX.len()..];
            assert_eq!(suffix.len(), 32, "{key}");
            assert!(suffix.chars().all(|c| c.is_ascii_hexdigit()), "{key}");
        }
        assert_ne!(
            random_bytes(),
            random_bytes(),
            "the payload must not be constant"
        );
    }
}
