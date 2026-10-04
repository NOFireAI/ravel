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
//! Two shipping callers run both: `ravel-cli tenant parquet-grant add`
//! (`services/ravel-cli/src/parquet_grant.rs`), and `CREATE EXTERNAL TABLE`
//! over `POST /api/v1/sql` (`crates/ravel-sql/src/ddl.rs`), which ravel-server
//! serves when built with its `sql` feature.

use std::sync::Arc;

use bytes::Bytes;
use rand::RngExt;

use crate::{GetRange, ObjectStoreBackend, Pin, PutOptions, StoreError};

/// Prefix for the objects [`probe_not_ravel_bucket`] writes.
///
/// Objects under it are meant to be transient. The probe deletes its own object
/// inline once the candidate has answered, and a drop guard covers the paths
/// that never reach that delete or that see it fail: the probe future dropped
/// mid-flight (a caller's deadline), a put reported as failed (which a put that
/// timed out after the object landed also is), and an inline delete that
/// returned an error. On those paths the guard spawns a best-effort delete on
/// the current tokio runtime.
///
/// An object can still be left behind when that spawned delete fails, when the
/// process exits before it runs, or when the guard fires outside a tokio
/// runtime and so has nowhere to spawn it. Nothing in Ravel reaps this prefix,
/// so what bounds the leak is whatever lifecycle rule the operator sets on
/// `sys/pq-probe/` in the bucket itself. Each leaked object is 32 bytes.
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
/// `ravel_store` serves the probe's put and its inline delete. `cleanup_store`
/// is the same bucket, and serves only the delete a [`ProbeObjectGuard`] spawns
/// when the probe cannot delete its object itself; it is an owned handle
/// because that task outlives the call. A caller with no cost accounting
/// passes the same store for both. A caller that counts its requests passes
/// its counting wrapper as `ravel_store` and the unwrapped store as
/// `cleanup_store`, so a background delete is never in the cost it reports:
/// whether that request landed before the cost was read would be a race.
///
/// Once the candidate has answered, the probe deletes its own object inline,
/// on every verdict. An inline delete that fails is logged and does not change
/// the verdict: the verdict is the answer the caller asked for, and losing it
/// to report a leaked probe object would be the worse trade. The guard then
/// retries it in the background. The guard also deletes the object when the
/// put is reported failed and when this future is dropped before the inline
/// delete returns. [`PROBE_PREFIX`] says what can still leave an object behind.
pub async fn probe_not_ravel_bucket(
    ravel_store: &dyn ObjectStoreBackend,
    cleanup_store: &Arc<dyn ObjectStoreBackend>,
    candidate_store: &dyn ObjectStoreBackend,
) -> Result<(), RavelBucketProbeFailure> {
    let key = probe_key();
    let payload = random_bytes();

    // Armed before the put is awaited: a put cancelled in flight, or reported
    // failed after it reached the store, may still have landed.
    let mut guard = ProbeObjectGuard {
        cleanup_store: Arc::clone(cleanup_store),
        key: key.clone(),
        armed: true,
    };

    if let Err(source) = ravel_store
        .put(&key, payload.clone(), PutOptions::default())
        .await
    {
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

    match ravel_store.delete(&key).await {
        Ok(()) => guard.disarm(),
        Err(e) => tracing::warn!(
            key = %key,
            error = %e,
            "the bucket probe object could not be deleted; retrying in the background"
        ),
    }
    verdict
}

/// Deletes the probe object of [`probe_not_ravel_bucket`] if dropped while
/// armed, through `cleanup_store`.
///
/// The delete is spawned on the current tokio runtime, best effort; with no
/// runtime it is logged and skipped. When the put never landed, the spawned
/// delete names a key that is not there. That is harmless because delete is
/// idempotent: docs/object-store-contract.md, "Operations", declares
/// `delete` with `// idempotent: NotFound => Ok`.
struct ProbeObjectGuard {
    cleanup_store: Arc<dyn ObjectStoreBackend>,
    key: String,
    armed: bool,
}

impl ProbeObjectGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ProbeObjectGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let key = std::mem::take(&mut self.key);
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(
                key = %key,
                "no tokio runtime to delete the bucket probe object from; it is left behind"
            );
            return;
        };
        let store = Arc::clone(&self.cleanup_store);
        runtime.spawn(async move {
            if let Err(e) = store.delete(&key).await {
                tracing::warn!(
                    key = %key,
                    error = %e,
                    "the bucket probe object could not be deleted in the background"
                );
            }
        });
    }
}

/// Refuse a candidate that holds Ravel's tenancy marker, and refuse one whose
/// answer about the marker is anything other than a clean absence.
async fn tenancy_marker_verdict(
    candidate_store: &dyn ObjectStoreBackend,
) -> Result<(), RavelBucketProbeFailure> {
    match candidate_store
        .get(TENANCY_MARKER_KEY, GetRange::Full)
        .await
    {
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
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::fault::{FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
    use crate::memory::MemoryStore;
    use crate::{
        Capabilities, DelimitedList, GetOutcome, InstrumentedStore, ListPage, ObjectMeta,
        PageToken, PutOutcome,
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
        let cleanup: Arc<dyn ObjectStoreBackend> = ravel.clone();

        let err = probe_not_ravel_bucket(ravel.as_ref(), &cleanup, candidate.as_ref())
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
        let ravel = Arc::new(MemoryStore::new());
        let cleanup: Arc<dyn ObjectStoreBackend> = ravel.clone();
        let candidate = MemoryStore::new();

        probe_not_ravel_bucket(ravel.as_ref(), &cleanup, &candidate)
            .await
            .expect("a different bucket must pass");
        assert!(
            probe_objects_left(ravel.as_ref()).await.is_empty(),
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
        let ravel = Arc::new(MemoryStore::new());
        let cleanup: Arc<dyn ObjectStoreBackend> = ravel.clone();
        let candidate = FaultStore::new(
            MemoryStore::new(),
            FaultPlan::empty().with_rule(Rule::new(
                Op::Get,
                ScriptedFault::Permanent("the grant does not cover this key".into()),
            )),
        );

        let err = probe_not_ravel_bucket(&ravel, &cleanup, &candidate)
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
        let ravel = Arc::new(MemoryStore::new());
        let cleanup: Arc<dyn ObjectStoreBackend> = ravel.clone();
        let candidate = MemoryStore::new();
        candidate
            .put(
                TENANCY_MARKER_KEY,
                Bytes::from_static(b"{\"tenant\":\"acme\"}"),
                PutOptions::default(),
            )
            .await
            .expect("seed the candidate's tenancy marker");

        let err = probe_not_ravel_bucket(&ravel, &cleanup, &candidate)
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
        let ravel = Arc::new(MemoryStore::new());
        let cleanup: Arc<dyn ObjectStoreBackend> = ravel.clone();
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

        let err = probe_not_ravel_bucket(&ravel, &cleanup, &candidate)
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
        let ravel = Arc::new(MemoryStore::new());
        let cleanup: Arc<dyn ObjectStoreBackend> = ravel.clone();
        let candidate = ServesPlaceholderForEveryKey;

        let err = probe_not_ravel_bucket(&ravel, &cleanup, &candidate)
            .await
            .expect_err("a candidate that served neither the payload nor a 404 is inconclusive");
        assert!(
            matches!(err, RavelBucketProbeFailure::Inconclusive { .. }),
            "got {err:?}"
        );
        assert!(probe_objects_left(&ravel).await.is_empty());
    }

    /// A put that never reached the store still leaves the guard armed, so it
    /// issues one delete of a key that is not there, and that delete succeeds.
    #[tokio::test]
    async fn a_probe_that_cannot_be_written_says_so_and_does_not_pass() {
        let memory = Arc::new(MemoryStore::new());
        let ravel = FaultStore::new(
            Arc::clone(&memory),
            FaultPlan::empty().with_rule(Rule::new(
                Op::Put,
                ScriptedFault::Transient("the bucket is unreachable".into()),
            )),
        );
        let counted_cleanup = Arc::new(InstrumentedStore::new(Arc::clone(&memory)));
        let cleanup: Arc<dyn ObjectStoreBackend> = counted_cleanup.clone();
        let candidate = MemoryStore::new();

        let err = probe_not_ravel_bucket(&ravel, &cleanup, &candidate)
            .await
            .expect_err("a probe that was never written answers nothing");
        assert!(
            matches!(err, RavelBucketProbeFailure::ProbeWriteFailed { .. }),
            "got {err:?}"
        );
        assert_eq!(ravel.fault_count(Op::Put, FaultKind::Transient), 1);
        assert!(probe_objects_left(memory.as_ref()).await.is_empty());

        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        let delete = counted_cleanup.metrics().snapshot().delete;
        assert_eq!(
            (delete.calls, delete.ok),
            (1, 1),
            "one background delete of the absent key, and it succeeds"
        );
    }

    /// Yield until no probe object is left in `store`, at most 100 times, and
    /// return what is left. `MemoryStore` never yields, so a spawned cleanup
    /// delete runs only at these yields; the bound keeps a leak a failure
    /// rather than a hang.
    async fn probe_objects_left_after_cleanup(store: &dyn ObjectStoreBackend) -> Vec<String> {
        let mut left = probe_objects_left(store).await;
        for _ in 0..100 {
            if left.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
            left = probe_objects_left(store).await;
        }
        left
    }

    fn put_lands_then_reports_failure() -> FaultPlan {
        FaultPlan::empty().with_rule(Rule::new(Op::Put, ScriptedFault::DuplicateDelivery))
    }

    /// The guard can fire with no tokio runtime, when another executor drives
    /// the probe. It logs and returns instead of panicking, and the object is
    /// left behind, which is the residual leak [`PROBE_PREFIX`] names.
    #[test]
    fn a_guard_that_fires_outside_a_runtime_does_not_panic() {
        assert!(tokio::runtime::Handle::try_current().is_err());
        let ravel = Arc::new(FaultStore::new(
            MemoryStore::new(),
            put_lands_then_reports_failure(),
        ));
        let cleanup: Arc<dyn ObjectStoreBackend> = ravel.clone();
        let candidate = MemoryStore::new();

        let err = futures::executor::block_on(probe_not_ravel_bucket(
            ravel.as_ref(),
            &cleanup,
            &candidate,
        ))
        .expect_err("a put reported as failed fails the probe");
        let key = match err {
            RavelBucketProbeFailure::ProbeWriteFailed { key, .. } => key,
            other => panic!("expected ProbeWriteFailed, got {other:?}"),
        };
        assert_eq!(ravel.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
        assert!(
            futures::executor::block_on(ravel.inner().head(&key)).is_ok(),
            "with no runtime there is nowhere to spawn the delete"
        );
    }

    /// The shape of `ravel-cli`: the probe runs under `block_on`, its error
    /// propagates out of `main`, and the runtime is dropped with no yield in
    /// between, which cancels any task the probe spawned before it is polled.
    /// A current-thread runtime makes that deterministic: nothing runs a
    /// spawned task except a yield of this thread. So the object is gone only
    /// if the probe deleted it before returning.
    #[test]
    fn a_put_reported_failed_leaves_no_object_when_the_runtime_drops_on_return() {
        let memory = Arc::new(MemoryStore::new());
        let ravel = FaultStore::new(Arc::clone(&memory), put_lands_then_reports_failure());
        let cleanup: Arc<dyn ObjectStoreBackend> = memory.clone();
        let candidate = MemoryStore::new();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("build a current-thread runtime");
        let verdict = runtime.block_on(probe_not_ravel_bucket(&ravel, &cleanup, &candidate));
        drop(runtime);

        let key = match verdict {
            Err(RavelBucketProbeFailure::ProbeWriteFailed { key, .. }) => key,
            other => panic!("expected ProbeWriteFailed, got {other:?}"),
        };
        assert_eq!(ravel.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
        assert!(
            matches!(
                futures::executor::block_on(memory.head(&key)),
                Err(StoreError::NotFound)
            ),
            "the probe object {key} outlived the runtime"
        );
    }

    /// On a clean return the probe deletes its object once, inline, and the
    /// guard spawns nothing: no second delete, and no other object under the
    /// prefix is touched.
    #[tokio::test]
    async fn a_clean_probe_deletes_once_inline_and_spawns_nothing() {
        let memory = Arc::new(MemoryStore::new());
        let sibling = format!("{PROBE_PREFIX}another-probe");
        memory
            .put(&sibling, Bytes::from_static(b"p"), PutOptions::default())
            .await
            .expect("seed a concurrent probe's object");
        let ravel = InstrumentedStore::new(Arc::clone(&memory));
        let counted_cleanup = Arc::new(InstrumentedStore::new(Arc::clone(&memory)));
        let cleanup: Arc<dyn ObjectStoreBackend> = counted_cleanup.clone();

        probe_not_ravel_bucket(&ravel, &cleanup, &MemoryStore::new())
            .await
            .expect("a different bucket must pass");
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }

        let inline = ravel.metrics().snapshot().delete;
        assert_eq!((inline.calls, inline.ok), (1, 1));
        assert_eq!(counted_cleanup.metrics().snapshot().delete.calls, 0);
        assert_eq!(probe_objects_left(memory.as_ref()).await, vec![sibling]);
    }

    /// A probe put that lands and is then reported as failed (a timeout after
    /// the write) returns before the inline delete. The guard deletes it.
    #[tokio::test]
    async fn a_probe_write_that_landed_and_was_reported_failed_is_deleted() {
        let ravel = Arc::new(FaultStore::new(
            MemoryStore::new(),
            put_lands_then_reports_failure(),
        ));
        let cleanup: Arc<dyn ObjectStoreBackend> = ravel.clone();

        let err = probe_not_ravel_bucket(ravel.as_ref(), &cleanup, &MemoryStore::new())
            .await
            .expect_err("a put reported as failed fails the probe");
        let key = match err {
            RavelBucketProbeFailure::ProbeWriteFailed { key, .. } => key,
            other => panic!("expected ProbeWriteFailed, got {other:?}"),
        };
        assert_eq!(ravel.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
        // The spawned delete has not run yet: `MemoryStore` never yields.
        assert_eq!(probe_objects_left(ravel.inner()).await, vec![key.clone()]);

        let left = probe_objects_left_after_cleanup(ravel.inner()).await;
        assert!(left.is_empty(), "the probe object {key} must be deleted");
    }

    /// The probe future dropped while the candidate's identity read is held,
    /// the shape of a caller's deadline expiring mid-probe.
    #[tokio::test]
    async fn a_probe_dropped_while_its_candidate_read_is_held_leaves_no_object() {
        let ravel = Arc::new(MemoryStore::new());
        let cleanup: Arc<dyn ObjectStoreBackend> = ravel.clone();
        let candidate = FaultStore::new(MemoryStore::new(), FaultPlan::empty());
        let gate = candidate.hold(Op::Get, Some(PROBE_PREFIX.to_string()), Occurrence::Always);

        let mut probe = Box::pin(probe_not_ravel_bucket(ravel.as_ref(), &cleanup, &candidate));
        tokio::select! {
            verdict = &mut probe => panic!("a held identity read cannot complete: {verdict:?}"),
            () = gate.wait_until_held(1) => {}
        }
        let held = gate.held_details();
        assert_eq!(held.len(), 1, "{held:?}");
        let (_, held_op, held_key) = &held[0];
        assert_eq!(*held_op, Op::Get);
        assert_eq!(
            probe_objects_left(ravel.as_ref()).await,
            vec![held_key.clone()],
            "the probe object is in Ravel's bucket while its identity read is held"
        );

        drop(probe);
        let left = probe_objects_left_after_cleanup(ravel.as_ref()).await;
        assert!(left.is_empty(), "a dropped probe left {left:?}");
    }

    /// Ravel's own store, with a put whose request reached the bucket and whose
    /// response never comes back: the object lands, then the call hangs.
    struct PutLandsThenHangs {
        inner: Arc<MemoryStore>,
        landed: AtomicBool,
        landed_notify: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for PutLandsThenHangs {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await?;
            self.landed.store(true, Ordering::SeqCst);
            self.landed_notify.notify_one();
            std::future::pending().await
        }
        async fn get(&self, _key: &str, _range: GetRange) -> Result<GetOutcome, StoreError> {
            Err(StoreError::Permanent("unused".into()))
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
        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities::mandatory()
        }
    }

    /// The probe future dropped while its own put is in flight, after the
    /// object landed. This is the case that needs the guard armed before the
    /// put is awaited rather than after it returns.
    #[tokio::test]
    async fn a_probe_dropped_after_its_put_landed_but_before_it_returned_leaves_no_object() {
        let memory = Arc::new(MemoryStore::new());
        let ravel = PutLandsThenHangs {
            inner: Arc::clone(&memory),
            landed: AtomicBool::new(false),
            landed_notify: tokio::sync::Notify::new(),
        };
        let cleanup: Arc<dyn ObjectStoreBackend> = memory.clone();
        let candidate = MemoryStore::new();

        let mut probe = Box::pin(probe_not_ravel_bucket(&ravel, &cleanup, &candidate));
        tokio::select! {
            verdict = &mut probe => panic!("a hanging put cannot complete: {verdict:?}"),
            () = ravel.landed_notify.notified() => {}
        }
        assert!(ravel.landed.load(Ordering::SeqCst));
        assert_eq!(probe_objects_left(memory.as_ref()).await.len(), 1);

        drop(probe);
        let left = probe_objects_left_after_cleanup(memory.as_ref()).await;
        assert!(left.is_empty(), "a dropped probe left {left:?}");
    }

    /// An inline delete that fails leaves the guard armed, and its background
    /// delete removes the object. The verdict is unchanged.
    #[tokio::test]
    async fn an_inline_delete_that_fails_is_retried_by_the_guard() {
        let ravel = Arc::new(FaultStore::new(
            MemoryStore::new(),
            FaultPlan::empty().with_rule(
                Rule::new(
                    Op::Delete,
                    ScriptedFault::Transient("the delete timed out".into()),
                )
                .with_occurrence(Occurrence::Nth(1)),
            ),
        ));
        let cleanup: Arc<dyn ObjectStoreBackend> = ravel.clone();

        probe_not_ravel_bucket(ravel.as_ref(), &cleanup, &MemoryStore::new())
            .await
            .expect("a failed inline delete does not change the verdict");
        assert_eq!(ravel.fault_count(Op::Delete, FaultKind::Transient), 1);
        assert_eq!(
            probe_objects_left(ravel.inner()).await.len(),
            1,
            "the failed inline delete left the object for the guard"
        );

        let left = probe_objects_left_after_cleanup(ravel.inner()).await;
        assert!(left.is_empty(), "the guard's retry left {left:?}");
    }

    /// The background delete goes through `cleanup_store`, never through
    /// `ravel_store`, so a caller counting `ravel_store` does not see it.
    #[tokio::test]
    async fn the_background_delete_goes_through_the_cleanup_store_only() {
        let memory = Arc::new(MemoryStore::new());
        let ravel = InstrumentedStore::new(FaultStore::new(
            Arc::clone(&memory),
            put_lands_then_reports_failure(),
        ));
        let counted_cleanup = Arc::new(InstrumentedStore::new(Arc::clone(&memory)));
        let cleanup: Arc<dyn ObjectStoreBackend> = counted_cleanup.clone();

        let err = probe_not_ravel_bucket(&ravel, &cleanup, &MemoryStore::new())
            .await
            .expect_err("a put reported as failed fails the probe");
        assert!(
            matches!(err, RavelBucketProbeFailure::ProbeWriteFailed { .. }),
            "got {err:?}"
        );
        assert_eq!(
            ravel
                .inner()
                .fault_count(Op::Put, FaultKind::DuplicateDelivery),
            1
        );
        assert_eq!(probe_objects_left(memory.as_ref()).await.len(), 1);

        let left = probe_objects_left_after_cleanup(memory.as_ref()).await;
        assert!(left.is_empty(), "{left:?}");
        assert_eq!(ravel.metrics().snapshot().delete.calls, 0);
        let background = counted_cleanup.metrics().snapshot().delete;
        assert_eq!((background.calls, background.ok), (1, 1));
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
