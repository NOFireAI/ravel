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

/// Prefix for the objects [`probe_not_ravel_bucket`] writes. Everything under
/// it is transient: the probe deletes its object before returning, on every
/// path.
pub const PROBE_PREFIX: &str = "sys/pq-probe/";

/// An ETag no store issues, used as the wrong half of the precondition probe.
/// Quoted because S3 ETags are quoted strings and an unquoted value could be
/// rejected as malformed rather than evaluated as a precondition, which would
/// make the probe pass for the wrong reason.
const WRONG_ETAG: &str = "\"0\"";

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
    #[error("the probe key {key:?} could not be HEADed, so preconditions were never tested: {source}")]
    Head { key: String, source: StoreError },
    /// Matching half: a read carrying the object's own ETag was refused.
    #[error("a ranged read of {key:?} pinned to the object's own ETag was refused: {source}")]
    MatchingPinRefused { key: String, source: StoreError },
    /// Refusing half: a read carrying a wrong ETag was served anyway, so the
    /// store ignores the precondition. A pinned read against it would return
    /// bytes from whatever version happens to be there.
    #[error("a ranged read of {key:?} pinned to a wrong ETag was served: the store ignores read preconditions")]
    WrongPinAccepted { key: String },
    /// Refusing half: the read was refused, but not as a precondition failure.
    /// Not a pass: the caller distinguishes `PreconditionFailed` from every
    /// other error, so a store that reports something else cannot be read
    /// through even though it did refuse.
    #[error("a ranged read of {key:?} pinned to a wrong ETag failed with {source} instead of a precondition failure")]
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
/// ranged reads, one carrying that identity and one carrying [`WRONG_ETAG`].
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
    let meta = store
        .head(key)
        .await
        .map_err(|source| PreconditionProbeFailure::Head {
            key: key.to_string(),
            source,
        })?;
    // The version half of the pin is only asserted when the store reports one
    // distinct from the ETag: sending the ETag back as a `versionId` would fail
    // for a reason that says nothing about precondition support.
    let version = (meta.version.0 != meta.etag.0).then(|| meta.version.0.clone());
    let matching = Pin {
        etag: meta.etag.0.clone(),
        version: version.clone(),
    };

    store
        .get_pinned(key, GetRange::Range(0, 1), &matching)
        .await
        .map_err(|source| PreconditionProbeFailure::MatchingPinRefused {
            key: key.to_string(),
            source,
        })?;

    let wrong = Pin {
        etag: WRONG_ETAG.to_string(),
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

/// Refuse a candidate bucket that is Ravel's own bucket reached under another
/// name.
///
/// Writes an object with a random key under [`PROBE_PREFIX`] and random
/// contents to `ravel_store`, then reads that key from `candidate_store`. The
/// key is random so no candidate can hold it by coincidence, and the contents
/// are random so a store that answers every key with the same placeholder
/// cannot be mistaken for Ravel's own.
///
/// The probe object is deleted before returning, on every path including the
/// error paths. A delete that itself fails is logged and does not change the
/// verdict: the verdict is the answer the caller asked for, and losing it to
/// report a leaked probe object would be the worse trade.
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
        // Ravel's object.
        Err(StoreError::NotFound) => Ok(()),
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
