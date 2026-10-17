//! The durable `sys/ipcrypt/policy` marker (ADR-2993 decision 9): a
//! deployment-wide, bucket-root object holding only a fingerprint of the
//! effective IPCrypt policy, never the policy text or any key material.
//!
//! [`check_marker`] is the one entry point every process uses at startup: no
//! marker and an empty local policy proceeds without writing anything; no
//! marker and a non-empty local policy creates it (`CreateIfAbsent`), with a
//! racing loser re-reading and comparing instead of overwriting the winner; a
//! present marker whose fingerprint disagrees with the local one is a
//! refusal naming both fingerprints in hex; any read failure is a refusal.

use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, Version};
use ravel_proto::sys::v1 as sysproto;

use crate::fingerprint::EMPTY_POLICY_FINGERPRINT;

/// The bucket-root marker key (ADR-2993 decision 9). Fixed, never under a
/// tenant prefix.
pub const IPCRYPT_POLICY_KEY: &str = "sys/ipcrypt/policy";

const FORMAT_VERSION: u32 = 1;

/// A decoded, version-checked marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredMarker {
    pub fingerprint: [u8; 32],
    pub format_version: u32,
}

/// Every typed refusal reading or decoding the marker can return.
#[derive(Debug, thiserror::Error)]
pub enum MarkerError {
    #[error("object store error resolving {IPCRYPT_POLICY_KEY}: {0}")]
    Store(#[from] StoreError),
    #[error("{IPCRYPT_POLICY_KEY} is corrupt and could not be decoded: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("{IPCRYPT_POLICY_KEY} has unsupported format_version {got}")]
    UnknownVersion { got: u32 },
    #[error("{IPCRYPT_POLICY_KEY} fingerprint is {got} bytes, expected 32")]
    BadFingerprintLen { got: usize },
}

/// Read and decode the marker. `Ok(None)` means absent (not an error); the
/// [`Version`] is the CAS token a later [`replace_marker`] call must carry as
/// `expected_version`, to guard that write against a marker that changed
/// since this read.
pub async fn read_marker(
    store: &dyn ObjectStoreBackend,
) -> Result<Option<(StoredMarker, Version)>, MarkerError> {
    match store.get(IPCRYPT_POLICY_KEY, GetRange::Full).await {
        Ok(outcome) => {
            let marker = sysproto::IpcryptPolicyMarker::decode(outcome.data.as_ref())?;
            if marker.format_version != FORMAT_VERSION {
                return Err(MarkerError::UnknownVersion {
                    got: marker.format_version,
                });
            }
            if marker.fingerprint.len() != 32 {
                return Err(MarkerError::BadFingerprintLen {
                    got: marker.fingerprint.len(),
                });
            }
            let mut fingerprint = [0u8; 32];
            fingerprint.copy_from_slice(&marker.fingerprint);
            Ok(Some((
                StoredMarker {
                    fingerprint,
                    format_version: marker.format_version,
                },
                outcome.version,
            )))
        }
        Err(StoreError::NotFound) => Ok(None),
        Err(err) => Err(MarkerError::Store(err)),
    }
}

fn encode(fingerprint: [u8; 32]) -> Vec<u8> {
    sysproto::IpcryptPolicyMarker {
        format_version: FORMAT_VERSION,
        fingerprint: fingerprint.to_vec(),
    }
    .encode_to_vec()
}

/// Write the marker once. Returns the raw store error so the caller can
/// distinguish the `AlreadyExists` race from a real failure.
async fn create_marker(
    store: &dyn ObjectStoreBackend,
    fingerprint: [u8; 32],
) -> Result<(), StoreError> {
    store
        .put(
            IPCRYPT_POLICY_KEY,
            encode(fingerprint).into(),
            PutOptions::create_if_absent(),
        )
        .await
        .map(|_| ())
}

/// Swap an existing marker, guarded by `expected_version`: the [`Version`]
/// [`read_marker`] returned for the marker this call means to replace. Not
/// used by [`check_marker`] (which never overwrites a disagreeing marker),
/// but is the primitive an explicit `set-policy` operation (ADR-2993
/// decision 9) needs: read first, then replace with the version just read.
pub async fn replace_marker(
    store: &dyn ObjectStoreBackend,
    fingerprint: [u8; 32],
    expected_version: Version,
) -> Result<(), StoreError> {
    store
        .put(
            IPCRYPT_POLICY_KEY,
            encode(fingerprint).into(),
            PutOptions {
                mode: PutMode::CasVersion(expected_version),
                checksum: None,
            },
        )
        .await
        .map(|_| ())
}

/// What [`check_marker`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckOutcome {
    /// No marker existed and the local policy is empty: nothing was written.
    ProceededNoMarker,
    /// No marker existed; this call created it with the local fingerprint.
    Created,
    /// A marker existed (or a racing creator won) and its fingerprint
    /// matches the local one.
    Matched,
}

/// Every typed refusal [`check_marker`] can return.
#[derive(Debug, thiserror::Error)]
pub enum CheckError {
    #[error("{IPCRYPT_POLICY_KEY} fingerprint mismatch: stored {stored}, local {local}")]
    Mismatch { stored: String, local: String },
    #[error(transparent)]
    Marker(#[from] MarkerError),
    #[error("could not create {IPCRYPT_POLICY_KEY}: {0}")]
    Create(StoreError),
}

/// Reconcile the local policy's fingerprint against the durable marker
/// (ADR-2993 decision 9), per the state machine documented on the module.
pub async fn check_marker(
    store: &dyn ObjectStoreBackend,
    local_fingerprint: [u8; 32],
) -> Result<CheckOutcome, CheckError> {
    match read_marker(store).await? {
        Some((stored, _version)) => {
            if stored.fingerprint == local_fingerprint {
                Ok(CheckOutcome::Matched)
            } else {
                Err(CheckError::Mismatch {
                    stored: hex::encode(stored.fingerprint),
                    local: hex::encode(local_fingerprint),
                })
            }
        }
        None => {
            if local_fingerprint == EMPTY_POLICY_FINGERPRINT {
                return Ok(CheckOutcome::ProceededNoMarker);
            }
            match create_marker(store, local_fingerprint).await {
                Ok(()) => Ok(CheckOutcome::Created),
                Err(StoreError::AlreadyExists) => {
                    // Lost the create race: re-read and compare against
                    // whichever fingerprint the winner wrote, rather than
                    // retrying the write.
                    match read_marker(store).await? {
                        Some((stored, _version)) if stored.fingerprint == local_fingerprint => {
                            Ok(CheckOutcome::Matched)
                        }
                        Some((stored, _version)) => Err(CheckError::Mismatch {
                            stored: hex::encode(stored.fingerprint),
                            local: hex::encode(local_fingerprint),
                        }),
                        None => Err(CheckError::Marker(MarkerError::Store(
                            StoreError::AlreadyExists,
                        ))),
                    }
                }
                Err(err) => Err(CheckError::Create(err)),
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;

    #[tokio::test]
    async fn matching_marker_matches() {
        let store = MemoryStore::new();
        let fp = [3u8; 32];
        create_marker(&store, fp).await.expect("create");
        let outcome = check_marker(&store, fp).await.expect("ok");
        assert_eq!(outcome, CheckOutcome::Matched);
    }

    #[tokio::test]
    async fn mismatched_marker_refuses_naming_both_fingerprints() {
        let store = MemoryStore::new();
        let stored_fp = [1u8; 32];
        let local_fp = [2u8; 32];
        create_marker(&store, stored_fp).await.expect("create");
        let err = check_marker(&store, local_fp).await.unwrap_err();
        match err {
            CheckError::Mismatch { stored, local } => {
                assert_eq!(stored, hex::encode(stored_fp));
                assert_eq!(local, hex::encode(local_fp));
            }
            other => unreachable!("expected Mismatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn empty_local_against_present_marker_is_a_mismatch_not_a_pass() {
        // Even the documented empty-policy sentinel must agree with a
        // present marker: a deployment that once had policy and removed it
        // still needs an explicit re-key of the marker, not a silent pass.
        let store = MemoryStore::new();
        let stored_fp = [9u8; 32];
        create_marker(&store, stored_fp).await.expect("create");
        let err = check_marker(&store, EMPTY_POLICY_FINGERPRINT)
            .await
            .unwrap_err();
        assert!(matches!(err, CheckError::Mismatch { .. }));
    }
}
