//! Rotating file-backed AWS credential provider (ADR-0072 decision 1,
//! [`super::S3Config::credentials_file`]).
//!
//! [`FileCredentialProvider`] implements `object_store`'s
//! `CredentialProvider` trait, so `object_store`'s AWS client calls
//! `get_credential` itself, once per signed S3 request, with no caching of
//! its own. That call is the only place this type ever touches the
//! filesystem: no background thread, no timer, no lifecycle beyond the
//! request that triggered it. The common case is one `stat()`: if the
//! file's mtime matches what was last observed, the cached credential is
//! returned (an `Arc` clone). A changed mtime triggers a re-read; on
//! success the cached state is replaced under a write lock (the atomic
//! swap), so every `get_credential` call after the swap observes the new
//! credential while a request that already obtained the old `Arc` keeps it,
//! unaffected, until that request finishes. A read or parse failure never
//! fails the request: it leaves the cached state untouched (so a broken
//! file is not re-parsed on every subsequent call, only on its next actual
//! change), fires a rate-limited warning, and returns the last-good
//! credential.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use object_store::CredentialProvider;
use object_store::aws::AwsCredential;
use parking_lot::RwLock;

use crate::StoreError;
use crate::instrument::{InstantClock, MonotonicClock};

/// Minimum gap between rotation-failure warnings, in [`MonotonicClock`]
/// nanoseconds: 60s, so a credentials file stuck broken logs about once a
/// minute rather than once per request.
const WARNING_INTERVAL_NANOS: u64 = 60_000_000_000;

#[derive(serde::Deserialize)]
struct CredentialsFileContents {
    access_key_id: String,
    secret_access_key: String,
    #[serde(default)]
    session_token: Option<String>,
}

fn read_credentials_file(path: &std::path::Path) -> Result<AwsCredential, String> {
    let bytes = fs::read(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
    let parsed: CredentialsFileContents =
        serde_json::from_slice(&bytes).map_err(|e| format!("parsing {}: {e}", path.display()))?;
    Ok(AwsCredential {
        key_id: parsed.access_key_id,
        secret_key: parsed.secret_access_key,
        token: parsed.session_token,
    })
}

fn file_mtime(path: &std::path::Path) -> Result<SystemTime, String> {
    fs::metadata(path)
        .and_then(|meta| meta.modified())
        .map_err(|e| format!("stat {}: {e}", path.display()))
}

/// Cached credential plus the mtime it (or its last failed re-read attempt)
/// was loaded at.
struct Loaded {
    mtime: SystemTime,
    credential: Arc<AwsCredential>,
}

/// See the [module docs](self) for the rotation contract.
pub(crate) struct FileCredentialProvider {
    path: PathBuf,
    state: RwLock<Loaded>,
    clock: Arc<dyn MonotonicClock>,
    last_warned_nanos: AtomicU64,
    rotation_failures: AtomicU64,
}

impl std::fmt::Debug for Loaded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Loaded")
            .field("mtime", &self.mtime)
            .field("credential", &self.credential)
            .finish()
    }
}

// Manual, not derived: `Arc<dyn MonotonicClock>` has no `Debug` impl (the
// trait carries no such bound), but `object_store::CredentialProvider`
// requires `Self: Debug`, so this exists to satisfy that supertrait without
// forcing one onto `MonotonicClock` itself.
impl std::fmt::Debug for FileCredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileCredentialProvider")
            .field("path", &self.path)
            .field("state", &self.state)
            .field("rotation_failures", &self.rotation_failures)
            .finish()
    }
}

impl FileCredentialProvider {
    /// Reads and parses `path` once, eagerly: an unreadable file or invalid
    /// JSON shape fails here, at construction, per
    /// [`super::S3Config::credentials_file`]'s fail-fast-at-startup
    /// contract. Only a later rotation (inside [`Self::refresh`]) falls
    /// back to last-good on failure.
    pub(crate) fn load(path: PathBuf) -> Result<Self, StoreError> {
        let mtime = file_mtime(&path).map_err(StoreError::Permanent)?;
        let credential = read_credentials_file(&path).map_err(StoreError::Permanent)?;
        Ok(FileCredentialProvider {
            path,
            state: RwLock::new(Loaded {
                mtime,
                credential: Arc::new(credential),
            }),
            clock: Arc::new(InstantClock::new()),
            last_warned_nanos: AtomicU64::new(0),
            rotation_failures: AtomicU64::new(0),
        })
    }

    /// Test seam: a fake [`MonotonicClock`] so a warning-rate-limit
    /// assertion does not depend on wall-clock timing.
    #[cfg(test)]
    pub(crate) fn with_clock(
        path: PathBuf,
        clock: Arc<dyn MonotonicClock>,
    ) -> Result<Self, StoreError> {
        let mut provider = Self::load(path)?;
        provider.clock = clock;
        Ok(provider)
    }

    /// Count of rotation attempts (a request-path mtime change) that failed
    /// to read or parse the file and fell back to last-good. Exposed for
    /// tests; also a legitimate operator signal for a broken rotation
    /// pipeline, so not `#[cfg(test)]`.
    pub(crate) fn rotation_failures(&self) -> u64 {
        self.rotation_failures.load(Ordering::Relaxed)
    }

    /// The mtime-gated re-read and atomic swap described in the module
    /// docs. Never fails: on any error it counts the failure, logs a
    /// rate-limited warning, and returns the last-good credential.
    fn refresh(&self) -> Arc<AwsCredential> {
        let current_mtime = match file_mtime(&self.path) {
            Ok(mtime) => mtime,
            Err(e) => {
                self.warn_rate_limited(&e);
                return Arc::clone(&self.state.read().credential);
            }
        };
        // Fast path: unchanged mtime, no re-read. This is the line a
        // flipped `==`/`!=` breaks -- see the `s3::credentials` tests.
        {
            let guard = self.state.read();
            if guard.mtime == current_mtime {
                return Arc::clone(&guard.credential);
            }
        }
        match read_credentials_file(&self.path) {
            Ok(credential) => {
                let credential = Arc::new(credential);
                let mut guard = self.state.write();
                guard.mtime = current_mtime;
                guard.credential = Arc::clone(&credential);
                credential
            }
            Err(e) => {
                self.warn_rate_limited(&e);
                // Remember this mtime as attempted-and-failed so a file that
                // stays broken at the same mtime is not re-parsed (and
                // re-warned) on every subsequent request; only a further
                // change to the file's mtime retries it. The credential
                // itself is untouched: last-good stands.
                let mut guard = self.state.write();
                guard.mtime = current_mtime;
                Arc::clone(&guard.credential)
            }
        }
    }

    fn warn_rate_limited(&self, message: &str) {
        self.rotation_failures.fetch_add(1, Ordering::Relaxed);
        // `last == 0` is the never-warned sentinel: without it, a provider
        // younger than the interval (clock nanos count from construction)
        // suppresses the very first warning, which is exactly the
        // broken-at-startup case an operator most needs to see. `now` is
        // clamped to 1 so a warning fired at instant zero still leaves the
        // sentinel state.
        let now = self.clock.now_nanos().max(1);
        let last = self.last_warned_nanos.load(Ordering::Relaxed);
        let due = last == 0 || now.saturating_sub(last) >= WARNING_INTERVAL_NANOS;
        if due
            && self
                .last_warned_nanos
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            tracing::warn!(
                path = %self.path.display(),
                error = message,
                "credentials file rotation failed, keeping last-good credentials"
            );
        }
    }
}

#[async_trait::async_trait]
impl CredentialProvider for FileCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        Ok(self.refresh())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::fs::File;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::time::Duration;

    use super::*;

    struct FakeClock(AtomicU64);

    impl MonotonicClock for FakeClock {
        fn now_nanos(&self) -> u64 {
            self.0.load(AtomicOrdering::Relaxed)
        }
    }

    fn write_creds(path: &std::path::Path, key_id: &str, secret: &str, token: Option<&str>) {
        let body = match token {
            Some(t) => format!(
                r#"{{"access_key_id":"{key_id}","secret_access_key":"{secret}","session_token":"{t}"}}"#
            ),
            None => format!(r#"{{"access_key_id":"{key_id}","secret_access_key":"{secret}"}}"#),
        };
        fs::write(path, body).expect("write credentials file");
    }

    /// Advances the file's mtime strictly forward via `File::set_modified`
    /// rather than relying on filesystem mtime granularity between two
    /// close writes, so the rotation test is deterministic.
    fn bump_mtime(path: &std::path::Path, by: Duration) {
        let current = file_mtime(path).expect("stat for bump");
        let file = File::options()
            .write(true)
            .open(path)
            .expect("open for mtime bump");
        file.set_modified(current + by).expect("set_modified");
    }

    /// Rotation test (prove-the-test): creds A load at construction, a
    /// request-path access after rewriting the file AND bumping its mtime
    /// observes creds B, and an `Arc` obtained before that rotation (modeling
    /// an in-flight request that already has its credential) still reads A
    /// afterward -- the swap only changes what a *later* `get_credential`
    /// call returns.
    #[tokio::test]
    async fn rotation_swaps_credentials_on_mtime_change() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("creds.json");
        write_creds(&path, "AKIA_A", "secret_A", None);

        let provider = FileCredentialProvider::load(path.clone()).expect("initial load");

        let in_flight = provider.get_credential().await.expect("creds A");
        assert_eq!(in_flight.key_id, "AKIA_A");

        write_creds(&path, "AKIA_B", "secret_B", Some("token_B"));
        bump_mtime(&path, Duration::from_secs(1));

        let after_rotation = provider.get_credential().await.expect("creds B");
        assert_eq!(after_rotation.key_id, "AKIA_B");
        assert_eq!(after_rotation.secret_key, "secret_B");
        assert_eq!(after_rotation.token.as_deref(), Some("token_B"));

        // The handle obtained before the rewrite still reads A: the swap
        // replaced the provider's cached `Arc`, not the credential value
        // `in_flight` already holds.
        assert_eq!(in_flight.key_id, "AKIA_A");
    }

    /// Invalid-rotation test: corrupting the file after a good load keeps
    /// last-good and counts the failure, without panicking and without
    /// failing the request (`get_credential` still returns `Ok`).
    #[tokio::test]
    async fn corrupt_rotation_keeps_last_good_and_counts_failure() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("creds.json");
        write_creds(&path, "AKIA_GOOD", "secret_good", None);

        let clock = Arc::new(FakeClock(AtomicU64::new(0)));
        let provider =
            FileCredentialProvider::with_clock(path.clone(), clock.clone()).expect("initial load");
        assert_eq!(provider.rotation_failures(), 0);

        fs::write(&path, "not json").expect("corrupt file");
        bump_mtime(&path, Duration::from_secs(1));

        let after_corruption = provider
            .get_credential()
            .await
            .expect("corrupt rotation must not fail the request");
        assert_eq!(after_corruption.key_id, "AKIA_GOOD");
        assert_eq!(provider.rotation_failures(), 1);

        // Same broken mtime again: no re-parse, no second failure count.
        let again = provider.get_credential().await.expect("still last-good");
        assert_eq!(again.key_id, "AKIA_GOOD");
        assert_eq!(provider.rotation_failures(), 1);
    }

    /// Construction-failure test: a missing file at startup is a typed
    /// `StoreError`, never a panic.
    #[test]
    fn missing_file_fails_construction() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("does-not-exist.json");
        let err = FileCredentialProvider::load(path).expect_err("missing file must fail to load");
        assert!(matches!(err, StoreError::Permanent(_)));
    }

    /// Construction-failure test: an existing but invalid-JSON file at
    /// startup also fails construction (fail-fast), unlike the same failure
    /// during rotation.
    #[test]
    fn invalid_json_fails_construction() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("creds.json");
        fs::write(&path, "not json").expect("write invalid file");
        let err = FileCredentialProvider::load(path).expect_err("invalid JSON must fail to load");
        assert!(matches!(err, StoreError::Permanent(_)));
    }
}
