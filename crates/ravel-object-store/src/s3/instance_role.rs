//! EC2 IAM instance-role credential provider (ADR-0106,
//! [`super::S3Config::auth`] = [`super::S3AuthMode::InstanceRole`]).
//!
//! [`InstanceRoleCredentialProvider`] implements `object_store`'s
//! `CredentialProvider` trait, the same seam
//! [`super::credentials::FileCredentialProvider`] installs through
//! `with_credentials`, so `object_store`'s AWS client calls `get_credential`
//! itself, once per signed S3 request. Credentials come from the EC2 instance
//! metadata service over IMDSv2 on the link-local address (default
//! `http://169.254.169.254`, overridable via
//! [`super::S3Config::instance_metadata_endpoint`] so tests can point at a
//! mock):
//!
//! 1. `PUT /latest/api/token` with `X-aws-ec2-metadata-token-ttl-seconds` to
//!    mint a session token.
//! 2. `GET /latest/meta-data/iam/security-credentials/` for the attached
//!    role name.
//! 3. `GET /latest/meta-data/iam/security-credentials/<role>` for the role
//!    document (`AccessKeyId`, `SecretAccessKey`, `Token`, `Expiration`).
//!
//! IMDSv2 only: every call carries the token header, and any non-success
//! (including a 403 from a hop-limited or disabled IMDS) is a typed error,
//! never a downgrade to the token-less IMDSv1 flow.
//!
//! The first fetch happens eagerly at construction, so a process misconfigured
//! for EC2 fails at startup with a typed [`StoreError`] rather than on its
//! first S3 request. After that the credential is cached and refreshed on the
//! request path when within [`REFRESH_MARGIN_NANOS`] of its `Expiration`. A
//! transient refresh failure keeps serving the cached credential while it is
//! still unexpired; once the cached credential has actually expired,
//! `get_credential` fails typed, increments [`Self::refresh_failures`], and
//! logs a warning rate-limited to one per [`WARNING_INTERVAL_NANOS`]. Serving
//! an expired credential silently would turn one IMDS outage into a stream of
//! confusing S3 403s. Credentials live only in memory: the manual `Debug` impl
//! redacts them, and nothing here writes them to disk or logs.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use object_store::CredentialProvider;
use object_store::aws::AwsCredential;
use parking_lot::RwLock;

use crate::StoreError;

/// The AWS link-local IMDS address, used when
/// [`super::S3Config::instance_metadata_endpoint`] is `None`.
pub(crate) const DEFAULT_IMDS_ENDPOINT: &str = "http://169.254.169.254";

/// TTL requested for the IMDSv2 session token, in seconds. AWS's maximum
/// (6 hours); the token is minted fresh on every fetch, so a long TTL only
/// avoids a token expiring mid-fetch and never outlives a process.
const IMDS_TOKEN_TTL_SECONDS: u32 = 21_600;

/// Connect timeout for every IMDS HTTP call. IMDS answers on the link-local
/// address in well under a second on a healthy instance; a short bound keeps
/// construction from hanging when the address is unreachable (not on EC2, or a
/// blocked hop).
const IMDS_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

/// Total (connect + response) timeout for every IMDS HTTP call. Bounds each of
/// the three calls independently, so construction can hang for at most a small
/// multiple of this regardless of how IMDS misbehaves.
const IMDS_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// Refresh the cached credential once the wall clock comes within this many
/// nanoseconds of its `Expiration`: 5 minutes. Wide enough that a transient
/// IMDS blip inside the window still leaves the current credential valid while
/// retries continue.
const REFRESH_MARGIN_NANOS: i64 = 5 * 60 * 1_000_000_000;

/// Minimum gap between expired-credential warnings, in wall-clock nanoseconds:
/// 60s, so an IMDS outage after expiry logs about once a minute rather than
/// once per S3 request.
const WARNING_INTERVAL_NANOS: i64 = 60 * 1_000_000_000;

/// Wall-clock time seam, in nanoseconds since the Unix epoch. Injected so the
/// expiry math against the IMDS `Expiration` (an absolute timestamp) and the
/// warning rate limit are deterministic in tests, mirroring how
/// [`crate::instrument::MonotonicClock`] is injected into
/// [`super::credentials::FileCredentialProvider`]. That sibling seam is
/// monotonic-elapsed only and cannot interpret an absolute `Expiration`, which
/// is why this one reports wall-clock time instead.
pub(crate) trait WallClock: Send + Sync + 'static {
    /// Nanoseconds since the Unix epoch. May be non-monotonic across a
    /// wall-clock adjustment; a backward jump can only delay a refresh or a
    /// warning, never serve an expired credential (the comparison is absolute).
    fn now_unix_nanos(&self) -> i64;
}

/// Default clock: `SystemTime::now()`. This is the one sanctioned
/// `SystemTime::now()` call for this provider, isolated behind the seam
/// exactly as [`crate::instrument::InstantClock`] isolates `Instant::now()`.
#[derive(Debug, Default)]
pub(crate) struct SystemTimeClock;

impl WallClock for SystemTimeClock {
    fn now_unix_nanos(&self) -> i64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
            // Before the epoch: only reachable with a wildly misset clock.
            // Clamp rather than panic in a credential path.
            Err(_) => 0,
        }
    }
}

/// A credential fetched from IMDS, with its `Expiration` resolved to Unix
/// nanoseconds for the cache's expiry math.
struct Fetched {
    credential: AwsCredential,
    expiration_unix_nanos: i64,
}

/// The cached credential plus the absolute instant it stops being valid.
struct Cached {
    credential: Arc<AwsCredential>,
    expiration_unix_nanos: i64,
}

impl std::fmt::Debug for Cached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the credential itself; only its expiry is safe to show.
        f.debug_struct("Cached")
            .field("expiration_unix_nanos", &self.expiration_unix_nanos)
            .finish_non_exhaustive()
    }
}

/// See the [module docs](self) for the fetch, cache, and refresh contract.
pub(crate) struct InstanceRoleCredentialProvider {
    endpoint: String,
    state: RwLock<Cached>,
    /// Serializes refresh attempts so a burst of request-path calls inside the
    /// refresh window issues one IMDS fetch, not one per call.
    refresh_lock: tokio::sync::Mutex<()>,
    clock: Arc<dyn WallClock>,
    last_warned_nanos: AtomicI64,
    refresh_failures: AtomicU64,
}

// Manual, not derived: `Arc<dyn WallClock>` has no `Debug` impl and the cached
// credential must never be printed, but `object_store::CredentialProvider`
// requires `Self: Debug`. This satisfies that supertrait while redacting the
// secret material.
impl std::fmt::Debug for InstanceRoleCredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstanceRoleCredentialProvider")
            .field("endpoint", &self.endpoint)
            .field("state", &self.state)
            .field(
                "refresh_failures",
                &self.refresh_failures.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl InstanceRoleCredentialProvider {
    /// Fetch the role credential once, eagerly, with the default wall clock: a
    /// misconfigured instance (IMDS unreachable, no role attached, a 403) fails
    /// here, at construction, with a typed [`StoreError`] (fail fast at
    /// startup, never a panic), per ADR-0106.
    pub(crate) fn load(endpoint: String) -> Result<Self, StoreError> {
        Self::with_clock(endpoint, Arc::new(SystemTimeClock))
    }

    /// [`Self::load`] with an injected [`WallClock`], for deterministic expiry
    /// and rate-limit tests.
    pub(crate) fn with_clock(
        endpoint: String,
        clock: Arc<dyn WallClock>,
    ) -> Result<Self, StoreError> {
        let fetched = fetch_blocking(&endpoint).map_err(|e| {
            StoreError::Permanent(format!("IMDS instance-role credential fetch failed: {e}"))
        })?;
        Ok(InstanceRoleCredentialProvider {
            endpoint,
            state: RwLock::new(Cached {
                credential: Arc::new(fetched.credential),
                expiration_unix_nanos: fetched.expiration_unix_nanos,
            }),
            refresh_lock: tokio::sync::Mutex::new(()),
            clock,
            last_warned_nanos: AtomicI64::new(0),
            refresh_failures: AtomicU64::new(0),
        })
    }

    /// Count of request-path refreshes that failed *and* found the cached
    /// credential already expired, so the S3 request had to fail. A transient
    /// failure that still served an unexpired last-good credential does not
    /// count. Exposed for observability (mirrors
    /// [`super::credentials::FileCredentialProvider::rotation_failures`]); wired
    /// through [`super::S3Store::credential_refresh_failures`].
    pub(crate) fn refresh_failures(&self) -> u64 {
        self.refresh_failures.load(Ordering::Relaxed)
    }

    /// Refresh the credential from IMDS and swap it into the cache. Serialized
    /// by [`Self::refresh_lock`]; the caller re-checks the cache after
    /// acquiring the lock so a credential another task already refreshed is not
    /// fetched again.
    async fn refresh(&self) -> Result<Arc<AwsCredential>, FetchError> {
        let fetched = fetch_once(&self.endpoint).await?;
        let credential = Arc::new(fetched.credential);
        let mut guard = self.state.write();
        guard.credential = Arc::clone(&credential);
        guard.expiration_unix_nanos = fetched.expiration_unix_nanos;
        Ok(credential)
    }

    fn warn_rate_limited(&self, message: &str) {
        // Counted before the rate-limit gate: every expired-and-failed refresh
        // increments the observability counter, even when its warning is
        // suppressed as a duplicate.
        self.refresh_failures.fetch_add(1, Ordering::Relaxed);
        let now = self.clock.now_unix_nanos().max(1);
        let last = self.last_warned_nanos.load(Ordering::Relaxed);
        // `last == 0` is the never-warned sentinel so the very first expiry
        // (the case an operator most needs) is never suppressed.
        let due = last == 0 || now.saturating_sub(last) >= WARNING_INTERVAL_NANOS;
        if due
            && self
                .last_warned_nanos
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            tracing::warn!(
                endpoint = %self.endpoint,
                error = message,
                "IMDS instance-role credential expired and refresh failed"
            );
        }
    }
}

#[async_trait::async_trait]
impl CredentialProvider for InstanceRoleCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let now = self.clock.now_unix_nanos();
        // Fast path: comfortably before the refresh margin, serve the cache
        // with no network call. A parking_lot guard is never held across an
        // await.
        {
            let guard = self.state.read();
            if now < guard.expiration_unix_nanos - REFRESH_MARGIN_NANOS {
                return Ok(Arc::clone(&guard.credential));
            }
        }

        // Inside the margin (or already expired): refresh under the lock, but
        // re-check first in case another task refreshed while we waited.
        let _refresh_guard = self.refresh_lock.lock().await;
        let now = self.clock.now_unix_nanos();
        {
            let guard = self.state.read();
            if now < guard.expiration_unix_nanos - REFRESH_MARGIN_NANOS {
                return Ok(Arc::clone(&guard.credential));
            }
        }

        match self.refresh().await {
            Ok(credential) => Ok(credential),
            Err(e) => {
                // A transient failure keeps serving the cached credential while
                // it is still valid; only an actually-expired credential fails
                // the request (and counts). Re-read the clock: a refresh runs
                // three bounded IMDS calls and can take seconds, so a credential
                // that was unexpired at the pre-refresh `now` may have crossed
                // its Expiration by the time the refresh failed. Deciding on the
                // stale `now` would serve it once, uncounted and unlogged.
                let now = self.clock.now_unix_nanos();
                let (credential, expiration) = {
                    let guard = self.state.read();
                    (Arc::clone(&guard.credential), guard.expiration_unix_nanos)
                };
                if now < expiration {
                    Ok(credential)
                } else {
                    self.warn_rate_limited(&e.to_string());
                    Err(object_store::Error::Generic {
                        store: "InstanceRoleCredentialProvider",
                        source: format!(
                            "instance-role credential expired and IMDS refresh failed: {e}"
                        )
                        .into(),
                    })
                }
            }
        }
    }
}

/// Run the eager construction fetch to completion on a throwaway
/// current-thread runtime on its own thread. `super::S3Store::builder` (and so
/// `new`) is synchronous and may itself be called from inside an async
/// runtime; owning the runtime on a separate thread keeps this fetch bounded
/// and safe to call from either context without touching the caller's runtime.
fn fetch_blocking(endpoint: &str) -> Result<Fetched, FetchError> {
    std::thread::scope(|scope| {
        // spawn_scoped, not scope.spawn: the latter panics if the OS refuses
        // the thread (EAGAIN under a thread-count or memory limit), which would
        // panic through this synchronous constructor. Map that failure to a
        // typed error like the runtime-build failure inside the thread already
        // is.
        let handle = std::thread::Builder::new()
            .spawn_scoped(scope, || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| FetchError::Client(e.to_string()))?;
                runtime.block_on(fetch_once(endpoint))
            })
            .map_err(|e| FetchError::Client(e.to_string()))?;
        handle.join().unwrap_or(Err(FetchError::Panicked))
    })
}

/// The three IMDSv2 calls. Builds a fresh bounded client per fetch so the
/// client always belongs to the runtime driving this call (the throwaway
/// runtime at construction, the process runtime on the request path) and never
/// outlives it. Fetches are rare (construction, then near expiry), so the
/// per-fetch client cost is immaterial.
async fn fetch_once(endpoint: &str) -> Result<Fetched, FetchError> {
    let client = reqwest::Client::builder()
        .connect_timeout(IMDS_CONNECT_TIMEOUT)
        .timeout(IMDS_REQUEST_TIMEOUT)
        .build()
        .map_err(|e| FetchError::Client(e.to_string()))?;

    let base = endpoint.trim_end_matches('/');

    // 1. Mint the IMDSv2 session token. A non-success here (403 on a disabled
    //    or hop-limited IMDS) is an error, never a fall-through to IMDSv1.
    let token = client
        .put(format!("{base}/latest/api/token"))
        .header(
            "X-aws-ec2-metadata-token-ttl-seconds",
            IMDS_TOKEN_TTL_SECONDS.to_string(),
        )
        .send()
        .await
        .map_err(|e| FetchError::Http(e.to_string()))?
        .error_for_status()
        .map_err(|e| FetchError::Http(e.to_string()))?
        .text()
        .await
        .map_err(|e| FetchError::Http(e.to_string()))?;

    // 2. The attached role name.
    let roles = client
        .get(format!("{base}/latest/meta-data/iam/security-credentials/"))
        .header("X-aws-ec2-metadata-token", &token)
        .send()
        .await
        .map_err(|e| FetchError::Http(e.to_string()))?
        .error_for_status()
        .map_err(|e| FetchError::Http(e.to_string()))?
        .text()
        .await
        .map_err(|e| FetchError::Http(e.to_string()))?;
    let role = roles
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .ok_or(FetchError::NoRole)?;

    // 3. The role credential document.
    let body = client
        .get(format!(
            "{base}/latest/meta-data/iam/security-credentials/{role}"
        ))
        .header("X-aws-ec2-metadata-token", &token)
        .send()
        .await
        .map_err(|e| FetchError::Http(e.to_string()))?
        .error_for_status()
        .map_err(|e| FetchError::Http(e.to_string()))?
        .text()
        .await
        .map_err(|e| FetchError::Http(e.to_string()))?;

    parse_role_document(&body)
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RoleDocument {
    access_key_id: String,
    secret_access_key: String,
    token: String,
    expiration: String,
}

fn parse_role_document(body: &str) -> Result<Fetched, FetchError> {
    let doc: RoleDocument =
        serde_json::from_str(body).map_err(|e| FetchError::Parse(e.to_string()))?;
    let expiration_unix_nanos = parse_imds_expiration(&doc.expiration)?;
    Ok(Fetched {
        credential: AwsCredential {
            key_id: doc.access_key_id,
            secret_key: doc.secret_access_key,
            token: Some(doc.token),
        },
        expiration_unix_nanos,
    })
}

/// Parse an IMDS `Expiration` (`YYYY-MM-DDTHH:MM:SSZ`, always UTC, an optional
/// fractional-second part tolerated and truncated) to Unix nanoseconds.
/// Returns a typed error, never a panic, on any malformation, so a corrupt
/// document fails the fetch cleanly.
fn parse_imds_expiration(value: &str) -> Result<i64, FetchError> {
    let bad = || FetchError::BadExpiration(value.to_string());

    let rest = value.strip_suffix('Z').ok_or_else(bad)?;
    let (date, time) = rest.split_once('T').ok_or_else(bad)?;

    let mut date_parts = date.split('-');
    let year: i64 = date_parts
        .next()
        .ok_or_else(bad)?
        .parse()
        .map_err(|_| bad())?;
    let month: i64 = date_parts
        .next()
        .ok_or_else(bad)?
        .parse()
        .map_err(|_| bad())?;
    let day: i64 = date_parts
        .next()
        .ok_or_else(bad)?
        .parse()
        .map_err(|_| bad())?;
    // Bound the year before the era math: days_from_civil multiplies the era
    // by 146_097, which overflows i64 for a year past ~2.5e16 (a debug-build
    // panic, a release-build wrap that can slip past the later checked_mul).
    // No real IMDS emits such a value; a hostile or mock endpoint can. Four
    // digits covers every Expiration IMDS can carry.
    if date_parts.next().is_some()
        || !(1..=9999).contains(&year)
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
    {
        return Err(bad());
    }

    // Drop any fractional-second part before splitting the clock fields.
    let clock = time.split('.').next().ok_or_else(bad)?;
    let mut time_parts = clock.split(':');
    let hour: i64 = time_parts
        .next()
        .ok_or_else(bad)?
        .parse()
        .map_err(|_| bad())?;
    let minute: i64 = time_parts
        .next()
        .ok_or_else(bad)?
        .parse()
        .map_err(|_| bad())?;
    let second: i64 = time_parts
        .next()
        .ok_or_else(bad)?
        .parse()
        .map_err(|_| bad())?;
    if time_parts.next().is_some()
        || !(0..24).contains(&hour)
        || !(0..60).contains(&minute)
        || !(0..=60).contains(&second)
    {
        return Err(bad());
    }

    let days = days_from_civil(year, month, day);
    let secs = days
        .checked_mul(86_400)
        .and_then(|d| d.checked_add(hour * 3600 + minute * 60 + second))
        .ok_or_else(bad)?;
    secs.checked_mul(1_000_000_000).ok_or_else(bad)
}

/// Days since the Unix epoch (1970-01-01) for a proleptic-Gregorian civil date,
/// Howard Hinnant's `days_from_civil` algorithm. Exact for all dates an IMDS
/// `Expiration` can carry.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Every way an IMDS fetch can fail. `String`-only payloads keep it
/// `Send + Sync` (so it can source an `object_store::Error`) without holding a
/// `reqwest::Error`.
#[derive(Debug)]
enum FetchError {
    /// Building the HTTP client or its runtime failed.
    Client(String),
    /// A request failed, timed out, or returned a non-success status
    /// (including 403 — never downgraded to IMDSv1).
    Http(String),
    /// The construction fetch thread panicked.
    Panicked,
    /// The role listing was empty (no role attached to the instance).
    NoRole,
    /// The role document was not the expected JSON shape.
    Parse(String),
    /// The `Expiration` field was not a parseable UTC timestamp.
    BadExpiration(String),
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FetchError::Client(e) => write!(f, "building IMDS HTTP client: {e}"),
            FetchError::Http(e) => write!(f, "IMDS request failed: {e}"),
            FetchError::Panicked => write!(f, "IMDS fetch thread panicked"),
            FetchError::NoRole => write!(f, "no IAM role attached to this instance"),
            FetchError::Parse(e) => write!(f, "parsing IMDS credential document: {e}"),
            FetchError::BadExpiration(v) => write!(f, "unparseable IMDS Expiration {v:?}"),
        }
    }
}

impl std::error::Error for FetchError {}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering as O};

    use axum::Router;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::{get, put};
    use parking_lot::Mutex;

    use super::*;

    /// A settable wall clock for deterministic expiry/rate-limit tests.
    struct FakeClock(AtomicI64);

    impl FakeClock {
        fn new(now: i64) -> Arc<Self> {
            Arc::new(FakeClock(AtomicI64::new(now)))
        }
        fn set(&self, now: i64) {
            self.0.store(now, O::Relaxed);
        }
    }

    impl WallClock for FakeClock {
        fn now_unix_nanos(&self) -> i64 {
            self.0.load(O::Relaxed)
        }
    }

    /// A clock that reports `before` until the mock IMDS has served its second
    /// credential-document GET (one at construction, one for the request-path
    /// refresh), then `after`. Models a credential whose `Expiration` crosses
    /// during the refresh's network window: unexpired when `get_credential`
    /// samples the clock before refreshing, expired by the time the refresh has
    /// run and failed.
    struct RefreshCrossingClock {
        imds: Arc<ImdsState>,
        before: i64,
        after: i64,
    }

    impl WallClock for RefreshCrossingClock {
        fn now_unix_nanos(&self) -> i64 {
            if self.imds.doc_requests.load(O::Relaxed) >= 2 {
                self.after
            } else {
                self.before
            }
        }
    }

    /// Mock IMDS state. Every knob a test flips lives here so one endpoint
    /// covers the happy path, refresh, transient failure, 403, and hang.
    struct ImdsState {
        /// HTTP status the token PUT returns (200 by default).
        token_status: AtomicU16,
        /// HTTP status the credential-document GET returns (200 by default).
        doc_status: AtomicU16,
        /// The JSON credential document served, swappable to model rotation.
        doc: Mutex<String>,
        /// Count of credential-document GETs, so a test can prove a refresh
        /// actually hit IMDS rather than serving the cache.
        doc_requests: AtomicUsize,
        /// When set, the token PUT blocks indefinitely (models a hung IMDS).
        hang: std::sync::atomic::AtomicBool,
    }

    impl ImdsState {
        fn new(doc: String) -> Arc<Self> {
            Arc::new(ImdsState {
                token_status: AtomicU16::new(200),
                doc_status: AtomicU16::new(200),
                doc: Mutex::new(doc),
                doc_requests: AtomicUsize::new(0),
                hang: std::sync::atomic::AtomicBool::new(false),
            })
        }
    }

    fn role_doc(key_id: &str, secret: &str, token: &str, expiration: &str) -> String {
        format!(
            r#"{{"Code":"Success","AccessKeyId":"{key_id}","SecretAccessKey":"{secret}",
                "Token":"{token}","Expiration":"{expiration}"}}"#
        )
    }

    async fn token_handler(State(state): State<Arc<ImdsState>>) -> impl IntoResponse {
        if state.hang.load(O::Relaxed) {
            // Longer than any test's own timeout; the client's request timeout
            // is what must fire, proving the bound is client-side.
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
        let status = StatusCode::from_u16(state.token_status.load(O::Relaxed)).expect("status");
        (status, "AQAE-mock-imds-token")
    }

    async fn role_handler() -> impl IntoResponse {
        (StatusCode::OK, "ravel-instance-role")
    }

    async fn doc_handler(State(state): State<Arc<ImdsState>>) -> impl IntoResponse {
        state.doc_requests.fetch_add(1, O::Relaxed);
        let status = StatusCode::from_u16(state.doc_status.load(O::Relaxed)).expect("status");
        (status, state.doc.lock().clone())
    }

    /// Bind an ephemeral loopback port and serve the mock IMDS until the test
    /// runtime shuts down. Returns the `http://addr` base for
    /// `instance_metadata_endpoint`.
    async fn spawn_imds(state: Arc<ImdsState>) -> String {
        let app = Router::new()
            .route("/latest/api/token", put(token_handler))
            .route(
                "/latest/meta-data/iam/security-credentials/",
                get(role_handler),
            )
            .route(
                "/latest/meta-data/iam/security-credentials/{role}",
                get(doc_handler),
            )
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr: SocketAddr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    /// Construct the provider off the runtime's worker threads: `with_clock` is
    /// synchronous and blocks on its own runtime, so running it inline on a
    /// current-thread test runtime would starve the mock server task.
    async fn build_provider(
        endpoint: String,
        clock: Arc<FakeClock>,
    ) -> Result<InstanceRoleCredentialProvider, StoreError> {
        tokio::task::spawn_blocking(move || {
            InstanceRoleCredentialProvider::with_clock(endpoint, clock)
        })
        .await
        .expect("join")
    }

    // A fixed absolute expiry, parsed through the crate's own parser so the
    // FakeClock and the served `Expiration` stay consistent without an inverse
    // formatter.
    const EXP_A: &str = "2033-11-14T22:13:20Z";
    const EXP_B: &str = "2033-11-14T23:13:20Z";
    const ONE_HOUR_NANOS: i64 = 3600 * 1_000_000_000;

    #[tokio::test(flavor = "multi_thread")]
    async fn fetch_serves_imdsv2_role_document() {
        let state = ImdsState::new(role_doc("AKIA_ROLE", "role-secret", "role-token", EXP_A));
        let endpoint = spawn_imds(Arc::clone(&state)).await;
        let exp_a = parse_imds_expiration(EXP_A).expect("exp");

        // Well before the refresh margin so get_credential serves the cache.
        let clock = FakeClock::new(exp_a - ONE_HOUR_NANOS);
        let provider = build_provider(endpoint, Arc::clone(&clock))
            .await
            .expect("eager fetch must succeed");

        let credential = provider.get_credential().await.expect("cached credential");
        assert_eq!(credential.key_id, "AKIA_ROLE");
        assert_eq!(credential.secret_key, "role-secret");
        assert_eq!(credential.token.as_deref(), Some("role-token"));
        // One document GET at construction; the cache read did not hit IMDS.
        assert_eq!(state.doc_requests.load(O::Relaxed), 1);
        assert_eq!(provider.refresh_failures(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn refreshes_before_expiry_via_injected_clock() {
        let state = ImdsState::new(role_doc("AKIA_A", "secret-a", "token-a", EXP_A));
        let endpoint = spawn_imds(Arc::clone(&state)).await;
        let exp_a = parse_imds_expiration(EXP_A).expect("exp");

        let clock = FakeClock::new(exp_a - ONE_HOUR_NANOS);
        let provider = build_provider(endpoint, Arc::clone(&clock))
            .await
            .expect("eager fetch");

        // Fast path: cached A, no new document GET beyond construction's one.
        let a = provider.get_credential().await.expect("cached A");
        assert_eq!(a.key_id, "AKIA_A");
        assert_eq!(state.doc_requests.load(O::Relaxed), 1);

        // Rotate the served document, then move the clock into the 5-minute
        // margin (but before expiry): the next call must refresh.
        *state.doc.lock() = role_doc("AKIA_B", "secret-b", "token-b", EXP_B);
        clock.set(exp_a - 60 * 1_000_000_000); // 1 min before expiry -> in margin

        let b = provider.get_credential().await.expect("refreshed B");
        assert_eq!(b.key_id, "AKIA_B", "must serve the refreshed credential");
        assert_eq!(b.token.as_deref(), Some("token-b"));
        assert_eq!(
            state.doc_requests.load(O::Relaxed),
            2,
            "the in-margin call must have hit IMDS exactly once more"
        );
        assert_eq!(provider.refresh_failures(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn last_good_served_within_validity_on_transient_failure() {
        let state = ImdsState::new(role_doc("AKIA_GOOD", "good-secret", "good-token", EXP_A));
        let endpoint = spawn_imds(Arc::clone(&state)).await;
        let exp_a = parse_imds_expiration(EXP_A).expect("exp");

        let clock = FakeClock::new(exp_a - ONE_HOUR_NANOS);
        let provider = build_provider(endpoint, Arc::clone(&clock))
            .await
            .expect("eager fetch");

        // Enter the refresh margin, but make refresh fail: still unexpired, so
        // the cached credential must be served and nothing counted.
        state.doc_status.store(500, O::Relaxed);
        clock.set(exp_a - 60 * 1_000_000_000);

        let served = provider
            .get_credential()
            .await
            .expect("last-good must be served while still valid");
        assert_eq!(served.key_id, "AKIA_GOOD");
        assert!(
            state.doc_requests.load(O::Relaxed) >= 2,
            "a refresh was attempted"
        );
        assert_eq!(
            provider.refresh_failures(),
            0,
            "a transient failure with a still-valid credential must not count"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn expired_credential_fails_typed_and_counts_failure() {
        let state = ImdsState::new(role_doc("AKIA_OLD", "old-secret", "old-token", EXP_A));
        let endpoint = spawn_imds(Arc::clone(&state)).await;
        let exp_a = parse_imds_expiration(EXP_A).expect("exp");

        let clock = FakeClock::new(exp_a - ONE_HOUR_NANOS);
        let provider = build_provider(endpoint, Arc::clone(&clock))
            .await
            .expect("eager fetch");

        // Past expiry and refresh broken: the request must fail typed, count
        // exactly one failure, and log (rate-limited).
        state.doc_status.store(500, O::Relaxed);
        clock.set(exp_a + 1);

        let err = provider
            .get_credential()
            .await
            .expect_err("an expired credential with a broken refresh must fail");
        assert!(
            matches!(err, object_store::Error::Generic { .. }),
            "{err:?}"
        );
        assert_eq!(provider.refresh_failures(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn metadata_403_is_error_not_v1_fallback() {
        let state = ImdsState::new(role_doc("AKIA_X", "x", "x", EXP_A));
        state.token_status.store(403, O::Relaxed);
        let endpoint = spawn_imds(Arc::clone(&state)).await;

        let clock = FakeClock::new(parse_imds_expiration(EXP_A).expect("exp") - ONE_HOUR_NANOS);
        let err = build_provider(endpoint, clock)
            .await
            .expect_err("a 403 from IMDS must fail construction, not downgrade to IMDSv1");
        assert!(matches!(err, StoreError::Permanent(_)), "{err:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn construction_bounded_by_timeout_when_imds_hangs() {
        let state = ImdsState::new(role_doc("AKIA_X", "x", "x", EXP_A));
        state.hang.store(true, O::Relaxed);
        let endpoint = spawn_imds(Arc::clone(&state)).await;

        let clock = FakeClock::new(parse_imds_expiration(EXP_A).expect("exp") - ONE_HOUR_NANOS);
        // The client request timeout is 2s; a generous 20s ceiling proves the
        // bound holds without depending on its exact value. Without a bounded
        // client, this future would never resolve and the test would hang.
        let built = tokio::time::timeout(Duration::from_secs(20), build_provider(endpoint, clock))
            .await
            .expect("construction must return within the bound, never hang");
        assert!(
            matches!(built, Err(StoreError::Permanent(_))),
            "a hung IMDS must fail construction typed, got {built:?}"
        );
    }

    #[test]
    fn parse_imds_expiration_matches_known_epoch() {
        // 2033-11-14T22:13:20Z is 2_015_619_200 s since the epoch
        // (`date -u -d 2033-11-14T22:13:20Z +%s`).
        assert_eq!(
            parse_imds_expiration("2033-11-14T22:13:20Z").expect("parse"),
            2_015_619_200 * 1_000_000_000
        );
        // The Unix epoch itself.
        assert_eq!(
            parse_imds_expiration("1970-01-01T00:00:00Z").expect("epoch"),
            0
        );
        // A fractional second is tolerated and truncated.
        assert_eq!(
            parse_imds_expiration("1970-01-01T00:00:01.500Z").expect("frac"),
            1_000_000_000
        );
    }

    #[test]
    fn parse_imds_expiration_rejects_malformed() {
        for bad in [
            "",
            "not-a-date",
            "2033-11-14T22:13:20",  // no Z
            "2033-13-01T00:00:00Z", // month 13
            "2033-11-14T24:00:00Z", // hour 24
            "2033/11/14T22:13:20Z", // wrong separators
        ] {
            assert!(
                parse_imds_expiration(bad).is_err(),
                "{bad:?} must be rejected, not parsed or panic"
            );
        }
    }

    /// Finding 1: a credential whose `Expiration` crosses during the refresh
    /// window must not be served once, uncounted. The refresh is broken and
    /// the clock is unexpired when sampled before the refresh but expired once
    /// the refresh has run and failed; the request must fail typed and count.
    #[tokio::test(flavor = "multi_thread")]
    async fn just_expired_during_refresh_fails_typed() {
        let state = ImdsState::new(role_doc("AKIA_CROSS", "cross-secret", "cross-token", EXP_A));
        let endpoint = spawn_imds(Arc::clone(&state)).await;
        let exp_a = parse_imds_expiration(EXP_A).expect("exp");

        // Inside the refresh margin but unexpired when sampled before the
        // refresh; one nanosecond past expiry after the refresh's document GET.
        let clock: Arc<dyn WallClock> = Arc::new(RefreshCrossingClock {
            imds: Arc::clone(&state),
            before: exp_a - 60 * 1_000_000_000,
            after: exp_a + 1,
        });
        let provider = tokio::task::spawn_blocking(move || {
            InstanceRoleCredentialProvider::with_clock(endpoint, clock)
        })
        .await
        .expect("join")
        .expect("eager fetch");

        // Break the refresh so it fails after its document GET has advanced the
        // clock past expiry.
        state.doc_status.store(500, O::Relaxed);

        let err = provider.get_credential().await.expect_err(
            "a credential that expires during the refresh must fail, not be served once",
        );
        assert!(
            matches!(err, object_store::Error::Generic { .. }),
            "{err:?}"
        );
        assert_eq!(
            provider.refresh_failures(),
            1,
            "the expiry that crossed during the refresh must be counted, not served silently"
        );
    }

    /// Finding 2: two expiries inside one warning interval both increment the
    /// failure counter (it accumulates past 1), while the warning is
    /// rate-limited to the first, and a later expiry past the interval warns
    /// again. Mirrors the `s3::credentials` rate-limit shape via the injected
    /// clock.
    #[tokio::test(flavor = "multi_thread")]
    async fn warning_rate_limited_and_counter_accumulates() {
        let state = ImdsState::new(role_doc("AKIA_EXP", "exp-secret", "exp-token", EXP_A));
        let endpoint = spawn_imds(Arc::clone(&state)).await;
        let exp_a = parse_imds_expiration(EXP_A).expect("exp");

        let clock = FakeClock::new(exp_a - ONE_HOUR_NANOS);
        let provider = build_provider(endpoint, Arc::clone(&clock))
            .await
            .expect("eager fetch");

        // Refresh broken and the credential expired: the request fails, counts,
        // and warns (first expiry stamps the clock).
        state.doc_status.store(500, O::Relaxed);
        clock.set(exp_a + 1);
        provider
            .get_credential()
            .await
            .expect_err("first expired-and-broken refresh must fail");
        assert_eq!(provider.refresh_failures(), 1);
        let first_warn = provider.last_warned_nanos.load(O::Relaxed);
        assert_eq!(
            first_warn,
            exp_a + 1,
            "the first expiry must warn and stamp the clock"
        );

        // A second expiry still inside WARNING_INTERVAL_NANOS: the counter must
        // accumulate past 1, but the warning is suppressed (stamp unchanged).
        clock.set(exp_a + 2);
        provider
            .get_credential()
            .await
            .expect_err("second expired failure");
        assert_eq!(
            provider.refresh_failures(),
            2,
            "every expiry counts, whether its warning is suppressed or not"
        );
        assert_eq!(
            provider.last_warned_nanos.load(O::Relaxed),
            first_warn,
            "a warning inside the interval must be rate-limited, not re-emitted"
        );

        // Past the interval: the warning is due again and re-stamps.
        let later = exp_a + 1 + WARNING_INTERVAL_NANOS;
        clock.set(later);
        provider
            .get_credential()
            .await
            .expect_err("third expired failure");
        assert_eq!(provider.refresh_failures(), 3);
        assert_eq!(
            provider.last_warned_nanos.load(O::Relaxed),
            later,
            "past the interval the warning fires again"
        );
    }

    /// Finding 3: an absurd parsed year (past the ~2.5e16 overflow threshold of
    /// `days_from_civil`) must be a typed parse error, never a debug-build panic
    /// or a release-build wrap. Unreachable from real IMDS, reachable from a
    /// hostile or mock endpoint.
    #[test]
    fn absurd_expiration_year_is_typed_error() {
        let err = parse_imds_expiration("30000000000000000-01-01T00:00:00Z")
            .expect_err("an absurd year must be a typed error, never a panic or overflow");
        assert!(matches!(err, FetchError::BadExpiration(_)), "{err:?}");
        // A normal far-future year (inside the i64-nanoseconds range, which
        // itself caps near year 2262) still parses: the bound rejects the
        // absurd, not ordinary timestamps.
        assert!(parse_imds_expiration("2200-06-15T12:00:00Z").is_ok());
    }
}
