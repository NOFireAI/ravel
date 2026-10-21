//! EKS Pod Identity credentials for `storage.s3.auth: podIdentity`
//! (ADR-0106, Pod Identity sidecar amendment; ADR-0034 decision 2).
//!
//! Two callers share the fetch code here:
//!
//! - The credentials sidecar, `ravel-operator pod-identity-credentials
//!   --output PATH`, which the operator renders as a native sidecar next to
//!   every `ravel-server` container and in the qualify Job. It fetches the
//!   pod's role credentials from the node-local Pod Identity agent and writes
//!   ADR-0072's credentials file, which the server reads through
//!   `--s3-credentials-file` and re-reads when its mtime changes.
//! - The operator process itself, which needs its own credentials for
//!   `sys/auth` and the `shardOverrides` reshard. It keeps them in memory
//!   ([`OperatorCredentialCache`]) and writes no file.
//!
//! The Pod Identity webhook injects two variables into a pod whose
//! ServiceAccount has an association: [`ENV_CREDENTIALS_URI`] (the agent,
//! `http://169.254.170.23/v1/credentials`, or `http://[fd00:ec2::23]/...` on
//! an IPv6 cluster) and [`ENV_AUTHORIZATION_TOKEN_FILE`]. The token file is a
//! kubelet-projected ServiceAccount token (audience `pods.eks.amazonaws.com`,
//! `expirationSeconds: 86400`). The kubelet rotates it and the agent only
//! validates it, so it is re-read on every fetch. There are no defaults for
//! either variable: without them this module refuses to start rather than
//! guess, which keeps ADR-0106's "no credential-chain magic" contract.
//!
//! Only EKS Pod Identity is supported. IRSA (`AWS_WEB_IDENTITY_TOKEN_FILE` and
//! STS `AssumeRoleWithWebIdentity`) is not implemented.
//!
//! Credential values are never logged: [`PodIdentityCredentials`]'s `Debug`
//! is written by hand to redact them, and a parse error reports where the
//! document broke, never what it held.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use http_body_util::{BodyExt as _, Empty, Limited};
use hyper::header::{AUTHORIZATION, HOST, HeaderValue};
use hyper::{Method, Request, Uri};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tracing::{error, info, warn};

/// The variable the Pod Identity webhook sets to the agent's credentials URI.
pub const ENV_CREDENTIALS_URI: &str = "AWS_CONTAINER_CREDENTIALS_FULL_URI";

/// The variable the Pod Identity webhook sets to the projected token's path.
pub const ENV_AUTHORIZATION_TOKEN_FILE: &str = "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE";

/// Bound on the TCP connect to the agent.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Bound on the whole request once connected: handshake, request, response.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// How long before the credential's `Expiration` the refresh is due at the
/// latest. The halfway point wins when it is earlier.
pub const DEFAULT_REFRESH_MARGIN: Duration = Duration::from_secs(15 * 60);

/// The soonest a refresh is ever scheduled after a successful write, so an
/// agent that hands out a nearly expired credential is not polled in a loop.
pub const MIN_REFRESH_DELAY: Duration = Duration::from_secs(30);

/// First retry delay after a failed fetch.
pub const BACKOFF_INITIAL: Duration = Duration::from_secs(1);

/// Ceiling of the retry delay after repeated failed fetches.
pub const BACKOFF_MAX: Duration = Duration::from_secs(60);

/// Largest agent response body read. A credential document is well under
/// 4 KiB; anything larger is not one.
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// Why a Pod Identity fetch, write or probe failed. No variant carries a
/// credential value.
#[derive(Debug, thiserror::Error)]
pub enum PodIdentityError {
    /// One or both webhook variables are absent (or empty).
    #[error(
        "{missing} not set: storage.s3.auth podIdentity needs both {ENV_CREDENTIALS_URI} and \
         {ENV_AUTHORIZATION_TOKEN_FILE}, which the EKS Pod Identity webhook injects; does this \
         pod's ServiceAccount carry a Pod Identity association, and does the cluster run the \
         eks-pod-identity-agent add-on?"
    )]
    MissingEnvironment {
        /// The missing variable names, joined with " and ".
        missing: String,
    },

    /// The credentials URI is not a plain `http://` URI with a host.
    #[error("credentials URI {uri:?} is not usable: {reason}")]
    InvalidUri {
        /// The URI as given.
        uri: String,
        /// What is wrong with it.
        reason: &'static str,
    },

    /// The projected token file could not be read.
    #[error("reading the authorization token file {path}: {source}")]
    TokenFile {
        /// The token file path.
        path: PathBuf,
        /// The read error.
        source: io::Error,
    },

    /// The projected token file is empty after trimming.
    #[error("the authorization token file {path} is empty")]
    EmptyToken {
        /// The token file path.
        path: PathBuf,
    },

    /// The token is not a valid HTTP header value.
    #[error("the authorization token in {path} is not a valid HTTP header value")]
    InvalidToken {
        /// The token file path.
        path: PathBuf,
    },

    /// Connecting to the agent failed or timed out.
    #[error("connecting to the Pod Identity agent at {authority}: {reason}")]
    Connect {
        /// `host:port` of the agent.
        authority: String,
        /// The connect error, or the timeout.
        reason: String,
    },

    /// The request to the agent failed after the connection was made.
    #[error("Pod Identity agent request to {authority} failed: {reason}")]
    Request {
        /// `host:port` of the agent.
        authority: String,
        /// The HTTP error, or the timeout.
        reason: String,
    },

    /// The agent answered with a non-2xx status.
    #[error("Pod Identity agent answered HTTP {status}: {body}")]
    Status {
        /// The HTTP status code.
        status: u16,
        /// The response body, truncated. An agent error body carries an
        /// error message, never a credential.
        body: String,
    },

    /// The agent's answer is not a credential document.
    #[error("Pod Identity agent response is not a credential document: {0}")]
    Parse(String),

    /// The credentials file could not be written.
    #[error("writing the credentials file {path}: {source}")]
    Write {
        /// The credentials file path.
        path: PathBuf,
        /// The I/O error.
        source: io::Error,
    },
}

/// Role credentials from the Pod Identity agent. `Debug` redacts the three
/// secrets and prints only the expiration.
#[derive(Clone, PartialEq, Eq)]
pub struct PodIdentityCredentials {
    /// `AccessKeyId`.
    pub access_key_id: String,
    /// `SecretAccessKey`.
    pub secret_access_key: String,
    /// `Token`, the STS session token.
    pub session_token: String,
    /// `Expiration`, as the agent reported it.
    pub expiration: SystemTime,
}

impl fmt::Debug for PodIdentityCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PodIdentityCredentials")
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .field("session_token", &"<redacted>")
            .field("expiration", &humantime::format_rfc3339(self.expiration))
            .finish()
    }
}

/// The agent's credential document. `AccountId` and any other field the agent
/// adds are ignored.
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AgentResponse {
    access_key_id: String,
    secret_access_key: String,
    token: String,
    expiration: String,
}

/// ADR-0072's credentials file, the shape `ravel-object-store`'s
/// `FileCredentialProvider` reads. The operator's round-trip test pins this
/// writer against that reader through `S3Store::new`.
#[derive(Serialize, Deserialize)]
struct CredentialsFile {
    access_key_id: String,
    secret_access_key: String,
    #[serde(default)]
    session_token: Option<String>,
}

/// Where to fetch credentials: the agent URI and the projected token file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentEndpoint {
    uri: Uri,
    host: String,
    port: u16,
    token_file: PathBuf,
}

impl AgentEndpoint {
    /// Build an endpoint from the two values, either of which may be absent
    /// (a missing value is reported naming both variables). An empty value
    /// counts as absent.
    pub fn resolve(uri: Option<&str>, token_file: Option<&Path>) -> Result<Self, PodIdentityError> {
        let uri = uri.filter(|value| !value.trim().is_empty());
        let token_file = token_file.filter(|path| !path.as_os_str().is_empty());
        let (uri, token_file) = match (uri, token_file) {
            (Some(uri), Some(token_file)) => (uri, token_file),
            (uri, token_file) => {
                let missing: Vec<&str> = [
                    uri.is_none().then_some(ENV_CREDENTIALS_URI),
                    token_file.is_none().then_some(ENV_AUTHORIZATION_TOKEN_FILE),
                ]
                .into_iter()
                .flatten()
                .collect();
                return Err(PodIdentityError::MissingEnvironment {
                    missing: missing.join(" and "),
                });
            }
        };
        Self::new(uri.trim(), token_file.to_path_buf())
    }

    /// Read the two webhook variables from this process's environment.
    pub fn from_env() -> Result<Self, PodIdentityError> {
        let uri = std::env::var(ENV_CREDENTIALS_URI).ok();
        let token_file = std::env::var_os(ENV_AUTHORIZATION_TOKEN_FILE).map(PathBuf::from);
        Self::resolve(uri.as_deref(), token_file.as_deref())
    }

    /// Validate `uri`: plain `http://` (the agent is link-local and serves no
    /// TLS), with a host. An IPv6 literal host loses its brackets for the
    /// connect.
    pub fn new(uri: &str, token_file: PathBuf) -> Result<Self, PodIdentityError> {
        let invalid = |reason| PodIdentityError::InvalidUri {
            uri: uri.to_string(),
            reason,
        };
        let parsed: Uri = uri.parse().map_err(|_| invalid("it does not parse"))?;
        if parsed.scheme_str() != Some("http") {
            return Err(invalid(
                "only http:// is supported; the EKS Pod Identity agent is link-local plain HTTP",
            ));
        }
        let host = parsed
            .host()
            .filter(|host| !host.is_empty())
            .ok_or_else(|| invalid("it has no host"))?;
        let host = host
            .strip_prefix('[')
            .and_then(|inner| inner.strip_suffix(']'))
            .unwrap_or(host)
            .to_string();
        let port = parsed.port_u16().unwrap_or(80);
        Ok(Self {
            uri: parsed,
            host,
            port,
            token_file,
        })
    }

    /// The projected token file this endpoint reads on every fetch.
    pub fn token_file(&self) -> &Path {
        &self.token_file
    }

    fn authority(&self) -> String {
        self.uri
            .authority()
            .map(|authority| authority.as_str().to_string())
            .unwrap_or_else(|| format!("{}:{}", self.host, self.port))
    }
}

/// Aborts the spawned connection driver when the fetch ends, including when
/// the fetch is dropped by its timeout.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Fetch the pod's role credentials from the agent: one GET with the token
/// file's trimmed contents as the `Authorization` header. The token file is
/// read on every call, because the kubelet rotates it.
pub async fn fetch(endpoint: &AgentEndpoint) -> Result<PodIdentityCredentials, PodIdentityError> {
    let raw_token = tokio::fs::read_to_string(&endpoint.token_file)
        .await
        .map_err(|source| PodIdentityError::TokenFile {
            path: endpoint.token_file.clone(),
            source,
        })?;
    let token = raw_token.trim();
    if token.is_empty() {
        return Err(PodIdentityError::EmptyToken {
            path: endpoint.token_file.clone(),
        });
    }
    let mut authorization =
        HeaderValue::from_str(token).map_err(|_| PodIdentityError::InvalidToken {
            path: endpoint.token_file.clone(),
        })?;
    authorization.set_sensitive(true);

    let authority = endpoint.authority();
    let stream = tokio::time::timeout(
        CONNECT_TIMEOUT,
        TcpStream::connect((endpoint.host.as_str(), endpoint.port)),
    )
    .await
    .map_err(|_| PodIdentityError::Connect {
        authority: authority.clone(),
        reason: format!("timed out after {}s", CONNECT_TIMEOUT.as_secs()),
    })?
    .map_err(|err| PodIdentityError::Connect {
        authority: authority.clone(),
        reason: err.to_string(),
    })?;

    let request_failed = |reason: String| PodIdentityError::Request {
        authority: authority.clone(),
        reason,
    };
    let path_and_query = endpoint
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let request = Request::builder()
        .method(Method::GET)
        .uri(path_and_query)
        .header(HOST, authority.as_str())
        .header(AUTHORIZATION, authorization)
        .body(Empty::<Bytes>::new())
        .map_err(|err| request_failed(err.to_string()))?;

    let exchange = async {
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
        let _driver = AbortOnDrop(tokio::spawn(async move {
            let _ = connection.await;
        }));
        let response = sender.send_request(request).await?;
        let status = response.status();
        let body = Limited::new(response.into_body(), MAX_RESPONSE_BYTES)
            .collect()
            .await
            .map(|collected| collected.to_bytes());
        Ok::<_, hyper::Error>((status, body))
    };
    let (status, body) = tokio::time::timeout(REQUEST_TIMEOUT, exchange)
        .await
        .map_err(|_| request_failed(format!("timed out after {}s", REQUEST_TIMEOUT.as_secs())))?
        .map_err(|err| request_failed(err.to_string()))?;
    let body = body.map_err(|err| request_failed(format!("reading the response body: {err}")))?;

    if !status.is_success() {
        let text = String::from_utf8_lossy(&body);
        let mut text: String = text.chars().take(256).collect();
        if text.is_empty() {
            text = "(empty body)".to_string();
        }
        return Err(PodIdentityError::Status {
            status: status.as_u16(),
            body: text,
        });
    }
    parse_agent_response(&body)
}

/// Parse the agent's credential document. A JSON error reports its category
/// and position only, so a malformed answer never puts a value in a log line.
fn parse_agent_response(body: &[u8]) -> Result<PodIdentityCredentials, PodIdentityError> {
    let parsed: AgentResponse = serde_json::from_slice(body).map_err(|err| {
        PodIdentityError::Parse(format!(
            "{:?} error at line {} column {}",
            err.classify(),
            err.line(),
            err.column()
        ))
    })?;
    for (field, value) in [
        ("AccessKeyId", &parsed.access_key_id),
        ("SecretAccessKey", &parsed.secret_access_key),
        ("Token", &parsed.token),
    ] {
        if value.is_empty() {
            return Err(PodIdentityError::Parse(format!("{field} is empty")));
        }
    }
    let expiration = parse_expiration(&parsed.expiration)?;
    Ok(PodIdentityCredentials {
        access_key_id: parsed.access_key_id,
        secret_access_key: parsed.secret_access_key,
        session_token: parsed.token,
        expiration,
    })
}

/// Parse `Expiration`: RFC 3339 in UTC, as the agent writes it (Go's
/// `time.RFC3339Nano`, which drops trailing zeros from the fraction, so the
/// fraction has anywhere from zero to nine digits).
pub fn parse_expiration(value: &str) -> Result<SystemTime, PodIdentityError> {
    humantime::parse_rfc3339(value).map_err(|err| {
        PodIdentityError::Parse(format!(
            "Expiration {value:?} is not an RFC 3339 UTC timestamp: {err}"
        ))
    })
}

/// Distinguishes temp files of concurrent writers in one process (tests).
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Write `credentials` to `path` as ADR-0072's credentials file, atomically:
/// a temp file in the same directory (so the rename stays inside one
/// filesystem, the pod's `emptyDir`), created with mode 0600, fsynced, renamed
/// over `path`, then the directory fsynced. A reader sees the old file or the
/// new one, never a partial one, and every write is a new inode with a new
/// mtime, which is what the server's re-read keys on.
pub fn write_credentials_file(
    path: &Path,
    credentials: &PodIdentityCredentials,
) -> Result<(), PodIdentityError> {
    let failed = |source: io::Error| PodIdentityError::Write {
        path: path.to_path_buf(),
        source,
    };
    let file_name = path.file_name().ok_or_else(|| {
        failed(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the output path names no file",
        ))
    })?;
    let directory = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let contents = serde_json::to_vec(&CredentialsFile {
        access_key_id: credentials.access_key_id.clone(),
        secret_access_key: credentials.secret_access_key.clone(),
        session_token: Some(credentials.session_token.clone()),
    })
    .map_err(|err| failed(io::Error::other(err)))?;

    let temp = directory.join(format!(
        ".{}.tmp-{}-{}",
        file_name.to_string_lossy(),
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let written = (|| -> io::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?;
        file.write_all(&contents)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp, path)
    })();
    if let Err(err) = written {
        // Best effort: the temp file may not exist if the open failed.
        let _ = fs::remove_file(&temp);
        return Err(failed(err));
    }
    File::open(&directory)
        .and_then(|dir| dir.sync_all())
        .map_err(failed)
}

/// The startup probe: `Ok` when `path` exists and parses as ADR-0072's
/// credentials file with both keys non-empty.
pub fn probe(path: &Path) -> Result<(), String> {
    let bytes = fs::read(path).map_err(|err| format!("reading {}: {err}", path.display()))?;
    let parsed: CredentialsFile = serde_json::from_slice(&bytes).map_err(|err| {
        format!(
            "{} is not a credentials file: {:?} error at line {} column {}",
            path.display(),
            err.classify(),
            err.line(),
            err.column()
        )
    })?;
    if parsed.access_key_id.is_empty() || parsed.secret_access_key.is_empty() {
        return Err(format!(
            "{} has an empty access_key_id or secret_access_key",
            path.display()
        ));
    }
    Ok(())
}

/// When the next refresh is due after a successful fetch at `now`: the
/// earlier of halfway to `expiration` and `margin` before it, never sooner
/// than [`MIN_REFRESH_DELAY`].
pub fn next_refresh_delay(now: SystemTime, expiration: SystemTime, margin: Duration) -> Duration {
    let remaining = expiration.duration_since(now).unwrap_or(Duration::ZERO);
    let halfway = remaining / 2;
    let before_margin = remaining.saturating_sub(margin);
    halfway.min(before_margin).max(MIN_REFRESH_DELAY)
}

/// The retry delay before failed attempt `attempt + 1`, for `attempt >= 1`:
/// [`BACKOFF_INITIAL`] doubling up to [`BACKOFF_MAX`].
pub fn backoff_delay(attempt: u32) -> Duration {
    let doublings = attempt.saturating_sub(1).min(16);
    BACKOFF_INITIAL
        .saturating_mul(1u32 << doublings)
        .min(BACKOFF_MAX)
}

/// What one refresh attempt decided; the sidecar loop logs it and sleeps for
/// the delay it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefreshOutcome {
    /// The file was written with a fresh credential.
    Wrote {
        /// Until the next refresh.
        next_in: Duration,
        /// Until the written credential expires.
        expires_in: Duration,
    },
    /// The fetch or the write failed; the old file, if any, is untouched.
    Failed {
        /// Consecutive failures, this one included.
        attempt: u32,
        /// Until the next attempt.
        retry_in: Duration,
        /// Time left on the credential in the file, `None` when nothing has
        /// been written yet, `Some(ZERO)` once it has expired.
        expires_in: Option<Duration>,
        /// True exactly once per expiry: the file's credential has expired
        /// and this attempt is the first to see it.
        report_expired: bool,
    },
}

/// The sidecar's refresh state: when the file's credential expires and how
/// many fetches have failed in a row.
#[derive(Debug, Clone)]
pub struct Refresher {
    margin: Duration,
    file_expiration: Option<SystemTime>,
    failures: u32,
    expired_reported: bool,
}

impl Refresher {
    /// A refresher with no file written yet.
    pub fn new(margin: Duration) -> Self {
        Self {
            margin,
            file_expiration: None,
            failures: 0,
            expired_reported: false,
        }
    }

    /// Record a successful write at `now` of a credential expiring at
    /// `expiration`.
    pub fn on_success(&mut self, now: SystemTime, expiration: SystemTime) -> RefreshOutcome {
        self.file_expiration = Some(expiration);
        self.failures = 0;
        self.expired_reported = false;
        RefreshOutcome::Wrote {
            next_in: next_refresh_delay(now, expiration, self.margin),
            expires_in: expiration.duration_since(now).unwrap_or(Duration::ZERO),
        }
    }

    /// Record a failed attempt at `now`.
    pub fn on_failure(&mut self, now: SystemTime) -> RefreshOutcome {
        self.failures = self.failures.saturating_add(1);
        let expires_in = self
            .file_expiration
            .map(|expiration| expiration.duration_since(now).unwrap_or(Duration::ZERO));
        let expired = expires_in == Some(Duration::ZERO);
        let report_expired = expired && !self.expired_reported;
        if report_expired {
            self.expired_reported = true;
        }
        RefreshOutcome::Failed {
            attempt: self.failures,
            retry_in: backoff_delay(self.failures),
            expires_in,
            report_expired,
        }
    }
}

/// One refresh attempt: fetch, write, and update `refresher` at `now`.
/// Returns the outcome and, on failure, the error for the log line.
pub async fn refresh_once(
    endpoint: &AgentEndpoint,
    output: &Path,
    refresher: &mut Refresher,
    now: SystemTime,
) -> (RefreshOutcome, Option<PodIdentityError>) {
    let written = match fetch(endpoint).await {
        Ok(credentials) => {
            write_credentials_file(output, &credentials).map(|()| credentials.expiration)
        }
        Err(err) => Err(err),
    };
    match written {
        Ok(expiration) => (refresher.on_success(now, expiration), None),
        Err(err) => (refresher.on_failure(now), Some(err)),
    }
}

/// Log one outcome. Values only: durations and counts, never a credential.
fn log_outcome(output: &Path, outcome: &RefreshOutcome, err: Option<&PodIdentityError>) {
    match outcome {
        RefreshOutcome::Wrote {
            next_in,
            expires_in,
        } => info!(
            path = %output.display(),
            expires_in_secs = expires_in.as_secs(),
            next_refresh_in_secs = next_in.as_secs(),
            "wrote Pod Identity credentials"
        ),
        RefreshOutcome::Failed {
            attempt,
            retry_in,
            expires_in,
            report_expired,
        } => {
            let reason = err.map(ToString::to_string).unwrap_or_default();
            match expires_in {
                Some(left) => warn!(
                    path = %output.display(),
                    attempt,
                    expires_in_secs = left.as_secs(),
                    retry_in_secs = retry_in.as_secs(),
                    error = %reason,
                    "Pod Identity credential refresh failed; keeping the current file"
                ),
                None => warn!(
                    path = %output.display(),
                    attempt,
                    retry_in_secs = retry_in.as_secs(),
                    error = %reason,
                    "Pod Identity credential fetch failed; no credentials file written yet"
                ),
            }
            if *report_expired {
                error!(
                    path = %output.display(),
                    attempt,
                    "the credential in the Pod Identity credentials file has expired; S3 \
                     requests signed with it will fail until a refresh succeeds"
                );
            }
        }
    }
}

/// `ravel-operator pod-identity-credentials`: write mode or probe mode.
#[derive(Debug, Clone, clap::Args)]
#[command(group(
    clap::ArgGroup::new("mode")
        .required(true)
        .multiple(false)
        .args(["output", "probe"])
))]
pub struct PodIdentityArgs {
    /// Write the credentials file here and keep it fresh (ADR-0072's JSON
    /// shape, mode 0600, replaced atomically).
    #[arg(long, value_name = "PATH")]
    pub output: Option<PathBuf>,

    /// Exit 0 when this credentials file exists and parses, non-zero
    /// otherwise. The startup probe of the rendered sidecar.
    #[arg(long, value_name = "PATH")]
    pub probe: Option<PathBuf>,

    /// The Pod Identity agent's credentials URI. No default: the EKS Pod
    /// Identity webhook sets the variable.
    #[arg(long, env = ENV_CREDENTIALS_URI, value_name = "URI")]
    pub credentials_uri: Option<String>,

    /// The projected ServiceAccount token sent as `Authorization`, re-read on
    /// every fetch. No default: the EKS Pod Identity webhook sets the
    /// variable.
    #[arg(long, env = ENV_AUTHORIZATION_TOKEN_FILE, value_name = "PATH")]
    pub authorization_token_file: Option<PathBuf>,

    /// Refresh at the latest this long before the credential expires (the
    /// halfway point wins when earlier, and never sooner than 30 s out).
    #[arg(long, value_name = "DURATION", value_parser = humantime::parse_duration, default_value = "15m")]
    pub refresh_margin: Duration,

    /// Fetch and write once, then exit (non-zero on failure).
    #[arg(long)]
    pub once: bool,
}

/// Run the subcommand to completion. Probe mode returns at once; write mode
/// loops until SIGTERM or SIGINT, or returns after one attempt with
/// `--once`.
pub async fn run(args: PodIdentityArgs) -> Result<(), String> {
    if let Some(path) = args.probe.as_deref() {
        return probe(path);
    }
    let Some(output) = args.output.as_deref() else {
        return Err("either --output or --probe is required".to_string());
    };
    let endpoint = AgentEndpoint::resolve(
        args.credentials_uri.as_deref(),
        args.authorization_token_file.as_deref(),
    )
    .map_err(|err| err.to_string())?;

    let mut refresher = Refresher::new(args.refresh_margin);
    if args.once {
        let (outcome, err) =
            refresh_once(&endpoint, output, &mut refresher, SystemTime::now()).await;
        log_outcome(output, &outcome, err.as_ref());
        return match err {
            Some(err) => Err(err.to_string()),
            None => Ok(()),
        };
    }

    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|err| format!("installing the SIGTERM handler: {err}"))?;
    info!(
        path = %output.display(),
        token_file = %endpoint.token_file().display(),
        "starting the Pod Identity credentials sidecar"
    );
    loop {
        let (outcome, err) =
            refresh_once(&endpoint, output, &mut refresher, SystemTime::now()).await;
        log_outcome(output, &outcome, err.as_ref());
        let delay = match outcome {
            RefreshOutcome::Wrote { next_in, .. } => next_in,
            RefreshOutcome::Failed { retry_in, .. } => retry_in,
        };
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            _ = terminate.recv() => {
                info!("SIGTERM received; the Pod Identity credentials sidecar is exiting");
                return Ok(());
            }
            _ = tokio::signal::ctrl_c() => {
                info!("SIGINT received; the Pod Identity credentials sidecar is exiting");
                return Ok(());
            }
        }
    }
}

/// The operator's own Pod Identity credentials, cached in memory until the
/// same refresh point the sidecar uses. The operator pod mounts no volume and
/// writes no file; each reconcile pass that needs an S3 handle asks this
/// cache.
#[derive(Debug)]
pub struct OperatorCredentialCache {
    margin: Duration,
    state: tokio::sync::Mutex<Option<Cached>>,
}

#[derive(Debug)]
struct Cached {
    credentials: PodIdentityCredentials,
    refresh_at: SystemTime,
}

impl Default for OperatorCredentialCache {
    fn default() -> Self {
        Self::new(DEFAULT_REFRESH_MARGIN)
    }
}

impl OperatorCredentialCache {
    /// An empty cache refreshing `margin` before expiry at the latest.
    pub fn new(margin: Duration) -> Self {
        Self {
            margin,
            state: tokio::sync::Mutex::new(None),
        }
    }

    /// The cached credential while it is before its refresh point; otherwise
    /// a fresh fetch. A failed fetch falls back to the cached credential
    /// while it has not expired, with a warning, and fails once it has.
    pub async fn get(
        &self,
        endpoint: &AgentEndpoint,
        now: SystemTime,
    ) -> Result<PodIdentityCredentials, PodIdentityError> {
        let mut state = self.state.lock().await;
        if let Some(cached) = state.as_ref()
            && now < cached.refresh_at
        {
            return Ok(cached.credentials.clone());
        }
        match fetch(endpoint).await {
            Ok(credentials) => {
                let refresh_at = now + next_refresh_delay(now, credentials.expiration, self.margin);
                *state = Some(Cached {
                    credentials: credentials.clone(),
                    refresh_at,
                });
                Ok(credentials)
            }
            Err(err) => match state.as_ref() {
                Some(cached) if now < cached.credentials.expiration => {
                    let expires_in = cached
                        .credentials
                        .expiration
                        .duration_since(now)
                        .unwrap_or(Duration::ZERO);
                    warn!(
                        error = %err,
                        expires_in_secs = expires_in.as_secs(),
                        "refreshing the operator's Pod Identity credentials failed; using the cached credential"
                    );
                    Ok(cached.credentials.clone())
                }
                _ => Err(err),
            },
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    use std::convert::Infallible;
    use std::os::unix::fs::PermissionsExt as _;
    use std::sync::{Arc, Mutex};

    use http_body_util::Full;
    use hyper::body::Incoming;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Response, StatusCode};
    use tokio::net::TcpListener;

    /// The agent's `Expiration` shape: Go `time.RFC3339Nano` in UTC
    /// (eks-pod-identity-agent pkg/credentials/model.go).
    const EXPIRATION: &str = "2026-10-21T18:00:00.123456789Z";

    /// A scratch directory under the system temp dir, removed on drop.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(label: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "ravel-pod-identity-{label}-{}-{}",
                std::process::id(),
                TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&dir).expect("create scratch dir");
            Self(dir)
        }

        fn path(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// What the mock agent answers with, and the token it accepts.
    struct Agent {
        expected_token: Mutex<String>,
        status: Mutex<StatusCode>,
        body: Mutex<String>,
        requests: Mutex<Vec<String>>,
    }

    fn agent_body(key: &str) -> String {
        format!(
            r#"{{"AccessKeyId":"{key}","SecretAccessKey":"secret-{key}","Token":"token-{key}","AccountId":"819709950457","Expiration":"{EXPIRATION}"}}"#
        )
    }

    impl Agent {
        fn new(token: &str) -> Arc<Self> {
            Arc::new(Self {
                expected_token: Mutex::new(token.to_string()),
                status: Mutex::new(StatusCode::OK),
                body: Mutex::new(agent_body("AKIA1")),
                requests: Mutex::new(Vec::new()),
            })
        }
    }

    async fn answer(
        agent: Arc<Agent>,
        req: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let presented = req
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        agent
            .requests
            .lock()
            .unwrap()
            .push(format!("{} {}", req.method(), req.uri().path()));
        let expected = agent.expected_token.lock().unwrap().clone();
        let (status, body) = if presented != expected {
            (
                StatusCode::UNAUTHORIZED,
                r#"{"message":"invalid token"}"#.to_string(),
            )
        } else {
            (
                *agent.status.lock().unwrap(),
                agent.body.lock().unwrap().clone(),
            )
        };
        let mut response = Response::new(Full::new(Bytes::from(body)));
        *response.status_mut() = status;
        Ok(response)
    }

    /// Serve `agent` on a loopback port; returns the credentials URI.
    async fn serve(agent: Arc<Agent>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                let agent = Arc::clone(&agent);
                tokio::spawn(async move {
                    let service = service_fn(move |req| answer(Arc::clone(&agent), req));
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        format!("http://{addr}/v1/credentials")
    }

    fn credentials(key: &str, expiration: SystemTime) -> PodIdentityCredentials {
        PodIdentityCredentials {
            access_key_id: key.to_string(),
            secret_access_key: format!("secret-{key}"),
            session_token: format!("token-{key}"),
            expiration,
        }
    }

    fn read_file(path: &Path) -> serde_json::Value {
        serde_json::from_slice(&fs::read(path).expect("read credentials file"))
            .expect("credentials file is JSON")
    }

    #[tokio::test]
    async fn happy_path_writes_a_parseable_0600_file() {
        let scratch = Scratch::new("happy");
        let token_file = scratch.path("token");
        fs::write(&token_file, "projected-token-1\n").expect("write token");
        let agent = Agent::new("projected-token-1");
        let uri = serve(Arc::clone(&agent)).await;
        let endpoint = AgentEndpoint::new(&uri, token_file).expect("endpoint");
        let output = scratch.path("credentials.json");
        let mut refresher = Refresher::new(DEFAULT_REFRESH_MARGIN);
        let now = parse_expiration("2026-10-21T12:00:00Z").expect("now");

        let (outcome, err) = refresh_once(&endpoint, &output, &mut refresher, now).await;

        assert!(err.is_none(), "unexpected error: {err:?}");
        assert!(matches!(outcome, RefreshOutcome::Wrote { .. }));
        assert_eq!(
            read_file(&output),
            serde_json::json!({
                "access_key_id": "AKIA1",
                "secret_access_key": "secret-AKIA1",
                "session_token": "token-AKIA1",
            })
        );
        let mode = fs::metadata(&output).expect("stat").permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "the credentials file is 0600");
        probe(&output).expect("the written file passes the probe");
        assert_eq!(
            agent.requests.lock().unwrap().as_slice(),
            ["GET /v1/credentials"]
        );
    }

    #[tokio::test]
    async fn the_token_file_is_reread_on_every_fetch() {
        let scratch = Scratch::new("reread");
        let token_file = scratch.path("token");
        fs::write(&token_file, "token-before").expect("write token");
        let agent = Agent::new("token-before");
        let uri = serve(Arc::clone(&agent)).await;
        let endpoint = AgentEndpoint::new(&uri, token_file.clone()).expect("endpoint");
        fetch(&endpoint).await.expect("first fetch");

        // The kubelet rotates the projected token; the agent now only
        // accepts the new one.
        fs::write(&token_file, "token-after").expect("rotate token");
        *agent.expected_token.lock().unwrap() = "token-after".to_string();
        *agent.body.lock().unwrap() = agent_body("AKIA2");

        let second = fetch(&endpoint)
            .await
            .expect("fetch with the rotated token");
        assert_eq!(second.access_key_id, "AKIA2");
    }

    #[tokio::test]
    async fn a_bad_authorization_is_refused() {
        let scratch = Scratch::new("badauth");
        let token_file = scratch.path("token");
        fs::write(&token_file, "not-the-token").expect("write token");
        let agent = Agent::new("the-token");
        let uri = serve(agent).await;
        let endpoint = AgentEndpoint::new(&uri, token_file).expect("endpoint");

        let err = fetch(&endpoint)
            .await
            .expect_err("a wrong token is refused");
        assert!(
            matches!(err, PodIdentityError::Status { status: 401, .. }),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_failed_refresh_keeps_the_old_file_and_backs_off() {
        let scratch = Scratch::new("keep");
        let token_file = scratch.path("token");
        fs::write(&token_file, "t").expect("write token");
        let agent = Agent::new("t");
        let uri = serve(Arc::clone(&agent)).await;
        let endpoint = AgentEndpoint::new(&uri, token_file).expect("endpoint");
        let output = scratch.path("credentials.json");
        let mut refresher = Refresher::new(DEFAULT_REFRESH_MARGIN);
        let start = parse_expiration("2026-10-21T12:00:00Z").expect("start");

        let (first, err) = refresh_once(&endpoint, &output, &mut refresher, start).await;
        assert!(err.is_none() && matches!(first, RefreshOutcome::Wrote { .. }));
        let bytes_before = fs::read(&output).expect("read");
        let mtime_before = fs::metadata(&output)
            .and_then(|m| m.modified())
            .expect("mtime");

        *agent.status.lock().unwrap() = StatusCode::INTERNAL_SERVER_ERROR;
        *agent.body.lock().unwrap() = "agent unavailable".to_string();
        let later = start + Duration::from_secs(3600);
        let (second, err) = refresh_once(&endpoint, &output, &mut refresher, later).await;
        let (third, _) = refresh_once(&endpoint, &output, &mut refresher, later).await;

        assert!(matches!(
            err,
            Some(PodIdentityError::Status { status: 500, .. })
        ));
        let left = parse_expiration(EXPIRATION)
            .expect("expiration")
            .duration_since(later)
            .expect("still valid");
        assert_eq!(
            second,
            RefreshOutcome::Failed {
                attempt: 1,
                retry_in: Duration::from_secs(1),
                expires_in: Some(left),
                report_expired: false,
            }
        );
        assert!(matches!(
            third,
            RefreshOutcome::Failed { attempt: 2, retry_in, .. } if retry_in == Duration::from_secs(2)
        ));
        assert_eq!(fs::read(&output).expect("read"), bytes_before);
        assert_eq!(
            fs::metadata(&output)
                .and_then(|m| m.modified())
                .expect("mtime"),
            mtime_before
        );
    }

    #[test]
    fn an_expired_file_is_reported_once_and_retries_continue() {
        let written = parse_expiration("2026-10-21T12:00:00Z").expect("t0");
        let expiration = written + Duration::from_secs(600);
        let mut refresher = Refresher::new(DEFAULT_REFRESH_MARGIN);
        refresher.on_success(written, expiration);

        let after = expiration + Duration::from_secs(5);
        let first = refresher.on_failure(after);
        let second = refresher.on_failure(after + Duration::from_secs(1));
        assert!(matches!(
            first,
            RefreshOutcome::Failed {
                expires_in: Some(Duration::ZERO),
                report_expired: true,
                ..
            }
        ));
        assert!(matches!(
            second,
            RefreshOutcome::Failed {
                attempt: 2,
                report_expired: false,
                ..
            }
        ));

        // A later success re-arms the report for the next expiry.
        refresher.on_success(after, after + Duration::from_secs(60));
        let again = refresher.on_failure(after + Duration::from_secs(120));
        assert!(matches!(
            again,
            RefreshOutcome::Failed {
                attempt: 1,
                report_expired: true,
                ..
            }
        ));
    }

    #[test]
    fn refresh_scheduling_takes_the_earlier_of_halfway_and_the_margin() {
        let now = parse_expiration("2026-10-21T12:00:00Z").expect("now");
        let margin = DEFAULT_REFRESH_MARGIN;
        // Six hours left: 15 minutes before expiry (5h45m) is later than
        // halfway (3h), so halfway wins.
        assert_eq!(
            next_refresh_delay(now, now + Duration::from_secs(6 * 3600), margin),
            Duration::from_secs(3 * 3600)
        );
        // Twenty minutes left: halfway is 10 minutes, 15 minutes before
        // expiry is 5 minutes, so the margin wins.
        assert_eq!(
            next_refresh_delay(now, now + Duration::from_secs(20 * 60), margin),
            Duration::from_secs(5 * 60)
        );
        // Under a minute left, or already expired: the 30 s floor.
        assert_eq!(
            next_refresh_delay(now, now + Duration::from_secs(50), margin),
            MIN_REFRESH_DELAY
        );
        assert_eq!(
            next_refresh_delay(now, now - Duration::from_secs(50), margin),
            MIN_REFRESH_DELAY
        );
    }

    #[test]
    fn backoff_doubles_from_one_second_to_a_minute() {
        let delays: Vec<u64> = (1..=9).map(|n| backoff_delay(n).as_secs()).collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 32, 60, 60, 60]);
        assert_eq!(backoff_delay(u32::MAX), BACKOFF_MAX);
    }

    #[test]
    fn an_atomic_write_leaves_no_temp_file_and_replaces_the_inode() {
        use std::os::unix::fs::MetadataExt as _;

        let scratch = Scratch::new("atomic");
        let output = scratch.path("credentials.json");
        let now = SystemTime::now();
        write_credentials_file(&output, &credentials("A", now)).expect("first write");
        let first_inode = fs::metadata(&output).expect("stat").ino();
        write_credentials_file(&output, &credentials("B", now)).expect("second write");

        let names: Vec<String> = fs::read_dir(&scratch.0)
            .expect("list")
            .map(|entry| {
                entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(names, ["credentials.json"]);
        assert_ne!(fs::metadata(&output).expect("stat").ino(), first_inode);
        assert_eq!(read_file(&output)["access_key_id"], "B");
    }

    #[test]
    fn missing_environment_is_refused_naming_both_variables() {
        for (uri, token) in [
            (None, Some(Path::new("/var/run/token"))),
            (Some("http://169.254.170.23/v1/credentials"), None),
            (None, None),
            (Some("   "), Some(Path::new(""))),
        ] {
            let err = AgentEndpoint::resolve(uri, token).expect_err("refused");
            let message = err.to_string();
            assert!(matches!(err, PodIdentityError::MissingEnvironment { .. }));
            assert!(message.contains(ENV_CREDENTIALS_URI), "{message}");
            assert!(message.contains(ENV_AUTHORIZATION_TOKEN_FILE), "{message}");
            assert!(message.contains("Pod Identity association"), "{message}");
        }
    }

    #[test]
    fn only_plain_http_agent_uris_are_accepted() {
        let token = PathBuf::from("/t");
        let v4 = AgentEndpoint::new("http://169.254.170.23/v1/credentials", token.clone())
            .expect("the IPv4 agent URI");
        assert_eq!((v4.host.as_str(), v4.port), ("169.254.170.23", 80));
        let v6 = AgentEndpoint::new("http://[fd00:ec2::23]/v1/credentials", token.clone())
            .expect("the IPv6 agent URI");
        assert_eq!((v6.host.as_str(), v6.port), ("fd00:ec2::23", 80));
        assert_eq!(v6.authority(), "[fd00:ec2::23]");
        for refused in [
            "https://169.254.170.23/v1/credentials",
            "169.254.170.23/v1/credentials",
            "not a uri",
        ] {
            assert!(
                matches!(
                    AgentEndpoint::new(refused, token.clone()),
                    Err(PodIdentityError::InvalidUri { .. })
                ),
                "{refused} must be refused"
            );
        }
    }

    #[test]
    fn expiration_parses_with_zero_three_and_nine_fraction_digits() {
        let whole = parse_expiration("2026-10-21T18:00:00Z").expect("no fraction");
        let millis = parse_expiration("2026-10-21T18:00:00.123Z").expect("three digits");
        let nanos = parse_expiration(EXPIRATION).expect("nine digits");
        assert_eq!(
            millis.duration_since(whole).expect("ordered").as_millis(),
            123
        );
        assert_eq!(
            nanos.duration_since(whole).expect("ordered").as_nanos(),
            123_456_789
        );
        assert!(parse_expiration("2026-10-21T18:00:00+02:00").is_err());
        assert!(parse_expiration("tomorrow").is_err());
    }

    #[test]
    fn agent_documents_missing_a_field_are_refused_without_echoing_values() {
        let missing_token = br#"{"AccessKeyId":"AKIASECRET","SecretAccessKey":"sk","Expiration":"2026-10-21T18:00:00Z"}"#;
        let err = parse_agent_response(missing_token).expect_err("no Token");
        assert!(!err.to_string().contains("AKIASECRET"), "{err}");
        let empty_secret = br#"{"AccessKeyId":"AKIA","SecretAccessKey":"","Token":"t","Expiration":"2026-10-21T18:00:00Z"}"#;
        assert!(parse_agent_response(empty_secret).is_err());
        let wrong_type = br#"{"AccessKeyId":12345678,"SecretAccessKey":"sk","Token":"t","Expiration":"2026-10-21T18:00:00Z"}"#;
        let err = parse_agent_response(wrong_type).expect_err("wrong type");
        assert!(!err.to_string().contains("12345678"), "{err}");
    }

    #[test]
    fn debug_redacts_every_secret() {
        let shown = format!("{:?}", credentials("AKIAEXAMPLE", SystemTime::UNIX_EPOCH));
        assert!(!shown.contains("AKIAEXAMPLE"), "{shown}");
        assert!(!shown.contains("secret-"), "{shown}");
        assert!(!shown.contains("token-"), "{shown}");
        assert!(shown.contains("<redacted>"), "{shown}");
    }

    #[test]
    fn probe_accepts_a_written_file_and_refuses_absent_or_malformed_ones() {
        let scratch = Scratch::new("probe");
        let output = scratch.path("credentials.json");
        assert!(probe(&output).is_err(), "absent");
        fs::write(&output, b"{\"access_key_id\":").expect("write");
        assert!(probe(&output).is_err(), "truncated JSON");
        fs::write(&output, br#"{"access_key_id":"","secret_access_key":"s"}"#).expect("write");
        assert!(probe(&output).is_err(), "empty key");
        write_credentials_file(&output, &credentials("A", SystemTime::now())).expect("write");
        assert!(probe(&output).is_ok(), "written by the sidecar");
    }

    #[tokio::test]
    async fn the_operator_cache_reuses_a_credential_until_its_refresh_point() {
        let scratch = Scratch::new("cache");
        let token_file = scratch.path("token");
        fs::write(&token_file, "t").expect("write token");
        let agent = Agent::new("t");
        let uri = serve(Arc::clone(&agent)).await;
        let endpoint = AgentEndpoint::new(&uri, token_file).expect("endpoint");
        let cache = OperatorCredentialCache::default();
        let expiration = parse_expiration(EXPIRATION).expect("expiration");
        let now = expiration - Duration::from_secs(6 * 3600);

        cache.get(&endpoint, now).await.expect("first");
        cache
            .get(&endpoint, now + Duration::from_secs(60))
            .await
            .expect("cached");
        assert_eq!(agent.requests.lock().unwrap().len(), 1, "served from cache");

        // Past the refresh point with the agent down: the cached credential
        // still serves while unexpired, then the failure surfaces.
        *agent.status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
        let past_refresh = now + Duration::from_secs(4 * 3600);
        let fallback = cache.get(&endpoint, past_refresh).await.expect("fallback");
        assert_eq!(fallback.access_key_id, "AKIA1");
        assert_eq!(agent.requests.lock().unwrap().len(), 2);
        let expired = expiration + Duration::from_secs(1);
        assert!(cache.get(&endpoint, expired).await.is_err());
    }

    /// The drift guard between this writer and the server's reader: a file
    /// written here must build an `S3Store` through the public entry point
    /// `ravel-server` uses (`FileCredentialProvider` is `pub(crate)` in
    /// `ravel-object-store`), and a malformed one must not. `S3Store::new`
    /// reads and parses the file eagerly and does no network I/O under
    /// `Static`.
    #[test]
    fn the_written_file_builds_an_s3_store_and_a_malformed_one_does_not() {
        use ravel_object_store::s3::{S3AuthMode, S3Config, S3Store};

        // Both rustls providers are linked into this crate (see main.rs).
        let _ = rustls::crypto::ring::default_provider().install_default();
        let config = |path: PathBuf| S3Config {
            auth: S3AuthMode::Static,
            credentials_file: Some(path),
            access_key_id: "unused".into(),
            secret_access_key: "unused".into(),
            session_token: None,
            bucket: "test".into(),
            region: "us-east-1".into(),
            endpoint: Some("http://127.0.0.1:1".into()),
            allow_http: true,
            force_path_style: true,
            kms_key_id: None,
            instance_metadata_endpoint: None,
        };
        let scratch = Scratch::new("roundtrip");
        let written = scratch.path("credentials.json");
        write_credentials_file(&written, &credentials("AKIA", SystemTime::now())).expect("write");
        S3Store::new(config(written)).expect("the sidecar's file builds an S3Store");

        let malformed = scratch.path("malformed.json");
        fs::write(
            &malformed,
            br#"{"accessKeyId":"AKIA","secretAccessKey":"s"}"#,
        )
        .expect("write");
        assert!(
            S3Store::new(config(malformed)).is_err(),
            "a file in another shape must fail store construction"
        );
    }

    #[tokio::test]
    async fn once_mode_with_missing_environment_names_both_variables() {
        let args = PodIdentityArgs {
            output: Some(PathBuf::from("/nonexistent/credentials.json")),
            probe: None,
            credentials_uri: None,
            authorization_token_file: None,
            refresh_margin: DEFAULT_REFRESH_MARGIN,
            once: true,
        };
        let err = run(args).await.expect_err("refused");
        assert!(err.contains(ENV_CREDENTIALS_URI) && err.contains(ENV_AUTHORIZATION_TOKEN_FILE));
    }
}
