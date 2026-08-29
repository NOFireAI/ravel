//! Object store construction, sharing `RAVEL_S3_*` env vars with `ravel-server`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{S3AuthMode, S3Config, S3Store};
use ravel_object_store::{GetRange, ObjectStoreBackend};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StoreKind {
    Memory,
    #[value(name = "s3")]
    S3,
}

/// Which credential source `--store s3` uses (ADR-0106). The CLI-facing mirror
/// of [`S3AuthMode`], which lives in a crate that does not depend on clap.
/// Same flag name and values as ravel-server's `--s3-auth`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum S3Auth {
    /// Inline keys from `--s3-access-key`/`--s3-secret-key`, optionally with
    /// `--s3-session-token` or `--s3-credentials-file`. Both keys are
    /// required, exactly as before ADR-0106.
    #[default]
    Static,
    /// Short-lived credentials fetched from the EC2 instance metadata service
    /// (IMDSv2). No inline credential flag may be set alongside it.
    InstanceRole,
}

impl S3Auth {
    /// The library-level mode this flag value selects.
    pub fn mode(self) -> S3AuthMode {
        match self {
            S3Auth::Static => S3AuthMode::Static,
            S3Auth::InstanceRole => S3AuthMode::InstanceRole,
        }
    }
}

#[derive(Debug, Parser)]
pub struct StoreArgs {
    #[arg(long, value_enum, default_value = "memory")]
    pub store: StoreKind,

    #[arg(long, env = "RAVEL_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    #[arg(long, env = "RAVEL_S3_BUCKET")]
    pub s3_bucket: Option<String>,

    #[arg(long, env = "RAVEL_S3_REGION")]
    pub s3_region: Option<String>,

    #[arg(long, env = "RAVEL_S3_ACCESS_KEY")]
    pub s3_access_key: Option<String>,

    #[arg(long, env = "RAVEL_S3_SECRET_KEY")]
    pub s3_secret_key: Option<String>,

    /// Where `--store s3` gets its credentials (ADR-0106). `static` (the
    /// default) is unchanged behavior: `--s3-access-key` and
    /// `--s3-secret-key` are both required. `instance-role` drops that
    /// requirement and fetches short-lived credentials from the EC2 instance
    /// metadata service instead; combining it with any inline credential flag
    /// is refused rather than resolved by precedence.
    #[arg(long, value_enum, default_value = "static", env = "RAVEL_S3_AUTH")]
    pub s3_auth: S3Auth,

    /// Temporary AWS session token paired with `--s3-access-key` /
    /// `--s3-secret-key` for STS-issued credentials (ADR-0072 decision 1).
    /// Ignored when `--s3-credentials-file` is set: the file wins. Only
    /// meaningful under `--s3-auth static`.
    #[arg(long, env = "RAVEL_S3_SESSION_TOKEN")]
    pub s3_session_token: Option<String>,

    /// Path to a JSON file of `{access_key_id, secret_access_key,
    /// session_token}` that an external process rotates on disk (ADR-0072
    /// decision 1). Read once at construction (an unreadable or malformed
    /// file is an error) and re-read lazily on the request path when its
    /// mtime changes. Wins over the inline key flags. Only meaningful under
    /// `--s3-auth static`.
    #[arg(long, env = "RAVEL_S3_CREDENTIALS_FILE", value_name = "PATH")]
    pub s3_credentials_file: Option<PathBuf>,

    /// Base URL of the EC2 instance metadata service, used only under
    /// `--s3-auth instance-role` (ADR-0106). Unset uses the AWS link-local
    /// address; a value redirects IMDS for tests and unusual deployments.
    #[arg(long, env = "RAVEL_S3_INSTANCE_METADATA_ENDPOINT", value_name = "URL")]
    pub s3_instance_metadata_endpoint: Option<String>,
}

impl StoreArgs {
    /// Human-readable backend identity for display and for the
    /// `sys/qualification` record (ADR-0050 section 6): distinguishes which
    /// bucket/endpoint a qualification result belongs to, without leaking
    /// credentials.
    pub fn backend_identity(&self) -> String {
        match self.store {
            StoreKind::Memory => "memory".to_string(),
            StoreKind::S3 => {
                let bucket = self.s3_bucket.as_deref().unwrap_or("<unset>");
                match self.s3_endpoint.as_deref() {
                    Some(endpoint) => format!("s3://{bucket}@{endpoint}"),
                    None => format!("s3://{bucket}"),
                }
            }
        }
    }
}

/// The argument error for `--s3-auth instance-role` combined with an inline
/// credential (ADR-0106), or `None` when no inline credential is set.
///
/// `S3Store::new` rejects the same mix, but its message is written for the
/// `S3Config` field names. Operators set flags, so the CLI names the flags
/// (and the env var clap also reads each one from, since a stray exported
/// `RAVEL_S3_*` is the likelier source of the conflict). Kept identical to
/// ravel-server's message: the two binaries share these flags and env vars.
fn instance_role_credential_conflict(args: &StoreArgs) -> Option<anyhow::Error> {
    let conflicting: Vec<&str> = [
        (
            args.s3_access_key.is_some(),
            "--s3-access-key (RAVEL_S3_ACCESS_KEY)",
        ),
        (
            args.s3_secret_key.is_some(),
            "--s3-secret-key (RAVEL_S3_SECRET_KEY)",
        ),
        (
            args.s3_session_token.is_some(),
            "--s3-session-token (RAVEL_S3_SESSION_TOKEN)",
        ),
        (
            args.s3_credentials_file.is_some(),
            "--s3-credentials-file (RAVEL_S3_CREDENTIALS_FILE)",
        ),
    ]
    .into_iter()
    .filter_map(|(present, name)| present.then_some(name))
    .collect();
    if conflicting.is_empty() {
        return None;
    }
    Some(anyhow::anyhow!(
        "--s3-auth instance-role conflicts with {}: under instance-role every \
         credential comes from the EC2 instance metadata service, so those \
         must be unset (or select --s3-auth static)",
        conflicting.join(", ")
    ))
}

pub fn build_store(args: &StoreArgs) -> anyhow::Result<Arc<dyn ObjectStoreBackend>> {
    match args.store {
        StoreKind::Memory => Ok(Arc::new(MemoryStore::new())),
        StoreKind::S3 => {
            let bucket = args
                .s3_bucket
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--store s3 requires RAVEL_S3_BUCKET"))?;
            let region = args
                .s3_region
                .clone()
                .unwrap_or_else(|| "us-east-1".to_string());
            let auth = args.s3_auth.mode();
            let (access_key_id, secret_access_key, session_token, credentials_file) = match auth {
                S3AuthMode::Static => (
                    args.s3_access_key.clone().ok_or_else(|| {
                        anyhow::anyhow!("--store s3 requires RAVEL_S3_ACCESS_KEY")
                    })?,
                    args.s3_secret_key.clone().ok_or_else(|| {
                        anyhow::anyhow!("--store s3 requires RAVEL_S3_SECRET_KEY")
                    })?,
                    args.s3_session_token.clone(),
                    args.s3_credentials_file.clone(),
                ),
                S3AuthMode::InstanceRole => {
                    if let Some(conflict) = instance_role_credential_conflict(args) {
                        return Err(conflict);
                    }
                    (String::new(), String::new(), None, None)
                }
            };
            let endpoint = args.s3_endpoint.clone();
            let allow_http = endpoint.is_some();
            let config = S3Config {
                bucket,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
                allow_http,
                force_path_style: true,
                kms_key_id: None,
                session_token,
                credentials_file,
                auth,
                instance_metadata_endpoint: args.s3_instance_metadata_endpoint.clone(),
            };
            let store = S3Store::new(config)
                .map_err(|err| anyhow::anyhow!("failed to build S3 store: {err}"))?;
            Ok(Arc::new(store))
        }
    }
}

/// Reads `key_or_path` from the local filesystem if it names an existing
/// file, otherwise fetches it as a key from the configured object store.
pub async fn read_bytes(args: &StoreArgs, key_or_path: &str) -> anyhow::Result<Vec<u8>> {
    if Path::new(key_or_path).is_file() {
        return tokio::fs::read(key_or_path)
            .await
            .map_err(|err| anyhow::anyhow!("failed to read {key_or_path}: {err}"));
    }
    let store = build_store(args)?;
    let outcome = store
        .get(key_or_path, GetRange::Full)
        .await
        .map_err(|err| anyhow::anyhow!("failed to fetch {key_or_path}: {err}"))?;
    Ok(outcome.data.to_vec())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use clap::Parser;
    use ravel_object_store::PutOptions;

    /// `build_store`'s error text, panicking with `context` if it succeeded.
    /// `Arc<dyn ObjectStoreBackend>` is not `Debug`, so `expect_err` is
    /// unavailable.
    fn build_store_error(args: &StoreArgs, context: &str) -> String {
        match build_store(args) {
            Ok(_) => panic!("{context}"),
            Err(err) => err.to_string(),
        }
    }

    /// Stand up a minimal always-succeeding mock IMDSv2 on an ephemeral
    /// loopback port and return its `http://addr` base. Mirrors
    /// ravel-object-store's own `spawn_ok_imds`; the credential it hands out
    /// is what the mock S3 below expects to see on the wire.
    async fn spawn_mock_imds() -> String {
        use axum::Router;
        use axum::routing::{get, put};

        let app = Router::new()
            .route("/latest/api/token", put(|| async { "mock-token" }))
            .route(
                "/latest/meta-data/iam/security-credentials/",
                get(|| async { "ravel-role" }),
            )
            .route(
                "/latest/meta-data/iam/security-credentials/{role}",
                get(|| async {
                    r#"{"Code":"Success","AccessKeyId":"AKIA_IMDS",
                        "SecretAccessKey":"imds-secret","Token":"imds-token",
                        "Expiration":"2033-11-14T22:13:20Z"}"#
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let endpoint = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        endpoint
    }

    /// A path-style S3 endpoint over an in-memory map: one PUT and one GET,
    /// plus the SigV4 credential the client signed the PUT with, so a test can
    /// prove which credential source the constructed store actually used.
    #[derive(Default)]
    struct MockS3 {
        objects: std::sync::Mutex<std::collections::HashMap<String, Bytes>>,
        signed_with: std::sync::Mutex<Option<(String, String)>>,
    }

    async fn spawn_mock_s3() -> (String, Arc<MockS3>) {
        use axum::Router;
        use axum::extract::{Path as AxumPath, State};
        use axum::http::{HeaderMap, StatusCode, header};
        use axum::response::{IntoResponse, Response};
        use axum::routing::put;

        async fn put_object(
            State(state): State<Arc<MockS3>>,
            AxumPath((_bucket, key)): AxumPath<(String, String)>,
            headers: HeaderMap,
            body: Bytes,
        ) -> Response {
            let header_text = |name: &str| {
                headers
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_string()
            };
            *state.signed_with.lock().expect("signed_with lock") = Some((
                header_text("authorization"),
                header_text("x-amz-security-token"),
            ));
            state
                .objects
                .lock()
                .expect("objects lock")
                .insert(key, body);
            (StatusCode::OK, [(header::ETAG, "\"mock-etag\"")], "").into_response()
        }

        /// Serves `Range` the way a real S3-compatible endpoint does: 206 with
        /// a `Content-Range`, or 416 when no part of the range exists. The
        /// adapter reads a whole object as bounded ranged requests, so a mock
        /// that answered 200 here would be rejected as a non-partial response
        /// before its body was ever read.
        async fn get_object(
            State(state): State<Arc<MockS3>>,
            AxumPath((_bucket, key)): AxumPath<(String, String)>,
            headers: HeaderMap,
        ) -> Response {
            let found = state
                .objects
                .lock()
                .expect("objects lock")
                .get(&key)
                .cloned();
            let Some(data) = found else {
                return StatusCode::NOT_FOUND.into_response();
            };
            // All three RFC 7233 byte-range forms, not just the closed one:
            // `S3Store::get` emits `bytes=-N` for `GetRange::Suffix`, which is
            // how every footer in this codebase is read, so a mock that parses
            // only `bytes=A-B` fails such a request with 416 for a reason that
            // has nothing to do with the code under test.
            let requested = headers
                .get(header::RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(|spec| spec.strip_prefix("bytes=")?.split_once('-'))
                .map(|(start, end)| {
                    let len = data.len();
                    match (start.trim(), end.trim()) {
                        // bytes=-N: the last N bytes.
                        ("", n) => n.parse::<usize>().ok().and_then(|n| {
                            (n > 0 && len > 0).then(|| (len.saturating_sub(n), len - 1))
                        }),
                        // bytes=A-: from A through the end.
                        (s, "") => s
                            .parse::<usize>()
                            .ok()
                            .and_then(|s| (s < len).then(|| (s, len - 1))),
                        // bytes=A-B, inclusive, clamped to the object.
                        (s, e) => match (s.parse::<usize>(), e.parse::<usize>()) {
                            (Ok(s), Ok(e)) if s < len && s <= e => Some((s, e.min(len - 1))),
                            _ => None,
                        },
                    }
                });
            let (status, body, content_range) = match requested {
                None => (StatusCode::OK, data.clone(), None),
                Some(Some((start, end))) => (
                    StatusCode::PARTIAL_CONTENT,
                    data.slice(start..end + 1),
                    Some(format!("bytes {start}-{end}/{}", data.len())),
                ),
                Some(None) => return StatusCode::RANGE_NOT_SATISFIABLE.into_response(),
            };
            let mut response = (
                status,
                [
                    (header::ETAG, "\"mock-etag\"".to_string()),
                    (
                        header::LAST_MODIFIED,
                        "Wed, 21 Oct 2020 07:28:00 GMT".to_string(),
                    ),
                ],
                body,
            )
                .into_response();
            if let Some(value) = content_range
                && let Ok(value) = value.parse()
            {
                response.headers_mut().insert(header::CONTENT_RANGE, value);
            }
            response
        }

        let state = Arc::new(MockS3::default());
        let app = Router::new()
            .route("/{bucket}/{*key}", put(put_object).get(get_object))
            .with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let endpoint = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (endpoint, state)
    }

    /// The ADR-0106 reachability acceptance test for ravel-cli: parsed CLI args
    /// go through the real `build_store` entry point with `--s3-auth
    /// instance-role` and no keys anywhere, the credential comes from a mock
    /// IMDS reached via `--s3-instance-metadata-endpoint`, and the constructed
    /// backend serves a put/get round trip.
    ///
    /// Non-vacuous on the credential source, not just on "it built": the mock
    /// S3 records the `Authorization` and `x-amz-security-token` headers, and
    /// they must carry the key id and token the mock IMDS minted.
    ///
    /// `spawn_blocking`: `S3Store::new` blocks on the eager IMDS fetch, which
    /// has to reach the mock task on this same runtime.
    #[tokio::test(flavor = "multi_thread")]
    async fn instance_role_auth_builds_serving_store() {
        let imds = spawn_mock_imds().await;
        let (s3_endpoint, mock) = spawn_mock_s3().await;

        let args = StoreArgs::try_parse_from([
            "ravel-cli",
            "--store",
            "s3",
            "--s3-bucket",
            "ravel-test",
            "--s3-endpoint",
            &s3_endpoint,
            "--s3-auth",
            "instance-role",
            "--s3-instance-metadata-endpoint",
            &imds,
        ])
        .expect("instance-role flags parse");
        assert!(
            args.s3_access_key.is_none() && args.s3_secret_key.is_none(),
            "precondition: instance-role starts with no inline keys set"
        );

        let store = tokio::task::spawn_blocking(move || build_store(&args))
            .await
            .expect("join")
            .expect("instance-role build_store must construct against the mock IMDS");

        store
            .put("t/k", Bytes::from_static(b"hello"), PutOptions::default())
            .await
            .expect("put through the instance-role store");
        let got = store
            .get("t/k", GetRange::Full)
            .await
            .expect("get through the instance-role store");
        assert_eq!(
            got.data.as_ref(),
            b"hello",
            "the constructed backend must serve back what it stored"
        );

        let (authorization, token) = mock
            .signed_with
            .lock()
            .expect("signed_with lock")
            .clone()
            .expect("the mock S3 must have seen the signed PUT");
        assert!(
            authorization.contains("AKIA_IMDS"),
            "the request must be signed with the IMDS key id, got: {authorization}"
        );
        assert_eq!(
            token, "imds-token",
            "the request must carry the IMDS session token"
        );
    }

    /// `--s3-auth instance-role` plus any inline credential flag is an
    /// argument error naming both flags, refused before any IMDS contact.
    /// Same contract, same message as ravel-server's.
    #[test]
    fn instance_role_auth_rejects_inline_keys() {
        for (flag, value) in [
            ("--s3-access-key", "AKIA_INLINE"),
            ("--s3-secret-key", "inline-secret"),
            ("--s3-session-token", "inline-token"),
            ("--s3-credentials-file", "/nonexistent/creds.json"),
        ] {
            let args = StoreArgs::try_parse_from([
                "ravel-cli",
                "--store",
                "s3",
                "--s3-bucket",
                "ravel-test",
                "--s3-endpoint",
                "http://127.0.0.1:9000",
                "--s3-auth",
                "instance-role",
                flag,
                value,
            ])
            .expect("flags parse");

            let rendered = build_store_error(
                &args,
                &format!("instance-role plus {flag} must be an error"),
            );
            assert!(
                rendered.contains("--s3-auth instance-role") && rendered.contains(flag),
                "the error must name both conflicting flags, got: {rendered}"
            );
        }
    }

    /// `--s3-auth` defaults to `static`, and static mode still requires both
    /// keys with their exact pre-ADR-0106 messages: the new flags must not
    /// change any existing invocation.
    #[test]
    fn static_auth_is_the_default_and_still_requires_both_keys() {
        let base = [
            "ravel-cli",
            "--store",
            "s3",
            "--s3-bucket",
            "ravel-test",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
        ];
        let args = StoreArgs::try_parse_from(base).expect("flags parse");
        assert_eq!(
            args.s3_auth,
            S3Auth::Static,
            "--s3-auth must default to static"
        );
        assert!(args.s3_session_token.is_none() && args.s3_credentials_file.is_none());
        assert!(args.s3_instance_metadata_endpoint.is_none());

        assert_eq!(
            build_store_error(&args, "static mode without keys must fail"),
            "--store s3 requires RAVEL_S3_ACCESS_KEY",
            "the access-key error text must be unchanged"
        );

        let mut with_key = base.to_vec();
        with_key.extend(["--s3-access-key", "test"]);
        let args = StoreArgs::try_parse_from(with_key).expect("flags parse");
        assert_eq!(
            build_store_error(&args, "static mode without a secret key must fail"),
            "--store s3 requires RAVEL_S3_SECRET_KEY",
            "the secret-key error text must be unchanged"
        );
    }

    /// The ADR-0072 decision 1 flags reach `S3Config` rather than being parsed
    /// and dropped: `--s3-credentials-file` is read at construction, so a
    /// missing file fails the build naming that path, and a valid one builds.
    #[test]
    fn session_token_and_credentials_file_flags_reach_the_store() {
        use std::io::Write;

        let base = [
            "ravel-cli",
            "--store",
            "s3",
            "--s3-bucket",
            "ravel-test",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
            "--s3-access-key",
            "test",
            "--s3-secret-key",
            "test",
        ];

        let mut with_token = base.to_vec();
        with_token.extend(["--s3-session-token", "sts-token"]);
        let args = StoreArgs::try_parse_from(with_token).expect("flags parse");
        assert_eq!(args.s3_session_token.as_deref(), Some("sts-token"));
        build_store(&args).expect("a session token must not break construction");

        let mut missing_file = base.to_vec();
        missing_file.extend(["--s3-credentials-file", "/nonexistent/ravel-creds.json"]);
        let args = StoreArgs::try_parse_from(missing_file).expect("flags parse");
        let err = build_store_error(
            &args,
            "an unreadable --s3-credentials-file must fail construction",
        );
        assert!(
            err.contains("/nonexistent/ravel-creds.json"),
            "the error must name the path the flag carried, got: {err}"
        );

        let mut file = tempfile::NamedTempFile::new().expect("create temp credentials file");
        file.write_all(br#"{"access_key_id":"AKIA_FILE","secret_access_key":"file-secret"}"#)
            .expect("write temp file");
        let mut with_file = base.to_vec();
        let file_path = file.path().to_str().expect("temp path is valid utf-8");
        with_file.extend(["--s3-credentials-file", file_path]);
        let args = StoreArgs::try_parse_from(with_file).expect("flags parse");
        build_store(&args).expect("a readable --s3-credentials-file must build");
    }
}
