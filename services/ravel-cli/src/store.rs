//! Object store construction, sharing `RAVEL_S3_*` env vars with `ravel-server`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use ravel_object_store::conformance::{
    BucketControlPlane, BucketProtectionParams, BucketProtectionReport, ConditionState,
    ProtectionConditionId, probe_bucket_protection,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{
    S3AuthMode, S3Config, S3HttpConfig, S3Store, UploadIntegrity, resolve_s3_allow_http,
};
use ravel_object_store::{GetRange, KmsRoutingStore, ObjectStoreBackend, StoreMetrics};
use ravel_types::{TenantHash, TenantId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StoreKind {
    Memory,
    #[value(name = "s3")]
    S3,
}

impl StoreKind {
    /// The `--store` spelling of this backend, so a report names the store with
    /// the same word the flag accepts.
    pub const fn flag_value(self) -> &'static str {
        match self {
            StoreKind::Memory => "memory",
            StoreKind::S3 => "s3",
        }
    }
}

/// Which backend a command runs against, and whether the operator chose it.
///
/// An explicit `--store memory` and an omitted `--store` both resolve to the
/// in-process [`MemoryStore`], but they are different operator intents: the
/// first asked for the empty store, the second got it by fallback. Every
/// walk-shaped command over tenant data reports which one it is
/// ([`StoreSelection::header`]) and refuses a walk that reaches no data at all
/// on the fallback ([`require_tenant_data_present`]), because a walk over an
/// unchosen empty store reports zero counters and exit 0, which reads as a
/// healthy no-op (issue #1024).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreSelection {
    kind: StoreKind,
    defaulted: bool,
}

impl StoreSelection {
    /// The operator passed `--store <kind>`.
    pub const fn explicit(kind: StoreKind) -> Self {
        Self {
            kind,
            defaulted: false,
        }
    }

    /// No `--store` was given, so the command runs against the empty
    /// in-process memory store.
    pub const fn defaulted_memory() -> Self {
        Self {
            kind: StoreKind::Memory,
            defaulted: true,
        }
    }

    /// The backend this invocation runs against.
    pub const fn kind(self) -> StoreKind {
        self.kind
    }

    /// Whether this is the fallback memory store rather than a chosen one.
    pub const fn is_defaulted_memory(self) -> bool {
        self.defaulted && matches!(self.kind, StoreKind::Memory)
    }

    /// The `store:` line every walk-shaped command's report header carries, so
    /// the choice of backend is never invisible in the output an operator
    /// reads: `store: memory (default)`, `store: memory`, or `store: s3`.
    pub fn header(self) -> String {
        if self.defaulted {
            format!("store: {} (default)", self.kind.flag_value())
        } else {
            format!("store: {}", self.kind.flag_value())
        }
    }

    /// Print [`StoreSelection::header`] as the first line of a command's
    /// report.
    pub fn print_header(self) {
        println!("{}", self.header());
    }
}

impl Default for StoreSelection {
    /// The fallback, matching an omitted `--store`: the shape that has to be
    /// caught, never the safe one, so a report built without an explicit
    /// selection cannot claim the operator chose its store.
    fn default() -> Self {
        Self::defaulted_memory()
    }
}

/// A walk-shaped command found nothing to walk on a memory store the operator
/// never asked for (issue #1024).
///
/// Typed so the message names the situation and the remedy rather than
/// surfacing as zero counters and exit 0. The same emptiness under an explicit
/// `--store memory` is a successful zero-count report: the operator chose that
/// store, which is what every in-process test does.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error(
    "--store defaulted to memory, which holds no data for tenant {tenant:?}; {command} found no \
     {searched} there and would have reported a healthy zero-work result. Pass --store s3 (with \
     RAVEL_S3_BUCKET and its credentials) to run against the real bucket, or load data first. An \
     explicit --store memory keeps the zero-count report."
)]
pub struct DefaultedMemoryEmptyWalk {
    /// The subcommand as an operator types it, e.g. `maintain compact-tenant`.
    pub command: &'static str,
    /// The `--tenant` the walk was asked for.
    pub tenant: String,
    /// What the command looked for under the tenant prefix and did not find.
    pub searched: &'static str,
}

impl DefaultedMemoryEmptyWalk {
    /// The refusal for `command`, or `Ok(())` when the operator chose this
    /// store or the walk did reach something (`found > 0`).
    pub fn check(
        selection: StoreSelection,
        command: &'static str,
        tenant: &str,
        searched: &'static str,
        found: usize,
    ) -> Result<(), Self> {
        if found > 0 || !selection.is_defaulted_memory() {
            return Ok(());
        }
        Err(Self {
            command,
            tenant: tenant.to_string(),
            searched,
        })
    }
}

/// The precondition every walk-shaped command over tenant data runs before it
/// reports anything: on a defaulted memory store, a tenant prefix that holds
/// no object at all means the command was pointed at the empty in-process
/// store by fallback, so it refuses instead of walking nothing.
///
/// Costs one `list_delimited` and only when `--store` defaulted: a real
/// backend never pays for this check.
pub async fn require_tenant_data_present(
    selection: StoreSelection,
    store: &dyn ObjectStoreBackend,
    command: &'static str,
    tenant: &str,
    tenant_hash: &TenantHash,
) -> anyhow::Result<()> {
    if !selection.is_defaulted_memory() {
        return Ok(());
    }
    let prefix = format!("t/{}/", tenant_hash.to_hex());
    let listed = store
        .list_delimited(&prefix)
        .await
        .map_err(|err| anyhow::anyhow!("failed to list {prefix}: {err}"))?;
    let found = listed.objects.len() + listed.common_prefixes.len();
    DefaultedMemoryEmptyWalk::check(selection, command, tenant, "objects", found)?;
    Ok(())
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

/// Which server-verified checksum `--store s3` attaches to every PUT. The
/// CLI-facing mirror of [`UploadIntegrity`], with the same flag name, values
/// and default as ravel-server's `--s3-upload-integrity`. The library default
/// is `Off`; the CLI carries the `crc64nvme` default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum S3UploadIntegrity {
    /// Attach no checksum.
    #[value(name = "off")]
    Off,
    /// Attach `x-amz-checksum-crc64nvme`.
    #[default]
    #[value(name = "crc64nvme")]
    Crc64Nvme,
    /// Attach `x-amz-checksum-sha256`.
    #[value(name = "sha256")]
    Sha256,
}

impl S3UploadIntegrity {
    /// The library-level mode this flag value selects.
    pub fn mode(self) -> UploadIntegrity {
        match self {
            S3UploadIntegrity::Off => UploadIntegrity::Off,
            S3UploadIntegrity::Crc64Nvme => UploadIntegrity::Crc64Nvme,
            S3UploadIntegrity::Sha256 => UploadIntegrity::Sha256,
        }
    }

    /// The `--s3-upload-integrity` spelling of this value.
    pub const fn flag_value(self) -> &'static str {
        match self {
            S3UploadIntegrity::Off => "off",
            S3UploadIntegrity::Crc64Nvme => "crc64nvme",
            S3UploadIntegrity::Sha256 => "sha256",
        }
    }
}

#[derive(Debug, Parser)]
pub struct StoreArgs {
    /// Which object store to run against. Unset means `memory`, the empty
    /// in-process store: a walk-shaped command over tenant data then reports
    /// `store: memory (default)` in its header, and refuses a walk that
    /// reaches no data at all rather than reporting zero counters at exit 0.
    /// An explicit `--store memory` keeps that zero-count report.
    // `Option`-typed rather than `default_value = "memory"` (issue #1024), the
    // same treatment ravel-server's resolution-sensitive flags got: a defaulted
    // `StoreKind` cannot express "nobody asked", so an operator who meant the
    // real bucket and one who meant the empty store would be indistinguishable.
    #[arg(long, value_enum)]
    pub store: Option<StoreKind>,

    #[arg(long, env = "RAVEL_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    /// Accept a plaintext `http://` `--s3-endpoint` whose host is not
    /// loopback. The S3 client's `allow_http` follows the endpoint's scheme,
    /// and a plaintext endpoint on the network carries every object this
    /// command writes and reads, plus the credentials signing those requests,
    /// in the clear; the command refuses that combination unless this flag
    /// says the operator meant it. A loopback `http://` endpoint (the local
    /// RustFS every development launcher here points at) needs no flag, and an
    /// `https://` endpoint is unaffected. Same flag, env var, and rule as
    /// ravel-server's.
    // Same rule literally, not by convention: both binaries call
    // `ravel_object_store::s3::resolve_s3_allow_http`. Kept out of the doc
    // comment because clap renders it into `--help` and into the generated
    // flag reference, where a Rust path means nothing to an operator.
    #[arg(long, env = "RAVEL_S3_ALLOW_HTTP")]
    pub s3_allow_http: bool,

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

    /// Server-verified checksum every `--store s3` PUT carries. `crc64nvme`
    /// (the default) attaches `x-amz-checksum-crc64nvme` and `sha256` attaches
    /// `x-amz-checksum-sha256`: the endpoint verifies the body against it,
    /// rejects a PUT whose bytes do not match, and stores the checksum with
    /// the object. `off` attaches none. An endpoint that does not support the
    /// header fails the first write; `off` is the remedy there, at the cost of
    /// unverified commit records. Same flag, values, default, and env var as
    /// ravel-server's.
    #[arg(
        long,
        value_enum,
        default_value = "crc64nvme",
        env = "RAVEL_S3_UPLOAD_INTEGRITY"
    )]
    pub s3_upload_integrity: S3UploadIntegrity,

    /// Ask the endpoint to return the checksum it stored at upload
    /// (`x-amz-checksum-mode: ENABLED`), so a whole-object read is verified
    /// against it before the bytes are used. On by default; pass
    /// `--s3-request-stored-checksum=false` for an endpoint that rejects the
    /// header, and every whole-object read is then served unverified. Same
    /// flag, default, and env var as ravel-server's.
    #[arg(
        long,
        num_args = 0..=1,
        default_value_t = true,
        default_missing_value = "true",
        action = clap::ArgAction::Set,
        env = "RAVEL_S3_REQUEST_STORED_CHECKSUM"
    )]
    pub s3_request_stored_checksum: bool,
}

impl StoreArgs {
    /// The backend this invocation runs against: `--store` when given, memory
    /// otherwise.
    pub fn store_kind(&self) -> StoreKind {
        self.store.unwrap_or(StoreKind::Memory)
    }

    /// The same choice, carrying whether the operator made it, for the report
    /// header and the empty-walk refusal.
    pub fn selection(&self) -> StoreSelection {
        match self.store {
            Some(kind) => StoreSelection::explicit(kind),
            None => StoreSelection::defaulted_memory(),
        }
    }

    /// Human-readable backend identity for display and for the
    /// `sys/qualification` record (ADR-0050 section 6): distinguishes which
    /// bucket/endpoint a qualification result belongs to, without leaking
    /// credentials.
    pub fn backend_identity(&self) -> String {
        match self.store_kind() {
            StoreKind::Memory => "memory".to_string(),
            StoreKind::S3 => ravel_object_store::conformance::s3_backend_identity(
                self.s3_bucket.as_deref(),
                self.s3_endpoint.as_deref(),
            ),
        }
    }

    /// The HTTP client configuration `--store s3` builds its store with: the
    /// library's tuning, with the two checksum switches taken from the flags.
    pub fn s3_http_config(&self) -> S3HttpConfig {
        S3HttpConfig {
            upload_integrity: self.s3_upload_integrity.mode(),
            request_stored_checksum: self.s3_request_stored_checksum,
            ..S3HttpConfig::default()
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
    build_store_with_list_page_size(args, None)
}

/// Same as [`build_store`], with an explicit override for the store's list
/// page size (`None` keeps each backend's own default). Exists for `store
/// qualify`, which must declare the exact page size it built so the
/// conformance suite's cross-page probe can be told the real boundary to
/// cross ([`ravel_object_store::conformance::run_conformance_suite`]).
pub fn build_store_with_list_page_size(
    args: &StoreArgs,
    page_size: Option<usize>,
) -> anyhow::Result<Arc<dyn ObjectStoreBackend>> {
    Ok(build_store_handle(args, page_size)?.backend())
}

/// A built store that keeps the concrete [`S3Store`] when the backend is S3.
///
/// The bucket-protection probes answer from the concrete type: `S3Store`
/// reads the bucket's configuration, while the same store held as
/// `dyn ObjectStoreBackend` reports every condition unknown. `store qualify`
/// and `store verify-protection` take this so they reach the real answers;
/// every other command uses [`BuiltStore::backend`].
#[derive(Clone)]
pub enum BuiltStore {
    S3 {
        store: Arc<S3Store>,
        /// The checksum settings the store was built with, which the store
        /// itself does not expose.
        http: S3HttpConfig,
    },
    Other(Arc<dyn ObjectStoreBackend>),
}

impl BuiltStore {
    /// The data-plane handle every command uses.
    pub fn backend(&self) -> Arc<dyn ObjectStoreBackend> {
        match self {
            BuiltStore::S3 { store, .. } => Arc::clone(store) as Arc<dyn ObjectStoreBackend>,
            BuiltStore::Other(store) => Arc::clone(store),
        }
    }
}

/// The `S3Config` `--store s3` builds its store from. `kms_key_id` is `None`:
/// the default store is written under the bucket's default encryption, and
/// only [`build_tenant_data_store`] overrides it, per tenant.
fn s3_config(args: &StoreArgs) -> anyhow::Result<S3Config> {
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
            args.s3_access_key
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--store s3 requires RAVEL_S3_ACCESS_KEY"))?,
            args.s3_secret_key
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--store s3 requires RAVEL_S3_SECRET_KEY"))?,
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
    let allow_http = resolve_s3_allow_http(endpoint.as_deref(), args.s3_allow_http)?;
    Ok(S3Config {
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
    })
}

// `--tenant-kms-config`, taken by the commands that write tenant data under
// the Maintain credential: `maintain compact-bucket`, `maintain
// compact-tenant`, `maintain migrate` and `catalog fold`. No other command
// takes it, so the control records Admin writes stay under the bucket's
// default encryption.
#[derive(Debug, Clone, Default, clap::Args)]
pub struct TenantKmsArgs {
    /// Path to the per-tenant SSE-KMS file ravel-server reads from its own
    /// `--tenant-kms-config` (ADR-0062 decision 1): a TOML `[tenants]` table
    /// mapping tenant name to KMS key ARN. When the file names the command's
    /// `--tenant`, every data object this command writes under that tenant's
    /// `t/<tenant_hash>/` prefix is encrypted under that key. The tenant's
    /// key-epoch record `t/<tenant_hash>/enc` is a control record that only
    /// ravel-server's startup records a key in: when it is absent, or records
    /// a different current key, the command refuses before any write; start
    /// ravel-server with the file first. A tenant the file does not name is
    /// written under the bucket's default encryption, as ravel-server writes
    /// it. Requires `--store s3`. A dry run validates the file, reads the
    /// key-epoch record and refuses as the real run would, and writes
    /// nothing. Absent (the default): every write uses the bucket's default
    /// encryption.
    #[arg(
        long = "tenant-kms-config",
        env = "RAVEL_TENANT_KMS_CONFIG",
        value_name = "PATH"
    )]
    pub tenant_kms_config: Option<PathBuf>,
}

/// Read and validate `--tenant-kms-config`, with ravel-server's refusals and
/// messages: the flag needs `--store s3`, and an unreadable or invalid file is
/// an error naming the path.
fn load_tenant_kms_config(
    args: &StoreArgs,
    path: &Path,
) -> anyhow::Result<ravel_catalog::tenant_kms::TenantKmsConfig> {
    if args.store_kind() != StoreKind::S3 {
        anyhow::bail!(
            "--tenant-kms-config requires --store s3: KmsRoutingStore's per-tenant builder \
             always constructs a real S3Store, which --store memory has no S3Config to build \
             one from."
        );
    }
    let text = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("could not read --tenant-kms-config {path:?}: {e}"))?;
    ravel_catalog::tenant_kms::parse_tenant_kms_config(&text)
        .map_err(|e| anyhow::anyhow!("invalid --tenant-kms-config {}: {e}", path.display()))
}

/// The store a command that writes `tenant`'s data under the Maintain
/// credential runs against.
///
/// Without `--tenant-kms-config` this is [`build_store`], unchanged. With it,
/// the S3 store is wrapped in a [`KmsRoutingStore`] the way ravel-server's
/// `build_store` wraps its own, and the file's entry for `tenant` is applied
/// with [`KeyChangePolicy::Refuse`](ravel_catalog::tenant_kms::KeyChangePolicy::Refuse):
/// a record whose current key is the file's routes with no write, and an
/// absent record or one whose current key differs refuses the command before
/// any write, since only ravel-server's startup records a configured or
/// changed key (ADR-0062 decision 1b). A record holding only the bootstrap
/// epoch 0, a first configuration a server began and did not finish, is
/// completed. The key is then registered, so every later data write under
/// `t/<tenant_hash>/` is encrypted under it. Only `tenant`'s entry is applied:
/// this command writes no other tenant's data. A tenant the file does not
/// name routes nowhere, exactly as in ravel-server: its writes go to the
/// default store.
///
/// For a dry run the file is read and validated and the key-epoch record is
/// read and checked the same way, so a bad file, an absent record or a
/// differing key fails before the real run, and the same routing line is
/// written to `log`; nothing is written to the store and the plain store is
/// returned.
///
/// Must run after the tenant-hash scheme is installed, since it hashes
/// `tenant`.
pub async fn build_tenant_data_store(
    args: &StoreArgs,
    kms_args: &TenantKmsArgs,
    tenant: &str,
    dry_run: bool,
    now_ns: i64,
    log: &mut (dyn std::io::Write + Send),
) -> anyhow::Result<Arc<dyn ObjectStoreBackend>> {
    let Some(path) = kms_args.tenant_kms_config.as_deref() else {
        return build_store(args);
    };
    let config = load_tenant_kms_config(args, path)?;
    let tenant_id = TenantId::new(tenant);
    let only_this_tenant = config.restricted_to(&tenant_id);
    let routing_failed = |err: ravel_catalog::tenant_kms::TenantKmsError| {
        anyhow::anyhow!(
            "failed to configure per-tenant SSE-KMS routing (--tenant-kms-config): {err}"
        )
    };

    let store: Arc<dyn ObjectStoreBackend> = if dry_run {
        let plain = build_store(args)?;
        ravel_catalog::tenant_kms::check_tenant_kms_records(plain.as_ref(), &only_this_tenant)
            .await
            .map_err(routing_failed)?;
        plain
    } else {
        let s3 = s3_config(args)?;
        let http = args.s3_http_config();
        let metrics = Arc::new(StoreMetrics::default());
        let base =
            S3Store::with_http_config_and_metrics(s3.clone(), http.clone(), Arc::clone(&metrics))
                .map_err(|err| anyhow::anyhow!("failed to build S3 store: {err}"))?;
        let kms = Arc::new(KmsRoutingStore::new(
            Arc::new(base) as Arc<dyn ObjectStoreBackend>,
            s3,
            http,
            metrics,
        ));
        ravel_catalog::tenant_kms::configure_tenant_kms_with_policy(
            kms.as_ref(),
            kms.as_ref(),
            &only_this_tenant,
            now_ns,
            ravel_catalog::tenant_kms::KeyChangePolicy::Refuse,
        )
        .await
        .map_err(routing_failed)?;
        kms
    };
    match config.key_for(&tenant_id) {
        Some(key_arn) => writeln!(
            log,
            "tenant-kms: tenant {tenant:?} writes are encrypted under {key_arn}"
        )?,
        None => writeln!(
            log,
            "tenant-kms: --tenant-kms-config names no key for tenant {tenant:?}; its writes use \
             the bucket's default encryption, as ravel-server's do"
        )?,
    }
    Ok(store)
}

/// [`build_store_with_list_page_size`], keeping the concrete S3 store
/// ([`BuiltStore`]).
pub fn build_store_handle(
    args: &StoreArgs,
    page_size: Option<usize>,
) -> anyhow::Result<BuiltStore> {
    match args.store_kind() {
        StoreKind::Memory => Ok(BuiltStore::Other(Arc::new(match page_size {
            Some(n) => MemoryStore::with_page_size(n),
            None => MemoryStore::new(),
        }))),
        StoreKind::S3 => {
            let config = s3_config(args)?;
            let http = args.s3_http_config();
            let store = match page_size {
                None => S3Store::with_http_config(config, http.clone()),
                Some(n) => S3Store::with_http_config_and_page_size(config, http.clone(), n),
            }
            .map_err(|err| anyhow::anyhow!("failed to build S3 store: {err}"))?;
            Ok(BuiltStore::S3 {
                store: Arc::new(store),
                http,
            })
        }
    }
}

/// Reads `key_or_path` from the local filesystem if it names an existing
/// file, otherwise fetches it as a key from the configured object store. A
/// value that is neither, such as a missing absolute path, which the store
/// cannot address, fails with an [`std::io::ErrorKind::NotFound`] error that
/// says so, without building the store.
pub async fn read_bytes(args: &StoreArgs, key_or_path: &str) -> anyhow::Result<Vec<u8>> {
    if Path::new(key_or_path).is_file() {
        return tokio::fs::read(key_or_path)
            .await
            .map_err(|err| anyhow::anyhow!("failed to read {key_or_path}: {err}"));
    }
    if !ravel_object_store::is_addressable_key(key_or_path) {
        return Err(
            anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::NotFound)).context(
                format!("no file at {key_or_path:?}, and it is not an object store key"),
            ),
        );
    }
    let store = build_store(args)?;
    let outcome = store
        .get(key_or_path, GetRange::Full)
        .await
        .map_err(|err| anyhow::anyhow!("failed to fetch {key_or_path}: {err}"))?;
    Ok(outcome.data.to_vec())
}

/// `store verify-protection` exit code when every expected condition passed.
pub const VERIFY_PROTECTION_PASS: i32 = 0;
/// Exit code when any expected condition failed.
pub const VERIFY_PROTECTION_FAIL: i32 = 1;
/// Exit code when no expected condition failed but at least one could not be
/// verified, including a control plane that could not be reached.
pub const VERIFY_PROTECTION_UNKNOWN: i32 = 2;

/// What `store verify-protection` expects of the bucket: the deployment's own
/// choices, as its flags state them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtectionExpectations {
    /// `--expected-noncurrent-days`: the `E_v` the covering noncurrent-version
    /// expiration rule must carry.
    pub expected_noncurrent_days: u32,
    /// `--expect-replication`: `delete-marker-replication` is expected.
    pub expect_replication: bool,
}

/// What `store verify-protection` prints for `object-retention`, which it
/// does not check (ADR-1727 decision 4).
pub const OBJECT_RETENTION_NOT_CHECKED: &str =
    "not checked by this command, does not affect the exit code";

impl ProtectionExpectations {
    /// Whether condition `id` counts toward the exit code. Every condition is
    /// expected except `delete-marker-replication`, which a deployment opts
    /// into, and `object-retention`, which this command does not check.
    pub fn expects(&self, id: ProtectionConditionId) -> bool {
        match id {
            ProtectionConditionId::DeleteMarkerReplication => self.expect_replication,
            ProtectionConditionId::ObjectRetention => false,
            ProtectionConditionId::Versioning
            | ProtectionConditionId::NoncurrentExpiration
            | ProtectionConditionId::ExpiredDeleteMarker
            | ProtectionConditionId::AbortMultipart
            | ProtectionConditionId::RuleScope
            | ProtectionConditionId::NoForeignRule
            | ProtectionConditionId::ObjectLock => true,
        }
    }

    /// The control-plane parameters these expectations ask for. No object is
    /// sampled for retention.
    pub fn params(&self) -> BucketProtectionParams {
        BucketProtectionParams {
            expected_noncurrent_days: Some(self.expected_noncurrent_days),
            expect_replication: self.expect_replication,
            sample_object_retention: false,
            protected_retention_prefixes: Vec::new(),
        }
    }
}

/// What `store verify-protection` prints, and the code it exits with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyProtectionOutcome {
    /// One line per condition, then one summary line.
    pub lines: Vec<String>,
    pub exit_code: i32,
}

/// Run `store verify-protection` against `store`: read the bucket-protection
/// report and render it.
pub async fn verify_protection(
    store: &BuiltStore,
    expectations: ProtectionExpectations,
) -> VerifyProtectionOutcome {
    match store {
        BuiltStore::S3 { store, .. } => verify_protection_with(store.as_ref(), expectations).await,
        BuiltStore::Other(store) => verify_protection_with(store.as_ref(), expectations).await,
    }
}

/// [`verify_protection`] over any [`BucketControlPlane`].
pub async fn verify_protection_with<S: BucketControlPlane + ?Sized>(
    source: &S,
    expectations: ProtectionExpectations,
) -> VerifyProtectionOutcome {
    let report = probe_bucket_protection(source, &expectations.params()).await;
    render_verify_protection(&report, expectations)
}

/// Write `outcome`'s lines to `out` and flush it. A reader that closed the
/// pipe early (`| head`) is not an error: the exit code the outcome carries
/// stands rather than turning into a write failure.
pub fn write_verify_protection(
    out: &mut impl std::io::Write,
    outcome: &VerifyProtectionOutcome,
) -> std::io::Result<()> {
    let written = outcome
        .lines
        .iter()
        .try_for_each(|line| writeln!(out, "{line}"))
        .and_then(|()| out.flush());
    match written {
        Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        other => other,
    }
}

/// The outcome for a store that could not be built, so its control plane was
/// never reached: every expected condition is unknown, and the exit code is
/// [`VERIFY_PROTECTION_UNKNOWN`].
pub fn verify_protection_unreachable(
    err: &anyhow::Error,
    expectations: ProtectionExpectations,
) -> VerifyProtectionOutcome {
    let report = BucketProtectionReport::all_unknown(format!(
        "could not reach the bucket control plane: {err}"
    ));
    render_verify_protection(&report, expectations)
}

/// Render `report` as one line per condition plus a summary line, and pick the
/// exit code: [`VERIFY_PROTECTION_FAIL`] when any expected condition failed,
/// else [`VERIFY_PROTECTION_UNKNOWN`] when any expected condition is unknown,
/// else [`VERIFY_PROTECTION_PASS`]. A condition that is not expected is printed
/// and marked as such, and never moves the exit code. A condition the report
/// does not carry is unknown. `object-retention` is printed as not checked
/// whatever the report says of it.
pub fn render_verify_protection(
    report: &BucketProtectionReport,
    expectations: ProtectionExpectations,
) -> VerifyProtectionOutcome {
    let mut lines = Vec::with_capacity(ProtectionConditionId::ALL.len() + 1);
    let mut failed: Vec<&'static str> = Vec::new();
    let mut unknown: Vec<&'static str> = Vec::new();
    for id in ProtectionConditionId::ALL {
        if id == ProtectionConditionId::ObjectRetention {
            lines.push(format!(
                "{:<26} unknown {OBJECT_RETENTION_NOT_CHECKED}",
                id.id()
            ));
            continue;
        }
        let state = report.state(id).cloned().unwrap_or_else(|| {
            ConditionState::Unknown("missing from the bucket-protection report".to_string())
        });
        let expected = expectations.expects(id);
        if expected {
            match state {
                ConditionState::Pass => {}
                ConditionState::Fail(_) => failed.push(id.id()),
                ConditionState::Unknown(_) => unknown.push(id.id()),
            }
        }
        let detail = match (expected, state.detail()) {
            (true, detail) => detail.to_string(),
            (false, "") => "not expected, does not affect the exit code".to_string(),
            (false, detail) => format!("not expected, does not affect the exit code: {detail}"),
        };
        lines.push(if detail.is_empty() {
            format!("{:<26} {}", id.id(), state.verdict())
        } else {
            format!("{:<26} {:<7} {detail}", id.id(), state.verdict())
        });
    }
    let (summary, exit_code) = if !failed.is_empty() {
        let mut summary = format!("verify-protection: FAIL: failed: {}", failed.join(", "));
        if !unknown.is_empty() {
            summary.push_str(&format!("; could not verify: {}", unknown.join(", ")));
        }
        (summary, VERIFY_PROTECTION_FAIL)
    } else if !unknown.is_empty() {
        (
            format!(
                "verify-protection: UNKNOWN: could not verify: {}",
                unknown.join(", ")
            ),
            VERIFY_PROTECTION_UNKNOWN,
        )
    } else {
        (
            "verify-protection: PASS: every expected condition passed".to_string(),
            VERIFY_PROTECTION_PASS,
        )
    };
    lines.push(summary);
    VerifyProtectionOutcome { lines, exit_code }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use clap::Parser;
    use ravel_object_store::PutOptions;
    use ravel_object_store::s3::{S3EndpointRefusal, SchemelessS3Endpoint};

    /// `build_store`'s error text, panicking with `context` if it succeeded.
    /// `Arc<dyn ObjectStoreBackend>` is not `Debug`, so `expect_err` is
    /// unavailable.
    fn build_store_error(args: &StoreArgs, context: &str) -> String {
        match build_store(args) {
            Ok(_) => panic!("{context}"),
            Err(err) => err.to_string(),
        }
    }

    /// A missing absolute path is no store key: the store's path encoding
    /// drops its leading `/`. `read_bytes` reports it not found, and says it
    /// is not a key, rather than the store's refusal to address it.
    #[tokio::test]
    async fn read_bytes_of_a_missing_absolute_path_is_not_found() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("absent.rseg");
        let missing = missing.to_str().expect("utf8");
        assert!(missing.starts_with('/'), "{missing}");
        let args = StoreArgs::try_parse_from(["ravel-cli", "--store", "memory"]).expect("parse");
        let err = read_bytes(&args, missing).await.expect_err("missing");
        assert_eq!(
            err.downcast_ref::<std::io::Error>()
                .map(std::io::Error::kind),
            Some(std::io::ErrorKind::NotFound),
            "{err:#}"
        );
        assert_eq!(
            err.to_string(),
            format!("no file at {missing:?}, and it is not an object store key")
        );
        assert!(
            !format!("{err:#}").contains("is not addressable"),
            "{err:#}"
        );
        // A missing key the store can address still reaches the store.
        let err = read_bytes(&args, "absent.rseg").await.expect_err("missing");
        assert_eq!(
            format!("{err:#}"),
            "failed to fetch absent.rseg: object not found"
        );
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

    /// `--store s3` flags for `endpoint`, with `--s3-allow-http` when
    /// `allow_http`. Mirrors ravel-server's `s3_cli` helper so the two
    /// binaries' tests exercise the same invocation shape.
    fn s3_args(endpoint: &str, allow_http: bool) -> StoreArgs {
        let mut args = vec![
            "ravel-cli",
            "--store",
            "s3",
            "--s3-bucket",
            "ravel-test",
            "--s3-endpoint",
            endpoint,
            "--s3-access-key",
            "test",
            "--s3-secret-key",
            "test",
        ];
        if allow_http {
            args.push("--s3-allow-http");
        }
        StoreArgs::try_parse_from(args).expect("flags parse")
    }

    /// Issues #1707 and #1911, the CLI half. `ravel-cli` ships in the server
    /// image and the operator's store-qualification Job runs it against the
    /// cluster's bucket with the cluster's credentials before any server pod
    /// exists, so it obeys the same rules the server does: `allow_http` is
    /// decided by the endpoint's URL scheme, not by whether an endpoint was set
    /// at all; a plaintext endpoint on the network is refused unless the
    /// operator passes the flag; and an endpoint with no scheme at all is
    /// refused outright.
    ///
    /// Both layers are asserted: [`resolve_s3_allow_http`] for the value the
    /// S3 client is configured with (`build_store` exposes no way to read it
    /// back off the built `S3Store`), and `build_store` itself, the S3 client
    /// constructor every `ravel-cli` subcommand reaches, for the refusal.
    ///
    /// Non-vacuity (prove-the-test): restore `let allow_http =
    /// endpoint.is_some();` in [`build_store_with_list_page_size`] and the
    /// https case builds a plaintext-capable client while both refusals
    /// vanish.
    #[test]
    fn s3_endpoint_scheme_decides_allow_http_and_plaintext_non_loopback_needs_the_flag() {
        // https, and real AWS with no endpoint at all: never plaintext, so a
        // redirect or a misconfigured proxy cannot downgrade the connection.
        assert_eq!(
            resolve_s3_allow_http(Some("https://s3.us-east-1.amazonaws.com"), false),
            Ok(false),
            "an https endpoint must not enable allow_http"
        );
        assert_eq!(
            resolve_s3_allow_http(None, false),
            Ok(false),
            "no endpoint (real AWS) must not enable allow_http"
        );
        build_store(&s3_args("https://s3.us-east-1.amazonaws.com", false))
            .expect("an https endpoint must build");

        // Loopback plaintext: allowed, and unflagged. Every local-development
        // launcher in this repo depends on this staying true.
        for endpoint in [
            "http://127.0.0.1:9000",
            "http://localhost:9000",
            "http://[::1]:9000",
        ] {
            assert_eq!(
                resolve_s3_allow_http(Some(endpoint), false),
                Ok(true),
                "{endpoint} is loopback plaintext and must enable allow_http unflagged"
            );
            build_store(&s3_args(endpoint, false))
                .unwrap_or_else(|e| panic!("{endpoint} must build without the flag, got: {e}"));
        }

        // Plaintext to a host on the network: refused, and the error names the
        // flag that accepts it.
        let refusal = resolve_s3_allow_http(Some("http://rustfs:9000"), false)
            .expect_err("plaintext to a non-loopback host must be refused");
        assert_eq!(refusal.endpoint(), "http://rustfs:9000");
        let rendered = refusal.to_string();
        assert!(
            rendered.contains("--s3-allow-http"),
            "the refusal must name the flag that accepts it, got: {rendered}"
        );
        let err = build_store_error(
            &s3_args("http://rustfs:9000", false),
            "build_store must refuse plaintext to a non-loopback host",
        );
        assert!(
            err.contains("--s3-allow-http"),
            "build_store's refusal must name the flag, got: {err}"
        );

        // The same endpoint with the flag: accepted, and plaintext is on.
        assert_eq!(
            resolve_s3_allow_http(Some("http://rustfs:9000"), true),
            Ok(true),
            "--s3-allow-http must enable plaintext to a non-loopback host"
        );
        build_store(&s3_args("http://rustfs:9000", true))
            .expect("--s3-allow-http must let a plaintext non-loopback endpoint build");

        // Scheme and host name are case-insensitive, so an upper-case
        // spelling decides the same way as the lower-case one.
        for endpoint in ["http://LOCALHOST:9000", "HTTP://localhost:9000"] {
            assert_eq!(
                resolve_s3_allow_http(Some(endpoint), false),
                Ok(true),
                "{endpoint} is loopback plaintext however it is spelled"
            );
        }
        assert!(
            resolve_s3_allow_http(Some("HTTP://rustfs:9000"), false).is_err(),
            "an upper-case scheme must not skip the plaintext refusal"
        );

        // The authority ends at the first `/`, `?` or `#`: a query or fragment
        // carrying `@localhost` must not satisfy the loopback check.
        for endpoint in [
            "http://s3.example.com?x=@localhost",
            "http://s3.example.com#@localhost",
        ] {
            assert!(
                resolve_s3_allow_http(Some(endpoint), false).is_err(),
                "{endpoint} points at a host on the network and must be refused"
            );
        }

        // An endpoint with no scheme is refused (issue #1911): it is not a
        // usable URL, and accepting it only moved the failure into
        // `object_store`'s request signing, where the message names neither the
        // endpoint nor the flag. That failure is what the operator's
        // store-qualification Job hit as exit 101 from `ravel-cli store
        // qualify`. A host name containing "http" is still schemeless, and
        // `build_store` refuses both.
        for endpoint in ["rustfs:9000", "my-http-proxy:9000"] {
            let refusal = resolve_s3_allow_http(Some(endpoint), false)
                .expect_err("a schemeless endpoint must be refused");
            assert_eq!(
                refusal,
                S3EndpointRefusal::Schemeless(SchemelessS3Endpoint {
                    endpoint: endpoint.to_string(),
                }),
                "{endpoint} must be refused as schemeless"
            );
            let err = build_store_error(
                &s3_args(endpoint, false),
                "build_store must refuse a schemeless endpoint",
            );
            assert!(
                err.contains(endpoint) && err.contains("https://"),
                "build_store's refusal must name the endpoint and the fix, got: {err}"
            );
            // The plaintext flag accepts deliberate plaintext, not a missing
            // scheme: there is no usable URL for it to accept.
            assert!(
                resolve_s3_allow_http(Some(endpoint), true).is_err(),
                "{endpoint} must stay refused even with --s3-allow-http"
            );
        }
        // An upper-case scheme is a scheme, and must not be swept up with them.
        assert_eq!(
            resolve_s3_allow_http(Some("HTTPS://rustfs:9000"), false),
            Ok(false),
            "an upper-case https scheme is valid and must be accepted"
        );
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

    /// Issue #1024: `--store` is `Option`-typed, so an omitted flag and an
    /// explicit `--store memory` are distinguishable, and each parsed
    /// invocation renders the report header line that names its effective
    /// store. The s3 case is asserted here rather than against a live bucket:
    /// the header comes from the parsed flag, before any construction.
    ///
    /// Non-vacuity (prove-the-test): restore `default_value = "memory"` on the
    /// flag (making the field a plain `StoreKind`, so `selection()` can no
    /// longer see the difference) and the first two assertions collapse onto
    /// the same string.
    #[test]
    fn the_store_flag_distinguishes_unset_from_an_explicit_choice() {
        let unset = StoreArgs::try_parse_from(["ravel-cli"]).expect("no flags parse");
        assert_eq!(unset.store, None, "an omitted --store stays unset");
        assert_eq!(unset.selection(), StoreSelection::defaulted_memory());
        assert_eq!(unset.selection().header(), "store: memory (default)");
        assert!(unset.selection().is_defaulted_memory());
        // Unchanged behavior: the fallback is still the memory store.
        assert_eq!(unset.store_kind(), StoreKind::Memory);
        assert_eq!(unset.backend_identity(), "memory");
        build_store(&unset).expect("an unset --store still builds the memory store");

        let explicit_memory = StoreArgs::try_parse_from(["ravel-cli", "--store", "memory"])
            .expect("--store memory parses");
        assert_eq!(explicit_memory.store, Some(StoreKind::Memory));
        assert_eq!(
            explicit_memory.selection(),
            StoreSelection::explicit(StoreKind::Memory)
        );
        assert_eq!(explicit_memory.selection().header(), "store: memory");
        assert!(
            !explicit_memory.selection().is_defaulted_memory(),
            "a chosen memory store is never the defaulted one"
        );

        let s3 = StoreArgs::try_parse_from([
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
        ])
        .expect("s3 flags parse");
        assert_eq!(s3.selection().header(), "store: s3");
        assert!(!s3.selection().is_defaulted_memory());
        assert_eq!(
            s3.backend_identity(),
            "s3://ravel-test@http://127.0.0.1:9000"
        );
        build_store(&s3).expect("the s3-configured invocation still constructs");
    }

    /// The empty-walk precondition (issue #1024) fires only on the defaulted
    /// memory store with nothing under the tenant prefix, and it is silent for
    /// an explicit choice or for a prefix that holds anything at all.
    ///
    /// Non-vacuity (prove-the-test): drop the `!selection.is_defaulted_memory()`
    /// early return in `require_tenant_data_present` and the explicit-memory
    /// case below starts failing.
    #[tokio::test]
    async fn the_empty_walk_precondition_fires_only_on_the_defaulted_store() {
        let store = MemoryStore::new();
        let tenant = "cli-empty-walk";
        let tenant_hash = ravel_types::TenantId::new(tenant).hash();
        let defaulted = StoreSelection::defaulted_memory();
        let chosen = StoreSelection::explicit(StoreKind::Memory);

        let err =
            require_tenant_data_present(defaulted, &store, "maintain sweep", tenant, &tenant_hash)
                .await
                .expect_err("an empty tenant prefix on the defaulted store must refuse");
        assert_eq!(
            *err.downcast_ref::<DefaultedMemoryEmptyWalk>()
                .expect("typed DefaultedMemoryEmptyWalk"),
            DefaultedMemoryEmptyWalk {
                command: "maintain sweep",
                tenant: tenant.to_string(),
                searched: "objects",
            }
        );

        require_tenant_data_present(chosen, &store, "maintain sweep", tenant, &tenant_hash)
            .await
            .expect("an explicitly chosen empty memory store is not refused");

        // One object anywhere under the tenant prefix is data reached.
        store
            .put(
                &format!("t/{}/l/prov", tenant_hash.to_hex()),
                Bytes::from_static(b"prov"),
                PutOptions::default(),
            )
            .await
            .expect("seed one object under the tenant prefix");
        require_tenant_data_present(defaulted, &store, "maintain sweep", tenant, &tenant_hash)
            .await
            .expect("a tenant prefix that holds something is walked, not refused");
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

    // --- store verify-protection ---

    /// A control plane that serves a fixed report, every condition `Pass`
    /// except the ones a test overrides.
    struct FixtureSource(BucketProtectionReport);

    #[async_trait::async_trait]
    impl BucketControlPlane for FixtureSource {
        async fn bucket_protection_report(
            &self,
            _params: &BucketProtectionParams,
        ) -> BucketProtectionReport {
            self.0.clone()
        }
    }

    fn fixture(overrides: &[(ProtectionConditionId, ConditionState)]) -> FixtureSource {
        let mut states: std::collections::HashMap<_, _> = ProtectionConditionId::ALL
            .iter()
            .map(|id| (*id, ConditionState::Pass))
            .collect();
        states.extend(overrides.iter().cloned());
        FixtureSource(BucketProtectionReport::from_states(states))
    }

    fn fail(detail: &str) -> ConditionState {
        ConditionState::Fail(detail.to_string())
    }

    fn unknown(detail: &str) -> ConditionState {
        ConditionState::Unknown(detail.to_string())
    }

    const EXPECT_CORE: ProtectionExpectations = ProtectionExpectations {
        expected_noncurrent_days: 30,
        expect_replication: false,
    };

    const EXPECT_ALL: ProtectionExpectations = ProtectionExpectations {
        expected_noncurrent_days: 30,
        expect_replication: true,
    };

    const RETENTION_NOT_CHECKED_LINE: &str = "object-retention           unknown not checked by \
                                              this command, does not affect the exit code";

    /// ADR-1727 follow-up task 2's acceptance test: two expected conditions
    /// fail, and the command prints each on its own line with its detail,
    /// names both in the summary, and exits 1.
    #[tokio::test]
    async fn verify_protection_names_each_failed_condition() {
        let source = fixture(&[
            (
                ProtectionConditionId::NoncurrentExpiration,
                fail("rule \"ravel\": NoncurrentDays is 10, expected 30"),
            ),
            (
                ProtectionConditionId::ObjectLock,
                fail("Object Lock is not enabled on the bucket"),
            ),
            (
                ProtectionConditionId::DeleteMarkerReplication,
                unknown("replication is not expected"),
            ),
        ]);
        let outcome = verify_protection_with(&source, EXPECT_CORE).await;
        assert_eq!(
            outcome.lines,
            vec![
                "versioning                 pass",
                "noncurrent-expiration      fail    rule \"ravel\": NoncurrentDays is 10, expected 30",
                "expired-delete-marker      pass",
                "abort-multipart            pass",
                "rule-scope                 pass",
                "no-foreign-rule            pass",
                "delete-marker-replication  unknown not expected, does not affect the exit code: \
                 replication is not expected",
                "object-lock                fail    Object Lock is not enabled on the bucket",
                RETENTION_NOT_CHECKED_LINE,
                "verify-protection: FAIL: failed: noncurrent-expiration, object-lock",
            ]
        );
        assert_eq!(outcome.exit_code, VERIFY_PROTECTION_FAIL);
    }

    /// Exit 0 needs every expected condition `Pass`. An unknown the
    /// deployment did not opt into does not move it; the same unknown under
    /// its flag does. `object-retention` is printed as not checked whatever
    /// the report says of it, and never moves the exit code.
    #[tokio::test]
    async fn verify_protection_exits_0_only_when_every_expected_condition_passes() {
        let source = fixture(&[
            (
                ProtectionConditionId::DeleteMarkerReplication,
                unknown("replication is not expected"),
            ),
            (
                ProtectionConditionId::ObjectRetention,
                fail("sys/tenancy: no retention"),
            ),
        ]);
        let outcome = verify_protection_with(&source, EXPECT_CORE).await;
        assert_eq!(
            outcome.lines,
            vec![
                "versioning                 pass",
                "noncurrent-expiration      pass",
                "expired-delete-marker      pass",
                "abort-multipart            pass",
                "rule-scope                 pass",
                "no-foreign-rule            pass",
                "delete-marker-replication  unknown not expected, does not affect the exit code: \
                 replication is not expected",
                "object-lock                pass",
                RETENTION_NOT_CHECKED_LINE,
                "verify-protection: PASS: every expected condition passed",
            ]
        );
        assert_eq!(outcome.exit_code, VERIFY_PROTECTION_PASS);

        let expecting_replication = ProtectionExpectations {
            expect_replication: true,
            ..EXPECT_CORE
        };
        let outcome = verify_protection_with(&source, expecting_replication).await;
        assert_eq!(
            outcome.lines[6],
            "delete-marker-replication  unknown replication is not expected"
        );
        assert_eq!(
            outcome.lines.last().map(String::as_str),
            Some("verify-protection: UNKNOWN: could not verify: delete-marker-replication")
        );
        assert_eq!(outcome.exit_code, VERIFY_PROTECTION_UNKNOWN);

        let all = fixture(&[]);
        let outcome = verify_protection_with(&all, EXPECT_ALL).await;
        assert_eq!(
            outcome.exit_code, VERIFY_PROTECTION_PASS,
            "{:?}",
            outcome.lines
        );
    }

    /// Exit 1 wins over exit 2: a failure alongside an unknown exits 1, and
    /// the summary names both.
    #[tokio::test]
    async fn verify_protection_exits_1_when_any_expected_condition_fails() {
        let source = fixture(&[
            (
                ProtectionConditionId::NoForeignRule,
                fail("rule \"archive\" transitions t/ to GLACIER"),
            ),
            (
                ProtectionConditionId::RuleScope,
                unknown("rules cover t/ in a form that cannot be proven"),
            ),
        ]);
        let outcome = verify_protection_with(&source, EXPECT_CORE).await;
        assert_eq!(
            outcome.lines[4],
            "rule-scope                 unknown rules cover t/ in a form that cannot be proven"
        );
        assert_eq!(
            outcome.lines[5],
            "no-foreign-rule            fail    rule \"archive\" transitions t/ to GLACIER"
        );
        assert_eq!(
            outcome.lines.last().map(String::as_str),
            Some("verify-protection: FAIL: failed: no-foreign-rule; could not verify: rule-scope")
        );
        assert_eq!(outcome.exit_code, VERIFY_PROTECTION_FAIL);
    }

    /// Exit 2 for an expected unknown, for a control plane that could not be
    /// reached, and for a backend with no control plane: never exit 0 for
    /// "could not verify". `object-retention` is not among the unknowns.
    #[tokio::test]
    async fn verify_protection_exits_2_when_an_expected_condition_is_unknown() {
        let source = fixture(&[(
            ProtectionConditionId::Versioning,
            unknown("GET ?versioning: 403 AccessDenied"),
        )]);
        let outcome = verify_protection_with(&source, EXPECT_CORE).await;
        assert_eq!(
            outcome.lines[0],
            "versioning                 unknown GET ?versioning: 403 AccessDenied"
        );
        assert_eq!(
            outcome.lines.last().map(String::as_str),
            Some("verify-protection: UNKNOWN: could not verify: versioning")
        );
        assert_eq!(outcome.exit_code, VERIFY_PROTECTION_UNKNOWN);

        let unreachable = verify_protection_unreachable(
            &anyhow::anyhow!("--store s3 requires RAVEL_S3_BUCKET"),
            EXPECT_CORE,
        );
        assert_eq!(
            unreachable.lines[0],
            "versioning                 unknown could not reach the bucket control plane: \
             --store s3 requires RAVEL_S3_BUCKET"
        );
        assert_eq!(
            unreachable.lines.last().map(String::as_str),
            Some(
                "verify-protection: UNKNOWN: could not verify: versioning, \
                 noncurrent-expiration, expired-delete-marker, abort-multipart, rule-scope, \
                 no-foreign-rule, object-lock"
            )
        );
        assert_eq!(unreachable.exit_code, VERIFY_PROTECTION_UNKNOWN);

        let memory = BuiltStore::Other(Arc::new(MemoryStore::new()));
        let outcome = verify_protection(&memory, EXPECT_ALL).await;
        assert_eq!(outcome.exit_code, VERIFY_PROTECTION_UNKNOWN);
        assert_eq!(outcome.lines[8], RETENTION_NOT_CHECKED_LINE);
        assert_eq!(
            outcome.lines.last().map(String::as_str),
            Some(
                "verify-protection: UNKNOWN: could not verify: versioning, \
                 noncurrent-expiration, expired-delete-marker, abort-multipart, rule-scope, \
                 no-foreign-rule, delete-marker-replication, object-lock"
            )
        );
    }

    /// No object is sampled for retention: the control plane is asked for no
    /// retention sample and no protected prefix.
    #[tokio::test]
    async fn verify_protection_asks_the_control_plane_for_no_retention_sample() {
        struct Recording(std::sync::Mutex<Vec<BucketProtectionParams>>);

        #[async_trait::async_trait]
        impl BucketControlPlane for Recording {
            async fn bucket_protection_report(
                &self,
                params: &BucketProtectionParams,
            ) -> BucketProtectionReport {
                self.0.lock().expect("lock").push(params.clone());
                fixture(&[]).0
            }
        }

        let source = Recording(std::sync::Mutex::new(Vec::new()));
        let outcome = verify_protection_with(&source, EXPECT_ALL).await;
        assert_eq!(outcome.exit_code, VERIFY_PROTECTION_PASS);
        let asked = source.0.lock().expect("lock").clone();
        assert_eq!(asked.len(), 1);
        assert!(!asked[0].sample_object_retention, "{asked:?}");
        assert!(
            asked[0].protected_retention_prefixes.is_empty(),
            "{asked:?}"
        );
    }

    /// A writer that accepts `accept` bytes, then fails every write and flush
    /// with `kind`.
    struct ClosingWriter {
        accept: usize,
        kind: std::io::ErrorKind,
        written: Vec<u8>,
    }

    impl std::io::Write for ClosingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.written.len() + buf.len() > self.accept {
                return Err(self.kind.into());
            }
            self.written.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            if self.written.len() >= self.accept {
                return Err(self.kind.into());
            }
            Ok(())
        }
    }

    /// A reader that closed the pipe, on a write or on the flush, leaves the
    /// outcome's exit code standing; any other write error is still an error.
    #[test]
    fn a_closed_pipe_does_not_replace_the_verify_protection_exit_code() {
        let outcome = VerifyProtectionOutcome {
            lines: vec!["versioning                 pass".to_string(); 3],
            exit_code: VERIFY_PROTECTION_UNKNOWN,
        };
        let line_bytes = outcome.lines[0].len() + 1;
        for accept in [0, line_bytes, 3 * line_bytes] {
            let mut out = ClosingWriter {
                accept,
                kind: std::io::ErrorKind::BrokenPipe,
                written: Vec::new(),
            };
            write_verify_protection(&mut out, &outcome).unwrap_or_else(|err| {
                panic!("accept {accept}: a closed pipe is not an error: {err}")
            });
        }
        let mut out = ClosingWriter {
            accept: 0,
            kind: std::io::ErrorKind::PermissionDenied,
            written: Vec::new(),
        };
        assert!(write_verify_protection(&mut out, &outcome).is_err());
    }

    /// A report that does not carry an expected condition cannot pass: the
    /// condition prints as unknown and the command exits 2. A missing
    /// condition that is not expected does not move the exit code.
    #[test]
    fn a_condition_missing_from_the_report_is_unknown() {
        let mut report = fixture(&[]).0;
        report
            .conditions
            .retain(|entry| entry.id != ProtectionConditionId::ObjectLock);
        let outcome = render_verify_protection(&report, EXPECT_CORE);
        assert_eq!(
            outcome.lines[7],
            "object-lock                unknown missing from the bucket-protection report"
        );
        assert_eq!(
            outcome.lines.last().map(String::as_str),
            Some("verify-protection: UNKNOWN: could not verify: object-lock")
        );
        assert_eq!(outcome.exit_code, VERIFY_PROTECTION_UNKNOWN);

        let mut report = fixture(&[]).0;
        report
            .conditions
            .retain(|entry| entry.id != ProtectionConditionId::DeleteMarkerReplication);
        let outcome = render_verify_protection(&report, EXPECT_CORE);
        assert_eq!(
            outcome.lines[6],
            "delete-marker-replication  unknown not expected, does not affect the exit code: \
             missing from the bucket-protection report"
        );
        assert_eq!(outcome.exit_code, VERIFY_PROTECTION_PASS);
    }

    const COMPLIANT_LIFECYCLE: &str = "<LifecycleConfiguration><Rule><Status>Enabled</Status>\
         <Filter><Prefix></Prefix></Filter>\
         <NoncurrentVersionExpiration><NoncurrentDays>30</NoncurrentDays>\
         </NoncurrentVersionExpiration>\
         <Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>\
         <AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation>\
         </AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>";

    fn compliant_bucket() -> [(&'static str, axum::http::StatusCode, &'static str); 4] {
        use axum::http::StatusCode;
        [
            (
                "versioning",
                StatusCode::OK,
                "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
            ),
            ("lifecycle", StatusCode::OK, COMPLIANT_LIFECYCLE),
            (
                "replication",
                StatusCode::OK,
                "<ReplicationConfiguration><Rule><Status>Enabled</Status><Filter/>\
                 <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>\
                 </Rule></ReplicationConfiguration>",
            ),
            (
                "object-lock",
                StatusCode::OK,
                "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                 </ObjectLockConfiguration>",
            ),
        ]
    }

    /// `--store s3` reaches the concrete `S3Store`, so verify-protection reads
    /// the bucket: four control-plane GETs, every expected condition
    /// `Pass`, exit 0. The same store handed over as `dyn ObjectStoreBackend`
    /// reads nothing and reports every condition unknown, exit 2.
    #[tokio::test]
    async fn verify_protection_against_s3_reads_the_bucket_through_the_concrete_store() {
        let (endpoint, fake) =
            crate::fake_s3::spawn(crate::fake_s3::Echo::Stored, &compliant_bucket()).await;
        let built = build_store_handle(&s3_args(&endpoint, false), None).expect("s3 builds");
        assert!(matches!(built, BuiltStore::S3 { .. }));

        let expectations = ProtectionExpectations {
            expect_replication: true,
            ..EXPECT_CORE
        };
        let outcome = verify_protection(&built, expectations).await;
        assert_eq!(
            fake.control_plane(),
            vec!["versioning", "lifecycle", "replication", "object-lock"]
        );
        assert_eq!(
            outcome.lines[..8],
            [
                "versioning                 pass",
                "noncurrent-expiration      pass",
                "expired-delete-marker      pass",
                "abort-multipart            pass",
                "rule-scope                 pass",
                "no-foreign-rule            pass",
                "delete-marker-replication  pass",
                "object-lock                pass",
            ]
        );
        assert_eq!(outcome.lines[8], RETENTION_NOT_CHECKED_LINE);
        assert_eq!(
            outcome.lines[9],
            "verify-protection: PASS: every expected condition passed"
        );
        assert_eq!(outcome.exit_code, VERIFY_PROTECTION_PASS);

        let through_dyn = BuiltStore::Other(built.backend());
        let outcome = verify_protection(&through_dyn, expectations).await;
        assert!(
            outcome.lines[..9]
                .iter()
                .all(|line| line.contains(" unknown ")),
            "{:?}",
            outcome.lines
        );
        assert_eq!(outcome.exit_code, VERIFY_PROTECTION_UNKNOWN);
        assert_eq!(fake.control_plane().len(), 4, "the dyn path reads nothing");
    }

    // --- S3 checksum options ---

    /// Both checksum options parse with the server's names, values, defaults
    /// and environment variables, and reach the `S3HttpConfig` the store is
    /// built with.
    #[test]
    fn s3_checksum_options_parse_and_reach_the_http_config() {
        use clap::CommandFactory;

        let base = ["ravel-cli", "--store", "s3"];
        let parse = |extra: &[&str]| {
            let mut argv = base.to_vec();
            argv.extend_from_slice(extra);
            StoreArgs::try_parse_from(argv).expect("flags parse")
        };

        let defaults = parse(&[]);
        assert_eq!(defaults.s3_upload_integrity, S3UploadIntegrity::Crc64Nvme);
        assert!(defaults.s3_request_stored_checksum);
        let http = defaults.s3_http_config();
        assert_eq!(http.upload_integrity, UploadIntegrity::Crc64Nvme);
        assert!(http.request_stored_checksum);

        for (value, mode) in [
            ("off", UploadIntegrity::Off),
            ("crc64nvme", UploadIntegrity::Crc64Nvme),
            ("sha256", UploadIntegrity::Sha256),
        ] {
            let args = parse(&["--s3-upload-integrity", value]);
            assert_eq!(args.s3_upload_integrity.flag_value(), value);
            assert_eq!(args.s3_http_config().upload_integrity, mode, "{value}");
        }
        assert!(
            StoreArgs::try_parse_from(["ravel-cli", "--s3-upload-integrity", "crc32c"]).is_err(),
            "an unsupported algorithm is refused at parse time"
        );

        for (argv, expected) in [
            (&["--s3-request-stored-checksum=false"][..], false),
            (&["--s3-request-stored-checksum=true"][..], true),
            (&["--s3-request-stored-checksum"][..], true),
            (&["--s3-request-stored-checksum", "false"][..], false),
        ] {
            let args = parse(argv);
            assert_eq!(args.s3_request_stored_checksum, expected, "{argv:?}");
            assert_eq!(
                args.s3_http_config().request_stored_checksum,
                expected,
                "{argv:?}"
            );
        }

        let command = StoreArgs::command();
        let env_of = |id: &str| {
            command
                .get_arguments()
                .find(|arg| arg.get_id() == id)
                .and_then(|arg| arg.get_env())
                .and_then(|env| env.to_str())
                .map(str::to_string)
        };
        assert_eq!(
            env_of("s3_upload_integrity").as_deref(),
            Some("RAVEL_S3_UPLOAD_INTEGRITY")
        );
        assert_eq!(
            env_of("s3_request_stored_checksum").as_deref(),
            Some("RAVEL_S3_REQUEST_STORED_CHECKSUM")
        );
    }

    /// A PUT through `--store s3` carries `x-amz-checksum-crc64nvme` by
    /// default and no checksum under `--s3-upload-integrity off`.
    #[tokio::test]
    async fn s3_put_carries_the_selected_upload_checksum() {
        let (endpoint, fake) = crate::fake_s3::spawn(crate::fake_s3::Echo::Stored, &[]).await;
        let mut argv = vec![
            "ravel-cli",
            "--store",
            "s3",
            "--s3-bucket",
            "ravel-test",
            "--s3-endpoint",
            &endpoint,
            "--s3-access-key",
            "test",
            "--s3-secret-key",
            "test",
        ];
        let default_store =
            build_store(&StoreArgs::try_parse_from(argv.clone()).expect("parse")).expect("build");
        default_store
            .put(
                "t/default",
                Bytes::from_static(b"hello"),
                PutOptions::default(),
            )
            .await
            .expect("put");

        argv.extend(["--s3-upload-integrity", "off"]);
        let off_store =
            build_store(&StoreArgs::try_parse_from(argv).expect("parse")).expect("build");
        off_store
            .put("t/off", Bytes::from_static(b"hello"), PutOptions::default())
            .await
            .expect("put");

        let puts = fake.puts();
        assert_eq!(puts.len(), 2, "{puts:?}");
        assert_eq!(puts[0].key, "t/default");
        let digest: Vec<&str> = puts[0]
            .checksum_headers
            .iter()
            .filter(|(name, _)| name == "x-amz-checksum-crc64nvme")
            .map(|(_, value)| value.as_str())
            .collect();
        assert_eq!(digest.len(), 1, "{puts:?}");
        assert_eq!(puts[1].key, "t/off");
        assert!(
            puts[1]
                .checksum_headers
                .iter()
                .all(|(name, _)| name == "x-amz-checksum-mode"),
            "no checksum under off: {puts:?}"
        );
    }

    /// `store qualify --list-page-size 2 --store s3`, as the disaster recovery
    /// rehearsal runs it, builds under the default `crc64nvme`: the page size
    /// reaches the store, so listing three keys takes two LIST requests where
    /// the production page size takes one, and the PUTs still carry the
    /// checksum.
    #[tokio::test]
    async fn a_small_list_page_size_on_s3_keeps_the_upload_checksum() {
        let (endpoint, fake) = crate::fake_s3::spawn(crate::fake_s3::Echo::Stored, &[]).await;
        let args = s3_args(&endpoint, false);
        assert_eq!(args.s3_upload_integrity, S3UploadIntegrity::Crc64Nvme);
        let built = build_store_handle(&args, Some(2))
            .expect("--list-page-size 2 builds under the default upload integrity");
        assert!(matches!(built, BuiltStore::S3 { .. }));
        let store = built.backend();
        for key in ["t/pages/a", "t/pages/b", "t/pages/c"] {
            store
                .put(key, Bytes::from_static(b"page"), PutOptions::default())
                .await
                .expect("put");
        }

        let listed = ravel_object_store::list_all(store.as_ref(), "t/pages/")
            .await
            .expect("list");
        let keys: Vec<&str> = listed.iter().map(|meta| meta.key.as_str()).collect();
        assert_eq!(keys, ["t/pages/a", "t/pages/b", "t/pages/c"]);
        let starts: Vec<Option<String>> = fake
            .lists()
            .into_iter()
            .map(|list| list.start_after)
            .collect();
        assert_eq!(starts, [None, Some("t/pages/b".to_string())]);

        let puts = fake.puts();
        assert_eq!(puts.len(), 3, "{puts:?}");
        for put in &puts {
            assert_eq!(
                put.checksum_headers
                    .iter()
                    .filter(|(name, _)| name == "x-amz-checksum-crc64nvme")
                    .count(),
                1,
                "{puts:?}"
            );
        }

        let production = build_store_handle(&args, Some(ravel_object_store::s3::LIST_PAGE_SIZE))
            .expect("the production page size builds")
            .backend();
        ravel_object_store::list_all(production.as_ref(), "t/pages/")
            .await
            .expect("list");
        assert_eq!(
            fake.lists().len(),
            3,
            "one more LIST at the production size"
        );
    }
}
