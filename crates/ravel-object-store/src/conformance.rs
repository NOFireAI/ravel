//! Empirical backend qualification (ADR-0050 section 6, adversarial review
//! finding S5-20, docs/object-store-contract.md "Semantics adapters MUST
//! honor").
//!
//! Ravel's commit protocol and catalog assume two properties of the backing
//! store that nothing previously checked at runtime: conditional writes
//! (`CreateIfAbsent` and `CasVersion` reject a losing writer without
//! applying it) and strong consistency (a `get`/`list` issued right after a
//! `put` always observes it). A backend can report `Capabilities{ .. }`
//! honestly or dishonestly; either way, [`run_conformance_suite`] exercises
//! the real behavior instead of trusting the self-report.
//!
//! The suite can only falsify these properties, never prove them: a pass
//! means the backend did not fail any probe run against it here and now, not
//! that it is correct under every load and network condition it will ever
//! see in production. Treat a pass as qualification, not proof.
//!
//! Every probe reports which specific [`Property`] it tested, pass or fail,
//! and a human-readable detail: an operator (or `ravel-cli store qualify`)
//! must be able to tell "this backend cannot do conditional writes" from
//! "this backend's listing is eventually consistent" rather than a single
//! opaque failure.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, list_all};

/// Version of the probe set itself, recorded alongside a pass in
/// `sys/qualification` (ADR-0050 section 6). Bump this whenever a probe is
/// added, tightened, or its pass criteria changes, so an old qualification
/// record can be told apart from one taken under the current suite.
pub const CONFORMANCE_SUITE_VERSION: u32 = 1;

/// Root-prefix key for the durable qualification record (ADR-0050 section 6,
/// "New durable objects and key-layout entries": root prefix `sys/`).
///
/// Relocated here from `ravel-cli`'s `qualify` module so the two crates that
/// need it -- `ravel-cli store qualify` (the writer) and `ravel-server` startup
/// (the reader, ADR-0050 section 6 enforcement, EC7) -- share one definition
/// rather than each declaring their own. Neither depends on the other, and
/// this module already owns [`CONFORMANCE_SUITE_VERSION`], the record's most
/// load-bearing field, so it is the natural shared home.
pub const QUALIFICATION_KEY: &str = "sys/qualification";

/// Durable record written to [`QUALIFICATION_KEY`] on a passing qualification
/// run. JSON, not protobuf: `proto/ravel/sys.proto` (which ADR-0050 section 6
/// names for the eventual durable `sys/*` messages) is out of scope; the field
/// names and JSON shape here are a frozen contract so an already-written
/// `sys/qualification` object from before this relocation still decodes
/// byte-for-byte.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualificationRecord {
    /// The [`CONFORMANCE_SUITE_VERSION`] the passing run was recorded under.
    /// Server startup refuses when this is below the running binary's required
    /// floor (ADR-0050 section 6).
    pub suite_version: u32,
    pub backend_identity: String,
    pub qualified_unix_ns: i64,
    pub passed_properties: Vec<String>,
}

/// One property the object store contract requires, named so a failure
/// report can point at exactly what a backend cannot do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Property {
    /// `PutMode::CreateIfAbsent`: a second create on an existing key must
    /// fail with `AlreadyExists` and must not apply its bytes.
    ConditionalWriteCreateIfAbsent,
    /// `PutMode::CasVersion`: a put against a stale version must fail with
    /// `PreconditionFailed` and must not apply its bytes.
    ConditionalWriteCasVersion,
    /// A `get` of a key immediately after the `put` that created it must
    /// return exactly those bytes, every time.
    ConsistentReadAfterWrite,
    /// A `list`/`list_all` of a key's prefix immediately after the `put`
    /// that created it must include that key, every time.
    ConsistentListAfterWrite,
}

impl Property {
    /// Stable, greppable identifier -- this is what lands in CLI output and
    /// the qualification record, so an operator can search the contract doc
    /// and this module for the exact string.
    pub fn name(&self) -> &'static str {
        match self {
            Property::ConditionalWriteCreateIfAbsent => "conditional_write_create_if_absent",
            Property::ConditionalWriteCasVersion => "conditional_write_cas_version",
            Property::ConsistentReadAfterWrite => "consistent_read_after_write",
            Property::ConsistentListAfterWrite => "consistent_list_after_write",
        }
    }
}

/// Outcome of probing one [`Property`].
#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub property: Property,
    pub passed: bool,
    /// Human-readable explanation: on failure, what was observed and why it
    /// violates the property; on pass, what was exercised.
    pub detail: String,
}

impl ProbeResult {
    fn pass(property: Property, detail: impl Into<String>) -> Self {
        ProbeResult {
            property,
            passed: true,
            detail: detail.into(),
        }
    }

    fn fail(property: Property, detail: impl Into<String>) -> Self {
        ProbeResult {
            property,
            passed: false,
            detail: detail.into(),
        }
    }
}

/// Result of a full conformance run: one [`ProbeResult`] per [`Property`].
#[derive(Debug, Clone)]
pub struct ConformanceReport {
    pub results: Vec<ProbeResult>,
}

impl ConformanceReport {
    /// Whether every probed property passed. A backend qualifies only when
    /// this is true.
    pub fn passed(&self) -> bool {
        self.results.iter().all(|r| r.passed)
    }

    /// The properties that failed, in probe order.
    pub fn failures(&self) -> impl Iterator<Item = &ProbeResult> {
        self.results.iter().filter(|r| !r.passed)
    }
}

/// Informational bucket-protection signal (ADR-0055 section 3, citing ADR-0042
/// decision 3). Reports whether the target bucket *appears* to have S3 Object
/// Lock or bucket versioning enabled, so an operator gets a startup-adjacent
/// signal about the WORM/deny-delete gap instead of discovering it during an
/// incident.
///
/// This is **informational only**. It never contributes to
/// [`ConformanceReport::passed`], never changes what `ravel-cli store qualify`
/// records in `sys/qualification`, and never gates server startup: the
/// mandatory ADR-0050 section 6 checks (record present, `suite_version` at or
/// above the binary floor) are completely independent of it, in every state.
/// `object_store` 0.14 exposes no per-PUT Object Lock / versioning API, and
/// ADR-0042 decision 3 reserved a real Object Lock capability for its own
/// trait-extending ADR, so this probe cannot become an enforcement point
/// without contradicting an already-accepted decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectLockStatus {
    /// The backend affirmatively reports Object Lock and/or versioning enabled.
    Enabled,
    /// The backend affirmatively reports neither Object Lock nor versioning.
    Disabled,
    /// The backend cannot answer: no API exists for it (the S3 adapter and
    /// [`crate::memory::MemoryStore`] today), the credential is denied the
    /// configuration read, or the backend kind does not model it at all. This
    /// is not an error and not a qualification failure -- it is the honest
    /// default per ADR-0055 section 3.
    Unknown,
}

impl ObjectLockStatus {
    /// Stable, greppable identifier -- this is what lands in `ravel-cli store
    /// qualify`'s output, so an operator can search for it.
    pub fn name(&self) -> &'static str {
        match self {
            ObjectLockStatus::Enabled => "enabled",
            ObjectLockStatus::Disabled => "disabled",
            ObjectLockStatus::Unknown => "unknown",
        }
    }
}

/// Outcome of the informational Object Lock / versioning probe: an
/// [`ObjectLockStatus`] plus a human-readable explanation of how it was
/// determined (or why it is unknown).
#[derive(Debug, Clone)]
pub struct ObjectLockProbe {
    pub status: ObjectLockStatus,
    pub detail: String,
}

impl ObjectLockProbe {
    /// The honest default: the backend could not answer. Not an error.
    pub fn unknown(detail: impl Into<String>) -> Self {
        ObjectLockProbe {
            status: ObjectLockStatus::Unknown,
            detail: detail.into(),
        }
    }

    /// The backend affirmatively reported Object Lock / versioning enabled.
    pub fn enabled(detail: impl Into<String>) -> Self {
        ObjectLockProbe {
            status: ObjectLockStatus::Enabled,
            detail: detail.into(),
        }
    }

    /// The backend affirmatively reported no Object Lock / versioning.
    pub fn disabled(detail: impl Into<String>) -> Self {
        ObjectLockProbe {
            status: ObjectLockStatus::Disabled,
            detail: detail.into(),
        }
    }
}

/// Source of the informational Object Lock / versioning signal, kept
/// deliberately **separate from [`ObjectStoreBackend`]**.
///
/// ADR-0042 decision 3 (reaffirmed by ADR-0055 section 3) reserves any real
/// Object Lock capability on the backend trait for its own capability-gated,
/// trait-extending ADR, following the existing `Capabilities` pattern. Adding a
/// method to [`ObjectStoreBackend`] for this informational task would pre-empt
/// that decision, so this trait is a separate seam instead. Every production
/// backend reports [`ObjectLockStatus::Unknown`] through it today, because
/// `object_store` 0.14 has no Object Lock / versioning query and this crate
/// never opens a second, direct-SDK side channel (an ADR-0042 rejected
/// alternative). Test fixtures implement it to represent enabled/disabled
/// buckets so the reporting path is exercised for every state.
#[async_trait::async_trait]
pub trait ObjectLockProbeSource {
    async fn object_lock_status(&self) -> ObjectLockProbe;
}

/// The production path: a store reached only through the [`ObjectStoreBackend`]
/// contract cannot answer, so the probe is always [`ObjectLockStatus::Unknown`]
/// here. Implemented on the trait object itself (not via a blanket `impl`) so
/// `ravel-cli store qualify`, which holds an `Arc<dyn ObjectStoreBackend>`, can
/// probe without threading a concrete type, while test fixtures remain free to
/// implement the trait for their own concrete types to report other states.
#[async_trait::async_trait]
impl ObjectLockProbeSource for dyn ObjectStoreBackend {
    async fn object_lock_status(&self) -> ObjectLockProbe {
        ObjectLockProbe::unknown(
            "the ObjectStoreBackend contract exposes no Object Lock / versioning query, and \
             object_store 0.14 has no API for one; a real probe needs its own trait-extending \
             ADR (ADR-0042 decision 3). Reporting unknown is the honest, non-blocking default \
             (ADR-0055 section 3): Object Lock / versioning may or may not be enabled at the \
             bucket level out of band",
        )
    }
}

/// Run the informational Object Lock / versioning probe against `source`.
///
/// Never fails, never panics, and never affects qualification: it returns an
/// [`ObjectLockProbe`] whose status the caller prints for the operator and
/// otherwise ignores when deciding pass/fail (ADR-0055 section 3). Kept
/// separate from [`run_conformance_suite`] precisely so a reader can see it is
/// not one of the gating properties.
pub async fn probe_object_lock<S: ObjectLockProbeSource + ?Sized>(source: &S) -> ObjectLockProbe {
    source.object_lock_status().await
}

// --- Required bucket configuration probe (ADR-0064 section 7, S2-16, S4-12) ---
//
// ADR-0064 makes bucket versioning and lifecycle rules a normative contract:
// versioning OFF unless paired with a noncurrent-version expiration rule, the
// `AbortIncompleteMultipartUpload` rule recommended, and no other expiration
// rule on any Ravel prefix. Like the Object Lock probe above, this is
// informational-plus-alarming, never startup-blocking: `object_store` 0.14 has
// no bucket-policy query, so a real backend reports every field `Unknown`
// through the trait contract, and this crate never opens a second, direct-SDK
// side channel (an ADR-0042 rejected alternative). Test fixtures implement the
// source to represent compliant and non-compliant buckets so the reporting and
// alarm paths are exercised for every state.

/// Whether a bucket appears to have object versioning enabled (ADR-0064 §7
/// point 1). A separate, three-valued signal rather than folding into
/// [`ObjectLockStatus`]: erasure guarantees depend specifically on versioning,
/// and the required lifecycle rule below is only meaningful when it is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersioningStatus {
    /// The backend affirmatively reports object versioning enabled.
    On,
    /// The backend affirmatively reports object versioning disabled.
    Off,
    /// The backend cannot answer (the trait contract exposes no query; the
    /// honest default, exactly as [`ObjectLockStatus::Unknown`]).
    Unknown,
}

impl VersioningStatus {
    /// Stable, greppable identifier for CLI output.
    pub fn name(&self) -> &'static str {
        match self {
            VersioningStatus::On => "on",
            VersioningStatus::Off => "off",
            VersioningStatus::Unknown => "unknown",
        }
    }
}

/// Whether one of ADR-0064 §7's sanctioned lifecycle rules appears configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleRuleStatus {
    /// The backend affirmatively reports the rule present.
    Present,
    /// The backend affirmatively reports the rule absent.
    Absent,
    /// The backend cannot answer (the honest default through the trait
    /// contract).
    Unknown,
}

impl LifecycleRuleStatus {
    /// Stable, greppable identifier for CLI output.
    pub fn name(&self) -> &'static str {
        match self {
            LifecycleRuleStatus::Present => "present",
            LifecycleRuleStatus::Absent => "absent",
            LifecycleRuleStatus::Unknown => "unknown",
        }
    }
}

/// Observed (or unknown) required-bucket-configuration state (ADR-0064 §7).
/// Reports the three settings the ADR names: object versioning, the
/// `AbortIncompleteMultipartUpload` lifecycle rule (recommended), and the
/// noncurrent-version expiration rule (required only when versioning is on).
#[derive(Debug, Clone)]
pub struct BucketConfigProbe {
    pub versioning: VersioningStatus,
    pub abort_incomplete_multipart_upload: LifecycleRuleStatus,
    pub noncurrent_version_expiration: LifecycleRuleStatus,
    /// Human-readable explanation of how the state was determined (or why it
    /// is unknown).
    pub detail: String,
}

impl BucketConfigProbe {
    /// The honest default: the backend could not answer any field. Not an
    /// error, and never a qualification failure.
    pub fn unknown(detail: impl Into<String>) -> Self {
        BucketConfigProbe {
            versioning: VersioningStatus::Unknown,
            abort_incomplete_multipart_upload: LifecycleRuleStatus::Unknown,
            noncurrent_version_expiration: LifecycleRuleStatus::Unknown,
            detail: detail.into(),
        }
    }
}

/// Assess a [`BucketConfigProbe`] against ADR-0064 §7 and return one alarm
/// string per observed contract violation, most-severe first. Informational
/// only: the caller prints these for the operator and never blocks on them.
///
/// An `Unknown` field raises no alarm: the platform cannot see the setting, so
/// it can neither confirm nor deny a violation (ADR-0055 §3's honest-gap
/// framing). Only an affirmative observation of a non-compliant state alarms.
pub fn bucket_config_alarms(probe: &BucketConfigProbe) -> Vec<String> {
    let mut alarms = Vec::new();
    // The load-bearing one (S4-12): versioning on without a noncurrent-version
    // expiration rule silently converts every Ravel delete into a soft delete,
    // inverting every deletion guarantee in the system. ADR-0064 §7 point 1
    // calls this an unsupported configuration.
    if probe.versioning == VersioningStatus::On
        && probe.noncurrent_version_expiration == LifecycleRuleStatus::Absent
    {
        alarms.push(
            "ALARM: object versioning is enabled but no noncurrent-version expiration rule is \
             configured. This silently converts every Ravel delete (retention, sweep, and \
             ADR-0064 erasure) into a soft delete, inverting every deletion guarantee, and is an \
             unsupported configuration (ADR-0064 §7 point 1). Configure noncurrent-version \
             expiration plus expired-delete-marker cleanup on all t/ prefixes, or disable \
             versioning."
                .to_string(),
        );
    }
    // Recommended, not required: the abort-incomplete-multipart rule
    // (ADR-0064 §7 point 3; also converts S5-19's undocumented dependency into
    // a documented one).
    if probe.abort_incomplete_multipart_upload == LifecycleRuleStatus::Absent {
        alarms.push(
            "NOTE: the recommended AbortIncompleteMultipartUpload lifecycle rule (7 days) is not \
             configured (ADR-0064 §7 point 3). Incomplete multipart uploads will accumulate."
                .to_string(),
        );
    }
    alarms
}

/// Source of the informational required-bucket-configuration signal, kept
/// **separate from [`ObjectStoreBackend`]** for the same reason
/// [`ObjectLockProbeSource`] is: a real bucket-policy capability belongs to its
/// own trait-extending ADR (ADR-0042 decision 3), and `object_store` 0.14 has
/// no query for it. Every production backend reports `Unknown` through the dyn
/// impl below; test fixtures implement it to represent compliant and
/// non-compliant buckets.
#[async_trait::async_trait]
pub trait BucketConfigProbeSource {
    async fn bucket_config(&self) -> BucketConfigProbe;
}

/// The production path: a store reached only through the [`ObjectStoreBackend`]
/// contract cannot answer, so every field is `Unknown`. Implemented on the
/// trait object itself so `ravel-cli store qualify`, holding an
/// `Arc<dyn ObjectStoreBackend>`, can probe without threading a concrete type.
#[async_trait::async_trait]
impl BucketConfigProbeSource for dyn ObjectStoreBackend {
    async fn bucket_config(&self) -> BucketConfigProbe {
        BucketConfigProbe::unknown(
            "the ObjectStoreBackend contract exposes no bucket versioning / lifecycle-rule query, \
             and object_store 0.14 has no API for one; a real probe needs its own trait-extending \
             ADR (ADR-0042 decision 3). Reporting unknown is the honest, non-blocking default \
             (ADR-0055 section 3, ADR-0064 §7): the operator must confirm the required \
             configuration out of band",
        )
    }
}

/// Run the informational required-bucket-configuration probe against `source`.
/// Never fails, never panics, never affects qualification (ADR-0064 §7).
pub async fn probe_bucket_config<S: BucketConfigProbeSource + ?Sized>(
    source: &S,
) -> BucketConfigProbe {
    source.bucket_config().await
}

// --- Noncurrent-version listing for verify-custody (ADR-0064 §7, S4-12) ---
//
// On a versioned bucket, a Ravel delete leaves a recoverable prior version
// invisible to `verify-custody`'s content-addressed walk (S4-12). ADR-0064 §7
// closes that clause by having `verify-custody` list noncurrent versions under
// swept keys and report "deleted but recoverable as prior version" as its own
// anomaly class. The `ObjectStoreBackend` contract has no versioned listing, so
// this is a separate seam, same as the probes above: the production dyn impl
// reports `supported: false` (an honest gap, not an anomaly), and a versioning-
// aware test fixture reports the noncurrent versions it holds.

/// One noncurrent (prior) object version observed under a Ravel key on a
/// versioned bucket. `version_id` is the backend's opaque version handle
/// (S3 `VersionId`); its exact form is backend-specific and used only for
/// display.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoncurrentVersion {
    pub key: String,
    pub version_id: String,
}

/// Result of listing noncurrent versions under a prefix. `supported` is false
/// when the source cannot observe versioned listings at all (every production
/// backend through the trait contract today), distinguishing "no noncurrent
/// versions exist" (`supported: true`, empty `versions`) from "cannot tell"
/// (`supported: false`). The former lets `verify-custody` affirm the bucket is
/// clean; the latter is an honest gap it reports without alarming.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoncurrentVersionListing {
    pub supported: bool,
    pub versions: Vec<NoncurrentVersion>,
}

impl NoncurrentVersionListing {
    /// The honest default: this source cannot observe noncurrent versions.
    pub fn unsupported() -> Self {
        NoncurrentVersionListing {
            supported: false,
            versions: Vec::new(),
        }
    }
}

/// Source of noncurrent-version listings, kept **separate from
/// [`ObjectStoreBackend`]** for the same reason as the probes above. The
/// production dyn impl reports [`NoncurrentVersionListing::unsupported`]; a
/// versioning-aware fixture reports the prior versions it holds under a prefix.
#[async_trait::async_trait]
pub trait NoncurrentVersionSource {
    async fn list_noncurrent_versions(
        &self,
        prefix: &str,
    ) -> Result<NoncurrentVersionListing, StoreError>;
}

/// The production path: a store reached only through the [`ObjectStoreBackend`]
/// contract cannot enumerate prior versions, so it always reports
/// `supported: false`. Implemented on the trait object itself so
/// `ravel-cli verify-custody`, holding an `Arc<dyn ObjectStoreBackend>`, can
/// probe without threading a concrete type.
#[async_trait::async_trait]
impl NoncurrentVersionSource for dyn ObjectStoreBackend {
    async fn list_noncurrent_versions(
        &self,
        _prefix: &str,
    ) -> Result<NoncurrentVersionListing, StoreError> {
        Ok(NoncurrentVersionListing::unsupported())
    }
}

/// Run every conformance probe against `store`, scoping all writes under
/// `scratch_prefix` (ADR-0050 section 6: `sys/qualify/<run-id>/`). Never
/// panics on a misbehaving backend: every probe treats an unexpected
/// `StoreError` or an unexpected success as a failed [`ProbeResult`], not a
/// crash, so this is safe to run against an unqualified or actively broken
/// backend.
pub async fn run_conformance_suite(
    store: &dyn ObjectStoreBackend,
    scratch_prefix: &str,
) -> ConformanceReport {
    let prefix = if scratch_prefix.ends_with('/') {
        scratch_prefix.to_string()
    } else {
        format!("{scratch_prefix}/")
    };
    let results = vec![
        probe_conditional_write_create_if_absent(store, &prefix).await,
        probe_conditional_write_cas_version(store, &prefix).await,
        probe_consistent_read_after_write(store, &prefix).await,
        probe_consistent_list_after_write(store, &prefix).await,
    ];
    ConformanceReport { results }
}

async fn probe_conditional_write_create_if_absent(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
) -> ProbeResult {
    let property = Property::ConditionalWriteCreateIfAbsent;
    let key = format!("{prefix}cas/create-if-absent");

    if let Err(err) = store
        .put(
            &key,
            Bytes::from_static(b"winner"),
            PutOptions::create_if_absent(),
        )
        .await
    {
        return ProbeResult::fail(
            property,
            format!("first CreateIfAbsent put on a fresh key failed: {err}"),
        );
    }

    match store
        .put(
            &key,
            Bytes::from_static(b"loser"),
            PutOptions::create_if_absent(),
        )
        .await
    {
        Ok(_) => ProbeResult::fail(
            property,
            "a second CreateIfAbsent put on the same key succeeded; this backend does not \
             enforce conditional-create preconditions"
                .to_string(),
        ),
        Err(StoreError::AlreadyExists) => match store.get(&key, GetRange::Full).await {
            Ok(outcome) if outcome.data == Bytes::from_static(b"winner") => ProbeResult::pass(
                property,
                "losing writer correctly rejected with AlreadyExists, winner's bytes intact",
            ),
            Ok(_) => ProbeResult::fail(
                property,
                "the losing writer's bytes were applied despite AlreadyExists being returned"
                    .to_string(),
            ),
            Err(err) => {
                ProbeResult::fail(property, format!("could not verify winner content: {err}"))
            }
        },
        Err(other) => ProbeResult::fail(
            property,
            format!(
                "second CreateIfAbsent put failed with {other} instead of AlreadyExists \
                 (docs/object-store-contract.md: conditional-put failure mapping)"
            ),
        ),
    }
}

async fn probe_conditional_write_cas_version(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
) -> ProbeResult {
    let property = Property::ConditionalWriteCasVersion;
    let key = format!("{prefix}cas/version");

    let first = match store
        .put(&key, Bytes::from_static(b"v1"), PutOptions::default())
        .await
    {
        Ok(outcome) => outcome,
        Err(err) => return ProbeResult::fail(property, format!("seed put failed: {err}")),
    };

    if let Err(err) = store
        .put(
            &key,
            Bytes::from_static(b"v2"),
            PutOptions {
                mode: PutMode::CasVersion(first.version.clone()),
                checksum: None,
            },
        )
        .await
    {
        return ProbeResult::fail(
            property,
            format!("CAS put against the current version was rejected: {err}"),
        );
    }

    match store
        .put(
            &key,
            Bytes::from_static(b"stale-writer"),
            PutOptions {
                mode: PutMode::CasVersion(first.version),
                checksum: None,
            },
        )
        .await
    {
        Ok(_) => ProbeResult::fail(
            property,
            "a CAS put against a stale version succeeded; this backend does not enforce \
             version preconditions"
                .to_string(),
        ),
        Err(StoreError::PreconditionFailed) => match store.get(&key, GetRange::Full).await {
            Ok(outcome) if outcome.data == Bytes::from_static(b"v2") => ProbeResult::pass(
                property,
                "stale-version writer correctly rejected with PreconditionFailed, current \
                 version's bytes intact",
            ),
            Ok(_) => ProbeResult::fail(
                property,
                "the stale writer's bytes were applied despite PreconditionFailed being returned"
                    .to_string(),
            ),
            Err(err) => {
                ProbeResult::fail(property, format!("could not verify current content: {err}"))
            }
        },
        Err(other) => ProbeResult::fail(
            property,
            format!(
                "stale CAS put failed with {other} instead of PreconditionFailed \
                 (docs/object-store-contract.md: conditional-put failure mapping)"
            ),
        ),
    }
}

/// How many put-then-check cycles each consistency probe runs. More than one
/// cycle matters: a backend can get lucky (or unlucky) once, and ADR-0050
/// section 6 explicitly calls for "repeated put-then-list cycles".
const CONSISTENCY_CYCLES: usize = 5;

async fn probe_consistent_read_after_write(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
) -> ProbeResult {
    let property = Property::ConsistentReadAfterWrite;

    for i in 0..CONSISTENCY_CYCLES {
        let key = format!("{prefix}raw/{i}");
        let payload = Bytes::from(format!("payload-{i}"));
        if let Err(err) = store
            .put(&key, payload.clone(), PutOptions::default())
            .await
        {
            return ProbeResult::fail(property, format!("put {key} failed: {err}"));
        }
        match store.get(&key, GetRange::Full).await {
            Ok(outcome) if outcome.data == payload => {}
            Ok(outcome) => {
                return ProbeResult::fail(
                    property,
                    format!(
                        "read of {key} immediately after write returned {} bytes, expected {} \
                         -- read-after-write is not strongly consistent",
                        outcome.data.len(),
                        payload.len()
                    ),
                );
            }
            Err(err) => {
                return ProbeResult::fail(
                    property,
                    format!(
                        "read of {key} immediately after write failed with {err}; \
                         read-after-write is not strongly consistent"
                    ),
                );
            }
        }
    }
    ProbeResult::pass(
        property,
        format!("{CONSISTENCY_CYCLES} put-then-get cycles all returned the just-written bytes"),
    )
}

async fn probe_consistent_list_after_write(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
) -> ProbeResult {
    let property = Property::ConsistentListAfterWrite;
    let list_prefix = format!("{prefix}law/");

    for i in 0..CONSISTENCY_CYCLES {
        let key = format!("{list_prefix}{i}");
        if let Err(err) = store
            .put(&key, Bytes::from_static(b"x"), PutOptions::default())
            .await
        {
            return ProbeResult::fail(property, format!("put {key} failed: {err}"));
        }
        match list_all(store, &list_prefix).await {
            Ok(objects) => {
                if !objects.iter().any(|meta| meta.key == key) {
                    return ProbeResult::fail(
                        property,
                        format!(
                            "listing {list_prefix} immediately after writing {key} did not \
                             include it -- listing is not strongly consistent"
                        ),
                    );
                }
            }
            Err(err) => {
                return ProbeResult::fail(property, format!("listing {list_prefix} failed: {err}"));
            }
        }
    }
    ProbeResult::pass(
        property,
        format!("{CONSISTENCY_CYCLES} put-then-list cycles all observed the just-written key"),
    )
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::HashSet;

    use parking_lot::Mutex;

    use super::*;
    use crate::fault::{FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
    use crate::memory::MemoryStore;
    use crate::{Capabilities, DelimitedList, GetOutcome, ListPage, ObjectMeta, PageToken};

    /// The `sys/qualification` JSON shape is a frozen contract (ADR-0050
    /// section 6): a record written before this struct was relocated out of
    /// `ravel-cli` must still decode, so the field names and encoding must not
    /// drift. Pins the exact serialized keys and a round-trip, so a rename or a
    /// serde-attribute change that would silently break an existing object
    /// fails this test instead.
    #[test]
    fn qualification_record_json_shape_is_stable() {
        let record = QualificationRecord {
            suite_version: 1,
            backend_identity: "s3://ravel-test @ minio:9000".to_string(),
            qualified_unix_ns: 1_700_000_000_000_000_000,
            passed_properties: vec![
                "conditional_write_create_if_absent".to_string(),
                "consistent_read_after_write".to_string(),
            ],
        };
        let value: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&record).expect("encode"))
                .expect("decode to a generic JSON value");
        let obj = value.as_object().expect("record encodes as a JSON object");
        // Exactly these four keys, no more, no fewer.
        let mut keys: Vec<&String> = obj.keys().collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                &"backend_identity".to_string(),
                &"passed_properties".to_string(),
                &"qualified_unix_ns".to_string(),
                &"suite_version".to_string(),
            ],
            "the sys/qualification field set is a frozen contract"
        );
        assert_eq!(obj["suite_version"], serde_json::json!(1));
        assert_eq!(
            obj["backend_identity"],
            serde_json::json!("s3://ravel-test @ minio:9000")
        );
        assert_eq!(
            obj["qualified_unix_ns"],
            serde_json::json!(1_700_000_000_000_000_000i64)
        );

        // Round-trip: an encoded record decodes back to the same fields.
        let decoded: QualificationRecord =
            serde_json::from_value(value).expect("round-trips back to the record type");
        assert_eq!(decoded.suite_version, record.suite_version);
        assert_eq!(decoded.backend_identity, record.backend_identity);
        assert_eq!(decoded.qualified_unix_ns, record.qualified_unix_ns);
        assert_eq!(decoded.passed_properties, record.passed_properties);
    }

    /// A fixture that reports a fixed [`ObjectLockStatus`], standing in for a
    /// bucket whose Object Lock / versioning state a real backend could observe
    /// but the `ObjectStoreBackend` contract cannot. Lets the probe be
    /// exercised for enabled/disabled/unknown without a live S3 bucket.
    struct FixedLockSource(ObjectLockStatus);

    #[async_trait::async_trait]
    impl ObjectLockProbeSource for FixedLockSource {
        async fn object_lock_status(&self) -> ObjectLockProbe {
            match self.0 {
                ObjectLockStatus::Enabled => {
                    ObjectLockProbe::enabled("fixture: bucket reports Object Lock enabled")
                }
                ObjectLockStatus::Disabled => {
                    ObjectLockProbe::disabled("fixture: bucket reports no Object Lock/versioning")
                }
                ObjectLockStatus::Unknown => {
                    ObjectLockProbe::unknown("fixture: backend cannot answer")
                }
            }
        }
    }

    /// The informational probe reports "enabled", "disabled", and "unknown" as
    /// distinct outcomes, a real backend reached only through
    /// [`ObjectStoreBackend`] reports "unknown", and none of the three affects
    /// the qualification pass/fail result -- the suite adds no gating property
    /// for it and [`ConformanceReport::passed`] never consults it (ADR-0055
    /// section 3, ADR-0042 decision 3).
    #[tokio::test]
    async fn object_lock_probe_reports_each_state_and_never_gates_qualification() {
        // (a) The three states are produced distinctly from fixtures.
        let enabled = probe_object_lock(&FixedLockSource(ObjectLockStatus::Enabled)).await;
        let disabled = probe_object_lock(&FixedLockSource(ObjectLockStatus::Disabled)).await;
        let unknown = probe_object_lock(&FixedLockSource(ObjectLockStatus::Unknown)).await;
        assert_eq!(enabled.status, ObjectLockStatus::Enabled);
        assert_eq!(disabled.status, ObjectLockStatus::Disabled);
        assert_eq!(unknown.status, ObjectLockStatus::Unknown);
        // Distinct as values and as greppable names.
        assert_ne!(enabled.status, disabled.status);
        assert_ne!(disabled.status, unknown.status);
        assert_eq!(
            [
                enabled.status.name(),
                disabled.status.name(),
                unknown.status.name()
            ],
            ["enabled", "disabled", "unknown"],
        );

        // (b) A real backend reached only through the trait contract cannot
        // answer, so the production path is "unknown" -- not an error.
        let store = MemoryStore::new();
        let via_backend = probe_object_lock(&store as &dyn ObjectStoreBackend).await;
        assert_eq!(via_backend.status, ObjectLockStatus::Unknown);

        // (c) Whatever the probe reports, qualification pass/fail is unchanged:
        // the conforming oracle passes, and the suite carries exactly the four
        // gating properties -- the probe is none of them.
        let report = run_conformance_suite(&store, "sys/qualify/object-lock/").await;
        assert!(
            report.passed(),
            "the informational probe must not change the qualification result"
        );
        assert_eq!(
            report.results.len(),
            4,
            "the Object Lock probe adds no gating property to the conformance suite"
        );
    }

    #[tokio::test]
    async fn conforming_backend_qualifies() {
        let store = MemoryStore::new();
        let report = run_conformance_suite(&store, "sys/qualify/test-1/").await;
        assert!(
            report.passed(),
            "expected every property to pass on the oracle, got failures: {:?}",
            report.failures().collect::<Vec<_>>()
        );
        assert_eq!(report.results.len(), 4);
    }

    /// Wraps `MemoryStore` and simulates eventually consistent listing: the
    /// call to `list`/`list_all` immediately following a key's `put` never
    /// includes it (the key becomes visible starting from the NEXT listing
    /// call instead). `put`, `get`, and `head` are untouched, so this isolates
    /// the list-after-write property alone -- conditional writes and
    /// read-after-write still pass on this store.
    struct WeakListStore {
        inner: MemoryStore,
        hidden_from_next_list: Mutex<HashSet<String>>,
    }

    impl WeakListStore {
        fn new() -> Self {
            WeakListStore {
                inner: MemoryStore::new(),
                hidden_from_next_list: Mutex::new(HashSet::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for WeakListStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<crate::PutOutcome, StoreError> {
            let outcome = self.inner.put(key, data, opts).await?;
            self.hidden_from_next_list.lock().insert(key.to_string());
            Ok(outcome)
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            let mut page_result = self.inner.list(prefix, page).await?;
            let mut hidden = self.hidden_from_next_list.lock();
            page_result.objects.retain(|meta| !hidden.remove(&meta.key));
            Ok(page_result)
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    #[tokio::test]
    async fn weak_list_backend_fails_qualification() {
        let store = WeakListStore::new();
        let report = run_conformance_suite(&store, "sys/qualify/test-2/").await;
        assert!(!report.passed());
        let failed: Vec<Property> = report.failures().map(|r| r.property).collect();
        assert_eq!(
            failed,
            vec![Property::ConsistentListAfterWrite],
            "only listing should be named; conditional writes and read-after-write are untouched"
        );
        let failure = report
            .results
            .iter()
            .find(|r| r.property == Property::ConsistentListAfterWrite)
            .expect("listing probe result present");
        assert!(failure.detail.contains("not strongly consistent"));
    }

    /// Wraps `MemoryStore` and drops every conditional-write precondition:
    /// every `put`, regardless of the requested `PutMode`, is applied as an
    /// unconditional overwrite. Models a backend that advertises S3
    /// compatibility but silently ignores `If-None-Match`/`If-Match`.
    struct NoCasStore {
        inner: MemoryStore,
    }

    impl NoCasStore {
        fn new() -> Self {
            NoCasStore {
                inner: MemoryStore::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for NoCasStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<crate::PutOutcome, StoreError> {
            self.inner
                .put(
                    key,
                    data,
                    PutOptions {
                        mode: PutMode::Overwrite,
                        checksum: opts.checksum,
                    },
                )
                .await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities {
                create_if_absent: false,
                cas_version: false,
                ..self.inner.capabilities()
            }
        }
    }

    #[tokio::test]
    async fn backend_without_conditional_writes_fails_qualification() {
        let store = NoCasStore::new();
        let report = run_conformance_suite(&store, "sys/qualify/test-3/").await;
        assert!(!report.passed());
        let failed: HashSet<&'static str> = report.failures().map(|r| r.property.name()).collect();
        assert!(failed.contains(Property::ConditionalWriteCreateIfAbsent.name()));
        assert!(failed.contains(Property::ConditionalWriteCasVersion.name()));
        // Listing and read-after-write are untouched by this backend.
        assert!(!failed.contains(Property::ConsistentListAfterWrite.name()));
        assert!(!failed.contains(Property::ConsistentReadAfterWrite.name()));
    }

    /// A transient fault on the very first probe call must surface as a
    /// named, typed [`ProbeResult`] failure, not a panic -- and the fault
    /// must actually have fired, not merely be configured (repo testing
    /// pattern: assert `FaultStore` counters).
    #[tokio::test]
    async fn transient_put_fault_surfaces_as_named_probe_failure() {
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Timeout)
                .with_key_contains("cas/create-if-absent")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(MemoryStore::new(), plan);
        let report = run_conformance_suite(&store, "sys/qualify/test-4/").await;
        assert!(!report.passed());
        let failure = report
            .results
            .iter()
            .find(|r| r.property == Property::ConditionalWriteCreateIfAbsent)
            .expect("probe result present");
        assert!(!failure.passed);
        assert!(failure.detail.contains("timeout"));
        assert_eq!(store.fault_count(Op::Put, FaultKind::Timeout), 1);
    }

    /// Retryability classification is a normative contract
    /// (docs/object-store-contract.md: "Retry classification"): `Throttled`,
    /// `Timeout`, and `Transient` are retryable; `NotFound`, `AlreadyExists`,
    /// `PreconditionFailed`, and `Permanent` are terminal. This pins both the
    /// `StoreError` variant AND its `is_retryable()` for a representative set of
    /// error shapes -- timeout, throttle, a permanent failure, a not-found, and
    /// a conditional-write (CAS) conflict under each mode -- as observed through
    /// a backend the conformance harness parameterizes (`FaultStore` over the
    /// `MemoryStore` oracle). The S3/MinIO adapter's own `Error::Generic`
    /// classification is pinned complementarily in `s3.rs` (issue #906), where
    /// `object_store::Error` values can be constructed directly; together they
    /// cover both the shared taxonomy and the S3-specific mapping.
    ///
    /// If a future change flipped a variant's retryability, or an adapter
    /// misclassified one of these shapes, this fails loudly instead of letting
    /// a retry loop spin forever on a terminal error (or give up on a
    /// retryable one).
    #[tokio::test]
    async fn retryability_classification_is_pinned_across_a_parameterized_backend() {
        // (op, scripted fault, put mode, expected variant tag, expected retryable)
        // Each row induces one error shape and asserts the resulting StoreError
        // and its is_retryable(). `variant` is a small tag matched below.
        #[derive(Clone, Copy)]
        enum Variant {
            Timeout,
            Throttled,
            Transient,
            Permanent,
            NotFound,
            AlreadyExists,
            PreconditionFailed,
        }

        async fn induce(fault: ScriptedFault, op: Op, mode: PutMode) -> StoreError {
            let plan = FaultPlan::empty().with_rule(Rule::new(op, fault));
            let store = FaultStore::new(MemoryStore::new(), plan);
            // Seed a key so read/head faults have something to shadow; the
            // fault fires before the backend is reached regardless.
            store
                .put("k", Bytes::from_static(b"seed"), PutOptions::default())
                .await
                .ok();
            match op {
                Op::Put => store
                    .put(
                        "k2",
                        Bytes::from_static(b"v"),
                        PutOptions {
                            mode,
                            checksum: None,
                        },
                    )
                    .await
                    .expect_err("put fault must surface"),
                Op::Get => store
                    .get("k", GetRange::Full)
                    .await
                    .expect_err("get fault must surface"),
                Op::Head => store.head("k").await.expect_err("head fault must surface"),
                Op::List => store
                    .list("", None)
                    .await
                    .expect_err("list fault must surface"),
                Op::Delete => store
                    .delete("k")
                    .await
                    .expect_err("delete fault must surface"),
            }
        }

        let cases: Vec<(ScriptedFault, Op, PutMode, Variant, bool)> = vec![
            (
                ScriptedFault::Timeout,
                Op::Get,
                PutMode::Overwrite,
                Variant::Timeout,
                true,
            ),
            (
                ScriptedFault::Throttled {
                    retry_after_ms: 250,
                },
                Op::Get,
                PutMode::Overwrite,
                Variant::Throttled,
                true,
            ),
            (
                ScriptedFault::Transient("blip".into()),
                Op::List,
                PutMode::Overwrite,
                Variant::Transient,
                true,
            ),
            (
                ScriptedFault::Permanent("nope".into()),
                Op::Head,
                PutMode::Overwrite,
                Variant::Permanent,
                false,
            ),
            (
                ScriptedFault::NotFoundBlip,
                Op::Get,
                PutMode::Overwrite,
                Variant::NotFound,
                false,
            ),
            (
                ScriptedFault::FailedConditionalWrite,
                Op::Put,
                PutMode::CreateIfAbsent,
                Variant::AlreadyExists,
                false,
            ),
            (
                ScriptedFault::FailedConditionalWrite,
                Op::Put,
                PutMode::CasVersion(crate::Version("v1".into())),
                Variant::PreconditionFailed,
                false,
            ),
        ];

        for (fault, op, mode, variant, retryable) in cases {
            let err = induce(fault.clone(), op, mode).await;
            let ok = match variant {
                Variant::Timeout => matches!(err, StoreError::Timeout),
                Variant::Throttled => matches!(err, StoreError::Throttled { .. }),
                Variant::Transient => matches!(err, StoreError::Transient(_)),
                Variant::Permanent => matches!(err, StoreError::Permanent(_)),
                Variant::NotFound => matches!(err, StoreError::NotFound),
                Variant::AlreadyExists => matches!(err, StoreError::AlreadyExists),
                Variant::PreconditionFailed => matches!(err, StoreError::PreconditionFailed),
            };
            assert!(ok, "{fault:?} on {op:?} produced unexpected {err:?}");
            assert_eq!(
                err.is_retryable(),
                retryable,
                "{fault:?} on {op:?} -> {err:?}: retryable mismatch"
            );
        }
    }

    /// A fixture standing in for a bucket whose versioning / lifecycle-rule
    /// state a real backend could observe but the `ObjectStoreBackend` contract
    /// cannot. Lets the required-bucket-configuration probe and its alarm
    /// assessment be exercised for compliant and non-compliant buckets without
    /// a live S3 bucket.
    struct FixedBucketConfig(BucketConfigProbe);

    #[async_trait::async_trait]
    impl BucketConfigProbeSource for FixedBucketConfig {
        async fn bucket_config(&self) -> BucketConfigProbe {
            self.0.clone()
        }
    }

    /// A compliant versioned bucket (ADR-0064 §7): versioning on, and both the
    /// noncurrent-version expiration and the recommended abort-incomplete rule
    /// present, raises no alarm.
    #[tokio::test]
    async fn bucket_config_compliant_versioned_bucket_has_no_alarms() {
        let source = FixedBucketConfig(BucketConfigProbe {
            versioning: VersioningStatus::On,
            abort_incomplete_multipart_upload: LifecycleRuleStatus::Present,
            noncurrent_version_expiration: LifecycleRuleStatus::Present,
            detail: "fixture: compliant versioned bucket".to_string(),
        });
        let probe = probe_bucket_config(&source).await;
        assert_eq!(probe.versioning, VersioningStatus::On);
        assert!(
            bucket_config_alarms(&probe).is_empty(),
            "a versioned bucket with both sanctioned rules must not alarm"
        );

        // A compliant unversioned bucket is equally clean: with versioning off,
        // the noncurrent-version rule is not required, so its absence does not
        // alarm.
        let source = FixedBucketConfig(BucketConfigProbe {
            versioning: VersioningStatus::Off,
            abort_incomplete_multipart_upload: LifecycleRuleStatus::Present,
            noncurrent_version_expiration: LifecycleRuleStatus::Absent,
            detail: "fixture: compliant unversioned bucket".to_string(),
        });
        let probe = probe_bucket_config(&source).await;
        assert!(
            bucket_config_alarms(&probe).is_empty(),
            "an unversioned bucket needs no noncurrent-version rule"
        );
    }

    /// The load-bearing non-compliant case (S4-12): versioning on with no
    /// noncurrent-version expiration rule alarms as an unsupported
    /// configuration; a missing abort-incomplete rule adds an advisory note.
    #[tokio::test]
    async fn bucket_config_versioned_without_noncurrent_rule_alarms() {
        let source = FixedBucketConfig(BucketConfigProbe {
            versioning: VersioningStatus::On,
            abort_incomplete_multipart_upload: LifecycleRuleStatus::Absent,
            noncurrent_version_expiration: LifecycleRuleStatus::Absent,
            detail: "fixture: non-compliant versioned bucket".to_string(),
        });
        let probe = probe_bucket_config(&source).await;
        let alarms = bucket_config_alarms(&probe);
        assert_eq!(
            alarms.len(),
            2,
            "expected the unsupported-config alarm plus the abort-incomplete note, got: {alarms:?}"
        );
        assert!(
            alarms[0].starts_with("ALARM:") && alarms[0].contains("unsupported configuration"),
            "the versioning-without-expiration alarm must be first and named: {alarms:?}"
        );
        assert!(
            alarms[1].contains("AbortIncompleteMultipartUpload"),
            "the abort-incomplete note must appear: {alarms:?}"
        );
    }

    /// An `Unknown` field never alarms: the platform cannot see the setting, so
    /// it neither confirms nor denies a violation (ADR-0055 §3 honest gap). A
    /// real backend reached only through the trait contract reports every field
    /// unknown and therefore raises no alarm.
    #[tokio::test]
    async fn bucket_config_unknown_never_alarms_and_is_the_production_default() {
        let store = MemoryStore::new();
        let probe = probe_bucket_config(&store as &dyn ObjectStoreBackend).await;
        assert_eq!(probe.versioning, VersioningStatus::Unknown);
        assert_eq!(
            probe.abort_incomplete_multipart_upload,
            LifecycleRuleStatus::Unknown
        );
        assert_eq!(
            probe.noncurrent_version_expiration,
            LifecycleRuleStatus::Unknown
        );
        assert!(
            bucket_config_alarms(&probe).is_empty(),
            "an all-unknown probe must not alarm"
        );

        // And it never touches qualification pass/fail.
        let report = run_conformance_suite(&store, "sys/qualify/bucket-config/").await;
        assert!(report.passed());
        assert_eq!(report.results.len(), 4);
    }

    /// A versioning-aware fixture that holds noncurrent versions, standing in
    /// for a versioned bucket whose prior versions the trait contract cannot
    /// enumerate.
    struct FixedNoncurrentVersions(NoncurrentVersionListing);

    #[async_trait::async_trait]
    impl NoncurrentVersionSource for FixedNoncurrentVersions {
        async fn list_noncurrent_versions(
            &self,
            _prefix: &str,
        ) -> Result<NoncurrentVersionListing, StoreError> {
            Ok(self.0.clone())
        }
    }

    /// A real backend cannot enumerate prior versions through the trait
    /// contract, so it reports `supported: false` -- an honest gap, not an
    /// empty-and-clean result.
    #[tokio::test]
    async fn noncurrent_version_source_production_default_is_unsupported() {
        let store = MemoryStore::new();
        let listing = NoncurrentVersionSource::list_noncurrent_versions(
            &store as &dyn ObjectStoreBackend,
            "t/",
        )
        .await
        .expect("the dyn impl never errors");
        assert!(!listing.supported);
        assert!(listing.versions.is_empty());
    }

    /// A versioning-aware fixture surfaces the noncurrent versions it holds,
    /// distinct from the unsupported production default.
    #[tokio::test]
    async fn noncurrent_version_fixture_reports_prior_versions() {
        let source = FixedNoncurrentVersions(NoncurrentVersionListing {
            supported: true,
            versions: vec![NoncurrentVersion {
                key: "t/ab/m/l0/0000/obj.rseg".to_string(),
                version_id: "v-prior-1".to_string(),
            }],
        });
        let listing = source
            .list_noncurrent_versions("t/")
            .await
            .expect("fixture never errors");
        assert!(listing.supported);
        assert_eq!(listing.versions.len(), 1);
        assert_eq!(listing.versions[0].version_id, "v-prior-1");
    }
}
