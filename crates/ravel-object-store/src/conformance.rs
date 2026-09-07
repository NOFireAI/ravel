//! Empirical backend qualification (ADR-0050 section 6, adversarial review
//! finding S5-20, docs/object-store-contract.md "Semantics adapters MUST
//! honor").
//!
//! Ravel's commit protocol and catalog assume properties of the backing store
//! that nothing previously checked at runtime: conditional writes
//! (`CreateIfAbsent` and `CasVersion` reject a losing writer without
//! applying it, including when the losing writer is concurrent rather than
//! later), strong consistency (a `get`/`list` issued right after a `put`
//! always observes it), listing shape (lexicographic key order, `start_after`
//! resumption, and no key lost across pages), and delete visibility. A backend
//! can report `Capabilities{ .. }` honestly or dishonestly; either way,
//! [`run_conformance_suite`] exercises the real behavior instead of trusting
//! the self-report.
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
    /// `PutMode::CreateIfAbsent` under contention: when several writers create
    /// the same absent key concurrently, exactly one must succeed, every other
    /// must observe `AlreadyExists`, and the surviving bytes must be the
    /// winner's. Empirical counterpart of the common TLA model's
    /// `CreateIfAbsentWinnerUnique` (formal/tla/common/traceability.md).
    ConcurrentCreateIfAbsentSingleWinner,
    /// `list` returns keys in lexicographic order, and `list_after` resumes
    /// strictly after its `start_after` marker in that same order
    /// (docs/object-store-contract.md: "Semantics adapters MUST honor").
    /// Empirical counterpart of `ListingConsumersConsistent`: a consumer that
    /// deduplicates by key can only agree with the delivered support if the
    /// delivery order is the ordering `S3Store::list` pagination assumes.
    LexicographicListingOrder,
    /// A paginated listing delivers every key present before the first page
    /// request, across as many pages as the backend chooses, losing none.
    /// Empirical counterpart of `ListReturn`/`ListEventuallyComplete`, and the
    /// cross-page listing consistency probe ADR-0050 section 6 names.
    CrossPageListing,
    /// After a delete, the key is gone from both access paths: a `get` returns
    /// `NotFound` and a listing of its prefix omits it, and deleting it again
    /// changes nothing. Empirical counterpart of `DeleteIdempotent`.
    DeleteVisibility,
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
            Property::ConcurrentCreateIfAbsentSingleWinner => {
                "concurrent_create_if_absent_single_winner"
            }
            Property::LexicographicListingOrder => "lexicographic_listing_order",
            Property::CrossPageListing => "cross_page_listing",
            Property::DeleteVisibility => "delete_visibility",
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
// `AbortIncompleteMultipartUpload` rule REQUIRED (#864), and no other expiration
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
/// `AbortIncompleteMultipartUpload` lifecycle rule (REQUIRED, #864), and the
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
    // REQUIRED (#864): the abort-incomplete-multipart rule (ADR-0064 §7 point 3;
    // also converts S5-19's undocumented dependency into a documented one).
    // Emitted under NOTE rather than ALARM only because no vendor API this crate
    // calls can observe the rule, so the probe cannot establish compliance
    // either way. The prefix reflects the probe's limits, not a weaker rule.
    if probe.abort_incomplete_multipart_upload == LifecycleRuleStatus::Absent {
        alarms.push(
            "NOTE: the REQUIRED AbortIncompleteMultipartUpload lifecycle rule (7 days or \
             less) is not configured (ADR-0064 §7 point 3). Its absence violates the bucket \
             configuration contract: nothing in Ravel reaps abandoned multipart uploads, so \
             their parts stay billable indefinitely. The NOTE prefix reflects the probe's \
             limits, not an optional requirement."
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
        // Appended, not interleaved with the four probes above, so a report's
        // result order stays stable for anything that already reads it.
        probe_concurrent_create_if_absent(store, &prefix).await,
        probe_lexicographic_listing_order(store, &prefix).await,
        probe_cross_page_listing(store, &prefix).await,
        probe_delete_visibility(store, &prefix).await,
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

/// How many writers race the same absent key in
/// [`probe_concurrent_create_if_absent`]. More than the two the contract doc's
/// worked example names: a backend that serializes two conflicting creates by
/// luck is less likely to serialize eight.
const CONCURRENT_CREATE_WRITERS: usize = 8;

/// The one key [`probe_concurrent_create_if_absent`] races, under the run's
/// scratch prefix. Named so a test fixture that has to recognize the raced key
/// shares this definition instead of retyping the literal.
const CONCURRENT_CREATE_KEY_SUFFIX: &str = "cas/concurrent-create";

/// Total `CreateIfAbsent` attempts one racing writer gets before the probe
/// gives up on it: the concurrent put itself plus up to three retries.
///
/// docs/object-store-contract.md ("Semantics adapters MUST honor") lets a
/// conditional write that races another writer for the same absent key surface
/// as a retryable transient conflict; the loser only reaches `AlreadyExists`
/// once it retries against a key that is by then present. `S3Store` produces
/// exactly that: its HEAD disambiguation of a 409 returns a retryable error
/// while the key still looks absent. So a losing writer's first answer is not
/// required to be `AlreadyExists`, and classifying it as a qualification
/// failure would report a conformant backend as broken.
///
/// Small and fixed rather than a backoff schedule: each retry re-races a key
/// that is already present on any backend whose conditional create is atomic,
/// so one retry is normally enough, and a backend that still cannot answer
/// after this many is reported as unable to settle rather than retried
/// indefinitely inside a qualification run.
const CONCURRENT_CREATE_MAX_ATTEMPTS: usize = 4;

/// Drive one racing writer's `CreateIfAbsent` outcome to a terminal one by
/// retrying the identical put while the outcome is retryable, for at most
/// [`CONCURRENT_CREATE_MAX_ATTEMPTS`] attempts in total (`first` is attempt 1).
///
/// Retryability comes from [`StoreError::is_retryable`], the same predicate
/// every production retry loop turns on, rather than a list restated here that
/// a new retryable variant would silently fall out of.
///
/// The returned outcome is `Ok` (this writer won), `Err(AlreadyExists)` (it
/// lost), a terminal error, or -- when the bound ran out -- still a retryable
/// error, which the caller reports as a failure rather than as a loss.
///
/// The second element of the pair is whether this writer took the retry path
/// at all (its first answer was retryable, so at least one further attempt
/// ran). The caller needs it because a durable create that lost its response
/// answers `AlreadyExists` on retry, indistinguishable from a genuine loser by
/// the terminal outcome alone: a run with zero winners but a retried writer
/// could not establish which writer won rather than having observed a
/// non-atomic create.
async fn settle_racing_create(
    store: &dyn ObjectStoreBackend,
    key: &str,
    payload: &Bytes,
    first: Result<crate::PutOutcome, StoreError>,
) -> (Result<crate::PutOutcome, StoreError>, bool) {
    let mut outcome = first;
    let mut retried = false;
    for _ in 1..CONCURRENT_CREATE_MAX_ATTEMPTS {
        match &outcome {
            Err(err) if err.is_retryable() => {}
            _ => break,
        }
        retried = true;
        outcome = store
            .put(key, payload.clone(), PutOptions::create_if_absent())
            .await;
    }
    (outcome, retried)
}

/// The single-winner probe ADR-0050 section 6 calls for: `CreateIfAbsent`
/// under *concurrent* same-key writers, not two sequential puts.
///
/// The sequential probe above cannot falsify a backend whose conditional
/// create is checked and applied non-atomically, because the second put starts
/// long after the first one finished. Here every writer's request is in flight
/// at once, so a read-then-write implementation has a window to lose in.
///
/// A losing writer is not required to answer `AlreadyExists` on its first
/// attempt. The contract lets a conditional write racing another writer for the
/// same absent key surface as a retryable transient conflict, so each writer's
/// outcome is driven to a terminal one by [`settle_racing_create`], bounded by
/// [`CONCURRENT_CREATE_MAX_ATTEMPTS`], before anything is counted. Only then do
/// the counts mean what the property says: exactly one `Ok`, exactly
/// `CONCURRENT_CREATE_WRITERS - 1` `AlreadyExists`, and the survivor holding the
/// winner's bytes. A writer still retryable at the bound is reported as a
/// failure naming that bound, never counted as a loser.
///
/// The requests are concurrent futures on one task, not spawned tasks: the
/// suite holds `&dyn ObjectStoreBackend`, which cannot be moved into a
/// `'static` task. Against a real backend that is a genuine race, since all
/// [`CONCURRENT_CREATE_WRITERS`] requests are on the wire together and the
/// contention that matters is at the backend. Against an in-process store
/// whose `put` completes on its first poll it is not a race at all, which is
/// one more reason a pass is qualification and not proof.
async fn probe_concurrent_create_if_absent(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
) -> ProbeResult {
    let property = Property::ConcurrentCreateIfAbsentSingleWinner;
    let key = format!("{prefix}{CONCURRENT_CREATE_KEY_SUFFIX}");

    // Distinct payloads: whichever writer wins, its bytes are identifiable, so
    // "the survivor is the winner's object" is checkable rather than assumed.
    let payloads: Vec<Bytes> = (0..CONCURRENT_CREATE_WRITERS)
        .map(|i| Bytes::from(format!("writer-{i}")))
        .collect();
    let outcomes = futures::future::join_all(payloads.iter().enumerate().map(|(i, payload)| {
        let key = &key;
        async move {
            (
                i,
                store
                    .put(key, payload.clone(), PutOptions::create_if_absent())
                    .await,
            )
        }
    }))
    .await;

    let mut winners: Vec<usize> = Vec::new();
    let mut losers = 0usize;
    let mut retried: Vec<usize> = Vec::new();
    let mut unsettled: Vec<String> = Vec::new();
    let mut unexpected: Vec<String> = Vec::new();
    for (i, outcome) in outcomes {
        let (settled, was_retried) = settle_racing_create(store, &key, &payloads[i], outcome).await;
        if was_retried {
            retried.push(i);
        }
        match settled {
            Ok(_) => winners.push(i),
            Err(StoreError::AlreadyExists) => losers += 1,
            Err(err) if err.is_retryable() => unsettled.push(format!("writer-{i}: {err}")),
            Err(other) => unexpected.push(format!("writer-{i}: {other}")),
        }
    }

    if !unsettled.is_empty() {
        return ProbeResult::fail(
            property,
            format!(
                "{} of {CONCURRENT_CREATE_WRITERS} concurrent CreateIfAbsent writers still \
                 returned a retryable error after {CONCURRENT_CREATE_MAX_ATTEMPTS} attempts \
                 each: {} (docs/object-store-contract.md allows a racing conditional write to \
                 surface as a retryable transient conflict, but a retry against the by-then \
                 present key must settle on AlreadyExists); this backend never settled, so its \
                 single-winner property could not be evaluated",
                unsettled.len(),
                unsettled.join(", ")
            ),
        );
    }
    if !unexpected.is_empty() {
        return ProbeResult::fail(
            property,
            format!(
                "{} of {CONCURRENT_CREATE_WRITERS} concurrent CreateIfAbsent writers failed with \
                 a terminal error other than AlreadyExists: {} \
                 (docs/object-store-contract.md: conditional-put failure mapping)",
                unexpected.len(),
                unexpected.join(", ")
            ),
        );
    }
    // A durable create whose response was lost is a permitted outcome
    // (docs/object-store-contract.md; TLA action PutCreateIfAbsentLostResponse
    // in formal/tla/common): the write landed but the writer saw a retryable
    // failure, so its retry against the by-then present key answers
    // AlreadyExists. That writer is then counted as a loser and the winner it
    // actually was disappears, leaving zero winners. The counts alone cannot
    // tell this apart from a non-atomic create that produced no winner, so when
    // a retried writer is present the run is reported as unable to establish a
    // winner, not as a single-winner violation. The run still FAILS: a
    // qualification pass requires exactly one established winner, which this run
    // did not produce.
    if winners.is_empty() && !retried.is_empty() {
        let retried_writers = retried
            .iter()
            .map(|i| format!("writer-{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        return ProbeResult::fail(
            property,
            format!(
                "no CreateIfAbsent writer succeeded, but {} of {CONCURRENT_CREATE_WRITERS} \
                 retried a retryable first response ({retried_writers}); a durable create whose \
                 response was lost answers AlreadyExists on retry (docs/object-store-contract.md), \
                 so this run could not establish which writer won and the single-winner property \
                 could not be evaluated",
                retried.len()
            ),
        );
    }
    if winners.len() != 1 {
        return ProbeResult::fail(
            property,
            format!(
                "{} of {CONCURRENT_CREATE_WRITERS} concurrent CreateIfAbsent writers on one \
                 absent key succeeded, expected exactly 1; this backend's conditional create is \
                 not atomic under contention",
                winners.len()
            ),
        );
    }
    if losers != CONCURRENT_CREATE_WRITERS - 1 {
        return ProbeResult::fail(
            property,
            format!(
                "one writer won but {losers} lost with AlreadyExists, expected exactly {}",
                CONCURRENT_CREATE_WRITERS - 1
            ),
        );
    }

    let winner = winners[0];
    match store.get(&key, GetRange::Full).await {
        Ok(outcome) if outcome.data == payloads[winner] => ProbeResult::pass(
            property,
            format!(
                "exactly 1 of {CONCURRENT_CREATE_WRITERS} concurrent CreateIfAbsent writers won, \
                 the other {} were rejected with AlreadyExists, and the surviving object holds \
                 the winner's bytes",
                CONCURRENT_CREATE_WRITERS - 1
            ),
        ),
        Ok(outcome) => ProbeResult::fail(
            property,
            format!(
                "writer-{winner} won the race but the surviving object holds {:?}, not that \
                 writer's bytes; a losing writer's bytes were applied despite AlreadyExists",
                String::from_utf8_lossy(&outcome.data)
            ),
        ),
        Err(err) => ProbeResult::fail(
            property,
            format!("could not read back the race winner's object: {err}"),
        ),
    }
}

/// Upper bound on the pages any probe here will drain. The listing probes
/// write a handful of keys, so a backend still handing out continuation tokens
/// past this is a broken pager: reported as a probe failure, never as a
/// qualification run that hangs.
const MAX_PROBE_PAGES: usize = 64;

/// Drain every page of `prefix` (from `start_after`, when given) and return
/// the keys in delivery order together with the number of pages served.
///
/// Deliberately not [`list_all`]: that helper deduplicates and discards both
/// the delivery order and the page count, which are exactly what the two
/// listing probes below examine. Errors come back as a ready-to-report detail
/// string so a misbehaving backend produces a failed [`ProbeResult`] rather
/// than an error the suite has to interpret twice.
async fn drain_pages(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
    start_after: Option<&str>,
) -> Result<(Vec<String>, usize), String> {
    let mut delivered: Vec<String> = Vec::new();
    let mut pages = 0usize;
    let mut token = None;
    loop {
        let page = store
            .list_after(prefix, start_after, token)
            .await
            .map_err(|err| format!("listing {prefix} failed: {err}"))?;
        pages += 1;
        delivered.extend(page.objects.into_iter().map(|meta| meta.key));
        match page.next {
            Some(next) => token = Some(next),
            None => return Ok((delivered, pages)),
        }
        if pages >= MAX_PROBE_PAGES {
            return Err(format!(
                "listing {prefix} still returned a continuation token after {pages} pages over \
                 far fewer keys; this backend's pagination does not terminate"
            ));
        }
    }
}

/// Keys in delivery order, with the repeats the cross-page guarantee allows
/// collapsed, keeping first-delivery order so an ordering violation survives
/// the deduplication.
fn distinct_in_delivery_order(delivered: &[String]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    delivered
        .iter()
        .filter(|key| seen.insert((*key).clone()))
        .cloned()
        .collect()
}

/// The first pair of adjacent deliveries that goes backwards, if any.
fn first_order_violation(delivered: &[String]) -> Option<(&str, &str)> {
    delivered
        .windows(2)
        .find(|pair| pair[1] < pair[0])
        .map(|pair| (pair[0].as_str(), pair[1].as_str()))
}

/// Written in this order, so a backend that simply echoes insertion order
/// fails the probe instead of passing it by accident.
const ORDER_PROBE_SUFFIXES: [&str; 5] = ["d", "a", "e", "c", "b"];

/// [`probe_lexicographic_listing_order`] indexes `expected[1]` for the
/// `start_after` marker, `expected[2..]` for the tail it must return, and
/// `expected_tail[0]` in its pass detail, so the constant above and that probe
/// are coupled. Reducing it below three entries would otherwise turn a
/// qualification run against a live bucket into a panic; here it fails the
/// build instead.
const _: () = assert!(
    ORDER_PROBE_SUFFIXES.len() >= 3,
    "probe_lexicographic_listing_order needs at least three keys: one before the start_after \
     marker, the marker, and a non-empty tail after it"
);

/// Lexicographic listing order and `start_after` resumption.
///
/// `S3Store::list` pagination and every catalog scan built on it assume both:
/// a continuation token means "resume after this key", which is only a
/// position if the order is total and lexicographic, and `list_after` is how
/// callers skip a key sub-range server-side. Nothing probed either, so a
/// backend that returned keys in insertion or hash order could qualify.
async fn probe_lexicographic_listing_order(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
) -> ProbeResult {
    let property = Property::LexicographicListingOrder;
    let list_prefix = format!("{prefix}order/");

    for suffix in ORDER_PROBE_SUFFIXES {
        let key = format!("{list_prefix}{suffix}");
        if let Err(err) = store
            .put(&key, Bytes::from_static(b"x"), PutOptions::default())
            .await
        {
            return ProbeResult::fail(property, format!("put {key} failed: {err}"));
        }
    }
    let mut expected: Vec<String> = ORDER_PROBE_SUFFIXES
        .iter()
        .map(|suffix| format!("{list_prefix}{suffix}"))
        .collect();
    expected.sort();

    let (delivered, _pages) = match drain_pages(store, &list_prefix, None).await {
        Ok(result) => result,
        Err(detail) => return ProbeResult::fail(property, detail),
    };
    if let Some((before, after)) = first_order_violation(&delivered) {
        return ProbeResult::fail(
            property,
            format!(
                "listing {list_prefix} delivered {after} after {before}, which sorts before it; \
                 this backend's listing is not in lexicographic key order, so a continuation \
                 token does not name a position in the key space"
            ),
        );
    }
    let distinct = distinct_in_delivery_order(&delivered);
    if distinct != expected {
        return ProbeResult::fail(
            property,
            format!(
                "listing {list_prefix} returned {} distinct keys, expected exactly {}: got \
                 {distinct:?}, expected {expected:?}",
                distinct.len(),
                expected.len()
            ),
        );
    }

    // start_after: resume strictly after the second key, which must yield
    // exactly the last three, still in order.
    let marker = expected[1].clone();
    let expected_tail: Vec<String> = expected[2..].to_vec();
    let (tail_delivered, _pages) = match drain_pages(store, &list_prefix, Some(&marker)).await {
        Ok(result) => result,
        Err(detail) => return ProbeResult::fail(property, detail),
    };
    if let Some(key) = tail_delivered.iter().find(|key| *key <= &marker) {
        return ProbeResult::fail(
            property,
            format!(
                "list_after({list_prefix}, start_after={marker}) returned {key}, which does not \
                 compare strictly greater than the marker (docs/object-store-contract.md: \
                 start_after is exclusive)"
            ),
        );
    }
    if let Some((before, after)) = first_order_violation(&tail_delivered) {
        return ProbeResult::fail(
            property,
            format!(
                "list_after({list_prefix}, start_after={marker}) delivered {after} after \
                 {before}, which sorts before it"
            ),
        );
    }
    let distinct_tail = distinct_in_delivery_order(&tail_delivered);
    if distinct_tail != expected_tail {
        return ProbeResult::fail(
            property,
            format!(
                "list_after({list_prefix}, start_after={marker}) returned {} distinct keys, \
                 expected exactly {}: got {distinct_tail:?}, expected {expected_tail:?}",
                distinct_tail.len(),
                expected_tail.len()
            ),
        );
    }

    ProbeResult::pass(
        property,
        format!(
            "{} keys written out of order were listed in lexicographic order, and \
             start_after={marker} resumed at {} with exactly {} keys",
            expected.len(),
            expected_tail[0],
            expected_tail.len()
        ),
    )
}

/// How many keys [`probe_cross_page_listing`] writes. Small enough to be cheap
/// on a real bucket, and an odd number so a page size that divides it evenly
/// is not the only shape exercised.
const PAGE_PROBE_KEYS: usize = 5;

/// Cross-page listing consistency, the probe ADR-0050 section 6 names and the
/// suite did not have: every key written before the first page request is
/// delivered, across however many pages the backend serves, with none lost.
///
/// A key lost between pages is invisible to a caller and to
/// [`probe_consistent_list_after_write`], which only ever looks for one key at
/// a time under a prefix small enough to fit one page.
async fn probe_cross_page_listing(store: &dyn ObjectStoreBackend, prefix: &str) -> ProbeResult {
    let property = Property::CrossPageListing;
    let list_prefix = format!("{prefix}pages/");

    let mut expected: Vec<String> = Vec::with_capacity(PAGE_PROBE_KEYS);
    for i in 0..PAGE_PROBE_KEYS {
        let key = format!("{list_prefix}k{i}");
        if let Err(err) = store
            .put(&key, Bytes::from_static(b"x"), PutOptions::default())
            .await
        {
            return ProbeResult::fail(property, format!("put {key} failed: {err}"));
        }
        expected.push(key);
    }
    expected.sort();

    let (delivered, pages) = match drain_pages(store, &list_prefix, None).await {
        Ok(result) => result,
        Err(detail) => return ProbeResult::fail(property, detail),
    };
    let mut distinct = distinct_in_delivery_order(&delivered);
    distinct.sort();
    if distinct != expected {
        return ProbeResult::fail(
            property,
            format!(
                "a paginated listing of {list_prefix} returned {} distinct keys across {pages} \
                 pages, expected exactly {PAGE_PROBE_KEYS}: got {distinct:?}, expected \
                 {expected:?}; a key was lost or invented across pages",
                distinct.len()
            ),
        );
    }

    ProbeResult::pass(
        property,
        format!(
            "{PAGE_PROBE_KEYS} distinct keys across {pages} pages, none lost \
             ({} deliveries; the cross-page guarantee allows repeats)",
            delivered.len()
        ),
    )
}

/// Delete visibility: after a delete the key is gone from both access paths a
/// caller has, and deleting it again changes nothing.
///
/// Nothing probed delete at all, so a backend that acknowledged a delete
/// without performing it, or performed it lazily, could qualify. Ravel's
/// retention sweep, GC, and ADR-0064 erasure all read a delete's
/// acknowledgement as the object being gone.
async fn probe_delete_visibility(store: &dyn ObjectStoreBackend, prefix: &str) -> ProbeResult {
    let property = Property::DeleteVisibility;
    let list_prefix = format!("{prefix}delete/");
    // Two keys, one deleted: the listing check then pins an exact survivor set,
    // so a backend that deletes the whole prefix fails as loudly as one that
    // deletes nothing.
    let kept = format!("{list_prefix}kept");
    let gone = format!("{list_prefix}gone");

    for key in [&kept, &gone] {
        if let Err(err) = store
            .put(key, Bytes::from_static(b"x"), PutOptions::default())
            .await
        {
            return ProbeResult::fail(property, format!("put {key} failed: {err}"));
        }
    }
    if let Err(err) = store.delete(&gone).await {
        return ProbeResult::fail(property, format!("delete of {gone} failed: {err}"));
    }

    match store.get(&gone, GetRange::Full).await {
        Err(StoreError::NotFound) => {}
        Ok(outcome) => {
            return ProbeResult::fail(
                property,
                format!(
                    "a get of {gone} after a successful delete still returned {} bytes; this \
                     backend acknowledges deletes it has not applied",
                    outcome.data.len()
                ),
            );
        }
        Err(other) => {
            return ProbeResult::fail(
                property,
                format!(
                    "a get of {gone} after a successful delete failed with {other} instead of NotFound"
                ),
            );
        }
    }

    let expected = vec![kept.clone()];
    let (delivered, _pages) = match drain_pages(store, &list_prefix, None).await {
        Ok(result) => result,
        Err(detail) => return ProbeResult::fail(property, detail),
    };
    let distinct = distinct_in_delivery_order(&delivered);
    if distinct != expected {
        return ProbeResult::fail(
            property,
            format!(
                "listing {list_prefix} after deleting {gone} returned {} keys, expected exactly \
                 1 ({kept}): got {distinct:?}",
                distinct.len()
            ),
        );
    }

    // Idempotence: a second delete of an absent key succeeds and changes no
    // observable state.
    if let Err(err) = store.delete(&gone).await {
        return ProbeResult::fail(
            property,
            format!(
                "a second delete of the already-deleted {gone} failed with {err}; delete of an absent key must succeed"
            ),
        );
    }
    let (delivered, _pages) = match drain_pages(store, &list_prefix, None).await {
        Ok(result) => result,
        Err(detail) => return ProbeResult::fail(property, detail),
    };
    let distinct = distinct_in_delivery_order(&delivered);
    if distinct != expected {
        return ProbeResult::fail(
            property,
            format!(
                "a second delete of the absent {gone} changed the listing of {list_prefix} to \
                 {distinct:?}, expected it to still hold exactly 1 key ({kept})"
            ),
        );
    }

    ProbeResult::pass(
        property,
        format!(
            "after deleting {gone}, a get returned NotFound and the listing held exactly 1 key \
             ({kept}); a second delete of the absent key succeeded and changed nothing"
        ),
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

    /// Every property [`run_conformance_suite`] gates on. Pinned here so a new
    /// probe has to be acknowledged in the tests that assert the suite's shape
    /// rather than silently widening them.
    const GATING_PROPERTIES: usize = 8;

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
        // the conforming oracle passes, and the suite carries exactly its
        // gating properties -- the probe is none of them.
        let report = run_conformance_suite(&store, "sys/qualify/object-lock/").await;
        assert!(
            report.passed(),
            "the informational probe must not change the qualification result"
        );
        assert_eq!(
            report.results.len(),
            GATING_PROPERTIES,
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
        assert_eq!(report.results.len(), GATING_PROPERTIES);
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
            vec![
                Property::ConsistentListAfterWrite,
                Property::LexicographicListingOrder,
                Property::CrossPageListing,
                Property::DeleteVisibility,
            ],
            "every listing-dependent property should be named (the delete probe confirms the \
             deletion through a listing too); conditional writes and read-after-write are \
             untouched"
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
        // The concurrent race is the same missing precondition seen under
        // contention: all eight writers win instead of one.
        assert!(failed.contains(Property::ConcurrentCreateIfAbsentSingleWinner.name()));
        assert_eq!(failed.len(), 3, "unexpected extra failures: {failed:?}");
        // Listing, read-after-write, and delete are untouched by this backend.
        assert!(!failed.contains(Property::ConsistentListAfterWrite.name()));
        assert!(!failed.contains(Property::ConsistentReadAfterWrite.name()));
        assert!(!failed.contains(Property::LexicographicListingOrder.name()));
        assert!(!failed.contains(Property::CrossPageListing.name()));
        assert!(!failed.contains(Property::DeleteVisibility.name()));

        let race = report
            .results
            .iter()
            .find(|r| r.property == Property::ConcurrentCreateIfAbsentSingleWinner)
            .expect("the concurrent create probe ran");
        assert!(
            race.detail
                .contains("8 of 8 concurrent CreateIfAbsent writers on one absent key succeeded"),
            "the failure must name the exact winner count: {}",
            race.detail
        );
    }

    /// Wraps `MemoryStore` and reverses the key order of every listing page it
    /// serves, leaving the page tokens, the delivered key set, and every other
    /// operation exactly as the oracle produced them. Models a backend whose
    /// listing is complete but not lexicographically ordered.
    ///
    /// Violated invariant: `ListingConsumersConsistent`
    /// (formal/tla/common/traceability.md row for `ListReturn`). A
    /// continuation token names "resume after this key", which is only a
    /// position when the delivery order is the lexicographic key order; under
    /// a reversed order a paging consumer's deduplicated view no longer tracks
    /// the delivered support, which is what `S3Store::list` pagination and
    /// every catalog scan built on it assume.
    struct UnsortedListStore {
        inner: MemoryStore,
    }

    impl UnsortedListStore {
        fn new() -> Self {
            UnsortedListStore {
                inner: MemoryStore::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for UnsortedListStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<crate::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
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
            page_result.objects.reverse();
            Ok(page_result)
        }

        async fn list_after(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            let mut page_result = self.inner.list_after(prefix, start_after, page).await?;
            page_result.objects.reverse();
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

    /// A backend whose listing is complete but unordered fails qualification,
    /// and fails it on exactly the ordering property: the delivered key set is
    /// untouched, so nothing that only checks membership can catch this.
    #[tokio::test]
    async fn unsorted_listing_backend_fails_qualification() {
        let store = UnsortedListStore::new();
        let report = run_conformance_suite(&store, "sys/qualify/unsorted/").await;
        assert!(!report.passed());
        let failed: Vec<Property> = report.failures().map(|r| r.property).collect();
        assert_eq!(
            failed,
            vec![Property::LexicographicListingOrder],
            "only the ordering property should be named: this backend loses no key, so the \
             membership-based probes are untouched"
        );
        let failure = report
            .results
            .iter()
            .find(|r| r.property == Property::LexicographicListingOrder)
            .expect("the ordering probe ran");
        assert!(
            failure.detail.contains("not in lexicographic key order"),
            "the failure must say what is wrong: {}",
            failure.detail
        );
    }

    /// Wraps `MemoryStore` and acknowledges every delete without applying it.
    /// Models a backend that returns 204 for a delete it never performed (or
    /// performs lazily), which every other probe is blind to.
    ///
    /// Violated invariant: `DeleteIdempotent`
    /// (formal/tla/common/traceability.md `Delete / DeleteIdempotent`), whose
    /// post-state requires the key to be absent after a delete.
    struct LyingDeleteStore {
        inner: MemoryStore,
    }

    impl LyingDeleteStore {
        fn new() -> Self {
            LyingDeleteStore {
                inner: MemoryStore::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for LyingDeleteStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<crate::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
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

        async fn list_after(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list_after(prefix, start_after, page).await
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, _key: &str) -> Result<(), StoreError> {
            Ok(())
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    /// A backend that acknowledges a delete it never applied fails
    /// qualification on exactly the delete-visibility property.
    #[tokio::test]
    async fn lying_delete_visibility_backend_fails_qualification() {
        let store = LyingDeleteStore::new();
        let report = run_conformance_suite(&store, "sys/qualify/lying-delete/").await;
        assert!(!report.passed());
        let failed: Vec<Property> = report.failures().map(|r| r.property).collect();
        assert_eq!(
            failed,
            vec![Property::DeleteVisibility],
            "only delete visibility should be named; writes, reads, and listing are untouched"
        );
        let failure = report
            .results
            .iter()
            .find(|r| r.property == Property::DeleteVisibility)
            .expect("the delete probe ran");
        assert!(
            failure
                .detail
                .contains("acknowledges deletes it has not applied"),
            "the failure must say what is wrong: {}",
            failure.detail
        );
    }

    /// The concurrent single-winner race (`CreateIfAbsentWinnerUnique`) with
    /// its exact counts, both directly against the oracle and through the
    /// probe: of eight writers creating one absent key with all eight requests
    /// in flight, exactly one gets `Ok`, exactly seven get `AlreadyExists`, and
    /// the surviving object holds the winner's bytes.
    #[tokio::test]
    async fn concurrent_create_if_absent_has_exactly_one_winner() {
        // (a) The oracle, raced directly, so the counts are pinned
        // independently of how the probe reports them.
        let store = MemoryStore::new();
        let key = "race/k";
        let payloads: Vec<Bytes> = (0..CONCURRENT_CREATE_WRITERS)
            .map(|i| Bytes::from(format!("writer-{i}")))
            .collect();
        let outcomes = futures::future::join_all(
            payloads
                .iter()
                .map(|payload| store.put(key, payload.clone(), PutOptions::create_if_absent())),
        )
        .await;
        let winners: Vec<usize> = outcomes
            .iter()
            .enumerate()
            .filter(|(_, outcome)| outcome.is_ok())
            .map(|(i, _)| i)
            .collect();
        let losers = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(StoreError::AlreadyExists)))
            .count();
        assert_eq!(
            winners.len(),
            1,
            "expected exactly one winner: {outcomes:?}"
        );
        assert_eq!(
            losers,
            CONCURRENT_CREATE_WRITERS - 1,
            "expected exactly {} AlreadyExists: {outcomes:?}",
            CONCURRENT_CREATE_WRITERS - 1
        );
        let survivor = store
            .get(key, GetRange::Full)
            .await
            .expect("the winner's object is readable");
        assert_eq!(
            survivor.data, payloads[winners[0]],
            "the surviving bytes must be the winner's"
        );

        // (b) The probe reaches the same verdict on the oracle, and says so
        // with the exact counts.
        let report = run_conformance_suite(&store, "sys/qualify/race/").await;
        assert!(
            report.passed(),
            "the oracle must pass every probe, got: {:?}",
            report.failures().collect::<Vec<_>>()
        );
        let race = report
            .results
            .iter()
            .find(|r| r.property == Property::ConcurrentCreateIfAbsentSingleWinner)
            .expect("the concurrent create probe ran");
        assert!(
            race.detail.contains(
                "exactly 1 of 8 concurrent CreateIfAbsent writers won, the other 7 were rejected"
            ),
            "the pass detail must name the exact counts: {}",
            race.detail
        );
    }

    /// Wraps `MemoryStore` and reports a losing racer of the concurrent-create
    /// probe's key as a retryable [`StoreError::Transient`] instead of
    /// `AlreadyExists`. `once()` serves exactly one `Transient` per losing
    /// payload and then lets the real `AlreadyExists` through, which is the
    /// shape `S3Store` produces when its HEAD disambiguation of a 409 finds the
    /// key still absent; `forever()` never settles.
    ///
    /// The deflection is scoped to the one key the suite actually races: the
    /// contract permits a retryable answer only for a conditional write racing
    /// another writer, so a sequential second create on a present key must
    /// still be `AlreadyExists` on its first attempt, and every other probe
    /// sees the untouched oracle.
    struct TransientLoserStore {
        inner: MemoryStore,
        forever: bool,
        state: Mutex<TransientLoserState>,
    }

    #[derive(Default)]
    struct TransientLoserState {
        /// Payloads already answered with one `Transient`.
        deflected: HashSet<Bytes>,
        /// Every `Transient` served, so a test can pin how many attempts the
        /// probe actually paid for rather than assuming the retry ran.
        served: usize,
    }

    impl TransientLoserStore {
        fn once() -> Self {
            TransientLoserStore {
                inner: MemoryStore::new(),
                forever: false,
                state: Mutex::new(TransientLoserState::default()),
            }
        }

        fn forever() -> Self {
            TransientLoserStore {
                inner: MemoryStore::new(),
                forever: true,
                state: Mutex::new(TransientLoserState::default()),
            }
        }

        fn transients_served(&self) -> usize {
            self.state.lock().served
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for TransientLoserStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<crate::PutOutcome, StoreError> {
            let racing_create = matches!(opts.mode, PutMode::CreateIfAbsent)
                && key.ends_with(CONCURRENT_CREATE_KEY_SUFFIX);
            let outcome = self.inner.put(key, data.clone(), opts).await;
            if !racing_create || !matches!(outcome, Err(StoreError::AlreadyExists)) {
                return outcome;
            }
            let mut state = self.state.lock();
            if !self.forever && !state.deflected.insert(data) {
                return outcome;
            }
            state.served += 1;
            Err(StoreError::Transient(
                "conditional request conflict while the key still looked absent".to_string(),
            ))
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
            self.inner.capabilities()
        }
    }

    /// A losing racer whose first answer is one retryable error is conformant
    /// (docs/object-store-contract.md: a concurrent conditional write MAY
    /// surface as a retryable transient conflict, and the loser lands on
    /// `AlreadyExists` after a retry), so the probe must retry it and still
    /// certify the single-winner property with the exact counts.
    ///
    /// Counterpart of `PutCreateIfAbsent / CreateIfAbsentWinnerUnique` composed
    /// with `TransientFailure / TransientLeavesNothing`
    /// (formal/tla/common/traceability.md): the transient answer applies
    /// nothing and the caller retries the identical operation, after which the
    /// winner is still unique.
    #[tokio::test]
    async fn losing_racer_transient_once_is_retried_and_the_probe_still_passes() {
        let store = TransientLoserStore::once();
        let report = run_conformance_suite(&store, "sys/qualify/transient-once/").await;
        assert!(
            report.passed(),
            "a backend whose losing racers are retryable exactly once is conformant, got \
             failures: {:?}",
            report.failures().collect::<Vec<_>>()
        );
        let race = report
            .results
            .iter()
            .find(|r| r.property == Property::ConcurrentCreateIfAbsentSingleWinner)
            .expect("the concurrent create probe ran");
        assert!(
            race.detail.contains(
                "exactly 1 of 8 concurrent CreateIfAbsent writers won, the other 7 were rejected"
            ),
            "the pass detail must still name the exact counts: {}",
            race.detail
        );
        assert_eq!(
            store.transients_served(),
            CONCURRENT_CREATE_WRITERS - 1,
            "every loser must have been served its one Transient, so the pass above was \
             reached through the retry and not by the fixture never firing"
        );
    }

    /// A losing racer that stays retryable forever never demonstrates the
    /// single-winner property, so the probe must fail naming its attempt bound
    /// rather than pass on an unsettled outcome.
    ///
    /// Counterpart of `PutCreateIfAbsent / CreateIfAbsentWinnerUnique`
    /// (formal/tla/common/traceability.md): the model's create resolves to a
    /// winner or an `AlreadyExists` loser, and a backend that resolves to
    /// neither has not been shown to satisfy it.
    #[tokio::test]
    async fn losing_racer_transient_forever_fails_the_probe_naming_the_attempt_bound() {
        let store = TransientLoserStore::forever();
        let report = run_conformance_suite(&store, "sys/qualify/transient-forever/").await;
        assert!(!report.passed(), "an unsettled racer must not qualify");
        let failed: HashSet<&'static str> = report.failures().map(|r| r.property.name()).collect();
        assert_eq!(
            failed,
            HashSet::from([Property::ConcurrentCreateIfAbsentSingleWinner.name()]),
            "only the concurrent-create property is affected: {failed:?}"
        );
        let race = report
            .results
            .iter()
            .find(|r| r.property == Property::ConcurrentCreateIfAbsentSingleWinner)
            .expect("the concurrent create probe ran");
        assert!(
            race.detail
                .contains(&format!("after {CONCURRENT_CREATE_MAX_ATTEMPTS} attempts")),
            "the failure must name the attempt bound: {}",
            race.detail
        );
        assert!(
            race.detail.contains("single-winner property"),
            "the failure must name the invariant it could not evaluate: {}",
            race.detail
        );
        assert_eq!(
            store.transients_served(),
            (CONCURRENT_CREATE_WRITERS - 1) * CONCURRENT_CREATE_MAX_ATTEMPTS,
            "each of the {} losers must have been retried up to the bound",
            CONCURRENT_CREATE_WRITERS - 1
        );
    }

    /// Wraps `MemoryStore` and loses the winning racer's response: the first
    /// concurrent create the oracle accepts is durable, but this store answers
    /// the winner with a retryable [`StoreError::Transient`] instead of `Ok`.
    /// The winner's retry then races the by-then present key and lands on
    /// `AlreadyExists`, so every writer reports `AlreadyExists` and no winner is
    /// ever observed even though exactly one write is durable.
    ///
    /// The deflection is scoped to the one key the suite races and fires exactly
    /// once, on the single accepted `Ok`; every real loser and every other probe
    /// sees the untouched oracle.
    struct LostWinnerResponseStore {
        inner: MemoryStore,
        state: Mutex<LostWinnerState>,
    }

    #[derive(Default)]
    struct LostWinnerState {
        /// Whether the one accepted create has already been deflected, so the
        /// winner's retry falls through to the real `AlreadyExists`.
        deflected: bool,
        /// The winner's payload after it was deflected, so a test can name which
        /// writer took the retry path rather than assume it.
        winner_payload: Option<Bytes>,
    }

    impl LostWinnerResponseStore {
        fn new() -> Self {
            LostWinnerResponseStore {
                inner: MemoryStore::new(),
                state: Mutex::new(LostWinnerState::default()),
            }
        }

        fn winner_payload(&self) -> Option<Bytes> {
            self.state.lock().winner_payload.clone()
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for LostWinnerResponseStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<crate::PutOutcome, StoreError> {
            let racing_create = matches!(opts.mode, PutMode::CreateIfAbsent)
                && key.ends_with(CONCURRENT_CREATE_KEY_SUFFIX);
            let outcome = self.inner.put(key, data.clone(), opts).await;
            if !racing_create || outcome.is_err() {
                return outcome;
            }
            let mut state = self.state.lock();
            if state.deflected {
                return outcome;
            }
            // The oracle already stored the object, so the create is durable.
            // Report the winner a retryable failure: its retry sees the present
            // key and answers AlreadyExists, dropping the winner from the counts.
            state.deflected = true;
            state.winner_payload = Some(data);
            Err(StoreError::Transient(
                "winner's response lost while the create was durable".to_string(),
            ))
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
            self.inner.capabilities()
        }
    }

    /// When the writer that actually won loses its response, its retry answers
    /// `AlreadyExists` and no winner is observed. The probe must not blame a
    /// non-atomic create -- the real cause is a lost response the counts cannot
    /// distinguish -- but it must still FAIL, because the run never established a
    /// single winner and qualification requires exactly one.
    ///
    /// Counterpart of the TLA action `PutCreateIfAbsentLostResponse`
    /// (formal/tla/common): a durable create surfaces a retryable failure, so the
    /// caller's retry sees the write already present.
    #[tokio::test]
    async fn lost_winner_response_fails_the_probe_as_unevaluable_not_non_atomic() {
        let store = LostWinnerResponseStore::new();
        let report = run_conformance_suite(&store, "sys/qualify/lost-winner/").await;
        assert!(
            !report.passed(),
            "a run that never established a winner must not qualify"
        );
        let failed: HashSet<&'static str> = report.failures().map(|r| r.property.name()).collect();
        assert_eq!(
            failed,
            HashSet::from([Property::ConcurrentCreateIfAbsentSingleWinner.name()]),
            "only the concurrent-create property is affected: {failed:?}"
        );
        let race = report
            .results
            .iter()
            .find(|r| r.property == Property::ConcurrentCreateIfAbsentSingleWinner)
            .expect("the concurrent create probe ran");
        assert!(
            race.detail.contains("could not establish which writer won"),
            "the failure must name the unevaluable cause: {}",
            race.detail
        );
        let winner_payload = store
            .winner_payload()
            .expect("the winner was deflected once");
        let winner_index = String::from_utf8_lossy(&winner_payload)
            .strip_prefix("writer-")
            .expect("payloads are writer-<i>")
            .to_string();
        assert!(
            race.detail.contains(&format!("writer-{winner_index}")),
            "the failure must name the retried writer ({winner_index}): {}",
            race.detail
        );
        assert!(
            !race.detail.contains("not atomic"),
            "the failure must not blame a non-atomic create when a writer was retried: {}",
            race.detail
        );
    }

    /// Cross-page listing with the pagination oracle
    /// (`MemoryStore::with_page_size(2)`): the probe's five keys come back as
    /// exactly five distinct keys across exactly three pages, none lost
    /// between pages.
    #[tokio::test]
    async fn cross_page_listing_over_five_keys_at_page_size_two_returns_all_five() {
        let store = MemoryStore::with_page_size(2);
        let report = run_conformance_suite(&store, "sys/qualify/pages/").await;
        assert!(
            report.passed(),
            "the oracle must pass every probe at page size 2, got: {:?}",
            report.failures().collect::<Vec<_>>()
        );
        let paging = report
            .results
            .iter()
            .find(|r| r.property == Property::CrossPageListing)
            .expect("the cross-page probe ran");
        assert!(
            paging.detail.contains("5 distinct keys across 3 pages"),
            "the pass detail must name the exact key and page counts: {}",
            paging.detail
        );

        // The same shape, walked directly over the keys the probe left behind:
        // three pages, five distinct keys, and the multi-page path really was
        // exercised (a one-page listing would prove nothing about it).
        let mut pages = 0usize;
        let mut keys: Vec<String> = Vec::new();
        let mut token = None;
        loop {
            let page = store
                .list("sys/qualify/pages/pages/", token)
                .await
                .expect("listing the probe's own prefix");
            pages += 1;
            keys.extend(page.objects.into_iter().map(|meta| meta.key));
            match page.next {
                Some(next) => token = Some(next),
                None => break,
            }
        }
        assert_eq!(pages, 3, "5 keys at page size 2 is exactly 3 pages");
        assert_eq!(keys.len(), 5);
        assert_eq!(
            keys.iter().collect::<HashSet<_>>().len(),
            5,
            "5 distinct keys: {keys:?}"
        );
    }

    /// A delete fault must surface as a named, typed [`ProbeResult`] failure
    /// rather than a panic, and the fault must actually have fired (repo
    /// testing pattern: assert `FaultStore` counters).
    #[tokio::test]
    async fn delete_fault_surfaces_as_named_probe_failure() {
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Delete, ScriptedFault::Timeout)
                .with_key_contains("delete/gone")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(MemoryStore::new(), plan);
        let report = run_conformance_suite(&store, "sys/qualify/delete-fault/").await;
        assert!(!report.passed());
        let failed: Vec<Property> = report.failures().map(|r| r.property).collect();
        assert_eq!(failed, vec![Property::DeleteVisibility]);
        let failure = report
            .results
            .iter()
            .find(|r| r.property == Property::DeleteVisibility)
            .expect("the delete probe ran");
        assert!(
            failure.detail.contains("timeout"),
            "the failure must carry the backend's own error: {}",
            failure.detail
        );
        assert_eq!(store.fault_count(Op::Delete, FaultKind::Timeout), 1);
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
    /// classification is pinned complementarily in `s3.rs`, where
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
    /// noncurrent-version expiration and the required abort-incomplete rule
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
    /// configuration; a missing abort-incomplete rule adds a NOTE, which marks a
    /// contract violation the probe cannot confirm rather than an optional gap.
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
        assert_eq!(report.results.len(), GATING_PROPERTIES);
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

    /// `PutOverwriteLostResponse / LostResponseEffectApplied`
    /// (formal/tla/common/traceability.md): a lost-response write still
    /// applies its durable effect even though the caller observes a
    /// failure, modeled here by `FaultKind::DuplicateDelivery` on `Put`
    /// (`FaultStore` calls the wrapped `put` for real, then returns
    /// `Transient`, per its module doc). This proves only the
    /// memory-oracle half of the row; the backend half stays an
    /// assumption until a probe exists against a real S3-compatible
    /// endpoint.
    #[tokio::test]
    async fn lost_ack_after_successful_put_leaves_object_visible() {
        let key = "lost-ack/k";
        let plan =
            FaultPlan::empty().with_rule(Rule::new(Op::Put, ScriptedFault::DuplicateDelivery));
        let store = FaultStore::new(MemoryStore::new(), plan);

        let err = store
            .put(key, Bytes::from_static(b"v1"), PutOptions::default())
            .await
            .expect_err("a lost-response PUT must surface an error to the caller");
        assert!(matches!(err, StoreError::Transient(_)), "got {err:?}");
        assert_eq!(store.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);

        let got = store
            .get(key, GetRange::Full)
            .await
            .expect("the object must be visible even though the caller saw a failure");
        assert_eq!(&got.data[..], b"v1");
    }

    /// `TransientFailure / TransientLeavesNothing`
    /// (formal/tla/common/traceability.md): a transient failure applies
    /// nothing, modeled here by `FaultKind::Transient` on `Put`
    /// (`FaultStore` never calls the wrapped backend for that kind, per
    /// its module doc). This proves only the memory-oracle half of the
    /// row; the backend half stays an assumption until a probe exists
    /// against a real S3-compatible endpoint.
    #[tokio::test]
    async fn failed_put_leaves_no_object() {
        let key = "transient-put/k";
        let plan = FaultPlan::empty()
            .with_rule(Rule::new(Op::Put, ScriptedFault::Transient("blip".into())));
        let store = FaultStore::new(MemoryStore::new(), plan);

        let err = store
            .put(key, Bytes::from_static(b"v1"), PutOptions::default())
            .await
            .expect_err("the transient fault must surface as an error");
        assert!(matches!(err, StoreError::Transient(_)), "got {err:?}");
        assert_eq!(store.fault_count(Op::Put, FaultKind::Transient), 1);

        let err = store
            .head(key)
            .await
            .expect_err("a failed PUT must leave no object, partial or otherwise");
        assert!(matches!(err, StoreError::NotFound), "got {err:?}");
    }
}
