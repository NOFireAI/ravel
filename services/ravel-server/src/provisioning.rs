//! Server-side wiring for the durable `shard_count` provisioning record
//! (ADR-0050 section 5, EC5). The record itself and the validate-or-adopt
//! decision live in `ravel_catalog::provisioning`; this module wires that one
//! shared function into the two consumers the server owns directly:
//!
//! - the ingest first-write path ([`ProvisioningRecordWriter`], threaded into
//!   every `IngestState` like the recovery-manifest writer), which pins the
//!   configured `shard_count` in the record as a tenant's data first lands, and
//! - the startup static-tenant check ([`validate_static_provisioning`]), which
//!   refuses to start only when a statically-known tenant's record is
//!   unreadable, has a structurally invalid generation history
//!   ([`ProvisioningError::CorruptGenerations`]), or when adopting the
//!   configured value would hide pre-ADR data. A decodable record with a valid
//!   history whose recorded `shard_count` differs from the live `--shards`
//!   default is tolerated: routing uses the record's own generation history
//!   (ADR-0082).
//!
//! The catalog resolve consumer is wired in `ravel_catalog` itself
//! (`Catalog::with_provisioning_enforcement`); the maintain per-tenant loop is
//! wired in [`crate::maintain`]. All three go through
//! [`ravel_catalog::validate_or_adopt`].
//!
//! Fresh-deployment safety: a brand-new tenant with no
//! prior writes and no provisioning record must never fail startup. The startup
//! check uses [`AbsentPolicy::AdoptIfData`], which returns
//! [`ProvisioningCheck::FreshNoData`] for a (tenant, signal) with no record and
//! no data, so an operator-managed cluster that starts with zero data and
//! configured tenant tokens passes through cleanly; only pre-ADR data a lower
//! `shard_count` would hide, an unreadable record, or a record with a
//! structurally invalid generation history, refuses. A decodable record with
//! a valid history whose recorded `shard_count` differs from the live default
//! is tolerated (ADR-0082).

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use ravel_catalog::{AbsentPolicy, ProvisioningCheck, ProvisioningError, validate_or_adopt};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::{Signal, TenantHash, TenantId};

/// The signals the server ingests and maintains, and therefore provisions a
/// `shard_count` record for. Mirrors `crate::maintain::MAINTAINED_SIGNALS` and
/// `crate::fold::FOLD_SIGNALS`; the startup check iterates every statically
/// known tenant across exactly these.
pub const PROVISIONED_SIGNALS: [Signal; 3] = [Signal::Metrics, Signal::Logs, Signal::Spans];

/// Count of dynamic-tenant first-touch hard provisioning failures: an
/// unreadable record (`UnsupportedVersion`, `CorruptRecord`, `Decode`) or
/// pre-ADR data a lower value would hide (`AdoptionWouldHideData`), rendered at
/// `/metrics` as `ravel_provisioning_shard_count_mismatch_total`. A recorded
/// `shard_count` that merely differs from the live default is no longer a
/// failure (ADR-0082): that drift is tolerated and counted separately by
/// `ravel_catalog::shard_count_drift_count`. Process-global with a single
/// source and no labels, mirroring [`crate::tenancy::v1_unkeyed_adoption_count`].
static SHARD_COUNT_MISMATCHES: AtomicU64 = AtomicU64::new(0);

/// The dynamic-mismatch count for the `/metrics` renderer.
pub fn shard_count_mismatch_count() -> u64 {
    SHARD_COUNT_MISMATCHES.load(Ordering::Relaxed)
}

/// Whether a provisioning failure must fail the touch that raised it rather
/// than be logged-and-ignored. A `Store` error is the one fail-open case: it is
/// transient and retry-friendly, so a store blip never blocks all ingest. A
/// recorded `shard_count` that differs from the live default is no longer a
/// failure at all (ADR-0082): it is tolerated and never reaches here. Every
/// remaining variant means the tenant's true `shard_count` is unknown or
/// adopting the live value would hide data -- an `AdoptionWouldHideData`
/// refusal, an unreadable record (`UnsupportedVersion`, `CorruptRecord`,
/// `Decode`), or a decodable record whose generation history fails its
/// structural invariants (`CorruptGenerations`) -- whose recorded
/// `shard_count` cannot be trusted. Proceeding with ingest under an unknown
/// `shard_count` is exactly the silent-data-hiding this record exists to
/// prevent (ADR-0050 section 5), so those all fail hard: the version guard
/// refuses "rather than misread a future record format", and a fail-open here
/// would defeat that guard's own stated purpose. `CorruptGenerations` must fail
/// hard for the same reason: `Catalog::enforce_provisioning_once` rejects the
/// identical record via the same `validate_or_adopt` call, so failing open
/// here would let the ingest router route a write the catalog refuses to
/// resolve.
fn is_hard_failure(err: &ProvisioningError) -> bool {
    matches!(
        err,
        ProvisioningError::AdoptionWouldHideData { .. }
            | ProvisioningError::UnsupportedVersion { .. }
            | ProvisioningError::CorruptRecord { .. }
            | ProvisioningError::Decode { .. }
            | ProvisioningError::CorruptGenerations { .. }
    )
}

/// Increment the process-global mismatch counter if `err` is a hard provisioning
/// failure. Shared by the consumers that catch a provisioning error off the
/// ingest hot path (the maintain per-tenant loop), so an alert keyed on the
/// counter fires for a maintain-only mismatch too, not just an ingest one.
pub(crate) fn note_provisioning_failure(err: &ProvisioningError) {
    if is_hard_failure(err) {
        SHARD_COUNT_MISMATCHES.fetch_add(1, Ordering::Relaxed);
    }
}

/// Writes and validates each (tenant, signal)'s provisioning record on the
/// tenant's first write for that signal in this process (ADR-0050 section 5),
/// mirroring [`crate::tenancy::RecoveryManifestWriter`]. A per-process
/// in-memory set makes every write after the first a cheap set hit; the
/// `CreateIfAbsent` in [`validate_or_adopt`] handles the cross-process race.
///
/// Built once per process from the configured `shard_count` and shared across
/// the metrics, logs, spans, and remote-write ingest paths, so the "seen"
/// set is shared and one tenant's first write across any transport provisions
/// the record once.
pub struct ProvisioningRecordWriter {
    store: Arc<dyn ObjectStoreBackend>,
    shard_count: u32,
    seen: Mutex<HashSet<(TenantHash, Signal)>>,
}

impl ProvisioningRecordWriter {
    pub fn new(store: Arc<dyn ObjectStoreBackend>, shard_count: u32) -> Self {
        ProvisioningRecordWriter {
            store,
            shard_count,
            seen: Mutex::new(HashSet::new()),
        }
    }

    /// Ensure the (tenant, signal) provisioning record on a first write.
    ///
    /// On a hard provisioning failure (the dynamic-tenant first-touch case,
    /// ADR-0050 section 5) -- a `shard_count` disagreement, or an unreadable
    /// record whose true `shard_count` cannot be trusted -- increments the
    /// process-global mismatch counter and returns the typed error so the caller
    /// fails that one request; the process is never taken down for a single
    /// dynamic tenant. Only a transient store error is logged and treated as
    /// success for the write path (ingest continues): it is retry-friendly and
    /// not the tenant's fault. A genuine success (created, adopted, or matched)
    /// is cached in `seen`.
    pub async fn ensure(
        &self,
        tenant_hash: &TenantHash,
        signal: Signal,
        now_ns: i64,
    ) -> Result<(), ProvisioningError> {
        if self.seen.lock().contains(&(*tenant_hash, signal)) {
            return Ok(());
        }
        match validate_or_adopt(
            self.store.as_ref(),
            tenant_hash,
            signal,
            self.shard_count,
            now_ns,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        {
            Ok(_) => {
                self.seen.lock().insert((*tenant_hash, signal));
                Ok(())
            }
            Err(err) if is_hard_failure(&err) => {
                SHARD_COUNT_MISMATCHES.fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
            Err(err) => {
                tracing::warn!(
                    %err,
                    signal = signal.key_prefix(),
                    "provisioning record write hit a transient store error; ingest continues"
                );
                Ok(())
            }
        }
    }
}

/// Best-effort provisioning-record write on an ingest request's tenant, called
/// from every server ingest handler before the write (ADR-0050 section 5), the
/// provisioning analogue of [`crate::tenancy::ensure_recovery_manifest`].
///
/// A no-op when `writer` is `None`. Returns the typed error on a hard
/// provisioning failure (a `shard_count` mismatch or an unreadable record), so
/// the handler fails that one request; a transient store error is logged inside
/// [`ProvisioningRecordWriter::ensure`] and reported as success here.
pub async fn ensure_provisioning_record(
    writer: &Option<Arc<ProvisioningRecordWriter>>,
    tenant: &TenantId,
    signal: Signal,
    now_ns: i64,
) -> Result<(), ProvisioningError> {
    match writer {
        Some(writer) => writer.ensure(&tenant.hash(), signal, now_ns).await,
        None => Ok(()),
    }
}

/// Validate the configured `shard_count` for every statically-known tenant at
/// startup (ADR-0050 section 5), refusing to start on the first disagreement.
/// The static tenant set is the union of `--tenant-token` and
/// `--maintain-tenant` (already hashed), so an OIDC/mTLS deployment with no
/// static tenants (an empty set) has nothing to validate here and every dynamic
/// tenant is validated at first touch instead.
///
/// Uses [`AbsentPolicy::AdoptIfData`]: a (tenant, signal) with no record and no
/// data passes through without refusing (the fresh-deployment case), a
/// (tenant, signal) with pre-ADR data is adopted once, and only a present
/// record that disagrees or pre-ADR data a lower value would hide refuses. This
/// is the fresh-deployment property: a brand-new tenant with no
/// prior writes and no provisioning record does not fail startup.
pub async fn validate_static_provisioning(
    store: &dyn ObjectStoreBackend,
    static_tenants: &[TenantHash],
    shard_count: u32,
    now_ns: i64,
) -> Result<(), ProvisioningError> {
    for tenant_hash in static_tenants {
        for signal in PROVISIONED_SIGNALS {
            let check = validate_or_adopt(
                store,
                tenant_hash,
                signal,
                shard_count,
                now_ns,
                AbsentPolicy::AdoptIfData,
            )
            .await?;
            if matches!(check, ProvisioningCheck::Written) {
                tracing::info!(
                    tenant_hash = %tenant_hash.to_hex(),
                    signal = signal.key_prefix(),
                    shard_count,
                    "adopted pre-ADR data into a shard_count provisioning record at startup"
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use prost::Message;
    use ravel_catalog::provisioning_key;
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError};
    use ravel_proto::sys::v1 as sysproto;

    fn store() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    async fn seed_record(
        store: &dyn ObjectStoreBackend,
        th: &TenantHash,
        signal: Signal,
        shard_count: u32,
    ) {
        let record = sysproto::ProvisioningRecord {
            format_version: 1,
            tenant_hash: th.0.to_vec(),
            signal: match signal {
                Signal::Metrics => sysproto::Signal::Metrics,
                Signal::Logs => sysproto::Signal::Logs,
                Signal::Spans => sysproto::Signal::Spans,
                _ => sysproto::Signal::Unspecified,
            } as i32,
            shard_count,
            created_unix_ns: 1,
            generations: Vec::new(),
            format_floors: Vec::new(),
        };
        store
            .put(
                &provisioning_key(th, signal),
                record.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("seed record");
    }

    /// The fresh-deployment guarantee, made a test: a brand-new tenant with no prior
    /// writes and no provisioning record does not fail startup. This is the
    /// exact fresh-`ravel-operator`-cluster shape (configured tenant tokens,
    /// zero data).
    #[tokio::test]
    async fn fresh_tenant_with_no_prior_data_starts_cleanly() {
        let store = store();
        let statics = [TenantId::new("acme").hash(), TenantId::new("globex").hash()];
        validate_static_provisioning(store.as_ref(), &statics, 4, 1_000)
            .await
            .expect("a fresh tenant with no record and no data must not fail startup");
        // And nothing was written for a signal with no data.
        let got = store
            .get(
                &provisioning_key(&statics[0], Signal::Metrics),
                GetRange::Full,
            )
            .await;
        assert!(
            matches!(got, Err(StoreError::NotFound)),
            "no record written"
        );
    }

    /// ADR-0082: a statically-known tenant whose recorded `shard_count` (4)
    /// differs from the live `--shards` default (2) no longer refuses startup.
    /// Before this change `validate_static_provisioning` returned
    /// `ProvisioningError::ShardCountMismatch` and the `.expect(...)` below
    /// panicked; now the drift is tolerated and startup proceeds.
    #[tokio::test]
    async fn static_tenant_drift_does_not_refuse_startup() {
        let store = store();
        let th = TenantId::new("acme").hash();
        seed_record(store.as_ref(), &th, Signal::Metrics, 4).await;
        validate_static_provisioning(store.as_ref(), &[th], 2, 1_000)
            .await
            .expect("a recorded shard_count above the live default is tolerated (ADR-0082)");
    }

    /// ADR-0082 on the dynamic path: a first-touch drift (recorded 4, live
    /// default 2) succeeds rather than failing that one request. Before this
    /// change `ensure` returned `ProvisioningError::ShardCountMismatch` and the
    /// `.expect(...)` below panicked.
    #[tokio::test]
    async fn dynamic_first_touch_drift_succeeds() {
        let store = store();
        let drifted = TenantId::new("acme").hash();
        seed_record(store.as_ref(), &drifted, Signal::Metrics, 4).await;
        let writer = Arc::new(ProvisioningRecordWriter::new(store.clone(), 2));

        writer
            .ensure(&drifted, Signal::Metrics, 1_000)
            .await
            .expect("a drifted dynamic tenant is tolerated on first touch (ADR-0082)");

        // A different tenant is unaffected: it provisions cleanly.
        let other = TenantId::new("globex").hash();
        writer
            .ensure(&other, Signal::Metrics, 1_000)
            .await
            .expect("an unrelated tenant provisions cleanly");
    }

    /// ADR-0082 maintain gate: the per-tenant loop's provisioning gate goes
    /// through `validate_or_adopt(.., AdoptIfData)`, exactly as
    /// [`crate::maintain`] calls it. A drifted record (recorded 4, live default
    /// 2) reports the tenant eligible (returns `RecordPresent`) rather than a
    /// hard failure that would skip its maintain tick. Before this change it
    /// returned `ProvisioningError::ShardCountMismatch` and the `.expect(...)`
    /// below panicked.
    #[tokio::test]
    async fn maintain_gate_tolerates_drift_reports_eligible() {
        let store = store();
        let th = TenantId::new("acme").hash();
        seed_record(store.as_ref(), &th, Signal::Metrics, 4).await;
        let check = validate_or_adopt(
            store.as_ref(),
            &th,
            Signal::Metrics,
            2,
            1_000,
            AbsentPolicy::AdoptIfData,
        )
        .await
        .expect("the maintain gate tolerates drift and reports the tenant eligible (ADR-0082)");
        assert_eq!(
            check,
            ProvisioningCheck::RecordPresent {
                recorded_shard_count: 4
            }
        );
    }

    /// A future-version record on a dynamic tenant's
    /// first-touch path fails that request with a typed `UnsupportedVersion`
    /// error and increments the mismatch counter, rather than being logged and
    /// letting ingest proceed under an unknown shard_count (ADR-0050 section 5:
    /// the version guard refuses rather than misread a future record format).
    #[tokio::test]
    async fn dynamic_first_touch_unsupported_version_fails_request() {
        let store = store();
        let th = TenantId::new("acme").hash();
        let record = sysproto::ProvisioningRecord {
            format_version: 999,
            tenant_hash: th.0.to_vec(),
            signal: sysproto::Signal::Metrics as i32,
            shard_count: 4,
            created_unix_ns: 1,
            generations: Vec::new(),
            format_floors: Vec::new(),
        };
        store
            .put(
                &provisioning_key(&th, Signal::Metrics),
                record.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("seed future-version record");
        let before = shard_count_mismatch_count();
        let writer = Arc::new(ProvisioningRecordWriter::new(store.clone(), 4));
        let err = writer
            .ensure(&th, Signal::Metrics, 1_000)
            .await
            .expect_err("a future-version record must fail the request, not fail-open");
        assert!(
            matches!(err, ProvisioningError::UnsupportedVersion { .. }),
            "got: {err}"
        );
        // Race-safe monotonic check (see the note in
        // `dynamic_first_touch_mismatch_fails_one_request_only`): the shared
        // global counter only grows, so a strict increase proves it fired here.
        assert!(
            shard_count_mismatch_count() > before,
            "a hard provisioning failure must increment the counter"
        );
    }

    /// An undecodable (corrupt) record on a dynamic
    /// tenant's first-touch path fails that request with a typed `Decode` error
    /// rather than being swallowed and letting ingest proceed under an unknown
    /// shard_count.
    #[tokio::test]
    async fn dynamic_first_touch_corrupt_record_fails_request() {
        let store = store();
        let th = TenantId::new("acme").hash();
        store
            .put(
                &provisioning_key(&th, Signal::Metrics),
                vec![0xFF, 0xFF, 0xFF, 0x07].into(),
                PutOptions::default(),
            )
            .await
            .expect("seed garbage record");
        let writer = Arc::new(ProvisioningRecordWriter::new(store.clone(), 4));
        let err = writer
            .ensure(&th, Signal::Metrics, 1_000)
            .await
            .expect_err("a corrupt record must fail the request, not fail-open");
        assert!(
            matches!(err, ProvisioningError::Decode { .. }),
            "got: {err}"
        );
    }

    /// A record with a corrupt generation history on a dynamic tenant's
    /// first-touch path must fail that request with a typed
    /// `CorruptGenerations` error and increment the mismatch counter, rather
    /// than being logged and letting ingest proceed and the router cache a
    /// tenant `Catalog::enforce_provisioning_once` would reject the same
    /// record for (split-brain between the ingest router's and the catalog's
    /// view of the record). Before the fix, `is_hard_failure` (this file, the
    /// `matches!` in `is_hard_failure` above) omitted
    /// `ProvisioningError::CorruptGenerations`, so `ensure` fell into the
    /// fail-open `Err(err) => ... Ok(())` arm and this `expect_err` panicked.
    #[tokio::test]
    async fn dynamic_first_touch_corrupt_generations_fails_request() {
        let store = store();
        let th = TenantId::new("acme").hash();
        let record = sysproto::ProvisioningRecord {
            format_version: 1,
            tenant_hash: th.0.to_vec(),
            signal: sysproto::Signal::Metrics as i32,
            shard_count: 4,
            created_unix_ns: 1,
            // ScalarMismatch: generations[0].shard_count (8) != scalar shard_count (4).
            generations: vec![sysproto::ShardGeneration {
                generation: 0,
                shard_count: 8,
                activation_hour: 0,
                appended_unix_ns: 0,
            }],
            format_floors: Vec::new(),
        };
        store
            .put(
                &provisioning_key(&th, Signal::Metrics),
                record.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("seed corrupt-generations record");
        let before = shard_count_mismatch_count();
        let writer = Arc::new(ProvisioningRecordWriter::new(store.clone(), 4));
        let err = writer
            .ensure(&th, Signal::Metrics, 1_000)
            .await
            .expect_err("a corrupt generation history must fail the request, not fail-open");
        assert!(
            matches!(err, ProvisioningError::CorruptGenerations { .. }),
            "got: {err}"
        );
        assert!(
            shard_count_mismatch_count() > before,
            "a hard provisioning failure must increment the counter"
        );
    }

    /// A tenant's first write pins the record from config; a second write is a
    /// cheap seen-set hit.
    #[tokio::test]
    async fn first_write_pins_record_from_config() {
        let store = store();
        let th = TenantId::new("acme").hash();
        let writer = Arc::new(ProvisioningRecordWriter::new(store.clone(), 4));
        writer
            .ensure(&th, Signal::Metrics, 1_000)
            .await
            .expect("first write");
        writer
            .ensure(&th, Signal::Metrics, 2_000)
            .await
            .expect("second is a no-op");
        let record = store
            .get(&provisioning_key(&th, Signal::Metrics), GetRange::Full)
            .await
            .expect("record present");
        let decoded = sysproto::ProvisioningRecord::decode(record.data.as_ref()).expect("decode");
        assert_eq!(decoded.shard_count, 4);
    }
}
