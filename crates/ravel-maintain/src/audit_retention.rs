//! Query-audit retention (issue #763, EL-6): a dedicated age-based sweep of the
//! query-audit shard ([`crate::query_audit::QUERY_AUDIT_SHARD`]).
//!
//! Query-audit records are an ever-growing, server-written activity log
//! ([`crate::query_audit`]): one immutable RLOG record per SQL request. Left
//! unmaintained they are unbounded, permanent growth. This module gives them a
//! dedicated retention window (`CompactorConfig::audit_retention_window_ns`,
//! default 90 days), separate from the ADR-0019 per-tenant *data* retention
//! ([`crate::config::RetentionConfig`]): audit is not tenant data and is not
//! excluded from any resolver snapshot, so it needs no tombstone flow -- an
//! expired record is simply deleted in place.
//!
//! # What the sweep mirrors
//!
//! The delete mechanics mirror the sweeper's **superseded-input sweep**
//! ([`crate::sweep::sweep_superseded`], rule 2): a candidate is gated on the
//! durable `created_unix_ns` plus `protection_horizon_ns` before any physical
//! delete, records are deleted before the data objects they name, and every
//! delete consults the [`LeaseCheck`] hook (so a legal hold blocks it). The
//! *expiry* test itself is modelled on age-based retention
//! ([`crate::retention::is_expired`]): a record is expired when its newest
//! event (`max_event_ts_ns`) is strictly older than `now - window`.
//!
//! Both gates are load-bearing, and for distinct reasons:
//!
//! - The **window** is anchored on the record's newest *event* time, so a
//!   record is retired by the age of the activity it logs, not by when its
//!   object happened to be written. For an L0 record these coincide
//!   (`created_unix_ns == max_event_ts_ns`, see `audit_write.rs`); for an L1
//!   compaction record they diverge -- the parts carry the original events'
//!   timestamps while the record's `created_unix_ns` is the (recent) compaction
//!   time -- so the window must read the parts' `max_event_ts_ns`, or a freshly
//!   compacted old record would look brand new and never expire.
//! - The **horizon** is anchored on `created_unix_ns` (when *this* object was
//!   written), exactly as the superseded sweep and `retention.rs`'s physical
//!   sweep anchor on their records' `created_unix_ns` / `retired_at_ns`. It
//!   spares an object for `protection_horizon_ns` after it was written, so a
//!   query that resolved just before the object appeared still has time to read
//!   it. This is what makes the just-compacted L1 case safe: the compaction
//!   record's recent `created_unix_ns` keeps its parts for the horizon even
//!   though the events they hold are already past the window.
//!
//! # Shard 0 (legal hold) is unreachable, forever
//!
//! This sweep is hard-scoped to [`QUERY_AUDIT_SHARD`]: it lists only that
//! shard's commit prefix and deletes only keys under it. The legal-hold shard
//! ([`crate::legal_hold::AUDIT_HOLD_SHARD`] = 0) is a different prefix that is
//! never listed and never deleted -- the deny-delete-forever precedent for hold
//! records (ADR-0055 §3) holds here structurally, not by policy. Only the
//! query-audit shard gains a delete path.

use ravel_commit::keys::{self, BucketEntry, KeyError};
use ravel_commit::record;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_proto::commit::v1::CompactionRecord;
use ravel_types::{Signal, TenantHash};

use prost::Message;

use crate::clock::Clock;
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::query_audit::QUERY_AUDIT_SHARD;
use crate::read::verify_commit_key;
use crate::sweep::LeaseCheck;

/// What one [`sweep_audit_retention`] pass over a tenant's query-audit shard
/// deleted (or, under `dry_run`, would have).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditRetentionOutcome {
    /// Expired L0 commit records and L1 compaction records deleted.
    pub records_deleted: usize,
    /// Expired L0 data objects deleted.
    pub data_deleted: usize,
    /// L1 part objects of expired compaction records deleted.
    pub parts_deleted: usize,
    /// Records left in place: not yet expired, still within the protection
    /// horizon, or lease/legal-hold protected.
    pub kept: usize,
}

/// Sweep expired query-audit records from a tenant's
/// [`QUERY_AUDIT_SHARD`] (issue #763, EL-6).
///
/// A record is deleted only when it is BOTH expired (its newest event is older
/// than `config.audit_retention_window_ns`) AND past the protection horizon
/// (`now >= created_unix_ns + config.protection_horizon_ns`), and only when the
/// [`LeaseCheck`] does not protect it -- so a legal hold covering the
/// query-audit shard blocks every delete, exactly as it does in the other sweep
/// rules. See the module docs for why both gates are load-bearing.
///
/// Hard-scoped to [`QUERY_AUDIT_SHARD`]: the legal-hold shard
/// ([`crate::legal_hold::AUDIT_HOLD_SHARD`]) is never listed and never deleted.
/// Stateless and idempotent: a crashed pass re-run from scratch converges
/// (every delete is a no-op if the object is already gone).
pub async fn sweep_audit_retention(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
) -> Result<AuditRetentionOutcome> {
    let signal = Signal::Audit;
    let shard = QUERY_AUDIT_SHARD;
    let now = clock.now_ns();
    // A record is expired when its newest event is strictly older than this
    // (mirrors `retention::is_expired`, the impossibility floor).
    let expiry_floor = now.saturating_sub(config.audit_retention_window_ns);
    let horizon = config.protection_horizon_ns;

    let prefix = keys::commit_shard_prefix(tenant, signal, shard)?;
    let metas = list_all(store, &prefix).await?;

    let mut outcome = AuditRetentionOutcome::default();
    for meta in metas {
        match keys::partition_bucket_entry(&meta.key) {
            Ok(BucketEntry::CommitRecord(_)) => {
                let commit = record::decode(&store.get(&meta.key, GetRange::Full).await?.data)?;
                // The record's key must reconstruct to the key we listed it at
                // (ADR-0010 §7): a corrupted-but-decodable record's own fields,
                // which reconstruct_data_key trusts, must not name a data object
                // outside the bucket this key implies.
                verify_commit_key(&commit, &meta.key)?;
                if !expired_and_past_horizon(
                    commit.max_event_ts_ns,
                    commit.created_unix_ns,
                    expiry_floor,
                    now,
                    horizon,
                ) {
                    outcome.kept += 1;
                    continue;
                }
                let data_key = keys::reconstruct_data_key(&commit)?;
                // Skip the whole record if either object is held: never leave a
                // commit record pointing at a deleted data object, or vice versa.
                if lease.is_protected(&meta.key) || lease.is_protected(&data_key) {
                    outcome.kept += 1;
                    continue;
                }
                // Record before data (mirrors the superseded sweep): a crash
                // between the two leaves record-less data for orphan GC, never a
                // record pointing at a deleted object.
                if !config.dry_run {
                    store.delete(&meta.key).await?;
                    store.delete(&data_key).await?;
                }
                outcome.records_deleted += 1;
                outcome.data_deleted += 1;
            }
            Ok(BucketEntry::CompactionRecord(_)) => {
                let crec = get_compaction_record(store, &meta.key).await?;
                // The window reads the parts' events, not the record's
                // (recent) compaction time; see the module docs.
                let newest_event = crec.parts.iter().map(|p| p.max_event_ts_ns).max();
                let expired = newest_event.is_some_and(|m| m < expiry_floor);
                if !(expired && now >= crec.created_unix_ns.saturating_add(horizon)) {
                    outcome.kept += 1;
                    continue;
                }
                let mut part_keys = Vec::with_capacity(crec.parts.len());
                for part in &crec.parts {
                    part_keys.push(keys::reconstruct_l1_part_key(&crec, part)?);
                }
                if lease.is_protected(&meta.key) || part_keys.iter().any(|k| lease.is_protected(k))
                {
                    outcome.kept += 1;
                    continue;
                }
                // Record before parts, same ordering discipline as above.
                if !config.dry_run {
                    store.delete(&meta.key).await?;
                    for key in &part_keys {
                        store.delete(key).await?;
                    }
                }
                outcome.records_deleted += 1;
                outcome.parts_deleted += part_keys.len();
            }
            // The query-audit shard is written only by the audit pipeline (L0
            // records) and compacted by this crate (compaction records + L1
            // parts). A selective-erasure rewrite record (ADR-0064) never
            // targets the audit keyspace, and this shard is never
            // tombstone-retired (it has this age sweep instead), so neither
            // shape is expected here; classify both explicitly and leave them
            // alone rather than fail loud on a shape that is safe to skip.
            Ok(BucketEntry::RewriteRecord(_) | BucketEntry::Tombstone(_)) => {
                outcome.kept += 1;
            }
            Err(KeyError::UnknownBucketEntryShape(k)) => {
                return Err(MaintainError::UnknownBucketEntry(k));
            }
            Err(e) => return Err(MaintainError::Key(e)),
        }
    }
    Ok(outcome)
}

/// A record is a delete candidate when it is expired (its newest event is
/// strictly older than `expiry_floor`) AND past the protection horizon
/// (`now >= created_unix_ns + horizon`). Both gates, both anchored on durable
/// record fields; see the module docs for why each is needed.
fn expired_and_past_horizon(
    max_event_ts_ns: i64,
    created_unix_ns: i64,
    expiry_floor: i64,
    now: i64,
    horizon: i64,
) -> bool {
    let expired = max_event_ts_ns < expiry_floor;
    let past_horizon = now >= created_unix_ns.saturating_add(horizon);
    expired && past_horizon
}

/// GET, decode, and key-verify a compaction record (ADR-0010 §7), matching the
/// discipline `retention.rs` and `sweep.rs` apply to every record they read.
async fn get_compaction_record(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<CompactionRecord> {
    let got = store.get(key, GetRange::Full).await?;
    let record = CompactionRecord::decode(got.data.as_ref())
        .map_err(|e| MaintainError::Invariant(format!("compaction record decode failed: {e}")))?;
    keys::verify_compaction_record_key(&record, key)?;
    Ok(record)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    use ravel_logseg::{LogRecord, Predicate, RlogConfig, RlogReader};
    use ravel_object_store::memory::MemoryStore;
    use uuid::Uuid;

    use crate::bucket::Bucket;
    use crate::clock::FixedClock;
    use crate::compact::{CompactionOutcome, compact_bucket};
    use crate::legal_hold::{AUDIT_HOLD_SHARD, LegalHoldCheck, shard_hold_scopes, write_hold_set};
    use crate::query_audit::{QueryStatus, write_query_audit};
    use crate::sweep::NoLeases;

    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const NS_PER_DAY: i64 = 24 * NS_PER_HOUR;

    fn tenant() -> TenantHash {
        TenantHash([3u8; 16])
    }

    /// Write one query-audit L0 record for `tenant` at `ts_ns` (event time ==
    /// created time, exactly as `write_audit_batch` stamps it), on
    /// [`QUERY_AUDIT_SHARD`].
    async fn seed_audit(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        ts_ns: i64,
        text: &str,
    ) {
        write_query_audit(
            store,
            tenant,
            ts_ns,
            text,
            "sql",
            QueryStatus::Ok,
            ts_ns,
            ts_ns,
        )
        .await
        .expect("write query-audit record");
    }

    /// Number of L0 data objects on the query-audit shard.
    async fn l0_data_count(store: &dyn ObjectStoreBackend, tenant: &TenantHash) -> usize {
        let prefix = format!("t/{}/u/l0/{:04}/", tenant.to_hex(), QUERY_AUDIT_SHARD);
        list_all(store, &prefix).await.unwrap().len()
    }

    /// Number of commit records (L0 + compaction) on the query-audit shard.
    async fn commit_count(store: &dyn ObjectStoreBackend, tenant: &TenantHash) -> usize {
        let prefix = keys::commit_shard_prefix(tenant, Signal::Audit, QUERY_AUDIT_SHARD).unwrap();
        list_all(store, &prefix).await.unwrap().len()
    }

    /// Deliverable 1: Signal::Audit / QUERY_AUDIT_SHARD records compact through
    /// the existing RLOG machinery, parameterized only by the new signal/shard.
    /// Two L0 audit records in one sealed bucket merge into one L1 part carrying
    /// both records (conservation), proving the codec seam handles Audit.
    #[tokio::test]
    async fn query_audit_records_compact_through_the_existing_machinery() {
        let store = MemoryStore::new();
        let tenant = tenant();
        // Two records in the same ingest hour, so they share one bucket.
        let hour: u32 = 4_000;
        let base = i64::from(hour) * NS_PER_HOUR;
        seed_audit(&store, &tenant, base + 10, "SELECT 1").await;
        seed_audit(&store, &tenant, base + 20, "SELECT 2").await;

        // Two L0 inputs before compaction.
        assert_eq!(l0_data_count(&store, &tenant).await, 2);

        // A clock well past the seal margin for `hour`.
        let now = base + 5 * NS_PER_HOUR;
        let clock = FixedClock::new(now);
        let bucket = Bucket::new(tenant, Signal::Audit, QUERY_AUDIT_SHARD, hour);
        let outcome = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket)
            .await
            .expect("audit compaction");
        assert!(
            matches!(outcome, CompactionOutcome::Compacted { .. }),
            "the two audit records must compact, got {outcome:?}"
        );

        // The one L1 part carries both records: conservation through the merge.
        let (record, parts) = read_output(&store, &tenant, hour).await;
        assert_eq!(record.level, 1);
        assert_eq!(parts.len(), 1, "a two-record corpus fits one part");
        let rows = decode_rlog(&parts[0]);
        assert_eq!(rows.len(), 2, "both audit records survive compaction");
    }

    /// Compaction is refused on the legal-hold shard (shard 0): the fold reads
    /// L0 records only, so compacting them would silently drop holds. This pins
    /// the guard in `compact_bucket`.
    #[tokio::test]
    async fn compaction_refused_on_legal_hold_shard() {
        let store = MemoryStore::new();
        let tenant = tenant();
        let clock = FixedClock::new(1_000 * NS_PER_HOUR);
        let bucket = Bucket::new(tenant, Signal::Audit, AUDIT_HOLD_SHARD, 5);
        // An empty bucket is `BelowMinInputs` before the codec dispatch, so seed
        // two hold records to reach the signal/shard dispatch and hit the guard.
        write_hold_set(
            &store,
            &tenant,
            Uuid::from_u128(1),
            5 * NS_PER_HOUR,
            "t/x/",
            "r",
        )
        .await
        .expect("hold 1");
        write_hold_set(
            &store,
            &tenant,
            Uuid::from_u128(2),
            5 * NS_PER_HOUR + 1,
            "t/y/",
            "r",
        )
        .await
        .expect("hold 2");
        let err = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket)
            .await
            .expect_err("legal-hold shard must never compact");
        assert!(matches!(err, MaintainError::Invariant(_)), "got {err:?}");
    }

    /// Deliverable 2: an expired query-audit record (past the 90-day window and
    /// past the horizon) is swept; one still inside the window is not.
    #[tokio::test]
    async fn expired_record_swept_recent_kept() {
        let store = MemoryStore::new();
        let tenant = tenant();
        let now = 400 * NS_PER_DAY;
        let clock = FixedClock::new(now);
        let config = CompactorConfig::default(); // 90-day window

        // Expired: 100 days old (past the 90-day window; created == event, so
        // also far past the 25h horizon).
        seed_audit(&store, &tenant, now - 100 * NS_PER_DAY, "old query").await;
        // Recent: 10 days old, well inside the window.
        seed_audit(&store, &tenant, now - 10 * NS_PER_DAY, "recent query").await;

        assert_eq!(l0_data_count(&store, &tenant).await, 2);

        let outcome = sweep_audit_retention(&store, &clock, &config, &NoLeases, &tenant)
            .await
            .expect("sweep");
        assert_eq!(outcome.records_deleted, 1, "only the expired record");
        assert_eq!(outcome.data_deleted, 1);
        assert_eq!(outcome.kept, 1, "the in-window record is kept");

        assert_eq!(
            l0_data_count(&store, &tenant).await,
            1,
            "the expired record's data object is gone; the recent one remains"
        );
        assert_eq!(commit_count(&store, &tenant).await, 1);
    }

    /// The horizon gate is real: a record whose events are past the window but
    /// whose object was written recently (a compaction record's recent
    /// `created_unix_ns`) is spared until the horizon elapses. Exercised on the
    /// L1 path, where `created_unix_ns` (compaction time) and the parts'
    /// `max_event_ts_ns` (original events) diverge.
    #[tokio::test]
    async fn just_compacted_old_records_spared_by_horizon() {
        let store = MemoryStore::new();
        let tenant = tenant();
        // Two audit records whose events are ~100 days before the compaction,
        // in one hour bucket.
        let hour: u32 = 4_000;
        let base = i64::from(hour) * NS_PER_HOUR;
        seed_audit(&store, &tenant, base + 10, "old a").await;
        seed_audit(&store, &tenant, base + 20, "old b").await;

        // Compact "now" == far after the events (so the compaction record's
        // created_unix_ns is this recent time).
        let compact_now = base + 100 * NS_PER_DAY;
        let clock = FixedClock::new(compact_now);
        let config = CompactorConfig::default();
        let bucket = Bucket::new(tenant, Signal::Audit, QUERY_AUDIT_SHARD, hour);
        compact_bucket(&store, &clock, &config, &bucket)
            .await
            .expect("compact");

        // Sweep at a time only just past the compaction: the events are far past
        // the 90-day window, but the compaction record is younger than the 25h
        // horizon, so its L1 parts must be spared.
        let sweep_now = compact_now + NS_PER_HOUR; // < protection horizon (25h)
        let sweep_clock = FixedClock::new(sweep_now);
        let outcome = sweep_audit_retention(&store, &sweep_clock, &config, &NoLeases, &tenant)
            .await
            .expect("sweep");
        assert_eq!(
            outcome.parts_deleted, 0,
            "a just-compacted record's parts are spared by the horizon despite old events"
        );

        // Well past the horizon: now the compaction record and its parts are
        // both expired and past the horizon, so they are swept.
        let later = compact_now + 2 * NS_PER_DAY;
        let later_clock = FixedClock::new(later);
        let outcome = sweep_audit_retention(&store, &later_clock, &config, &NoLeases, &tenant)
            .await
            .expect("sweep");
        assert!(
            outcome.records_deleted >= 1 && outcome.parts_deleted >= 1,
            "past the horizon the compaction record and its parts are swept, got {outcome:?}"
        );
    }

    /// Deliverable 3: the legal-hold shard (shard 0) is excluded from the delete
    /// path forever. An old hold record on shard 0 survives a sweep that would
    /// have deleted it by age if the sweep ever reached shard 0.
    #[tokio::test]
    async fn legal_hold_shard_zero_survives_the_sweep() {
        let store = MemoryStore::new();
        let tenant = tenant();
        let now = 400 * NS_PER_DAY;

        // A hold record on shard 0, timestamped 200 days ago -- old enough that
        // a shard-agnostic age sweep would delete it.
        write_hold_set(
            &store,
            &tenant,
            Uuid::from_u128(9),
            now - 200 * NS_PER_DAY,
            "t/held/",
            "hold",
        )
        .await
        .expect("set hold");

        let hold_prefix =
            keys::commit_shard_prefix(&tenant, Signal::Audit, AUDIT_HOLD_SHARD).unwrap();
        let before = list_all(&store, &hold_prefix).await.unwrap().len();
        assert_eq!(before, 1, "one hold commit record on shard 0");

        let clock = FixedClock::new(now);
        let outcome = sweep_audit_retention(
            &store,
            &clock,
            &CompactorConfig::default(),
            &NoLeases,
            &tenant,
        )
        .await
        .expect("sweep");
        assert_eq!(
            outcome,
            AuditRetentionOutcome::default(),
            "the sweep touches nothing: shard 0 is not even listed"
        );
        assert_eq!(
            list_all(&store, &hold_prefix).await.unwrap().len(),
            before,
            "the legal-hold shard-0 record must survive the query-audit sweep"
        );
    }

    /// Deliverable: LegalHoldCheck genuinely gates the new sweep. A held
    /// query-audit record past its retention window is NOT deleted; the same
    /// record with no hold IS deleted (the control), so the assertion is not
    /// vacuous.
    #[tokio::test]
    async fn legal_hold_gates_the_audit_retention_sweep() {
        let tenant = tenant();
        let now = 400 * NS_PER_DAY;
        let clock = FixedClock::new(now);
        let config = CompactorConfig::default();
        let old_ts = now - 200 * NS_PER_DAY;

        // Control: with no hold, an old record is swept.
        {
            let store = MemoryStore::new();
            seed_audit(&store, &tenant, old_ts, "held query").await;
            let outcome = sweep_audit_retention(&store, &clock, &config, &NoLeases, &tenant)
                .await
                .expect("control sweep");
            assert_eq!(outcome.records_deleted, 1, "control: no hold, record swept");
            assert_eq!(l0_data_count(&store, &tenant).await, 0);
        }

        // With a legal hold over the query-audit shard, the same old record
        // survives. The hold is set on all three of the shard's key prefixes
        // (shard_hold_scopes), refreshed into a LegalHoldCheck snapshot, and
        // passed as the sweep's LeaseCheck.
        {
            let store = MemoryStore::new();
            seed_audit(&store, &tenant, old_ts, "held query").await;
            let scopes = shard_hold_scopes(&tenant, Signal::Audit, QUERY_AUDIT_SHARD).unwrap();
            for (i, scope) in scopes.iter().enumerate() {
                write_hold_set(
                    &store,
                    &tenant,
                    Uuid::from_u128(100 + i as u128),
                    old_ts,
                    scope,
                    "audit hold",
                )
                .await
                .expect("set hold on scope");
            }
            let hold = LegalHoldCheck::refresh(&store, &tenant)
                .await
                .expect("refresh");
            assert!(!hold.is_empty(), "the hold must be active");

            let outcome = sweep_audit_retention(&store, &clock, &config, &hold, &tenant)
                .await
                .expect("held sweep");
            assert_eq!(
                outcome.records_deleted, 0,
                "a held query-audit record past its window must not be deleted"
            );
            assert_eq!(
                outcome.kept, 1,
                "the held record is counted kept, not deleted"
            );
            assert_eq!(
                l0_data_count(&store, &tenant).await,
                1,
                "the held record's data object must survive"
            );
        }
    }

    // --- helpers ------------------------------------------------------------

    /// The single compaction record in the query-audit bucket plus every L1
    /// part it names.
    async fn read_output(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        hour: u32,
    ) -> (CompactionRecord, Vec<bytes::Bytes>) {
        let prefix =
            keys::commit_shard_hour_prefix(tenant, Signal::Audit, QUERY_AUDIT_SHARD, hour).unwrap();
        let metas = list_all(store, &prefix).await.unwrap();
        let mut rec_keys: Vec<String> = metas
            .into_iter()
            .map(|m| m.key)
            .filter(|k| {
                matches!(
                    keys::partition_bucket_entry(k),
                    Ok(BucketEntry::CompactionRecord(_))
                )
            })
            .collect();
        rec_keys.sort();
        assert_eq!(rec_keys.len(), 1, "exactly one compaction record");
        let got = store.get(&rec_keys[0], GetRange::Full).await.unwrap();
        let record = CompactionRecord::decode(got.data.as_ref()).unwrap();
        let mut parts = Vec::new();
        for p in &record.parts {
            let key = keys::reconstruct_l1_part_key(&record, p).unwrap();
            parts.push(store.get(&key, GetRange::Full).await.unwrap().data);
        }
        (record, parts)
    }

    /// Decode every record of an RLOG object (no predicate).
    fn decode_rlog(bytes: &[u8]) -> Vec<LogRecord> {
        let reader = RlogReader::new(bytes, &RlogConfig::default()).expect("open rlog");
        let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        rows
    }
}
