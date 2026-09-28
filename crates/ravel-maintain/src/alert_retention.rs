//! Alert-signal retention (ADR-1688): a tombstone-free age sweep of the
//! `Signal::Alerts` transition history that keeps every identity's
//! current-state record.
//!
//! The alert evaluator writes one RLOG object plus one L0 commit record per
//! alert transition and nothing ever removes them, so the history grows for
//! the life of the deployment. It is also the evaluator's own input: a
//! cold-start fold reads the whole prefix to recover each identity's current
//! state. This sweep gives that prefix a bound.
//!
//! # What the sweep mirrors, and what it adds
//!
//! The mechanics are [`crate::audit_retention::sweep_audit_retention`]'s: list
//! one shard's commit prefix, skip a record whose key-derived
//! `ingest_hour_bucket` already proves it cannot be expired, apply the same
//! [`expired_and_past_horizon`] test to the record's `max_event_ts_ns` and
//! `created_unix_ns`, consult the caller's [`LeaseCheck`] before any physical
//! delete, and delete the commit record before the data object it names. No
//! tombstone is written at any point.
//!
//! Tombstone-freedom is load-bearing rather than incidental here (ADR-1688
//! decision 1). The evaluator's fold refuses any bucket entry that is not a
//! commit or compaction record, so a single ADR-0019 retention tombstone under
//! the alerts commit prefix would stop every evaluation tick for that tenant.
//! A tombstone would also have no reader: alert records are not folded into the
//! catalog, so nothing resolves a bucket-wide exclusion for them.
//!
//! What this sweep adds over the audit one is the **keep set** (ADR-1688
//! decision 2): a set of `(epoch, seq)` pairs naming, per alert identity, the
//! record that identity's current state comes from. An expired record whose key
//! parses to a pair in the keep set is kept and counted separately. That is what
//! makes the deletion safe for the fold: after a sweep the surviving prefix is
//! every transition inside the window plus one current-state record per
//! identity, so a cold-start fold over the survivors yields the same
//! latest-record-per-identity map it would have yielded over the unswept
//! history. Without it, an alert that has been firing for longer than the
//! window would read as inactive after a restart and re-fire with a fresh
//! `generation`, which is exactly the outcome ADR-1294's retention contract
//! ("prune only into a compact form, never by deletion") forbids.
//!
//! # The keep set is checked after the expiry test, not before
//!
//! A keep-set record is spared either way, so checking the set first would save
//! a GET per identity per sweep. The order here is deliberate: it makes
//! [`AlertRetentionOutcome::kept_current_state`] mean exactly "expired records
//! that only the keep set saved", which is the figure that says how much the
//! keep set is actually holding back. Checked first, the counter would also
//! absorb every keep-set record that was never at risk, and an operator reading
//! it could not tell the two apart. The cost is one GET per identity per sweep,
//! bounded by the identity count, not by the history length.
//!
//! # Compaction never reaches this shard
//!
//! `Signal::Alerts` is not in the server's `MAINTAINED_SIGNALS` and
//! [`crate::compact::compact_bucket`] rejects the signal outright, so no alert
//! record is ever compacted (ADR-1688 decision 1). A compaction record found
//! under the alerts prefix is therefore a layout invariant that has already
//! been broken somewhere else; this sweep logs it and keeps it, rather than
//! deleting L0 records whose only surviving copy might be inside it.

use std::collections::BTreeSet;

use ravel_commit::keys::{self, BucketEntry, KeyError};
use ravel_commit::record;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_types::{Signal, TenantHash};

use crate::audit_retention::{expired_and_past_horizon, hour_certainly_not_expired};
use crate::clock::Clock;
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::read::verify_commit_key;
use crate::sweep::LeaseCheck;

/// The `(epoch, seq)` pairs naming each alert identity's current-state record
/// (ADR-1688 decision 2). A commit record whose key parses to a pair in this
/// set is never deleted, however old it is.
///
/// The pair is the identity the evaluator's fold breaks `ts_ns` ties on, and it
/// is carried in the commit key, so the sweep can test membership without
/// reading the record's body. It deliberately does not carry `writer_id`: the
/// evaluator stamps every record with a constant `writer_epoch` and a `seq`
/// that restarts at 1 in each evaluator process, so one pair can name records
/// written by two different evaluator runs. Every such collision keeps a record
/// that would otherwise have been deleted, never deletes one that should have
/// been kept, which is the direction ADR-1688 decision 3 requires of any
/// imprecision in the keep set.
pub type AlertKeepSet = BTreeSet<(u64, u64)>;

/// What one [`sweep_alert_retention`] pass over a tenant's alert shard deleted
/// (or, under `dry_run`, would have).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AlertRetentionOutcome {
    /// Expired alert commit records deleted.
    pub records_deleted: usize,
    /// Data objects of expired alert commit records deleted.
    pub data_deleted: usize,
    /// Records left in place: not yet expired, still within the protection
    /// horizon, named by the keep set, or lease/legal-hold protected.
    pub kept: usize,
    /// Records that were expired and past the horizon and were kept only
    /// because the keep set names them as an identity's current state. A subset
    /// of `kept`.
    pub kept_current_state: usize,
    /// Commit records whose GET and decode was skipped because the key's
    /// `ingest_hour_bucket` alone proved the record cannot be expired. A subset
    /// of `kept`.
    pub gets_skipped_by_hour_prefilter: usize,
}

/// Sweep expired alert transition records from a tenant's alert shard,
/// keeping every identity's current-state record (ADR-1688 decisions 1 and 2).
///
/// A record is deleted only when all of these hold: it is expired (its newest
/// event is older than `config.alert_retention_window_ns`), it is past the
/// protection horizon (`now >= created_unix_ns + config.protection_horizon_ns`),
/// its `(epoch, seq)` is not in `keep`, and the [`LeaseCheck`] does not protect
/// it or its data object. The commit record is deleted before its data object,
/// so a crash between the two leaves record-less data for orphan GC rather than
/// a record pointing at a deleted object. No tombstone is ever written.
///
/// `shard` is the alert shard to sweep. It is a parameter rather than a
/// constant because the evaluator's `ALERT_SHARD` is defined in
/// `services/ravel-server` (`alerting.rs`), which this crate does not depend
/// on; the driver passes that constant through.
///
/// A non-positive `config.alert_retention_window_ns` disables the sweep
/// entirely (ADR-1688 decision 5): it returns the empty outcome without even
/// listing the prefix. Zero is the documented opt-out. A negative window is
/// refused for the same reason rather than honoured, because arithmetically it
/// would put the expiry floor in the future and make every record on the shard
/// a delete candidate at once.
///
/// Stateless and idempotent: a crashed pass re-run from scratch converges,
/// since every delete is a no-op if the object is already gone.
pub async fn sweep_alert_retention(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    tenant: &TenantHash,
    shard: u32,
    keep: &AlertKeepSet,
) -> Result<AlertRetentionOutcome> {
    if config.alert_retention_window_ns <= 0 {
        return Ok(AlertRetentionOutcome::default());
    }

    let now = clock.now_ns();
    // A record is expired when its newest event is strictly older than this,
    // the same floor `retention::is_expired` and the audit sweep apply.
    let expiry_floor = now.saturating_sub(config.alert_retention_window_ns);
    let horizon = config.protection_horizon_ns;

    let prefix = keys::commit_shard_prefix(tenant, Signal::Alerts, shard)?;
    let metas = list_all(store, &prefix).await?;

    let mut outcome = AlertRetentionOutcome::default();
    for meta in metas {
        match keys::partition_bucket_entry(&meta.key) {
            Ok(BucketEntry::CommitRecord(parsed)) => {
                // The evaluator mints `ingest_hour_bucket` as
                // `floor(max_event_ts_ns / NS_PER_HOUR)` from the same stamp it
                // writes into `max_event_ts_ns`, so the key alone lower-bounds
                // the record's newest event with no GET. See
                // `audit_retention::hour_certainly_not_expired`, whose soundness
                // argument holds here for the same reason.
                if hour_certainly_not_expired(parsed.ingest_hour_bucket, expiry_floor) {
                    outcome.kept += 1;
                    outcome.gets_skipped_by_hour_prefilter += 1;
                    continue;
                }
                let commit = record::decode(&store.get(&meta.key, GetRange::Full).await?.data)?;
                // The record's own identity fields must reconstruct the key we
                // listed it at (ADR-0010 section 7), or a corrupted-but-decodable
                // record could name a data object outside this bucket.
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
                // Expired and past the horizon, but the memo names it as some
                // identity's current state: keep it forever (decision 2).
                if keep.contains(&(parsed.epoch, parsed.seq)) {
                    outcome.kept += 1;
                    outcome.kept_current_state += 1;
                    continue;
                }
                let data_key = keys::reconstruct_data_key(&commit)?;
                // Skip the whole record if either object is held: never leave a
                // commit record pointing at a deleted data object, or vice versa.
                if lease.is_protected(&meta.key) || lease.is_protected(&data_key) {
                    outcome.kept += 1;
                    continue;
                }
                if !config.dry_run {
                    store.delete(&meta.key).await?;
                    store.delete(&data_key).await?;
                }
                outcome.records_deleted += 1;
                outcome.data_deleted += 1;
            }
            // No alert record is ever compacted (decision 1), and this sweep
            // writes no tombstone, so neither shape belongs here. A rewrite
            // record does not either: selective erasure never targets the alert
            // keyspace. Keep all three: a compaction record may be the only
            // surviving copy of L0 records the fold still needs, and deleting a
            // shape this sweep does not understand is not a recoverable
            // mistake.
            Ok(BucketEntry::CompactionRecord(_)) => {
                tracing::warn!(
                    key = %meta.key,
                    "compaction record under the alerts commit prefix: alert records are never \
                     compacted (ADR-1688 decision 1); keeping it"
                );
                outcome.kept += 1;
            }
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use ravel_commit::record::NewCommitRecord;
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::instrument::StoreOp;
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{InstrumentedStore, PutOptions};
    use uuid::Uuid;

    use crate::bucket::Bucket;
    use crate::clock::FixedClock;
    use crate::compact::compact_bucket;
    use crate::config::NS_PER_HOUR;
    use crate::sweep::NoLeases;

    const NS_PER_DAY: i64 = 24 * NS_PER_HOUR;

    /// The evaluator's own shard for `Signal::Alerts`
    /// (`services/ravel-server/src/alerting.rs`, `ALERT_SHARD`). Mirrored here
    /// as a test constant because this crate cannot depend on `ravel-server`;
    /// production callers pass the real constant through.
    const ALERT_SHARD: u32 = 0;

    /// The evaluator's constant `ALERT_WRITER_EPOCH`.
    const ALERT_WRITER_EPOCH: u64 = 1;

    fn tenant() -> TenantHash {
        TenantHash([7u8; 16])
    }

    /// The keys one seeded alert transition occupies.
    #[derive(Debug, Clone)]
    struct Seeded {
        commit_key: String,
        data_key: String,
        epoch: u64,
        seq: u64,
    }

    /// Write one alert transition exactly as the evaluator's `publish` does:
    /// a data object plus an L0 commit record whose `min`/`max` event and
    /// ingest stamps, `created_unix_ns` and `ingest_hour_bucket` all come from
    /// the single transition stamp `ts_ns`.
    ///
    /// The object body is opaque here: the sweep never decodes it, it only
    /// deletes it at the key the commit record names.
    async fn seed_alert(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        writer_id: Uuid,
        seq: u64,
        ts_ns: i64,
    ) -> Seeded {
        let body = Bytes::from(format!("alert transition seq={seq} ts={ts_ns}"));
        let content_hash: [u8; 32] = *blake3::hash(&body).as_bytes();
        let ingest_hour_bucket = u32::try_from(ts_ns.div_euclid(NS_PER_HOUR)).expect("hour bucket");
        let commit = record::build(NewCommitRecord {
            tenant_hash: *tenant,
            signal: Signal::Alerts,
            shard: ALERT_SHARD,
            writer_id,
            writer_epoch: ALERT_WRITER_EPOCH,
            writer_seq: seq,
            object_size: body.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: ts_ns,
            max_event_ts_ns: ts_ns,
            min_ingest_ts_ns: ts_ns,
            max_ingest_ts_ns: ts_ns,
            segment_format_version: 1,
            created_unix_ns: ts_ns,
            ingest_hour_bucket,
        })
        .expect("build alert commit record");

        let data_key = keys::reconstruct_data_key(&commit).expect("data key");
        store
            .put(&data_key, body, PutOptions::create_if_absent())
            .await
            .expect("put alert data object");
        let commit_key = keys::commit_key_for_record(&commit).expect("commit key");
        store
            .put(
                &commit_key,
                record::encode(&commit),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put alert commit record");

        Seeded {
            commit_key,
            data_key,
            epoch: ALERT_WRITER_EPOCH,
            seq,
        }
    }

    /// Whether `key` is present. A missing object is `NotFound`; any other
    /// store error is a test-harness failure and must not read as absence.
    async fn exists(store: &dyn ObjectStoreBackend, key: &str) -> bool {
        match store.head(key).await {
            Ok(_) => true,
            Err(ravel_object_store::StoreError::NotFound) => false,
            Err(e) => panic!("head({key}) failed: {e}"),
        }
    }

    /// Every key under the tenant's alert commit prefix.
    async fn commit_keys(store: &dyn ObjectStoreBackend, tenant: &TenantHash) -> Vec<String> {
        let prefix = keys::commit_shard_prefix(tenant, Signal::Alerts, ALERT_SHARD).unwrap();
        let mut keys: Vec<String> = list_all(store, &prefix)
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.key)
            .collect();
        keys.sort();
        keys
    }

    /// One writer, one seq counter, one transition per hour: what the
    /// evaluator produces over time.
    ///
    /// `hours_ago` is measured from `now` in whole hours; `seq` is the
    /// evaluator's monotonic per-process counter.
    async fn seed_history(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        writer: Uuid,
        now: i64,
        spec: &[(i64, u64)],
    ) -> Vec<Seeded> {
        let mut out = Vec::new();
        for (hours_ago, seq) in spec {
            let ts = now - hours_ago * NS_PER_HOUR;
            out.push(seed_alert(store, tenant, writer, *seq, ts).await);
        }
        out
    }

    /// The acceptance test (ADR-1688 follow-up task 1). An alert history
    /// spanning several expired hours plus one live hour is swept: every
    /// expired record and its data object are gone by exact key, except the one
    /// the keep set names as an identity's current state, which survives with
    /// its data object; the live-hour records are untouched; and every report
    /// count is exact.
    #[tokio::test]
    async fn alert_retention_sweeps_expired_history_and_keeps_current_state() {
        let store = MemoryStore::new();
        let tenant = tenant();
        let now = 400 * NS_PER_DAY;
        let clock = FixedClock::new(now);
        let config = CompactorConfig::default(); // 90-day alert window
        let writer = Uuid::from_u128(0xA1);

        // Four expired transitions, in four distinct hours 100 to 97 days back
        // (past the 90-day window and far past the 25h horizon), then two live
        // ones an hour apart inside the window.
        let expired = seed_history(
            &store,
            &tenant,
            writer,
            now,
            &[
                (100 * 24, 1),
                (99 * 24, 2),
                (98 * 24, 3),
                (97 * 24, 4), // the current-state record of a long-lived identity
            ],
        )
        .await;
        let live = seed_history(&store, &tenant, writer, now, &[(5, 5), (4, 6)]).await;

        assert_eq!(commit_keys(&store, &tenant).await.len(), 6);

        // The memo names seq 4 as some identity's current state: an alert that
        // last transitioned 97 days ago and is still in that state.
        let kept_record = expired[3].clone();
        let keep: AlertKeepSet = [(kept_record.epoch, kept_record.seq)].into_iter().collect();

        let outcome = sweep_alert_retention(
            &store,
            &clock,
            &config,
            &NoLeases,
            &tenant,
            ALERT_SHARD,
            &keep,
        )
        .await
        .expect("sweep");

        assert_eq!(
            outcome,
            AlertRetentionOutcome {
                records_deleted: 3,
                data_deleted: 3,
                // 2 live (cleared by the hour prefilter) + 1 keep-set record.
                kept: 3,
                kept_current_state: 1,
                gets_skipped_by_hour_prefilter: 2,
            },
            "exact counts, not just nonzero"
        );

        for gone in &expired[..3] {
            assert!(
                !exists(&store, &gone.commit_key).await,
                "expired commit record {} must be deleted",
                gone.commit_key
            );
            assert!(
                !exists(&store, &gone.data_key).await,
                "expired data object {} must be deleted",
                gone.data_key
            );
        }
        assert!(
            exists(&store, &kept_record.commit_key).await,
            "the keep-set commit record {} must survive its expiry",
            kept_record.commit_key
        );
        assert!(
            exists(&store, &kept_record.data_key).await,
            "the keep-set data object {} must survive its expiry",
            kept_record.data_key
        );
        for alive in &live {
            assert!(exists(&store, &alive.commit_key).await);
            assert!(exists(&store, &alive.data_key).await);
        }

        // And by exact listing, so a stray key added anywhere under the prefix
        // would fail here too.
        let mut expected: Vec<String> = live
            .iter()
            .chain(std::iter::once(&kept_record))
            .map(|s| s.commit_key.clone())
            .collect();
        expected.sort();
        assert_eq!(commit_keys(&store, &tenant).await, expected);
    }

    /// `alert_retention_window_ns == 0` is the documented opt-out: the sweep
    /// deletes nothing on the same store the acceptance test empties, and does
    /// not even list the prefix.
    #[tokio::test]
    async fn zero_window_disables_the_sweep_entirely() {
        let store = InstrumentedStore::new(MemoryStore::new());
        let tenant = tenant();
        let now = 400 * NS_PER_DAY;
        let clock = FixedClock::new(now);
        let writer = Uuid::from_u128(0xA2);
        let config = CompactorConfig {
            alert_retention_window_ns: 0,
            ..CompactorConfig::default()
        };

        let seeded = seed_history(
            &store,
            &tenant,
            writer,
            now,
            &[(100 * 24, 1), (99 * 24, 2), (5, 3)],
        )
        .await;
        let before = commit_keys(&store, &tenant).await;

        let lists_before = store.metrics().snapshot().op(StoreOp::List).calls;
        let deletes_before = store.metrics().snapshot().op(StoreOp::Delete).calls;
        let outcome = sweep_alert_retention(
            &store,
            &clock,
            &config,
            &NoLeases,
            &tenant,
            ALERT_SHARD,
            &AlertKeepSet::new(),
        )
        .await
        .expect("sweep");

        assert_eq!(
            outcome,
            AlertRetentionOutcome::default(),
            "a zero window must do nothing at all"
        );
        assert_eq!(
            store.metrics().snapshot().op(StoreOp::List).calls - lists_before,
            0,
            "a disabled sweep must not even list the prefix"
        );
        assert_eq!(
            store.metrics().snapshot().op(StoreOp::Delete).calls - deletes_before,
            0
        );
        assert_eq!(commit_keys(&store, &tenant).await, before);
        for s in &seeded {
            assert!(exists(&store, &s.data_key).await);
        }
    }

    /// The protection horizon is a second, independent gate: a record whose
    /// event time is long past the window but whose object was written moments
    /// ago survives until the horizon elapses. Seeded with a `created_unix_ns`
    /// that diverges from `max_event_ts_ns`, which the evaluator itself never
    /// produces but a backfill or a clock correction can.
    #[tokio::test]
    async fn record_inside_the_protection_horizon_survives_its_expired_event_time() {
        let store = MemoryStore::new();
        let tenant = tenant();
        let now = 400 * NS_PER_DAY;
        let clock = FixedClock::new(now);
        let config = CompactorConfig::default();
        assert!(config.protection_horizon_ns > 0, "the gate must be armed");

        let event_ts = now - 200 * NS_PER_DAY; // far past the 90-day window
        let created = now - config.protection_horizon_ns / 2; // inside the horizon
        let body = Bytes::from("recently written, long-expired event");
        let content_hash: [u8; 32] = *blake3::hash(&body).as_bytes();
        let commit = record::build(NewCommitRecord {
            tenant_hash: tenant,
            signal: Signal::Alerts,
            shard: ALERT_SHARD,
            writer_id: Uuid::from_u128(0xA3),
            writer_epoch: ALERT_WRITER_EPOCH,
            writer_seq: 1,
            object_size: body.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: event_ts,
            max_event_ts_ns: event_ts,
            min_ingest_ts_ns: event_ts,
            max_ingest_ts_ns: event_ts,
            segment_format_version: 1,
            created_unix_ns: created,
            ingest_hour_bucket: u32::try_from(event_ts.div_euclid(NS_PER_HOUR)).unwrap(),
        })
        .expect("build");
        let data_key = keys::reconstruct_data_key(&commit).unwrap();
        store
            .put(&data_key, body, PutOptions::create_if_absent())
            .await
            .unwrap();
        let commit_key = keys::commit_key_for_record(&commit).unwrap();
        store
            .put(
                &commit_key,
                record::encode(&commit),
                PutOptions::create_if_absent(),
            )
            .await
            .unwrap();

        let outcome = sweep_alert_retention(
            &store,
            &clock,
            &config,
            &NoLeases,
            &tenant,
            ALERT_SHARD,
            &AlertKeepSet::new(),
        )
        .await
        .expect("sweep");
        assert_eq!(
            outcome,
            AlertRetentionOutcome {
                records_deleted: 0,
                data_deleted: 0,
                kept: 1,
                kept_current_state: 0,
                // The hour bucket is the expired event's, so the prefilter
                // cannot clear it: the horizon is what saves the record, and it
                // is read from the record body.
                gets_skipped_by_hour_prefilter: 0,
            },
            "the horizon gate alone must keep this record"
        );
        assert!(exists(&store, &commit_key).await);
        assert!(exists(&store, &data_key).await);

        // Control: past the horizon the same record is swept, so the assertion
        // above is about the horizon and not about some other gate.
        let later = FixedClock::new(created + config.protection_horizon_ns);
        let outcome = sweep_alert_retention(
            &store,
            &later,
            &config,
            &NoLeases,
            &tenant,
            ALERT_SHARD,
            &AlertKeepSet::new(),
        )
        .await
        .expect("sweep");
        assert_eq!(outcome.records_deleted, 1);
        assert_eq!(outcome.data_deleted, 1);
        assert!(!exists(&store, &commit_key).await);
        assert!(!exists(&store, &data_key).await);
    }

    /// Delete ordering under failure: the commit record goes first, so a failure
    /// on the data-object delete leaves the record already gone and record-less
    /// data for orphan GC, never a commit record naming a deleted object. The
    /// error surfaces to the caller, and the FaultStore counter proves the fault
    /// actually fired.
    #[tokio::test]
    async fn data_delete_failure_leaves_the_commit_record_deleted_and_reports() {
        let tenant = tenant();
        let now = 400 * NS_PER_DAY;
        let clock = FixedClock::new(now);
        let config = CompactorConfig::default();

        // Only the data object's delete fails: alert data objects are the only
        // keys on this shard ending in the RSEG data suffix.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Delete,
                ScriptedFault::Permanent("data delete refused".to_string()),
            )
            .with_key_contains(".rseg"),
        );
        let store = FaultStore::new(MemoryStore::new(), plan);
        let seeded = seed_alert(
            &store,
            &tenant,
            Uuid::from_u128(0xA4),
            1,
            now - 200 * NS_PER_DAY,
        )
        .await;

        let err = sweep_alert_retention(
            &store,
            &clock,
            &config,
            &NoLeases,
            &tenant,
            ALERT_SHARD,
            &AlertKeepSet::new(),
        )
        .await
        .expect_err("the data-object delete must surface as an error");
        assert!(
            matches!(err, MaintainError::Store(_)),
            "the store failure must surface as-is, got {err:?}"
        );

        assert_eq!(
            store.fault_count(Op::Delete, FaultKind::Permanent),
            1,
            "the injected delete fault must have fired exactly once"
        );
        assert!(
            !exists(&store, &seeded.commit_key).await,
            "the commit record is deleted before its data object, so it is already gone"
        );
        assert!(
            exists(&store, &seeded.data_key).await,
            "the data object survives the failed delete, as record-less data for orphan GC"
        );

        // Idempotent re-run: the record is gone, so the second pass has nothing
        // to list and the orphaned data object is left to orphan GC.
        let outcome = sweep_alert_retention(
            &store,
            &clock,
            &config,
            &NoLeases,
            &tenant,
            ALERT_SHARD,
            &AlertKeepSet::new(),
        )
        .await
        .expect("re-run converges");
        assert_eq!(outcome, AlertRetentionOutcome::default());
    }

    /// A legal hold over an alert record's keys blocks the sweep, with a
    /// no-hold control on the same input so the assertion cannot be vacuous.
    #[tokio::test]
    async fn a_lease_hold_blocks_the_alert_sweep() {
        let tenant = tenant();
        let now = 400 * NS_PER_DAY;
        let clock = FixedClock::new(now);
        let config = CompactorConfig::default();
        let old = now - 200 * NS_PER_DAY;

        // Control: no hold, the record is swept.
        {
            let store = MemoryStore::new();
            seed_alert(&store, &tenant, Uuid::from_u128(0xA5), 1, old).await;
            let outcome = sweep_alert_retention(
                &store,
                &clock,
                &config,
                &NoLeases,
                &tenant,
                ALERT_SHARD,
                &AlertKeepSet::new(),
            )
            .await
            .expect("control sweep");
            assert_eq!(outcome.records_deleted, 1);
        }

        {
            let store = MemoryStore::new();
            let seeded = seed_alert(&store, &tenant, Uuid::from_u128(0xA5), 1, old).await;
            let hold = HoldOne(seeded.data_key.clone());
            let outcome = sweep_alert_retention(
                &store,
                &clock,
                &config,
                &hold,
                &tenant,
                ALERT_SHARD,
                &AlertKeepSet::new(),
            )
            .await
            .expect("held sweep");
            assert_eq!(
                outcome,
                AlertRetentionOutcome {
                    records_deleted: 0,
                    data_deleted: 0,
                    kept: 1,
                    kept_current_state: 0,
                    gets_skipped_by_hour_prefilter: 0,
                },
                "a held record is kept, and not counted as a keep-set record"
            );
            assert!(exists(&store, &seeded.commit_key).await);
            assert!(exists(&store, &seeded.data_key).await);
        }
    }

    /// A [`LeaseCheck`] protecting exactly one key.
    struct HoldOne(String);

    impl LeaseCheck for HoldOne {
        fn is_protected(&self, key: &str) -> bool {
            key == self.0
        }
    }

    /// ADR-1688 decision 1: no alert record is ever compacted. `compact_bucket`
    /// refuses the signal, so the tombstone-and-compaction flow the data signals
    /// run cannot reach the alert prefix even if a caller asked for it.
    #[tokio::test]
    async fn compact_bucket_refuses_the_alerts_signal() {
        let store = MemoryStore::new();
        let tenant = tenant();
        let now = 400 * NS_PER_DAY;
        let hour = u32::try_from((now - 200 * NS_PER_DAY).div_euclid(NS_PER_HOUR)).unwrap();
        let base = i64::from(hour) * NS_PER_HOUR;
        // Enough inputs in one sealed bucket to reach the signal dispatch.
        let writer = Uuid::from_u128(0xA6);
        for seq in 1..=4u64 {
            seed_alert(&store, &tenant, writer, seq, base + seq as i64).await;
        }

        let clock = FixedClock::new(now);
        let bucket = Bucket::new(tenant, Signal::Alerts, ALERT_SHARD, hour);
        let err = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket)
            .await
            .expect_err("alerts must never compact");
        assert!(
            matches!(err, MaintainError::Invariant(_)),
            "got {err:?}, expected the signal dispatch to refuse Alerts"
        );
    }
}
