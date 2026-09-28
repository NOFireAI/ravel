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
//! What this sweep adds over the audit one is the **keep set**
//! ([`AlertKeepSet`], ADR-1688 decision 2 and its 2026-09-28 amendment): the
//! memo's `watermark_hour` together with the `ts_ns` of each alert identity's
//! current-state record. An expired record whose decoded `max_event_ts_ns` is
//! in that set is kept and counted separately, and no record is deleted at all
//! unless its ingest hour is strictly below the watermark. That is what makes
//! the deletion safe for the fold: after a sweep the surviving prefix is every
//! transition inside the window plus one current-state record per identity, so
//! a cold-start fold over the survivors yields the same
//! latest-record-per-identity map it would have yielded over the unswept
//! history. Without it, an alert that has been firing for longer than the
//! window would read as inactive after a restart and re-fire with a fresh
//! `generation`, which is exactly the outcome ADR-1294's retention contract
//! ("prune only into a compact form, never by deletion") forbids.
//!
//! # The watermark is a strict floor on what may be deleted
//!
//! The memo names each identity's latest record only for hours strictly below
//! its `watermark_hour`: a late write from an overlapping lease holder can land
//! in the watermark hour itself after the memo was written, and the memo would
//! not name it. Deleting an expired record in that hour because the keep set
//! does not mention it would drop an identity's newest transition, and a
//! cold-start fold would then return that identity's older, kept record. So the
//! watermark travels with the keep set and the sweep applies it per record,
//! strictly: an ingest hour at or above `watermark_hour` is kept without a GET.
//!
//! The sweep does not require the watermark to sit at or above the expiry
//! floor, because a watermark below it can only keep more. Refusing a memo too
//! stale to be worth sweeping under is the driver's job (ADR-1688 decision 3).
//! What the sweep does refuse is a call with no memo behind it: the keep set has
//! no "absent" value, [`AlertKeepSet::new`] demands a watermark, and there is no
//! `Default`, so a caller that could not read a memo has nothing it can pass.
//! An empty `ts_ns` set with a real watermark is a different thing and is
//! legitimate: it is a tenant whose memo holds no identities.
//!
//! # The keep set cannot be checked before the expiry test
//!
//! Membership is on `max_event_ts_ns`, a record body field, so the GET the
//! expiry test also reads is needed either way: no ordering of the two saves a
//! request. What the order still decides is the counters, and it is deliberate.
//! [`AlertRetentionOutcome::kept_current_state`] means exactly "expired,
//! past-horizon records that only the keep set saved", the figure that says how
//! much the keep set is holding back; a record the keep set names that is still
//! inside the window, or still inside the protection horizon, was never at risk
//! and is counted under `kept` alone.
//!
//! One sweep costs one GET per record that is below the watermark and not
//! cleared by the hour prefilter. Once a shard has been swept once, that is the
//! kept identities themselves plus whatever shares their exact `ts_ns` stamp:
//! bounded by the identity count and one tick's transitions, not by the length
//! of the history.
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

/// What the alert state memo tells the sweep: the hour the memo is complete
/// below, and the `ts_ns` of each identity's current-state record (ADR-1688
/// decision 2, as amended on 2026-09-28).
///
/// The memo's records carry `alert_id`, `rule_id`, `state`, `generation`,
/// `ts_ns`, labels, annotations and body, and no commit identity, so a keep set
/// of `(epoch, seq)` pairs has no source. The pair is not unique either: the
/// evaluator writes a constant epoch and a `seq` that restarts at 1 in every
/// evaluator process, so one pair names a record from every process lifetime.
/// The `ts_ns` is what both sides do carry. Every alert commit record's
/// `max_event_ts_ns` equals its RLOG row's `ts_ns` because the evaluator stamps
/// both from the same value, and this sweep decodes the record anyway, so
/// membership costs no extra request and no memo format change.
///
/// Keying by `ts_ns` can only over-keep: two transitions written in the same
/// tick share one stamp, so a record that is not any identity's current state
/// can be spared for sharing a kept identity's stamp. The surplus is one tick's
/// records per kept identity, not a quantity that grows with history, and
/// over-keeping is the direction ADR-1688 decision 3 requires of any
/// imprecision here.
///
/// There is no `Default` and no empty constructor: a keep set cannot exist
/// without a watermark, which is what makes "sweep with no memo" unwritable
/// rather than a call that deletes every expired record. An empty `ts_ns` set
/// under a real watermark is a tenant with no identities in its memo, and is
/// swept normally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertKeepSet {
    watermark_hour: u32,
    ts_ns: BTreeSet<i64>,
}

impl AlertKeepSet {
    /// Build a keep set from a memo's `watermark_hour` and the `ts_ns` of the
    /// records it holds.
    pub fn new(watermark_hour: u32, ts_ns: impl IntoIterator<Item = i64>) -> Self {
        AlertKeepSet {
            watermark_hour,
            ts_ns: ts_ns.into_iter().collect(),
        }
    }

    /// The memo's watermark hour. The sweep deletes only below it, strictly:
    /// the memo is complete only for hours strictly below this one, so a record
    /// in the watermark hour that the memo does not name may still be its
    /// identity's newest transition.
    pub fn watermark_hour(&self) -> u32 {
        self.watermark_hour
    }

    /// Whether a commit record's `max_event_ts_ns` is one of the memo's
    /// current-state stamps.
    pub fn contains(&self, max_event_ts_ns: i64) -> bool {
        self.ts_ns.contains(&max_event_ts_ns)
    }
}

/// What one [`sweep_alert_retention`] pass over a tenant's alert shard deleted
/// (or, under `dry_run`, would have).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AlertRetentionOutcome {
    /// Expired alert commit records deleted.
    pub records_deleted: usize,
    /// Data objects of expired alert commit records deleted.
    pub data_deleted: usize,
    /// Entries left in place: not yet expired, still within the protection
    /// horizon, at or above the keep set's watermark, named by the keep set, or
    /// lease/legal-hold protected. Every entry the sweep does not delete is
    /// counted here, including the compaction, rewrite and tombstone entries it
    /// keeps without understanding them (none of the three belongs under the
    /// alerts commit prefix, and this sweep never removes one).
    pub kept: usize,
    /// Records that were expired and past the horizon and were kept only
    /// because the keep set names them as an identity's current state. A subset
    /// of `kept`.
    pub kept_current_state: usize,
    /// Commit records whose GET and decode was skipped because their ingest
    /// hour is at or above the keep set's watermark, so the memo cannot be
    /// complete for them. A subset of `kept`.
    pub kept_at_or_above_watermark: usize,
    /// Commit records whose GET and decode was skipped because the key's
    /// `ingest_hour_bucket` alone proved the record cannot be expired. A subset
    /// of `kept`.
    pub gets_skipped_by_hour_prefilter: usize,
}

/// Sweep expired alert transition records from a tenant's alert shard,
/// keeping every identity's current-state record (ADR-1688 decisions 1 and 2).
///
/// A record is deleted only when all of these hold: its `ingest_hour_bucket` is
/// strictly below `keep.watermark_hour()`, it is expired (its newest event is
/// older than `config.alert_retention_window_ns`), it is past the protection
/// horizon (`now >= created_unix_ns + config.protection_horizon_ns`), its
/// `max_event_ts_ns` is not in `keep`, and the [`LeaseCheck`] does not protect
/// it or its data object.
///
/// The commit record is deleted before its data object, so a crash between the
/// two leaves a data object no record points at, never a record pointing at a
/// deleted object. Nothing reclaims that object today: the orphan sweep over
/// the alerts shard is ADR-1688 follow-up task 2, and until it lands the object
/// leaks. No tombstone is ever written.
///
/// `shard` is the alert shard to sweep. It is a parameter rather than a
/// constant because the evaluator's `ALERT_SHARD` is defined in
/// `services/ravel-server` (`alerting.rs`), which this crate does not depend
/// on; the driver passes that constant through.
///
/// `keep` is an [`AlertKeepSet`] by reference, not an `Option`: the driver builds
/// one from a memo it actually read, and a tenant whose memo is missing,
/// undecodable, of an unsupported version, or too stale is skipped there
/// (ADR-1688 decision 3) rather than swept under a set that means nothing. An
/// empty `ts_ns` set is not that case; it is a memo holding no identities, and
/// the sweep honours it, deleting the shard's expired records below the
/// watermark.
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
                // The memo behind the keep set is complete only for hours
                // strictly below its watermark, so a record in the watermark
                // hour may be its identity's newest transition and still be
                // absent from the set. Keep it, without a GET.
                if parsed.ingest_hour_bucket >= keep.watermark_hour() {
                    outcome.kept += 1;
                    outcome.kept_at_or_above_watermark += 1;
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
                if keep.contains(commit.max_event_ts_ns) {
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

    /// The keys one seeded alert transition occupies, and the stamp a memo
    /// naming it as an identity's current state would carry.
    #[derive(Debug, Clone)]
    struct Seeded {
        commit_key: String,
        data_key: String,
        ts_ns: i64,
    }

    /// The `ingest_hour_bucket` a transition stamped `ts_ns` lands in.
    fn hour_of(ts_ns: i64) -> u32 {
        u32::try_from(ts_ns.div_euclid(NS_PER_HOUR)).expect("hour bucket")
    }

    /// A keep set whose watermark is `now`'s own hour: every record seeded
    /// strictly before this hour is below the watermark and so a candidate for
    /// deletion, which is what tests about the other gates want.
    fn keep_none_below(now: i64) -> AlertKeepSet {
        AlertKeepSet::new(hour_of(now), [])
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
        seed_alert_created(store, tenant, writer_id, seq, ts_ns, ts_ns).await
    }

    /// As [`seed_alert`], but with `created_unix_ns` diverging from the
    /// transition stamp. The evaluator never produces that, a backfill or a
    /// clock correction can, and it is the only way to exercise the protection
    /// horizon apart from the retention window.
    async fn seed_alert_created(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        writer_id: Uuid,
        seq: u64,
        ts_ns: i64,
        created_unix_ns: i64,
    ) -> Seeded {
        let body = Bytes::from(format!("alert transition seq={seq} ts={ts_ns}"));
        let content_hash: [u8; 32] = *blake3::hash(&body).as_bytes();
        let ingest_hour_bucket = hour_of(ts_ns);
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
            created_unix_ns,
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
            ts_ns,
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

        // The memo names the 97-day-old transition as some identity's current
        // state: an alert that last transitioned then and is still in that
        // state. Its watermark is this tick's hour, so every seeded record is
        // below it.
        let kept_record = expired[3].clone();
        let keep = AlertKeepSet::new(hour_of(now), [kept_record.ts_ns]);

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
                // The live records are cleared by the prefilter before the
                // watermark is consulted, and everything seeded is below it.
                kept_at_or_above_watermark: 0,
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
            &keep_none_below(now),
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

    /// A negative `alert_retention_window_ns` is refused by the same
    /// non-positive guard `0` hits, not honoured as a window in the future that
    /// would make every record on the shard a delete candidate at once.
    #[tokio::test]
    async fn a_negative_window_disables_the_sweep_entirely() {
        let store = InstrumentedStore::new(MemoryStore::new());
        let tenant = tenant();
        let now = 400 * NS_PER_DAY;
        let clock = FixedClock::new(now);
        let writer = Uuid::from_u128(0xA8);
        let config = CompactorConfig {
            alert_retention_window_ns: -NS_PER_DAY,
            ..CompactorConfig::default()
        };

        // Records old enough that a window read as `now + 1 day` would sweep
        // every one past the protection horizon.
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
            &keep_none_below(now),
        )
        .await
        .expect("sweep");

        assert_eq!(
            outcome,
            AlertRetentionOutcome::default(),
            "a negative window must do nothing at all"
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

    /// The watermark boundary is strict (ADR-1688 keep-set amendment). The memo
    /// is complete only for hours strictly below its `watermark_hour`, so an
    /// expired record in the watermark hour that the memo does not name may
    /// still be its identity's newest transition and must survive. One hour
    /// below the watermark, the very same record is deleted, so the assertion
    /// is about the boundary and not about some other gate.
    #[tokio::test]
    async fn an_expired_record_in_the_watermark_hour_survives_and_one_hour_below_does_not() {
        let store = MemoryStore::new();
        let tenant = tenant();
        let now = 400 * NS_PER_DAY;
        let clock = FixedClock::new(now);
        let config = CompactorConfig::default();

        // Far past the 90-day window and the protection horizon: every other
        // gate would let this record go.
        let ts = now - 200 * NS_PER_DAY;
        let seeded = seed_alert(&store, &tenant, Uuid::from_u128(0xA7), 1, ts).await;
        let record_hour = hour_of(ts);

        let outcome = sweep_alert_retention(
            &store,
            &clock,
            &config,
            &NoLeases,
            &tenant,
            ALERT_SHARD,
            &AlertKeepSet::new(record_hour, []),
        )
        .await
        .expect("at-watermark sweep");
        assert_eq!(
            outcome,
            AlertRetentionOutcome {
                records_deleted: 0,
                data_deleted: 0,
                kept: 1,
                // Kept by the watermark, not by the keep set: the set is empty.
                kept_current_state: 0,
                kept_at_or_above_watermark: 1,
                gets_skipped_by_hour_prefilter: 0,
            },
            "a record in the watermark hour must not be deleted"
        );
        assert!(exists(&store, &seeded.commit_key).await);
        assert!(exists(&store, &seeded.data_key).await);

        // The same record, the same empty keep set, a watermark one hour above
        // it: now strictly below, and swept.
        let outcome = sweep_alert_retention(
            &store,
            &clock,
            &config,
            &NoLeases,
            &tenant,
            ALERT_SHARD,
            &AlertKeepSet::new(record_hour + 1, []),
        )
        .await
        .expect("below-watermark sweep");
        assert_eq!(
            outcome,
            AlertRetentionOutcome {
                records_deleted: 1,
                data_deleted: 1,
                kept: 0,
                kept_current_state: 0,
                kept_at_or_above_watermark: 0,
                gets_skipped_by_hour_prefilter: 0,
            },
            "one hour below the watermark the same record is deleted"
        );
        assert!(!exists(&store, &seeded.commit_key).await);
        assert!(!exists(&store, &seeded.data_key).await);
    }

    /// The keep set keys on `ts_ns`, not on `(epoch, seq)`. Two evaluator
    /// processes each write a record with the constant `ALERT_WRITER_EPOCH` and
    /// a `seq` counter restarted at 1, so the two share a pair and differ only
    /// in their stamp. With one of the two stamps in the keep set, exactly one
    /// survives; keyed by the pair, both would.
    #[tokio::test]
    async fn two_writers_sharing_a_seq_are_told_apart_by_ts_ns() {
        let store = MemoryStore::new();
        let tenant = tenant();
        let now = 400 * NS_PER_DAY;
        let clock = FixedClock::new(now);
        let config = CompactorConfig::default();

        let kept = seed_alert(
            &store,
            &tenant,
            Uuid::from_u128(0xB1),
            1,
            now - 200 * NS_PER_DAY,
        )
        .await;
        let doomed = seed_alert(
            &store,
            &tenant,
            Uuid::from_u128(0xB2),
            1,
            now - 199 * NS_PER_DAY,
        )
        .await;
        assert_ne!(
            kept.ts_ns, doomed.ts_ns,
            "the two records must differ in the field the keep set reads"
        );

        let outcome = sweep_alert_retention(
            &store,
            &clock,
            &config,
            &NoLeases,
            &tenant,
            ALERT_SHARD,
            &AlertKeepSet::new(hour_of(now), [kept.ts_ns]),
        )
        .await
        .expect("sweep");
        assert_eq!(
            outcome,
            AlertRetentionOutcome {
                records_deleted: 1,
                data_deleted: 1,
                kept: 1,
                kept_current_state: 1,
                kept_at_or_above_watermark: 0,
                gets_skipped_by_hour_prefilter: 0,
            },
            "exactly one of the two records is kept, and by the keep set"
        );
        assert!(exists(&store, &kept.commit_key).await);
        assert!(exists(&store, &kept.data_key).await);
        assert!(!exists(&store, &doomed.commit_key).await);
        assert!(!exists(&store, &doomed.data_key).await);
    }

    /// An empty `ts_ns` set is a memo that holds no identities, not a missing
    /// memo: the sweep runs, deletes the expired records strictly below the
    /// watermark, and keeps everything at or above it.
    #[tokio::test]
    async fn an_empty_ts_ns_set_sweeps_below_its_watermark_only() {
        let store = MemoryStore::new();
        let tenant = tenant();
        let now = 400 * NS_PER_DAY;
        let clock = FixedClock::new(now);
        let config = CompactorConfig::default();
        let writer = Uuid::from_u128(0xC1);

        let watermark_ts = now - 150 * NS_PER_DAY;
        let watermark = hour_of(watermark_ts);
        // Two expired records below the watermark, one expired record in the
        // watermark hour itself, one expired record above it, and one live
        // record the hour prefilter clears before the watermark is consulted.
        let below = seed_history(
            &store,
            &tenant,
            writer,
            now,
            &[(200 * 24, 1), (199 * 24, 2)],
        )
        .await;
        let at = seed_alert(&store, &tenant, writer, 3, watermark_ts).await;
        let above = seed_alert(&store, &tenant, writer, 4, now - 100 * NS_PER_DAY).await;
        let live = seed_alert(&store, &tenant, writer, 5, now - 5 * NS_PER_HOUR).await;

        let outcome = sweep_alert_retention(
            &store,
            &clock,
            &config,
            &NoLeases,
            &tenant,
            ALERT_SHARD,
            &AlertKeepSet::new(watermark, []),
        )
        .await
        .expect("sweep");
        assert_eq!(
            outcome,
            AlertRetentionOutcome {
                records_deleted: 2,
                data_deleted: 2,
                kept: 3,
                kept_current_state: 0,
                kept_at_or_above_watermark: 2,
                gets_skipped_by_hour_prefilter: 1,
            },
            "an empty set still sweeps, but only strictly below the watermark"
        );
        for gone in &below {
            assert!(!exists(&store, &gone.commit_key).await);
            assert!(!exists(&store, &gone.data_key).await);
        }
        for alive in [&at, &above, &live] {
            assert!(
                exists(&store, &alive.commit_key).await,
                "{} must survive",
                alive.commit_key
            );
            assert!(exists(&store, &alive.data_key).await);
        }
    }

    /// The keep-set test runs after the expiry and horizon tests, which is what
    /// makes `kept_current_state` mean "expired, past-horizon records that only
    /// the keep set saved". Both records here are named by the keep set and
    /// both are kept, one by the window and one by the horizon, so neither is
    /// counted as kept by the set.
    #[tokio::test]
    async fn a_keep_set_record_that_was_never_at_risk_is_not_counted_as_kept_by_the_set() {
        let store = MemoryStore::new();
        let tenant = tenant();
        // Half past the hour, so the expiry floor falls inside an hour bucket
        // and the hour prefilter cannot clear a record that sits just inside
        // the window: the expiry test itself has to be what keeps it.
        let now = 400 * NS_PER_DAY + NS_PER_HOUR / 2;
        let clock = FixedClock::new(now);
        let config = CompactorConfig::default();
        let expiry_floor = now - config.alert_retention_window_ns;

        // Inside the window by a quarter of an hour, in the hour bucket that
        // starts below the floor.
        let inside_window = seed_alert(
            &store,
            &tenant,
            Uuid::from_u128(0xD1),
            1,
            expiry_floor + NS_PER_HOUR / 4,
        )
        .await;
        assert!(
            i64::from(hour_of(inside_window.ts_ns)) * NS_PER_HOUR < expiry_floor,
            "the prefilter must not be able to clear this record"
        );
        // Expired long ago, but written moments ago: inside the horizon.
        let inside_horizon = seed_alert_created(
            &store,
            &tenant,
            Uuid::from_u128(0xD2),
            1,
            now - 200 * NS_PER_DAY,
            now - config.protection_horizon_ns / 2,
        )
        .await;

        let keep = AlertKeepSet::new(hour_of(now), [inside_window.ts_ns, inside_horizon.ts_ns]);
        assert!(keep.contains(inside_window.ts_ns) && keep.contains(inside_horizon.ts_ns));

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
                records_deleted: 0,
                data_deleted: 0,
                kept: 2,
                // Checked before the expiry and horizon tests, this would be 2.
                kept_current_state: 0,
                kept_at_or_above_watermark: 0,
                gets_skipped_by_hour_prefilter: 0,
            },
            "a keep-set record that was never at risk is counted under kept alone"
        );
        assert!(exists(&store, &inside_window.commit_key).await);
        assert!(exists(&store, &inside_horizon.commit_key).await);
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
        let seeded =
            seed_alert_created(&store, &tenant, Uuid::from_u128(0xA3), 1, event_ts, created).await;
        let commit_key = seeded.commit_key.clone();
        let data_key = seeded.data_key.clone();

        let outcome = sweep_alert_retention(
            &store,
            &clock,
            &config,
            &NoLeases,
            &tenant,
            ALERT_SHARD,
            &keep_none_below(now),
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
                kept_at_or_above_watermark: 0,
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
            &keep_none_below(now),
        )
        .await
        .expect("sweep");
        assert_eq!(outcome.records_deleted, 1);
        assert_eq!(outcome.data_deleted, 1);
        assert!(!exists(&store, &commit_key).await);
        assert!(!exists(&store, &data_key).await);
    }

    /// Delete ordering under failure: the commit record goes first, so a failure
    /// on the data-object delete leaves the record already gone and a data
    /// object nothing points at, never a commit record naming a deleted object.
    /// That object is reclaimed by the alerts-shard orphan sweep ADR-1688
    /// follow-up task 2 adds; until then it leaks. The error surfaces to the
    /// caller, and the FaultStore counter proves the fault actually fired.
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
            &keep_none_below(now),
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
            "the data object survives the failed delete, with no record naming it"
        );

        // Idempotent re-run: the record is gone, so the second pass has nothing
        // to list, and the orphaned data object stays until the alerts-shard
        // orphan sweep of ADR-1688 follow-up task 2 reclaims it.
        let outcome = sweep_alert_retention(
            &store,
            &clock,
            &config,
            &NoLeases,
            &tenant,
            ALERT_SHARD,
            &keep_none_below(now),
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
                &keep_none_below(now),
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
                &keep_none_below(now),
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
                    kept_at_or_above_watermark: 0,
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
