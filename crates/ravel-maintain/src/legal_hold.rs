//! Legal hold: a real [`LeaseCheck`] over immutable hold records (ADR-0042
//! decision 2).
//!
//! The sweeper consults a [`LeaseCheck`] before every physical delete. Legal
//! hold is the first non-trivial implementation of that seam: an object a hold
//! covers is not deleted by any sweep rule or retention batch whose hold
//! re-read saw the hold, until the hold that covers it is cleared.
//!
//! # The sync/async tension
//!
//! [`LeaseCheck::is_protected`] is synchronous and called once per candidate
//! key inside a sweep pass, but deciding whether a key is under a hold requires
//! reading hold records from object storage, which is async. Holds are read
//! at two points. [`LegalHoldCheck::refresh`] loads the per-tick snapshot that
//! planning decisions read. Every delete batch then re-reads the hold set from
//! the store, through the snapshot's [`HoldSource`], immediately before its
//! first delete, and judges its deletes from that read (ADR-0042, the
//! delete-batch re-read amendment, #4501). A batch whose re-read fails deletes
//! nothing and is counted on [`HoldSource::reread_failures`].
//! [`LegalHoldCheck::is_protected`] itself never touches I/O.
//!
//! The guarantee: a legal hold whose set record was durable before a delete
//! batch re-read the hold set is honored by that batch. A hold written after
//! the re-read and before the batch's delete requests return is not.
//!
//! A re-read is one LIST of the hold shard. Hold records are immutable, so the
//! commit-record and audit-object GETs behind each listed record are served
//! from a [`HoldRecordCache`] after the first read; the LIST is never cached.
//!
//! # Hold records (ADR-0040, ADR-0042 decision 2)
//!
//! A hold is not a mutable flag. Each set and each clear is a new immutable
//! record on [`Signal::Audit`], written in RLOG's format (ADR-0040), never an
//! in-place mutation. Every record carries these attrs:
//!
//! - `kind` = `legal_hold` (records without this attr are ordinary audit
//!   records and are ignored);
//! - `hold.op` = `set` or `clear`;
//! - `hold.scope` = one object-key prefix the hold covers. A whole tenant is
//!   `t/<tenant_hex>/`. Holding one shard is NOT a single prefix: L0 data,
//!   commit records, and L1 parts for one `(tenant, signal, shard)` live under
//!   three separate sibling prefixes (`.../l0/<shard>/`, `.../c/<shard>/`,
//!   `.../l1/<shard>/`), each checked against `is_protected` independently by
//!   the sweeper/retention. [`shard_hold_scopes`] returns all three; a
//!   shard-level hold means calling [`write_hold_set`] once per prefix it
//!   returns, not once with a single L0-only scope (that would leave the
//!   shard's commit records and compacted L1 parts unprotected - the
//!   sweeper's and retention's own delete rules never look at the L0 prefix
//!   to gate an L1 or commit-record delete);
//! - `hold.reason` (optional, on a set) = free-text justification.
//!
//! The record's `ts_ns` is the set-at / cleared-at timestamp. Current hold
//! state is derived by fold, never stored: for each distinct `hold.scope`, the
//! record with the greatest `ts_ns` wins (ties resolve to `set`, the
//! fail-safe: an object stays protected when set and clear are
//! indistinguishable in time). A scope whose winning record is a `set` is
//! active; a `clear` (or no record) leaves it inactive. This is exactly the
//! fold-to-derive-current-state rule ADR-0040 defines for alert state.
//!
//! All of a tenant's legal-hold records live on one control-plane bucket,
//! [`Signal::Audit`] shard [`AUDIT_HOLD_SHARD`], so a refresh is a single
//! shard listing. That single-listing property is about legal hold's own
//! refresh, not a claim that [`AUDIT_HOLD_SHARD`] is the only shard under
//! [`Signal::Audit`]: query-audit records live on a separate sibling shard
//! ([`crate::query_audit::QUERY_AUDIT_SHARD`]), kept deliberately apart from
//! this control-plane shard so an unbounded query-audit stream never bloats a
//! hold refresh (see [`crate::query_audit`] for the full rationale). Those
//! records are on a different shard, so a hold refresh never lists them and
//! they are correctly invisible to the fold. An empty snapshot (a tenant that
//! has never set a hold) protects nothing, so it behaves identically to
//! [`crate::NoLeases`].

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use ravel_commit::keys::{self, BucketEntry, KeyError};
use ravel_commit::record;
use ravel_logseg::{
    AttrValue, LogRecord, LogStreamId, Predicate, RlogConfig, RlogReader, stream_attrs_bytes,
};
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

use crate::audit_write::{AuditWrite, write_audit_object_with_compat};
use crate::compat_policy::MaintainCompatPolicy;
use crate::error::{MaintainError, Result};
use crate::sweep::LeaseCheck;

/// The [`Signal::Audit`] shard every legal-hold record for a tenant is written
/// to and read from. Legal hold is a small per-tenant control plane, so a
/// single fixed shard makes a refresh one LIST and keeps set/clear records
/// colocated for the fold; it is unrelated to the data shards a hold's scope
/// may cover.
pub const AUDIT_HOLD_SHARD: u32 = 0;
// Moving this writer to a shard outside the reader's floor (ADR-1101
// decision 2) fails the build here rather than silently dropping every
// legal-hold record from audit queries.
const _: () = assert!(AUDIT_HOLD_SHARD < Signal::Audit.fixed_read_shards());

/// `attrs` key marking an audit record as a legal-hold record. An audit record
/// without this key (an ordinary query/admin audit entry) is ignored here.
const ATTR_KIND: &str = "kind";
/// `attrs[ATTR_KIND]` value identifying a legal-hold record.
const KIND_LEGAL_HOLD: &str = "legal_hold";
/// `attrs` key holding the operation: [`OP_SET`] or [`OP_CLEAR`].
const ATTR_OP: &str = "hold.op";
/// `attrs` key holding the object-key prefix the hold covers.
const ATTR_SCOPE: &str = "hold.scope";
/// `attrs` key holding an optional free-text justification on a set.
const ATTR_REASON: &str = "hold.reason";
const OP_SET: &str = "set";
const OP_CLEAR: &str = "clear";

/// Resource-attr `record_type` value for the shared legal-hold log stream. All
/// hold records ride one stream; the fold keys on `hold.scope`, not the stream.
const STREAM_RECORD_TYPE: &str = "legal_hold";
/// Scope name of the shared legal-hold log stream.
const STREAM_SCOPE_NAME: &str = "ravel.legal_hold";
const STREAM_SCOPE_VERSION: &str = "1";

/// The three real key prefixes one `(tenant, signal, shard)` spans: L0 data
/// objects, commit records (which live under an hour-bucketed sub-prefix, so
/// the commit prefix here is the hour-agnostic parent of all of them), and L1
/// compacted parts (`docs/catalog-and-mvcc.md`'s key layout). A hold scoped to
/// only one of these - e.g. just the L0 prefix - does NOT protect the other
/// two: the sweeper deletes commit records once their data is superseded, and
/// retention deletes L1 parts directly, both checked against `is_protected`
/// independently of the L0 prefix. Callers wanting to hold everything under
/// one shard MUST hold all three prefixes this returns, not just one of them;
/// this function exists so that mistake is a documented, single call rather
/// than three independently-reasoned string literals.
pub fn shard_hold_scopes(tenant: &TenantHash, signal: Signal, shard: u32) -> Result<[String; 3]> {
    if shard > 9999 {
        return Err(MaintainError::Invariant(format!(
            "shard {shard} out of range for a hold scope (max 9999)"
        )));
    }
    let shard_str = format!("{shard:04}");
    let tenant_hex = tenant.to_hex();
    let signal_prefix = signal.key_prefix();
    Ok([
        format!("t/{tenant_hex}/{signal_prefix}/l0/{shard_str}/"),
        keys::commit_shard_prefix(tenant, signal, shard)?,
        format!(
            "t/{tenant_hex}/{signal_prefix}/{}/{shard_str}/",
            keys::L1_DIR
        ),
    ])
}

/// A hold set or clear, either direction of the fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HoldOp {
    Set,
    Clear,
}

impl HoldOp {
    /// Tie-break rank when two records share a `ts_ns`: `Set` outranks `Clear`
    /// so an object stays protected under an exact-timestamp tie (fail-safe).
    fn rank(self) -> u8 {
        match self {
            HoldOp::Clear => 0,
            HoldOp::Set => 1,
        }
    }
}

/// One decoded legal-hold record: the scope it covers, its direction, and its
/// set-at / cleared-at timestamp (the fold key).
#[derive(Debug, Clone)]
struct HoldRecord {
    scope: String,
    op: HoldOp,
    ts_ns: i64,
}

/// Decoded hold records per hold commit-record key, shared by every re-read
/// that goes through it. A hold record and the audit object it names are
/// immutable, so an entry never goes stale; a record deleted from the store
/// stops being listed and so stops being read, whatever the cache holds.
///
/// Cloning shares the map. The server keeps one per process in its
/// [`crate::MaintainMemo`], so the GETs behind a record are paid once per
/// process; each entry is keyed by the full commit key, which names the
/// tenant.
#[derive(Debug, Clone, Default)]
pub struct HoldRecordCache {
    records: Arc<Mutex<HashMap<String, Arc<[HoldRecord]>>>>,
}

impl HoldRecordCache {
    fn get(&self, key: &str) -> Option<Arc<[HoldRecord]>> {
        self.records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key)
            .cloned()
    }

    fn insert(&self, key: String, records: Arc<[HoldRecord]>) {
        self.records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key, records);
    }

    /// The number of commit records cached (introspection and tests).
    pub fn len(&self) -> usize {
        self.records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Whether nothing is cached.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Where a delete batch re-reads one tenant's hold set (ADR-0042, the
/// delete-batch re-read amendment). Every [`LegalHoldCheck`] built by
/// [`LegalHoldCheck::refresh`] or [`LegalHoldCheck::refresh_from`] carries
/// one, and the sweeps reach it through [`LeaseCheck::hold_source`].
///
/// The store is not held here: a batch re-reads from the store it is about to
/// delete from, which is the store the caller passed the sweep.
#[derive(Debug)]
pub struct HoldSource {
    tenant: TenantHash,
    cache: HoldRecordCache,
    reread_failures: AtomicU64,
}

impl HoldSource {
    /// A source for `tenant` with a cache of its own.
    pub fn new(tenant: TenantHash) -> Self {
        Self::with_cache(tenant, HoldRecordCache::default())
    }

    /// A source for `tenant` reading through `cache`, which may be shared with
    /// other sources (the server shares one per process).
    pub fn with_cache(tenant: TenantHash, cache: HoldRecordCache) -> Self {
        HoldSource {
            tenant,
            cache,
            reread_failures: AtomicU64::new(0),
        }
    }

    /// The tenant whose holds this source reads.
    pub fn tenant(&self) -> &TenantHash {
        &self.tenant
    }

    /// The tenant's hold set as the store holds it now: one LIST of the hold
    /// shard, with each listed record's GETs served from the cache when it
    /// has them. The returned snapshot carries no source of its own.
    pub async fn current(&self, store: &dyn ObjectStoreBackend) -> Result<LegalHoldCheck> {
        let records = load_hold_records(store, &self.tenant, Some(&self.cache)).await?;
        Ok(LegalHoldCheck {
            held_prefixes: fold_active_holds(records),
            source: None,
        })
    }

    /// Hold re-reads that failed since this source was made: a delete batch
    /// whose re-read failed deleted nothing, and an erasure-request pass whose
    /// re-read failed kept every candidate request. The server adds these to
    /// its legal-hold refresh-failure counter.
    pub fn reread_failures(&self) -> u64 {
        self.reread_failures.load(Ordering::Relaxed)
    }

    pub(crate) fn note_reread_failure(&self) {
        self.reread_failures.fetch_add(1, Ordering::Relaxed);
    }
}

/// A pre-resolved, in-memory snapshot of a tenant's active legal holds,
/// implementing [`LeaseCheck`]. Built once per maintain tick by
/// [`LegalHoldCheck::refresh`]; [`LegalHoldCheck::is_protected`] consults only
/// this snapshot and never blocks on I/O. Planning decisions read the
/// snapshot; each delete batch re-reads through [`Self::source`].
///
/// An empty snapshot protects nothing and is byte-for-byte equivalent to
/// [`crate::NoLeases`] as a sweep gate, which is exactly what a tenant with no
/// configured holds gets.
#[derive(Debug, Clone, Default)]
pub struct LegalHoldCheck {
    /// Active held key-prefixes (each scope whose latest record is a set),
    /// sorted and deduplicated for a deterministic snapshot.
    held_prefixes: Vec<String>,
    /// Where each delete batch re-reads the hold set. `None` on
    /// [`Self::empty`] and on a snapshot [`HoldSource::current`] returned.
    source: Option<Arc<HoldSource>>,
}

impl LegalHoldCheck {
    /// An empty snapshot: nothing is held. Identical in effect to
    /// [`crate::NoLeases`], and with no [`HoldSource`], so a sweep under it
    /// re-reads nothing. Useful as a default before the first refresh.
    pub fn empty() -> Self {
        LegalHoldCheck::default()
    }

    /// An empty snapshot bound to `source`: planning sees no hold, and every
    /// delete batch still re-reads through `source`.
    pub fn empty_with_source(source: Arc<HoldSource>) -> Self {
        LegalHoldCheck {
            held_prefixes: Vec::new(),
            source: Some(source),
        }
    }

    /// Load and fold a tenant's legal-hold records from
    /// [`Signal::Audit`] shard [`AUDIT_HOLD_SHARD`] into a fresh snapshot,
    /// bound to a new [`HoldSource`] for the tenant so the sweeps it is
    /// passed to re-read holds before each delete batch.
    ///
    /// Lists the shard's commit records, reads each referenced audit object,
    /// decodes its RLOG records, keeps the legal-hold ones, and folds them to
    /// the set of currently-active scopes.
    pub async fn refresh(store: &dyn ObjectStoreBackend, tenant: &TenantHash) -> Result<Self> {
        Self::refresh_from(store, Arc::new(HoldSource::new(*tenant))).await
    }

    /// [`Self::refresh`] through an existing `source`, whose cache the load
    /// fills and whose failure counter the sweeps' re-reads move.
    pub async fn refresh_from(
        store: &dyn ObjectStoreBackend,
        source: Arc<HoldSource>,
    ) -> Result<Self> {
        let current = source.current(store).await?;
        Ok(LegalHoldCheck {
            held_prefixes: current.held_prefixes,
            source: Some(source),
        })
    }

    /// The source each delete batch re-reads through, if any.
    pub fn source(&self) -> Option<&Arc<HoldSource>> {
        self.source.as_ref()
    }

    /// The currently-active held key-prefixes (introspection and tests).
    pub fn active_prefixes(&self) -> &[String] {
        &self.held_prefixes
    }

    /// Whether the snapshot holds nothing (so it protects nothing, exactly like
    /// [`crate::NoLeases`]).
    pub fn is_empty(&self) -> bool {
        self.held_prefixes.is_empty()
    }
}

impl LeaseCheck for LegalHoldCheck {
    /// Synchronous, snapshot-only: `key` is protected if any active held
    /// prefix is a prefix of it. No I/O.
    fn is_protected(&self, key: &str) -> bool {
        self.held_prefixes
            .iter()
            .any(|prefix| key.starts_with(prefix.as_str()))
    }

    fn hold_source(&self) -> Option<&HoldSource> {
        self.source.as_deref()
    }
}

/// What a [`DeleteBatch`] says about deleting a set of keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Admit {
    /// No hold the batch observed covers any of the keys.
    Delete,
    /// A hold the batch observed covers at least one of the keys.
    Held,
    /// The batch's hold re-read failed: the batch deletes nothing.
    ReadFailed,
}

/// One delete batch's hold set (ADR-0042, the delete-batch re-read
/// amendment). The first [`Self::admit`] re-reads the hold set through the
/// pass lease's [`HoldSource`], and every later one in the batch judges from
/// that same read, not from the pass snapshot. A failed re-read is counted
/// once on the source and makes every later [`Self::admit`] of the batch
/// answer [`Admit::ReadFailed`].
///
/// A lease with no source ([`crate::NoLeases`], a test lease) is judged as
/// itself, with no re-read.
pub(crate) struct DeleteBatch<'a> {
    lease: &'a dyn LeaseCheck,
    tenant: TenantHash,
    batch: &'static str,
    observe_unsourced: bool,
    state: BatchState,
}

enum BatchState {
    Unread,
    Read {
        /// The batch's own re-read; `None` for a lease with no source.
        fresh: Option<LegalHoldCheck>,
        /// For a lease with no source, the hold set re-read only for the
        /// HeldObjectNeverDeleted assertion; never consulted to decide.
        observed: Option<LegalHoldCheck>,
        /// Whether the re-read succeeded. `false` here only under the
        /// `lifecycle/refresh-failure-is-no-hold` mutant.
        read_ok: bool,
    },
    Failed,
}

impl<'a> DeleteBatch<'a> {
    /// A batch of deletes of `tenant`'s objects, judged against `lease`'s
    /// hold source. `batch` names the delete site in logs and assertions.
    pub(crate) fn new(lease: &'a dyn LeaseCheck, tenant: &TenantHash, batch: &'static str) -> Self {
        DeleteBatch {
            lease,
            tenant: *tenant,
            batch,
            observe_unsourced: false,
            state: BatchState::Unread,
        }
    }

    /// Under a lease with no source, still re-read the store's holds for the
    /// HeldObjectNeverDeleted assertion where assertions evaluate, so the
    /// assertion can catch a caller that passed no hold at all.
    pub(crate) fn observing(mut self) -> Self {
        self.observe_unsourced = true;
        self
    }

    async fn read(&mut self, store: &dyn ObjectStoreBackend) {
        if !matches!(self.state, BatchState::Unread) {
            return;
        }
        let Some(source) = self.lease.hold_source() else {
            let observed = if ravel_invariant::EVALUATES && self.observe_unsourced {
                LegalHoldCheck::refresh(store, &self.tenant).await.ok()
            } else {
                None
            };
            self.state = BatchState::Read {
                fresh: None,
                observed,
                read_ok: true,
            };
            return;
        };
        self.state = match source.current(store).await {
            Ok(fresh) => BatchState::Read {
                fresh: Some(fresh),
                observed: None,
                read_ok: true,
            },
            // Mutant lifecycle/refresh-failure-is-no-hold (ADR-4326): treat a
            // failed re-read as an empty hold set and delete.
            Err(_) if ravel_invariant::inv_mutant!("lifecycle/refresh-failure-is-no-hold") => {
                BatchState::Read {
                    fresh: Some(LegalHoldCheck::empty()),
                    observed: None,
                    read_ok: false,
                }
            }
            Err(err) => {
                source.note_reread_failure();
                tracing::error!(
                    tenant = %self.tenant.to_hex(),
                    batch = self.batch,
                    error = %err,
                    "maintenance: legal hold re-read failed before a delete batch; the batch \
                     deletes nothing and is retried on a later pass"
                );
                ravel_invariant::inv_reachable!(
                    "gc: a failed hold re-read skips a delete batch",
                    std::collections::BTreeMap::from([
                        ("tenant", self.tenant.to_hex()),
                        ("batch", self.batch.to_string()),
                    ])
                );
                BatchState::Failed
            }
        };
    }

    /// Re-read the hold set if this batch has not yet, then judge `keys`
    /// from the batch's read.
    pub(crate) async fn admit(&mut self, store: &dyn ObjectStoreBackend, keys: &[&str]) -> Admit {
        self.read(store).await;
        match &self.state {
            BatchState::Unread | BatchState::Failed => Admit::ReadFailed,
            BatchState::Read { fresh, .. } => {
                let held = match fresh {
                    Some(fresh) => keys.iter().any(|k| fresh.is_protected(k)),
                    None => keys.iter().any(|k| self.lease.is_protected(k)),
                };
                if held { Admit::Held } else { Admit::Delete }
            }
        }
    }

    /// The batch's own judgement of `key`, after an [`Self::admit`]; a batch
    /// that has not read, or whose read failed, holds everything.
    pub(crate) fn is_held(&self, key: &str) -> bool {
        match &self.state {
            BatchState::Unread | BatchState::Failed => true,
            BatchState::Read { fresh: Some(f), .. } => f.is_protected(key),
            BatchState::Read { fresh: None, .. } => self.lease.is_protected(key),
        }
    }

    /// HeldObjectNeverDeleted and RefreshFailureNeverSweeps at a delete's own
    /// step, judged from the hold set this batch observed. Call immediately
    /// before deleting `keys`, the keys the delete removes or that protect it.
    pub(crate) fn assert_delete(&self, keys: &[&str]) {
        let (observed, _read_ok) = match &self.state {
            BatchState::Read {
                fresh,
                observed,
                read_ok,
            } => (fresh.as_ref().or(observed.as_ref()), *read_ok),
            BatchState::Unread | BatchState::Failed => (None, false),
        };
        let _verdict = observed.map(|holds| !keys.iter().any(|k| holds.is_protected(k)));
        let _key = keys.first().map(|k| (*k).to_string()).unwrap_or_default();
        ravel_invariant::inv_always_if_known!(
            _verdict,
            "gc: a held object is never deleted",
            std::collections::BTreeMap::from([
                ("key", _key.clone()),
                ("tenant", self.tenant.to_hex()),
                ("batch", self.batch.to_string()),
            ])
        );
        ravel_invariant::inv_always!(
            _read_ok,
            "gc: a failed hold refresh sweeps nothing",
            std::collections::BTreeMap::from([
                ("tenant", self.tenant.to_hex()),
                ("batch", self.batch.to_string()),
                ("batch_reread_ok", _read_ok.to_string()),
                ("key", _key),
            ])
        );
    }
}

/// Fold decoded hold records to the sorted set of currently-active scopes:
/// per distinct `hold.scope`, the record with the greatest `(ts_ns, op.rank())`
/// wins, and the scope is active iff that winner is a `set` (ADR-0040 decision
/// 3, ADR-0042 decision 2).
fn fold_active_holds(records: Vec<HoldRecord>) -> Vec<String> {
    // Latest record per scope, tie-broken so a `set` wins an exact-ts tie.
    let mut latest: HashMap<String, (i64, HoldOp)> = HashMap::new();
    for rec in records {
        let candidate = (rec.ts_ns, rec.op);
        latest
            .entry(rec.scope)
            .and_modify(|current| {
                if (candidate.0, candidate.1.rank()) > (current.0, current.1.rank()) {
                    *current = candidate;
                }
            })
            .or_insert(candidate);
    }
    let mut active: Vec<String> = latest
        .into_iter()
        .filter(|(_, (_, op))| *op == HoldOp::Set)
        .map(|(scope, _)| scope)
        .collect();
    active.sort();
    active
}

/// List and decode every legal-hold record for a tenant from
/// [`Signal::Audit`] shard [`AUDIT_HOLD_SHARD`]. Non-legal-hold audit records
/// are skipped; a record marked `legal_hold` but missing a required attr is a
/// typed [`MaintainError::Invariant`] (fail loud, never a silent skip).
///
/// With a `cache`, a listed record whose decoded hold records the cache has is
/// not read again; the LIST always runs.
async fn load_hold_records(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    cache: Option<&HoldRecordCache>,
) -> Result<Vec<HoldRecord>> {
    let prefix = keys::commit_shard_prefix(tenant, Signal::Audit, AUDIT_HOLD_SHARD)?;
    let metas = list_all(store, &prefix).await?;
    let cfg = RlogConfig::default();
    let mut out = Vec::new();
    for meta in metas {
        match keys::partition_bucket_entry(&meta.key) {
            Ok(BucketEntry::CommitRecord(_)) => {}
            // A tenant's audit hold shard holds only L0 commit records and their
            // data objects; a compaction record, rewrite record, or tombstone
            // here is fine to ignore for the fold (it names the same folded
            // records). Selective-erasure rewrite records (ADR-0064) never
            // target the audit keyspace, but classifying one explicitly keeps
            // this loop from failing loud on a shape it can safely skip.
            Ok(
                BucketEntry::CompactionRecord(_)
                | BucketEntry::RewriteRecord(_)
                | BucketEntry::Tombstone(_),
            ) => continue,
            Err(KeyError::UnknownBucketEntryShape(k)) => {
                return Err(MaintainError::UnknownBucketEntry(k));
            }
            Err(e) => return Err(MaintainError::Key(e)),
        }
        if let Some(cached) = cache.and_then(|c| c.get(&meta.key)) {
            out.extend(cached.iter().cloned());
            continue;
        }
        let got = store.get(&meta.key, GetRange::Full).await?;
        let commit = record::decode(&got.data)?;
        let data_key = keys::reconstruct_data_key(&commit)?;
        let object = store.get(&data_key, GetRange::Full).await?;
        let reader = RlogReader::new(object.data.as_ref(), &cfg)?;
        let (rows, _stats) = reader.scan(&Predicate::And(Vec::new()))?;
        let mut holds = Vec::new();
        for row in &rows {
            if let Some(hold) = parse_hold_record(row)? {
                holds.push(hold);
            }
        }
        out.extend(holds.iter().cloned());
        if let Some(cache) = cache {
            cache.insert(meta.key, holds.into());
        }
    }
    Ok(out)
}

/// Decode one RLOG record into a [`HoldRecord`], or `None` if it is not a
/// legal-hold record. A record marked `legal_hold` but missing `hold.scope` or
/// carrying an unknown `hold.op` is an invariant breach.
fn parse_hold_record(row: &LogRecord) -> Result<Option<HoldRecord>> {
    // Look up the raw `kind` value, not through `str_attr`: `str_attr` returns
    // `None` both when the attr is absent and when it is present with a
    // non-string type, collapsing two cases that must stay distinct. Absent (or
    // a different string, e.g. `query` from a query-audit record) legitimately
    // means "not a legal-hold record". But a `kind` present with a non-string
    // type is a malformed record masquerading near the legal-hold keyspace;
    // treating it as "not a hold" would silently drop a real hold from the fold
    // and un-protect held data with no error anywhere, so it is an invariant
    // breach.
    match raw_attr(row, ATTR_KIND) {
        None => return Ok(None),
        Some(AttrValue::Str(s)) if s == KIND_LEGAL_HOLD => {}
        Some(AttrValue::Str(_)) => return Ok(None),
        Some(other) => {
            return Err(MaintainError::Invariant(format!(
                "legal-hold record has non-string kind attr {other:?}"
            )));
        }
    }
    let scope = str_attr(row, ATTR_SCOPE)
        .ok_or_else(|| MaintainError::Invariant("legal-hold record missing hold.scope".into()))?;
    let op = match str_attr(row, ATTR_OP) {
        Some(OP_SET) => HoldOp::Set,
        Some(OP_CLEAR) => HoldOp::Clear,
        other => {
            return Err(MaintainError::Invariant(format!(
                "legal-hold record has invalid hold.op {other:?}"
            )));
        }
    };
    Ok(Some(HoldRecord {
        scope: scope.to_string(),
        op,
        ts_ns: row.ts_ns,
    }))
}

/// The raw [`AttrValue`] of an `attrs` entry, or `None` if the key is absent.
/// Unlike [`str_attr`], this preserves the value's type so a present-but-wrong
/// type can be told apart from an absent key.
fn raw_attr<'a>(row: &'a LogRecord, key: &str) -> Option<&'a AttrValue> {
    row.attrs.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

/// The value of a string `attrs` entry, or `None` if absent or non-string.
fn str_attr<'a>(row: &'a LogRecord, key: &str) -> Option<&'a str> {
    row.attrs.iter().find(|(k, _)| k == key).and_then(|(_, v)| {
        if let AttrValue::Str(s) = v {
            Some(s.as_str())
        } else {
            None
        }
    })
}

/// Write a new immutable `set` record placing `scope` under legal hold at
/// `now_ns`. `record_id` is a caller-supplied unique object identity (a fresh
/// `Uuid`), keeping this library function free of hidden nondeterminism. Never
/// mutates any prior record: a set and its later clear are distinct objects.
pub async fn write_hold_set(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    record_id: Uuid,
    now_ns: i64,
    scope: &str,
    reason: &str,
) -> Result<()> {
    write_hold_set_with_compat(store, tenant, record_id, now_ns, scope, reason, None).await
}

/// [`write_hold_set`] under a `sys/compat` write policy (ADR-4233): the RLOG
/// version is fixed before the data PUT, every PUT attempt re-checks the
/// view's age, and the commit record is abandoned without its PUT once `P`
/// has passed since the start. `None` writes exactly as [`write_hold_set`].
pub async fn write_hold_set_with_compat(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    record_id: Uuid,
    now_ns: i64,
    scope: &str,
    reason: &str,
    compat: Option<&MaintainCompatPolicy>,
) -> Result<()> {
    // An empty scope is a prefix of every key (every match here is
    // `key.starts_with(scope)`), so it would silently hold the entire tenant.
    // Reject before writing anything.
    if scope.is_empty() {
        return Err(MaintainError::Invariant(
            "legal-hold set scope must not be empty".into(),
        ));
    }
    let mut attrs = vec![
        (
            ATTR_KIND.to_string(),
            AttrValue::Str(KIND_LEGAL_HOLD.into()),
        ),
        (ATTR_OP.to_string(), AttrValue::Str(OP_SET.into())),
        (ATTR_SCOPE.to_string(), AttrValue::Str(scope.into())),
    ];
    if !reason.is_empty() {
        attrs.push((ATTR_REASON.to_string(), AttrValue::Str(reason.into())));
    }
    let body = format!("legal hold set scope={scope}");
    write_hold_object(store, tenant, record_id, now_ns, body, attrs, compat).await?;
    ravel_invariant::inv_reachable!(
        "gc: a legal hold is placed",
        std::collections::BTreeMap::from([("scope", scope.to_string())])
    );
    Ok(())
}

/// Write a new immutable `clear` record releasing the legal hold on `scope` at
/// `now_ns`. The clear references the same `scope` string a prior set used; the
/// fold makes the later record win. Never mutates the set it supersedes.
pub async fn write_hold_clear(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    record_id: Uuid,
    now_ns: i64,
    scope: &str,
) -> Result<()> {
    write_hold_clear_with_compat(store, tenant, record_id, now_ns, scope, None).await
}

/// [`write_hold_clear`] under a `sys/compat` write policy, as
/// [`write_hold_set_with_compat`] writes a set.
pub async fn write_hold_clear_with_compat(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    record_id: Uuid,
    now_ns: i64,
    scope: &str,
    compat: Option<&MaintainCompatPolicy>,
) -> Result<()> {
    // Empty scope would match every key; reject before writing anything (see
    // `write_hold_set`).
    if scope.is_empty() {
        return Err(MaintainError::Invariant(
            "legal-hold clear scope must not be empty".into(),
        ));
    }
    let attrs = vec![
        (
            ATTR_KIND.to_string(),
            AttrValue::Str(KIND_LEGAL_HOLD.into()),
        ),
        (ATTR_OP.to_string(), AttrValue::Str(OP_CLEAR.into())),
        (ATTR_SCOPE.to_string(), AttrValue::Str(scope.into())),
    ];
    let body = format!("legal hold clear scope={scope}");
    write_hold_object(store, tenant, record_id, now_ns, body, attrs, compat).await?;
    ravel_invariant::inv_reachable!(
        "gc: a legal hold is released",
        std::collections::BTreeMap::from([("scope", scope.to_string())])
    );
    Ok(())
}

/// Encode one legal-hold RLOG record as an L0 [`Signal::Audit`] object and its
/// commit record, and PUT both. The object/commit mechanics are shared with
/// every other audit kind through [`crate::audit_write::write_audit_object_with_compat`];
/// this function supplies only the legal-hold-specific stream, severity, body,
/// and attrs.
async fn write_hold_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    record_id: Uuid,
    now_ns: i64,
    body: String,
    attrs: Vec<(String, AttrValue)>,
    compat: Option<&MaintainCompatPolicy>,
) -> Result<()> {
    let (stream_id, stream_attrs) = hold_stream();
    write_audit_object_with_compat(
        store,
        tenant,
        AuditWrite {
            shard: AUDIT_HOLD_SHARD,
            record_id,
            now_ns,
            stream_id,
            stream_attrs,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body,
            attrs,
        },
        compat.map(|policy| (policy, "legal hold")),
    )
    .await
}

/// The shared legal-hold log stream's id and canonical resource+scope blob. The
/// id is the true hash of the blob (no placeholder), so the object records real
/// stream identity.
fn hold_stream() -> (LogStreamId, Vec<u8>) {
    let resource = vec![(
        "ravel.record_type".to_string(),
        AttrValue::Str(STREAM_RECORD_TYPE.into()),
    )];
    let id = log_stream_id(&resource, STREAM_SCOPE_NAME, STREAM_SCOPE_VERSION, &[]);
    let blob = stream_attrs_bytes(&resource, STREAM_SCOPE_NAME, STREAM_SCOPE_VERSION, &[]);
    (id, blob)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn set(scope: &str, ts_ns: i64) -> HoldRecord {
        HoldRecord {
            scope: scope.to_string(),
            op: HoldOp::Set,
            ts_ns,
        }
    }
    fn clear(scope: &str, ts_ns: i64) -> HoldRecord {
        HoldRecord {
            scope: scope.to_string(),
            op: HoldOp::Clear,
            ts_ns,
        }
    }

    #[test]
    fn fold_latest_record_per_scope_wins() {
        // A later clear releases an earlier set; a later set re-holds an earlier
        // clear. Both scopes fold independently to their latest record.
        let active = fold_active_holds(vec![
            set("t/a/", 100),
            clear("t/a/", 200),
            clear("t/b/", 100),
            set("t/b/", 200),
        ]);
        assert_eq!(active, vec!["t/b/".to_string()]);
    }

    #[test]
    fn fold_tie_breaks_to_set_failsafe() {
        // set and clear at the exact same ts: the object stays protected.
        let active = fold_active_holds(vec![clear("t/a/", 100), set("t/a/", 100)]);
        assert_eq!(active, vec!["t/a/".to_string()]);
    }

    /// Build a bare [`LogRecord`] carrying only the given attrs, enough to drive
    /// [`parse_hold_record`] directly.
    fn record_with_attrs(attrs: Vec<(String, AttrValue)>) -> LogRecord {
        LogRecord {
            stream_id: LogStreamId([0u8; 16]),
            stream_attrs: Vec::new(),
            ts_ns: 42,
            observed_ts_ns: 42,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: String::new(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs,
        }
    }

    #[test]
    fn parse_kind_non_string_is_invariant_error() {
        // A `kind` attr present with a non-Str value is a malformed record near
        // the legal-hold keyspace: it must be an error, never a silent skip that
        // could drop a real hold from the fold.
        let row = record_with_attrs(vec![(ATTR_KIND.to_string(), AttrValue::I64(1))]);
        let err = parse_hold_record(&row).expect_err("non-string kind must error");
        assert!(matches!(err, MaintainError::Invariant(_)));
    }

    #[test]
    fn parse_kind_absent_or_other_string_is_none() {
        // Absent kind, and a different string value, both legitimately mean "not
        // a legal-hold record" and must fold away as Ok(None), not error.
        let absent = record_with_attrs(vec![(ATTR_OP.to_string(), AttrValue::Str(OP_SET.into()))]);
        assert!(
            parse_hold_record(&absent)
                .expect("absent kind is Ok(None)")
                .is_none()
        );

        let other = record_with_attrs(vec![(
            ATTR_KIND.to_string(),
            AttrValue::Str("query".into()),
        )]);
        assert!(
            parse_hold_record(&other)
                .expect("other-string kind is Ok(None)")
                .is_none()
        );
    }

    #[test]
    fn empty_snapshot_protects_nothing() {
        let snapshot = LegalHoldCheck::empty();
        assert!(snapshot.is_empty());
        assert!(!snapshot.is_protected("t/acme/m/l0/0007/anything"));
    }

    #[test]
    fn prefix_match_is_scope_containment() {
        let snapshot = LegalHoldCheck {
            held_prefixes: vec!["t/acme/m/l0/0007/".to_string()],
            source: None,
        };
        assert!(snapshot.is_protected("t/acme/m/l0/0007/w.10.0000000000000000001.h"));
        assert!(!snapshot.is_protected("t/acme/m/l0/0008/other"));
        assert!(!snapshot.is_protected("t/acme/l/l0/0007/other"));
    }

    /// A re-read through a [`HoldSource`] LISTs the hold shard every time but
    /// GETs a listed record's commit record and audit object only the first
    /// time this source's cache sees it: hold records are immutable.
    #[tokio::test]
    async fn a_second_re_read_lists_again_and_gets_nothing_the_cache_holds() {
        use ravel_object_store::InstrumentedStore;
        use ravel_object_store::instrument::StoreOp;
        use ravel_object_store::memory::MemoryStore;

        let store = InstrumentedStore::new(MemoryStore::new());
        let tenant = TenantHash([0x45; 16]);
        for (i, scope) in ["t/a/", "t/b/"].iter().enumerate() {
            write_hold_set(
                &store,
                &tenant,
                Uuid::from_u128(i as u128 + 1),
                1,
                scope,
                "",
            )
            .await
            .expect("set");
        }
        let source = HoldSource::new(tenant);
        let counts = |store: &InstrumentedStore<MemoryStore>| {
            let snap = store.metrics().snapshot();
            (snap.op(StoreOp::List).calls, snap.op(StoreOp::Get).calls)
        };

        let before = counts(&store);
        let first = source.current(&store).await.expect("first read");
        let after_first = counts(&store);
        assert_eq!(first.active_prefixes(), ["t/a/", "t/b/"]);
        assert_eq!(
            (after_first.0 - before.0, after_first.1 - before.1),
            (1, 4),
            "a cold read: one LIST, then a commit-record GET and an audit-object GET per record"
        );
        assert_eq!(source.cache.len(), 2);

        let second = source.current(&store).await.expect("second read");
        let after_second = counts(&store);
        assert_eq!(second.active_prefixes(), first.active_prefixes());
        assert_eq!(
            (
                after_second.0 - after_first.0,
                after_second.1 - after_first.1
            ),
            (1, 0),
            "a warm read LISTs again and serves both records from the cache"
        );

        write_hold_clear(&store, &tenant, Uuid::from_u128(3), 2, "t/a/")
            .await
            .expect("clear");
        let after_write = counts(&store);
        let third = source.current(&store).await.expect("third read");
        let after_third = counts(&store);
        assert_eq!(
            third.active_prefixes(),
            ["t/b/"],
            "the LIST is not cached: a record written since is folded in"
        );
        assert_eq!(
            (after_third.0 - after_write.0, after_third.1 - after_write.1),
            (1, 2),
            "only the new record is read"
        );
        assert_eq!(
            source.reread_failures(),
            0,
            "current() alone counts nothing"
        );
    }

    /// A delete batch reads once, before its first admission, and a failed
    /// read is counted once for the batch however many keys it then refuses;
    /// the next batch reads again.
    #[tokio::test]
    async fn a_failed_batch_re_read_is_counted_once_and_the_next_batch_reads_again() {
        use ravel_object_store::fault::{
            FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault,
        };
        use ravel_object_store::memory::MemoryStore;

        let tenant = TenantHash([0x46; 16]);
        let hold_prefix =
            keys::commit_shard_prefix(&tenant, Signal::Audit, AUDIT_HOLD_SHARD).expect("prefix");
        let store = FaultStore::new(
            MemoryStore::new(),
            FaultPlan::empty().with_rule(
                Rule::new(Op::List, ScriptedFault::Transient("hold shard down".into()))
                    .with_key_contains(hold_prefix)
                    .with_occurrence(ravel_object_store::fault::Occurrence::Nth(2)),
            ),
        );
        let snapshot = LegalHoldCheck::refresh(&store, &tenant)
            .await
            .expect("the tick's refresh, LIST 1, succeeds");
        let source = Arc::clone(snapshot.source().expect("bound to a source"));

        let mut failed = DeleteBatch::new(&snapshot, &tenant, "test batch one");
        assert_eq!(failed.admit(&store, &["t/x/1"]).await, Admit::ReadFailed);
        assert_eq!(failed.admit(&store, &["t/x/2"]).await, Admit::ReadFailed);
        assert!(failed.is_held("t/x/1"), "a failed batch holds everything");
        assert_eq!(
            store.fault_count(Op::List, FaultKind::Transient),
            1,
            "the batch's re-read, LIST 2, really faulted"
        );
        assert_eq!(source.reread_failures(), 1, "one failed batch, one count");

        let mut healthy = DeleteBatch::new(&snapshot, &tenant, "test batch two");
        assert_eq!(healthy.admit(&store, &["t/x/1"]).await, Admit::Delete);
        assert_eq!(store.fault_count(Op::List, FaultKind::Transient), 1);
        assert_eq!(
            source.reread_failures(),
            1,
            "a healthy batch counts nothing"
        );
    }

    /// A batch judges from its own re-read, not the pass snapshot: a hold
    /// written after the snapshot holds the batch's keys, and a hold cleared
    /// after the snapshot no longer does.
    #[tokio::test]
    async fn a_batch_judges_from_its_re_read_not_the_snapshot() {
        use ravel_object_store::memory::MemoryStore;

        let store = MemoryStore::new();
        let tenant = TenantHash([0x47; 16]);
        write_hold_set(&store, &tenant, Uuid::from_u128(1), 1, "t/old/", "")
            .await
            .expect("set");
        let snapshot = LegalHoldCheck::refresh(&store, &tenant)
            .await
            .expect("refresh");
        assert!(snapshot.is_protected("t/old/k"));
        write_hold_clear(&store, &tenant, Uuid::from_u128(2), 2, "t/old/")
            .await
            .expect("clear");
        write_hold_set(&store, &tenant, Uuid::from_u128(3), 2, "t/new/", "")
            .await
            .expect("set");

        let mut batch = DeleteBatch::new(&snapshot, &tenant, "test batch");
        assert_eq!(batch.admit(&store, &["t/new/k"]).await, Admit::Held);
        assert_eq!(batch.admit(&store, &["t/old/k"]).await, Admit::Delete);
    }

    /// ADR-4233 decision 3: a hold write whose commit-record version this
    /// build cannot write is refused before its data PUT, counted.
    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn a_hold_write_refuses_an_unwritable_commit_record_version_before_any_put() {
        use std::sync::Arc;

        use crate::compat_policy::CompatRunRefusal;
        use crate::compat_policy::test_support::{ManualClock, T0, family, policy, write_compat};
        use ravel_catalog::compat::{CompatRefusal, FAMILY_COMMIT_RECORD};
        use ravel_object_store::list_all;
        use ravel_object_store::memory::MemoryStore;

        let store = Arc::new(MemoryStore::new());
        write_compat(
            store.as_ref(),
            vec![family(FAMILY_COMMIT_RECORD, 2)],
            3_600_000_000_000,
        )
        .await;
        let policy = policy(store.clone(), ManualClock::at(T0)).await;
        let before = list_all(store.as_ref(), "").await.expect("list");
        let refused = crate::compat_publish_refused_total(
            FAMILY_COMMIT_RECORD,
            "active_version_not_writable",
        );

        let err = write_hold_set_with_compat(
            store.as_ref(),
            &TenantHash([5; 16]),
            Uuid::from_u128(1),
            T0,
            "t/acme/m/l0/0007/",
            "litigation",
            Some(&policy),
        )
        .await
        .expect_err("refused");

        match err {
            MaintainError::CompatRefused {
                family,
                refusal:
                    CompatRunRefusal::Compat(CompatRefusal::ActiveVersionNotWritable {
                        active: 2, ..
                    }),
                ..
            } => assert_eq!(family, FAMILY_COMMIT_RECORD),
            other => panic!("expected a commit-record refusal, got {other:?}"),
        }
        assert_eq!(
            list_all(store.as_ref(), "").await.expect("list"),
            before,
            "nothing is written"
        );
        assert_eq!(
            crate::compat_publish_refused_total(
                FAMILY_COMMIT_RECORD,
                "active_version_not_writable"
            ),
            refused + 1
        );
    }
}
