//! Legal hold: a real [`LeaseCheck`] over immutable hold records (ADR-0042
//! decision 2, issue #368).
//!
//! The sweeper consults a [`LeaseCheck`] before every physical delete. Legal
//! hold is the first non-trivial implementation of that seam: a held object is
//! never deleted, in any of the three sweep rules or the retention physical
//! sweep, until the hold that covers it is cleared.
//!
//! # The sync/async tension
//!
//! [`LeaseCheck::is_protected`] is synchronous and called once per candidate
//! key inside a sweep pass, but deciding whether a key is under a hold requires
//! reading hold records from object storage, which is async. This module
//! resolves that exactly the way [`crate::config::RetentionConfig`] resolves
//! it: state is loaded once, before the pass, into an in-memory snapshot, and
//! the per-key check reads only that snapshot. [`LegalHoldCheck::refresh`] is
//! the async load; [`LegalHoldCheck::is_protected`] never touches I/O.
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
//! shard listing. An empty snapshot (a tenant that has never set a hold)
//! protects nothing, so it behaves identically to [`crate::NoLeases`].

use std::collections::HashMap;

use ravel_commit::keys::{self, BucketEntry, KeyError};
use ravel_commit::record;
use ravel_logseg::{
    AttrValue, LogRecord, LogStreamId, Predicate, RlogConfig, RlogReader, stream_attrs_bytes,
};
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

use crate::audit_write::{AuditWrite, write_audit_object};
use crate::error::{MaintainError, Result};
use crate::sweep::LeaseCheck;

/// The [`Signal::Audit`] shard every legal-hold record for a tenant is written
/// to and read from. Legal hold is a small per-tenant control plane, so a
/// single fixed shard makes a refresh one LIST and keeps set/clear records
/// colocated for the fold; it is unrelated to the data shards a hold's scope
/// may cover.
pub const AUDIT_HOLD_SHARD: u32 = 0;

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

/// A pre-resolved, in-memory snapshot of a tenant's active legal holds,
/// implementing [`LeaseCheck`]. Built once per sweep pass by
/// [`LegalHoldCheck::refresh`]; [`LegalHoldCheck::is_protected`] consults only
/// this snapshot and never blocks on I/O.
///
/// An empty snapshot protects nothing and is byte-for-byte equivalent to
/// [`crate::NoLeases`] as a sweep gate, which is exactly what a tenant with no
/// configured holds gets.
#[derive(Debug, Clone, Default)]
pub struct LegalHoldCheck {
    /// Active held key-prefixes (each scope whose latest record is a set),
    /// sorted and deduplicated for a deterministic snapshot.
    held_prefixes: Vec<String>,
}

impl LegalHoldCheck {
    /// An empty snapshot: nothing is held. Identical in effect to
    /// [`crate::NoLeases`]. Useful as a default before the first refresh.
    pub fn empty() -> Self {
        LegalHoldCheck::default()
    }

    /// Load and fold a tenant's legal-hold records from
    /// [`Signal::Audit`] shard [`AUDIT_HOLD_SHARD`] into a fresh snapshot.
    ///
    /// This is the async load point, called once before a sweep pass begins
    /// (mirroring how [`crate::config::RetentionConfig`] is loaded once and
    /// passed in, never re-fetched per key). Lists the shard's commit records,
    /// reads each referenced audit object, decodes its RLOG records, keeps the
    /// legal-hold ones, and folds them to the set of currently-active scopes.
    pub async fn refresh(store: &dyn ObjectStoreBackend, tenant: &TenantHash) -> Result<Self> {
        let records = load_hold_records(store, tenant).await?;
        Ok(LegalHoldCheck {
            held_prefixes: fold_active_holds(records),
        })
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
async fn load_hold_records(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
) -> Result<Vec<HoldRecord>> {
    let prefix = keys::commit_shard_prefix(tenant, Signal::Audit, AUDIT_HOLD_SHARD)?;
    let metas = list_all(store, &prefix).await?;
    let cfg = RlogConfig::default();
    let mut out = Vec::new();
    for meta in metas {
        match keys::partition_bucket_entry(&meta.key) {
            Ok(BucketEntry::CommitRecord(_)) => {}
            // A tenant's audit hold shard holds only L0 commit records and their
            // data objects; a compaction record or tombstone here is fine to
            // ignore for the fold (it names the same folded records).
            Ok(BucketEntry::CompactionRecord(_) | BucketEntry::Tombstone(_)) => continue,
            Err(KeyError::UnknownBucketEntryShape(k)) => {
                return Err(MaintainError::UnknownBucketEntry(k));
            }
            Err(e) => return Err(MaintainError::Key(e)),
        }
        let got = store.get(&meta.key, GetRange::Full).await?;
        let commit = record::decode(&got.data)?;
        let data_key = keys::reconstruct_data_key(&commit)?;
        let object = store.get(&data_key, GetRange::Full).await?;
        let reader = RlogReader::new(object.data.as_ref(), &cfg)?;
        let (rows, _stats) = reader.scan(&Predicate::And(Vec::new()))?;
        for row in &rows {
            if let Some(hold) = parse_hold_record(row)? {
                out.push(hold);
            }
        }
    }
    Ok(out)
}

/// Decode one RLOG record into a [`HoldRecord`], or `None` if it is not a
/// legal-hold record. A record marked `legal_hold` but missing `hold.scope` or
/// carrying an unknown `hold.op` is an invariant breach.
fn parse_hold_record(row: &LogRecord) -> Result<Option<HoldRecord>> {
    match str_attr(row, ATTR_KIND) {
        Some(KIND_LEGAL_HOLD) => {}
        _ => return Ok(None),
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
    write_hold_object(store, tenant, record_id, now_ns, body, attrs).await
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
    let attrs = vec![
        (
            ATTR_KIND.to_string(),
            AttrValue::Str(KIND_LEGAL_HOLD.into()),
        ),
        (ATTR_OP.to_string(), AttrValue::Str(OP_CLEAR.into())),
        (ATTR_SCOPE.to_string(), AttrValue::Str(scope.into())),
    ];
    let body = format!("legal hold clear scope={scope}");
    write_hold_object(store, tenant, record_id, now_ns, body, attrs).await
}

/// Encode one legal-hold RLOG record as an L0 [`Signal::Audit`] object and its
/// commit record, and PUT both. The object/commit mechanics are shared with
/// every other audit kind through [`crate::audit_write::write_audit_object`];
/// this function supplies only the legal-hold-specific stream, severity, body,
/// and attrs.
async fn write_hold_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    record_id: Uuid,
    now_ns: i64,
    body: String,
    attrs: Vec<(String, AttrValue)>,
) -> Result<()> {
    let (stream_id, stream_attrs) = hold_stream();
    write_audit_object(
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
        };
        assert!(snapshot.is_protected("t/acme/m/l0/0007/w.10.0000000000000000001.h"));
        assert!(!snapshot.is_protected("t/acme/m/l0/0008/other"));
        assert!(!snapshot.is_protected("t/acme/l/l0/0007/other"));
    }
}
