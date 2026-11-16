//! Durable per-worker memo snapshots (ADR-0065 decision 3): the store
//! I/O half of warm start and ownership handoff.
//!
//! Each maintain-role process persists, on its discovery cadence and debounced
//! (skipped when nothing changed), a compact summary of the terminal buckets it
//! has verified to a key it alone ever writes:
//!
//! ```text
//! sys/maintain/memo/<process_id>
//! ```
//!
//! `process_id` is the same UUID the process's [`crate::worker_set::WorkerSet`]
//! heartbeats under (the ADR-0057 self-owned-snapshot convention). The write is
//! [`PutMode::Overwrite`] -- one writer per key, no CAS, no contention. On
//! startup, and whenever a membership change moves ownership of a unit to this
//! process, the worker lists this prefix and GETs the siblings' snapshots (its
//! own previous one included) modified within [`MEMO_SNAPSHOT_MTIME_SKIP_NS`],
//! up to a fixed per-reseed budget of objects and bytes ([`MemoReseedBudget`],
//! issue #3211), seeding its in-memory [`crate::scan::MaintainMemo`]
//! from the freshest non-stale entries for the units it now owns rather than
//! starting cold.
//!
//! Every snapshot carries a keyed BLAKE3 MAC under the `--maintain-key-file`
//! keys (ADR-3130, memo snapshot amendment), over a claim that includes the
//! writer's process id. The reader takes that id from the object key and
//! verifies each object before decoding any of it, so a principal holding the
//! maintain role's `sys/maintain/*` write grant but not the key file cannot seed
//! verdicts that no key holder wrote. It can replay one: put a process's own
//! earlier honest snapshot back at that process's key, reinstating a verdict
//! from before an invalidation. The staleness bound
//! ([`crate::scan::DEFAULT_MEMO_SNAPSHOT_STALENESS_NS`], 1 hour) limits a
//! replay to snapshots under an hour old, so it defers a bucket's
//! re-evaluation by at most that hour.
//!
//! The payload format and its RLE frontier/exception encoding live with the
//! memo in [`crate::scan`] ([`crate::scan::MaintainMemo::encode_snapshot`] /
//! [`crate::scan::MaintainMemo::seed_from_snapshot`]); this module only owns the
//! key layout and the two store operations. Everything here is advisory and
//! reconstructible: a lost, stale, or corrupt snapshot costs a rescan for the
//! affected units, never correctness (the ADR-0003 HEAD-pointer precedent, the
//! same the advisory compaction cursor already relies on).
//!
//! This is **not** a lease: like the heartbeat prefix beside it, the memo
//! prefix is membership/warm-start state, distinct from the GC reader-protection
//! [`crate::sweep::LeaseCheck`] gate (ADR-0065 decision 1's naming split).

use std::cmp::Reverse;
use std::collections::HashSet;

use ravel_fleet::worker_set::{MaintainRecordKeys, mtime_stale, reap_horizon_ns};
use ravel_object_store::{
    GetRange, ObjectMeta, ObjectStoreBackend, PutMode, PutOptions, StoreError, list_all,
};
use uuid::Uuid;

#[cfg(doc)]
use crate::scan::MAX_MEMO_SNAPSHOT_BYTES;
use crate::scan::{
    DEFAULT_MEMO_SNAPSHOT_STALENESS_NS, MemoSnapshotLimits, MemoSnapshotReject,
    MemoSnapshotRejects, VerifiedSnapshot,
};

/// The prefix every maintain process's memo snapshot lives under (ADR-0065
/// decision 3): a second additive control-plane prefix under `sys/maintain/`,
/// beside `sys/maintain/workers/`.
pub const MEMO_PREFIX: &str = "sys/maintain/memo/";

/// The memo snapshot key one maintain process owns and alone ever writes.
pub fn memo_key(process_id: &Uuid) -> String {
    format!("{MEMO_PREFIX}{process_id}")
}

/// Write this process's memo snapshot (`Overwrite`: single writer per key, no
/// CAS). A failed write self-corrects on the next debounced cycle; the caller
/// logs it and continues (fail-open, ADR-0065 decision 3). `bytes` is the framed
/// payload from [`crate::scan::MaintainMemo::encode_snapshot`], minted for this
/// same `process_id`: a reader verifies it against the id its key names.
pub async fn write_memo_snapshot(
    store: &dyn ObjectStoreBackend,
    process_id: &Uuid,
    bytes: Vec<u8>,
) -> Result<(), StoreError> {
    let key = memo_key(process_id);
    store
        .put(
            &key,
            bytes.into(),
            PutOptions {
                mode: PutMode::Overwrite,
                checksum: None,
            },
        )
        .await?;
    Ok(())
}

/// What one [`read_all_memo_snapshots`] call returned.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemoSnapshotRead {
    /// Every snapshot object whose MAC verified, in listing order.
    pub snapshots: Vec<VerifiedSnapshot>,
    /// Objects not returned, by the first check each one failed.
    pub rejects: MemoSnapshotRejects,
}

/// The writer's process id named by a memo snapshot key, or `None` when the
/// suffix under [`MEMO_PREFIX`] is not a UUID in the lowercase hyphenated form
/// [`memo_key`] writes. `Uuid::parse_str` also accepts the simple, braced, urn
/// and uppercase spellings, which would give one process several keys.
fn process_id_of(key: &str) -> Option<Uuid> {
    let raw = key.strip_prefix(MEMO_PREFIX)?;
    let id = Uuid::parse_str(raw).ok()?;
    (id.to_string() == raw).then_some(id)
}

/// How old a listed snapshot's modification time may be before the read skips
/// it unread: the staleness bound
/// ([`crate::scan::DEFAULT_MEMO_SNAPSHOT_STALENESS_NS`]) widened by the
/// worker registry's clock-skew margin between the object store's clock and
/// this reader's ([`reap_horizon_ns`], the window times
/// [`ravel_fleet::worker_set::REAP_WINDOW_FACTOR`]), so 2 hours.
pub const MEMO_SNAPSHOT_MTIME_SKIP_NS: i64 = reap_horizon_ns(DEFAULT_MEMO_SNAPSHOT_STALENESS_NS);

/// Most memo snapshot objects one [`read_all_memo_snapshots`] call GETs: 256.
/// Sized for a deployment of up to 64 maintain processes, each with its live
/// snapshot plus the snapshots of up to three earlier incarnations (every
/// process start takes a fresh id) still inside the 2-hour
/// [`MEMO_SNAPSHOT_MTIME_SKIP_NS`] window: 64 * 4 = 256.
pub const MAX_MEMO_RESEED_OBJECTS: usize = 256;

/// Most listed snapshot bytes one [`read_all_memo_snapshots`] call GETs: 1 GiB,
/// 32 snapshots at the [`MAX_MEMO_SNAPSHOT_BYTES`] cap. Honest snapshots that
/// average up to 4 MiB all fit beside [`MAX_MEMO_RESEED_OBJECTS`]
/// (256 * 4 MiB = 1 GiB).
pub const MAX_MEMO_RESEED_BYTES: u64 = 1024 * 1024 * 1024;

/// The per-reseed read budget of [`read_all_memo_snapshots`] (issue #3211):
/// how many snapshot objects, and how many listed bytes, one call may GET,
/// whatever number of objects sits under [`MEMO_PREFIX`]. Production uses
/// [`MemoReseedBudget::DEFAULT`]; tests inject small budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoReseedBudget {
    /// Most objects one read GETs.
    pub max_objects: usize,
    /// Most bytes one read GETs, summed over the listed sizes of the objects
    /// it reads.
    pub max_bytes: u64,
}

impl MemoReseedBudget {
    /// [`MAX_MEMO_RESEED_OBJECTS`] and [`MAX_MEMO_RESEED_BYTES`].
    pub const DEFAULT: Self = Self {
        max_objects: MAX_MEMO_RESEED_OBJECTS,
        max_bytes: MAX_MEMO_RESEED_BYTES,
    };
}

impl Default for MemoReseedBudget {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// List `sys/maintain/memo/` and GET the snapshot objects under it that the
/// checks below do not reject unread (this process's own previous snapshot
/// included), at most `budget.max_objects` of them and at most
/// `budget.max_bytes` of listed size, returning only those whose MAC
/// verifies under `keys` for the process id named by the object's key
/// (ADR-3130, memo snapshot amendment). Decoding and per-snapshot
/// staleness/ownership filtering happen at the seed site
/// ([`crate::scan::MaintainMemo::seed_from_snapshot`]), which takes only a
/// [`VerifiedSnapshot`].
///
/// Each object is checked in this order, and counted in
/// [`MemoSnapshotRead::rejects`] under the first check it fails, before any of
/// its body is decoded: a key whose suffix is not a canonical UUID
/// ([`MemoSnapshotReject::NonUuidKey`], no GET); a LIST modification time
/// older than [`MEMO_SNAPSHOT_MTIME_SKIP_NS`] before `now_ns`
/// ([`MemoSnapshotReject::Stale`], no GET); a listed size over
/// `limits.max_bytes` ([`MemoSnapshotReject::Oversize`], no GET); an empty
/// listed object ([`MemoSnapshotReject::Malformed`], no GET); outside the read
/// budget ([`MemoSnapshotReject::OverBudget`], no GET); then a range GET
/// of the cap plus one byte, whose body is
/// [`MemoSnapshotReject::Oversize`] when longer than the cap, and otherwise
/// goes through [`VerifiedSnapshot::verify`] (malformed, unknown tag, bad
/// MAC). A rejected body is dropped before the next GET, so the read holds the
/// verified snapshots plus at most one object in flight. Production passes
/// [`MemoSnapshotLimits::DEFAULT`], whose byte cap is
/// [`MAX_MEMO_SNAPSHOT_BYTES`] (issue #3199). Each reject is logged at debug
/// with its key; the caller decides what to raise.
///
/// The budget (issue #3211) makes the GETs of one call at most
/// `budget.max_objects`, a bound that does not grow with the number of objects
/// under the prefix: a principal holding the `sys/maintain/*` write grant but
/// not the key file can list any number of fresh, cap-sized objects under
/// canonical UUID keys, and without the budget each reseed GETs and MACs every
/// one before rejecting it. Objects that pass the unread checks are admitted in
/// read priority, then read in listing order: first those whose key names a
/// process in `live_set`, then the newest LIST modification time first, then
/// listing order. Each admitted object is charged its listed size; an object
/// that would take the read past `budget.max_bytes` is skipped and later,
/// smaller ones are still admitted, and admission stops at `budget.max_objects`.
/// The byte bound holds over listed sizes; a backend whose GET returns more
/// than it listed can add at most the cap plus one byte per GET.
///
/// `live_set` holds only processes whose heartbeat verified under the same key
/// file, so forged objects never displace a live writer's snapshot. The
/// snapshot of a process that left recently, which a survivor needs for the
/// handoff, has no such protection: it competes on modification time, and
/// enough fresh forged objects push it past the budget. That is the accepted
/// cost of the bound. The memo is advisory, so a skipped snapshot is a cold
/// rescan for the units it would have seeded, never wrong maintenance. With no
/// such flood, the snapshots of a few dozen live and recently departed
/// processes are all read under [`MemoReseedBudget::DEFAULT`] as long as they
/// average up to 4 MiB (256 of those fit in 1 GiB).
///
/// The modification time check keeps the objects of processes long gone (a
/// tag-1 object from before the upgrade, a snapshot MAC'd under a key since
/// rotated out) out of the other reasons, since nothing deletes them. A
/// snapshot's `snapshot_unix_ns` is stamped no later than the write that set
/// its modification time, so while the writer's clock and the store's are
/// within the 1-hour margin of each other, an object skipped this way could
/// not pass [`crate::scan::MaintainMemo::seed_from_snapshot`]'s staleness gate.
/// `now_ns` is the caller's injected clock reading.
///
/// Only a failed LIST or GET is an `Err`, which the caller treats fail-open
/// (cold start for the affected units), never freezing the loop on a transient
/// read fault.
pub async fn read_all_memo_snapshots(
    store: &dyn ObjectStoreBackend,
    keys: &MaintainRecordKeys,
    limits: MemoSnapshotLimits,
    budget: MemoReseedBudget,
    live_set: &[Uuid],
    now_ns: i64,
) -> Result<MemoSnapshotRead, StoreError> {
    let cap = limits.max_bytes;
    let objects = list_all(store, MEMO_PREFIX).await?;
    let mut read = MemoSnapshotRead::default();
    let mut candidates = Vec::new();
    for meta in &objects {
        match check_listed(cap, now_ns, meta) {
            Ok(process_id) => candidates.push((process_id, meta)),
            Err((reason, size)) => reject(&mut read, meta, reason, size, cap),
        }
    }
    let admitted = admit(&candidates, budget, live_set);
    for ((process_id, meta), admitted) in candidates.into_iter().zip(admitted) {
        let checked = if admitted {
            fetch_and_verify(store, keys, cap, meta, process_id).await?
        } else {
            Err((MemoSnapshotReject::OverBudget, meta.size))
        };
        match checked {
            Ok(snapshot) => read.snapshots.push(snapshot),
            Err((reason, size)) => reject(&mut read, meta, reason, size, cap),
        }
    }
    Ok(read)
}

fn reject(
    read: &mut MemoSnapshotRead,
    meta: &ObjectMeta,
    reason: MemoSnapshotReject,
    size: u64,
    cap: u64,
) {
    tracing::debug!(
        key = %meta.key,
        size,
        cap,
        reason = reason.reason(),
        "maintenance: rejected a memo snapshot object"
    );
    read.rejects.record(reason);
}

/// Which of `candidates` (in listing order) [`read_all_memo_snapshots`] GETs
/// under `budget`, chosen in the read priority its doc describes.
fn admit(
    candidates: &[(Uuid, &ObjectMeta)],
    budget: MemoReseedBudget,
    live_set: &[Uuid],
) -> Vec<bool> {
    let live: HashSet<&Uuid> = live_set.iter().collect();
    let mut order: Vec<usize> = (0..candidates.len()).collect();
    order.sort_by_key(|&index| {
        let (process_id, meta) = candidates[index];
        (
            !live.contains(&process_id),
            Reverse(meta.last_modified_unix_ms),
            index,
        )
    });
    let mut admitted = vec![false; candidates.len()];
    let (mut objects, mut bytes) = (0usize, 0u64);
    for index in order {
        if objects >= budget.max_objects {
            break;
        }
        let size = candidates[index].1.size;
        let Some(after) = bytes.checked_add(size).filter(|&b| b <= budget.max_bytes) else {
            continue;
        };
        admitted[index] = true;
        objects += 1;
        bytes = after;
    }
    admitted
}

/// The checks of [`read_all_memo_snapshots`] decided from the listing alone:
/// the writer's process id, or the reject reason with the listed size.
fn check_listed(
    cap: u64,
    now_ns: i64,
    meta: &ObjectMeta,
) -> Result<Uuid, (MemoSnapshotReject, u64)> {
    let Some(process_id) = process_id_of(&meta.key) else {
        return Err((MemoSnapshotReject::NonUuidKey, meta.size));
    };
    // A backend that reports no modification time (`<= 0`, as MemoryStore's
    // default clock does) is never skipped here: the object falls through to
    // the checks below and the seed-time staleness gate.
    if mtime_stale(
        now_ns,
        meta.last_modified_unix_ms,
        MEMO_SNAPSHOT_MTIME_SKIP_NS,
    ) {
        return Err((MemoSnapshotReject::Stale, meta.size));
    }
    if meta.size > cap {
        return Err((MemoSnapshotReject::Oversize, meta.size));
    }
    if meta.size == 0 {
        return Err((MemoSnapshotReject::Malformed, 0));
    }
    Ok(process_id)
}

/// GET one admitted object of [`read_all_memo_snapshots`]: the verified
/// snapshot, or the reject reason with the size it was judged at.
async fn fetch_and_verify(
    store: &dyn ObjectStoreBackend,
    keys: &MaintainRecordKeys,
    cap: u64,
    meta: &ObjectMeta,
    process_id: Uuid,
) -> Result<Result<VerifiedSnapshot, (MemoSnapshotReject, u64)>, StoreError> {
    let got = match store
        .get(&meta.key, GetRange::Range(0, cap.saturating_add(1)))
        .await
    {
        Ok(got) => got,
        // The object shrank to nothing between the LIST and the GET.
        Err(StoreError::InvalidRange(_)) => {
            return Ok(Err((MemoSnapshotReject::Malformed, 0)));
        }
        Err(err) => return Err(err),
    };
    let len = u64::try_from(got.data.len()).unwrap_or(u64::MAX);
    if len > cap {
        return Ok(Err((MemoSnapshotReject::Oversize, len)));
    }
    Ok(VerifiedSnapshot::verify(process_id, got.data, keys).map_err(|reason| (reason, len)))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::scan::{
        MAX_MEMO_SNAPSHOT_BYTES, MEMO_SNAPSHOT_MAC_BYTES, MIN_MEMO_SNAPSHOT_FRAME_BYTES,
        MaintainMemo,
    };
    use ravel_object_store::instrument::InstrumentedStore;
    use ravel_object_store::memory::MemoryStore;

    const KEY_1: [u8; 32] = [0x4D; 32];
    const KEY_2: [u8; 32] = [0x2B; 32];

    /// The reader's clock in the read tests: 100 hours past the epoch, so a
    /// modification time past [`MEMO_SNAPSHOT_MTIME_SKIP_NS`] is positive.
    const NOW_NS: i64 = 100 * 3_600_000_000_000;

    fn keys() -> MaintainRecordKeys {
        MaintainRecordKeys::from_file_keys(&[KEY_1]).expect("one key")
    }

    /// An honest frame of `body` written by `id` under [`keys`].
    fn frame(id: Uuid, body: &[u8], snapshot_unix_ns: i64) -> Vec<u8> {
        MaintainMemo::snapshot_bytes_from_body(body, snapshot_unix_ns, &id, &keys())
    }

    /// The body of an empty memo: a zero unit count.
    fn empty_body() -> Vec<u8> {
        MaintainMemo::with_default_interval().snapshot_body()
    }

    fn verified(id: Uuid, bytes: &[u8]) -> VerifiedSnapshot {
        VerifiedSnapshot::verify(id, bytes.to_vec(), &keys()).expect("honest frame verifies")
    }

    fn only(reason: MemoSnapshotReject) -> MemoSnapshotRejects {
        let mut rejects = MemoSnapshotRejects::default();
        rejects.record(reason);
        rejects
    }

    async fn read_default(store: &dyn ObjectStoreBackend) -> MemoSnapshotRead {
        read_all_memo_snapshots(
            store,
            &keys(),
            MemoSnapshotLimits::DEFAULT,
            MemoReseedBudget::DEFAULT,
            &[],
            NOW_NS,
        )
        .await
        .expect("read")
    }

    /// [`read_all_memo_snapshots`] under `limits` and the default budget, with
    /// no live writer.
    async fn read_limited(
        store: &dyn ObjectStoreBackend,
        limits: MemoSnapshotLimits,
        now_ns: i64,
    ) -> MemoSnapshotRead {
        read_all_memo_snapshots(
            store,
            &keys(),
            limits,
            MemoReseedBudget::DEFAULT,
            &[],
            now_ns,
        )
        .await
        .expect("read")
    }

    #[test]
    fn memo_key_is_under_the_memo_prefix() {
        let id = Uuid::from_u128(42);
        let key = memo_key(&id);
        assert!(key.starts_with(MEMO_PREFIX));
        assert!(key.ends_with(&id.to_string()));
    }

    /// deploy/iam/maintain.json's MaintainList `sys/maintain/memo/*` prefix and
    /// MaintainRead/MaintainWrite `sys/maintain/*` grants depend on this key;
    /// `maintain_template_covers_every_memo_snapshot_read_call` in
    /// crates/ravel-commit/tests/iam_templates.rs copies it by hand.
    #[test]
    fn memo_key_matches_the_iam_template_witness() {
        assert_eq!(MEMO_PREFIX, "sys/maintain/memo/");
        assert_eq!(
            memo_key(&Uuid::from_u128(7)),
            "sys/maintain/memo/00000000-0000-0000-0000-000000000007"
        );
    }

    /// The frame is `[tag 2][snapshot_unix_ns LE][body][mac 32]`, and the MAC
    /// is the memo key's keyed hash of `tag || process_id || ts || body`.
    #[test]
    fn frame_layout_and_claim_are_exact() {
        let id = Uuid::from_u128(0x0102_0304);
        let body = [9u8, 8, 7];
        let bytes = frame(id, &body, 0x1122_3344);
        assert_eq!(bytes.len(), 1 + 8 + body.len() + MEMO_SNAPSHOT_MAC_BYTES);
        assert_eq!(bytes[0], 2);
        assert_eq!(&bytes[1..9], &0x1122_3344i64.to_le_bytes());
        assert_eq!(&bytes[9..12], &body);
        let mut claim = vec![2u8];
        claim.extend_from_slice(id.as_bytes());
        claim.extend_from_slice(&0x1122_3344i64.to_le_bytes());
        claim.extend_from_slice(&body);
        assert_eq!(&bytes[12..], &keys().memo_mac(&claim));
        assert_eq!(MIN_MEMO_SNAPSHOT_FRAME_BYTES, 41);
    }

    /// A written snapshot round-trips back through a list+get as the verified
    /// frame of the process whose key it sits under.
    #[tokio::test]
    async fn write_then_read_all_returns_the_verified_snapshot() {
        let store = MemoryStore::new();
        let id = Uuid::from_u128(7);
        let payload = frame(id, &empty_body(), 5);
        write_memo_snapshot(&store, &id, payload.clone())
            .await
            .expect("write");

        let read = read_default(&store).await;
        assert_eq!(read.snapshots, vec![verified(id, &payload)]);
        assert_eq!(read.snapshots[0].process_id(), id);
        assert_eq!(read.rejects.total(), 0);
    }

    /// `read_all_memo_snapshots` returns every sibling's object, not just one.
    #[tokio::test]
    async fn read_all_returns_every_siblings_snapshot() {
        let store = MemoryStore::new();
        for id in [Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)] {
            write_memo_snapshot(&store, &id, frame(id, &empty_body(), 1))
                .await
                .expect("write");
        }
        let read = read_default(&store).await;
        assert_eq!(read.snapshots.len(), 3);
        assert_eq!(read.rejects.total(), 0);
    }

    /// `write_memo_snapshot` uses `PutMode::Overwrite`, never a conditional
    /// mode: a second write to the same process's key with different bytes
    /// succeeds unconditionally (a `CreateIfAbsent` would refuse it outright,
    /// a `CasVersion` would need the first write's version), and the stored
    /// content becomes the second payload, not the first.
    #[tokio::test]
    async fn memo_snapshot_write_uses_overwrite_not_cas() {
        let store = MemoryStore::new();
        let id = Uuid::from_u128(99);
        let first = frame(id, &empty_body(), 1);
        let second = frame(id, &empty_body(), 2);

        write_memo_snapshot(&store, &id, first)
            .await
            .expect("first write");
        write_memo_snapshot(&store, &id, second.clone())
            .await
            .expect("a second write to the same key succeeds unconditionally under Overwrite");

        assert_eq!(
            read_default(&store).await.snapshots,
            vec![verified(id, &second)],
            "the second write's bytes are what's stored"
        );
    }

    /// Byte limits small enough that a test object over the cap is a few
    /// hundred bytes.
    const SMALL: MemoSnapshotLimits = MemoSnapshotLimits {
        max_bytes: 256,
        max_entries: 16,
    };

    /// A frame of exactly `len` bytes, MAC included, written by `id`.
    fn frame_of_len(id: Uuid, len: usize) -> Vec<u8> {
        let bytes = frame(id, &vec![0xA5u8; len - MIN_MEMO_SNAPSHOT_FRAME_BYTES], 3);
        assert_eq!(bytes.len(), len);
        bytes
    }

    /// An object one byte over the byte cap, MAC included, is rejected and
    /// counted without a GET, while an object of exactly the cap and a small
    /// sibling are both returned; the oversized object fails nothing.
    #[tokio::test]
    async fn read_all_skips_an_object_over_the_size_cap() {
        let store = InstrumentedStore::new(MemoryStore::new());
        let cap = usize::try_from(SMALL.max_bytes).expect("cap fits usize");
        let (over_id, at_id, small_id) =
            (Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3));
        let at_cap = frame_of_len(at_id, cap);
        let small = frame(small_id, &empty_body(), 3);
        write_memo_snapshot(&store, &over_id, frame_of_len(over_id, cap + 1))
            .await
            .expect("write oversized");
        write_memo_snapshot(&store, &at_id, at_cap.clone())
            .await
            .expect("write at cap");
        write_memo_snapshot(&store, &small_id, small.clone())
            .await
            .expect("write small");

        let before = store.metrics().snapshot();
        let read = read_limited(&store, SMALL, NOW_NS).await;
        let after = store.metrics().snapshot();

        assert_eq!(read.rejects, only(MemoSnapshotReject::Oversize));
        assert_eq!(
            read.snapshots,
            vec![verified(at_id, &at_cap), verified(small_id, &small)]
        );
        assert_eq!(
            after.get.calls - before.get.calls,
            2,
            "the oversized object is skipped from its listed size, unread"
        );
    }

    /// An empty object is rejected as malformed without a GET, not turned into
    /// a read error by the range GET.
    #[tokio::test]
    async fn read_all_rejects_an_empty_object_as_malformed() {
        let store = MemoryStore::new();
        write_memo_snapshot(&store, &Uuid::from_u128(1), Vec::new())
            .await
            .expect("write empty");
        let read = read_default(&store).await;
        assert!(read.snapshots.is_empty());
        assert_eq!(read.rejects, only(MemoSnapshotReject::Malformed));
    }

    /// Delegates to a [`MemoryStore`] but lists every object at no more than
    /// `listed_size`, as a store whose LIST and GET disagree would.
    struct UnderListingStore {
        inner: MemoryStore,
        listed_size: u64,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for UnderListingStore {
        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }
        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            self.inner.get(key, range).await
        }
        async fn head(&self, key: &str) -> Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }
        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            let mut listed = self.inner.list(prefix, page).await?;
            for meta in &mut listed.objects {
                meta.size = meta.size.min(self.listed_size);
            }
            Ok(listed)
        }
        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }
        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// An object listed at the byte cap whose body is longer is still rejected
    /// as oversize: the range GET asks for one byte past the cap, and the
    /// returned length is checked against the cap. A frame of exactly the cap
    /// listed the same way is returned.
    #[tokio::test]
    async fn read_all_skips_a_body_over_the_cap_listed_under_it() {
        let cap = usize::try_from(SMALL.max_bytes).expect("cap fits usize");
        let store = UnderListingStore {
            inner: MemoryStore::new(),
            listed_size: SMALL.max_bytes,
        };
        let (over_id, at_id) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let at_cap = frame_of_len(at_id, cap);
        write_memo_snapshot(&store, &over_id, frame_of_len(over_id, cap + 40))
            .await
            .expect("write oversized");
        write_memo_snapshot(&store, &at_id, at_cap.clone())
            .await
            .expect("write at cap");

        let read = read_limited(&store, SMALL, NOW_NS).await;
        assert_eq!(read.rejects, only(MemoSnapshotReject::Oversize));
        assert_eq!(read.snapshots, vec![verified(at_id, &at_cap)]);
    }

    /// An empty prefix reads back as an empty set, not an error.
    #[tokio::test]
    async fn read_all_over_empty_prefix_is_empty() {
        let read = read_default(&MemoryStore::new()).await;
        assert!(read.snapshots.is_empty());
        assert_eq!(read.rejects.total(), 0);
    }

    /// A key under the prefix whose suffix is not a UUID is rejected without a
    /// GET, whatever it holds.
    #[tokio::test]
    async fn non_uuid_key_is_rejected_without_a_get() {
        let store = InstrumentedStore::new(MemoryStore::new());
        let id = Uuid::from_u128(5);
        store
            .put(
                &format!("{MEMO_PREFIX}not-a-uuid"),
                frame(id, &empty_body(), 1).into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("put");
        let before = store.metrics().snapshot();
        let read = read_default(&store).await;
        let after = store.metrics().snapshot();
        assert!(read.snapshots.is_empty());
        assert_eq!(read.rejects, only(MemoSnapshotReject::NonUuidKey));
        assert_eq!(after.get.calls - before.get.calls, 0);
    }

    /// A valid process id under its braced, simple, urn and uppercase
    /// spellings is rejected as a non-UUID key without a GET, so one process
    /// has one key; the lowercase hyphenated spelling [`memo_key`] writes is
    /// read and verifies. The store refuses a braced key at PUT, so that
    /// spelling is checked on the key parser alone.
    #[tokio::test]
    async fn non_canonical_uuid_spellings_are_rejected_without_a_get() {
        let id = Uuid::from_u128(0xABCD_EF01_2345_6789_ABCD_EF01_2345_6789);
        let bytes = frame(id, &empty_body(), 1);
        let braced = id.braced().to_string();
        assert_eq!(Uuid::parse_str(&braced).expect("parses"), id);
        assert_eq!(process_id_of(&format!("{MEMO_PREFIX}{braced}")), None);
        assert_eq!(process_id_of(&memo_key(&id)), Some(id));
        let spellings = [
            id.simple().to_string(),
            id.urn().to_string(),
            id.to_string().to_uppercase(),
        ];
        for spelling in &spellings {
            assert_eq!(Uuid::parse_str(spelling).expect("parses"), id);
            let store = InstrumentedStore::new(MemoryStore::new());
            store
                .put(
                    &format!("{MEMO_PREFIX}{spelling}"),
                    bytes.clone().into(),
                    PutOptions {
                        mode: PutMode::Overwrite,
                        checksum: None,
                    },
                )
                .await
                .expect("put");
            let read = read_default(&store).await;
            assert!(read.snapshots.is_empty(), "{spelling}");
            assert_eq!(read.rejects, only(MemoSnapshotReject::NonUuidKey));
            assert_eq!(store.metrics().snapshot().get.calls, 0, "{spelling}");
        }

        let store = MemoryStore::new();
        write_memo_snapshot(&store, &id, bytes.clone())
            .await
            .expect("write");
        assert_eq!(
            read_default(&store).await.snapshots,
            vec![verified(id, &bytes)]
        );
    }

    /// The object store clock as milliseconds for a reader clock `ns`.
    fn ms(ns: i64) -> u64 {
        u64::try_from(ns / 1_000_000).expect("positive")
    }

    /// Write the two objects a process long gone leaves behind (a tag-1
    /// frame from before the MAC, and a frame MAC'd under a key since rotated
    /// out) with the store clock at `mtime_ms`.
    async fn write_leftovers(store: &InstrumentedStore<MemoryStore>, mtime_ms: u64) {
        store.inner().set_clock_ms(mtime_ms);
        let (old, rotated) = (Uuid::from_u128(21), Uuid::from_u128(22));
        let mut tag_one = vec![1u8];
        tag_one.extend_from_slice(&1i64.to_le_bytes());
        tag_one.extend_from_slice(&[0u8; 40]);
        write_memo_snapshot(store, &old, tag_one)
            .await
            .expect("write");
        let other = MaintainRecordKeys::from_file_keys(&[KEY_2]).expect("key");
        let bytes = MaintainMemo::snapshot_bytes_from_body(&empty_body(), 1, &rotated, &other);
        write_memo_snapshot(store, &rotated, bytes)
            .await
            .expect("write");
    }

    /// Objects whose LIST modification time is one millisecond past
    /// [`MEMO_SNAPSHOT_MTIME_SKIP_NS`] are counted as stale without a GET; the
    /// same objects with a modification time exactly at the bound are read
    /// and count as an unknown tag and a bad MAC.
    #[tokio::test]
    async fn objects_past_the_mtime_bound_are_stale_without_a_get() {
        assert_eq!(MEMO_SNAPSHOT_MTIME_SKIP_NS, 2 * 3_600_000_000_000);
        let at_bound = ms(NOW_NS - MEMO_SNAPSHOT_MTIME_SKIP_NS);

        let old = InstrumentedStore::new(MemoryStore::new());
        write_leftovers(&old, at_bound - 1).await;
        let read = read_default(&old).await;
        assert!(read.snapshots.is_empty());
        assert_eq!(read.rejects.get(MemoSnapshotReject::Stale), 2);
        assert_eq!(read.rejects.total(), 2);
        assert_eq!(old.metrics().snapshot().get.calls, 0);

        let fresh = InstrumentedStore::new(MemoryStore::new());
        write_leftovers(&fresh, at_bound).await;
        let read = read_default(&fresh).await;
        assert!(read.snapshots.is_empty());
        assert_eq!(read.rejects.get(MemoSnapshotReject::UnknownTag), 1);
        assert_eq!(read.rejects.get(MemoSnapshotReject::BadMac), 1);
        assert_eq!(read.rejects.total(), 2);
        assert_eq!(fresh.metrics().snapshot().get.calls, 2);
    }

    /// A backend that reports no modification time (MemoryStore's default
    /// clock lists 0) is never skipped as stale, however far the reader's
    /// clock is past the bound: its objects reach the GET and the MAC check.
    #[tokio::test]
    async fn a_missing_mtime_is_not_stale() {
        let store = InstrumentedStore::new(MemoryStore::new());
        write_leftovers(&store, 0).await;
        let read = read_limited(&store, MemoSnapshotLimits::DEFAULT, i64::MAX).await;
        assert_eq!(read.rejects.get(MemoSnapshotReject::Stale), 0);
        assert_eq!(read.rejects.get(MemoSnapshotReject::UnknownTag), 1);
        assert_eq!(read.rejects.get(MemoSnapshotReject::BadMac), 1);
        assert_eq!(store.metrics().snapshot().get.calls, 2);
    }

    /// The bytes a writer without the key file can produce: a tag-2 frame
    /// whose MAC was minted under a different file key, and a tag-1 frame of
    /// the format before the MAC (`[1][ts][body]`). Each is rejected for its
    /// own reason, exactly once.
    #[tokio::test]
    async fn frames_from_a_writer_without_the_key_are_rejected() {
        let id = Uuid::from_u128(11);
        let other = MaintainRecordKeys::from_file_keys(&[KEY_2]).expect("key");

        let store = MemoryStore::new();
        let forged = MaintainMemo::snapshot_bytes_from_body(&empty_body(), 1, &id, &other);
        write_memo_snapshot(&store, &id, forged)
            .await
            .expect("write");
        let read = read_default(&store).await;
        assert!(read.snapshots.is_empty());
        assert_eq!(read.rejects, only(MemoSnapshotReject::BadMac));

        let store = MemoryStore::new();
        let mut tag_one = vec![1u8];
        tag_one.extend_from_slice(&1i64.to_le_bytes());
        tag_one.extend_from_slice(&[0u8; 40]);
        write_memo_snapshot(&store, &id, tag_one)
            .await
            .expect("write");
        let read = read_default(&store).await;
        assert!(read.snapshots.is_empty());
        assert_eq!(read.rejects, only(MemoSnapshotReject::UnknownTag));
    }

    /// A snapshot copied verbatim from process A's key to process B's key does
    /// not verify: the claim carries the process id the reader took from the
    /// key, so A's MAC is not B's. A's own copy still verifies beside it.
    #[tokio::test]
    async fn snapshot_copied_to_another_process_key_is_bad_mac() {
        let (a, b) = (Uuid::from_u128(0xA), Uuid::from_u128(0xB));
        let bytes = frame(a, &empty_body(), 1);
        let store = MemoryStore::new();
        write_memo_snapshot(&store, &a, bytes.clone())
            .await
            .expect("write a");
        write_memo_snapshot(&store, &b, bytes.clone())
            .await
            .expect("copy to b");
        let read = read_default(&store).await;
        assert_eq!(read.snapshots, vec![verified(a, &bytes)]);
        assert_eq!(read.rejects, only(MemoSnapshotReject::BadMac));
        assert_eq!(
            VerifiedSnapshot::verify(b, bytes, &keys()),
            Err(MemoSnapshotReject::BadMac)
        );
    }

    /// Every field of the frame is covered: changing the tag is an unknown
    /// tag, and changing one timestamp byte, one body byte or one MAC byte is
    /// a bad MAC.
    #[test]
    fn each_tampered_field_is_rejected() {
        let id = Uuid::from_u128(3);
        let honest = frame(id, &[1, 2, 3, 4, 5], 77);
        assert!(VerifiedSnapshot::verify(id, honest.clone(), &keys()).is_ok());
        let tampered = |at: usize| {
            let mut bytes = honest.clone();
            bytes[at] ^= 0x01;
            VerifiedSnapshot::verify(id, bytes, &keys())
        };
        assert_eq!(tampered(0), Err(MemoSnapshotReject::UnknownTag), "tag");
        assert_eq!(tampered(4), Err(MemoSnapshotReject::BadMac), "timestamp");
        assert_eq!(tampered(11), Err(MemoSnapshotReject::BadMac), "body");
        assert_eq!(
            tampered(honest.len() - 1),
            Err(MemoSnapshotReject::BadMac),
            "mac"
        );
    }

    /// A frame cut short inside its MAC is malformed once it is below the
    /// 41-byte minimum (here an empty-memo frame of 45 bytes cut to 40), and a
    /// bad MAC above it, never decoded either way.
    #[test]
    fn truncated_mac_is_malformed() {
        let id = Uuid::from_u128(3);
        let honest = frame(id, &empty_body(), 77);
        assert_eq!(honest.len(), 45);
        assert_eq!(
            VerifiedSnapshot::verify(id, honest[..40].to_vec(), &keys()),
            Err(MemoSnapshotReject::Malformed)
        );
        assert_eq!(
            VerifiedSnapshot::verify(id, honest[..44].to_vec(), &keys()),
            Err(MemoSnapshotReject::BadMac)
        );
        assert_eq!(
            VerifiedSnapshot::verify(id, Vec::new(), &keys()),
            Err(MemoSnapshotReject::Malformed)
        );
    }

    /// A snapshot minted by a writer whose first key is key 2 verifies for a
    /// reader holding [key 1, key 2], and a writer holding [key 1, key 2]
    /// mints under key 1: a reader holding only key 1 verifies it and a reader
    /// holding only key 2 does not.
    #[test]
    fn second_key_verifies_and_the_first_key_mints() {
        let id = Uuid::from_u128(8);
        let only_2 = MaintainRecordKeys::from_file_keys(&[KEY_2]).expect("key");
        let both = MaintainRecordKeys::from_file_keys(&[KEY_1, KEY_2]).expect("keys");
        let only_1 = keys();

        let by_2 = MaintainMemo::snapshot_bytes_from_body(&empty_body(), 1, &id, &only_2);
        assert!(VerifiedSnapshot::verify(id, by_2.clone(), &both).is_ok());
        assert_eq!(
            VerifiedSnapshot::verify(id, by_2, &only_1),
            Err(MemoSnapshotReject::BadMac)
        );

        let by_both = MaintainMemo::snapshot_bytes_from_body(&empty_body(), 1, &id, &both);
        assert!(VerifiedSnapshot::verify(id, by_both.clone(), &only_1).is_ok());
        assert_eq!(
            VerifiedSnapshot::verify(id, by_both, &only_2),
            Err(MemoSnapshotReject::BadMac)
        );
    }

    /// Fifty forged objects (each a well-formed snapshot body under a foreign
    /// key) produce zero snapshots and fifty bad-MAC rejects, and the decoder
    /// runs zero times on this thread: verification precedes any decode. The
    /// control: seeding from one honest snapshot moves the same counter by
    /// exactly one.
    #[tokio::test]
    async fn forged_objects_are_rejected_and_never_decoded() {
        const N: u128 = 50;
        let other = MaintainRecordKeys::from_file_keys(&[KEY_2]).expect("key");
        let store = MemoryStore::new();
        for i in 0..N {
            let id = Uuid::from_u128(1000 + i);
            let forged = MaintainMemo::snapshot_bytes_from_body(&empty_body(), 1, &id, &other);
            write_memo_snapshot(&store, &id, forged)
                .await
                .expect("write");
        }
        let before = crate::scan::decode_calls::get();
        let read = read_default(&store).await;
        assert!(read.snapshots.is_empty());
        assert_eq!(read.rejects.get(MemoSnapshotReject::BadMac), 50);
        assert_eq!(read.rejects.total(), 50);
        assert_eq!(crate::scan::decode_calls::get() - before, 0);

        let honest_id = Uuid::from_u128(1);
        write_memo_snapshot(&store, &honest_id, frame(honest_id, &empty_body(), 1))
            .await
            .expect("write");
        let read = read_default(&store).await;
        assert_eq!(read.snapshots.len(), 1);
        assert_eq!(crate::scan::decode_calls::get() - before, 0);
        let stats = MaintainMemo::with_default_interval()
            .seed_from_snapshot(
                &read.snapshots[0],
                MemoSnapshotLimits::DEFAULT,
                1,
                crate::scan::DEFAULT_MEMO_SNAPSHOT_STALENESS_NS,
                |_, _, _| true,
            )
            .expect("decode");
        assert!(!stats.stale);
        assert_eq!(crate::scan::decode_calls::get() - before, 1);
    }

    /// The default budget is the derivation its doc gives: 64 processes times
    /// four incarnations, and 32 snapshots at the byte cap, which is also 256
    /// snapshots of 4 MiB.
    #[test]
    fn default_reseed_budget_matches_its_derivation() {
        assert_eq!(MAX_MEMO_RESEED_OBJECTS, 64 * 4);
        assert_eq!(MAX_MEMO_RESEED_BYTES, 32 * MAX_MEMO_SNAPSHOT_BYTES);
        assert_eq!(MAX_MEMO_RESEED_BYTES, 256 * 4 * 1024 * 1024);
        assert_eq!(
            MemoReseedBudget::default(),
            MemoReseedBudget {
                max_objects: MAX_MEMO_RESEED_OBJECTS,
                max_bytes: MAX_MEMO_RESEED_BYTES,
            }
        );
    }

    /// The store clock, in milliseconds, at the reader's [`NOW_NS`].
    const NOW_MS: u64 = (NOW_NS / 1_000_000) as u64;
    const HOUR_MS: u64 = 3_600_000;

    /// Write `n` objects a principal without the key file can produce: a
    /// tag-2 frame of exactly the [`SMALL`] byte cap, MAC'd under another
    /// key, under fresh canonical UUID keys, with the store clock at
    /// `mtime_ms`.
    async fn plant_forged(store: &InstrumentedStore<MemoryStore>, n: u128, mtime_ms: u64) {
        let cap = usize::try_from(SMALL.max_bytes).expect("cap fits usize");
        let other = MaintainRecordKeys::from_file_keys(&[KEY_2]).expect("key");
        store.inner().set_clock_ms(mtime_ms);
        for i in 0..n {
            let id = Uuid::from_u128(1000 + i);
            let body = vec![0xA5u8; cap - MIN_MEMO_SNAPSHOT_FRAME_BYTES];
            let forged = MaintainMemo::snapshot_bytes_from_body(&body, 3, &id, &other);
            assert_eq!(forged.len(), cap);
            write_memo_snapshot(store, &id, forged)
                .await
                .expect("write");
        }
    }

    /// Write `id`'s honest 45-byte snapshot with the store clock at
    /// `mtime_ms`, returning its bytes.
    async fn plant_honest(
        store: &InstrumentedStore<MemoryStore>,
        id: Uuid,
        mtime_ms: u64,
    ) -> Vec<u8> {
        let bytes = frame(id, &empty_body(), 3);
        assert_eq!(bytes.len(), 45);
        store.inner().set_clock_ms(mtime_ms);
        write_memo_snapshot(store, &id, bytes.clone())
            .await
            .expect("write");
        bytes
    }

    fn rejects_of(counts: &[(MemoSnapshotReject, usize)]) -> MemoSnapshotRejects {
        let mut rejects = MemoSnapshotRejects::default();
        for &(reason, count) in counts {
            for _ in 0..count {
                rejects.record(reason);
            }
        }
        rejects
    }

    /// Read `store` under [`SMALL`] and `budget` with `live_set`, returning
    /// the read and the GETs and GET bytes it issued.
    async fn read_counting(
        store: &InstrumentedStore<MemoryStore>,
        budget: MemoReseedBudget,
        live_set: &[Uuid],
    ) -> (MemoSnapshotRead, u64, u64) {
        let before = store.metrics().snapshot().get;
        let read = read_all_memo_snapshots(store, &keys(), SMALL, budget, live_set, NOW_NS)
            .await
            .expect("read");
        let after = store.metrics().snapshot().get;
        (read, after.calls - before.calls, after.bytes - before.bytes)
    }

    /// Issue #3211: with an object budget of 3, a read over 4 forged objects
    /// and one over 64 each issue exactly 3 GETs (557 bytes: the 45-byte live
    /// snapshot and two 256-byte forgeries), and the rest are counted as over
    /// budget, unread. The live writer's snapshot is read and verified in both
    /// although its key lists last and its modification time is an hour older
    /// than every forgery's: a live-set writer is read first.
    #[tokio::test]
    async fn reseed_gets_are_bounded_by_the_object_budget() {
        let budget = MemoReseedBudget {
            max_objects: 3,
            max_bytes: u64::MAX,
        };
        let live = Uuid::from_u128(u128::MAX);
        for n in [4u128, 64] {
            let store = InstrumentedStore::new(MemoryStore::new());
            plant_forged(&store, n, NOW_MS).await;
            let honest = plant_honest(&store, live, NOW_MS - HOUR_MS).await;

            let (read, calls, bytes) = read_counting(&store, budget, &[live]).await;
            assert_eq!((calls, bytes), (3, 557), "GETs and GET bytes, n={n}");
            assert_eq!(read.snapshots, vec![verified(live, &honest)], "n={n}");
            let unread = usize::try_from(n).expect("small") - 2;
            assert_eq!(
                read.rejects,
                rejects_of(&[
                    (MemoSnapshotReject::BadMac, 2),
                    (MemoSnapshotReject::OverBudget, unread),
                ]),
                "n={n}"
            );
        }
    }

    /// Issue #3211: with a byte budget of 768 (three [`SMALL`]-cap objects),
    /// the live 45-byte snapshot and two 256-byte forgeries are read (557
    /// bytes); every further forgery would pass 768 and is skipped, and a
    /// 45-byte snapshot from a departed writer, older than every forgery,
    /// still fits after them and is read and verified. Exactly 4 GETs of 602
    /// bytes for 4 forged objects and for 64.
    #[tokio::test]
    async fn reseed_gets_are_bounded_by_the_byte_budget() {
        let budget = MemoReseedBudget {
            max_objects: usize::MAX,
            max_bytes: 3 * SMALL.max_bytes,
        };
        let (live, departed) = (Uuid::from_u128(1), Uuid::from_u128(2));
        for n in [4u128, 64] {
            let store = InstrumentedStore::new(MemoryStore::new());
            plant_forged(&store, n, NOW_MS).await;
            let live_bytes = plant_honest(&store, live, NOW_MS - HOUR_MS).await;
            let departed_bytes = plant_honest(&store, departed, NOW_MS - HOUR_MS).await;

            let (read, calls, bytes) = read_counting(&store, budget, &[live]).await;
            assert_eq!((calls, bytes), (4, 602), "GETs and GET bytes, n={n}");
            assert_eq!(
                read.snapshots,
                vec![
                    verified(live, &live_bytes),
                    verified(departed, &departed_bytes)
                ],
                "n={n}"
            );
            let unread = usize::try_from(n).expect("small") - 2;
            assert_eq!(
                read.rejects,
                rejects_of(&[
                    (MemoSnapshotReject::BadMac, 2),
                    (MemoSnapshotReject::OverBudget, unread),
                ]),
                "n={n}"
            );
        }
    }

    /// A departed writer's snapshot (not in the live set) is read among
    /// forged objects while they fit the budget, and when it is the newest
    /// object. The accepted cost the read's doc names: enough forgeries
    /// fresher than it push it past the budget, counted as over budget.
    #[tokio::test]
    async fn departed_snapshot_competes_on_modification_time() {
        let budget = MemoReseedBudget {
            max_objects: 8,
            max_bytes: u64::MAX,
        };
        // Lists after every forgery, so only its modification time can put
        // it ahead of them.
        let departed = Uuid::from_u128(u128::MAX - 1);
        let cases = [
            // (forged, forged mtime, departed mtime, departed read)
            (4u128, NOW_MS, NOW_MS - HOUR_MS, true),
            (64, NOW_MS - HOUR_MS, NOW_MS, true),
            (64, NOW_MS, NOW_MS - HOUR_MS, false),
        ];
        for (n, forged_ms, departed_ms, read_it) in cases {
            let store = InstrumentedStore::new(MemoryStore::new());
            plant_forged(&store, n, forged_ms).await;
            let honest = plant_honest(&store, departed, departed_ms).await;
            let (read, calls, _) = read_counting(&store, budget, &[]).await;
            let n_usize = usize::try_from(n).expect("small");
            assert_eq!(calls, u64::try_from(n_usize.min(7) + 1).expect("small"));
            if read_it {
                assert_eq!(read.snapshots, vec![verified(departed, &honest)], "n={n}");
                assert_eq!(read.rejects.get(MemoSnapshotReject::BadMac), n_usize.min(7));
            } else {
                assert!(read.snapshots.is_empty(), "n={n}");
                assert_eq!(read.rejects.get(MemoSnapshotReject::BadMac), 8);
            }
            assert_eq!(
                read.rejects.get(MemoSnapshotReject::OverBudget),
                n_usize + 1 - 8.min(n_usize + 1)
            );
        }
    }
}
