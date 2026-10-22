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
//! process, the worker lists this prefix and GETs every sibling's snapshot (its
//! own previous one included), seeding its in-memory [`crate::scan::MaintainMemo`]
//! from the freshest non-stale entries for the units it now owns rather than
//! starting cold.
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

use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, list_all};
use uuid::Uuid;

use crate::scan::MAX_MEMO_SNAPSHOT_BYTES;

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
/// payload from [`crate::scan::MaintainMemo::encode_snapshot`].
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
    /// Each read snapshot object's raw framed bytes, in listing order.
    pub snapshots: Vec<Vec<u8>>,
    /// Objects skipped unread or undecoded because they were longer than
    /// [`MAX_MEMO_SNAPSHOT_BYTES`], by their listed size or by the bytes the
    /// bounded GET returned.
    pub skipped_oversize: usize,
}

/// List `sys/maintain/memo/` and GET every snapshot object under it, returning
/// each one's raw framed bytes (this process's own previous snapshot included).
/// Decoding and per-snapshot staleness/ownership filtering happen at the seed
/// site ([`crate::scan::MaintainMemo::seed_from_snapshot`]), so an undecodable
/// sibling is skipped there rather than failing the whole read.
///
/// The read is bounded by [`MAX_MEMO_SNAPSHOT_BYTES`] (issue #3199): an object
/// whose listed size is over the cap is skipped without a GET, every other
/// object is read with a range GET of the cap plus one byte, and a body longer
/// than the cap is skipped. Each skip is logged at debug with the key and size
/// and counted in [`MemoSnapshotRead::skipped_oversize`]; it is not an error.
///
/// Only a failed LIST or GET is an `Err`, which the caller treats fail-open
/// (cold start for the affected units), never freezing the loop on a transient
/// read fault.
pub async fn read_all_memo_snapshots(
    store: &dyn ObjectStoreBackend,
) -> Result<MemoSnapshotRead, StoreError> {
    let objects = list_all(store, MEMO_PREFIX).await?;
    let mut read = MemoSnapshotRead::default();
    for meta in objects {
        if meta.size > MAX_MEMO_SNAPSHOT_BYTES {
            tracing::debug!(
                key = %meta.key,
                size = meta.size,
                cap = MAX_MEMO_SNAPSHOT_BYTES,
                "maintenance: skipping a memo snapshot listed over the size cap"
            );
            read.skipped_oversize += 1;
            continue;
        }
        if meta.size == 0 {
            // A range GET of an empty object is an invalid range; the empty
            // body is passed on for the seed site to reject as truncated.
            read.snapshots.push(Vec::new());
            continue;
        }
        let got = match store
            .get(&meta.key, GetRange::Range(0, MAX_MEMO_SNAPSHOT_BYTES + 1))
            .await
        {
            Ok(got) => got,
            // The object shrank to nothing between the LIST and the GET.
            Err(StoreError::InvalidRange(_)) => {
                read.snapshots.push(Vec::new());
                continue;
            }
            Err(err) => return Err(err),
        };
        let len = u64::try_from(got.data.len()).unwrap_or(u64::MAX);
        if len > MAX_MEMO_SNAPSHOT_BYTES {
            tracing::debug!(
                key = %meta.key,
                size = len,
                cap = MAX_MEMO_SNAPSHOT_BYTES,
                "maintenance: skipping a memo snapshot read over the size cap"
            );
            read.skipped_oversize += 1;
            continue;
        }
        read.snapshots.push(got.data.to_vec());
    }
    Ok(read)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;

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

    /// A written snapshot round-trips its exact bytes back through a list+get.
    #[tokio::test]
    async fn write_then_read_all_returns_the_bytes() {
        let store = MemoryStore::new();
        let id = Uuid::from_u128(7);
        let payload = vec![1u8, 2, 3, 4, 5];
        write_memo_snapshot(&store, &id, payload.clone())
            .await
            .expect("write");

        let all = read_all_memo_snapshots(&store)
            .await
            .expect("read")
            .snapshots;
        assert_eq!(all, vec![payload]);
    }

    /// `read_all_memo_snapshots` returns every sibling's object, not just one.
    #[tokio::test]
    async fn read_all_returns_every_siblings_snapshot() {
        let store = MemoryStore::new();
        for (i, id) in [Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)]
            .into_iter()
            .enumerate()
        {
            write_memo_snapshot(&store, &id, vec![i as u8])
                .await
                .expect("write");
        }
        let all = read_all_memo_snapshots(&store)
            .await
            .expect("read")
            .snapshots;
        assert_eq!(all.len(), 3);
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
        let first = vec![1u8, 2, 3];
        let second = vec![9u8, 9, 9, 9];

        write_memo_snapshot(&store, &id, first)
            .await
            .expect("first write");
        write_memo_snapshot(&store, &id, second.clone())
            .await
            .expect("a second write to the same key succeeds unconditionally under Overwrite");

        let all = read_all_memo_snapshots(&store)
            .await
            .expect("read")
            .snapshots;
        assert_eq!(
            all,
            vec![second],
            "the second write's bytes are what's stored"
        );
    }

    /// An object one byte over [`MAX_MEMO_SNAPSHOT_BYTES`] is skipped and
    /// counted without a GET, while an object of exactly the cap and a small
    /// sibling are both returned in full; the oversized object fails nothing.
    #[tokio::test]
    async fn read_all_skips_an_object_over_the_size_cap() {
        use ravel_object_store::instrument::InstrumentedStore;

        let store = InstrumentedStore::new(MemoryStore::new());
        let cap = usize::try_from(MAX_MEMO_SNAPSHOT_BYTES).expect("cap fits usize");
        let at_cap = vec![0xA5u8; cap];
        let small = vec![1u8, 2, 3];
        write_memo_snapshot(&store, &Uuid::from_u128(1), vec![0x5Au8; cap + 1])
            .await
            .expect("write oversized");
        write_memo_snapshot(&store, &Uuid::from_u128(2), at_cap.clone())
            .await
            .expect("write at cap");
        write_memo_snapshot(&store, &Uuid::from_u128(3), small.clone())
            .await
            .expect("write small");

        let before = store.metrics().snapshot();
        let read = read_all_memo_snapshots(&store).await.expect("read");
        let after = store.metrics().snapshot();

        assert_eq!(read.skipped_oversize, 1, "exactly the oversized object");
        assert_eq!(read.snapshots.len(), 2);
        assert!(read.snapshots[0] == at_cap, "the at-cap object, in full");
        assert_eq!(read.snapshots[1], small);
        assert_eq!(
            after.get.calls - before.get.calls,
            2,
            "the oversized object is skipped from its listed size, unread"
        );
    }

    /// An empty object is passed through as empty bytes for the seed site to
    /// reject, not turned into a read error by the range GET.
    #[tokio::test]
    async fn read_all_passes_an_empty_object_through() {
        let store = MemoryStore::new();
        write_memo_snapshot(&store, &Uuid::from_u128(1), Vec::new())
            .await
            .expect("write empty");
        let read = read_all_memo_snapshots(&store).await.expect("read");
        assert_eq!(read.snapshots, vec![Vec::<u8>::new()]);
        assert_eq!(read.skipped_oversize, 0);
    }

    /// An empty prefix reads back as an empty set, not an error.
    #[tokio::test]
    async fn read_all_over_empty_prefix_is_empty() {
        let store = MemoryStore::new();
        assert!(
            read_all_memo_snapshots(&store)
                .await
                .expect("read")
                .snapshots
                .is_empty()
        );
    }
}
