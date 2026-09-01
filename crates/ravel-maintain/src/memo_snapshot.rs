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

use ravel_object_store::{ObjectStoreBackend, PutMode, PutOptions, StoreError, list_all};
use uuid::Uuid;

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

/// List `sys/maintain/memo/` and GET every snapshot object under it, returning
/// each one's raw framed bytes (this process's own previous snapshot included).
/// Decoding and per-snapshot staleness/ownership filtering happen at the seed
/// site ([`crate::scan::MaintainMemo::seed_from_snapshot`]), so an undecodable
/// sibling is skipped there rather than failing the whole read.
///
/// Only a failed LIST or GET is an `Err`, which the caller treats fail-open
/// (cold start for the affected units), never freezing the loop on a transient
/// read fault.
pub async fn read_all_memo_snapshots(
    store: &dyn ObjectStoreBackend,
) -> Result<Vec<Vec<u8>>, StoreError> {
    let objects = list_all(store, MEMO_PREFIX).await?;
    let mut snapshots = Vec::with_capacity(objects.len());
    for meta in objects {
        let got = store
            .get(&meta.key, ravel_object_store::GetRange::Full)
            .await?;
        snapshots.push(got.data.to_vec());
    }
    Ok(snapshots)
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

    /// A written snapshot round-trips its exact bytes back through a list+get.
    #[tokio::test]
    async fn write_then_read_all_returns_the_bytes() {
        let store = MemoryStore::new();
        let id = Uuid::from_u128(7);
        let payload = vec![1u8, 2, 3, 4, 5];
        write_memo_snapshot(&store, &id, payload.clone())
            .await
            .expect("write");

        let all = read_all_memo_snapshots(&store).await.expect("read");
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
        let all = read_all_memo_snapshots(&store).await.expect("read");
        assert_eq!(all.len(), 3);
    }

    /// An empty prefix reads back as an empty set, not an error.
    #[tokio::test]
    async fn read_all_over_empty_prefix_is_empty() {
        let store = MemoryStore::new();
        assert!(
            read_all_memo_snapshots(&store)
                .await
                .expect("read")
                .is_empty()
        );
    }
}
