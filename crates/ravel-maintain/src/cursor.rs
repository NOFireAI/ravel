//! Advisory CAS scan cursor at `t/<th>/m/maint/<shard>/cursor` (plan
//! section 3.1). Mutable, exempt from the immutability rule; losing or
//! corrupting it only costs a rescan, never correctness (plan section
//! 3.1, 3.2).
//!
//! The plan pins the key but not a payload byte grammar. This crate
//! encodes the cursor value as a 4-byte little-endian `u32`: the next
//! `ingest_hour_bucket` to examine for the (tenant, signal, shard) the key
//! names. This mirrors `Footer.ingest_hour_bucket`'s own `u32` type
//! (proto/ravel/segment.proto) and is a genuine spec gap disclosed in the
//! implementation report rather than silently assumed.

use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, Version};
use ravel_types::{Signal, TenantHash};

use crate::error::MaintainError;

/// The cursor's current position plus the store version it was read at,
/// needed to CAS the next advance. `None` version means the key does not
/// exist yet (first scan for this shard).
#[derive(Debug, Clone)]
pub struct CursorState {
    pub next_hour_bucket: u32,
    pub version: Option<Version>,
}

/// Read the cursor, defaulting to hour bucket 0 with no version if the key
/// has never been written (plan section 3.2: the scan starts at hour 0
/// absent any prior progress).
pub async fn read_cursor(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<CursorState, MaintainError> {
    let key = ravel_commit::keys::maint_cursor_key(tenant_hash, signal, shard)?;
    match store.get(&key, GetRange::Full).await {
        Ok(outcome) => {
            let bytes: [u8; 4] = outcome.data.as_ref().try_into().unwrap_or([0; 4]);
            Ok(CursorState {
                next_hour_bucket: u32::from_le_bytes(bytes),
                version: Some(outcome.version),
            })
        }
        Err(StoreError::NotFound) => Ok(CursorState {
            next_hour_bucket: 0,
            version: None,
        }),
        Err(err) => Err(err.into()),
    }
}

/// Advance the cursor to `next_hour_bucket`, CAS-guarded on the version it
/// was last read at. Losing the race (another maintainer already advanced
/// it, or wrote it for the first time) is not an error: the cursor is
/// advisory, so the caller simply keeps scanning from its own in-memory
/// position. Returns the new version on a successful write, `None` if the
/// race was lost (the caller should not reuse the stale version for a
/// further CAS in the same loop).
pub async fn advance_cursor(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    shard: u32,
    current: &CursorState,
    next_hour_bucket: u32,
) -> Result<Option<Version>, MaintainError> {
    let key = ravel_commit::keys::maint_cursor_key(tenant_hash, signal, shard)?;
    let mode = match &current.version {
        Some(version) => PutMode::CasVersion(version.clone()),
        None => PutMode::CreateIfAbsent,
    };
    let opts = PutOptions {
        mode,
        checksum: None,
    };
    match store
        .put(&key, next_hour_bucket.to_le_bytes().to_vec().into(), opts)
        .await
    {
        Ok(outcome) => Ok(Some(outcome.version)),
        Err(StoreError::PreconditionFailed | StoreError::AlreadyExists) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;

    fn tenant_hash() -> TenantHash {
        TenantHash([0x42; 16])
    }

    #[tokio::test]
    async fn read_cursor_defaults_to_zero_when_absent() {
        let store = MemoryStore::new();
        let state = read_cursor(&store, &tenant_hash(), Signal::Metrics, 1)
            .await
            .expect("read");
        assert_eq!(state.next_hour_bucket, 0);
        assert!(state.version.is_none());
    }

    #[tokio::test]
    async fn advance_cursor_then_read_round_trips() {
        let store = MemoryStore::new();
        let th = tenant_hash();
        let state = read_cursor(&store, &th, Signal::Metrics, 1)
            .await
            .expect("read");
        let version = advance_cursor(&store, &th, Signal::Metrics, 1, &state, 7)
            .await
            .expect("advance");
        assert!(version.is_some());

        let state2 = read_cursor(&store, &th, Signal::Metrics, 1)
            .await
            .expect("read");
        assert_eq!(state2.next_hour_bucket, 7);
    }

    #[tokio::test]
    async fn advance_cursor_loses_race_on_stale_version() {
        let store = MemoryStore::new();
        let th = tenant_hash();
        let state = read_cursor(&store, &th, Signal::Metrics, 1)
            .await
            .expect("read");
        advance_cursor(&store, &th, Signal::Metrics, 1, &state, 3)
            .await
            .expect("advance");

        // `state` is now stale (version predates the write above).
        let lost = advance_cursor(&store, &th, Signal::Metrics, 1, &state, 5)
            .await
            .expect("advance does not error on lost race");
        assert!(lost.is_none());

        let current = read_cursor(&store, &th, Signal::Metrics, 1)
            .await
            .expect("read");
        assert_eq!(current.next_hour_bucket, 3);
    }
}
