//! Idempotency marker store for logs and spans (ADR-0051 section 5).
//!
//! A keyed log/span ingest request writes a marker object after a successful
//! flush; a retry of that same request consults the marker before re-ingesting
//! and, on a hit, replays the stored [`IdempotencyReceipt`] instead. The marker
//! keyspace is additive to the frozen key layout (docs/catalog-and-mvcc.md):
//!
//! ```text
//! t/<tenant_hash>/<signal>/idem/<keyhash32>.<ingest_hour>.idm
//! keyhash32 = hex(blake3("ravel-idem-v1" || tenant_id || client_key)[0..16])
//! ```
//!
//! `keyhash32` is derived from the tenant's logical [`TenantId`], not its
//! [`TenantHash`]: the ADR's formula hashes the tenant id together with the
//! client-supplied key, and a hash cannot be un-hashed to recover the id for
//! that computation. Every function here therefore takes `&TenantId` and
//! derives the path's `tenant_hash` segment from it internally
//! (`TenantId::hash`), rather than taking a pre-hashed `TenantHash` as a
//! second, redundant argument.
//!
//! The marker body is versioned and checksummed (`RIDM` magic, u16 version,
//! crc32c over the payload); a corrupt or truncated marker decodes to a typed
//! [`MarkerError`], never a panic, and callers treat that as a miss
//! (fail-open to at-least-once, ADR-0051 section 5).
//!
//! There is no dual-reader question: the `idem/` prefix is new, no old data
//! exists under it, and no existing read, resolve, or sweep path lists it.

use blake3::Hasher;
use bytes::Bytes;
use ravel_commit::keys::{ingest_hour_string, parse_ingest_hour_string};
use ravel_object_store::{ObjectStoreBackend, PutOptions, StoreError, list_all};
use ravel_types::{Signal, TenantId};

/// Domain-separation prefix for the keyhash, distinct from `TenantId::hash`'s
/// own domain string (`ravel-tenant-v1`) so the two blake3 uses can never
/// collide by construction.
const KEYHASH_DOMAIN: &[u8] = b"ravel-idem-v1";
const IDEM_DIR: &str = "idem";
/// Marker object filename suffix.
pub const MARKER_SUFFIX: &str = "idm";

const MAGIC: &[u8; 4] = b"RIDM";
const VERSION: u16 = 1;
const HEADER_LEN: usize = MAGIC.len() + 2 + 4;
/// `written_count` (u64) + `commit_token` length prefix (u16).
const RECEIPT_HEADER_LEN: usize = 8 + 2;

/// The write outcome a marker replays on a dedup hit: cheap-to-round-trip
/// facts about the original flush, not the flushed data itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyReceipt {
    /// Rows (logs) or spans written by the original request.
    pub written_count: u64,
    /// `CommitToken::encode()` output of the commit the original flush
    /// produced. Stored as its already-opaque encoded string rather than the
    /// parsed struct: this module has no reason to interpret it, only to
    /// round-trip it back to the caller on replay.
    pub commit_token: String,
}

/// Errors decoding a marker body. All are typed and non-fatal to the caller:
/// every variant is treated as a miss (ADR-0051 section 5, fail-open).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MarkerError {
    #[error("marker body truncated: {0} bytes, need at least {HEADER_LEN}")]
    Truncated(usize),
    #[error("bad marker magic {0:?}, expected {MAGIC:?}")]
    BadMagic([u8; 4]),
    #[error("unsupported marker version {0}, expected {VERSION}")]
    UnsupportedVersion(u16),
    #[error("marker checksum mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    ChecksumMismatch { stored: u32, computed: u32 },
    #[error("malformed receipt payload: {0}")]
    MalformedReceipt(String),
    #[error("receipt commit token is {0} bytes, exceeds the u16 length-prefix limit")]
    ReceiptTooLarge(usize),
}

/// Combines the two ways [`write_marker`] can fail: the store rejecting the
/// PUT, or (rarely) the receipt itself failing to encode.
#[derive(Debug, thiserror::Error)]
pub enum MarkerWriteError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Codec(#[from] MarkerError),
}

/// Outcome of a marker lookup, whether reached via [`read_marker`]'s prefix
/// LIST or via [`write_marker`] losing a `CreateIfAbsent` race. An enum, not
/// a `bool` or `Option`, so a corrupt marker is distinguishable from a clean
/// miss for the caller's corruption counter (ADR-0051 section 5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupOutcome {
    /// A marker was found, decoded cleanly, and its receipt is ready to
    /// replay.
    Hit(IdempotencyReceipt),
    /// No marker exists for this key (within the searched window).
    Miss,
    /// A marker object exists but failed to decode (truncated, bad magic,
    /// bad checksum, or malformed payload). Treated as a miss by the caller,
    /// but counted separately.
    Corrupt,
}

/// Outcome of [`write_marker`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOutcome {
    /// This call's marker was written; it is the winner of any race.
    Written,
    /// Another writer's marker already occupies this key
    /// (`PutMode::CreateIfAbsent` lost the race). Carries what that marker
    /// held, so the loser can replay the winner's receipt instead of
    /// erroring or overwriting.
    Existing(LookupOutcome),
}

/// `hex(blake3("ravel-idem-v1" || tenant_id || client_key)[0..16])`
/// (ADR-0051 section 5). `client_key` is the opaque `x-ravel-idempotency-key`
/// value (≤128 bytes), carried as transport metadata by the caller, never a
/// proto field.
pub fn keyhash32(tenant_id: &TenantId, client_key: &[u8]) -> String {
    let mut hasher = Hasher::new();
    hasher.update(KEYHASH_DOMAIN);
    hasher.update(tenant_id.as_str().as_bytes());
    hasher.update(client_key);
    let digest = hasher.finalize();
    hex::encode(&digest.as_bytes()[..16])
}

/// Prefix covering every marker for one (tenant, signal, client_key) pair,
/// across all ingest hours: `t/<tenant_hash>/<signal>/idem/<keyhash32>.`.
/// This is the one-prefix LIST [`read_marker`] performs.
fn marker_prefix(tenant_id: &TenantId, signal: Signal, client_key: &[u8]) -> String {
    format!(
        "t/{}/{}/{IDEM_DIR}/{}.",
        tenant_id.hash().to_hex(),
        signal.key_prefix(),
        keyhash32(tenant_id, client_key),
    )
}

/// Build the exact marker key for a pinned ingest-hour bucket
/// (`t/<tenant_hash>/<signal>/idem/<keyhash32>.<ingest_hour>.idm`).
/// [`write_marker`] always knows this hour (pinned at flush open, the same
/// convention as the commit-record key); [`read_marker`] does not, because a
/// retry's flush can land in a different hour than the original, which is
/// why lookup instead LISTs [`marker_prefix`] over a window.
pub fn marker_key(
    tenant_id: &TenantId,
    signal: Signal,
    client_key: &[u8],
    ingest_hour_bucket: u32,
) -> String {
    format!(
        "{}{}.{MARKER_SUFFIX}",
        marker_prefix(tenant_id, signal, client_key),
        ingest_hour_string(ingest_hour_bucket),
    )
}

fn encode_receipt(receipt: &IdempotencyReceipt) -> Result<Vec<u8>, MarkerError> {
    let token_bytes = receipt.commit_token.as_bytes();
    let token_len = u16::try_from(token_bytes.len())
        .map_err(|_| MarkerError::ReceiptTooLarge(token_bytes.len()))?;
    let mut out = Vec::with_capacity(RECEIPT_HEADER_LEN + token_bytes.len());
    out.extend_from_slice(&receipt.written_count.to_le_bytes());
    out.extend_from_slice(&token_len.to_le_bytes());
    out.extend_from_slice(token_bytes);
    Ok(out)
}

fn decode_receipt(bytes: &[u8]) -> Result<IdempotencyReceipt, MarkerError> {
    if bytes.len() < RECEIPT_HEADER_LEN {
        return Err(MarkerError::MalformedReceipt(format!(
            "receipt payload is {} bytes, need at least {RECEIPT_HEADER_LEN}",
            bytes.len()
        )));
    }
    let (header, rest) = bytes.split_at(RECEIPT_HEADER_LEN);
    let written_count = u64::from_le_bytes(header[0..8].try_into().unwrap_or([0; 8]));
    let token_len = u16::from_le_bytes([header[8], header[9]]) as usize;
    if rest.len() != token_len {
        return Err(MarkerError::MalformedReceipt(format!(
            "commit token length prefix says {token_len} bytes, {} remain",
            rest.len()
        )));
    }
    let commit_token = String::from_utf8(rest.to_vec()).map_err(|e| {
        MarkerError::MalformedReceipt(format!("commit token is not valid utf-8: {e}"))
    })?;
    Ok(IdempotencyReceipt {
        written_count,
        commit_token,
    })
}

/// Encode a marker body: `RIDM` magic, u16 version, crc32c over the payload,
/// then the payload (the serialized receipt).
fn encode_marker(receipt: &IdempotencyReceipt) -> Result<Vec<u8>, MarkerError> {
    let payload = encode_receipt(receipt)?;
    let crc = crc32c::crc32c(&payload);
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decode and verify a marker body. Every failure mode (truncation, bad
/// magic, unsupported version, checksum mismatch, malformed payload) is a
/// typed [`MarkerError`], never a panic.
fn decode_marker(bytes: &[u8]) -> Result<IdempotencyReceipt, MarkerError> {
    if bytes.len() < HEADER_LEN {
        return Err(MarkerError::Truncated(bytes.len()));
    }
    let (header, payload) = bytes.split_at(HEADER_LEN);
    let magic = &header[0..4];
    if magic != MAGIC {
        let mut got = [0u8; 4];
        got.copy_from_slice(magic);
        return Err(MarkerError::BadMagic(got));
    }
    let version = u16::from_le_bytes([header[4], header[5]]);
    if version != VERSION {
        return Err(MarkerError::UnsupportedVersion(version));
    }
    let stored_crc = u32::from_le_bytes(header[6..10].try_into().unwrap_or([0; 4]));
    let computed_crc = crc32c::crc32c(payload);
    if stored_crc != computed_crc {
        return Err(MarkerError::ChecksumMismatch {
            stored: stored_crc,
            computed: computed_crc,
        });
    }
    decode_receipt(payload)
}

/// GET one exact marker key and decode it. `NotFound` is a miss; a decode
/// failure is [`LookupOutcome::Corrupt`], logged so the corruption is
/// observable even though it is fail-open to the caller.
async fn read_marker_at(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<LookupOutcome, StoreError> {
    match store.get(key, ravel_object_store::GetRange::Full).await {
        Ok(outcome) => match decode_marker(&outcome.data) {
            Ok(receipt) => Ok(LookupOutcome::Hit(receipt)),
            Err(err) => {
                tracing::warn!(key, %err, "idempotency marker failed to decode; treating as miss");
                Ok(LookupOutcome::Corrupt)
            }
        },
        Err(StoreError::NotFound) => Ok(LookupOutcome::Miss),
        Err(other) => Err(other),
    }
}

/// Write an idempotency marker via `PutMode::CreateIfAbsent`
/// (ADR-0051 section 5): the natural fit, since the marker's whole purpose
/// is "first writer wins, everyone else replays the winner", which is
/// exactly what create-if-absent gives for free — no separate CAS-read step,
/// and no lock beyond what the backend already provides for a fresh key.
///
/// On a race, the loser does not error or overwrite: it reads back and
/// decodes whatever the winner wrote and returns it as
/// [`WriteOutcome::Existing`].
pub async fn write_marker(
    store: &dyn ObjectStoreBackend,
    tenant_id: &TenantId,
    signal: Signal,
    client_key: &[u8],
    ingest_hour_bucket: u32,
    receipt: &IdempotencyReceipt,
) -> Result<WriteOutcome, MarkerWriteError> {
    let key = marker_key(tenant_id, signal, client_key, ingest_hour_bucket);
    let body = encode_marker(receipt)?;
    match store
        .put(&key, Bytes::from(body), PutOptions::create_if_absent())
        .await
    {
        Ok(_) => Ok(WriteOutcome::Written),
        Err(StoreError::AlreadyExists) => {
            Ok(WriteOutcome::Existing(read_marker_at(store, &key).await?))
        }
        Err(other) => Err(other.into()),
    }
}

/// Look up a marker for a retried request via the one-prefix LIST ADR-0051
/// section 5 describes. The exact ingest hour a retry would land in is not
/// known in advance (a retry's flush can pin a different hour than the
/// original request pinned), so this lists every marker under
/// [`marker_prefix`] and considers only those whose `ingest_hour` falls in
/// the closed window `[now_ingest_hour_bucket - dedup_window_hours,
/// now_ingest_hour_bucket]`, picking the most recent in-window hit.
///
/// Returns [`LookupOutcome::Miss`] if nothing in-window is found (or
/// everything found there has already been swept), and
/// [`LookupOutcome::Corrupt`] if the best in-window candidate exists but
/// fails to decode.
pub async fn read_marker(
    store: &dyn ObjectStoreBackend,
    tenant_id: &TenantId,
    signal: Signal,
    client_key: &[u8],
    now_ingest_hour_bucket: u32,
    dedup_window_hours: u32,
) -> Result<LookupOutcome, StoreError> {
    let prefix = marker_prefix(tenant_id, signal, client_key);
    let min_hour = now_ingest_hour_bucket.saturating_sub(dedup_window_hours);
    let candidates = list_all(store, &prefix).await?;

    let mut best: Option<(u32, String)> = None;
    for meta in candidates {
        let Some(basename) = meta.key.strip_prefix(&prefix) else {
            continue;
        };
        let Some(hour_text) = basename.strip_suffix(&format!(".{MARKER_SUFFIX}")) else {
            continue;
        };
        let Ok(hour) = parse_ingest_hour_string(hour_text) else {
            continue;
        };
        if hour < min_hour || hour > now_ingest_hour_bucket {
            continue;
        }
        if best.as_ref().is_none_or(|(best_hour, _)| hour > *best_hour) {
            best = Some((hour, meta.key));
        }
    }

    match best {
        None => Ok(LookupOutcome::Miss),
        Some((_, key)) => read_marker_at(store, &key).await,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, PutMode};

    fn tenant(id: &str) -> TenantId {
        TenantId::new(id)
    }

    fn receipt(written_count: u64, commit_token: &str) -> IdempotencyReceipt {
        IdempotencyReceipt {
            written_count,
            commit_token: commit_token.to_string(),
        }
    }

    #[tokio::test]
    async fn marker_replay_returns_stored_receipt() {
        let store = MemoryStore::new();
        let tenant = tenant("acme");
        let receipt = receipt(42, "v2:token-abc");

        let outcome = write_marker(
            &store,
            &tenant,
            Signal::Logs,
            b"client-key-1",
            495_972,
            &receipt,
        )
        .await
        .expect("first write must succeed");
        assert_eq!(outcome, WriteOutcome::Written);

        let looked_up = read_marker(&store, &tenant, Signal::Logs, b"client-key-1", 495_972, 24)
            .await
            .expect("lookup must succeed");
        assert_eq!(looked_up, LookupOutcome::Hit(receipt));
    }

    #[tokio::test]
    async fn corrupt_marker_is_typed_miss() {
        let store = MemoryStore::new();
        let tenant = tenant("acme");
        let receipt = receipt(7, "v2:token-xyz");
        let ingest_hour_bucket = 495_972;

        write_marker(
            &store,
            &tenant,
            Signal::Spans,
            b"client-key-2",
            ingest_hour_bucket,
            &receipt,
        )
        .await
        .expect("write must succeed");

        let key = marker_key(&tenant, Signal::Spans, b"client-key-2", ingest_hour_bucket);
        let mut bytes = store
            .get(&key, GetRange::Full)
            .await
            .expect("marker must exist")
            .data
            .to_vec();
        // Flip a byte inside the payload (past the fixed header) so the
        // stored crc32c no longer matches.
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        store
            .put(
                &key,
                Bytes::from(bytes),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("overwrite must succeed");

        let looked_up = read_marker(
            &store,
            &tenant,
            Signal::Spans,
            b"client-key-2",
            ingest_hour_bucket,
            24,
        )
        .await
        .expect("lookup itself must not fail on a corrupt marker");
        assert_eq!(looked_up, LookupOutcome::Corrupt);

        // Truncation is corrupt too, never a panic.
        assert!(matches!(
            decode_marker(b"RI"),
            Err(MarkerError::Truncated(2))
        ));
    }

    #[tokio::test]
    async fn concurrent_write_marker_race_has_exactly_one_winner() {
        let store = std::sync::Arc::new(MemoryStore::new());
        let tenant = tenant("acme");
        let ingest_hour_bucket = 495_972;

        let winner_side = {
            let store = store.clone();
            let tenant = tenant.clone();
            let receipt = receipt(10, "v2:token-first");
            tokio::spawn(async move {
                write_marker(
                    store.as_ref(),
                    &tenant,
                    Signal::Logs,
                    b"race-key",
                    ingest_hour_bucket,
                    &receipt,
                )
                .await
            })
        };
        let loser_side = {
            let store = store.clone();
            let tenant = tenant.clone();
            let receipt = receipt(20, "v2:token-second");
            tokio::spawn(async move {
                write_marker(
                    store.as_ref(),
                    &tenant,
                    Signal::Logs,
                    b"race-key",
                    ingest_hour_bucket,
                    &receipt,
                )
                .await
            })
        };

        let first = winner_side.await.expect("task must not panic");
        let second = loser_side.await.expect("task must not panic");
        let outcomes = [
            first.expect("write_marker must not error"),
            second.expect("write_marker must not error"),
        ];

        let written_count = outcomes
            .iter()
            .filter(|o| matches!(o, WriteOutcome::Written))
            .count();
        assert_eq!(written_count, 1, "exactly one side must win the race");

        let existing = outcomes
            .iter()
            .find_map(|o| match o {
                WriteOutcome::Existing(existing) => Some(existing.clone()),
                WriteOutcome::Written => None,
            })
            .expect("exactly one side must lose the race");

        // The loser observes the winner's marker, never its own attempted
        // receipt and never an error.
        let winning_receipt = match existing {
            LookupOutcome::Hit(receipt) => receipt,
            other => panic!("loser must see a clean hit on the winner's marker, got {other:?}"),
        };
        assert!(
            winning_receipt.commit_token == "v2:token-first"
                || winning_receipt.commit_token == "v2:token-second",
            "the replayed receipt must be one of the two attempts, not corrupted data"
        );

        // Only one object exists at the key: the loser never overwrote it.
        let key = marker_key(&tenant, Signal::Logs, b"race-key", ingest_hour_bucket);
        let stored = read_marker_at(store.as_ref(), &key)
            .await
            .expect("final read must succeed");
        assert_eq!(stored, LookupOutcome::Hit(winning_receipt));
    }

    proptest! {
        #[test]
        fn receipt_codec_round_trips(
            written_count in any::<u64>(),
            commit_token in "[a-zA-Z0-9:_-]{0,200}",
        ) {
            let original = IdempotencyReceipt { written_count, commit_token };
            let encoded = encode_marker(&original).expect("encode must succeed for a reasonable token");
            let decoded = decode_marker(&encoded).expect("decode must round-trip what was just encoded");
            prop_assert_eq!(decoded, original);
        }

        #[test]
        fn decode_never_panics_on_arbitrary_bytes(bytes in proptest::collection::vec(any::<u8>(), 0..300)) {
            let _ = decode_marker(&bytes);
        }
    }
}
