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
//! crc32c over `magic || version || payload`, so a bit flip in the header is
//! caught exactly like one in the payload); a corrupt or truncated marker
//! decodes to a typed [`MarkerError`], never a panic, and callers treat that
//! as a miss (fail-open to at-least-once, ADR-0051 section 5). Byte layout
//! and checksum coverage are documented in docs/catalog-and-mvcc.md.
//!
//! There is no dual-reader question: the `idem/` prefix is new, no old data
//! exists under it, and no existing read, resolve, or sweep path lists it.

use std::sync::atomic::{AtomicU64, Ordering};

use blake3::Hasher;
use bytes::Bytes;
use futures::StreamExt;
use ravel_commit::keys::ingest_hour_string;
use ravel_object_store::{ObjectStoreBackend, PutOptions, StoreError, UploadChecksum};
use ravel_types::{Signal, TenantId};

/// Domain-separation prefix for the keyhash, distinct from `TenantId::hash`'s
/// own domain string (`ravel-tenant-v1`) so the two blake3 uses can never
/// collide by construction.
const KEYHASH_DOMAIN: &[u8] = b"ravel-idem-v1";
const IDEM_DIR: &str = "idem";
/// Marker object filename suffix.
pub const MARKER_SUFFIX: &str = "idm";

/// Forward clock-skew tolerance for a marker's `<ingest_hour>`, in hours:
/// [`read_marker`] still honors a marker up to this many hours ahead of the
/// reader's own current ingest-hour bucket, absorbing a writer whose clock
/// ran slightly ahead across an hour boundary. `ravel-maintain`'s sweep
/// subtracts this same constant from its own age gate
/// (`crates/ravel-maintain/src/sweep.rs`), so a marker this path would still
/// honor is never swept out from under it by a sweeper whose clock lags an
/// ingest node's by up to this much.
pub const IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS: u32 = 1;

/// Most marker GETs [`read_marker`] keeps in flight at once. Its first batch
/// (the forward skew hours, the current hour and the one before it) is
/// smaller than this; the rest of the window fans out under this bound, so a
/// miss at the default window costs about four round trips of latency rather
/// than twenty-six.
pub const IDEM_MARKER_PROBE_CONCURRENCY: usize = 8;

/// Marker GETs [`read_marker`] has issued, logs then spans, hits and misses
/// alike. Process-global, like the ingest path's lookup-failure count.
static PROBE_GETS: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];

fn probe_gets_slot(signal: Signal) -> Option<&'static AtomicU64> {
    match signal {
        Signal::Logs => Some(&PROBE_GETS[0]),
        Signal::Spans => Some(&PROBE_GETS[1]),
        _ => None,
    }
}

/// The marker GETs [`read_marker`] has issued for `signal` in this process,
/// hits and misses alike, rendered as
/// `ravel_ingest_idempotency_probe_gets_total`. Zero for a signal that writes
/// no markers.
pub fn idempotency_probe_gets(signal: Signal) -> u64 {
    probe_gets_slot(signal).map_or(0, |slot| slot.load(Ordering::Relaxed))
}

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
    /// The `x-ravel-commit-token` header value the original flush produced:
    /// one comma-separated, already-`CommitToken::encode()`-d token per
    /// shard the request's points flushed through (docs/consistency-
    /// model.md), not a single token. Stored as that opaque encoded string
    /// rather than parsed tokens: this module has no reason to interpret
    /// it, only to round-trip it back to the caller on replay.
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

/// A [`read_marker`] probe failed with a store error other than `NotFound`.
/// Carries the key whose GET failed, for the caller's server-side log; it is
/// not meant for a client-facing message.
#[derive(Debug, thiserror::Error)]
#[error("idempotency marker GET {key} failed: {source}")]
pub struct MarkerLookupError {
    pub key: String,
    #[source]
    pub source: StoreError,
}

/// Outcome of a marker lookup, whether reached via [`read_marker`]'s
/// per-hour probes or via [`write_marker`] losing a `CreateIfAbsent` race. An enum, not
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

/// Build the exact marker key for a pinned ingest-hour bucket
/// (`t/<tenant_hash>/<signal>/idem/<keyhash32>.<ingest_hour>.idm`).
/// [`write_marker`] always knows this hour (pinned at request receive, from
/// the receiver's admission-time clock, not the commit record's flush-open
/// hour); [`read_marker`] does not, because a retry's request can land in a
/// different hour than the original, which is why lookup probes this key for
/// every hour of a window.
pub fn marker_key(
    tenant_id: &TenantId,
    signal: Signal,
    client_key: &[u8],
    ingest_hour_bucket: u32,
) -> String {
    format!(
        "t/{}/{}/{IDEM_DIR}/{}.{}.{MARKER_SUFFIX}",
        tenant_id.hash().to_hex(),
        signal.key_prefix(),
        keyhash32(tenant_id, client_key),
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

/// Encode a marker body: `RIDM` magic, u16 version, crc32c over
/// `magic || version || payload`, then the payload (the serialized receipt).
/// Folding the header into the checksum means a bit flip in `magic` or
/// `version` is caught here rather than surfacing as a misdecode under a
/// future version's body layout.
fn encode_marker(receipt: &IdempotencyReceipt) -> Result<Vec<u8>, MarkerError> {
    let payload = encode_receipt(receipt)?;
    let mut prefix = Vec::with_capacity(MAGIC.len() + 2);
    prefix.extend_from_slice(MAGIC);
    prefix.extend_from_slice(&VERSION.to_le_bytes());
    let crc = crc32c::crc32c_append(crc32c::crc32c(&prefix), &payload);
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&prefix);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decode and verify a marker body. Every failure mode (truncation, bad
/// magic, unsupported version, checksum mismatch, malformed payload) is a
/// typed [`MarkerError`], never a panic.
///
/// Public so `ravel-cli`'s `idem inspect` can report the specific
/// failure reason [`read_marker`]'s [`LookupOutcome::Corrupt`] collapses:
/// the one and only decoder for this frozen format, not a second copy.
pub fn decode_marker(bytes: &[u8]) -> Result<IdempotencyReceipt, MarkerError> {
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
    // Coverage is magic || version || payload, matching encode_marker: the
    // crc field itself (header[6..10]) is excluded, same as any self-
    // describing checksum has to exclude its own bytes.
    let computed_crc = crc32c::crc32c_append(crc32c::crc32c(&header[0..6]), payload);
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
    let checksum = UploadChecksum::Crc32c(crc32c::crc32c(&body));
    match store
        .put(
            &key,
            Bytes::from(body),
            PutOptions::create_if_absent().with_checksum(checksum),
        )
        .await
    {
        Ok(_) => Ok(WriteOutcome::Written),
        Err(StoreError::AlreadyExists) => {
            Ok(WriteOutcome::Existing(read_marker_at(store, &key).await?))
        }
        Err(other) => Err(other.into()),
    }
}

/// Look up a marker for a retried request by probing the exact marker key
/// for each ingest hour of the closed window
/// `[now_ingest_hour_bucket - dedup_window_hours,
/// now_ingest_hour_bucket + IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS]`
/// (ADR-0051 section 5 and its 2026-10-03 probe amendment). A retry cannot
/// know the hour the original request pinned, so every hour of the window is
/// a candidate. The forward tolerance absorbs the original writer's clock
/// running slightly ahead of the reader's across an hour boundary; a marker
/// further in the future than that is never probed and is left for the sweep.
///
/// The probes run in two batches, each with at most
/// [`IDEM_MARKER_PROBE_CONCURRENCY`] GETs in flight: first the forward skew
/// hours, the current hour and the hour before it, where a retry's marker
/// almost always sits; then, only if that batch found nothing, every remaining
/// hour of the window. A batch is decided once all of its probes answered,
/// and decided the way a sequential newest-first scan stopping at the first
/// object or the first store error would decide it, so the result for a given
/// store state is the one that scan returns.
///
/// The probes are GETs, not a listing of the key's `<keyhash32>.` prefix: the
/// S3 adapter appends `/` to every list prefix, so that listing finds no
/// marker on S3.
///
/// Returns [`LookupOutcome::Miss`] when every probe answers `NotFound`, and
/// [`LookupOutcome::Corrupt`] when the newest marker found fails to decode.
/// A store error other than `NotFound` fails the lookup when it sits at a
/// newer hour than every marker found, since the lookup cannot tell a marker
/// it could not read there from none. A store error at an hour older than a
/// marker found is ignored: the scan stops at that marker and never reaches
/// it. This function sets no deadline of its own; the caller bounds it.
pub async fn read_marker(
    store: &dyn ObjectStoreBackend,
    tenant_id: &TenantId,
    signal: Signal,
    client_key: &[u8],
    now_ingest_hour_bucket: u32,
    dedup_window_hours: u32,
) -> Result<LookupOutcome, MarkerLookupError> {
    let oldest = now_ingest_hour_bucket.saturating_sub(dedup_window_hours);
    let newest = now_ingest_hour_bucket.saturating_add(IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS);
    let hours: Vec<u32> = (oldest..=newest).rev().collect();
    let first_batch_floor = now_ingest_hour_bucket.saturating_sub(1);
    let split = hours.partition_point(|&hour| hour >= first_batch_floor);
    let (first, rest) = hours.split_at(split);
    for batch in [first, rest] {
        match probe_batch(store, tenant_id, signal, client_key, batch).await? {
            LookupOutcome::Miss => {}
            found => return Ok(found),
        }
    }
    Ok(LookupOutcome::Miss)
}

/// Probe every hour of `hours` (newest first), at most
/// [`IDEM_MARKER_PROBE_CONCURRENCY`] at a time, then decide the batch the way
/// a sequential newest-first scan would: walking the answers from the newest
/// hour down, the first object found is the outcome and the first store error
/// fails the batch, whichever comes first. Anything older than that hour is
/// ignored, store errors included.
async fn probe_batch(
    store: &dyn ObjectStoreBackend,
    tenant_id: &TenantId,
    signal: Signal,
    client_key: &[u8],
    hours: &[u32],
) -> Result<LookupOutcome, MarkerLookupError> {
    // Built before the first await so the stream holds named futures, not the
    // mapping closure, which keeps the lookup future `Send` for the handlers.
    let probes: Vec<_> = hours
        .iter()
        .map(|&hour| {
            probe_key(
                store,
                signal,
                marker_key(tenant_id, signal, client_key, hour),
            )
        })
        .collect();
    let results: Vec<(String, Result<LookupOutcome, StoreError>)> = futures::stream::iter(probes)
        .buffered(IDEM_MARKER_PROBE_CONCURRENCY)
        .collect()
        .await;
    // `buffered` yields in input order, so `results` runs newest hour first.
    for (key, result) in results {
        match result {
            Err(source) => return Err(MarkerLookupError { key, source }),
            Ok(LookupOutcome::Miss) => {}
            Ok(found) => return Ok(found),
        }
    }
    Ok(LookupOutcome::Miss)
}

async fn probe_key(
    store: &dyn ObjectStoreBackend,
    signal: Signal,
    key: String,
) -> (String, Result<LookupOutcome, StoreError>) {
    if let Some(slot) = probe_gets_slot(signal) {
        slot.fetch_add(1, Ordering::Relaxed);
    }
    let result = read_marker_at(store, &key).await;
    (key, result)
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

    /// The exact key `read_marker` probes, as one literal.
    /// `gateway_template_reads_the_idempotency_marker_by_exact_key` in
    /// `crates/ravel-commit/tests/iam_templates.rs` pins its own mirror of
    /// `marker_key` to this same literal.
    #[test]
    fn marker_key_is_the_shape_the_gateway_template_reads() {
        assert_eq!(
            marker_key(&tenant("acme"), Signal::Logs, b"client-key-1", 495_972),
            "t/86bc967f6b7c19288226b362b9a7b013/l/idem/\
             2fc38ed8fb3f5e9c1ed3eb38b4e0d1bc.20260731T12.idm"
        );
    }

    /// A `MemoryStore` that lists the way `S3Store` does
    /// (`crates/ravel-object-store/src/s3.rs`, "Prefix listing is
    /// segment-based"): `object_store` appends the path delimiter to every
    /// non-empty prefix, so a prefix that does not end in `/` matches only keys
    /// one segment below it. It also records the key of every GET, in order.
    struct SegmentAlignedStore {
        inner: MemoryStore,
        gets: std::sync::Mutex<Vec<String>>,
    }

    impl SegmentAlignedStore {
        fn new() -> Self {
            Self {
                inner: MemoryStore::new(),
                gets: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn take_gets(&self) -> Vec<String> {
            std::mem::take(&mut *self.gets.lock().expect("gets lock"))
        }
    }

    fn segment_aligned(prefix: &str) -> String {
        let trimmed = prefix.trim_end_matches('/');
        if trimmed.is_empty() {
            String::new()
        } else {
            format!("{trimmed}/")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for SegmentAlignedStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            self.gets.lock().expect("gets lock").push(key.to_string());
            self.inner.get(key, range).await
        }

        async fn put_multipart<'a>(
            &'a self,
            key: &str,
        ) -> Result<Box<dyn ravel_object_store::MultipartUpload + 'a>, StoreError> {
            self.inner.put_multipart(key).await
        }

        async fn head(&self, key: &str) -> Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list(&segment_aligned(prefix), page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(&segment_aligned(prefix)).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// The wrapper lists the way the S3 adapter does: a prefix ending mid
    /// segment matches nothing, the same prefix's directory matches the key.
    #[tokio::test]
    async fn segment_aligned_store_lists_by_whole_segment() {
        let store = SegmentAlignedStore::new();
        store
            .put(
                "a/b.c",
                Bytes::from_static(b"x"),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put");
        let keys = |page: ravel_object_store::ListPage| -> Vec<String> {
            page.objects.into_iter().map(|m| m.key).collect()
        };
        assert!(
            keys(store.list("a/b.", None).await.expect("list"))
                .into_iter()
                .next()
                .is_none()
        );
        assert_eq!(
            keys(store.list("a", None).await.expect("list")),
            vec!["a/b.c".to_string()]
        );
    }

    /// The lookup finds a marker written at hour `H` for every reader hour
    /// `now` in `[H - skew, H + window]`, and for none outside it, on a store
    /// that lists the way S3 does. A lookup that lists the marker's
    /// `<keyhash32>.` prefix finds nothing there at any hour, because the
    /// store appends `/` to that prefix.
    #[tokio::test]
    async fn marker_is_found_across_the_window_on_a_segment_aligned_store() {
        let _probes = PROBE_GETS_COUNTER.lock().await;
        let store = SegmentAlignedStore::new();
        let tenant = tenant("acme");
        let written_at = 495_972u32;
        let window = 24u32;
        let skew = IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS;
        let stored = receipt(3, "v2:token-window");
        write_marker(
            &store,
            &tenant,
            Signal::Logs,
            b"window-key",
            written_at,
            &stored,
        )
        .await
        .expect("write");

        for now in (written_at - skew - 3)..=(written_at + window + 3) {
            let looked_up = read_marker(&store, &tenant, Signal::Logs, b"window-key", now, window)
                .await
                .expect("lookup");
            let in_window = now + skew >= written_at && now <= written_at + window;
            let expected = if in_window {
                LookupOutcome::Hit(stored.clone())
            } else {
                LookupOutcome::Miss
            };
            assert_eq!(
                looked_up, expected,
                "reader hour {now}, marker hour {written_at}, window {window}, skew {skew}"
            );
        }
    }

    /// Held by every test here that calls [`read_marker`], so a test reading
    /// a delta of the process-global probe counter counts only its own GETs.
    static PROBE_GETS_COUNTER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Every marker GET the lookup issues is counted once under its signal,
    /// hits and misses alike, and under no other signal. With a 24-hour
    /// window a keyed write's first lookup misses after 26 probes (the 24
    /// hours back, the current hour and the one forward skew hour); its
    /// retry, once the marker exists at the current hour, hits in the first
    /// batch after 3 probes (the forward skew hour, the current hour and the
    /// one before it).
    ///
    /// Non-vacuity: dropping the `fetch_add` in `probe_key` leaves both deltas
    /// at zero; counting once per batch instead of per probe counts 2 and 1.
    #[tokio::test]
    async fn each_marker_probe_get_is_counted_once_under_its_signal() {
        let _probes = PROBE_GETS_COUNTER.lock().await;
        let tenant = tenant("acme");
        let now = 495_972u32;
        let window = 24;
        for (signal, other) in [(Signal::Logs, Signal::Spans), (Signal::Spans, Signal::Logs)] {
            let store = SegmentAlignedStore::new();
            let other_before = idempotency_probe_gets(other);

            let before = idempotency_probe_gets(signal);
            let first = read_marker(&store, &tenant, signal, b"counted", now, window)
                .await
                .expect("lookup");
            assert_eq!(first, LookupOutcome::Miss);
            let miss_probes = idempotency_probe_gets(signal) - before;
            assert_eq!(
                miss_probes, 26,
                "{signal:?}: one probe per hour of the window"
            );
            assert_eq!(store.take_gets().len() as u64, miss_probes);

            let written = receipt(1, "v2:token-counted");
            write_marker(&store, &tenant, signal, b"counted", now, &written)
                .await
                .expect("write");
            store.take_gets();
            let before = idempotency_probe_gets(signal);
            let retry = read_marker(&store, &tenant, signal, b"counted", now, window)
                .await
                .expect("lookup");
            assert_eq!(retry, LookupOutcome::Hit(written));
            let hit_probes = idempotency_probe_gets(signal) - before;
            assert_eq!(hit_probes, 3, "{signal:?}: the first batch's probes");
            assert_eq!(store.take_gets().len() as u64, hit_probes);

            assert_eq!(idempotency_probe_gets(other), other_before);
        }
        assert_eq!(idempotency_probe_gets(Signal::Metrics), 0);
    }

    /// Sorted, so a batch's GETs compare as a set: the probes of one batch
    /// run concurrently and their issue order is not part of the contract.
    fn sorted(mut keys: Vec<String>) -> Vec<String> {
        keys.sort();
        keys
    }

    /// The hours of the first batch at reader hour `now`: the forward skew
    /// hours, `now` and `now - 1`.
    fn first_batch_hours(now: u32) -> Vec<u32> {
        (now - 1..=now + IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS).collect()
    }

    /// A hit in the first batch costs exactly that batch's GETs and reads
    /// nothing older: with markers at `now - 1` and `now - 3`, the newer
    /// receipt is returned after three GETs at the default skew.
    #[tokio::test]
    async fn a_hit_near_now_costs_only_the_first_batch() {
        let _probes = PROBE_GETS_COUNTER.lock().await;
        let store = SegmentAlignedStore::new();
        let tenant = tenant("acme");
        let now = 495_972u32;
        let older = receipt(1, "v2:token-older");
        let newer = receipt(2, "v2:token-newer");
        for (hour, receipt) in [(now - 3, &older), (now - 1, &newer)] {
            write_marker(&store, &tenant, Signal::Spans, b"two-hours", hour, receipt)
                .await
                .expect("write");
        }
        store.take_gets();

        let looked_up = read_marker(&store, &tenant, Signal::Spans, b"two-hours", now, 24)
            .await
            .expect("lookup");

        assert_eq!(looked_up, LookupOutcome::Hit(newer));
        let probe = |hour| marker_key(&tenant, Signal::Spans, b"two-hours", hour);
        let gets = store.take_gets();
        assert_eq!(gets.len(), 3);
        assert_eq!(
            sorted(gets),
            sorted(first_batch_hours(now).into_iter().map(probe).collect())
        );
    }

    /// A hit at `now - 10` misses the first batch and is found by the
    /// fan-out, which probes every remaining hour once: 26 GETs at the
    /// 24-hour default, the newest of the fan-out's markers winning.
    #[tokio::test]
    async fn a_hit_below_the_first_batch_costs_the_full_fan_out() {
        let _probes = PROBE_GETS_COUNTER.lock().await;
        let store = SegmentAlignedStore::new();
        let tenant = tenant("acme");
        let now = 495_972u32;
        let newest = receipt(10, "v2:token-ten");
        for (hour, receipt) in [
            (now - 20, receipt(20, "v2:token-twenty")),
            (now - 10, newest.clone()),
            (now - 14, receipt(14, "v2:token-fourteen")),
        ] {
            write_marker(&store, &tenant, Signal::Logs, b"fan-out", hour, &receipt)
                .await
                .expect("write");
        }
        store.take_gets();

        let looked_up = read_marker(&store, &tenant, Signal::Logs, b"fan-out", now, 24)
            .await
            .expect("lookup");

        assert_eq!(looked_up, LookupOutcome::Hit(newest));
        assert_eq!(store.take_gets().len(), 26);
    }

    /// A miss probes every hour of the window exactly once:
    /// `dedup_window_hours + IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS + 1`
    /// GETs, 26 at the 24-hour default. The first batch's GETs are all issued
    /// before any of the fan-out's.
    #[tokio::test]
    async fn a_miss_probes_each_hour_of_the_window_once() {
        let _probes = PROBE_GETS_COUNTER.lock().await;
        let store = SegmentAlignedStore::new();
        let tenant = tenant("acme");
        let now = 495_972u32;

        let looked_up = read_marker(&store, &tenant, Signal::Logs, b"absent", now, 24)
            .await
            .expect("lookup");

        assert_eq!(looked_up, LookupOutcome::Miss);
        let probe = |hour| marker_key(&tenant, Signal::Logs, b"absent", hour);
        let expected: Vec<String> = (now - 24..=now + IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS)
            .map(probe)
            .collect();
        assert_eq!(expected.len(), 26);
        let gets = store.take_gets();
        assert_eq!(sorted(gets.clone()), sorted(expected));
        assert_eq!(
            sorted(gets[..3].to_vec()),
            sorted(first_batch_hours(now).into_iter().map(probe).collect())
        );
    }

    /// The batched lookup returns, for every store state tried, exactly what
    /// a sequential newest-first scan stopping at the first object or the
    /// first store error returns: markers at several hours, some of them
    /// corrupt, some hours whose GET the store refuses, read from reader hours
    /// on both sides of each.
    #[tokio::test]
    async fn batched_lookup_matches_a_sequential_newest_first_scan() {
        let _probes = PROBE_GETS_COUNTER.lock().await;
        use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};

        /// `Err` carries the key of the refused GET.
        async fn sequential(
            store: &dyn ObjectStoreBackend,
            tenant: &TenantId,
            now: u32,
            window: u32,
        ) -> Result<LookupOutcome, String> {
            let newest = now + IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS;
            for hour in (now.saturating_sub(window)..=newest).rev() {
                let key = marker_key(tenant, Signal::Logs, b"same", hour);
                match read_marker_at(store, &key).await {
                    Err(_) => return Err(key),
                    Ok(LookupOutcome::Miss) => {}
                    Ok(found) => return Ok(found),
                }
            }
            Ok(LookupOutcome::Miss)
        }

        let tenant = tenant("acme");
        let base = 495_972u32;
        // (marker hour offsets from base, offsets whose marker is corrupt,
        // offsets whose GET the store refuses)
        let states: [(&[u32], &[u32], &[u32]); 10] = [
            (&[0], &[], &[]),
            (&[0, 1, 2, 5, 11, 23], &[], &[]),
            (&[3, 9, 17], &[9], &[]),
            (&[2, 7, 30], &[2], &[]),
            (&[], &[], &[]),
            (&[], &[], &[4]),
            (&[10], &[], &[9, 11]),
            (&[3, 20], &[3], &[2, 4, 19]),
            (&[0, 6, 12, 18, 24], &[], &[1, 7, 13, 25, 30]),
            (&[15], &[], &[14, 15, 16]),
        ];
        let mut refused = 0usize;
        let mut found_over_an_older_fault = 0usize;
        for (markers, corrupt, faulted) in states {
            let plan = faulted.iter().fold(FaultPlan::empty(), |plan, &offset| {
                plan.with_rule(
                    Rule::new(Op::Get, ScriptedFault::Permanent("AccessDenied".into()))
                        .with_key_contains(marker_key(
                            &tenant,
                            Signal::Logs,
                            b"same",
                            base + offset,
                        )),
                )
            });
            let store = FaultStore::new(MemoryStore::new(), plan);
            for &offset in markers {
                let hour = base + offset;
                let key = marker_key(&tenant, Signal::Logs, b"same", hour);
                let body = if corrupt.contains(&offset) {
                    b"RIDM-not-a-marker".to_vec()
                } else {
                    encode_marker(&receipt(u64::from(offset), &format!("v2:token-{offset}")))
                        .expect("encode")
                };
                store
                    .put(&key, Bytes::from(body), PutOptions::create_if_absent())
                    .await
                    .expect("seed marker");
            }
            for window in [0u32, 1, 2, 5, 24] {
                for now in base..=base + 40 {
                    let batched = read_marker(&store, &tenant, Signal::Logs, b"same", now, window)
                        .await
                        .map_err(|err| err.key);
                    let expected = sequential(&store, &tenant, now, window).await;
                    assert_eq!(
                        batched, expected,
                        "markers {markers:?}, corrupt {corrupt:?}, faulted {faulted:?}, \
                         reader {now}, window {window}"
                    );
                    let oldest = now.saturating_sub(window);
                    let found_at = markers
                        .iter()
                        .map(|&offset| base + offset)
                        .filter(|&hour| {
                            hour >= oldest && hour <= now + IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS
                        })
                        .max();
                    match (&expected, found_at) {
                        (Err(_), _) => refused += 1,
                        (Ok(_), Some(found_at))
                            if faulted
                                .iter()
                                .any(|&offset| (oldest..found_at).contains(&(base + offset))) =>
                        {
                            found_over_an_older_fault += 1;
                        }
                        _ => {}
                    }
                }
            }
        }
        // Both sides of the decision rule were exercised, not only fault-free states.
        assert!(refused > 0);
        assert!(found_over_an_older_fault > 0);
    }

    /// A store whose GET records how many GETs are in flight, then yields a
    /// few times before answering, so concurrently issued probes overlap.
    struct InFlightStore {
        inner: MemoryStore,
        in_flight: std::sync::atomic::AtomicUsize,
        max_in_flight: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for InFlightStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            use std::sync::atomic::Ordering;
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            self.inner.get(key, range).await
        }

        async fn put_multipart<'a>(
            &'a self,
            key: &str,
        ) -> Result<Box<dyn ravel_object_store::MultipartUpload + 'a>, StoreError> {
            self.inner.put_multipart(key).await
        }

        async fn head(&self, key: &str) -> Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list(prefix, page).await
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

    /// The probes run concurrently and never more than
    /// `IDEM_MARKER_PROBE_CONCURRENCY` at once: a miss over the default window
    /// reaches exactly that many GETs in flight, and a hit at `now` reaches
    /// the first batch's three.
    #[tokio::test]
    async fn probes_run_concurrently_up_to_the_bound() {
        let _probes = PROBE_GETS_COUNTER.lock().await;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let tenant = tenant("acme");
        let now = 495_972u32;
        let store = InFlightStore {
            inner: MemoryStore::new(),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
        };

        let missed = read_marker(&store, &tenant, Signal::Logs, b"bound", now, 24)
            .await
            .expect("lookup");
        assert_eq!(missed, LookupOutcome::Miss);
        assert_eq!(
            store.max_in_flight.swap(0, Ordering::SeqCst),
            IDEM_MARKER_PROBE_CONCURRENCY
        );

        write_marker(
            &store,
            &tenant,
            Signal::Logs,
            b"bound",
            now,
            &receipt(1, "v2:token-now"),
        )
        .await
        .expect("write");
        let hit = read_marker(&store, &tenant, Signal::Logs, b"bound", now, 24)
            .await
            .expect("lookup");
        assert!(matches!(hit, LookupOutcome::Hit(_)), "{hit:?}");
        assert_eq!(store.max_in_flight.load(Ordering::SeqCst), 3);
    }

    /// A probe that fails with anything but `NotFound` fails the lookup, even
    /// when an older hour of the same batch holds a marker that batch read:
    /// the lookup cannot tell a newer marker from none at the hour it could
    /// not read. The error names the key whose GET failed and carries the
    /// store error.
    #[tokio::test]
    async fn a_failed_probe_fails_the_lookup() {
        let _probes = PROBE_GETS_COUNTER.lock().await;
        use ravel_object_store::fault::{
            FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault,
        };

        let tenant = tenant("acme");
        let now = 495_972u32;
        let failing = marker_key(&tenant, Signal::Logs, b"faulted", now - 2);
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Permanent("AccessDenied".into()))
                .with_key_contains(failing.clone()),
        );
        let store = FaultStore::new(SegmentAlignedStore::new(), plan);
        let below = marker_key(&tenant, Signal::Logs, b"faulted", now - 5);
        write_marker(
            &store,
            &tenant,
            Signal::Logs,
            b"faulted",
            now - 5,
            &receipt(9, "v2:token-below"),
        )
        .await
        .expect("write");

        let err = read_marker(&store, &tenant, Signal::Logs, b"faulted", now, 24)
            .await
            .expect_err("a failed probe must fail the lookup");

        assert_eq!(err.key, failing);
        assert!(
            matches!(&err.source, StoreError::Permanent(text) if text == "AccessDenied"),
            "the store error must propagate untouched, got {err:?}"
        );
        assert_eq!(store.fault_count(Op::Get, FaultKind::Permanent), 1);
        // The fault answers the failed probe before the wrapped store sees it.
        // The marker below it was read, and still did not mask the failure.
        let gets = store.inner().take_gets();
        assert_eq!(gets.len(), 25);
        assert!(!gets.contains(&failing));
        assert!(gets.contains(&below));
    }

    /// An error in the first batch at an hour newer than the batch's marker
    /// fails the lookup: a refused GET at `now` is not masked by a marker at
    /// `now - 1`, since the newest-first scan would have stopped at `now`.
    #[tokio::test]
    async fn an_error_newer_than_a_hit_in_the_same_batch_fails_the_lookup() {
        let _probes = PROBE_GETS_COUNTER.lock().await;
        use ravel_object_store::fault::{
            FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault,
        };

        let tenant = tenant("acme");
        let now = 495_972u32;
        let failing = marker_key(&tenant, Signal::Logs, b"masked", now);
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Permanent("AccessDenied".into()))
                .with_key_contains(failing.clone()),
        );
        let store = FaultStore::new(SegmentAlignedStore::new(), plan);
        write_marker(
            &store,
            &tenant,
            Signal::Logs,
            b"masked",
            now - 1,
            &receipt(1, "v2:token-behind"),
        )
        .await
        .expect("write");

        let err = read_marker(&store, &tenant, Signal::Logs, b"masked", now, 24)
            .await
            .expect_err("an error newer than a hit must fail the lookup");

        assert_eq!(err.key, failing);
        assert_eq!(store.fault_count(Op::Get, FaultKind::Permanent), 1);
        let gets = store.inner().take_gets();
        assert_eq!(gets.len(), 2);
        assert!(gets.contains(&marker_key(&tenant, Signal::Logs, b"masked", now - 1)));
    }

    /// A marker at `now` wins over a refused GET at `now - 1` in the same
    /// batch: the newest-first scan stops at the marker and never reaches the
    /// older hour, so the error there does not turn a replay into a refusal.
    #[tokio::test]
    async fn a_newer_hit_wins_over_an_older_error_in_the_same_batch() {
        let _probes = PROBE_GETS_COUNTER.lock().await;
        use ravel_object_store::fault::{
            FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault,
        };

        let tenant = tenant("acme");
        let now = 495_972u32;
        let failing = marker_key(&tenant, Signal::Logs, b"replay", now - 1);
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Permanent("AccessDenied".into()))
                .with_key_contains(failing),
        );
        let store = FaultStore::new(SegmentAlignedStore::new(), plan);
        let stored = receipt(4, "v2:token-now");
        write_marker(&store, &tenant, Signal::Logs, b"replay", now, &stored)
            .await
            .expect("write");

        let looked_up = read_marker(&store, &tenant, Signal::Logs, b"replay", now, 24)
            .await
            .expect("an older error must not fail a lookup a newer marker decides");

        assert_eq!(looked_up, LookupOutcome::Hit(stored));
        // The faulted probe ran: the batch still issued it.
        assert_eq!(store.fault_count(Op::Get, FaultKind::Permanent), 1);
        assert_eq!(store.inner().take_gets().len(), 2);
    }

    #[tokio::test]
    async fn marker_replay_returns_stored_receipt() {
        let _probes = PROBE_GETS_COUNTER.lock().await;
        let store = MemoryStore::new();
        let tenant = tenant("acme");
        // A representative multi-shard ack: the x-ravel-commit-token header
        // carries one token per shard, comma-separated, not a single token.
        let receipt = receipt(42, "v2:token-abc,v2:token-def,v2:token-ghi");

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
        let _probes = PROBE_GETS_COUNTER.lock().await;
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

    #[tokio::test]
    async fn write_marker_race_loser_surfaces_readback_failure() {
        use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};

        let tenant = tenant("acme");
        let ingest_hour_bucket = 495_972;
        let key = marker_key(&tenant, Signal::Logs, b"race-key", ingest_hour_bucket);

        // The Get rule only fires once the winner's marker already exists at
        // `key`, modeling: this call loses the CreateIfAbsent race, then the
        // mandatory read-back of the winner's marker itself fails.
        let plan = FaultPlan::empty()
            .with_rule(Rule::new(Op::Get, ScriptedFault::Timeout).with_key_contains(key.clone()));
        let store = FaultStore::new(MemoryStore::new(), plan);

        let winner = receipt(10, "v2:token-first");
        store
            .inner()
            .put(
                &key,
                Bytes::from(encode_marker(&winner).expect("encode must succeed")),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("seeding the winner's marker must succeed");

        let loser = receipt(20, "v2:token-second");
        let err = write_marker(
            &store,
            &tenant,
            Signal::Logs,
            b"race-key",
            ingest_hour_bucket,
            &loser,
        )
        .await
        .expect_err("the loser's read-back must surface the fault, not a receipt");

        assert!(
            matches!(err, MarkerWriteError::Store(StoreError::Timeout)),
            "expected the read-back's fault to propagate untouched, got {err:?}"
        );
        assert_eq!(
            store.fault_count(Op::Get, ravel_object_store::fault::FaultKind::Timeout),
            1,
            "the fault must actually have fired, not passed through"
        );
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

        // Uniform-random bytes almost never clear the magic check, so that
        // alone barely exercises decode_marker past its first branch.
        // Instead start from a valid encoded frame and tamper it three
        // distinct ways, still asserting only "never panics": truncation,
        // a single flipped byte anywhere (header or payload), and a
        // token-length prefix set independently of what actually follows
        // it (the one field decode_receipt trusts most).
        #[test]
        fn decode_never_panics_on_truncated_frame(
            written_count in any::<u64>(),
            commit_token in "[a-zA-Z0-9:_-]{0,64}",
            cut_at in any::<usize>(),
        ) {
            let original = IdempotencyReceipt { written_count, commit_token };
            let encoded = encode_marker(&original).expect("encode must succeed for a reasonable token");
            let cut = cut_at % (encoded.len() + 1);
            let _ = decode_marker(&encoded[..cut]);
        }

        #[test]
        fn decode_never_panics_on_single_bit_flip(
            written_count in any::<u64>(),
            commit_token in "[a-zA-Z0-9:_-]{0,64}",
            flip_at in any::<usize>(),
            flip_bit in 0u8..8,
        ) {
            let original = IdempotencyReceipt { written_count, commit_token };
            let mut encoded = encode_marker(&original).expect("encode must succeed for a reasonable token");
            let idx = flip_at % encoded.len();
            encoded[idx] ^= 1 << flip_bit;
            let _ = decode_marker(&encoded);
        }

        // Tamper the length prefix directly against decode_receipt (below
        // the crc, which would otherwise catch the mismatch first and mask
        // this branch): the length prefix must never be trusted past what
        // actually remains in the payload.
        #[test]
        fn decode_never_panics_on_tampered_token_length(
            written_count in any::<u64>(),
            commit_token in "[a-zA-Z0-9:_-]{0,64}",
            bogus_len in any::<u16>(),
        ) {
            let original = IdempotencyReceipt { written_count, commit_token };
            let mut payload = encode_receipt(&original).expect("encode must succeed for a reasonable token");
            payload[8..10].copy_from_slice(&bogus_len.to_le_bytes());
            let _ = decode_receipt(&payload);
        }
    }
}
