//! `ravel-cli idem inspect` (ADR-0051 section 5, issue #532 / EB-12): decode
//! and render one idempotency marker object by its exact key
//! (`t/<tenant_hash>/<signal>/idem/<keyhash32>.<ingest_hour>.idm`).
//!
//! The wire layout mirrored here (`RIDM` magic, u16 version, crc32c over
//! `magic || version || payload`, then `written_count` / commit-token-length
//! / commit-token payload) is the frozen contract documented in
//! docs/catalog-and-mvcc.md ("Idempotency marker body layout") and encoded by
//! `ravel_ingest::idempotency`. That module's own encoder/decoder pair is
//! private to the crate (only `write_marker`, `read_marker`, `marker_key`,
//! and the `IdempotencyReceipt` / `MarkerError` / `LookupOutcome` types are
//! exported), and `read_marker`'s public `LookupOutcome::Corrupt` collapses
//! every decode failure to one variant with no detail retained. Neither
//! exposes what this inspector needs: the specific `MarkerError` for a given
//! marker's bytes. This module therefore re-implements the documented decode
//! against the normative doc rather than `ravel_ingest`'s own (private)
//! decoder; see the final task report for the upstream visibility gap this
//! works around.
use std::sync::Arc;

use ravel_ingest::{IdempotencyReceipt, MarkerError};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};

const MAGIC: [u8; 4] = *b"RIDM";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 10;
/// `written_count` (u64) + commit-token length prefix (u16).
const RECEIPT_HEADER_LEN: usize = 10;

/// Decode a receipt payload (post-header bytes): `written_count` (u64 LE),
/// then a u16 LE length prefix, then that many UTF-8 bytes of comma-joined
/// commit tokens. Mirrors `ravel_ingest::idempotency`'s private
/// `decode_receipt`.
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

/// Decode and verify a marker body. Every failure mode (truncation, bad
/// magic, unsupported version, checksum mismatch, malformed payload) is a
/// typed `MarkerError`, never a panic. Mirrors `ravel_ingest::idempotency`'s
/// private `decode_marker`.
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
    // Coverage is magic || version || payload: the crc field itself
    // (header[6..10]) is excluded, matching docs/catalog-and-mvcc.md.
    let computed_crc = crc32c::crc32c_append(crc32c::crc32c(&header[0..6]), payload);
    if stored_crc != computed_crc {
        return Err(MarkerError::ChecksumMismatch {
            stored: stored_crc,
            computed: computed_crc,
        });
    }
    decode_receipt(payload)
}

/// Render a decoded marker's fields for `idem inspect`'s stdout report.
fn render(receipt: &IdempotencyReceipt) -> String {
    let tokens: Vec<&str> = receipt
        .commit_token
        .split(',')
        .filter(|t| !t.is_empty())
        .collect();
    format!(
        "magic: valid (RIDM)\nversion: valid ({VERSION})\ncrc32c: valid\nwritten_count: {}\ncommit_tokens: [{}]",
        receipt.written_count,
        tokens.join(", ")
    )
}

/// `idem inspect <key>`: GET the exact marker object and decode it. A
/// missing object, a store error, or a decode failure are all typed,
/// returned as `Err`, and never panic; the specific `MarkerError` variant
/// (truncated, bad magic, unsupported version, checksum mismatch, or
/// malformed payload) is carried in the error's `Display`.
pub async fn inspect(store: Arc<dyn ObjectStoreBackend>, key: &str) -> anyhow::Result<String> {
    let outcome = match store.get(key, GetRange::Full).await {
        Ok(outcome) => outcome,
        Err(StoreError::NotFound) => anyhow::bail!("no marker object at key {key}"),
        Err(err) => anyhow::bail!("failed to fetch marker at {key}: {err}"),
    };
    let receipt = decode_marker(&outcome.data)
        .map_err(|err| anyhow::anyhow!("marker at {key} failed to decode: {err}"))?;
    Ok(render(&receipt))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_ingest::{IdempotencyReceipt as Receipt, marker_key, write_marker};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{PutMode, PutOptions};
    use ravel_types::{Signal, TenantId};

    #[tokio::test]
    async fn renders_marker_fields() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = TenantId::new("acme");
        let ingest_hour_bucket = 495_972;
        let receipt = Receipt {
            written_count: 42,
            commit_token: "v2:token-abc,v2:token-def".to_string(),
        };

        write_marker(
            store.as_ref(),
            &tenant,
            Signal::Logs,
            b"client-key-1",
            ingest_hour_bucket,
            &receipt,
        )
        .await
        .expect("write must succeed");

        let key = marker_key(&tenant, Signal::Logs, b"client-key-1", ingest_hour_bucket);
        let report = inspect(store, &key).await.expect("inspect must succeed");

        assert!(report.contains("written_count: 42"), "report: {report}");
        assert!(
            report.contains("commit_tokens: [v2:token-abc, v2:token-def]"),
            "report: {report}"
        );
        assert!(report.contains("magic: valid"), "report: {report}");
        assert!(report.contains("crc32c: valid"), "report: {report}");
    }

    #[tokio::test]
    async fn corrupt_marker_reports_specific_reason() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = TenantId::new("acme");
        let ingest_hour_bucket = 495_972;
        let receipt = Receipt {
            written_count: 7,
            commit_token: "v2:token-xyz".to_string(),
        };

        write_marker(
            store.as_ref(),
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
        // Flip a byte past the fixed header so the stored crc32c no longer
        // matches, same technique as
        // `ravel_ingest::idempotency::tests::corrupt_marker_is_typed_miss`.
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        store
            .put(
                &key,
                bytes::Bytes::from(bytes),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("overwrite must succeed");

        let err = inspect(store, &key)
            .await
            .expect_err("a corrupt marker must be a typed, non-zero failure, not a panic");
        assert!(
            err.to_string().contains("checksum mismatch"),
            "expected the specific ChecksumMismatch reason, got: {err}"
        );
    }

    #[tokio::test]
    async fn truncated_marker_reports_truncated_reason() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let key = "t/deadbeefdeadbeefdeadbeefdeadbeef/l/idem/abc.0495972.idm";
        store
            .put(
                key,
                bytes::Bytes::from_static(b"RI"),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("put must succeed");

        let err = inspect(store, key)
            .await
            .expect_err("a truncated marker must be a typed, non-zero failure, not a panic");
        assert!(
            err.to_string().contains("truncated"),
            "expected the specific Truncated reason, got: {err}"
        );
    }

    #[tokio::test]
    async fn missing_marker_is_a_typed_error() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let err = inspect(store, "t/deadbeefdeadbeefdeadbeefdeadbeef/l/idem/none.0495972.idm")
            .await
            .expect_err("a missing marker must error, not print a false report");
        assert!(err.to_string().contains("no marker object"), "err: {err}");
    }
}
