//! `ravel-cli idem inspect` (ADR-0051 section 5, issue #532 / EB-12): decode
//! and render one idempotency marker object by its exact key
//! (`t/<tenant_hash>/<signal>/idem/<keyhash32>.<ingest_hour>.idm`).
//!
//! Decoding goes through `ravel_ingest::idempotency::decode_marker`, the same
//! function the ingest path itself uses (via `read_marker`) to interpret a
//! marker's bytes. There is exactly one decoder for this frozen, checksummed
//! format; this module must never fork a second copy, or a future version
//! bump could update one and silently leave the other reporting stale
//! results (a marker the ingest path treats as corrupt could report `valid`
//! here, or vice versa).
use std::sync::Arc;

use ravel_ingest::decode_marker;
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};

/// Render a decoded marker's fields for `idem inspect`'s stdout report.
/// `commit_token` is the raw stored comma-joined string: split without
/// filtering empty fields, so a malformed value like `"a,,b"` is shown
/// faithfully (three fields, one empty) rather than silently collapsed to
/// two -- this is a diagnostic tool, and hiding an anomaly defeats it.
fn render(receipt: &ravel_ingest::IdempotencyReceipt) -> String {
    let tokens: Vec<&str> = if receipt.commit_token.is_empty() {
        Vec::new()
    } else {
        receipt.commit_token.split(',').collect()
    };
    format!(
        "magic: valid (RIDM)\nversion: valid\ncrc32c: valid\nwritten_count: {}\ncommit_tokens: [{}]",
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
        let err = inspect(
            store,
            "t/deadbeefdeadbeefdeadbeefdeadbeef/l/idem/none.0495972.idm",
        )
        .await
        .expect_err("a missing marker must error, not print a false report");
        assert!(err.to_string().contains("no marker object"), "err: {err}");
    }

    /// Encodes a raw marker frame matching the frozen wire format
    /// (docs/catalog-and-mvcc.md "Idempotency marker body layout"), for
    /// exercising decode failure modes `renders_marker_fields` and the
    /// existing corrupt/truncated tests don't reach: `decode_marker` is
    /// reused directly by `inspect` (no second copy in this crate), so this
    /// only needs to build bytes, not duplicate any decode logic.
    fn raw_marker(magic: &[u8; 4], version: u16, written_count: u64, token: &str) -> Vec<u8> {
        let token_bytes = token.as_bytes();
        let mut payload = Vec::with_capacity(10 + token_bytes.len());
        payload.extend_from_slice(&written_count.to_le_bytes());
        payload.extend_from_slice(&(token_bytes.len() as u16).to_le_bytes());
        payload.extend_from_slice(token_bytes);

        let mut header = Vec::with_capacity(10);
        header.extend_from_slice(magic);
        header.extend_from_slice(&version.to_le_bytes());
        let crc = crc32c::crc32c_append(crc32c::crc32c(&header), &payload);
        header.extend_from_slice(&crc.to_le_bytes());

        let mut out = header;
        out.extend_from_slice(&payload);
        out
    }

    async fn put_raw(store: &dyn ObjectStoreBackend, key: &str, bytes: Vec<u8>) {
        store
            .put(
                key,
                bytes::Bytes::from(bytes),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("put must succeed");
    }

    #[tokio::test]
    async fn bad_magic_reports_specific_reason() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let key = "t/deadbeefdeadbeefdeadbeefdeadbeef/l/idem/abc.0495972.idm";
        put_raw(store.as_ref(), key, raw_marker(b"XXXX", 1, 1, "v2:token")).await;

        let err = inspect(store, key)
            .await
            .expect_err("a bad-magic marker must be a typed, non-zero failure, not a panic");
        assert!(
            err.to_string().contains("magic"),
            "expected the specific BadMagic reason, got: {err}"
        );
    }

    #[tokio::test]
    async fn unsupported_version_reports_specific_reason() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let key = "t/deadbeefdeadbeefdeadbeefdeadbeef/l/idem/abc.0495972.idm";
        put_raw(store.as_ref(), key, raw_marker(b"RIDM", 2, 1, "v2:token")).await;

        let err = inspect(store, key)
            .await
            .expect_err("an unsupported-version marker must be a typed, non-zero failure");
        assert!(
            err.to_string().contains("version"),
            "expected the specific UnsupportedVersion reason, got: {err}"
        );
    }

    #[tokio::test]
    async fn malformed_receipt_token_length_mismatch_reports_specific_reason() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let key = "t/deadbeefdeadbeefdeadbeefdeadbeef/l/idem/abc.0495972.idm";

        // Build a frame, then lie about the token-length prefix and
        // recompute the crc over the now-inconsistent payload, so the frame
        // passes the crc check and reaches decode_receipt's own length
        // check (`rest.len() != token_len`) instead of failing earlier.
        let mut bytes = raw_marker(b"RIDM", 1, 1, "v2:token");
        let token_len_offset = 10 + 8; // header (10) + written_count (8)
        let real_len = u16::from_le_bytes([bytes[token_len_offset], bytes[token_len_offset + 1]]);
        let lied_len = real_len + 5;
        bytes[token_len_offset..token_len_offset + 2].copy_from_slice(&lied_len.to_le_bytes());
        let header = bytes[..6].to_vec();
        let payload = bytes[10..].to_vec();
        let crc = crc32c::crc32c_append(crc32c::crc32c(&header), &payload);
        bytes[6..10].copy_from_slice(&crc.to_le_bytes());

        put_raw(store.as_ref(), key, bytes).await;

        let err = inspect(store, key)
            .await
            .expect_err("a token-length lie must be a typed, non-zero failure, not a panic");
        assert!(
            err.to_string().contains("length prefix"),
            "expected the specific MalformedReceipt reason, got: {err}"
        );
    }
}
