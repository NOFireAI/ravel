//! Snappy block-format decompression with a decompressed-size cap enforced
//! before allocation (ADR-0015; same discipline as the OTAP zstd caps).
//!
//! Remote Write bodies are snappy block format (not the framed format), so
//! `snap::raw` is used directly. `decompress_len` reads only the varint
//! header to report the decompressed size, which lets the cap be checked
//! before the output buffer is allocated: a hostile or misconfigured sender
//! cannot force an oversized allocation just by claiming one in the header.

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SnappyError {
    #[error("snappy header is malformed: {0}")]
    InvalidHeader(String),
    #[error("decompressed size {actual} bytes exceeds the cap of {cap} bytes")]
    TooLarge { actual: usize, cap: usize },
    #[error("snappy body is corrupt: {0}")]
    Corrupt(String),
}

/// Decompress `input` (snappy block format), rejecting it before allocating
/// the output buffer if the header claims more than `cap` decompressed
/// bytes.
pub fn decompress(input: &[u8], cap: usize) -> Result<Vec<u8>, SnappyError> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    let len = snap::raw::decompress_len(input)
        .map_err(|err| SnappyError::InvalidHeader(err.to_string()))?;
    if len > cap {
        return Err(SnappyError::TooLarge { actual: len, cap });
    }
    let mut out = vec![0u8; len];
    let mut decoder = snap::raw::Decoder::new();
    let written = decoder
        .decompress(input, &mut out)
        .map_err(|err| SnappyError::Corrupt(err.to_string()))?;
    out.truncate(written);
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn compress(input: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new()
            .compress_vec(input)
            .expect("compress")
    }

    #[test]
    fn roundtrip() {
        let original = b"hello remote write hello remote write hello remote write";
        let compressed = compress(original);
        let decompressed = decompress(&compressed, 1_000_000).expect("decompress");
        assert_eq!(decompressed, original);
    }

    #[test]
    fn empty_input_roundtrips() {
        let compressed = compress(b"");
        let decompressed = decompress(&compressed, 1_000).expect("decompress");
        assert!(decompressed.is_empty());
    }

    #[test]
    fn cap_enforced_before_allocation() {
        let original = vec![b'x'; 10_000];
        let compressed = compress(&original);
        let err = decompress(&compressed, 100).expect_err("must be capped");
        assert_eq!(
            err,
            SnappyError::TooLarge {
                actual: 10_000,
                cap: 100
            }
        );
    }

    #[test]
    fn cap_exactly_at_decompressed_size_passes() {
        let original = vec![b'x'; 100];
        let compressed = compress(&original);
        let decompressed = decompress(&compressed, 100).expect("decompress");
        assert_eq!(decompressed.len(), 100);
    }

    #[test]
    fn empty_input_is_not_an_error() {
        // Snappy block format treats a zero-length input as a valid,
        // zero-length payload rather than a missing header.
        let decompressed = decompress(&[], 1_000).expect("decompress");
        assert!(decompressed.is_empty());
    }

    #[test]
    fn malformed_header_is_typed_error_not_panic() {
        // A varint header with the continuation bit set on every byte never
        // terminates within the 5-byte limit for a uint32 length.
        let err = decompress(&[0xff; 6], 1_000).expect_err("unterminated varint header");
        assert!(matches!(err, SnappyError::InvalidHeader(_)));
    }

    #[test]
    fn truncated_body_is_typed_error_not_panic() {
        let original = vec![b'x'; 1_000];
        let compressed = compress(&original);
        // Keep the header (claims 1000 bytes) but cut the body short.
        let truncated = &compressed[..compressed.len() / 2];
        let err = decompress(truncated, 1_000_000).expect_err("truncated body must not decode");
        assert!(matches!(err, SnappyError::Corrupt(_)));
    }

    proptest::proptest! {
        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..2048)) {
            let _ = decompress(&bytes, 10_000_000);
        }

        #[test]
        fn mutated_valid_payload_never_panics(
            original in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..512),
            flip_index in proptest::prelude::any::<usize>(),
            flip_byte in proptest::prelude::any::<u8>(),
        ) {
            let mut compressed = compress(&original);
            if !compressed.is_empty() {
                let idx = flip_index % compressed.len();
                compressed[idx] ^= flip_byte;
            }
            let _ = decompress(&compressed, 10_000_000);
        }
    }
}
