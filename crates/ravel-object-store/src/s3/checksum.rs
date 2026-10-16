//! Read-side checksum verification for the S3 adapter (ADR-1696 decisions 2
//! to 4).
//!
//! S3 stores the checksum a PUT attached (`x-amz-checksum-crc64nvme` or
//! `-sha256` under [`crate::s3::UploadIntegrity`]) alongside the object and
//! returns it on a GET issued with `x-amz-checksum-mode: ENABLED`. This module
//! turns that response header into something the adapter can recompute over the
//! bytes it received, so a body corrupted at rest or in transit surfaces as
//! [`StoreError::Corrupted`] before it reaches a caller.
//!
//! Two digests are recomputable here: CRC-64/NVME (the algorithm ADR-1696
//! decision 1 makes the write-side default) and CRC-32C (`crc32c`, already a
//! dependency of this crate). SHA-256 is not: no SHA-2 implementation is a
//! workspace dependency, so a `x-amz-checksum-sha256` response is reported as
//! [`ObservedChecksum::Unsupported`] and the read is counted unverified rather
//! than being claimed as verified. A *composite* multipart digest (S3 suffixes
//! those with `-{part count}`) is unsupported for a different reason: it is a
//! checksum of part checksums, not of the body, so nothing recomputed over the
//! bytes could match it.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

use crate::StoreError;

/// Request header that asks S3 to return the checksum it stored at upload.
/// Sent on every request (`client_options` installs it as a default header),
/// because `object_store` 0.14 has exactly one hook that lands a header in the
/// request *before* SigV4 signs it and that hook is whole-client. It is only
/// meaningful on `GetObject`/`HeadObject`; S3 ignores it elsewhere.
pub(crate) const CHECKSUM_MODE_HEADER: &str = "x-amz-checksum-mode";

/// The value [`CHECKSUM_MODE_HEADER`] carries.
pub(crate) const CHECKSUM_MODE_ENABLED: &str = "ENABLED";

/// Response header prefix S3 returns a stored checksum under. The
/// `-algorithm` header is a *request* header naming an algorithm and carries no
/// digest, so it is excluded by name below.
const CHECKSUM_HEADER_PREFIX: &str = "x-amz-checksum-";

/// Reflected CRC-64/NVME polynomial: the bit reverse of the catalogue's
/// `0xad93d23594c93659`. Init and final xor are both `!0`, refin/refout true,
/// which the `check` vector in this module's tests pins.
const CRC64_NVME_POLY: u64 = 0x9a6c_9329_ac4b_c9b5;

/// Byte-at-a-time table for [`crc64_nvme`], built at compile time. A bitwise
/// loop costs eight shifts per byte, which a whole-object verification pays on
/// every full-object GET; the table makes it one lookup per byte.
const CRC64_NVME_TABLE: [u64; 256] = {
    let mut table = [0u64; 256];
    let mut index = 0usize;
    while index < 256 {
        let mut crc = index as u64;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ CRC64_NVME_POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
};

/// CRC-64/NVME over `data`, the algorithm behind `x-amz-checksum-crc64nvme`.
pub(crate) fn crc64_nvme(data: &[u8]) -> u64 {
    let mut crc = !0u64;
    for &byte in data {
        let index = ((crc ^ u64::from(byte)) & 0xff) as usize;
        crc = (crc >> 8) ^ CRC64_NVME_TABLE[index];
    }
    !crc
}

/// A stored checksum this adapter can recompute over a response body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoredChecksum {
    Crc64Nvme(u64),
    Crc32c(u32),
}

impl StoredChecksum {
    /// Recompute over `data` and compare. A mismatch is
    /// [`StoreError::Corrupted`], the variant the contract reserves for a
    /// checksum mismatch, so no reader needs a new error arm.
    pub(crate) fn verify(self, key: &str, data: &[u8]) -> Result<(), StoreError> {
        match self {
            StoredChecksum::Crc64Nvme(expected) => {
                let actual = crc64_nvme(data);
                if actual != expected {
                    return Err(StoreError::Corrupted(format!(
                        "get of {key}: stored crc64nvme {expected:016x} does not match \
                         {actual:016x} computed over the {} bytes received",
                        data.len()
                    )));
                }
            }
            StoredChecksum::Crc32c(expected) => {
                let actual = crc32c::crc32c(data);
                if actual != expected {
                    return Err(StoreError::Corrupted(format!(
                        "get of {key}: stored crc32c {expected:08x} does not match \
                         {actual:08x} computed over the {} bytes received",
                        data.len()
                    )));
                }
            }
        }
        Ok(())
    }
}

/// What a GET response said about the checksum S3 has stored for the object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ObservedChecksum {
    /// No `x-amz-checksum-*` header: the endpoint stores no checksum for this
    /// object, or ignored `x-amz-checksum-mode`. ADR-1696 decision 3 serves the
    /// body and counts it unverified rather than refusing it.
    Absent,
    /// A digest this adapter recomputes over the body it received.
    Verifiable(StoredChecksum),
    /// A header that carries a digest this adapter cannot check: a composite
    /// multipart digest (checksum of part checksums), or an algorithm with no
    /// implementation in this crate. Counted unverified, never claimed as
    /// verified. Carries the header name for the counter's diagnostic.
    Unsupported(String),
}

/// Read the stored checksum out of a GET response's headers.
///
/// `header` returns each `x-amz-checksum-*` header the response carried, in
/// response order; the first one with a digest decides. S3 returns at most one
/// per object, but an endpoint that echoes several is read deterministically
/// rather than arbitrarily.
pub(crate) fn observe<'a, I>(headers: I) -> ObservedChecksum
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    for (name, value) in headers {
        if !name.starts_with(CHECKSUM_HEADER_PREFIX) || name == CHECKSUM_MODE_HEADER {
            continue;
        }
        // `x-amz-checksum-algorithm` names an algorithm; the digest lives in
        // the per-algorithm header beside it.
        let Some(algorithm) = name.strip_prefix(CHECKSUM_HEADER_PREFIX) else {
            continue;
        };
        if algorithm == "algorithm" || algorithm == "type" {
            continue;
        }
        // A composite (multipart) digest is suffixed with the part count and
        // digests part digests, not the body.
        if value.rsplit_once('-').is_some_and(|(_, parts)| {
            !parts.is_empty() && parts.chars().all(|c| c.is_ascii_digit())
        }) {
            return ObservedChecksum::Unsupported(name.to_string());
        }
        let Ok(digest) = STANDARD.decode(value) else {
            return ObservedChecksum::Unsupported(name.to_string());
        };
        return match (algorithm, digest.len()) {
            ("crc64nvme", 8) => {
                let mut be = [0u8; 8];
                be.copy_from_slice(&digest);
                ObservedChecksum::Verifiable(StoredChecksum::Crc64Nvme(u64::from_be_bytes(be)))
            }
            ("crc32c", 4) => {
                let mut be = [0u8; 4];
                be.copy_from_slice(&digest);
                ObservedChecksum::Verifiable(StoredChecksum::Crc32c(u32::from_be_bytes(be)))
            }
            _ => ObservedChecksum::Unsupported(name.to_string()),
        };
    }
    ObservedChecksum::Absent
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// The catalogue `check` value for CRC-64/NVME: the digest of the ASCII
    /// string `123456789`. Pinning it is what makes the table generated above a
    /// CRC-64/NVME rather than "some 64-bit CRC".
    #[test]
    fn crc64_nvme_matches_the_catalogue_check_vector() {
        assert_eq!(crc64_nvme(b"123456789"), 0xae8b_1486_0a79_9888);
    }

    /// Empty input is the all-ones init xored with the all-ones final xor.
    #[test]
    fn crc64_nvme_of_empty_input_is_zero() {
        assert_eq!(crc64_nvme(b""), 0);
    }

    /// One flipped bit changes the digest: the property the whole read-side
    /// check rests on, asserted rather than assumed.
    #[test]
    fn crc64_nvme_changes_on_a_single_flipped_bit() {
        let clean = crc64_nvme(b"ravel commit record");
        let mut corrupt = b"ravel commit record".to_vec();
        corrupt[3] ^= 0x40;
        assert_ne!(crc64_nvme(&corrupt), clean);
    }

    #[test]
    fn a_crc64nvme_header_is_verifiable_and_round_trips() {
        let digest = crc64_nvme(b"payload");
        let encoded = STANDARD.encode(digest.to_be_bytes());
        let observed = observe([("x-amz-checksum-crc64nvme", encoded.as_str())]);
        assert_eq!(
            observed,
            ObservedChecksum::Verifiable(StoredChecksum::Crc64Nvme(digest))
        );
        let ObservedChecksum::Verifiable(stored) = observed else {
            panic!("just asserted verifiable");
        };
        stored.verify("k", b"payload").expect("matching body");
        let err = stored
            .verify("k", b"payloae")
            .expect_err("a changed body must not verify");
        assert!(matches!(err, StoreError::Corrupted(_)), "got {err:?}");
    }

    #[test]
    fn a_crc32c_header_is_verifiable() {
        let digest = crc32c::crc32c(b"payload");
        let encoded = STANDARD.encode(digest.to_be_bytes());
        assert_eq!(
            observe([("x-amz-checksum-crc32c", encoded.as_str())]),
            ObservedChecksum::Verifiable(StoredChecksum::Crc32c(digest))
        );
    }

    /// No checksum header at all is [`ObservedChecksum::Absent`], which is what
    /// the unverified counter counts (ADR-1696 decision 3).
    #[test]
    fn no_checksum_header_is_absent() {
        assert_eq!(
            observe([("etag", "\"abc\""), ("content-length", "7")]),
            ObservedChecksum::Absent
        );
    }

    /// `x-amz-checksum-algorithm` names an algorithm and carries no digest, so
    /// a response echoing it alone is still unverified, not a parse failure.
    #[test]
    fn the_algorithm_header_alone_is_absent() {
        assert_eq!(
            observe([("x-amz-checksum-algorithm", "CRC64NVME")]),
            ObservedChecksum::Absent
        );
    }

    /// A composite multipart digest (`-{part count}`) digests part digests, so
    /// nothing recomputed over the body can match it: unsupported, not a
    /// mismatch.
    #[test]
    fn a_composite_multipart_digest_is_unsupported() {
        assert_eq!(
            observe([("x-amz-checksum-crc32c", "mGYnhg==-4")]),
            ObservedChecksum::Unsupported("x-amz-checksum-crc32c".to_string())
        );
    }

    /// SHA-256 has no implementation in this crate, so a response carrying one
    /// is counted unverified rather than claimed verified.
    #[test]
    fn a_sha256_digest_is_unsupported() {
        let encoded = STANDARD.encode([0u8; 32]);
        assert_eq!(
            observe([("x-amz-checksum-sha256", encoded.as_str())]),
            ObservedChecksum::Unsupported("x-amz-checksum-sha256".to_string())
        );
    }

    /// A digest of the wrong length for its algorithm is unsupported, never
    /// silently truncated into a comparison that could pass.
    #[test]
    fn a_wrong_length_digest_is_unsupported() {
        let encoded = STANDARD.encode([0u8; 4]);
        assert_eq!(
            observe([("x-amz-checksum-crc64nvme", encoded.as_str())]),
            ObservedChecksum::Unsupported("x-amz-checksum-crc64nvme".to_string())
        );
    }
}
