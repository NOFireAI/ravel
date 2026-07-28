//! Page compression envelope (docs/log-segment-format.md "Page compression
//! envelope"). A page's stored bytes are its encoded codec bytes, optionally
//! zstd-compressed. The writer compresses only when the encoded page is at
//! least the 512-byte floor and zstd is strictly smaller; below the floor the
//! zstd overhead exceeds the win.

use crate::encoding::Enc;
use crate::error::LogSegError;

/// Page compression floor: pages under this many encoded bytes stay raw.
pub const COMPRESSION_FLOOR: usize = 512;
/// `comp` tag: stored raw.
pub const COMP_NONE: u8 = 0;
/// `comp` tag: zstd frame.
pub const COMP_ZSTD: u8 = 2;
/// Default per-page decompressed-size cap (zstd bomb guard).
pub const DEFAULT_MAX_UNCOMP: u64 = 64 << 20;

/// One page's descriptor, stored in the block header.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PageDesc {
    pub column_id: u32,
    pub enc: Enc,
    pub comp: u8,
    pub len: u64,
    pub uncomp_len: u64,
}

/// Appends one page's stored bytes to `out` and returns its descriptor.
///
/// Compresses with zstd only when `encoded.len() >= COMPRESSION_FLOOR` and the
/// compressed form is strictly smaller; otherwise stores the encoded bytes
/// raw. A compression backend error also falls back to raw, so this never
/// fails.
pub fn write_page(
    out: &mut Vec<u8>,
    column_id: u32,
    enc: Enc,
    encoded: &[u8],
    zstd_level: i32,
) -> PageDesc {
    let uncomp_len = encoded.len() as u64;
    let (comp, stored): (u8, std::borrow::Cow<'_, [u8]>) = if encoded.len() >= COMPRESSION_FLOOR {
        match zstd::bulk::compress(encoded, zstd_level) {
            Ok(z) if z.len() < encoded.len() => (COMP_ZSTD, std::borrow::Cow::Owned(z)),
            _ => (COMP_NONE, std::borrow::Cow::Borrowed(encoded)),
        }
    } else {
        (COMP_NONE, std::borrow::Cow::Borrowed(encoded))
    };
    let len = stored.len() as u64;
    out.extend_from_slice(&stored);
    PageDesc {
        column_id,
        enc,
        comp,
        len,
        uncomp_len,
    }
}

/// Decodes one page's stored bytes back to its encoded codec bytes.
///
/// `bytes` is exactly the page's stored payload (`desc.len` bytes). Rejects a
/// stored length that disagrees with the descriptor, an unknown `comp` tag, an
/// `uncomp_len` above `max_uncomp` (before allocating), and a decompressed
/// length that does not equal `uncomp_len`. The decompressor allocates exactly
/// `uncomp_len` bytes so a lying frame cannot expand past the cap.
pub fn read_page(bytes: &[u8], desc: &PageDesc, max_uncomp: u64) -> Result<Vec<u8>, LogSegError> {
    if bytes.len() as u64 != desc.len {
        return Err(LogSegError::Corrupted(format!(
            "page stored length {} != descriptor {}",
            bytes.len(),
            desc.len
        )));
    }
    if desc.uncomp_len > max_uncomp {
        return Err(LogSegError::Corrupted(format!(
            "page uncomp_len {} exceeds cap {max_uncomp}",
            desc.uncomp_len
        )));
    }
    match desc.comp {
        COMP_NONE => {
            if bytes.len() as u64 != desc.uncomp_len {
                return Err(LogSegError::Corrupted(format!(
                    "raw page length {} != uncomp_len {}",
                    bytes.len(),
                    desc.uncomp_len
                )));
            }
            Ok(bytes.to_vec())
        }
        COMP_ZSTD => {
            let decoded = zstd::bulk::decompress(bytes, desc.uncomp_len as usize)
                .map_err(|e| LogSegError::Corrupted(format!("zstd decompress: {e}")))?;
            if decoded.len() as u64 != desc.uncomp_len {
                return Err(LogSegError::Corrupted(format!(
                    "decompressed length {} != uncomp_len {}",
                    decoded.len(),
                    desc.uncomp_len
                )));
            }
            Ok(decoded)
        }
        other => Err(LogSegError::Corrupted(format!("unknown comp tag {other}"))),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn raw_page_below_floor_roundtrips() {
        let encoded = vec![7u8; 100];
        let mut buf = Vec::new();
        let desc = write_page(&mut buf, 5, Enc::Plain, &encoded, 3);
        assert_eq!(desc.comp, COMP_NONE);
        assert_eq!(desc.len, 100);
        assert_eq!(desc.uncomp_len, 100);
        let got = read_page(&buf, &desc, DEFAULT_MAX_UNCOMP).expect("read");
        assert_eq!(got, encoded);
    }

    #[test]
    fn compressible_page_above_floor_uses_zstd() {
        let encoded = vec![0u8; 4000];
        let mut buf = Vec::new();
        let desc = write_page(&mut buf, 1, Enc::Bitmap, &encoded, 3);
        assert_eq!(desc.comp, COMP_ZSTD);
        assert!(desc.len < desc.uncomp_len);
        assert_eq!(buf.len() as u64, desc.len);
        let got = read_page(&buf, &desc, DEFAULT_MAX_UNCOMP).expect("read");
        assert_eq!(got, encoded);
    }

    #[test]
    fn incompressible_page_above_floor_stays_raw() {
        // High-entropy bytes (blake3 keystream) that zstd cannot shrink stay
        // raw even above the floor.
        let mut encoded = Vec::new();
        let mut reader = blake3::Hasher::new().finalize_xof();
        let mut chunk = [0u8; 1024];
        blake3::OutputReader::fill(&mut reader, &mut chunk);
        encoded.extend_from_slice(&chunk);
        let mut buf = Vec::new();
        let desc = write_page(&mut buf, 2, Enc::Plain, &encoded, 3);
        assert_eq!(desc.comp, COMP_NONE);
        let got = read_page(&buf, &desc, DEFAULT_MAX_UNCOMP).expect("read");
        assert_eq!(got, encoded);
    }

    #[test]
    fn rejects_uncomp_len_over_cap_before_alloc() {
        let encoded = vec![0u8; 4000];
        let mut buf = Vec::new();
        let mut desc = write_page(&mut buf, 1, Enc::Plain, &encoded, 3);
        // A hostile descriptor claiming a huge decompressed size.
        desc.uncomp_len = u64::MAX;
        assert!(matches!(
            read_page(&buf, &desc, DEFAULT_MAX_UNCOMP),
            Err(LogSegError::Corrupted(_))
        ));
    }

    #[test]
    fn rejects_lying_uncomp_len_within_cap() {
        let encoded = vec![0u8; 4000];
        let mut buf = Vec::new();
        let mut desc = write_page(&mut buf, 1, Enc::Plain, &encoded, 3);
        // Within the cap but not the true decompressed size.
        desc.uncomp_len = 3999;
        assert!(matches!(
            read_page(&buf, &desc, DEFAULT_MAX_UNCOMP),
            Err(LogSegError::Corrupted(_))
        ));
    }

    #[test]
    fn rejects_unknown_comp_tag() {
        let encoded = vec![1u8; 8];
        let mut buf = Vec::new();
        let mut desc = write_page(&mut buf, 0, Enc::Plain, &encoded, 3);
        desc.comp = 1; // lz4 is not a valid RLOG comp tag
        assert!(matches!(
            read_page(&buf, &desc, DEFAULT_MAX_UNCOMP),
            Err(LogSegError::Corrupted(_))
        ));
    }

    #[test]
    fn rejects_truncated_payload() {
        let encoded = vec![0u8; 4000];
        let mut buf = Vec::new();
        let desc = write_page(&mut buf, 1, Enc::Plain, &encoded, 3);
        assert_eq!(desc.comp, COMP_ZSTD);
        buf.truncate(buf.len() - 1);
        // The descriptor still claims the full stored length.
        assert!(matches!(
            read_page(&buf, &desc, DEFAULT_MAX_UNCOMP),
            Err(LogSegError::Corrupted(_))
        ));
    }
}

/// Any encoded page round-trips through the envelope, and read_page never
/// panics on an arbitrary stored payload paired with an arbitrary descriptor.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn write_then_read_roundtrips(encoded in proptest::collection::vec(any::<u8>(), 0..8192)) {
            let mut buf = Vec::new();
            let desc = write_page(&mut buf, 3, Enc::Plain, &encoded, 3);
            let got = read_page(&buf, &desc, DEFAULT_MAX_UNCOMP).expect("read");
            prop_assert_eq!(got, encoded);
        }

        #[test]
        fn read_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..512),
            comp in any::<u8>(),
            uncomp_len in any::<u64>(),
        ) {
            let desc = PageDesc {
                column_id: 0,
                enc: Enc::Plain,
                comp,
                len: bytes.len() as u64,
                uncomp_len,
            };
            let _ = read_page(&bytes, &desc, DEFAULT_MAX_UNCOMP);
        }
    }
}
