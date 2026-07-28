//! LogFooter, the 16-byte trailer, and the reader open protocol
//! (docs/log-segment-format.md "Reader protocol", "LogFooter").
//!
//! The footer is the `ravel.logseg.v1.LogFooter` protobuf; the trailer mirrors
//! RSEG's shape. `footer_crc32c` covers the footer bytes plus every trailer
//! byte except the crc field itself (ADR-0010 §4). [`open`] performs the full
//! reader protocol on a whole object and validates the section table before
//! returning any descriptor.

use prost::Message;
use ravel_proto::logseg::v1 as pb;

use crate::error::LogSegError;

/// Trailer magic, last 4 bytes of every RLOG object.
pub const MAGIC: [u8; 4] = *b"RLG1";
/// RLOG v1 trailer version.
pub const VERSION: u16 = 1;
/// Signal byte for log segments.
pub const SIGNAL_LOGS: u8 = 2;
/// Reserved trailer byte; must be zero in v1.
pub const RESERVED: u8 = 0;
/// Trailer size: footer_len(4) + footer_crc32c(4) + version(2) + signal(1) +
/// reserved(1) + magic(4).
pub const TRAILER_LEN: usize = 16;

/// Section `comp` tag: stored raw.
pub const COMP_NONE: u8 = 0;
/// Section `comp` tag: zstd frame.
pub const COMP_ZSTD: u8 = 2;

/// Default per-section decompressed-size cap used during open validation.
pub const DEFAULT_MAX_SECTION_UNCOMP: u64 = 1 << 30;

/// Section kinds (docs/log-segment-format.md). The five v1 kinds are mandatory;
/// any other kind is skipped.
pub mod kind {
    pub const STREAM_DIR: u32 = 1;
    pub const FIELD_DIR: u32 = 2;
    pub const BLOCKS: u32 = 3;
    pub const SKIP_IDX: u32 = 4;
    pub const BLOOM: u32 = 5;
}

/// One section table entry (mirrors `ravel.logseg.v1.Section`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SectionDesc {
    pub kind: u32,
    pub offset: u64,
    pub len: u64,
    pub crc32c: u32,
    pub comp: u8,
    pub uncomp_len: u64,
}

/// The decoded footer (mirrors `ravel.logseg.v1.LogFooter`).
#[derive(Clone, Debug, PartialEq)]
pub struct LogFooter {
    pub tenant_hash: [u8; 16],
    pub shard: u32,
    pub writer_id: [u8; 16],
    pub writer_epoch: u64,
    pub writer_seq: u64,
    pub min_ts_ns: i64,
    pub max_ts_ns: i64,
    pub min_observed_ts_ns: i64,
    pub max_observed_ts_ns: i64,
    pub record_count: u64,
    pub block_count: u64,
    pub stream_count: u64,
    pub sections: Vec<SectionDesc>,
}

impl LogFooter {
    /// The descriptor for a known section kind, if present.
    pub fn section(&self, kind: u32) -> Option<&SectionDesc> {
        self.sections.iter().find(|s| s.kind == kind)
    }

    fn to_proto(&self) -> pb::LogFooter {
        pb::LogFooter {
            tenant_hash: self.tenant_hash.to_vec(),
            shard: self.shard,
            writer_id: self.writer_id.to_vec(),
            writer_epoch: self.writer_epoch,
            writer_seq: self.writer_seq,
            min_ts_ns: self.min_ts_ns,
            max_ts_ns: self.max_ts_ns,
            min_observed_ts_ns: self.min_observed_ts_ns,
            max_observed_ts_ns: self.max_observed_ts_ns,
            record_count: self.record_count,
            block_count: self.block_count,
            stream_count: self.stream_count,
            sections: self
                .sections
                .iter()
                .map(|s| pb::Section {
                    kind: s.kind,
                    offset: s.offset,
                    len: s.len,
                    crc32c: s.crc32c,
                    comp: i32::from(s.comp),
                    uncompressed_len: s.uncomp_len,
                })
                .collect(),
        }
    }

    fn from_proto(p: pb::LogFooter) -> Result<Self, LogSegError> {
        let tenant_hash = <[u8; 16]>::try_from(p.tenant_hash.as_slice())
            .map_err(|_| LogSegError::Corrupted("footer tenant_hash not 16 bytes".into()))?;
        let writer_id = <[u8; 16]>::try_from(p.writer_id.as_slice())
            .map_err(|_| LogSegError::Corrupted("footer writer_id not 16 bytes".into()))?;
        let mut sections = Vec::with_capacity(p.sections.len());
        for s in p.sections {
            let comp = u8::try_from(s.comp)
                .map_err(|_| LogSegError::Corrupted("footer section comp range".into()))?;
            sections.push(SectionDesc {
                kind: s.kind,
                offset: s.offset,
                len: s.len,
                crc32c: s.crc32c,
                comp,
                uncomp_len: s.uncompressed_len,
            });
        }
        Ok(LogFooter {
            tenant_hash,
            shard: p.shard,
            writer_id,
            writer_epoch: p.writer_epoch,
            writer_seq: p.writer_seq,
            min_ts_ns: p.min_ts_ns,
            max_ts_ns: p.max_ts_ns,
            min_observed_ts_ns: p.min_observed_ts_ns,
            max_observed_ts_ns: p.max_observed_ts_ns,
            record_count: p.record_count,
            block_count: p.block_count,
            stream_count: p.stream_count,
            sections,
        })
    }
}

/// `footer_crc32c`: covers the footer protobuf bytes, then `footer_len`
/// (u32 LE), `version` (u16 LE), `signal`, `reserved`, `magic` — every trailer
/// byte except the crc field itself (ADR-0010 §4).
fn footer_crc(footer_bytes: &[u8], footer_len: u32, version: u16, signal: u8, reserved: u8) -> u32 {
    let mut crc = crc32c::crc32c(footer_bytes);
    crc = crc32c::crc32c_append(crc, &footer_len.to_le_bytes());
    crc = crc32c::crc32c_append(crc, &version.to_le_bytes());
    crc = crc32c::crc32c_append(crc, &[signal, reserved]);
    crc32c::crc32c_append(crc, &MAGIC)
}

/// Appends the footer protobuf bytes and the 16-byte trailer to `out`.
pub fn write_footer_and_trailer(out: &mut Vec<u8>, footer: &LogFooter) {
    let footer_bytes = footer.to_proto().encode_to_vec();
    let footer_len = footer_bytes.len() as u32;
    let crc = footer_crc(&footer_bytes, footer_len, VERSION, SIGNAL_LOGS, RESERVED);
    out.extend_from_slice(&footer_bytes);
    out.extend_from_slice(&footer_len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.push(SIGNAL_LOGS);
    out.push(RESERVED);
    out.extend_from_slice(&MAGIC);
}

/// Opens a whole RLOG object: verifies the trailer, the footer crc, decodes the
/// footer, and validates the section table. Every violation is `Corrupted`.
pub fn open(bytes: &[u8]) -> Result<LogFooter, LogSegError> {
    let total = bytes.len();
    if total < TRAILER_LEN {
        return Err(LogSegError::Corrupted("object smaller than trailer".into()));
    }
    let trailer = &bytes[total - TRAILER_LEN..];
    let footer_len = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
    let footer_crc32c = u32::from_le_bytes([trailer[4], trailer[5], trailer[6], trailer[7]]);
    let version = u16::from_le_bytes([trailer[8], trailer[9]]);
    let signal = trailer[10];
    let reserved = trailer[11];
    let magic = &trailer[12..16];

    if magic != MAGIC {
        return Err(LogSegError::Corrupted("bad magic".into()));
    }
    if version != VERSION {
        return Err(LogSegError::Corrupted(format!(
            "unsupported version {version}"
        )));
    }
    if signal != SIGNAL_LOGS {
        return Err(LogSegError::Corrupted(format!(
            "unsupported signal {signal}"
        )));
    }
    if reserved != RESERVED {
        return Err(LogSegError::Corrupted("reserved byte nonzero".into()));
    }
    if footer_len == 0 {
        return Err(LogSegError::Corrupted("footer_len is zero".into()));
    }
    let footer_start = total
        .checked_sub(TRAILER_LEN)
        .and_then(|v| v.checked_sub(footer_len as usize))
        .ok_or_else(|| LogSegError::Corrupted("footer_len past object".into()))?;
    let footer_bytes = &bytes[footer_start..total - TRAILER_LEN];

    let expected = footer_crc(footer_bytes, footer_len, version, signal, reserved);
    if expected != footer_crc32c {
        return Err(LogSegError::Corrupted("footer crc mismatch".into()));
    }

    let proto = pb::LogFooter::decode(footer_bytes)
        .map_err(|e| LogSegError::Corrupted(format!("footer decode: {e}")))?;
    let footer = LogFooter::from_proto(proto)?;

    validate_sections(&footer, footer_start as u64, DEFAULT_MAX_SECTION_UNCOMP)?;
    Ok(footer)
}

/// Validates the section table: at most one of each known kind, all five v1
/// kinds present, every range inside the section area with overflow-checked
/// arithmetic, and every `uncomp_len` within the cap.
pub fn validate_sections(
    footer: &LogFooter,
    section_area_end: u64,
    max_uncomp: u64,
) -> Result<(), LogSegError> {
    let known = [
        kind::STREAM_DIR,
        kind::FIELD_DIR,
        kind::BLOCKS,
        kind::SKIP_IDX,
        kind::BLOOM,
    ];
    for k in known {
        let n = footer.sections.iter().filter(|s| s.kind == k).count();
        if n == 0 {
            return Err(LogSegError::Corrupted(format!("missing section kind {k}")));
        }
        if n > 1 {
            return Err(LogSegError::Corrupted(format!(
                "duplicate section kind {k}"
            )));
        }
    }
    for s in &footer.sections {
        // Unknown kinds are skipped but their ranges are still bounds-checked so
        // a hostile descriptor cannot point outside the object.
        let end = s
            .offset
            .checked_add(s.len)
            .ok_or_else(|| LogSegError::Corrupted("section range overflow".into()))?;
        if end > section_area_end {
            return Err(LogSegError::Corrupted("section range out of bounds".into()));
        }
        if s.uncomp_len > max_uncomp {
            return Err(LogSegError::Corrupted("section uncomp_len over cap".into()));
        }
        if s.comp != COMP_NONE && s.comp != COMP_ZSTD {
            return Err(LogSegError::Corrupted(format!(
                "unknown comp tag {}",
                s.comp
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn sample_footer(sections: Vec<SectionDesc>) -> LogFooter {
        LogFooter {
            tenant_hash: [1u8; 16],
            shard: 7,
            writer_id: [2u8; 16],
            writer_epoch: 3,
            writer_seq: 4,
            min_ts_ns: -5,
            max_ts_ns: 900,
            min_observed_ts_ns: 0,
            max_observed_ts_ns: 1000,
            record_count: 42,
            block_count: 2,
            stream_count: 3,
            sections,
        }
    }

    /// Builds a minimal valid object: a body area of `body_len` zero bytes
    /// carrying the five mandatory sections, then footer+trailer.
    fn build_object(body_len: usize) -> Vec<u8> {
        let sections = vec![
            SectionDesc {
                kind: kind::STREAM_DIR,
                offset: 0,
                len: 1,
                crc32c: 0,
                comp: COMP_ZSTD,
                uncomp_len: 1,
            },
            SectionDesc {
                kind: kind::FIELD_DIR,
                offset: 1,
                len: 1,
                crc32c: 0,
                comp: COMP_ZSTD,
                uncomp_len: 1,
            },
            SectionDesc {
                kind: kind::BLOCKS,
                offset: 2,
                len: 1,
                crc32c: 0,
                comp: COMP_NONE,
                uncomp_len: 1,
            },
            SectionDesc {
                kind: kind::SKIP_IDX,
                offset: 3,
                len: 1,
                crc32c: 0,
                comp: COMP_ZSTD,
                uncomp_len: 1,
            },
            SectionDesc {
                kind: kind::BLOOM,
                offset: 4,
                len: 1,
                crc32c: 0,
                comp: COMP_NONE,
                uncomp_len: 1,
            },
        ];
        let mut obj = vec![0u8; body_len];
        write_footer_and_trailer(&mut obj, &sample_footer(sections));
        obj
    }

    #[test]
    fn roundtrip_open() {
        let obj = build_object(5);
        let got = open(&obj).expect("open");
        assert_eq!(got.shard, 7);
        assert_eq!(got.record_count, 42);
        assert_eq!(got.section(kind::BLOCKS).map(|s| s.offset), Some(2));
    }

    #[test]
    fn rejects_each_corruption() {
        let obj = build_object(5);
        // Too small.
        assert!(matches!(open(&obj[..8]), Err(LogSegError::Corrupted(_))));

        // Bad magic.
        let mut bad = obj.clone();
        let n = bad.len();
        bad[n - 1] ^= 0xff;
        assert!(matches!(open(&bad), Err(LogSegError::Corrupted(_))));

        // Wrong signal.
        let mut bad = obj.clone();
        bad[n - 6] = 1;
        assert!(matches!(open(&bad), Err(LogSegError::Corrupted(_))));

        // Reserved nonzero.
        let mut bad = obj.clone();
        bad[n - 5] = 9;
        assert!(matches!(open(&bad), Err(LogSegError::Corrupted(_))));

        // footer crc mismatch (flip a footer proto byte, which the crc covers;
        // the 5-byte body area precedes the footer).
        let mut bad = obj.clone();
        bad[n - TRAILER_LEN - 1] ^= 0xff;
        assert!(matches!(open(&bad), Err(LogSegError::Corrupted(_))));

        // footer_len overflow (huge).
        let mut bad = obj.clone();
        bad[n - 16..n - 12].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(open(&bad), Err(LogSegError::Corrupted(_))));
    }

    #[test]
    fn rejects_missing_and_out_of_range_sections() {
        // Missing BLOOM.
        let sections = vec![
            SectionDesc {
                kind: kind::STREAM_DIR,
                offset: 0,
                len: 1,
                crc32c: 0,
                comp: COMP_ZSTD,
                uncomp_len: 1,
            },
            SectionDesc {
                kind: kind::FIELD_DIR,
                offset: 1,
                len: 1,
                crc32c: 0,
                comp: COMP_ZSTD,
                uncomp_len: 1,
            },
            SectionDesc {
                kind: kind::BLOCKS,
                offset: 2,
                len: 1,
                crc32c: 0,
                comp: COMP_NONE,
                uncomp_len: 1,
            },
            SectionDesc {
                kind: kind::SKIP_IDX,
                offset: 3,
                len: 1,
                crc32c: 0,
                comp: COMP_ZSTD,
                uncomp_len: 1,
            },
        ];
        let mut obj = vec![0u8; 5];
        write_footer_and_trailer(&mut obj, &sample_footer(sections));
        assert!(matches!(open(&obj), Err(LogSegError::Corrupted(_))));

        // A section pointing past the section area.
        let mut sections = sample_sections();
        sections[2].len = 10_000;
        let mut obj = vec![0u8; 5];
        write_footer_and_trailer(&mut obj, &sample_footer(sections));
        assert!(matches!(open(&obj), Err(LogSegError::Corrupted(_))));
    }

    fn sample_sections() -> Vec<SectionDesc> {
        vec![
            SectionDesc {
                kind: kind::STREAM_DIR,
                offset: 0,
                len: 1,
                crc32c: 0,
                comp: COMP_ZSTD,
                uncomp_len: 1,
            },
            SectionDesc {
                kind: kind::FIELD_DIR,
                offset: 1,
                len: 1,
                crc32c: 0,
                comp: COMP_ZSTD,
                uncomp_len: 1,
            },
            SectionDesc {
                kind: kind::BLOCKS,
                offset: 2,
                len: 1,
                crc32c: 0,
                comp: COMP_NONE,
                uncomp_len: 1,
            },
            SectionDesc {
                kind: kind::SKIP_IDX,
                offset: 3,
                len: 1,
                crc32c: 0,
                comp: COMP_ZSTD,
                uncomp_len: 1,
            },
            SectionDesc {
                kind: kind::BLOOM,
                offset: 4,
                len: 1,
                crc32c: 0,
                comp: COMP_NONE,
                uncomp_len: 1,
            },
        ]
    }
}
