//! SpanFooter, the 16-byte trailer, and the reader open protocol
//! (docs/span-segment-format.md "Reader protocol", "SpanFooter").
//!
//! The footer is the `ravel.rspan.v1.SpanFooter` protobuf; the trailer mirrors
//! RLOG's/RSEG's shape. `footer_crc32c` covers the footer bytes plus every
//! trailer byte except the crc field itself. [`open`] performs the full reader
//! protocol on a whole object and validates the section table before returning
//! any descriptor. Every violation is a typed `Corrupted` error, never a panic.

use prost::Message;

use crate::error::SpanSegError;
use crate::pb;

/// Trailer magic, last 4 bytes of every RSPAN object.
pub const MAGIC: [u8; 4] = *b"RSP1";
/// RSPAN trailer version. v3 (ADR-0054) added a mandatory per-block BLOOM
/// section over `service.name` and span-name tokens ([`kind::BLOOM`]) and a
/// block-local dictionary-encoded `service_name` column (id 9); v2 (ADR-0045)
/// had added per-block duration bounds and a status mask to SKIP_IDX. Any
/// earlier version (v1, v2) is rejected with the same typed `unsupported
/// version` error as any other unknown version (ADR-0045 decision 4, ADR-0054
/// decision 1: single supported version, no dual reader).
pub const VERSION: u16 = 3;
/// Signal byte for span segments (Metrics=1 in RSEG, Logs=2 in RLOG, Spans=3).
pub const SIGNAL_SPANS: u8 = 3;
/// Reserved trailer byte; must be zero.
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

/// Section kinds (docs/span-segment-format.md). All three kinds are mandatory
/// as of v3 (ADR-0054); any other kind is skipped.
pub mod kind {
    /// Columnar row blocks.
    pub const BLOCKS: u32 = 1;
    /// Interval + trace_id min/max skip index.
    pub const SKIP_IDX: u32 = 2;
    /// Per-block token bloom over `service.name` and span-name tokens (v3,
    /// ADR-0054). Entry `i` covers block `i`, positionally addressed; built and
    /// read through `ravel-codec`'s `bloom_section`. Mandatory: a missing BLOOM
    /// is a `Corrupted` error, the same as a missing SKIP_IDX.
    pub const BLOOM: u32 = 3;
}

/// One section table entry (mirrors `ravel.rspan.v1.Section`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SectionDesc {
    pub kind: u32,
    pub offset: u64,
    pub len: u64,
    pub crc32c: u32,
    pub comp: u8,
    pub uncomp_len: u64,
}

/// The decoded footer (mirrors `ravel.rspan.v1.SpanFooter`).
#[derive(Clone, Debug, PartialEq)]
pub struct SpanFooter {
    pub tenant_hash: [u8; 16],
    pub shard: u32,
    pub writer_id: [u8; 16],
    pub writer_epoch: u64,
    pub writer_seq: u64,
    pub min_start_ts_ns: i64,
    pub max_end_ts_ns: i64,
    pub record_count: u64,
    pub block_count: u64,
    pub min_trace_id: [u8; 16],
    pub max_trace_id: [u8; 16],
    pub sections: Vec<SectionDesc>,
    /// Compaction level: 0 = L0 flush object, 1 = L1 compacted part.
    pub level: u32,
    /// Hash over the sorted input list of a compaction (empty on an L0 object).
    pub input_set_hash: Vec<u8>,
    /// Part ordinal within one compaction output (0 on an L0 object).
    pub part_index: u32,
}

impl SpanFooter {
    /// The descriptor for a known section kind, if present.
    pub fn section(&self, kind: u32) -> Option<&SectionDesc> {
        self.sections.iter().find(|s| s.kind == kind)
    }

    fn to_proto(&self) -> pb::SpanFooter {
        pb::SpanFooter {
            tenant_hash: self.tenant_hash.to_vec(),
            shard: self.shard,
            writer_id: self.writer_id.to_vec(),
            writer_epoch: self.writer_epoch,
            writer_seq: self.writer_seq,
            min_start_ts_ns: self.min_start_ts_ns,
            max_end_ts_ns: self.max_end_ts_ns,
            record_count: self.record_count,
            block_count: self.block_count,
            min_trace_id: self.min_trace_id.to_vec(),
            max_trace_id: self.max_trace_id.to_vec(),
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
            level: self.level,
            input_set_hash: self.input_set_hash.clone(),
            part_index: self.part_index,
        }
    }

    fn from_proto(p: pb::SpanFooter) -> Result<Self, SpanSegError> {
        let tenant_hash = <[u8; 16]>::try_from(p.tenant_hash.as_slice())
            .map_err(|_| SpanSegError::Corrupted("footer tenant_hash not 16 bytes".into()))?;
        let writer_id = <[u8; 16]>::try_from(p.writer_id.as_slice())
            .map_err(|_| SpanSegError::Corrupted("footer writer_id not 16 bytes".into()))?;
        let min_trace_id = <[u8; 16]>::try_from(p.min_trace_id.as_slice())
            .map_err(|_| SpanSegError::Corrupted("footer min_trace_id not 16 bytes".into()))?;
        let max_trace_id = <[u8; 16]>::try_from(p.max_trace_id.as_slice())
            .map_err(|_| SpanSegError::Corrupted("footer max_trace_id not 16 bytes".into()))?;
        let mut sections = Vec::with_capacity(p.sections.len());
        for s in p.sections {
            let comp = u8::try_from(s.comp)
                .map_err(|_| SpanSegError::Corrupted("footer section comp range".into()))?;
            sections.push(SectionDesc {
                kind: s.kind,
                offset: s.offset,
                len: s.len,
                crc32c: s.crc32c,
                comp,
                uncomp_len: s.uncompressed_len,
            });
        }
        Ok(SpanFooter {
            tenant_hash,
            shard: p.shard,
            writer_id,
            writer_epoch: p.writer_epoch,
            writer_seq: p.writer_seq,
            min_start_ts_ns: p.min_start_ts_ns,
            max_end_ts_ns: p.max_end_ts_ns,
            record_count: p.record_count,
            block_count: p.block_count,
            min_trace_id,
            max_trace_id,
            sections,
            level: p.level,
            input_set_hash: p.input_set_hash,
            part_index: p.part_index,
        })
    }
}

/// `footer_crc32c`: covers the footer protobuf bytes, then `footer_len` (u32
/// LE), `version` (u16 LE), `signal`, `reserved`, `magic` — every trailer byte
/// except the crc field itself.
fn footer_crc(footer_bytes: &[u8], footer_len: u32, version: u16, signal: u8, reserved: u8) -> u32 {
    let mut crc = crc32c::crc32c(footer_bytes);
    crc = crc32c::crc32c_append(crc, &footer_len.to_le_bytes());
    crc = crc32c::crc32c_append(crc, &version.to_le_bytes());
    crc = crc32c::crc32c_append(crc, &[signal, reserved]);
    crc32c::crc32c_append(crc, &MAGIC)
}

/// Appends the footer protobuf bytes and the 16-byte trailer to `out`.
pub fn write_footer_and_trailer(out: &mut Vec<u8>, footer: &SpanFooter) {
    let footer_bytes = footer.to_proto().encode_to_vec();
    let footer_len = footer_bytes.len() as u32;
    let crc = footer_crc(&footer_bytes, footer_len, VERSION, SIGNAL_SPANS, RESERVED);
    out.extend_from_slice(&footer_bytes);
    out.extend_from_slice(&footer_len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.push(SIGNAL_SPANS);
    out.push(RESERVED);
    out.extend_from_slice(&MAGIC);
}

/// Outcome of opening an RSPAN object from a suffix probe (the ranged reader
/// protocol): either the footer parsed, or the suffix did not reach the whole
/// footer and the caller must fetch the named absolute byte range (itself a
/// true suffix of the object) and call [`open_from_suffix`] again with it.
#[derive(Clone, Debug, PartialEq)]
pub enum SuffixOutcome {
    /// The footer was fully covered and validated.
    Ready(SpanFooter),
    /// The suffix covered the trailer but not the whole footer. Fetch
    /// `[offset, offset + len)` (footer + trailer) and retry.
    NeedRange { offset: u64, len: u64 },
}

/// Opens a whole RSPAN object: verifies the trailer, the footer crc, decodes the
/// footer, and validates the section table. Every violation is `Corrupted`. The
/// whole object always covers the footer, so this never asks for a range.
pub fn open(bytes: &[u8]) -> Result<SpanFooter, SpanSegError> {
    match open_from_suffix(bytes, bytes.len() as u64)? {
        SuffixOutcome::Ready(footer) => Ok(footer),
        SuffixOutcome::NeedRange { .. } => Err(SpanSegError::Corrupted(
            "footer not covered by whole object".into(),
        )),
    }
}

/// Opens an RSPAN object from a suffix of its bytes (docs/span-segment-format.md
/// "Reader protocol"). `suffix` is the last `suffix.len()` bytes of an object of
/// `total_size` bytes. When the suffix covers the footer and trailer, this
/// validates the trailer, the footer crc, and the section table, then returns
/// the footer; when it covers the trailer but not the whole footer, it returns
/// [`SuffixOutcome::NeedRange`] naming the absolute footer+trailer range to
/// fetch and retry. Every violation is `Corrupted`.
pub fn open_from_suffix(suffix: &[u8], total_size: u64) -> Result<SuffixOutcome, SpanSegError> {
    let total = usize::try_from(total_size)
        .map_err(|_| SpanSegError::Corrupted("total_size range".into()))?;
    if total < TRAILER_LEN {
        return Err(SpanSegError::Corrupted(
            "object smaller than trailer".into(),
        ));
    }
    if suffix.len() < TRAILER_LEN || suffix.len() > total {
        return Err(SpanSegError::Corrupted(
            "suffix does not cover the trailer".into(),
        ));
    }
    let trailer = &suffix[suffix.len() - TRAILER_LEN..];
    let footer_len = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
    let footer_crc32c = u32::from_le_bytes([trailer[4], trailer[5], trailer[6], trailer[7]]);
    let version = u16::from_le_bytes([trailer[8], trailer[9]]);
    let signal = trailer[10];
    let reserved = trailer[11];
    let magic = &trailer[12..16];

    if magic != MAGIC {
        return Err(SpanSegError::Corrupted("bad magic".into()));
    }
    if version != VERSION {
        return Err(SpanSegError::UnsupportedVersion(version));
    }
    if signal != SIGNAL_SPANS {
        return Err(SpanSegError::Corrupted(format!(
            "unsupported signal {signal}"
        )));
    }
    if reserved != RESERVED {
        return Err(SpanSegError::Corrupted("reserved byte nonzero".into()));
    }
    if footer_len == 0 {
        return Err(SpanSegError::Corrupted("footer_len is zero".into()));
    }
    let footer_start = total
        .checked_sub(TRAILER_LEN)
        .and_then(|v| v.checked_sub(footer_len as usize))
        .ok_or_else(|| SpanSegError::Corrupted("footer_len past object".into()))?;

    let suffix_start = total - suffix.len();
    if footer_start < suffix_start {
        return Ok(SuffixOutcome::NeedRange {
            offset: footer_start as u64,
            len: (total - footer_start) as u64,
        });
    }
    let rel_start = footer_start - suffix_start;
    let rel_end = suffix.len() - TRAILER_LEN;
    let footer_bytes = &suffix[rel_start..rel_end];

    let expected = footer_crc(footer_bytes, footer_len, version, signal, reserved);
    if expected != footer_crc32c {
        return Err(SpanSegError::Corrupted("footer crc mismatch".into()));
    }

    let proto = pb::SpanFooter::decode(footer_bytes)
        .map_err(|e| SpanSegError::Corrupted(format!("footer decode: {e}")))?;
    let footer = SpanFooter::from_proto(proto)?;

    validate_sections(&footer, footer_start as u64, DEFAULT_MAX_SECTION_UNCOMP)?;
    Ok(SuffixOutcome::Ready(footer))
}

/// Validates the section table: at most one of each known kind, all three
/// mandatory kinds present, every range inside the section area with
/// overflow-checked arithmetic, and every `uncomp_len` within the cap.
pub fn validate_sections(
    footer: &SpanFooter,
    section_area_end: u64,
    max_uncomp: u64,
) -> Result<(), SpanSegError> {
    let known = [kind::BLOCKS, kind::SKIP_IDX, kind::BLOOM];
    for k in known {
        let n = footer.sections.iter().filter(|s| s.kind == k).count();
        if n == 0 {
            return Err(SpanSegError::Corrupted(format!("missing section kind {k}")));
        }
        if n > 1 {
            return Err(SpanSegError::Corrupted(format!(
                "duplicate section kind {k}"
            )));
        }
    }
    for s in &footer.sections {
        let end = s
            .offset
            .checked_add(s.len)
            .ok_or_else(|| SpanSegError::Corrupted("section range overflow".into()))?;
        if end > section_area_end {
            return Err(SpanSegError::Corrupted(
                "section range out of bounds".into(),
            ));
        }
        if s.uncomp_len > max_uncomp {
            return Err(SpanSegError::Corrupted(
                "section uncomp_len over cap".into(),
            ));
        }
        if s.comp != COMP_NONE && s.comp != COMP_ZSTD {
            return Err(SpanSegError::Corrupted(format!(
                "unknown comp tag {}",
                s.comp
            )));
        }
    }
    Ok(())
}

/// Reads and decompresses a whole-read section, verifying its crc first.
///
/// Slices the section's stored bytes from `desc`, verifies `crc32c` against
/// `desc.crc32c`, rejects an `uncomp_len` over `max_uncomp`, then zstd-
/// decompresses or passes the raw bytes through, checking the result is exactly
/// `desc.uncomp_len` long. This is the section-access path for the whole-read
/// SKIP_IDX section; BLOCKS is read per block, not through here. Exposed so the
/// `ravel-cli` inspector can reconstruct a section without reimplementing the
/// crc-and-decompress discipline.
pub fn read_section(
    bytes: &[u8],
    desc: &SectionDesc,
    max_uncomp: u64,
) -> Result<Vec<u8>, SpanSegError> {
    let start = usize::try_from(desc.offset)
        .map_err(|_| SpanSegError::Corrupted("section offset range".into()))?;
    let len = usize::try_from(desc.len)
        .map_err(|_| SpanSegError::Corrupted("section len range".into()))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| SpanSegError::Corrupted("section range overflow".into()))?;
    let stored = bytes
        .get(start..end)
        .ok_or_else(|| SpanSegError::Corrupted("section out of bounds".into()))?;
    decode_section(stored, desc, max_uncomp)
}

/// The crc-and-decompress half of [`read_section`], taking a section's
/// stored bytes directly (offset 0) rather than slicing them out of a
/// whole object.
///
/// This is what a ranged reader ([`crate::ranged::RspanRangeReader`]) uses:
/// it fetches exactly `[desc.offset, desc.offset + desc.len)` with a ranged
/// GET, so the fetched buffer *is* the stored section, and passes it here.
/// `stored` MUST be exactly `desc.len` bytes. The crc is verified against
/// `desc.crc32c` before any decompression, `uncomp_len` is rejected above
/// `max_uncomp` before allocating, and the decompressed length must equal
/// `desc.uncomp_len` exactly (the same discipline [`read_section`] applies
/// to a whole-object slice).
pub fn decode_section(
    stored: &[u8],
    desc: &SectionDesc,
    max_uncomp: u64,
) -> Result<Vec<u8>, SpanSegError> {
    if stored.len() as u64 != desc.len {
        return Err(SpanSegError::Corrupted(
            "section stored length != desc.len".into(),
        ));
    }
    if crc32c::crc32c(stored) != desc.crc32c {
        return Err(SpanSegError::Corrupted("section crc mismatch".into()));
    }
    if desc.uncomp_len > max_uncomp {
        return Err(SpanSegError::Corrupted(
            "section uncomp_len over cap".into(),
        ));
    }
    if desc.comp == COMP_ZSTD {
        let raw = zstd::bulk::decompress(stored, desc.uncomp_len as usize)
            .map_err(|e| SpanSegError::Corrupted(format!("section zstd: {e}")))?;
        if raw.len() as u64 != desc.uncomp_len {
            return Err(SpanSegError::Corrupted(
                "section decompressed length".into(),
            ));
        }
        Ok(raw)
    } else {
        if stored.len() as u64 != desc.uncomp_len {
            return Err(SpanSegError::Corrupted(
                "raw section length != uncomp_len".into(),
            ));
        }
        Ok(stored.to_vec())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// The three mandatory v3 sections, each one byte inside a 3-byte body.
    fn sample_sections() -> Vec<SectionDesc> {
        vec![
            SectionDesc {
                kind: kind::BLOCKS,
                offset: 0,
                len: 1,
                crc32c: 0,
                comp: COMP_NONE,
                uncomp_len: 1,
            },
            SectionDesc {
                kind: kind::SKIP_IDX,
                offset: 1,
                len: 1,
                crc32c: 0,
                comp: COMP_ZSTD,
                uncomp_len: 1,
            },
            SectionDesc {
                kind: kind::BLOOM,
                offset: 2,
                len: 1,
                crc32c: 0,
                comp: COMP_NONE,
                uncomp_len: 1,
            },
        ]
    }

    /// Body length covered by [`sample_sections`]: one byte per section.
    const SAMPLE_BODY_LEN: usize = 3;

    fn sample_footer(sections: Vec<SectionDesc>) -> SpanFooter {
        SpanFooter {
            tenant_hash: [1u8; 16],
            shard: 7,
            writer_id: [2u8; 16],
            writer_epoch: 3,
            writer_seq: 4,
            min_start_ts_ns: -5,
            max_end_ts_ns: 900,
            record_count: 42,
            block_count: 2,
            min_trace_id: [0u8; 16],
            max_trace_id: [9u8; 16],
            sections,
            level: 0,
            input_set_hash: Vec::new(),
            part_index: 0,
        }
    }

    fn build_object(body_len: usize) -> Vec<u8> {
        let mut obj = vec![0u8; body_len];
        write_footer_and_trailer(&mut obj, &sample_footer(sample_sections()));
        obj
    }

    #[test]
    fn roundtrip_open() {
        let obj = build_object(SAMPLE_BODY_LEN);
        let got = open(&obj).expect("open");
        assert_eq!(got.shard, 7);
        assert_eq!(got.record_count, 42);
        assert_eq!(got.section(kind::SKIP_IDX).map(|s| s.offset), Some(1));
        assert_eq!(got.section(kind::BLOOM).map(|s| s.offset), Some(2));
        assert_eq!(got.max_trace_id, [9u8; 16]);
    }

    #[test]
    fn compaction_identity_round_trips() {
        let mut footer = sample_footer(sample_sections());
        footer.level = 1;
        footer.input_set_hash = vec![0xde, 0xad, 0xbe, 0xef];
        footer.part_index = 7;
        let mut obj = vec![0u8; SAMPLE_BODY_LEN];
        write_footer_and_trailer(&mut obj, &footer);
        let got = open(&obj).expect("open");
        assert_eq!(got.level, 1);
        assert_eq!(got.input_set_hash, vec![0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(got.part_index, 7);
        assert_eq!(got, footer);
    }

    #[test]
    fn rejects_each_corruption() {
        let obj = build_object(SAMPLE_BODY_LEN);
        let n = obj.len();
        assert!(matches!(open(&obj[..8]), Err(SpanSegError::Corrupted(_))));

        // Bad magic.
        let mut bad = obj.clone();
        bad[n - 1] ^= 0xff;
        assert!(matches!(open(&bad), Err(SpanSegError::Corrupted(_))));

        // Wrong signal.
        let mut bad = obj.clone();
        bad[n - 6] = 2;
        assert!(matches!(open(&bad), Err(SpanSegError::Corrupted(_))));

        // Reserved nonzero.
        let mut bad = obj.clone();
        bad[n - 5] = 9;
        assert!(matches!(open(&bad), Err(SpanSegError::Corrupted(_))));

        // Footer crc mismatch (flip a footer proto byte; the 3-byte body precedes it).
        let mut bad = obj.clone();
        bad[n - TRAILER_LEN - 1] ^= 0xff;
        assert!(matches!(open(&bad), Err(SpanSegError::Corrupted(_))));

        // footer_len overflow.
        let mut bad = obj.clone();
        bad[n - 16..n - 12].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(open(&bad), Err(SpanSegError::Corrupted(_))));
    }

    #[test]
    fn rejects_v1_object() {
        // A v1 object (trailer version 1) is rejected with the typed
        // `UnsupportedVersion` variant, not `Corrupted` (ADR-0045 decision 4:
        // no dual reader; ADR-0066 decision 2: typed). The version gate runs
        // before the crc check, so flipping only the version byte fires the
        // version rejection, not an incidental crc mismatch.
        let obj = build_object(SAMPLE_BODY_LEN);
        let n = obj.len();
        let mut v1 = obj.clone();
        v1[n - TRAILER_LEN + 8..n - TRAILER_LEN + 10].copy_from_slice(&1u16.to_le_bytes());
        assert!(matches!(
            open(&v1),
            Err(SpanSegError::UnsupportedVersion(1))
        ));
    }

    /// A trailer one version higher than the supported constant is the typed
    /// `UnsupportedVersion(N)` variant, not `Corrupted` (ADR-0066 decision 2:
    /// fail-closed-on-newer, typed). The crc is recomputed for the newer
    /// version so the rejection is the version gate, not a crc mismatch.
    #[test]
    fn rejects_newer_trailer_as_unsupported_version() {
        let newer = VERSION + 1;
        let footer = sample_footer(sample_sections());
        let footer_bytes = footer.to_proto().encode_to_vec();
        let footer_len = footer_bytes.len() as u32;
        let crc = footer_crc(&footer_bytes, footer_len, newer, SIGNAL_SPANS, RESERVED);

        let mut obj = vec![0u8; SAMPLE_BODY_LEN];
        obj.extend_from_slice(&footer_bytes);
        obj.extend_from_slice(&footer_len.to_le_bytes());
        obj.extend_from_slice(&crc.to_le_bytes());
        obj.extend_from_slice(&newer.to_le_bytes());
        obj.push(SIGNAL_SPANS);
        obj.push(RESERVED);
        obj.extend_from_slice(&MAGIC);

        match open(&obj) {
            Err(SpanSegError::UnsupportedVersion(v)) => assert_eq!(v, newer),
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
    }

    #[test]
    fn rejects_missing_and_out_of_range_sections() {
        // Missing SKIP_IDX (and BLOOM): only BLOCKS present.
        let sections = vec![SectionDesc {
            kind: kind::BLOCKS,
            offset: 0,
            len: 1,
            crc32c: 0,
            comp: COMP_NONE,
            uncomp_len: 1,
        }];
        let mut obj = vec![0u8; SAMPLE_BODY_LEN];
        write_footer_and_trailer(&mut obj, &sample_footer(sections));
        assert!(matches!(open(&obj), Err(SpanSegError::Corrupted(_))));

        // Missing the mandatory BLOOM section (BLOCKS + SKIP_IDX only): the
        // same mandatory-section rejection a missing SKIP_IDX takes (ADR-0054).
        let sections = vec![
            SectionDesc {
                kind: kind::BLOCKS,
                offset: 0,
                len: 1,
                crc32c: 0,
                comp: COMP_NONE,
                uncomp_len: 1,
            },
            SectionDesc {
                kind: kind::SKIP_IDX,
                offset: 1,
                len: 1,
                crc32c: 0,
                comp: COMP_ZSTD,
                uncomp_len: 1,
            },
        ];
        let mut obj = vec![0u8; SAMPLE_BODY_LEN];
        write_footer_and_trailer(&mut obj, &sample_footer(sections));
        match open(&obj) {
            Err(SpanSegError::Corrupted(msg)) => {
                assert!(
                    msg.contains(&format!("{}", kind::BLOOM)),
                    "unexpected: {msg}"
                );
            }
            other => panic!("expected missing-BLOOM Corrupted, got {other:?}"),
        }

        // A section pointing past the section area.
        let mut sections = sample_sections();
        sections[0].len = 10_000;
        let mut obj = vec![0u8; SAMPLE_BODY_LEN];
        write_footer_and_trailer(&mut obj, &sample_footer(sections));
        assert!(matches!(open(&obj), Err(SpanSegError::Corrupted(_))));
    }

    /// Epic checkpoint acceptance test (issue #649, ADR-0054). Writes a v3
    /// segment with known service names and span names across multiple blocks,
    /// reads it back, and confirms: the trailer is at the live `footer::VERSION`
    /// (never a mirrored literal, issue #482); the mandatory BLOOM section
    /// exists and answers membership with no false negatives (a false positive
    /// is allowed); the `service_name` column decodes correctly per row; and a
    /// corrupted or truncated BLOOM section is a typed `Corrupted` error, not a
    /// panic.
    #[test]
    fn v3_bloom_and_service_name_column_roundtrip() {
        use ravel_codec::bloom_section::BloomSection;
        use ravel_codec::tokenizer::tokens;

        use crate::reader::{RspanReader, SpanQuery};
        use crate::record::{COL_NAME, COL_SERVICE_NAME, SpanRecord, StatusCode};
        use crate::skip_index::BloomPredicate;
        use crate::writer::{ObjectIdentity, RspanConfig, RspanWriter};

        // One span per block (block_target_records = 1) so each block's bloom
        // and service_name column has a single, known content.
        let cfg = RspanConfig {
            block_target_records: 1,
            ..RspanConfig::default()
        };
        // (service.name, span name); the trace_id is the row index so records
        // stay in this order and land one per block.
        let rows = [
            ("checkout", "GET /cart"),
            ("payments", "charge card"),
            ("checkout", "POST /order"),
            ("inventory", "list items"),
        ];
        let mut w = RspanWriter::new(
            cfg,
            ObjectIdentity {
                tenant_hash: [1u8; 16],
                shard: 0,
                writer_id: [2u8; 16],
                writer_epoch: 1,
                writer_seq: 1,
            },
        );
        for (i, (service, name)) in rows.iter().enumerate() {
            w.push(SpanRecord {
                trace_id: [i as u8; 16],
                span_id: [i as u8; 8],
                parent_span_id: None,
                name: (*name).to_string(),
                start_ts_ns: i as i64,
                end_ts_ns: i as i64 + 1,
                status_code: StatusCode::Ok,
                status_message: None,
                // service.name plus one extra attr that stays in the blob.
                attrs: vec![
                    ("service.name".to_string(), (*service).to_string()),
                    ("http.method".to_string(), "GET".to_string()),
                ],
            });
        }
        let obj = w.finish().expect("finish");

        // Trailer version is the live constant, and the BLOOM section exists.
        let footer = open(&obj).expect("open");
        assert_eq!(
            u16::from_le_bytes([obj[obj.len() - 8], obj[obj.len() - 7]]),
            VERSION,
            "trailer must carry the live footer::VERSION"
        );
        assert_eq!(footer.block_count, 4);
        let bloom_desc = *footer.section(kind::BLOOM).expect("BLOOM section present");

        let reader = RspanReader::new(&obj, &cfg).expect("reader");
        let bloom = reader.bloom().expect("parse bloom");
        assert_eq!(bloom.len(), 4, "one bloom entry per block");

        // No false negatives: every service token present in a block probes
        // positive for that block.
        for (i, (service, name)) in rows.iter().enumerate() {
            let view = bloom.entry(i).expect("entry");
            for tok in tokens(service) {
                assert!(
                    view.may_contain(COL_SERVICE_NAME, &tok),
                    "block {i} service token must probe positive"
                );
            }
            for tok in tokens(name) {
                assert!(
                    view.may_contain(COL_NAME, &tok),
                    "block {i} name token must probe positive"
                );
            }
        }

        // Bloom-backed pruning through the caller-facing surface. A block that
        // owns the service must survive (soundness); "payments" lives only in
        // block 1.
        let skip = reader.skip_index();
        let payments = [BloomPredicate {
            field_id: COL_SERVICE_NAME,
            literal: "payments",
        }];
        let cands = skip
            .candidate_blocks_with_bloom(None, i64::MIN, i64::MAX, None, None, &bloom, &payments)
            .expect("prune");
        assert!(cands.contains(&1), "the payments block must survive");
        assert!(
            !cands.contains(&3),
            "the inventory-only block cannot contain payments"
        );

        // An absent service prunes every block (a bloom negative is proof).
        let absent = [BloomPredicate {
            field_id: COL_SERVICE_NAME,
            literal: "no-such-service-abcdef",
        }];
        let cands = skip
            .candidate_blocks_with_bloom(None, i64::MIN, i64::MAX, None, None, &bloom, &absent)
            .expect("prune");
        assert!(cands.is_empty(), "an absent service prunes all blocks");

        // service_name decodes correctly per row: the whole-object scan rebuilds
        // each record with service.name re-inserted from the column (it is not
        // duplicated in the attrs blob on disk).
        let (mut got, _stats) = reader
            .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
            .expect("scan");
        got.sort_by_key(|a| a.trace_id);
        assert_eq!(got.len(), 4);
        for (i, rec) in got.iter().enumerate() {
            let service = rec
                .attrs
                .iter()
                .find(|(k, _)| k == "service.name")
                .map(|(_, v)| v.as_str());
            assert_eq!(service, Some(rows[i].0), "row {i} service_name");
            // The other attr survives unchanged.
            assert_eq!(
                rec.attrs
                    .iter()
                    .find(|(k, _)| k == "http.method")
                    .map(|(_, v)| v.as_str()),
                Some("GET")
            );
        }

        // Corrupted BLOOM: flipping a byte in the section breaks its
        // whole-section crc, so open-time `read_section` rejects it.
        let bloom_mid = (bloom_desc.offset + bloom_desc.len / 2) as usize;
        let mut corrupt = obj.clone();
        corrupt[bloom_mid] ^= 0xff;
        assert!(
            matches!(
                RspanReader::new(&corrupt, &cfg),
                Err(SpanSegError::Corrupted(_))
            ),
            "a corrupt BLOOM section is a typed Corrupted error"
        );

        // Truncated BLOOM container: parsing fewer bytes than the framing needs
        // is a typed error from the shared codec, converted at the boundary.
        let bloom_raw = read_section(&obj, &bloom_desc, DEFAULT_MAX_SECTION_UNCOMP).expect("read");
        let truncated = &bloom_raw[..bloom_raw.len() - 1];
        let parse_err: Result<(), SpanSegError> = BloomSection::parse(truncated)
            .map(|_| ())
            .map_err(Into::into);
        assert!(
            matches!(parse_err, Err(SpanSegError::Corrupted(_))),
            "a truncated BLOOM container is a typed Corrupted error"
        );

        // Per-entry corruption: flip a byte inside an entry payload; the parse
        // still succeeds (container framing is intact) but probing the entry
        // fails its per-entry crc as a typed error, never a panic.
        let mut entry_corrupt = bloom_raw.clone();
        let last = entry_corrupt.len() - 1;
        entry_corrupt[last] ^= 0xff;
        let bad = BloomSection::parse(&entry_corrupt).expect("container still parses");
        let probe = [BloomPredicate {
            field_id: COL_SERVICE_NAME,
            literal: "checkout",
        }];
        assert!(matches!(
            skip.candidate_blocks_with_bloom(None, i64::MIN, i64::MAX, None, None, &bad, &probe),
            Err(SpanSegError::Corrupted(_))
        ));
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

        #[test]
        fn footer_compaction_fields_round_trip(
            level in any::<u32>(),
            part_index in any::<u32>(),
            hash in proptest::collection::vec(any::<u8>(), 0..40),
        ) {
            let mut footer = sample_footer(sample_sections());
            footer.level = level;
            footer.part_index = part_index;
            footer.input_set_hash = hash.clone();
            let mut obj = vec![0u8; SAMPLE_BODY_LEN];
            write_footer_and_trailer(&mut obj, &footer);
            let got = open(&obj).expect("open");
            prop_assert_eq!(got.level, level);
            prop_assert_eq!(got.part_index, part_index);
            prop_assert_eq!(got.input_set_hash, hash);
        }

        #[test]
        fn footer_corruption_is_typed_error(
            flip in any::<usize>(),
            xor in any::<u8>(),
        ) {
            let mut obj = build_object(SAMPLE_BODY_LEN);
            let start = SAMPLE_BODY_LEN;
            let end = obj.len() - TRAILER_LEN;
            let i = start + (flip % (end - start));
            obj[i] ^= xor | 1;
            prop_assert!(matches!(open(&obj), Err(SpanSegError::Corrupted(_))));
        }
    }
}
