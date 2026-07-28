//! Corrupt-input corpus for the snapshot part envelope and HEAD record
//! codec: every violation in docs/metric-index-plan.md 3.1's validation
//! list must surface as a typed error, never a panic, and truncation at
//! every structural boundary must be safe (docs/metric-index-plan.md P1
//! tests, .claude/skills/format-change).

#![allow(clippy::expect_used)]

use prost::Message;
use ravel_catalog::{PartLimits, SnapshotFormatError, decode_head, decode_part};
use ravel_proto::catalog::v1::{SnapshotEntry, SnapshotHead, SnapshotPartHeader, SnapshotPartRef};

fn base_entries() -> Vec<SnapshotEntry> {
    vec![
        SnapshotEntry {
            level: 0,
            shard: 0,
            ingest_hour_bucket: 5,
            writer_id: vec![0xAA; 16],
            writer_epoch: 1,
            writer_seq: 1,
            content_hash: vec![0xBB; 32],
            object_size: 100,
            min_event_ts_ns: 0,
            max_event_ts_ns: 100,
            sample_count: 1,
            series_count: 1,
            segment_format_version: 1,
            created_unix_ns: 1_000,
        },
        SnapshotEntry {
            level: 0,
            shard: 0,
            ingest_hour_bucket: 5,
            writer_id: vec![0xAA; 16],
            writer_epoch: 1,
            writer_seq: 2,
            content_hash: vec![0xCC; 32],
            object_size: 100,
            min_event_ts_ns: 0,
            max_event_ts_ns: 100,
            sample_count: 1,
            series_count: 1,
            segment_format_version: 1,
            created_unix_ns: 1_000,
        },
    ]
}

fn base_header(entries: &[SnapshotEntry], entries_uncompressed_len: u64) -> SnapshotPartHeader {
    SnapshotPartHeader {
        format_version: 1,
        tenant_hash: vec![0x11; 16],
        signal: 1,
        shard_count: 8,
        watermark_hour: 10,
        entry_count: entries.len() as u64,
        entries_uncompressed_len,
    }
}

fn encode_entries_raw(entries: &[SnapshotEntry]) -> Vec<u8> {
    let mut raw = Vec::new();
    for entry in entries {
        raw.extend_from_slice(&entry.encode_length_delimited_to_vec());
    }
    raw
}

/// Assembles a part envelope byte-for-byte from raw components, bypassing
/// `encode_part`'s validation so tests can construct any corrupt shape.
fn assemble(
    header: &SnapshotPartHeader,
    entries_raw: &[u8],
    header_crc_override: Option<u32>,
    body_crc_override: Option<u32>,
) -> Vec<u8> {
    let header_bytes = header.encode_to_vec();
    let header_len = u32::try_from(header_bytes.len()).expect("header fits u32");
    let body = zstd::bulk::compress(entries_raw, 3).expect("compress");

    let mut out = Vec::new();
    out.extend_from_slice(b"RCS1");
    out.push(1u8);
    out.extend_from_slice(&[0, 0, 0]);
    out.extend_from_slice(&header_len.to_le_bytes());
    out.extend_from_slice(&header_bytes);
    let header_crc = header_crc_override.unwrap_or_else(|| crc32c::crc32c(&out));

    let body_len = body.len() as u64;
    out.extend_from_slice(&body_len.to_le_bytes());
    out.extend_from_slice(&body);
    let body_crc = body_crc_override.unwrap_or_else(|| crc32c::crc32c(&body));

    out.extend_from_slice(&body_crc.to_le_bytes());
    out.extend_from_slice(&header_crc.to_le_bytes());
    out
}

fn valid_part() -> Vec<u8> {
    let entries = base_entries();
    let raw = encode_entries_raw(&entries);
    let header = base_header(&entries, raw.len() as u64);
    assemble(&header, &raw, None, None)
}

#[test]
fn too_small() {
    for len in 0..28 {
        let bytes = vec![0u8; len];
        let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
        assert_eq!(err, SnapshotFormatError::TooSmall { size: len });
    }
}

#[test]
fn bad_magic() {
    let mut bytes = valid_part();
    bytes[0] = b'X';
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::BadMagic);
}

#[test]
fn unsupported_version() {
    let mut bytes = valid_part();
    bytes[4] = 2;
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::UnsupportedVersion(2));
}

#[test]
fn reserved_non_zero() {
    let mut bytes = valid_part();
    bytes[5] = 1;
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::ReservedNonZero);
}

#[test]
fn header_crc_mismatch() {
    let entries = base_entries();
    let raw = encode_entries_raw(&entries);
    let header = base_header(&entries, raw.len() as u64);
    let bytes = assemble(&header, &raw, Some(0xDEAD_BEEF), None);
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::HeaderCrcMismatch);
}

#[test]
fn body_crc_mismatch() {
    let entries = base_entries();
    let raw = encode_entries_raw(&entries);
    let header = base_header(&entries, raw.len() as u64);
    let bytes = assemble(&header, &raw, None, Some(0xDEAD_BEEF));
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::BodyCrcMismatch);
}

#[test]
fn header_decode_error() {
    // A header region filled with continuation-bit-set bytes never
    // terminates a protobuf varint tag, so decode always fails, whatever
    // the declared length.
    let garbage_header_bytes = vec![0xFFu8; 8];
    let header_len = u32::try_from(garbage_header_bytes.len()).expect("fits");
    let mut out = Vec::new();
    out.extend_from_slice(b"RCS1");
    out.push(1u8);
    out.extend_from_slice(&[0, 0, 0]);
    out.extend_from_slice(&header_len.to_le_bytes());
    out.extend_from_slice(&garbage_header_bytes);
    let header_crc = crc32c::crc32c(&out);

    let body = zstd::bulk::compress(&[], 3).expect("compress");
    out.extend_from_slice(&(body.len() as u64).to_le_bytes());
    out.extend_from_slice(&body);
    let body_crc = crc32c::crc32c(&body);
    out.extend_from_slice(&body_crc.to_le_bytes());
    out.extend_from_slice(&header_crc.to_le_bytes());

    let err = decode_part(&out, &PartLimits::default()).expect_err("decode must fail");
    assert!(matches!(err, SnapshotFormatError::HeaderDecode(_)));
}

#[test]
fn header_version_mismatch() {
    let entries = base_entries();
    let raw = encode_entries_raw(&entries);
    let mut header = base_header(&entries, raw.len() as u64);
    header.format_version = 2;
    let bytes = assemble(&header, &raw, None, None);
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::HeaderVersionMismatch {
            header: 2,
            envelope: 1
        }
    );
}

#[test]
fn bad_tenant_hash_len() {
    let entries = base_entries();
    let raw = encode_entries_raw(&entries);
    let mut header = base_header(&entries, raw.len() as u64);
    header.tenant_hash = vec![0x11; 15];
    let bytes = assemble(&header, &raw, None, None);
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::BadTenantHashLen(15));
}

#[test]
fn decompressed_too_large() {
    let entries = base_entries();
    let raw = encode_entries_raw(&entries);
    let header = base_header(&entries, raw.len() as u64);
    let bytes = assemble(&header, &raw, None, None);
    let limits = PartLimits {
        max_snapshot_part_bytes: 1,
    };
    let err = decode_part(&bytes, &limits).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::DecompressedTooLarge {
            declared: raw.len() as u64,
            cap: 1,
        }
    );
}

#[test]
fn decompressed_len_mismatch() {
    let entries = base_entries();
    let raw = encode_entries_raw(&entries);
    let header = base_header(&entries, raw.len() as u64 + 1);
    let bytes = assemble(&header, &raw, None, None);
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::DecompressedLenMismatch {
            expected: raw.len() as u64 + 1,
            actual: raw.len() as u64,
        }
    );
}

#[test]
fn entry_decode_error() {
    // A single 0xFF byte is an unterminated varint length prefix for
    // decode_length_delimited.
    let raw = vec![0xFFu8; 4];
    let header = base_header(&[], raw.len() as u64);
    let bytes = assemble(&header, &raw, None, None);
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert!(matches!(err, SnapshotFormatError::EntryDecode(_)));
}

#[test]
fn entry_count_mismatch() {
    let entries = base_entries();
    let raw = encode_entries_raw(&entries);
    let mut header = base_header(&entries, raw.len() as u64);
    header.entry_count = 3;
    let bytes = assemble(&header, &raw, None, None);
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::EntryCountMismatch {
            expected: 3,
            actual: 2,
        }
    );
}

#[test]
fn entries_unsorted() {
    let mut entries = base_entries();
    entries.reverse();
    let raw = encode_entries_raw(&entries);
    let header = base_header(&entries, raw.len() as u64);
    let bytes = assemble(&header, &raw, None, None);
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::EntriesUnsorted);
}

#[test]
fn duplicate_entry() {
    let mut entries = base_entries();
    entries[1] = entries[0].clone();
    let raw = encode_entries_raw(&entries);
    let header = base_header(&entries, raw.len() as u64);
    let bytes = assemble(&header, &raw, None, None);
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::DuplicateEntry);
}

#[test]
fn watermark_exceeded() {
    let mut entries = base_entries();
    entries[1].ingest_hour_bucket = 11;
    entries.sort_by_key(|e| e.ingest_hour_bucket);
    let raw = encode_entries_raw(&entries);
    let header = base_header(&entries, raw.len() as u64);
    let bytes = assemble(&header, &raw, None, None);
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::WatermarkExceeded {
            hour: 11,
            watermark: 10,
        }
    );
}

#[test]
fn unsupported_level() {
    let mut entries = base_entries();
    entries[0].level = 1;
    let raw = encode_entries_raw(&entries);
    let header = base_header(&entries, raw.len() as u64);
    let bytes = assemble(&header, &raw, None, None);
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::UnsupportedLevel(1));
}

#[test]
fn bad_writer_id_len() {
    let mut entries = base_entries();
    entries[0].writer_id = vec![0xAA; 15];
    let raw = encode_entries_raw(&entries);
    let header = base_header(&entries, raw.len() as u64);
    let bytes = assemble(&header, &raw, None, None);
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::BadFieldLen {
            field: "writer_id",
            expected: 16,
            actual: 15,
        }
    );
}

#[test]
fn bad_content_hash_len() {
    let mut entries = base_entries();
    entries[0].content_hash = vec![0xBB; 31];
    let raw = encode_entries_raw(&entries);
    let header = base_header(&entries, raw.len() as u64);
    let bytes = assemble(&header, &raw, None, None);
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::BadFieldLen {
            field: "content_hash",
            expected: 32,
            actual: 31,
        }
    );
}

#[test]
fn trailing_bytes() {
    let mut bytes = valid_part();
    bytes.push(0);
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::TrailingBytes);
}

#[test]
fn truncation_never_panics_at_any_boundary() {
    let full = valid_part();
    for len in 0..full.len() {
        // Must return a typed error, never panic.
        let _ = decode_part(&full[..len], &PartLimits::default());
    }
}

// --- HEAD ---

fn base_head() -> SnapshotHead {
    SnapshotHead {
        format_version: 1,
        tenant_hash: vec![0x11; 16],
        signal: 1,
        shard_count: 8,
        watermark_hour: 5,
        parts: vec![SnapshotPartRef {
            key: "t/abc/catalog/m/snap/2026010100.aaaa.csnap".to_string(),
            blake3: vec![0x22; 32],
            size: 100,
            entry_count: 2,
            watermark_hour: 5,
        }],
        folder_id: vec![0x33; 16],
        created_unix_ns: 1_000,
    }
}

#[test]
fn head_decode_error() {
    let err = decode_head(&[0xFFu8; 8]).expect_err("decode must fail");
    assert!(matches!(err, SnapshotFormatError::HeadDecode(_)));
}

#[test]
fn head_unsupported_version() {
    let mut head = base_head();
    head.format_version = 2;
    let bytes = head.encode_to_vec();
    let err = decode_head(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::UnsupportedHeadVersion(2));
}

#[test]
fn head_bad_tenant_hash_len() {
    let mut head = base_head();
    head.tenant_hash = vec![0x11; 15];
    let bytes = head.encode_to_vec();
    let err = decode_head(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::BadHeadTenantHashLen(15));
}

#[test]
fn head_bad_folder_id_len() {
    let mut head = base_head();
    head.folder_id = vec![0x33; 15];
    let bytes = head.encode_to_vec();
    let err = decode_head(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::BadFolderIdLen(15));
}

#[test]
fn head_no_parts() {
    let mut head = base_head();
    head.parts.clear();
    let bytes = head.encode_to_vec();
    let err = decode_head(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::HeadNoParts);
}

#[test]
fn head_bad_part_blake3_len() {
    let mut head = base_head();
    head.parts[0].blake3 = vec![0x22; 31];
    let bytes = head.encode_to_vec();
    let err = decode_head(&bytes).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::BadPartRefFieldLen {
            index: 0,
            field: "blake3",
            expected: 32,
            actual: 31,
        }
    );
}

#[test]
fn head_empty_part_key() {
    let mut head = base_head();
    head.parts[0].key.clear();
    let bytes = head.encode_to_vec();
    let err = decode_head(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::EmptyPartKey { index: 0 });
}

#[test]
fn head_watermark_mismatch() {
    let mut head = base_head();
    head.watermark_hour = 6;
    let bytes = head.encode_to_vec();
    let err = decode_head(&bytes).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::HeadWatermarkMismatch {
            head: 6,
            max_part: 5,
        }
    );
}
