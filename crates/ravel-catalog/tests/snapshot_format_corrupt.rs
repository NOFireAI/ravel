//! Corrupt-input corpus for the snapshot part envelope and HEAD record
//! codec: every violation in the part envelope's validation
//! list must surface as a typed error, never a panic, and truncation at
//! every structural boundary must be safe (.claude/skills/format-change).

#![allow(clippy::expect_used)]

use prost::Message;
use ravel_catalog::{
    PartLimits, PostingsLimits, SnapshotFormatError, decode_head, decode_part, decode_postings,
};
use ravel_proto::catalog::v1::SnapshotPostingsHeader;
use ravel_proto::catalog::v1::{
    SnapshotEntry, SnapshotHead, SnapshotPartHeader, SnapshotPartRef, SnapshotPostingsRef,
};

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
        min_hour: 0,
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
fn below_min_hour() {
    // Entries sit at hour 5; a header claiming the part starts at hour 6
    // makes them fall below the part's declared floor (ADR-0063).
    let entries = base_entries();
    let raw = encode_entries_raw(&entries);
    let mut header = base_header(&entries, raw.len() as u64);
    header.min_hour = 6;
    let bytes = assemble(&header, &raw, None, None);
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::BelowMinHour {
            hour: 5,
            min_hour: 6,
        }
    );
}

#[test]
fn header_min_hour_exceeds_watermark() {
    // An inverted range (min_hour 11 > watermark_hour 10) is a malformed
    // header, rejected before any per-entry bound is checked.
    let entries = base_entries();
    let raw = encode_entries_raw(&entries);
    let mut header = base_header(&entries, raw.len() as u64);
    header.min_hour = 11;
    let bytes = assemble(&header, &raw, None, None);
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::MinHourExceedsWatermark {
            min_hour: 11,
            watermark: 10,
        }
    );
}

#[test]
fn unsupported_level() {
    // Level 0 (L0 commit) and level 1 (L1 compaction part) are defined;
    // anything higher is reserved and must be rejected, not guessed at.
    let mut entries = base_entries();
    entries[0].level = 2;
    let raw = encode_entries_raw(&entries);
    let header = base_header(&entries, raw.len() as u64);
    let bytes = assemble(&header, &raw, None, None);
    let err = decode_part(&bytes, &PartLimits::default()).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::UnsupportedLevel(2));
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
            min_hour: 0,
        }],
        postings: None,
        folder_id: vec![0x33; 16],
        created_unix_ns: 1_000,
        shard_generation_count: 1,
        column_stats: None,
        column_stats_part: None,
    }
}

fn head_with_postings() -> SnapshotHead {
    let mut head = base_head();
    head.postings = Some(SnapshotPostingsRef {
        key: "t/abc/catalog/m/idx/2026010100.dddd.npost".to_string(),
        blake3: vec![0x44; 32],
        size: 50,
        name_count: 3,
        part_blake3: vec![vec![0x22; 32]],
    });
    head
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

#[test]
fn head_bad_postings_blake3_len() {
    let mut head = head_with_postings();
    head.postings.as_mut().expect("postings").blake3 = vec![0x44; 31];
    let bytes = head.encode_to_vec();
    let err = decode_head(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::BadPostingsRefBlake3Len(31));
}

#[test]
fn head_empty_postings_key() {
    let mut head = head_with_postings();
    head.postings.as_mut().expect("postings").key.clear();
    let bytes = head.encode_to_vec();
    let err = decode_head(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::EmptyPostingsKey);
}

#[test]
fn head_postings_part_count_mismatch() {
    let mut head = head_with_postings();
    head.postings
        .as_mut()
        .expect("postings")
        .part_blake3
        .push(vec![0x55; 32]);
    let bytes = head.encode_to_vec();
    let err = decode_head(&bytes).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::PostingsRefPartCountMismatch {
            postings_parts: 2,
            head_parts: 1,
        }
    );
}

#[test]
fn head_bad_postings_part_blake3_len() {
    let mut head = head_with_postings();
    head.postings.as_mut().expect("postings").part_blake3[0] = vec![0x22; 31];
    let bytes = head.encode_to_vec();
    let err = decode_head(&bytes).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::BadPostingsRefPartBlake3Len {
            index: 0,
            actual: 31,
        }
    );
}

#[test]
fn head_postings_part_blake3_mismatch() {
    let mut head = head_with_postings();
    head.postings.as_mut().expect("postings").part_blake3[0] = vec![0x99; 32];
    let bytes = head.encode_to_vec();
    let err = decode_head(&bytes).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::PostingsRefPartBlake3Mismatch { index: 0 }
    );
}

// --- postings envelope ---

fn write_uvarint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

fn encode_name_block(out: &mut Vec<u8>, name: &[u8], ordinals: &[u64]) {
    write_uvarint(out, name.len() as u64);
    out.extend_from_slice(name);
    write_uvarint(out, ordinals.len() as u64);
    let mut prev = 0u64;
    for (i, &ordinal) in ordinals.iter().enumerate() {
        let delta = if i == 0 { ordinal } else { ordinal - prev };
        write_uvarint(out, delta);
        prev = ordinal;
    }
}

fn valid_part_blake3() -> Vec<[u8; 32]> {
    vec![[0x22; 32]]
}

fn base_postings_header(name_count: u32, body_uncompressed_len: u64) -> SnapshotPostingsHeader {
    SnapshotPostingsHeader {
        format_version: 1,
        tenant_hash: vec![0x11; 16],
        signal: 1,
        part_blake3: valid_part_blake3().iter().map(|h| h.to_vec()).collect(),
        entry_count: 3,
        name_count,
        body_uncompressed_len,
    }
}

fn valid_postings_body() -> Vec<u8> {
    let mut body = Vec::new();
    encode_name_block(&mut body, b"cpu", &[0, 2]);
    encode_name_block(&mut body, b"mem", &[1]);
    body
}

/// Assembles a postings envelope byte-for-byte from raw components,
/// bypassing `encode_postings`'s validation so tests can construct any
/// corrupt shape.
fn assemble_postings(
    header: &SnapshotPostingsHeader,
    body_raw: &[u8],
    header_crc_override: Option<u32>,
    body_crc_override: Option<u32>,
) -> Vec<u8> {
    let header_bytes = header.encode_to_vec();
    let header_len = u32::try_from(header_bytes.len()).expect("header fits u32");
    let body = zstd::bulk::compress(body_raw, 3).expect("compress");

    let mut out = Vec::new();
    out.extend_from_slice(b"RNP1");
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

fn valid_postings() -> Vec<u8> {
    let body = valid_postings_body();
    let header = base_postings_header(2, body.len() as u64);
    assemble_postings(&header, &body, None, None)
}

fn decode_valid(bytes: &[u8]) -> Result<ravel_catalog::DecodedPostings, SnapshotFormatError> {
    decode_postings(bytes, &PostingsLimits::default(), &valid_part_blake3())
}

#[test]
fn postings_too_small() {
    for len in 0..24 {
        let bytes = vec![0u8; len];
        let err = decode_valid(&bytes).expect_err("decode must fail");
        assert_eq!(err, SnapshotFormatError::PostingsTooSmall { size: len });
    }
}

#[test]
fn postings_bad_magic() {
    let mut bytes = valid_postings();
    bytes[0] = b'X';
    let err = decode_valid(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::BadMagic);
}

#[test]
fn postings_unsupported_version() {
    let mut bytes = valid_postings();
    bytes[4] = 2;
    let err = decode_valid(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::PostingsUnsupportedVersion(2));
}

#[test]
fn postings_reserved_non_zero() {
    let mut bytes = valid_postings();
    bytes[5] = 1;
    let err = decode_valid(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::ReservedNonZero);
}

#[test]
fn postings_header_crc_mismatch() {
    let body = valid_postings_body();
    let header = base_postings_header(2, body.len() as u64);
    let bytes = assemble_postings(&header, &body, Some(0xDEAD_BEEF), None);
    let err = decode_valid(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::HeaderCrcMismatch);
}

#[test]
fn postings_body_crc_mismatch() {
    let body = valid_postings_body();
    let header = base_postings_header(2, body.len() as u64);
    let bytes = assemble_postings(&header, &body, None, Some(0xDEAD_BEEF));
    let err = decode_valid(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::BodyCrcMismatch);
}

#[test]
fn postings_header_decode_error() {
    let garbage_header_bytes = vec![0xFFu8; 8];
    let header_len = u32::try_from(garbage_header_bytes.len()).expect("fits");
    let mut out = Vec::new();
    out.extend_from_slice(b"RNP1");
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

    let err = decode_valid(&out).expect_err("decode must fail");
    assert!(matches!(err, SnapshotFormatError::PostingsHeaderDecode(_)));
}

#[test]
fn postings_header_version_mismatch() {
    let body = valid_postings_body();
    let mut header = base_postings_header(2, body.len() as u64);
    header.format_version = 2;
    let bytes = assemble_postings(&header, &body, None, None);
    let err = decode_valid(&bytes).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::HeaderVersionMismatch {
            header: 2,
            envelope: 1,
        }
    );
}

#[test]
fn postings_bad_tenant_hash_len() {
    let body = valid_postings_body();
    let mut header = base_postings_header(2, body.len() as u64);
    header.tenant_hash = vec![0x11; 15];
    let bytes = assemble_postings(&header, &body, None, None);
    let err = decode_valid(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::BadTenantHashLen(15));
}

#[test]
fn postings_bad_part_blake3_len() {
    let body = valid_postings_body();
    let mut header = base_postings_header(2, body.len() as u64);
    header.part_blake3[0] = vec![0x22; 31];
    let bytes = assemble_postings(&header, &body, None, None);
    let err = decode_valid(&bytes).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::PostingsPartBlake3Len {
            index: 0,
            actual: 31,
        }
    );
}

#[test]
fn postings_part_binding_mismatch() {
    let bytes = valid_postings();
    let wrong_part = vec![[0x99; 32]];
    let err = decode_postings(&bytes, &PostingsLimits::default(), &wrong_part)
        .expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::PostingsPartBindingMismatch);
}

#[test]
fn postings_decompressed_too_large() {
    let bytes = valid_postings();
    let limits = PostingsLimits {
        max_postings_bytes: 1,
    };
    let body_len = valid_postings_body().len() as u64;
    let err = decode_postings(&bytes, &limits, &valid_part_blake3()).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::DecompressedTooLarge {
            declared: body_len,
            cap: 1,
        }
    );
}

#[test]
fn postings_decompressed_len_mismatch() {
    let body = valid_postings_body();
    let header = base_postings_header(2, body.len() as u64 + 1);
    let bytes = assemble_postings(&header, &body, None, None);
    let err = decode_valid(&bytes).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::DecompressedLenMismatch {
            expected: body.len() as u64 + 1,
            actual: body.len() as u64,
        }
    );
}

#[test]
fn postings_name_not_utf8() {
    let mut body = Vec::new();
    write_uvarint(&mut body, 2);
    body.extend_from_slice(&[0xFF, 0xFE]);
    write_uvarint(&mut body, 0);
    let header = base_postings_header(1, body.len() as u64);
    let bytes = assemble_postings(&header, &body, None, None);
    let err = decode_valid(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::PostingsNameNotUtf8);
}

#[test]
fn postings_names_unsorted() {
    let mut body = Vec::new();
    encode_name_block(&mut body, b"mem", &[0]);
    encode_name_block(&mut body, b"cpu", &[1]);
    let header = base_postings_header(2, body.len() as u64);
    let bytes = assemble_postings(&header, &body, None, None);
    let err = decode_valid(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::PostingsNamesUnsorted);
}

#[test]
fn postings_duplicate_name() {
    let mut body = Vec::new();
    encode_name_block(&mut body, b"cpu", &[0]);
    encode_name_block(&mut body, b"cpu", &[1]);
    let header = base_postings_header(2, body.len() as u64);
    let bytes = assemble_postings(&header, &body, None, None);
    let err = decode_valid(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::PostingsDuplicateName);
}

#[test]
fn postings_name_count_mismatch() {
    let body = valid_postings_body();
    let header = base_postings_header(3, body.len() as u64);
    let bytes = assemble_postings(&header, &body, None, None);
    let err = decode_valid(&bytes).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::PostingsNameCountMismatch {
            expected: 3,
            actual: 2,
        }
    );
}

#[test]
fn postings_ordinal_out_of_bounds() {
    let mut body = Vec::new();
    encode_name_block(&mut body, b"cpu", &[5]);
    let mut header = base_postings_header(1, body.len() as u64);
    header.entry_count = 3;
    let bytes = assemble_postings(&header, &body, None, None);
    let err = decode_valid(&bytes).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::PostingsOrdinalOutOfBounds {
            name: "cpu".to_string(),
            ordinal: 5,
            entry_count: 3,
        }
    );
}

#[test]
fn postings_ordinals_not_strictly_increasing() {
    let mut body = Vec::new();
    write_uvarint(&mut body, 3);
    body.extend_from_slice(b"cpu");
    write_uvarint(&mut body, 2);
    write_uvarint(&mut body, 1);
    write_uvarint(&mut body, 0);
    let header = base_postings_header(1, body.len() as u64);
    let bytes = assemble_postings(&header, &body, None, None);
    let err = decode_valid(&bytes).expect_err("decode must fail");
    assert_eq!(
        err,
        SnapshotFormatError::PostingsOrdinalsNotStrictlyIncreasing {
            name: "cpu".to_string(),
        }
    );
}

#[test]
fn postings_trailing_bytes() {
    let mut bytes = valid_postings();
    bytes.push(0);
    let err = decode_valid(&bytes).expect_err("decode must fail");
    assert_eq!(err, SnapshotFormatError::TrailingBytes);
}

#[test]
fn postings_truncation_never_panics_at_any_boundary() {
    let full = valid_postings();
    for len in 0..full.len() {
        let _ = decode_valid(&full[..len]);
    }
}
