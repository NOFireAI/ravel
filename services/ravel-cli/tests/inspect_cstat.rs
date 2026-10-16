//! `ravel-cli inspect cstat` (#1598 deliverable 4): prints the envelope
//! version, `header_len`, every `ColumnStatsHeader` field, and a per-column
//! `dictionary_present` listing, driven through the crate's own
//! `catalog::render_inspect_cstat` entry point. An object whose declared
//! `body_uncompressed_len` exceeds the decode ceiling must still print the
//! header and the over-ceiling verdict, without decompressing (no reader can
//! decompress such an object). A truncated or corrupt object must produce a
//! typed error, never a panic.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_catalog::{
    ColumnStatsLimits, DEFAULT_MAX_COLUMN_STATS_BYTES, decode_column_stats_header,
    encode_column_stats_v3,
};
use ravel_cli::catalog::render_inspect_cstat;
use ravel_proto::catalog::v1::column_value::Kind;
use ravel_proto::catalog::v1::{
    ColumnStat, ColumnStatsHeader, ColumnStatsSegment, ColumnValue, DictEntry,
};

use proptest::prelude::*;
use prost::Message as _;

fn i64_value(v: i64) -> ColumnValue {
    ColumnValue {
        kind: Some(Kind::I64(v)),
    }
}

/// Mirrors `crates/ravel-catalog/src/snapshot_format/column_stats.rs`'s own
/// `segment()` test fixture: a single I64 column, 10-entry dictionary,
/// `sum = 45` (0+1+...+9). `writer_id` is 32 bytes: v3 keys segments by
/// content hash (ADR-1413 decision 6, #1600), not the 16-byte writer
/// identity v1 used.
fn fixture_segment() -> ColumnStatsSegment {
    let dictionary: Vec<DictEntry> = (0..10)
        .map(|v| DictEntry {
            value: Some(i64_value(v)),
            count: 1,
        })
        .collect();
    ColumnStatsSegment {
        ingest_hour_bucket: 1,
        shard: 0,
        writer_id: vec![0xAA; 32],
        writer_epoch: 1,
        writer_seq: 1,
        columns: vec![ColumnStat {
            name: "AdvEngineID".to_string(),
            declared_type: 2,
            non_null_count: 10,
            null_count: 0,
            min: Some(i64_value(0)),
            max: Some(i64_value(9)),
            dictionary_present: true,
            dictionary,
            sum: Some(45),
        }],
    }
}

#[test]
fn inspect_cstat_prints_exact_header_values_and_dictionary_presence() {
    let segments = vec![fixture_segment()];
    let bytes = encode_column_stats_v3(
        [0x11; 16],
        3,
        [0x22; 32],
        &segments,
        DEFAULT_MAX_COLUMN_STATS_BYTES,
    )
    .expect("encodes a valid v3 object");

    let mut out = String::new();
    render_inspect_cstat(&bytes, &mut out).expect("a valid object under ceiling decodes cleanly");

    assert!(out.contains("envelope_version: 3\n"), "{out}");
    assert!(out.contains("format_version: 3\n"), "{out}");
    assert!(
        out.contains(&format!("tenant_hash: {}\n", hex::encode([0x11; 16]))),
        "{out}"
    );
    assert!(out.contains("signal: 3\n"), "{out}");
    assert!(
        out.contains(&format!("part_blake3: {}\n", hex::encode([0x22; 32]))),
        "{out}"
    );
    assert!(out.contains("segment_count: 1\n"), "{out}");
    assert!(
        out.contains(&format!(
            "over_ceiling (body_uncompressed_len > {DEFAULT_MAX_COLUMN_STATS_BYTES}): false\n"
        )),
        "{out}"
    );
    assert!(
        out.contains(
            "segment shard=0 ingest_hour_bucket=1 writer_epoch=1 writer_seq=1 \
             column=AdvEngineID dictionary_present=true\n"
        ),
        "{out}"
    );
}

/// Hand-assembles a `.cstat` envelope byte-for-byte per the layout documented
/// at `crates/ravel-catalog/src/snapshot_format/column_stats.rs:1-19`:
/// `magic(4) | version(1) | reserved(3) | header_len(u32 LE) | header |
/// body_len(u64 LE) | body | body_crc32c(u32 LE, over body) |
/// header_crc32c(u32 LE, over magic..header)`. `decode_column_stats_prefix`
/// never checks `body_len`/`body` against the header's declared
/// `body_uncompressed_len` -- only `decode_column_stats` does, after
/// decompressing -- so an envelope with a small, arbitrary `body` and a
/// header declaring `body_uncompressed_len` above the ceiling is a valid,
/// decodable-at-the-header-level object that no full decode can ever read.
fn encode_forged_envelope(header: &ColumnStatsHeader, body: &[u8]) -> Vec<u8> {
    let header_bytes = header.encode_to_vec();
    let mut prefix = Vec::new();
    prefix.extend_from_slice(b"RCST");
    prefix.push(3u8);
    prefix.extend_from_slice(&[0u8; 3]);
    prefix.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());
    prefix.extend_from_slice(&header_bytes);
    let header_crc = crc32c::crc32c(&prefix);

    let mut out = prefix;
    out.extend_from_slice(&(body.len() as u64).to_le_bytes());
    out.extend_from_slice(body);
    out.extend_from_slice(&crc32c::crc32c(body).to_le_bytes());
    out.extend_from_slice(&header_crc.to_le_bytes());
    out
}

#[test]
fn inspect_cstat_reports_over_ceiling_without_decompressing() {
    let header = ColumnStatsHeader {
        format_version: 3,
        tenant_hash: vec![0x33; 16],
        signal: 2,
        part_blake3: vec![vec![0x44; 32]],
        segment_count: 1,
        body_uncompressed_len: DEFAULT_MAX_COLUMN_STATS_BYTES + 1,
    };
    // Deliberately not a valid zstd stream: the over-ceiling path must never
    // attempt to decompress this.
    let body = b"not zstd, and that must never matter here";
    let bytes = encode_forged_envelope(&header, body);

    // The header-only path proves this by construction: it decodes cleanly
    // even though `decode_column_stats` (which does decompress) would fail
    // on this body.
    decode_column_stats_header(&bytes).expect("header parses without touching the body");
    ravel_catalog::decode_column_stats(&bytes, &ColumnStatsLimits::default())
        .expect_err("a real decode must refuse an over-ceiling declared length");

    let mut out = String::new();
    render_inspect_cstat(&bytes, &mut out)
        .expect("the over-ceiling verdict is a successful report, not an error");

    assert!(
        out.contains(&format!(
            "body_uncompressed_len: {}\n",
            DEFAULT_MAX_COLUMN_STATS_BYTES + 1
        )),
        "{out}"
    );
    assert!(
        out.contains(&format!(
            "over_ceiling (body_uncompressed_len > {DEFAULT_MAX_COLUMN_STATS_BYTES}): true\n"
        )),
        "{out}"
    );
    assert!(
        out.contains("dictionary_present listing: unavailable"),
        "{out}"
    );
}

#[test]
fn inspect_cstat_on_a_truncated_object_returns_a_typed_error_not_a_panic() {
    let segments = vec![fixture_segment()];
    let bytes = encode_column_stats_v3(
        [0x11; 16],
        3,
        [0x22; 32],
        &segments,
        DEFAULT_MAX_COLUMN_STATS_BYTES,
    )
    .expect("encodes a valid v3 object");
    let truncated = &bytes[..bytes.len() - 5];

    let mut out = String::new();
    let result = render_inspect_cstat(truncated, &mut out);
    assert!(
        result.is_err(),
        "a truncated object must be a typed error, not a silent success"
    );
}

#[test]
fn inspect_cstat_on_a_corrupt_object_returns_a_typed_error_not_a_panic() {
    let segments = vec![fixture_segment()];
    let mut bytes = encode_column_stats_v3(
        [0x11; 16],
        3,
        [0x22; 32],
        &segments,
        DEFAULT_MAX_COLUMN_STATS_BYTES,
    )
    .expect("encodes a valid v3 object");
    // Corrupt the magic so this fails the very first envelope check.
    bytes[0] ^= 0xFF;

    let mut out = String::new();
    let result = render_inspect_cstat(&bytes, &mut out);
    assert!(
        result.is_err(),
        "a corrupt magic must be a typed error, not a silent success"
    );
}

proptest! {
    /// The header parse (`decode_column_stats_header`, which
    /// `render_inspect_cstat` calls first) must never panic on arbitrary
    /// bytes: every rejection is a typed `SnapshotFormatError`, never a
    /// panic. This drives the same function the CLI command calls, so a
    /// counterexample here is a counterexample against the reachable
    /// `inspect cstat` surface.
    #[test]
    fn decode_column_stats_header_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = decode_column_stats_header(&bytes);
    }
}
