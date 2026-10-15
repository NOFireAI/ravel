//! `ravel-cli inspect kidx` (#2760): verifies every bucket frame of a
//! per-part key-index leaf, then prints every header field, the bucket-size
//! distribution and, with `--entries`, every entry. A corrupt or truncated
//! leaf exits non-zero with the typed rejection and prints nothing.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use std::process::{Command, Output};

use ravel_catalog::{
    KeyIndexLeafEntry, KeyIndexLeafInput, KeyIndexLeafLimits,
    encode_key_index_leaf_with_bucket_bits,
};
use ravel_codec::key_idx::KeyType;
use ravel_types::{Signal, TenantHash};

fn entry(key: u64, entry_ordinal: u64, blocks: &[u32]) -> KeyIndexLeafEntry {
    KeyIndexLeafEntry {
        key8: key.to_be_bytes(),
        entry_ordinal,
        blocks: blocks.to_vec(),
    }
}

fn fixture_leaf() -> Vec<u8> {
    let entries = [
        entry(0x0100, 0, &[0, 3, 7]),
        entry(0x0100, 4, &[2]),
        entry(0x0200, 1, &[1, 2]),
        entry(0x0300, 2, &[5]),
        entry(0x0400, 5, &[0]),
    ];
    encode_slice(&entries, 0, u64::MAX, &[3, 6])
}

fn encode_slice(
    entries: &[KeyIndexLeafEntry],
    key8_lo: u64,
    key8_hi: u64,
    uncovered_entry_ordinals: &[u64],
) -> Vec<u8> {
    let input = KeyIndexLeafInput {
        tenant_hash: TenantHash([0x11; 16]),
        signal: Signal::Logs,
        part_blake3: [0x22; 32],
        part_entry_count: 7,
        field: "service.name",
        key_type: KeyType::Str,
        key8_lo,
        key8_hi,
        uncovered_entry_ordinals,
        entries,
    };
    encode_key_index_leaf_with_bucket_bits(&input, 3, &KeyIndexLeafLimits::default())
        .expect("valid fixture")
        .bytes
}

fn run_inspect(path: &Path, entries: bool) -> Output {
    run_inspect_all(&[path], entries)
}

fn run_inspect_all(paths: &[&Path], entries: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ravel-cli"));
    command
        .args(["--store", "memory", "inspect", "kidx"])
        .args(paths);
    if entries {
        command.arg("--entries");
    }
    command.output().expect("ravel-cli runs")
}

fn write_leaf(dir: &tempfile::TempDir, name: &str, bytes: &[u8]) -> std::path::PathBuf {
    let path = dir.path().join(name);
    std::fs::write(&path, bytes).expect("writes leaf");
    path
}

fn scratch() -> tempfile::TempDir {
    tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("scratch dir")
}

// body_uncompressed_len is the five entries' encoded sizes: 13 + 11 + 12 +
// 11 + 11. Four distinct keys over eight buckets leave four empty.
const GOLDEN_HEADER: &str = "\
format_version: 1
tenant_hash: 11111111111111111111111111111111
signal: 2 (Logs)
part_blake3: 2222222222222222222222222222222222222222222222222222222222222222
field: service.name
key_type: 3 (Str)
key8_lo: 0x0000000000000000
key8_hi: 0xffffffffffffffff
bucket_bits: 3
entry_count: 5
part_entry_count: 7
uncovered_entry_ordinals: [3, 6]
body_len: 142
prefix_len: 148
size: 290
blake3: ca7633f6e4c56394f4df4ae4c5da96ea53f20df604185ae6fc653d1805b244b4
body_uncompressed_len: 58
buckets: 8 min=0 median=0 max=2 empty=4
";

const GOLDEN_ENTRIES: &str = "  bucket=1 key8=0000000000000100 entry_ordinal=0 blocks=[0, 3, 7]
  bucket=1 key8=0000000000000100 entry_ordinal=4 blocks=[2]
  bucket=3 key8=0000000000000200 entry_ordinal=1 blocks=[1, 2]
  bucket=5 key8=0000000000000300 entry_ordinal=2 blocks=[5]
  bucket=6 key8=0000000000000400 entry_ordinal=5 blocks=[0]
";

#[test]
fn inspect_kidx_prints_the_golden_report() {
    let dir = scratch();
    let path = write_leaf(&dir, "leaf.kidx", &fixture_leaf());

    let output = run_inspect(&path, false);
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8(output.stdout).expect("utf-8"),
        GOLDEN_HEADER
    );

    let output = run_inspect(&path, true);
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8(output.stdout).expect("utf-8"),
        format!("{GOLDEN_HEADER}{GOLDEN_ENTRIES}")
    );
}

#[test]
fn corrupt_or_truncated_leaf_exits_non_zero_with_the_typed_error() {
    let dir = scratch();
    let good = fixture_leaf();
    let mut flipped = good.clone();
    let last = flipped.len() - 1;
    flipped[last] ^= 0x01;
    let cases: [(&str, Vec<u8>, &str); 4] = [
        ("frame.kidx", flipped, "frame crc32c"),
        (
            "truncated.kidx",
            good[..good.len() - 1].to_vec(),
            "prefix_len + body_len",
        ),
        (
            "prefix.kidx",
            good[..30].to_vec(),
            "corrupted key-index leaf",
        ),
        ("magic.kidx", b"RKI2".repeat(10), "magic"),
    ];
    for (name, bytes, needle) in cases {
        let path = write_leaf(&dir, name, &bytes);
        let output = run_inspect(&path, true);
        assert!(!output.status.success(), "{name}: {output:?}");
        assert!(output.stdout.is_empty(), "{name}: {output:?}");
        let stderr = String::from_utf8(output.stderr).expect("utf-8");
        assert!(
            stderr.contains("failed verification: corrupted key-index leaf")
                && stderr.contains(needle),
            "{name}: {stderr}"
        );
    }
}

#[test]
fn slices_of_one_part_field_must_agree_and_tile() {
    let dir = scratch();
    let low = [entry(0x0100, 0, &[0])];
    let high = [entry(0x9000, 1, &[4])];
    let a = write_leaf(&dir, "a.kidx", &encode_slice(&low, 0, 0x8fff, &[2]));
    let b = write_leaf(&dir, "b.kidx", &encode_slice(&high, 0x9000, u64::MAX, &[2]));
    let output = run_inspect_all(&[&a, &b], false);
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("utf-8");
    assert_eq!(stdout.matches("leaf: ").count(), 2, "{stdout}");
    assert!(
        stdout.contains(&format!("leaf: {}\n", a.display())),
        "{stdout}"
    );
    assert!(
        stdout.ends_with("slice set: 2 leaves agree and tile the key space\n"),
        "{stdout}"
    );

    let other_uncovered = write_leaf(
        &dir,
        "c.kidx",
        &encode_slice(&high, 0x9000, u64::MAX, &[2, 5]),
    );
    let higher = [entry(0x9100, 1, &[4])];
    let gap = write_leaf(
        &dir,
        "d.kidx",
        &encode_slice(&higher, 0x9001, u64::MAX, &[2]),
    );
    for other in [&other_uncovered, &gap] {
        let output = run_inspect_all(&[&a, other], false);
        assert!(!output.status.success(), "{output:?}");
        assert!(output.stdout.is_empty(), "{output:?}");
        let stderr = String::from_utf8(output.stderr).expect("utf-8");
        assert!(
            stderr.contains("kidx leaves are not one slice set: corrupted key-index leaf"),
            "{stderr}"
        );
    }
}

#[test]
fn one_partial_slice_inspects_alone_but_not_beside_a_gap() {
    let dir = scratch();
    let low = [entry(0x10, 0, &[0, 2]), entry(0x20, 1, &[1])];
    let slice = write_leaf(&dir, "slice.kidx", &encode_slice(&low, 0, 99, &[2]));

    let output = run_inspect(&slice, true);
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("utf-8");
    assert!(stdout.starts_with("format_version: 1\n"), "{stdout}");
    assert!(
        stdout.contains("key8_lo: 0x0000000000000000\nkey8_hi: 0x0000000000000063\n"),
        "{stdout}"
    );
    assert!(stdout.contains("entry_count: 2\n"), "{stdout}");
    assert!(
        stdout.contains("uncovered_entry_ordinals: [2]\n"),
        "{stdout}"
    );
    assert!(
        stdout.contains("key8=0000000000000010 entry_ordinal=0 blocks=[0, 2]\n")
            && stdout.contains("key8=0000000000000020 entry_ordinal=1 blocks=[1]\n"),
        "{stdout}"
    );
    assert!(!stdout.contains("leaf: "), "{stdout}");
    assert!(!stdout.contains("slice set:"), "{stdout}");

    let high = [entry(0x9000, 3, &[4])];
    let gap = write_leaf(&dir, "gap.kidx", &encode_slice(&high, 101, u64::MAX, &[2]));
    let output = run_inspect_all(&[&slice, &gap], false);
    assert!(!output.status.success(), "{output:?}");
    assert!(output.stdout.is_empty(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).expect("utf-8");
    assert!(
        stderr.contains("kidx leaves are not one slice set: corrupted key-index leaf"),
        "{stderr}"
    );
}
