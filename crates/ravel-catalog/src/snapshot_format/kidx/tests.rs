use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;

use futures::executor::block_on;
use proptest::prelude::*;
use ravel_codec::key_idx::key8_str;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::catalog::v1::SnapshotPartRef;

use super::*;
use crate::CatalogConfig;

thread_local! {
    /// zstd decompressions `decode_frame` started on this thread: a frame
    /// refused before allocation leaves it unchanged.
    pub(super) static FRAME_DECOMPRESSIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Frame crc32c computations `check_frame_header` ran on this thread.
    pub(super) static FRAME_CRC_CHECKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

const TENANT: TenantHash = TenantHash([7u8; 16]);
const SIGNAL: Signal = Signal::Logs;
const PART_ENTRY_COUNT: u64 = 8;
const FIELD: &str = "service.name";
const UNCOVERED: [u64; 2] = [3, 6];

type RefEdit = fn(&mut SnapshotKeyIndexLeafRef);
type FrameEdit = fn(&mut [u8]);

fn part_hash(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

fn entry(key8: Key8, entry_ordinal: u64, blocks: &[u32]) -> KeyIndexLeafEntry {
    KeyIndexLeafEntry {
        key8,
        entry_ordinal,
        blocks: blocks.to_vec(),
    }
}

fn input<'a>(
    part_blake3: [u8; 32],
    entries: &'a [KeyIndexLeafEntry],
    uncovered: &'a [u64],
) -> KeyIndexLeafInput<'a> {
    KeyIndexLeafInput {
        tenant_hash: TENANT,
        signal: SIGNAL,
        part_blake3,
        part_entry_count: PART_ENTRY_COUNT,
        field: FIELD,
        key_type: KeyType::Str,
        key8_lo: 0,
        key8_hi: u64::MAX,
        uncovered_entry_ordinals: uncovered,
        entries,
    }
}

fn request(part_blake3: &[u8]) -> KeyIndexLeafRequest<'_> {
    KeyIndexLeafRequest {
        tenant_hash: TENANT,
        signal: SIGNAL,
        part_blake3,
        part_entry_count: PART_ENTRY_COUNT,
        field: FIELD,
        key_type: KeyType::Str,
    }
}

/// Probes `leaf` through an in-memory ranged source, returning the result
/// and every range read. A range past the end reads short, as a truncated
/// object does.
fn probe_with(
    leaf: &[u8],
    leaf_ref: &SnapshotKeyIndexLeafRef,
    request: &KeyIndexLeafRequest<'_>,
    key8: &Key8,
    limits: &KeyIndexLeafLimits,
) -> (
    Result<KeyIndexLeafProbe, KeyIndexLeafBreach>,
    Vec<Range<u64>>,
) {
    let reads = RefCell::new(Vec::new());
    let result = block_on(probe_key_index_leaf_ranged(
        leaf_ref,
        request,
        key8,
        limits,
        |range: Range<u64>| {
            reads.borrow_mut().push(range.clone());
            let end = (range.end as usize).min(leaf.len());
            let out = leaf
                .get(range.start as usize..end)
                .map(Bytes::copy_from_slice)
                .ok_or_else(|| "range starts past the object".to_string());
            async move { out }
        },
    ));
    (result, reads.into_inner())
}

fn probe(
    leaf: &[u8],
    leaf_ref: &SnapshotKeyIndexLeafRef,
    request: &KeyIndexLeafRequest<'_>,
    key8: &Key8,
) -> (KeyIndexLeafProbe, Vec<Range<u64>>) {
    let (result, reads) = probe_with(
        leaf,
        leaf_ref,
        request,
        key8,
        &KeyIndexLeafLimits::default(),
    );
    (result.expect("no isolation breach"), reads)
}

fn matches(pairs: &[(u64, &[u32])]) -> Vec<KeyIndexMatch> {
    pairs
        .iter()
        .map(|(ordinal, blocks)| KeyIndexMatch {
            entry_ordinal: *ordinal,
            blocks: blocks.to_vec(),
        })
        .collect()
}

const FIXTURE_BUCKET_BITS: u8 = 3;

struct Fixture {
    alpha: Key8,
    beta: Key8,
    /// Absent, but in alpha's (non-empty) bucket.
    absent_same_bucket: Key8,
    part: [u8; 32],
    entries: Vec<KeyIndexLeafEntry>,
    leaf: EncodedKeyIndexLeaf,
}

fn fixture() -> Fixture {
    let alpha = key8_str("alpha");
    let beta = key8_str("beta");
    let part = part_hash(b"part object bytes A");
    let mut entries = vec![
        entry(alpha, 4, &[2, 5]),
        entry(beta, 2, &[1]),
        entry(alpha, 1, &[0, 3, 7]),
    ];
    for i in 0..12u32 {
        let ordinal = [0u64, 1, 2, 4, 5, 7][i as usize % 6];
        entries.push(entry(
            key8_str(&format!("filler-{i}")),
            ordinal,
            &[i, i + 9],
        ));
    }
    let alpha_bucket = bucket_index(&alpha, FIXTURE_BUCKET_BITS).expect("bits");
    let absent_same_bucket = (0..)
        .map(|i| key8_str(&format!("absent-{i}")))
        .find(|k| bucket_index(k, FIXTURE_BUCKET_BITS).expect("bits") == alpha_bucket)
        .expect("some key mixes to alpha's bucket");
    let leaf = encode_key_index_leaf_with_bucket_bits(
        &input(part, &entries, &UNCOVERED),
        FIXTURE_BUCKET_BITS,
        &KeyIndexLeafLimits::default(),
    )
    .expect("encode");
    Fixture {
        alpha,
        beta,
        absent_same_bucket,
        part,
        entries,
        leaf,
    }
}

#[test]
fn leaf_probe_names_the_entries_and_blocks_holding_the_key() {
    let f = fixture();
    let req = request(&f.part);
    let leaf = &f.leaf.bytes;
    let leaf_ref = &f.leaf.leaf_ref;

    let (alpha, reads) = probe(leaf, leaf_ref, &req, &f.alpha);
    assert_eq!(
        alpha,
        KeyIndexLeafProbe::Matches {
            matches: matches(&[(1, &[0, 3, 7]), (4, &[2, 5])]),
            uncovered_entry_ordinals: UNCOVERED.to_vec(),
        }
    );
    assert_eq!(reads.len(), 2, "exactly two ranged GETs");
    assert_eq!(reads[0], 0..u64::from(leaf_ref.prefix_len));

    let (beta, _) = probe(leaf, leaf_ref, &req, &f.beta);
    assert_eq!(
        beta,
        KeyIndexLeafProbe::Matches {
            matches: matches(&[(2, &[1])]),
            uncovered_entry_ordinals: UNCOVERED.to_vec(),
        }
    );

    let (absent, reads) = probe(leaf, leaf_ref, &req, &f.absent_same_bucket);
    assert_eq!(
        absent,
        KeyIndexLeafProbe::Absent {
            uncovered_entry_ordinals: UNCOVERED.to_vec(),
        }
    );
    assert_eq!(reads.len(), 2, "the absent key's bucket is not empty");

    // The bucket comes from the header's bucket_bits, never the ref's: a ref
    // that disagrees rejects the leaf after GET 1.
    let mut lying_ref = leaf_ref.clone();
    lying_ref.bucket_bits = u32::from(FIXTURE_BUCKET_BITS) - 1;
    let (lied, reads) = probe(leaf, &lying_ref, &req, &f.alpha);
    assert!(
        matches!(
            &lied,
            KeyIndexLeafProbe::Rejected(KeyIndexLeafRejection::Corrupted(msg))
                if msg.contains("ref bucket_bits")
        ),
        "{lied:?}"
    );
    assert_eq!(reads.len(), 1);
}

#[test]
fn leaf_for_other_part_bytes_at_the_same_watermark_hour_is_uncovered() {
    let f = fixture();
    let part_a = SnapshotPartRef {
        key: "t/a/catalog/logs/snap/a.csnap".into(),
        blake3: f.part.to_vec(),
        size: 100,
        entry_count: PART_ENTRY_COUNT,
        watermark_hour: 500_000,
        ..Default::default()
    };
    let part_b = SnapshotPartRef {
        key: "t/a/catalog/logs/snap/b.csnap".into(),
        blake3: part_hash(b"part object bytes B").to_vec(),
        ..part_a.clone()
    };
    assert_eq!(part_a.watermark_hour, part_b.watermark_hour);

    let (bound, _) = probe(
        &f.leaf.bytes,
        &f.leaf.leaf_ref,
        &request(&part_a.blake3),
        &f.alpha,
    );
    assert!(
        matches!(bound, KeyIndexLeafProbe::Matches { .. }),
        "{bound:?}"
    );

    let (other, reads) = probe(
        &f.leaf.bytes,
        &f.leaf.leaf_ref,
        &request(&part_b.blake3),
        &f.alpha,
    );
    assert_eq!(
        other,
        KeyIndexLeafProbe::Rejected(KeyIndexLeafRejection::PartBindingMismatch {
            expected: hex::encode(&part_b.blake3),
            actual: hex::encode(&part_a.blake3),
        })
    );
    assert_eq!(other.uncovered_entry_ordinals(), None);
    assert_eq!(reads.len(), 1, "no bucket read for a leaf of another part");
}

fn catalog_with_leaf(key: &str, bytes: &[u8]) -> Catalog {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    block_on(store.put(key, Bytes::copy_from_slice(bytes), PutOptions::default()))
        .expect("put leaf");
    Catalog::new(
        store,
        CatalogConfig {
            shard_count: 1,
            ..Default::default()
        },
    )
    .expect("catalog")
}

#[tokio::test]
async fn foreign_tenant_or_signal_leaf_is_field_mismatch_and_counts_a_breach() {
    let f = fixture();
    let key = "t/0707/catalog/logs/idx/500000.0123456789abcdef.kidx";
    let catalog = catalog_with_leaf(key, &f.leaf.bytes);
    let mut leaf_ref = f.leaf.leaf_ref.clone();
    leaf_ref.key = key.to_string();
    let limits = KeyIndexLeafLimits::default();

    let ok = catalog
        .probe_key_index_leaf(&leaf_ref, &request(&f.part), &f.alpha, &limits)
        .await
        .expect("own tenant");
    assert!(matches!(ok, KeyIndexLeafProbe::Matches { .. }), "{ok:?}");
    assert_eq!(catalog.isolation_breaches(), 0);

    let other_part = part_hash(b"another part");
    let rejected = catalog
        .probe_key_index_leaf(&leaf_ref, &request(&other_part), &f.alpha, &limits)
        .await
        .expect("a part binding mismatch is not a breach");
    assert!(matches!(rejected, KeyIndexLeafProbe::Rejected(_)));
    assert_eq!(catalog.isolation_breaches(), 0);

    let foreign_tenant = KeyIndexLeafRequest {
        tenant_hash: TenantHash([8u8; 16]),
        ..request(&f.part)
    };
    let err = catalog
        .probe_key_index_leaf(&leaf_ref, &foreign_tenant, &f.alpha, &limits)
        .await
        .expect_err("foreign tenant");
    match err {
        CatalogError::FieldMismatch {
            key: err_key,
            field,
            expected,
            actual,
        } => {
            assert_eq!(err_key, key);
            assert_eq!(field, "tenant_hash");
            assert_eq!(expected, TenantHash([8u8; 16]).to_hex());
            assert_eq!(actual, TENANT.to_hex());
        }
        other => panic!("expected FieldMismatch, got {other:?}"),
    }
    assert_eq!(catalog.isolation_breaches(), 1);

    let foreign_signal = KeyIndexLeafRequest {
        signal: Signal::Spans,
        ..request(&f.part)
    };
    let err = catalog
        .probe_key_index_leaf(&leaf_ref, &foreign_signal, &f.alpha, &limits)
        .await
        .expect_err("foreign signal");
    assert!(
        matches!(
            err,
            CatalogError::FieldMismatch {
                field: "signal",
                ..
            }
        ),
        "{err:?}"
    );
    assert_eq!(catalog.isolation_breaches(), 2);
}

#[test]
fn uncovered_ordinals_ride_on_every_probe_outcome() {
    let f = fixture();
    let req = request(&f.part);
    let (hit, _) = probe(&f.leaf.bytes, &f.leaf.leaf_ref, &req, &f.alpha);
    assert_eq!(hit.uncovered_entry_ordinals(), Some(&UNCOVERED[..]));
    let KeyIndexLeafProbe::Matches { matches, .. } = &hit else {
        panic!("{hit:?}");
    };
    assert!(
        matches
            .iter()
            .all(|m| !UNCOVERED.contains(&m.entry_ordinal))
    );

    let (absent, _) = probe(&f.leaf.bytes, &f.leaf.leaf_ref, &req, &f.absent_same_bucket);
    assert_eq!(absent.uncovered_entry_ordinals(), Some(&UNCOVERED[..]));

    // One entry under 16 buckets leaves most buckets empty: a key there is
    // absent after GET 1 alone, and still reports the uncovered entries.
    let lonely = [entry(f.alpha, 0, &[0])];
    let sparse = encode_key_index_leaf_with_bucket_bits(
        &input(f.part, &lonely, &[7, 5]),
        4,
        &KeyIndexLeafLimits::default(),
    )
    .expect("encode");
    let alpha_bucket = bucket_index(&f.alpha, 4).expect("bits");
    let empty_bucket_key = (0..)
        .map(|i| key8_str(&format!("elsewhere-{i}")))
        .find(|k| bucket_index(k, 4).expect("bits") != alpha_bucket)
        .expect("key in another bucket");
    let (absent, reads) = probe(&sparse.bytes, &sparse.leaf_ref, &req, &empty_bucket_key);
    assert_eq!(
        absent,
        KeyIndexLeafProbe::Absent {
            uncovered_entry_ordinals: vec![5, 7],
        }
    );
    assert_eq!(reads.len(), 1);
}

#[test]
fn a_failed_get_rejects_the_leaf_as_a_read_failure() {
    let f = fixture();
    for failing_read in 0..2 {
        let calls = RefCell::new(0usize);
        let result = block_on(probe_key_index_leaf_ranged(
            &f.leaf.leaf_ref,
            &request(&f.part),
            &f.alpha,
            &KeyIndexLeafLimits::default(),
            |range: Range<u64>| {
                let call = *calls.borrow();
                *calls.borrow_mut() += 1;
                let out = if call == failing_read {
                    Err(format!("injected failure on read {call}"))
                } else {
                    Ok(Bytes::copy_from_slice(
                        &f.leaf.bytes[range.start as usize..range.end as usize],
                    ))
                };
                async move { out }
            },
        ))
        .expect("no breach");
        assert_eq!(
            result,
            KeyIndexLeafProbe::Rejected(KeyIndexLeafRejection::Read(format!(
                "injected failure on read {failing_read}"
            )))
        );
        assert_eq!(*calls.borrow(), failing_read + 1);
    }
}

#[test]
fn format_constants_and_bucket_bits_balance_are_pinned() {
    assert_eq!(&KEY_INDEX_LEAF_MAGIC, b"RKI1");
    assert_eq!(KEY_INDEX_LEAF_FORMAT_VERSION, 2);
    assert_eq!(KEY_INDEX_LEAF_FORMAT_VERSION_1, 1);
    assert_eq!(KEY_INDEX_LEAF_MIN_BUCKET_BITS, 1);
    assert_eq!(KEY_INDEX_LEAF_WRITER_MAX_BUCKET_BITS, 13);
    assert_eq!(KEY_INDEX_LEAF_READER_MAX_BUCKET_BITS, 16);
    assert_eq!(DEFAULT_MAX_KEY_INDEX_LEAF_BODY_BYTES, 256 << 20);
    assert_eq!(choose_key_index_bucket_bits(0), 1);
    assert_eq!(choose_key_index_bucket_bits(1_200_000), 9);
    assert_eq!(choose_key_index_bucket_bits(256 << 20), 13);
    assert_eq!(choose_key_index_bucket_bits(u64::MAX), 13);
}

#[test]
fn version_one_is_refused_for_logs_and_answers_for_spans() {
    assert!(key_index_leaf_version_accepted(Signal::Logs, 2));
    assert!(key_index_leaf_version_accepted(Signal::Spans, 2));
    assert!(key_index_leaf_version_accepted(Signal::Spans, 1));
    assert!(!key_index_leaf_version_accepted(Signal::Logs, 1));
    assert!(!key_index_leaf_version_accepted(Signal::Spans, 3));
    assert!(!key_index_leaf_version_accepted(Signal::Spans, 0));

    let f = fixture();
    let expected = KeyIndexLeafProbe::Matches {
        matches: matches(&[(1, &[0, 3, 7]), (4, &[2, 5])]),
        uncovered_entry_ordinals: UNCOVERED.to_vec(),
    };
    for signal in [Signal::Logs, Signal::Spans] {
        let leaf = encode_key_index_leaf_with_bucket_bits(
            &KeyIndexLeafInput {
                signal,
                ..input(f.part, &f.entries, &UNCOVERED)
            },
            FIXTURE_BUCKET_BITS,
            &KeyIndexLeafLimits::default(),
        )
        .expect("encode");
        let req = KeyIndexLeafRequest {
            signal,
            ..request(&f.part)
        };
        let v1 = restamp_key_index_leaf_version(&leaf.bytes, 1).expect("restamp");
        let summary = verify_key_index_leaf(&v1.bytes, &KeyIndexLeafLimits::default())
            .expect("a version-1 leaf is well formed for every signal");
        assert_eq!(summary.header.format_version, 1);
        assert_eq!(summary.leaf_ref(), v1.leaf_ref);

        let (v2_probe, _) = probe(&leaf.bytes, &leaf.leaf_ref, &req, &f.alpha);
        assert_eq!(v2_probe, expected, "{signal:?} version 2");
        let (v1_probe, reads) = probe(&v1.bytes, &v1.leaf_ref, &req, &f.alpha);
        if signal == Signal::Logs {
            assert_eq!(
                v1_probe,
                KeyIndexLeafProbe::Rejected(KeyIndexLeafRejection::UnsupportedVersion(1))
            );
            assert_eq!(reads.len(), 1, "refused on the prefix GET");
        } else {
            assert_eq!(v1_probe, expected, "{signal:?} version 1");
        }
    }
}

#[test]
fn layout_fields_sit_where_the_doc_puts_them() {
    let f = fixture();
    let bytes = &f.leaf.bytes;
    let header_len = u32::from_le_bytes(bytes[4..8].try_into().expect("4")) as u64;
    let prefix_len = u32::from_le_bytes(bytes[8..12].try_into().expect("4")) as u64;
    assert_eq!(&bytes[..4], b"RKI1");
    assert_eq!(prefix_len, 4 + 4 + 4 + header_len + 4 + 4 * 8 + 4);
    assert_eq!(prefix_len, u64::from(f.leaf.leaf_ref.prefix_len));
    let header = KeyIndexLeafHeader::decode(&bytes[12..12 + header_len as usize]).expect("header");
    assert_eq!(prefix_len + header.body_len, bytes.len() as u64);
    assert_eq!(header.entry_count, f.entries.len() as u64);
    assert_eq!(header.uncovered_entry_ordinals, UNCOVERED.to_vec());
    assert_eq!(header.bucket_bits, u32::from(FIXTURE_BUCKET_BITS));
    let r = &f.leaf.leaf_ref;
    assert_eq!(r.blake3, blake3::hash(bytes).as_bytes().to_vec());
    assert_eq!(r.size, bytes.len() as u64);
    assert_eq!(r.uncovered_count, 2);
    assert_eq!(r.part_blake3, f.part.to_vec());
    assert!(r.key.is_empty());
}

#[test]
fn encoder_refuses_invalid_input() {
    let part = part_hash(b"p");
    let k = key8_str("k");
    let limits = KeyIndexLeafLimits::default();
    let refuse = |entries: &[KeyIndexLeafEntry], uncovered: &[u64]| {
        encode_key_index_leaf(&input(part, entries, uncovered), &limits)
    };
    let invalid = |r: Result<EncodedKeyIndexLeaf, KeyIndexLeafEncodeError>| {
        matches!(r, Err(KeyIndexLeafEncodeError::InvalidInput(_)))
    };
    assert!(invalid(refuse(
        &[entry(k, 1, &[0]), entry(k, 1, &[2])],
        &[]
    )));
    assert!(invalid(refuse(&[entry(k, 1, &[])], &[])));
    assert!(invalid(refuse(&[entry(k, 1, &[3, 3])], &[])));
    assert!(invalid(refuse(&[entry(k, 1, &[4, 2])], &[])));
    assert!(invalid(refuse(&[entry(k, PART_ENTRY_COUNT, &[0])], &[])));
    assert!(invalid(refuse(&[entry(k, 1, &[0])], &[1])));
    assert!(invalid(refuse(&[], &[PART_ENTRY_COUNT])));
    let mut narrow = input(part, &[], &[]);
    narrow.key8_lo = 5;
    narrow.key8_hi = 4;
    assert!(invalid(encode_key_index_leaf(&narrow, &limits)));
    let outside = [entry(0u64.to_be_bytes(), 0, &[0])];
    let mut sliced = input(part, &outside, &[]);
    sliced.key8_lo = 1;
    assert!(invalid(encode_key_index_leaf(&sliced, &limits)));
    let mut unnamed = input(part, &[], &[]);
    unnamed.field = "";
    assert!(invalid(encode_key_index_leaf(&unnamed, &limits)));
}

#[test]
fn bucket_bits_entry_points_refuse_past_their_bound() {
    let part = part_hash(b"p");
    let limits = KeyIndexLeafLimits::default();
    let leaf_input = input(part, &[], &[]);
    let out_of_range = |bucket_bits, max| {
        Err(KeyIndexLeafEncodeError::BucketBitsOutOfRange {
            bucket_bits,
            min: 1,
            max,
        })
    };
    for bits in [1, KEY_INDEX_LEAF_WRITER_MAX_BUCKET_BITS] {
        let leaf = encode_key_index_leaf_with_bucket_bits(&leaf_input, bits, &limits)
            .expect("inside the writer bound");
        assert_eq!(leaf.leaf_ref.bucket_bits, u32::from(bits));
    }
    for bits in [0, 14, 16, 17] {
        assert_eq!(
            encode_key_index_leaf_with_bucket_bits(&leaf_input, bits, &limits),
            out_of_range(bits, 13)
        );
    }
    for bits in [14, KEY_INDEX_LEAF_READER_MAX_BUCKET_BITS] {
        let leaf = encode_key_index_leaf_with_reader_bucket_bits(&leaf_input, bits, &limits)
            .expect("inside the reader bound");
        verify_key_index_leaf(&leaf.bytes, &limits).expect("the reader accepts it");
    }
    for bits in [0, 17] {
        assert_eq!(
            encode_key_index_leaf_with_reader_bucket_bits(&leaf_input, bits, &limits),
            out_of_range(bits, 16)
        );
    }
}

#[test]
fn encoder_refuses_a_body_over_the_ceiling() {
    let part = part_hash(b"p");
    let entries: Vec<_> = (0..4u32)
        .map(|i| entry(key8_str(&i.to_string()), u64::from(i), &[i]))
        .collect();
    let body: u64 = entries.iter().map(encoded_entry_len).sum();
    let at = KeyIndexLeafLimits {
        max_body_bytes: body,
    };
    encode_key_index_leaf(&input(part, &entries, &[]), &at).expect("at the ceiling");
    let under = KeyIndexLeafLimits {
        max_body_bytes: body - 1,
    };
    assert_eq!(
        encode_key_index_leaf(&input(part, &entries, &[]), &under),
        Err(KeyIndexLeafEncodeError::BodyOverCeiling {
            body,
            ceiling: body - 1,
        })
    );
}

fn sorted(mut entries: Vec<KeyIndexLeafEntry>) -> Vec<KeyIndexLeafEntry> {
    entries.sort_by_key(|e| (e.key8, e.entry_ordinal));
    entries
}

#[test]
fn split_tiles_the_key_space_and_every_slice_answers_its_keys() {
    let part = part_hash(b"split part");
    let mut entries = Vec::new();
    for i in 0..40u32 {
        let key = key8_str(&format!("split-{i}"));
        for ordinal in 0..(u64::from(i % 3) + 1) {
            entries.push(entry(key, ordinal, &[i, i + 1]));
        }
    }
    let entries = sorted(entries);
    let ceiling = 120;
    let slices = split_key_index_entries(&entries, ceiling).expect("split");
    assert!(slices.len() > 3, "{}", slices.len());
    assert_eq!(slices[0].key8_lo, 0);
    assert_eq!(slices.last().map(|s| s.key8_hi), Some(u64::MAX));
    assert_eq!(slices[0].entries.start, 0);
    assert_eq!(slices.last().map(|s| s.entries.end), Some(entries.len()));
    for pair in slices.windows(2) {
        assert_eq!(pair[1].key8_lo, pair[0].key8_hi + 1);
        assert_eq!(pair[1].entries.start, pair[0].entries.end);
    }
    let limits = KeyIndexLeafLimits {
        max_body_bytes: ceiling,
    };
    let mut refs = Vec::new();
    let mut leaves = Vec::new();
    for slice in &slices {
        let body: u64 = entries[slice.entries.clone()]
            .iter()
            .map(encoded_entry_len)
            .sum();
        assert!(body <= ceiling, "{body}");
        for e in &entries[slice.entries.clone()] {
            let key = u64::from_be_bytes(e.key8);
            assert!(slice.key8_lo <= key && key <= slice.key8_hi);
        }
        let mut leaf_input = input(part, &entries[slice.entries.clone()], &[5]);
        leaf_input.key8_lo = slice.key8_lo;
        leaf_input.key8_hi = slice.key8_hi;
        let leaf = encode_key_index_leaf(&leaf_input, &limits).expect("slice encodes");
        leaves.push(verify_key_index_leaf(&leaf.bytes, &limits).expect("verifies"));
        let mut leaf_ref = leaf.leaf_ref;
        leaf_ref.key = format!("slice-{}", refs.len());
        refs.push(leaf_ref);
    }
    check_key_index_leaf_set(&leaves).expect("slices agree and tile");

    let by_key: BTreeMap<Key8, Vec<u64>> = entries.iter().fold(BTreeMap::new(), |mut acc, e| {
        acc.entry(e.key8).or_default().push(e.entry_ordinal);
        acc
    });
    for (i, slice) in slices.iter().enumerate() {
        for e in &entries[slice.entries.clone()] {
            let chosen = select_key_index_slice(&refs, FIELD, &e.key8)
                .expect("tiles")
                .expect("covered");
            assert_eq!(chosen.key, refs[i].key);
        }
    }
    let leaf_bytes: Vec<Vec<u8>> = slices
        .iter()
        .map(|slice| {
            let mut leaf_input = input(part, &entries[slice.entries.clone()], &[5]);
            leaf_input.key8_lo = slice.key8_lo;
            leaf_input.key8_hi = slice.key8_hi;
            encode_key_index_leaf(&leaf_input, &limits)
                .expect("encode")
                .bytes
        })
        .collect();
    for (key8, ordinals) in &by_key {
        let chosen = select_key_index_slice(&refs, FIELD, key8)
            .expect("tiles")
            .expect("covered");
        let index = refs
            .iter()
            .position(|r| r.key == chosen.key)
            .expect("found");
        let (got, _) = probe_with(&leaf_bytes[index], chosen, &request(&part), key8, &limits);
        let KeyIndexLeafProbe::Matches { matches, .. } = got.expect("no breach") else {
            panic!("key in its slice must match");
        };
        let got: Vec<u64> = matches.iter().map(|m| m.entry_ordinal).collect();
        assert_eq!(&got, ordinals);
    }
    assert!(
        select_key_index_slice(&refs, "other.field", &key8_str("x"))
            .expect("no refs is fine")
            .is_none()
    );
}

#[test]
fn split_edge_cases() {
    assert_eq!(
        split_key_index_entries(&[], 10).expect("empty"),
        vec![KeyIndexSlice {
            key8_lo: 0,
            key8_hi: u64::MAX,
            entries: 0..0,
        }]
    );
    let k = key8_str("hot");
    let run = sorted(vec![entry(k, 0, &[0]), entry(k, 1, &[0])]);
    let run_body: u64 = run.iter().map(encoded_entry_len).sum();
    assert_eq!(
        split_key_index_entries(&run, run_body - 1),
        Err(KeyIndexLeafEncodeError::BodyOverCeiling {
            body: run_body,
            ceiling: run_body - 1,
        })
    );
    let unsorted = vec![entry([9; 8], 0, &[0]), entry([1; 8], 0, &[0])];
    assert!(matches!(
        split_key_index_entries(&unsorted, 1000),
        Err(KeyIndexLeafEncodeError::InvalidInput(_))
    ));
}

#[test]
fn slice_selection_rejects_gaps_overlaps_and_disagreeing_slices() {
    let base = SnapshotKeyIndexLeafRef {
        field: FIELD.into(),
        key_type: 3,
        uncovered_count: 1,
        part_blake3: vec![1; 32],
        ..Default::default()
    };
    let slice = |lo: u64, hi: u64| SnapshotKeyIndexLeafRef {
        key8_lo: lo,
        key8_hi: hi,
        ..base.clone()
    };
    let k = 50u64.to_be_bytes();
    let corrupted = |refs: &[SnapshotKeyIndexLeafRef]| {
        matches!(
            select_key_index_slice(refs, FIELD, &k),
            Err(KeyIndexLeafRejection::Corrupted(_))
        )
    };
    assert!(corrupted(&[slice(0, 10), slice(12, u64::MAX)]));
    assert!(corrupted(&[slice(0, 10), slice(10, u64::MAX)]));
    assert!(corrupted(&[slice(1, u64::MAX)]));
    assert!(corrupted(&[slice(0, 100)]));
    assert!(corrupted(&[slice(0, u64::MAX), slice(0, u64::MAX)]));
    let mut disagree = slice(101, u64::MAX);
    disagree.uncovered_count = 2;
    assert!(corrupted(&[slice(0, 100), disagree]));
    let tiled = [slice(101, u64::MAX), slice(0, 100)];
    let chosen = select_key_index_slice(&tiled, FIELD, &k)
        .expect("tiles")
        .expect("covered");
    assert_eq!((chosen.key8_lo, chosen.key8_hi), (0, 100));
}

#[test]
fn leaf_set_check_rejects_slices_with_different_uncovered_lists() {
    let part = part_hash(b"p");
    let limits = KeyIndexLeafLimits::default();
    let low = [entry(1u64.to_be_bytes(), 0, &[0])];
    let high = [entry(u64::MAX.to_be_bytes(), 1, &[0])];
    let build = |entries: &[KeyIndexLeafEntry], lo: u64, hi: u64, uncovered: &[u64]| {
        let mut leaf_input = input(part, entries, uncovered);
        leaf_input.key8_lo = lo;
        leaf_input.key8_hi = hi;
        let leaf = encode_key_index_leaf(&leaf_input, &limits).expect("encode");
        verify_key_index_leaf(&leaf.bytes, &limits).expect("verify")
    };
    let a = build(&low, 0, 99, &[5]);
    let b = build(&high, 100, u64::MAX, &[5]);
    check_key_index_leaf_set(&[a.clone(), b]).expect("agree");
    let b_other = build(&high, 100, u64::MAX, &[4, 5]);
    assert!(matches!(
        check_key_index_leaf_set(&[a.clone(), b_other]),
        Err(KeyIndexLeafRejection::Corrupted(_))
    ));
    let gap = build(&high, 101, u64::MAX, &[5]);
    assert!(matches!(
        check_key_index_leaf_set(&[a, gap]),
        Err(KeyIndexLeafRejection::Corrupted(_))
    ));
}

// Hand-built leaves for the corruption conditions an encoder never emits.

fn base_header(bucket_bits: u32) -> KeyIndexLeafHeader {
    KeyIndexLeafHeader {
        format_version: KEY_INDEX_LEAF_FORMAT_VERSION,
        tenant_hash: TENANT.0.to_vec(),
        signal: signal_code(SIGNAL),
        part_blake3: part_hash(b"hand").to_vec(),
        field: FIELD.into(),
        key_type: 3,
        key8_lo: 0,
        key8_hi: u64::MAX,
        bucket_bits,
        entry_count: 1,
        part_entry_count: PART_ENTRY_COUNT,
        uncovered_entry_ordinals: Vec::new(),
        body_len: 0,
    }
}

fn raw_entry(key8: Key8, ordinal: u64, deltas: &[u64]) -> Vec<u8> {
    let mut out = key8.to_vec();
    write_uvarint(&mut out, ordinal);
    write_uvarint(&mut out, deltas.len() as u64);
    for &d in deltas {
        write_uvarint(&mut out, d);
    }
    out
}

fn frame_declaring(entry_count: u32, declared: u32, raw: &[u8]) -> Vec<u8> {
    let compressed = zstd::bulk::compress(raw, ZSTD_LEVEL).expect("compress");
    let mut out = Vec::new();
    out.extend_from_slice(&entry_count.to_le_bytes());
    out.extend_from_slice(&declared.to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(&compressed).to_le_bytes());
    out.extend_from_slice(&compressed);
    out
}

fn frame(entry_count: u32, raw: &[u8]) -> Vec<u8> {
    frame_declaring(entry_count, raw.len() as u32, raw)
}

/// Assembles a leaf from a header and per-bucket frames, sealing every crc,
/// so only the condition under test is wrong. `ends` overrides the
/// directory.
fn assemble(
    mut header: KeyIndexLeafHeader,
    frames: &BTreeMap<u32, Vec<u8>>,
    ends_override: Option<Vec<u32>>,
) -> (Vec<u8>, SnapshotKeyIndexLeafRef) {
    let buckets = 1u32 << header.bucket_bits;
    let mut body = Vec::new();
    let mut ends = Vec::new();
    for bucket in 0..buckets {
        if let Some(frame) = frames.get(&bucket) {
            body.extend_from_slice(frame);
        }
        ends.push(body.len() as u32);
    }
    let ends = ends_override.unwrap_or(ends);
    if header.body_len == 0 {
        header.body_len = body.len() as u64;
    }
    let header_bytes = header.encode_to_vec();
    let prefix_len = 20 + header_bytes.len() as u32 + 4 * ends.len() as u32;
    let mut bytes = KEY_INDEX_LEAF_MAGIC.to_vec();
    bytes.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&prefix_len.to_le_bytes());
    bytes.extend_from_slice(&header_bytes);
    let crc = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    let dir: Vec<u8> = ends.iter().flat_map(|e| e.to_le_bytes()).collect();
    bytes.extend_from_slice(&dir);
    bytes.extend_from_slice(&crc32c::crc32c(&dir).to_le_bytes());
    bytes.extend_from_slice(&body);
    let leaf_ref = SnapshotKeyIndexLeafRef {
        key: "hand.kidx".into(),
        blake3: blake3::hash(&bytes).as_bytes().to_vec(),
        size: bytes.len() as u64,
        field: header.field.clone(),
        key_type: header.key_type,
        key8_lo: header.key8_lo,
        key8_hi: header.key8_hi,
        prefix_len,
        bucket_bits: header.bucket_bits,
        entry_count: header.entry_count,
        uncovered_count: header.uncovered_entry_ordinals.len() as u64,
        part_blake3: header.part_blake3.clone(),
    };
    (bytes, leaf_ref)
}

fn single_bucket(key8: Key8, frame: Vec<u8>) -> BTreeMap<u32, Vec<u8>> {
    BTreeMap::from([(bucket_index(&key8, 1).expect("bits"), frame)])
}

fn expect_rejected(
    (bytes, leaf_ref): &(Vec<u8>, SnapshotKeyIndexLeafRef),
    key8: &Key8,
    limits: &KeyIndexLeafLimits,
    needle: &str,
) {
    let part = part_hash(b"hand");
    let (result, _) = probe_with(bytes, leaf_ref, &request(&part), key8, limits);
    let got = result.expect("no breach");
    assert!(
        matches!(&got, KeyIndexLeafProbe::Rejected(KeyIndexLeafRejection::Corrupted(m)) if m.contains(needle)),
        "wanted Corrupted containing {needle:?}, got {got:?}"
    );
}

#[test]
fn hand_built_leaf_decodes_so_the_corruptions_below_are_isolated() {
    let k = key8_str("hand-key");
    let leaf = assemble(
        base_header(1),
        &single_bucket(k, frame(1, &raw_entry(k, 2, &[4, 3]))),
        None,
    );
    let (result, _) = probe_with(
        &leaf.0,
        &leaf.1,
        &request(&part_hash(b"hand")),
        &k,
        &KeyIndexLeafLimits::default(),
    );
    assert_eq!(
        result.expect("no breach"),
        KeyIndexLeafProbe::Matches {
            matches: matches(&[(2, &[3, 6])]),
            uncovered_entry_ordinals: Vec::new(),
        }
    );
    verify_key_index_leaf(&leaf.0, &KeyIndexLeafLimits::default()).expect("verifies");
}

#[test]
fn every_frame_corruption_is_a_typed_rejection() {
    let k = key8_str("hand-key");
    let limits = KeyIndexLeafLimits::default();
    let one = |raw: Vec<u8>, count: u32| {
        assemble(base_header(1), &single_bucket(k, frame(count, &raw)), None)
    };
    expect_rejected(
        &one(raw_entry(k, 2, &[0]), 1),
        &k,
        &limits,
        "zero or overflows",
    );
    expect_rejected(
        &one(raw_entry(k, 2, &[1, 0]), 1),
        &k,
        &limits,
        "zero or overflows",
    );
    expect_rejected(
        &one(raw_entry(k, 2, &[u64::from(u32::MAX), 5]), 1),
        &k,
        &limits,
        "zero or overflows",
    );
    expect_rejected(
        &one(raw_entry(k, 2, &[u64::from(u32::MAX) + 2]), 1),
        &k,
        &limits,
        "zero or overflows",
    );
    expect_rejected(
        &one(raw_entry(k, 2, &[]), 1),
        &k,
        &limits,
        "empty block set",
    );
    expect_rejected(
        &one(raw_entry(k, PART_ENTRY_COUNT, &[1]), 1),
        &k,
        &limits,
        "at or past part_entry_count",
    );
    expect_rejected(&one(raw_entry(k, 2, &[1]), 0), &k, &limits, "zero entries");
    // Long enough that two entries pass the one-per-10-bytes bound, so the
    // second entry's unterminated varint is what trips.
    let mut cut = raw_entry(k, 2, &[1]);
    cut.extend_from_slice(&k);
    cut.extend_from_slice(&[0x80, 0x80]);
    expect_rejected(&one(cut, 2), &k, &limits, "truncated");
    // One entry spending the bytes a claimed second entry would need.
    expect_rejected(
        &one(raw_entry(k, 2, &[1; 11]), 2),
        &k,
        &limits,
        "with 1 entries still to come",
    );
    let mut trailing = raw_entry(k, 2, &[1]);
    trailing.push(0);
    expect_rejected(&one(trailing, 1), &k, &limits, "past its 1 entries");
    let mut twice = raw_entry(k, 2, &[1]);
    twice.extend(raw_entry(k, 2, &[2]));
    expect_rejected(&one(twice, 2), &k, &limits, "unsorted or duplicated");
    let mut descending = raw_entry(k, 3, &[1]);
    descending.extend(raw_entry(k, 2, &[1]));
    expect_rejected(&one(descending, 2), &k, &limits, "unsorted or duplicated");

    // A key8 that mixes to the other bucket than the frame it sits in.
    let other = (0..)
        .map(|i| key8_str(&format!("other-{i}")))
        .find(|o| bucket_index(o, 1).expect("bits") != bucket_index(&k, 1).expect("bits"))
        .expect("other bucket");
    let mut misplaced = raw_entry(k, 1, &[1]);
    misplaced.extend(raw_entry(other, 2, &[1]));
    let misplaced = if k < other {
        misplaced
    } else {
        let mut swapped = raw_entry(other, 2, &[1]);
        swapped.extend(raw_entry(k, 1, &[1]));
        swapped
    };
    expect_rejected(&one(misplaced, 2), &k, &limits, "does not mix");

    // A key8 outside the header's slice.
    let mut narrow = base_header(1);
    narrow.key8_lo = u64::from_be_bytes(k);
    let below = (u64::from_be_bytes(k) - 1).to_be_bytes();
    let mut raw = raw_entry(below, 1, &[1]);
    raw.extend(raw_entry(k, 2, &[1]));
    let leaf = if bucket_index(&below, 1).expect("bits") == bucket_index(&k, 1).expect("bits") {
        assemble(narrow, &single_bucket(k, frame(2, &raw)), None)
    } else {
        let mut header = narrow;
        header.entry_count = 2;
        assemble(
            header,
            &BTreeMap::from([
                (
                    bucket_index(&below, 1).expect("bits"),
                    frame(1, &raw_entry(below, 1, &[1])),
                ),
                (
                    bucket_index(&k, 1).expect("bits"),
                    frame(1, &raw_entry(k, 2, &[1])),
                ),
            ]),
            None,
        )
    };
    let summary = verify_key_index_leaf(&leaf.0, &limits);
    assert!(
        matches!(&summary, Err(KeyIndexLeafRejection::Corrupted(m)) if m.contains("outside the leaf's slice")),
        "{summary:?}"
    );

    // Declared decompressed length: over the ceiling (refused before the crc
    // is even checked), longer and shorter than the real bytes.
    let raw = raw_entry(k, 2, &[1]);
    let mut over = frame_declaring(1, 64, &raw);
    over[8] ^= 0xff;
    let small = KeyIndexLeafLimits { max_body_bytes: 63 };
    expect_rejected(
        &assemble(base_header(1), &single_bucket(k, over), None),
        &k,
        &small,
        "over the 63-byte ceiling",
    );
    expect_rejected(
        &assemble(
            base_header(1),
            &single_bucket(k, frame_declaring(1, raw.len() as u32 + 1, &raw)),
            None,
        ),
        &k,
        &limits,
        "decompressed to",
    );
    expect_rejected(
        &assemble(
            base_header(1),
            &single_bucket(k, frame_declaring(1, raw.len() as u32 - 1, &raw)),
            None,
        ),
        &k,
        &limits,
        "zstd",
    );
    let mut bad_crc = frame(1, &raw);
    bad_crc[8] ^= 1;
    expect_rejected(
        &assemble(base_header(1), &single_bucket(k, bad_crc), None),
        &k,
        &limits,
        "frame crc32c",
    );
}

#[test]
fn every_prefix_corruption_is_a_typed_rejection() {
    let k = key8_str("hand-key");
    let limits = KeyIndexLeafLimits::default();
    let frames = || single_bucket(k, frame(1, &raw_entry(k, 2, &[1])));
    let with = |edit: &dyn Fn(&mut KeyIndexLeafHeader)| {
        let mut header = base_header(1);
        edit(&mut header);
        assemble(header, &frames(), None)
    };

    let part = part_hash(b"hand");
    let (result, _) = probe_with(
        &with(&|h| h.format_version = 3).0,
        &with(&|h| h.format_version = 3).1,
        &request(&part),
        &k,
        &limits,
    );
    assert_eq!(
        result.expect("no breach"),
        KeyIndexLeafProbe::Rejected(KeyIndexLeafRejection::UnsupportedVersion(3))
    );
    expect_rejected(
        &with(&|h| h.bucket_bits = 17),
        &k,
        &limits,
        "bucket_bits 17",
    );
    expect_rejected(&with(&|h| h.bucket_bits = 0), &k, &limits, "bucket_bits 0");
    expect_rejected(&with(&|h| h.key_type = 9), &k, &limits, "unknown key_type");
    expect_rejected(
        &with(&|h| h.uncovered_entry_ordinals = vec![5, 5]),
        &k,
        &limits,
        "strictly ascending",
    );
    expect_rejected(
        &with(&|h| h.uncovered_entry_ordinals = vec![PART_ENTRY_COUNT]),
        &k,
        &limits,
        "uncovered entry ordinal",
    );
    expect_rejected(
        &with(&|h| h.part_entry_count = 9),
        &k,
        &limits,
        "part_entry_count 9",
    );
    expect_rejected(&with(&|h| h.body_len = 1), &k, &limits, "body_len");
    expect_rejected(
        &with(&|h| h.part_blake3.truncate(31)),
        &k,
        &limits,
        "wrong length",
    );
    expect_rejected(&with(&|h| h.field.clear()), &k, &limits, "field name empty");
    expect_rejected(
        &with(&|h| h.field = "f".repeat(KEY_IDX_MAX_NAME_LEN + 1)),
        &k,
        &limits,
        "too long",
    );
    expect_rejected(
        &with(&|h| {
            h.key8_lo = 1;
            h.key8_hi = 0;
        }),
        &k,
        &limits,
        "key8_lo above key8_hi",
    );

    // A short tenant_hash: a probe compares the tenant before anything
    // else, a whole-object reader has no request and rejects its length.
    let (bytes, leaf_ref) = with(&|h| h.tenant_hash.truncate(15));
    let (result, _) = probe_with(&bytes, &leaf_ref, &request(&part), &k, &limits);
    assert!(
        matches!(
            result,
            Err(KeyIndexLeafBreach {
                field: "tenant_hash",
                ..
            })
        ),
        "{result:?}"
    );
    assert!(matches!(
        verify_key_index_leaf(&bytes, &limits),
        Err(KeyIndexLeafRejection::Corrupted(m)) if m.contains("wrong length")
    ));

    // A directory that decreases, sealed under a valid crc.
    let frame_len = frames().values().next().expect("one").len() as u32;
    let decreasing = assemble(base_header(1), &frames(), Some(vec![frame_len, 0]));
    let mut header = base_header(1);
    header.body_len = u64::from(frame_len);
    let decreasing_with_len = assemble(header, &frames(), Some(vec![frame_len, 0]));
    expect_rejected(&decreasing, &k, &limits, "decrease");
    expect_rejected(&decreasing_with_len, &k, &limits, "decrease");

    // A header prefix_len that is not the layout's, resealed.
    let (mut bytes, mut leaf_ref) = with(&|_| {});
    let header_len = u32::from_le_bytes(bytes[4..8].try_into().expect("4")) as usize;
    let wrong = leaf_ref.prefix_len + 4;
    bytes[8..12].copy_from_slice(&wrong.to_le_bytes());
    let crc = crc32c::crc32c(&bytes[..12 + header_len]);
    bytes[12 + header_len..16 + header_len].copy_from_slice(&crc.to_le_bytes());
    leaf_ref.prefix_len = wrong;
    expect_rejected(&(bytes, leaf_ref), &k, &limits, "differs from the layout's");
}

#[test]
fn byte_level_corruption_and_bad_refs_are_typed_rejections() {
    let f = fixture();
    let req = request(&f.part);
    let good = (&f.leaf.bytes, &f.leaf.leaf_ref);
    let prefix_len = good.1.prefix_len as usize;
    let rejected_with = |bytes: &[u8], leaf_ref: &SnapshotKeyIndexLeafRef, needle: &str| {
        let (got, reads) = probe(bytes, leaf_ref, &req, &f.alpha);
        assert!(
            matches!(&got, KeyIndexLeafProbe::Rejected(KeyIndexLeafRejection::Corrupted(m)) if m.contains(needle)),
            "wanted Corrupted containing {needle:?}, got {got:?}"
        );
        reads
    };
    let flipped = |at: usize| {
        let mut bytes = good.0.clone();
        bytes[at] ^= 0x01;
        bytes
    };
    rejected_with(&flipped(0), good.1, "magic");
    rejected_with(&flipped(14), good.1, "header crc32c");
    rejected_with(&flipped(prefix_len - 6), good.1, "directory crc32c");
    let alpha_frame = {
        let (prefix, _) = probe(good.0, good.1, &req, &f.alpha);
        assert!(matches!(prefix, KeyIndexLeafProbe::Matches { .. }));
        let reads = probe(good.0, good.1, &req, &f.alpha).1;
        reads[1].clone()
    };
    rejected_with(
        &flipped(alpha_frame.end as usize - 1),
        good.1,
        "frame crc32c",
    );
    rejected_with(&flipped(alpha_frame.start as usize), good.1, "");

    // Truncation: inside the directory, inside the frame.
    rejected_with(
        &good.0[..prefix_len - 1],
        good.1,
        "shorter than the header prefix_len",
    );
    rejected_with(
        &good.0[..alpha_frame.end as usize - 1],
        good.1,
        "read returned",
    );
    for cut in [0, 3, 11, 13] {
        rejected_with(&good.0[..cut], good.1, "");
    }

    // A ref prefix_len of zero or past the size reads nothing.
    let mut zero = good.1.clone();
    zero.prefix_len = 0;
    assert!(rejected_with(good.0, &zero, "zero or greater").is_empty());
    let mut past = good.1.clone();
    past.prefix_len = u32::try_from(past.size + 1).expect("small");
    assert!(rejected_with(good.0, &past, "zero or greater").is_empty());

    // A short ref copy stops the probe instead of truncating the directory.
    let mut short = good.1.clone();
    short.prefix_len -= 1;
    rejected_with(good.0, &short, "shorter than the header prefix_len");

    // Every other ref copy that disagrees with the header.
    let edits: [(&str, RefEdit); 9] = [
        ("ref prefix_len", |r| {
            r.prefix_len += 1;
            r.size += 1;
        }),
        ("ref size", |r| r.size += 1),
        ("ref field", |r| r.field = "other".into()),
        ("ref key_type", |r| r.key_type = 2),
        ("ref key8_lo", |r| r.key8_lo = 1),
        ("ref key8_hi", |r| r.key8_hi = 1),
        ("ref entry_count", |r| r.entry_count += 1),
        ("ref uncovered_count", |r| r.uncovered_count = 1),
        ("ref part_blake3", |r| r.part_blake3 = vec![0; 32]),
    ];
    for (needle, edit) in edits {
        let mut leaf_ref = good.1.clone();
        edit(&mut leaf_ref);
        rejected_with(good.0, &leaf_ref, needle);
    }

    // The request's part_entry_count is the HEAD part ref's entry_count.
    let narrower = KeyIndexLeafRequest {
        part_entry_count: PART_ENTRY_COUNT + 1,
        ..req
    };
    let (got, _) = probe(good.0, good.1, &narrower, &f.alpha);
    assert!(
        matches!(&got, KeyIndexLeafProbe::Rejected(KeyIndexLeafRejection::Corrupted(m)) if m.contains("part_entry_count")),
        "{got:?}"
    );
    let other_field = KeyIndexLeafRequest {
        field: "other",
        ..req
    };
    let (got, _) = probe(good.0, good.1, &other_field, &f.alpha);
    assert!(matches!(
        got,
        KeyIndexLeafProbe::Rejected(KeyIndexLeafRejection::RequestMismatch(_))
    ));
}

#[test]
fn whole_object_verify_checks_what_a_probe_cannot() {
    let f = fixture();
    let limits = KeyIndexLeafLimits::default();
    let summary = verify_key_index_leaf(&f.leaf.bytes, &limits).expect("verifies");
    assert_eq!(summary.leaf_ref(), f.leaf.leaf_ref);
    assert_eq!(sorted(summary.entries.clone()), sorted(f.entries.clone()));
    assert_eq!(
        summary
            .bucket_entry_counts
            .iter()
            .map(|&c| c as usize)
            .sum::<usize>(),
        f.entries.len()
    );
    assert_eq!(summary.bucket_entry_counts.len(), 1 << FIXTURE_BUCKET_BITS);

    // The header total is advisory for a probe, checked whole-object.
    let k = key8_str("hand-key");
    let mut header = base_header(1);
    header.entry_count = 2;
    let leaf = assemble(
        header,
        &single_bucket(k, frame(1, &raw_entry(k, 2, &[1]))),
        None,
    );
    let mut lying_ref = leaf.1.clone();
    lying_ref.entry_count = 2;
    let (got, _) = probe(&leaf.0, &lying_ref, &request(&part_hash(b"hand")), &k);
    assert!(matches!(got, KeyIndexLeafProbe::Matches { .. }), "{got:?}");
    assert!(matches!(
        verify_key_index_leaf(&leaf.0, &limits),
        Err(KeyIndexLeafRejection::Corrupted(m)) if m.contains("header entry_count")
    ));

    // Trailing bytes past prefix_len + body_len.
    let mut long = f.leaf.bytes.clone();
    long.push(0);
    assert!(matches!(
        verify_key_index_leaf(&long, &limits),
        Err(KeyIndexLeafRejection::Corrupted(m)) if m.contains("prefix_len + body_len")
    ));

    // Frames under the per-frame ceiling whose sum exceeds the leaf ceiling.
    let per_frame_max = summary.body_uncompressed_len - 1;
    assert!(matches!(
        verify_key_index_leaf(&f.leaf.bytes, &KeyIndexLeafLimits { max_body_bytes: per_frame_max }),
        Err(KeyIndexLeafRejection::Corrupted(m)) if m.contains("frames decompress to")
    ));
}

fn frame_decompressions() -> u64 {
    FRAME_DECOMPRESSIONS.with(|n| n.get())
}

#[test]
fn frame_claiming_more_entries_than_bytes_is_corrupted_before_allocation() {
    let k = key8_str("hand-key");
    let raw = raw_entry(k, 2, &[4]);
    assert_eq!(raw.len(), 11);
    let limits = KeyIndexLeafLimits::default();
    let part = part_hash(b"hand");

    // The honest frame decodes, so the counter is live on this path.
    let honest = assemble(base_header(1), &single_bucket(k, frame(1, &raw)), None);
    let before = frame_decompressions();
    let (result, _) = probe_with(&honest.0, &honest.1, &request(&part), &k, &limits);
    assert!(matches!(
        result.expect("no breach"),
        KeyIndexLeafProbe::Matches { .. }
    ));
    assert_eq!(frame_decompressions(), before + 1);

    // 11 bytes hold at most one entry; a frame claiming two, or a million,
    // is refused on its header and never reaches zstd or `with_capacity`.
    for claimed in [2u32, 1_000_000, u32::MAX] {
        let hostile = assemble(
            base_header(1),
            &single_bucket(k, frame(claimed, &raw)),
            None,
        );
        let before = frame_decompressions();
        expect_rejected(&hostile, &k, &limits, "more than one per 10 bytes");
        assert_eq!(
            frame_decompressions(),
            before,
            "a frame claiming {claimed} entries reached the decompressor"
        );
    }
}

/// Heap one decode of `frame` holds at its peak, measured from the decoded
/// value's own capacities: the raw buffer beside the entries, or the entries
/// beside the matches a probe copies out of them (at most the entries).
fn measured_decode_peak(prefix: &KeyIndexLeafPrefix, bucket: u32, frame: &[u8]) -> u64 {
    let decoded = prefix
        .decode_frame(bucket, frame, &KeyIndexLeafLimits::default())
        .expect("decodes");
    let entries = (decoded.entries.capacity() * std::mem::size_of::<KeyIndexLeafEntry>()) as u64
        + decoded
            .entries
            .iter()
            .map(|e| (e.blocks.capacity() * std::mem::size_of::<u32>()) as u64)
            .sum::<u64>();
    let raw = u64::from(decoded.uncompressed_len);
    (raw + entries).max(2 * entries)
}

#[test]
fn decoded_bound_dominates_the_adversarial_frame() {
    assert_eq!(std::mem::size_of::<KeyIndexLeafEntry>(), 40);
    let k = key8_str("hand-key");
    let bucket = bucket_index(&k, 1).expect("bits");
    let n = 100u32;

    // Most entries per byte: the same key8 at ordinals 0..n, one block each,
    // 11 bytes apiece.
    let many: Vec<u8> = (0..n)
        .flat_map(|ordinal| raw_entry(k, u64::from(ordinal), &[1]))
        .collect();
    // Most blocks per byte: one entry whose block list is all one-byte deltas.
    let wide = raw_entry(k, 0, &[1; 4_000]);

    for (raw, entry_count) in [(&many, n), (&wide, 1)] {
        let mut header = base_header(1);
        header.entry_count = u64::from(entry_count);
        header.part_entry_count = u64::from(n);
        let leaf = assemble(header, &single_bucket(k, frame(entry_count, raw)), None);
        let part = part_hash(b"hand");
        let req = KeyIndexLeafRequest {
            part_entry_count: u64::from(n),
            ..request(&part)
        };
        let prefix_len = leaf.1.prefix_len as usize;
        let prefix =
            KeyIndexLeafPrefix::parse(&leaf.0[..prefix_len], &leaf.1, &req).expect("parses");
        let len = raw.len() as u32;
        let peak = measured_decode_peak(&prefix, bucket, &leaf.0[prefix_len..]);
        assert!(
            peak <= decoded_bound(len),
            "{entry_count}-entry frame of {len} bytes peaks at {peak}, bound {}",
            decoded_bound(len)
        );
    }

    // A hostile header claiming `c` entries makes `with_capacity` allocate
    // 40c before a single entry decodes, and the block-count check leaves
    // the decoded entries `len - 10c` bytes of deltas at most. Whatever
    // split the frame really holds, that heap beside the raw buffer, or
    // twice it once matches are copied out, stays under the bound.
    for len in [10u32, 11, 4_096, 1 << 20, u32::MAX] {
        let len64 = u64::from(len);
        for claimed in [1, u64::from(len / MIN_FRAME_ENTRY_WIRE_BYTES)] {
            let with_capacity = claimed * std::mem::size_of::<KeyIndexLeafEntry>() as u64;
            let blocks = 4 * (len64 - u64::from(MIN_FRAME_ENTRY_WIRE_BYTES) * claimed);
            let entries = with_capacity + blocks;
            assert!(
                (len64 + entries).max(2 * entries) <= decoded_bound(len),
                "{len} bytes claiming {claimed}"
            );
        }
    }

    // That split refused: one entry spending the deltas the claimed others
    // would need is corrupted before its blocks are allocated.
    let mut header = base_header(1);
    header.entry_count = 100;
    let hostile = assemble(
        header,
        &single_bucket(k, frame(100, &raw_entry(k, 0, &[1; 990]))),
        None,
    );
    expect_rejected(
        &hostile,
        &k,
        &KeyIndexLeafLimits::default(),
        "with 99 entries still to come",
    );
}

#[test]
fn leaf_get_ceiling_and_part_cap_are_the_adr_constants() {
    assert_eq!(KEY_INDEX_MAX_PROBED_PARTS, 1_024);
    assert_eq!(KEY_INDEX_LEAF_GET_CEILING, 2_048);
    assert_eq!(
        KEY_INDEX_LEAF_GET_CEILING as usize,
        2 * KEY_INDEX_MAX_PROBED_PARTS,
        "L_max is two GETs per probed part"
    );
    assert_eq!(key_index_root_shape(), (1_024, 2_048));
    assert_eq!(KEY_INDEX_SET_PROBE_MAX_RUNS, 64);
    assert_eq!(KEY_INDEX_T1_BYTES, 11 * 1024);
    assert_eq!(
        KeyIndexSetPolicy::for_part(3),
        KeyIndexSetPolicy {
            b_run: 64,
            max_bridged_bytes: 3 * 11 * 1024,
        }
    );
}

/// A set probe through an in-memory ranged source, returning the result and
/// every range read.
fn set_probe_with(
    leaf: &[u8],
    leaf_ref: &SnapshotKeyIndexLeafRef,
    request: &KeyIndexLeafRequest<'_>,
    keys: &[Key8],
    policy: &KeyIndexSetPolicy,
) -> (
    Result<KeyIndexSetProbe, KeyIndexLeafBreach>,
    Vec<Range<u64>>,
) {
    let reads = RefCell::new(Vec::new());
    let result = block_on(probe_key_index_leaf_set_ranged(
        leaf_ref,
        request,
        keys,
        &KeyIndexLeafLimits::default(),
        policy,
        |range: Range<u64>| {
            reads.borrow_mut().push(range.clone());
            let end = (range.end as usize).min(leaf.len());
            let out = leaf
                .get(range.start as usize..end)
                .map(Bytes::copy_from_slice)
                .ok_or_else(|| "range starts past the object".to_string());
            async move { out }
        },
        |_bytes| Some(()),
    ));
    (result, reads.into_inner())
}

/// A 13-bit leaf over 1000 distinct keys, and each key's truth.
fn wide_leaf() -> (
    [u8; 32],
    EncodedKeyIndexLeaf,
    BTreeMap<Key8, Vec<KeyIndexMatch>>,
) {
    let part = part_hash(b"wide part");
    let mut entries: Vec<KeyIndexLeafEntry> = (0..1000u32)
        .map(|i| {
            entry(
                key8_str(&format!("wide-{i}")),
                [0u64, 1, 2, 4, 5, 7][i as usize % 6],
                &[i, i + 1],
            )
        })
        .collect();
    entries.sort_by_key(|a| (a.key8, a.entry_ordinal));
    let leaf = encode_key_index_leaf_with_bucket_bits(
        &input(part, &entries, &UNCOVERED),
        13,
        &KeyIndexLeafLimits::default(),
    )
    .expect("encode");
    let mut truth: BTreeMap<Key8, Vec<KeyIndexMatch>> = BTreeMap::new();
    for e in &entries {
        truth.entry(e.key8).or_default().push(KeyIndexMatch {
            entry_ordinal: e.entry_ordinal,
            blocks: e.blocks.clone(),
        });
    }
    (part, leaf, truth)
}

fn expect_answered(
    probe: &KeyIndexSetProbe,
    keys: &[Key8],
    truth: &BTreeMap<Key8, Vec<KeyIndexMatch>>,
) {
    let KeyIndexSetAnswer::Answered {
        per_key,
        uncovered_entry_ordinals,
    } = &probe.answer
    else {
        panic!("expected Answered, got {:?}", probe.answer);
    };
    assert_eq!(uncovered_entry_ordinals, &UNCOVERED.to_vec());
    assert_eq!(per_key.len(), keys.len());
    for (key8, answer) in keys.iter().zip(per_key) {
        let want = match truth.get(key8) {
            Some(matches) => KeyIndexKeyAnswer::Matches(matches.clone()),
            None => KeyIndexKeyAnswer::Absent,
        };
        assert_eq!(answer, &want, "key {key8:?}");
    }
}

#[test]
fn set_probe_reads_at_most_b_run_gets_and_bridges_above_it() {
    let (part, leaf, truth) = wide_leaf();
    assert_eq!(leaf.leaf_ref.bucket_bits, 13);
    let req = request(&part);
    let prefix_range = 0..u64::from(leaf.leaf_ref.prefix_len);
    let prefix = KeyIndexLeafPrefix::parse(
        &leaf.bytes[..leaf.leaf_ref.prefix_len as usize],
        &leaf.leaf_ref,
        &req,
    )
    .expect("parses");

    // Every key, plus one absent key: far more distinct buckets than B_run.
    let mut keys: Vec<Key8> = truth.keys().copied().collect();
    keys.push(key8_str("not-in-the-leaf"));
    keys.sort_unstable();
    let distinct: std::collections::BTreeSet<u32> = keys
        .iter()
        .filter_map(|k| prefix.locate(k).expect("in slice").map(|l| l.bucket))
        .collect();
    assert!(
        distinct.len() > KEY_INDEX_SET_PROBE_MAX_RUNS,
        "{}",
        distinct.len()
    );
    let frames_uncompressed: u64 = distinct
        .iter()
        .map(|&bucket| {
            let start =
                (prefix.prefix_len() + prefix.bucket_body_range(bucket as usize).start) as usize;
            u64::from(le_u32(&leaf.bytes[start..], 4).expect("frame header"))
        })
        .sum();

    let roomy = KeyIndexSetPolicy {
        b_run: KEY_INDEX_SET_PROBE_MAX_RUNS,
        max_bridged_bytes: u64::MAX,
    };
    let (result, reads) = set_probe_with(&leaf.bytes, &leaf.leaf_ref, &req, &keys, &roomy);
    let probe = result.expect("no breach");
    expect_answered(&probe, &keys, &truth);
    assert_eq!(reads[0], prefix_range);
    assert!(
        reads.len() - 1 <= KEY_INDEX_SET_PROBE_MAX_RUNS,
        "{}",
        reads.len()
    );
    assert_eq!(probe.get_requests as usize, reads.len());
    assert_eq!(probe.distinct_buckets, distinct.len());
    let read_bytes: u64 = reads.iter().map(|r| r.end - r.start).sum();
    assert_eq!(probe.wire_bytes, read_bytes);
    assert_eq!(
        probe.bridged_bytes,
        read_bytes - prefix_range.end,
        "bridged bytes are the bucket GETs' bytes, gaps included"
    );
    assert!(probe.bridged_bytes > 0);
    assert_eq!(probe.decompressed_bytes, frames_uncompressed);

    // One byte short of what the bridged runs need: declined on the prefix.
    let tight = KeyIndexSetPolicy {
        max_bridged_bytes: probe.bridged_bytes - 1,
        ..roomy
    };
    let (result, reads) = set_probe_with(&leaf.bytes, &leaf.leaf_ref, &req, &keys, &tight);
    let declined = result.expect("no breach");
    assert_eq!(
        declined.answer,
        KeyIndexSetAnswer::Declined(KeyIndexSetDecline::OverBudget {
            distinct_buckets: distinct.len(),
            bridged_bytes: probe.bridged_bytes,
        })
    );
    assert_eq!(reads, vec![prefix_range.clone()], "no read past the prefix");
    assert_eq!(declined.get_requests, 1);
    assert_eq!(declined.decompressed_bytes, 0);

    // At or under B_run buckets: one GET per bucket, nothing bridged, and the
    // byte budget does not apply.
    let few: Vec<Key8> = keys.iter().copied().take(20).collect();
    let few_buckets: std::collections::BTreeSet<u32> = few
        .iter()
        .filter_map(|k| prefix.locate(k).expect("in slice").map(|l| l.bucket))
        .collect();
    let starved = KeyIndexSetPolicy {
        max_bridged_bytes: 0,
        ..roomy
    };
    let (result, reads) = set_probe_with(&leaf.bytes, &leaf.leaf_ref, &req, &few, &starved);
    let probe = result.expect("no breach");
    expect_answered(&probe, &few, &truth);
    assert_eq!(reads.len(), 1 + few_buckets.len());
    assert_eq!(probe.bridged_bytes, 0);
}

#[test]
fn set_probe_refused_reservation_declines_and_bad_keys_are_rejected() {
    let (part, leaf, truth) = wide_leaf();
    let req = request(&part);
    let keys: Vec<Key8> = truth.keys().copied().take(3).collect();
    let reserved = RefCell::new(Vec::new());
    let probe = block_on(probe_key_index_leaf_set_ranged(
        &leaf.leaf_ref,
        &req,
        &keys,
        &KeyIndexLeafLimits::default(),
        &KeyIndexSetPolicy::for_part(PART_ENTRY_COUNT),
        |range: Range<u64>| {
            let out = Ok(Bytes::copy_from_slice(
                &leaf.bytes[range.start as usize..range.end as usize],
            ));
            async move { out }
        },
        |bytes| {
            reserved.borrow_mut().push(bytes);
            None::<()>
        },
    ))
    .expect("no breach");
    let reserved = reserved.into_inner();
    assert_eq!(reserved.len(), 1, "the first refusal ends the probe");
    assert_eq!(
        probe.answer,
        KeyIndexSetAnswer::Declined(KeyIndexSetDecline::ReservationRefused { bytes: reserved[0] })
    );
    assert_eq!(probe.decompressed_bytes, 0);

    for bad in [Vec::new(), vec![keys[1], keys[0]], vec![keys[0], keys[0]]] {
        let (result, reads) = set_probe_with(
            &leaf.bytes,
            &leaf.leaf_ref,
            &req,
            &bad,
            &KeyIndexSetPolicy::for_part(PART_ENTRY_COUNT),
        );
        assert!(
            matches!(
                result.expect("no breach").answer,
                KeyIndexSetAnswer::Rejected(KeyIndexLeafRejection::RequestMismatch(_))
            ),
            "{bad:?}"
        );
        assert!(reads.is_empty());
    }
}

/// What one set probe did, frame by frame.
struct CountedSetProbe {
    probe: KeyIndexSetProbe,
    /// Each reservation asked for: its bytes, and the frame decompressions
    /// this probe had started when it was asked.
    reserved: Vec<(u64, u64)>,
    crc_checks: u64,
    decompressions: u64,
}

fn counted_set_probe(
    leaf: &[u8],
    leaf_ref: &SnapshotKeyIndexLeafRef,
    request: &KeyIndexLeafRequest<'_>,
    keys: &[Key8],
    policy: &KeyIndexSetPolicy,
) -> CountedSetProbe {
    let crc_before = FRAME_CRC_CHECKS.with(|n| n.get());
    let decompressions_before = frame_decompressions();
    let reserved = RefCell::new(Vec::new());
    let probe = block_on(probe_key_index_leaf_set_ranged(
        leaf_ref,
        request,
        keys,
        &KeyIndexLeafLimits::default(),
        policy,
        |range: Range<u64>| {
            let out = Ok(Bytes::copy_from_slice(
                &leaf[range.start as usize..range.end as usize],
            ));
            async move { out }
        },
        |bytes| {
            reserved
                .borrow_mut()
                .push((bytes, frame_decompressions() - decompressions_before));
            Some(())
        },
    ))
    .expect("no breach");
    CountedSetProbe {
        probe,
        reserved: reserved.into_inner(),
        crc_checks: FRAME_CRC_CHECKS.with(|n| n.get()) - crc_before,
        decompressions: frame_decompressions() - decompressions_before,
    }
}

/// Seven keys of the wide leaf in seven distinct buckets, read as one
/// bridged run, so the non-empty buckets between them are read and not
/// selected. Returns the keys, their buckets' frame byte ranges in the leaf
/// (ascending), and the count of unselected non-empty buckets in the run.
fn bridged_selection(
    leaf: &EncodedKeyIndexLeaf,
    req: &KeyIndexLeafRequest<'_>,
    truth: &BTreeMap<Key8, Vec<KeyIndexMatch>>,
) -> (Vec<Key8>, Vec<Range<usize>>, usize) {
    let prefix = KeyIndexLeafPrefix::parse(
        &leaf.bytes[..leaf.leaf_ref.prefix_len as usize],
        &leaf.leaf_ref,
        req,
    )
    .expect("parses");
    let keys: Vec<Key8> = truth.keys().copied().step_by(150).collect();
    assert_eq!(keys.len(), 7);
    let buckets: std::collections::BTreeSet<u32> = keys
        .iter()
        .map(|k| prefix.locate(k).expect("in slice").expect("present").bucket)
        .collect();
    assert_eq!(buckets.len(), 7, "seven distinct buckets");
    let frames: Vec<Range<usize>> = buckets
        .iter()
        .map(|&b| {
            let body = prefix.bucket_body_range(b as usize);
            (prefix.prefix_len() + body.start) as usize..(prefix.prefix_len() + body.end) as usize
        })
        .collect();
    let (first, last) = (*buckets.first().expect("7"), *buckets.last().expect("7"));
    let unselected = (first..=last)
        .filter(|b| !buckets.contains(b) && !prefix.bucket_body_range(*b as usize).is_empty())
        .count();
    (keys, frames, unselected)
}

const ONE_RUN: KeyIndexSetPolicy = KeyIndexSetPolicy {
    b_run: 1,
    max_bridged_bytes: u64::MAX,
};

#[test]
fn the_set_probe_checks_each_frames_crc_once() {
    let (part, leaf, truth) = wide_leaf();
    let req = request(&part);
    let (keys, frames, unselected) = bridged_selection(&leaf, &req, &truth);
    assert!(unselected > 0, "the run holds unselected frames");
    let counted = counted_set_probe(&leaf.bytes, &leaf.leaf_ref, &req, &keys, &ONE_RUN);
    expect_answered(&counted.probe, &keys, &truth);
    assert_eq!(counted.probe.get_requests, 2, "prefix and one bridged run");
    assert!(counted.probe.bridged_bytes > 0);
    assert_eq!(frames.len(), 7);
    assert_eq!(
        counted.crc_checks, 7,
        "one crc per selected frame, none for gaps"
    );
    assert_eq!(counted.decompressions, 7);
    assert_eq!(counted.reserved.len(), 7);
}

#[test]
fn a_corrupt_frame_body_is_rejected_by_the_set_probe() {
    let (part, leaf, truth) = wide_leaf();
    let req = request(&part);
    let (keys, frames, _) = bridged_selection(&leaf, &req, &truth);
    // The first selected frame: header fields at +0 (entry_count) and +4
    // (uncompressed_len), body from +12.
    let frame = frames[0].clone();
    let cases: [(&str, FrameEdit, &str); 3] = [
        (
            "flipped body byte",
            |f| f[FRAME_HEADER_LEN + 1] ^= 0x40,
            "frame crc32c mismatch",
        ),
        (
            "zero entries",
            |f| f[0..4].copy_from_slice(&0u32.to_le_bytes()),
            "declares zero entries",
        ),
        (
            "over the body ceiling",
            |f| f[4..8].copy_from_slice(&u32::MAX.to_le_bytes()),
            "over the",
        ),
    ];
    for (name, corrupt, want) in cases {
        let mut bytes = leaf.bytes.clone();
        corrupt(&mut bytes[frame.clone()]);
        let counted = counted_set_probe(&bytes, &leaf.leaf_ref, &req, &keys, &ONE_RUN);
        match &counted.probe.answer {
            KeyIndexSetAnswer::Rejected(KeyIndexLeafRejection::Corrupted(msg)) => {
                assert!(msg.contains(want), "{name}: {msg}")
            }
            other => panic!("{name}: expected Corrupted, got {other:?}"),
        }
        assert!(
            counted.reserved.is_empty(),
            "{name}: rejected before reserving"
        );
        assert_eq!(counted.decompressions, 0, "{name}");
        assert_eq!(counted.probe.decompressed_bytes, 0, "{name}");
    }
}

#[test]
fn the_set_probe_reserves_each_frame_before_decoding_it() {
    let (part, leaf, truth) = wide_leaf();
    let req = request(&part);
    let (keys, frames, _) = bridged_selection(&leaf, &req, &truth);
    let counted = counted_set_probe(&leaf.bytes, &leaf.leaf_ref, &req, &keys, &ONE_RUN);
    expect_answered(&counted.probe, &keys, &truth);
    let want: Vec<(u64, u64)> = frames
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let uncompressed_len = le_u32(&leaf.bytes[f.clone()], 4).expect("frame header");
            (decoded_bound(uncompressed_len), i as u64)
        })
        .collect();
    assert_eq!(
        counted.reserved, want,
        "frame i's decoded bound is reserved after i decompressions"
    );
}

fn entries_strategy() -> impl Strategy<Value = Vec<KeyIndexLeafEntry>> {
    let key = prop_oneof![
        (0u8..12).prop_map(|i| key8_str(&format!("pool-{i}"))),
        any::<u64>().prop_map(u64::to_be_bytes),
        Just([0u8; 8]),
        Just([0xffu8; 8]),
    ];
    let blocks = proptest::collection::btree_set(0u32..2000, 1..6)
        .prop_map(|set| set.into_iter().collect::<Vec<u32>>());
    proptest::collection::vec((key, 0u64..50, blocks), 0..80).prop_map(|raw| {
        let unique: BTreeMap<(Key8, u64), Vec<u32>> = raw
            .into_iter()
            .map(|(key8, ordinal, blocks)| ((key8, ordinal), blocks))
            .collect();
        unique
            .into_iter()
            .map(|((key8, entry_ordinal), blocks)| KeyIndexLeafEntry {
                key8,
                entry_ordinal,
                blocks,
            })
            .collect()
    })
}

proptest! {
    #[test]
    fn leaf_round_trips_and_every_key_probes_exactly(
        entries in entries_strategy(),
        uncovered in proptest::collection::vec(50u64..60, 0..6),
        bucket_bits in prop::option::of(1u8..=16),
        lookup in any::<u64>(),
        reversed in any::<bool>(),
    ) {
        let part = part_hash(b"prop part");
        let mut given = entries.clone();
        if reversed {
            given.reverse();
        }
        let mut leaf_input = input(part, &given, &uncovered);
        leaf_input.part_entry_count = 60;
        let limits = KeyIndexLeafLimits::default();
        let leaf = match bucket_bits {
            Some(bits) => encode_key_index_leaf_with_reader_bucket_bits(&leaf_input, bits, &limits),
            None => encode_key_index_leaf(&leaf_input, &limits),
        }
        .expect("encode");
        let mut expected_uncovered = uncovered.clone();
        expected_uncovered.sort_unstable();
        expected_uncovered.dedup();

        let summary = verify_key_index_leaf(&leaf.bytes, &limits).expect("verify");
        prop_assert_eq!(sorted(summary.entries.clone()), entries.clone());
        prop_assert_eq!(summary.leaf_ref(), leaf.leaf_ref.clone());
        prop_assert_eq!(&summary.header.uncovered_entry_ordinals, &expected_uncovered);

        let mut req = request(&part);
        req.part_entry_count = 60;
        let mut keys: Vec<Key8> = entries.iter().map(|e| e.key8).collect();
        keys.push(lookup.to_be_bytes());
        keys.dedup();
        for key8 in keys {
            let truth: Vec<KeyIndexMatch> = entries
                .iter()
                .filter(|e| e.key8 == key8)
                .map(|e| KeyIndexMatch { entry_ordinal: e.entry_ordinal, blocks: e.blocks.clone() })
                .collect();
            let (got, reads) = probe_with(&leaf.bytes, &leaf.leaf_ref, &req, &key8, &limits);
            let got = got.expect("no breach");
            let want = if truth.is_empty() {
                KeyIndexLeafProbe::Absent { uncovered_entry_ordinals: expected_uncovered.clone() }
            } else {
                KeyIndexLeafProbe::Matches {
                    matches: truth,
                    uncovered_entry_ordinals: expected_uncovered.clone(),
                }
            };
            prop_assert_eq!(got, want);
            prop_assert!(reads.len() <= 2);
        }
    }
}

// Mutation fuzz, in the style of crates/ravel-segment/tests/fuzz_mutation.rs:
// mutated leaf bytes must decode to a typed rejection or to exactly the
// unmutated answer, never panic and never a subset of the true matches.

#[derive(Debug, Clone)]
enum Mutation {
    FlipBit {
        index: prop::sample::Index,
        bit: u8,
    },
    SetByte {
        index: prop::sample::Index,
        value: u8,
    },
    Overwrite {
        index: prop::sample::Index,
        bytes: Vec<u8>,
    },
    Truncate {
        index: prop::sample::Index,
    },
}

fn mutation_strategy() -> impl Strategy<Value = Mutation> {
    prop_oneof![
        (any::<prop::sample::Index>(), 0u8..8)
            .prop_map(|(index, bit)| Mutation::FlipBit { index, bit }),
        (any::<prop::sample::Index>(), any::<u8>())
            .prop_map(|(index, value)| Mutation::SetByte { index, value }),
        (
            any::<prop::sample::Index>(),
            proptest::collection::vec(any::<u8>(), 1..=4)
        )
            .prop_map(|(index, bytes)| Mutation::Overwrite { index, bytes }),
        any::<prop::sample::Index>().prop_map(|index| Mutation::Truncate { index }),
    ]
}

fn mutate(bytes: &[u8], mutation: &Mutation) -> Vec<u8> {
    let mut out = bytes.to_vec();
    match mutation {
        Mutation::FlipBit { index, bit } => out[index.index(bytes.len())] ^= 1 << bit,
        Mutation::SetByte { index, value } => out[index.index(bytes.len())] = *value,
        Mutation::Overwrite {
            index,
            bytes: patch,
        } => {
            let at = index.index(bytes.len());
            for (i, b) in patch.iter().enumerate() {
                if let Some(slot) = out.get_mut(at + i) {
                    *slot = *b;
                }
            }
        }
        Mutation::Truncate { index } => out.truncate(index.index(bytes.len())),
    }
    out
}

fn fuzz_fixture() -> &'static Fixture {
    static FIXTURE: std::sync::OnceLock<Fixture> = std::sync::OnceLock::new();
    FIXTURE.get_or_init(fixture)
}

proptest! {
    #[test]
    fn mutated_leaf_is_rejected_or_answers_unchanged(mutation in mutation_strategy()) {
        let f = fuzz_fixture();
        let mutated = mutate(&f.leaf.bytes, &mutation);
        let limits = KeyIndexLeafLimits::default();
        let req = request(&f.part);
        for key8 in [f.alpha, f.beta, f.absent_same_bucket, key8_str("filler-3")] {
            let (truth, _) = probe(&f.leaf.bytes, &f.leaf.leaf_ref, &req, &key8);
            let (got, _) = probe_with(&mutated, &f.leaf.leaf_ref, &req, &key8, &limits);
            match got {
                Ok(KeyIndexLeafProbe::Rejected(_)) => {}
                Ok(answer) => prop_assert_eq!(answer, truth),
                Err(breach) => {
                    // Only a mutation inside the crc-covered header could
                    // reach the binding check, and the crc catches it.
                    prop_assert!(false, "breach from a mutated own-tenant leaf: {breach:?}");
                }
            }
        }
        if let Ok(summary) = verify_key_index_leaf(&mutated, &limits) {
            prop_assert_eq!(sorted(summary.entries), sorted(f.entries.clone()));
        }
    }

    #[test]
    fn arbitrary_bytes_never_panic(
        bytes in proptest::collection::vec(any::<u8>(), 0..512),
        prefix_len in any::<u32>(),
        size in any::<u64>(),
        key in any::<u64>(),
    ) {
        let leaf_ref = SnapshotKeyIndexLeafRef { prefix_len, size, ..fuzz_fixture().leaf.leaf_ref.clone() };
        let limits = KeyIndexLeafLimits::default();
        let _ = probe_with(&bytes, &leaf_ref, &request(&fuzz_fixture().part), &key.to_be_bytes(), &limits);
        let _ = verify_key_index_leaf(&bytes, &limits);
        let mut headed = KEY_INDEX_LEAF_MAGIC.to_vec();
        headed.extend_from_slice(&bytes);
        let _ = verify_key_index_leaf(&headed, &limits);
    }
}
