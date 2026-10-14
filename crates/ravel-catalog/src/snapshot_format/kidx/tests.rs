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

const TENANT: TenantHash = TenantHash([7u8; 16]);
const SIGNAL: Signal = Signal::Logs;
const PART_ENTRY_COUNT: u64 = 8;
const FIELD: &str = "service.name";
const UNCOVERED: [u64; 2] = [3, 6];

type RefEdit = fn(&mut SnapshotKeyIndexLeafRef);

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
    assert_eq!(KEY_INDEX_LEAF_FORMAT_VERSION, 1);
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
    assert!(invalid(encode_key_index_leaf_with_bucket_bits(
        &input(part, &[], &[]),
        17,
        &limits
    )));
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
    expect_rejected(&one(raw_entry(k, 2, &[1]), 2), &k, &limits, "truncated");
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
        &with(&|h| h.format_version = 2).0,
        &with(&|h| h.format_version = 2).1,
        &request(&part),
        &k,
        &limits,
    );
    assert_eq!(
        result.expect("no breach"),
        KeyIndexLeafProbe::Rejected(KeyIndexLeafRejection::UnsupportedVersion(2))
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
            Some(bits) => encode_key_index_leaf_with_bucket_bits(&leaf_input, bits, &limits),
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
