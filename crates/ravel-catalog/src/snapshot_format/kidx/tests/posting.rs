//! Entry kind 1: the posting codec, its decode bound, and the entry-kind
//! checks of the version 3 reader (ADR-4146 decision 5).

use super::*;

const POSTING_FIELD: &str = METRICS_SERIES_LABEL_FIELD;

fn posting(key8: Key8, values: &[u64]) -> KeyIndexPostingEntry {
    KeyIndexPostingEntry {
        key8,
        values: values.to_vec(),
    }
}

fn posting_input<'a>(
    part_blake3: [u8; 32],
    postings: &'a [KeyIndexPostingEntry],
    uncovered: &'a [u64],
) -> KeyIndexPostingLeafInput<'a> {
    KeyIndexPostingLeafInput {
        tenant_hash: TENANT,
        signal: Signal::Metrics,
        part_blake3,
        part_entry_count: PART_ENTRY_COUNT,
        field: POSTING_FIELD,
        key_type: KeyType::Bytes,
        key8_lo: 0,
        key8_hi: u64::MAX,
        uncovered_entry_ordinals: uncovered,
        postings,
    }
}

fn posting_request(part_blake3: &[u8]) -> KeyIndexLeafRequest<'_> {
    KeyIndexLeafRequest {
        tenant_hash: TENANT,
        signal: Signal::Metrics,
        part_blake3,
        part_entry_count: PART_ENTRY_COUNT,
        field: POSTING_FIELD,
        key_type: KeyType::Bytes,
    }
}

/// GET 1 then GET 2 of a posting probe over in-memory bytes: `key8`'s
/// posting, `None` when the leaf has none.
fn posting_probe(
    bytes: &[u8],
    leaf_ref: &SnapshotKeyIndexLeafRef,
    request: &KeyIndexLeafRequest<'_>,
    key8: &Key8,
) -> Result<Option<Vec<u64>>, KeyIndexLeafRejection> {
    let prefix_end = (leaf_ref.prefix_len as usize).min(bytes.len());
    let prefix = KeyIndexLeafPrefix::parse(&bytes[..prefix_end], leaf_ref, request).map_err(
        |error| match error {
            KeyIndexLeafError::Rejected(rejection) => rejection,
            KeyIndexLeafError::Breach(breach) => panic!("breach from an own-tenant leaf: {breach:?}"),
        },
    )?;
    let Some(located) = prefix.locate(key8)? else {
        return Ok(None);
    };
    let start = (located.range.start as usize).min(bytes.len());
    let end = (located.range.end as usize).min(bytes.len());
    prefix.probe_posting_bucket(
        &located,
        &bytes[start..end],
        key8,
        &KeyIndexLeafLimits::default(),
    )
}

/// A kind-1 header at version 3 for the metrics posting field.
fn posting_header(bucket_bits: u32) -> KeyIndexLeafHeader {
    KeyIndexLeafHeader {
        format_version: KEY_INDEX_LEAF_FORMAT_VERSION_3,
        signal: signal_code(Signal::Metrics),
        field: POSTING_FIELD.into(),
        key_type: u32::from(KeyType::Bytes.as_u8()),
        entry_kind: KeyIndexEntryKind::Posting.as_u32(),
        ..base_header(bucket_bits)
    }
}

fn raw_posting(key8: Key8, deltas: &[u64]) -> Vec<u8> {
    raw_posting_claiming(key8, deltas.len() as u64, deltas)
}

fn raw_posting_claiming(key8: Key8, count: u64, deltas: &[u64]) -> Vec<u8> {
    let mut out = key8.to_vec();
    write_uvarint(&mut out, count);
    for &d in deltas {
        write_uvarint(&mut out, d);
    }
    out
}

fn expect_posting_rejected(
    (bytes, leaf_ref): &(Vec<u8>, SnapshotKeyIndexLeafRef),
    key8: &Key8,
    needle: &str,
) {
    let part = part_hash(b"hand");
    let got = posting_probe(bytes, leaf_ref, &posting_request(&part), key8);
    assert!(
        matches!(&got, Err(KeyIndexLeafRejection::Corrupted(m)) if m.contains(needle)),
        "wanted Corrupted containing {needle:?}, got {got:?}"
    );
    let whole = verify_key_index_leaf(bytes, &KeyIndexLeafLimits::default());
    assert!(
        matches!(&whole, Err(KeyIndexLeafRejection::Corrupted(_))),
        "whole-object verify of a {needle:?} leaf gave {whole:?}"
    );
}

/// Keys that mix to `bucket` at `bucket_bits` 1, ascending.
fn keys_in_bucket(bucket: u32, n: usize) -> Vec<Key8> {
    let mut keys: Vec<Key8> = (0..)
        .map(|i| key8_str(&format!("posting-key-{i}")))
        .filter(|k| bucket_index(k, 1).expect("bits") == bucket)
        .take(n)
        .collect();
    keys.sort();
    keys
}

fn posting_allocations() -> Vec<usize> {
    POSTING_ALLOCATIONS.with(|a| a.borrow().clone())
}

fn clear_posting_allocations() {
    POSTING_ALLOCATIONS.with(|a| a.borrow_mut().clear());
}

#[test]
fn series_label_pair_bytes_are_pinned() {
    assert_eq!(SERIES_LABEL_METRIC_NAME, "__name__");
    let mut name_pair = vec![8, 0];
    name_pair.extend_from_slice(b"__name__");
    name_pair.extend_from_slice(&[2, 0]);
    name_pair.extend_from_slice(b"up");
    assert_eq!(
        series_label_pair_bytes(SERIES_LABEL_METRIC_NAME, "up").expect("pair"),
        name_pair
    );
    let mut ordinary = vec![3, 0];
    ordinary.extend_from_slice(b"job");
    ordinary.extend_from_slice(&[10, 0]);
    ordinary.extend_from_slice(b"api-server");
    assert_eq!(
        series_label_pair_bytes("job", "api-server").expect("pair"),
        ordinary
    );
    assert_eq!(
        series_label_pair_bytes("", "").expect("pair"),
        vec![0, 0, 0, 0]
    );
    // The lengths are little-endian u16: 300 is 0x012c.
    let long = "v".repeat(300);
    let bytes = series_label_pair_bytes("a", &long).expect("pair");
    assert_eq!(&bytes[..5], &[1, 0, b'a', 0x2c, 0x01]);

    assert_eq!(
        series_label_pair_key8("job", "api-server").expect("key8"),
        key8_bytes(&ordinary)
    );
    // The length prefix keeps a split point from colliding.
    assert_ne!(
        series_label_pair_bytes("ab", "c").expect("pair"),
        series_label_pair_bytes("a", "bc").expect("pair")
    );

    let max = "x".repeat(usize::from(u16::MAX));
    let over = "x".repeat(usize::from(u16::MAX) + 1);
    assert!(series_label_pair_bytes(&max, &max).is_ok());
    for (name, value) in [(over.as_str(), "v"), ("n", over.as_str())] {
        assert!(matches!(
            series_label_pair_bytes(name, value),
            Err(KeyIndexLeafEncodeError::InvalidInput(_))
        ));
        assert!(matches!(
            series_label_pair_key8(name, value),
            Err(KeyIndexLeafEncodeError::InvalidInput(_))
        ));
    }
}

#[test]
fn field_entry_kinds_and_their_codes_are_pinned() {
    assert_eq!(KeyIndexEntryKind::Entry.as_u32(), 0);
    assert_eq!(KeyIndexEntryKind::Posting.as_u32(), 1);
    assert_eq!(KeyIndexEntryKind::from_u32(0), Some(KeyIndexEntryKind::Entry));
    assert_eq!(
        KeyIndexEntryKind::from_u32(1),
        Some(KeyIndexEntryKind::Posting)
    );
    assert_eq!(KeyIndexEntryKind::from_u32(2), None);

    assert_eq!(METRICS_SERIES_LABEL_FIELD, "series_label");
    assert_eq!(METRICS_SERIES_CLASS_FIELD, "series_class");
    assert_eq!(METRICS_CLASS_OBJECTS_FIELD, "class_objects");
    for (signal, field, want) in [
        (
            Signal::Metrics,
            METRICS_SERIES_LABEL_FIELD,
            KeyIndexEntryKind::Posting,
        ),
        (
            Signal::Metrics,
            METRICS_SERIES_CLASS_FIELD,
            KeyIndexEntryKind::Posting,
        ),
        (
            Signal::Metrics,
            METRICS_CLASS_OBJECTS_FIELD,
            KeyIndexEntryKind::Entry,
        ),
        (Signal::Logs, METRICS_SERIES_LABEL_FIELD, KeyIndexEntryKind::Entry),
        (Signal::Spans, METRICS_SERIES_CLASS_FIELD, KeyIndexEntryKind::Entry),
        (Signal::Logs, FIELD, KeyIndexEntryKind::Entry),
    ] {
        assert_eq!(key_index_field_entry_kind(signal, field), want, "{signal:?} {field}");
    }

    assert_eq!(key_index_leaf_versions_for_signal(Signal::Metrics), &[1, 2, 3]);
    assert_eq!(key_index_leaf_versions_for_signal(Signal::Logs), &[1, 2]);
    assert_eq!(key_index_leaf_versions_for_signal(Signal::Spans), &[1, 2]);
}

/// Postings over 60 label pairs, some spanning one bucket, some sharing it.
fn posting_fixture() -> Vec<KeyIndexPostingEntry> {
    let mut postings: Vec<KeyIndexPostingEntry> = (0..60u64)
        .map(|i| {
            let key8 = series_label_pair_key8("job", &format!("svc-{i}")).expect("key8");
            let values: Vec<u64> = (0..=i % 7).map(|j| i * 1_000 + j * (i + 1)).collect();
            posting(key8, &values)
        })
        .collect();
    postings.push(posting(
        series_label_pair_key8(SERIES_LABEL_METRIC_NAME, "up").expect("key8"),
        &[0, 1, 300, u64::MAX],
    ));
    postings.sort_by_key(|p| p.key8);
    postings
}

#[test]
fn posting_leaf_round_trips_through_every_reader() {
    let postings = posting_fixture();
    let part = part_hash(b"posting part");
    let limits = KeyIndexLeafLimits::default();
    let leaf = encode_key_index_posting_leaf_with_bucket_bits(
        &posting_input(part, &postings, &UNCOVERED),
        3,
        &limits,
    )
    .expect("encode");

    let summary = verify_key_index_leaf(&leaf.bytes, &limits).expect("verifies");
    assert_eq!(summary.header.format_version, KEY_INDEX_LEAF_FORMAT_VERSION_3);
    assert_eq!(summary.header.entry_kind, 1);
    assert_eq!(summary.entry_kind, KeyIndexEntryKind::Posting);
    assert_eq!(summary.header.entry_count, postings.len() as u64);
    assert!(summary.entries.is_empty());
    let mut got = summary.postings.clone();
    got.sort_by_key(|p| p.key8);
    assert_eq!(got, postings);
    assert_eq!(summary.leaf_ref(), SnapshotKeyIndexLeafRef { key: String::new(), ..leaf.leaf_ref.clone() });

    let mut streamed = Vec::new();
    verify_key_index_posting_leaf_frames(&leaf.bytes, &limits, |frame| streamed.extend(frame))
        .expect("verifies");
    streamed.sort_by_key(|p| p.key8);
    assert_eq!(streamed, postings);
    assert_eq!(
        verify_key_index_leaf_frames(&leaf.bytes, &limits, |_| {}).map(|_| ()),
        Err(KeyIndexLeafRejection::EntryKindMismatch {
            expected: 0,
            actual: 1
        })
    );

    let req = posting_request(&part);
    for p in &postings {
        assert_eq!(
            posting_probe(&leaf.bytes, &leaf.leaf_ref, &req, &p.key8),
            Ok(Some(p.values.clone()))
        );
    }
    let absent = series_label_pair_key8("job", "absent").expect("key8");
    assert_eq!(
        posting_probe(&leaf.bytes, &leaf.leaf_ref, &req, &absent),
        Ok(None)
    );

    // The kind-0 probe refuses the leaf rather than misreading it.
    let (kind0, _) = probe_with(
        &leaf.bytes,
        &leaf.leaf_ref,
        &req,
        &postings[0].key8,
        &limits,
    );
    assert_eq!(
        kind0.expect("no breach"),
        KeyIndexLeafProbe::Rejected(KeyIndexLeafRejection::EntryKindMismatch {
            expected: 0,
            actual: 1
        })
    );

    // A leaf set mixing kinds for one field is refused.
    let mut kind0_twin = summary.clone();
    kind0_twin.header.entry_kind = 0;
    assert!(check_key_index_leaf_set(std::slice::from_ref(&summary)).is_ok());
    assert!(matches!(
        check_key_index_leaf_set(&[summary, kind0_twin]),
        Err(KeyIndexLeafRejection::Corrupted(m)) if m.contains("entry kinds")
    ));
}

#[test]
fn posting_set_probe_answers_and_reserves_the_posting_bound() {
    let postings = posting_fixture();
    let part = part_hash(b"posting part");
    let limits = KeyIndexLeafLimits::default();
    let leaf = encode_key_index_posting_leaf_with_bucket_bits(
        &posting_input(part, &postings, &UNCOVERED),
        3,
        &limits,
    )
    .expect("encode");
    let absent = series_label_pair_key8("job", "absent").expect("key8");
    let mut keys: Vec<Key8> = postings.iter().step_by(3).map(|p| p.key8).collect();
    keys.push(absent);
    keys.sort();
    let req = posting_request(&part);
    let prefix = &leaf.bytes[..leaf.leaf_ref.prefix_len as usize];
    let plan = plan_key_index_leaf_set(prefix, &leaf.leaf_ref, &req, &keys, 64).expect("plan");
    assert_eq!(plan.entry_kind(), KeyIndexEntryKind::Posting);

    let mut reserved = Vec::new();
    let mut decoder = plan.posting_decoder::<()>();
    let mut reserve = |bytes: u64| {
        reserved.push(bytes);
        Some(())
    };
    for run in plan.runs() {
        decoder
            .decode_next_run(
                &leaf.bytes[run.start as usize..run.end as usize],
                &limits,
                &mut reserve,
            )
            .expect("run decodes");
    }
    let decompressed = decoder.decompressed_bytes();
    let KeyIndexPostingSetAnswer::Answered {
        per_key,
        uncovered_entry_ordinals,
    } = decoder.finish()
    else {
        panic!("expected Answered");
    };
    assert_eq!(uncovered_entry_ordinals, UNCOVERED.to_vec());
    for (key8, answer) in keys.iter().zip(&per_key) {
        let want = postings
            .iter()
            .find(|p| &p.key8 == key8)
            .map(|p| p.values.clone());
        assert_eq!(answer, &want);
    }

    // Each frame reserves 16 bytes per wire byte plus 64, the posting bound,
    // not the kind-0 bound. Under b_run every run is one bucket's frame.
    assert_eq!(plan.bridged_bytes(), 0);
    assert_eq!(plan.runs().len(), plan.distinct_buckets());
    let lens: Vec<u32> = plan
        .runs()
        .iter()
        .map(|run| le_u32(&leaf.bytes[run.start as usize..], 4).expect("frame header"))
        .collect();
    let want: Vec<u64> = lens.iter().map(|&len| 16 * u64::from(len) + 64).collect();
    assert_eq!(reserved, want);
    assert!(
        lens.iter()
            .any(|&len| posting_decoded_bound(len) != decoded_bound(len))
    );
    assert_eq!(
        decompressed,
        lens.iter().map(|&len| u64::from(len)).sum::<u64>()
    );

    // A refused reservation declines with the posting bound it asked for.
    let mut refusing = plan.posting_decoder::<()>();
    let first = plan.runs()[0].clone();
    let err = refusing
        .decode_next_run(
            &leaf.bytes[first.start as usize..first.end as usize],
            &limits,
            &mut |_| None,
        )
        .expect_err("refused");
    assert_eq!(
        KeyIndexPostingSetAnswer::from(err),
        KeyIndexPostingSetAnswer::Declined(KeyIndexSetDecline::ReservationRefused {
            bytes: want[0]
        })
    );

    // The kind-0 decoder refuses a posting plan before reading a frame.
    let mut kind0 = plan.decoder::<()>();
    assert_eq!(
        kind0.decode_next_run(
            &leaf.bytes[first.start as usize..first.end as usize],
            &limits,
            &mut |_| Some(()),
        ),
        Err(KeyIndexSetRunError::Rejected(
            KeyIndexLeafRejection::EntryKindMismatch {
                expected: 0,
                actual: 1
            }
        ))
    );
}

#[test]
fn posting_splitter_tiles_the_key_space_and_every_slice_encodes() {
    let k = key8_str("len");
    // key8, a one-byte count, deltas 1, 1, 299: 8 + 1 + 1 + 1 + 2.
    assert_eq!(encoded_posting_len(&posting(k, &[0, 1, 300])), 13);
    assert_eq!(encoded_posting_len(&posting(k, &[u64::MAX - 1])), 8 + 1 + 10);

    let postings = posting_fixture();
    let part = part_hash(b"posting part");
    let total: u64 = postings.iter().map(encoded_posting_len).sum();
    let ceiling = total / 5;
    let slices = split_key_index_postings(&postings, ceiling).expect("split");
    assert!(slices.len() >= 5, "{} slices", slices.len());
    assert_eq!(slices[0].key8_lo, 0);
    assert_eq!(slices.last().expect("a slice").key8_hi, u64::MAX);
    let mut next = 0usize;
    for (i, slice) in slices.iter().enumerate() {
        assert_eq!(slice.entries.start, next);
        next = slice.entries.end;
        if i > 0 {
            assert_eq!(slice.key8_lo, slices[i - 1].key8_hi + 1);
        }
        let in_slice = &postings[slice.entries.clone()];
        assert!(in_slice.iter().map(encoded_posting_len).sum::<u64>() <= ceiling);
        let leaf = encode_key_index_posting_leaf(
            &KeyIndexPostingLeafInput {
                key8_lo: slice.key8_lo,
                key8_hi: slice.key8_hi,
                ..posting_input(part, in_slice, &UNCOVERED)
            },
            &KeyIndexLeafLimits {
                max_body_bytes: ceiling,
            },
        )
        .expect("every slice encodes under the ceiling it was cut for");
        let req = posting_request(&part);
        for p in in_slice {
            assert_eq!(
                posting_probe(&leaf.bytes, &leaf.leaf_ref, &req, &p.key8),
                Ok(Some(p.values.clone()))
            );
        }
    }
    assert_eq!(next, postings.len());

    assert_eq!(
        split_key_index_postings(&[], 10).expect("empty"),
        vec![KeyIndexSlice {
            key8_lo: 0,
            key8_hi: u64::MAX,
            entries: 0..0
        }]
    );
    let big = posting(k, &[0, 1, 300]);
    assert!(matches!(
        split_key_index_postings(std::slice::from_ref(&big), 12),
        Err(KeyIndexLeafEncodeError::BodyOverCeiling {
            body: 13,
            ceiling: 12
        })
    ));
    let mut reversed = postings.clone();
    reversed.reverse();
    let mut repeated = postings.clone();
    repeated.insert(1, repeated[0].clone());
    for bad in [reversed, repeated] {
        assert!(matches!(
            split_key_index_postings(&bad, total),
            Err(KeyIndexLeafEncodeError::InvalidInput(_))
        ));
    }
}

#[test]
fn posting_encoder_refuses_invalid_input() {
    let part = part_hash(b"posting part");
    let limits = KeyIndexLeafLimits::default();
    let k = key8_str("a");
    let good = [posting(k, &[1, 2])];
    let refused = |input: KeyIndexPostingLeafInput<'_>, why: &str| {
        let got = encode_key_index_posting_leaf(&input, &limits);
        assert!(
            matches!(got, Err(KeyIndexLeafEncodeError::InvalidInput(_))),
            "{why}: {got:?}"
        );
    };
    refused(
        KeyIndexPostingLeafInput {
            field: METRICS_CLASS_OBJECTS_FIELD,
            ..posting_input(part, &good, &[])
        },
        "a kind-0 metrics field",
    );
    refused(
        KeyIndexPostingLeafInput {
            signal: Signal::Logs,
            ..posting_input(part, &good, &[])
        },
        "series_label of logs",
    );
    let key = u64::from_be_bytes(k);
    refused(
        KeyIndexPostingLeafInput {
            key8_lo: key.checked_add(1).expect("not the top key"),
            ..posting_input(part, &good, &[])
        },
        "a key8 outside the slice",
    );
    for (postings, why) in [
        (vec![posting(k, &[1]), posting(k, &[2])], "two postings for one key8"),
        (vec![posting(k, &[])], "an empty posting"),
        (vec![posting(k, &[2, 1])], "descending values"),
        (vec![posting(k, &[1, 1])], "a repeated value"),
        (vec![posting(k, &[u64::MAX])], "a posting starting at u64::MAX"),
    ] {
        refused(posting_input(part, &postings, &[]), why);
    }
    for bits in [0u8, KEY_INDEX_LEAF_WRITER_MAX_BUCKET_BITS + 1] {
        assert!(matches!(
            encode_key_index_posting_leaf_with_bucket_bits(
                &posting_input(part, &good, &[]),
                bits,
                &limits
            ),
            Err(KeyIndexLeafEncodeError::BucketBitsOutOfRange { .. })
        ));
    }
    assert!(matches!(
        encode_key_index_posting_leaf(
            &posting_input(part, &good, &[]),
            &KeyIndexLeafLimits { max_body_bytes: 10 }
        ),
        Err(KeyIndexLeafEncodeError::BodyOverCeiling {
            body: 11,
            ceiling: 10
        })
    ));

    // The kind-0 encoder refuses a posting field rather than writing a leaf
    // every reader would reject.
    let entries = [entry(k, 0, &[1])];
    for field in [METRICS_SERIES_LABEL_FIELD, METRICS_SERIES_CLASS_FIELD] {
        assert!(matches!(
            encode_key_index_leaf(
                &KeyIndexLeafInput {
                    signal: Signal::Metrics,
                    field,
                    ..input(part, &entries, &[])
                },
                &limits
            ),
            Err(KeyIndexLeafEncodeError::InvalidInput(_))
        ));
    }

    // Unsorted input is sorted, and a posting may end at u64::MAX.
    let k2 = key8_str("b");
    let edge = [posting(k2, &[u64::MAX - 1, u64::MAX]), posting(k, &[0])];
    let leaf = encode_key_index_posting_leaf(&posting_input(part, &edge, &[]), &limits)
        .expect("encodes");
    let req = posting_request(&part);
    for p in &edge {
        assert_eq!(
            posting_probe(&leaf.bytes, &leaf.leaf_ref, &req, &p.key8),
            Ok(Some(p.values.clone()))
        );
    }
}

/// A one-bucket kind-1 leaf whose header declares `count` postings.
fn hand(key8: Key8, count: u32, raw: &[u8]) -> (Vec<u8>, SnapshotKeyIndexLeafRef) {
    let mut header = posting_header(1);
    header.entry_count = u64::from(count);
    assemble(header, &single_bucket(key8, frame(count, raw)), None)
}

#[test]
fn hand_built_posting_leaf_decodes_so_the_corruptions_below_are_isolated() {
    let k = key8_str("hand-key");
    let leaf = hand(k, 1, &raw_posting(k, &[3, 2]));
    let part = part_hash(b"hand");
    assert_eq!(
        posting_probe(&leaf.0, &leaf.1, &posting_request(&part), &k),
        Ok(Some(vec![2, 4]))
    );
    let summary = verify_key_index_leaf(&leaf.0, &KeyIndexLeafLimits::default()).expect("verifies");
    assert_eq!(summary.postings, vec![posting(k, &[2, 4])]);
}

#[test]
fn every_posting_frame_corruption_is_a_typed_rejection() {
    let keys = keys_in_bucket(0, 2);
    let (a, b) = (keys[0], keys[1]);
    let other_bucket = keys_in_bucket(1, 1)[0];
    let cases: Vec<(&str, (Vec<u8>, SnapshotKeyIndexLeafRef), &str)> = vec![
        (
            "zero count",
            hand(a, 1, &raw_posting_claiming(a, 0, &[1])),
            "zero-length posting",
        ),
        (
            "count over the frame",
            hand(a, 1, &raw_posting_claiming(a, 6, &[1; 5])),
            "posting count 6 exceeds the bytes left with 0 postings",
        ),
        (
            "count eating a later posting's bytes",
            hand(
                a,
                2,
                &[raw_posting_claiming(a, 7, &[1; 6]), raw_posting(b, &[1])].concat(),
            ),
            "posting count 7 exceeds the bytes left with 1 postings",
        ),
        (
            "unsorted postings",
            hand(a, 2, &[raw_posting(b, &[1]), raw_posting(a, &[1])].concat()),
            "postings unsorted or repeated",
        ),
        (
            "repeated key8",
            hand(a, 2, &[raw_posting(a, &[1]), raw_posting(a, &[2])].concat()),
            "postings unsorted or repeated",
        ),
        (
            "zero delta: an unsorted posting",
            hand(a, 1, &raw_posting(a, &[1, 0, 1])),
            "is zero or overflows u64",
        ),
        (
            "delta past u64::MAX",
            hand(a, 1, &raw_posting(a, &[u64::MAX, 2])),
            "is zero or overflows u64",
        ),
        (
            "trailing bytes",
            hand(a, 1, &[raw_posting(a, &[1]), vec![0]].concat()),
            "bytes past its 1 postings",
        ),
        (
            "truncated delta",
            hand(a, 1, &[a.to_vec(), vec![1, 0x80]].concat()),
            "truncated inside a uvarint",
        ),
        (
            "more postings than one per 10 bytes",
            hand(a, 2, &raw_posting(a, &[1])),
            "more than one per 10 bytes",
        ),
        (
            "posting in the wrong bucket",
            {
                let mut header = posting_header(1);
                header.entry_count = 1;
                let bucket = bucket_index(&a, 1).expect("bits");
                assemble(
                    header,
                    &BTreeMap::from([(bucket, frame(1, &raw_posting(other_bucket, &[1])))]),
                    None,
                )
            },
            "does not mix to bucket",
        ),
    ];
    for (why, leaf, needle) in &cases {
        eprintln!("case: {why}");
        expect_posting_rejected(leaf, &a, needle);
    }

    // A key8 below the slice: probed with b, which the slice still holds.
    let mut header = posting_header(1);
    header.key8_lo = u64::from_be_bytes(b);
    let below = assemble(header, &single_bucket(a, frame(1, &raw_posting(a, &[1]))), None);
    expect_posting_rejected(&below, &b, "outside the leaf's slice");

    // The object cut short mid-frame.
    let whole = hand(a, 1, &raw_posting(a, &[1; 40]));
    let cut = (whole.0[..whole.0.len() - 3].to_vec(), whole.1.clone());
    expect_posting_rejected(&cut, &a, "read returned");
}

#[test]
fn an_entry_kind_the_field_does_not_carry_is_a_typed_rejection() {
    let k = key8_str("hand-key");
    let part = part_hash(b"hand");
    let limits = KeyIndexLeafLimits::default();
    let kind1_frame = frame(1, &raw_posting(k, &[3, 2]));
    let kind0_frame = frame(1, &raw_entry(k, 2, &[4]));
    let mismatch = |expected: u32, actual: u32| KeyIndexLeafRejection::EntryKindMismatch {
        expected,
        actual,
    };
    assert_eq!(
        mismatch(1, 0).to_string(),
        "leaf header entry_kind is 0, expected 1"
    );

    // A series_label leaf of entry kind 0.
    let mut header = posting_header(1);
    header.entry_kind = 0;
    let leaf = assemble(header, &single_bucket(k, kind0_frame.clone()), None);
    assert_eq!(
        posting_probe(&leaf.0, &leaf.1, &posting_request(&part), &k),
        Err(mismatch(1, 0))
    );
    assert_eq!(
        verify_key_index_leaf(&leaf.0, &limits).map(|_| ()),
        Err(mismatch(1, 0))
    );

    // A class_objects leaf (kind 0 at version 3) whose header says postings.
    let mut header = posting_header(1);
    header.field = METRICS_CLASS_OBJECTS_FIELD.into();
    header.key_type = u32::from(KeyType::I64.as_u8());
    let leaf = assemble(header, &single_bucket(k, kind1_frame.clone()), None);
    let req = KeyIndexLeafRequest {
        field: METRICS_CLASS_OBJECTS_FIELD,
        key_type: KeyType::I64,
        ..posting_request(&part)
    };
    let (got, _) = probe_with(&leaf.0, &leaf.1, &req, &k, &limits);
    assert_eq!(
        got.expect("no breach"),
        KeyIndexLeafProbe::Rejected(mismatch(0, 1))
    );
    assert_eq!(
        verify_key_index_leaf(&leaf.0, &limits).map(|_| ()),
        Err(mismatch(0, 1))
    );

    // Kind 1 below version 3 is corrupted whatever the field; at version 3
    // a logs leaf is refused by the version gate first.
    let mut logs = base_header(1);
    logs.entry_kind = 1;
    let leaf = assemble(logs.clone(), &single_bucket(k, kind1_frame.clone()), None);
    expect_rejected(&leaf, &k, &limits, "entry_kind 1 at format_version 2");
    let mut metrics_v2 = posting_header(1);
    metrics_v2.format_version = KEY_INDEX_LEAF_FORMAT_VERSION;
    let leaf = assemble(metrics_v2, &single_bucket(k, kind1_frame.clone()), None);
    assert!(matches!(
        posting_probe(&leaf.0, &leaf.1, &posting_request(&part), &k),
        Err(KeyIndexLeafRejection::Corrupted(m)) if m.contains("entry_kind 1 at format_version 2")
    ));
    logs.format_version = KEY_INDEX_LEAF_FORMAT_VERSION_3;
    let leaf = assemble(logs, &single_bucket(k, kind1_frame.clone()), None);
    let (got, _) = probe_with(&leaf.0, &leaf.1, &request(&part), &k, &limits);
    assert_eq!(
        got.expect("no breach"),
        KeyIndexLeafProbe::Rejected(KeyIndexLeafRejection::UnsupportedVersion(3))
    );

    // An entry kind no version defines.
    let mut header = posting_header(1);
    header.entry_kind = 2;
    let leaf = assemble(header, &single_bucket(k, kind1_frame), None);
    assert!(matches!(
        posting_probe(&leaf.0, &leaf.1, &posting_request(&part), &k),
        Err(KeyIndexLeafRejection::Corrupted(m)) if m.contains("unknown entry_kind 2")
    ));

    // Every kind-1 decoder refuses a kind-0 leaf.
    let leaf = assemble(base_header(1), &single_bucket(k, kind0_frame), None);
    let req = request(&part);
    let prefix_len = leaf.1.prefix_len as usize;
    let prefix = KeyIndexLeafPrefix::parse(&leaf.0[..prefix_len], &leaf.1, &req).expect("parses");
    assert_eq!(prefix.entry_kind(), KeyIndexEntryKind::Entry);
    let located = prefix.locate(&k).expect("in slice").expect("non-empty");
    let frame_bytes = &leaf.0[located.range.start as usize..located.range.end as usize];
    assert_eq!(
        prefix.probe_posting_bucket(&located, frame_bytes, &k, &limits),
        Err(mismatch(1, 0))
    );
    assert_eq!(
        verify_key_index_posting_leaf_frames(&leaf.0, &limits, |_| {}).map(|_| ()),
        Err(mismatch(1, 0))
    );
    let plan = plan_key_index_leaf_set(&leaf.0[..prefix_len], &leaf.1, &req, &[k], 4).expect("plan");
    let run = plan.runs()[0].clone();
    assert_eq!(
        plan.posting_decoder::<()>().decode_next_run(
            &leaf.0[run.start as usize..run.end as usize],
            &limits,
            &mut |_| Some(()),
        ),
        Err(KeyIndexSetRunError::Rejected(mismatch(1, 0)))
    );
}

#[test]
fn version_three_reads_for_metrics_only_and_older_leaves_still_read() {
    let f = fixture();
    let expected = KeyIndexLeafProbe::Matches {
        matches: matches(&[(1, &[0, 3, 7]), (4, &[2, 5])]),
        uncovered_entry_ordinals: UNCOVERED.to_vec(),
    };
    for signal in [Signal::Logs, Signal::Spans, Signal::Metrics] {
        let field = if signal == Signal::Metrics {
            METRICS_CLASS_OBJECTS_FIELD
        } else {
            FIELD
        };
        let leaf = encode_key_index_leaf_with_bucket_bits(
            &KeyIndexLeafInput {
                signal,
                field,
                ..input(f.part, &f.entries, &UNCOVERED)
            },
            FIXTURE_BUCKET_BITS,
            &KeyIndexLeafLimits::default(),
        )
        .expect("encode");
        let req = KeyIndexLeafRequest {
            signal,
            field,
            ..request(&f.part)
        };
        let v2 = verify_key_index_leaf(&leaf.bytes, &KeyIndexLeafLimits::default()).expect("v2");
        assert_eq!(v2.header.format_version, KEY_INDEX_LEAF_FORMAT_VERSION);
        assert_eq!(v2.entry_kind, KeyIndexEntryKind::Entry);
        let (got, _) = probe(&leaf.bytes, &leaf.leaf_ref, &req, &f.alpha);
        assert_eq!(got, expected, "{signal:?} version 2");

        let v3 = restamp_key_index_leaf_version(&leaf.bytes, 3).expect("restamp");
        let (got, _) = probe(&v3.bytes, &v3.leaf_ref, &req, &f.alpha);
        if signal == Signal::Metrics {
            assert_eq!(got, expected, "a version-3 kind-0 leaf reads as version 2");
            let summary =
                verify_key_index_leaf(&v3.bytes, &KeyIndexLeafLimits::default()).expect("v3");
            assert_eq!(summary.header.format_version, 3);
            assert_eq!(summary.entries, v2.entries);
        } else {
            assert_eq!(
                got,
                KeyIndexLeafProbe::Rejected(KeyIndexLeafRejection::UnsupportedVersion(3)),
                "{signal:?} version 3"
            );
        }

        let v1 = restamp_key_index_leaf_version(&leaf.bytes, 1).expect("restamp");
        let (got, _) = probe(&v1.bytes, &v1.leaf_ref, &req, &f.alpha);
        if signal == Signal::Logs {
            assert_eq!(
                got,
                KeyIndexLeafProbe::Rejected(KeyIndexLeafRejection::UnsupportedVersion(1))
            );
        } else {
            assert_eq!(got, expected, "{signal:?} version 1");
        }
    }
}

/// Heap one kind-1 decode of `frame` holds at its peak, measured from the
/// decoded value's own capacities: the raw buffer beside the postings, or
/// the postings beside the values a probe copies out of them.
fn measured_posting_decode_peak(prefix: &KeyIndexLeafPrefix, bucket: u32, frame: &[u8]) -> u64 {
    let decoded = prefix
        .decode_posting_frame(bucket, frame, &KeyIndexLeafLimits::default())
        .expect("decodes");
    let postings = (decoded.postings.capacity() * std::mem::size_of::<KeyIndexPostingEntry>())
        as u64
        + decoded
            .postings
            .iter()
            .map(|p| (p.values.capacity() * std::mem::size_of::<u64>()) as u64)
            .sum::<u64>();
    let raw = u64::from(decoded.uncompressed_len);
    (raw + postings).max(2 * postings)
}

#[test]
fn posting_decoded_bound_dominates_the_adversarial_frame() {
    assert_eq!(std::mem::size_of::<KeyIndexPostingEntry>(), 32);
    let n = 100usize;
    let keys = keys_in_bucket(0, n);
    let part = part_hash(b"hand");
    let req = posting_request(&part);

    // Most postings per byte: n keys of one one-byte delta, 10 bytes apiece.
    let many: Vec<u8> = keys.iter().flat_map(|k| raw_posting(*k, &[1])).collect();
    assert_eq!(many.len(), 10 * n);
    // Most values per byte: one posting of all one-byte deltas.
    let wide = raw_posting(keys[0], &[1; 4_000]);

    let mut peaks = Vec::new();
    for (raw, count, allocations) in [
        (&many, n as u32, vec![1; n]),
        (&wide, 1, vec![4_000]),
    ] {
        let leaf = hand(keys[0], count, raw);
        let prefix_len = leaf.1.prefix_len as usize;
        let prefix =
            KeyIndexLeafPrefix::parse(&leaf.0[..prefix_len], &leaf.1, &req).expect("parses");
        let len = raw.len() as u32;
        clear_posting_allocations();
        let peak = measured_posting_decode_peak(&prefix, 0, &leaf.0[prefix_len..]);
        assert_eq!(
            posting_allocations(),
            allocations,
            "each list is allocated at exactly its count"
        );
        assert!(
            peak <= posting_decoded_bound(len),
            "{count}-posting frame of {len} bytes peaks at {peak}, bound {}",
            posting_decoded_bound(len)
        );
        peaks.push((len, peak));
    }
    // The kind-0 bound does not cover the wide frame: 32 + 8 * 4000 bytes of
    // postings, twice over once a probe copies them, against 8 * 4010 + 64.
    let (wide_len, wide_peak) = peaks[1];
    assert_eq!(wide_peak, 2 * (32 + 8 * 4_000));
    assert!(
        wide_peak > decoded_bound(wide_len),
        "the kind-0 bound {} covers {wide_peak}",
        decoded_bound(wide_len)
    );

    // A hostile header claiming `c` postings makes `with_capacity` allocate
    // 32c before one decodes, and every posting spends at least 9 bytes
    // before its deltas, so the value lists hold at most `len - 9c` one-byte
    // deltas. That heap beside the raw buffer, or twice it once values are
    // copied out, stays under the bound.
    for len in [10u32, 11, 4_096, 1 << 20, u32::MAX] {
        let len64 = u64::from(len);
        for claimed in [1, u64::from(len / MIN_FRAME_ENTRY_WIRE_BYTES)] {
            let postings = 32 * claimed + 8 * (len64 - 9 * claimed);
            assert!(
                (len64 + postings).max(2 * postings) <= posting_decoded_bound(len),
                "{len} bytes claiming {claimed}"
            );
        }
    }

    // That split refused: one posting spending the bytes the claimed others
    // would need is corrupted before its list is allocated.
    clear_posting_allocations();
    let hostile = hand(keys[0], 100, &raw_posting(keys[0], &[1; 990]));
    expect_posting_rejected(&hostile, &keys[0], "with 99 postings still to come");
    assert_eq!(posting_allocations(), Vec::<usize>::new());
}

#[test]
fn posting_count_is_checked_before_its_list_is_allocated() {
    let keys = keys_in_bucket(0, 2);
    let (a, b) = (keys[0], keys[1]);
    let part = part_hash(b"hand");

    // The honest frame decodes, so the recorder is live on this path.
    clear_posting_allocations();
    let honest = hand(
        a,
        2,
        &[raw_posting(a, &[1; 6]), raw_posting(b, &[1])].concat(),
    );
    assert_eq!(
        posting_probe(&honest.0, &honest.1, &posting_request(&part), &a),
        Ok(Some(vec![0, 1, 2, 3, 4, 5]))
    );
    assert_eq!(posting_allocations(), vec![6, 1]);

    for (raw, count, needle) in [
        (
            raw_posting_claiming(a, 1_000_000, &[1; 5]),
            1,
            "posting count 1000000 exceeds",
        ),
        (
            raw_posting_claiming(a, 6, &[1; 5]),
            1,
            "posting count 6 exceeds",
        ),
        (
            [raw_posting_claiming(a, 7, &[1; 6]), raw_posting(b, &[1])].concat(),
            2,
            "with 1 postings still to come",
        ),
        (raw_posting_claiming(a, 0, &[1]), 1, "zero-length posting"),
    ] {
        clear_posting_allocations();
        expect_posting_rejected(&hand(a, count, &raw), &a, needle);
        assert_eq!(
            posting_allocations(),
            Vec::<usize>::new(),
            "{needle}: a list was allocated before its count was checked"
        );
    }
}

fn postings_strategy() -> impl Strategy<Value = Vec<KeyIndexPostingEntry>> {
    proptest::collection::btree_map(
        any::<u64>(),
        proptest::collection::btree_set(prop_oneof![0u64..64, any::<u64>()], 1..24),
        0..40,
    )
    .prop_map(|by_key| {
        by_key
            .into_iter()
            .map(|(key, values)| KeyIndexPostingEntry {
                key8: key.to_be_bytes(),
                values: values.into_iter().collect(),
            })
            .collect::<Vec<_>>()
    })
    .prop_filter("no posting starts at u64::MAX", |postings| {
        postings.iter().all(|p| p.values[0] != u64::MAX)
    })
}

fn posting_fuzz_fixture() -> &'static ([u8; 32], EncodedKeyIndexLeaf) {
    static FIXTURE: std::sync::OnceLock<([u8; 32], EncodedKeyIndexLeaf)> =
        std::sync::OnceLock::new();
    FIXTURE.get_or_init(|| {
        let part = part_hash(b"posting part");
        let leaf = encode_key_index_posting_leaf_with_bucket_bits(
            &posting_input(part, &posting_fixture(), &UNCOVERED),
            3,
            &KeyIndexLeafLimits::default(),
        )
        .expect("encode");
        (part, leaf)
    })
}

/// A frame body built from posting-shaped pieces with arbitrary counts and
/// deltas, then arbitrary trailing bytes.
fn posting_shaped_raw() -> impl Strategy<Value = Vec<u8>> {
    (
        proptest::collection::vec(
            (
                any::<u64>(),
                0u64..12,
                proptest::collection::vec(prop_oneof![0u64..3, any::<u64>()], 0..10),
            ),
            0..6,
        ),
        proptest::collection::vec(any::<u8>(), 0..4),
    )
        .prop_map(|(pieces, tail)| {
            let mut raw = Vec::new();
            for (key, count, deltas) in pieces {
                raw.extend(raw_posting_claiming(key.to_be_bytes(), count, &deltas));
            }
            raw.extend(tail);
            raw
        })
}

proptest! {
    #[test]
    fn posting_leaf_round_trips(
        postings in postings_strategy(),
        bucket_bits in 1u8..=6,
        probe_key in any::<u64>(),
    ) {
        let part = part_hash(b"posting part");
        let limits = KeyIndexLeafLimits::default();
        let leaf = encode_key_index_posting_leaf_with_bucket_bits(
            &posting_input(part, &postings, &UNCOVERED),
            bucket_bits,
            &limits,
        )
        .expect("encodes");
        let summary = verify_key_index_leaf(&leaf.bytes, &limits).expect("verifies");
        prop_assert_eq!(
            summary.body_uncompressed_len,
            postings.iter().map(encoded_posting_len).sum::<u64>()
        );
        let mut got = summary.postings;
        got.sort_by_key(|p| p.key8);
        prop_assert_eq!(&got, &postings);
        let req = posting_request(&part);
        for p in &postings {
            prop_assert_eq!(
                posting_probe(&leaf.bytes, &leaf.leaf_ref, &req, &p.key8),
                Ok(Some(p.values.clone()))
            );
        }
        let probe_key8 = probe_key.to_be_bytes();
        let want = postings
            .iter()
            .find(|p| p.key8 == probe_key8)
            .map(|p| p.values.clone());
        prop_assert_eq!(
            posting_probe(&leaf.bytes, &leaf.leaf_ref, &req, &probe_key8),
            Ok(want)
        );
    }

    #[test]
    fn mutated_posting_leaf_is_rejected_or_answers_unchanged(mutation in mutation_strategy()) {
        let (part, leaf) = posting_fuzz_fixture();
        let mutated = mutate(&leaf.bytes, &mutation);
        let limits = KeyIndexLeafLimits::default();
        let req = posting_request(part);
        let postings = posting_fixture();
        let absent = series_label_pair_key8("job", "absent").expect("key8");
        for key8 in postings.iter().step_by(7).map(|p| p.key8).chain([absent]) {
            let truth = posting_probe(&leaf.bytes, &leaf.leaf_ref, &req, &key8);
            if let Ok(answer) = posting_probe(&mutated, &leaf.leaf_ref, &req, &key8) {
                prop_assert_eq!(Ok(answer), truth);
            }
        }
        if let Ok(summary) = verify_key_index_leaf(&mutated, &limits) {
            let mut got = summary.postings;
            got.sort_by_key(|p| p.key8);
            prop_assert_eq!(got, postings);
        }
    }

    #[test]
    fn arbitrary_posting_frames_are_typed_and_allocate_within_their_bytes(
        raw in prop_oneof![posting_shaped_raw(), proptest::collection::vec(any::<u8>(), 0..256)],
        claimed in 0u32..8,
        key in any::<u64>(),
    ) {
        let k = key.to_be_bytes();
        let leaf = hand(k, claimed, &raw);
        let part = part_hash(b"hand");
        clear_posting_allocations();
        let probed = posting_probe(&leaf.0, &leaf.1, &posting_request(&part), &k);
        let probe_allocated: usize = posting_allocations().iter().sum();
        prop_assert!(probe_allocated <= raw.len(), "{probe_allocated} values from {} bytes", raw.len());
        clear_posting_allocations();
        let verified = verify_key_index_leaf(&leaf.0, &KeyIndexLeafLimits::default());
        let verify_allocated: usize = posting_allocations().iter().sum();
        prop_assert!(verify_allocated <= raw.len(), "{verify_allocated} values from {} bytes", raw.len());
        if let Ok(summary) = verified {
            let want = summary.postings.iter().find(|p| p.key8 == k).map(|p| p.values.clone());
            prop_assert_eq!(probed, Ok(want));
            for p in &summary.postings {
                prop_assert!(!p.values.is_empty());
                prop_assert!(p.values.windows(2).all(|w| w[0] < w[1]));
            }
        }
    }
}
