//! RLOG v3 NumStat agreement (ADR-0095), plus hostile SKIP_IDX inputs at v3.
//!
//! The claim under test is the one range pruning will rest on: a block's
//! numeric stat for a `(name, type)` column bounds exactly the values a reader
//! materializing that typed column produces for the rows of that block, and
//! counts every other row as a null. Two shapes make that non-trivial. When a
//! record carries one attribute name more than once -- two types, or a
//! same-type duplicate that spilled into `attrs_raw` -- the reader resolves one
//! winning occurrence for the name, and the losing occurrences (which still sit
//! in the value pages) must not widen the bounds. When a record carries the
//! name nowhere on itself but its resource or scope does, the reader resolves
//! that stream-level value, and the bounds must cover it even though no value
//! page of the block holds it.
//!
//! The expected value is never recomputed here from the writer's own rule. The
//! record layer is read back off the object through `RlogReader::scan`, whose
//! rebuilt records list a row's columnar attributes in FIELD_DIR order followed
//! by its `attrs_raw` overflow, then folded last-wins by name exactly as
//! `ravel_sql::rlog_attrs::merged_attrs` folds the same list for the SQL
//! `attrs` column. The stream layer comes from the same generated description
//! the written blob was built from (this crate does not expose a stream-blob
//! decoder), folded under it in `merged_attrs` order. So a writer that resolved
//! a value wrongly disagrees with what the read side reports and fails these
//! tests.
#![allow(clippy::expect_used)]

use std::collections::BTreeMap;

use proptest::prelude::*;
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{self, kind};
use ravel_logseg::record::FieldType;
use ravel_logseg::skip_index::SkipIndex;
use ravel_logseg::{
    AttrValue, LogRecord, LogStreamId, ObjectIdentity, Predicate, RlogConfig, RlogReader,
    RlogWriter, read_section, stream_attrs_bytes,
};

/// Attribute names the generator reuses, so duplicate keys (same name, mixed
/// types) are the common case rather than a rare coincidence.
const NAMES: &[&str] = &["dur", "code", "flag"];

fn sid(n: u8) -> LogStreamId {
    let mut a = [0u8; 16];
    a[0] = n;
    LogStreamId(a)
}

/// Where a generated stream-level attribute sits, per stream. `None` means the
/// stream carries only `service.name` on its resource.
///
/// A stream's blob has to be byte-identical across its records (the writer
/// refuses an object where it is not), so this is fixed per stream index, not
/// per record.
type StreamSpec = Option<(String, AttrValue, bool)>;

/// The generator's three streams, each optionally carrying one of the tracked
/// `NAMES` at resource or scope level. Without this a tracked name could only
/// ever occur per-record, and the stream-only-occurrence case -- the ordinary
/// OTLP shape, and the one that under-bounded a stat before ADR-0095's
/// corrected decision 1 -- would never be generated.
fn arb_stream_spec() -> impl Strategy<Value = StreamSpec> {
    proptest::option::of((
        proptest::sample::select(NAMES).prop_map(str::to_string),
        arb_value(),
        any::<bool>(),
    ))
}

fn arb_stream_specs() -> impl Strategy<Value = [StreamSpec; 3]> {
    (arb_stream_spec(), arb_stream_spec(), arb_stream_spec()).prop_map(|(a, b, c)| [a, b, c])
}

/// One stream's resource set and scope set.
type StreamLayers = (Vec<(String, AttrValue)>, Vec<(String, AttrValue)>);

/// The resource set and the scope set of stream `n`. One source for both the
/// blob the writer is handed and the pairs the oracle folds, so the two cannot
/// describe different streams.
fn stream_layers(n: u8, specs: &[StreamSpec; 3]) -> StreamLayers {
    let mut resource = vec![("service.name".to_string(), AttrValue::Str(format!("s{n}")))];
    let mut scope: Vec<(String, AttrValue)> = Vec::new();
    if let Some((name, value, at_scope)) = &specs[n as usize % 3] {
        if *at_scope {
            scope.push((name.clone(), value.clone()));
        } else {
            resource.push((name.clone(), value.clone()));
        }
    }
    (resource, scope)
}

/// The stream-level `(name, value)` pairs of stream `n` in the order
/// `ravel_sql::rlog_attrs::merged_attrs` sees them: the resource set, then the
/// scope set. The canonical encoding sorts each set by key bytes, which this
/// does not reproduce and does not need to: no name occurs twice within one
/// set here, so the first occurrence of a name across the two sets -- all
/// `find_attr` and the oracle below read -- is the same either way.
fn stream_pairs(n: u8, specs: &[StreamSpec; 3]) -> Vec<(String, AttrValue)> {
    let (resource, scope) = stream_layers(n, specs);
    [resource, scope].concat()
}

fn stream_blob(n: u8, specs: &[StreamSpec; 3]) -> Vec<u8> {
    let (resource, scope) = stream_layers(n, specs);
    stream_attrs_bytes(&resource, "scope", "1", &scope)
}

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [3u8; 16],
        shard: 1,
        writer_id: [4u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// Values across every scalar type, with the f64 edge cases (NaN payload,
/// -0.0) the stat code treats specially.
fn arb_value() -> impl Strategy<Value = AttrValue> {
    prop_oneof![
        any::<i64>().prop_map(AttrValue::I64),
        any::<f64>().prop_map(AttrValue::F64),
        Just(AttrValue::F64(-0.0)),
        Just(AttrValue::F64(f64::from_bits(f64::NAN.to_bits() | 0x5))),
        any::<bool>().prop_map(AttrValue::Bool),
        proptest::sample::select(NAMES).prop_map(|s| AttrValue::Str(s.to_string())),
        proptest::collection::vec(any::<u8>(), 0..4).prop_map(AttrValue::Bytes),
    ]
}

/// 0..5 attributes drawn from three names, so a record commonly carries one
/// name two or three times over different types.
fn arb_attrs() -> impl Strategy<Value = Vec<(String, AttrValue)>> {
    proptest::collection::vec((proptest::sample::select(NAMES), arb_value()), 0..5)
        .prop_map(|v| v.into_iter().map(|(n, val)| (n.to_string(), val)).collect())
}

fn arb_corpus() -> impl Strategy<Value = Vec<Vec<(String, AttrValue)>>> {
    proptest::collection::vec(arb_attrs(), 1..12)
}

/// One record per generated attribute set, each with a unique `ts_ns` (its
/// index), spread over the three streams `specs` describes.
/// `block_target_records` is the caller's: 1 puts one record per block (a
/// level-0 entry's `min_ts` then names exactly the record whose winners its
/// stats must describe), a large value puts them all in one block (where a
/// stat has to bound several records' winners at once).
fn write_object(
    corpus: &[Vec<(String, AttrValue)>],
    specs: &[StreamSpec; 3],
    block_target_records: usize,
) -> Vec<u8> {
    let cfg = RlogConfig {
        block_target_records,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(cfg, identity());
    for (i, attrs) in corpus.iter().enumerate() {
        let stream = (i % 3) as u8;
        w.push(LogRecord {
            stream_id: sid(stream),
            stream_attrs: stream_blob(stream, specs),
            ts_ns: i as i64,
            observed_ts_ns: i as i64,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: "b".to_string(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: attrs.clone(),
        })
        .expect("push");
    }
    w.finish().expect("finish")
}

fn read_field_dir(obj: &[u8]) -> FieldDir {
    let cfg = RlogConfig::default();
    let f = footer::open(obj).expect("open");
    let raw = read_section(obj, f.section(kind::FIELD_DIR).expect("field_dir"), &cfg)
        .expect("read field_dir");
    FieldDir::decode(&raw, 1 << 20).expect("decode field_dir")
}

fn read_skip_index(obj: &[u8]) -> SkipIndex {
    let cfg = RlogConfig::default();
    let f = footer::open(obj).expect("open");
    let raw =
        read_section(obj, f.section(kind::SKIP_IDX).expect("skip_idx"), &cfg).expect("read skip");
    SkipIndex::decode(&raw, 1 << 20).expect("decode skip")
}

/// The merged-view value per attribute name a reader resolves for one rebuilt
/// record, which is `find_attr` over `merged_attrs`: the stream layer
/// (`stream`, resource pairs then scope pairs) seeds it and the *first*
/// occurrence of a name there is the one that stands, then the record layer
/// overrides -- `rec.attrs` in the order `rebuild_record` emits it (columnar
/// attributes in FIELD_DIR order, then `attrs_raw`), last occurrence winning,
/// exactly `merged_attrs`'s last-wins fold over the record's own list.
///
/// A name the record does not carry at all keeps its stream-level value here
/// rather than dropping out. That is the whole point: a reader reports that
/// value, so a stat over the name has to bound it.
fn winners(rec: &LogRecord, stream: &[(String, AttrValue)]) -> BTreeMap<String, AttrValue> {
    let mut out = BTreeMap::new();
    for (k, v) in stream {
        out.entry(k.clone()).or_insert_with(|| v.clone());
    }
    for (k, v) in &rec.attrs {
        out.insert(k.clone(), v.clone());
    }
    out
}

/// The stream `n` a rebuilt record came from, as the oracle's stream pairs.
/// Keyed off the record's own `stream_id` (a faithful round-trip of what the
/// writer was handed), not its position in the scan.
fn stream_of(rec: &LogRecord, specs: &[StreamSpec; 3]) -> Vec<(String, AttrValue)> {
    stream_pairs(rec.stream_id.0[0], specs)
}

/// The bits a NumStat stores for a winner of the stat's own type, or `None`
/// when the winner is of another type and the row is a null contribution.
fn winner_bits(ty: FieldType, v: &AttrValue) -> Option<u64> {
    match (ty, v) {
        (FieldType::I64, AttrValue::I64(x)) => Some(*x as u64),
        (FieldType::F64, AttrValue::F64(f)) => Some(f.to_bits()),
        (FieldType::Bool, AttrValue::Bool(b)) => Some(u64::from(*b)),
        _ => None,
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 200, ..ProptestConfig::default() })]

    /// Every level-0 numeric stat describes exactly its block's single record's
    /// merged-view winner for that column's name: the winner's bits when the
    /// types match (NaN excluded from min/max and flagged), and a null
    /// otherwise. And in the soundness direction: a record whose winner for a
    /// name *is* of a numeric column's type always has a stat for that column
    /// in its own block, bounding it -- never a missing or disjoint stat, which
    /// is what would let a range prune drop the block holding the match.
    #[test]
    fn numstat_bounds_the_merged_winner_per_block(
        corpus in arb_corpus(),
        specs in arb_stream_specs(),
    ) {
        let obj = write_object(&corpus, &specs, 1);
        let fd = read_field_dir(&obj);
        let skip = read_skip_index(&obj);

        let cfg = RlogConfig::default();
        let reader = RlogReader::new(&obj, &cfg).expect("open reader");
        let (rebuilt, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        prop_assert_eq!(rebuilt.len(), corpus.len());
        prop_assert_eq!(skip.l0.len(), corpus.len(), "one record per block");

        // column_id -> (name, type), for reading a stat back as an attribute.
        let col: BTreeMap<u32, (String, FieldType)> = fd
            .entries()
            .iter()
            .map(|e| (e.column_id, (e.name.clone(), e.ty)))
            .collect();

        for entry in &skip.l0 {
            prop_assert_eq!(entry.record_count, 1);
            let rec = rebuilt
                .iter()
                .find(|r| r.ts_ns == entry.min_ts)
                .expect("every block's ts names a scanned record");
            let win = winners(rec, &stream_of(rec, &specs));

            for stat in &entry.stats {
                let (name, ty) = col.get(&stat.column_id).expect("stat names a real column");
                prop_assert_eq!(*ty, stat.ty, "stat type matches its column's type");
                match win.get(name).and_then(|v| winner_bits(*ty, v)) {
                    Some(bits) => {
                        let is_nan =
                            *ty == FieldType::F64 && f64::from_bits(bits).is_nan();
                        if is_nan {
                            prop_assert!(stat.has_nan, "a NaN winner sets has_nan");
                            prop_assert_eq!(stat.null_count, 0);
                        } else {
                            prop_assert_eq!(stat.min_bits, bits, "min is the winner");
                            prop_assert_eq!(stat.max_bits, bits, "max is the winner");
                            prop_assert_eq!(
                                stat.null_count, 0,
                                "a contributing row is not a null"
                            );
                        }
                    }
                    // The row's winner for this name is of another type (or the
                    // row carries no such name): a losing occupant of the value
                    // page contributes nothing but a null.
                    None => prop_assert_eq!(
                        stat.null_count, 1,
                        "a cross-type loser counts only as a null"
                    ),
                }
            }

            // Soundness: no resolved value of a numeric type is bounded out of
            // its own block. A stat for the column must contain it.
            for (name, value) in &win {
                for ty in [FieldType::I64, FieldType::F64, FieldType::Bool] {
                    let Some(bits) = winner_bits(ty, value) else {
                        continue;
                    };
                    let Some(cid) = fd.column(name, ty).map(|e| e.column_id) else {
                        continue;
                    };
                    let stat = entry.stats.iter().find(|s| s.column_id == cid);
                    let Some(stat) = stat else {
                        // No stat at all for the column in this block. Legal
                        // only when the block has no page for it, which here
                        // means the record resolved the name off its stream and
                        // carries no occurrence of its own: `write_block` writes
                        // a stat per column the block actually carries, and an
                        // absent stat is "no information" -- it prunes nothing
                        // (docs/log-segment-format.md, SKIP_IDX). A record-level
                        // occurrence of this (name, type) always takes the
                        // column, so it always has a stat.
                        prop_assert!(
                            !rec.attrs.iter().any(|(k, _)| k == name),
                            "a record-level occurrence must have a stat in its block"
                        );
                        continue;
                    };
                    if ty == FieldType::F64 && f64::from_bits(bits).is_nan() {
                        prop_assert!(stat.has_nan);
                    } else {
                        prop_assert_eq!(stat.min_bits, bits);
                        prop_assert_eq!(stat.max_bits, bits);
                    }
                }
            }
        }
    }

    /// The same claim where a block holds *several* records: each numeric stat
    /// is exactly the aggregate of its block's records' resolved values for the
    /// column's name -- min/max over the contributing ones, a null per
    /// non-contributing one, `has_nan` for a NaN.
    ///
    /// This is the shape the per-block test above cannot reach. With one record
    /// per block, a record that resolves a name off its stream sits in a block
    /// with no page for that name's column and so no stat to be wrong about.
    /// Mixed into a block with a record that does carry the name per-record,
    /// the stat exists and has to cover both, which is what a writer that
    /// resolved stats from the record layer alone got wrong (it excluded the
    /// stream-resolved value and counted the record as a null instead, so a
    /// range prune could drop the block holding the match).
    #[test]
    fn numstat_bounds_every_merged_winner_in_a_shared_block(
        corpus in arb_corpus(),
        specs in arb_stream_specs(),
    ) {
        let obj = write_object(&corpus, &specs, 1 << 20);
        let fd = read_field_dir(&obj);
        let skip = read_skip_index(&obj);
        prop_assert_eq!(skip.l0.len(), 1, "every record in one block");

        let cfg = RlogConfig::default();
        let reader = RlogReader::new(&obj, &cfg).expect("open reader");
        let (rebuilt, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        prop_assert_eq!(rebuilt.len(), corpus.len());

        let resolved: Vec<BTreeMap<String, AttrValue>> = rebuilt
            .iter()
            .map(|r| winners(r, &stream_of(r, &specs)))
            .collect();

        let col: BTreeMap<u32, (String, FieldType)> = fd
            .entries()
            .iter()
            .map(|e| (e.column_id, (e.name.clone(), e.ty)))
            .collect();

        let entry = &skip.l0[0];
        prop_assert_eq!(entry.record_count as usize, corpus.len());

        for stat in &entry.stats {
            let (name, ty) = col.get(&stat.column_id).expect("stat names a real column");
            prop_assert_eq!(*ty, stat.ty);

            // The oracle's own fold, over every record of the block.
            let mut nulls = 0u32;
            let mut has_nan = false;
            let mut vals: Vec<u64> = Vec::new();
            for win in &resolved {
                match win.get(name).and_then(|v| winner_bits(*ty, v)) {
                    Some(bits) if *ty == FieldType::F64 && f64::from_bits(bits).is_nan() => {
                        has_nan = true;
                    }
                    Some(bits) => vals.push(bits),
                    None => nulls += 1,
                }
            }
            prop_assert_eq!(stat.null_count, nulls, "null count for {}", name);
            prop_assert_eq!(stat.has_nan, has_nan, "has_nan for {}", name);
            if vals.is_empty() {
                prop_assert_eq!(stat.min_bits, 0);
                prop_assert_eq!(stat.max_bits, 0);
                continue;
            }
            // i64 compares as i64, f64 by `total_cmp`, bool as its 0/1 bit.
            let (min, max) = match ty {
                FieldType::I64 => {
                    let mut v: Vec<i64> = vals.iter().map(|b| *b as i64).collect();
                    v.sort_unstable();
                    (v[0] as u64, v[v.len() - 1] as u64)
                }
                FieldType::F64 => {
                    let mut v: Vec<f64> = vals.iter().map(|b| f64::from_bits(*b)).collect();
                    v.sort_by(f64::total_cmp);
                    (v[0].to_bits(), v[v.len() - 1].to_bits())
                }
                _ => {
                    let mut v = vals.clone();
                    v.sort_unstable();
                    (v[0], v[v.len() - 1])
                }
            };
            prop_assert_eq!(stat.min_bits, min, "min for {}", name);
            prop_assert_eq!(stat.max_bits, max, "max for {}", name);
        }

        // And no resolved numeric value is bounded out of the block: every one
        // whose column exists and whose column the block carries a stat for
        // sits inside that stat's range.
        for win in &resolved {
            for (name, value) in win {
                for ty in [FieldType::I64, FieldType::F64, FieldType::Bool] {
                    let Some(bits) = winner_bits(ty, value) else {
                        continue;
                    };
                    let Some(cid) = fd.column(name, ty).map(|e| e.column_id) else {
                        continue;
                    };
                    let Some(stat) = entry.stats.iter().find(|s| s.column_id == cid) else {
                        continue;
                    };
                    match ty {
                        FieldType::F64 if f64::from_bits(bits).is_nan() => {
                            prop_assert!(stat.has_nan);
                        }
                        FieldType::F64 => {
                            let f = f64::from_bits(bits);
                            prop_assert!(
                                f64::from_bits(stat.min_bits).total_cmp(&f).is_le()
                                    && f64::from_bits(stat.max_bits).total_cmp(&f).is_ge(),
                                "{} = {:?} outside [{:?}, {:?}]",
                                name,
                                f,
                                f64::from_bits(stat.min_bits),
                                f64::from_bits(stat.max_bits)
                            );
                        }
                        FieldType::I64 => {
                            let v = bits as i64;
                            prop_assert!(
                                (stat.min_bits as i64) <= v && (stat.max_bits as i64) >= v,
                                "{} = {} outside its stat range",
                                name,
                                v
                            );
                        }
                        _ => prop_assert!(stat.min_bits <= bits && stat.max_bits >= bits),
                    }
                }
            }
        }
    }

    /// Flipping any byte inside the SKIP_IDX section of a v3 object always
    /// fails the open with a typed error: the section carries a whole-section
    /// crc, and the skip index carries the block framing, so corruption there
    /// is loud rather than a degrade (docs/log-segment-format.md "Pruning
    /// soundness"). Never a panic, and never a reader that proceeds on
    /// guessed-at block bounds.
    #[test]
    fn corrupt_skip_idx_section_is_a_typed_error(
        corpus in arb_corpus(),
        specs in arb_stream_specs(),
        at in any::<usize>(),
        xor in any::<u8>(),
    ) {
        let obj = write_object(&corpus, &specs, 1);
        let f = footer::open(&obj).expect("open");
        let desc = *f.section(kind::SKIP_IDX).expect("skip_idx");
        let mut bad = obj.clone();
        let i = desc.offset as usize + (at % desc.len as usize);
        bad[i] ^= xor | 1;

        let cfg = RlogConfig::default();
        match RlogReader::new(&bad, &cfg) {
            Ok(_) => prop_assert!(false, "a corrupt SKIP_IDX must not open"),
            Err(ravel_logseg::LogSegError::Corrupted(_)) => {}
            Err(other) => prop_assert!(false, "expected Corrupted, got {:?}", other),
        }
    }

    /// Truncating a v3 object anywhere inside or after its SKIP_IDX section is
    /// a typed error too, never a panic and never a partially decoded index.
    #[test]
    fn truncated_object_at_skip_idx_is_a_typed_error(
        corpus in arb_corpus(),
        specs in arb_stream_specs(),
        at in any::<usize>(),
    ) {
        let obj = write_object(&corpus, &specs, 1);
        let f = footer::open(&obj).expect("open");
        let desc = *f.section(kind::SKIP_IDX).expect("skip_idx");
        let start = desc.offset as usize;
        let cut = start + (at % (obj.len() - start));
        let cfg = RlogConfig::default();
        prop_assert!(RlogReader::new(&obj[..cut], &cfg).is_err());
    }
}

/// The v4 trailer is what the writer emits, and the accepted window is v4 plus
/// the v3 objects the N-1 reader still decodes (ADR-0699 decision 3). Any other
/// version is refused with the typed `UnsupportedVersion`: v2 read support was
/// deleted by ADR-0095 and stays deleted, and a v5 object is fail-closed-on-newer.
/// Exercised on a real written object rather than a hand-built footer.
#[test]
fn writer_emits_v4_and_reader_refuses_outside_the_window() {
    let specs: [StreamSpec; 3] = [None, None, None];
    let obj = write_object(&[vec![("dur".to_string(), AttrValue::I64(5))]], &specs, 1);
    let n = obj.len();
    assert_eq!(
        u16::from_le_bytes([obj[n - 8], obj[n - 7]]),
        4,
        "the writer emits trailer version 4"
    );
    assert_eq!(footer::VERSION, 4);
    assert!(footer::SUPPORTED_VERSIONS.contains(4));
    assert!(
        footer::SUPPORTED_VERSIONS.contains(3),
        "v3 is the N-1 half of the window"
    );
    assert!(
        !footer::SUPPORTED_VERSIONS.contains(2),
        "v2 read support is deleted, not windowed"
    );

    // The version check in open_from_suffix runs before the footer crc is
    // verified, so rewriting just the version field is rejected as
    // UnsupportedVersion specifically, not masked by a stale-crc Corrupted
    // error -- confirmed by asserting the exact variant, not just is_err().
    for bad in [2u16, 5u16] {
        let mut retagged = obj.clone();
        retagged[n - 8..n - 6].copy_from_slice(&bad.to_le_bytes());
        match footer::open(&retagged) {
            Err(ravel_logseg::LogSegError::UnsupportedVersion(v)) if v == bad => {}
            other => panic!("expected UnsupportedVersion({bad}), got {other:?}"),
        }
    }
}

/// The stream-only-occurrence regression, as an explicit two-record case rather
/// than a generated one (ADR-0095 decision 1).
///
/// Record A carries `dur: I64(5)` per-record. Record B carries `dur` nowhere on
/// itself: only its resource has it, as `I64(9999)`. A reader resolves `dur` for
/// B through `merged_attrs` + `find_attr` and gets 9999, so the block's stat for
/// the `(dur, i64)` column has to bound both records: `min = 5`, `max = 9999`,
/// and no null.
///
/// A writer that resolved the stat from the record layer alone reported
/// `min = 5`, `max = 5`, `null_count = 1` here, silently excluding 9999 -- a
/// range query for `dur > 100` would then prune away the block holding the
/// record it wanted.
#[test]
fn stream_only_occurrence_still_bounds_the_stat() {
    // Two streams: B's resource carries `dur`, A's does not. They must be
    // distinct streams -- one stream id may not carry two different blobs.
    let specs: [StreamSpec; 3] = [
        None,
        Some(("dur".to_string(), AttrValue::I64(9999), false)),
        None,
    ];
    let cfg = RlogConfig::default();
    let mut w = RlogWriter::new(cfg, identity());
    for (i, (stream, attrs)) in [
        (0u8, vec![("dur".to_string(), AttrValue::I64(5))]),
        (1u8, Vec::new()),
    ]
    .into_iter()
    .enumerate()
    {
        w.push(LogRecord {
            stream_id: sid(stream),
            stream_attrs: stream_blob(stream, &specs),
            ts_ns: i as i64,
            observed_ts_ns: i as i64,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: "b".to_string(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs,
        })
        .expect("push");
    }
    let obj = w.finish().expect("finish");

    // The reader really does report 9999 for B: the oracle this test asserts
    // against is read off the object, not assumed.
    let reader = RlogReader::new(&obj, &cfg).expect("open reader");
    let (rebuilt, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
    assert_eq!(rebuilt.len(), 2);
    let b = rebuilt
        .iter()
        .find(|r| r.stream_id == sid(1))
        .expect("record B");
    assert!(
        !b.attrs.iter().any(|(k, _)| k == "dur"),
        "record B carries no per-record `dur`"
    );
    assert_eq!(
        winners(b, &stream_of(b, &specs)).get("dur"),
        Some(&AttrValue::I64(9999)),
        "a reader resolves B's `dur` off its resource"
    );

    let fd = read_field_dir(&obj);
    let cid = fd
        .column("dur", FieldType::I64)
        .expect("dur i64 column")
        .column_id;
    let skip = read_skip_index(&obj);
    assert_eq!(skip.l0.len(), 1, "both records land in one block");
    let stat = skip.l0[0]
        .stats
        .iter()
        .find(|s| s.column_id == cid)
        .copied()
        .expect("a stat for the dur i64 column");
    assert_eq!(stat.min_bits as i64, 5, "min is record A's own value");
    assert_eq!(
        stat.max_bits as i64, 9999,
        "max is record B's resource-level value"
    );
    assert_eq!(
        stat.null_count, 0,
        "a record that resolves the name off its stream is not a null"
    );
}

/// The level-1 form of the test above: the same two records, but one per block,
/// so the group summary has to merge a block whose only `dur` value comes off
/// the stream layer.
///
/// Block 0 is record A (stream 0, `dur` as its own `I64(5)`). Block 1 is record
/// B (stream 1, `dur` only on its resource as `I64(9999)`, no per-record
/// occurrence at all). `SkipIndex::merge_stats` folds only the children that
/// carry a stat for a column, so block 1 has to carry one: a block that carries
/// none is read as "no information about this column", and the group summary
/// then bounds the group over block 0 alone -- reporting `max = 5` and
/// `null_count = 0` across two records while silently dropping 9999 and the
/// fact that block 1 held a `dur` value at all. A range query for `dur > 100`
/// pruning on the level-1 entry would drop the whole group before ever probing
/// level 0.
///
/// This is the level-0 plan that decides it: a block plans a NumStat only for
/// the columns some row of it resolves a value for, which has to include the
/// columns resolved through the stream layer, not just the ones with a
/// per-record columnar occurrence.
#[test]
fn l1_group_summary_covers_a_block_whose_value_is_stream_only() {
    let specs: [StreamSpec; 3] = [
        None,
        Some(("dur".to_string(), AttrValue::I64(9999), false)),
        None,
    ];
    // One record per block, so the two records land in two level-0 entries and
    // one level-1 group (FANOUT is 64).
    let cfg = RlogConfig {
        block_target_records: 1,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(cfg, identity());
    for (i, (stream, attrs)) in [
        (0u8, vec![("dur".to_string(), AttrValue::I64(5))]),
        (1u8, Vec::new()),
    ]
    .into_iter()
    .enumerate()
    {
        w.push(LogRecord {
            stream_id: sid(stream),
            stream_attrs: stream_blob(stream, &specs),
            ts_ns: i as i64,
            observed_ts_ns: i as i64,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: "b".to_string(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs,
        })
        .expect("push");
    }
    let obj = w.finish().expect("finish");

    // The oracle is read off the object: a reader really does resolve 9999 for
    // record B, so the group summary really does have to bound it.
    let reader = RlogReader::new(&obj, &cfg).expect("open reader");
    let (rebuilt, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
    assert_eq!(rebuilt.len(), 2);
    let b = rebuilt
        .iter()
        .find(|r| r.stream_id == sid(1))
        .expect("record B");
    assert!(
        !b.attrs.iter().any(|(k, _)| k == "dur"),
        "record B carries no per-record `dur`"
    );
    assert_eq!(
        winners(b, &stream_of(b, &specs)).get("dur"),
        Some(&AttrValue::I64(9999)),
        "a reader resolves B's `dur` off its resource"
    );

    let fd = read_field_dir(&obj);
    let cid = fd
        .column("dur", FieldType::I64)
        .expect("dur i64 column")
        .column_id;
    let skip = read_skip_index(&obj);
    assert_eq!(skip.l0.len(), 2, "one record per block");
    assert_eq!(skip.l1.len(), 1, "both blocks in one level-1 group");

    // The group summary is the claim under test: assert it before its inputs,
    // so a regression reports the wrong bounds a query would prune on rather
    // than the missing level-0 stat that caused them.
    let find =
        |stats: &[ravel_logseg::block::NumStat]| stats.iter().find(|s| s.column_id == cid).copied();
    let group = find(&skip.l1[0].stats).expect("the group has a stat for the dur i64 column");
    assert_eq!(
        (
            group.min_bits as i64,
            group.max_bits as i64,
            group.null_count
        ),
        (5, 9999, 0),
        "the group bounds both records: block 0's own value and block 1's \
         resource-level one, with no null"
    );

    // Its input: block 1 carries no per-record `dur`, and still has to carry a
    // stat for the column, because `merge_stats` folds only children that do.
    let b1 = find(&skip.l0[1].stats).expect("block 1 has a stat for the dur i64 column");
    assert_eq!(b1.min_bits as i64, 9999);
    assert_eq!(b1.max_bits as i64, 9999);
    assert_eq!(b1.null_count, 0);
}

/// A numeric attribute name that lives *only* on the stream layer, on every
/// stream, with no per-record occurrence anywhere in the object, still gets a
/// dynamic column -- so it lands in the NumStat-eligible name set and its
/// resolved values get bounded, the same way an indexed stream-only key already
/// gets a column to key its postings by.
///
/// Without the column there is no column id to key the stat by, so no stat is
/// written for the name anywhere and a range query on the declared column
/// cannot prune at all.
#[test]
fn a_stream_only_numeric_name_still_gets_a_column_and_a_stat() {
    let specs: [StreamSpec; 3] = [
        Some(("dur".to_string(), AttrValue::I64(7), false)),
        Some(("dur".to_string(), AttrValue::I64(11), true)),
        None,
    ];
    let cfg = RlogConfig::default();
    let mut w = RlogWriter::new(cfg, identity());
    for (i, stream) in [0u8, 1u8].into_iter().enumerate() {
        w.push(LogRecord {
            stream_id: sid(stream),
            stream_attrs: stream_blob(stream, &specs),
            ts_ns: i as i64,
            observed_ts_ns: i as i64,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: "b".to_string(),
            trace_id: None,
            span_id: None,
            flags: 0,
            // No record carries `dur` itself, at any point in the object.
            attrs: Vec::new(),
        })
        .expect("push");
    }
    let obj = w.finish().expect("finish");

    let fd = read_field_dir(&obj);
    let cid = fd
        .column("dur", FieldType::I64)
        .expect("a stream-only numeric name still takes a dynamic column")
        .column_id;
    let skip = read_skip_index(&obj);
    let stat = skip
        .l0
        .iter()
        .flat_map(|e| e.stats.iter())
        .find(|s| s.column_id == cid)
        .copied()
        .expect("a stat for the dur i64 column");
    assert_eq!(stat.null_count, 0);
    let (min, max) = skip
        .l0
        .iter()
        .flat_map(|e| e.stats.iter())
        .filter(|s| s.column_id == cid)
        .fold((i64::MAX, i64::MIN), |(lo, hi), s| {
            (lo.min(s.min_bits as i64), hi.max(s.max_bits as i64))
        });
    assert_eq!((min, max), (7, 11), "both streams' resolved values bounded");
}
