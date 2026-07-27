//! Manual stage-timing profiles for the GitHub issue #16 hotspots.
//!
//! `perf` is not available in the fleet sandbox, so this bin attributes
//! cost by re-timing the writer/reader stages in isolation against the
//! same generated workloads the criterion benches use. Internal writer and
//! reader stages are reimplemented inline here (they are private in
//! ravel-segment); the reimplementations mirror the shipped code so the
//! stage timings attribute the cost of the real pipeline.
//!
//! Run: `cargo run -p ravel-bench --release --bin profile_hotspots`
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeSet, HashMap};
use std::time::Instant;

use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{
    LABEL_DICT, SERIES_IDS, SERIES_META, SERIES_TABLE, VAL_PAGES, bench_bounds, bench_identity,
    build_segment, decode_entries, section_bytes,
};
use ravel_segment::{ReaderLimits, SegmentWriter, SeriesInput, decode_pages, plan_ranges, select};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, SeriesId, TenantId};

/// TS_PAGES section kind (docs/segment-format.md); not re-exported by
/// `segment_support`, named once here for the object-assembly bucket.
const TS_PAGES: u32 = 3;

/// FNV-1a, mirroring the writer's private interner hasher.
#[derive(Default)]
struct Fnv(u64);

impl std::hash::Hasher for Fnv {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = if self.0 == 0 {
            0xcbf2_9ce4_8422_2325
        } else {
            self.0
        };
        for &byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        self.0 = hash;
    }
}

fn time<R>(label: &str, mut f: impl FnMut() -> R) -> R {
    let start = Instant::now();
    let out = f();
    println!(
        "  {label:<44} {:>10.2} ms",
        start.elapsed().as_secs_f64() * 1e3
    );
    out
}

fn main() {
    profile_series_id();
    profile_encode();
    profile_encode_v1_vs_v2();
    profile_selective_decode();
}

// ---------------------------------------------------------------- series id

fn buffered_series_id(tenant: &TenantId, metric_name: &str, labels: &LabelSet) -> SeriesId {
    let mut buf = Vec::with_capacity(1024);
    buf.extend_from_slice(b"ravel-series-v1\0");
    let push = |buf: &mut Vec<u8>, s: &str| {
        let len = u16::try_from(s.len()).expect("component fits u16");
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(s.as_bytes());
    };
    push(&mut buf, tenant.as_str());
    push(&mut buf, metric_name);
    let count = labels.iter().filter(|l| l.name != "__name__").count() as u16;
    buf.extend_from_slice(&count.to_le_bytes());
    for label in labels.iter().filter(|l| l.name != "__name__") {
        push(&mut buf, &label.name);
        push(&mut buf, &label.value);
    }
    let digest = blake3::hash(&buf);
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest.as_bytes()[..16]);
    SeriesId(out)
}

fn profile_series_id() {
    println!("== series_id_hash (20k computations) ==");
    let tenant = TenantId::new("bench-tenant");
    for (name, profile) in [
        ("few_big", CardinalityProfile::few_big(20_000)),
        ("many_small", CardinalityProfile::many_small(20_000)),
    ] {
        let config = WorkloadConfig {
            series_count: 20_000,
            samples_per_series: 1,
            cardinality: profile,
            ..Default::default()
        };
        let raw = generate_raw(&config).expect("generate");
        let label_sets: Vec<_> = raw.into_iter().map(|(_, labels, _)| labels).collect();
        println!(" profile {name}:");

        let current = time("current SeriesId::compute", || {
            let mut acc = 0u8;
            for labels in &label_sets {
                let id = SeriesId::compute(&tenant, "bench_gauge", labels).expect("compute");
                acc ^= id.0[0];
            }
            acc
        });
        let buffered = time("buffered encode + one-shot blake3", || {
            let mut acc = 0u8;
            for labels in &label_sets {
                let id = buffered_series_id(&tenant, "bench_gauge", labels);
                acc ^= id.0[0];
            }
            acc
        });
        assert_eq!(current, buffered, "buffered variant must match ids");
        time("buffer build only (no hash)", || {
            let mut total = 0usize;
            for labels in &label_sets {
                let mut buf = Vec::with_capacity(1024);
                buf.extend_from_slice(b"ravel-series-v1\0");
                for label in labels.iter() {
                    let len = label.name.len() as u16;
                    buf.extend_from_slice(&len.to_le_bytes());
                    buf.extend_from_slice(label.name.as_bytes());
                    let len = label.value.len() as u16;
                    buf.extend_from_slice(&len.to_le_bytes());
                    buf.extend_from_slice(label.value.as_bytes());
                }
                total += buf.len();
            }
            total
        });
    }
}

// ------------------------------------------------------------------- encode

const ENCODE_SERIES: usize = 100_000;
const TOTAL_SAMPLES: usize = 200_000;

fn encode_inputs() -> Vec<SeriesInput> {
    let config = WorkloadConfig {
        series_count: ENCODE_SERIES,
        samples_per_series: (TOTAL_SAMPLES / ENCODE_SERIES).max(1),
        cardinality: CardinalityProfile::many_small(ENCODE_SERIES),
        ..Default::default()
    };
    generate_raw(&config)
        .expect("generate")
        .into_iter()
        .map(|(series_id, labels, samples)| SeriesInput {
            series_id,
            labels,
            samples,
        })
        .collect()
}

fn clone_inputs(inputs: &[SeriesInput]) -> Vec<SeriesInput> {
    inputs
        .iter()
        .map(|s| SeriesInput {
            series_id: s.series_id,
            labels: s.labels.clone(),
            samples: s.samples.clone(),
        })
        .collect()
}

fn profile_encode() {
    println!("== segment_encode (100k series, 200k samples, many_small) ==");
    let inputs = encode_inputs();

    time("clone_inputs (bench-loop overhead)", || {
        clone_inputs(&inputs).len()
    });

    let mut series = clone_inputs(&inputs);
    time("per-series samples sort_by_key", || {
        for s in &mut series {
            s.samples.sort_by_key(|sample| sample.ts_ns);
        }
    });
    time("duplicate-id check (ids vec + sort)", || {
        let mut ids: Vec<[u8; 16]> = series.iter().map(|s| s.series_id.0).collect();
        ids.sort();
        ids.windows(2).any(|w| w[0] == w[1])
    });
    time("series sort_by_key(series_id)", || {
        series.sort_by_key(|s| s.series_id.0);
    });

    let dict = time("dict build: BTreeSet<&str> + to_string", || {
        let mut set: BTreeSet<&str> = BTreeSet::new();
        for s in &series {
            for label in s.labels.iter() {
                set.insert(label.name.as_str());
                set.insert(label.value.as_str());
            }
        }
        set.remove("__name__");
        let mut dict = Vec::with_capacity(set.len() + 1);
        dict.push("__name__".to_string());
        dict.extend(set.into_iter().map(str::to_string));
        dict
    });
    println!("    (dict strings: {})", dict.len());

    let sorted = time("dict build alt: occurrence vec sort+dedup", || {
        let mut all: Vec<&str> = Vec::with_capacity(series.len() * 10);
        for s in &series {
            for label in s.labels.iter() {
                all.push(label.name.as_str());
                all.push(label.value.as_str());
            }
        }
        all.sort_unstable();
        all.dedup();
        all
    });
    println!("    (distinct strings: {})", sorted.len());

    let ordinal_of = time("ordinal map build: HashMap<&str,u32> siphash", || {
        let map: HashMap<&str, u32> = dict
            .iter()
            .enumerate()
            .map(|(i, s)| (s.as_str(), i as u32))
            .collect();
        map
    });
    time("ordinal lookups: 1M siphash map gets", || {
        let mut acc = 0u64;
        for s in &series {
            for label in s.labels.iter() {
                acc += u64::from(*ordinal_of.get(label.name.as_str()).expect("name"));
                acc += u64::from(*ordinal_of.get(label.value.as_str()).expect("value"));
            }
        }
        acc
    });
    time("ordinal lookups: 1M binary searches", || {
        let mut acc = 0u64;
        for s in &series {
            for label in s.labels.iter() {
                acc += sorted.binary_search(&label.name.as_str()).unwrap_or(0) as u64;
                acc += sorted.binary_search(&label.value.as_str()).unwrap_or(0) as u64;
            }
        }
        acc
    });

    time("lz4 compress of 100k tiny ts payloads", || {
        let payload: [u8; 10] = [0x94, 0xd1, 0x8c, 0xfa, 0xd0, 0xc9, 0xa9, 0x2f, 0x80, 0x94];
        let mut total = 0usize;
        for _ in 0..ENCODE_SERIES {
            total += lz4_flex::compress_prepend_size(&payload).len();
        }
        total
    });
    time("crc32c of 100k tiny pages (16B seed + 12B)", || {
        let payload = [0u8; 12];
        let seed = [7u8; 16];
        let mut acc = 0u32;
        for _ in 0..ENCODE_SERIES {
            let mut crc = crc32c::crc32c(&seed);
            crc = crc32c::crc32c_append(crc, &payload);
            acc ^= crc;
        }
        acc
    });

    // Current-pipeline stages (post issue-16 rework): FNV interner pass,
    // prefix-key sort of distinct strings, ordinal remap.
    let (distinct, occ) = time("interner pass: FNV map + occurrence ids", || {
        let mut interner: HashMap<&str, u32, std::hash::BuildHasherDefault<Fnv>> =
            HashMap::with_capacity_and_hasher(
                series.len() * 10,
                std::hash::BuildHasherDefault::default(),
            );
        let mut distinct: Vec<&str> = Vec::new();
        let mut occ: Vec<u32> = Vec::with_capacity(series.len() * 10);
        for s in &series {
            for label in s.labels.iter() {
                for text in [label.name.as_str(), label.value.as_str()] {
                    let id = match interner.entry(text) {
                        std::collections::hash_map::Entry::Occupied(e) => *e.get(),
                        std::collections::hash_map::Entry::Vacant(e) => {
                            let id = distinct.len() as u32;
                            distinct.push(text);
                            e.insert(id);
                            id
                        }
                    };
                    occ.push(id);
                }
            }
        }
        (distinct, occ)
    });
    let order = time("prefix-key sort of distinct strings", || {
        let mut order: Vec<(u64, u32)> = distinct
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let b = s.as_bytes();
                let mut k = [0u8; 8];
                let n = b.len().min(8);
                k[..n].copy_from_slice(&b[..n]);
                (u64::from_be_bytes(k), i as u32)
            })
            .collect();
        order.sort_unstable_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| distinct[a.1 as usize].cmp(distinct[b.1 as usize]))
        });
        order
    });
    time("rank assign + occurrence remap", || {
        let mut rank = vec![0u32; distinct.len()];
        for (i, &(_, id)) in order.iter().enumerate() {
            rank[id as usize] = i as u32 + 1;
        }
        occ.iter()
            .map(|&id| u64::from(rank[id as usize]))
            .sum::<u64>()
    });

    // Real section payloads for zstd/blake3 attribution.
    let written = SegmentWriter::write(clone_inputs(&inputs), bench_identity(), bench_bounds())
        .expect("write");
    let bytes = written.bytes.clone();
    println!("    (object size: {} bytes)", bytes.len());
    let footer = ravel_segment::open_from_full(&bytes, ReaderLimits::default())
        .expect("open")
        .footer;
    for (kind, name) in [(LABEL_DICT, "LABEL_DICT"), (SERIES_TABLE, "SERIES_TABLE")] {
        let section = footer
            .sections
            .iter()
            .find(|s| s.kind == kind)
            .expect("section");
        let stored = section_bytes(&bytes, &footer, kind);
        let raw = zstd::bulk::decompress(stored, section.uncompressed_len as usize).expect("zstd");
        println!("    ({name} raw {} bytes)", raw.len());
        time(&format!("zstd -3 compress {name}"), || {
            zstd::bulk::compress(&raw, 3).expect("zstd").len()
        });
    }
    time("crc32c of full page sections (~7 MB)", || {
        crc32c::crc32c(&bytes)
    });
    time("blake3 of whole object", || {
        *blake3::hash(&bytes).as_bytes()
    });

    time("full SegmentWriter::write (reference)", || {
        SegmentWriter::write(clone_inputs(&inputs), bench_identity(), bench_bounds())
            .expect("write")
            .bytes
            .len()
    });
}

// ------------------------------------------------- v1 vs v2 encode buckets

/// Local LEB128 uvarint, mirroring `crate::varint::write_uvarint` in
/// ravel-segment (private there).
fn uvarint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if v == 0 {
            break;
        }
    }
}

/// Local zigzag varint, mirroring `crate::varint::write_zigzag_varint`.
fn zigzag(buf: &mut Vec<u8>, v: i64) {
    uvarint(buf, ((v << 1) ^ (v >> 63)) as u64);
}

/// Local page crc, mirroring `crate::crc::page_crc`: covers
/// `series_id || enc || comp || payload`.
fn page_crc(series_id: &[u8; 16], enc: u8, comp: u8, payload: &[u8]) -> u32 {
    let mut crc = crc32c::crc32c(series_id);
    crc = crc32c::crc32c_append(crc, &[enc, comp]);
    crc32c::crc32c_append(crc, payload)
}

fn prefix_key(s: &str) -> u64 {
    let bytes = s.as_bytes();
    let mut key = [0u8; 8];
    let n = bytes.len().min(8);
    key[..n].copy_from_slice(&bytes[..n]);
    u64::from_be_bytes(key)
}

fn timed<R>(f: impl FnOnce() -> R) -> (R, f64) {
    let start = Instant::now();
    let out = f();
    (out, start.elapsed().as_secs_f64() * 1e3)
}

const TS_DELTA_VARINT: u8 = 1;
const VAL_RAW_F64: u8 = 17;
const COMP_NONE: u8 = 0;
const COMP_LZ4: u8 = 1;

/// Applies `write`/`write_v2`'s shared preamble (sort samples, drop empty
/// series, sort by id) so the bucket mirrors run over the same ordering the
/// real writer sees.
fn prepared_series() -> Vec<SeriesInput> {
    let mut series = clone_inputs(&encode_inputs());
    for s in &mut series {
        s.samples.sort_by_key(|sample| sample.ts_ns);
    }
    series.retain(|s| !s.samples.is_empty());
    series.sort_unstable_by_key(|s| s.series_id.0);
    series
}

/// v1 interner pass: single FNV pass to distinct strings + per-occurrence
/// ordinals (mirrors `build_dictionary` before its sort).
#[allow(clippy::type_complexity)]
fn v1_interner(series: &[SeriesInput]) -> (Vec<&str>, Vec<u32>) {
    let total: usize = series.iter().map(|s| s.labels.len() * 2).sum();
    let mut interner: HashMap<&str, u32, std::hash::BuildHasherDefault<Fnv>> =
        HashMap::with_capacity_and_hasher(total, std::hash::BuildHasherDefault::default());
    let mut distinct: Vec<&str> = Vec::new();
    let mut occ: Vec<u32> = Vec::with_capacity(total);
    for s in series {
        for label in s.labels.iter() {
            for text in [label.name.as_str(), label.value.as_str()] {
                let id = match interner.entry(text) {
                    std::collections::hash_map::Entry::Occupied(e) => *e.get(),
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let id = distinct.len() as u32;
                        distinct.push(text);
                        e.insert(id);
                        id
                    }
                };
                occ.push(id);
            }
        }
    }
    (distinct, occ)
}

/// Frames one series' TS and VAL page into the buffers, mirroring
/// `append_ts_page` (real ts-delta varint) and `append_val_page`. The VAL
/// payload is emitted as raw f64 for both versions: the Gorilla-vs-raw
/// choice cost is not the framing bottleneck, and using one encoding for
/// both means the v1/v2 framing delta isolates the v2 alignment-pad branch
/// (`align_v2`), which is the only real per-page difference in v2.
fn frame_pages(
    s: &SeriesInput,
    ts_pages: &mut Vec<u8>,
    val_pages: &mut Vec<u8>,
    align_v2: bool,
) -> (u64, u64, u64) {
    // TS page: zigzag-delta varints, no lz4 at this payload size.
    let mut ts_payload = Vec::new();
    let mut prev = 0i64;
    for (i, sm) in s.samples.iter().enumerate() {
        let delta = if i == 0 { sm.ts_ns } else { sm.ts_ns - prev };
        zigzag(&mut ts_payload, delta);
        prev = sm.ts_ns;
    }
    let (comp, payload): (u8, &[u8]) = if ts_payload.len() >= 64 {
        let cand = lz4_flex::compress_prepend_size(&ts_payload);
        if cand.len() < ts_payload.len() {
            (COMP_LZ4, ts_payload.as_slice())
        } else {
            (COMP_NONE, ts_payload.as_slice())
        }
    } else {
        (COMP_NONE, ts_payload.as_slice())
    };
    let ts_len = {
        let before = ts_pages.len();
        let crc = page_crc(&s.series_id.0, TS_DELTA_VARINT, comp, payload);
        ts_pages.push(TS_DELTA_VARINT);
        ts_pages.push(comp);
        ts_pages.extend_from_slice(&crc.to_le_bytes());
        ts_pages.extend_from_slice(payload);
        (ts_pages.len() - before) as u64
    };

    // VAL page: raw f64 payload, with the v2 alignment pad when requested.
    let mut val_payload = Vec::with_capacity(s.samples.len() * 8);
    for sm in &s.samples {
        val_payload.extend_from_slice(&sm.value.to_le_bytes());
    }
    let mut val_gap = 0u64;
    if align_v2 {
        let unaligned_payload_start = val_pages.len() as u64 + 6;
        let rem = unaligned_payload_start % 8;
        if rem != 0 {
            val_gap = 8 - rem;
            val_pages.extend(std::iter::repeat_n(0u8, val_gap as usize));
        }
    }
    let val_len = {
        let before = val_pages.len();
        let crc = page_crc(&s.series_id.0, VAL_RAW_F64, COMP_NONE, &val_payload);
        val_pages.push(VAL_RAW_F64);
        val_pages.push(COMP_NONE);
        val_pages.extend_from_slice(&crc.to_le_bytes());
        val_pages.extend_from_slice(&val_payload);
        (val_pages.len() - before) as u64 - val_gap
    };
    (ts_len, val_gap, val_len)
}

/// Times the real raw-then-recompress cost of one catalog section, matching
/// `profile_encode`'s zstd attribution: decompress the writer's stored
/// bytes to the raw form, then time a zstd -3 compress of that raw form.
fn time_zstd_section(bytes: &[u8], footer: &ravel_segment::Footer, kind: u32) -> f64 {
    let section = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .expect("section");
    let stored = section_bytes(bytes, footer, kind);
    let raw = if section.comp == 0 {
        stored.to_vec()
    } else {
        zstd::bulk::decompress(stored, section.uncompressed_len as usize).expect("zstd")
    };
    timed(|| zstd::bulk::compress(&raw, 3).expect("zstd").len()).1
}

/// Times object assembly (section memcpys + whole-object blake3) from a
/// written segment's real section bytes.
fn time_object_assembly(bytes: &[u8], footer: &ravel_segment::Footer, kinds: &[u32]) -> f64 {
    let sections: Vec<&[u8]> = kinds
        .iter()
        .map(|&k| section_bytes(bytes, footer, k))
        .collect();
    timed(|| {
        let mut object = Vec::with_capacity(bytes.len());
        for sec in &sections {
            object.extend_from_slice(sec);
        }
        *blake3::hash(&object).as_bytes()
    })
    .1
}

fn profile_encode_v1_vs_v2() {
    println!("== segment_encode v1 vs v2 buckets (100k series, 200k samples, many_small) ==");
    let series = prepared_series();

    // Footer minimum event ts, needed for the v2 min_ts_delta column.
    let footer_min = series
        .iter()
        .filter_map(|s| s.samples.first().map(|sm| sm.ts_ns))
        .min()
        .unwrap_or(0);

    // ---- v1 buckets ----
    let ((distinct, occ_raw), v1_interner_ms) = timed(|| v1_interner(&series));

    let ((sorted_non_name, occ_v1), v1_sort_ms) = timed(|| {
        let mut order: Vec<(u64, u32)> = distinct
            .iter()
            .enumerate()
            .map(|(i, s)| (prefix_key(s), i as u32))
            .collect();
        order.sort_unstable_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| distinct[a.1 as usize].cmp(distinct[b.1 as usize]))
        });
        let mut rank = vec![0u32; distinct.len()];
        let mut next = 1u32;
        for &(_, id) in &order {
            if distinct[id as usize] == METRIC_NAME_LABEL {
                rank[id as usize] = 0;
            } else {
                rank[id as usize] = next;
                next += 1;
            }
        }
        let occ: Vec<u32> = occ_raw.iter().map(|&id| rank[id as usize]).collect();
        let mut sorted: Vec<&str> = Vec::with_capacity(order.len());
        for &(_, id) in &order {
            let t = distinct[id as usize];
            if t != METRIC_NAME_LABEL {
                sorted.push(t);
            }
        }
        (sorted, occ)
    });

    let ((_v1_ts_pages, _v1_val_pages, v1_page_meta), v1_framing_ms) = timed(|| {
        let mut ts_pages = Vec::new();
        let mut val_pages = Vec::new();
        let mut meta: Vec<(u64, u64, u64, u64)> = Vec::with_capacity(series.len());
        for s in &series {
            let ts_off = ts_pages.len() as u64;
            let val_off = val_pages.len() as u64;
            let (ts_len, _gap, val_len) = frame_pages(s, &mut ts_pages, &mut val_pages, false);
            meta.push((ts_off, ts_len, val_off, val_len));
        }
        (ts_pages, val_pages, meta)
    });

    let (_v1_table_len, v1_emit_ms) = timed(|| {
        let mut buf = Vec::with_capacity(4 + series.len() * 46);
        buf.extend_from_slice(&(series.len() as u32).to_le_bytes());
        let mut occ = occ_v1.iter();
        for (s, &(ts_off, ts_len, val_off, val_len)) in series.iter().zip(&v1_page_meta) {
            buf.extend_from_slice(&s.series_id.0);
            buf.extend_from_slice(&(s.labels.len() as u16).to_le_bytes());
            for _ in 0..s.labels.len() {
                uvarint(&mut buf, u64::from(*occ.next().expect("name")));
                uvarint(&mut buf, u64::from(*occ.next().expect("value")));
            }
            buf.extend_from_slice(&(s.samples.len() as u32).to_le_bytes());
            let min_ts = s.samples.first().map_or(0, |sm| sm.ts_ns);
            let max_ts = s.samples.last().map_or(0, |sm| sm.ts_ns);
            buf.extend_from_slice(&min_ts.to_le_bytes());
            buf.extend_from_slice(&max_ts.to_le_bytes());
            uvarint(&mut buf, ts_off);
            uvarint(&mut buf, ts_len);
            uvarint(&mut buf, val_off);
            uvarint(&mut buf, val_len);
        }
        // LABEL_DICT raw
        let mut dict = Vec::new();
        dict.extend_from_slice(&((sorted_non_name.len() + 1) as u32).to_le_bytes());
        uvarint(&mut dict, METRIC_NAME_LABEL.len() as u64);
        dict.extend_from_slice(METRIC_NAME_LABEL.as_bytes());
        for st in &sorted_non_name {
            uvarint(&mut dict, st.len() as u64);
            dict.extend_from_slice(st.as_bytes());
        }
        buf.len() + dict.len()
    });

    // ---- v2 buckets ----
    let ((order_v2, occ_v2), v2_interner_ms) = timed(|| {
        let total: usize = series.iter().map(|s| s.labels.len() * 2).sum();
        let mut interner: HashMap<&str, u32, std::hash::BuildHasherDefault<Fnv>> =
            HashMap::with_capacity_and_hasher(total + 1, std::hash::BuildHasherDefault::default());
        interner.insert(METRIC_NAME_LABEL, 0);
        let mut order: Vec<&str> = Vec::new();
        let mut occ: Vec<u32> = Vec::with_capacity(total);
        let mut next = 1u32;
        for s in &series {
            for label in s.labels.iter() {
                for text in [label.name.as_str(), label.value.as_str()] {
                    let id = match interner.entry(text) {
                        std::collections::hash_map::Entry::Occupied(e) => *e.get(),
                        std::collections::hash_map::Entry::Vacant(e) => {
                            let id = next;
                            next += 1;
                            order.push(text);
                            e.insert(id);
                            id
                        }
                    };
                    occ.push(id);
                }
            }
        }
        (order, occ)
    });

    // Schema-dictionary build: dedupe name-ordinal lists, emit schema_ref +
    // value_ord columns (mirrors the schema layer of `build_series_meta_v2`).
    let ((schemas, col_schema_ref, col_value_ord), v2_schema_ms) = timed(|| {
        let mut schema_index: HashMap<Vec<u32>, u32> = HashMap::new();
        let mut schemas: Vec<Vec<u32>> = Vec::new();
        let mut col_schema_ref: Vec<u8> = Vec::new();
        let mut col_value_ord: Vec<u8> = Vec::new();
        let mut occ = occ_v2.iter();
        for s in &series {
            let mut name_ords: Vec<u32> = Vec::with_capacity(s.labels.len());
            let mut value_ords: Vec<u32> = Vec::with_capacity(s.labels.len());
            for _ in 0..s.labels.len() {
                name_ords.push(*occ.next().expect("name"));
                value_ords.push(*occ.next().expect("value"));
            }
            let schema_ref = match schema_index.get(&name_ords) {
                Some(&idx) => idx,
                None => {
                    let idx = schemas.len() as u32;
                    schema_index.insert(name_ords.clone(), idx);
                    schemas.push(name_ords);
                    idx
                }
            };
            uvarint(&mut col_schema_ref, u64::from(schema_ref));
            for v in &value_ords {
                uvarint(&mut col_value_ord, u64::from(*v));
            }
        }
        (schemas, col_schema_ref, col_value_ord)
    });

    let ((_v2_ts_pages, _v2_val_pages, v2_page_meta), v2_framing_ms) = timed(|| {
        let mut ts_pages = Vec::new();
        let mut val_pages = Vec::new();
        let mut meta: Vec<(u64, u64, u64)> = Vec::with_capacity(series.len());
        for s in &series {
            let (ts_len, val_gap, val_len) = frame_pages(s, &mut ts_pages, &mut val_pages, true);
            meta.push((ts_len, val_gap, val_len));
        }
        (ts_pages, val_pages, meta)
    });

    let (_v2_meta_len, v2_emit_ms) = timed(|| {
        let mut col_sample_count = Vec::new();
        let mut col_min_ts_delta = Vec::new();
        let mut col_ts_span = Vec::new();
        let mut col_ts_gap = Vec::new();
        let mut col_ts_len = Vec::new();
        let mut col_val_gap = Vec::new();
        let mut col_val_len = Vec::new();
        for (s, &(ts_len, val_gap, val_len)) in series.iter().zip(&v2_page_meta) {
            uvarint(&mut col_sample_count, s.samples.len() as u64);
            let min_ts = s.samples.first().map_or(0, |sm| sm.ts_ns);
            let max_ts = s.samples.last().map_or(0, |sm| sm.ts_ns);
            let min_ts_delta = (i128::from(min_ts) - i128::from(footer_min)) as u64;
            let ts_span = (i128::from(max_ts) - i128::from(min_ts)) as u64;
            uvarint(&mut col_min_ts_delta, min_ts_delta);
            uvarint(&mut col_ts_span, ts_span);
            uvarint(&mut col_ts_gap, 0);
            uvarint(&mut col_ts_len, ts_len);
            uvarint(&mut col_val_gap, val_gap);
            uvarint(&mut col_val_len, val_len);
        }
        // SERIES_META assembly: count, schema_count, schemas, 9 blocks.
        let mut meta = Vec::new();
        meta.extend_from_slice(&(series.len() as u32).to_le_bytes());
        meta.extend_from_slice(&(schemas.len() as u32).to_le_bytes());
        for schema in &schemas {
            uvarint(&mut meta, schema.len() as u64);
            for n in schema {
                uvarint(&mut meta, u64::from(*n));
            }
        }
        for col in [
            &col_schema_ref,
            &col_value_ord,
            &col_sample_count,
            &col_min_ts_delta,
            &col_ts_span,
            &col_ts_gap,
            &col_ts_len,
            &col_val_gap,
            &col_val_len,
        ] {
            uvarint(&mut meta, col.len() as u64);
            meta.extend_from_slice(col);
        }
        // SERIES_IDS (never zstd'd): count + 16 bytes per series.
        let mut ids = Vec::with_capacity(4 + series.len() * 16);
        ids.extend_from_slice(&(series.len() as u32).to_le_bytes());
        for s in &series {
            ids.extend_from_slice(&s.series_id.0);
        }
        // LABEL_DICT raw (first-occurrence order).
        let mut dict = Vec::new();
        dict.extend_from_slice(&((order_v2.len() + 1) as u32).to_le_bytes());
        uvarint(&mut dict, METRIC_NAME_LABEL.len() as u64);
        dict.extend_from_slice(METRIC_NAME_LABEL.as_bytes());
        for st in &order_v2 {
            uvarint(&mut dict, st.len() as u64);
            dict.extend_from_slice(st.as_bytes());
        }
        meta.len() + ids.len() + dict.len()
    });

    // ---- zstd + object assembly from the real written segments ----
    let w1 = SegmentWriter::write(prepared_inputs(), bench_identity(), bench_bounds())
        .expect("write v1");
    let w2 = SegmentWriter::write_v2(prepared_inputs(), bench_identity(), bench_bounds())
        .expect("write v2");
    let f1 = ravel_segment::open_from_full(&w1.bytes, ReaderLimits::default())
        .expect("open v1")
        .footer;
    let f2 = ravel_segment::open_from_full(&w2.bytes, ReaderLimits::default())
        .expect("open v2")
        .footer;

    let v1_zstd_dict = time_zstd_section(&w1.bytes, &f1, LABEL_DICT);
    let v1_zstd_table = time_zstd_section(&w1.bytes, &f1, SERIES_TABLE);
    let v2_zstd_dict = time_zstd_section(&w2.bytes, &f2, LABEL_DICT);
    let v2_zstd_meta = time_zstd_section(&w2.bytes, &f2, SERIES_META);
    // SERIES_IDS is deliberately never zstd-compressed in v2.

    let v1_assembly = time_object_assembly(
        &w1.bytes,
        &f1,
        &[LABEL_DICT, SERIES_TABLE, TS_PAGES, VAL_PAGES],
    );
    let v2_assembly = time_object_assembly(
        &w2.bytes,
        &f2,
        &[LABEL_DICT, SERIES_IDS, SERIES_META, TS_PAGES, VAL_PAGES],
    );

    let v1_full = timed(|| {
        SegmentWriter::write(prepared_inputs(), bench_identity(), bench_bounds())
            .expect("write v1")
            .bytes
            .len()
    })
    .1;
    let v2_full = timed(|| {
        SegmentWriter::write_v2(prepared_inputs(), bench_identity(), bench_bounds())
            .expect("write v2")
            .bytes
            .len()
    })
    .1;

    let v1_zstd = v1_zstd_dict + v1_zstd_table;
    let v2_zstd = v2_zstd_dict + v2_zstd_meta;

    println!("  {:<40} {:>10} {:>10}", "bucket", "v1 ms", "v2 ms");
    row(
        "interner / dictionary build",
        Some(v1_interner_ms),
        Some(v2_interner_ms),
    );
    row("distinct-string sort (v1 only)", Some(v1_sort_ms), None);
    row(
        "schema-dictionary build (v2 only)",
        None,
        Some(v2_schema_ms),
    );
    row(
        "catalog section emission",
        Some(v1_emit_ms),
        Some(v2_emit_ms),
    );
    row("zstd LABEL_DICT", Some(v1_zstd_dict), Some(v2_zstd_dict));
    row(
        "zstd SERIES_TABLE(v1)/SERIES_META(v2)",
        Some(v1_zstd_table),
        Some(v2_zstd_meta),
    );
    row("zstd catalog total", Some(v1_zstd), Some(v2_zstd));
    row(
        "page framing (TS+VAL)",
        Some(v1_framing_ms),
        Some(v2_framing_ms),
    );
    row(
        "object assembly + blake3",
        Some(v1_assembly),
        Some(v2_assembly),
    );
    println!("  {:-<62}", "");
    row(
        "full SegmentWriter::write/write_v2",
        Some(v1_full),
        Some(v2_full),
    );
    println!(
        "  (full-write speedup v1/v2: {:.3}x; schemas in v2 meta: {})",
        v1_full / v2_full,
        schemas.len()
    );
    println!("  (note: VAL payload mirrored as raw-f64 in framing bucket; v1/v2 framing");
    println!("   delta isolates the v2 raw-f64 alignment-pad branch)");
}

fn prepared_inputs() -> Vec<SeriesInput> {
    prepared_series()
}

fn row(label: &str, v1: Option<f64>, v2: Option<f64>) {
    let fmt = |v: Option<f64>| match v {
        Some(ms) => format!("{ms:>10.2}"),
        None => format!("{:>10}", "n/a"),
    };
    println!("  {:<40} {} {}", label, fmt(v1), fmt(v2));
}

// -------------------------------------------------------------- selective decode

fn read_uvarint(bytes: &[u8], pos: &mut usize) -> u64 {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let byte = bytes[*pos];
        *pos += 1;
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return result;
        }
        shift += 7;
    }
}

fn profile_selective_decode() {
    println!("== segment_decode_selective (10k series, 60 samples, 1% match) ==");
    let config = WorkloadConfig {
        series_count: 10_000,
        samples_per_series: 60,
        cardinality: CardinalityProfile {
            distinct_sets: 100,
            labels_per_set: 6,
        },
        ..Default::default()
    };
    let raw = generate_raw(&config).expect("generate");
    let written = build_segment(raw);
    let bytes = written.bytes;

    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(&bytes, limits).expect("open");
    let dict_stored = section_bytes(&bytes, &loc.footer, LABEL_DICT);
    let table_stored = section_bytes(&bytes, &loc.footer, SERIES_TABLE);
    let dict_section = loc
        .footer
        .sections
        .iter()
        .find(|s| s.kind == LABEL_DICT)
        .expect("dict");
    let table_section = loc
        .footer
        .sections
        .iter()
        .find(|s| s.kind == SERIES_TABLE)
        .expect("table");

    let dict_raw = time("zstd decompress LABEL_DICT", || {
        zstd::bulk::decompress(dict_stored, dict_section.uncompressed_len as usize).expect("zstd")
    });
    let table_raw = time("zstd decompress SERIES_TABLE", || {
        zstd::bulk::decompress(table_stored, table_section.uncompressed_len as usize).expect("zstd")
    });
    println!(
        "    (dict {} bytes raw, table {} bytes raw)",
        dict_raw.len(),
        table_raw.len()
    );

    let dict = time("parse dict into Vec<String>", || {
        let mut pos = 0usize;
        let count = u32::from_le_bytes(dict_raw[0..4].try_into().unwrap());
        pos += 4;
        let mut out = Vec::new();
        for _ in 0..count {
            let len = read_uvarint(&dict_raw, &mut pos) as usize;
            out.push(String::from_utf8(dict_raw[pos..pos + len].to_vec()).expect("utf8"));
            pos += len;
        }
        out
    });
    println!("    (dict strings: {})", dict.len());

    let raw_entries = time("parse series table (ordinals only)", || {
        let mut pos = 0usize;
        let count = u32::from_le_bytes(table_raw[0..4].try_into().unwrap());
        pos += 4;
        let mut out = Vec::with_capacity(count as usize);
        for _ in 0..count {
            pos += 16;
            let label_count =
                u16::from_le_bytes(table_raw[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let mut pairs = Vec::with_capacity(label_count);
            for _ in 0..label_count {
                let n = read_uvarint(&table_raw, &mut pos);
                let v = read_uvarint(&table_raw, &mut pos);
                pairs.push((n, v));
            }
            pos += 4 + 8 + 8;
            for _ in 0..4 {
                read_uvarint(&table_raw, &mut pos);
            }
            out.push(pairs);
        }
        out
    });

    time("materialize all LabelSets (String clones)", || {
        let mut total = 0usize;
        for pairs in &raw_entries {
            let labels: Vec<Label> = pairs
                .iter()
                .map(|&(n, v)| Label {
                    name: dict[n as usize].clone(),
                    value: dict[v as usize].clone(),
                })
                .collect();
            total += LabelSet::new(labels).expect("labels").len();
        }
        total
    });

    let (footer, entries) = time("full decode_entries (reference)", || decode_entries(&bytes));
    let selected = select(&entries, &[("label_000", "v0_0")], None);
    let ranges = plan_ranges(&footer, &selected).expect("plan");
    time("decode 1% of pages (100 series x 60)", || {
        let mut decoded = 0usize;
        for (entry, range) in selected.iter().zip(ranges.iter()) {
            let ts =
                &bytes[range.ts_range.0 as usize..(range.ts_range.0 + range.ts_range.1) as usize];
            let val = &bytes
                [range.val_range.0 as usize..(range.val_range.0 + range.val_range.1) as usize];
            decoded += decode_pages(entry, ts, val, limits).expect("decode").len();
        }
        decoded
    });
    let full_samples: usize = {
        let selected_all: Vec<_> = select(&entries, &[], None);
        selected_all.iter().map(|e| e.sample_count as usize).sum()
    };
    println!("    (total samples in segment: {full_samples})");
}
