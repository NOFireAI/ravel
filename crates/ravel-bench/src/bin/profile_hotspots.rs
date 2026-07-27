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
    LABEL_DICT, SERIES_TABLE, bench_bounds, bench_identity, build_segment, decode_entries,
    section_bytes,
};
use ravel_segment::{ReaderLimits, SegmentWriter, SeriesInput, decode_pages, plan_ranges, select};
use ravel_types::{Label, LabelSet, SeriesId, TenantId};

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

    // Real section payloads for zstd/blake3 attribution.
    let written = SegmentWriter::write(clone_inputs(&inputs), bench_identity(), bench_bounds())
        .expect("write");
    let bytes = written.bytes.clone();
    println!("    (object size: {} bytes)", bytes.len());
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
