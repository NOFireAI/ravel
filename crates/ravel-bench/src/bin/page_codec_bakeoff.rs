//! Page-level codec bake-off (issue #1509, epic #1507).
//!
//! Measures candidate page compressors against the *encoded* per-column page
//! payloads production writers actually produce, so #1507 can reject a weak
//! candidate before any integration work is dispatched. This binary changes
//! no production path: it is report-only tooling reachable only via
//! `cargo run -p ravel-bench --bin page_codec_bakeoff --profile ci`, and its
//! consumer is #1507's design gate, not any shipping code path.
//!
//! ## What is measured
//!
//! Every dataset below is obtained by driving the real production writer
//! entry points -- [`ravel_logseg::block::write_block`] for RLOG page
//! payloads, [`ravel_segment::SegmentWriter::write_v5`] (through
//! `ravel_bench::segment_support::build_segment_v5`) for RSEG whole-section
//! payloads -- never hand-rolled byte buffers. `write_block` does not expose
//! a column's pre-envelope `encoded` bytes directly (it feeds them straight
//! into [`ravel_logseg::page::write_page`]), so this bake-off recovers them
//! by the same trick [`ravel_logseg::page::read_page`] uses for real reads:
//! decompress (or pass through) the stored page bytes, which is a lossless
//! inverse of `write_page` and therefore reproduces the exact `encoded` bytes
//! the production writer computed. The RSEG whole-section datasets use the
//! same trick against the section's `uncompressed_len`.
//!
//! ## Page geometry
//!
//! [`PAGE_ROW_COUNTS`] is derived from
//! `ravel_logseg::writer::RlogConfig::default().block_target_records` (8192):
//! a small fragment (64), an eighth of a block (1024), and a full block
//! (8192). The RSEG datasets and the below-floor dataset each carry a single
//! fixed-size point instead of being swept across this list (a whole RSEG
//! section is not row-shaped, and the below-floor dataset exists to prove one
//! specific point below [`ravel_logseg::page::COMPRESSION_FLOOR`]).
//!
//! ## Deviation from the issue text: SERIES_TABLE -> SERIES_META
//!
//! The issue asks for "the two RSEG whole-section payloads compressed today,
//! LABEL_DICT and SERIES_TABLE". `ravel_segment::format::section_kind::SERIES_TABLE`
//! (kind 2) is retired with RSEG v1 (ADR-0027) and is never emitted by any v5
//! object (confirmed in docs/segment-format.md's section table: "retired with
//! RSEG v1; never emitted, number reserved forever"). The payload that is
//! actually whole-section zstd-compressed today alongside LABEL_DICT is
//! SERIES_META (kind 6, `ravel_segment::writer` compresses both
//! `label_dict_raw` and `series_meta_raw` through the same `zstd_compress_v4`
//! call). This bake-off measures LABEL_DICT and SERIES_META instead, matching
//! actual current production behavior and the issue's evident intent ("the
//! two ... payloads that are compressed today"). Reported, not silently
//! patched into the issue text: `crates/ravel-segment/src/format.rs`'s
//! `ZSTD_LEVEL` doc comment is itself stale in the same way, still naming
//! SERIES_TABLE instead of SERIES_META -- a bug in a crate outside this
//! task's scope (`crates/ravel-bench` only), flagged in the task report
//! rather than fixed here.
//!
//! ## Arms
//!
//! `raw` (never compresses); `zstd-1`/`zstd-3`/`zstd-6` (one-shot
//! `zstd::bulk::compress`, applying the exact same floor-and-shrink policy
//! [`ravel_logseg::page::write_page`] uses); `zstd-3` is the PRODUCTION
//! BASELINE (`RlogConfig::default().zstd_level == 3`) and its policy decision
//! is cross-checked against a real `page::write_page` call on every page;
//! `zstd-3-reused` (same level, a single `zstd::bulk::Compressor`/
//! `Decompressor` reused across every page in the run instead of one
//! constructed per call), verified byte-identical to the `zstd-3` one-shot
//! arm on every page; `lz4` (`lz4_flex::compress_prepend_size`, same floor
//! policy).
//!
//! ## Integrity, enforced by exit code
//!
//! Every arm round-trips every page bit-exactly; every (dataset, page-size,
//! arm) cell is emitted exactly once (checked by set equality plus a
//! duplicate count, not by "at least one"); the `zstd-3` arm's compress/keep
//! decision matches real `page::write_page` on every page; `zstd-3-reused`'s
//! compressed bytes match the `zstd-3` one-shot's on every page. Any failure
//! prints the offending cell and exits non-zero; `main` never silently
//! swallows a mismatch.
//!
//! ## Output
//!
//! A human-readable table, followed by one JSON object per cell for
//! mechanical diffing. The header carries git SHA, build profile, resolved
//! `zstd`/`lz4_flex` versions, target triple, and load averages sampled at
//! the start and end of the run.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::Instant;

use ravel_bench::bench_env::{env_header, git_commit};
use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{LABEL_DICT, SERIES_META, build_segment_v5, section_bytes};
use ravel_logseg::block::{BlockWriteOut, ColumnPlan, write_block};
use ravel_logseg::encoding::Enc;
use ravel_logseg::page::{COMP_ZSTD, COMPRESSION_FLOOR, DEFAULT_MAX_UNCOMP, read_page, write_page};
use ravel_logseg::record::{
    COL_BODY, COL_SEVERITY_TEXT, COL_TRACE_ID, ColumnValue, FIRST_DYNAMIC_COL, FieldType,
    ResolvedRow,
};
use ravel_segment::{FooterOutcome, parse_footer};
use serde::Serialize;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// `RlogConfig::default().block_target_records` (8192): full block, an
/// eighth of it, and a small fragment.
const PAGE_ROW_COUNTS: [usize; 3] = [64, 1024, 8192];
/// `RlogConfig::default().zstd_level`: the level the production writer uses,
/// and therefore the level the `zstd-3` arm measures as the baseline.
const PRODUCTION_ZSTD_LEVEL: i32 = 3;
/// Timed outer rounds per cell (spec floor: at least 5).
const MEASURE_ROUNDS: usize = 7;
/// Warmup calls discarded before timing starts.
const WARMUP_CALLS: usize = 2;
const BASE_TS_NS: i64 = 1_700_000_000_000_000_000;
const TS_STEP_NS: i64 = 1_000_000_000;

const SEVERITIES: [&str; 4] = ["INFO", "WARN", "ERROR", "DEBUG"];
const SERVICE_NAMES: [&str; 5] = [
    "api-gateway",
    "auth-service",
    "billing-service",
    "frontend",
    "checkout-service",
];
const BODY_TEMPLATES: [&str; 4] = [
    "user {u} logged in from 10.0.{a}.{b}",
    "request {u} completed in {ms}ms with status {code}",
    "connection to backend-{a} failed: timeout after {ms}ms",
    "cache miss for key shard-{a}-{b} on host worker-{u}",
];

// --- dataset construction -------------------------------------------------

fn base_row(ts_ns: i64) -> ResolvedRow {
    ResolvedRow {
        stream_ref: 0,
        ts_ns,
        observed_ts_ns: ts_ns,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: String::new(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs_raw: None,
        columns: Vec::new(),
        indexed_terms: Vec::new(),
        stat_winners: Vec::new(),
    }
}

/// The stored page bytes for `column_id`, decoded back through the real
/// [`read_page`] to recover the exact `encoded` bytes [`write_block`] fed
/// [`write_page`] -- the reconstruction trick this bake-off relies on
/// throughout (see the module doc).
fn column_encoded_bytes(out: &BlockWriteOut, column_id: u32) -> Vec<u8> {
    let mut offset = 0usize;
    for desc in &out.descs {
        let len = desc.len as usize;
        let slice = &out.payload[offset..offset + len];
        offset += len;
        if desc.column_id == column_id {
            return read_page(slice, desc, DEFAULT_MAX_UNCOMP).expect("read back production page");
        }
    }
    panic!("column {column_id} not staged in block");
}

fn structured_body(rng: &mut StdRng) -> String {
    let template = BODY_TEMPLATES[rng.random_range(0..BODY_TEMPLATES.len())];
    template
        .replace("{u}", &rng.random_range(1..10_000).to_string())
        .replace("{a}", &rng.random_range(0..256).to_string())
        .replace("{b}", &rng.random_range(0..256).to_string())
        .replace("{ms}", &rng.random_range(1..5000).to_string())
        .replace(
            "{code}",
            &[200, 201, 404, 500][rng.random_range(0..4)].to_string(),
        )
}

fn structured_logs_page(n: usize) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(0xA5F0_0001 ^ n as u64);
    let rows: Vec<ResolvedRow> = (0..n)
        .map(|i| {
            let mut row = base_row(BASE_TS_NS + i as i64 * TS_STEP_NS);
            row.severity_text = SEVERITIES[i % SEVERITIES.len()].to_string();
            row.body = structured_body(&mut rng);
            row
        })
        .collect();
    let out = write_block(&rows, &[], PRODUCTION_ZSTD_LEVEL).expect("encode structured logs");
    column_encoded_bytes(&out, COL_BODY)
}

fn high_entropy_ids_page(n: usize) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(0xA5F0_0002 ^ n as u64);
    let rows: Vec<ResolvedRow> = (0..n)
        .map(|i| {
            let mut row = base_row(BASE_TS_NS + i as i64 * TS_STEP_NS);
            let mut id = [0u8; 16];
            for b in &mut id {
                *b = rng.random_range(0u16..=255) as u8;
            }
            row.trace_id = Some(id);
            row
        })
        .collect();
    let out = write_block(&rows, &[], PRODUCTION_ZSTD_LEVEL).expect("encode high-entropy ids");
    column_encoded_bytes(&out, COL_TRACE_ID)
}

fn low_cardinality_level_page(n: usize) -> Vec<u8> {
    let rows: Vec<ResolvedRow> = (0..n)
        .map(|i| {
            let mut row = base_row(BASE_TS_NS + i as i64 * TS_STEP_NS);
            row.severity_text = SEVERITIES[i % SEVERITIES.len()].to_string();
            row
        })
        .collect();
    let out = write_block(&rows, &[], PRODUCTION_ZSTD_LEVEL).expect("encode low-cardinality level");
    column_encoded_bytes(&out, COL_SEVERITY_TEXT)
}

fn low_cardinality_service_page(n: usize) -> Vec<u8> {
    let plans = [ColumnPlan {
        column_id: FIRST_DYNAMIC_COL,
        ty: FieldType::Str,
    }];
    let rows: Vec<ResolvedRow> = (0..n)
        .map(|i| {
            let mut row = base_row(BASE_TS_NS + i as i64 * TS_STEP_NS);
            let name = SERVICE_NAMES[i % SERVICE_NAMES.len()];
            row.columns.push((
                FIRST_DYNAMIC_COL,
                ColumnValue::Str(name.as_bytes().to_vec()),
            ));
            row
        })
        .collect();
    let out =
        write_block(&rows, &plans, PRODUCTION_ZSTD_LEVEL).expect("encode low-cardinality service");
    column_encoded_bytes(&out, FIRST_DYNAMIC_COL)
}

fn incompressible_page(n: usize) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(0xA5F0_0004 ^ n as u64);
    let plans = [ColumnPlan {
        column_id: FIRST_DYNAMIC_COL,
        ty: FieldType::Bytes,
    }];
    let rows: Vec<ResolvedRow> = (0..n)
        .map(|i| {
            let mut row = base_row(BASE_TS_NS + i as i64 * TS_STEP_NS);
            let bytes: Vec<u8> = (0..64)
                .map(|_| rng.random_range(0u16..=255) as u8)
                .collect();
            row.columns
                .push((FIRST_DYNAMIC_COL, ColumnValue::Bytes(bytes)));
            row
        })
        .collect();
    let out = write_block(&rows, &plans, PRODUCTION_ZSTD_LEVEL).expect("encode incompressible");
    column_encoded_bytes(&out, FIRST_DYNAMIC_COL)
}

fn tiny_below_floor_page() -> Vec<u8> {
    let rows: Vec<ResolvedRow> = (0..4)
        .map(|i| {
            let mut row = base_row(BASE_TS_NS + i as i64 * TS_STEP_NS);
            row.body = "ok".to_string();
            row
        })
        .collect();
    let out = write_block(&rows, &[], PRODUCTION_ZSTD_LEVEL).expect("encode tiny page");
    let bytes = column_encoded_bytes(&out, COL_BODY);
    assert!(
        bytes.len() < COMPRESSION_FLOOR,
        "tiny_below_floor dataset must stay under the {COMPRESSION_FLOOR}-byte floor, got {} bytes",
        bytes.len()
    );
    bytes
}

/// LABEL_DICT and SERIES_META whole-section payloads (see the module doc's
/// SERIES_TABLE -> SERIES_META deviation note), recovered from a real RSEG v5
/// object built through [`build_segment_v5`] with a series count safely below
/// `ravel_segment::V5_SPARSE_THRESHOLD` so both sections are emitted whole
/// (not chunked).
fn rseg_whole_section_pages() -> Vec<(String, Vec<u8>)> {
    let config = WorkloadConfig {
        series_count: 500,
        samples_per_series: 2,
        cardinality: CardinalityProfile::many_small(500),
        ..Default::default()
    };
    let raw = generate_raw(&config).expect("generate rseg workload");
    let built = build_segment_v5(raw);
    let obj = built.bytes.as_ref();
    let footer = match parse_footer(obj.len() as u64, obj).expect("parse rseg footer") {
        FooterOutcome::Ready(loc) => loc.footer,
        FooterOutcome::NeedRange { .. } => panic!("whole bench object must cover its own footer"),
    };
    // `Compression` is not re-exported by ravel_segment; ADR-0004 freezes its
    // wire value at 2 for zstd, mirrored here exactly as
    // `ravel_bench::segment_support` mirrors the section_kind numbers.
    const SECTION_COMP_ZSTD: i32 = 2;
    [
        ("label_dict".to_string(), LABEL_DICT),
        ("series_meta".to_string(), SERIES_META),
    ]
    .into_iter()
    .map(|(name, kind)| {
        let section = footer
            .sections
            .iter()
            .find(|s| s.kind == kind)
            .unwrap_or_else(|| panic!("section {kind} present in bench object"));
        let stored = section_bytes(obj, &footer, kind);
        let raw_bytes = if section.comp == SECTION_COMP_ZSTD {
            zstd::bulk::decompress(stored, section.uncompressed_len as usize)
                .expect("decompress rseg section")
        } else {
            stored.to_vec()
        };
        (name, raw_bytes)
    })
    .collect()
}

struct Dataset {
    name: String,
    /// (size label, encoded bytes) points; RSEG and below-floor datasets
    /// carry exactly one point instead of sweeping [`PAGE_ROW_COUNTS`].
    points: Vec<(String, Vec<u8>)>,
}

fn all_datasets() -> Vec<Dataset> {
    let mut out = vec![
        Dataset {
            name: "structured_logs".to_string(),
            points: PAGE_ROW_COUNTS
                .iter()
                .map(|&n| (format!("{n}_rows"), structured_logs_page(n)))
                .collect(),
        },
        Dataset {
            name: "high_entropy_ids".to_string(),
            points: PAGE_ROW_COUNTS
                .iter()
                .map(|&n| (format!("{n}_rows"), high_entropy_ids_page(n)))
                .collect(),
        },
        Dataset {
            name: "low_cardinality_level".to_string(),
            points: PAGE_ROW_COUNTS
                .iter()
                .map(|&n| (format!("{n}_rows"), low_cardinality_level_page(n)))
                .collect(),
        },
        Dataset {
            name: "low_cardinality_service".to_string(),
            points: PAGE_ROW_COUNTS
                .iter()
                .map(|&n| (format!("{n}_rows"), low_cardinality_service_page(n)))
                .collect(),
        },
        Dataset {
            name: "incompressible".to_string(),
            points: PAGE_ROW_COUNTS
                .iter()
                .map(|&n| (format!("{n}_rows"), incompressible_page(n)))
                .collect(),
        },
        Dataset {
            name: "tiny_below_floor".to_string(),
            points: vec![("4_rows".to_string(), tiny_below_floor_page())],
        },
    ];
    for (name, bytes) in rseg_whole_section_pages() {
        out.push(Dataset {
            name: format!("rseg_{name}"),
            points: vec![("whole_section".to_string(), bytes)],
        });
    }
    out
}

// --- arms ------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Arm {
    Raw,
    Zstd1,
    Zstd3,
    Zstd6,
    Zstd3Reused,
    Lz4,
}

const ARMS: [Arm; 6] = [
    Arm::Raw,
    Arm::Zstd1,
    Arm::Zstd3,
    Arm::Zstd6,
    Arm::Zstd3Reused,
    Arm::Lz4,
];

impl Arm {
    fn name(self) -> &'static str {
        match self {
            Arm::Raw => "raw",
            Arm::Zstd1 => "zstd-1",
            Arm::Zstd3 => "zstd-3 (production baseline)",
            Arm::Zstd6 => "zstd-6",
            Arm::Zstd3Reused => "zstd-3-reused",
            Arm::Lz4 => "lz4",
        }
    }
}

/// The exact floor-and-shrink policy [`ravel_logseg::page::write_page`]
/// applies: compress only at or above [`COMPRESSION_FLOOR`], and only keep
/// the compressed form when it is strictly smaller.
fn should_compress(encoded_len: usize, compressed_len: usize) -> bool {
    encoded_len >= COMPRESSION_FLOOR && compressed_len < encoded_len
}

/// Repetitions per inner timed batch, sized down for large pages so a round
/// stays fast without dropping below the timer's noise floor.
fn inner_reps_for(n: usize) -> usize {
    (200_000 / n.max(1)).clamp(3, 200)
}

/// Runs `f` [`WARMUP_CALLS`] times (discarded), then [`MEASURE_ROUNDS`] timed
/// batches of `inner_reps` calls each. Returns the last computed value (so
/// dead-code elimination cannot skip the work) plus (median, min, max)
/// per-call seconds.
fn timed<F: FnMut() -> Vec<u8>>(mut f: F, inner_reps: usize) -> (Vec<u8>, f64, f64, f64) {
    for _ in 0..WARMUP_CALLS {
        let _ = f();
    }
    let mut samples = Vec::with_capacity(MEASURE_ROUNDS);
    let mut last = Vec::new();
    for _ in 0..MEASURE_ROUNDS {
        let start = Instant::now();
        for _ in 0..inner_reps {
            last = f();
        }
        let elapsed = start.elapsed().as_secs_f64() / inner_reps as f64;
        samples.push(elapsed);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).expect("timings are finite"));
    let median = samples[samples.len() / 2];
    let min = samples[0];
    let max = samples[samples.len() - 1];
    (last, median, min, max)
}

fn mb_per_sec(bytes: usize, seconds: f64) -> f64 {
    if seconds <= 0.0 {
        return 0.0;
    }
    (bytes as f64 / 1e6) / seconds
}

#[derive(Serialize)]
struct CellReport {
    dataset: String,
    size_label: String,
    arm: String,
    input_bytes: usize,
    stored_bytes: usize,
    ratio: f64,
    stored_compressed: bool,
    encode_median_us: f64,
    encode_min_us: f64,
    encode_max_us: f64,
    decode_median_us: f64,
    decode_min_us: f64,
    decode_max_us: f64,
    encode_mb_s: f64,
    decode_mb_s: f64,
    roundtrip_ok: bool,
}

/// (median, min, max) seconds-per-call, as returned by [`timed`].
type TimingStats = (f64, f64, f64);
/// One arm's outcome for a cell: stored bytes, whether it compressed, then
/// its encode and decode timing stats.
type ArmOutcome = (Vec<u8>, bool, TimingStats, TimingStats);

/// Measures one (dataset, size, arm) cell. Pushes a human-readable string
/// into `failures` for every integrity violation found (round-trip mismatch,
/// production-baseline policy mismatch, `zstd-3-reused` byte mismatch); never
/// panics on a mismatch itself, so one bad cell does not hide the rest.
fn measure_cell(
    dataset: &str,
    size_label: &str,
    arm: Arm,
    encoded: &[u8],
    reused_compressor: &mut zstd::bulk::Compressor<'static>,
    reused_decompressor: &mut zstd::bulk::Decompressor<'static>,
    failures: &mut Vec<String>,
) -> CellReport {
    let n = encoded.len();
    let reps = inner_reps_for(n);
    let cell_id = format!("{dataset}/{size_label}/{}", arm.name());

    let (stored, stored_compressed, enc_stats, dec_stats): ArmOutcome = match arm {
        Arm::Raw => {
            let (_, em, emin, emax) = timed(|| encoded.to_vec(), reps);
            let (_, dm, dmin, dmax) = timed(|| encoded.to_vec(), reps);
            (encoded.to_vec(), false, (em, emin, emax), (dm, dmin, dmax))
        }
        Arm::Zstd1 | Arm::Zstd3 | Arm::Zstd6 => {
            let level = match arm {
                Arm::Zstd1 => 1,
                Arm::Zstd3 => PRODUCTION_ZSTD_LEVEL,
                Arm::Zstd6 => 6,
                _ => unreachable!(),
            };
            let (compressed, em, emin, emax) = timed(
                || zstd::bulk::compress(encoded, level).expect("zstd compress"),
                reps,
            );
            let use_compressed = should_compress(n, compressed.len());
            let stored = if use_compressed {
                compressed.clone()
            } else {
                encoded.to_vec()
            };
            if arm == Arm::Zstd3 {
                let mut scratch = Vec::new();
                let desc = write_page(&mut scratch, 0, Enc::Plain, encoded, PRODUCTION_ZSTD_LEVEL);
                let production_compressed = desc.comp == COMP_ZSTD;
                if production_compressed != use_compressed {
                    failures.push(format!(
                        "production-baseline policy mismatch on {cell_id}: bake-off decided compress={use_compressed}, page::write_page decided compress={production_compressed}"
                    ));
                }
            }
            let stored_for_decode = stored.clone();
            let (_, dm, dmin, dmax) = timed(
                || {
                    if use_compressed {
                        zstd::bulk::decompress(&stored_for_decode, n).expect("zstd decompress")
                    } else {
                        stored_for_decode.clone()
                    }
                },
                reps,
            );
            (stored, use_compressed, (em, emin, emax), (dm, dmin, dmax))
        }
        Arm::Zstd3Reused => {
            let (compressed, em, emin, emax) = timed(
                || {
                    reused_compressor
                        .compress(encoded)
                        .expect("reused zstd compress")
                },
                reps,
            );
            let one_shot = zstd::bulk::compress(encoded, PRODUCTION_ZSTD_LEVEL)
                .expect("zstd compress for identity check");
            if compressed != one_shot {
                failures.push(format!(
                    "zstd-3-reused NOT byte-identical to zstd-3 one-shot on {cell_id}"
                ));
            }
            let use_compressed = should_compress(n, compressed.len());
            let stored = if use_compressed {
                compressed.clone()
            } else {
                encoded.to_vec()
            };
            let stored_for_decode = stored.clone();
            let (_, dm, dmin, dmax) = timed(
                || {
                    if use_compressed {
                        reused_decompressor
                            .decompress(&stored_for_decode, n)
                            .expect("reused zstd decompress")
                    } else {
                        stored_for_decode.clone()
                    }
                },
                reps,
            );
            (stored, use_compressed, (em, emin, emax), (dm, dmin, dmax))
        }
        Arm::Lz4 => {
            let (compressed, em, emin, emax) =
                timed(|| lz4_flex::compress_prepend_size(encoded), reps);
            let use_compressed = should_compress(n, compressed.len());
            let stored = if use_compressed {
                compressed.clone()
            } else {
                encoded.to_vec()
            };
            let stored_for_decode = stored.clone();
            let (_, dm, dmin, dmax) = timed(
                || {
                    if use_compressed {
                        lz4_flex::decompress_size_prepended(&stored_for_decode)
                            .expect("lz4 decompress")
                    } else {
                        stored_for_decode.clone()
                    }
                },
                reps,
            );
            (stored, use_compressed, (em, emin, emax), (dm, dmin, dmax))
        }
    };

    let decoded = if stored_compressed {
        match arm {
            Arm::Zstd1 | Arm::Zstd3 | Arm::Zstd6 => {
                zstd::bulk::decompress(&stored, n).expect("verify: zstd decompress")
            }
            Arm::Zstd3Reused => reused_decompressor
                .decompress(&stored, n)
                .expect("verify: reused zstd decompress"),
            Arm::Lz4 => {
                lz4_flex::decompress_size_prepended(&stored).expect("verify: lz4 decompress")
            }
            Arm::Raw => unreachable!("raw arm never sets stored_compressed"),
        }
    } else {
        stored.clone()
    };
    let roundtrip_ok = decoded == encoded;
    if !roundtrip_ok {
        failures.push(format!("round-trip FAILED for {cell_id}"));
    }

    CellReport {
        dataset: dataset.to_string(),
        size_label: size_label.to_string(),
        arm: arm.name().to_string(),
        input_bytes: n,
        stored_bytes: stored.len(),
        ratio: stored.len() as f64 / n.max(1) as f64,
        stored_compressed,
        encode_median_us: enc_stats.0 * 1e6,
        encode_min_us: enc_stats.1 * 1e6,
        encode_max_us: enc_stats.2 * 1e6,
        decode_median_us: dec_stats.0 * 1e6,
        decode_min_us: dec_stats.1 * 1e6,
        decode_max_us: dec_stats.2 * 1e6,
        encode_mb_s: mb_per_sec(n, enc_stats.0),
        decode_mb_s: mb_per_sec(n, dec_stats.0),
        roundtrip_ok,
    }
}

// --- cell completeness -----------------------------------------------------

type CellKey = (String, String, String);

fn expected_cells(datasets: &[Dataset]) -> Vec<CellKey> {
    let mut out = Vec::new();
    for d in datasets {
        for (size_label, _) in &d.points {
            for arm in ARMS {
                out.push((d.name.clone(), size_label.clone(), arm.name().to_string()));
            }
        }
    }
    out
}

/// Every expected cell present exactly once in `emitted`: catches both a
/// dropped cell (missing from the set) and a duplicated one (set equality
/// alone would hide it, so the lengths must also agree).
fn check_cell_completeness(expected: &[CellKey], emitted: &[CellKey]) -> Result<(), String> {
    use std::collections::HashSet;
    if expected.len() != emitted.len() {
        return Err(format!(
            "cell count mismatch: expected {} cells, emitted {}",
            expected.len(),
            emitted.len()
        ));
    }
    let expected_set: HashSet<&CellKey> = expected.iter().collect();
    let emitted_set: HashSet<&CellKey> = emitted.iter().collect();
    if emitted_set.len() != emitted.len() {
        return Err("emitted cells contain a duplicate".to_string());
    }
    if expected_set != emitted_set {
        let missing: Vec<&&CellKey> = expected_set.difference(&emitted_set).collect();
        let extra: Vec<&&CellKey> = emitted_set.difference(&expected_set).collect();
        return Err(format!(
            "cell set mismatch: missing={missing:?} extra={extra:?}"
        ));
    }
    Ok(())
}

// --- provenance header ------------------------------------------------------

fn target_triple() -> String {
    let Ok(output) = std::process::Command::new("rustc").arg("-vV").output() else {
        return "unknown".to_string();
    };
    let Ok(text) = String::from_utf8(output.stdout) else {
        return "unknown".to_string();
    };
    text.lines()
        .find_map(|l| l.strip_prefix("host: "))
        .map(str::to_string)
        .unwrap_or_else(|| "unknown".to_string())
}

fn build_profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

fn load_average() -> String {
    let Ok(text) = std::fs::read_to_string("/proc/loadavg") else {
        return "unknown".to_string();
    };
    let fields: Vec<&str> = text.split_whitespace().take(3).collect();
    if fields.len() == 3 {
        fields.join(" ")
    } else {
        "unknown".to_string()
    }
}

/// Version of `crate_name` as cargo actually resolved it for this crate,
/// read from `cargo metadata` rather than the workspace manifest's version
/// range. Best-effort: `"unknown"` on any failure, matching
/// `ravel_bench::bench_env`'s philosophy of never failing the stamp.
fn resolved_dependency_version(crate_name: &str) -> String {
    let Ok(output) = std::process::Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps", "-q"])
        .output()
    else {
        return dependency_version_from_full_graph(crate_name);
    };
    let _ = output;
    dependency_version_from_full_graph(crate_name)
}

fn dependency_version_from_full_graph(crate_name: &str) -> String {
    let Ok(output) = std::process::Command::new("cargo")
        .args(["metadata", "--format-version", "1", "-q"])
        .output()
    else {
        return "unknown".to_string();
    };
    if !output.status.success() {
        return "unknown".to_string();
    }
    let Ok(json) = serde_json::from_slice::<serde_json::Value>(&output.stdout) else {
        return "unknown".to_string();
    };
    let Some(packages) = json.get("packages").and_then(|p| p.as_array()) else {
        return "unknown".to_string();
    };
    // ravel-bench's own edge, not just any crate of this name in the graph:
    // find ravel-bench's node in the resolve graph and read the version its
    // dependency edge actually names. Package ids are opaque PackageIdSpec
    // strings (e.g. `path+file:///.../ravel-bench#0.15.0`); resolve the id via
    // the packages array by name rather than pattern-matching the id shape.
    let Some(bench_pkg_id) = packages
        .iter()
        .find(|p| p.get("name").and_then(|n| n.as_str()) == Some("ravel-bench"))
        .and_then(|p| p.get("id").and_then(|i| i.as_str()))
    else {
        return "unknown".to_string();
    };
    let Some(resolve) = json.get("resolve") else {
        return "unknown".to_string();
    };
    let Some(nodes) = resolve.get("nodes").and_then(|n| n.as_array()) else {
        return "unknown".to_string();
    };
    let bench_node = nodes
        .iter()
        .find(|n| n.get("id").and_then(|i| i.as_str()) == Some(bench_pkg_id));
    let Some(bench_node) = bench_node else {
        return "unknown".to_string();
    };
    let Some(deps) = bench_node.get("deps").and_then(|d| d.as_array()) else {
        return "unknown".to_string();
    };
    let Some(dep_pkg_id) = deps
        .iter()
        .find(|d| d.get("name").and_then(|n| n.as_str()) == Some(crate_name))
        .and_then(|d| d.get("pkg").and_then(|p| p.as_str()))
    else {
        return "unknown".to_string();
    };
    packages
        .iter()
        .find(|p| p.get("id").and_then(|i| i.as_str()) == Some(dep_pkg_id))
        .and_then(|p| p.get("version"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| "unknown".to_string())
}

// --- main --------------------------------------------------------------

fn main() {
    let load_start = load_average();
    let mut reused_compressor =
        zstd::bulk::Compressor::new(PRODUCTION_ZSTD_LEVEL).expect("build reused zstd compressor");
    let mut reused_decompressor =
        zstd::bulk::Decompressor::new().expect("build reused zstd decompressor");

    let datasets = all_datasets();
    let mut failures: Vec<String> = Vec::new();
    let mut emitted: Vec<CellKey> = Vec::new();
    let mut reports: Vec<CellReport> = Vec::new();

    for dataset in &datasets {
        for (size_label, encoded) in &dataset.points {
            for arm in ARMS {
                let report = measure_cell(
                    &dataset.name,
                    size_label,
                    arm,
                    encoded,
                    &mut reused_compressor,
                    &mut reused_decompressor,
                    &mut failures,
                );
                emitted.push((
                    dataset.name.clone(),
                    size_label.clone(),
                    arm.name().to_string(),
                ));
                reports.push(report);
            }
        }
    }

    let expected = expected_cells(&datasets);
    if let Err(e) = check_cell_completeness(&expected, &emitted) {
        failures.push(e);
    }

    let load_end = load_average();

    print!("{}", env_header("page_codec_bakeoff"));
    println!("commit:        {}", git_commit());
    println!("profile:       {}", build_profile());
    println!("target:        {}", target_triple());
    println!("zstd crate:    {}", resolved_dependency_version("zstd"));
    println!("lz4_flex crate:{}", resolved_dependency_version("lz4_flex"));
    println!("load avg (start): {load_start}");
    println!("load avg (end):   {load_end}");
    println!("========================================================================");
    println!(
        "{:<24} {:<12} {:<28} {:>10} {:>10} {:>8} {:>6} {:>10} {:>10} {:>6}",
        "dataset",
        "size",
        "arm",
        "in_bytes",
        "out_bytes",
        "ratio",
        "comp?",
        "enc_MB/s",
        "dec_MB/s",
        "rt_ok"
    );
    for r in &reports {
        println!(
            "{:<24} {:<12} {:<28} {:>10} {:>10} {:>8.3} {:>6} {:>10.1} {:>10.1} {:>6}",
            r.dataset,
            r.size_label,
            r.arm,
            r.input_bytes,
            r.stored_bytes,
            r.ratio,
            r.stored_compressed,
            r.encode_mb_s,
            r.decode_mb_s,
            r.roundtrip_ok,
        );
    }

    println!("========================================================================");
    println!("JSON (one object per line, for mechanical diffing):");
    for r in &reports {
        println!(
            "{}",
            serde_json::to_string(r).expect("serialize cell report")
        );
    }

    if !failures.is_empty() {
        eprintln!("========================================================================");
        eprintln!("FAILURES ({}):", failures.len());
        for f in &failures {
            eprintln!("  - {f}");
        }
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Same rule as [`should_compress`] but with the floor mismodeled as `>`
    /// instead of `>=` -- the exact off-by-one issue #1509 asks the
    /// production-baseline check to be able to catch. Test-only: this never
    /// touches `main`'s actual policy, it exists to prove
    /// `production_baseline_policy_matches_write_page` below has teeth.
    fn should_compress_buggy_floor(encoded_len: usize, compressed_len: usize) -> bool {
        encoded_len > COMPRESSION_FLOOR && compressed_len < encoded_len
    }

    fn compressible_payload(len: usize) -> Vec<u8> {
        // Highly repetitive so zstd always shrinks it once the floor is met.
        (0..len).map(|i| (i % 4) as u8).collect()
    }

    /// At exactly the 512-byte floor a compressible page must compress under
    /// the real `page::write_page` rule; the correct predicate agrees, the
    /// buggy `>`-floor variant disagrees. Demonstrates catching the mutation
    /// issue #1509 names: substituting `>` for `>=` at the boundary.
    #[test]
    fn production_baseline_policy_matches_write_page_at_the_floor() {
        for &len in &[511usize, 512, 513] {
            let encoded = compressible_payload(len);
            let mut scratch = Vec::new();
            let desc = write_page(&mut scratch, 0, Enc::Plain, &encoded, PRODUCTION_ZSTD_LEVEL);
            let production_compressed = desc.comp == COMP_ZSTD;

            let compressed =
                zstd::bulk::compress(&encoded, PRODUCTION_ZSTD_LEVEL).expect("zstd compress");
            let predicted = should_compress(len, compressed.len());
            assert_eq!(
                predicted, production_compressed,
                "should_compress disagreed with page::write_page at len={len}"
            );

            if len >= COMPRESSION_FLOOR {
                // The buggy `>`-floor variant only diverges exactly at the
                // floor boundary (511 is below either way, 513 is above
                // either way); this is where it must disagree.
                if len == COMPRESSION_FLOOR {
                    let buggy = should_compress_buggy_floor(len, compressed.len());
                    assert_ne!(
                        buggy, production_compressed,
                        "buggy `>` floor variant should disagree with page::write_page exactly at len=512"
                    );
                }
            }
        }
    }

    /// Corrupting one byte of a decoded page must be caught by the same
    /// equality the binary uses for its round-trip check.
    #[test]
    fn roundtrip_check_rejects_a_single_flipped_byte() {
        let encoded = compressible_payload(4000);
        let compressed = zstd::bulk::compress(&encoded, PRODUCTION_ZSTD_LEVEL).expect("compress");
        let decoded = zstd::bulk::decompress(&compressed, encoded.len()).expect("decompress");
        assert_eq!(decoded, encoded, "clean round-trip must be bit-exact");

        let mut corrupted = decoded.clone();
        corrupted[0] ^= 0xFF;
        assert_ne!(
            corrupted, encoded,
            "flipping byte 0 of the decoded copy must be detected as a round-trip failure"
        );
    }

    /// A dropped cell (line under test: the `emitted.remove(3)` below) must
    /// fail [`check_cell_completeness`]; a checker that only asserted
    /// non-emptiness would pass this dataset regardless.
    #[test]
    fn cell_completeness_fails_on_a_dropped_cell() {
        let expected: Vec<CellKey> = (0..2)
            .flat_map(|d| {
                (0..2).flat_map(move |s| {
                    (0..2).map(move |a| (format!("d{d}"), format!("s{s}"), format!("a{a}")))
                })
            })
            .collect();
        assert_eq!(expected.len(), 8);

        let complete = expected.clone();
        assert!(check_cell_completeness(&expected, &complete).is_ok());

        let mut dropped = expected.clone();
        // Remove index 3 (the 2nd size's 1st arm of the 1st dataset) to
        // simulate one cell silently missing from the run.
        dropped.remove(3);
        let err = check_cell_completeness(&expected, &dropped)
            .expect_err("dropping one cell must fail completeness");
        assert!(err.contains("mismatch"), "unexpected error message: {err}");
    }

    /// A duplicated cell must also fail: set equality alone (ignoring count)
    /// would hide this, so the checker must compare lengths too.
    #[test]
    fn cell_completeness_fails_on_a_duplicated_cell() {
        let expected: Vec<CellKey> = vec![
            ("d0".to_string(), "s0".to_string(), "a0".to_string()),
            ("d0".to_string(), "s0".to_string(), "a1".to_string()),
        ];
        let mut duplicated = expected.clone();
        duplicated.push(expected[0].clone());
        let err = check_cell_completeness(&expected, &duplicated)
            .expect_err("a duplicated cell must fail completeness");
        assert!(err.contains("mismatch"), "unexpected error message: {err}");
    }

    /// The tiny dataset's whole point is proving the floor is respected: its
    /// one page must actually land under [`COMPRESSION_FLOOR`].
    #[test]
    fn tiny_below_floor_dataset_stays_under_the_floor() {
        let page = tiny_below_floor_page();
        assert!(page.len() < COMPRESSION_FLOOR);
        let mut scratch = Vec::new();
        let desc = write_page(&mut scratch, 0, Enc::Plain, &page, PRODUCTION_ZSTD_LEVEL);
        assert_eq!(
            desc.comp,
            ravel_logseg::page::COMP_NONE,
            "a page below the floor must stay raw under page::write_page"
        );
    }
}
