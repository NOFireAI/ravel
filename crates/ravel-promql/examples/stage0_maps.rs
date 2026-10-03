//! Stage 0 measurement driver for issue #2443 (epic #2425): what share of
//! PromQL aggregation's and vector matching's own evaluation time is index
//! work (hashing keys, cloning keys into the index, probing, inserting), and
//! how many probes hit against miss.
//!
//! Calls the grouping/matching operators directly
//! (`ravel_promql::stage0_eval_sum_by`, `ravel_promql::stage0_one_to_one_add`),
//! bypassing `eval_aggregate`/`eval_binary`'s expression evaluation,
//! `Evaluator`/`SeriesSource` machinery, and (for aggregation) the
//! pre-dispatch sort of the input vector, none of which touch the index
//! work being measured. No storage, no parser.
//!
//! Three cases, built in memory, inputs constructed outside the timed
//! region:
//! - AGG_100: `sum by (group_a, group_b)` over 100,000 series, exactly 100
//!   output groups.
//! - AGG_10000: the same, exactly 10,000 output groups.
//! - MATCH_HALF: `lhs + rhs`, one-to-one, matching on the full label set (no
//!   `on`/`ignoring`: this crate's default, same as Prometheus'), between
//!   two 100,000-series vectors where exactly 50,000 left-side series have a
//!   partner on the right.
//!
//! Every series carries six labels (`__name__` plus five others), names 6 to
//! 12 bytes, values 8 to 24 bytes.
//!
//! Mode 0 is the unmodified operator; mode 1 is the same operator plus the
//! stage 0 replay/counting described in `ravel_promql::stage0`. One
//! iteration times the operator call alone. 3 warm-ups discarded, then 5
//! runs of 10 iterations each; modes interleave inside each run, cases
//! interleave inside each mode.

use ravel_promql::{InstantSample, InstantVector, QueryWindow, stage0_eval_sum_by, stage0_one_to_one_add};
use ravel_types::{Label, LabelSet};
use std::time::{Duration, Instant};

const SERIES_COUNT: usize = 100_000;
const WARMUP_ITERS: usize = 3;
const RUNS: usize = 5;
const ITERS_PER_RUN: usize = 10;

fn label(name: &str, value: String) -> Label {
    assert!(
        (6..=12).contains(&name.len()),
        "label name {name:?} must be 6 to 12 bytes, was {}",
        name.len()
    );
    assert!(
        (8..=24).contains(&value.len()),
        "label value {value:?} (for {name}) must be 8 to 24 bytes, was {}",
        value.len()
    );
    Label {
        name: name.to_string(),
        value,
    }
}

/// Six labels: `__name__` plus five realistic-length labels. `group_a`/
/// `group_b` are the two aggregation-grouping dimensions; `job_name`,
/// `instance_id`, `pod_name` vary with `id` but are not grouped on (dropped
/// by `by (group_a, group_b)`), so they have no effect on group count — they
/// are there only so every series carries six realistic labels, not two.
fn agg_labels(id: usize, group_a: usize, group_b: usize) -> LabelSet {
    LabelSet::new(vec![
        label("__name__", "http_requests_total".to_string()),
        label("job_name", format!("svc-{id:06}")),
        label("instance_id", format!("host-{id:08}")),
        label("pod_name", format!("pod-{id:08}")),
        label("group_a", format!("grpval-{group_a:03}")),
        label("group_b", format!("grpval-{group_b:03}")),
    ])
    .expect("distinct label names")
}

fn build_agg_vector(groups_a: usize, groups_b: usize) -> InstantVector {
    (0..SERIES_COUNT)
        .map(|i| {
            let a = i % groups_a;
            let b = (i / groups_a) % groups_b;
            InstantSample {
                labels: agg_labels(i, a, b),
                ts_ns: 0,
                orig_sample_ts_ns: 0,
                value: (i % 97) as f64,
                histogram: None,
            }
        })
        .collect()
}

/// Six labels: `__name__` plus five that all participate in the (default,
/// full-label-set) matching signature. `job_name`/`instance_id`/`pod_name`
/// are derived from `id` and so are unique per `id`; `region`/`zone_id`
/// cycle through a small set purely for label-value realism and add no
/// collision risk, since `job_name`/`instance_id`/`pod_name` alone already
/// make the five-label signature unique per `id`.
fn match_labels(id: usize, metric_name: &str) -> LabelSet {
    const REGIONS: [&str; 4] = ["us-east-1", "us-west-2", "eu-west-1", "ap-south-1"];
    LabelSet::new(vec![
        label("__name__", metric_name.to_string()),
        label("job_name", format!("svc-{id:06}")),
        label("instance_id", format!("host-{id:08}")),
        label("pod_name", format!("pod-{id:08}")),
        label("region", REGIONS[id % REGIONS.len()].to_string()),
        label("zone_id", format!("z-{:06}", id % 16)),
    ])
    .expect("distinct label names")
}

/// `lhs` has `SERIES_COUNT` series with ids `0..SERIES_COUNT`. `rhs` shares
/// ids `0..SERIES_COUNT/2` with `lhs` (so those match) and uses ids
/// `SERIES_COUNT..SERIES_COUNT*3/2` for its other half (disjoint from every
/// `lhs` id, so those never match anything). Every id, on either side, is
/// globally unique, so no accidental duplicate-signature ambiguous-match
/// error is possible, and `lhs`'s unmatched half (ids `SERIES_COUNT/2..`)
/// has no partner by construction.
fn build_match_vectors() -> (InstantVector, InstantVector) {
    let half = SERIES_COUNT / 2;
    let lhs: InstantVector = (0..SERIES_COUNT)
        .map(|id| InstantSample {
            labels: match_labels(id, "left_metric"),
            ts_ns: 0,
            orig_sample_ts_ns: 0,
            value: (id % 97) as f64,
            histogram: None,
        })
        .collect();
    let rhs: InstantVector = (0..SERIES_COUNT)
        .map(|i| {
            let id = if i < half { i } else { SERIES_COUNT + i };
            InstantSample {
                labels: match_labels(id, "right_metric"),
                ts_ns: 0,
                orig_sample_ts_ns: 0,
                value: (i % 53) as f64,
                histogram: None,
            }
        })
        .collect();
    (lhs, rhs)
}

#[derive(Clone, Copy, Default)]
struct Totals {
    op_ns: u64,
    agg_index_ns: u64,
    agg_key_build_ns: u64,
    agg_hits: u64,
    agg_misses: u64,
    match_index_ns: u64,
    match_key_build_ns: u64,
    match_probe_hits: u64,
    match_probe_misses: u64,
    output_len: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Case {
    Agg100,
    Agg10000,
    MatchHalf,
}

const CASES: [Case; 3] = [Case::Agg100, Case::Agg10000, Case::MatchHalf];

fn run_case(case: Case, mode: u8, out: &mut Totals) {
    ravel_promql::stage0::set_mode(mode);
    ravel_promql::stage0::reset();
    let ctx = QueryWindow::bare(0, 0);

    let op_ns = match case {
        Case::Agg100 => {
            let input = build_agg_vector(10, 10);
            let t0 = Instant::now();
            let result = stage0_eval_sum_by(&["group_a", "group_b"], input, 0, &ctx);
            let elapsed = t0.elapsed();
            out.output_len = result.len();
            elapsed
        }
        Case::Agg10000 => {
            let input = build_agg_vector(100, 100);
            let t0 = Instant::now();
            let result = stage0_eval_sum_by(&["group_a", "group_b"], input, 0, &ctx);
            let elapsed = t0.elapsed();
            out.output_len = result.len();
            elapsed
        }
        Case::MatchHalf => {
            let (lhs, rhs) = build_match_vectors();
            let t0 = Instant::now();
            let result = stage0_one_to_one_add(lhs, rhs, &ctx).expect("no ambiguous match");
            let elapsed = t0.elapsed();
            out.output_len = result.len();
            elapsed
        }
    };
    out.op_ns = elapsed_ns(op_ns);

    if mode == 1 {
        let snap = ravel_promql::stage0::snapshot();
        match case {
            Case::Agg100 | Case::Agg10000 => {
                out.agg_index_ns = snap.agg_index_ns;
                out.agg_key_build_ns = snap.agg_key_build_ns;
                out.agg_hits = snap.agg_hits;
                out.agg_misses = snap.agg_misses;
            }
            Case::MatchHalf => {
                out.match_index_ns = snap.match_index_ns();
                out.match_key_build_ns = snap.match_key_build_ns;
                out.match_probe_hits = snap.match_probe_hits;
                out.match_probe_misses = snap.match_probe_misses;
            }
        }
    }
}

fn elapsed_ns(d: Duration) -> u64 {
    d.as_nanos() as u64
}

fn assert_eq_u64(label: &str, got: u64, want: u64) {
    if got != want {
        eprintln!("ASSERTION FAILED: {label}: got {got}, want {want}");
        std::process::exit(1);
    }
}

fn main() {
    // Warm-ups: discarded, both modes, every case, so allocator/cache state
    // is representative before any measured run.
    for _ in 0..WARMUP_ITERS {
        for mode in [0u8, 1u8] {
            for case in CASES {
                let mut t = Totals::default();
                run_case(case, mode, &mut t);
            }
        }
    }

    // run_results[case][mode] = one Vec<Totals> per run (RUNS entries), each
    // entry itself a mean over ITERS_PER_RUN iterations.
    let mut run_means: [[Vec<Totals>; 2]; 3] = Default::default();

    for _run in 0..RUNS {
        let mut sums: [[Totals; 2]; 3] = [[Totals::default(); 2]; 3];
        for _iter in 0..ITERS_PER_RUN {
            // Modes interleaved inside each run, cases interleaved inside
            // each mode, per the pre-registered method.
            for mode in [0u8, 1u8] {
                for (ci, case) in CASES.iter().enumerate() {
                    let mut t = Totals::default();
                    run_case(*case, mode, &mut t);
                    let s = &mut sums[ci][mode as usize];
                    s.op_ns += t.op_ns;
                    s.agg_index_ns += t.agg_index_ns;
                    s.agg_key_build_ns += t.agg_key_build_ns;
                    s.agg_hits += t.agg_hits;
                    s.agg_misses += t.agg_misses;
                    s.match_index_ns += t.match_index_ns;
                    s.match_key_build_ns += t.match_key_build_ns;
                    s.match_probe_hits += t.match_probe_hits;
                    s.match_probe_misses += t.match_probe_misses;
                    s.output_len = t.output_len;
                }
            }
        }
        for ci in 0..3 {
            for mode in 0..2 {
                let s = sums[ci][mode];
                let n = ITERS_PER_RUN as u64;
                run_means[ci][mode].push(Totals {
                    op_ns: s.op_ns / n,
                    agg_index_ns: s.agg_index_ns / n,
                    agg_key_build_ns: s.agg_key_build_ns / n,
                    agg_hits: s.agg_hits / n,
                    agg_misses: s.agg_misses / n,
                    match_index_ns: s.match_index_ns / n,
                    match_key_build_ns: s.match_key_build_ns / n,
                    match_probe_hits: s.match_probe_hits / n,
                    match_probe_misses: s.match_probe_misses / n,
                    output_len: s.output_len,
                });
            }
        }
    }

    // Assertions (pre-registered, issue #2443). Exit non-zero on violation.
    for ci in 0..3 {
        let mode1 = &run_means[ci][1];
        let last = *mode1.last().expect("at least one run");
        match CASES[ci] {
            Case::Agg100 => {
                assert_eq_u64("AGG_100 output groups", last.output_len as u64, 100);
                assert_eq_u64(
                    "AGG_100 probes",
                    last.agg_hits + last.agg_misses,
                    SERIES_COUNT as u64,
                );
                assert_eq_u64("AGG_100 misses", last.agg_misses, 100);
            }
            Case::Agg10000 => {
                assert_eq_u64("AGG_10000 output groups", last.output_len as u64, 10_000);
                assert_eq_u64(
                    "AGG_10000 probes",
                    last.agg_hits + last.agg_misses,
                    SERIES_COUNT as u64,
                );
                assert_eq_u64("AGG_10000 misses", last.agg_misses, 10_000);
            }
            Case::MatchHalf => {
                assert_eq_u64(
                    "MATCH_HALF output series",
                    last.output_len as u64,
                    (SERIES_COUNT / 2) as u64,
                );
                assert_eq_u64(
                    "MATCH_HALF probing-side misses",
                    last.match_probe_misses,
                    (SERIES_COUNT / 2) as u64,
                );
            }
        }
    }

    print_report(&run_means);
}

fn median_min_max(values: &[u64]) -> (f64, u64, u64) {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let mid = sorted.len() / 2;
    let median = if sorted.len() % 2 == 0 {
        (sorted[mid - 1] + sorted[mid]) as f64 / 2.0
    } else {
        sorted[mid] as f64
    };
    (median, *sorted.first().unwrap(), *sorted.last().unwrap())
}

fn print_report(run_means: &[[Vec<Totals>; 2]; 3]) {
    println!("case,mode,run_idx,op_ns,index_ns,key_build_ns,hits,misses,output_len");
    for ci in 0..3 {
        let name = match CASES[ci] {
            Case::Agg100 => "AGG_100",
            Case::Agg10000 => "AGG_10000",
            Case::MatchHalf => "MATCH_HALF",
        };
        for mode in 0..2 {
            for (ri, t) in run_means[ci][mode].iter().enumerate() {
                let (index_ns, key_build_ns, hits, misses) = match CASES[ci] {
                    Case::Agg100 | Case::Agg10000 => {
                        (t.agg_index_ns, t.agg_key_build_ns, t.agg_hits, t.agg_misses)
                    }
                    Case::MatchHalf => (
                        t.match_index_ns,
                        t.match_key_build_ns,
                        t.match_probe_hits,
                        t.match_probe_misses,
                    ),
                };
                println!(
                    "{name},{mode},{ri},{},{index_ns},{key_build_ns},{hits},{misses},{}",
                    t.op_ns, t.output_len
                );
            }
        }
    }

    println!();
    println!("--- summary (median [min, max] over {RUNS} run-means) ---");
    for ci in 0..3 {
        let name = match CASES[ci] {
            Case::Agg100 => "AGG_100",
            Case::Agg10000 => "AGG_10000",
            Case::MatchHalf => "MATCH_HALF",
        };
        let mode0_op: Vec<u64> = run_means[ci][0].iter().map(|t| t.op_ns).collect();
        let mode1_op: Vec<u64> = run_means[ci][1].iter().map(|t| t.op_ns).collect();
        let (m0_med, m0_min, m0_max) = median_min_max(&mode0_op);
        let (m1_med, m1_min, m1_max) = median_min_max(&mode1_op);
        println!("{name}: mode0 op_ns median={m0_med} min={m0_min} max={m0_max}");
        println!("{name}: mode1 op_ns median={m1_med} min={m1_min} max={m1_max}");

        let index_ns: Vec<u64> = run_means[ci][1]
            .iter()
            .map(|t| match CASES[ci] {
                Case::Agg100 | Case::Agg10000 => t.agg_index_ns,
                Case::MatchHalf => t.match_index_ns,
            })
            .collect();
        let key_build_ns: Vec<u64> = run_means[ci][1]
            .iter()
            .map(|t| match CASES[ci] {
                Case::Agg100 | Case::Agg10000 => t.agg_key_build_ns,
                Case::MatchHalf => t.match_key_build_ns,
            })
            .collect();
        let (idx_med, idx_min, idx_max) = median_min_max(&index_ns);
        let (kb_med, kb_min, kb_max) = median_min_max(&key_build_ns);
        println!("{name}: replayed index_ns median={idx_med} min={idx_min} max={idx_max}");
        println!("{name}: KEY_BUILD_ns median={kb_med} min={kb_min} max={kb_max}");

        let share: Vec<f64> = run_means[ci][1]
            .iter()
            .map(|t| {
                let idx = match CASES[ci] {
                    Case::Agg100 | Case::Agg10000 => t.agg_index_ns,
                    Case::MatchHalf => t.match_index_ns,
                } as f64;
                let rest = t.op_ns as f64 - idx;
                100.0 * idx / rest
            })
            .collect();
        let mut sorted_share = share.clone();
        sorted_share.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mid = sorted_share.len() / 2;
        let share_med = if sorted_share.len() % 2 == 0 {
            (sorted_share[mid - 1] + sorted_share[mid]) / 2.0
        } else {
            sorted_share[mid]
        };
        println!(
            "{name}: INDEX_SHARE median={share_med:.2}% min={:.2}% max={:.2}%",
            sorted_share.first().unwrap(),
            sorted_share.last().unwrap()
        );

        let kb_share: Vec<f64> = run_means[ci][1]
            .iter()
            .map(|t| {
                let idx = match CASES[ci] {
                    Case::Agg100 | Case::Agg10000 => t.agg_index_ns,
                    Case::MatchHalf => t.match_index_ns,
                } as f64;
                let kb = match CASES[ci] {
                    Case::Agg100 | Case::Agg10000 => t.agg_key_build_ns,
                    Case::MatchHalf => t.match_key_build_ns,
                } as f64;
                let rest = t.op_ns as f64 - idx;
                100.0 * kb / rest
            })
            .collect();
        let mut sorted_kb_share = kb_share.clone();
        sorted_kb_share.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mid = sorted_kb_share.len() / 2;
        let kb_share_med = if sorted_kb_share.len() % 2 == 0 {
            (sorted_kb_share[mid - 1] + sorted_kb_share[mid]) / 2.0
        } else {
            sorted_kb_share[mid]
        };
        println!(
            "{name}: KEY_BUILD_SHARE median={kb_share_med:.2}% min={:.2}% max={:.2}%",
            sorted_kb_share.first().unwrap(),
            sorted_kb_share.last().unwrap()
        );

        let last = run_means[ci][1].last().unwrap();
        let (hits, misses) = match CASES[ci] {
            Case::Agg100 | Case::Agg10000 => (last.agg_hits, last.agg_misses),
            Case::MatchHalf => (last.match_probe_hits, last.match_probe_misses),
        };
        println!("{name}: hits={hits} misses={misses} output_len={}", last.output_len);
        println!();
    }
}
