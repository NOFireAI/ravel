//! Stage 0 measurement for issue #2426: share of RLOG row-path encode time
//! spent in the `ref_of` map. See stage0-ref-of.md at the repository root.

#[path = "../benches/common/mod.rs"]
mod common;

use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

use common::{bench_config, bench_identity, build_corpus};
use ravel_logseg::writer::stage0;
use ravel_logseg::{LogRecord, RlogWriter};

const RECORDS: u64 = 20_000;
const WARMUP: usize = 3;
const RUNS: usize = 5;
const ITERS: usize = 20;

const SHAPES: [(&str, usize, usize); 3] = [
    ("1_stream", 1, 20_000),
    ("1000_streams", 1_000, 20),
    ("20000_streams", 20_000, 1),
];

fn encode(corpus: Vec<LogRecord>) -> Vec<u8> {
    let mut w = RlogWriter::new(bench_config(), bench_identity());
    for r in corpus {
        w.push(r).expect("push");
    }
    w.finish().expect("finish")
}

fn fail(msg: String) -> ! {
    eprintln!("ASSERTION FAILED: {msg}");
    std::process::exit(1);
}

#[derive(Default, Clone, Copy)]
struct Cell {
    encode_ns: f64,
    build_ns: f64,
    replay_ns: f64,
    lookups: f64,
    entries: u64,
}

fn median(v: &mut Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let corpora: Vec<Vec<LogRecord>> = SHAPES
        .iter()
        .map(|&(_, s, p)| {
            let c = build_corpus(s, p);
            assert_eq!(c.len() as u64, RECORDS);
            c
        })
        .collect();

    // Byte identity of mode 2 against mode 0, before any timing.
    for (i, &(name, _, _)) in SHAPES.iter().enumerate() {
        stage0::MODE.store(0, Relaxed);
        let a = encode(corpora[i].clone());
        stage0::MODE.store(2, Relaxed);
        let b = encode(corpora[i].clone());
        if a != b {
            fail(format!("mode 2 bytes differ from mode 0 for {name}"));
        }
        println!("bytes identical mode0==mode2 shape={name} len={}", a.len());
    }

    // cells[run][mode][shape]
    let mut cells = [[[Cell::default(); 3]; 3]; RUNS];

    for (si, _) in SHAPES.iter().enumerate() {
        for mode in 0..3u8 {
            stage0::MODE.store(mode, Relaxed);
            for _ in 0..WARMUP {
                let _ = encode(corpora[si].clone());
            }
        }
    }

    for run in 0..RUNS {
        for mode in 0..3u8 {
            stage0::MODE.store(mode, Relaxed);
            for (si, &(name, streams, _)) in SHAPES.iter().enumerate() {
                let mut total_ns = 0u128;
                let mut build = 0u64;
                let mut replay = 0u64;
                for _ in 0..ITERS {
                    let corpus = corpora[si].clone();
                    stage0::BUILD_NS.store(0, Relaxed);
                    stage0::REPLAY_NS.store(0, Relaxed);
                    stage0::LOOKUPS.store(0, Relaxed);
                    stage0::ENTRIES.store(0, Relaxed);
                    let t = Instant::now();
                    let out = encode(corpus);
                    total_ns += t.elapsed().as_nanos();
                    std::hint::black_box(out);
                    if mode == 1 {
                        let l = stage0::LOOKUPS.load(Relaxed);
                        let e = stage0::ENTRIES.load(Relaxed);
                        if l != RECORDS {
                            fail(format!("{name}: lookups {l} != {RECORDS}"));
                        }
                        if e != streams as u64 {
                            fail(format!("{name}: entries {e} != {streams}"));
                        }
                        build += stage0::BUILD_NS.load(Relaxed);
                        replay += stage0::REPLAY_NS.load(Relaxed);
                    } else if stage0::LOOKUPS.load(Relaxed) != 0 {
                        fail(format!("{name}: counters moved in mode {mode}"));
                    }
                }
                let n = ITERS as f64;
                cells[run][mode as usize][si] = Cell {
                    encode_ns: total_ns as f64 / n,
                    build_ns: build as f64 / n,
                    replay_ns: replay as f64 / n,
                    lookups: if mode == 1 { RECORDS as f64 } else { 0.0 },
                    entries: if mode == 1 { streams as u64 } else { 0 },
                };
            }
        }
    }

    println!("\nRAW (per object, ns): run mode shape encode_ns build_ns replay_ns lookups entries");
    for run in 0..RUNS {
        for mode in 0..3 {
            for (si, &(name, _, _)) in SHAPES.iter().enumerate() {
                let c = cells[run][mode][si];
                println!(
                    "RAW {} {} {} {:.0} {:.0} {:.0} {:.0} {}",
                    run + 1, mode, name, c.encode_ns, c.build_ns, c.replay_ns, c.lookups, c.entries
                );
            }
        }
    }

    println!("\nSUMMARY (median [min..max] over {RUNS} run means)");
    for (si, &(name, _, _)) in SHAPES.iter().enumerate() {
        let col = |mode: usize, f: &dyn Fn(&Cell) -> f64| -> Vec<f64> {
            (0..RUNS).map(|r| f(&cells[r][mode][si])).collect()
        };
        let fmt = |mut v: Vec<f64>| -> String {
            let (lo, hi) = (
                v.iter().cloned().fold(f64::MAX, f64::min),
                v.iter().cloned().fold(f64::MIN, f64::max),
            );
            format!("{:.1} [{:.1}..{:.1}]", median(&mut v), lo, hi)
        };
        let ms = |v: Vec<f64>| v.into_iter().map(|x| x / 1e6).collect::<Vec<_>>();
        let us = |v: Vec<f64>| v.into_iter().map(|x| x / 1e3).collect::<Vec<_>>();
        let pct = |v: Vec<f64>| v.into_iter().map(|x| x * 100.0).collect::<Vec<_>>();
        println!("{name}");
        println!("  mode0 encode ms: {}", fmt(ms(col(0, &|c| c.encode_ns))));
        println!("  mode1 encode ms: {}", fmt(ms(col(1, &|c| c.encode_ns))));
        println!("  mode1 build us: {}", fmt(us(col(1, &|c| c.build_ns))));
        println!("  mode1 replay us: {}", fmt(us(col(1, &|c| c.replay_ns))));
        let share: Vec<f64> = (0..RUNS)
            .map(|r| {
                let c = cells[r][1][si];
                (c.build_ns + c.replay_ns) / (c.encode_ns - c.replay_ns)
            })
            .collect();
        println!("  SHARE %: {}", fmt(pct(share)));
        println!("  mode2 encode ms: {}", fmt(ms(col(2, &|c| c.encode_ns))));
        let ratio: Vec<f64> = (0..RUNS)
            .map(|r| cells[r][2][si].encode_ns / cells[r][0][si].encode_ns)
            .collect();
        println!("  mode2/mode0 ratio: {}", fmt(ratio));
    }
}
