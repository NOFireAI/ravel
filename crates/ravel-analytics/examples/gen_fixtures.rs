//! Fixture generator for the change_point evidence corpus (ADR-0028
//! decision 6). This is the committed script that produces the synthetic
//! series with known ground truth checked in under `tests/fixtures/`.
//!
//! Run it from the crate root to regenerate the corpus:
//!
//! ```sh
//! cargo run -p ravel-analytics --example gen_fixtures
//! ```
//!
//! It depends on std only, and its noise comes from a fixed-seed xorshift64
//! PRNG, so the generated files are byte-for-byte reproducible on any host.
//! Each file begins with three header lines naming the expected kind, the
//! ground-truth change index (`-1` for none), and the tolerance window (in
//! samples) the detection test allows, then `ts_ns,value` rows.

use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

/// A tiny deterministic xorshift64 PRNG. std-only, so the fixtures are
/// reproducible without a dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // Avoid the zero state, which xorshift cannot leave.
        Rng(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Uniform noise in `[-amp, amp)`.
    fn noise(&mut self, amp: f64) -> f64 {
        let u = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        (u * 2.0 - 1.0) * amp
    }
}

const N: usize = 240;
const STEP_NS: i64 = 60_000_000_000; // 60s grid.

struct Fixture {
    name: &'static str,
    kind: &'static str,
    change_index: i64,
    tolerance: usize,
    values: Vec<f64>,
}

fn build() -> Vec<Fixture> {
    let mut out = Vec::new();

    // 1. Step change: level 10 -> 30 at index 120.
    {
        let mut rng = Rng::new(0x5175_0001);
        let values = (0..N)
            .map(|i| {
                let base = if i < 120 { 10.0 } else { 30.0 };
                base + rng.noise(0.5)
            })
            .collect();
        out.push(Fixture {
            name: "step",
            kind: "StepChange",
            change_index: 120,
            tolerance: 20,
            values,
        });
    }

    // 2. Spike: single point far above a flat baseline at index 150.
    {
        let mut rng = Rng::new(0x5175_0002);
        let mut values: Vec<f64> = (0..N).map(|_| 10.0 + rng.noise(0.5)).collect();
        values[150] = 100.0;
        out.push(Fixture {
            name: "spike",
            kind: "Spike",
            change_index: 150,
            tolerance: 10,
            values,
        });
    }

    // 3. Dip: single point far below a flat baseline at index 90.
    {
        let mut rng = Rng::new(0x5175_0003);
        let mut values: Vec<f64> = (0..N).map(|_| 50.0 + rng.noise(0.5)).collect();
        values[90] = -40.0;
        out.push(Fixture {
            name: "dip",
            kind: "Dip",
            change_index: 90,
            tolerance: 10,
            values,
        });
    }

    // 4. Trend break: slope 0.05 then slope 0.40 at index 120 (a kink).
    {
        let mut rng = Rng::new(0x5175_0004);
        let values = (0..N)
            .map(|i| {
                let base = if i < 120 {
                    0.05 * i as f64
                } else {
                    0.05 * 120.0 + 0.40 * (i - 120) as f64
                };
                base + rng.noise(0.3)
            })
            .collect();
        out.push(Fixture {
            name: "trend_break",
            kind: "TrendChange",
            change_index: 120,
            tolerance: 25,
            values,
        });
    }

    // 5. Variance shift: same mean, noise amplitude 0.5 -> 8.0 at index 120.
    {
        let mut rng = Rng::new(0x5175_0005);
        let values = (0..N)
            .map(|i| {
                let amp = if i < 120 { 0.5 } else { 8.0 };
                20.0 + rng.noise(amp)
            })
            .collect();
        out.push(Fixture {
            name: "variance_shift",
            kind: "DistributionChange",
            change_index: 120,
            tolerance: 25,
            values,
        });
    }

    // 6. Stationary noise: one level throughout.
    {
        let mut rng = Rng::new(0x5175_0006);
        let values = (0..N).map(|_| 5.0 + rng.noise(1.0)).collect();
        out.push(Fixture {
            name: "stationary",
            kind: "Stationary",
            change_index: -1,
            tolerance: 0,
            values,
        });
    }

    // 7. Constant series: no variation at all.
    {
        let values = vec![7.0; N];
        out.push(Fixture {
            name: "constant",
            kind: "Stationary",
            change_index: -1,
            tolerance: 0,
            values,
        });
    }

    out
}

fn render(fixture: &Fixture) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "# kind={}", fixture.kind);
    let _ = writeln!(s, "# change_index={}", fixture.change_index);
    let _ = writeln!(s, "# tolerance={}", fixture.tolerance);
    let _ = writeln!(s, "ts_ns,value");
    for (i, &v) in fixture.values.iter().enumerate() {
        let ts = i as i64 * STEP_NS;
        // {:?} prints the shortest round-tripping decimal, so the parsed
        // value is bit-identical to the generated one.
        let _ = writeln!(s, "{ts},{v:?}");
    }
    s
}

fn main() -> std::io::Result<()> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures");
    fs::create_dir_all(&dir)?;
    for fixture in build() {
        let path = dir.join(format!("{}.csv", fixture.name));
        fs::write(&path, render(&fixture))?;
        println!("wrote {}", path.display());
    }
    Ok(())
}
