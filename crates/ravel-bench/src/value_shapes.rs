//! Deterministic value-stream shapes shared by the value codec bake-off
//! (`src/bin/codec_bakeoff.rs`) and the catalog byte gate
//! (`tests/catalog_byte_gates.rs`).
//!
//! The bake-off (ADR-0092 decision 6, round two) added realistic integer and
//! decimal metric shapes so the codec measurement stopped being driven by a
//! full-mantissa random float that no integer-model codec can compress. The
//! byte gate historically measured that same uncompressible float and quoted
//! the result as the format's cost (issue #370). Both now draw their values
//! from one place, so a shape is defined once and measured identically by the
//! codec bench and by the gate.
//!
//! Each stream is deterministic in `(shape, n, salt)`: a fixed seed derived
//! from those three, no wall-clock time, so the byte figures reproduce. The
//! bake-off passes `salt = 0` (one stream per dataset); the gate passes the
//! series index as salt, so its 500 series each get a distinct walk and the
//! zstd-compressed VAL section cannot collapse identical streams into an
//! unrealistically small figure.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// One synthetic value-stream shape. Names match the `kind` strings the
/// round-two bake-off used, so its seeds (and therefore its committed report
/// numbers) are unchanged when `salt == 0`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ValueShape {
    /// Cumulative integer counter, small integer increments, ~2% reset to 0.
    CounterIntResets,
    /// Integer gauge random walk, non-negative.
    GaugeInt,
    /// 2-decimal gauge (e.g. a price) random walk.
    GaugeDec2,
    /// 3-decimal gauge random walk.
    GaugeDec3,
    /// Arbitrary noisy floats: no low decimal exponent reproduces them.
    NoisyFloat,
    /// Constant series.
    Constant,
    /// Sparse: mostly zero, ~10% integer spikes.
    Sparse,
    /// The round-one control: a counter that increments by a random float.
    FloatCounter,
}

/// Every shape, in the bake-off's report order.
pub const ALL_SHAPES: [ValueShape; 8] = [
    ValueShape::CounterIntResets,
    ValueShape::GaugeInt,
    ValueShape::GaugeDec2,
    ValueShape::GaugeDec3,
    ValueShape::NoisyFloat,
    ValueShape::Constant,
    ValueShape::Sparse,
    ValueShape::FloatCounter,
];

impl ValueShape {
    /// The seed key: the exact `kind` string the round-two bake-off hashed, so
    /// `value_stream(shape, n, 0)` reproduces the bake-off's committed values.
    pub fn key(self) -> &'static str {
        match self {
            ValueShape::CounterIntResets => "counter_int_resets",
            ValueShape::GaugeInt => "gauge_int",
            ValueShape::GaugeDec2 => "gauge_dec2",
            ValueShape::GaugeDec3 => "gauge_dec3",
            ValueShape::NoisyFloat => "noisy_float",
            ValueShape::Constant => "constant",
            ValueShape::Sparse => "sparse",
            ValueShape::FloatCounter => "float_counter",
        }
    }

    /// Human-facing label; the float control is tagged so a reader cannot
    /// mistake it for a realistic shape.
    pub fn label(self) -> &'static str {
        match self {
            ValueShape::FloatCounter => "float_counter(ctrl)",
            other => other.key(),
        }
    }
}

/// Round a float to `places` decimal places, the way a real decimal-valued
/// gauge (a price, a ratio) arrives: the stored f64 is the nearest double to
/// the rounded decimal.
fn round_to(v: f64, places: i32) -> f64 {
    let scale = 10f64.powi(places);
    (v * scale).round() / scale
}

/// FNV-1a over the shape key, so distinct shapes get well-separated seeds.
fn seed_of(key: &str) -> u64 {
    key.bytes().fold(1469598103934665603u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(1099511628211)
    })
}

/// One value stream of exactly `n` samples for `shape`, deterministic in
/// `(shape, n, salt)`. `salt` separates otherwise-identical streams (e.g. one
/// per series); pass `0` to reproduce the bake-off's single-stream values.
pub fn value_stream(shape: ValueShape, n: usize, salt: u64) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(0x0C0D_EC00 ^ (n as u64) ^ seed_of(shape.key()) ^ salt);
    match shape {
        ValueShape::CounterIntResets => {
            let mut acc = 0i64;
            (0..n)
                .map(|_| {
                    if rng.random_bool(0.02) {
                        acc = 0;
                    } else {
                        acc += rng.random_range(1..=10);
                    }
                    acc as f64
                })
                .collect()
        }
        ValueShape::GaugeInt => {
            let mut acc = rng.random_range(0..1000) as i64;
            (0..n)
                .map(|_| {
                    acc = (acc + rng.random_range(-5..=5)).max(0);
                    acc as f64
                })
                .collect()
        }
        ValueShape::GaugeDec2 => {
            let mut acc = round_to(rng.random_range(0.0..1000.0), 2);
            (0..n)
                .map(|_| {
                    acc = round_to(acc + rng.random_range(-1.0..1.0), 2);
                    acc
                })
                .collect()
        }
        ValueShape::GaugeDec3 => {
            let mut acc = round_to(rng.random_range(0.0..1000.0), 3);
            (0..n)
                .map(|_| {
                    acc = round_to(acc + rng.random_range(-1.0..1.0), 3);
                    acc
                })
                .collect()
        }
        ValueShape::NoisyFloat => (0..n)
            .map(|_| rng.random_range(-1_000_000.0..1_000_000.0))
            .collect(),
        ValueShape::Constant => vec![42.0; n],
        ValueShape::Sparse => (0..n)
            .map(|_| {
                if rng.random_bool(0.1) {
                    rng.random_range(1..=1000) as f64
                } else {
                    0.0
                }
            })
            .collect(),
        ValueShape::FloatCounter => {
            let mut acc = 0.0f64;
            (0..n)
                .map(|_| {
                    acc += rng.random_range(0.0..5.0);
                    acc
                })
                .collect()
        }
    }
}
