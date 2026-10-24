//! Integer page decode throughput (issue #3235).
//!
//! Builds one 8,192-row page per shape and times
//! `ravel_codec::encoding::decode_i64_into` over it, repeating until at least
//! one second has elapsed. Prints one line per shape: the shape, the encoding
//! the page carries, nanoseconds per block row, the shape's ceiling, and
//! PASS or MISS. An optional argument keeps only the shapes whose name starts
//! with it.
//!
//! Exits 1 after printing every shape when any selected shape measured above
//! its ceiling or produced no figure, or when the filter selects no shape.
//! Exits 2 when built without `--release`. The ceilings are the release
//! figures from the change that introduced bulk decode times about 1.5.
//!
//! Every page comes from `encode_i64` except the two Plain shapes: the
//! encoder picks ForBitpack for both value ranges (a 7-bit and a 40-bit pack
//! beat one- and six-byte varints), so those pages are written here in the
//! Plain layout (one zigzag LEB128 varint per value) and checked against
//! `decode_i64` before timing. Each line asserts the encoding it expected.
//!
//! Zigzag maps 64..100 to two-byte varints, so about a third of
//! `c_plain_0_100` is two bytes long; `c2_plain_0_64` is the all-one-byte
//! page.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::hint::black_box;
use std::time::{Duration, Instant};

use ravel_codec::encoding::{Enc, decode_i64, decode_i64_into, encode_i64};

const ROWS: usize = 8_192;
const MIN_TIME: Duration = Duration::from_secs(1);

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn plain_page(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    for &v in values {
        let mut z = ((v as u64) << 1) ^ ((v >> 63) as u64);
        loop {
            let byte = (z & 0x7f) as u8;
            z >>= 7;
            if z == 0 {
                out.push(byte);
                break;
            }
            out.push(byte | 0x80);
        }
    }
    out
}

/// Runs of about 400 rows, mostly 0 with a few small values.
fn rle_values(rng: &mut Rng, n: usize) -> Vec<i64> {
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let run = 200 + rng.below(401) as usize;
        let v = if rng.below(5) == 0 {
            1 + rng.below(9) as i64
        } else {
            0
        };
        for _ in 0..run.min(n - out.len()) {
            out.push(v);
        }
    }
    out
}

struct Shape {
    name: &'static str,
    enc: Enc,
    bytes: Vec<u8>,
    presence: Option<Vec<u8>>,
    expect: Vec<i64>,
    ceiling_ns: f64,
}

fn encoded(name: &'static str, ceiling_ns: f64, want: Enc, values: Vec<i64>) -> Shape {
    let (enc, bytes) = encode_i64(&values);
    assert_eq!(enc, want, "shape {name}: encode_i64 picked {enc:?}");
    Shape {
        name,
        enc,
        bytes,
        presence: None,
        expect: values,
        ceiling_ns,
    }
}

fn plain(name: &'static str, ceiling_ns: f64, values: Vec<i64>) -> Shape {
    let bytes = plain_page(&values);
    assert_eq!(
        decode_i64(Enc::Plain, &bytes, values.len()).expect("plain page decodes"),
        values
    );
    Shape {
        name,
        enc: Enc::Plain,
        bytes,
        presence: None,
        expect: values,
        ceiling_ns,
    }
}

fn shapes() -> Vec<Shape> {
    let mut rng = Rng(0x3235);
    let mut out = vec![encoded("a_constant_0", 0.25, Enc::Constant, vec![0; ROWS])];
    out.push(encoded(
        "b_rle_runs_400",
        0.25,
        Enc::Rle,
        rle_values(&mut rng, ROWS),
    ));
    out.push(plain(
        "c_plain_0_100",
        3.0,
        (0..ROWS).map(|_| rng.below(100) as i64).collect(),
    ));
    out.push(plain(
        "c2_plain_0_64",
        0.9,
        (0..ROWS).map(|_| rng.below(64) as i64).collect(),
    ));
    out.push(plain(
        "d_plain_0_2e40",
        6.5,
        (0..ROWS).map(|_| rng.below(1 << 40) as i64).collect(),
    ));
    let mut acc = 1_700_000_000_000i64;
    out.push(encoded(
        "e_delta_rising",
        1.8,
        Enc::DeltaZigzag,
        (0..ROWS)
            .map(|_| {
                acc += rng.below(4) as i64;
                acc
            })
            .collect(),
    ));
    out.push(encoded(
        "f_for_8bit",
        1.3,
        Enc::ForBitpack,
        (0..ROWS).map(|_| 1_000 + rng.below(256) as i64).collect(),
    ));

    let present: Vec<bool> = (0..ROWS).map(|_| rng.below(10) != 0).collect();
    let mut presence = vec![0u8; ROWS.div_ceil(8)];
    for (i, &p) in present.iter().enumerate() {
        if p {
            presence[i / 8] |= 1 << (i % 8);
        }
    }
    let n_present = present.iter().filter(|&&p| p).count();
    let dense = rle_values(&mut rng, n_present);
    let (enc, bytes) = encode_i64(&dense);
    assert_eq!(enc, Enc::Rle, "shape g: encode_i64 picked {enc:?}");
    let mut it = dense.iter();
    let expect = present
        .iter()
        .map(|&p| if p { *it.next().unwrap() } else { 0 })
        .collect();
    out.push(Shape {
        name: "g_rle_presence_90",
        enc,
        bytes,
        presence: Some(presence),
        expect,
        ceiling_ns: 1.5,
    });
    out
}

fn main() {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    println!("int_page_decode profile={profile} rows={ROWS}");
    if cfg!(debug_assertions) {
        eprintln!("int_page_decode: refusing to time a debug build; rerun with --release");
        std::process::exit(2);
    }

    let only = std::env::args().nth(1);
    let mut out = vec![0i64; ROWS];
    let mut expected = Vec::new();
    let mut figures: Vec<(&'static str, f64)> = Vec::new();
    for s in shapes() {
        if only.as_deref().is_some_and(|p| !s.name.starts_with(p)) {
            continue;
        }
        expected.push((s.name, s.ceiling_ns));
        let presence = s.presence.as_deref();
        decode_i64_into(s.enc, &s.bytes, presence, &mut out, Ok).expect("decodes");
        assert_eq!(out, s.expect, "shape {} decoded wrong values", s.name);

        let start = Instant::now();
        let mut reps: u64 = 0;
        while start.elapsed() < MIN_TIME {
            for _ in 0..64 {
                let n = decode_i64_into(
                    s.enc,
                    black_box(&s.bytes),
                    black_box(presence),
                    black_box(&mut out),
                    Ok,
                )
                .expect("decodes");
                black_box(n);
            }
            reps += 64;
        }
        let ns = start.elapsed().as_nanos() as f64 / (reps as f64 * ROWS as f64);
        let verdict = if ns.is_finite() && ns <= s.ceiling_ns {
            "PASS"
        } else {
            "MISS"
        };
        println!(
            "{:<20} {:<12} {:>8.3} ns/value  ceiling {:>5.2}  {}  ({} bytes, {} reps)",
            s.name,
            format!("{:?}", s.enc),
            ns,
            s.ceiling_ns,
            verdict,
            s.bytes.len(),
            reps
        );
        if reps > 0 && ns.is_finite() {
            figures.push((s.name, ns));
        }
    }

    let mut failed = Vec::new();
    if expected.is_empty() {
        failed.push(format!(
            "no shape matches filter {:?}",
            only.as_deref().unwrap_or("")
        ));
    }
    for &(name, ceiling) in &expected {
        let got: Vec<f64> = figures
            .iter()
            .filter(|(n, _)| *n == name)
            .map(|&(_, ns)| ns)
            .collect();
        match got.as_slice() {
            [ns] if *ns <= ceiling => {}
            [ns] => failed.push(format!(
                "{name}: {ns:.3} ns/value above ceiling {ceiling:.2}"
            )),
            [] => failed.push(format!("{name}: no figure")),
            many => failed.push(format!("{name}: {} figures, expected one", many.len())),
        }
    }
    if !failed.is_empty() {
        for f in &failed {
            eprintln!("int_page_decode MISS {f}");
        }
        std::process::exit(1);
    }
}
