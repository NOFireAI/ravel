# Two-level RLOG merge CPU per record (issue #4141)

Since ADR-3853 phase B (#4107, #4108, #4109), production's two-level merge
spends about twice the CPU per record at both levels: about 27 us/record per
level before phase B, about 52 and 53 after. This file finds out which phase-B
change costs that. Each arm changes one variable from the phase-B default and
is measured on one synthetic bucket. The command and harness are described in
[README.md](README.md).

## Stamp

| What | Value |
|---|---|
| Host | x86_64, 8 vCPU (`nproc`), 15.2 GiB RAM (`MemTotal` 15,988,672 kB), kernel 6.8.0-60-generic; fleet executor shared with other sessions' builds |
| Toolchain | rustc 1.97.1 (8bab26f4f 2026-07-14) |
| Source | timing binary built from commit 6087f2e24e2f (the knobs commit on this task's branch; a merge rewrites commit ids) |
| Timing build | `cargo build --release -p ravel-bench --bin rlog_compaction_profile` (`CARGO_BUILD_JOBS=2`) |
| Store | in-memory `MemoryStore` behind a timing wrapper; scratch runs on the local disk under `.gate-logs/` |
| Load average | taken just before and just after each run, in the run table |

## Pre-registration (written 2026-11-13, before the first measured run)

### Bucket and route

`--inputs 460`, seed 3819: 5,408,099 records, 2,477 streams, 460 L0 objects,
776,585,811 L0 bytes. Every arm runs with `--scratch-dir` and
`--merge-resident-ceiling-bytes 4294967296` (U = 4 GiB). The default ceiling
on this host is 20 GiB, under which the bucket plans resident and merges
single-level. At 4 GiB the bucket-level plan is cursor mode, and the bucket
takes the two-level route. One calibration run (`calib-460`, 2026-11-13, not a
measured arm) gave the following: the route was two-level. The derived workers
were W1 = 1 and W2 = 2, with group size 37, 13 runs, 3 level-2 ranges, one
encode builder per level-2 worker, and 5,321 run blocks of 3,921,438 bytes
each. Level 1 took 50.5 us/record and level 2 took 49.4 us/record. The two
levels covered 0.996 of the process CPU.

A second probe (`probe-rb8192`, 2026-11-13) set `--run-block-records 8192`
with derived workers. It was refused before level 1 with
`PredictedLevelTwoFit { block_bytes: 447273370, ... }`. The joint W1/W2
derivation prices a level-2 block at the highest price per record of any
input block, times the run block target. That predicts 447 MB for an 8,192-record
run block, against 3.9 MB measured for a 1,024-record one. So no derived pair
admits level 2 at 8,192. Arms that change the run block or W1 therefore pin
`--level1-workers 1 --level2-workers 2`, the default's derived pair. Pinning
the same pair takes the same code path: with W1 set, level 1 uses `U / W1` and
the same group size, and `level2_plans` uses the configured W2 instead of the
predicted one. The event fields of every run are checked against the pin.

The range quantum stays at the shipped 2,500,000 in the default. On this
bucket that cuts 3 ranges, which caps W2 at 3. In production, hour 498272's
167 M records cut about 67 ranges.

### Arms

| Arm | Flags beyond the common ones | Variable changed from the default |
|---|---|---|
| A default | (none) | none: phase-B defaults, derived W1 = 1, W2 = 2 |
| B run block 8192 | `--run-block-records 8192 --level1-workers 1 --level2-workers 2` | run block target 1,024 -> 8,192 (candidate a, #4107) |
| C level2 workers 1 | `--level2-workers 1` | W2 2 -> 1: one range worker with D3's pipelined encode, E builders, instead of inline encode with one builder per worker (candidate b, #4108) |
| D range very large | `--range-records 1000000000000` | 3 ranges -> 1. With one range, W2 is capped at 1, so D also changes W2 the way C does. D against C isolates the range cut |
| E pre-phase-B route | `--run-block-records 8192 --level1-workers 1 --level2-workers 1 --range-records 1000000000000` | B, C and D together. This is the route before phase B: 8,192-record runs, one level-2 worker with pipelined encode, no range cut. Multi-variable by design |
| S single-level | no `--scratch-dir` and no ceiling | reference: the same bucket merged in one level (resident) |

Each arm runs 3 times. The runs are interleaved, A B C D E S, then again, then
again, so host load drift spreads over all arms. Each run reports the median
and the spread (max - min) of level-1 and level-2 CPU per record. Here "CPU
per record" is the process CPU between the compactor's events, divided by the
input records. Both levels handle every record.

### Expected figures and what each result points at

Noise: the calibration run is a single sample. Pre-registered spread within
an arm is at most 5% of the median. A difference between two arms counts only
if it exceeds 10% of the default's median and is more than twice the larger
spread.

Figures in us/record, level 1 / level 2:

- A default: 50 / 49 (calibration), band 45-56 / 44-55.
- B run block 8192:
  - If candidate (a) explains the doubling, both levels fall by at least 25%,
    toward about 27 / 27. A 1,024-record run block multiplies the run
    blocks level 1 encodes and level 2 decodes by 8, and each block pays a
    fixed per-column cost.
  - A fall of 10-25% at a level means (a) explains part of it.
  - Under 10% means (a) does not explain it.
  - Expected: level 1 falls (encode side) and level 2 falls (decode side).
- C level2 workers 1:
  - Only level 2 can move; level 1 must stay within 10% of A as a control.
  - If candidate (b) explains level 2's doubling, level 2 falls by at least
    25%.
  - Expected: under 10% either way. Pipelined and inline encode do the same
    encode work, so (b) should move wall time, not CPU.
- D range very large:
  - Expected: level 2 within 10% of C, level 1 within 10% of A.
  - A difference beyond that from C is the cost of the range cut itself:
    smaller final parts and one cutter per range.
- E pre-phase-B: expected about B's level 1 and the lower of B's and C's
  level 2.
  - If E is not at least 25% below A at both levels, the phase-B fields do
    not explain the doubling on this bucket. The cause then lies in
    something this bench does not vary, and the report says so.
- S single-level: about 50 us/record for the whole merge. The calibration at
  40 inputs gave 49.6 us/record, single-threaded.

Every run must also pass these checks:

- records out == records in;
- every two-level figure (level-1 and level-2 wall, CPU and CPU per record,
  and the levels' share of process CPU) present exactly once and positive;
- the levels' share of process CPU in [0.80, 1.0];
- every pinned knob present in the resolved config and in the two-level
  event.

A run that fails a check is reported, not averaged.

### Flamegraphs

There are two profiled runs: arm A, and the arm whose CPU per record moves
most against A. Each gives the top 15 exclusive leaves, overall and per
level, and the leaves whose share moved by more than 2 percentage points.
