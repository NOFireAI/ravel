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

## Deviations from the pre-registration (decided after rep 1, 2026-11-14)

- Arm C, as registered (`--level2-workers 1` alone), changed two variables.
  With W2 fixed at 1, the joint derivation picks W1 = 2, which gives 28
  level-1 groups instead of 13. Arm C1 (`--level1-workers 1
  --level2-workers 1`) was added. It changes W2 alone. C stays in the table
  as run.
- Arm S, as registered (no scratch, default 20 GiB ceiling), would plan the
  460-input bucket resident on a 15.2 GiB host. At an 8 GiB ceiling the bucket
  plans cursor mode, and the single-level path refuses it with
  `GeometryUnsupported { predicted_decoded_rows: 7754049402 }` (log
  `refused-single8g-1`). S therefore runs the bench's standard 230-input
  bucket (2,739,830 records, resident, single-level). That is below the
  5M-record floor, and S is a reference only.
- Arm F (`--level1-workers 4`) was added after rep 1. It is exploratory, not
  pre-registered. It tests the parallel level-1 contention candidate, which
  the registered arms never vary because the default derives W1 = 1 here.

## Results

All runs were measured between 2026-11-13T23:08Z and 2026-11-14T01:37Z with
one timing binary, sha256 prefix `3654aa51dae624b0`, built from 6087f2e. The
logs are `.gate-logs/arms/<arm>-<rep>.{json,log,stamp}` in the executor
checkout; they are not committed. Every run exited 0 with an empty
`checks.failures`:

- records out equalled records in on every run: 5,408,099 at 460 inputs and
  2,739,830 at 230;
- every two-level figure was present exactly once;
- the levels' share of process CPU stayed between 0.996 and 0.997 on every
  two-level run;
- the event fields matched every pinned knob.

The L1 parts were 72 parts and 702,927,049 bytes on every three-range arm,
and 70 parts and 702,704,828 bytes on the one-range arms (D, E). Resolved
config on every run: `rlog_zstd_level` 3, L1 part memory target 268,435,456,
`rlog_run_memory_target_bytes` 268,435,456, `merge_cursor_budget_bytes`
21,474,836,480, `unit_concurrency` 1.

### Arm table

CPU per record in us/record, as the median of 3 reps with the spread
(max - min) in brackets. The change is against arm A's median. "Counts"
means the difference passes the pre-registered noise rule: more than 10% of
A's median and more than twice the larger spread.

| Arm | Log | W1 / W2 | Level-1 groups / ranges / run blocks / encode builders | L1 CPU/record | change | counts | L2 CPU/record | change | counts | L1 wall s | L2 wall s |
|---|---|---|---|---|---|---|---|---|---|---|---|
| A default | `default-{1,2,3}` | 1 / 2 (derived) | 13 / 3 / 5,321 / 1 | 50.6 (1.1) | | | 49.3 (3.2) | | | 274 | 146 |
| B run block 8192 | `rb8192-{1,2,3}` | 1 / 2 (pinned) | 13 / 3 / 703 / 1 | 47.7 (3.2) | -5.7% | no | 49.6 (0.8) | +0.6% | no | 258 | 145 |
| C level2 workers 1 | `l2w1-{1,2,3}` | 2 (derived) / 1 | 28 / 3 / 5,325 / 8 | 49.5 (1.3) | -2.1% | no | 64.1 (2.4) | +30.2% | yes | 140 | 98 |
| C1 level2 workers 1, W1 pinned | `l2w1pin-{1,2,3}` | 1 / 1 (pinned) | 13 / 3 / 5,321 / 8 | 49.8 (1.2) | -1.7% | no | 62.6 (1.0) | +27.1% | yes | 269 | 93 |
| D range very large | `range1-{1,2,3}` | 1 / 1 (one range) | 13 / 1 / 5,321 / 8 | 49.7 (0.7) | -1.8% | no | 61.6 (0.5) | +25.0% | yes | 269 | 91 |
| E pre-phase-B route | `prephaseb-{1,2,3}` | 1 / 1 (pinned) | 13 / 1 / 703 / 8 | 47.5 (0.8) | -6.2% | no | 59.1 (7.1) | +19.9% | no (spread) | 257 | 85 |
| F level1 workers 4 (exploratory) | `l1w4-{1,2,3}` | 4 (pinned) / 3 (derived) | 66 / 3 / 5,340 / 1 | 49.8 (2.1) | -1.6% | no | 50.4 (0.7) | +2.3% | no | 69 | 130 |
| S single-level, 230 inputs | `single230-{1,2,3}` | single-level, resident | (n/a) | 53.8 (0.7) whole merge | | | (n/a) | | | 147 whole merge | |

D against C1, the range cut alone: L2 -1.7%, L1 -0.2%. Arm F ran level 1 at
3.9 cores (269 core-s in 69 s) with no change in CPU per record. Arm C ran
it at 1.9 cores, also with no change.

Spread against the pre-registered 5%: A's L2 (6.6%), B's L1 (6.7%) and E's L2
(12.0%) exceeded it. Each of those spreads comes from one rep: A rep 3 L2
47.2, B rep 1 L1 50.6, E rep 1 L2 65.5. The noise rule above already
discounts them.

Load average before -> after each run (1/5/15 min):

| Arm | rep 1 | rep 2 | rep 3 |
|---|---|---|---|
| A | 2.35/3.11/3.36 -> 4.65/4.28/3.91 | 2.01/2.91/3.14 -> 1.87/2.10/2.69 | 2.12/2.74/2.67 -> 1.78/2.03/2.38 |
| B | 4.86/4.35/3.94 -> 2.96/4.93/4.62 | 1.72/2.07/2.67 -> 2.10/2.23/2.54 | 1.64/1.99/2.37 -> 1.83/1.94/2.23 |
| C | 2.73/4.81/4.58 -> 3.39/4.46/4.59 | 2.10/2.23/2.54 -> 3.10/2.93/2.79 | 1.68/1.90/2.22 -> 3.15/2.74/2.52 |
| C1 | 1.65/3.05/3.41 -> 3.05/2.61/3.05 | 2.85/2.88/2.78 -> 3.17/2.56/2.64 | 2.90/2.69/2.51 -> 2.93/2.43/2.44 |
| D | 2.94/4.33/4.55 -> 2.70/2.79/3.73 | 2.92/2.52/2.63 -> 3.18/2.46/2.54 | 2.70/2.39/2.43 -> 3.28/2.47/2.44 |
| E | 2.57/2.76/3.72 -> 3.01/2.52/3.26 | 2.93/2.42/2.53 -> 3.12/2.56/2.56 | 3.02/2.43/2.43 -> 2.69/2.32/2.38 |
| F | 2.97/2.60/3.05 -> 2.68/3.09/3.20 | 1.26/2.14/2.41 -> 2.30/2.78/2.68 | 1.48/2.08/2.29 -> 2.38/2.75/2.58 |
| S | 5.47/3.60/3.58 -> 1.93/3.18/3.46 | 2.87/2.52/2.55 -> 1.37/2.17/2.43 | 2.47/2.28/2.37 -> 1.48/2.08/2.29 |

### Flamegraph comparison

There were two profiled runs on 2026-11-14 with the frame-pointer build
(`cargo profile-build`, sha256 prefix `573f07cef721a42e`, same source):

- arm A (`.gate-logs/prof/profdefault-1`): 319,298 samples at 997 Hz;
- arm C1 (`.gate-logs/prof/profl2w1pin-1`): 277,397 samples. C1 is the arm
  whose CPU per record moves most against A on one variable.

Unattributed share was 0.0% in both. The profiled runs measured L1 / L2 at
51.5 / 49.6 (A) and 51.2 / 64.9 (C1), within 4% of the unprofiled medians. The
SVGs are 0.8 MB and 0.6 MB and are not committed.

Read the shares with two limits:

- The samples cover 320 of A's 549 process core-s and 278 of C1's 630. The
  sampler drops every sample whose leaf is in libc (memcpy, malloc), as the
  README says. The gap is larger in C1, whose extra level-2 CPU runs on the
  encode builder threads.
- Most level-1 work runs in tasks spawned outside the `rlog::level1_worker`
  frame. The per-level stack-marker split therefore puts 56% (A) and 85%
  (C1) of the samples in "neither", so it does not attribute CPU to a level.
  The pre-registration named this split as secondary.

Top 15 exclusive leaves, share of samples:

| Leaf | A | C1 |
|---|---|---|
| `ravel_logseg::block::row_column::{closure#0}` | 10.0 | 6.3 |
| `<[u8] as PartialEq>::eq` | 9.3 | below 1.5 |
| `core::ptr::copy_nonoverlapping::<u8>` | 6.8 | 14.8 |
| `<u64>::wrapping_add` | 4.7 | 1.5 |
| `<sip::Hasher<Sip13Rounds> as Hasher>::write` | 3.3 | 5.0 |
| `ravel_logseg::dict_values::span` | 3.2 | 1.7 |
| `ravel_codec::tokenizer::ascii_tokens::<writer::insert_text::{closure#0}>` | 2.9 | 4.4 |
| `<u8>::is_ascii_alphanumeric` | 2.6 | below 1.5 |
| `<Sip13Rounds as Sip>::d_rounds` | 2.4 | below 1.5 |
| `Iter<ResolvedRow>::fold` (map_fold) | 2.3 | below 1.5 |
| `ravel_logseg::record::resolve_value` | 2.2 | 2.9 |
| `<u8 as SliceOrd>::compare` | 2.2 | 1.9 |
| `<isize as Ord>::cmp` | 2.2 | below 1.5 |
| `ravel_object_store::memory::crc32c` | 1.9 | 1.6 |
| `<RlogWriter>::build_object` | 1.8 | below 1.5 |
| `<NonNull<(u32, ColumnValue)> as PartialEq>::eq` | below 1.8 | 6.1 |
| `btree NodeRef<LogStreamId, &[u8]>` insert | below 1.8 | 3.1 |
| `huf_decompress_amd64.S:551` | below 1.8 | 2.6 |
| `<PlainColumn>::cell` | below 1.8 | 2.3 |
| `hashbrown RawTable<Box<[u8]>>::find` | below 1.8 | 1.9 |
| `ZSTD_compressBlock_fast_noDict_generic` | below 1.8 | 1.7 |

"Below X" means outside that run's top 15, whose 15th leaf has that share.

Leaves whose share moved by more than 2 points, A to C1:

- `copy_nonoverlapping::<u8>`: +8.0.
- `NonNull<(u32, ColumnValue)>::eq`, the end test of a column-value slice
  iterator: +4.2 or more.
- `row_column`: -3.7.
- `wrapping_add`: -3.2.
- `[u8]::eq`: -7.8 or more.

Pipelined encode (W2 = 1, E = 8 builders) adds copies and per-builder
column-value and stream-key work that inline encode does not do. The leaves
are consistent with the +27% level-2 CPU in C1, but because of the sample
loss they cannot apportion it.

One more reading of A's profile concerns bloom staging, that is,
`BloomBuilder::insert` hashing every `column_id || token` into a SipHash
`HashSet<Box<[u8]>>` (crates/ravel-codec/src/bloom.rs).

- The two SipHash leaves, `ascii_tokens` and `is_ascii_alphanumeric` are
  bloom staging.
- `wrapping_add` (SipHash rounds) and `[u8]::eq` (the set probe) most likely
  are too. Exclusive leaves do not show their callers, so this is not
  proven.

Together these six hold 25.3% of A's sampled leaves (81 of 320 sampled
core-s). Level-1 runs are written with the output's `bloom_scope`
(`PartWriterSettings` in `two_level_merge`, crates/ravel-maintain/src/rlog.rs,
`bloom_scope: output.writer.bloom_scope.clone()`). So level 1 builds a token
bloom for every run block, and level 2 never consults it, because it merges
every run whole.

## Conclusion

On this bucket, none of the phase-B changes explains the doubling. Share of
the issue's doubling each candidate explains here:

| Candidate | Arm(s) | Measured effect | Share of the doubling explained here |
|---|---|---|---|
| (a) 1,024-record run block (#4107) | B against A | L1 -5.7%, L2 +0.6%, neither counts | 0% |
| (b) inline encode, one builder per level-2 worker (#4108) | C1 against A | L2 +27.1% for the pre-phase-B pipelined encode: inline is cheaper | 0% (opposite sign) |
| parallel level-1 contention | F (W1 4) and C (W1 2) against A (W1 1) | L1 -1.6% and -2.1% | 0% |
| range cuts (`rlog_range_records` 2.5M) | D against C1 | L2 -1.7%, L1 -0.2% | 0% |
| smaller final parts | D against C1 (72 parts against 70) | the same as the range cut | 0% |
| all of the above together | E against A | L1 -6.2% (does not count), L2 +19.9% (does not count, spread 7.1) | 0% |

The bench instead shows a shape that does not depend on phase B: each level
of a two-level merge costs about one whole single-level merge. Level 1 cost
50.6 us/record and level 2 cost 49.3, against 53.8 for a single-level merge
(S). S is a reference, not a like-for-like arm: it merged 230 inputs and
2,739,830 records in resident read mode, while every two-level arm merged
460 inputs and 5,408,099 records in cursor mode (see the deviations above).
Bucket size and read mode both differ, so the per-record comparison is
suggestive only: no same-bucket single-level control exists yet, and
follow-up 2 adds one. The suggestion is what both levels doing a full
decode, merge and encode of every record predicts. The issue's pre-phase-B
figure of about 27 us/record per level, about 55 for both levels, is
what one pass costs here, not two. So the pre-phase-B figure is the anomaly
against this bench, not the phase-B one. Run 2 and M1 shard 0 differ in
ingest hour (242M against 167M records), in binary, and possibly in host. The
issue's 2x compares two runs that differ in more than the phase-B change.

## Follow-up

1. Re-measure the production pair with the binary as the only variable: the
   pre-phase-B and the phase-B `ravel-cli` on the same host, over the same
   sealed hour (M1 shard 0, 167M records). Record level-1 and level-2 core-s
   from the two-level event, plus one single-level pass over a sub-bucket as
   a reference. Expected effect: if this bench holds, the two binaries come
   out within 10% of each other per level, and the doubling disappears from
   the comparison. If the pre-phase-B binary really does run at about
   27 us/record per level on the same input, the cause is outside every
   field this bench varies, and the next step is a profile of that pair.
2. Production change, described and not made here: write level-1 runs with
   no token blooms. This is a new run-only writer setting, because
   `BloomScope` has no empty variant today; `BloomScope::Text` is the
   nearest existing one and keeps the `body` and `severity_text` blooms.
   - Expected effect: level 1 sheds its share of the bloom-staging leaves.
     Those leaves hold 81 sampled core-s over both levels' encode, about
     15 us/record. The marker split cannot say how much of that is level
     1's. If half is, level 1 drops by about 7.5 us/record, about 15% of its
     50.6. That is an estimate from leaf shares, not a measurement.
   - Before committing to it, check that a run without blooms is still a
     valid RLOG object under docs/log-segment-format.md, because the format
     is frozen. If it is not, this needs an ADR.
   - Measure it with one arm against A on this bucket, and in the same run
     add the missing control: a single-level merge of this same 460-input
     bucket in cursor read mode, so each level's CPU per record can be
     compared against one pass over the same records and read mode.
3. Level-2 worker derivation: never derive W2 = 1 into the pipelined path
   when inline encode fits, because the pipelined path costs 25-30% more
   level-2 CPU per record. Today W2 = 1 is reached by configuration, by a
   unit with one range, or by derivation on a tight ceiling. Expected effect:
   up to -20% level-2 CPU on those units. D at 61.6 against A at 49.3 bounds
   it.

## Class closure

Every `CompactorConfig` field that ADR-3853 phase B (#4107, #4108, #4109)
added, or whose default it changed:

| Field | Phase-B change | Varied by | Verdict |
|---|---|---|---|
| `rlog_run_block_target_records` | new; default 1,024 (`RUN_BLOCK_TARGET_RECORDS`), was the writer's 8,192 | B, E | no effect that counts at either level |
| `level2_workers` | new; `None` derives W2 jointly with W1 | C, C1, and D and E through the one-range cap | W2 = 1 (pipelined) costs +25-30% level-2 CPU; the phase-B default is the cheaper one |
| `rlog_range_records` | new; default 2,500,000 (`LEVEL2_RANGE_RECORDS`) | D against C1 | -1.7% level 2; no effect |
| `level1_workers` | field predates phase B (#3874); phase B changed its `None` derivation to the joint W1/W2 rule | C (W1 2), F (W1 4), pins in B, C1, E | no effect on level-1 CPU per record from 1 to 4 workers |
| `unit_concurrency` | core share amendment (ADR-3853 D4); default 1 | not varied | it changes only the core share the worker counts derive from, so it acts through W1 and W2, which the arms above varied directly without moving level-1 CPU and with level-2 CPU moving only for W2 = 1 |
| `rlog_run_memory_target_bytes`, `scratch_dir`, `scratch_max_bytes`, `geometry_floor_decoded_rows`, `merge_resident_ceiling_bytes` | two-level plumbing from phase A; not changed by phase B | `scratch_dir` and the ceiling are set on every arm to reach the route; the others are not varied | the route preconditions and the run split are the same in every arm, and none of them is a phase-B change |

`git log` in this checkout holds only the commits since dispatch, so the
added fields and changed defaults above come from the field documentation in
crates/ravel-maintain/src/config.rs and ADR-3853, not from a diff of the
phase-B commits.

## Findings outside this task's scope (reported, not fixed)

- The joint W1/W2 derivation's price for a level-2 block is about 14x too
  pessimistic on this bucket. It predicts 447,273,370 bytes for an
  8,192-record run block (about 55 KB per record), while A measured
  3,921,438 bytes per 1,024-record block (about 3.8 KB per record). The
  prediction refuses `--run-block-records 8192` with derived workers
  (`probe-rb8192`) and drives W1 to 1 at U = 4 GiB.
- ADR-3853 D0 says fetch-per-pass plans also take the two-level route,
  while `compact_bucket` routes only cursor plans there.
