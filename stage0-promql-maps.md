# Stage 0 measurement: PromQL aggregation/vector-matching index work

Issue #2443 (epic #2425). Measurement only: the branch carrying this file and
the scaffolding it describes is never merged; the orchestrator reads this
report directly.

## Host and run

- Host: `ci-16gb-fsn1-1`, `Linux 6.8.0-60-generic #63-Ubuntu SMP PREEMPT_DYNAMIC
  Tue Apr 15 19:04:15 UTC 2025 x86_64 x86_64 x86_64 GNU/Linux` (amd64 fleet
  executor class, 8 vCPU). This host is itself a fleet executor (see
  CLAUDE.md "Fleet executor environment"); its own queued builds run here
  unannounced, and both `uptime` lines below show load well above the core
  count, so absolute `op_ns` figures carry contention noise. The driver
  interleaves mode 0/mode 1 and all three cases every iteration specifically
  so a transient contention burst lands on both modes rather than skewing one
  side of the share calculation.
- `uptime` before: `11:06:36 up 421 days, 20:23,  0 user,  load average: 6.48, 5.04, 3.68`
- `uptime` after: `11:09:01 up 421 days, 20:25,  0 user,  load average: 7.50, 6.22, 4.34`
- Command: `cargo run --release -p ravel-promql --example stage0_maps`
- `CARGO_BUILD_JOBS=2` (8 cores, min(4, max(2, cores/4)) = 2; RAM is 15 GB, so
  the <=11 GB override did not apply).
- `df -h /tmp . "$HOME"` (unchanged since before the run):

  ```
  Filesystem      Size  Used Avail Use% Mounted on
  /dev/sda1       226G  137G   80G  64% /
  /dev/sda1       226G  137G   80G  64% /var/lib/fleet
  tmpfs           256M   64K  256M   1% /var/lib/fleet/secrets
  ```

## Method notes / deviations

- Entry points called directly (no parser, no `Evaluator`/`SeriesSource`,
  no storage): `aggregate::eval_plain_aggregate(T_SUM, ...)` via a
  doc-hidden `stage0_eval_sum_by` wrapper, and `binop::one_to_one` via a
  doc-hidden `stage0_one_to_one_add` wrapper (`lhs + rhs`, no modifier).
  This skips `eval_aggregate`'s pre-dispatch sort of the input vector by
  label set, which is unrelated to the grouping index measured here, and
  skips `eval_binary`/`eval_vector_vector`'s cardinality dispatch, which is
  irrelevant once the cardinality (`OneToOne`) is already known and
  `one_to_one` itself only reads `modifier.matching`/`modifier.return_bool`.
- MATCH_HALF's binary operation is addition (`+`) with no `on`/`ignoring`
  modifier, i.e. matching on the full label set minus `__name__` — this
  crate's (and Prometheus') default.
- `one_to_one` touches three map/set structures, not one: `rhs_map` (built
  by `insert`, duplicate-checked), the lhs-probes-`rhs_map` lookup (`get`,
  the structure the MATCH_HALF pre-registered assertion is about), and
  `matched_sigs` (an `insert`-based dedup check, run only for lhs entries
  that found a match). All three are replayed and timed; `MATCH_INDEX_NS`
  in this report is their sum. In this benchmark's well-formed inputs (every
  signature on either side is globally unique except for the intended
  50,000 matched pairs), the `rhs_map` build and `matched_sigs` dedup are
  always misses (100,000 and 50,000 respectively) — expected, not a defect,
  since neither side ever contains a real duplicate signature here.
- Stage 1 mode records, per real-path sample, a clone of that sample's raw
  labels (so the post-pass replay can recompute keys rather than reuse
  recorded ones) — this recording is itself extra work included in mode 1's
  measured operator time but is neither index time nor key-build time. It
  inflates `mode 1 operator time - replayed index time` (the denominator of
  INDEX_SHARE) beyond the true mode-0-equivalent cost, which biases the
  reported INDEX_SHARE **down** (in addition to the "replay measures cost
  from below" bias already called out below). Both biases push the same
  direction, so the numbers below should be read as conservative.
- The replay's HashMap/HashSet starts empty and is filled in one pass with
  the keys already hot in CPU cache (just built, in L1/L2) and no
  concurrent mutation of `groups`/`out` alongside it. It is a reasonable
  proxy for the real in-place cost but **estimates that cost from below**:
  the real maps grow interleaved with other per-sample work, with less
  favorable cache locality.
- Because of the above, operator time (mode 0) is itself an **upper bound**
  on real query share: a real query also reads and decodes storage, which
  this benchmark does not do at all (in-memory `InstantVector`s built
  outside the timed region).

## Results

| Case | mode0 op (ns) median [min,max] | mode1 op (ns) median [min,max] | replayed index (ns) median [min,max] | KEY_BUILD (ns) median [min,max] | hits | misses |
|---|---|---|---|---|---|---|
| AGG_100 | 81,285,633 [79,428,008, 91,770,807] | 158,184,770 [153,416,147, 180,334,885] | 10,855,932 [10,358,962, 11,933,410] | 11,196,792 [10,651,500, 12,662,803] | 99,900 | 100 |
| AGG_10000 | 104,288,484 [98,993,886, 125,866,030] | 228,313,999 [224,596,668, 258,204,705] | 27,293,920 [24,594,088, 29,894,350] | 16,815,948 [16,288,466, 19,559,882] | 90,000 | 10,000 |
| MATCH_HALF | 471,270,990 [448,997,238, 537,118,080] | 1,129,129,121 [1,108,082,217, 1,318,044,877] | 150,957,944 [149,487,847, 189,524,230] | 93,133,695 [91,973,854, 107,442,716] | 50,000 | 50,000 |

INDEX_SHARE = replayed index time / (mode 1 op time − replayed index time), per run, then median/min/max over the 5 runs. KEY_BUILD_SHARE is the same formula with KEY_BUILD time in the numerator.

| Case | INDEX_SHARE median [min,max] | KEY_BUILD_SHARE median [min,max] | Pre-registered band | Verdict |
|---|---|---|---|---|
| AGG_100 | 7.26% [7.09%, 7.41%] | 7.60% [7.35%, 7.68%] | 20%–50% | **below band** (below the 25% no-table-work floor) |
| AGG_10000 | 13.09% [12.30%, 13.60%] | 8.33% [8.08%, 8.78%] | 20%–50% | **below band** (below the 25% no-table-work floor) |
| MATCH_HALF | 15.70% [15.33%, 16.79%] | 9.56% [9.42%, 9.65%] | 30%–70% | **below band** (below the 25% no-table-work floor) |

Per the pre-registration: "Below 25% in every case means no table work." All
three cases measured below 25%. This measurement does **not** support a
hash-table-focused Stage 1; the share of operator time spent on
hashing/cloning/probing/inserting into the grouping or matching
maps/sets is small relative to the rest of each operator's own work, even
before accounting for the conservative biases noted above (which, if
corrected for, would lower these numbers further, not raise them above the
floor).

Probe counts, from the real (non-replayed) code path, mode 1:

- AGG_100: 100,000 probes (100,000 input series), 100 miss (new groups), 99,900 hit — exactly 100 output groups.
- AGG_10000: 100,000 probes, 10,000 miss, 90,000 hit — exactly 10,000 output groups.
- MATCH_HALF: 100,000 probes on the lhs-probes-rhs_map lookup, 50,000 miss, 50,000 hit — exactly 50,000 output series.

## What the index stores (from reading the code)

`aggregate::group_by`'s index is `HashMap<LabelSet, usize>`; `LabelSet` is
`ravel_types::LabelSet(Vec<Label>)` with `Label { name: String, value:
String }` (`crates/ravel-types/src/lib.rs`). `std::mem::size_of::<LabelSet>()`
is 24 bytes (one `Vec`'s pointer/len/cap) and `size_of::<Label>()` is 48
bytes (two `String`s) — the type's own stack footprint, not its true cost.
The true cost is in the heap: `group_labels`/`matching_signature` build the
key by `.cloned()`-ing the kept `Label`s, which clones both the `name` and
`value` `String`s (new heap allocations each), on top of the `Vec<Label>`
buffer itself. The index owns a full, independent clone of the key
(`index.insert(key.clone(), i)`) separate from the copy the output also
keeps: for aggregation, `groups.push((key, Vec::new()))` pushes the
*original* (un-cloned) key, so the index and the output group each end up
with their own heap-allocated `LabelSet`, not a shared/reference-counted
one. For `one_to_one`, the same pattern holds across all three structures
(`rhs_map`, `matched_sigs`) and the output sample's own label set
(`one_to_one_output_labels`): none of them alias another's allocation. The
2-label (by two of six) AGG key and the 5-label (all-but-`__name__`)
MATCH_HALF key both carry several small heap allocations per clone (one
`Vec<Label>` buffer plus two `String` allocations — name and value — per
kept label), which is consistent with KEY_BUILD being a comparable or
larger share of operator time than the index probe/insert itself in every
case measured above.

## Appendix: raw per-run figures

CSV as printed by the driver (`case,mode,run_idx,op_ns,index_ns,key_build_ns,hits,misses,output_len`):

```
AGG_100,0,0,91770807,0,0,0,0,100
AGG_100,0,1,79428008,0,0,0,0,100
AGG_100,0,2,81285633,0,0,0,0,100
AGG_100,0,3,81706748,0,0,0,0,100
AGG_100,0,4,79472240,0,0,0,0,100
AGG_100,1,0,180334885,11933410,12662803,99900,100,100
AGG_100,1,1,153416147,10589138,10971538,99900,100,100
AGG_100,1,2,158184770,10855932,11196792,99900,100,100
AGG_100,1,3,166495152,11272198,11791769,99900,100,100
AGG_100,1,4,155315420,10358962,10651500,99900,100,100
AGG_10000,0,0,125866030,0,0,0,0,10000
AGG_10000,0,1,104288484,0,0,0,0,10000
AGG_10000,0,2,100974690,0,0,0,0,10000
AGG_10000,0,3,113191127,0,0,0,0,10000
AGG_10000,0,4,98993886,0,0,0,0,10000
AGG_10000,1,0,258204705,29894350,19559882,90000,10000,10000
AGG_10000,1,1,228313999,27334523,16749937,90000,10000,10000
AGG_10000,1,2,235374377,27293920,16815948,90000,10000,10000
AGG_10000,1,3,228203306,25382791,17815001,90000,10000,10000
AGG_10000,1,4,224596668,24594088,16288466,90000,10000,10000
MATCH_HALF,0,0,537118080,0,0,0,0,50000
MATCH_HALF,0,1,472663922,0,0,0,0,50000
MATCH_HALF,0,2,471270990,0,0,0,0,50000
MATCH_HALF,0,3,466368795,0,0,0,0,50000
MATCH_HALF,0,4,448997238,0,0,0,0,50000
MATCH_HALF,1,0,1318044877,189524230,107442716,50000,50000,50000
MATCH_HALF,1,1,1108082217,149487847,92496256,50000,50000,50000
MATCH_HALF,1,2,1148312869,159675462,93133695,50000,50000,50000
MATCH_HALF,1,3,1129129121,150047545,94089853,50000,50000,50000
MATCH_HALF,1,4,1112675111,150957944,91973854,50000,50000,50000
```

Each row is already a mean over `ITERS_PER_RUN` = 10 iterations (3 warm-up
iterations discarded before the 5 runs above); `index_ns`/`key_build_ns` are
only populated for mode 1 (mode 0 runs the unmodified operator with no
replay).
