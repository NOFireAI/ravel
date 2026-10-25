# bbtree's single-owner map on the compaction grouping stage (issue #3254)

Whether `bbtree::unsync::BBTreeMap` (bbtree 0.1.5) beats std's `BTreeMap`
on one Ravel hot path: the series grouping map of metrics compaction. Every
figure below names the command that produced it and the run it came from.
Logs live under `.gate-logs/` (ignored) in the checkout that ran them; the
numbers are copied here.

## What was measured and why this candidate

`build_parts` in `crates/ravel-maintain/src/build.rs` runs on every metrics
(RSEG) compaction. Its grouping stage is:

```rust
let mut by_series: BTreeMap<[u8; 16], Vec<(usize, &SeriesPlan)>> = BTreeMap::new();
for (idx, catalog) in catalogs.iter().enumerate() {
    for series in &catalog.series {
        by_series.entry(series.series_id.0).or_default().push((idx, series));
    }
}
...
for (_id, contributions) in by_series { /* mixed-kind check, collect runs */ }
```

Per input series (one contribution): one `entry` lookup on a 16-byte key,
`or_default` (a new empty `Vec` the first time an id is seen) and one
`push`, whose `Vec` reallocates at 4, 8, 16, ... contributions. Then one
ordered drain: every group is moved out once, in ascending id order, and the
consume loop reads each contribution (input index, plan kind, runs) and
drops the `Vec`. Inserting in canonical input order is what puts each
group's contributions in canonical input order, the run tie-break rule; the
drain order is what makes parts contiguous, disjoint id ranges.

The exemplar map at `build.rs:203` (`BTreeMap<[u8; 16], Vec<ExemplarInput>>`)
has a different consume shape: it is filled the same way, but consumed by a
keyed `remove` per emitted series, then `keys()` over the leftovers for the
conservation error. Its population is the input objects' EXEMPLARS sections,
which are optional and absent unless a sample carried an exemplar
(docs/segment-format.md, "EXEMPLARS (kind 10) is optional"). Of the
ravel-maintain integration tests, only `tests/force2_reencode.rs` builds an
input with exemplars, and its L0 helper says "L0 fixtures carry no
histograms or exemplars". The exemplar group is therefore skipped here.

### What else the build does (read from `build.rs` and `read.rs`, not measured)

Around the grouping stage, the same compaction:

- before it, loads every input's catalog (`read.rs`, `load_catalog_from_object`):
  a footer GET and catalog section GETs per input, then
  `decode_catalog_plans`, which per input series decodes the catalog entry,
  clones its `LabelSet` (one `Vec` plus a name and a value `String` per
  label) and allocates its `runs` `Vec`. So every contribution the grouping
  stage inserts already cost at least two heap allocations and a catalog
  decode;
- after it, per series: coalesced, concurrent page GETs per fetch window
  (`fetch_batch_pages`), then `materialize_series` on the read gate, which
  copies a single-run series' page bytes and, for a multi-run series,
  decodes every run's pages, merges and sorts the samples, and re-encodes
  one run with a provenance column;
- per part: `SegmentWriter::write_v7_with_provenance` encodes the object,
  BLAKE3 hashes it for the content-addressed key, and one `CreateIfAbsent`
  PUT writes it.

The grouping stage touches no page bytes, issues no request, and does one
map operation per contribution against at least one catalog decode, two
allocations and a share of a GET per contribution before it. So, by
reading, it is a minority of the catalog-load stage's CPU, which is itself
one of three CPU stages next to object-store latency. That reading bounds
the share from above only loosely: it cannot by itself say whether the
grouping stage is above or below 2% of a whole build's CPU on a large
compaction, since a series with few samples per contribution makes
`materialize_series` cheap. The decision rule's 2% clause is therefore
settled by measurement, and only if an arm passes the stage-level rule
(below).

### Fixture geometry

The existing tests do not exercise scale. `tests/determinism.rs` merges 3
inputs of 1 or 2 series; `tests/memory.rs` (RSEG half) merges 3,600 inputs
of the same 10 hot series; `tests/compaction_clustered.rs` is the RLOG path
(KEYS `a`,`b`,`c` over 2 days x 2 streams). So the benchmark uses the
geometries the issue names instead, chosen to span the axes the map's cost
depends on: input count N (contributions per group), series per input S
(map size), and the fraction of ids shared across inputs (new-key inserts
versus pushes to an existing group):

| geometry | inputs N | series per input S | repeated | contributions | groups |
|---|---|---|---|---|---|
| small | 4 | 1,000 | 100% | 4,000 | 1,000 |
| medium | 8 | 50,000 | 100% | 400,000 | 50,000 |
| large | 16 | 200,000 | 100% | 3,200,000 | 200,000 |
| distinct | 8 | 50,000 | 0% | 400,000 | 400,000 |
| mixed | 8 | 50,000 | 50% | 400,000 | 225,000 |

A bucket is one (tenant, signal, shard, ingest hour), so N is the number of
L0 flushes a shard wrote in that hour and S its active series; 100% repeated
is the steady state of a scraped fleet, 0% is pure churn. Series ids are
random 16-byte values from `StdRng::seed_from_u64(0x3254_0b7e_e5ee_d001)`,
which is what hashed series ids look like, and each input's series are
sorted by id, as `InputCatalog::series` is. Plans are real
`ravel_maintain::read::SeriesPlan` values with empty labels and runs: the
map only stores a reference to the plan, so the plan's heap payload is not
part of the stage.

## Method

- `crates/ravel-maintain/benches/compaction_grouping.rs` (criterion, plain
  system allocator, no instrumentation) times build plus drain plus consume
  per iteration. Arms, in `benches/common/grouping.rs`:
  - A: `std::collections::BTreeMap<[u8; 16], Vec<(usize, &SeriesPlan)>>`,
    the code as it is;
  - B: `bbtree::unsync::BBTreeMap` with the same key and value types,
    `entry().or_default().push()`, consumed with `into_iter`;
  - C: `HashMap` with the same types, then the keys collected into a `Vec`,
    `sort_unstable`, and each group `remove`d in that order; the key `Vec`
    and the sort are charged to C.
  Before timing, every arm's order-sensitive checksum (over drain order and
  per-group contribution order) is asserted equal to A's on every geometry.
- Run 1: `GROUPING_ARM_ORDER=ABC`; run 2: `GROUPING_ARM_ORDER=CBA`, so the
  baseline runs first in run 1 and last in run 2. Both:
  `CARGO_BUILD_JOBS=2 cargo bench -p ravel-maintain --bench
  compaction_grouping -- --noplot`, release (`bench`) profile, default
  target CPU.
- `crates/ravel-maintain/benches/compaction_grouping_alloc.rs` measures peak
  live bytes and allocation counts on `large`, one untimed run per arm. The
  task asked for a counting allocator in the timing binary, a static
  `AtomicUsize` wrapper around `System`; the workspace sets
  `unsafe_code = "forbid"`, an `impl GlobalAlloc` needs `unsafe`, and the one
  allocator that counts without it (`stats_alloc`, already pinned at 0.1.10
  by four crates) counts every call with no off switch. So memory is its own
  binary and the timing binary is uninstrumented. See that file's header for
  the sampled peak versus the transient bound.

## Pre-registered expectations and decision rule

Written and committed before any B or C figure was produced.

Expected A medians on this box (Raspberry Pi 5, see Environment), with the
band outside which the figure counts as a miss to investigate before
anything is built on it:

| geometry | expected A median | miss band |
|---|---|---|
| small | 0.2 ms | outside 0.07 to 0.6 ms |
| medium | 60 ms | outside 20 to 180 ms |
| large | 700 ms | outside 230 ms to 2.1 s |
| distinct | 120 ms | outside 40 to 360 ms |
| mixed | 90 ms | outside 30 to 270 ms |

Expected A memory on large: peak live about 64 MB (200,000 groups x a
16-slot `Vec` of 16-byte contributions = 51.2 MB, plus about 12 MB of tree
nodes at 40 bytes per entry and two-thirds leaf fill); about 200,000
allocations and 400,000 reallocations (each group's `Vec` allocates at 4 and
reallocates to 8 and to 16), plus tens of thousands of node allocations.
Miss band: peak outside 40 to 100 MB.

Expectations for the alternatives, stated so the result can surprise: B
within 20% of A either way on every geometry (its inline `u128` prefix key
makes a compare cheap, but A's compare of a 16-byte array is already
cheap and both are bound by cache misses on the descent); C 20 to 40%
faster than A on medium and large (one hash probe instead of a descent) and
closest to A on distinct, where it pays a 400,000-key sort; C's peak within
10% of A's, since the contribution `Vec`s dominate both and its resize
transient (old plus new table) is about 16 MB at 200,000 groups.

Decision rule (fixed in the issue before measuring): adopt an alternative
only if, in both runs, it is at least 10% faster than A on medium AND large,
no slower than A beyond noise on small and distinct, and its peak live bytes
on large are not more than 5% above A. If B and C both pass, take the one
with the smaller diff in `src/`. If the grouping stage is under 2% of a
whole compaction build, a stage win stays a documented experiment and
`src/` does not change. "Beyond noise" is read as: the alternative's
criterion confidence interval lies wholly above A's.

## Environment and versions

- `uname -m`: aarch64 (Raspberry Pi 5, kernel 6.12.25+rpt-rpi-2712);
  `nproc`: 4; MemTotal 8,257,232 kB; cpufreq governor `ondemand`, max
  2.4 GHz.
- rustc 1.97.1 (8bab26f4f 2026-07-14), LLVM 22.1.6, host
  aarch64-unknown-linux-gnu. bbtree 0.1.5 built on this stable toolchain
  with no feature flags or toolchain change.
- criterion 0.8.2, stats_alloc 0.1.10, rand 0.10.2 (Cargo.lock).
- `CARGO_BUILD_JOBS=2` on every cargo command.
- Run 1 started at load average 0.12. Run 2 started at load average 3.04:
  another session on the host was compiling (two `rustc` processes) during
  it. Run 2's absolute figures are therefore noisier; its A-to-B and A-to-C
  ordering is the same as run 1's on every geometry.
- Benches use the system allocator. The shipped `ravel-server` and
  `ravel-cli` binaries use jemalloc, so allocation-heavy arms may rank
  differently there (see Not tested).

## Results

Medians and criterion 95% confidence intervals of the median, in ms, from
`target/criterion/grouping_<geometry>/<arm>/new/estimates.json`.

Run 1, `GROUPING_ARM_ORDER=ABC`:

| geometry | A std BTreeMap | B bbtree | C HashMap + sort | B vs A | C vs A |
|---|---|---|---|---|---|
| small | 0.362 (0.360 to 0.363) | 0.520 (0.517 to 0.522) | 0.363 (0.357 to 0.364) | +44% | +0% |
| medium | 72.7 (71.4 to 73.7) | 84.2 (83.6 to 84.5) | 100.7 (97.8 to 101.7) | +16% | +38% |
| large | 570.9 (561.2 to 580.7) | 630.2 (620.2 to 633.2) | 892.2 (890.3 to 902.6) | +10% | +56% |
| distinct | 198.0 (193.1 to 221.0) | 174.5 (172.1 to 177.4) | 259.1 (242.5 to 259.4) | -12% | +31% |
| mixed | 107.7 (107.1 to 107.9) | 127.6 (127.4 to 128.5) | 172.6 (172.4 to 172.7) | +19% | +60% |

Run 2, `GROUPING_ARM_ORDER=CBA`:

| geometry | A std BTreeMap | B bbtree | C HashMap + sort | B vs A | C vs A |
|---|---|---|---|---|---|
| small | 0.353 (0.352 to 0.353) | 0.507 (0.507 to 0.507) | 0.337 (0.337 to 0.337) | +44% | -5% |
| medium | 66.9 (66.2 to 67.4) | 77.4 (76.8 to 77.9) | 105.7 (105.2 to 107.9) | +16% | +58% |
| large | 589.2 (583.9 to 598.0) | 639.4 (631.8 to 642.9) | 954.5 (877.0 to 976.3) | +9% | +62% |
| distinct | 224.4 (210.5 to 253.5) | 199.2 (198.0 to 200.7) | 323.1 (318.7 to 329.5) | -11% | +44% |
| mixed | 119.7 (118.2 to 120.6) | 141.2 (140.2 to 143.7) | 215.1 (213.8 to 219.2) | +18% | +80% |

Every arm's checksum matched A's on every geometry before timing (the bench
asserts it). Every A median fell inside its pre-registered band. Two
pre-registered expectations missed:

- C was expected 20 to 40% faster than A on medium and large; it was 38 to
  62% slower. Hypothesis, not tested: each input's series arrive sorted by
  id, so A's successive inserts land in the same or the adjacent leaf and
  its descent stays in cache, while C's hashed probes scatter across a
  table far larger than the cache. Shuffling each input's order would test
  it (see Not tested).
- B was expected within 20% of A everywhere; on small it was 44% slower in
  both runs. On the other geometries it was within 20% (9 to 19% slower on
  the 100%- and 50%-repeated ones, 11 to 12% faster on distinct).

## Memory

`CARGO_BUILD_JOBS=2 cargo bench -p ravel-maintain --bench
compaction_grouping_alloc -- large` (exit 0; 3,200,000 contributions,
200,000 groups, 3,400,000 samples for A and B and 3,400,001 for C, as the
binary checks; every arm left 0 bytes live and produced checksum
0x699fda67775cc32f):

| arm | peak live, sampled (bytes) | peak live, bound (bytes) | allocations | reallocations |
|---|---|---|---|---|
| A std BTreeMap | 66,855,896 | 66,856,024 | 233,331 | 400,000 |
| B bbtree | 64,174,145 (-4.0%) | 64,174,273 | 212,839 | 400,000 |
| C HashMap + sort | 65,147,912 (-2.6%) | 65,147,912 | 200,018 | 400,000 |

A's peak sits inside its pre-registered 40 to 100 MB band, and its 400,000
reallocations match the predicted two per group. The 33,331 allocations
above one per group are A's tree nodes; B allocates 12,839 nodes and C
18 (table growth plus the key `Vec`).

## Decision

No change to `build.rs`. Neither alternative is at least 10% faster than A
on medium and large in either run; both are slower on both (B +9 to +16%,
C +38 to +62%). B passes the memory clause (-4.0%) and fails the speed
clause; C fails the speed clause. Because no arm passed the stage-level
rule, the 2% whole-build clause was not reached and the stage was not
profiled against a whole build. The benchmark stays in the tree as the
record of the measurement.

## Not tested

(filled in after the runs)
