# bbtree's single-owner map on two Ravel hot paths (issue #3254)

Whether `bbtree::unsync::BBTreeMap` (bbtree 0.1.5) beats std's `BTreeMap`
on two Ravel hot paths: the series grouping map of metrics compaction
(candidate 1, the sections up to Decision) and the RLOG writer's postings
maps (candidate 2, its own section below). Every
figure below names the command that produced it and the run it came from.
The original runs were made on 2026-10-25 from base commit 4303edc77; their
logs were not kept, and the numbers are copied here. The evidence that the
expectations were written before the results is the commit order on the
task checkpoint ref
`refs/heads/task/602cef5b-bf26-4fcf-ad5a-b16532c82482/checkpoint`:
482999253 pre-registers candidate 1 and f168d72bc candidate 2, each
committed before the commit that records that candidate's results.

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
histograms or exemplars". That is test coverage, not evidence about
production: how often production inputs carry exemplars is untested. The
exemplar group is skipped here and left untested.

### What else the build does (read from `build.rs` and `read.rs`, not measured)

Around the grouping stage, the same compaction:

- before it, loads every input's catalog (`read.rs`, `load_catalog_from_object`):
  a footer GET per input, then either three ranged catalog section GETs or,
  for an object at or above `V5_SPARSE_THRESHOLD` (4,096 series,
  `crates/ravel-segment/src/format.rs`), which carries the sparse catalog,
  one GET of the whole object (`read.rs`, the `sparse` branch); every
  geometry below except `small` has inputs above that threshold. Then
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
allocations and a share of a GET per contribution before it; above the
sparse threshold that GET is the whole object, which only widens the gap.
So, by
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
    `entry().or_default().push()`, consumed with `into_iter` (removed from
    the landed benches after measuring; see Reproducing arm B);
  - C: std SipHash `HashMap` (`std::collections::HashMap` with its default
    `RandomState`), no capacity hint, remove-per-key: the same key and value
    types, then the keys collected into a `Vec`, `sort_unstable`, and each
    group `remove`d in that order; the key `Vec` and the sort are charged to
    C. Other hash-then-sort variants (a faster hasher, a capacity hint, a
    sort of the drained `(key, group)` pairs instead of a keyed `remove`)
    were not measured, so C's result is about this variant only.
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
  by six other workspace crates) counts every call with no off switch. So
  memory is its own binary and the timing binary is uninstrumented. See
  that file's header for the sampled peak versus the transient bound.

### Reproducing arm B

Arm B is not in the landed benches: a dev-dependency is compiled by every
`cargo test -p ravel-maintain` and every `--all-targets` clippy, and the
result says not to use bbtree. To regenerate a B figure, add `bbtree =
"=0.1.5"` to `[dev-dependencies]` in `crates/ravel-maintain/Cargo.toml`,
add `'B'` to `arm_label`, `run_arm` and the alloc binaries' arm list, and
put this body back in `benches/common/grouping.rs` as `arm_bbtree`, with
`arm_std_btree`'s signature:

```rust
let mut by_series: bbtree::unsync::BBTreeMap<[u8; 16], Contributions<'_>> =
    bbtree::unsync::BBTreeMap::new();
for (idx, input) in inputs.iter().enumerate() {
    for series in input {
        by_series.entry(series.series_id.0).or_default().push((idx, series));
        tick();
    }
}
let mut acc = 0u64;
for (id, contributions) in by_series {
    consume(&mut acc, &id, contributions);
    tick();
}
acc
```

The postings arm B in `benches/common/postings.rs` is arm A with
`bbtree::unsync::BBTreeMap` and `BBTreeSet` in place of the std types, and
each removed field map converted at the boundary with
`map.into_iter().map(|(t, b)| (t, b.into_iter().collect())).collect()`.

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

Decision rule (fixed before measuring). Issue #3254 asks for at least 10%
in the measured stage, a visible gain in the complete operation and no
material regression in memory; the 5% memory clause and the 2% whole-build
clause below come from the task's dispatch spec, not from the issue. Adopt
an alternative only if, in both runs, it is at least 10% faster than A on
medium AND large, no slower than A beyond noise on small and distinct, and
its peak live bytes on large are not more than 5% above A. If B and C both pass, take the one
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
  it. Run 2's absolute figures are therefore noisier. Its A-to-B and A-to-C
  ordering is the same as run 1's on every geometry but one: on small, C was
  slower than A in run 1 (0.363 against 0.362 ms) and faster in run 2
  (0.337 against 0.353 ms).
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

No change to `build.rs`. Every figure here is from a Raspberry Pi 5
(aarch64, 2 MB L3) on the glibc system allocator; bbtree's search is SIMD
and the cache-miss explanation for C depends on cache size, so neither
result carries to other hardware or allocators without a rerun. Neither
alternative is at least 10% faster than A on medium and large in either
run; both are slower on both (B +9 to +16%, C +38 to +62%). B passes the memory clause (-4.0%) and fails the speed
clause; C fails the speed clause. Because no arm passed the stage-level
rule, the 2% whole-build clause was not reached and the stage was not
profiled against a whole build. The benchmark stays in the tree as the
record of the measurement.

## Candidate 2: the RLOG writer's postings maps

Issue #3263 reran arm C against A on stream-sorted rows with a dhat peak
and a whole-build share; see the Follow-up section below (no change).

### What was measured

`RlogWriter::build_object` and `build_object_columnar` in
`crates/ravel-logseg/src/writer.rs` accumulate the POSTINGS section in

```rust
let mut postings_terms: BTreeMap<u32, BTreeMap<Vec<u8>, BTreeSet<u32>>> = BTreeMap::new();
...
for row in block_rows {
    for (cid, v) in &row.indexed_terms {
        if postings_capped.contains(cid) { continue; }
        let field_map = postings_terms.entry(*cid).or_default();
        field_map.entry(term_key(v)).or_default().insert(blk_idx_u32);
        if field_map.len() > self.cfg.postings_max_distinct { /* drop, mark capped */ }
    }
}
```

and hand each field's term map to the encoder as `FieldTerms::Terms(map)`,
which takes a `BTreeMap<Vec<u8>, BTreeSet<u32>>`. This runs on every logs
flush: `crates/ravel-ingest/src/log_shard.rs` builds an `RlogWriter` with
`with_indexed_fields`, and the server's default indexed fields are
`service.name`, `k8s.namespace.name` and `http.status_code`
(`services/ravel-server/src/postings_config.rs`). RLOG compaction in this
crate re-encodes through the same writer (`RlogWriter::finish_compacted`,
called from `src/rlog.rs`), which is why the bench sits in
`crates/ravel-maintain/benches` next to candidate 1 rather than in
ravel-logseg: one gate scope for both candidates.

Which of the two paths production ingest reaches, as the issue asked: both
accumulate postings the same way (`writer.rs:773` and `:1612`), and
`RlogWriter::build` takes `build_object_columnar` only when
`push_columnar` filled the writer's batches. `services/ravel-server`
reaches the row path, `build_object`: its OTLP logs handlers
(`otlp_http.rs`, `otlp_grpc_logs.rs`) call
`logs_ingest::handle_export_logs_releasing`, which submits through
`LogIngestRouter::submit_admitted`; that sends `LogShardMsg::Write`, whose
flush calls `RlogWriter::push` per record (`log_shard.rs`, the
`FlushPayload::Rows` arm). The columnar path is reached through
`LogIngestRouter::write_columnar`, which ravel-server does not call
(ravel-bench's columnar load and ravel-cli's load do). RLOG compaction in
this crate also pushes rows (`src/rlog.rs`, `w.push(r)`).

Per indexed term: one `term_key` (an owned `Vec<u8>` clone of the value, or
the 8 big-endian bytes of an integer), a lookup in a 3-entry outer map, an
`entry` on the field's term map (a `Vec<u8>` key compare per node visited;
the key is dropped when the term already exists), and a `BTreeSet<u32>`
insert of the block index, which is a no-op after a term's first row in a
block. Then one ordered conversion per field at the encoding boundary.

### Fixture and arms

`crates/ravel-maintain/benches/common/postings.rs`. Rows of 8,192 per block
(`block_target_records`), each with three indexed values drawn uniformly:
two string fields whose terms are 8 to 40 bytes over 4 or 5 shared prefixes
(`payments-`, `prod-eu-west-1-`, ...), and an integer status field with 20
values. Seed `0x3254_0b7e_e5ee_d002`. No geometry reaches the 10,000
distinct-term cap (`postings_max_distinct`), so every insert is counted.

| geometry | blocks | rows | inserts | string field terms |
|---|---|---|---|---|
| small | 2 | 16,384 | 49,152 | 200 and 40 |
| medium | 16 | 131,072 | 393,216 | 200 and 40 |
| large | 64 | 524,288 | 1,572,864 | 2,000 and 200 |
| highcard | 16 | 131,072 | 393,216 | 5,000 and 5,000 |

Arms: A, std maps as the writer has them; B, `bbtree::unsync::BBTreeMap`
and `BBTreeSet` throughout, converted to the std types at the boundary; C,
std SipHash `HashMap` (default `RandomState`), no capacity hint, for the
outer and term maps (block sets stay `BTreeSet`), each field's term map
removed and drained by `into_iter().collect()` into a `BTreeMap` at the
boundary (std's `from_iter`: collect into a `Vec`, sort, bulk build). No
other hasher or capacity was measured. Conversions are
charged to their arm. A checksum over every field, term and block id in the
converted maps, in order, is asserted equal to A's before timing. Timing:
`benches/rlog_postings.rs`, same command shape and same two arm orders as
candidate 1. Memory: `benches/rlog_postings_alloc.rs` on large.

Row order caveat: the fixture draws `service.name` and
`k8s.namespace.name` at random per row. The writer sorts rows by
`(stream_ref, ts_ns)` before cutting blocks (`writer.rs:721`; the columnar
path applies the same order at `:1543`, and a clustered sort descriptor
still leads with `stream_ref`), and those two names are resource
attributes, so per stream: in a real block they arrive in long runs of one
value. Random order is the worst case for a `BTreeMap`:
successive inserts land in unrelated leaves, where on sorted input they
repeat the previous insert's path. The fixture was not changed in this
round.

### Pre-registered expectations

Written and committed before any figure from these benches was produced.

| geometry | expected A median | miss band |
|---|---|---|
| small | 7 ms | outside 2.3 to 21 ms |
| medium | 60 ms | outside 20 to 180 ms |
| large | 280 ms | outside 90 to 840 ms |
| highcard | 80 ms | outside 27 to 240 ms |

Basis: candidate 1's A cost about 180 ns per contribution on this box;
per term here, a small allocation and free (the term key), a descent over
at most a few thousand terms with prefix-sharing compares, and a set insert
that is usually a hit.

Expected A memory on large: peak live about 1 MB (2,220 terms, each a key
of about 25 bytes plus a block set of up to 64 ids plus its tree slot), miss
band outside 0.3 to 3 MB; allocations about 1.58 million, dominated by one
`term_key` per insert; reallocations near zero.

Expectations for the alternatives: B within 20% of A either way; C 10 to 30%
faster than A on medium, large and highcard (one SipHash of about 25 bytes
against a descent with several prefix-sharing compares); C's peak within
10% of A's.

Decision rule: candidate 1's, with highcard in distinct's role (the
geometry with the most distinct keys): adopt only if, in both runs, the arm
is at least 10% faster than A on medium AND large, no slower beyond noise on
small and highcard, and its peak live bytes on large are not more than 5%
above A. If both pass, take the smaller `src/` diff. If the accumulation is
under 2% of a whole RLOG object build, a stage win stays a documented
experiment.

### Results

Same box, toolchain and `CARGO_BUILD_JOBS=2` as candidate 1. Both runs:
`GROUPING_ARM_ORDER=<order> CARGO_BUILD_JOBS=2 cargo bench -p
ravel-maintain --bench rlog_postings -- --noplot`; medians and 95%
confidence intervals of the median, in ms, from
`target/criterion/postings_<geometry>/<arm>/new/estimates.json`. Another
session on the host was compiling during both runs (load average 1.41
rising to 2.94 across run 1, 2.66 rising to 3.86 across run 2, one `rustc`
at 99.7% CPU when run 2 started), so absolute figures moved between runs:
A on large read 482.1 ms in run 1 and 340.4 ms in run 2.

Run 1, `GROUPING_ARM_ORDER=ABC`:

| geometry | A std BTreeMap | B bbtree | C HashMap + collect | B vs A | C vs A |
|---|---|---|---|---|---|
| small | 6.027 (6.026 to 6.029) | 9.438 (9.429 to 9.455) | 4.532 (4.529 to 4.534) | +57% | -25% |
| medium | 52.2 (52.2 to 52.2) | 77.3 (76.8 to 77.3) | 40.3 (39.2 to 41.5) | +48% | -23% |
| large | 482.1 (462.4 to 497.6) | 576.6 (567.0 to 612.4) | 301.4 (283.6 to 315.1) | +20% | -37% |
| highcard | 187.2 (184.6 to 190.6) | 242.8 (238.5 to 255.4) | 124.7 (123.0 to 126.4) | +30% | -33% |

Run 2, `GROUPING_ARM_ORDER=CBA`:

| geometry | A std BTreeMap | B bbtree | C HashMap + collect | B vs A | C vs A |
|---|---|---|---|---|---|
| small | 6.330 (6.306 to 6.348) | 10.054 (10.010 to 10.126) | 4.801 (4.781 to 4.835) | +59% | -24% |
| medium | 53.7 (53.7 to 53.8) | 80.2 (79.6 to 80.8) | 41.1 (40.7 to 41.3) | +49% | -23% |
| large | 340.4 (339.1 to 341.0) | 483.6 (441.9 to 555.5) | 230.1 (219.7 to 251.1) | +42% | -32% |
| highcard | 123.3 (122.4 to 192.9) | 231.2 (227.7 to 233.0) | 125.4 (80.3 to 128.5) | +87% | +2% |

Every arm's checksum matched A's on every geometry before timing. Every A
median fell inside its pre-registered band. Pre-registered expectations
that missed: B was 20 to 87% slower, outside the expected 20% either way
on every geometry except run 1 large, where its +20% sits on the boundary;
C on large (32 to 37% faster) beat the expected 10 to 30%; C on highcard
was 33% faster in run 1 and level in run 2, where both A's and C's
intervals are wide (A 122.4 to 192.9 ms, C 80.3 to 128.5 ms).

### Memory

`CARGO_BUILD_JOBS=4 cargo bench -p ravel-maintain --bench
rlog_postings_alloc -- large`, rerun on 2026-10-25 after the bench moved
to one sample per field at the encoding boundary (below), on an x86_64
fleet executor (16 vCPU, glibc system allocator), not the Pi (exit 0;
1,572,864 inserts, 1,572,867 samples per arm as the binary checks; every
arm left 0 bytes live and produced checksum 0x353d05aff71b5dee; the binary
printed `band arm=A geometry=large peak_live_sampled=1681633
band=300000..=3000000 PASS`):

| arm | peak live, sampled (bytes) | peak live, bound (bytes) | allocations | reallocations |
|---|---|---|---|---|
| A std BTreeMap | 1,681,633 | 1,681,633 | 1,595,340 | 0 |
| B bbtree (earlier method, Pi) | 2,674,945 (+59%) | 4,644,873 (not comparable) | 1,593,251 | 0 |
| C HashMap + collect | 1,724,901 (+2.6%) | 2,018,757 (+20.0%) | 1,595,266 | 0 |

B's row is from the first run on the Pi, before arm B was removed, when
the bench sampled once after all three fields' conversions: its bound
sums three conversions and is not comparable with the bound column above.
Its sampled figure is comparable. Figures are requested sizes, so A reads
the same on both machines; C's sampled figure is 32 bytes above the Pi
run's 1,724,869, which matches hashbrown's control bytes (one per bucket
plus one group width: 16 on x86_64, 8 on aarch64) over C's four hash
tables; that attribution is arithmetic, not a measurement of both sample
methods on one machine. The same 8 bytes per
table separate the grouping C figure from the Pi's: this host read
65,147,920 against 65,147,912 (A unchanged at 66,855,896).

A's peak sits inside its pre-registered 0.3 to 3 MB band, and its
allocation count matches the prediction of about 1.58 million (one
`term_key` per insert). The sampled figure is a lower bound on an arm's
true peak and the bound an upper one. For C they straddle the 5% line. The
bench now samples after each field's conversion, so the bound charges one
`BTreeMap::from_iter` at a time instead of all three: C's bound fell from
2,051,781 bytes on the Pi's run to 2,018,757 here (+22% to +20.0% of A;
the hash-table difference between the machines accounts for tens of
bytes of the 33,024), so almost all of the
excess is inside one field's conversion, the 2,000-term `service.name`
map. Within that call `from_iter` collects the hash map into a `Vec`,
drops the hash table, sorts, and builds nodes; the bound adds the `Vec` and
the nodes to the live figure before the call and cannot subtract the table
freed between them. Placing C's true peak between 1,724,901 and 2,018,757
bytes more tightly needs a running maximum inside the allocator (see the
next experiment under Not tested).

### Decision

No change to `writer.rs`. The timing figures are from a Raspberry Pi 5
(aarch64, 2 MB L3) on the glibc system allocator, and the memory figures
for A and C from an x86_64 host on the same allocator; neither ranking is
shown to hold on amd64 production hardware or under jemalloc. B fails the
speed clause on every geometry and
the memory clause. C passes the speed clauses in both runs: at least 10%
faster than A on medium (23%, 23%) and large (37%, 32%), faster than A
beyond noise on small in both runs, and not slower beyond noise on
highcard (its interval in run 2 is not wholly above A's). Those 23 to 37%
are measured on a row order that is the worst case for a `BTreeMap`
(random per row, where real blocks carry each stream's terms in runs) and
may shrink on sorted input. C does not
demonstrably pass the memory clause: its sampled peak is 2.6% above A's,
its upper bound 20.0% above, and the rule adopts only on evidence that the
peak is within 5%. Sampling once per field moved the bound from 22% to
20.0%, still outside 5%: the excess sits inside one field's conversion,
which this method cannot split. Because no arm passed every clause, the 2%
whole-build clause was not assessed. In absolute terms the gap in question
is at most 337,124 bytes (2,018,757 minus 1,681,633) on a 524,288-row
object; the rule is applied as fixed rather than re-weighed after the
fact.

### Follow-up (issue #3263): sorted rows, a true peak, the whole-build share

#### Fixture: the sorted family

`benches/common/postings.rs` now builds two row-order families per
geometry from the same term sets (same seed, same terms, same row counts,
8,192 rows per block, the 10,000 distinct cap). `random` is the fixture
above, unchanged. `sorted` gives each geometry twice its service term count
in streams (400, 400, 4,000 and 10,000), draws each stream's `service.name`
and `k8s.namespace.name` once (seed `SEED ^ 2`), then per row draws a
stream and an `http.status_code` uniformly, and groups the rows by stream
in draw order, which is the writer's `(stream_ref, ts_ns)` order. A block
then carries the two string fields in runs, about 131 rows long on large.
Since streams draw their terms, some terms go unused; the distinct counts
the timing bench prints (`fixture family=sorted ...` lines) are:

| geometry | rows | streams | distinct service, namespace, status (sorted) |
|---|---|---|---|
| small | 16,384 | 400 | 176, 40, 20 |
| medium | 131,072 | 400 | 176, 40, 20 |
| large | 524,288 | 4,000 | 1,705, 200, 20 |
| highcard | 131,072 | 10,000 | 4,339, 4,326, 20 |

#### Method

- Timing: `benches/rlog_postings.rs` runs both families as criterion groups
  `postings_<family>/<geometry>`. Run 1 `GROUPING_ARM_ORDER=AC`, run 2
  `GROUPING_ARM_ORDER=CA`, each `CARGO_BUILD_JOBS=4 cargo bench -p
  ravel-maintain --bench rlog_postings -- --noplot`.
- Peak: `benches/rlog_postings_dhat.rs`, dhat 0.3.3's
  `HeapStats::max_bytes`, the live-byte maximum its global allocator keeps
  on every call. The parent re-executes the binary once per (family, arm),
  so each maximum covers one arm alone; the fixture is built before the
  profiler starts and dhat ignores blocks it did not see allocated.
  `CARGO_BUILD_JOBS=4 cargo bench -p ravel-maintain --bench
  rlog_postings_dhat -- large`. The stats_alloc binary also runs on both
  families (`-- large random`, `-- large sorted`).
- Share: `benches/rlog_postings_share.rs` on sorted large, 21 iterations,
  each timing arm A's loop, `RlogWriter::finish_with_stats` over the same
  rows as `LogRecord`s with the three fields indexed (push included), and
  the same build with no indexed field, in an order that rotates per
  iteration. `share_loop` is loop over indexed build per iteration;
  `share_delta` is the indexed build's extra time over the plain one, which
  also charges merged-view resolution and POSTINGS encoding. Each is the
  median with a distribution-free 95% interval (binomial order
  statistics). The records carry the fixture's two string fields as
  resource attributes and its status as a record attribute, plus the body
  and the three other attributes of the logseg benches' `make_record`.
  `CARGO_BUILD_JOBS=4 cargo bench -p ravel-maintain --bench
  rlog_postings_share`.
- Host: an amd64 fleet executor (x86_64, 16 vCPU, 30.6 GB, glibc system
  allocator, rustc 1.97.1), not the Pi; load average 0.61 before the
  calibration run.

#### Pre-registered expectations and rule (#3263)

Written and committed before any sorted-family figure or any C figure on
this host was produced. One calibration run came first, arm A on the
random family only (`GROUPING_ARM_ORDER=A CARGO_BUILD_JOBS=4 cargo bench
-p ravel-maintain --bench rlog_postings -- --noplot postings_random`):
small 3.722 ms, medium 31.21 ms, large 206.15 ms, highcard 71.66 ms
(criterion medians). The sorted A expectations scale from it.

| geometry | expected A median, sorted | miss band |
|---|---|---|
| small | 2.0 ms | outside 0.7 to 6 ms |
| medium | 16 ms | outside 5.3 to 48 ms |
| large | 105 ms | outside 35 to 315 ms |
| highcard | 50 ms | outside 17 to 150 ms |

Basis: on sorted rows consecutive inserts repeat the previous key, so A's
descent is cached and its branches predictable while every insert still
pays the `term_key` allocation and free; about half the random cost,
somewhat more on highcard, where runs are about 13 rows long.

Expected peaks on large, dhat `max_bytes`, and the stats_alloc sampled
peak for A:

- A random: 1,681,633 bytes, the stats_alloc figure above, since A's
  sampled peak and bound agree; band 0.3 to 3 MB (the #3254 band).
- A sorted: about 0.36 MB. Each of the 1,705 service terms sits in about
  2.3 blocks (one 56-byte set leaf), against 64 blocks and about 12 nodes
  per term on random. Band 0.15 to 1.0 MB, checked by both binaries.
- C against A: 9% above on random and 10 to 15% above on sorted. At the
  service field's conversion `BTreeMap::from_iter` collects the hash map
  into a `Vec` of `(Vec<u8>, BTreeSet<u32>)` pairs (48 bytes each) while
  the table is still allocated, then a stable sort allocates scratch of the
  same size after the table is freed; the `Vec` is the excess over A, less
  the difference between the table and A's term-map nodes.

Expected C against A in time: on random, 20 to 40% faster (the Pi's 23 to
37%); on sorted, 5 to 20% faster, since A's cached descent removes most of
what the hash probe saved.

Expected share on sorted large: `share_loop` about 5% (A's loop about
105 ms against a whole build of about 2 s), band 1 to 12%, checked by the
share binary; `share_delta` above `share_loop`.

Rule, as fixed in issue #3263: adopt C only if, on the sorted family, C is
at least 10% faster than A on medium and large in both runs (C's median at
most 0.9 of A's), its dhat `max_bytes` on large is at most 1.05 of A's on
both families, and the postings pass is at least 2% of the whole object
build (read as: the lower end of `share_loop`'s 95% interval at or above
2%). Otherwise `writer.rs` does not change. The random family is reported
alongside and does not enter the rule.

Predicted outcome from the figures above: the memory clause fails on both
families, so no change.

#### Results (#3263)

All runs on 2026-10-25 on the amd64 executor above, `CARGO_BUILD_JOBS=4`,
each exit 0. The host was not quiet: load average rose from 0.49 to 2.39
over run 1 and from 2.20 to 4.05 over run 2, another task's build sharing
the box. Every C interval below lies wholly below A's in the same group, so
no comparison rests on overlapping intervals.

Timing, criterion medians in ms, C against A in the last column. Run 1:
`GROUPING_ARM_ORDER=AC CARGO_BUILD_JOBS=4 cargo bench -p ravel-maintain
--bench rlog_postings -- --noplot`. Run 2: the same with
`GROUPING_ARM_ORDER=CA`. Both runs printed the distinct counts of the
fixture table above, and the random family's arms all produced checksum
0x353d05aff71b5dee on large, as before #3263, so that family is unchanged.

| family | geometry | run 1 A | run 1 C | run 1 C vs A | run 2 A | run 2 C | run 2 C vs A |
|---|---|---|---|---|---|---|---|
| sorted | small | 2.994 | 2.159 | -27.9% | 2.927 | 2.110 | -27.9% |
| sorted | medium | 22.43 | 17.18 | -23.4% | 22.62 | 16.77 | -25.9% |
| sorted | large | 113.16 | 71.40 | -36.9% | 116.43 | 75.51 | -35.1% |
| sorted | highcard | 39.14 | 21.13 | -46.0% | 38.64 | 21.22 | -45.1% |
| random | small | 3.740 | 2.352 | -37.1% | 3.754 | 2.355 | -37.3% |
| random | medium | 30.95 | 20.20 | -34.8% | 31.72 | 20.64 | -34.9% |
| random | large | 206.32 | 111.25 | -46.1% | 207.68 | 113.27 | -45.5% |
| random | highcard | 73.16 | 33.29 | -54.5% | 76.92 | 35.49 | -53.9% |

Every sorted A median is inside its miss band. The sorted A figures came
in near the calibration on medium and large (22 ms against an expected 16,
113 against 105), so sorted rows saved A less than predicted, and C's lead
on sorted input (23 to 37% on medium and large) is outside the 5 to 20%
predicted: most of C's saving survives stream-sorted input.
The random family's 35 to 55% on this host is above the Pi's 23 to 37%.

Peak, `CARGO_BUILD_JOBS=4 cargo bench -p ravel-maintain --bench
rlog_postings_dhat -- large`, one process per (family, arm), every arm 0
bytes live after it returned and checksums equal within each family
(sorted 0x3c1fd24b953191b8). It printed `band arm=A family=random
geometry=large max_bytes=1681633 band=300000..=3000000 PASS` and `band
arm=A family=sorted geometry=large max_bytes=354682 band=150000..=1000000
PASS`. The stats_alloc columns are from `CARGO_BUILD_JOBS=4 cargo bench -p
ravel-maintain --bench rlog_postings_alloc -- large random` and `-- large
sorted` (1,572,867 samples per arm; the same two A figures with `PASS`
band lines).

| family | arm | dhat `max_bytes` | vs A | stats_alloc sampled | stats_alloc bound |
|---|---|---|---|---|---|
| random | A | 1,681,633 | | 1,681,633 | 1,681,633 |
| random | C | 1,820,901 | +8.3% | 1,724,901 | 2,018,757 |
| sorted | A | 354,682 | | 354,682 | 354,682 |
| sorted | C | 408,494 | +15.2% | 326,654 | 576,574 |

Both dhat C peaks fall inside the stats_alloc bracket, and both A peaks
equal stats_alloc's figures, so the two instruments agree where they
overlap. On sorted rows C's sampled figure is below A's while its true
peak is 15.2% above: the excess is transient, inside one operation
between two samples, which by the Memory analysis above is a field's
`BTreeMap::from_iter` (inferred, not split by field here). That is the
case the earlier method could not settle. In absolute terms C's peak is 139,268 bytes above A's on random
and 53,812 bytes on sorted, per 524,288-row object.

Share, `CARGO_BUILD_JOBS=4 cargo bench -p ravel-maintain --bench
rlog_postings_share`, sorted large, 21 iterations (the indexed build
reported 3 indexed fields, 0 capped, 1,925 distinct terms, 33,466
postings bytes, as the binary checks):

| figure | median | 95% interval |
|---|---|---|
| A's postings loop | 109.99 ms | 108.79 to 113.46 |
| indexed build | 2,730.74 ms | 2,715.05 to 2,808.56 |
| plain build | 2,362.20 ms | 2,338.25 to 2,460.00 |
| `share_loop` | 4.01% | 3.96 to 4.09 |
| `share_delta` | 13.54% | 12.63 to 15.35 |

It printed `band share_loop geometry=large median_pct=4.01 band=1..=12
PASS` and `clause share_loop>=2% ci95_lower_pct=3.96 MET`. Indexing costs
about 369 ms per build over the plain one, of which the accumulation loop
is about 110; the remainder is in the work `share_delta` also charges
(merged-view resolution and POSTINGS encoding), not split here. From
these medians, C's 35 to 37% on sorted large would take about 40 ms off a
2.7 s build, about 1.5%; that is arithmetic over two benches, not a
measurement of a changed writer.

#### Decision (#3263)

No change to `writer.rs`. Of the three clauses, two pass: C is at least
10% faster than A on sorted medium (23.4%, 25.9%) and large (36.9%, 35.1%)
in both runs, and the postings loop is 4.01% of the indexed build with an
interval lower end of 3.96%. The memory clause fails on both families: C's
dhat peak is 8.3% above A's on random and 15.2% on sorted, against a 5%
limit. The rule is applied as fixed. This closes the question #3254 left
open: with a true peak in hand, C's speed comes with a transient peak
above the limit, on both row orders.

## Class closure: `BTreeMap<[u8; 16]` across crates/

`grep -rn "BTreeMap<\[u8; 16\]" crates/`, excluding the benches added here:

| site | status |
|---|---|
| `ravel-maintain/src/build.rs:219` (`by_series`) | measured as candidate 1; no change |
| `ravel-maintain/src/build.rs:203` (exemplars) | out of scope: optional section, absent in the scale fixtures (see What was measured) |
| `ravel-maintain/src/erasure_rewrite.rs:1090` (`build_rewrite`'s `by_series`) | out of scope: same type and fill/drain shape as `build.rs:219`, so candidate 1's result is expected to carry over (an inference, not a result); runs on erasure rewrites, not every compaction; not measured separately |
| `ravel-maintain/src/rspan_codec.rs:1301`, `:1844`, `:1967` | out of scope: inside the `#[cfg(test)]` module (from line 918) |
| `ravel-maintain/src/compact.rs:1361` | out of scope: inside the `#[cfg(test)]` module (from line 757) |
| `ravel-maintain/tests/common/mod.rs:278`, `:286`, `:321`, `:322`, `:793`, `:795`, `:811`, `:812` | out of scope: test helpers |
| `ravel-maintain/tests/erasure_rewrite_provenance.rs:511` to `:820`, `ravel-maintain/tests/erasure_sweep.rs:1183` | out of scope: test code |
| `ravel-query/tests/differential_compaction.rs:236`, `:238` | out of scope: test oracle |
| `ravel-sql/tests/differential.rs:700`, `ravel-sql/tests/util/gate.rs:373` | out of scope: test oracle |

## Not tested

- Candidate 3, named only: `fold_value` in ravel-catalog's
  `column_stats_build.rs`, the catalog cache's recency index, and the
  ravel-cache bulk region. None was read or measured.
- The exemplar map at `build.rs:203` and the erasure-rewrite grouping at
  `erasure_rewrite.rs:1090` (see Class closure).
- Production's allocator and hardware. Every bench runs on the glibc
  system allocator; the shipped `ravel-server` and `ravel-cli` use
  jemalloc, where the per-insert `term_key` allocation and free that every
  postings arm pays costs differently, and where the size classes behind a
  peak differ (dhat and stats_alloc count requested sizes). The #3254
  postings timings are from a Raspberry Pi 5 (aarch64, 2 MB L3); the #3263
  ones are from an amd64 fleet executor, which is not production hardware
  either.
- Whole-build share for candidate 1. Its stage was not timed against a
  whole compaction. Candidate 2's postings loop was timed against a whole
  RLOG object build on sorted large only, arm A only, in the row path
  (`RlogWriter::push` and `finish_with_stats`), not the columnar path.
- A quiet host. Both #3254 postings runs, candidate 1's run 2, and both
  #3263 timing runs shared the box with another build (load averages in
  the #3263 Results).
- The sorted-input hypothesis for candidate 1's C result (shuffle each
  input's series order and rerun).
- Candidate 2 on a production corpus. The sorted family carries each
  stream's two string fields in runs, as the writer's blocks do, but its
  stream count, term draw and uniform status codes are synthetic; no
  corpus's term distribution was replayed.
- Per-field split of C's dhat peak. The 8.3% and 15.2% excesses are whole
  arm maxima; which field's conversion sets them is inferred from the
  stats_alloc analysis, not measured.

Next experiment: none is queued for candidate 2. Under the #3263 rule C
fails on memory, not speed, and by the share figures its saving is about
1.5% of a sorted large build. Reopening it would mean a change outside
`writer.rs`: the excess is attributed (by the analysis, not a per-field
measurement) to the intermediate `Vec` that std's `BTreeMap::from_iter`
collects and sorts before its bulk build, so the lever is an encoder boundary that takes the terms as a
sorted `Vec` rather than a `BTreeMap` (`FieldTerms::Terms`), measured with
the same dhat binary against a rule fixed before the run. Candidate 3
remains unread.
