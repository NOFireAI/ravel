# ADR-3326: a byte-budgeted cache of decoded column blocks for log scans

Status: Accepted (2026-10-28; Stage 0 passed, see the Stage 0 result
amendment). Issue #3326, under epic #3231 (hot-path
program, workstream E). No persistent format change: the cache holds
in-process copies of what the reader already decodes from immutable
objects, and nothing it holds is written anywhere.

## Context

On `main` 909aac68c the hot ClickBench sweep spends most of its server CPU
turning cached object bytes back into columns. The cpu-stage profile of run
`prof-20261027T0415Z` (stamps and method in #3231 comment 6438733052; raw
profiles under
`s3://ravel-clickbench-5b7101/reports/cold-floor/prof-20261027T0415Z/`)
puts `decode_block` at 36.6% of the sweep's 703 core-seconds per pass, zstd
alone at 12.1%. Every hot try repeats that work on bytes the fetch cache
already holds, because nothing decoded survives the statement.

The program's other levers are smaller. Ranked by their ceiling on the hot
geomean (569 ms over 43 statements), with the rule that a lever is built
only if its ceiling is 0.95 or lower and it keeps answers exact (#3231
comments 6438733052 and 6438869054):

| Lever | Geomean ceiling | Basis |
|---|---|---|
| Resolve floor, exact part (overlap the provisioning GET) | about 0.99 | #3231 comment 6438869054 |
| Fused single-stage aggregate, if the repartition also goes | 0.905 | #3231 comment 6438733052 |
| Plain string pages as one buffer plus offsets, realistic half | 0.960 | #3231 comment 6438733052 |
| This ADR, at an 8 GB budget (modelled, below) | 0.715 | predicted |

### What a cache can reuse

The decode path for one block (all on `main` 2c0528a1b):

1. `LogSegmentScan::next_block_columnar`
   (`crates/ravel-query/src/log_fetcher.rs:598`) calls
   `BlockScan::next_block_columnar` and `decode_block`
   (`crates/ravel-logseg/src/reader.rs:1422`, `:1470`).
2. `decode_v4_block_with` (`reader.rs:1739`) reads the projected pages,
   skipping pages outside the projection (`:1761-1764` for dictionary
   pages, `:1796-1800` for data pages). The projection
   includes every column a pushed predicate names
   (`crates/ravel-sql/src/logs_scan/mod.rs:254-266`). Through
   `read_block_pages_with_shared_dicts` and `decode_columns`
   (`crates/ravel-logseg/src/block.rs:1642`, `:1736`), it produces one
   column per kind:
   - `I64Column { values: Arc<[i64]>, validity }` (`block.rs:1048`);
   - `StrColumn::Plain(Vec<Option<Vec<u8>>>)` or
     `StrColumn::Dict(DictColumn { Arc<DictValues>, keys: Vec<i32> })`
     (`block.rs:785-787`, `:797-800`, `:1858-1882`);
   - f64, bool and fixed-width columns as owned
     `Vec<Option<u64>>`, `Vec<Option<bool>>` and `Vec<Option<Vec<u8>>>`
     (`block.rs:1226-1230`).

   Of these, only `I64Column`'s values and a dictionary's `DictValues` sit
   behind an `Arc` today; every other form is an owned `Vec`.
3. `block_survivors` (`reader.rs:1526`, `:2171`) evaluates the pushed
   predicate over the whole block. `build_survivor_values` (`:1535`)
   narrows a Contains-tested column to the survivors
   (`DictColumn::restrict_to`, `block.rs:912`).
4. `build_columnar_batches` (`crates/ravel-sql/src/logs_scan/batch.rs:1645`)
   builds Arrow arrays for surviving rows only.

Step 2's output, for a column not deferred by a Contains arm, is a pure
function of the object's bytes, the block index and the column id:
- no row selection narrows it, since no path decodes a subset of a page's
  rows;
- late materialization's second phase decodes whole blocks and picks rows
  by position (`logs_scan/mod.rs:2808-2860`). It reaches the decoder
  through the row path (`next_block_on_gate`), not the columnar path, so
  this ADR does not serve it from the cache;
- the tenant's declared-column list enters only at step 4.

Steps 3 and 4 depend on the statement. In the profile, step 2
(`decode_v4_block_with`) is 64.0% of q21, 69.4% of q23, 24.3% of q34 and
67.3% of q04. Steps 3 and 4 (`block_survivors`, `build_columnar_batches`)
are 15.3%/1.5% of q21 and 0.2%/4.7% of q34. A cache at step 2 removes step
2's share and leaves steps 3 and 4 in place.

Objects are immutable and content-addressed
(`docs/adrs/0046-read-cache-tier.md:14`,
`docs/adrs/0010-spec-amendments-review-1.md:14-21`). The fetch cache
already keys on the object's 32-byte content hash
(`crates/ravel-cache/src/key.rs:113-117`). A decoded copy keyed the same
way never needs invalidating.

### How big the working set is

ClickBench restarts the server and clears caches before each statement's
first try. Its README, section "Caching", item 2.a: "it is _required_ to
restart the database before the first run of each query". A decoded cache
therefore holds one statement's columns between try 1 and tries 2 and 3,
never the whole sweep's.

The model (script `analysis/budget.py` under the run's results; inputs are each
statement's per-call `decompressedBytes`, `blocksScanned` and its column
list): decoded bytes per statement are 12 B per row for `ts` and
`stream_ref` on every scan, plus:
- 8 B per row per integer column;
- per string column, the column's string data plus a per-row cost that
  depends on the layout:
  - 4 B in a buffer-plus-offsets layout;
  - about 40 B in today's `Vec<Option<Vec<u8>>>`: 24 B for the `Option`
    plus about 16 B of allocator overhead per cell.

Statements that scan a block subset are scaled by `blocksScanned / 13235`.
Each column's string data is inferred from the statements that read only
that column: URL 5.29 GB, Title 3.64 GB, SearchPhrase 1.55 GB, Referer
3.83 GB. q24 (`SELECT *`, late materialization) is left out.

| Statement | Buffer + offsets | Today's layout | Basis |
|---|---|---|---|
| q04 (UserID) | 2.0 GB | 2.0 GB | predicted |
| q17 (UserID, SearchPhrase) | 4.0 GB | 7.6 GB | predicted |
| q21, q34, q35 (URL) | 6.9 GB | 10.5 GB | predicted |
| q22 (URL, SearchPhrase) | 8.8 GB | 16.0 GB | predicted |
| q23 (URL, Title, SearchPhrase, UserID) | 13.7 GB | 24.5 GB | predicted |
| q29 (Referer) | 5.4 GB | 9.0 GB | predicted |

The hot-geomean ceiling assumes that every statement whose set fits
loses its whole step 2 share and nothing else changes. q21 to q24 gain
nothing, because their string columns are Contains-tested (decision 1;
the LIKE-deferred amendment below makes those columns cacheable).

| Budget | Fit, buffer + offsets | Geomean ratio | Fit, today's layout | Geomean ratio | Basis |
|---|---|---|---|---|---|
| 2 GB | 19 of 43 | 0.860 | 19 of 43 | 0.860 | predicted |
| 4 GB | 30 of 43 | 0.771 | 21 of 43 | 0.842 | predicted |
| 8 GB | 39 of 43 | 0.715 | 32 of 43 | 0.762 | predicted |
| 12 GB | 39 of 43 | 0.715 | 39 of 43 | 0.715 | predicted |
| unbounded | 39 of 43 | 0.715 | 39 of 43 | 0.715 | predicted |

Biases:
- Dictionary-shaped string columns hold keys plus distinct values, smaller
  than the model's per-row string data, so string sizes are biased high.
- A hit is costed at zero, so the ratio is biased low.
- All-or-nothing fitting ignores a statement that fits partly, which
  biases the ratio high at small budgets.

### Memory on the reference box

On the c6a.4xlarge (32 GB) the server's memory budget is 27.8 GB. The
fetch cache takes 25% of it (6.9 GB, 24% on a loopback store), and the
catalog byte and column-statistics caches take fixed shares. That leaves
an 18.9 GB remainder shared by SQL, the fetchers and PromQL
(`services/ravel-server/src/lib.rs:757-767`, `:2800`;
`docs/internal/clickbench.md:823-840`). The largest recorded SQL figure is
q33's aggregate state at 10.09 GB
(`crates/ravel-sql/src/bounded_topk.rs:7-9`). A fixed carve of 8 GB would
leave q33 about 0.8 GB of slack. That is why decision 4 charges the cache
to the shared budget and evicts it under pressure instead.

## Decision

### 1. The cache holds step 2's output, per (object, block, column)

- **Key:** tenant hash, object content hash (32 bytes), block index,
  column id, and a layout tag.
- **Value:** the decoded column as `decode_columns` produces it for a
  column that no Contains arm defers:
  - `I64Column` (`Arc<[i64]>` plus validity), shareable as it is;
  - a dictionary column as `Arc<DictValues>` plus `Arc<[i32]>` keys and an
    `Arc`'d validity, where today's `DictColumn` holds `keys: Vec<i32>`;
  - a plain string column in decision 2's layout, behind an `Arc`;
  - f64, bool and fixed-width columns in `Arc`'d buffers, where today they
    are owned `Vec`s.
- **Hits share the entry by `Arc`**, so a hit copies nothing. Today that
  holds only for `I64Column` and `DictValues`. Making the other forms
  shareable is a representation change in `DecodedBlock`, delivered as its
  own pull request after decision 2's layout (Delivery, step 2). Until it
  lands no other kind is
  cached, because a hit that clones a column costs what the model counts
  as free. Steps 3 and 4 run on the hit exactly as on a fresh decode.

Exclusions, each a property the implementation tests:
- **Deferred Contains columns.** A column a Contains arm defers is decoded
  with its dictionary unvalidated and then narrowed to survivors
  (`reader.rs:1766-1784`, `:1554-1580`), so it is neither cached nor served
  from the cache. Caching a validated copy for later Contains statements
  is a separate decision, not taken here, and taken by the LIKE-deferred
  amendment below, which serves and admits such a column only validated.
- **Narrowed values never populate an entry.** No narrowed, survivor-only
  or Arrow value populates an entry, and nothing after step 2 reads from
  one (narrowed by the zero-copy values amendment below: an entry keeps one
  whole-block Arrow values array over its own buffers, and step 4 reads it).
- **Blocks with an erasure pending bypass the cache.** The columnar path
  decodes nothing while an erasure is pending (`log_fetcher.rs:599-601`).
  Such blocks take the row path, where erased rows are filtered after
  decode. The row path produces the same step-2 `DecodedBlock`, so it is
  not cache-free by construction: it must neither read nor fill an entry,
  and the implementation tests that (narrowed by the Stage 0 result
  amendment below).
- **Entries expire.** Every entry carries the fetch cache's maximum age
  and is dropped at its first access or sweep after it (see the Stage 0
  result amendment below).

Entries of an object no longer referenced by any snapshot age out by
eviction, as its raw bytes do in the fetch cache. Whether erasure
completion purges both caches eagerly for the compliance path (ADR-0042)
is a Stage 0 check below, not an assumption. It does not, so every entry
also carries the fetch cache's maximum age (see the Stage 0 result
amendment below).

### 2. Plain string columns use one values buffer plus offsets

A cached plain string column is one contiguous values buffer, `u32`
offsets and a validity bitmap. It costs 4 B per row plus its data, against
about 40 B per row plus data for `Vec<Option<Vec<u8>>>`. At an 8 GB budget
that difference is 39 against 32 statements fitting (Context).

The decoder produces this layout for every plain string page, cached or
not. This is the plain-page layout lever from #3231 (realistic ceiling
0.960 on its own). On its own it fails the program's 0.95 rule, so it
lands only as the first step of this lever: its own pull request with its
own A/B, merged only once Stage 0 has passed.

Only one consumer reads plain cells through the `cell()` accessor
(`crates/ravel-logseg/src/block.rs:982`). The others destructure
`StrColumn::Plain` and use the `Vec<Option<Vec<u8>>>` payload directly:
- the contains and equality kernels (`reader.rs:2292`, `:2320`, `:2439`,
  `:2810`);
- `str_dict` (`block.rs:1494`);
- `decoded_heap_bytes` (`block.rs:1374`).

The layout PR rewrites each site against offsets. `decoded_heap_bytes`
is a memory-charge figure whose value changes with the layout, so that PR
pins the new figure in a test, beside the existing kernel tests and the
proptest against row-by-row `eval` that pin answers.

### 3. Admission lets a statement's later tries hit, and a one-pass scan does not displace a working set

- **Interactive statements** (`AccessIntent::Interactive`,
  `crates/ravel-sql/src/access_intent.rs:47-103`) admit on first decode.
- **Bulk-scan statements** admit on a second touch: the first decode
  records the key in a ghost list, and a decode whose key is in the ghost
  admits the entry. The ghost window spans the whole budget in bytes. A
  ghost key costs about 64 B, and a full scan of 13,235 blocks over five
  columns is about 66,000 keys, about 4 MB.

In the profile run, 36 of the 43 statements are classified Interactive
and admit on try 1. Seven are BulkScan: q01, q03 to q07 and q30. A
BulkScan statement decodes on tries 1 and 2 and hits on try 3. That keeps
its whole gain under ClickBench's hot metric (README, "Results"): "For the
Hot Run, the smaller of the 2nd and 3rd runtime is used if both runs are
successful". A one-pass scan in production fills no entry at all. The S3-FIFO
in the fetch cache does not have that property for a working set larger
than its 10% probation queue: only the ghost list carries it into the
main queue, and #3323 sized that ghost at 30% of the budget.

Eviction is CLOCK over admitted entries, weighted by bytes. Stage 0
compares this policy with admit-on-first-decode by simulation before
anything is built (Measurement). An admission that would evict an entry
accessed at least as often as the newcomer is refused (see the
LIKE-deferred amendment below).

### 4. Entries are charged to the shared memory budget and give way to queries

- **Charging:** each entry holds a reservation on the process
  `MemoryBudget` (`crates/ravel-memory/src/lib.rs:32`) for the bytes its
  buffers hold, taken best effort as the PromQL selector cache does
  (`crates/ravel-promql/src/selector_cache.rs:537`). The cache is not a
  separate carve.
- **A hit is charged once.** Today the scan charges the whole decoded
  block to its resident reservation (`crates/ravel-sql/src/logs_scan/scan.rs:663`,
  `DecodedBlock::decoded_heap_bytes`, `crates/ravel-logseg/src/columnar.rs:435-445`).
  For a column served from the cache, the scan's charge at that call site
  leaves out the column's bytes, because the entry's reservation already
  covers them. `decoded_heap_bytes` itself is unchanged for its other
  callers (`crates/ravel-maintain/src/rlog.rs:2265`,
  `crates/ravel-logseg/src/ranged.rs:717`). A pinned test asserts the
  scan's charge for a block with one cached column and one fresh column.
  Charged once means once on the process budget: the per-query and
  per-tenant ceilings do not count served columns (see the tenant-ceiling
  amendment below).
- **The reservation travels with the bytes.** It is owned by the shared
  buffer and released when the last `Arc` drops, not when the entry leaves
  the index. Evicting an entry a live scan still holds removes it from the
  index but frees nothing until that scan drops it. A pressure-eviction
  pass therefore counts toward the request only the bytes of entries it
  holds the last reference to. Evicting a referenced entry does not
  pretend to free memory the host still holds. The shared buffer includes
  an Arrow array built over the entry's buffers, so a batch that holds one
  keeps the reservation (see the zero-copy values amendment below).
- **Eviction under pressure:** when a reservation from any other consumer
  would be refused, the budget first asks the decoded cache to evict until
  the request fits. Both refusal causes trigger the pass: the budget's
  limit, and the host memory gate that `try_reserve` also checks
  (`crates/ravel-memory/src/lib.rs:262`). A cache that grows RSS closes the
  gate before it reaches the limit, so evicting only on the limit would
  refuse queries while the cache still holds memory. It refuses only if
  evicting every unreferenced entry would not make room. This narrows
  ADR-1702's eviction-skip test. That test counts entries a live reader
  still holds and errs toward running the pass
  (`docs/adrs/1702-cpu-gate-and-isolated-liveness.md:629-633`). Here a
  referenced entry's bytes stay resident after eviction, so counting them
  would admit a reservation the host has no room for. This needs a
  registered-evictor hook on `MemoryBudget`, which does not exist today.
  The catalog's `DecodedCaches::evict_until_fits`
  (`crates/ravel-catalog/src/cache.rs:1173-1240`) is crate-private and is
  not wired in the server (`services/ravel-server/src/lib.rs:2930`).
  The hook alone does not keep a query admitted, so admission also keeps
  a headroom margin (see the admission-headroom amendment below), and the
  pass frees the shortfall plus that margin, not the shortfall alone (see
  the evictor-slack amendment below).
- **Cap:** a `--decoded-cache-max-bytes` flag sets the cap, with a default
  expressed as a share of the memory budget. Stage 0 picks the default;
  until then it is 0, which disables the cache.
- **Metrics:** occupancy, hits, misses, admissions, ghost hits,
  pressure evictions and margin skips are exported, per tenant and per
  process.

### 5. Answers do not change

- A differential test runs the SQL logs suite twice per statement: once
  with the cache forced cold, once with every eligible column forced to
  hit. It asserts identical results and identical errors.
- A property test asserts that a cached column equals a fresh decode, bit
  for bit, for every column kind and layout.
- A cached column's UTF-8 validation state is part of the entry, so a hit
  validates nothing again (narrowed to the columns it holds for by the
  validation-state amendment below; the one build of an entry's Arrow
  values array validates again, see the zero-copy values amendment below).
  `str_cells_validated` therefore counts
  validation work done by this statement. A hot try served from the cache
  reports fewer cells than the cold try that filled it. The counter
  documentation says so, and the differential test asserts results and
  errors, not counters. `contains_cells_tested` is unaffected, because
  Contains-tested columns are not cached (they are cached under the
  LIKE-deferred amendment below, which states when the count still holds).

### 6. Whether the ClickBench entry runs with the cache is an owner decision

ClickBench's README, "Caching":
- "Caching source data (e.g. buffer pools) is fine."
- "Query result caches should be disabled."
- Caches for intermediate data are "generally okay, however if such caches
  are located near the end of the query execution pipeline, the effects
  are similar to query result caching and such caches should thus be
  disabled."

The cache holds source columns ahead of every predicate and operator, and
the restart before each first try empties it, so cold runs are unaffected.
The owner withdrew the fold from the competing entry on fairness grounds
(ADR-3217, decision 2 fairness amendment). Whether the entry runs with a
nonzero default is the owner's decision. It was made on 2026-10-28: the
entry runs with the cache once it ships (see the Stage 0 result amendment
below).
Every hot figure from a run with the cache on states the cap in its stamp.

## Measurement

### Stage 0, before any code that merges

1. **Admission simulation.** Extend the cache simulation harness
   (`crates/ravel-cache/src/s3fifo/ghost_window.rs` is the pattern) with
   this policy and with admit-on-first-decode. The trace is each
   statement's (object, block, column) access sequence, three tries per
   statement, each statement under its measured access intent, entry sizes
   from the model, at caps of 4, 8 and 12 GB. Add
   one production-shaped trace: an interactive working set interleaved
   with a full scan larger than the cap.
2. **Measurement branch** (not for merge): an unvalidated prototype of
   decisions 1, 3 and 4 with the cap flag, over today's string layout.
   It wraps each cached column in an `Arc` itself, so its hits copy
   nothing, and it charges each hit once.
   Run it paired against `main` on one c6a.4xlarge, using the `cpuab`
   stage (A B B A B A A B). Cap 8 GB in one pair and 4 GB in a second.
   Report:
   - per statement: hot wall, server CPU, decoded-cache hits and misses;
   - resident and RSS peak per statement;
   - the same-binary stamp.

   The `cpuab` stage runs no concurrent phase, so the branch at 8 GB and
   `main` also take one full entry run each on the same box type, for QPS
   and the error ratio.
3. **Erasure check.** Read whether erasure completion purges the fetch
   cache's entries for the object. If it does, decision 1 purges the
   decoded cache the same way.
4. **q33 simulation and re-run.** After the first paired runs, Stage 0
   adds a simulation of q33's shape and an 8 GB re-run with the admission
   margin (see the admission-headroom amendment below). Their rows are
   the three whose figure names "with the margin" or "without the margin":
   the simulation's control arm, its margin arm, and the re-run. Both ran
   on 2026-10-27. The simulation met its rows. The re-run missed its row,
   which led to a second simulation and re-run (see the evictor-slack
   amendment below). Their rows are the last three of the table. Stage 0
   passed on 2026-10-28 (see the Stage 0 result amendment below).

| Stage 0 figure | Band | Miss | Basis |
|---|---|---|---|
| Simulation: try-3 hit share for statements whose set fits, chosen policy, 8 GB | at least 0.95 | under 0.80 | predicted; met, measured 2026-10-27: 1.000, #3326 comment 6441843449 |
| Simulation: interactive working set retained through a scan larger than the cap, chosen policy | at least 0.90 | under 0.50 | predicted; met, measured 2026-10-27: 1.000, #3326 comment 6441843449 |
| Branch at 8 GB, today's string layout: hot geomean, B/A | 0.76 to 0.88 | above 0.92 | predicted; met, measured 2026-10-28: 0.7675, #3326 comment 6461898958 |
| Branch at 8 GB: q04 (BulkScan) server CPU on try 3, B/A | 0.30 to 0.50 | above 0.70 | predicted; met, measured 2026-10-28: 0.309, #3326 comment 6461898958 |
| Branch at 8 GB: q34 (modelled set 10.5 GB, larger than the cap) hot wall, B/A | 0.95 to 1.05 | above 1.10 | predicted; outside the band on the fast side, not a miss, measured 2026-10-28: 0.776, #3326 comment 6461898958 (the Stage 0 result amendment explains it) |
| Branch at 8 GB: any statement refused for memory or above 1.10 hot wall | none | any | predicted; met, measured 2026-10-28: every statement answered, highest 1.013, #3326 comment 6461898958 |
| Branch at 8 GB, full entry run with the concurrent phase: QPS | at least 0.60 and at least main's on the same box minus 5% | under 0.60 | ADR-3217 standing bar (#3217 comment 6366582370); met on the same box type, measured 2026-10-28: 0.817 against 0.777, #3326 comment 6462646965 |
| Branch at 8 GB, full entry run with the concurrent phase: error ratio | at most 0.058 | above 0.058 | ADR-2023 decision 4, `docs/adrs/2023-loopback-fetch-cache-share-and-independent-catalog-cache.md:100`; met, measured 2026-10-28: 0.058, #3326 comment 6462646965 |
| Same-binary stamp, A{4,6}/A{1,7} non-target median | 0.98 to 1.02 | outside | `cpuab` stage, #3231 comment 6433600863; met, measured 2026-10-28: 0.9887 |
| Simulation, q33 shape, without the margin (control): aggregate refusals at an 8 GB cap | at least one | none | met, measured 2026-10-27: 116, #3326 comment 6452444728 |
| Simulation, q33 shape, with the margin: aggregate refusals at caps of 4, 8 and 12 GB | none | any | met, measured 2026-10-27: 0 at each cap, #3326 comment 6452444728 |
| Branch at 8 GB with the margin: q33 answers on every B run | yes | any B run refused | missed, measured 2026-10-27: 0 of 4, #3326 comment 6455179106; superseded by the last row |
| Simulation, q33 shape with racing checked reservations, evictor freeing the exact shortfall (control): aggregate refusals at an 8 GB cap | at least one | none | met, measured 2026-10-28: 75, #3326 comment 6459628404 |
| Simulation, same shape, evictor freeing the shortfall plus the margin: aggregate refusals at caps of 4, 8 and 12 GB | none | any | met, measured 2026-10-28: 0 at each cap, #3326 comment 6459628404 |
| Branch at 8 GB, evictor slack added to margin build: q33 answers on every B run | yes | any B run refused | met, measured 2026-10-28: 4 of 4, #3326 comment 6461898958 |

The bands are copied to #3326 before launch. A miss on any row stops the
work, and the readout names the step that missed.

### Delivery, after Stage 0 passes

1. The decision 2 layout, as its own pull request and A/B, banded against
   its 0.960 standalone ceiling.
2. The representation change that puts every decoded column form behind
   an `Arc` (decision 1), as its own pull request. Its A/B bands no
   regression, since nothing copies before the cache exists.
3. The cache with the cap defaulting to 0, the hook of decision 4 and
   its admission margin (see the admission-headroom amendment below), the
   evictor's slack, the entry age bound and the row-path exclusion (see
   the Stage 0 result amendment below), the tests of decision 5 and the
   metrics.
4. The default cap, chosen from Stage 0 and the owner's decision 6, with a
   paired A/B on the reference box, bands pre-registered on #3326.

## Consequences

- Hot repeated scans stop paying zstd and page decode for columns that
  fit. Cold runs are unchanged, because the cache starts empty after the
  restart.
- The process holds decoded data that the fetch cache already holds in
  compressed form, up to the cap. Three rules together are meant to keep
  queries ahead of the cache: the budget hook, the admission margin (see
  the admission-headroom amendment below), and the hook freeing the
  shortfall plus that margin (see the evictor-slack amendment below).
  Stage 0 measured the hook alone and the hook with the margin, and both
  left q33 refused. RSS can be higher between
  statements than today; the
  jemalloc purge thread (`services/ravel-server/src/main.rs:769-774`)
  returns freed pages.
- A pressure eviction adds latency to the reservation that triggers it,
  bounded by the bytes it frees: the shortfall plus the margin (see the
  evictor-slack amendment below).
- Contains-tested columns gain nothing from this ADR. Their share of q21
  to q24 stays (changed by the LIKE-deferred amendment below).
- The `MemoryBudget` hook is new shared machinery and needs its own
  tests for concurrent reservers and for a hook that cannot free enough.

## Alternatives considered

- **Cache Arrow arrays at step 4.** Rejected: they are survivor-only and
  built under the tenant's declared-column list, so they depend on the
  statement and the tenant config. A whole-block values array depends on
  neither, and an entry keeps one (see the zero-copy values amendment
  below).
- **Cache decompressed pages only (skip zstd, keep page decode).** The
  ceiling is zstd's 12.1% share against step 2's whole share. It is kept
  as the fallback if Stage 0 shows decoded entries cost more memory than
  modelled.
- **A fixed carve beside the fetch cache.** Rejected: at 8 GB it leaves
  q33 about 0.8 GB of slack, against an aggregate state of 10.09 GB.
- **Shrink the fetch cache to make room.** Rejected for now: hot tries
  are served from it (#3231), and a decoded miss falls back to it.

## Amendment (2026-10-27): admission keeps a headroom margin

<!-- amendment-applies: sections="4. Entries are charged to the shared memory budget and give way to queries|Stage 0, before any code that merges|Delivery, after Stage 0 passes|Consequences" pointer="admission-headroom amendment" -->

Stage 0 measured a prototype of decisions 1, 3 and 4 on paired box runs
against `main` (#3326 comments 6447399361 and 6450626784):

| Run | Hot geomean B/A, 43 statements | Without q33, 42 statements | q33 | Basis |
|---|---|---|---|---|
| `dc8-20261027T1431Z`, 8 GB cap, no pressure hook | 0.749 | 0.753 | refused on 4 of 4 runs | #3326 comment 6447399361 |
| `dc4-20261027T1431Z`, 4 GB cap, no pressure hook | 0.839 | 0.846 | refused on 4 of 4 runs | #3326 comment 6447399361 |
| `dc8h-20261027T1717Z`, 8 GB cap, with the decision 4 hook | 0.749 | 0.753 | refused on 4 of 4 runs | #3326 comment 6450626784 |

The 43-statement figure counts q33's B time as the time it took to be
refused, so it is not comparable with a sweep in which q33 answers. The
42-statement figure leaves q33 out of both arms.

With the hook in place q33 was still refused, on both the aggregate
envelope and the tenant accountant's process charge, and both paths reach
`MemoryBudget::try_reserve`. The cache stays at its cap through q33: while
the aggregate grows, q33's own column misses are admitted on 16 partitions
through `reserve_best_effort`. Bytes the hook frees for a refused
reservation are open to the next admission before the refused caller
retries. Evicting on refusal is therefore not enough while admission can
take back what eviction freed. This explanation is not yet measured: a
refused response carries no statistics.

Decision 4 gains one rule. A column of `bytes` is admitted only if
`reserved + bytes + margin <= limit` and the host memory gate is open,
where `margin = max(1 GiB, 5% of the budget's limit)`. The test and the
reservation are one step on the budget's ledger, evaluated on the
projected total before any byte is taken. A reserve-then-check would
briefly hold the bytes the hook just freed, which is the window the rule
closes. Below the margin, a miss is decoded and served as today, and
nothing is admitted.

- **The freed bytes go to the refused caller.** The refused requests
  Stage 0 recorded asked for 132 to 147 MB each (the requested bytes in
  the refusal messages, not the overshoot past the limit). That is under
  the 1 GiB floor of the margin. While the refused caller retries, the
  unreserved bytes are below the margin, so no admission can take them.
- **On a small budget the margin turns admission off.** The margin is
  measured against the process budget, which is the remainder after the
  fetch, catalog and column-statistics carves, not host memory. On the
  reference box that remainder is 18.9 GB and the margin is the 1 GiB
  floor. On a budget near or under 1 GiB, a nonzero cap admits nothing.
  The margin-skip counter shows it, and the server logs a warning at
  startup when the cap is nonzero and the budget's limit is under twice
  the margin.
- **The margin is stateless.** It needs no back-off timer or recovery
  rule. Once the query that took the memory releases it, the margin is
  met again and admission resumes.
- **The pressure hook is instrumented.** Each pass logs the bytes asked,
  the bytes freed, the cache's resident bytes and the bytes it could not
  evict because a live scan held them. A refusal that survives the hook
  can then be read from the server log, since the refused response
  carries no statistics.

Stage 0 continues with this rule before anything merges. A simulation of
the q33 shape comes first: 16 partitions interleave column admissions with
aggregate growth against one budget. It checks that the aggregate is never
refused with the margin and is refused without it. Then the 8 GB pair is
re-run against the bands registered on #3326 (comment 6448580460), with
the same rows. Both ran on 2026-10-27; their results are in the
evictor-slack amendment below.

## Amendment (2026-10-28): the evictor frees the shortfall plus the margin

<!-- amendment-applies: sections="4. Entries are charged to the shared memory budget and give way to queries|Stage 0, before any code that merges|Consequences" pointer="evictor-slack amendment" -->

The admission-headroom amendment's simulation and re-run both ran on
2026-10-27.

- **The admission-headroom amendment's q33-shape simulation (Trace C)
  met both of its rows** (#3326 comment 6452444728). Without the
  margin, q33's aggregate was refused 116 times at an 8 GB cap. With the
  margin it was refused none at 4, 8 and 12 GB.
- **The 8 GB control is the informative one.** At 4 GB the evictor never
  runs, because the budget never comes near its limit, so a 4 GB control
  could not show a refusal. At 12 GB the second simulation below shows the
  control refusing as well.
- **The re-run missed** (`dc8m-20261027T2059Z`, #3326 comment 6455179106).
  The hot geomean was 0.753 over 43 statements and 0.757 over 42, but q33
  was refused on all 4 B runs.

The server log now carries what the refused response cannot.

- **The evictor made 312 passes, and 71 refusals stood after it ran.** In
  each standing refusal the evictor freed close to the shortfall it was
  asked for: `held_bytes` was near 0, so live scans were not the cause.
  The caller was still short at its retry by 0.5 to 22 MB. Example:
  requested 146,800,640, freed 146,457,106, unreserved at the retry
  138,937,043.
- **The margin did its part.** It stopped cache admissions from taking the
  freed bytes: `marginSkips` was counted. What took them was q33's other
  aggregate streams and scans, reserving through `try_reserve` between
  one stream's eviction and its retry.
- **The admission-headroom amendment's explanation was half right.** It
  named admissions as the race. The race also includes the refused query's
  own concurrent reservations, which no admission rule can hold back.

A second simulation, Trace D (#3326 comment 6459628404), makes the racing
step a checked 2 MB reservation by each other partition.

- **Freeing the exact shortfall reproduces the miss:** 75 refusals at
  8 GB and 132 at 12 GB, where the aggregate stalls at 7.0 GB.
- **Freeing the shortfall plus the margin refuses none** at 4, 8 and
  12 GB, and the aggregate reaches 10.1 GB at each.

Decision 4 changes in one place. A pressure pass asks the cache to free
the shortfall plus the admission margin, `max(1 GiB, 5% of the budget's
limit)`, not the shortfall alone, then retries once. The request is best
effort: the retry runs on whatever the pass freed, so a reservation that
fits after freeing only the shortfall is still admitted, and decision 4's
rule stands that the refusal holds only if evicting every unreferenced
entry would not make room. Admission and eviction use one definition of
the margin.

- **Concurrent reservers find room.** A refused caller's own sibling
  streams can take up to the margin before its retry and still leave it
  room, and the cache shrinks in steps of about the margin while a query
  grows.
- **The cost is a larger eviction per pass:** the shortfall plus
  `max(1 GiB, 5% of the limit)` of cached columns per refused
  reservation. On the reference box the margin is the 1 GiB floor (its
  remainder is 18.9 GB, so 5% is 0.95 GB). At the request sizes Stage 0
  recorded (132 to 147 MB, which were also the shortfalls because the
  budget was at its limit) a pass evicts about 1.2 GB. On a budget above about
  21.5 GB the 5% term is larger. The cache refills once the query
  releases its memory and the admission margin is met again.

Stage 0 continues with this change. A prototype adds it to the measurement
branch, and the 8 GB pair is re-run against the same bands, with the last
row of the Stage 0 table, pre-registered on #3326 before launch.

## Amendment (2026-10-28): Stage 0 result, the entry decision and the age bound

<!-- amendment-applies: sections="1. The cache holds step 2's output, per (object, block, column)|Stage 0, before any code that merges|Delivery, after Stage 0 passes|6. Whether the ClickBench entry runs with the cache is an owner decision" pointer="Stage 0 result amendment" -->

**Stage 0 passed on 2026-10-28.** Each row of the Stage 0 table now
carries its measured figure. Every row is met, with two exceptions:
- the margin-only re-run row is missed and superseded by the
  evictor-slack re-run row below it;
- the q34 row lands outside its band on the fast side, not past its miss
  line.

| Stage 0 step | Result | Basis |
|---|---|---|
| Admission simulation (Traces A and B) | try-3 hit share 1.000 for every fitting statement at 8 GB; the interactive set retained 1.000 through scans larger than the cap | #3326 comment 6441843449 |
| q33-shape simulations (Traces C and D) | without the margin or with an exact-shortfall evictor: refused; with the margin and the evictor's slack: no refusal at 4, 8 or 12 GB | #3326 comments 6452444728 and 6459628404 |
| Paired 8 GB run with the hook, the margin and the slack (`dc8s-20261028T0428Z`) | hot geomean 0.7675 over 43 statements; q04 try-3 CPU 0.309; q34 0.776; every statement answered, q33 on 4 of 4 runs | #3326 comment 6461898958 |
| Erasure check | no purge on erasure anywhere in main; see the age bound below | #3326 comment 6461939175 |
| Full entry runs, one per arm (`qps-20261028T0558Z-A` and `-B`) | QPS 0.817 against main's 0.777; error ratio 0.058 against main's 0.064; cold sum 0.997 of main | #3326 comment 6462646965 |

Three of the paired run's figures need a reading beyond the table.

- **q34 is faster than predicted.** q34 was banded at no change because
  the model put its decoded set at 10.5 GB, over the 8 GB cap. On the
  box it hit 39,699 of 39,705 column accesses, so its set fit. The model
  costs each plain string row at about 40 B. The ADR named that string
  bias as running high; this is a measured instance of it.
- **Refusals happened inside q33 and were resolved.** The no-refusal row
  asks that no statement be refused. None was: every statement answered,
  q33 included. Across the run's four B rounds the server log records 38
  pressure-eviction passes, each one a refused reservation that the hook
  resolved before the refusal reached the statement. It has no
  `reservation refused after the pressure evictor ran` line.
- **QPS and the error ratio come from two boxes.** The full entry runs
  ran one arm per box, on the same box type, launched together. The rows
  are met on that same-box-type basis.
  - QPS: B's 0.817 is above A's 0.777. The row fails only if box-to-box
    noise exceeds about 10% in A's favour.
  - Error ratio: an absolute bar, independent of A.

The full entry runs are also the first measurement under the ClickBench
protocol, which restarts the server before each statement. There the hot
geomean was 0.719 of main and the hot sum 0.738. These carry the same
box-to-box noise; the paired figure is the banded one.

**Decision 1 gains an age bound.** Erasure completion purges no cache in
main. The fetch cache bounds erased residue by age: 23 hours per entry
plus a 1 hour sweep (`crates/ravel-cache/src/limits.rs:1-14`). ADR-0064's
acceptance criterion, that a later query cannot return erased bytes from
a cache, covers every tier (`docs/adrs/0064-selective-subject-erasure.md:101-104`).
The decoded cache therefore carries the same maximum age, from the same
constants, and drops an entry at its first access or sweep after that
age. Two more protections complete the argument.
- **New content hash after a rewrite.** This holds in main: a rewritten
  part has a new content hash, so snapshots taken after the rewrite never
  read the old entries.
- **The row path never touches the cache.** This is a requirement on the
  implementation, not a property of main. A block with an erasure pending
  takes the row path, and the row path produces the same step-2
  `DecodedBlock` the cache keys. Decision 1's exclusions now say so, and
  Delivery step 3 tests it.

Delivery step 3 now lists the age bound, the row-path exclusion and the
evictor's slack beside the hook and the margin.

ADR-0064's promise of tombstone-style invalidation for RAM-tier decoded
structures is not implemented in main. That is filed as #3401 and is not
decided here.

**Decision 6 is decided.** The owner decided on 2026-10-28 that the
ClickBench entry runs with the cache once the delivery steps ship. The
entry's default cap is chosen in Delivery step 4 from a paired A/B on the
reference box, with its bands registered on #3326. Every hot figure from a
run with the cache on states the cap in its stamp, as decision 6 requires.

## Amendment (2026-10-29): served columns are off the tenant ceiling

<!-- amendment-applies: sections="4. Entries are charged to the shared memory budget and give way to queries" pointer="tenant-ceiling amendment" -->

Decision 4 says a hit is charged once. Since the cache merged (#3439) that
charge is on the process budget only. A scan's resident charge goes
through the SQL memory pool, which charges the per-query ceiling, the
per-tenant ceiling and the process budget together. For a column served
from the cache, the scan leaves the column's bytes out of that charge,
because the entry's reservation already covers them on the process budget.
The per-query ceiling (`--sql-max-query-bytes`) and the per-tenant ceiling
(`--sql-tenant-max-bytes`) therefore do not count served columns.

The owner decided on 2026-10-29 to keep this by design (#3448, item 1).
The alternative was to charge the two ceilings for served columns while a
scan holds them, keeping the process budget charged once by the entry.

What this narrows: ADR-0088 describes the per-tenant ceiling as the
multi-tenant isolation bound, so that one tenant's wide scans cannot starve
another tenant's query pool. That bound no longer covers the served columns
of blocks a tenant's scans hold. The bytes involved are those of the blocks
in flight, not the cache's resident total: a scan holds the entries for the
block it is reading and lets them go when it moves on, and entries no scan
holds are evicted, as far as needed, when another consumer's reservation
would be refused (decision 4). Entries are keyed by tenant, so one tenant's statements are
never served another tenant's entries. The process budget still counts
every served byte once, so the process bound is unchanged.

The help text and flag reference rows for `--sql-max-query-bytes` and
`--sql-tenant-max-bytes`, `docs/guides/operations/configuration.md`,
`docs/guides/query.md` and `docs/query-engine.md` state this, and ADR-0088
carries an amendment pointing here.

## Amendment (2026-10-30): which cached string columns keep their validation

<!-- amendment-applies: sections="5. Answers do not change" pointer="validation-state amendment" -->

Decision 5 says a cached column's validation state is part of the entry.
For dictionary columns that was already so (`DictValues`, validated once
when the dictionary is built). For plain columns it was not: the cached
`PlainColumn` held raw bytes, and the columnar build validated every cell
it read with `std::str::from_utf8`, on a hit as on a miss (#3231).

A plain page of a `Str` attribute column that the columnar exit decodes on
a cache miss, and that the cache will admit when it is offered after the
decode, is now validated once at decode (`PlainColumn::validate_text`,
`crates/ravel-logseg/src/block.rs`). The set is the reader's `missed` list in
`BlockScan::decode_block`, `crates/ravel-logseg/src/reader.rs`, narrowed to
the columns `DecodedColumnCache::would_admit` accepts: under an interactive
intent every missed column, under any other intent only a column whose key
holds a ghost record, the second touch. `would_admit` reads the same
`Inner::intent_admits` check `offer` applies and changes no ghost record
(`would_admit_agrees_with_offer_and_changes_nothing` in
`crates/ravel-logseg/src/decoded_cache.rs`). A column a `Contains` arm tests
is never in the `missed` list (until the LIKE-deferred amendment below, whose
decision 3 puts a missed deferred column there). No other decode validates a plain page whole,
since nothing would keep that validation for a later read: the row path, the
columnar exit with no cache attached, a column not offered to the cache, and
a bulk-scan or maintenance first touch, which the cache answers with a ghost
record and does not admit, keep the raw buffer, and the build validates the
cells of the surviving rows one by one as before
(`the_row_path_validates_no_plain_page_at_decode` and
`an_uncached_selective_scan_validates_only_surviving_cells` in
`crates/ravel-sql/tests/logs_decoded_cache.rs`,
`a_plain_page_with_no_cache_is_validated_per_surviving_cell` and
`a_bulk_scan_first_miss_is_validated_per_surviving_cell` in
`crates/ravel-sql/src/logs_scan/batch.rs`; the last pins a bulk scan's first
touch at 2 decode-time validations and 4 per build, its second at 8 and 0,
and its hit at 0). A size, budget or concurrent-admission refusal in `offer`,
or a refusal by an admission gate (the LIKE-deferred amendment below
decides one; it is not on main, see that amendment's status), can still
leave a column validated at decode and not admitted; its
validation is then spent on this read alone, as it was for every missed
column before. When the whole values buffer is UTF-8
and every cell offset is a character boundary, the buffer is kept as a
`String`, so every cell is UTF-8 and a read is a slice with a boundary
check (`PlainColumn::text`). `StrCursor::text_at` in
`crates/ravel-sql/src/logs_scan/batch.rs` reads that slice through
`BytesCursor::validated_text_at` and validates nothing. The decode counts
the column's present cells in `str_cells_validated`, and a hit, which does
not decode, adds nothing: `a_cache_hit_on_a_plain_str_column_validates_nothing`
in `crates/ravel-sql/tests/logs_decoded_cache.rs` pins 48 on the cold run
and 0 on each hot run. The build of an entry's Arrow values array also
counts, once per entry, on whichever call builds it, a hit included (see
the zero-copy values amendment below), and the pull request that adds the
array re-pins that test. The `String` is the same allocation the raw buffer
was, so the entry's charge does not change
(`plain_text_is_validated_once_and_falls_back_per_cell` asserts equal
`heap_bytes` before and after validation).

The sentence holds for those columns only. These cached string columns
still validate on a hit, cell by cell as before:

- a `Str` attribute page whose buffer fails that check, either because a
  cell is not UTF-8 or because a cell offset falls inside a character. Its
  cells read exactly as before, a non-UTF-8 cell as absent so the
  resource/scope fallback applies (`validated_text_reads_as_per_cell_validation`,
  a property test against per-cell `from_utf8`);
- `body` and `severity_text`, which the columnar build reads through
  `view_str`, and which this change does not validate at decode.

A `Str` attribute column a `Contains` arm defers is neither validated at
decode nor cached (`BlockScan::decode_block` skips a deferred column before
the cache lookup; the LIKE-deferred amendment below changes this), so the cells of its surviving rows are validated in the
build, as before (`decoded_cache_serves_identical_rows_and_never_caches_contains_column`
pins that it is never cached;
`pushed_like_matches_the_unpushed_scan_and_counts_its_cells` in
`crates/ravel-sql/src/logs_provider.rs` pins the kept rows' cells as the
validated count).

## Amendment (2026-10-31): LIKE-deferred columns become cacheable, behind frequency-aware admission

<!-- amendment-applies: sections="1. The cache holds step 2's output, per (object, block, column)|3. Admission lets a statement's later tries hit, and a one-pass scan does not displace a working set|5. Answers do not change|How big the working set is|Consequences|Amendment (2026-10-30): which cached string columns keep their validation" pointer="LIKE-deferred amendment" -->
<!-- amendment-supersedes: phrase="Caching a validated copy for later Contains statements is a separate decision, not taken here" pointer="LIKE-deferred amendment" -->
<!-- amendment-supersedes: phrase="`contains_cells_tested` is unaffected, because Contains-tested columns are not cached" pointer="LIKE-deferred amendment" -->

**Status (2026-10-31): decision 1 was reverted, and decisions 2 to 4 are on
hold.** The gate landed and missed decision 6's hot bands on the paired run
(#3326 comment 6546999545). Hot wall geomean B/A was 1.0446 against a band
of 0.98 to 1.02, and q28 was 1.854. When one server process runs all 43
statements, the 8 GB cache is full. The gate refused 413,347 admissions
where CLOCK would have evicted, and it weighs access counts alone, so it
kept blocks that were cheaper to decode again than the ones it refused.
The revert put `decoded_cache.rs` back to its state before the gate. The
gate, its `frequency_refusals` counter and its tests are not in the tree.
The citations to `decoded_cache.rs` lines and gate tests in decision 1
below describe that reverted change, not the current file. A later
amendment replaces decision 1 with an admission rule that weighs decode
cost, and decisions 2 to 4 wait for it.

On the v0.28.0 profile run `prof028-20261030T1244Z` (8 GB cap), most of
the decode left in the hot sum is in the columns a `Contains` arm defers.
q21 to q24 decompress URL, and q23 Title as well, on every call: 4,980 MB
per hot call for q21 and q22, 8,417 MB for q23 and 3,459 MB for q24. Their
other columns hit on every hot call. Weighted by hot wall, that is about
3.0 s of the 38.688 s hot sum. These figures come from an investigation of
that run (per-call statistics and profile shares; scripts `callstats.py`,
`decowner.py`, `predict.py` and `clocksim.py`). The admission models below
come from `gatesim2.py` and `tracesim.py`, with their outputs
`gatesim2.out` and `tracesim.out`. The investigation is #3326 comment
6542227753, and the scripts and model outputs are #3326 comment
6542228107.

On main, `contains_columns` (`crates/ravel-logseg/src/reader.rs:2132`)
puts each such column in `BlockScan.deferred` (`reader.rs:776`).
`decode_block` skips it before the cache lookup (`reader.rs:1554`), so it
is never served and never offered. Its dictionary is built unvalidated
(`reader.rs:1887-1906`; `crates/ravel-logseg/src/block.rs:2912-2913`) and
then narrowed to the survivors (`reader.rs:1663`).

Two facts make caching it possible, and a third makes it dangerous.
- The kernel is already exact over a validated dictionary.
  `EntryTexts::text_of` answers from the dictionary's own validity bits
  when `DictValues::validated()` holds, and counts nothing
  (`crates/ravel-logseg/src/dict_values.rs:329-333`, `:185`).
- Both builds decide UTF-8 with one function, `validate_entry`
  (`dict_values.rs:435-438`).
- An unvalidated dictionary served to a later scan without the arm reads
  wrong. `DictValues::unvalidated` marks no entry valid
  (`dict_values.rs:165-180`), and the columnar build reads a key only if
  `is_valid` holds for it (`crates/ravel-sql/src/logs_scan/batch.rs:328-331`).
  Every row of such a column would read as absent and take the fallback.
  `DecodedBlock::cached_column` hands out whatever the block holds
  (`block.rs:2341`), so removing only the `reader.rs:1554` exclusion would
  cache exactly that.

This amendment changes three decisions:
- decision 1, the deferred-column exclusion;
- decision 3, CLOCK admission;
- decision 5, the `contains_cells_tested` sentence.

Two pull requests deliver it, in order. The first is decision 1 below on
its own. The second is decisions 2 to 4, stacked on the first.

1. **An admission that would evict an entry accessed at least as often as
   the newcomer is refused.** This is option (c) of the investigation, and
   it lands as its own pull request.
   - **Counts.** Each resident slot (`Slot`,
     `crates/ravel-logseg/src/decoded_cache.rs:151`) and each ghost record
     (`decoded_cache.rs:167`) carries an access count, a `u8` that saturates
     at 15. Each columnar `lookup` (`decoded_cache.rs:401`) adds one:
     - a hit adds it to the slot's count;
     - a miss adds it to the key's ghost record, if the key has one.

     An admitted entry starts at its ghost record's count, or at 1 when it
     has none.
   - **Ghost records.** A non-interactive first touch still records one
     (`decoded_cache.rs:470-473`). So, now, does an admission the gate
     refuses, under any intent; the record carries the newcomer's count.
     Because an interactive key can now hold a record, an admission releases
     the key's ghost record and its bytes under every intent, not only a
     non-interactive one as today (`decoded_cache.rs:516-519`); otherwise a
     resident key's stale record would hold bytes of the ghost window and
     push out the records a non-interactive second touch depends on. An
     entry that leaves the index by CLOCK, pressure or age eviction leaves
     no record. In the model, ghost records on eviction made q22 and q23
     slightly worse (`gatesim2.out`). They would also let a bulk-scan
     re-read admit on its first touch. The ghost window still spans the
     cap in bytes (`decoded_cache.rs:580`).
   - **The gate.** When an offer has to evict to fit under the cap,
     `evict_until` (`decoded_cache.rs:597`) compares each unreferenced
     victim the hand reaches with the newcomer.
     - A victim whose count is at least the newcomer's ends the walk and
       refuses the admission, so ties refuse.
     - Victims the walk evicted before that stay evicted, and reference
       bits it cleared stay cleared.
   - **Aging.** Every count, on slots and on ghost records, is halved when
     the lookups since the last halving reach 10 times the number of slots
     plus ghost records, with a floor of 65,536. This is TinyLFU's reset. A
     ClickBench statement makes at most three lookups per key, so no
     halving happens inside one statement (model: no halvings for q21 to
     q24, `gatesim2.out`). A stale set therefore gives way when a newcomer
     out-counts it or when halving ages it out.
   - **Order in `offer`** (`decoded_cache.rs:453`):
     1. the size check;
     2. the intent rule (`Inner::intent_admits`, `decoded_cache.rs:180`,
        unchanged);
     3. decision 4's check;
     4. the gate, during eviction;
     5. the reservation with the headroom margin, unchanged.

     The second eviction, after the reservation (`decoded_cache.rs:524`),
     applies the gate too, and a refusal there drops the reservation. A
     refusal by the gate, at either eviction, leaves the ghost record and
     the ghost-hit counters as they were before the offer, the outcome a
     refused reservation already has
     (`a_refused_reservation_keeps_the_ghost_record_and_its_hit_uncounted`,
     `decoded_cache.rs:778`). Today the ghost record is removed and
     `ghost_hits` counted at `decoded_cache.rs:516-521`, before the second
     eviction, so the gate PR must move that consumption after the second
     eviction's gate decision (or restore the record and the counts on a
     refusal there), and a test must pin a second-eviction refusal leaving
     both unchanged.
   - **Intents.** The gate acts only after the intent rule passes, and only
     when admission needs an eviction.
     - An interactive statement still admits on first decode while the
       cache has room.
     - A bulk-scan or maintenance statement still admits only on its
       second touch.
     - A statement whose set fits under the cap never meets the gate.
   - **The pressure evictor ignores counts.** `evict_for_pressure`
     (`decoded_cache.rs:342`) frees any unreferenced entry no scan holds,
     as before, so a query's reservation never waits on a count. The
     admission margin and the evictor's slack are unchanged.
   - **`would_admit`** (`decoded_cache.rs:446`) stays the intent rule
     alone. A column validated at decode for admission can still be refused
     by the gate (see the open questions below). Its agreement with `offer`
     (`would_admit_agrees_with_offer_and_changes_nothing`,
     `decoded_cache.rs:705`) now holds only while the newcomer fits without
     eviction or outranks its victim, and that test is narrowed to say so.
   - **Metrics.** A `frequency_refusals` counter is exported per statement
     and per process, beside admissions and margin skips.

   Reason: CLOCK as built admits every miss and evicts the first
   unreferenced slot, and a new slot enters unreferenced
   (`decoded_cache.rs:534`). A repeated scan larger than the cap therefore
   evicts the entries just ahead of its own front. Once decision 2 caches
   q23's deferred columns, its set is about 12.5 GB against an 8 GB cap.
   The model puts its try-2 and try-3 hit share at 0.000 under CLOCK, base
   columns included, and at 0.620 to 0.629 with this gate (decision 5's
   table). Inference: without the gate q23 would regress, so the gate
   lands first.

2. **A deferred column with a cache entry is served from it** (option (a)).
   - `decode_block` looks up a deferred column like any other projected
     column; the `reader.rs:1554` exclusion goes. A hit seeds the block,
     and the column's pages are not read.
   - **The kernel runs over the cached strings.** For a dictionary, an
     entry's verdict comes from its bytes, plus its validity bit where the
     verdict and the stream fallback differ (`reader.rs:2574-2596`). For a
     plain column, a cell's verdict comes from its bytes, plus
     `from_utf8` where they differ (`reader.rs:2560-2571`). A cached copy
     holds the same bytes as a fresh decode and the same UTF-8 decision
     (`validate_entry` on both builds), so the surviving rows are the same
     rows.
   - **`contains_cells_tested` stays the same for per-block dictionaries
     and for row groups served or decoded whole.** The verdict memo is keyed
     by the identity of the `Arc<DictValues>` (`dict_slot`,
     `reader.rs:2267-2283`), so each entry is tested once per distinct
     dictionary the scan meets.
     - A per-block dictionary is one dictionary per block on either path,
       so the count equals a cold run's. URL and Title take this path in
       ClickBench.
     - For a row group dictionary, a call that serves every block of a
       group, or decodes every block of it, still meets one dictionary per
       group. A call that mixes the two within one group meets two and can
       test an entry twice. Only the counter changes then, not the results.
     - A plain cell is tested once per present selected cell on either
       path.
   - **`str_cells_validated`.** A hit validates nothing, and narrowing a
     validated dictionary reads its bits and counts nothing
     (`dict_values.rs:329-333`). Prediction: hot q21 counts 0, against
     10,138 today.
   - **Narrowing applies to every deferred column, not only unvalidated
     ones.** Today `build_survivor_values` skips a validated dictionary
     (`reader.rs:1675-1677`). It now narrows every deferred dictionary
     column. Without that, step 4 would build each block's whole URL
     dictionary (inference from q34, where `dict_values_array` is 2.7% of
     45 CPU-s). `restrict_to` gives the block its own keys and values and
     leaves the cached copy alone (`block.rs:1465`;
     `restrict_to_on_a_shared_column_leaves_the_other_holder_unchanged`,
     `block.rs:4580`).
   - **A narrowed served column is charged to the scan.** No code does
     this today: `BlockScan.served` is pushed at `reader.rs:1567`, cleared
     at `reader.rs:1524` and read through the getter at `reader.rs:1332`,
     and `build_survivor_values` (`reader.rs:1645-1686`) never sees it. The
     serving PR must remove a narrowed served column's id from the block's
     served list when it narrows it, so `resident_charge_bytes`
     (`crates/ravel-logseg/src/columnar.rs:499`), which excludes the served
     columns, charges the narrowed copy the block now owns and the entry's
     reservation does not cover. Without it the scan is undercharged by
     that copy on every such block. A test pins the scan's charge for a
     block whose served deferred column was narrowed.

   Reason: a hit skips the page read, the crc, zstd and the decode. Those
   are 59.6% to 68.1% of q21 to q24's server CPU (inclusive
   `decode_v4_block_with`). The kernel, the narrowing and step 4 stay.

3. **On a miss, a deferred column the cache will admit is decoded
   validated and offered** (option (b)).
   - **Which columns.** A missed deferred column joins `missed` like any
     other. The validate set (the missed columns `would_admit` accepts,
     `reader.rs:1574-1585`) now includes deferred columns, and the deferred
     list passed to the decode (`reader.rs:1593`) leaves them out.
   - **How they are decoded.** Their dictionaries are built with
     `from_entries` on both paths (`reader.rs:1908`, `block.rs:2915-2916`).
     A plain page is validated whole (`PlainColumn::validate_text`,
     `block.rs:1697`), as for any other admitted column.
   - **Row group dictionaries.** `DictCache` keeps validated dictionaries
     in `entries` and unvalidated ones in `raw` (`reader.rs:1785-1789`). A
     group whose blocks differ on `would_admit` holds both, each decoded
     once.
   - **A deferred column the cache will not admit** keeps today's path: an
     unvalidated decode, then the survivor-only build. It is still offered,
     so a bulk-scan first touch records its ghost record, and decision 4's
     check refuses it before any admission.
   - **With no cache attached, nothing changes.**
   - **Cold cost.** Try 1 validates every entry, not only the survivors'.
     Inference from the investigation: about 5 GB of URL text (8.4 GB of
     URL and Title for q23), 0.5 to 1 CPU-s per call, under 0.1 s of wall
     time with 12 or more cores busy. The cold-sum band covers it.

   Reason: decision 2 needs entries, and decision 4 admits only validated
   ones. Validating only what the cache will keep spends that cost where a
   later hit pays it back.

4. **`offer` refuses an unvalidated dictionary.** A `CachedColumn::Dict`
   whose `DictValues::validated()` is false is not admitted. `offer`
   returns `None`, the scan uses the column for this block as decoded, and
   an `unvalidated_refusals` counter adds one. The check runs after the
   intent rule, so a bulk-scan first touch of a raw column still records
   its ghost record.
   - **A refusal and a counter, not a typed error or a debug assertion.**
     The column is correct for the scan that decoded it, so failing that
     scan would turn a caching defect into a query error. The counter can
     also be nonzero without a defect: a ghost record that another scan
     creates between `would_admit` and `offer` makes the intent rule pass
     on a column decoded raw. A debug assertion would fire on that race.
     Tests assert the counter is zero only on single-threaded runs.
   - **Plain columns get no validation-state check.** A raw plain column
     reads exactly for every reader, which validates each cell it reads
     (`validated_text_reads_as_per_cell_validation`, `block.rs:4070`). The
     validation-state amendment keeps raw `body` and `severity_text`
     columns, and `Str` pages that fail the check, cached on purpose.
     Refusing them would lose that caching and protect nothing.
   - **Order keeps narrowed copies out.** `offer` runs before
     `build_survivor_values` (`reader.rs:1601`, `:1645`). No flag can do
     this: `DictColumn`'s `narrow` flag also marks every row group
     dictionary (`block.rs:1353-1356`).
   - **Tests.**
     - `offer_refuses_an_unvalidated_dictionary`, in `decoded_cache.rs`,
       offers one directly. It asserts no entry and one refusal.
     - `a_column_cached_by_a_contains_scan_serves_a_later_scan_exactly`, in
       `crates/ravel-sql/tests/logs_decoded_cache.rs`, is the test that pins
       the guard. It runs a LIKE scan, then a scan of the same column
       without the arm, served from the cache. It asserts that the second
       scan's rows equal a cold scan's and that `unvalidated_refusals` is
       zero. Two mutations must each fail it. Leaving the deferred columns
       out of decision 3's validate set offers them raw, and the counter
       goes above zero. Removing the guard as well makes the rows differ.
     - `a_contains_deferred_column_is_never_cached` (`reader.rs:7076`) is
       replaced. The new test asserts that the deferred column is served
       on the second scan and equals a fresh full decode bit for bit, which
       pins that no narrowed copy was admitted.

5. **Memory is charged as before.** Deferred columns go through the same
   `lookup` and `offer`. The process-budget charge, the headroom margin,
   the evictor's slack, the 23 h age bound and the tenant-ceiling rule
   apply to their entries as to every other entry.
   - **The ClickBench restart protocol.** The decoded sets in the table
     below are derived from the run's measured residencies, with Title's
     entry inferred. q22's set is about 2.4 MB over the cap. The largest
     set measured today is 6.89 GB (q28, q34 and q35).
   - **A long-lived server.** The union of the columns that full scans
     read is about 19.5 GB today and about 23.2 GB with Title. The loop
     server already churns it through 8 GB (about 2.2 admissions per union
     entry).

   The table is model output, not measurement: `clocksim.py`, and
   `gatesim2.py` under decision 1's exact rule, with 16 partitions, three
   tries, interactive intent and entry sizes from the run, over two seeds.

   | Statement, cap | Decoded set | CLOCK as built: try-2/3 hit share | With the gate: try-2/3 hit share |
   |---|---|---|---|
   | q21, 8 GB | 6.90 GB | 1.000 | 1.000 |
   | q22, 8 GB | 8.002 GB | 0.902 to 0.920 | 0.999 to 1.000 |
   | q23, 8 GB | about 12.5 GB | 0.000 | 0.620 to 0.629 |
   | q23, 12 GB | about 12.5 GB | 0.104 to 0.149 | 0.939 to 0.951 |
   | q24, 8 GB | about 5.2 GB | 1.000 | 1.000 |

   Two production-shaped traces from `tracesim.py` (a model with an 8 GB
   cap and uniform 400 KB entries):
   - **Trace B.** A 4 GB interactive working set is read through a 12 GB
     scan, every round. With a bulk-scan scan, the set is retained 1.000
     with or without the gate. With an interactive scan, it is retained
     0.000 under CLOCK and 0.486 with the gate.
   - **Trace E.** A 6 GB set B replaces a 6 GB set A that was read k
     times. Under CLOCK, B reaches a 0.90 hit share by its third pass. With
     the gate it needs 6 passes for k = 4, 12 for k = 15, and 9 for k = 30
     (halving ages A's saturated counts). This is the gate's cost: a
     working set that shifts takes longer to displace a stale one.

6. **Measurement.** Each pull request runs under the ClickBench restart
   protocol, where the hot figure is the smaller of tries 2 and 3. The runs
   are paired `cpuab` (A B B A B A A B) on one c6a.4xlarge at an 8 GB cap.
   Each A is the main commit its B is built on: main before the gate for
   the first pull request, main with the gate for the second. A per-try
   counter is read on the try the hot figure takes, except where a row
   names the try. The counter rows hold under this protocol only, not on a
   long-lived server. Trace B and Trace E run against `DecodedColumnCache`
   itself. They are scaled in bytes, not in entries: the model's entry and
   lookup counts, with smaller entries and cap. That keeps the aging
   window, whose 65,536 floor would otherwise govern, at the model's
   cadence. The bands are copied to #3326 before launch, and a miss on any
   row stops that pull request.

   | Gate pull request (decision 1 alone) | Band | Miss | Basis |
   |---|---|---|---|
   | Hot geomean, B/A, 43 statements | 0.98 to 1.02 | outside | predicted: no hot try on main is cap-bound |
   | Every statement, hot wall B/A | 0.95 to 1.05 | above 1.05 | predicted |
   | Every statement but q33: decoded-cache misses and `frequency_refusals` on try 3 (bulk-scan statements admit on try 2 by design) | 0 | above 0 | predicted; misses measured 2026-10-30: 0 on main |
   | q33 answers on every B run | yes | any B run refused | predicted |
   | Trace B, bulk-scan scan: interactive set retained | at least 0.90 | under 0.50 | predicted; model 1.000 |
   | Trace B, interactive scan: interactive set retained | at least 0.40 | under 0.10 | predicted; model 0.486, CLOCK 0.000 |
   | Trace E, A read 15 times: first pass of B at a 0.90 hit share | at most 13 | above 20, or never | predicted; model 12 |
   | Same-binary stamp, A{4,6}/A{1,7} non-target median | 0.98 to 1.02 | outside | `cpuab` stage, #3231 comment 6433600863 |

   | Serving pull request (decisions 2 to 4), per hot try | Band | Miss | Basis |
   |---|---|---|---|
   | q21 decoded-cache hits | exactly 39,423 (3 x 13,141) | any other | predicted; 26,282 on main, measured 2026-10-30 |
   | q21 decoded-cache misses | 0 | above 0 | predicted |
   | q21 `decompressedBytes` | 40 to 70 MB | above 200 MB | predicted; 4,980 MB on main, measured 2026-10-30 |
   | q21 `containsCellsTested` | 36,727,795, unchanged | any change | measured 2026-10-30 |
   | q21 `strCellsValidated` | 0 | above 0 | predicted; 10,138 on main, measured 2026-10-30 |
   | q21 hot wall, B/A | 0.35 to 0.55 | above 0.70 | predicted 0.40 |
   | q21 server CPU, B/A | 0.32 to 0.50 | above 0.65 | predicted 0.39 |
   | q22 hits / (hits + misses) | at least 0.95 | under 0.85 | predicted; model 0.999 |
   | q22 hot wall, B/A | 0.35 to 0.60 | above 0.80 | predicted 0.41 |
   | q23 hits / (hits + misses) | 0.45 to 0.75 | under 0.30 | predicted; model 0.62 |
   | q23 hot wall, B/A | 0.55 to 0.85 | above 1.00 | predicted 0.64; `predict.py` does not cost the re-decode of the about 38% of base-column accesses the gate refuses, or the full validation of refused deferred blocks (inference: a few hundredths of a second each) |
   | q24 `decompressedBytes` | under 100 MB | above 500 MB | predicted; 3,459 MB on main, measured 2026-10-30 |
   | q24 hot wall, B/A | 0.35 to 0.60 | above 0.75 | predicted 0.41 |
   | Every other statement, hot wall B/A | 0.95 to 1.05 | above 1.10 | predicted |
   | q33 answers on every B run | yes | any B run refused | predicted |
   | Hot sum, B/A | 0.90 to 0.95 | above 0.97 | predicted 0.924 |
   | Hot geomean, B/A | 0.91 to 0.95 | above 0.97 | predicted 0.929, the program's 0.95 rule |
   | Cold sum, B/A | 0.98 to 1.03 | above 1.05 | predicted about 1.01 |
   | Query results | identical | any difference | predicted |
   | Same-binary stamp, A{4,6}/A{1,7} non-target median | 0.98 to 1.02 | outside | `cpuab` stage, #3231 comment 6433600863 |

   The hot-sum and geomean predictions take q23 at 64% of its full saving
   (cb028 `predict.py`). The full saving for q21 to q24 is 3.61 s if every
   deferred entry hits.

**Out of scope.**
- **Arrow-ready string values kept with the entry** (option (e2), about
  1.8 s). It caches a step-4 form, which the alternatives rejected, and
  needs its own amendment.
- **A larger cap.** The model fits q23 whole at 13 GB, but that is the
  owner's cap decision. It would first need this ADR's q33 Trace D and the full
  entry run's QPS and error-ratio rows at that cap.
- **A decompressed-page cache for deferred columns** (option (d)). It
  saves about half as much for about the same memory.

**Open questions, each with the answer this amendment proposes.**
- **Should `would_admit` mirror the gate?** On q23 at 8 GB the gate
  refuses about 28,000 of 78,846 accesses per hot try in the model
  (85,116 refusals over three tries). Some are deferred columns that were
  validated whole and then refused. Proposed: no, in these pull requests.
  Inference: the waste is under about 0.05 s of wall time, about 36% of
  the cold validation cost above. The serving pull request's readout
  reports q23's `strCellsValidated` per hot try, and the change is made
  only if q23 misses its band.
- **Is the shift latency acceptable** (Trace E: up to 12 passes against
  CLOCK's 3)? Proposed: yes. A stale set costs hits for a bounded number
  of passes, while CLOCK's failure mode loses the whole set for as long as
  the over-cap scan repeats. The aging window is the setting to shorten if
  production shows otherwise.
- **q22 is 2.4 MB over the cap.** Proposed: band it as fitting (0.999 in
  the model), and read a miss on its hit share as a sizing finding, not as
  a defect of the gate.

## Amendment (2026-10-31): each cached string column keeps a zero-copy Arrow values array

<!-- amendment-applies: sections="1. The cache holds step 2's output, per (object, block, column)|4. Entries are charged to the shared memory budget and give way to queries|5. Answers do not change|Alternatives considered|Amendment (2026-10-30): which cached string columns keep their validation" pointer="zero-copy values amendment" -->
<!-- amendment-supersedes: phrase="or Arrow value populates an entry, and nothing after step 2 reads from one" pointer="zero-copy values amendment" -->

This is the separate amendment that the LIKE-deferred amendment's
out-of-scope list asks for (option (e2), Arrow-ready string values kept
with the entry). The evidence is an investigation of step 4 on cache hits,
#3231 comment 6548679399. It reads `main` 250106f77, the profile run
`prof028-20261030T1244Z` (8 GB cap), and two paired `cpuab` runs:
`abe2a-20261031T1346Z` (#3515 alone) and `abe1-20261031T1346Z` (#3524).
Its scripts are `strsplit.py`, `reapportion.py`, `remaining.py` and
`predict.py`, with the outputs `remaining.out` and `predict.out`.

**What a hit still pays.** A hit seeds the cached column into the block and
copies nothing. Step 4 then rebuilds the column as Arrow on every call.
- **Plain pages.** `build_str_block`
  (`crates/ravel-sql/src/logs_scan/batch.rs:1366`) reads each surviving row,
  pushes a key and a null bit, and `StrValuesCache::block_values`
  (`batch.rs:524`) copies every cell into a fresh `StringBuilder`. The
  cached `PlainColumn` already holds the block as one validated `String`
  plus `u32` offsets (`crates/ravel-logseg/src/block.rs:1597`). The
  `SearchPhrase` statements push no string filter, so every row of every
  block survives.
- **Dictionary pages.** `dict_values_array` (`batch.rs:778`) copies the
  whole dictionary text once per dictionary per scan. URL and SearchPhrase
  dictionaries are per block, so that is about one copy per block:
  13,086 to 13,132 per fully hit call (measured 2026-10-31, abe2a B).
- **Size.** `build_str_block` is 3.28 s of the 38.69 s hot sum, weighted
  by hot wall (measured 2026-10-30, prof028): 2.13 s in the plain
  SearchPhrase statements and 1.15 s in the dictionary-page statements.
  #3515 removed 0.47 s of it (measured 2026-10-31, abe2a), against a
  profile ceiling of 0.85 s. What is left is 1.28 to 1.66 s on plain pages
  and 1.15 s on dictionary pages (inference: profile share less the
  measured cut).

This amendment makes six decisions.

1. **An entry's string column keeps one Arrow values array, zero-copy
   over the entry's own validated buffers.**
   - **Plain.** `PlainColumn` gains a memo slot,
     `OnceLock<Arc<dyn Any + Send + Sync>>`, so `ravel-logseg` takes no
     Arrow dependency. `ravel-sql` fills it with the block's `StringArray`
     the first time step 4 takes the fast path below. Arrow `Utf8` takes
     `i32` offsets and the column holds `u32`; up to `i32::MAX` the bits are
     the same.
   - **Dictionary.** `DictValues` gains the same slot, and
     `StrValuesCache::values` (`batch.rs:464`) takes the memo instead of
     calling `dict_values_array`. A `Region::Text` shares its `String` and
     offsets. A `Region::Mixed` shares its `text` and `text_offsets`, with
     a `NullBuffer` from `valid`. A `Region::Raw` dictionary gets no memo,
     and an array extended by fallback values stays per scan.
   - **Which columns.** The memo is filled only on a column a cache entry
     holds, served or just admitted, and for a dictionary only when that
     entry charges the dictionary in full (`values_charge`, `block.rs:1363`).
     A row group's dictionary is shared by several entries, each charging a
     share, so no single reservation covers an array over all of it; it
     keeps today's per-scan build. Any other column keeps today's build.
     The dictionary rows of the bands below assume per-block dictionaries.
     That is measured for the statements in the `strDictValuesBuilt` row,
     about one build per block, and assumed for q37 to q40 (inference).
   - **Why decision 1's exclusion is narrowed, not dropped.** The array
     covers the whole block, before survivors, and is a pure function of
     (object, block, column). The declared-column list decides only whether
     step 4 builds it, not what it holds. Narrowed and survivor-only values
     still never populate an entry.
   - **One `ArrayRef` per scan.** `ChargedValues::held` is
     `Arc::strong_count > 1` (`batch.rs:424`). A scan that stored the memo's
     own `ArrayRef` would look held for good, and a threshold of 2 fails
     when two scans read one block. Each scan wraps the memo in a new `Arc`
     around a `StringArray` clone that shares the buffers, so `held`,
     `owns` and the `ptr_eq` checks work unchanged.

2. **A served block that no filter narrowed skips the per-row loop.** The
   fast path runs only when all of these hold. Each is a precondition of
   today's loop, so the arrays are byte-identical:
   - `single_matching`;
   - a `Text` buffer;
   - `surviving_count == record_count`, which means identity rows, since
     `rows` is ascending;
   - every row present, by a popcount of the presence bitmap;
   - a last offset at most `i32::MAX`.

   The keys are then `0..n`, built in one pass, and the values are the
   memo. Any other block, a narrowed one or one with absent rows, takes
   today's loop unchanged. The output type stays `Dictionary(Int32, Utf8)`.

   The second pull request extends this to dictionary keys. On an
   unnarrowed block whose column is fully present and `all_valid()`, the
   keys are the cached `Arc<[i32]>` (`block.rs:1351`), shared zero-copy,
   instead of one `dict_key` call per row. `all_valid()` means no entry is
   NULL, so no valid key points at a NULL entry (#3535).

3. **No cycle.** Every owner behind an Arrow buffer holds an `Arc` to an
   inner buffer (the values, the offsets, the keys), never to the
   `PlainColumn` or the `DictValues`. The memo lives inside the column, so
   an owner holding the column would make the memo keep its own column
   alive and leak the entry's bytes. The column's buffers therefore move
   behind `Arc`s of their own.

4. **`unsafe` stays denied.** Every step uses a safe constructor:
   `arrow_buffer::ToByteSlice` for the `u32` offsets' bytes,
   `bytes::Bytes::from_owner` (bytes 1.12.1), `Buffer::from(bytes::Bytes)`
   (arrow-buffer 59.3.0), `ScalarBuffer<i32>`, and `StringArray::try_new`.
   `try_new` validates the buffer: one `from_utf8` and a boundary check
   per offset. `new_unchecked` is `unsafe` and is not used.
   `DictionaryArray::try_new` still checks every key.

5. **The reservation moves onto the shared buffers.** Today it lives in
   `DecodedEntry` (`crates/ravel-logseg/src/decoded_cache.rs:60-64`). It
   moves into a cell that the entry and every Arrow buffer owner over the
   entry's buffers hold. The cell holds the reservation and nothing else,
   so no owner reaches the column through it. `offer` fills the cell, and
   the reservation drops with the last holder: entry, block or array.
   - **Why not an exception.** Once batches hold buffers over an entry,
     two kinds of holder escape both the entry and the scan's charge. A
     scan's `held` test counts its own `ArrayRef`, not the buffers. And
     Arrow's dictionary-to-view cast, which GROUP BY keys go through
     (`crates/ravel-sql/src/group_keys.rs:240`), appends the values
     `Buffer` itself to the views it builds (arrow-cast 59.3.0,
     `cast/dictionary.rs:136`). Under an exception, `evict_for_pressure`
     counts an entry as held only while `Arc::strong_count(&slot.entry) > 1`
     (`decoded_cache.rs:361`). It would report those bytes as freed while
     the host still holds them. Decision 4 forbids that false free, because
     it would admit a reservation the host has no room for. The only bound
     an exception could state is whatever downstream operators keep, which
     no counter reports.
     Moving the reservation keeps decision 4's sentence true as written.
   - **The pressure pass reads the memo's buffers.** The cell's own count
     does not move when a batch takes the array: a clone of the array, a
     batch holding it, and the view cast each clone a `Buffer`, which
     shares the `Bytes` and not its owner. `Buffer::strong_count`
     (arrow-buffer 59.3.0, `buffer/immutable.rs:124`) does move. An entry
     counts as held while its entry `Arc` is shared, as today, or while any
     of its memo's buffers has a count above the memo's own reference. An
     evicted entry whose buffers a batch still holds frees nothing until
     that batch drops.
   - **The test moves with it.**
     `the_reservation_is_released_by_the_last_handle_not_by_eviction`
     (`decoded_cache.rs:870`) moves to the cell. It gains a case where an
     Arrow array, not a scan, is the last holder.
   - **The scan's own charge stays.** While it holds a memo array, the scan
     charges the array's layout bytes, as it charges its copy today
     (`batch.rs:424`, `:568`). ADR-1170 pool accounting and the
     tenant-ceiling amendment do not move. Those bytes are then counted
     twice on the process budget while a scan holds the array: by the
     entry and by the scan. This narrows decision 4's "a hit is charged
     once" for the values array only. It errs high, and the total is
     today's: today the scan holds a copy of the same size beside the
     entry.

6. **Building the memo counts as validation.** `try_new` is UTF-8
   validation that the statement does, and `str_cells_validated` counts
   validation work done by this statement (decision 5). A plain build adds
   the block's rows, which are all present on the fast path. A dictionary
   build adds the entries it validates. Each entry builds once, so a call
   after the build counts 0, as a hit does today. Two things change from
   the validation-state amendment's text, which now points here:
   - a hit can count, once, when it is the first call to build the
     entry's memo: a bulk scan's admitting call, or an entry admitted by a
     scan whose block a filter narrowed;
   - a cold try validates each admitted plain column twice, once in
     `PlainColumn::validate_text` and once in `try_new`.

   Not counting it would need decision 5 narrowed, and the counter would
   then hide CPU the statement spends.

**Memory.** The memo shares the entry's buffers. Its own cost is the array
struct and the `Buffer` and `Bytes` headers, about 200 to 500 B per entry
(inference), or 3 to 7 MB for a column of 13,141 blocks. `offer` adds that
constant to the column's `heap_bytes` when it takes the reservation, since
the memo is filled after `offer` fixes the charge. `heap_bytes` and
`decoded_heap_bytes` stay unchanged for their other callers
(`crates/ravel-maintain/src/rlog.rs`, `crates/ravel-logseg/src/ranged.rs`).
A copying memo is not an option: it would take URL's dictionaries, most of
its 5.295 GB entry set, a second time, and push q28, q34 and q35 from
6.89 GB to about 11 to 12 GB, over the 8 GB cap (inference).

**What it saves.** In hot-sum seconds on the prof028 basis, with CPU
geomeans over 43 statements from `predict.out` (predicted). The hot-wall
estimate assumes wall tracks CPU for statements at or above 0.3 s and
gains half as much below that (inference).
- The whole lever, both pull requests: 2.02 to 2.41 s; CPU geomean 0.922
  to 0.936; hot-wall geomean 0.932 to 0.944, which meets the 0.95 rule.
- The first pull request alone: 1.71 to 2.10 s; CPU geomean 0.934 to
  0.948 on ClickBench's hit pattern, 0.939 to 0.952 on the `cpuab` loop's;
  hot-wall geomean 0.943 to 0.955 and 0.948 to 0.959 on the same two
  bases. The first pull request is at the edge of the rule. The rule is
  met by the lever as a whole, not by the first pull request.
- The plain memo alone: 1.03 to 1.42 s, CPU geomean 0.952 to 0.966, which
  fails the rule.

**Delivery.** Two pull requests, in order, each with its own paired A/B.
1. **The memo**, for plain and dictionary values: the slot on
   `PlainColumn` and `DictValues`, the reservation cell, and the plain
   identity-key fast path. Its tests:
   - byte-identical arrays against today's loop: a proptest over plain and
     dictionary blocks, with absent rows, non-UTF-8 cells, narrowed blocks
     and multi-typed keys falling back;
   - no leak: an evicted entry's bytes are freed once the last batch
     drops, and a pressure pass counts nothing a batch still holds, in two
     cases: a batch holding a clone of the memo array, and a view array
     from the dictionary-to-view cast;
   - the entry's charge includes the memo constant;
   - `StrValuesCache` releases a memo array's charge once no batch holds
     it, also when two concurrent scans read one block;
   - `str_cells_validated` counts the build once per entry, on the call
     that builds it.
2. **Dictionary keys without the per-row loop**, stacked on the first.

**Measurement.** Paired `cpuab` (A B B A B A A B) on one c6a.4xlarge at an
8 GB cap. For the first pull request, A is current main and B is A with
it. In that loop q13 to q19, q25 to q27 and q32 hit fully, q06 never hits
SearchPhrase, and q31 and q29 hit in part (measured 2026-10-31, abe2a B).
The bands are copied to #3326 before launch. A miss on any row stops that
pull request, except the q06 row, which does not gate.

| First pull request figure, B/A unless stated | Band | Miss | Basis |
|---|---|---|---|
| Same-binary stamp, A{4,6}/A{1,7} non-target median | 0.98 to 1.02 | outside | `cpuab` stage, #3231 comment 6433600863 |
| q13 server CPU | 0.80 to 0.88 | above 0.93 | predicted 0.816 to 0.861, #3231 comment 6548679399 |
| q14 server CPU | 0.84 to 0.92 | above 0.95 | predicted 0.859 to 0.900 |
| q15 server CPU | 0.79 to 0.89 | above 0.94 | predicted 0.807 to 0.875 |
| q17, q18 server CPU | 0.89 to 0.95 | above 0.97 | predicted 0.908 to 0.938 |
| q19 server CPU | 0.93 to 0.97 | above 0.985 | predicted 0.943 to 0.959 |
| q26 server CPU | 0.65 to 0.75 | above 0.82 | predicted 0.677 to 0.716 |
| q32 server CPU | 0.79 to 0.89 | above 0.94 | predicted 0.812 to 0.869 |
| q25, q27 server CPU (each about 0.24 CPU-s) | 0.75 to 0.92 | above 0.97 | predicted 0.78 to 0.88 |
| q31 server CPU (about 45% of its SearchPhrase accesses miss in the loop) | 0.86 to 0.96 | above 0.99 | predicted 0.903 to 0.928: the saving times 0.55 hits |
| q06 server CPU, not gating (SearchPhrase never hits in the loop) | 0.95 to 1.05 | outside 0.90 to 1.10 | predicted 1.00; abe2a B misses all 13,141 blocks on every call, yet abe2a's B/A was an unexplained 0.915 on that path, measured 2026-10-31 |
| q28 server CPU | 0.77 to 0.86 | above 0.92 | predicted 0.800 (dictionary values are 19.0% of its CPU) |
| q34, q35 server CPU | 0.93 to 0.97 | above 0.99 | predicted 0.949 to 0.950 |
| q37 to q40 server CPU geomean | 0.88 to 0.96 | above 0.99 | predicted 0.869 to 0.961 each |
| q11, q12, q29 server CPU | 0.96 to 1.01 | above 1.03 | predicted 0.978 to 0.986 |
| Fully hit target set CPU geomean (q11 to q15, q17 to q19, q25 to q28, q32, q34, q35, q37 to q40) | 0.86 to 0.91 | above 0.94 | predicted 0.873 to 0.900 |
| CPU geomean, all 43 | 0.935 to 0.960 | above 0.975 | predicted 0.939 to 0.952, `cpuab` hit pattern |
| Hot wall geomean, all 43 | 0.94 to 0.965 | above 0.98 | predicted 0.948 to 0.959, `cpuab` hit pattern; the band reaches past 0.95 |
| Every other statement, hot wall | 0.95 to 1.05 | above 1.10 | predicted 1.00; a statement under 0.3 s whose server CPU B/A is within 1.03 is read as noise (q20 read 1.095 in abe1, measured 2026-10-31) |
| `strDictValuesBuilt` per fully hit timed call, q13 to q19, q26, q32, q34, q35 | at most 1% of A | above 10% of A | predicted about 0; A is 13,086 to 13,132, measured 2026-10-31 |
| `strCellsValidated` per fully hit timed call | equal to A per statement (0 on q13 to q19, q25 to q27, q32) | any other value | predicted: the memo builds once per entry, in the warm call (decision 6 above) |
| Decoded-cache hits and misses per call | within 1% of A per statement | outside 2% | predicted: the memo changes no admission |
| Decoded-cache `residentBytes` at the end of the loop | 1.000 to 1.01 | above 1.02 | predicted: a constant per entry; higher means the memo copied |
| `peakIntermediateBytes` per statement | at most 1.05 | above 1.10 | predicted: higher means a memo array stayed charged until the scan ended |
| Cold (try 1) sum, where the run records both arms | 0.98 to 1.03 | above 1.05 | predicted: one more whole validation per admitted entry |
| Answers | equal except q18 and q25 | any other difference | q18 and q25 differ between arms on main today, measured 2026-10-31 (abe2a, abe1) |
| Errors | none | any | predicted |

Passing the hot-wall row does not meet the program's 0.95 rule, since the
band reaches past it. The rule is judged on the product of the two pull
requests' hot-wall geomeans, against the lever's 0.932 to 0.944. If the
product is above 0.95, the lever has missed the rule, and the second
readout says so.

The q06 row is wide on purpose. abe2a validated the same 64,681,096 cells
in both arms and missed SearchPhrase on every call, so this lever has no
mechanism to move it. A reading outside its miss line is reported with
those counters and is not a reason to stop.

For the second pull request, A is main with the first. The rows below are
the investigation's predictions. Their miss lines, and the same-binary
stamp, other-statement, answers and errors rows, go on #3326 before it
launches.

| Second pull request figure, B/A | Band | Basis |
|---|---|---|
| q11, q12 server CPU | 0.88 to 0.92 | predicted; `dict_key` is 13.2% and 10.9% of their CPU |
| q28 server CPU | 0.95 to 0.98 | predicted |

The investigation also predicts the 43-statement CPU geomean 0.012 lower
than A (predicted, `predict.out`).

**Out of scope.**
- **A view memo for GROUP BY.** The dictionary-to-view cast costs 1.07 s
  of hot wall and is rebuilt per batch even over a shared values array
  (measured 2026-10-30, prof028). Once the first pull request makes those
  arrays stable across calls, a memo keyed by the values array could keep
  the views. Its memory is not sized.
- **Building the array per call without a memo.** A safe constructor
  validates the whole buffer on every call. It saves about 0.4 to 0.65 s
  (inference) and needs no amendment. It is the fallback if this one is
  refused.
- **`Utf8View` output.** `StringViewArray::try_new` validates every view,
  which brings per-cell validation back, and it changes the declared `Str`
  schema. Rejected.
