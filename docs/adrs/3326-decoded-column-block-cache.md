# ADR-3326: a byte-budgeted cache of decoded column blocks for log scans

Status: Proposed (2026-10-27). Issue #3326, under epic #3231 (hot-path
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
nothing, because their string columns are Contains-tested (decision 1).

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
  shareable is a representation change in `DecodedBlock`, delivered with
  decision 2's layout (Delivery, step 1). Until it lands no other kind is
  cached, because a hit that clones a column costs what the model counts
  as free. Steps 3 and 4 run on the hit exactly as on a fresh decode.

Exclusions, each a property the implementation tests:
- **Deferred Contains columns.** A column a Contains arm defers is decoded
  with its dictionary unvalidated and then narrowed to survivors
  (`reader.rs:1766-1784`, `:1554-1580`), so it is neither cached nor served
  from the cache. Caching a validated copy for later Contains statements
  is a separate decision, not taken here.
- **Narrowed values never populate an entry.** No narrowed, survivor-only
  or Arrow value populates an entry, and nothing after step 2 reads from
  one.
- **Erasure runs after the hit.** A block whose object has an erasure
  pending is not decoded today (`log_fetcher.rs:599-601`), and erased rows
  are filtered after decode. A cached column is filtered the same way, so
  an erasure applies to a hit exactly as to a fresh decode.

Entries of an object no longer referenced by any snapshot age out by
eviction, as its raw bytes do in the fetch cache. Whether erasure
completion purges both caches eagerly for the compliance path (ADR-0042)
is a Stage 0 check below, not an assumption.

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
anything is built (Measurement).

### 4. Entries are charged to the shared memory budget and give way to queries

- **Charging:** each entry holds a reservation on the process
  `MemoryBudget` (`crates/ravel-memory/src/lib.rs:32`) for the bytes its
  buffers hold, taken best effort as the PromQL selector cache does
  (`crates/ravel-promql/src/selector_cache.rs:537`). The cache is not a
  separate carve.
- **A hit is charged once.** Today the scan charges the whole decoded
  block to its resident reservation (`crates/ravel-sql/src/logs_scan/scan.rs:663`,
  `DecodedBlock::decoded_heap_bytes`, `crates/ravel-logseg/src/columnar.rs:435-445`;
  the fetch side does the same at `crates/ravel-query/src/log_fetcher.rs:1347`).
  For a column served from the cache, those bytes are excluded from that
  figure, because the entry's reservation already covers them. A pinned
  test asserts the scan's charge for a block with one cached column and
  one fresh column.
- **The reservation travels with the bytes.** It is owned by the shared
  buffer and released when the last `Arc` drops, not when the entry leaves
  the index. Evicting an entry a live scan still holds removes it from the
  index but frees nothing until that scan drops it. A pressure-eviction
  pass therefore counts toward the request only the bytes of entries it
  holds the last reference to. Evicting a referenced entry does not
  pretend to free memory the host still holds.
- **Eviction under pressure:** when a reservation from any other consumer
  would be refused, the budget first asks the decoded cache to evict until
  the request fits. Both refusal causes trigger the pass: the budget's
  limit, and the host memory gate that `try_reserve` also checks
  (`crates/ravel-memory/src/lib.rs:262`). A cache that grows RSS closes the
  gate before it reaches the limit, so evicting only on the limit would
  refuse queries while the cache still holds memory. It refuses only if
  evicting every unreferenced entry would not make room, the same rule as
  ADR-1702's eviction-skip amendment. This needs a
  registered-evictor hook on `MemoryBudget`, which does not exist today.
  The catalog's `DecodedCaches::evict_until_fits`
  (`crates/ravel-catalog/src/cache.rs:1173-1240`) is crate-private and is
  not wired in the server (`services/ravel-server/src/lib.rs:2930`).
- **Cap:** a `--decoded-cache-max-bytes` flag sets the cap, with a default
  expressed as a share of the memory budget. Stage 0 picks the default;
  until then it is 0, which disables the cache.
- **Metrics:** occupancy, hits, misses, admissions, ghost hits and
  pressure evictions are exported, per tenant and per process.

### 5. Answers do not change

- A differential test runs the SQL logs suite twice per statement: once
  with the cache forced cold, once with every eligible column forced to
  hit. It asserts identical results and identical errors.
- A property test asserts that a cached column equals a fresh decode, bit
  for bit, for every column kind and layout.
- A cached column's UTF-8 validation state is part of the entry, so a hit
  validates nothing again. `str_cells_validated` therefore counts
  validation work done by this statement. A hot try served from the cache
  reports fewer cells than the cold try that filled it. The counter
  documentation says so, and the differential test asserts results and
  errors, not counters. `contains_cells_tested` is unaffected, because
  Contains-tested columns are not cached.

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
nonzero default is the owner's decision, recorded here once it is made.
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

| Stage 0 figure | Band | Miss | Basis |
|---|---|---|---|
| Simulation: try-3 hit share for statements whose set fits, chosen policy, 8 GB | at least 0.95 | under 0.80 | predicted |
| Simulation: interactive working set retained through a scan larger than the cap, chosen policy | at least 0.90 | under 0.50 | predicted |
| Branch at 8 GB, today's string layout: hot geomean, B/A | 0.76 to 0.88 | above 0.92 | predicted |
| Branch at 8 GB: q04 (BulkScan) server CPU on try 3, B/A | 0.30 to 0.50 | above 0.70 | predicted |
| Branch at 8 GB: q34 (modelled set 10.5 GB, larger than the cap) hot wall, B/A | 0.95 to 1.05 | above 1.10 | predicted |
| Branch at 8 GB: any statement refused for memory or above 1.10 hot wall | none | any | predicted |
| Branch at 8 GB, full entry run with the concurrent phase: QPS | at least 0.60 and at least main's on the same box minus 5% | under 0.60 | ADR-3217 standing bar (#3217 comment 6366582370) |
| Branch at 8 GB, full entry run with the concurrent phase: error ratio | at most 0.058 | above 0.058 | ADR-2023 decision 4, `docs/adrs/2023-loopback-fetch-cache-share-and-independent-catalog-cache.md:100` |
| Same-binary stamp, A{4,6}/A{1,7} non-target median | 0.98 to 1.02 | outside | `cpuab` stage, #3231 comment 6433600863 |

The bands are copied to #3326 before launch. A miss on any row stops the
work, and the readout names the step that missed.

### Delivery, after Stage 0 passes

1. The decision 2 layout, together with the representation change that
   puts every decoded column form behind an `Arc` (decision 1), as its own
   pull request and A/B.
2. The cache with the cap defaulting to 0, the hook of decision 4, the
   tests of decision 5 and the metrics.
3. The default cap, chosen from Stage 0 and the owner's decision 6, with a
   paired A/B on the reference box, bands pre-registered on #3326.

## Consequences

- Hot repeated scans stop paying zstd and page decode for columns that
  fit. Cold runs are unchanged, because the cache starts empty after the
  restart.
- The process holds decoded data that the fetch cache already holds in
  compressed form, up to the cap. The budget hook keeps queries ahead of
  the cache, but RSS can be higher between statements than today; the
  jemalloc purge thread (`services/ravel-server/src/main.rs:769-774`)
  returns freed pages.
- A pressure eviction adds latency to the reservation that triggers it,
  bounded by the bytes it frees.
- Contains-tested columns gain nothing from this ADR. Their share of q21
  to q24 stays.
- The `MemoryBudget` hook is new shared machinery and needs its own
  tests for concurrent reservers and for a hook that cannot free enough.

## Alternatives considered

- **Cache Arrow arrays at step 4.** Rejected: they are survivor-only and
  built under the tenant's declared-column list, so they depend on the
  statement and the tenant config.
- **Cache decompressed pages only (skip zstd, keep page decode).** The
  ceiling is zstd's 12.1% share against step 2's whole share. It is kept
  as the fallback if Stage 0 shows decoded entries cost more memory than
  modelled.
- **A fixed carve beside the fetch cache.** Rejected: at 8 GB it leaves
  q33 about 0.8 GB of slack, against an aggregate state of 10.09 GB.
- **Shrink the fetch cache to make room.** Rejected for now: hot tries
  are served from it (#3231), and a decoded miss falls back to it.
