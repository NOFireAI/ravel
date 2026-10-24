# ADR-3217: the ClickBench cold floor: remove the per-object overhead the stock entry pays, then divide the object count by four

Status: Proposed (2026-10-24). Issue #3217 (epic). Source: the design
approved by the owner on 2026-10-23 on #3217, measured against run 1
(`coldfloor-20261023T135710Z`, v0.25.1). No persistent format change:
every RLOG object keeps its grammar, its trailer version and its footer.
Decision 4 moves two sections inside the writer's emission order, which
`docs/log-segment-format.md` names a writer convention, and lands as an
ADR-0699 amendment with no trailer bump (the basis is quoted in decision
4). Every other decision is an entry-script change, a loader flag or a
tenant setting the v5 format already carries.

## Context

The stock Ravel entry on ClickBench's c6a.4xlarge (16 vCPU, 32 GB, a
500 GB gp2 root volume at 250 MiB/s and 1,500 IOPS baseline, RustFS on
loopback) measures a cold sum of 367.2 s on v0.25.1 (run 1, #3217 comment
6368995087) and 367.0 s on the v0.24.0 release box (#2044 comment
6221541969), against a published 498 s on v0.23.0. "Stock" means the
entry's own `load` and `start` with no server flag and no cache tier; the
entry's `load` script is ours and may set loader flags and tenant
settings. The corpus is 407 L0 objects of about 25 MB, one row group on
the median object and two on the largest, 104 declared columns. 34 of 43
statements scan every object.

Run 1 settled what the cold sum is made of (readout #3217 comment
6368995087; interim comment 6367709053; pre-registration comment
6366582370; artifacts under
`s3://ravel-clickbench-5b7101/reports/cold-floor/coldfloor-20261023T135710Z/`):

- **The device is at its throughput cap.** On every full-scan statement
  iostat shows the root volume at a mean 220,000 to 243,000 rkB/s against
  a 256,000 cap, 1,100 to 1,500 r/s (never near the 3,000 burst ceiling),
  72 to 85 percent utilisation. Over the 32 statements longer than 3 s:
  wall 361.4 s, 100.4 GB read from disk for 49.5 GB of wire, and
  100.4 GB / 262 MB/s = 383 s. The cold sum is the disk bandwidth floor at
  a 2x store over-read. The mean GET latency of 68 to 243 ms cold is
  queueing at 32 in flight on a saturated volume (under 1 ms hot), so the
  request-bound and latency-bound readings are excluded.
- **The store over-reads every ranged GET.** `O = (disk - wire) / GETs`
  reads 0.39 MB on q2, 0.49 on q23, 0.58 on q34, 0.62 on q33; the
  device check's tail-corrected figure is about 0.8 MB. Every GET removed
  from an object is therefore worth 0.5 to 0.8 MB of disk on top of the
  wire it saves. 0.5 is the central value in every estimate below and 0.8
  the upper bound.
- **A narrow statement spends its bytes on fixed per-object metadata.**
  q2 and q8 open every object with 5.50 GETs and 1.58 MB (2,239 GETs,
  645 MB per statement); q34 6.57 GETs and 5.15 MB; q33 7.93 and 5.84;
  q12 7.49 and 2.89. The per-object GETs are a 256 KiB suffix probe, a
  SKIP_IDX+PAGE_DIR chase, a STREAM_DIR+FIELD_DIR front GET, a BLOOM GET of
  1,191,906 B that no ClickBench statement reads, and 1 to 4 chunk runs.
  The median object's footprint puts the fixed bytes at BLOOM 1.19 MB
  plus the 256 KiB probe plus about 30 KB of directories (PAGE_DIR 22,312
  B, SKIP_IDX 5,970, FIELD_DIR 1,043, STREAM_DIR 201): 1.48 MB per object
  per statement, about 170 s of the 367 s at the measured over-read. q2's
  projected column is 3.8 KB of the 1.58 MB it reads per object. Stored
  column bytes on the median object: AdvEngineID 3.8 KB, UserID 0.80 MB,
  SearchPhrase 0.65 MB, URL 2.44 MB; summed over the 30 footprinted
  statements the projected bytes are about 19 GB.
- **The other classes are settled.** The four LIKE statements (q21 to
  q24) run the fast path with 0 or 1 plan GET, so there is no reader fix
  for an undecidable LIKE branch in this round. q37 to q43 keep 30
  survivors of 407 (`segmentsPrunedByStats` 377) and cost about 3 s in
  total. Hot tries are served from the fetch cache (resident maximum
  4.41 GB, no evictions) and the hot wall is decode: hot 59.3 s, QPS 0.663,
  error ratio 0.020, each inside its band.

Already on `main`, not a decision here: #3219 (pull request #3221, merge
commit 46df74786) stops the reader fetching BLOOM and POSTINGS when no arm
needs them. On this corpus that removes the 1.19 MB BLOOM GET from every
object a scanning statement opens, predicted at about 90 s (407 objects x
(1.19 MB + 0.5 MB over-read) / 262 MB/s on 34 statements), landing near
277 s. Every A commit in the run plan below carries it.

The cost model every estimate uses, per scanning statement over `N`
objects on the fast path:

```
disk bytes = N x (wire overhead + G x O) + C
cold wall  = T0 + disk bytes / 262 MB/s
```

`G` is GETs per object, `O` the over-read per ranged GET, `C` the
projected column pages (moved only by decisions 3 and 7), `T0` about
0.2 s. Run 1 puts `disk bytes / 262 MB/s` about 1 s above each full-scan
statement's wall, so the formula is a floor to within 5%. The `O = 0`
column in the tables below is the no-amplification reference, kept so the
request term stays visible.

Per-object request map for the narrow predicate-free class (29 of the 34
scanning statements), `R` chunk runs per object (1 for one column on a
one-group object, up to `min(k, 4)` for `k` columns; a two-row-group
object adds a run; `chunk_run_cap` at
`crates/ravel-query/src/log_fetcher.rs:8762-8767`,
`MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT` at `crates/ravel-query/src/fetcher.rs:294`),
`W` projected page bytes, `g` bridged gap bytes, `B` the 1.19 MB BLOOM:

| stage | GETs per object | wire per object | objects |
|---|---|---|---|
| v0.25.1 stock (run 1) | probe, chase, front, BLOOM, R runs = 4 + R | 0.30 MB + B + W + g | 407 |
| after #3219 | probe, chase, front, R runs = 3 + R | 0.30 + W + g | 407 |
| decision 1 (bloom scope `text`) | probe, front, R runs = 2 + R | 0.27 + W + g | 407 |
| decision 4 (directories at the tail) | probe, R runs = 1 + R | 0.26 + W + g | 407 |
| decision 3 (zstd 9) | 1 + R | 0.26 + 0.93 W + g | 407 |
| decisions 5 and 6 (100 MB one-group objects) | 1 + R | 0.26 + 4 x 0.93 W + 4 g | 103 |
| decision 7 (clustering key) | 1 + R | 0.26 + 4 x 0.80 W + 4 g | 103 |

The goal is a cold sum under 200 s on the stock entry, under 150 s as the
stretch, open-ended rather than bound to v0.26 or the next ClickBench pin
(owner decision, 2026-10-23). The hot sum stays inside 54 to 64 s, the
concurrent phase at or above 0.60 QPS and at or under an error ratio of
0.058 (ADR-2023 decision 4), and no change may alter an answer.

## Decision

Seven decisions, in delivery order. Each names the design change it
carries, its class, what it does not change, and its own pre-registered
A/B band. Every A/B compares all 43 statements against its A box under the
answer comparison rule in Measurement, and a difference stops the change.
The GET-removing decisions (1, 4, 5 with 6) go first because on this box a
GET removed per object is worth its over-read on disk whatever the store's
mechanism; the byte trims (3, 7) follow.

### 1. The entry's tenant sets bloom scope `text` (design change 1)

Class: entry load script; a tenant setting the v5 format already carries
(ADR-2135 decision 5,
`docs/adrs/2135-rlog-v5-storage-footprint.md:245-257`). No Ravel code.

Mechanism. Under scope `text` every dynamic Str column is excluded from
BLOOM coverage (`bloom_coverage`,
`crates/ravel-logseg/src/writer.rs:3141-3163`; `BloomScope::Text => false`
at `:3152`). The hits corpus has no body or severity text, so each block's
filter is the 512-bit minimum (`finish_exact` at `writer.rs:895`,
`crates/ravel-codec/src/bloom.rs:218-220`) and the section is about 2 KB
on a 30-block object (`encode_rlog_bloom_section`,
`crates/ravel-logseg/src/rlog_bloom.rs:21-36`). The tail from SKIP_IDX to
the trailer is then SKIP_IDX (about 15 KB) + PAGE_DIR (about 14 KB) +
BLOOM (2 KB) + footer (200 B), inside the 256 KiB probe
(`DEFAULT_LOG_SUFFIX_LEN` at `crates/ravel-query/src/log_fetcher.rs:4080`,
`derive_suffix_len` at `:4127-4129`). `place_sections_coalesced` filters
on `asm.covers` before issuing any GET (`log_fetcher.rs:7869-7871`), so
the SKIP_IDX+PAGE_DIR chase (`:7389-7399`) no longer fires;
`probe_window_covers` is offset arithmetic only (`:8947-8950`). The ingest
flush reads the tenant's scope and builds every writer with it
(`crates/ravel-ingest/src/log_shard.rs:869-882`, `:906-910`), the loader
runs that flush, and `services/ravel-cli/tests/load_clustered.rs:1-14`
pins that `bloom-scope set` reaches the objects a load writes.

The entry's `load` runs, after `typed-attr-column set` and before `load`:

    "$RAVEL_CLI" --store s3 --s3-auth static bloom-scope set \
        --tenant "$RAVEL_TENANT" --scope text --readers-rolled-out

(`services/ravel-cli/src/main.rs:1330-1343`; `--readers-rolled-out` is
required, `:1357-1366`, and holds because the bucket is this entry's own
binary; `BloomScopeArg::Text` at
`services/ravel-cli/src/storage_layout.rs:69-77`.)

What it does not change: no ClickBench statement builds a bloom arm (a
substring LIKE is neither a prune nor a bloom probe,
`crates/ravel-sql/src/late_materialization.rs:18-19`; `has_word` and
`attrs[]` appear in no statement), so no answer and no hot figure moves.
Wide statements are unchanged. An uncovered column is scanned, never
pruned (ADR-2135 decision 5), so the scope cannot drop a row.

Effect: minus one GET and about 30 KB per object on the narrow, point
lookup and LIKE classes; minus one GET per opened object on the pruned
class. Cold sum (model): minus 28 s at `O = 0.5` (277 to 249 s), 44 s at
`O = 0.8`, 2 s at `O = 0`.

Pre-registered A/B (run 2, shared with decision 2 because their stamps are
disjoint and decision 2's cold effect is inside noise). Stamps:
`bloom-scope show` prints `text`; one object's `rlog inspect` shows a
BLOOM section under 8 KB and one row group; the binary digest equals the A
box's. Each figure present exactly once:

| figure | band | miss | source |
|---|---|---|---|
| scan GETs per object on q2 and q34 (`P.scan.s3GetRequests / 407`) | run 1's 5.50 on q2 and 6.57 on q34 minus 1 (the chase) on a binary carrying #3219, minus 2 without it, to within 0.05; the 0.50 above the one-run count on q2 is the second run on the two-row-group objects and stays | still at run 1's value: the scope did not reach the objects (check the BLOOM covered list on one object) | run 1 values comment 6368995087; pre-registered as run 2, comment 6372406698 |
| scan wire bytes per object on q2 | run 1's 1.58 MB minus 30 KB = 1.55 MB with #3219 (0.36 MB without it) | outside by more than 0.1 MB | run 1 value comment 6368995087; run 2 pre-registration comment 6372406698 |
| cold sum | run 1 minus 2 to 45 s with #3219 (minus 55 to 155 s without it) | a drop under 40 s on a binary without #3219, with the GET stamp passing, means `O` is under 0.2 MB and the model is wrong: re-derive `O` from this run's disk/wire before any later step | run 2 pre-registration comment 6372406698; the model figures are predicted |
| answers | all 43 equal the A box under the Measurement comparison rule | any difference: stop, this change must not change an answer | run 2 pre-registration comment 6372406698 |
| hot sum, QPS, error ratio | 54 to 64 s; at least 0.60; at most 0.058 | outside | run 1 hot 59.3 s, QPS 0.663, error ratio 0.020, comment 6368995087; the 0.058 bar is ADR-2023 decision 4, `docs/adrs/2023-loopback-fetch-cache-share-and-independent-catalog-cache.md:100` |

Proof. A test beside
`probe_covers_the_plan_sections_on_a_wide_row_group_object`
(`crates/ravel-query/tests/log_page_dir_fetch.rs:1738`): a 30-block,
104-column object written at scope `text`, opened on the narrow fast path,
reports `probe_misses == 0` and `metadata_gets == 1` (the front GET only);
the same object at scope `all` with a 1 MB BLOOM reports `probe_misses ==
2` and `metadata_gets == 2`. The mutation "writer ignores the scope" fails
the first; the mutation "`probe_window_covers` returns true" fails the
second.

### 2. The entry's load folds the catalog before it exits (design change 2)

Class: loader flag, shipped in 0.24.0 (`--fold-after-load`,
`services/ravel-cli/src/main.rs:866-867`,
`services/ravel-cli/src/load/cli.rs:248`; ADR-2677 decision 1).

Mechanism. The fold seals through the load's last ingest hour, so each
resolve's listing suffix above the watermark is empty
(`docs/adrs/2677-clickbench-floor-fetch-and-pruning.md:139-143`).
Unfolded, every resolve lists each shard's suffix from the window's start
hour to now, which is above the prefix-list crossover
(`crates/ravel-catalog/src/catalog.rs:2582-2594`), so the prefix scan
runs rather than the bounded per-shard `list_after` (`:3112-3128`): 4 LISTs
on `--shards 4` plus the erasure LIST, the 5 run 1 read on every
statement. After the fold the round survives: once the clock crosses the
hour after the load, the resolve issues one bounded `list_after` per shard
on an empty prefix, `shards + 1` LISTs of nothing. What the fold removes
is the listing payload (about 100 keys per shard) and the commit-record
cache lookups, not the LIST round; the commit-record GETs are already
hidden by the ADR-0046 warm-up (`services/ravel-server/src/cache_warm.rs:67`).

What it does not change: no object, no answer, no hot decode. Load time
grows by the fold (one HEAD and a handful of parts).

Effect: unknown, under 2 s on the sum; the stamp decides. It is not
counted in the cold-sum table. If the resolve wall does not move, the
change stays as hygiene (two lines, independently revertible) and claims
nothing.

Pre-registered A/B (shares run 2). Stamp: the load log prints the fold
report with the seal-through hour equal to the load's last ingest hour.

| figure | band | miss | source |
|---|---|---|---|
| resolve LISTs per statement (`P.resolve` list requests) | exactly 1 inside the load's last hour, exactly 5 after it | other | run 1 read 5, comment 6368995087; run 2 pre-registration comment 6372406698 |
| listing keys returned per LIST, and resolve wall | 0 keys per shard folded (about 100 unfolded); resolve wall at or under run 1's 29 to 33 ms | keys per LIST at run 1's count: the hour was not sealed | run 1 values comment 6368995087; run 2 pre-registration comment 6372406698 |
| load time | under run 1's plus 60 s | above | run 2 pre-registration comment 6372406698 |

Proof: ADR-2677 decision 1's own tests;
`services/ravel-cli/tests/load_fold_after_load_sql_e2e.rs` drives
`fold_with_seal_through` (`:142-143`, `:303`). The builder adds the
assertion that the fold report's seal-through hour equals the load's
highest `ingest_hour_bucket` if that file does not pin it (mutation: a
fold at the current watermark only, which seals nothing).

### 3. The entry's load compresses at zstd level 9 (design change 3)

Class: loader flag (`--zstd-level`,
`services/ravel-cli/src/main.rs:785-802`, default 3).

Mechanism. The level is writer configuration and readers are indifferent
to it (ADR-2135 decision 4,
`docs/adrs/2135-rlog-v5-storage-footprint.md:236-244`); the loader threads
it as `rlog_zstd_level` (`services/ravel-cli/src/load/logs.rs:476-481`,
`crates/ravel-ingest/src/log_shard.rs:888-891`). ADR-2135's simulator
puts level 9 at minus 7.1% of page bytes and level 19 at minus 11.9%
(`2135-rlog-v5-storage-footprint.md:66-67`). Level 19 is out: the one
measured load at 19 took 5,498 s against the entry's 931 s (a figure from
the epic-2135 record, not in the repository).

What it does not change: no request, no answer; decode is
level-independent, so the hot sum should not move. Load time at level 9
is unknown; the 90 s ack deadline (`write_ack_deadline`,
`services/ravel-cli/src/load/mod.rs:234-240`, `max(60 s, delay + 60 s)`)
bounds a flush's encode.

Effect: bytes only, 0.93 W on every scanning class. Cold sum (model): 7 s
at the model's 25 GB of projected bytes, about 5 s at run 1's 19 GB
(249 to 242 s at `O = 0.5`).

Pre-registered A/B (run 3, on top of run 2's script; its own run because
its effect is bytes and would confound the per-object `O` reading run 2
produces). Stamps: the load log shows `--zstd-level 9`; one object's pages
decode.

| figure | band | miss | source |
|---|---|---|---|
| `data_size` | 0.925 to 0.95 x run 2's measured value (run 2's script already carries decision 1, which takes about 0.5 GB of BLOOM out of run 1's 10.21 GB, so the band is relative to run 2) | above 0.97 x run 2's: the level did not reach the pages | 7.1% from `docs/adrs/2135-rlog-v5-storage-footprint.md:67`; run 1 `data_size` 10.21 GB comment 6368995087; band predicted |
| load time | 840 to 1,100 s | above 1,100 s abandons the change: a cold gain under 5 s is not worth 170 s of load | predicted (the entry's load today is about 931 s) |
| cold sum | run 2 minus 4 to 12 s | outside | predicted |
| hot sum | within 5% of run 2 | outside | predicted |
| answers | equal under the Measurement comparison rule | any difference | predicted |

Proof: `services/ravel-cli/tests/load_zstd_level.rs` pins the flag
reaching every page; the on-box stamps.

### 4. STREAM_DIR and FIELD_DIR are written at the tail, after PAGE_DIR (design change 4)

Class: writer convention in `ravel-logseg`, all three writers at once
(loader, ingest flush and compactor share the two emission sites). Lands
as an ADR-0699 amendment and a `docs/log-segment-format.md` edit with no
trailer bump (owner decision, 2026-10-23).

Mechanism. The writer emits sections as a fixed sequence of `push_section`
calls in two places, the columnar path
(`crates/ravel-logseg/src/writer.rs:1009-1067`: STREAM_DIR, FIELD_DIR,
BLOCKS, KEY_IDX, SKIP_IDX, PAGE_DIR, BLOOM, POSTINGS) and the row path
(`:2032` onward, the same "Assemble sections in kind order" comment);
`push_section` appends at the current length (`:3907-3912`). PAGE_DIR
chunk offsets are into BLOCKS, not the object
(`docs/log-segment-format.md:625`), validated against the BLOCKS length
only (`crates/ravel-logseg/src/page_dir.rs:387-404`), so moving a section
that precedes BLOCKS changes no page offset. The footer validator checks
presence, uniqueness and bounds, never order
(`crates/ravel-logseg/src/footer.rs:655-692`). On the read side
`place_front_sections` skips each of the two sections when `asm.covers`
it (`crates/ravel-query/src/log_fetcher.rs:7984-8003`), so once they sit
inside the probe window the front GET costs nothing, with no reader code
change. No production code reads a leading range of an RLOG object as a
header.

New order: BLOCKS, KEY_IDX, POSTINGS, BLOOM, SKIP_IDX, PAGE_DIR,
STREAM_DIR, FIELD_DIR, footer, trailer. The two large optional indexes go
before the small directories, so on any tenant the 256 KiB probe holds the
footer, PAGE_DIR, SKIP_IDX, STREAM_DIR and FIELD_DIR, and BLOOM or
POSTINGS cost the one GET they cost today when large. On this entry with
decision 1 the whole tail is under 50 KB.

Why an amendment and not a trailer bump. The format doc states its own
bump rule (`docs/log-segment-format.md:1288-1295`): "Only a change to an
*existing* section's grammar shape, or to a mandatory/optional kind's
legality, needs a trailer version bump and an ADR." A section order change
is neither, and the reader rule (`docs/log-segment-format.md:125-129`,
"readers rely only on the footer's section offsets, never on adjacency or
on kind order") makes the order a convention. Against that: the same
paragraph states the order and its reason, ADR-0699 section 5.2 gives the
probe's rationale (`docs/adrs/0699-rlog-row-groups-and-page-directory.md:376-392`,
the front pair "sit at the object's front, where no suffix probe of any
length reaches them"), and
`crates/ravel-logseg/tests/golden_bytes_v5.rs:1-9` says to regenerate the
golden bytes only after "a deliberate, versioned format change", the one
text whose word "versioned" reads the other way. A bump buys no
performance (the bytes and GETs per object are identical either way) and
costs a two-version reader window: the reader accepts version 5 only
(`crates/ravel-logseg/src/footer.rs:44`, `:106`), so a bump would be a
separate epic after v0.26 while the no-bump path is one writer pull
request plus docs and the test re-pins below.

The ADR-0699 amendment it carries, written when the change lands and not
here: a section headed "Amendment (ADR-3217): STREAM_DIR and FIELD_DIR move
to the tail" at the end of ADR-0699, with an `amendment-applies` marker
naming the section that holds 5.2 (the probe rationale and
`probe_misses`' exclusion of the front pair) and an
`amendment-supersedes` marker for the wording "where no suffix probe of
any length reaches them". It cites `docs/log-segment-format.md:1288-1295`
as the no-bump basis and names section 5.2's old order as history. The
same commit edits the format doc's figure and sentence
(`docs/log-segment-format.md:101-129`), regenerates `golden_rlog_v5.bin`
with this ADR cited in the commit, amends the `golden_bytes_v5.rs:7-9`
header so no text is left reading the other way, and updates the reader
comments that say a front section is never inside the probe window
(`log_fetcher.rs:4252-4256`, `:6005-6009`; no code change).

Test re-pins, written against `main` with #3221 in it (where #3221 has
already moved a count, the old count is the one that tree reads): the
ADR-2066 request-shape pin at
`crates/ravel-sql/tests/logs_fast_path_projection_routing.rs:566-574`
(four range GETs per segment become three); the ADR-2066 headline test
`version_4_narrow_projection_of_one_row_group_object_costs_three_gets`
(`crates/ravel-query/tests/log_page_dir_fetch.rs:469`), which pins
`metadata_gets == 1` and three GETs and under the new order reads
`metadata_gets == 0` and two; the callers of `narrow_projection_placed_bytes`
(`log_page_dir_fetch.rs:2485`) and
`per_phase_wire_bytes_sum_to_the_pooled_object_store_bytes` (`:2369`),
which sum front bytes as a separate GET; the `log_fetcher.rs` test
`carried_directories_fetch_no_front_section` (`:15434`); and
`probe_covers_the_plan_sections_on_a_wide_row_group_object` (`:1738`),
which gains the assertion that STREAM_DIR and FIELD_DIR are inside the
window.

What it does not change: no grammar, no offsets, no answer. Old and new
objects coexist under the offsets rule; the compactor rewrites inputs
through the same writer. Wide statements are unchanged (an all-columns
read with no numeric arm skips FIELD_DIR already). The probe length stays
at 256 KiB: with decisions 1 and 4 landed the probe holds about 30 KB of
tail, so a probe sized to the tail would save about 0.23 MB of wire per
object (12 s on the sum) and up to one over-read on disk (39 s, the
ceiling), but only if it never misses the tail, since a miss costs the
chase GET back, and whether a 32 KiB read moves fewer disk bytes than a
256 KiB one under the store's read-ahead is what the single-GET probe
measurement on RustFS (epic step 5) answers. Until it does the length
stays, and if it ever changes the plan's probe must be replayed into the
scan by the recorded `PrimedExtent` (`log_fetcher.rs:8559`) rather than
recomputed, or the cached replay becomes a live GET per object.

Effect: minus one GET and about 5 KB per object on the narrow, point
lookup and LIKE classes; minus one per opened object on the pruned class.
Cold sum (model): minus 27 s at `O = 0.5` (242 to 215 s), 43 s at
`O = 0.8`, 5 s at `O = 0` (request occupancy only).

Pre-registered A/B (run 4, the first run on a binary carrying this
change; pairs with run 3). Stamps: the binary digest equals the merge
commit; one object's `rlog inspect` lists STREAM_DIR and FIELD_DIR at
offsets above PAGE_DIR's, tail under 200 KB.

| figure | band | miss | source |
|---|---|---|---|
| scan GETs per object on q2, q34, q33; plan GETs per object on q20 | q2 at run 2's value minus 1 (2.50: probe and one run, plus the second run on the two-row-group objects); q34 run 2's minus 1 (3.57); q33 (4 columns) 2 to 5; q20 exactly 1 (probe only) | q2 at run 2's value: the front GET still fires (order unchanged, or the tail exceeds 256 KiB: read the inspect stamp) | run 1 values comment 6368995087; band predicted |
| cold sum | run 3 minus 20 to 45 s (`O` = 0.5 to 0.8), or minus 3 to 8 s at `O = 0` | outside with the GET stamp passing means `O` moved | predicted |
| hot sum, QPS, error ratio | 54 to 64 s; at least 0.60; at most 0.058 | outside | run 1 comment 6368995087; the 0.058 bar is ADR-2023 decision 4, `docs/adrs/2023-loopback-fetch-cache-share-and-independent-catalog-cache.md:100` |
| answers | equal under the Measurement comparison rule | any difference | predicted |

Proof. Writer test: on a 30-block, 104-column object,
`footer.section(STREAM_DIR).offset > footer.section(PAGE_DIR).offset + len`
and the probe window covers all four directories (mutation: the old
order, fails). Fetcher test (counting store) on the narrow fast path:
`metadata_gets == 0` and `probe_misses == 0`, which fails under the
mutation "old order" with `metadata_gets == 1`. Each re-pin above is
shown failing at the old count first.

Cancellation read against run 1: the change would have been cancelled had
q2's disk/wire been at most 1.3 and the mean GET latency under 3 ms (a
removed GET then worth under 0.2 s per statement). Run 1 read disk/wire
2.36 to 2.50 on q2 and 68 ms: not cancelled.

### 5. The loader exposes `group_target_blocks` (design change 6)

Class: writer configuration (ADR-0699 decision 4,
`docs/adrs/0699-rlog-row-groups-and-page-directory.md:186-198`); a code
surface in `ravel-ingest` and `ravel-cli`; inert at its default. No format
change: PAGE_DIR records each group's `first_block` and `block_count`
(`docs/log-segment-format.md:620-621`).

Mechanism, and why it is the precondition of decision 6. The writer
flushes a row group at `pending.len() >= group_target_blocks.max(1)`
(`crates/ravel-logseg/src/writer.rs:3510-3512`; default 32 at `:88`; doc
at `:66-74`). Today's 25 MB object is 30 blocks, one group by luck of
sizing. A 100 MB object on today's writer is 123 blocks, 4 groups. On the
fast path a segment is opened whole with no fences
(`open_segment_ranged`, `crates/ravel-sql/src/logs_scan/scan.rs:243`;
fences only on a partial share, `log_fetcher.rs:7505-7519`), so a
3-column projection wants 12 chunk extents (`projected_page_extents`,
`log_fetcher.rs:8897`), `bound_runs` bridges the 8 smallest gaps first
(`crates/ravel-query/src/fetcher.rs:945-971`), the within-group gaps
bridge before the cross-group ones, and the bridged bytes become four
times the projection's column-id span. Column ids are assigned in
`(name bytes, type)` order (`writer.rs:535-537`, `:596`), so AdvEngineID
sits near the front and URL and UserID near the end: the span is most of
the group, and the 0.75 coverage crossover
(`DEFAULT_LOG_COVERAGE_THRESHOLD`, `log_fetcher.rs:4193`, applied at
`:7564-7568`) takes a covering read of the whole object. Raising
`--target-bytes` alone therefore turns a multi-column narrow read into a
whole-object read, silently; a one-column read survives at 4 runs.

Every production writer builds `RlogConfig { zstd_level, ..default() }`
(`crates/ravel-ingest/src/log_shard.rs:888-891`; the compactor at
`crates/ravel-maintain/src/rlog.rs:2001-2004`); `IngestConfig` carries
only `rlog_zstd_level` (`crates/ravel-ingest/src/config.rs:708-711`,
default at `:778`); the loader carries `zstd_level`
(`services/ravel-cli/src/load/cli.rs:246`, `load/logs.rs:476-481`). No
loader or server surface exists for the group size (`cli.rs:242-250`).

Files, mirroring the `rlog_zstd_level` path:
`crates/ravel-ingest/src/config.rs` adds `rlog_group_target_blocks: usize`
beside `rlog_zstd_level`, default 32, refusing 0 at construction (0 means
one block per group, the opposite of what a caller raising it wants);
`log_shard.rs:888-891` threads it into `RlogConfig`;
`services/ravel-cli/src/main.rs:785-802` adds `--group-target-blocks <N>`
beside `--zstd-level` (the name mirrors `--zstd-level`, which carries no
`rlog` prefix); `load/cli.rs` and `load/logs.rs` thread it into
`build_router`; the loader flag table in `docs/guides/ingest.md` gains the
row, with one line that a group larger than 32 blocks raises the
compaction merge's per-cursor raw residency
(`crates/ravel-logseg/src/ranged.rs:84-95`: one raw row group per
cursor). No server flag: the entry loads through `ravel-cli`. The
compactor keeps `RlogConfig::default()`; the entry does not compact.

What it does not change: nothing at the default. The request map is
unchanged at 25 MB (already one group). Writer memory: the group buffer is
the object's compressed pages (`writer.rs:3427-3437`), so a 100 MB object
holds 100 MB per shard flush (4 concurrent = 400 MB); dictionary
candidates are capped at `dict_budget = block_max_bytes` (8 MiB) per group
(`writer.rs:3376-3401`). Nothing on the reader binds below `MAX_BLOCKS`
(`crates/ravel-logseg/src/page_dir.rs:449-473`).

Effect on the cold sum: none by itself.

Pre-registered A/B: none on the bot image; the decision is inert at the
default and its stamp is decision 6's load stamp. Its bands are tests:

| figure | band | miss | source |
|---|---|---|---|
| PAGE_DIR groups on a 100-block flush at `rlog_group_target_blocks: 512` (MemoryStore ingest test) | exactly 1 group of 100 blocks; the same flush at the default decodes to 4 groups (32/32/32/4) | 4 groups at 512: the knob is not threaded | predicted (a test, not a box figure) |
| chunk-run GETs on a 120-block, 104-column one-group object, 3 projected columns at far-apart ids, fast-path open, counting store | exactly 3 and no whole read; the same rows at 32-block groups issue at most 4 runs and either a covering read or bridged bytes above 3x the chunk bytes | the first half passes under the mutation "write with the default group size" | predicted (a test, not a box figure) |

Proof: the two tests above, plus a CLI parse test in the shape of
`zstd_level_flag_defaults_to_3_and_refuses_out_of_range`
(`services/ravel-cli/src/main.rs:4904`) pinning the default 32 and the
refusal of 0. The fetcher test starts from the `ONE_GROUP_BLOCKS` fixture
(`crates/ravel-query/tests/log_page_dir_fetch.rs:469-478`) and pins the
coalescing gap with `with_coalesce_gap` (or uses an object whose column
span exceeds the 1.8 MiB gap): on a MemoryStore fixture of test-sized
records the three chunks lie inside one gap and fuse into one run, and
without the pin the first half fails under no mutation.

### 6. The entry's load tiers move to 1M-row objects, about 100 MB, one row group (design change 7)

Class: loader flags in the entry's `load`, paired with decision 5 (a fork
pinned to the release that carries it). After v0.26, and only after the
two-stage A/B below.

Mechanism. 100M rows / 1M rows is about 100 objects (25 per shard) of
about 100 MB stored (the 74x estimate-to-stored ratio,
`docs/guides/ingest.md:1355-1359`), one row group each with decision 5.
Rows per object are bounded by `--pipeline-depth x --batch-rows / --shards`
(`services/ravel-cli/src/main.rs:632-636`; `ingest.md:1366-1368`); an
explicit `--target-bytes` turns the depth flush off
(`services/ravel-cli/src/load/geometry.rs:151-153`); the age trigger
closes a buffer that has not reached the size; the ack deadline is
`max(60 s, delay + 60 s)` (`load/mod.rs:234-240`); a memory budget below
what the window needs makes the loader flush early and silently shrinks
the objects (`load/memory.rs:1-12`, stall flusher at `:47-49`). Today's
recipe at depth 32 measured 7.03 GB peak loader RSS for 25.1 MB objects on
a 128 GB host (`ingest.md:1478-1500`, 408 objects, split 390 / age 14 /
final 4); dividing that peak by the in-flight window (32 x 100k rows)
gives 2.2 KB per row, an RSS figure that includes whatever floor the
loader carries, which stage 1 below stamps.

Flags on the >= 30 GB tier:

| flag | today | proposed | why |
|---|---|---|---|
| `--target-bytes` | 1,850,000,000 | 7,400,000,000 | 74x ratio, 100 MB stored |
| `--pipeline-depth` | 32 | 56 | 1M rows needs 40 at even slices; 1.4x slack because the CounterID-sorted input hashes whole counters to one shard (`shard_for_log`, `crates/ravel-types/src/lib.rs:512`), so slices are uneven |
| `--max-flush-delay` | 30s | 90s | about 27k rows/s per shard (931 s for 100M), so 1M rows fill in about 37 s; 30 s would close every object by age; the ack deadline follows to 150 s |
| `--load-memory-bytes` | 6,500,000,000 | 14,000,000,000 | 56 x 100k x 2.2 KB = 12.3 GB for the in-flight window at the stamped per-row figure, with margin, since a budget below the window shrinks objects silently and only the object-count stamp catches it |
| `--group-target-blocks` | (32) | 512 | decision 5; 512 covers a 2.5M-row object even if the 8 MiB block estimate cap closes hits blocks early |
| `--batch-rows`, `--read-cursors`, `--shards` | 100000, 16, 4 | unchanged | batch rows set per-row decode memory; raise depth, not batches |

The >= 15 GB tier (c6a.2xlarge) goes to 500k-row objects (target
3,700,000,000, depth 32, delay 60s, budget 6,500,000,000, group 512):
about 200 objects of 50 MB. Lower tiers are unchanged. The fork's `load`
refuses the large tier when `ravel-cli load --help` lacks
`--group-target-blocks`: decision 6 without decision 5 is the trap above.

Why 100 MB and not 250 MB in this round: the fast path needs at least
`target_partitions` (32) relevant segments
(`crates/ravel-sql/src/logs_scan/mod.rs:2550-2552`) and deals them
round-robin (`crates/ravel-sql/src/logs_scan/plan.rs:1004-1012`), so 100
segments over 32 partitions is 4 x 4 and 28 x 3, a hot-sum tail of at most
1.28x on a decode-bound statement (41 objects would be 1.56x); with
decision 1 the tail metadata at 123 blocks is about 130 KB and fits the
256 KiB probe, while 250 MB objects (about 330 KB) would not; and the
fetch cache's 64 MiB entry cap (`services/ravel-server/src/store.rs:21`,
admission at `crates/ravel-cache/src/cache.rs:541`) is not a bound on
object size, because a run that crosses the coverage crossover becomes a
covering read split into cached 64 MiB sub-ranges
(`DEFAULT_LOG_MAX_FETCH_RUN_BYTES`, `crates/ravel-query/src/config.rs:337`;
`log_fetcher.rs:5355-5412`), so no run at 100 MB escapes the cache. The
stage 2 stamp on the largest fetch run checks this on the box.

What it does not change: no server flag, no format, no answer. The
pruned class regresses slightly: a 1M-row object spans about four times
the CounterIDs (the file has 8 descents in 100M rows,
`docs/adrs/0979-bounded-memory-rlog-compaction-merge.md:47-49`), so q37 to
q43 open 2 to 4 objects instead of 1 to 2, about 2 s on the sum. The point
lookup's plan pass opens 103 objects instead of 407, and its scan
survivors stay page-granular (`projected_page_ranges`,
`crates/ravel-logseg/src/page_dir.rs:261`). Second-order costs, each with
a stamp: per-open memory (a URL chunk of about 15 MB per object, 64 opens
in flight per statement, up to 1 GB per URL statement against the
per-query pool; the concurrent phase is the risk); row-group dictionaries
now span 1M rows (direction unknown, within a few percent); compaction of
such a tenant later holds 100 MB per cursor (a documented cost, not run by
the entry).

Effect (after #3219 and decisions 1 and 4, per statement): a narrow
statement goes from 1,230 GETs (3 per object at `R = 2`) to 309, 921
fewer GETs and 80 MB fewer probe bytes. Cold sum (model): minus 70 s at
`O = 0.5` less 2 s of pruned-class regression (215 to 147 s); 106 s less
2 at `O = 0.8`; 22 s less 2 at `O = 0`.

Pre-registered A/B, two stages. Stage 1 is the load alone on a
c6a.4xlarge, server stopped, before any statement runs; every figure
present exactly once and inside its band, or the query run does not
start. Stage 2 is the full entry run (run 5, paired with run 4). A miss on
any hot or concurrent figure abandons 1M for 500k before giving up the
lever.

| figure | band | miss | source |
|---|---|---|---|
| stage 1: objects; median stored size; one object's `rlog inspect` | 95 to 110 objects; 90 to 115 MB; 1 row group and 115 to 260 blocks | outside: the query run does not start | predicted; the 74x ratio is `docs/guides/ingest.md:1355-1359` |
| stage 1: age-trigger share; loader peak RSS; load time | under 5% of objects; at most 16 GB sampled every second; 840 to 1,100 s | above 20 GB abandons the 1M tier for 500k; above 1,100 s means flushes queue behind the 37 s fill | predicted; the 7.03 GB peak at depth 32 is `docs/guides/ingest.md:1478-1482` |
| stage 2: narrow scan GETs per statement; q20 | 200 to 500 (q2 at exactly 206: 103 x 2); q20 at most 400 GETs and at most 0.5 GB scan wire | outside | predicted |
| stage 2: q37 to q43 | cold sum at most 7 s; `segmentsPrunedByStats` at least 70 of about 100 | outside | run 1 keeps 30 of 407 at about 3 s, comment 6368995087; band predicted |
| stage 2: hot | hot sum 54 to 64 s; q34, q35, q29 hot at most 1.3x run 4's | outside abandons 1M for 500k | run 1 hot 59.3 s comment 6368995087; 1.3x predicted from the 1.28x partition tail |
| stage 2: concurrent | QPS at least 0.60, error ratio at most 0.058, with the per-partition `prefetch_memory_reopens` and `prefetch_revocations` counters (`crates/ravel-sql/src/logs_scan/mod.rs:1424-1434`) stamped per phase | outside abandons 1M for 500k | run 1 QPS 0.663, error ratio 0.020, comment 6368995087; the 0.058 bar is ADR-2023 decision 4, `docs/adrs/2023-loopback-fetch-cache-share-and-independent-catalog-cache.md:100` |
| stage 2: largest fetch run (`peak_fetch_run_bytes`, `log_fetcher.rs:5287`) on q31 and q32 | at or under 64 MiB | larger: the covering split did not engage and the hot re-read would miss the cache | predicted |
| stage 2: cold sum | run 4 minus 50 to 110 s (`O` = 0.5 to 0.8), or minus 15 to 30 s at `O = 0` | outside | predicted |
| stage 2: answers | equal under the Measurement comparison rule | any difference | predicted |

Proof: decision 5's tests; the fork script asserts the stage 1 stamps
after the load (object count, median size, inspect, age share) and
refuses to continue on a miss; a `page_dir` test that two surviving blocks
100 blocks apart in one group yield two extents (the existing
`projected_page_ranges_keeps_only_the_named_blocks_and_columns`,
`crates/ravel-logseg/src/page_dir.rs:747`, covers the narrowing; the gap
case is the addition); a `ravel-maintain` test that a merge over
one-group 100-block inputs under a cursor budget below one group refuses
typed (`MemoryExhausted`) rather than allocating past it.

Cancellation read against run 1: the change would have been cancelled had
the narrow class sat within 1.3x of its bandwidth floor (disk/wire at most
1.3), since the gain is then under 30 s and bytes are the lever. Run 1
read disk/wire 2.24 to 2.84 on the narrow class: not cancelled. Run 1
read no footer, so the stage 1 `rlog inspect` stamp is the first direct
reading of the row-group count.

### 7. The entry's tenant sets clustering key `EventDate, UserID` at a 1-day bucket (design change 8)

Class: tenant setting the v5 format already carries (ADR-2135 decisions 1
and 2), set in the entry's `load` before the load:

    "$RAVEL_CLI" --store s3 --s3-auth static clustering-key set \
        --tenant "$RAVEL_TENANT" --column EventDate --column UserID \
        --bucket-width 1d --readers-rolled-out

(`services/ravel-cli/src/main.rs:1286-1304`; both columns are declared
i64 in `benchmarks/clickbench/hits.mapping.toml:94-95`, `:109-110`.)
After v0.26, and only after its own A/B.

Mechanism. Rows sort `(stream, day, EventDate, UserID, ts)` inside each
object (`with_sort_descriptor`, applied by the flush at
`crates/ravel-ingest/src/log_shard.rs:906-910`). CounterID is the stream,
so a key on it would add nothing, and the CounterID stamps that prune q37
to q43 are stream-level, so the key does not move run 1's 30 survivors of
407. ADR-2135's simulator: minus 13.1% page bytes for the sort
(`docs/adrs/2135-rlog-v5-storage-footprint.md:65`), minus 1.8% for the
`observed_ts` reference that v5 already banks (`:71`); the combined row is
11.24 to 9.30 GB, minus 17.3%, at zstd 3 (`:90`, `:92`).

What it does not change: no format, no server flag; an uncovered column
is scanned, not pruned. Risk: the ordered `ORDER BY ts LIMIT k` skip (q24,
q25, q27) keeps its answer under any row order (`CHANGELOG.md:1374-1384`)
but loses selectivity, since a block's minimum `ts` no longer rises
monotonically through an object; the cold cost of that is unknown and is
the figure that decides the change. ADR-0815 acceptance 12 forbids a
time-leading key on a measured tenant without a pre-registered A/B
(`docs/adrs/0815-clustered-compaction-and-object-pruning.md:1568-1580`);
this key is ADR-2135's, and the same discipline applies.

Effect: bytes on every scanning class (0.80 to 0.87 W); q20's scan half
shrinks by block pruning on UserID (a block's UserID `[min, max]` is
narrow for every stream longer than a block, and the NumRange arms
resolved at `log_fetcher.rs:7452-7456` drop most blocks; figure
unknown). Cold sum (model): 13 to 16% of W, 15 s at the model's 25 GB
(about 12 s at run 1's 19 GB): 147 to 132 s at `O = 0.5`.

Pre-registered A/B (run 6, paired with run 5). Stamps: `clustering-key
show` prints the key; one object's footer carries the descriptor.

| figure | band | miss | source |
|---|---|---|---|
| answers | equal run 5 under the Measurement comparison rule | any difference abandons the change at once: it is a correctness question | predicted |
| `data_size` | 10 to 16% under run 5's | outside | 13.1% and 1.8% from `docs/adrs/2135-rlog-v5-storage-footprint.md:65` and `:71`; band predicted |
| q20 scan GETs and wire | below run 5's | not below | predicted |
| q24 + q25 + q27 cold sum | growth under 15 s | at or above: abandon | predicted; run 1's walls for the three are the baseline, comment 6368995087 |
| cold sum | run 5 minus 10 to 20 s | outside | predicted |
| hot sum | 54 to 64 s | outside | run 1 hot 59.3 s comment 6368995087; band predicted |

Proof: `crates/ravel-ingest/tests/clustering_key_flush.rs` and
`services/ravel-cli/tests/load_clustered.rs` exist; add a q20-shape test
that `UserID = N` over a keyed 30-block object yields fewer candidate
blocks than over the unkeyed twin (mutation: the unkeyed object), and an
answer-equality test for `ORDER BY ts LIMIT 10` on keyed objects.

## Consequences

Positive.

- The stock entry's cold sum is predicted at about 155 s at the measured
  central over-read and about 255 s with no amplification (Measurement),
  from 367 s, with no server flag, no cache tier and no format bump.
- Every object a scanning statement opens costs one probe plus its chunk
  runs after decisions 1 and 4, on every tenant whose tail fits the probe,
  not only on this entry; the compactor and the ingest flush get the same
  order.
- `--group-target-blocks` gives an operator the row-group size ADR-0699
  decision 4 made writer configuration, with the trap it closes documented
  in the loader guide.
- Each decision has its own A/B with a stamp and a miss condition, so a
  miss names the layer rather than the design.

Negative.

- Decision 4 breaks the golden-bytes tripwire on purpose once and leaves
  ADR-0699 section 5.2 as history. A reader older than the change still
  opens the new objects (offsets rule), but a reader that assumed the old
  adjacency in a comment reads it wrong until the five comments are
  updated.
- Decision 6 raises the loader's working set to about 14 GB on the
  reference box and holds 100 MB per shard flush in the writer; compaction
  of a tenant written this way holds one 100 MB row group per cursor
  (ADR-0979). The entry does not compact; an operator who copies the
  recipe should read the ingest guide's row.
- Decision 7's ordered-`LIMIT` cost is unknown until run 6 and can abandon
  the change; its hot and concurrent bands are the ones most likely to be
  the binding ones at 100 MB objects.
- Decision 3 adds load time of unknown size; its band abandons the change
  above 1,100 s.
- Changes 1 to 3 (entry script) ship with the next ClickBench pin, which
  the owner holds; decision 4 ships in whichever release follows its
  merge, and decisions 5 to 7 after v0.26, each behind its own
  measurement. Until a release carries decision 5 the fork's `load`
  refuses the large tier.

Follow-ups the design leaves to the builder or to a later pass:

- Decision 1: the exact `bloom-scope show` output to stamp, and whether
  `rlog inspect` (`RlogCommand::Inspect`,
  `services/ravel-cli/src/main.rs:1793-1794`) prints section sizes and the
  row-group count; `rlog footprint` emits no row-group count.
- Decision 2: the fold's load-time cost, and whether
  `load_fold_after_load_sql_e2e.rs` already pins the seal-through hour
  against the load's highest ingest hour.
- Decision 4: the amendment text and marker, the golden regeneration
  (`cargo test -p ravel-logseg --test golden_bytes_v5 -- --ignored
  --nocapture`) and the header wording, the exact lines of the re-pins and
  the comments.
- Decision 5: the `IngestConfig` field, the refusal of 0, the ingest guide
  row. Decision 6: the 16 GB tier values, the fork's `--help` guard, the
  RSS sampler for the stage 1 stamp, how the fork script asserts each
  stamp (present exactly once, inside its band, or exit non-zero).
- Observability, optional and reader-only: `probe_misses` and
  `metadata_gets` exist on `BlockRangeStats`
  (`crates/ravel-query/src/log_fetcher.rs:4223-4264`) but are not
  rendered in the response; the phases carry `s3GetRequests` and
  `s3GetWireBytes` (`crates/ravel-sql/src/stats_json.rs:44-45`).
  Rendering the two under `accounting` would let runs 2 and 4 band on
  them directly instead of inferring the chase and front GETs from GETs
  per object. No band above needs it.
- The single-GET probe measurement on RustFS (epic step 5) decides whether
  a shorter probe saves disk under the store's read-ahead; until then the
  probe length is not a lever.
- Stale text seen while checking, reported and not fixed here: ADR-0699
  section 5.3 describes a 64 KiB `coalesce_gap` while the code's gap is the
  request cost with 64 KiB as a floor
  (`log_fetcher.rs:4151-4161`, `:5445-5448`);
  `docs/log-segment-format.md:470-476` states the physical chunk order as
  ascending column id, which `validate_extents` (`page_dir.rs:387-404`)
  checks only for bounds, so a writer could violate the sentence
  undetected.

## Rejected alternatives

- **A trailer version bump for decision 4.** Not required by the format
  doc's bump rule or its reader rule (quoted in decision 4); a bump buys no
  performance, because the bytes and GETs per object are identical either
  way, and costs a two-version reader window on a reader that accepts
  version 5 only, which would make it a separate epic after v0.26. Owner
  decision, 2026-10-23.
- **A loopback request-cost default derived from the measured `O`**
  (design change 5: resolve `--logs-request-cost-bytes` to 524,288 when
  `store_is_loopback()`,
  `services/ravel-server/src/config.rs:6762-6768`, with no flag set, by
  the precedent of the 0.24.0 loopback policy default at `:6783-6796`,
  moving the coalescing gap `effective_coalesce_gap` at
  `log_fetcher.rs:5445-5448` from the S3-calibrated 1,887,437 B at
  `:4151`). Dropped by owner decision on 2026-10-23: a figure measured on
  one gp2 volume is not a product default, for about 4 s of gain. It stays
  a measured experiment with no run of its own, out of the run plan and
  the cold-sum table.
- **The reader-first path on its own** (a tail-aware probe in long and
  short modes, granule-aligned small GETs, 64 GET permits, concurrent
  chase and front GETs, a different coverage crossover). The long probe is
  moot once decisions 1 and 4 land, since the 256 KiB probe already covers
  everything; the short probe is folded into decision 4's analysis and
  waits on the single-GET measurement; more permits add queueing, not
  throughput, on a volume already at its cap at 32 in flight; concurrent
  GETs change neither GETs nor bytes and are invisible on a saturated
  device; no statement projects between 75% and 90% of an object. Its two
  sound grafts (the request-cost default, now dropped, and the LIKE-branch
  check, which run 1 answered) came through.
- **The geometry-first path on its own** (250 MB objects, or compaction
  into large L1 parts, before the per-object overhead is removed). 250 MB
  objects cost a 1.56x fast-path hot tail at 41 objects, a tail that
  exceeds the probe, and a loader budget of about 30 GB that does not fit
  the box; compaction's derived target makes 12 to 18 MB parts on this
  box (smaller than L0), takes 2.7 to 8 h against a 931 s load, is the
  post-load step the ClickBench rule forbids, and its parts in BLAKE3
  stream order are predicted to undo the CounterID object pruning. Its
  row-group trap, flag table and load stamps came through as decisions 5
  and 6.
- **A disk cache tier (`--cache-dir`) for the stock entry.** That is the
  tuned entry, its own separately labelled result; the stock result is
  what this ADR moves.
- Also out of this round, each a format change: chunk order inside a row
  group (the format doc states ascending column id,
  `docs/log-segment-format.md:470-476`; byte order puts URL at slot 87 and
  UserID at 99 of 104, so the change would help); larger blocks (coarsens
  every block-level index 1:1, ADR-0699 rejected alternative 1); an
  object-level string dictionary or one zstd frame per chunk (ADR-2135
  rejected alternatives, `docs/adrs/2135-rlog-v5-storage-footprint.md:345-350`).
  zstd 19 is out on load time (5,498 s against 931 s, one measurement). A
  device read-ahead setting in `install` is neither a flag nor a cache
  tier, but whether the benchmark allows a block-device setting is an
  owner question and it is not proposed.

## Measurement

Run plan. Run 1 is done (367.2 s, v0.25.1, A box the v0.24.0 release box
at 367.0 s). Each later run carries one change and pairs with the run
before it:

| run | carries | A box | B box |
|---|---|---|---|
| 2 | decisions 1 and 2 (fork PR 1) | current `main` with #3221, stock `load` (band both rows of decision 1 if #3221 has not merged) | the same commit, fork PR 1 |
| 3 | decision 3 (fork PR 2) | one `main` commit, fork PR 1 | the same commit, fork PR 2 |
| 4 | decision 4 (Ravel, one PR) | the `main` commit just before decision 4 merged, fork PR 2 | the merge commit of decision 4's PR, fork PR 2 |
| 5 | decision 6 (fork PR 3), after the stage 1 load stamp | a `main` commit carrying decision 5, fork PR 2 | the same commit, fork PR 3 |
| 6 | decision 7 (fork PR 4) | run 5's B commit, fork PR 3 | the same commit, fork PR 4 |

A/B binaries. Every measurement run builds Ravel from source at a `main`
commit through the entry's `RAVEL_REF` path, as the kit runs on #3072
did; only the published ClickBench pin uses a release tag. Each A/B is two
boxes launched together: A at the `main` commit just before the change
merged, B at the merge commit, so the delta is the change's own and not
the sum of whatever else landed on `main` between runs. When the two
commits are adjacent, A's commit is the previous run's B commit; when they
are not, the run has one extra A box at a commit no earlier run measured.
Run 4's A is the commit just before decision 4 merged, not run 3's binary.
For a fork change (runs 2, 3, 5, 6) the two boxes share one `main` commit
and differ only in the fork's `load`. A band that reads against the
previous run ("run 2's value", "run 3 minus") reads against the A box
launched with it. Every run stamps the burst-credit state: the cold sweep
moves under 10% of the credit bucket and run 1 saw no fall in the r/s
ceiling, but the stamp is taken all the same.

Answer comparison. A statement is compared by byte-equal response. A
LIMIT statement with no total order on the selected rows is compared as a
row set instead, the same rows in any order, and a difference in which
tied rows surface counts as a difference too. Three statements are exempt
from that last clause because run 1 already showed them differing between
tries of one binary: q18 and q25 by design, q22 by tie order. Those three
are compared as row sets alone. A change in any row's values is always a
difference, on every statement.

Pull requests, one behaviour change each (ADR-2867 change 4): decision 4
(`ravel-logseg` plus docs, the ADR-0699 amendment, the golden regeneration
with its header amended, the reader comments, the test re-pins; gate
`scripts/gates.sh -p ravel-logseg`); decision 5 (`ravel-ingest`,
`ravel-cli`, docs; gate `-p ravel-ingest -p ravel-cli`). Each carries a
`changelog.d` fragment. Fork pull requests: four, one per run row above;
decision 2 shares fork PR 1 with decision 1 as two independently
revertible lines.

Cumulative cold-sum arithmetic, in delivery order. The anchor is run 1's
367.2 s. The `O = 0.5 MB` column is the central estimate and is
cumulative; the `O = 0` column is the no-amplification reference; the last
column is each change's own saving at the upper bound and is not
cumulative, because those upper bounds sum to about 320 s, more than the
GET-attributable disk bytes hold, so each is the most that change can
show on its own A/B. Every figure below the anchor is predicted.

| after | GETs per object, narrow one-column | cold sum at O = 0.5 MB | cold sum at O = 0 | own saving at O = 0.8 | basis |
|---|---|---|---|---|---|
| v0.25.1 stock (run 1) | 5.5 | 367.2 (anchor) | 367.2 (anchor) | | measured 2026-10-23, comment 6368995087 |
| #3219, PR #3221 (landed) | 4.5 | 277 | 303 | 106 | predicted |
| decision 1, bloom scope `text` | 3.5 | 249 | 301 | 44 | predicted |
| decision 2, fold after load | 3.5 | 249 (0 to 2 s, not counted) | 301 | 0 to 2 | predicted |
| decision 3, zstd 9 | 3.5 | 242 | 294 | 7 (bytes only) | predicted |
| decision 4, directories at the tail | 2.5 | 215 | 289 | 43 | predicted |
| decisions 5 and 6, 100 MB one-group objects | 2 over 103 objects | 147 | 269 | 104 | predicted |
| decision 7, clustering key | 2 over 103 | 132 | 254 | 15 (bytes only) | predicted |

Stated estimate (predicted): about 155 s at the measured central `O`
(132 s central plus about 20 s for non-additivity and a 3 to 5% box
spread); about 255 s with no amplification. Run 1's `O` of 0.39 to
0.62 MB per GET confirms the central column, and that column says the
goal (under 200 s) needs decisions 1 to 4 and 5 with 6: decisions 1 to 4
alone land at about 215 s at `O = 0.5` (289 s at `O = 0`), and decision 6,
with decision 5 under it, is the one that crosses 200 s, to 147 s. The
stretch (under 150 s) needs the central reading and all seven decisions.
With no amplification the design reaches about 255 s and the remaining
term is column bytes, about 80 s after decisions 3 and 7, which only a
format change (object-level dictionaries, or one frame per chunk, both
rejected by ADR-2135) or the tuned entry's cache tier moves; neither is
in this ADR, and that is stated rather than stretched.

Device check after the design: a narrow statement at 103 objects x 3 GETs
is 309 GETs and 0.28 GB of wire; its bandwidth floor is 1.1 s with no
amplification and 3.5 s at a MinIO-like 2 MiB per GET, against today's 2.7
to 4.2 s and 16 to 23 s. The design moves the per-statement floor 2.5x
with no amplification and 5 to 6x under it, which is why the two sums
differ so much: request count only matters if the store charges per
request, in money or in disk bytes.
