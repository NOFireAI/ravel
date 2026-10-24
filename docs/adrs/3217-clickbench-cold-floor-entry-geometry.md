# ADR-3217: the ClickBench cold floor: remove the per-object overhead the stock entry pays, then divide the object count by four

Status: Proposed (2026-10-24). Issue #3217 (epic). Source: the design
approved by the owner on 2026-10-23 on #3217, measured against run 1
(`coldfloor-20261023T135710Z`, v0.25.1). No persistent format change:
every RLOG object keeps its grammar, its trailer version and its footer.
Decision 4 moves two sections inside the writer's emission order, which
`docs/log-segment-format.md` names a writer convention, and lands as an
ADR-0699 amendment with no trailer bump (the basis is quoted in decision
4). Decision 8 changes the order in which the writer assigns column ids,
also a writer convention. Every other decision is an entry-script change,
a loader flag or a tenant setting the v5 format already carries.

**Correction (2026-10-24).** The cost model this ADR was written with,
"disk bytes = requested bytes plus about 0.5 MB per GET", was refuted on
the day it merged. Run 2, the #3219 A/B (comment 6384701235), removed one
GET and 1.17 MB of wire per object on every ranged scan, and the cold sum
moved from 367.9 s to 364.9 s: disk bytes per object were unchanged within
1 % on every statement and the answers byte-equal on all 43. The store
probe (comment 6384802590) then measured the read granularity directly,
and run 2's own per-object figures fix it on the bench box: a request
costs its requested bytes plus about 0.8 to 0.9 MB (run 2's q2, q34 and
q23; the probe read 0.6 to 4 MB across two boxes) for each contiguous span
of the object it reads, the store reading every span out to whole
1 MiB-aligned blocks, and a request inside a block an earlier request
already read costs almost nothing. The model is now
the block model stated in Context; every band and estimate below is
restated in it; the delivery order moves the position-removing decisions
(4, then 5 with 6, then the new decision 8) ahead of the byte trims; and
the cumulative table starts from run 2's 364.9 s. This ADR is Proposed, so it
is edited in place (`docs/adrs/README.md`, "Amending an ADR", applies to
Accepted decisions). The two measured anchors are run 1 and run 2;
every other figure below is predicted until the run that reads it.

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
- **The store reads about 1 MB around every distinct block position a
  request touches, not a fixed amount per GET and not the requested
  bytes.** Run
  1 read the over-read as `O = (disk - wire) / GETs`, 0.39 to 0.62 MB per
  GET, and this ADR first priced every removed GET at 0.5 MB of disk. Run
  2 (comment 6384701235) refuted that: #3219 removed one GET and 1.17 MB
  of wire per object on every ranged scan and the disk bytes per object
  did not move (q2 4.00 to 3.96 MB for 1.585 to 0.411 MB of wire; q34
  8.94 to 8.90 for 5.147 to 3.981; q23 14.47 to 14.41 for 10.837 to
  9.670); the cold sum went from 367.9 s to 364.9 s (B / A 0.992) and the
  answers were byte-equal on all 43 statements. The probe (comment
  6384802590) then measured the granularity on a 24.4 MB object with the
  server stopped and the page cache dropped before each request: a cold
  10 KB request costs RustFS 2.8 to 3.95 MB of file reads wherever it sits
  on one box (the first request after a drop also carries the store's own
  code pages) and 0.6 to 1.8 MB on a second; 1 MiB costs about 2.9 MB;
  4 MiB about 6 MB; the whole object its own size plus 0.4 to 0.8 MB; and
  a range inside a block an earlier request already read costs 4 KB,
  because RustFS keeps an in-process cache. The probe's device column is
  confounded by the AWS CLI's own page-ins and is not evidence. So
  removing requested bytes inside a position a statement still touches
  saves nothing, and removing a position saves about `K`, whatever its
  requested size.
- **A narrow statement spends its disk time on fixed per-object positions.**
  q2 and q8 open every object with 5.50 GETs and 1.58 MB (2,239 GETs,
  645 MB per statement) on run 1; q34 6.57 GETs and 5.15 MB; q33 7.93 and
  5.84; q12 7.49 and 2.89. The per-object GETs are a 256 KiB suffix probe,
  a SKIP_IDX+PAGE_DIR chase, a STREAM_DIR+FIELD_DIR front GET, a BLOOM GET
  of 1,191,906 B that no ClickBench statement reads (gone since #3219),
  and 1 to 4 chunk runs. Each GET reads one contiguous span; after #3219
  the spans are the front directories (STREAM_DIR 201 B and FIELD_DIR
  1,043 B on the median object) at offset 0, the chase (SKIP_IDX 5,970 B
  and PAGE_DIR 22,312 B) about 1.22 MB before the end, the probe (the last
  256 KiB), and each column run. The 1.19 MB BLOOM sits between the chase
  and the probe, so those two are about 0.95 MB apart and land in
  different 1 MiB blocks. The store reads every span out to whole
  1 MiB-aligned blocks at both ends: a span of `s` bytes touches about
  `s` / 1 MiB + 1 blocks, so it costs about `s` + 1 MiB of disk, the
  requested bytes plus about one block of edge waste (less on the probe's
  span, which ends at the object's partial last block). The requested
  bytes are the wire; the edge waste per span, `K`, is the overhead.
  Dividing run 2's disk-minus-wire by its GETs per object (arm B) gives
  the same `K` on statements of very different width: q2 (3.96 - 0.41) /
  4.51 = 0.79 MB, q34 (8.90 - 3.98) / 5.58 = 0.88 MB, q23 (14.41 - 9.67) /
  5.51 = 0.86 MB, against 9.7 MB of requested bytes on q23 and 0.41 on q2.
  So the bench box reads `K` of about 0.8 to 0.9 MB per span, a little
  under one whole 1 MiB block (1.05 MB). The front and chase spans are
  then about 1.6 to 1.8 MB per object per statement, about 80 to 90 s of
  the 365 s (x 407 objects x 32 statements above 3 s / 262 MB/s).
  q2's projected column is 3.8 KB of the 0.41 MB it requests per object.
  Stored column bytes on the median object: AdvEngineID 3.8 KB, UserID
  0.80 MB, SearchPhrase 0.65 MB, URL 2.44 MB; summed over the 30
  footprinted statements the projected bytes are about 19 GB.
- **The other classes are settled.** The four LIKE statements (q21 to
  q24) run the fast path with 0 or 1 plan GET, so there is no reader fix
  for an undecidable LIKE branch in this round. q37 to q43 keep 30
  survivors of 407 (`segmentsPrunedByStats` 377) and cost about 3 s in
  total. Hot tries are served from the fetch cache (resident maximum
  4.41 GB, no evictions) and the hot wall is decode: hot 59.3 s, QPS 0.663,
  error ratio 0.020, each inside its band. The hot profile (comment
  6385475852) reads the heavy statements CPU-bound at 9 to 14 of 16 cores
  and the sub-second ones idle, waiting on resolve round trips, so none
  of the cold levers here moves hot; hot belongs to a hot-path epic.

Already on `main`, not a decision here: #3219 (pull request #3221, landed
on `main` as a7194f402 then 46df74786) stops the reader fetching BLOOM and
POSTINGS when no arm needs them. Run 2 measured it (comment 6384701235):
minus one GET and 1.17 MB of wire per object on every ranged scan (wire
over the statements above 3 s 49.6 to 34.4 GB), the cold sum 367.9 to
364.9 s, disk bytes per object unchanged within 1 %, hot 58.1 to 57.3 s,
answers byte-equal. It stays: it costs nothing and the wire it saves is
the cost on S3. The 90 s this ADR first credited it with was the per-GET
model's figure and is withdrawn. Every A commit in the run plan below
carries it.

The cost model every estimate uses, per scanning statement over `N`
objects on the fast path:

```
disk bytes per object = requested bytes + positions x K
disk bytes            = N x (requested bytes + positions x K)
cold wall             = T0 + disk bytes / 262 MB/s
```

A position is one contiguous span of the object a request reads (one GET,
one span). The store reads it out to whole 1 MiB-aligned blocks at both
ends, so a span of `s` requested bytes costs about `s` + `K` of disk: the
requested bytes are in the wire term and `K` is the edge waste, about one
block per span; two spans inside one block share it. The bytes a long
span forces through many blocks are its own requested bytes, already
counted in the wire, so `K` does not grow with span length. `K` is 0.79,
0.88 and 0.86 MB on run 2's q2, q34 and q23 (above), a little under one
whole 1 MiB block (1.05 MB), with the probe's 0.6 to 4 MB per cold request
across two boxes (comment 6384802590) as the outer bound. Every table below prices `K` at 0.9 MB and 0.8 MB, and every
pool of disk bytes is
taken over the 32 statements above 3 s that run 2 measured, 407 x 32 =
13,024 object-statements. The requested
bytes are the probe, the directories and the projected column pages `W`
plus the bridged gaps `g`; decisions 3 and 7 move `W`, decisions 5 and 6
the probe count. `T0` is about 0.2 s. Run 1 puts `disk bytes / 262 MB/s`
about 1 s above each full-scan statement's wall, so the formula is a floor
to within 5%. The wire floor, disk equal to wire with every position cost
gone, bounds every column: run 2's 34.4 GB of wire over the statements
above 3 s is 131 s at 262 MB/s, plus about 6 s of statements under 3 s.

Per-object request map for the narrow predicate-free class (29 of the 34
scanning statements), in delivery order; `R` chunk runs per object (1 for
one column on a one-group object, up to `min(k, 4)` for `k` columns; a
two-row-group object adds a run; `chunk_run_cap` at
`crates/ravel-query/src/log_fetcher.rs:8762-8767`,
`MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT` at `crates/ravel-query/src/fetcher.rs:294`),
`W` projected page bytes, `g` bridged gap bytes, `B` the 1.19 MB BLOOM.
The wire column is rounded to 0.01 MB, so decision 4's 1.2 KB front pair
is below its resolution and the 0.03 MB step on that row is the chase
(SKIP_IDX plus PAGE_DIR), which the new section order also takes out of
the request set. `R` here counts chunk runs, not positions; the probe
is one position, two on the objects where its 256 KiB straddles a block
boundary (about a quarter):

| stage | GETs per object | positions per object | wire per object | objects |
|---|---|---|---|---|
| v0.25.1 stock (run 1) | probe, chase, front, BLOOM, R runs = 4 + R | front, chase, probe, R runs = 3 + R | 0.30 MB + B + W + g | 407 |
| after #3219 (run 2) | probe, chase, front, R runs = 3 + R | 3 + R | 0.30 + W + g | 407 |
| decision 4 (directories at the tail) | probe, R runs = 1 + R | probe, R runs = 1 + R (2 + R when the probe straddles a block boundary) | 0.27 + W + g | 407 |
| decisions 5 and 6 (100 MB one-group objects) | 1 + R | 1 + R | 0.27 + 4 W + 4 g | 103 |
| decision 8 (column ids in affinity order) | 1 + R', R' at most R | 1 + R' | 0.27 + 4 W + 4 g', g' under g | 103 |
| decision 1 (bloom scope `text`) | 1 + R' | 1 + R' (the tail shrinks; it was one position already) | 0.27 + 4 W + 4 g' | 103 |
| decision 3 (zstd 9) | 1 + R' | 1 + R' | 0.27 + 4 x 0.93 W + 4 g' | 103 |
| decision 7 (clustering key) | 1 + R' | 1 + R' | 0.27 + 4 x 0.80 W + 4 g' | 103 |

The goal is a cold sum under 200 s on the stock entry, under 150 s as the
stretch, open-ended rather than bound to v0.26 or the next ClickBench pin
(owner decision, 2026-10-23). The hot sum stays inside 54 to 64 s, the
concurrent phase at or above 0.60 QPS (this epic's standing bar, carried
from run 1's pre-registration, comment 6366582370, which banded QPS at
0.60 to 0.75; ADR-2023 decision 4 sets 0.40) and at or under an error
ratio of 0.058 (ADR-2023 decision 4), and no change may alter an answer.

## Decision

Eight decisions, numbered as first proposed (the numbers are cited on
#3217 and in the run readouts, so they keep their numbers). Delivery
order: 4, then 5 with 6, then 8, then 1 with 2, then 3, then 7. Each names
the design change it carries, its class, what it does not change, and its
own pre-registered A/B band. Every A/B compares all 43 statements against
its A box under the answer comparison rule in Measurement, and a
difference stops the change. The position-removing decisions (4; 5 with
6; 8) go first because on this box a position removed per object is worth
about `K` of disk whatever its requested size (run 2, the probe); the
byte trims (1, 3, 7) follow, each small.

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
the trailer is then SKIP_IDX (5,970 B) + PAGE_DIR (22,312 B) + BLOOM
(2 KB) + footer (200 B), about 30 KB, inside the 256 KiB probe
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

Effect: small. The tail shrinks from about 1.5 MB to about 30 KB, but
after decision 4 the probe already reads the whole directory tail in one
position, and a position's cost is `K`, not its size (run 2: removing the
1.19 MB BLOOM request changed no disk byte).
After decision 4 the chase is already gone, so on this corpus the scope
removes no request and no position; it takes about 0.5 GB of BLOOM out of
`data_size` and keeps the whole tail, BLOOM included, inside the 256 KiB
probe, so a statement that does build a bloom arm (none here) reads no
extra position, and a wide tenant with a BLOOM larger than the probe gets
the same. Cold sum (model): under 2 s, not counted in the cumulative
table. The 28 s this ADR first credited it with was the per-GET model's
figure and is withdrawn.

Pre-registered A/B (run 6, shared with decision 2 because their stamps are
disjoint and both cold effects are inside noise; the A box is run 5's B).
Stamps: `bloom-scope show` prints `text`; one object's `rlog inspect`
shows a BLOOM section under 8 KB and one row group; the binary digest
equals the A box's. Each figure present exactly once:

| figure | band | miss | source |
|---|---|---|---|
| scan GETs per object on q2 and q34 (`P.scan.s3GetRequests / objects`) | equal to the A box's to within 0.05 (the chase is gone since decision 4; the scope removes no request) | a change in either direction: read the request shape before crediting or blaming the scope | run 2 values 4.51 and 5.58 on 407 objects, comment 6384701235; band predicted |
| one object's BLOOM section, and `data_size` | BLOOM under 8 KB; `data_size` 0.4 to 0.6 GB under the A box's | BLOOM near 1.19 MB: the scope did not reach the objects (check the BLOOM covered list on one object) | run 1 BLOOM 1,191,906 B and `data_size` 10.21 GB, comment 6368995087; band predicted |
| disk bytes per object on q2 and q34 | the A box's within 5 % | a drop above 5 % with the GET stamp passing means the probe straddled block boundaries often enough to matter and the shorter tail removed the straddle: record the per-position figure it implies | run 2 values 3.96 and 8.90 MB, comment 6384701235; band predicted |
| cold sum | the A box's within minus 5 to plus 5 s | outside: read the disk-per-object row first | predicted |
| answers | all 43 equal the A box under the Measurement comparison rule | any difference: stop, this change must not change an answer | predicted |
| hot sum, QPS, error ratio | 54 to 64 s; at least 0.60; at most 0.058 | outside | run 2 hot 57.3 s, QPS 0.703, error ratio 0.023, comment 6384701235; the 0.058 bar is ADR-2023 decision 4, `docs/adrs/2023-loopback-fetch-cache-share-and-independent-catalog-cache.md:100` |

Proof. A test beside
`probe_covers_the_plan_sections_on_a_wide_row_group_object`
(`crates/ravel-query/tests/log_page_dir_fetch.rs:1738`): a 30-block,
104-column object written at scope `text`, opened on the narrow fast path,
reports `probe_misses == 0` and `metadata_gets == 1` (the front GET only;
0 once decision 4 has landed, which this test is written after); the same
object at scope `all` with a 1 MB BLOOM reports `probe_misses == 2` and
`metadata_gets == 2` on today's order. The mutation "writer ignores the scope" fails
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

Pre-registered A/B (shares run 6). Stamp: the load log prints the fold
report with the seal-through hour equal to the load's last ingest hour.

| figure | band | miss | source |
|---|---|---|---|
| resolve LISTs per statement (`P.resolve` list requests) | exactly 1 inside the load's last hour, exactly 5 after it | other | run 1 read 5, comment 6368995087; band predicted |
| listing keys returned per LIST, and resolve wall | 0 keys per shard folded (about 100 unfolded); resolve wall at or under run 1's 29 to 33 ms | keys per LIST at run 1's count: the hour was not sealed | run 1 values comment 6368995087; band predicted |
| load time | under the A box's plus 60 s | above | run 2 load 865 s, comment 6384701235; band predicted |

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

Effect: bytes only, 0.93 W on every scanning class, and no position. Cold
sum (model): about 8 s at run 2's 30.8 GB of column wire (34.4 GB over the
statements above 3 s less 3.6 GB of probe bytes, x 0.07 / 262 MB/s); the
cumulative table uses that basis, and the same figure at either `K`.
Last in the delivery order because it is the smallest lever that is not
free.

Pre-registered A/B (run 7, on top of run 6's script; its own run because
its effect is bytes and would confound the per-position reading the runs
before it produce). Stamps: the load log shows `--zstd-level 9`; one
object's pages decode.

| figure | band | miss | source |
|---|---|---|---|
| `data_size` | 0.925 to 0.95 x run 6's measured value (run 6's script already carries decision 1, which takes about 0.5 GB of BLOOM out of run 1's 10.21 GB, so the band is relative to run 6) | above 0.97 x run 6's: the level did not reach the pages | 7.1% from `docs/adrs/2135-rlog-v5-storage-footprint.md:67`; run 1 `data_size` 10.21 GB comment 6368995087; band predicted |
| load time | 840 to 1,100 s | above 1,100 s abandons the change: a cold gain under 8 s is not worth 170 s of load | predicted (the entry's load today is about 865 to 931 s, run 2 comment 6384701235) |
| cold sum | run 6's B minus 4 to 12 s | outside | predicted |
| hot sum | within 5% of run 6's B | outside | predicted |
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
STREAM_DIR, FIELD_DIR, footer, trailer. Besides moving the front pair to
the tail, this moves BLOOM and POSTINGS ahead of SKIP_IDX and PAGE_DIR:
today the two optional indexes sit after PAGE_DIR, so a BLOOM larger than
the probe pushes SKIP_IDX and PAGE_DIR out of the 256 KiB window and the
chase GET fires. With the two large optional indexes before the four small
directories, on any tenant the 256 KiB probe holds the footer, PAGE_DIR,
SKIP_IDX, STREAM_DIR and FIELD_DIR, and BLOOM or POSTINGS cost the one GET
they cost today when large. On this entry the probe holds the four
directories under the new order whether or not decision 1 has landed,
because the 1.19 MB BLOOM sits before them; with decision 1 the whole
tail is under 50 KB. Both the front GET and the chase GET go, so on this
corpus the change removes two GETs per object and two positions: the
front, at offset 0, and the chase, about 1.22 MB before the end and so
about 0.95 MB from the probe's block, behind the 1.19 MB BLOOM (Context).
Run 3's disk-per-object row reads whether both positions go.

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
`metadata_gets == 0` and two;
`per_phase_wire_bytes_sum_to_the_pooled_object_store_bytes`
(`log_page_dir_fetch.rs:2369`) and the callers of
`narrow_projection_placed_bytes` (`:2485`), which sum front bytes as a
separate GET; the `log_fetcher.rs` test
`carried_directories_fetch_no_front_section` (`:15434`); and
`probe_covers_the_plan_sections_on_a_wide_row_group_object` (`:1738`),
which gains the assertion that STREAM_DIR and FIELD_DIR are inside the
window.

What it does not change: no grammar, no offsets, no answer. Old and new
objects coexist under the offsets rule; the compactor rewrites inputs
through the same writer. Wide statements are unchanged (an all-columns
read with no numeric arm skips FIELD_DIR already). Writer memory:
`stream_dir_entries` and `field_dir` (about 1.2 KB encoded on the median
object) stay alive until the tail push, where today `writer.rs:1022` and
`:1029` drop them right after the front push, unless the builder
compresses early and pushes the bytes late. The probe length stays
at 256 KiB. The single-GET probe (comment 6384802590) answered the
question this paragraph first left open: a 10 KB read at the end of the
object costs 2.8 MB of file reads and the 256 KiB suffix 2.9 MB, so a
probe sized to the tail would save about 0.23 MB of wire per object
(12 s of wire on the sum, which on this volume is paid once as part of the
probe's block and not again) and no position; it is not a disk lever here. If the
length ever changes the plan's probe must be replayed into the scan by
the recorded `PrimedExtent` (`log_fetcher.rs:8559`) rather than
recomputed, or the cached replay becomes a live GET per object.

Effect: minus two positions per object on every statement that opens an
object with a front GET (the narrow, point lookup and LIKE classes, and
each opened object on the pruned class): the front directories' block at
offset 0 and the chase's block before BLOOM. In requests, minus two GETs
and about 30 KB per object (the chase's SKIP_IDX plus PAGE_DIR; the front
pair itself is STREAM_DIR 201 B plus FIELD_DIR 1,043 B on the median
object). Cold sum (model): 2 x `K` x 13,024 object-statements / 262 MB/s,
minus 89 s at `K = 0.9 MB` (23.4 GB; 364.9 to 276 s) and minus 79 s at
`K = 0.8 MB` (20.8 GB; to 286 s), a little less if the probe straddles a
block boundary more often than a quarter of the time; q2 from 3.96 to
about 2.2 to 2.4 MB of disk per object (0.38 MB requested plus about 2.5
positions). The 26 s this ADR first credited it with was the per-GET
model's figure and is withdrawn: under the block model this is the
largest single lever, which is why it now goes first.

Pre-registered A/B (run 3, the first run on a binary carrying this
change; pairs with run 2, whose B arm is the A box). Stamps: the binary
digest equals the B commit (the last of the pull request's commits as
they landed on `main`, Measurement); one object's `rlog inspect` lists
STREAM_DIR and FIELD_DIR at offsets above PAGE_DIR's, tail under 200 KB.

| figure | band | miss | source |
|---|---|---|---|
| scan GETs per object on q2, q34, q33; plan GETs per object on q20 | q2 at run 2's 4.51 minus 2 (2.51: probe and one run, plus the second run on the two-row-group objects); q34 run 2's 5.58 minus 2 (3.58); q33 (4 columns) run 2's 6.93 minus 2, to within 0.1; q20 exactly 1 (probe only) | q2 at run 2's value: the front GET still fires (order unchanged, or the tail exceeds 256 KiB: read the inspect stamp); minus 1 only: the chase still fires, BLOOM is not ahead of the directories | run 2 values comment 6384701235; band predicted |
| disk bytes per object on q2, q34, q23 (`disk read / objects`, cold try) | q2 2.0 to 2.6 MB from run 2's 3.96; q34 6.9 to 7.5 from 8.90; q23 12.4 to 13.0 from 14.41 (minus two spans at `K` = 0.8 to 0.9 MB, plus the 30 KB the chase requested) | unchanged within 5 % with the GET stamp passing: the positions are not the unit this store charges, re-read the block map from this run; minus one position only: the chase shared the probe's block, record `K` | run 2 values comment 6384701235; band predicted |
| cold sum | run 2's B (364.9 s) minus 65 to 100 s | a drop under 30 s with the GET and disk stamps passing means the disk is not the floor on the statements that moved: read the per-statement walls against their disk time before any later step | run 2 value comment 6384701235; band predicted |
| hot sum, QPS, error ratio | 54 to 64 s; at least 0.60; at most 0.058 | outside | run 2 hot 57.3 s, QPS 0.703, error ratio 0.023, comment 6384701235; the 0.058 bar is ADR-2023 decision 4, `docs/adrs/2023-loopback-fetch-cache-share-and-independent-catalog-cache.md:100` |
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
2.36 to 2.50 on q2 and 68 ms: not cancelled. Run 2 read q2 at 3.96 MB of
disk for 0.41 MB of wire, disk/wire 9.6, with the front and chase GETs two
of about 4.5 positions: the strongest reading for this change so far.

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

Effect (after #3219 and decision 4, per statement): a narrow statement
goes from 1,230 GETs (3 per object) to 309, 921 fewer GETs and 80 MB
fewer probe bytes. A 100 MB one-group object holds what four 25 MB
objects held, so each of a statement's column runs becomes one span four
times as long instead of four spans: under aligned blocks four spans of
`s` bytes touch about 4 `s` / 1 MiB + 4 blocks and the merged span
4 `s` / 1 MiB + 1. The requested bytes are unchanged (they are the wire
term) and the edge waste per run falls from four `K` to one; the probe's
span goes from four to one the same way. So every span a statement pays
per object is paid 103 times instead of 407, and what this decision
divides is the edge waste, never the column bytes. Cold sum (model), from
the measured overhead rather than from `K` alone: run 2 read 102.8 GB of disk for
34.4 GB of wire over the 32 statements above 3 s (comment 6384701235), so
68.4 GB is edge waste, the disk-minus-wire that holds no requested byte;
decision 4 removes 2 x 13,024 x `K` of it
(23.4 GB at `K = 0.9 MB`, 20.8 GB at `K = 0.8 MB`), leaving 45.0 or
47.6 GB; decision 6 divides what is left by 407 / 103 = 3.95, saving 0.75
of it: 33.6 GB (128 s) at `K = 0.9` and 35.6 GB (136 s) at `K = 0.8`,
less about 2 s of pruned-class regression. The lower `K` gives the larger
saving here because more of the measured overhead is then left in the
positions this decision divides. Cold sum 276 to 150 s at `K = 0.9`, 286
to 152 s at `K = 0.8`.

Pre-registered A/B, two stages. Stage 1 is the load alone on a
c6a.4xlarge, server stopped, before any statement runs; every figure
present exactly once and inside its band, or the query run does not
start. Stage 2 is the full entry run (run 4, paired with run 3). A miss on
any hot or concurrent figure abandons 1M for 500k before giving up the
lever.

| figure | band | miss | source |
|---|---|---|---|
| stage 1: objects; median stored size; one object's `rlog inspect` | 95 to 110 objects; 90 to 115 MB; 1 row group and 115 to 260 blocks | outside: the query run does not start | predicted; the 74x ratio is `docs/guides/ingest.md:1355-1359` |
| stage 1: age-trigger share; loader peak RSS; load time | under 5% of objects; at most 16 GB sampled every second; 840 to 1,100 s | above 20 GB abandons the 1M tier for 500k; above 1,100 s means flushes queue behind the 37 s fill | predicted; the 7.03 GB peak at depth 32 is `docs/guides/ingest.md:1478-1482` |
| stage 2: narrow scan GETs per statement; q20 | 200 to 500 (q2 at exactly 206: 103 x 2); q20 at most 400 GETs and at most 0.5 GB scan wire | outside | predicted |
| stage 2: q37 to q43 | cold sum at most 7 s; `segmentsPrunedByStats` at least 70 of about 100 | outside | run 1 keeps 30 of 407 at about 3 s, comment 6368995087; band predicted |
| stage 2: hot | hot sum 54 to 64 s; q34, q35, q29 hot at most 1.3x run 3's | outside abandons 1M for 500k | run 1 hot 59.3 s comment 6368995087; 1.3x predicted from the 1.28x partition tail |
| stage 2: concurrent | QPS at least 0.60, error ratio at most 0.058, with the per-partition `prefetch_memory_reopens` and `prefetch_revocations` counters (`crates/ravel-sql/src/logs_scan/mod.rs:1424-1434`) stamped per phase | outside abandons 1M for 500k | run 1 QPS 0.663, error ratio 0.020, comment 6368995087; the 0.058 bar is ADR-2023 decision 4, `docs/adrs/2023-loopback-fetch-cache-share-and-independent-catalog-cache.md:100` |
| stage 2: largest fetch run (`peak_fetch_run_bytes`, `log_fetcher.rs:5287`) on q31 and q32 | at or under 64 MiB | larger: the covering split did not engage and the hot re-read would miss the cache | predicted |
| stage 2: disk bytes per statement over the statements above 3 s | at most 0.40 of run 3's position overhead (disk minus wire) plus run 3's wire | the overhead falls by less than half: positions are not the unit this store charges, re-read the probe | run 2's 68.4 GB of overhead, comment 6384701235; band predicted |
| stage 2: cold sum | run 3 minus 95 to 140 s | under 60 s with the object-count and disk stamps passing: the block model overstates the per-object cost, re-derive it from this run before any later step | run 2 value comment 6384701235; band predicted |
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
to q43 are stream-level, so the key is predicted not to move the survivor
count decision 6's run reads (about 30 survivors of about 100 objects,
from run 1's pre-decision-6 figure of 30 of 407); run 8 bands that prediction below
rather than assuming it. ADR-2135's simulator measured a
`(stream, day, user, ts)` sort, UserID alone: minus 13.1% page bytes
(`docs/adrs/2135-rlog-v5-storage-footprint.md:65`), minus 1.8% for the
`observed_ts` reference that v5 already banks (`:71`); the combined row is
11.24 to 9.30 GB, minus 17.3%, at zstd 3 (`:90`, `:92`). This key puts
EventDate ahead of UserID, which is expected to be free against that
figure because the 1-day bucket already leads the sort, so EventDate is
nearly constant inside each bucket and adds no new order; the 13.1% is
carried over on that expectation, and the `data_size` band below is where
it is checked.

What it does not change: no format, no server flag; an uncovered column
is scanned, not pruned. Risk: the ordered `ORDER BY ts LIMIT k` skip (q24,
q25, q27) keeps its answer under any row order (`CHANGELOG.md:1374-1384`)
but loses selectivity, since a block's minimum `ts` no longer rises
monotonically through an object; the cold cost of that is unknown and is
the figure that decides the change. ADR-0815 acceptance 12 forbids a
time-leading key on a measured tenant without a pre-registered A/B
(`docs/adrs/0815-clustered-compaction-and-object-pruning.md:1568-1580`);
this key leads with EventDate, the same discipline applies, and run 6 is
that A/B.

Effect: bytes on every scanning class (0.80 to 0.87 W); q20's scan half
shrinks by block pruning on UserID (a block's UserID `[min, max]` is
narrow for every stream longer than a block, and the NumRange arms
resolved at `log_fetcher.rs:7452-7456` drop most blocks; figure
unknown). Cold sum (model): bytes only, so it moves the requested bytes
and not the positions: 13 to 16% of W at run 2's 30.8 GB of column wire
less decision 3's 7 %, about 14 s, and the q20 pruning on top, figure
unknown.

Pre-registered A/B (run 8, paired with run 7). Stamps: `clustering-key
show` prints the key; one object's footer carries the descriptor.

| figure | band | miss | source |
|---|---|---|---|
| answers | equal run 7 under the Measurement comparison rule | any difference abandons the change at once: it is a correctness question | predicted |
| `data_size` | 10 to 16% under run 7's | outside | 13.1% and 1.8% from `docs/adrs/2135-rlog-v5-storage-footprint.md:65` and `:71`; band predicted |
| q20 scan GETs and wire | below run 7's | not below | predicted |
| q24 + q25 + q27 cold sum | growth under 15 s | at or above: abandon | predicted; run 1's walls for the three are the baseline, comment 6368995087 |
| q37 to q43: `segmentsPrunedByStats` per statement on arm B against arm A, and the seven statements' cold sum | each statement prunes the same count as arm A (run 4's band: at least 70 of about 100), to within 2 objects; the cold sum of the seven at or under arm A's plus 1 s | fewer pruned on any statement, or the sum above arm A's plus 1 s: the key moved the stream-level CounterID stamps and the pruned class regressed, which the cold-sum band below cannot show because the byte saving absorbs it; stop and read the per-statement survivor counts before the byte saving is credited | run 4's stage 2 q37 to q43 band (decision 6), predicted; the per-statement discipline is ADR-0815 acceptance 12, `docs/adrs/0815-clustered-compaction-and-object-pruning.md:1568-1612` |
| cold sum | run 7 minus 10 to 20 s | outside | predicted |
| hot sum | 54 to 64 s | outside | run 1 hot 59.3 s comment 6368995087; band predicted |

Proof: `crates/ravel-ingest/tests/clustering_key_flush.rs` and
`services/ravel-cli/tests/load_clustered.rs` exist; add a q20-shape test
that `UserID = N` over a keyed 30-block object yields fewer candidate
blocks than over the unkeyed twin (mutation: the unkeyed object), and an
answer-equality test for `ORDER BY ts LIMIT 10` on keyed objects.

### 8. The writer assigns column ids in a tenant-declared affinity order (new, 2026-10-24)

Class: writer convention in `ravel-logseg`, plus a tenant setting that
carries the order; no format change. After decision 6, and only after its
own A/B.

Mechanism. The writer assigns each object's dynamic column ids from a
`BTreeSet<(String, u8)>` of the names and types it saw, in both of its
object builders: `build_object` (`crates/ravel-logseg/src/writer.rs:537`,
ids handed out at `:592-602`) and `build_object_columnar` (`:1243`, ids at
`:1260-1270`). `RlogWriter::build` routes between them on whether any
columnar batch was pushed (`:415-419`), both are live (the ingest flush
sends `FlushPayload::Rows` to one and `FlushPayload::Columnar` to the
other, `crates/ravel-ingest/src/log_shard.rs:911-916`), and `build`
promises the two produce the same bytes for the same records
(`writer.rs:397-399`), so the change goes into both sites at once, as
decision 4's does. BLOCKS stores a row group's column chunks in ascending
column id (`docs/log-segment-format.md:472-476`). So a row group's chunks
lie in alphabetical order of column name, and the column names are the SQL
identifiers, so the entry cannot reorder them by editing its mapping.
Every reader maps a name to its id through FIELD_DIR and reads chunk
extents from PAGE_DIR, never assuming an order, so any id assignment is
readable. The writer takes an ordered list of column names from the
tenant's configuration, assigns those ids first in list order, and
assigns the rest alphabetically after them. Co-projected columns then sit
next to each other in every row group, and a statement that projects
them reads one block position where it read several.

The entry declares one ordered list, built from the columns its
statements project together (`queries.sql` co-occurrence, counted
2026-10-24): `WatchID, SearchEngineID, ClientIP, IsRefresh,
ResolutionWidth, SearchPhrase, UserID, EventDate, DontCountHits, URL`.
q33 projects `WatchID, ClientIP, IsRefresh, ResolutionWidth`; q32 the
same plus `SearchPhrase`; q31 `SearchEngineID, ClientIP, IsRefresh,
ResolutionWidth, SearchPhrase`; so all three read one contiguous span
(q32's and q33's spans carry `SearchEngineID`, one small column,
bridged; q31's is contiguous).
`SearchPhrase, UserID` co-occur on 5 statements. `EventDate, IsRefresh,
URL` and `DontCountHits` serve the pruned class q37 to q43 (`DontCountHits`
on q37, q38, q42 and q43; CounterID is the stream attribute), whose
objects are few after pruning, so their order is the least valuable.

What it does not change: no format, no reader code, no answer; a tenant
without the setting keeps today's alphabetical order byte for byte.

Effect (model): on the 16 to 18 multi-column statements, about one
position fewer per object, 103 x 17 x `K` = 1.6 GB (6 s) at `K = 0.9` and
1.4 GB (5 s) at `K = 0.8`; nothing on the one-column statements.

Pre-registered A/B (run 5, paired with run 4, both after decision 6).

| figure | band | miss | source |
|---|---|---|---|
| chunk-run GETs per object on q31, q32, q33 | run 4's minus 1 to 3 | unchanged: the chunks were already within one bridged run, or the order did not reach the objects (read one object's FIELD_DIR) | predicted |
| disk bytes per object on q33 | run 4's minus `K` (0.8 to 0.9 MB) | unchanged within 5 % | predicted |
| cold sum | run 4 minus 3 to 10 s | outside | predicted |
| answers | equal run 4 under the Measurement comparison rule | any difference | predicted |

Proof: a writer test that the declared names receive the lowest dynamic
ids in list order and the rest follow alphabetically (mutation: the
`BTreeSet` order); a fetcher test that a three-column projection of
adjacent declared columns issues one chunk run where the alphabetical
order issued three.

## Consequences

Positive.

- The stock entry's cold sum is predicted at about 120 to 125 s
  (Measurement), from run 2's 364.9 s, with no server flag, no cache tier
  and no format bump; under 200 s after decisions 4, 5 and 6 at either
  `K`.
- Every object a scanning statement opens costs one tail position plus its
  chunk runs after decision 4, on every tenant whose tail fits the probe,
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
- Decision 7's ordered-`LIMIT` cost is unknown until run 8 and can abandon
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
  Rendering the two under `accounting` would let run 3 band on them
  directly instead of inferring the chase and front GETs from GETs per
  object. No band above needs it.
- The store probe (comment 6384802590) read 0.6 to 4 MB per cold request
  across two boxes, with the first request after a cache drop carrying
  the store's own code pages. A cleaner probe (RustFS restarted and the
  page cache dropped before each object, a client that stays warm, 10 KB
  reads at offsets 0, 512 KiB, 1 MiB less 5 KB and a far block, repeated
  on a second fresh object far block first, and the device's
  `read_ahead_kb` and RustFS's erasure block size recorded) tells aligned
  blocks from a per-object first-touch cost and decides whether a probe
  sized to end on a block boundary is a lever: under aligned blocks a
  256 KiB probe straddles a boundary on about a quarter of objects.
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
  group other than ascending column id (the format doc states it,
  `docs/log-segment-format.md:470-476`; decision 8 gets the same adjacency
  without it, by assigning the ids in a declared order); larger blocks (coarsens
  every block-level index 1:1, ADR-0699 rejected alternative 1); an
  object-level string dictionary or one zstd frame per chunk (ADR-2135
  rejected alternatives, `docs/adrs/2135-rlog-v5-storage-footprint.md:345-350`).
  zstd 19 is out on load time (5,498 s against 931 s, one measurement). A
  device read-ahead setting in `install` is neither a flag nor a cache
  tier, but whether the benchmark allows a block-device setting is an
  owner question and it is not proposed.

## Measurement

Run plan. Runs 1 and 2 are done (run 1: 367.2 s, v0.25.1, A box the
v0.24.0 release box at 367.0 s; run 2: the #3219 A/B, 367.9 s against
364.9 s, comment 6384701235). Each later run carries one change and pairs
with the run before it, in the delivery order:

| run | carries | A box | B box |
|---|---|---|---|
| 3 | decision 4 (Ravel, one PR, #3227) | the parent of the first of decision 4's commits as they landed on `main`, stock `load` | the last of decision 4's commits as they landed on `main`, stock `load` |
| 4 | decision 6 (fork PR 1), after the stage 1 load stamp | a `main` commit carrying decisions 4 and 5, stock `load` | the same commit, fork PR 1 |
| 5 | decision 8 (Ravel PR plus a fork line) | the parent of decision 8's first landed commit, fork PR 1 | its last landed commit, fork PR 1 plus the declared order |
| 6 | decisions 1 and 2 (fork PR 2) | one `main` commit, the previous fork head | the same commit, fork PR 2 |
| 7 | decision 3 (fork PR 3) | one `main` commit, fork PR 2 | the same commit, fork PR 3 |
| 8 | decision 7 (fork PR 4) | one `main` commit, fork PR 3 | the same commit, fork PR 4 |


A/B binaries. Every measurement run builds Ravel from source at a `main`
commit through the entry's `RAVEL_REF` path, as the kit runs on #3072
did; only the published ClickBench pin uses a release tag. `main` lands a
pull request through a rebasing merge queue, as one or more rebased
commits and never a merge commit, so a Ravel change's two commits are read
off `main`'s history: B is the last of the pull request's commits as they
landed on `main`, and A is the parent of the first of them (for a
one-commit pull request, A = B^1). #3221 is the worked example: it landed
as a7194f402 then 46df74786, so B = 46df74786 and A = ccac76756, the parent
of a7194f402. Each A/B is two boxes launched together at A and B, so the
delta is the change's own and not the sum of whatever else landed on
`main` between runs; pull requests that landed ahead of the change in the
same queue batch sit on both arms. When the two commits are adjacent, A's
commit is the previous run's B commit; when they are not, the run has one
extra A box at a commit no earlier run measured. Run 3's A is the parent
of decision 4's first landed commit, not run 2's B binary; run 5
(decision 8) is Ravel-side the same way. For a fork-side change (runs 4,
6, 7 and 8) the two boxes share one `main` commit and differ only in the
fork's `load`. A band that reads against the previous run ("run 2's
value", "run 3 minus") reads against the A box launched with it. Every run stamps the burst-credit state: the cold sweep
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

Cumulative cold-sum arithmetic, in delivery order, under the block
model in Context. The anchor is run 2's arm B, 364.9 s, with 102.8 GB of
disk for 34.4 GB of wire over the 32 statements above 3 s, so 68.4 GB of
position overhead (comment 6384701235); every pool below is over those 32
statements, 13,024 object-statements. Decision 4 removes two positions
per object, 2 x 13,024 x `K`; decisions 5 and 6 divide what remains by
3.95 (saving 0.75 of it); decision 8 removes about one position per
object on 17 statements at 103 objects; decisions 1, 2, 3 and 7 move
requested bytes only. Both columns are cumulative and every figure below
the anchors is predicted.

| after | positions per object, narrow one-column | cold sum at `K` = 0.9 MB | cold sum at `K` = 0.8 MB | basis |
|---|---|---|---|---|
| v0.25.1 stock (run 1) | 4.5 | 367.2 (anchor) | 367.2 (anchor) | measured 2026-10-23, comment 6368995087 |
| #3219, PR #3221 (run 2, landed) | 4.5 | 364.9 (anchor) | 364.9 (anchor) | measured 2026-10-24, comment 6384701235 |
| decision 4, directories at the tail | 2.5 | 276 (23.4 GB of overhead removed) | 286 (20.8 GB) | predicted |
| decisions 5 and 6, 100 MB one-group objects | 2 over 103 objects | 150 (33.6 GB, less 2 s of pruned-class cost) | 152 (35.6 GB, less 2 s) | predicted |
| decision 8, columns in affinity order | 2 over 103 (unchanged; the saving is on the 17 multi-column statements) | 144 (1.6 GB) | 147 (1.4 GB) | predicted |
| decisions 1 and 2, bloom scope `text`, fold | unchanged | 143 | 146 | predicted |
| decision 3, zstd 9 | unchanged | 135 (7 % of 30.8 GB) | 138 | predicted |
| decision 7, clustering key | unchanged | 121 (13 % of the rest) | 124 | predicted |

Stated estimate (predicted): about 120 to 125 s, under 200 s once
decisions 4, 5 and 6 land at either `K`. The wire floor, every position
cost gone, is about 105 s after decisions 3 and 7 (run 2's 34.4 GB of
wire less three quarters of the probe bytes and the byte trims, at
262 MB/s). The stretch (under 150 s) needs decision 8 at either `K`
(150 and 152 s after decisions 5 and 6). Run 3 (decision 4) reads `K`
on this box directly from its per-object disk delta, and every later band
is restated against that figure before its run.

Device check after the design: a narrow one-column statement at 103
objects is about 2 positions and 0.3 MB requested per object, about
0.21 GB of disk at `K = 0.9`, 0.8 s against today's 5 s. The levers move the floor
because they remove positions; the requested bytes inside a position the
statement still touches are nearly free on this store.
