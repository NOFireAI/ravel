# Recommendation

Revision `ee2070a0`. Every wall-clock figure below names its backend. None
is an S3 number. The declared variable in every run: fetch policy
`cost-based`, resolved request cost `u64::MAX`, so every log object is read
whole in one GET; no Ravel cache; 8 partitions unless stated.

## Attribute extraction: do it, at the planner, before anything else

Measured (backend `memory`, 40 objects, 200,000 rows, 33 record attributes,
LEDGER run 3): the same question asked through `attrs['attr_3']` and through
a declared `"attr_3"` moves identical bytes over the wire (1,086,226 on the
projection statement, 1,945,646 on the equality statement, both forms) and
differs only in CPU:

| statement | map form cold ms | declared cold ms | map process CPU ms | declared CPU ms | stored page bytes decoded, map / declared |
|---|---|---|---|---|---|
| attr_eq_count | 1070.7 | 4.8 | 3640 | 10 | 84,691 / 3,531 |
| attr_group | 655.1 | 4.4 | 3390 | 20 | 84,691 / 3,531 |
| attr_project_limit | 402.6 | 3.4 | 2890 | 10 | 84,691 / 3,531 |

The split of the map form's cost (partition-summed, overlapping): decode and
row rebuild 566 to 1119 ms, row-batch build with the `Map` column 1706 to
2164 ms, open stall under 3 ms. So the cost is not I/O and not the fetch
policy; it is (a) `all_attrs` selecting every FIELD_DIR column's pages
(24x the page bytes), (b) `merged_attrs` rebuilding a map per row, and
(c) `build_batch` materializing a `Map(Utf8,Utf8)` column that `get_field`
then reads one key from. This is CPU saving only; under the stock policy
there is no byte saving to claim, and the study did not verify the ranged
policy on real objects (BLOCKERS.md).

Semantics are pinned by `crates/ravel-sql/tests/attrs_map_vs_declared.rs`
on the columnar layout and under `attrs_raw` overflow: the two forms agree
on Str values, resource-level fallback, record-over-resource precedence,
absent keys, literal dotted keys, and I64 through `TRY_CAST`; they diverge
by definition when a record holds a non-Str under a Str declaration (map
renders "7", declared reads NULL). Any pushdown of `attrs['k']` must keep
the MAP form's rendering, so it cannot simply alias the declared column;
it must produce the stringified per-key value (record wins, last duplicate
wins, `attrs_raw` included, nested resource values dropped, `record.rs:111-126`
rendering) without building the whole map. WORKITEMS.md item 1 is that
change, bounded to the planner and the scan's column resolution, with the
existing per-key columnar readers as the implementation. No new variant
type, no format change.

Two more measured facts shape the same work:

- A declared range predicate is a prune predicate and takes the planning
  path; on a fixture where no block prunes, `"duration_ms" >= 1000` spent
  1.8x the bytes of the unprunable map form for 1/150 of the CPU (LEDGER
  `dur_sum_threshold`). The planning path's carry keeps only the first
  `plan_concurrency` objects and the scan re-fetches the rest (measured 32
  of 40 at 8 partitions, 24 at 16, 8 at 32; code at `logs_scan.rs:2630-2700`).
  With the server's default RAM cache the re-read is a hit while the cache
  holds the objects; this study ran without a cache and did not measure the
  warm case. WORKITEMS.md item 2.
- An unrelated `attrs_raw` overflow anywhere in a block sends the rest of
  that (partition, segment) to the row path (`logs_scan.rs:3578`). The
  test shows both forms stay correct there; the cost is the same row path
  measured above. Narrowing it to "overflow of a selected key" is
  WORKITEMS.md item 3.

## Bounded read-ahead: do not build it now

Measured (backend `memory+get-delay-20ms`, LEDGER runs 5 and 6): with 40
segments over 8 partitions a fast-path statement's scan critical path is
107 ms of serialized opens plus 2 to 5 ms of decode per partition. Depth-1
read-ahead can only hide decode behind the next open, a ceiling under 5 ms
here, inside the 15 percent noise floor. Raising the partition count to 16
and 32 cut the same stall to 65 and 43 ms with identical requests and
bytes. At a fixed in-flight object budget, depth `d` on `P` partitions and
depth 0 on `P(d+1)` partitions hold the same objects; the second needs no
new accountant. Inferred for S3 (not measured): the reference corpus has
decode comparable to open time, so overlap matters there, but it is the
same overlap the partition count buys. The measurement that decides whether
read-ahead has a distinct role is an S3 sweep of `--sql-partition-count` at
fixed `--store-get-concurrency` (ADR-1195 unbundles them), and it needs
the reference host (BLOCKERS.md).

The prerequisite is independent of the verdict: no fetch byte is reserved
against any budget today (`FetchMemoryExhausted` does not exist in the
tree; `engine.rs:446-452` says peak assembly memory scales with fan-out).
PR #1284 is the accountant; read-ahead of any depth, and any partition
count increase, should wait for it. S5-READAHEAD-CHECKLIST.md is the design
of record if the S3 sweep shows a gap.

## Instrumentation: keep it

S2 adds `Time` metrics beside the existing counters in `BlockMetrics`,
folds them into `SqlStats::scan_timing` through the same plan walk the
block counters use, and surfaces them plus process CPU and peak RSS per run
in `sql_latency_bench`. Overhead measured at 6 and 1 percent on the two
statements above 10 ms (LEDGER B4), inside the noise floor. The injected
GET stall (`--inject-get-delay-ms`) is the device that validated the
boundaries (LEDGER run 2: the stall lands entirely in `open_elapsed`, never
in `decode_build_elapsed`; the plan barrier counts once).

## Limits

- No S3, no MinIO, no reference host: every latency here is in-process or
  injected. Requests and bytes are backend-independent and are the primary
  evidence.
- The dataset is one stream, 33 Str/I64 attributes, one block per object;
  it exercises the mechanism, not the reference corpus's shape.
- B5.3 (decode unchanged under the stall) missed on every statement, 1.8x
  to 2.5x on small absolute figures; recorded, unexplained beyond a cold-core
  hypothesis, and not load-bearing for either verdict.
- `attr_group`'s three-run spread was 0.34, above the floor; no conclusion
  rests on that statement alone.

## Appendix: `check_bands.py` summary

28 misses out of 118 checks; every miss is explained in LEDGER.md under
"Results of runs 3 to 6": 14 are the pre-registration error of classing
three prune-predicate statements as fast path (and the carry-bound
re-fetch they expose), 8 are B5.3, 2 are one-GET resolve-phase differences,
2 are the `dur_sum_threshold` path difference, 2 are B5.4 open_max on the
planning path. No miss contradicts a claim made above.
