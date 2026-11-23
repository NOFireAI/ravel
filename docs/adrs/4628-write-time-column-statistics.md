# ADR-4628: write-time column statistics on the declared-column stamp

Status: Accepted (2026-11-23). Issue #4628, epic #3231. Approved by the
owner on 2026-11-23 with the answers under "Questions decided". Builds on ADR-0873 (the
declared-column stamp on commit records, compaction parts, snapshot entries
and `SegmentRef`), ADR-0850, ADR-0942 and ADR-1413 (the fold-built `.cstat`),
ADR-0825 (exact `i128` integer `avg`) and ADR-4233 (the retained-data
compatibility contract). Depends on issue #4651, a planner change with no
format change, for Q2 and Q29.

Migration classes touched (ADR-0066 decision 4): class C for three additive
fields on `DeclaredColumnMinMax`, the message `CommitRecord` field 20 and
`CompactionPart` field 12 already carry, and the same three fields on its
mirror in `proto/ravel/catalog.proto`, which `SnapshotEntry` field 15
carries inside a class B `.csnap` part. No version is bumped: not the commit
record's `format_version` (`commit-record` family), not the compaction
record's (`compaction-record` family, versions 1 and 2 by content), not the
`.csnap` envelope, not the RLOG trailer. Decision 1 states why the
two-condition rule for an unversioned proto field holds, path by path.

This document and its index row are the whole change. The normative docs
(`docs/catalog-and-mvcc.md`, `docs/ingest.md`, `docs/query-engine.md`, and
for decision 6 `docs/deletion-and-gc.md`, `docs/consistency-model.md`,
`docs/object-store-contract.md` and an ADR-0064 amendment) are amended by
the tasks that land the code, in the same commits.

## Context

### The problem

On an unfolded catalog, an aggregate over a whole declared integer column is
a full scan. The ClickBench entry does not fold as part of its load (owner
decision of 2026-10-25, #3243 comment 6397195913), so a statement that runs
before the loaded hour seals reads an unfolded catalog, and four statements
decode a whole column on every call. Statement numbers below are 0-indexed,
as in the result files; Q1, Q2, Q3 and Q29 are lines 2, 3, 4 and 30 of
ClickBench's `queries.sql`, the numbering #3231 and #4651 use:

- Q1: `COUNT(*) WHERE "AdvEngineID" <> 0`
- Q2: `SUM("AdvEngineID"), COUNT(*), AVG("ResolutionWidth")`
- Q3: `AVG("UserID")`
- Q29: ninety terms of `SUM("ResolutionWidth" + k)`

ClickHouse 26.12.1.207 answers all four without reading rows, from
statistics it writes during the insert with default settings: Q2, Q3 and Q29
from a `_statistics_projection` fed by `statistics.packed`, and Q1 from a
count of the column's default value (#3231 comment 7086477633). On
2026-11-22 the owner reopened the 2026-10-25 ruling for statistics written
at write time, conditional on that measurement.

**Expected effect (predicted, a what-if).** With Q1, Q2, Q3 and Q29 at the
time of line 1 (Q0), the hot geomean ratio against ClickHouse's 2026-11-22
rows goes from 6.60 to 4.71 on c6a.large and from 3.95 to 3.38 on
c6a.4xlarge (#4651, recomputed 2026-11-23 from `ravel/results/20261122` and
`clickhouse/results/20261122` at ClickBench commit `7ab6f8df0`; the ratio is
the geometric mean of (Ravel + 10 ms) / (ClickHouse + 10 ms) over 43
statements, and hot is the lower of tries 2 and 3). This ADR alone does not
reach that figure: Q2 and Q29 also need #4651, and Q3 needs the wide sum of
decision 1.

### Why folding does not stand in for write time

Whether a run reads a folded catalog depends on wall time, not on
configuration: the entry's server runs the scheduled fold every 300 s, and
an hour seals 80 minutes after it ends (#3231 comment 7094007593). A run
that starts inside those 80 minutes reads the loaded hour unfolded. Which
state each published run was in is not recorded (#3231 comment 7105934607).
So a statistic that exists only after a fold cannot be counted on for a
benchmark run, and one written with the data can.

Folding also leaves most of the cost in place (measured 2026-11-23 on
c6a.4xlarge, box r1):

- The per-statement resolve does not get cheaper. Its median over the
  eleven floor statements is 12.61 ms unfolded and 12.43 ms folded, with
  the same 5 LISTs and 1 GET on all 43 statements (comments 7103964581 and
  7105787527).
- Q1 falls from 0.098 s to 0.022 s once folded (comment 7105787527), where
  it is answered from statistics with 0 scans (#4651).
- Q2 and Q3 do not change, from 0.198 to 0.203 s and from 0.156 to 0.167 s
  (comment 7105787527), and Q29 still opens all 222 segments in 0.179 s
  (#4651). Gaps 1 to 3 below give the reasons.

### What exists today

Two carriers of exact per-object statistics for declared typed columns
exist, and they differ in three ways that decide this ADR.

| | The stamp (ADR-0873) | `.cstat` (ADR-0850, 0942, 1413) |
|---|---|---|
| Written by | the ingest flush and the compaction recompute | `Catalog::fold` only |
| Reaches an unfolded catalog | yes: it is on the commit record the resolve already reads | no |
| Basis | the merged attribute view SQL returns | record-level cells only |
| Holds | min, max, null count, for I64 and BOOL | counts, min, max, an `int64` sum, a dictionary of up to 10,000 values |
| Read cost | none beyond the resolve | a load in the plan phase |

The stamp's producers already visit every value at write time:

- The flush folds each buffered write into `DeclaredStatAccum`
  (`crates/ravel-ingest/src/log_declared_stats.rs`), through
  `observe_records` on the row path and `observe_batch` on the columnar
  path, and `log_shard.rs` stamps the result onto the `CommitRecord` beside
  `sample_count`.
- `ravel-cli load` drives the same shard actor through `LogIngestRouter`
  and `write_columnar` (`services/ravel-cli/src/load/logs.rs`), so the bulk
  loader stamps through `observe_batch` with no code of its own.
- The compactor recomputes per output part in its own `DeclaredStatAccum`
  (`crates/ravel-maintain/src/rlog.rs`).

The read side already carries the stamp everywhere a segment is resolved.
`derive_l0` in `crates/ravel-catalog/src/catalog.rs` fills
`SegmentRef.declared_column_stats` for a listed or token-resolved commit
record, the fold copies validated stamps onto `SnapshotEntry` field 15
(`carry_commit_record`, `carry_compaction_part` in
`crates/ravel-catalog/src/declared_stats.rs`), and a rewrite output is
forced empty by both the fold and the live resolver (ADR-0873, force-empty
amendment).

The sum and the dictionary are answered only from `.cstat`, and only behind
a matching stamp: `merged_view_entry` in
`crates/ravel-sql/src/logs_scan/mod.rs` uses an entry only when the
segment's stamp states the same min, max and null count (ADR-0873,
merged-value read amendment). So the stamp is already the authority on what
SQL returns, and `.cstat` is a second source that needs reconciling.

### Three gaps found while reading the code

These shape the decision. All three are also reported under
"Contradictions found" (items 2 and 5).

1. **Q2 is not answerable today, folded or not.** `classify_aggregate` in
   `crates/ravel-sql/src/metadata_agg.rs` declines any aggregate node with
   more than one aggregate expression (the `aggr_exprs.len() != 1` check,
   line 274 on `origin/main` `476b3ad1b`), and
   `logical_aggregate_answerable` matches single-call shapes only. The test
   `sum_plus_literal_beside_a_second_aggregate_scans_exactly`
   (`crates/ravel-sql/tests/logs_metadata_agg.rs`) pins that. Q2 has three
   calls over two columns. Measured on the folded entry tenant, where both
   columns have a `.cstat` sum on all 222 objects, Q2 opens all 222
   segments (0.203 s, #4651; comment 7105787527). Issue #4651 changes the
   rule with no format change, and decision 5 depends on it.
2. **The `.cstat` sum is `optional int64` and is omitted when one object's
   sum leaves `i64`** (`proto/ravel/catalog.proto`, `ColumnStat.sum`). The
   scan computes integer `avg` and `sum` in `i128` (ADR-0825,
   `crates/ravel-sql/src/sum_plus.rs`). `docs/query-engine.md` quotes a
   `"UserID"` value of 435090932899640449; twenty-two rows of that size
   leave `i64`. Real objects do overflow (measured 2026-11-23, #3231
   comment 7105787527): on the entry tenant, folded, all 222 objects carry
   no `.cstat` sum for `UserID`, and none for `WatchID`, `FUniqID` and
   `URLHash`; `RefererHash` lacks it on 221 of 222; every other integer
   column has a sum on all 222. One missing per-object sum declines the
   whole answer (comment 7094007593), so Q3 cannot be answered from
   `.cstat` as stored, and the wide sum of decision 1 is required for Q3.
3. **Q29 is one call by the source, and still scans.**
   `ExactSumPlusLiteral` rewrites the ninety terms to one `exact_sum_count`
   aggregate under a projection, which the rule classifies as `SumCount`;
   `q30_shape_runs_one_aggregate` (`crates/ravel-sql/src/sum_plus.rs`, line
   1123 on `origin/main` `476b3ad1b`) pins one aggregate on a plain table.
   Measured on the folded entry tenant with every `ResolutionWidth` sum
   present, Q29
   still opens all 222 segments (0.179 s, #4651). #4651 attributes that to
   more than one aggregate expression reaching the rule; this ADR did not
   trace the plan the logs table produces. It is a planner matter with no
   format change, and #4651 owns it.

## Decision

### 1. Placement: three additive fields on the stamp, no version bump

The statistics ride the ADR-0873 stamp. `DeclaredColumnMinMax` gains three
fields in `proto/ravel/commit.proto`, and its mirror in
`proto/ravel/catalog.proto` gains the same three with the same numbers.
Fields 1 to 5 are in use in both; 6, 7 and 8 are the lowest free numbers.

```proto
message DeclaredColumnMinMax {
  // fields 1 to 5 unchanged
  // Exact sum of the column's non-null values in this object, when it
  // fits i64. I64 columns only.
  optional sint64 sum = 6;
  // The same sum as 16 bytes, little-endian two's complement, written
  // only when it does not fit i64. Never present beside field 6.
  bytes sum_wide = 7;
  // Rows whose value is non-null and equals the type's zero: 0 for I64,
  // false for BOOL.
  optional uint64 zero_count = 8;
}
```

The three placements compared, for the read on an unfolded catalog:

| | A: new RLOG section | B: the stamp (chosen) | C: sidecar object |
|---|---|---|---|
| Requests per statement, cold | at least one suffix GET per object | none beyond the resolve | one GET per object |
| Bytes read per statement | the footer and one section per object; not measured | the growth of the commit records: at most about 2.2 KB each for the entry tenant (derived, decision 7) | one sidecar per object; not measured |
| Phase that pays | plan | resolve, already paid | plan |
| Meets "exactly zero data GETs" | no: the read is a GET on the data object | yes | zero data GETs, but one sidecar GET per object |
| Write cost | bytes in the object | bytes in the commit record | one more PUT per flush, and a third object in a two-object commit |
| Older reader | skips an unknown section kind (ADR-0029 carve-out, no trailer bump) | ignores unknown fields | never looks |
| Erasure | the rewrite re-encodes the object, so the section is recomputed | rewrite parts are forced empty | a new object class to delete or rewrite (#1848's shape) |
| Reaches a token-resolved segment | yes, per object | yes, on the record `derive_l0` reads | yes, per object |
| Basis | what the writer encoded per column | the merged view | whichever producer writes it |
| Fold reuse | the fold still decodes or fetches per object | the fold copies the entry it already carries | the fold fetches per object |

The entry tenant has 222 l0 objects (#3231 comment 7105787527) holding
8,328,210,617 bytes (comment 7103964581), about 37.5 MB each (derived). A
and C each add at least 222
plan-phase requests to a cold statement that ClickHouse answers from one
row. Their byte cost is not measured; for A it would be read from
`ravel-cli`'s RLOG footprint report on one loaded object. B adds no
request: an unfolded resolve already lists and reads every commit record.
On the entry tenant measured 2026-11-23 that is 222 records per resolve,
one per l0 object (222 unfolded segments resolved on every call, the 222
records served from the record cache, comment 7103964581), so the added
bytes are counted over 222: at most 222 x 2,204 bytes, about 0.49 MB per
resolve (derived from decision 7's per-record bound). The only measured
record bytes come from an earlier load of #3243 (1.0 MB over 409 records
resolved, comment 6392034172, arm A), about 2,445 bytes per record
(derived), and over that load's 409 records the bound is about 0.90 MB
(derived). On either count the bound is up to 2,204 bytes added to about
2,445, so resolve record bytes can come close to doubling on every
statement of the tenant.

A has a second defect. The writer's per-column statistics are not the
merged view: a row whose value comes from its stream's resource or scope
attributes is a null to a per-column tally. ADR-0873's record-fold
amendment moved the stamp off the writer's statistics for exactly that
reason, and its merged-value read amendment retired `.cstat` as an
independent answer for the same reason. A statistic in the object would be
a third carrier with the `.cstat` basis problem.

**Version bump: none.** The rule for an unversioned proto field needs two
things to leave query answers, durability, retention and fencing unchanged.

- *An older reader that ignores the fields.* It sees the stamp it sees
  today. Statements that need a sum or a zero count scan, as they do today.
- *An older writer that rewrites the record without them.* A commit record
  and a compaction record are immutable and are not rewritten in place; a
  stamp reaches a new object only through the paths below (read on
  `origin/main` `476b3ad1b`; decision 3 has the producer side).
  - The fold and a HEAD rebuild, onto `SnapshotEntry` field 15:
    `carry_commit_record` and `carry_compaction_part`
    (`crates/ravel-catalog/src/declared_stats.rs:283`, `:308`) decode the
    stamp to the typed form and re-encode the validated entries. An older
    fold drops fields 6 to 8.
  - Compaction, the migrate primitive (`migrate_bucket_format`,
    `crates/ravel-maintain/src/rewrite.rs:809`) and the force-2 re-encode
    (`reencode_compaction_parts`, `rewrite.rs:1246`), onto `CompactionPart`
    field 12 of a new compaction record: each recomputes the stamp from the
    rows of the part it writes, in the one `DeclaredStatAccum` of
    `crates/ravel-maintain/src/rlog.rs` (`:6504`, stamped at `:8284`),
    through `build_parts` (`:501`; the migrate primitive calls it at
    `rewrite.rs:627`) or `merge_catalogs` (`:1274`; force 2
    passes the predecessor's covered columns, `rewrite.rs:1620`, `:1734`).
    None copies an input stamp, so an older build writes parts without the
    fields.
  - The erasure rewrite, metrics parts and span parts write empty stamps
    (`erasure_rewrite.rs:1595`, `:1746`; `build.rs:1310`;
    `rspan_codec.rs:1038-1044`, all in `crates/ravel-maintain/src`).

  Every drop leaves segments whose statements scan; no path carries a
  field 6 to 8 forward with a changed meaning. No answer changes, and
  nothing durable, retained or fenced depends on the fields.

So `CommitRecord.format_version` stays 1 and the `commit-record` family in
`sys/compat` is untouched. The compaction record's version is set by
whether the record supersedes another (1, or 2 for a force-2 record,
`crates/ravel-commit/src/record.rs:14-20`; `docs/compatibility.md` lists
the `compaction-record` family as versions 1 and 2 "by content"), not by
stamp content, so the new fields leave that family untouched as well.
`SnapshotEntry` is a proto record carried inside
a `.csnap` part and follows the same rule, as field 15 did (ADR-0873
decision 1), so the `.csnap` envelope stays at 1. No new exported
format-version constant exists, so `docs/compatibility.md`'s inventory
gains no row.

The message keeps its name. `DeclaredColumnMinMax` now carries more than a
min and a max; renaming a message changes no wire byte but touches every
user, and is left out.

### 2. What is stored, and when absence means scan

All three figures are folded over the merged attribute view, the basis the
stamp already has: for each row, the record's own attribute when the record
sets the key at any value kind, otherwise the stream's resource or scope
attribute of the same name. A row whose merged value is absent or of
another type is null.

- **Sum.** For an I64 column, the exact sum of the non-null values,
  accumulated in `i128`. It is written to field 6 when it fits `i64` and to
  field 7 otherwise. With zero non-null rows the sum is 0 in field 6. An
  object's row count is a `u64` and each value an `i64`, so the sum cannot
  leave `i128`: (2^64 - 1) x (2^63 - 1) is below `i128::MAX` and
  -(2^64 - 1) x 2^63 is above `i128::MIN`, and the same holds for the
  `non_null * min` and `non_null * max` bounds of the validity clauses. This departs from ADR-0850, which omits a sum that leaves
  `i64`: the scan's own `avg` and `sum` run in `i128`, and a statistic
  narrower than the scan cannot answer what the scan answers. On the entry
  tenant the `.cstat` sum of Q3's column leaves `i64` on all 222 objects
  (gap 2, measured 2026-11-23, #3231 comment 7105787527), so field 7 is
  what makes Q3 answerable at all.
- **Zero count.** For an I64 or BOOL column, the count of non-null rows
  whose value is 0 or false. This is the one value-to-count entry
  ClickHouse's default-value count corresponds to. No dictionary is
  written; see "Rejected alternatives" and question 1.
- **Non-null count.** Not stored. It stays `sample_count - null_count`, as
  ADR-0873 defines it.

A BOOL column carries no sum. STR, BYTES and F64 stay outside the stamp
vocabulary (ADR-0873 decision 2), so they carry none of the three.

**Validity.** `ravel_commit::declared_stats::decode` gains these clauses.
An entry that fails one is dropped whole and counted on the existing drop
metric under its carrier label, as any defective entry is today.

- fields 6 and 7 both present; field 7 present with a length other than 16;
  field 7 holding a value that fits `i64` (one encoding per value);
- a sum on a column that is not I64;
- a sum outside `[non_null * min, non_null * max]`, computed in `i128`, or
  a nonzero sum with zero non-null rows;
- a zero count above the non-null count; a nonzero zero count when 0 (or
  false) lies outside `[min, max]`; a zero count below the non-null count
  when `min == max == 0`.

**Absent means scan.** Each field has its own presence. An entry with no
sum cannot answer `SUM` or `AVG` for that segment; an entry with no zero
count cannot answer a count against the zero. Every record written before
this ADR is in that state forever, and so is every entry an older fold,
compactor, migrate or force-2 re-encode wrote. A reader never treats an
absent field as zero. The existing absences carry over unchanged: no entry
at all (column declared after the flush opened, a stream blob that did not
decode, a rewrite part), and an ineligible type.

### 3. Producers

- **Flush and bulk load** (`ravel-ingest`). `Running` gains an `i128` sum
  and a zero count beside `min`, `max` and `non_null`. The row path adds one
  add and one compare per observed value. The columnar path adds them to
  the loop that already walks every cell for min and max. `fold_fallbacks`
  adds `value * count` and, for a zero value, `count`, per stream. The
  loader needs no change.
- **Compaction** (`ravel-maintain`). The per-part accumulator in `rlog.rs`
  gains the same two figures and recomputes them from the rows the part
  holds. It never merges input stamps, as today.
- **Erasure rewrite.** Unchanged: a rewrite part carries no stamp, so it
  carries none of the new fields.
- **Migrate and the force-2 re-encode.** No change of their own. Both
  build parts through the compactor's accumulator in `rlog.rs` (decision
  1, older-writer paths), so T4's change to that accumulator teaches them
  the fields with compaction. Metrics and span parts keep empty stamps.
  The audit covered `crates/ravel-maintain` and the fold's carry
  functions; a writer outside those that re-encodes a stamp was not looked
  for beyond the stamp-field grep that found these.

### 4. Carriage and the fold

`ravel_types::declared_stats::DeclaredColumnStat` gains an optional `i128`
sum and an optional `u64` zero count, set through added methods so existing
constructors keep compiling. The catalog mirror codec and
`carry_commit_record` and `carry_compaction_part` then carry the fields to
`SnapshotEntry` with no logic of their own, because the fold already copies
the validated typed form. Resolution fills `SegmentRef` from whichever
record it reads, as today.

The fold reuses the write-time figures by copying them. It does not
recompute them and does not read the data object for them.

The fold keeps building `.cstat` exactly as it does now, sums included, and
does not copy a stamp's sum into a `.cstat` entry. The two differ in basis
(merged view against record-level cells) and now in width, so a copy would
make `.cstat` state something its own definition does not. The fold keeps
tallying `.cstat` sums for segments whose stamp carries one (question 5):
segments whose stamp lacks the field, and older readers, have no other
source for a sum.

The record cache charges each entry for the stamp list it keeps
(`docs/catalog-and-mvcc.md`, "Snapshot resolution"). The kept list grows,
so the charge estimate is updated in the same task and its test re-pinned.

Two routes stay empty, as they are for min and max: a rewrite output, and a
`SegmentRef` rebuilt from a Flight `SegmentPin`, whose wire layout does not
carry stamps. A distributed statement therefore scans. That is a coverage
limit, not an answer change, and is left for a follow-up.

### 5. Planner use, and composition with `.cstat`

**One carrier per segment per column.** For each touched segment the reader
resolves a figure from exactly one source:

1. the segment's stamp, when the stamp carries the field; otherwise
2. the `.cstat` entry, when one is loaded and `merged_view_entry` grants it
   (the rule in force today); otherwise
3. nothing, and the whole statement scans.

The two are never added, compared or merged. Each describes all rows of one
immutable object, so a statement sums one figure per segment over the
snapshot's deduplicated segment list. A row cannot be counted from both,
because no code path reads both for one segment. After a fold seals an
hour, the stamp arrives on the `SnapshotEntry` instead of the commit record
and still answers first, so sealing changes which record carried the
figure and nothing else. This is not ADR-0873's retired union: that union
let `.cstat` answer on its own and compared the carriers; here `.cstat`
answers only behind a matching stamp, as it already does, and only for a
segment whose stamp lacks the field.

**The readers.** `LogsScanExec::declared_column_sum` and
`declared_not_equal_count` take the stamp first, per segment, under the
gate they already call (`metadata_rewrite_stats_are_exact`).

- `SUM(col + k)`, `AVG(col)` and `exact_sum_count(col)`: the sum of the
  per-segment sums in `i128` and the sum of the non-null counts, exactly
  the inputs `sum_scalar`, `avg_scalar` and `sum_count_batch` take today.
- `COUNT(*) WHERE col <> lit`: per segment, the count of rows equal to
  `lit` is the zero count when `lit` is the zero, 0 when `lit` lies outside
  `[min, max]`, and the non-null count when `min == max == lit`. Any other
  case has no stamp answer for that segment and falls to rule 2 or 3. The
  result is the sum of `non_null - equal` over the segments.
- `GROUP BY col` counts are not answered from the stamp. They keep the
  `.cstat` dictionary path.
- A filter-free `COUNT(*)`, as in Q2, needs no new field: it is the sum of
  `sample_count` over the touched segments, which every `SegmentRef`
  already carries (`crates/ravel-catalog/src/snapshot.rs:56` on
  `origin/main` `476b3ad1b`). On its own it is answered today by
  DataFusion's `AggregateStatistics` rule from
  `LogsScanExec::statistics_from_inputs`; beside other aggregates, reading
  it is part of #4651.

**Several aggregates in one statement (Q2, and Q29 as measured).** Not
specified here. Today `classify_aggregate` declines a statement with more
than one aggregate expression (`crates/ravel-sql/src/metadata_agg.rs`, line
274 on `origin/main` `476b3ad1b`). Issue #4651 lets the rule answer a
statement whose every aggregate expression the existing
`stats_aggregate` classifies, with `logical_aggregate_answerable` changed
in step, and declines the whole
statement when any aggregate or any touched segment cannot answer. It
needs no format change and does not depend on this ADR. This ADR depends
on it for Q2 and Q29: the readers above supply the per-segment figures,
and #4651 decides which statement shapes reach them.

**No statistics load when the stamps answer.** `logs_column_stats_eligible`
skips the `.cstat` load when every touched segment's stamp carries what the
shape reads. The snapshot is in hand at that point, so the check is a walk
of the segment list with no I/O. On an unfolded catalog there is nothing to
load anyway; on a folded one this keeps a stamp-covered statement from
paying the load #3244 measured.

**Marker.** The rewritten plan is a `MetadataOnlyExec`, as today. The
per-query accounting reports, per carrier, how many segments answered from
the stamp and how many from `.cstat`, so a coverage loss shows as a
counter and not only as a slow statement.

### 6. Exactness: what forces the scan, and where

| Condition | Enforced by |
|---|---|
| A pending erasure on the snapshot | `metadata_rewrite_stats_are_exact` (`self.erasure.is_empty()`) in `logs_scan/mod.rs`, and `logs_column_stats_eligible` in `executor.rs` before any plan is built |
| A stale statistic that could hold an erased row | the pending-erasure gate of the first row. No erasure step recomputes or removes `.cstat`, the commit-record stamps or the `.csnap` stamps, and the old values leave only when the sweep deletes the old objects (`ravel-maintain/src/sweep.rs:1306`). Rewrite outputs carry no stamp: written empty (`ravel-maintain/src/erasure_rewrite.rs:1595`, `:1746`) and forced empty in `ravel-catalog/src/fold.rs:911` (the rewrite snapshot entry) and `ravel-catalog/src/catalog.rs:6215` (the live rewrite `SegmentRef`). Read in #3231 comment 7094007593; line numbers re-checked on `origin/main` `476b3ad1b`. See the paragraph below |
| A pinned commit token admitting a segment the statistics do not cover | coverage is per segment of the snapshot being queried: `derive_l0` stamps a token-resolved record from itself, and the reader loops return `None` at the first segment with no usable figure |
| A total that leaves the result type | `sum_scalar` (`checked_mul`, `checked_add`, then the `i64` conversion) and `count_to_i64` in `metadata_agg.rs` decline; for `exact_sum_count` the projection's own check raises the same `IntegerSumOverflow` on both paths |
| A ts bound that clips a segment, a content predicate, a row filter, a key equality, a prune arm other than `NumNotEq` | `metadata_rewrite_stats_are_exact`, unchanged |
| A defective entry | dropped at decode by the clauses in decision 2, counted, and the segment reads as uncovered |

On the stale statistic: the stamp of a pre-rewrite input, on its commit
record or on a `.csnap` entry, holds figures that include the erased rows,
and no erasure step removes it. It can answer only if that input is still
in the resolved segment list while no erasure is pending. The normative
doc states the mechanism that rules that out:
`docs/deletion-and-gc.md:1366-1368` on `origin/main` `476b3ad1b` says the
request stays live for as long as any held input does, so the filter does
not retire under a snapshot that can still resolve one, and
`docs/deletion-and-gc.md:1389-1398` applies the same condition to a stale
`.cstat` the live HEAD still names. The code sites, from #3231 comment
7094007593 with line numbers re-checked on `476b3ad1b`:

- The live resolver leaves out the inputs a listed rewrite record
  supersedes (`crates/ravel-catalog/src/catalog.rs:3964`).
- Every `.dreq` under `del/` becomes a pending erasure on the resolved
  snapshot (`list_pending_erasure`, `catalog.rs:4656`), and a pending
  erasure refuses the metadata-only answers this ADR's readers give
  (`logs_column_stats_eligible`, `executor.rs:5585`;
  `metadata_rewrite_stats_are_exact`, `logs_scan/mod.rs:2549`; both in
  `crates/ravel-sql/src`).
- The sweep's documentation states that it keeps a `.dreq` until its
  `.done` record is older than the protection horizon and no superseded
  input of its rewrites is still held, for example by the live HEAD
  snapshot (`crates/ravel-maintain/src/sweep.rs:4405-4440`).

The doc and the gates were traced. The code path that keeps the request
live while an input is held (the sweep's rule-2 holds feeding the `.dreq`
decision) was read only as that function's documentation, not traced
through its body. A folded snapshot can name a pre-rewrite input outside
the 26 h reconcile window (`crates/ravel-catalog/src/fold.rs:1381`; #3231
comment 7094007593); by the doc, the live HEAD's reference is a hold, so
the request stays live for that case too. A Flight ticket pinned past the
horizon was not read; a Flight `SegmentPin` carries no stamp (decision 4),
so a statement on that path scans.
`crates/ravel-catalog/tests/token_cache_erasure_4570.rs` asserts that an
erased row is not served on the token paths. The same exposure exists
today for `MIN`, `MAX` and `COUNT(col)` from the stamp and for `.cstat`,
so it is not new, but a sum widens what it would misstate. T5 owes test
6 below, which checks the code against the doc; a failure there is a
defect against `docs/deletion-and-gc.md`, and the readers would then have
to decline for any segment a listed rewrite record supersedes.

On the sum overflow of #3847: a per-object sum can no longer overflow,
because it is kept in `i128`. What can overflow is the statement's total
against its `i64` result type, and that still declines at `sum_scalar`, so
the scan produces the error. #3847's open defect (the scan's per-row
`col + k` wraps before `sum` sees it) is not changed by this ADR. The
`SUM(col + k)` shapes here go through `exact_sum_count`, which does not
evaluate `col + k` per row.

On #1848: the new fields store no attribute value as such, but a sum makes
values recoverable. With one non-null row the sum is that value, and with
two the min and max already state both. With three, the sum minus the min
minus the max is the third value exactly, which no field of today's stamp
states; with more rows a sum and a zero count still narrow what the values
can be. The exposure is wider than those small objects: a retained
pre-rewrite sum, less the sum over the rows the rewrite kept, is the sum
of the erased rows, and with one erased row that is its exact value at any
object size. A rule that withholds the sum below some row count would
therefore not help. This is derived, not run.

How long a stamp is retained (code lines at `origin/main` `4cf6bbb3d`). A
raw commit record is deleted before its data object
(`crates/ravel-maintain/src/sweep.rs:1881-1893`, `:2710-2714`), a
compaction record in the same sweep pass as its parts (`sweep.rs:3525-3526`),
and a `.csnap` part a later fold replaced at the catalog sweep's age gate
(`sweep.rs:4191`). So with no Object Lock, a stamp outlives no bound the
erasure guarantee states: matching records gone from served segments at
`.done`, superseded inputs swept by `.done` plus the protection horizon
plus two full sweeps (`docs/consistency-model.md:902-903`). Under scoped
per-object retention on the commit-record or catalog families
(`docs/object-store-contract.md:1680-1682`), data under `l0/` and `l1/`
is not locked, and a locked record keeps its stamp until its retain-until,
as it keeps the min and max today. Under a bucket-default lock the data is
locked as well and the stamp adds nothing.

Those two locked cases differ in what the documents state. A locked
commit record is inside the stated guarantee: its physical-removal bound
is `max(bound + E_v, R)` (`docs/consistency-model.md:903`,
`docs/deletion-and-gc.md:1204-1213`), and its stamp goes with it. A
locked `.csnap` part is not: contract point 4's pattern
`t/*/catalog/*/*` reaches it, and no document states a bound for its
stamp, because they describe snapshot entries as holding no value
(`docs/consistency-model.md:902`, `docs/deletion-and-gc.md:1342-1343`;
"Contradictions found", item 6). The stated catalog-family bound covers
`.cstat` only. So today's min and max on a locked `.csnap` part already
have no stated bound, and the documentation condition below has to state
one.

Decided 2026-11-23 (question 6, with 6a and 6b): the sum and the zero
count are emitted, and #1848 does not gate this ADR, on three conditions.
A test pins the lifetimes above (test plan, item 11).
`docs/deletion-and-gc.md`, `docs/consistency-model.md` and
`docs/object-store-contract.md` state what a retained stamp reveals, in
the same commit as the producers. They state that the guarantee covers
numeric values and that the catalog-keyspace R modifier applies to a
tenant with any declared I64 or BOOL column; ADR-0064 gains an amendment,
as it did for .kidx. And #1848's text is widened to name the stamps.
Widening #1848's work first would shorten none of these lifetimes.
Question 6a reads the guarantee as covering every value of an erased
record, numeric included; question 6b adds the upgrade note and the
producer opt-out of decision 8. See "Contradictions found", items 3, 6
and 7.

Not established here, and needed by a reader of the conditions:

- Whether any deployment with erasure obligations locks `t/*/*/c/*` or the
  catalog family past its erasure SLA. That fact decides whether the
  opt-out can be dropped (question 6b); until the owner confirms it, the
  opt-out stays.
- Whether the bound the documents state for a locked `.csnap` stamp equals
  the contract's `.cstat` formula
  (`max(max(T_f, T_w + protection_horizon) + S + E_v, R)`,
  `docs/object-store-contract.md`, "A lock on the catalog family"). The
  sweep code for `.csnap` parts was not read for this; the task that
  writes the documents does.

**Measured 2026-11-23.** Q3 could not have been answered from the `int64`
sum of `.cstat`: no folded object of the entry tenant carries a `UserID`
sum (222 of 222 without, #3231 comment 7105787527; gap 2). After a load
with this change, the count of `UserID` stamp entries carrying field 7 is
the A/B's check of the same fact on the stamp (decision 7).

### 7. Cost

Nothing here was measured. Derived figures follow from the proto encoding
and are marked derived.

- **Bytes per object (derived).** Per I64 column: field 6 costs 2 to 11
  bytes or field 7 costs 18, and field 8 costs 2 to 11. Per BOOL column:
  field 8 only. The upper bound is 29 bytes per I64 column and 11 per BOOL
  column per commit record, and the same again on the `SnapshotEntry` once
  sealed. Of the entry tenant's 104 declared columns, 76 are I64, 28 are
  STR and none is BOOL (`benchmarks/clickbench/hits.mapping.toml` on
  `origin/main` `476b3ad1b`, the mapping `typed-attr-column set
  --from-mapping` declares from), so the upper bound is 76 x 29 = 2,204
  bytes, about 2.2 KB per record; today's records are about 2.5 KB each
  (1.0 MB over 409 records, #3243 comment 6392034172). Nothing is added to
  the data object.
- **Flush and load CPU.** One `i128` add and one compare per observed
  declared value, in loops that already run. Not measured. The measurement
  is `ravel-bench`'s ingest stage timing (`--features stage-timing`) before
  and after, and the load wall in the A/B below. No existing stage isolates
  the declared-stat fold (`FlushPhase::CommitBuild` covers only the stamp
  build at flush), so T3 adds one or reports the enclosing stage.
- **Read requests.** Zero added in every phase. Resolve issues the same
  LISTs and reads the same records; plan issues none for a stamp-covered
  statement; scan and decode issue none.
- **Read bytes.** Resolve record bytes grow by the per-record figure above
  on every statement of the tenant, not only the four targets. These are
  record-cache bytes after the first resolve, and wire bytes of the commit
  record GETs on a cold one. The record cache's per-entry charge grows with
  the kept stamp list; the size of that growth is a figure the task reads
  from the existing charge test, not one this ADR states.
- **The A/B.** One same-box A/B on c6a.4xlarge, main against the branch,
  pre-registered on #4628 before launch. The table gives the figures that
  need a band and the ones already fixed by the mechanism.

| Figure | Bar | Basis |
|---|---|---|
| Q1, Q2, Q3, Q29: plan node | `MetadataOnlyExec`, no `LogsScanExec` | predicted |
| Q1, Q2, Q3, Q29: data GETs, cold and hot | exactly 0 | predicted |
| Q1, Q2, Q3, Q29: decompressed bytes | exactly 0 | predicted |
| Q1, Q2, Q3, Q29: plan-phase GETs | exactly 0 | predicted |
| Q1, Q2, Q3, Q29: segments answered from the stamp | equal to segments touched | predicted |
| Q2, Q3, Q29 hot wall today | 0.14 to 0.20 s | measured 2026-11-22; the 2026-11-23 box r1 run measured 0.156 to 0.203 s (#3231 comment 7105787527, #4651) |
| `UserID` stamp entries carrying field 7 | 222 of 222 | predicted, from the `.cstat` census of #3231 comment 7105787527 |
| Resolve record bytes per statement, every statement | at most arm A plus 2.2 KB per record | predicted |
| Answers, all 43 statements | equal to arm A | predicted |

Bands for the four statements' walls, the hot total, the cold total, the
load wall and `resolveMs` on a non-target statement are set in the
pre-registration comment, not here.

### 8. Rollout and compatibility

- **Regime.** Upgrade safety covers v0.33 onward. This change adds no
  format version, so nothing is activated through `sys/compat` and no
  certificate is involved. Installing the release starts the stamping,
  unless the opt-out below is set.
- **Older query reader.** Ignores the fields and scans. Same answers.
- **Older fold, newer ingest.** The fold seals an hour with entries that
  lack the fields. Sealed parts are carried forward by reference, so those
  hours stay without them until the records are recompacted or a rebuild
  fold re-reads commit records that still exist. Until then those segments
  fall to `.cstat` behind a matching stamp, or scan.
- **Older compactor, migrate or force-2 re-encode, newer ingest.** The L1
  part lacks the fields, and the L0 records or the record it replaces are
  swept. Same consequence.
- **Order.** Upgrade fold and maintain processes before ingest, the order
  ADR-0873 decision 6 already requires for the stamp. The per-carrier
  coverage counter of decision 5 is how a wrong order shows up. Emission
  is not gated on a switch (question 4).
- **Opt-out.** One producer process setting withholds fields 6 to 8;
  default emit (question 6b). It is a process setting, not a persisted
  record. With it set, the flush, the bulk loader and the compactor write
  stamps without the fields, so those segments scan, as for any record
  written before this ADR. Whether the loader and the compactor can share
  one setting was not established (code not read). The release that first
  emits carries an upgrade note naming the operator's levers: `R` on
  `t/*/*/c/*` inside the erasure SLA, and a catalog lock scoped to
  `catalog/<signal>/HEAD` (`docs/deletion-and-gc.md:1204-1213`,
  `docs/object-store-contract.md:1859-1861`).
- **Rollback.** An older binary reads the new records and ignores the
  fields. Nothing has to be undone.
- **Existing data.** Class C records never gain the fields. A tenant
  converges for new flushes and new compaction outputs only. The ClickBench
  entry is a fresh load, so every object is covered from the first
  statement.
- **Dual-reader question** (ADR-0066 decision 1). There is no second wire
  reader. The absent-field branch is the reader for every older record and
  is permanent.
- **Checksum coverage.** The fields sit inside the commit record and the
  `.csnap` part, under the checks that cover every other byte of each.
- **Inspectors.** `ravel-cli`'s commit-record and catalog inspectors print
  the sum and zero count where they print a stamp.

## Test plan

Plan-shape tests, not timings, in `crates/ravel-sql/tests/`, over objects
written by the real writer and stamped by the real fold of records.

Acceptance, for each of the four statements: an exact answer with zero
data-block GETs on freshly written, unfolded data (tests 1 and 9), and an
answer equal to a forced scan of the same objects in three cases: old
formats, objects whose stamps lack the new fields (test 1); missing
coverage, one segment without the statistic (test 4); and a pending
erasure (test 2). Q2 and Q29 meet it once #4651 has landed.

1. For each of Q1, Q2, Q3 and Q29 in its ClickBench form (Q2 and Q29 after
   #4651), on a snapshot resolved from commit records with no fold: the
   plan holds `MetadataOnlyExec` and no `LogsScanExec`, and a counting
   store reports exactly zero data GETs. Paired with the same statement on
   the same objects with the fields stripped from the stamps: the plan
   scans and the answer is byte-identical.
2. A pending erasure forces the scan for each shape, asserted on the plan
   and on a nonzero data GET count, and the answer equals a forced scan of
   the same objects under the same erasure.
3. A total that leaves `i64` forces the scan: `SUM(col)` over objects whose
   stamp sums add past `i64::MAX`: the rewrite declines and the scan raises
   `IntegerSumOverflow`. A second case has one object whose own sum
   leaves `i64` (field 7), and `AVG(col)` answers from the stamp with the
   scan's exact value.
4. A token-resolved snapshot holding one segment without the fields forces
   the scan, with an answer equal to a forced scan, and the same snapshot
   with the fields answers from the stamp with the same answer.
5. Composition: a folded snapshot where some segments carry the fields and
   others have only a reconciled `.cstat` entry answers from statistics,
   equals the scan, and reports the per-carrier split exactly. A segment
   with both is counted once, shown by an answer that differs if either
   carrier is added twice.
6. A rewrite part carries none of the fields, and a statement over it
   scans. After the erasure completes, with a sealed part and a warm record
   cache that still name the pre-rewrite input, no statement answers from
   that input's stamp: the request is still live, as
   `docs/deletion-and-gc.md:1366-1368` states, and the answer equals a
   forced scan.
7. Codec: property tests for encode and decode of the three fields in both
   proto mirrors, and one corrupt-input case per validity clause of
   decision 2, each producing a counted drop and never a panic. The
   regression seed files are checked in.
8. Producers: the row path and the columnar path stamp the same sum and
   zero count for the same rows, including a stream-level fallback value, a
   record override, a wrong-typed value and a zero value; the compaction
   recompute matches a fold of the part's rows.
9. Loader: `ravel-cli load` of a small file, then the four statements on
   the unfolded tenant, each metadata-only with zero data GETs.
10. Mixed version: a stamp with the fields carried through a fold that
    strips them is reported by the coverage counter.
11. Stamp lifetimes (question 6): after an erasure rewrite and a
    superseded sweep past the horizon, no raw commit record or compaction
    record of a superseded input remains under `c/`; after the catalog
    sweep's age gate, no replaced `.csnap` part remains.
    `crates/ravel-maintain/tests/erasure_sweep.rs` already asserts the
    compaction record and its parts are gone; the raw commit record case
    is the new assertion.
12. Opt-out (decision 8, question 6b): with the opt-out set, so emission
    is off, no producer writes fields 6 to 8, and the four statements scan
    with answers equal to those over the same rows with emission on.

Then the A/B of decision 7.

## Rejected alternatives

- **A new RLOG section (placement A).** One request per object per cold
  statement, a GET on the data object where the acceptance asks for none,
  and a per-column basis that is not the merged view. It needs no trailer
  bump, which is its one advantage.
- **A sidecar object per data object (placement C).** One request per
  object per cold statement, one more PUT per flush, a third object in a
  two-object commit protocol with its own orphan and sweep rules, and a new
  class of object for erasure to reach.
- **Running the fold inside the load.** Ruled out by the owner on
  2026-10-25 (#3243 comment 6397195913), and measured at 291 s of fold on an
  858 s load (#3243 comment 6392034172).
- **A value-to-count dictionary on the stamp.** It would answer `<>`
  against any literal and `GROUP BY` counts. The commit record is read by
  every statement of an unfolded tenant and kept in a record cache with a
  byte budget; the fold-time statistics for this tenant, dictionaries
  included, are 72 MB (#3244).
  A dictionary also puts attribute values on an immutable record, and the
  owner's ruling named the dictionary as a pre-aggregated index. The
  grouped statement (Q7) is a scan in ClickHouse too. A small ceiling would
  bound the size but not the other two points.
- **An `int64` sum omitted on overflow, as `.cstat` has.** A statement
  whose objects overflow would scan with no way to fix it short of smaller
  objects, while the scan itself computes in `i128`. On the entry tenant
  that is Q3 on every object (222 of 222, measured 2026-11-23, #3231
  comment 7105787527).
- **One 16-byte sum field always.** Simpler to validate, but 18 bytes per
  I64 column per record for sums that mostly fit two to five.
- **Copying the stamp's sum into `.cstat` at fold time.** It would make a
  record-level statistic hold a merged-view figure.
- **A version bump of the commit record.** The rule does not require one,
  and an immutable record's older versions never converge, so a bump would
  put a second commit-record version in every reader forever.
- **Recomputing statistics for an erasure rewrite part.** ADR-0873 rejected
  it for min and max; the same reasoning holds.

## Consequences

- On an unfolded catalog, the four statements answer from the commit
  records the resolve already holds, with no data GET, for every tenant
  with declared I64 or BOOL columns, not only the benchmark entry. Q2 and
  Q29 do so once #4651 has landed.
- `SUM` and `AVG` over a declared integer column, and a count against the
  zero, are answered for the live tail and for token-resolved segments,
  which no fold-built object reaches.
- Every commit record of a tenant with declared I64 or BOOL columns grows,
  and every statement's resolve reads the larger records.
- A segment whose stamp lacks the fields keeps today's behaviour.
- `.cstat` stays as it is. For a stamp-covered segment its sum is no longer
  read.
- Distributed statements and erasure-rewritten buckets scan, as they do for
  min and max.
- A record under Object Lock keeps its sum and zero count until its
  retain-until. A deployment that cannot accept that sets the opt-out of
  decision 8; whether any deployment needs it is not established
  (decision 6).

## Questions decided

No question is open. The owner approved this ADR on 2026-11-23 with the
answers below.

1. **Value counts: the zero count only, or a bounded dictionary?**
   Decided 2026-11-23, by the owner's delegation: the zero count only. The
   ruling of 2026-10-25 called a dictionary a pre-aggregated index,
   ClickHouse answers Q1 from a default-value count and scans the grouped
   statement, and a dictionary would put attribute values verbatim on a
   record that can be locked. Sources: #3243 comment 6397195913; #3231
   comment 7086477633.
2. **Does a sum and a zero count stamped during the load meet the
   ClickBench rules as the reopening intended?** Decided 2026-11-23, by
   the owner's delegation: yes, while the fields are written by default
   for every tenant with declared columns and the entry sets nothing to
   get them. ClickBench's README at `7ab6f8df0` says "if a database creates
   additional indexes automatically, it is considered ok" (line 152) and
   "It's better to use the default settings and avoid fine-tuning" (line
   106). The sum matches what the ClickHouse entry does since ClickBench
   #2047 (its description: "Auto statistics are ok now") and ClickHouse
   #123950; the zero count matches the default-value count ClickHouse
   records at insert, and is of the type's zero, not a statement's
   literal, which keeps it clear of the README's rule against
   pre-aggregated indices (line 156). Sources: #3231 comment 7085989852,
   owner decision 1 ("group A", which includes Q1).
3. Answered 2026-11-23: the sum width question (`i128` in two fields, or
   `int64` omitted on overflow) is settled by the `.cstat` census of #3231
   comment 7105787527, where no object carries a `UserID` sum, so Q3 needs
   the `i128` field. An independent review proposes the same answer.
4. **Rollout: deployment order plus the coverage counter, or an explicit
   switch that starts emission?** Decided 2026-11-23, by the owner's
   delegation: deployment order plus the coverage counter. A wrong order
   costs coverage and never an answer, and a switch that defaults off
   would make the entry set a non-default setting, against question 2. The
   opt-out of 6b is a separate setting and emits by default. Sources:
   ADR-0873 decision 6; `docs/compatibility.md:119`.
5. **Should the fold stop tallying `.cstat` sums for segments whose stamp
   carries one?** Decided 2026-11-23, by the owner's delegation: no.
   Segments whose stamp lacks the field, and older readers, have no other
   source for a sum (decision 5 rule 2, decision 8). Stopping the tally
   needs its own issue with the fold cost measured.
6. Decided 2026-11-23, by the owner's delegation: sums may sit on these
   records, and #1848 does not gate this ADR. A raw commit record is
   deleted before its data object, a compaction record in the same sweep
   pass as its parts, and a replaced `.csnap` part at the catalog sweep's
   age gate, so with no Object Lock a sum outlives no bound the erasure
   guarantee states; under scoped retention on commit records or the
   catalog family the locked version keeps the sum and zero count until
   its retain-until, as it keeps the min and max today. For a commit
   record that retain-until is inside the stated `R` bound; for a `.csnap`
   part no bound is stated (decision 6). The conditions are in decision 6,
   "On #1848". Its two sub-points were decided the same day:
   - **6a. Does the erasure guarantee cover an erased record's numeric
     values, or only its records and the matched identifier?** Decided
     2026-11-23, by the owner's delegation, conservatively: the guarantee
     covers every label and attribute value of an erased record, numeric
     included, and any figure one can be recovered from. The texts
     disagree, so the wider reading is taken: ADR-0064's `.kidx` amendment
     counts "an `I64` value verbatim" as a subject value
     (`docs/adrs/0064-selective-subject-erasure.md:925-933`), while its
     matcher text says "a non-string value never matches" (`:984-985`) and
     `docs/deletion-and-gc.md:1342-1343` exempts I64 and BOOL extrema. The
     guarantee itself is stated at `docs/consistency-model.md:902-903`.
     Question 6 stands, with wider conditions: a locked commit record's
     stamp sits inside the stated `R` bound, and a locked `.csnap` part's
     stamp has no stated bound, as its min and max have had none, so the
     documentation condition must state one. The earlier decision on
     question 6 is not reversed, since gating on #1848 shortens no
     lifetime. The narrower reading returns only on an owner statement
     that the guarantee covers the matched identifier alone.
   - **6b. Emission is one-way under a lock: accept it?** A sum written to
     a locked record cannot be removed before its retain-until. Decided
     2026-11-23, by the owner's delegation, conservatively: accepted. Ravel
     promises no removal of a stamp field from a locked version before its
     retain-until. The release that first emits carries an upgrade note
     naming the operator's levers: `R` on `t/*/*/c/*` inside the erasure
     SLA, and a catalog lock scoped to `catalog/<signal>/HEAD`. The
     producers take one process setting, not a persisted record, that
     withholds fields 6 to 8 and emits by default (decision 8). It may be
     dropped once the owner confirms no deployment with erasure obligations
     holds such retention longer than its erasure SLA. Commit-record
     retention is a documented posture, not a hypothetical: contract point
     4 names `t/*/*/c/*` (`docs/object-store-contract.md:1680-1682`), and
     `docs/deletion-and-gc.md:1183` says "An operator with erasure
     obligations must budget them deliberately". Sources:
     `docs/object-store-contract.md:1680-1682`, `:1859-1861`;
     `docs/deletion-and-gc.md:1183`, `:1204-1213`.
7. **The A/B box.** Decided 2026-11-23, by the owner's delegation: covered
   by the approval of 2026-11-22 (#3231 comment 7085989852, owner decision
   2). One c6a.4xlarge, one same-box run, pre-registered on #4628,
   self-terminating, confirmed terminated in the readout.
8. Withdrawn 2026-11-23: whether a pre-rewrite input can be resolved after
   an erasure completes is answered by `docs/deletion-and-gc.md:1366-1368`
   (the request stays live while any held input does); test 6 checks the
   code against it, and a failure there is a defect, not a design choice.

## Contradictions found, reported and not fixed

1. **Statement labels.** ADR-0850, #861 and the module doc of
   `crates/ravel-sql/src/metadata_agg.rs` use 1-indexed labels and attach
   `SUM(col + k)` to q03 and q04 and `AVG(col)` to q30. In the 1-indexed
   file, q03 is the three-aggregate statement, q04 is `AVG("UserID")` and
   q30 is the ninety sums. #4628 and the result files are 0-indexed.
2. **ADR-0850 and #861 say q03 and q04 answer from the fold-time sum.** By
   the code, the three-aggregate statement is declined in every case (gap 1
   above), and `AVG("UserID")` needs every object's sum to fit `i64` (gap
   2). Measured 2026-11-23 on the folded entry tenant, neither is answered
   from statistics: the three-aggregate statement opens all 222 segments
   (#4651), and no object carries a `UserID` sum (#3231 comment
   7105787527).
3. **#1848 names `.cstat` only.** The stamp has put exact I64 and BOOL
   extrema on commit records and snapshot entries since ADR-0873. A
   superseded commit record, and a `.csnap` part a later fold replaced,
   hold the pre-erasure extrema until a sweep removes them, the same shape
   #1848 describes. This ADR adds a sum and a zero count to those objects.
   No STR or BYTES value is involved, but with the min and max a sum
   recovers a third I64 value exactly (decision 6, "On #1848"; question
   6).
4. **#4628 asks for a fallback "when a sum overflowed `i64`".** Under this
   ADR a per-object sum does not overflow; the fallback applies to the
   statement's total. The acceptance test is written against the total.
5. **Why Q29 scans is not established.** #4651 first said the `sum_plus`
   rewrite turns Q29 into more than one aggregate expression;
   `q30_shape_runs_one_aggregate` (`crates/ravel-sql/src/sum_plus.rs`)
   pins one aggregate on a plain table, and the issue was corrected on
   2026-11-23 to say the cause is open. The measured scan is not in
   question; its cause on the logs table is, and #4651 owns it (gap 3).
6. **Three documents describe the stamp as holding less than it does.**
   `docs/deletion-and-gc.md:1342-1343` says a snapshot entry's stamp is
   restricted to I64 and BOOL extrema; `docs/object-store-contract.md:1792`
   says snapshot entries hold no attribute value; and ADR-0064's
   2026-10-15 amendment treats a verbatim I64 in `.kidx` as a subject
   value while the stamp passage does not. An extremum is a value. These
   are the sentences the documentation condition of decision 6 has to
   correct.
7. **`.cstat` integer statistics are scoped out of the guarantee.** Under
   the reading question 6a takes, a `.cstat` I64 min, max and sum are
   covered too, while `docs/consistency-model.md:902-903` and the
   contract's "A lock on the catalog family" scope the `.cstat` exposure
   to a tenant with a `STR` or `BYTES` typed column. Outside this ADR's
   scope.
8. **Advice attributed to the contract that the contract does not give.**
   `docs/deletion-and-gc.md:1200-1202` says the object store contract's
   "Required bucket configuration" advises operators with erasure
   obligations "to prefer scoped legal holds over blanket default
   retention". `docs/object-store-contract.md` on `origin/main`
   `f97267a76` has no such sentence: it never mentions a legal hold.
   Outside this ADR's scope.

## Task decomposition

In dependency order. Each is one crate or a small disjoint file set and is
sized for one executor in under four hours. Every spec declares its
`gate_tier:` and `disk_floor_gb:` lines.

- **T1, vocabulary and wire.** `proto/ravel/commit.proto`,
  `proto/ravel/catalog.proto`, `crates/ravel-types/src/declared_stats.rs`,
  `crates/ravel-commit/src/declared_stats.rs`. Fields 6 to 8, the typed
  form's added methods, encode, decode, the validity clauses, property and
  corrupt-input tests, and the commit-record passage of
  `docs/catalog-and-mvcc.md`. Checks ravel-catalog's
  `sys_proto_format_version_classification` test still classifies the
  records. The generated structs are built with exhaustive literals, so
  this task also adds the three fields, absent, to every literal of the
  two messages: up to 29 sites in 8 files across `ravel-commit`,
  `ravel-catalog` and
  `crates/ravel-sql/tests/logs_declared_minmax_from_stamps.rs`. That
  edit is mechanical and keeps every later task compiling alone.
- **T2, catalog carriage.** `crates/ravel-catalog`: the mirror codec, the
  fold's carry functions, the record-cache charge and its test,
  `tests/declared_stats_carriage.rs`, and the snapshot-entry and
  `SegmentRef` passages of `docs/catalog-and-mvcc.md`. After T1.
- **T3, flush and load producers.** `crates/ravel-ingest`: `Running`, both
  observe paths, `fold_fallbacks`, the opt-out setting (decision 8) with
  test 12 for these producers, `tests/declared_column_stamps.rs`,
  `docs/ingest.md`. As the first task that emits, it also carries the
  erasure documents of decision 6's second condition
  (`docs/deletion-and-gc.md`, `docs/consistency-model.md`,
  `docs/object-store-contract.md`), the ADR-0064 amendment and the upgrade
  note. After T1, beside T2.
- **T4, compaction producer.** `crates/ravel-maintain`: the per-part
  accumulator, the opt-out setting with test 12 for the compactor, test
  11, a test that a rewrite part stays empty, and a test that a migrate
  output and a force-2 re-encode carry the fields, since both use the same
  accumulator (decision 1). After T1, beside T2; merges after T3, which
  lands the erasure documents.
- **T5, stamp-first readers.** `crates/ravel-sql`: the sum and not-equal
  readers, the load gate in the executor, the per-carrier counter, tests 1
  (Q1 and Q3), 2, 3, 4, 5 and 6, `docs/query-engine.md`. After T2.
- **T6, several aggregates in one statement.** Not a task of this ADR:
  issue #4651, which needs no format change and can land before T1. It
  carries the rule's multi-aggregate shape, its logical mirror and the
  re-pin of `sum_plus_literal_beside_a_second_aggregate_scans_exactly`.
  Test 1 for Q2 and Q29 lands with whichever of T5 and #4651 is second.
- **T7, inspectors and the loader test.** `services/ravel-cli`: print the
  fields, test 9, and test 12's four statements over a load with the
  opt-out set. After T3, T5 and #4651.
- **T8, the A/B.** Pre-registration on #4628, one run, readout. After T7,
  on the box approved 2026-11-22 (question 7).

Refs: #4628, #3231, #4651, #850, #861, #873, #3243, #3244, #1848, #3847
