# ADR-4628: write-time column statistics on the declared-column stamp

Status: Proposed. Issue #4628, epic #3231. Builds on ADR-0873 (the
declared-column stamp on commit records, compaction parts, snapshot entries
and `SegmentRef`), ADR-0850, ADR-0942 and ADR-1413 (the fold-built `.cstat`),
ADR-0825 (exact `i128` integer `avg`) and ADR-4233 (the retained-data
compatibility contract).

Migration classes touched (ADR-0066 decision 4): class C for three additive
fields on `DeclaredColumnMinMax`, the message `CommitRecord` field 20 and
`CompactionPart` field 12 already carry, and the same three fields on its
mirror in `proto/ravel/catalog.proto`, which `SnapshotEntry` field 15
carries inside a class B `.csnap` part. No version is bumped: not the commit
record's `format_version`, not the `.csnap` envelope, not the RLOG trailer.
Decision 1 states why the two-condition rule for an unversioned proto field
holds.

This document and its index row are the whole change. The normative docs
(`docs/catalog-and-mvcc.md`, `docs/ingest.md`, `docs/query-engine.md`) are
amended by the tasks that land the code, in the same commits.

## Context

### The problem

On an unfolded catalog, an aggregate over a whole declared integer column is
a full scan. The ClickBench entry queries an unfolded catalog (owner
decision of 2026-10-25, #3243 comment 6397195913), so four statements decode
a whole column on every call. Statement numbers below are 0-indexed, as in
the result files:

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

These shape the decision. The first two are also reported under
"Contradictions found".

1. **Q2 is not answerable today, folded or not.** `classify_aggregate` in
   `crates/ravel-sql/src/metadata_agg.rs` declines any aggregate node with
   more than one call, and `logical_aggregate_answerable` matches
   single-call shapes only. The test
   `sum_plus_literal_beside_a_second_aggregate_scans_exactly`
   (`crates/ravel-sql/tests/logs_metadata_agg.rs`) pins that. Q2 has three
   calls over two columns.
2. **The `.cstat` sum is `optional int64` and is omitted when one object's
   sum leaves `i64`** (`proto/ravel/catalog.proto`, `ColumnStat.sum`). The
   scan computes integer `avg` and `sum` in `i128` (ADR-0825,
   `crates/ravel-sql/src/sum_plus.rs`). `docs/query-engine.md` quotes a
   `"UserID"` value of 435090932899640449; twenty-two rows of that size
   leave `i64`. Whether real objects of the entry tenant overflow is
   inferred, not measured, and decision 6 names the measurement.
3. **Q29 is already one call.** `ExactSumPlusLiteral` rewrites the ninety
   terms to one `exact_sum_count` aggregate under a projection, which the
   rule classifies as `SumCount`.

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
| Bytes read per statement | the footer and one section per object; not measured | the growth of the commit records: at most about 3.0 KB each (derived, decision 7) | one sidecar per object; not measured |
| Phase that pays | plan | resolve, already paid | plan |
| Meets "exactly zero data GETs" | no: the read is a GET on the data object | yes | zero data GETs, but one sidecar GET per object |
| Write cost | bytes in the object | bytes in the commit record | one more PUT per flush, and a third object in a two-object commit |
| Older reader | skips an unknown section kind (ADR-0029 carve-out, no trailer bump) | ignores unknown fields | never looks |
| Erasure | the rewrite re-encodes the object, so the section is recomputed | rewrite parts are forced empty | a new object class to delete or rewrite (#1848's shape) |
| Reaches a token-resolved segment | yes, per object | yes, on the record `derive_l0` reads | yes, per object |
| Basis | what the writer encoded per column | the merged view | whichever producer writes it |
| Fold reuse | the fold still decodes or fetches per object | the fold copies the entry it already carries | the fold fetches per object |

The entry tenant has 222 objects of about 37 MB each (the figure given in
the task for this ADR, not re-measured here). A and C each add at least 222
plan-phase requests to a cold statement that ClickHouse answers from one
row. Their byte cost is not measured; for A it would be read from
`ravel-cli`'s RLOG footprint report on one loaded object. B adds no
request: an unfolded resolve already lists and reads every commit record (5
LISTs and 1.0 MB of records served from the record cache with 409 records
resolved, #3243 comment 6392034172, arm A). For 222 records B's upper bound
is about 0.67 MB of added record bytes per resolve (derived).

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
  is immutable and never rewritten. The processes that re-encode a stamp
  are the fold and the compactor. An older fold copies min, max and null
  count through the typed form and drops fields 6 to 8; an older compactor
  writes an L1 part without them. Either leaves segments whose statements
  scan. No answer changes, and nothing durable, retained or fenced depends
  on the fields.

So `CommitRecord.format_version` stays 1 and the `commit-record` family in
`sys/compat` is untouched. `SnapshotEntry` is a proto record carried inside
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
  leave `i128`. This departs from ADR-0850, which omits a sum that leaves
  `i64`: the scan's own `avg` and `sum` run in `i128`, and a statistic
  narrower than the scan cannot answer what the scan answers.
- **Zero count.** For an I64 or BOOL column, the count of non-null rows
  whose value is 0 or false. This is the one value-to-count entry
  ClickHouse's default-value count corresponds to. No dictionary is
  written; see "Rejected alternatives" and open question 1.
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
this ADR is in that state forever, and so is every entry an older fold or
compactor re-encoded. A reader never treats an absent field as zero. The
existing absences carry over unchanged: no entry at all (column declared
after the flush opened, a stream blob that did not decode, a rewrite part),
and an ineligible type.

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
- **Every other producer of a stamp** (migrate, the force-2 re-encode)
  keeps whatever it does for stamps today. Task T4 audits them and states
  the result; this ADR did not read them.

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
make `.cstat` state something its own definition does not. Whether the fold
should stop tallying sums for segments whose stamp carries one is open
question 5.

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

**Several aggregates in one ungrouped statement (Q2).** The rule accepts an
ungrouped, filter-free aggregate whose every call is one of `COUNT(*)`,
`SUM(col + k)`, `AVG(col)` or `exact_sum_count(col)` over declared I64
columns, and emits one row. `COUNT(*)` is the sum of `sample_count`. If any
call declines, the whole statement scans. `logical_aggregate_answerable`
mirrors the shape, as the two are required to stay in step.

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
| A stale statistic that could hold an erased row | rewrite parts carry no stamp: forced empty in `fold.rs` (the rewrite snapshot entry) and in `catalog.rs` (the live rewrite `SegmentRef`). That the pre-rewrite input's own stamp never answers after the erasure completes is inferred, not traced; see the paragraph below |
| A pinned commit token admitting a segment the statistics do not cover | coverage is per segment of the snapshot being queried: `derive_l0` stamps a token-resolved record from itself, and the reader loops return `None` at the first segment with no usable figure |
| A total that leaves the result type | `sum_scalar` (`checked_mul`, `checked_add`, then the `i64` conversion) and `count_to_i64` in `metadata_agg.rs` decline; for `exact_sum_count` the projection's own check raises the same `IntegerSumOverflow` on both paths |
| A ts bound that clips a segment, a content predicate, a row filter, a key equality, a prune arm other than `NumNotEq` | `metadata_rewrite_stats_are_exact`, unchanged |
| A defective entry | dropped at decode by the clauses in decision 2, counted, and the segment reads as uncovered |

On the stale statistic: the stamp of a pre-rewrite input holds figures
that include the erased rows. It can answer only if that input is still in
the resolved segment list while no erasure is pending. This ADR did not
trace the code that rules that out. `docs/catalog-and-mvcc.md` ("Targeted
re-fold requests") says a sealed part can keep naming pre-rewrite inputs
for an hour no reconcile pass re-lists, and
`crates/ravel-catalog/tests/token_cache_erasure_4570.rs` asserts that an
erased row is never served on the token paths. The same exposure exists
today for `MIN`, `MAX` and `COUNT(col)` from the stamp, so it is not new,
but a sum widens what it would misstate. T5 owes a test for it (test 6
below), and if the test shows the input can be resolved, the readers must
decline for any segment a listed rewrite record supersedes. Open question
8.

On the sum overflow of #3847: a per-object sum can no longer overflow,
because it is kept in `i128`. What can overflow is the statement's total
against its `i64` result type, and that still declines at `sum_scalar`, so
the scan produces the error. #3847's open defect (the scan's per-row
`col + k` wraps before `sum` sees it) is not changed by this ADR. The
`SUM(col + k)` shapes here go through `exact_sum_count`, which does not
evaluate `col + k` per row.

On #1848: the new fields add no verbatim attribute value to any object. A
zero count is a count. A sum equals a value only for an object with one
non-null row, which the stamp's min and max already state. See
"Contradictions found", item 3, for the part of #1848 this does not
settle.

**Measurement owed.** The share of the entry tenant's objects whose
`"UserID"` sum leaves `i64`, read from the stamps after a load with this
change (count of entries carrying field 7). It decides nothing in the
design, which is exact either way; it says whether Q3 could ever have been
answered from the `int64` sum of `.cstat`.

### 7. Cost

Nothing here was measured. Derived figures follow from the proto encoding
and are marked derived.

- **Bytes per object (derived).** Per I64 column: field 6 costs 2 to 11
  bytes or field 7 costs 18, and field 8 costs 2 to 11. Per BOOL column:
  field 8 only. The upper bound is 29 bytes per I64 column and 11 per BOOL
  column per commit record, and the same again on the `SnapshotEntry` once
  sealed. For the entry tenant's 104 declared columns the upper bound is
  about 3.0 KB per record; today's records are about 2.5 KB each (1.0 MB
  over 409 records, #3243 comment 6392034172). Nothing is added to the data
  object.
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
| Q2, Q3, Q29 hot wall today | 0.14 to 0.20 s | measured 2026-11-22 |
| Resolve record bytes per statement, every statement | at most arm A plus 3.0 KB per record | predicted |
| Answers, all 43 statements | equal to arm A | predicted |

Bands for the four statements' walls, the hot total, the cold total, the
load wall and `resolveMs` on a non-target statement are set in the
pre-registration comment, not here.

### 8. Rollout and compatibility

- **Regime.** Upgrade safety covers v0.33 onward. This change adds no
  format version, so nothing is activated through `sys/compat` and no
  certificate is involved. Installing the release starts the stamping.
- **Older query reader.** Ignores the fields and scans. Same answers.
- **Older fold, newer ingest.** The fold seals an hour with entries that
  lack the fields. Sealed parts are carried forward by reference, so those
  hours stay without them until the records are recompacted or a rebuild
  fold re-reads commit records that still exist. Until then those segments
  fall to `.cstat` behind a matching stamp, or scan.
- **Older compactor, newer ingest.** The L1 part lacks the fields, and the
  L0 records it replaces are swept. Same consequence.
- **Order.** Upgrade fold and maintain processes before ingest, the order
  ADR-0873 decision 6 already requires for the stamp. The per-carrier
  coverage counter of decision 5 is how a wrong order shows up. Whether to
  gate emission instead is open question 4.
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

1. For each of Q1, Q2, Q3 and Q29 in its ClickBench form, on a snapshot
   resolved from commit records with no fold: the plan holds
   `MetadataOnlyExec` and no `LogsScanExec`, and a counting store reports
   exactly zero data GETs. Paired with the same statement on the same
   objects with the fields stripped from the stamps: the plan scans and the
   answer is byte-identical.
2. A pending erasure forces the scan for each shape, asserted on the plan
   and on a nonzero data GET count.
3. A total that leaves `i64` forces the scan: `SUM(col)` over objects whose
   stamp sums add past `i64::MAX`: the rewrite declines and the scan raises
   `IntegerSumOverflow`. A second case has one object whose own sum
   leaves `i64` (field 7), and `AVG(col)` answers from the stamp with the
   scan's exact value.
4. A token-resolved snapshot holding one segment without the fields forces
   the scan, and the same snapshot with the fields answers from the stamp.
5. Composition: a folded snapshot where some segments carry the fields and
   others have only a reconciled `.cstat` entry answers from statistics,
   equals the scan, and reports the per-carrier split exactly. A segment
   with both is counted once, shown by an answer that differs if either
   carrier is added twice.
6. A rewrite part carries none of the fields, and a statement over it
   scans. After the erasure completes, with a sealed part and a warm record
   cache that still name the pre-rewrite input, no statement answers from
   that input's stamp.
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
  objects, while the scan itself computes in `i128`.
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
  with declared I64 or BOOL columns, not only the benchmark entry.
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

## Open questions for the owner

1. **Value counts: the zero count only, or a bounded dictionary?**
   Recommended: the zero count only. It is what the measured ClickHouse
   behaviour corresponds to and it stays clear of the 2026-10-25 objection
   to dictionaries.
2. **Does a sum and a zero count stamped during the load meet the
   ClickBench rules as the reopening intended?** Recommended: yes. They are
   written for every tenant with declared columns, during the write, with
   no entry-specific setting.
3. **Sum width: `i128` in two fields, or `int64` omitted on overflow as
   ADR-0850 has?** Recommended: `i128`, because the scan is `i128` and Q3
   likely needs it (inferred).
4. **Rollout: deployment order plus the coverage counter, or an explicit
   switch that starts emission?** Recommended: deployment order. A wrong
   order costs coverage on some hours and never an answer.
5. **Should the fold stop tallying `.cstat` sums for segments whose stamp
   carries one?** Recommended: not in this epic. Decide after the A/B.
6. **Is #1848 widened to cover stamps before this ships?** Recommended:
   widen #1848, do not block on it. See "Contradictions found", item 3.
7. **The A/B box.** One c6a.4xlarge, one same-box run, needs approval.
8. **If test 6 shows a pre-rewrite input can be resolved after an erasure
   completes, is that fixed here or in its own issue?** Recommended: its
   own issue, landed before T5, because it already affects `MIN`, `MAX`
   and `COUNT(col)` from the stamp.

## Contradictions found, reported and not fixed

1. **Statement labels.** ADR-0850, #861 and the module doc of
   `crates/ravel-sql/src/metadata_agg.rs` use 1-indexed labels and attach
   `SUM(col + k)` to q03 and q04 and `AVG(col)` to q30. In the 1-indexed
   file, q03 is the three-aggregate statement, q04 is `AVG("UserID")` and
   q30 is the ninety sums. #4628 and the result files are 0-indexed.
2. **ADR-0850 and #861 say q03 and q04 answer from the fold-time sum.** By
   the code, the three-aggregate statement is declined in every case (gap 1
   above), and `AVG("UserID")` needs every object's sum to fit `i64` (gap
   2), which is unlikely for that column (inferred, not measured).
3. **#1848 names `.cstat` only.** The stamp has put exact I64 and BOOL
   extrema on commit records and snapshot entries since ADR-0873. A
   superseded commit record, and a `.csnap` part a later fold replaced,
   hold the pre-erasure extrema until a sweep removes them, the same shape
   #1848 describes. This ADR adds a sum and a zero count to those objects.
   No STR or BYTES value is involved.
4. **#4628 asks for a fallback "when a sum overflowed `i64`".** Under this
   ADR a per-object sum does not overflow; the fallback applies to the
   statement's total. The acceptance test is written against the total.

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
  two messages: up to 29 sites in 8 files across `ravel-commit`, `ravel-catalog`
  and `crates/ravel-sql/tests/logs_declared_minmax_from_stamps.rs`. That
  edit is mechanical and keeps every later task compiling alone.
- **T2, catalog carriage.** `crates/ravel-catalog`: the mirror codec, the
  fold's carry functions, the record-cache charge and its test,
  `tests/declared_stats_carriage.rs`, and the snapshot-entry and
  `SegmentRef` passages of `docs/catalog-and-mvcc.md`. After T1.
- **T3, flush and load producers.** `crates/ravel-ingest`: `Running`, both
  observe paths, `fold_fallbacks`, `tests/declared_column_stamps.rs`,
  `docs/ingest.md`. After T1, beside T2.
- **T4, compaction producer.** `crates/ravel-maintain`: the per-part
  accumulator, a test that a rewrite part stays empty, and the audit of
  migrate and the force-2 re-encode. After T1, beside T2 and T3.
- **T5, stamp-first readers.** `crates/ravel-sql`: the sum and not-equal
  readers, the load gate in the executor, the per-carrier counter, tests 1
  (Q1, Q3, Q29), 2, 3, 4, 5 and 6, `docs/query-engine.md`. After T2.
- **T6, several aggregates in one statement.** `crates/ravel-sql`: the
  rule's multi-call shape and its logical mirror, test 1 for Q2, and the
  re-pin of `sum_plus_literal_beside_a_second_aggregate_scans_exactly`.
  After T5, same crate, so not beside it.
- **T7, inspectors and the loader test.** `services/ravel-cli`: print the
  fields, and test 9. After T3 and T6.
- **T8, the A/B.** Pre-registration on #4628, one run, readout. After T7,
  on the owner's box approval.

Refs: #4628, #3231, #850, #861, #873, #3243, #3244, #1848, #3847
