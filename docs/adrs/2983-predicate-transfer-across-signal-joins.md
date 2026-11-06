# ADR-2983: Predicate transfer across signal joins

Status: Accepted (2026-10-15). Not implemented: no code before the gates in
Sequencing are met. Stage 0 measured 2026-11-06 (Stage 0 amendment).
Date: 2026-10-14
Issue: #2983.
Builds on: ADR-2904 (cross-signal SQL statements), ADR-2707 (exact-key
index). Amends nothing.

## Context

ADR-2904 lets one SQL statement join two signal tables, for example
`spans JOIN logs ON spans.trace_id = logs.trace_id`. Each side is read by its
own scan, and each scan reads what its own predicates allow. Nothing the
join learns from one side reaches the other side's scan. A statement that
selects a few traces on `spans` (by service, status and window) and joins
them to `logs` reads every `logs` object and block in the window, then
throws away the rows whose `trace_id` the join does not match.

Predicate transfer closes that gap. Before the second scan chooses what to
read, the key set the first side produced is applied to it as a pruning
predicate, so it opens fewer objects and blocks. The key set ends as a
key-set predicate pushed into a scan.

### What DataFusion 55.2 already does

DataFusion 55.2 ships the mechanism and turns it on by default. The
optimizer option `enable_join_dynamic_filter_pushdown` (default true) makes
a hash join build a dynamic filter from its build side's keys and push it to
the probe side's scan in the `Post` filter-pushdown phase. The filter holds
the build keys' bounds and a membership test. The membership test is an
`IN` list when the build side has at most
`hash_join_inlist_pushdown_max_distinct_values` distinct keys (default 150)
and its key columns take at most `hash_join_inlist_pushdown_max_size` bytes
(default 128 KiB); otherwise it is a reference to the join's hash table. A
consumer waits for the filter with `DynamicFilterPhysicalExpr::wait_complete`.

Ravel's session sets none of these options, nor the master switch
`enable_dynamic_filter_pushdown` that overrides the per-kind switches when
set; nothing under `crates/` or `services/` names them. It does set
`with_repartition_joins(false)` (`session_config` in `crates/ravel-sql/src/session.rs`), so a
hash join builds its table once and one key set exists per join.

DataFusion's own Parquet scan already adopts these filters, so a join over
the tenant's Parquet tables (ADR-2040) gets them today. The filter reaches
Ravel's signal scans and is dropped. The logs scan
receives `Post`-phase filters in `handle_child_pushdown_result`
(`crates/ravel-sql/src/logs_scan/mod.rs:2769`), answers `PushedDown::No` for
every one, and adopts only a TopK threshold; its doc comment
(`crates/ravel-sql/src/logs_scan/mod.rs:2765-2768`) names a join's filter as
one it does nothing with. The spans scan implements no such hook. (Stage 0
amendment: both citations have moved, and the logs scan still answers
`PushedDown::No` there.)

DataFusion's own gate for which joins push a filter
(`allow_join_dynamic_filter_pushdown` on the hash join) admits a probe side
whenever the join type reports the probe as preserved for `ON`-clause
pushdown. This ADR does not rely on that gate (decision 3).

### What the receiving scans accept today

The receiving scan needs an arm that takes a set of keys. None exists:

- The `trace_id` hex literal rewrite covers `=` and `!=` only. `IN` lists
  never reach it, because `datafusion-sql` builds `Expr::InList` without
  consulting a registered planner
  (`crates/ravel-sql/src/trace_id_planner.rs:22-25`).
- The spans pushdown extracts a single `trace_id` equality into a point
  lookup (`crates/ravel-sql/src/spans_pushdown.rs:18-27`) and refuses a
  `trace_id` disjunction, since RSPAN exposes no set primitive
  (`crates/ravel-sql/src/spans_pushdown.rs:61-66`).
- The logs pushdown extracts nothing from the fixed `trace_id` column: of
  the nine fixed columns only `ts` extracts anything
  (`crates/ravel-sql/src/logs_pushdown.rs:334`). POSTINGS never serve an
  `IN`: `attrs['k'] IN (...)` is not extracted
  (`crates/ravel-sql/src/logs_pushdown.rs:62-64`), and neither is a
  declared `Str` or `Bytes` column's `IN`
  (`crates/ravel-sql/src/logs_pushdown.rs:102-103`).
- ADR-2707 decision 7 ("Read-path wiring and statistics") adds a `KeyEquals`
  arm on the logs side and the KEY_IDX probe on the spans side. Both are
  equality lookups. Neither takes a set.

(Stage 0 amendment: ADR-3692 decision 6 has since given both scans a set
arm for a literal `IN` list. A join's dynamic filter still reaches none of
them.)

### What is not known

There is no join benchmark in this repository, and no measurement of how
many blocks a correlated `trace_id` join reads on either side. This ADR
makes no performance claim. Stage 0 builds the baseline, and every figure
in "Pre-registered figures" below is predicted or deferred to it.

## Decision

### 1. Transfer reuses DataFusion's hash-join dynamic filter, in its `IN`-list form only

A transfer is one hash join's dynamic filter, adopted by one Ravel scan on
the join's probe side, used to choose that scan's read set. Ravel builds no
second mechanism beside DataFusion's.

- The scan adopts the filter only in its `IN`-list form, over a probe key
  that is a bare column of the scan's own table (no cast, no computed
  expression). The hash-table reference form is declined: it can test a row
  only after the row is decoded, which the join's own probe already does.
- The scan waits for the filter (`wait_complete`) before it chooses which
  objects and blocks to read. The hash join does not poll its probe input
  until its build side is complete, so the wait adds no latency of its own,
  and a scan that would prefetch before its first poll does not prefetch
  while a filter it adopted is pending.
- Only a hash join transfers. A sort-merge or nested-loop join transfers
  nothing.
- Transfer is behind a session switch, off by default until Stage 3's
  acceptance amendment turns it on. The property test in decision 10 uses
  the same switch. With the switch off, the session configuration is
  exactly today's.
- With the switch on, the session sets `enable_dynamic_filter_pushdown`,
  `enable_join_dynamic_filter_pushdown` and both `IN`-list caps explicitly
  (decision 5). A DataFusion upgrade that changes a default then cannot
  change what Ravel transfers. These settings are session-wide, so a
  transfer-on statement that also joins Parquet tables gives DataFusion's
  Parquet scan the same caps.

### 2. Exact by default: transfer is an optimisation and never changes a result

A statement with transfer returns the same rows as the same statement
without it. Transfer only removes objects and blocks from a scan's read set.

- The scan answers `PushedDown::No` for an adopted filter, as it does for
  the TopK threshold. It never evaluates the filter as the exact per-row
  test, and the join re-checks key equality on every probe row it reads.
- Decision 3 lets a filter reach only a side whose rows without a match
  are never emitted. Removing a block that holds no key from the set
  therefore cannot remove an output row, and keeping a block that turns out
  to hold no match costs work and nothing else. The join's re-check is what
  makes extra rows harmless; the side rule is what makes missing rows
  impossible. Neither is enough alone: a filtered preserved side of an
  outer join loses output rows even though the join re-checks keys.
- Declining to transfer is always correct. Every shape this ADR does not
  classify (decisions 3 and 4) transfers nothing.

### 3. Which side may receive a transferred filter

This decision applies ADR-2904 "### 5. Plan-derived work is attributed per
table". A transferred filter that removes an object or block from a scan is
a new prune source, and it may prune a scan only when every row that scan
contributes to the result must satisfy it. Getting this wrong returns wrong
rows, the class of issue #2907.

| Join, as written | Side that may receive the other side's keys | Side that may not | Reason |
|---|---|---|---|
| Inner | The probe side, with the build side's keys | The build side | A row without a partner in the other side's key set is never emitted. Either side would be sound, but the hash join builds its key set from the build side only, so only the probe side receives one. |
| Left outer | Neither in this ADR | Left, right | The left side is preserved: an unmatched left row is emitted with nulls, so filtering it drops output rows. |
| Right outer | Neither in this ADR | Left, right | The mirror of left outer. |
| Full outer | Neither | Left, right | Both sides are preserved. |
| Left semi (`EXISTS`, `IN` subquery) | Left, the null-rejecting side, with the right side's keys | Right | A left row without a match is rejected by the semi join itself. |
| Left anti (`NOT EXISTS`, `NOT IN`) | Neither | Left, right | The left side's output is exactly its rows without a match. |

Three cases are excluded although a row-level argument says they are
sound: the non-preserved side of an outer join, the subquery side of a semi
join, and the subquery side of an anti join. `NOT IN` alone shows why the
argument is not enough: under null-aware semantics a null key on the
subquery side empties the result, and a key filter removes that null. Each
case is left to a later amendment that extends decision 10's property test
to it first.

The check is physical, not textual. Join selection swaps inputs, so a left
semi join in SQL may run as a right semi join with the other input as build
side. Ravel decides on the `HashJoinExec` after join selection, from its
join type and which input is the probe:

- `Inner`: the probe may receive.
- `RightSemi`: the probe may receive. It is the side the join emits, the
  null-rejecting side.
- Every other join type (`Left`, `Right`, `Full`, `LeftSemi`, `LeftAnti`,
  `RightAnti`, `LeftMark`, `RightMark`): no transfer.
- A join whose key comparison treats null as equal to null
  (`IS NOT DISTINCT FROM`) transfers nothing: the key set holds no null, and
  a null probe key that should match would be filtered out.

Ravel applies this check itself and does not trust DataFusion's
`on_lr_is_preserved` gate, which admits more join types than this table.

The path from the join's probe input down to the scan must also hold the
rule. The filter may pass through a filter, a projection that passes the key
column through unchanged, batch coalescing, repartitioning, and the input
of another inner hash join. It never passes a `UNION`: a key set from one
branch is never applied to a scan in another, and a scan below a `UNION`
under the join receives nothing. It never passes a limit, a TopK or a sort
with a fetch, since filtering below a limit changes which rows the limit
keeps. It never passes an aggregate or a window. A path this list does not
name transfers nothing. A Ravel physical rule, run after join selection and
DataFusion's filter pushdown, records for each scan the joins whose filter it
may adopt; the scan ignores every other dynamic filter it receives.

### 4. The prune is per scan, never on the shared snapshot

ADR-2904 "### 5. Plan-derived work is attributed per table" prunes table
T's snapshot only by a predicate that every scan of T must satisfy. A
transferred filter holds for one scan instance, so it prunes only that
instance's own read set, at execution time:

- It removes objects and blocks from the receiving scan's list after the
  snapshot is resolved and the scan is planned. It never removes a segment
  from the snapshot, so another scan of the same table (a self-join, a
  second `UNION` branch) reads what it read before.
- It changes nothing the resolve or admission phases compute: the snapshot,
  `max_segments` and the cost estimate under ADR-2904 "### 6. Admission and
  budgets are per statement; lane checks are per signal" are the figures
  of the untransferred plan.

### 5. The key set lives inside the statement's memory pool, capped at k

ADR-2904 "### 8. Joins run on the coordinator" keeps joins on the
coordinator with no fan-out, under the statement's one memory pool, with no
spill. The key set lives inside that pool.

- k = 1,024 distinct keys. For a 16-byte `trace_id` that is 16 KiB of key
  bytes. k is predicted, not measured; Stage 0 measures the probe cost per
  key and Stage 3 confirms or changes k by amendment.
- With transfer on, the session sets
  `hash_join_inlist_pushdown_max_distinct_values` to k, so a build side
  with more than k distinct keys yields the hash-table form and transfers
  nothing. It sets `hash_join_inlist_pushdown_max_size` to 1 MiB. With
  transfer off, both keep today's defaults. That cap bounds the build side's key columns over all build rows,
  duplicates included, so it is larger than k times the key width; Stage 0
  checks it against the corpus.
- The scan copies the adopted keys into its own sorted key set and reserves
  that set through the pool's fallible `try_grow` path before it uses it.
  The infallible `grow` path is the one issue #1370 shows bypassing both
  ceilings, and the key set never uses it.
- When the reservation is refused, or the build side has more than k
  distinct keys, the scan does not transfer. The statement does not fail.
- Above k there is no Bloom-filter fallback (rejected alternative D).

### 6. A transferred key set comes from this statement's snapshot of the other side

ADR-2904 "### 2. One snapshot per named signal, resolved together, with no
order between them" gives the joined signals no ordering. The key set comes
from the build side's snapshot, which may be older or newer than the
receiving side's. That is correct for its purpose: it filters rows this
statement reads, and the join compares those same rows.

- A key set is never cached or reused across statements, and never across
  the attempts of one statement. A retry resolves every signal again
  (ADR-2904 decision 2's retry bullet) and builds every key set again.
- A key set is never written anywhere. It is memory of one statement's
  execution and is dropped with it.

### 7. Cost is reported against the signal that paid it

ADR-2904 "### 7. Cost is reported per phase, per signal" reports requests
and bytes per signal and per phase. Transfer follows it:

- Requests and bytes a transferred filter causes on the receiving scan are
  counted in the receiving signal's `stats.signals` entry, under the phase
  that issued them: KEY_IDX leaf and section probes under ADR-2707's
  `keyIndex` phase, block reads under `scan`.
- The build side's cost stays in its own signal's entry and does not change
  with transfer.
- Each `stats.signals` entry gains a `transfer` object: whether a filter was
  adopted, the number of keys, `objectsPrunedByTransfer`,
  `blocksPrunedByTransfer`, and when nothing was adopted, the reason
  (`disabled`, `join_type`, `path`, `not_in_list`, `over_k`,
  `memory_refused`, `no_receiving_arm`). This adds a field to the entry
  ADR-2904 decision 7 defines; `EXPLAIN` carries it with the rest of the
  list.
- `segmentsPruned` and `stats.pruning.segments` keep their snapshot
  meaning and never include a prune that transfer caused. Transfer prunes
  are reported only in the `transfer` object, so the two figures are not
  to be summed.

### 8. The receiving arm is a dependency, not part of this ADR

The receiving scan needs an arm that takes a key set and turns it into
objects and blocks to skip. On the logs side that is an `IN` or key-set arm
beside ADR-2707's `KeyEquals` (issue #2762, task T11 of epic #2961). On the
spans side it is the same over the spans KEY_IDX probe (ADR-2707 task T10).
Both are planned as a follow-on to epic #2961 and are not decided here. Until
a signal has that arm, its scan reports `no_receiving_arm` and transfers
nothing.

### 9. Where transfer applies first

The first shape is `spans` and `logs` joined on `trace_id`, in either
direction. Both columns are `FixedSizeBinary(16)`, so the probe key is a
bare column with no cast. A join on any other key, or between other
signals, transfers once its receiving scan has a key-set arm and decision
10's tests cover it.

### 10. Tests pin the claims

- **Equality, property test.** Generated join shapes (inner, left, right and
  full outer, semi, anti, a `UNION` under and over a join, a limit under a
  join, a self-join) over generated `spans` and `logs` rows with shared,
  missing and null `trace_id` values. Each statement runs with transfer on
  and off, and the two results are the same multiset of rows, compared
  by value with floats by bit pattern. The regression seed file is
  checked in.
- **Count test.** A fixed corpus where the build side selects fewer than k
  traces. With transfer on, the receiving scan opens strictly fewer blocks
  than with it off, and `blocksPrunedByTransfer` equals the difference.
- **Refusal tests.** For each join type and path the table and decision 3
  exclude, the plan adopts no filter, the reason is reported, and the
  receiving scan's counters equal the transfer-off run.
- **Memory test.** A pool too small for the key set refuses the
  reservation; the statement succeeds, reports `memory_refused`, and
  returns the transfer-off rows.

## Sequencing

This ADR is gated. No code lands until:

1. Issue #1370 is fixed. Join operators reach the pool through
   DataFusion's infallible `grow` path and bypass both SQL memory ceilings
   (ADR-0013, "Amendment: memory ceilings are best-effort for joins"). A
   key set charged to a pool that the join beside it escapes does not bound
   the statement. The fix must also cover the hash join's own `IN`-list
   copy, which DataFusion builds from the build side's key columns.
2. Epic #2904 is closed: multi-signal statements run, per-scan attribution
   (ADR-2904 decision 5) and `stats.signals` (decision 7) are in place.
3. The receiving key-set arm of decision 8 exists for the signal that
   receives. (Stage 0 amendment: it exists for literal lists on both
   signals; routing a dynamic filter into it is Stage 2's work.)

Joins are not spill-eligible (ADR-0954, "Relationship to ADR-0013 and
ADR-0102"), so a join over the memory budget fails with the typed memory
error. Transfer does not change that.

Stages:

- **Stage 0: a join baseline.** A `spans` plus `logs` corpus with a
  correlated `trace_id` join, and a statement set covering build sides of
  1, k/10, k and 10k distinct traces. For each statement and each scan:
  objects and blocks opened, and requests and wire bytes per phase (as
  transferred, ranged reads and retries included). Before the run, the
  tracking issue carries the pre-registered figures. The report stamps the
  verified state: catalog folded to the last write, KEY_IDX present on the
  spans objects, the cache state. Stage 0 needs no code from this ADR.
- **Stage 1: the receiving arm**, as the follow-on to epic #2961 (decision
  8), with its own issue.
- **Stage 2: transfer.** Decisions 1 to 7 and 10, behind the session switch,
  off by default.
- **Stage 3: re-run Stage 0 with transfer on and off**, and an acceptance
  amendment that records the figures, confirms or changes k, and turns the
  switch on if the figures hold.

## Pre-registered figures

A figure outside its band is a miss, investigated before Stage 3 turns the
switch on. A figure marked as set at Stage 0 or Stage 1 gets its number in
that stage's pre-registration comment on the tracking issue, before the
run. (Stage 0 amendment: the Stage 0 and Stage 1 figures below are now
set.)

| Figure | Expected band | Miss | Basis |
|---|---|---|---|
| Rows returned, transfer on against off, every generated shape | identical multiset, 0 differing rows | any differing row | predicted: exact by construction (decision 2) |
| Build-side scan requests and wire bytes, on against off | equal | any difference | predicted: transfer never touches the build side (decision 7) |
| Receiving scan blocks opened, build side at or under k keys | strictly fewer with transfer on; the ratio is set at Stage 0 | not fewer | predicted; figure deferred to Stage 0 |
| Receiving scan objects opened, build side at or under k keys | at most the transfer-off count; the ratio is set at Stage 0 | more than off | predicted; figure deferred to Stage 0 |
| Receiving scan `keyIndex` requests (leaf and section probes) | set when Stage 1 fixes the key-set arm's probe shape | outside the Stage 1 band | predicted; figure deferred to Stage 1 |
| Receiving scan wire bytes per phase, on against off | set at Stage 0 | outside the Stage 0 band | predicted; figure deferred to Stage 0 |
| KEY_IDX requests caused by transfer, build side over k keys | 0 | any | predicted: no transfer above k (decision 5) |
| Key-set reservation | at most k times 16 bytes plus the set's own overhead, reserved before use | any unreserved key set | predicted (decision 5) |

## Rejected alternatives

**A. Semi-join reduction as a separate pre-query.** Run the build side as
its own statement, collect its keys, and add them to the second statement
as an `IN` list. The two statements resolve two snapshots of each signal,
which breaks ADR-2904 "### 2. One snapshot per named signal, resolved
together, with no order between them": the keys would come from a snapshot
the join never reads. It also pays every resolve twice and needs a client
or coordinator to hold the keys between the two.

**B. Cache key sets across statements.** A key set is a fact about one
snapshot of one signal. A second statement resolves new snapshots, so a
cached key set is stale by construction and would prune rows the new
snapshot holds. ADR-2904 decision 4 also caches nothing across statements.

**C. Push transfer into distributed workers.** ADR-2904 "### 8. Joins run
on the coordinator" refuses fan-out for any statement that names more than
one table. Shipping a key set to workers is a design for after a decision
allows a join to fan out.

**D. A Bloom filter above k.** No read structure in Ravel can use a Bloom
filter of keys to skip an object or a block. KEY_IDX is probed by key
(ADR-2707 decision 5), and the RSPAN block blooms cover `service_name` and
`name` only (`crates/ravel-sql/src/spans_pushdown.rs:36-41`). A Bloom filter
could only drop rows after decode, which the join's hash probe already
does. It would add memory and save no reads.

**E. Rely on DataFusion's join-type gate.** It admits the probe of every
join type whose probe is preserved for `ON`-clause pushdown, which includes
cases decision 3 excludes. A DataFusion change to that gate would change
Ravel's results. Ravel keeps its own narrower check.

**F. Prune the shared snapshot with the transferred filter.** It would save
the resolve-time segment list as well, but another scan of the same table
reads that snapshot, and the filter holds only for one scan. This is the
wrong-rows class of issue #2907.

**G. Adopt the hash-table form of the filter.** It tests a row only after
the row is decoded. That saves the join no reads and duplicates work the
join's probe does.

## Consequences

- A correlated `spans`/`logs` join on `trace_id` can read fewer objects and
  blocks on the receiving side once the gates in Sequencing are met. Until
  Stage 3, nobody knows by how much, and the default stays off.
- No statement changes its result. A statement whose shape decision 3 does
  not admit runs exactly as without this ADR.
- The receiving scan pays KEY_IDX probes to save block reads. Above k, and
  for a join with no receiving arm, it pays nothing new.
- `stats.signals` entries gain a `transfer` object. ADR-2904 is not
  amended now: its amendment section for that field lands with Stage 2's
  code, after issue #2912 has built the `stats.signals` shape and epic
  #2904 is closed.
- A transfer-on session pins four DataFusion options that today ride on
  defaults. A transfer-off session is configured as today.
- No persistent format changes: no RSEG, RLOG or RSPAN layout, no protobuf
  schema, no commit token, no object key.

## Amendment (issue #3708): Stage 0 baseline, and the receiving arm ADR-3692 landed

<!-- amendment-applies: sections="What DataFusion 55.2 already does|What the receiving scans accept today|Sequencing|Pre-registered figures" pointer="Stage 0 amendment" -->

Since this ADR was accepted, two things have moved. Stage 0 was measured,
and the read side gained a set arm for literal lists. Decisions 1 to 10
are unchanged. Transfer is still not implemented, and no switch exists yet.

### The receiving arm exists for literal lists

ADR-3692 decision 6 and its key set amendment (PR #3906) gave both
receiving scans a set arm:

- **Logs:** a top-level `column IN (...)` becomes `LogsPushdown::key_in`.
  The column is a declared key-index column, or `trace_id` when the
  declaration indexes it. The list is 1 to 1,024 distinct literals of
  exactly the column's type (`crates/ravel-sql/src/logs_pushdown.rs:183`,
  `key_in_list` at `:451`).
- **Spans:** a `trace_id IN (...)` of 2 to 1,024 distinct ids becomes
  `SpansPushdown::trace_ids` (`crates/ravel-sql/src/spans_pushdown.rs:68-75`).
- **Hex ids:** `trace_id IN ('<32-hex>', ...)` now plans.
  `TraceIdHexInListRule` rewrites the list before type coercion
  (`crates/ravel-sql/src/trace_id_planner.rs:22-29`).
- **Probe shape:** a set probe reads the KEY_IDX prefix, then each touched
  bucket, or makes one ranged read over the touched span above 64 buckets.
  An object every key is absent from is not opened. A scan with exactly
  one key arm routes through the key-index leaves.

That is Stage 1 for a set written in the statement. It is not yet the arm
transfer needs. Decision 1's filter arrives as a hash-join dynamic filter
through `handle_child_pushdown_result`, and the logs scan still answers
`PushedDown::No` for every parent filter there
(`crates/ravel-sql/src/logs_scan/mod.rs:3264`). Its doc comment, at
`:3255-3263`, still names a join's filter as one it does nothing with.
Stage 2 routes a dynamic `IN`-list filter into `key_in` and `trace_ids`.
The arm's 1,024-key cap equals k (decision 5), so it never binds before k
does.

### Corrected citations

The line references in Context were taken on 2026-10-14. On main 87eff26db
they read:

| Context cites | Now |
|---|---|
| `logs_scan/mod.rs:2769` (`handle_child_pushdown_result`) | `:3264`, still `PushedDown::No` for every parent filter |
| `logs_scan/mod.rs:2765-2768` (its doc comment) | `:3255-3263` |
| `trace_id_planner.rs:22-25` (`IN` lists never reach the planner) | `:22-29`, which names `TraceIdHexInListRule` as the path they take now |
| `spans_pushdown.rs:61-66` (a `trace_id` disjunction refused) | `:61-75`: a disjunction of `=` is still refused; an `IN` list is carried |
| `logs_pushdown.rs:334` (only `ts` extracts from the fixed columns) | `trace_id` now feeds `key_equals` (`key_equality`, `:716`) and `key_in` (`key_in_list`, `:451`) |
| `logs_pushdown.rs:62-64` (`attrs['k'] IN` not extracted) | `:82-84`, still true |
| `logs_pushdown.rs:102-103` (a declared `Str` or `Bytes` `IN` not extracted) | `:122-123` for POSTINGS, still true; a key-index column's list feeds `key_in` |

The module comment at `logs_pushdown.rs:623` still says only `ts` extracts
anything from the fixed columns. That is stale, and it is outside this
amendment.

Two consequences for Stage 2:

- DataFusion builds the `IN`-list form only up to
  `hash_join_inlist_pushdown_max_distinct_values`, 150 by default. A
  transfer-on session must raise it to k, among the four options this ADR
  already says it pins.
- DataFusion rewrites a literal list of two or three values into `=`
  disjuncts before the scan sees it, and neither arm carries a disjunction
  (issue #3913). A dynamic filter is not a literal list, so this does not
  reach transfer, but Stage 3 must not compare transfer against a two- or
  three-key literal list.

### Stage 0: the join baseline

Stage 0 was measured in the Stage 1 run of ADR-3692. The run was box C,
`investigations-20261106T154511Z-47463-23195`: main b54ecc52a, 20,000,000
traces over 24 hours, on real S3 in us-east-1. Its pre-registration is
https://github.com/NOFireAI/ravel/issues/3692#issuecomment-6688876402 and
its result is
https://github.com/NOFireAI/ravel/issues/3692#issuecomment-6698223574.

The verified state, from the folded_leaves stamps:

- the catalog was folded through watermark hour 498331;
- the spans head holds one part of 800 entries, with a `trace_id` leaf of
  20,000,000 entries and 0 uncovered;
- every spans object carries KEY_IDX, version 1;
- each call was cold, after a server restart and a page-cache drop.

The statement was Journey C: `logs JOIN spans ON trace_id`, the build side
the first K root spans of the failing hour. Transfer was absent. Each
figure is from 20 cold calls, and all 20 gave the same counts:

| Build side K | Scan | Objects opened | Requests (resolve, scan; probe and keyIndex 0) | Wire bytes | p50 s |
|---|---|---|---|---|---|
| 1, 102, 1,024, 10,240 | logs (probe side) | 548 of 548 | 545 (1, 544) | 3,806,990,963 | |
| 1, 102, 1,024, 10,240 | spans (build side) | 8 of 44 | 12 (4, 8) | 51,364,567 | |
| 1 | statement | 556 | 557 | 3,858,355,530 | 10.00 |
| 102 | statement | 556 | 557 | 3,858,355,530 | 9.93 |
| 1,024 | statement | 556 | 557 | 3,858,355,530 | 9.94 |
| 10,240 | statement | 556 | 557 | 3,858,355,530 | 10.05 |

Blocks are reported per statement, not per scan: 12,511 of 12,511 at every
K. That is the block count `journey_b`, a logs-only statement over the
same hour, scans, so the figure counts logs blocks. Mixed state: 601 requests
and 3,877,200,781 B. Unfolded: 1,356 requests, 803 of them spans resolve
GETs. Source for every figure:
`s3://ravel-inv-3692-d-runs/investigations/runs/investigations-20261106T154511Z-47463-23195/out/calls.jsonl`.

The headline is that the build side's size changes nothing. The logs scan
reads every object and every block whether the join keeps 1 trace or
10,240.

### Stage 3 figures

The receiving arm's own cost bounds what transfer can save. The same run
measured it as literal `trace_id IN` lists on logs, folded with leaves.
These are the "set at Stage 0" ratios, taken against the Journey C logs
scan above (12,511 blocks, 3,806,990,963 B):

| Receiving logs scan, transfer on against off | band | miss | source |
|---|---|---|---|
| blocks, K = 1 | at most 0.0013 of off (16 of 12,511, the 1-key list) | not fewer than off | s3://ravel-inv-3692-d-runs/investigations/runs/investigations-20261106T154511Z-47463-23195/out/calls.jsonl (`logs_trace_in_1`) |
| wire bytes, K = 1 | at most 0.0013 of off (4,835,567 B) | over 0.01 of off | s3://ravel-inv-3692-d-runs/investigations/runs/investigations-20261106T154511Z-47463-23195/out/calls.jsonl (`logs_trace_in_1`) |
| blocks, K = 102 | at most 0.20 of off (2,425 of 12,511 at 100 keys) | not fewer than off | s3://ravel-inv-3692-d-runs/investigations/runs/investigations-20261106T154511Z-47463-23195/out/calls.jsonl (`logs_trace_in_100`) |
| wire bytes, K = 102 | at most 0.28 of off (1,051,558,209 B at 100 keys) | over off | s3://ravel-inv-3692-d-runs/investigations/runs/investigations-20261106T154511Z-47463-23195/out/calls.jsonl (`logs_trace_in_100`) |
| blocks, K = 1,024 | at most 0.87 of off (10,780 of 12,511 at 1,000 keys) | not fewer than off | s3://ravel-inv-3692-d-runs/investigations/runs/investigations-20261106T154511Z-47463-23195/out/calls.jsonl (`logs_trace_in_1000`) |
| wire bytes, K = 1,024 | at most 0.95 of off (3,607,235,510 B at 1,000 keys) | over off | s3://ravel-inv-3692-d-runs/investigations/runs/investigations-20261106T154511Z-47463-23195/out/calls.jsonl (`logs_trace_in_1000`) |
| every figure, K = 10,240 | equal to off: above k, no transfer | any difference | predicted (decision 5) |
| `keyIndex` requests, K at most k | at most the literal list of K keys, measured in the same Stage 3 run | over it | predicted; probe shape from `logs_trace_in_1` (2 leaf GETs), `_10` (11) and `_100` (1 leaf GET, part skipped by budget, then 1,041 tier-1 GETs) in calls.jsonl above |

K = 102 and K = 1,024 are compared with the 100- and 1,000-key lists, the
nearest sizes this run measured. Stage 3 runs literal lists of exactly
102 and 1,024 keys beside the join, and judges each band against its own
twin.

One observation bears on k, and Stage 3 decides it. At 100 keys and above,
the logs `trace_id` leaf, at 13 bits and 20,000,000 entries, is skipped by
budget (`skippedByBudget` 1). The tier-1 probe then reads every object's
prefix. At 1,000 keys the arm saves 5 % of the wire bytes on this corpus.
If Stage 3 confirms this, k = 1,024 admits transfers that cost a reservation
and buy little.
