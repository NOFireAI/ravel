# ADR-2983: Predicate transfer across signal joins

Status: Accepted (2026-10-15). Not implemented: no code before the gates in
Sequencing are met.
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
one it does nothing with. The spans scan implements no such hook.

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
   receives.

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
run.

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
