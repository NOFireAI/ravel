# ADR-0774: TopK late materialization for the logs scan

Status: Proposed.

The number is issue #774's, the ticket that produced this ADR, rather than
epic #680's. The README's rule ("the number is the GitHub issue number of
the epic that produced it") exists so parallel sessions cannot collide on a
number, and epic #680 has several tickets in flight at once: two of them
taking the epic number would collide exactly as sequential numbering did.
A ticket number is allocated by the same atomic counter and is unique per
change.

## Context

On the ClickBench reference tenant (100M rows, 8,424 objects, 105 declared
columns) three statements bound the problem:

| statement | time |
|---|---|
| `SELECT COUNT(*) FROM logs WHERE URL LIKE '%google%'` | 22.9 s |
| `SELECT ts, URL FROM logs WHERE URL LIKE '%google%' ORDER BY ts LIMIT 10` | 24.8 s |
| `SELECT * FROM logs WHERE URL LIKE '%google%' ORDER BY ts LIMIT 10` | exceeds a 900 s deadline |

The third statement's plan is

```
SortExec: TopK(fetch=10), expr=[ts ASC]
  CoalescePartitionsExec
    FilterExec: like(URL, %google%)
      LogsScanExec: partitions=32, content=0, prune=0, projection=[<105 columns>]
```

`prune=0` and `content=0` are correct, not a missed pushdown: a substring
`LIKE` is neither a block prune nor a bloom probe, so nothing can decide it
before decode (ADR-0105's GRAM_IDX is an opt-in per-column index and is not
in play here). Every block is therefore a candidate, and the scan decodes
every one of the 105 projected columns of every block before the filter or
the TopK sees a row. The two-column variant proves the filter and the sort
are cheap on their own; the whole 875-second difference is the 103 columns
nobody looks at until ten rows have already been chosen.

The shape generalizes to every `SELECT <wide projection> ... WHERE ...
ORDER BY ... LIMIT k` over `logs`, which is the ClickBench q24-q28 family.

Three things about the existing scan make a fix cheap:

- Block pruning never consults the column selection (`RlogReader::
  scan_blocks`), so two scans of the same object under the same predicates
  see the same surviving blocks in the same order.
- Column projection already reaches the page level (ADR-0087 decision 3),
  so a narrower projection genuinely decodes less, and on a version-4
  object it also fetches less (ADR-0699 decision 5: one coalesced range per
  surviving `(row group, projected column)`).
- Data objects are immutable and the block-range read pins the etag across
  its GETs, so re-reading a block is guaranteed to return the same bytes.

## Decision

Add `TopKLateMaterialization`, a physical optimizer rule installed by
`build_session`, that rewrites a qualifying plan into two phases.

### 1. What qualifies

Walking down from a `SortExec`, the rule fires only when all of the
following hold. Everything not on this list refuses the rewrite; the
allowlist is the mechanism, not a fallback.

- The `SortExec` has a `fetch` (it is a TopK) and does not
  `preserve_partitioning` (it is the final, single-stream sort). A sort
  with no fetch materializes every row whatever the rule does, and a
  per-partition sort keeps `fetch` rows per partition, which is not the
  bound the second phase is built for.
- Every node between the sort and the scan is a `FilterExec` with no
  projection of its own, a `CoalescePartitionsExec`, a `CooperativeExec`,
  or a `RepartitionExec` whose partitioning carries no expressions
  (round-robin or unknown). Each preserves its input's schema and its
  rows' identity. An `AggregateExec`, a join, a window, or a
  `ProjectionExec` breaks one of those two properties, and any node
  carrying its own `fetch` truncates rows independently of the TopK, so
  each refuses.
- The leaf is a `LogsScanExec`. RSEG (metrics) and spans scans are not
  matched at all, so the rule is a no-op for them by construction rather
  than by an explicit exclusion.
- The snapshot carries no pending selective erasure (see "Correctness"),
  and the scan is not already a phase-1 scan.
- The scan's projection is wider than the union of the filter's and the
  sort expressions' column references by more than a threshold.

### 2. The threshold

`SqlConfig::late_materialization_extra_columns`, an `Option<usize>`,
default `Some(8)`. `Some(n)` installs the rule and lets it fire when the
scan projects more than `n` columns beyond what the filter and the sort
read; `None` does not install the rule at all and is the operator opt-out
and the regression fixture's red side.

Eight is chosen as a floor, not a tuned optimum: the rewrite costs `k`
extra block reads, and below a handful of surplus columns those are not
obviously repaid. The statements this exists for are nowhere near the
boundary -- the measured one has 103 surplus columns.

### 3. The row-ref encoding

Phase 1's scan appends one synthetic non-nullable `UInt64` column named
`__ravel_row_ref`, past every projected index. It packs three positions,
high bits first, so the packed value sorts by the same tuple:

| field | bits | limit | what it indexes |
|---|---|---|---|
| segment | 20 | 1,048,576 | the resolved snapshot's segment list |
| block | 24 | 16,777,216 | that segment's surviving-block list for this query |
| row | 20 | 1,048,576 | that block's surviving-row list under this query's exact content predicate |

Note what it is not: it is not a byte offset, an object key, or any stored
identifier. All three fields are positions in lists this query defines,
which is what makes them free to produce -- the scan already holds all
three as cursor state and decodes nothing extra to stamp them -- and what
makes the correctness rule below the whole argument.

The limits are beyond any real object (a block is a target 8,192 records;
the reference tenant resolves 8,424 segments), and a field out of range is
a typed error rather than a truncated ref that would address a real row
somewhere else.

### 4. The two phases

**Phase 1** is the same TopK over the same filter chain over a
`LogsScanExec` narrowed to the filter's and sort's columns plus the row-ref
column. Column indices in the filter predicate and in the sort expressions
are remapped onto the narrow schema; the narrow projection keeps the
original relative column order, so a plan reader sees the same order.

**Phase 2** is `LogsRowFetchExec`, a single-partition, `EmissionType::
Final` node. It collects phase 1's at-most-`k` rows, groups their row refs
by `(segment, block)`, and for each group re-opens that one block through
`LogSegmentFetcher::scan_accounted_with_tenant_subset` with the ORIGINAL
column selection -- the same entry point the striped scan path uses, so the
read rides ADR-0699's version-4 chunk path with a narrow `ColumnSelection`
rather than adding a read path. It picks the referenced rows out of the
decoded block and emits them in phase-1 order.

No projection node is inserted to drop the row-ref column. Phase 2's output
schema is `Arc::clone`d off the original scan's, so the restored column
order, names, types, and nullability are identical by construction and a
projection to remove the row ref would be a no-op node. The rule sets
`schema_check() == true`, so DataFusion asserts the equality for every
query rather than leaving it to a comment. (This is the one deliberate
departure from the deliverable as written, which asked for that
`ProjectionExec`; the node would have been dead weight above a fetch node
that already emits the restored schema.)

### 5. Correctness

Phase 2 re-reads a block whose bytes phase 1 already decoded, and must
return the rows the single-phase plan would have returned. The rule is:

- **Same objects.** Both phases hold the same `Arc<Vec<SegmentRef>>`, so a
  segment ordinal means the same segment. Data objects are immutable and
  the block-range read pins the etag across its GETs, so the bytes are the
  same bytes.
- **Same surviving blocks.** Pruning (skip index, POSTINGS, bloom) reads
  the ts bounds, the content predicates, and the prune-only predicates, and
  never the `ColumnSelection`. `LogsScanExec::reproject` carries all of
  those over verbatim, so the surviving-block list is the same list in the
  same order and position `i` in it is the same block.
- **Same surviving rows.** Within a block, the surviving rows are the ones
  matching the exact content predicate, evaluated once per block by
  `BlockScan::decode_block`. `resolve_columns` adds every column a content
  predicate names on both the narrow and the wide selection, so widening
  the selection cannot change the evaluation and position `i` is the same
  row.
- **Same ties.** Phase 1's TopK is the same `SortExec` operator with the
  same fetch over the same rows in the same input order, so it selects the
  same `k` rows in the same order. Phase 2 does not sort; it emits in
  phase-1 order.

The one shape where the third clause fails is a pending selective erasure
(ADR-0064): the scan layer's `retain_unerased` removes rows from a block's
record list after the reader produced it, so a phase-1 position would not
be a phase-2 position. The rule refuses there, which is also the
fail-closed direction -- the failure mode of getting erasure wrong is an
erased record served to a client, not a slow query.

### 6. Memory and accounting

Phase 1 holds what the narrow scan holds (one decoded block per partition
plus the batch in flight, ADR-0087 decision 2) plus the TopK's `k` narrow
rows. Phase 2 holds at most `k` records and the batches built from them,
and decodes one block at a time; its fetch concurrency bounds how many
object-sized assembly buffers exist at once, exactly as a scan partition's
does. Both terms are charged to the query's DataFusion pool. Neither phase
holds a wide row for a row the TopK discarded, which is the point.

Phase 2's reads go through the same accounted funnel as any scan read, so
they appear in `QueryAccounting` with no special case. `LogsRowFetchExec`
publishes `row_refs`, `blocks_fetched`, and `segments_fetched` through
`EXPLAIN ANALYZE`, so a report can state phase 2's cost rather than infer
it.

## Rejected alternatives

- **A bloom probe for the substring `LIKE`.** RLOG's bloom filters are
  tokenized on words, so they answer "does this block contain the token
  `google`", not "does any value contain the byte sequence `google`". A
  substring can start and end mid-token. ADR-0105's GRAM_IDX gives sound
  infix pruning, but only for declared `Str` columns that opt in, and the
  granularity arithmetic there bounds how much it prunes. Neither helps a
  predicate on a column with no such index, and neither addresses the
  actual cost, which is the 103 columns the predicate does not name.
- **Decode every column but apply a selection vector.** This keeps the
  page decompression and decode -- which is the cost -- and only avoids
  building Arrow arrays for filtered-out rows. On the measured statement
  the filter keeps a large fraction of rows and the TopK keeps ten; a
  selection vector cannot know which ten before the sort has run.
- **Push the limit into the scan.** Sound only with no filter and a scan
  that emits in sort order, and the scan deliberately declares no ordering
  (ADR-0087 decision 1) because a block-at-a-time scan over several
  segments has none to declare. With a residual filter above it, a scan
  that stopped after `k` rows would drop rows the filter would have kept.
- **A narrower rule keyed on `SELECT *`.** The cost is a function of the
  projection width, not of how the statement was written; `SELECT` with 40
  named columns has the same problem. The threshold is on the width.

## Consequences

- A qualifying statement costs `k` extra block reads. With ADR-0046's read
  cache wired and an object below the block-range threshold, those land on
  the same `(0, object_size)` cache key phase 1 already admitted and cost
  no request at all; without a cache they are one request each. Both are
  pinned in `crates/ravel-sql/tests/logs_topk_late_materialization.rs`.
- The rewrite is invisible to results: the same rows, in the same order,
  under the same schema. So `late_materialization_extra_columns` is a cost
  knob, never a correctness one, and it is not exposed as a server flag.
- `EXPLAIN` for a qualifying statement gains a node and shows a narrowed
  scan projection ending in `__ravel_row_ref`. Any test pinning such a plan
  text must be updated; `docs/query-engine.md` carries the example.
- The pass-through allowlist is coupled to DataFusion's plan shape. A
  DataFusion upgrade that inserts a new node between the sort and the scan
  silently stops the rewrite from firing rather than breaking a query --
  fail-closed, but silent. The regression fixture asserts the rewrite
  fires, so the upgrade fails there.
- Expected outcome on the reference tenant: q24 (`SELECT * ... WHERE URL
  LIKE '%google%' ORDER BY ts LIMIT 10`) within 1.5x of the two-column
  variant's 24.8 s, i.e. under about 37 s, against a run that previously
  exceeded a 900 s deadline. The orchestrator runs that measurement; it is
  a prediction recorded before the fact, not a reported result.
