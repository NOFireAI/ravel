# ADR-4203: ClickBench parity with ClickHouse: plan-phase bytes and loopback store fan-out that do not grow with cores, and a profiled bulk load

- Status: Accepted (2026-11-14)
- Date: 2026-11-14
- Refs: #4203, #3714, #3217, #3231, #3081, #3072, ADR-0102, ADR-0107,
  ADR-0109, ADR-2614, ADR-3217

## Context

On the ClickBench entry (v0.32.0, 222 objects, 8.33 GB, loopback s3s-fs on
a gp2 volume) Ravel's cold total is 103.5 s on c6a.4xlarge and 219.9 s on
c6a.metal. ClickHouse goes from 95.1 s to 73.9 s on the same pair. Four LIKE
statements (Q21-Q24) account for 107.4 s of Ravel's 116.4 s regression.
Measured as ClickBench scores it (geometric mean over 43 statements of
`(ravel + 0.01) / (clickhouse + 0.01)`), the cold ratio is 1.35 on
c6a.4xlarge, 2.25 on c6a.metal, 2.09 on c7a.metal-48xl, 1.15 on c8g.4xlarge
and 2.03 on c8g.metal-48xl. Bulk load takes 397 to 666 s against
ClickHouse's 106 to 183 s and does not get faster with cores.

Three facts in the code produce the large-machine regression.

1. `resolve_performance_defaults`
   (`services/ravel-server/src/config.rs:6058-6079`) derives SQL partitions,
   store GET permits and the PromQL fan-out from one value,
   `max(8, 2 * cores)`: 32 on 16 vCPU and 384 on 192 vCPU.
2. `LogsScanExec::whole_segment_fast_path`
   (`crates/ravel-sql/src/logs_scan/mod.rs:2945-2972`) rejects a scan with
   fewer relevant segments than partitions (`mod.rs:2968-2970`). 222
   objects pass at 32 partitions and fail at 384, so on a large machine
   every logs scan takes plan-then-stripe.
3. The SQL route check and the fetcher's plan disagree about `Contains`.
   `route_predicate_free` (`mod.rs:1431`) treats a Contains-only content
   list as neutral. `LogSegmentFetcher::plan_segment_primed`
   (`crates/ravel-query/src/log_fetcher.rs:2277`) does not: its two
   directory-only branches require no content arm, so a Contains statement
   falls to the fallback (`log_fetcher.rs:2439`), which fetches with
   `ColumnSelection::all()`. No index covers Contains, so the plan pass
   moves every column of every object, 384 reads at a time, to learn a
   survivor count that no Contains arm can reduce. On objects above the
   whole-object bound that read is ranged and is not carried to the scan
   (`log_fetcher.rs:2484-2497`).

What is not yet known is which consequence costs the 31 to 35 s: the bytes
at the volume's throughput cap, or a connection stall from a 384-wide burst
against a store whose pool is empty on every cold statement. Issue #3714
measured 169 listen overflows and 1,743 body-read retries at 384 on
c6a.metal, and c8g.metal finishes sooner than the read alone could. The
decision below does not depend on the answer, because it removes both the
bytes and the uncapped burst; Stage 0 measures which one carried the time
so the figures in Consequences can be checked.

On c6a.4xlarge the cold ratio is carried by two statements: Q2 at 41x and
Q1 at 7.7x (0.485 s and 0.082 s against 0.002 s). Without them the ratio is
1.18; with Q8, Q28, Q30 and Q18 also at parity it is 1.04.

On load: 581 s on c6a.4xlarge against 165 s for ClickHouse, and 562 s on
c6a.metal. The loader decodes every read cursor on one blocking thread
(`services/ravel-cli/src/load/logs.rs:1509`), encodes each flush inline
with pages sealed serially because no write CPU gate is attached
(`logs.rs:493-503`, `crates/ravel-ingest/src/write_gate.rs:184`), and
selects `PageChoice::Exhaustive` (`logs.rs:2143`). The last load profile is
ADR-0109's, taken in August on the row path. ADR-2614 removed dictionaries
from the batch on purpose, and ADR-3217 showed that changing object
geometry costs load time. So the load has a history of changes that looked
right and measured flat.

## Target

Parity means, on each of the six published machines:

- cold geometric-mean ratio against ClickHouse at or under 1.10, and
- cold total at or under ClickHouse's, and
- load time at or under ClickHouse's (106 to 183 s by machine).

Milestones, each checked by a pre-registered run on the epic:

| Milestone | Bar | Basis |
|---|---|---|
| M-A | c6a.metal cold total at or under 115 s | predicted from per-query.tsv on #4203 (Q21-Q24, Q25, Q27 at their c6a.4xlarge times) |
| M-B | c6a.4xlarge cold ratio at or under 1.18 | predicted from per-query.tsv on #4203 (Q1 and Q2 at parity) |
| M-C | c6a.4xlarge cold ratio at or under 1.10 | predicted from per-query.tsv on #4203 |
| L-A | c6a.4xlarge load at or under 250 s | predicted; replaced by the Stage 0 load profile |
| L-B | c6a.4xlarge load at or under 165 s | measured 2026-11-14 (ClickHouse's published load, results 20261114) |

What this architecture cannot reach, stated so the target is not read as a
per-statement promise:

- **Q1 and Q2 within 10% each.** ClickHouse answers Q1 in 0.002 s, so the
  per-statement bar is 0.0032 s with the 0.01 shift. A restarted Ravel
  server resolves the tenant's catalog from object storage before it can
  answer: about 27 ms hot today (five LISTs and the cached catalog records,
  #3231) and 0.082 s cold. At 27 ms the shifted ratio is 3.1x. Q2 has the
  same floor plus one read. Two statements at 3.1x add a factor of 1.054 to
  the geomean, so the other 41 must average 1.04 or better for 1.10. Parity
  here is a geomean and total target, not every statement.
- **c6a.metal cold total at or under 73.9 s and load at or under 165 s.**
  Not shown reachable by anything measured so far. Both are marked unknown
  until the metal confirmation run and the Stage 0 load profile, rather
  than predicted.

## Decision

1. **A Contains-only content list is plan-neutral in the fetcher, as it is
   in the SQL route check.** The new branch of `plan_segment_primed` applies
   only when all of these hold: the content list holds Contains arms and
   nothing else, there is no stream-attribute arm, the prune list is empty
   or decidable from the skip step, and the object is above
   `plan_whole_object_bound()` (at or under the bound the scan already reads
   the whole object once). Then it plans from the footer and directories:
   the survivor list is the skip-index candidates for the time window and
   any skip-step prune arms, and no BLOCKS byte is read in the plan phase.
   The carried footer and directories go to the subset opens as they do for
   the other directory-only branches. The scan evaluates Contains per row,
   as it does on the fast path today. Any other content list keeps the
   fallback.

   The task must prove the survivor list equals the reader's own for a
   Contains query, including on a tenant with bloom scope `text`. If the
   reader can drop a block for a Contains arm, the plan uses the reader's
   prune over the directories instead of the skip step alone.

2. **The rejection for fewer segments than partitions stays unless Stage 0
   says otherwise.** For objects with several row groups, striping is what
   lets a large machine use its cores on few objects, and the route is
   correct once decision 1 makes its plan pass cheap. The entry's objects
   have one row group each, so on that tenant striping has no finer unit
   than the object and the rejected route only adds the plan barrier. If
   the Stage 0 boundary pair shows Q25, Q27 and the hot small statements
   stepping at the object count, a scan whose relevant objects all have one
   row group takes whole-object assignment over
   `min(partitions, objects)` partitions, as an amendment to this ADR with
   its own task.

3. **The derived store GET concurrency is capped at 64 when the store is a
   loopback endpoint.** `flags.store_is_loopback` already exists
   (`config.rs:5649`); its doc comment, which says only the fetcher cache
   reads it, is updated with the change. The cap applies only to the
   derived value; an explicit `--store-get-concurrency` or
   `--fetch-concurrency` is used as given. SQL partitions keep their own
   derivation and `promql_fetch_fanout` is unchanged. The value 64 is not
   yet measured at 384 partitions: the #3714 reading at 64 permits also cut
   partitions to 64, which restored the fast path, and Stage 0 runs 32 and
   384 permits only. The metal confirmation run is the first measurement of
   64 permits at 384 partitions; a different value goes in as an amendment.
   Real S3 is not capped here; #3714 keeps that question.

4. **Under an ascending-ts TopK the first round of opens is bounded
   independently of the partition count.** Specified only if Stage 0 shows
   `blocks_scanned` for Q25 rising with partitions.

5. **The parity work outside decisions 1 to 4 and 6 is routed, not
   dropped.** Each item names its home and the figure it must move:

   | Item | Figure | Home |
   |---|---|---|
   | Cold resolve floor after a restart | Q1 0.082 s against 0.002 s | #3231 |
   | Narrow-column cold read | Q2 0.485 s (41x), Q8 | ADR-3217 amendment, #3217 |
   | String length from offsets | Q28 5.415 s against 0.792 s | ADR-3217 amendment, #3217 |
   | Q30 cold | from the per-phase split | ADR-3217 amendment, #3217 |
   | Q18 string GROUP BY | from the per-phase split | #3231 |
   | Plan streamed into the scan | 120 ms barrier, hot geomean 1.20x to 1.70x on metal | #3081, #3072 |
   | Partition width per query | 162 of 384 partitions empty; Q43 48 to 204 ms hot | #3231, after #3081 |
   | Cold on metal uses its cores | 40 of 43 statements about 10% slower on metal | amendment to this ADR after the metal run |

6. **No load change is specified until a profile of the current loader
   exists.** Stage 0 produces five numbers (decoder busy share, encode CPU
   share and core use, CPU on discarded candidates, CPU in
   `build_columnar_batch`, non-loader seconds). Then, in this order and
   only where the number supports it:
   - decode each cursor on its own blocking task, merged in the existing
     deal order so batch composition is unchanged;
   - attach the write CPU gate so pages of one flush encode in parallel;
   - choose page encoding by estimate for bulk load if stored bytes stay
     within the band and cold query bytes do not grow;
   - derive loader shards, cursors and flush concurrency from cores;
   - parallelise the flush sort and content hash if they exceed 15% of
     load CPU;
   - remove non-loader seconds from the timed load if they exceed 20 s.

7. **No persistent format changes.** Page choice selects among encodings
   the format already defines (`docs/log-segment-format.md:737-738`:
   readers are unaffected by which choice wrote an object). That passage
   also names `Exhaustive` as the loader's choice, so the page-choice task
   edits it; it is a writer note, not a format change. Decode workers and
   the write gate change who runs the work, not what is written; with the
   same page choice the objects must be byte-identical to today's.
   Dictionaries are an in-memory batch representation. No RLOG layout,
   proto, key layout or commit token is touched, so no version bump.

```mermaid
flowchart TD
    Q[Logs scan with a Contains arm] --> R{route_predicate_free and<br/>every segment inside the window}
    R -- no --> P[Plan pass]
    R -- yes --> C{relevant segments >= partitions}
    C -- "yes: 222 >= 32" --> F[Whole-segment route<br/>no plan pass, projected columns only]
    C -- "no: 222 < 384" --> P
    P --> B{fetcher plan branch}
    B -- "no predicate, contained" --> D1[Footer and directories only]
    B -- "skip-step prune arms" --> D2[Footer and directories only]
    B -- "Contains only, today" --> X[Fallback: ColumnSelection all<br/>every column of every block]
    B -- "Contains only, decision 1" --> D3[Footer and directories only]
    D1 --> S[Stripe blocks over partitions<br/>projected columns only]
    D2 --> S
    D3 --> S
    X --> S2[Stripe: scan fetches projected columns again<br/>unless the read cache holds them]
```

```mermaid
flowchart LR
    subgraph today
      PQ[Parquet file] --> DEC[One blocking thread:<br/>16 cursors in turn,<br/>decode + build batch]
      DEC -->|queue of 2| SUB[Submit loop,<br/>depth 56]
      SUB --> SH[4 shard actors:<br/>partition + merge]
      SH --> FL[Up to 16 flushes:<br/>sort, encode pages serially,<br/>exhaustive choice, blake3]
      FL --> PUT[Async PUT] --> CM[Commit publish,<br/>acks oldest first]
    end
    subgraph decision 6
      PQ2[Parquet file] --> DW[One blocking task per cursor]
      DW -->|merged in deal order| SUB2[Submit loop]
      SUB2 --> SH2[Shard actors]
      SH2 --> FL2[Flushes: pages encoded<br/>through the write CPU gate,<br/>estimated choice if measured]
      FL2 --> PUT2[Async PUT] --> CM2[Commit publish,<br/>acks oldest first]
    end
```

## Rejected alternatives

- **Drop the fewer-segments rejection and assign whole objects when objects
  are fewer than partitions.** It would fix this tenant, and on this
  geometry (one row group per object) striping has no finer unit anyway.
  Rejected as the primary fix because the all-column plan read would remain
  for every Contains statement that leaves the fast path for another reason
  (a partial time window, a pending erasure, a mixed predicate), because
  four test files pin the rejection as the contract of ADR-0102, and because
  objects with several row groups can stripe where one-group objects
  cannot. It is kept as a conditional follow-up (decision 2), limited to
  objects with nothing finer to stripe.
- **Change the derived partition count (cores instead of 2 x cores, or a
  fixed ceiling).** It moves the cliff to a different object count. A
  tenant with 100 objects would meet it on a 64 vCPU box.
- **Cap GET concurrency everywhere.** No measurement on a real object store
  at 384 permits exists. A cap without one trades a local-store problem for
  a possible regression on the stores customers run. #3714 owns that.
- **Size partitions per query from surviving objects, projected bytes and
  CPU cost, in this ADR.** This is the right long-term shape and is too
  large to specify from one published table. The plan barrier (#3081)
  changes the same code and has to land first. Routed to #3231 (decision
  5).
- **One ADR for all parity work.** ADR-3217 is accepted with its own
  measured geometry history, and #3081 changes the same files as decision
  4. A hub decision that keeps growing stalls the work that depends on it
  (ADR-2867 change 4). Decision 5 routes each item instead.
- **Tell the entry to pass `--fetch-concurrency 64`.** The entry is
  published as untuned, and the defect is in the defaults.
- **Carry Parquet dictionaries into the writer.** ADR-2614 dropped them for
  memory accounting, and `partition_columnar` discards them
  (`crates/ravel-ingest/src/log_router.rs:1234`). No measurement of a
  dictionary-carrying load exists. Reopened only if the profile puts
  `build_columnar_batch` above 15% of load CPU.
- **Parallelise the load now, from the code reading.** The same reasoning
  produced the columnar path, which measured flat. The profile comes first.
- **Raise zstd level or change object geometry to shrink cold reads.**
  Epic #3217 measured both: zstd level 9 took the load from 927 s to
  1,152 s against the same-wave baseline, and 1M-row one-group objects took
  it to 1,388 s against a band of 840 to 1,100 s.

## Consequences

- For objects above the whole-object bound, a Contains statement's
  plan-phase bytes no longer depend on whether the object count is above or
  below the partition count. Objects at or under the bound are read whole
  once by the scan and are not affected. A test pins this across the
  boundary through the server's SQL entry point.
- Large machines keep the striped route, so they still pay the plan
  barrier's directory reads (about 120 ms hot on 407 objects, #3231) until
  #3081 removes it.
- Expected on c6a.metal after decisions 1 and 3: Q21-Q24 near their
  c6a.4xlarge times, cold total at or under 115 s (M-A) from 219.9 s, cold
  ratio about 1.8 from 2.25.
- c6a.4xlarge cold is unchanged by decisions 1 to 4. Its ratio moves only
  with the items decision 5 routes to #3217 and #3231 (M-B, M-C).
- Expected load after the decision 6 changes the profile supports: 300 to
  400 s on c6a.4xlarge, low confidence until the profile exists. L-B (165
  s) is not shown reachable.
- A loopback deployment on a large machine issues fewer concurrent GETs
  than before. Hot statements that were GET-bound at 384 may slow; the
  metal confirmation run checks hot total and geomean against the published
  figures.
- Load changes that alter object count move every object-count boundary.
  Each load task pins rows, object count and bytes in a band, and a task
  that changes stored bytes (page choice) re-runs the cold statements
  before it lands.
- The published load and size figures change if page choice changes; the
  entry is re-measured and the docs that quote those figures are updated in
  the same pull request.
- The `--read-cursors` help text says cursors read in parallel; it is
  corrected by the task that makes it true.
