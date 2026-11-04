# ADR-3819: The RLOG merge reads each input once

- Status: Accepted (2026-11-04)
- Date: 2026-11-04
- Refs: #3819, #3492, #3802, #2861, ADR-0018, ADR-0032, ADR-0065, ADR-0699, ADR-0979, ADR-2135

## Context

An RLOG L0 to L1 merge reads each input row group once per stream it carries.
On a many-stream logs tenant that makes compaction about 2,000 times more
read-heavy than its input, and slower than ingest by orders of magnitude.

**Where it was found.** The logs-benchmark publication runs (#3492) left a
GCS bucket of L0 objects that `--mode all` never compacts. ravel-server 0.30.1
`--mode maintain` was run on it (#3802 made that possible on GCS). The bucket:

- 122,344 L0 objects, 69,970 MiB, one tenant, 2 shards, 5 ingest hours;
- 5,000 streams (125 hosts × 40 apps);
- about 17,000 L0 objects per shard in ingest hour 07 (17,066 and 17,120
  commit records).

After 72 minutes, two merges (hour 07, shards 0 and 1) had issued 461,608 GETs
for 48.8 GB and written four L1 parts of 16 MiB in total, at 0.17 to 0.36
cores (#3819).

**Why.** `merge_catalogs` walks `for stream_id in merged.keys()`
(`crates/ravel-maintain/src/rlog.rs:1136`) and finishes each stream across all
of its inputs before the next one starts. For each stream it opens a new
`StreamCursor` on every input that carries that stream. The cursor GETs the
stream's location (`rlog.rs:2549`), and a location names the whole row group
(`crates/ravel-logseg/src/ranged.rs:85-95`, ADR-0699 decision 1), so every
stream's cursor downloads the same bytes. Nothing keeps them between streams.
The writer cuts blocks on a record count or a byte estimate, never at a stream
boundary (`crates/ravel-logseg/src/writer.rs:1646-1666`), so each block holds
rows from many streams.

| L0 object (`ravel-cli rlog inspect`) | records | blocks | streams |
|---|---|---|---|
| 98,886 bytes | 951 | 1 | 804 |
| 399,804 bytes | 4,966 | 1 | 2,150 |
| 674,188 bytes | 8,988 | 2 | 2,411 |
| 875,743 bytes | 11,993 | 2 | 2,458 |
| 1,093,189 bytes | 15,843 | 2 | 2,471 |

A shard-hour of N inputs carrying S streams each costs about N × S row-group
GETs where N would do. With no sort descriptor (`sort_descriptor: none` on
every object above), the frontier admits about one cursor at a time, and
the emit loop refills one `.await` at a time (`rlog.rs:3159`), so each merge
has about one GET in flight.

**Why no test caught it.** Every compaction test fixture and the ClickBench
logs tenant carry one stream, or a handful, per object. With S near 1, N × S
is N.

### Figures

| figure | value | source |
|---|---|---|
| streams per L0 object | 804 to 2,471 | five objects inspected, #3819 comment 6637559313 |
| GETs and bytes after 72 min, two merges | 461,608 GETs, 48.8 GB | #3819 |
| GETs per shard-hour merge at S = 2,000 | about 34 million (model) | N × S with N = 17,000 |
| GET rate per merge, this run | about 53 GETs/s, catalog GETs included | 461,608 GETs / 4,320 s / 2 merges, #3819 |
| merge wall time per shard-hour at that rate | about 180 h (model) | 34 million / 53 |
| decoded (row heap estimate) over stored, L1 output | 55 to 67 | two L1 parts closed on the 269,565,184-byte memory target at 4,933,909 and 4,024,942 bytes |

## Decision

The merge emits exactly the record sequence it emits today, so every L1 part
keeps its bytes and its content-addressed key. Only how inputs are read
changes.

```mermaid
flowchart LR
  subgraph today[Today]
    S1[stream 1] --> I1[GET all N inputs]
    S2[stream 2] --> I2[GET all N inputs again]
    S3[stream ... S] --> I3[GET all N inputs again]
  end
  subgraph after[This ADR]
    F[fetch phase: GET each input once, k in flight] --> R[(raw row groups, charged to the merge budget)]
    R --> P1[pass 1: decode each block once, keep streams in range 1]
    R --> P2[pass 2: decode each block once, keep streams in range 2]
    P1 --> E[emit in today's order: stream, then ts bucket, then input]
    P2 --> E
  end
```

### D1. Fetch each input's row groups once, concurrently

After the catalog phase, the merge fetches every input's row groups once with
`k` GETs in flight. `k` is the existing `input_read_concurrency`. The raw bytes
stay resident for the whole merge and are charged at fetch time to
`merge_cursor_budget_bytes`, the same running charge `StreamCursor`
reservations use today (`crates/ravel-maintain/src/rlog.rs:2942, 2978`).

The merge takes the resident path only when one sum fits a resident ceiling:
the inputs' raw bytes, plus the pass budget's floor (D2), plus the worst-case
cursor charge of any range that will use D3. The worst case is the largest
D3 range's cursor reservations, with their raw term counted once, since those
cursors read the resident bytes. Otherwise the merge fetches per pass (D2), so
each input row group is fetched once per pass. Going over the budget during a
merge is a hard abort today (`MergeCursorBudgetExceeded`,
`crates/ravel-maintain/src/error.rs:233`), so the choice is made before any
block is fetched, from the PAGE_DIR sizes the catalog phase already holds,
and the merge logs which path it took and the sum it compared.

The resident ceiling is a new resolved input on `CompactorConfig`,
`merge_resident_ceiling_bytes`. The merge reads it and computes nothing
about concurrency itself. Each entry point resolves it from what it already
knows, the way ravel-server resolves the part memory target
(`services/ravel-server/src/config.rs:8605-8620`):

| entry point | concurrent merges | `merge_resident_ceiling_bytes` |
|---|---|---|
| `ravel-server --mode maintain` | `--maintain-unit-concurrency` (default 4) | `merge_cursor_budget_bytes` / that concurrency: 5 GiB at the defaults |
| `ravel-cli maintain compact-tenant` | `--bucket-concurrency` | the per-bucket `merge_cursor_budget_bytes` it already sets (`services/ravel-cli/src/maintain.rs:823`), with no second division |
| `ravel-cli maintain compact-bucket` | 1 | `merge_cursor_budget_bytes` |
| library default | 1 | `merge_cursor_budget_bytes` |

The budget is per merge, and ravel-server passes it once for all of its
concurrent merges (`crates/ravel-maintain/src/config.rs:659-664`). Without
the division, four merges could each hold their inputs resident. This
bounds the resident term per process. It is not a cross-merge admission
scheme: a shared budget that merges wait on is #3825.

### D2. Emit in stream-range passes, decoding each block once per pass

The merge splits its sorted stream ids into contiguous ranges, each sized so
that its retained rows fit the pass budget (defined below). For each range, it decodes every
input's blocks once, from the resident bytes or from one fetch, and keeps the
rows of the range's streams in columnar form (`StreamBlockRows`, ADR-0979).
It then emits those streams with today's k-way merge, in today's order:
stream id, then `ts.div_euclid(W)`, then canonical input order. Part cuts
(`PartSink`, the memory split target and the stored-size target) run on that
sequence unchanged.

On clustered inputs (W above 1), today's merge splits a stream whose
per-bucket reservation sum exceeds `merge_cursor_budget_bytes` into several
batches in input order (ADR-2135 decision 2, `reservation_batches`,
`crates/ravel-maintain/src/rlog.rs:2594-2644`), and batching changes which
records share a part (`rlog.rs:2904-2913`). D2 reproduces those batches
exactly: it computes them per stream from the same reservation inputs, and
against the same `merge_cursor_budget_bytes` value as today, never against
the budget less the resident term. The batches then run in today's order.

A range's size comes from per-stream record counts that the catalog phase
reads, multiplied by a decoded-bytes-per-record estimate. Measuring that
estimate on this fixture is the first task. The row-heap ratio of 55 to 67 is
an upper bound for the columnar form, not its value.

**The pass budget.** Each merge derives it from `merge_resident_ceiling_bytes`
and the catalog sizes it already holds. It is not a separate config input,
so each entry point sets it only through the ceiling in D1's table:

| path | `pass_budget` |
|---|---|
| resident | ceiling − inputs' raw bytes − worst-case D3 charge |
| fetch-per-pass | ceiling − worst-case D3 charge − the raw bytes of the row groups being decoded at once (at most `input_read_concurrency` groups) |

- **Floor.** The budget is never below a floor of `l1_part_memory_target_bytes`,
  the derived part memory target (256 MiB at the least). D1 takes the
  resident path only when the resident row's value is at or above the floor.
  When even the fetch-per-pass value is below the floor, the merge emits
  every stream through D3, which is today's merge, unchanged.
- **Charge.** A pass's retained rows are charged to `merge_cursor_budget_bytes`
  at decode time, through the same running charge as everything else. When
  the charge would pass `pass_budget` before the range is complete, because
  the estimate was low, the merge halves the range and decodes it again. It
  does not abort. The output is unchanged, since a range's boundaries move
  only memory and passes, never the emitted sequence.
- **D3.** A stream whose estimate alone exceeds `pass_budget` is D3's stream.

### D3. A stream larger than one pass keeps today's streaming cursors

A single stream whose estimate exceeds `pass_budget` is a range of its own,
emitted with today's per-input `StreamCursor`s. It reads from the resident
bytes when D1 holds them, and fetches as today otherwise. This keeps
ADR-0065 decision 4's bound for one hot stream, the 45.7 GB case #711 fixed:
no path gathers a whole stream's rows.

ADR-0065 decision 4 states that bound on the `MergeMemoryTracker`. Its
transient term is at most one decoded block, plus the raw bytes of the row
groups being decoded, per input. D1's resident bytes are not counted under
that term. They are a separate `resident` term, at most
`merge_resident_ceiling_bytes`, which the tracker records on its own. The
transient term's bound and its independence from stream size stay exactly
as ADR-0065 decision 4 states them.

### Commitments and their tests

<!-- band-sources-allow: these are acceptance commitments of this ADR, predicted until the implementation measures them -->
| commitment | band | test |
|---|---|---|
| L1 parts are byte-identical to today's | same keys and bytes on three fixtures: many-stream (N ≥ 50 inputs, S ≥ 200 streams per input, unkeyed); one hot stream; and clustered inputs at W above 1 with one stream whose per-bucket reservation sum exceeds the budget, so that ADR-2135 batching runs | a golden comparison against the current merge |
| block fetches per input row group | exactly 1 with D1 and exactly P with the per-pass fallback, counted per (object, row-group range) | a counting `MemoryStore`. It must fail against a per-cursor cache (still N × S) and against a small LRU of row groups (thrashes, because every group spans the whole stream range) |
| hot-stream memory | the tracker's transient term stays under ADR-0065 decision 4's bound and does not grow with the stream; the separate `resident` term stays at most `merge_resident_ceiling_bytes` | `merge_peak_memory_is_bounded_independently_of_stream_size` (`crates/ravel-maintain/src/rlog.rs:7426`), extended to assert the resident term, plus the same hot stream inside a many-stream bucket |
| resident path choice | the merge takes fetch-per-pass whenever raw bytes plus one pass plus the worst-case D3 charge exceed `merge_resident_ceiling_bytes`, and never aborts with `MergeCursorBudgetExceeded` where today's merge completes | a fixture sized just over and just under the ceiling, with a D3 range present; plus one test per entry point that it passes the ceiling the D1 table states |
| this fixture, hours 08 and 09 | the band follows the path each run's logged decision line names. Resident: GETs per merge at most 1.1 × (N × catalog GETs + N row groups). Fetch-per-pass: at most 1.1 × (N × catalog GETs + P × N row groups), with P logged. Either way, wall time within 10 × that run's own fetch time at `k` in flight. Run once with `ravel-cli maintain compact-bucket` (a 20 GiB ceiling) and once with ravel-server at its defaults (a 5 GiB ceiling). | measured on run 2's bucket, which is kept (predicted) |
| a mixed fleet | a 0.30.x compactor and a new one converge on the same parts through `CreateIfAbsent` | follows from byte-identical parts; the golden test above pins it |

## Rejected alternatives

- **A per-cursor cache.** Each `StreamCursor` would keep its own fetched
  bytes, but each stream opens new cursors, so the merge still costs N × S.
- **A bounded LRU of raw row groups.** Every row group carries streams from
  across the whole sorted range, so a stream-major walk touches almost every
  input for every stream. Any LRU smaller than all the inputs evicts every
  group before its next use, which is today's cost.
- **Shuffle through transient objects.** One read pass would write P
  stream-range intermediates, then a second pass would merge each. Reads stay
  at about 2 × and memory is bounded whatever the decoded ratio is. The cost
  is P × N extra PUTs, a new transient object class for GC to reap, and new
  crash and convergence states. D1 and D2 reach one read per input with no
  new durable state. This stays the fallback design if the measured pass
  count makes D2's decode CPU the bottleneck.
- **Change the output order to block-major.** Reading every input once in
  block order and emitting rows in that order would need no passes. But L1
  parts are content-addressed and readers rely on stream-major order inside
  an object, so this changes every part's bytes, needs a format decision,
  and breaks convergence with existing compactors.
- **Fix it at ingest only.** A clustering key or stream-aware block cuts
  would lower S per block. That helps queries too, but the merge would still
  cost N × S, and every existing bucket would still pay it.

## Consequences

- A many-stream shard-hour compacts with about one GET per input instead of
  about S, and with `k` fetches in flight instead of one.
- Decode CPU grows from once per block per stream (today's total, which a
  single GET in flight hides) to once per block per pass. When P is large,
  decode CPU replaces GET latency as the limit, which is the trigger for the
  shuffle alternative.
- Resident memory per merge grows by the inputs' raw bytes, about 5 to 7 GB
  per shard-hour on this fixture. It is bounded by D1's
  `merge_resident_ceiling_bytes`, which ravel-server sets to 5 GiB at its
  defaults. That is below this fixture's raw bytes per shard-hour, so on
  this fixture ravel-server at its defaults takes the fetch-per-pass path.
  When the bytes do not fit, the merge falls back to one fetch per row
  group per pass.
- ADR-0065 decision 4 and the ADR-0032 memory text gain an amendment that
  states the new peak: catalog metadata, plus resident raw bytes (D1), plus
  one pass's retained rows, plus the in-progress part. The `rlog.rs` module
  note changes with them.
- No persistent format changes. L1 parts keep their bytes, keys and version.

### Not in this ADR (separate tickets)

- Cross-merge admission: a shared budget that concurrent merges wait on, in
  place of D1's per-process division of the per-merge budget (#3825).
- The catalog phase re-GETs sections already inside its 64 KiB footer probe,
  and reads the whole POSTINGS section only to learn field names: 5 to 8 GETs
  per input, a large share of the 461,608.
- L1 parts close on the decoded-heap memory target at about 4 to 5 MB stored
  on this workload, so a shard-hour yields hundreds of small L1 objects.
- The fold's postings build fails with "bad magic bytes" on this bucket
  (#2861).
