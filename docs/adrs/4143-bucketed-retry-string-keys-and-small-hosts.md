# ADR-4143: Bucketed spill retry for string group keys and small hosts

- Status: Accepted (2026-11-12)
- Date: 2026-11-12
- Refs: #4143, #4132, #4133, #4072, ADR-3565, ADR-0954, ADR-1195

## Context

A SQL statement refused for memory gets one retry. When the final aggregate is
one `BucketedFinalAggregateRule` admits and the derivation of ADR-3565
amendment 2, item 4 returns a tuning, the retry runs a hash exchange and
`BucketedFinalAggregateExec` with spill on. Each routed row is written to its
bucket's scratch file once and read back once. Otherwise the retry keeps the
sort-merge plan of ADR-0954, which writes and re-reads its runs through merge
passes.

v0.32.0 shipped this. Two refused shapes still take the sort-merge retry, and
both were measured (#4072 comments 6802934028 and 6818187104):

1. **An admitted shape on a small host (#4133).** ClickBench statement 33
   (`GROUP BY "WatchID", "ClientIP"`) on c6a.large derives `bits` 7 and a
   largest routed chunk below the 1,024-row floor, so the derivation returns
   `chunk_too_small`. The retry runs sort-merge: about 112 s hot and 11.6 GB
   of spill per try. The same statement runs bucketed on c6a.xlarge (`bits`
   5, 16.8 s) and c6a.2xlarge (`bits` 4, 9.6 s, 1.72 GB of spill).
2. **A string group key (#4132).** `admits` takes fixed-width keys only, so
   statements 19, 34 and 35, which group by `"SearchPhrase"` or `"URL"`, retry
   on sort-merge on every box at 3.3 to 6.2 GB of spill each. In the two
   concurrent windows of M8 on c6a.2xlarge, 12 and 11 retries wrote 53.9 and
   47.8 GB. Concurrent QPS was 0.744, 0.761 and 0.847 of v0.28.0's on
   c6a.2xlarge, c6a.xlarge and c6a.large.

### Why statement 33 gets no tuning on c6a.large

The derivation (`bucket_spill_tuning.rs`, at origin/main 2e0b6fc3b) splits the
per-stream share `S = (L_eff - H) / C` into fixed parts:

- `bits` is the least `b` with `2T <= 2^b x P x 0.75 x S`. The table for one
  bucket, with a margin of 2, fits in three quarters of the share.
- `routed_chunk_rows` is the largest `c` up to 8,192 with
  `(2^bits + 1) x b(c) <= floor(0.25 x S)`. The open chunks and one read-back
  chunk fit in the remaining quarter.

On c6a.large `S` is 32,585,819 B. The routed row is 64 B, not the 52 B item 4
predicted (ADR-3565 amendment 3). The first attempt projected `T` at 12.65 to
12.78 GB, which gives `bits` 7 and a chunk charge that fits 972 rows. That is
below the floor, so there is no tuning. (ADR-3565 amendment 3 gives 980 rows
from `w` alone; 972 is what `routed_chunk_charge` allows once it adds each
column's validity bitmap and the 64-byte rounding of every buffer.) Under the
fixed quarter, any `T` above 12.51 GB fails on that box.

Two facts in the code make the fixed quarter unnecessary:

- **The floor has no stated reason.** `MIN_ROUTED_CHUNK_ROWS` is documented
  only as the point below which the retry keeps sort-merge
  (`bucket_spill_tuning.rs:52-54`). It is not part of any bound. The
  descriptor need `2 x P x 2^bits + P` does not depend on the chunk size, and
  a smaller chunk is charged less. The floor limits per-message IPC framing
  and per-batch overhead.
- **The open chunks and the phase 2 table are never held together.** `engage`
  and `force_route` empty the table before routing
  (`bucketed_final_aggregate.rs:1460-1526`), `finish_input` writes every
  retained chunk after a trigger (`:1699-1724`), and during the drain
  `relieve` writes later buckets' chunks (`:1534-1557`). Only one read-back
  chunk sits beside a phase 2 table. ADR-3565 item 3 adds the parts as if
  they were held together, so the bound holds with room to spare.

The first-attempt projection `T = A x N / n` runs high when the refusal
comes early. `A` is the peak asked bytes of every consumer, charged at
capacity, and an early refusal lands while the group tables are just past a
doubling. On c6a.large `A / n` was 126.5 to 127.8 B per consumed row, against
the 94 B per group ADR-3565 measured for a finished table. An overestimate
costs bits; an underestimate costs a typed failure with no third attempt. So
the projection stays as it is.

### Why string keys are not admitted

Fixed width is assumed in the routed chunk layout and the derivation, not in
the spill path:

- **Admission:** `admits` and `is_bucketed_key` (`bucketed_final_aggregate.rs:239-275`, `:334-362`).
- **Chunk layout:** a routed column is one values buffer of `rows x width`
  plus validity (`:686-803`). The router copies `width` bytes per row and
  closes a chunk on its row count (`:930-1004`). Forced-route slices are
  sized in rows (`:1493-1508`).
- **Derivation:** `column_widths`, `routed_chunk_charge` and the row search
  (`bucket_spill_tuning.rs:73-75`, `:164-177`, `:229-241`).
- **Not width-dependent:** the scratch write through Arrow `take`
  (`:1063-1083`), the spill schema, the read-back, `charge_table` (charged by
  `size()`), and the descriptor bound.

The scratch files are Arrow IPC through DataFusion's `SpillManager`, deleted
when dropped. They are not a persistent format, so this ADR changes no frozen
contract.

By the time the bucketed rule runs, `DictionaryGroupKeysAsViews` has cast a
declared `str` key to `Utf8View` (`session.rs:977` before `:1043`), and
DataFusion 55.2 has group tables for `Utf8View`.

ADR-3565 rejected bucketing string keys for the first attempt, because it was
measured slower on every string shape in memory. That finding is about CPU on
a statement that fits. This ADR is about a statement that has already been
refused and is bound by spill bytes, so it admits string keys in the retry
only.

### The risk the string route carries

ADR-3565 decision 3 notes that retained input is up to R times the group
state, where R is rows per group. The partial aggregate of these statements
gives up early (its reduction probe sees too few repeats), so after the
trigger every row the exchange hands the final is routed. For
`GROUP BY "URL"` that is about 100 million rows of URL bytes, a 16 B view and
an 8 B count before compression, where sort-merge writes rows its table has
already combined. Without combining, the write-once route can write more
bytes than the sort-merge it replaces.

## Decision

### 1. The open chunks take what the tables leave (#4133)

The derivation keeps its `bits` rule and drops the fixed quarter for the
chunks. With `table(b) = ceil(2T / (P x 2^b))`:

- `bits` is the least `b` with `table(b) <= 0.75 x S` **and**
  `table(b) + (2^b + 1) x b(c_min) <= S`, where `c_min` is the chunk floor.
  The second condition takes one more bit only at the top of a `bits` step,
  where the table leaves no room for even floor-sized chunks.
- `routed_chunk_rows` is the largest `c` up to 8,192 with
  `(2^bits + 1) x b(c) <= S - table(bits)`.

Every bound of ADR-3565 item 3 still holds. The chunk area is at least
today's quarter, `table + chunks <= S` holds as written, and each bucket's
table still gets at least `2T / (P x 2^b)`. The descriptor rule is unchanged.

| box | `T` | today | this decision |
|---|---|---|---|
| c6a.large | 12.65 to 12.78 GB (measured) | `bits` 7, 972 rows, no tuning | `bits` 7, 2,416 rows at 12.65 GB and 2,400 at 12.78 GB, descriptor need 4,112 |
| c6a.large | largest that tunes | 12.51 GB | about 31.8 GB |
| c6a.xlarge | 9.48 GB (item 4) | `bits` 5, 8,192 rows | unchanged |
| c6a.2xlarge | 9.48 GB (item 4) | `bits` 4, 8,192 rows | unchanged |

The 1,024-row floor stays. Under this rule it binds only past about 31.8 GB
on c6a.large. The test fixture `Q33_WIDTHS` moves from 52 B to the measured
64 B. `bits` 7 needs a soft `nofile` limit of at least 8,224 on c6a.large
(one slot); the M8 and publication boxes ran at 8,320 or more.

ADR-3565 item 5 says the slack from rounding `bits` up is extra safety
against an under-shoot. Under this rule the chunks spend that slack in
phase 1, so the margin in phase 2 rests on the phase disjointness above: the
open chunks are gone before a phase 2 table is built. The amendment to
ADR-3565 rewords item 5 to say so.

This decision ships on its own, ahead of decisions 2 and 3.

### 2. First, predict whether routing string keys writes fewer bytes

Decisions 3 and 4 are built only if this prediction says they pay. Before
any string-key code, one task predicts, for statements 19, 34 and 35 on each
M8 box, the bytes an uncombined write-once route would write:

- the rows the exchange hands the final after the trigger, from a run of the
  first attempt on the ClickBench corpus;
- the average routed row bytes, from the declared key types and the corpus's
  key lengths;
- the compression ratio of the scratch format, from writing a sample of those
  rows through the same spill writer.

The threshold, per statement and box, is the sort-merge retry's
`spill_bytes_written` in M8's serial pass on that box (3.3 to 6.2 GB on
c6a.2xlarge, read from the M8 server logs). The M8 logs show these three
statements retrying in serial passes as well as concurrent windows on all
three boxes.

- If the prediction is below the threshold for every statement and box,
  decision 3 is built and decision 4 is not.
- If it misses, decision 3 is built with decision 4, and the prediction is
  repeated with combining before decision 3 ships.
- If combining cannot bring it under the threshold either, decisions 3 and 4
  are not built, and the epic records why.

### 3. String keys are routed in byte-budgeted chunks, in the retry only (#4132)

A routed column is either fixed-width, as today, or variable-width: Utf8,
LargeUtf8, Utf8View, Binary, LargeBinary or BinaryView.

- **Chunk layout.** A variable-width column holds offsets for at most `c`
  rows and a data block of `D` bytes. Both are allocated and charged before
  any row is written. A chunk seals when either its rows or its data block
  fill.
- **Copies, not views.** Strings longer than the 12-byte inline limit are
  copied into the chunk's own data block. A view into the source batch would
  keep that batch's buffers alive without a charge.
- **One value larger than `D`** gets its own block, charged before it is
  copied like any chunk. A refused charge is a spill trigger, as for a fixed
  chunk. If it is still refused once every retained chunk has been written,
  the statement fails with the bucketed retry's typed refusal. The existing
  uncharged slice path is not used for variable-width columns, because its
  bound, `c x (w + 12)`, is in rows times a fixed width and does not bound
  bytes when values have no fixed width.
- **The derivation.** `bits` comes from `T`, as now. The chunk area,
  `(S - table(bits)) / (2^bits + 1)`, becomes a byte budget per chunk,
  split between offsets, validity and data. No bytes-per-row input is needed.
- **Admission.** `admits` takes these key types only when the rule is built
  `with_spill(true)`, which is the refusal retry. A first attempt plans as
  today.
- **Exactness.** The bucket is a function of the key, as for a fixed-width
  key, so every row of a group lands in one bucket and is aggregated by one
  table.

### 4. A sealed string-keyed chunk is combined before it is written, if decision 2 calls for it

When decision 2's prediction misses, a sealed chunk of a route with a
variable-width key is merged by key before it is written, with the
aggregate's own state merge, the same operation the phase 2 table applies
when the bucket is read back. The result is exact. Its memory is one chunk's
group table, bounded by the chunk's rows and charged like the phase 2 table.

Fixed-width routing is unchanged. Statement 33, the fixed-width shape M8 and
the publication runs validated, has about one row per group, so combining
would cost one hash pass per chunk and save nothing.

### 5. The gate (M9)

The change ships only after a pre-registered run on c6a.large, c6a.xlarge and
c6a.2xlarge, built and judged the way M8 was (ADR-0954 parallel bucket spill
gate). The bands are written before any box is launched and include:

- statement 33 on c6a.large derives a tuning (`bits` 7 at the measured `T`)
  and answers, with its retry spill at most 2.0 GB;
- if decision 3 was built: every retry of statements 19, 34 and 35 the run
  logs reports `retry_plan="bucket"`, and each one's
  `bucket_spill_bytes_written` comes in within the band decision 2's
  prediction set and below that statement's M8 sort-merge bytes on the same
  box. The number of retries expected per pass and box is read from M8's
  logs and written into the pre-registration, not assumed;
- statement 33's hot time on c6a.2xlarge and c6a.xlarge stays within 1.10 of
  M8's 9.4 s and 17.5 s (decision 1 must not move a tuned box);
- no null answer on any box;
- concurrent QPS against v0.28.0 on the same box, reported against the 0.95
  bar the epic #4072 release bar started from.

```mermaid
flowchart TD
    R[first attempt refused] --> A{admits?}
    A -- "fixed-width keys, as today" --> D[derive tuning from T, S]
    A -- "string keys, retry only (decision 3)" --> D
    A -- other shapes --> SM[sort-merge retry]
    D -- "no tuning: descriptors or bits cap" --> SM
    D -- "bits; chunks take S minus table, decision 1" --> X[hash exchange to P final streams]
    X --> T1[phase 1 table per stream]
    T1 -- "refused grow = trigger" --> RT[route rows to 2^bits buckets]
    RT --> CH[open chunk per bucket: rows or bytes fill]
    CH -- "seal, fixed-width key" --> W[("scratch file per bucket, written once")]
    CH -- "seal, string key" --> CB["combine by key if decision 2 calls for it (decision 4)"]
    CB --> W
    W --> RB[read each bucket back once]
    RB --> T2[phase 2 table per bucket]
    T2 --> OUT[exact result]
```

## Rejected alternatives

- **Change the 0.75 / 0.25 split.** A 0.77 / 0.23 split tunes c6a.large only
  while `T` stays under 12.84 GB, 0.5% past the measured value. It moves the
  failure point instead of removing it.
- **A margin below 2 in the `bits` rule.** It would give `bits` 6, but it
  weakens the under-shoot argument of ADR-3565 item 5, and a bucketed retry
  that under-shoots fails the statement with no third attempt.
- **Lower the floor to 512 rows.** It tunes c6a.large at `bits` 7, but `bits`
  8 then gives 488 rows and fails again from about 25 GB, and it changes a
  floor whose purpose was never stated instead of the split that causes the
  failure.
- **Fewer final partitions on the retry.** Chunk rows scale like
  `P / (Q + 2P)^2`, which peaks at `P = Q / 2` with at most about 1.56 times
  the room. It would also change the validated c6a.xlarge plan and couple
  this ADR to ADR-1195's partition floor and the server's `retry_partitions`
  line.
- **A better projection of `T`.** No surrogate exists today: the ledger keeps
  charged bytes, not asked bytes per consumer, and no group count is
  reported. An underestimate fails the statement, so the conservative
  projection stays.
- **Hash string keys to a fixed-width surrogate.** Without side storage it is
  not exact (a 64-bit collision has probability about 3 x 10^-4 at 10^8
  keys). With side storage the string bytes are still routed and written, so
  nothing is saved.
- **A per-bucket dictionary.** It needs a key-to-code table per bucket for
  the whole of phase 1, which is the group table bucketing exists to avoid,
  and the share does not bound it.
- **Bucket string keys on the first attempt too.** ADR-3565 measured it
  slower on every string shape in memory, and nothing here changes that.

## Consequences

- c6a.large-sized hosts get the bucketed retry for statement 33 and for any
  admitted shape whose projected tables fit in three quarters of the share,
  up to about 2.5 times the `T` they reach today.
- If decision 3 is built, a refused statement grouping by a string key
  retries bucketed when its derivation tunes. Its scratch bytes no longer depend on the memory share,
  so they do not grow when concurrent statements shrink it.
- Fixed-width routing, which M8 and the publication runs validated, changes
  only in its chunk size (decision 1). Combining, if built, applies to
  string-keyed routes only.
- Decisions 3 and 4 are built only on decision 2's prediction. If it says
  they do not pay, string keys keep the sort-merge retry and the epic says
  why.
- The retry log names the new reasons a string-keyed statement keeps
  sort-merge. The shipped reason codes `chunk_too_small` and
  `not_fixed_width` keep their strings (ADR-0954 fixes shipped reason codes).
- Production cases this leaves exposed, stated for the release notes: a
  string key whose values are mostly larger than the chunk's data block
  spills through the uncharged one-slice path; a statement whose `T` exceeds
  about 31.8 GB on a 4 GiB host still takes sort-merge; and the projection's
  conservative bias still costs bits on an early refusal.
- ADR-3565 gets an amendment that points item 3, item 4, item 5 and the
  string-key rejection at this ADR. ADR-0954 gets one for the M9 gate.
- No frozen format changes: RSEG, `proto/`, series identity, commit tokens
  and the object key layout are untouched.
