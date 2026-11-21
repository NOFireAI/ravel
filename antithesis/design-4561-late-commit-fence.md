# Design #4561: fence commit records that land in an hour after maintenance sealed it

Status: design and decision, no code. Written against `main` at `9961091`.
Every claim about the code or the documents carries a `file:line` on that
checkout. A claim about S3 itself that this checkout cannot show is marked
**[external]** and listed again in section 7.

Terms used throughout:

- **Hour `H`, `end(H)`**: an ingest-hour bucket and its exclusive end
  (`crates/ravel-maintain/src/bucket.rs:33-40`).
- **Maintain seal**: `now >= end(H) + max_flush_lifetime +
  clock_skew_allowance` (`crates/ravel-maintain/src/bucket.rs:47-52`,
  `crates/ravel-maintain/src/config.rs:2778-2781`), 65 minutes at defaults
  (`crates/ravel-maintain/src/config.rs:1090-1094`).
- **Fold seal**: the maintain seal plus `fold_safety_margin`
  (`crates/ravel-catalog/src/fold.rs:623-630`), 80 minutes at defaults
  (`crates/ravel-catalog/src/config.rs:10`, `:328`, `:332`).
- **Late L0**: a commit record (and its data object) that becomes visible in
  `H`'s bucket after a maintenance pass has listed `H` as sealed.
- **Partial cover**: a version 1 compaction record that shares inputs with a
  live erasure rewrite that does not hold all of them
  (`crates/ravel-catalog/src/catalog.rs:6055-6057`, `:6142-6146`).

## 0. Decision in five lines

<!-- section 0 -->

## 1. The durability premise

Question: can a commit record that lands after the maintain seal belong to a
write that was acknowledged to the client?

Short answer. **Buffered mode: yes, with every clock inside its contract.
Strict mode and an idempotency-keyed replay: yes, but only outside the clock
contract (a writer clock far behind the store with the lag check not
enforcing, or a process suspension the timers do not count).** In every mode
the fence therefore cannot delete or hide a late record on the theory that
nobody was told it was durable.

### 1.1 The writer path, all three signals

The three shard actors run the same sequence. Lines are metrics / logs /
spans.

| Step | `shard.rs` | `log_shard.rs` | `span_shard.rs` |
|---|---|---|---|
| Deadline pinned at flush open: `raw_ns + max_flush_lifetime`, `raw_ns` from `Clock::now_ns` | `:2773`, `:3150-3151` | `:2782`, `:3158-3159` | `:1813`, `:2161-2162` |
| Deadline re-derived at permit grant by `grant_deadline_ns`; `None` abandons with no PUT | `:3273-3286` | `:3291-3303` | `:2268-2280` |
| Data PUT, then commit PUT, each inside `bound_to_deadline` | `:1269`, `:1352` | `:1487` (commit) | `:727` (commit) |
| Ack `Ok(token)` only in the `Ok` arm of `publish_with_retry` | `:878` | `:1276` | `:530` |
| Abandon at the commit PUT: waiters get `Abandoned` | `:883-897` | `:1281-1294` | `:535-548` |
| `bound_to_deadline` | `:1220-1233` | `:1400-1413` | `:645-658` |

Facts that follow from those lines:

1. **The ack is sent after the commit PUT's response, never before.** The
   only earlier `Ok` is a test mutant (`shard.rs:661-670`). So a strict
   waiter is told `Ok` only when the store answered the create-if-absent PUT
   of the commit record (`crates/ravel-commit/src/publish.rs:120-131`), or
   answered `AlreadyExists` and the stored record carried this flush's
   content hash (`crates/ravel-commit/src/publish.rs:133-134`, `:231-267`).
2. **The deadline covers the whole flush, including the commit PUT, but it
   is a bound on how long the writer waits, not on when the store applies
   the request.** `grant_deadline_ns` returns
   `min(granted + max_flush_lifetime, end(H) + max_flush_lifetime)`
   (`crates/ravel-ingest/src/config.rs:449-461`). `bound_to_deadline` reads
   the wall clock once, turns the remainder into a duration, and races the
   store future against `Clock::sleep` of that duration
   (`crates/ravel-ingest/src/shard.rs:1224-1232`). The production clock
   reads `SystemTime::now()` for `now_ns`
   (`crates/ravel-ingest/src/clock.rs:59-66`) and sleeps on the tokio timer
   (`crates/ravel-ingest/src/clock.rs:37-39`). So the deadline is computed
   on the wall clock and waited out on a monotonic one.
3. **Nothing re-reads a clock after the PUT wins the race.** After
   `Some(Ok(token))` the only work is the instrumented build's HEAD of the
   data object, then `return Ok(token)`
   (`crates/ravel-ingest/src/shard.rs:1357-1366`,
   `crates/ravel-ingest/src/log_shard.rs:1488-1496`,
   `crates/ravel-ingest/src/span_shard.rs:728-736`). No site compares `now`
   with the deadline there and no site reads the commit record's
   `last_modified`.
4. **Retries and per-attempt timeouts.** The shard gives
   `ravel_commit::publish` a zero-retry policy and loops itself
   (`crates/ravel-ingest/src/shard.rs:1314-1318`): a retryable error retries
   while `attempt < put_retry_max_attempts` and `now < deadline`
   (`crates/ravel-ingest/src/shard.rs:1379-1391`), with defaults of 4
   retries, 100 ms base and 2 s cap
   (`crates/ravel-ingest/src/config.rs:923-925`). Below that, the S3 adapter
   times one request out at 20 s
   (`crates/ravel-object-store/src/s3.rs:271`) and does not retry a
   timed-out create-if-absent PUT, but does retry 5xx and 429 for up to
   about 200 s inside one logical call
   (`crates/ravel-object-store/src/s3.rs:684-695`). Every one of those HTTP
   attempts is a separate request the store may apply.
5. **A PUT whose future is dropped.** When the sleep wins, `select!` drops
   the store future and the flush returns `PutAbort::Exhausted`
   (`crates/ravel-ingest/src/shard.rs:1393`). The object-store contract says
   only that "the trait honors cancellation by drop"
   (`docs/object-store-contract.md:261-262`); it does not say a request the
   server already received is undone, and for multipart it says the opposite
   (`docs/object-store-contract.md:683-686`). The default build does nothing
   further with the key: the HEAD that remembers an abandoned commit key
   runs inside an invariant condition the default build compiles out
   (`crates/ravel-ingest/src/shard.rs:899-900`, `:958-959`), and the logs and
   spans shards never record it at all
   (`crates/ravel-ingest/src/log_shard.rs:1283-1287`,
   `crates/ravel-ingest/src/span_shard.rs:537-540`).
6. **What the client is told at an abandon.** `Abandoned` is retryable
   (`crates/ravel-ingest/src/error.rs:164-168`) and maps to HTTP 503
   (`services/ravel-server/src/otlp_http.rs:318-321`). The rows are gone
   from the buffer; a retry is a new flush with a new sequence number.

### 1.2 Per ack mode

**Buffered.** The ack is sent at enqueue, before any PUT
(`crates/ravel-ingest/src/router.rs:1147-1151`,
`crates/ravel-ingest/src/log_router.rs:746-750`,
`crates/ravel-ingest/src/span_router.rs:726-730`). The flush behind it runs
until `end(H) + max_flush_lifetime` on the writer's clock. A commit PUT sent
just before that instant and dropped at it can be applied by the store
later. Maintenance seals `clock_skew_allowance` (5 minutes) after that
instant, so the PUT lands in a sealed hour if the store applies it more than
`5 min - writer lag` after the writer gave up, where the writer may lag the
store by up to 5 minutes and still pass its own check
(`crates/ravel-ingest/src/config.rs:233-251`). With the writer at 4 min 50 s
of lag, 10 s on the wire is enough. Those rows were acknowledged, in the
buffered sense. The buffered contract already lists this ending as a drop of
acknowledged rows: "neither PUT completes within `max_flush_lifetime`"
(`docs/consistency-model.md:118-128`). So a late buffered record is data the
contract permits to be lost, but it was acknowledged, and nothing today
tells the fence which mode a commit record came from.

**Strict and strict-arrival.** Two timers apply. The shard's deadline above,
and the client-facing ack round, which is a `tokio::time::timeout` of
`ack_deadline` over the shard acks
(`crates/ravel-ingest/src/router.rs:123-125`,
`crates/ravel-ingest/src/log_router.rs:800`,
`crates/ravel-ingest/src/span_router.rs:141`), 10 s in the server
(`services/ravel-server/src/lib.rs:147`). A client that sees success
therefore had its commit PUT answered within about 10 s of monotonic time
after its rows were enqueued, and the hour `H` was pinned from the writer's
wall clock inside that same window. For that record to carry a store stamp
at or past `end(H) + 65 min`, the store's clock has to be 65 minutes or more
ahead of the instant the writer's wall clock named, with only 10 s of
counted time in between. Three ways exist, none inside the clock contract:

- The writer's clock lags the store by more than the seal margin and the
  lag check does not refuse. It refuses a lag over 5 minutes
  (`crates/ravel-ingest/src/config.rs:238`) only when an observation exists;
  with none it counts and proceeds
  (`crates/ravel-ingest/src/shard.rs:2514`), and a teardown drain bypasses
  it with the log line "the commit may land in a sealed hour"
  (`crates/ravel-ingest/src/shard.rs:2526-2533`).
  `docs/consistency-model.md:606-617` documents this direction for the fold.
- The process or its VM is suspended between the pin and the PUT reaching
  the store, in a way neither tokio timer counts. No seal document discusses
  suspension; only ADR-4233 does, for format activation: "A process paused
  between its last check and a PUT is not bounded by any timer"
  (`docs/adrs/4233-retained-data-compatibility-contract.md:254-256`).
- The wall clock is stepped forward between the pin and the PUT. The sleep
  is a duration fixed before the step, so the PUT can still win and be
  acked. The logs and spans shards say so in their own words: a flush "still
  in flight when a write times out could land past the seal on correct code"
  under forward jumps of 9000 s
  (`crates/ravel-ingest/src/log_shard.rs:1283-1287`).

A strict write whose ack round timed out is not acknowledged: the client got
a retryable error and "each shard's flush keeps running and may commit
*after* the client gave up" (`docs/consistency-model.md:356`).

**Idempotency-keyed replay.** A replay answers from a marker object, and the
marker is written only after the router acknowledged the write in full
(`services/ravel-server/src/logs_ingest.rs:549`, `:584-591`;
`crates/ravel-ingest/src/idempotency.rs:5-11`). A flush abandoned at its
commit PUT leaves no marker, so the keyed retry "re-ingests exactly like an
unkeyed one" (`docs/consistency-model.md:356`). A replay never turns an
abandoned flush's late record into a success. A replayed success is a strict
success, so the strict analysis above covers it.

### 1.3 Consequence for the fence

An acknowledged write can land after the maintain seal: routinely possible
in buffered mode, and in strict mode whenever a clock or a pause is outside
the contract. In the strict cases the client holds a commit token, and the
token path GETs the exact commit key with no seal check
(`crates/ravel-catalog/src/catalog.rs:4538-4545`), so that record is durable
and readable today. A fence that deleted, quarantined or hid late records
would turn those into lost acknowledged writes. The chosen fence must leave
a late record where it is and readable as it is.

## 2. What a late L0 does today

The read side was built to tolerate a late L0; the maintenance side was built
on the belief that there is none.

### 2.1 Read side: a late L0 is an extra segment, by decision

- ADR-0018 decided it: "A record present in the bucket but absent from the
  input list stays included ... an interlock violation (a late commit
  landing in a sealed bucket) degrades to an extra segment or harmless
  duplication, never to data loss"
  (`docs/adrs/0018-l0-l1-compaction.md:185-189`). The fold's classifier does
  the same: it contributes every L0 "not named by a compaction or rewrite
  input list" (`crates/ravel-catalog/src/fold.rs:8049-8051`).
- Whether a query sees it depends on the path, not on a rule about lateness:
  - Snapshot path: hours at or below the watermark are served from the
    folded snapshot and the listing starts above it
    (`crates/ravel-catalog/src/catalog.rs:2966-2972`). A reconcile skips a
    bucket that holds only L0 records
    (`crates/ravel-catalog/src/fold.rs:7674-7684`,
    `crates/ravel-catalog/src/fold.rs:1365-1376`). So a record that lands
    after the fold seal is "invisible to a read without a commit token until
    HEAD is rebuilt" (`docs/catalog-and-mvcc.md:2921-2926`), and one that
    lands between the maintain seal and the fold seal is folded like any
    other.
  - Token path: served, by exact key
    (`crates/ravel-catalog/src/catalog.rs:4538-4545`).
  - A later compaction, rewrite or tombstone in the bucket makes the fold
    reclassify it, and the late L0 then enters the snapshot as a side effect
    (`crates/ravel-catalog/src/fold.rs:1369-1372`, `:8049-8051`).
- No read-side code reads `last_modified`. The field's own doc forbids
  ordering commits by it (`crates/ravel-object-store/src/lib.rs:209-211`).
- Detection exists: `verify_seal_divergence` re-lists sealed hours and
  reports such a record as missing from the snapshot
  (`crates/ravel-catalog/src/seal_divergence.rs:233`, test at `:1316`).

### 2.2 Maintenance side: one LIST is taken as the final set

- Every planner gates on `Bucket::is_sealed` with its own injected clock and
  then lists once: compaction (`crates/ravel-maintain/src/compact.rs:684-689`),
  the migrate primitive (`crates/ravel-maintain/src/rewrite.rs:865-871`) and
  the erasure rewrite (`crates/ravel-maintain/src/erasure_rewrite.rs:2484-2492`).
- The listing keeps only the key of each commit record. `last_modified` is
  on the listed `ObjectMeta` and is discarded in
  `partition_bucket_listing` (`crates/ravel-maintain/src/read.rs:166-185`);
  `BucketListing` has no field for it
  (`crates/ravel-maintain/src/read.rs:66-80`).
- Inputs are all-or-nothing: any compaction or rewrite record in the bucket
  stops a compaction (`crates/ravel-maintain/src/compact.rs:1004-1020`) and
  a migration (`crates/ravel-maintain/src/rewrite.rs:1036-1053`); otherwise
  every listed commit key is an input
  (`crates/ravel-maintain/src/compact.rs:705-712`,
  `crates/ravel-maintain/src/rewrite.rs:884-891`,
  `crates/ravel-maintain/src/erasure_rewrite.rs:714-715`).
- A pass re-lists before its record PUT and publishes nothing if the record
  set moved (`crates/ravel-maintain/src/rewrite.rs:417-451`, called at
  `:636-641`). `same_record_set` compares the commit keys too
  (`crates/ravel-maintain/src/rewrite.rs:405-415`), so a late L0 that lands
  between plan and re-list aborts the pass; one that lands after the re-list
  is not seen.
- Once a bucket holds a compaction record, nothing compacts it again: the
  outcome is `AlreadyCompacted` (`crates/ravel-maintain/src/compact.rs:1008-1010`),
  counted as done (`crates/ravel-maintain/src/scan.rs:165-172`) and
  memoized terminal (`crates/ravel-maintain/src/scan.rs:1941`). A late L0
  there stays a raw L0 for the life of the bucket.
- The sweep never deletes it: the superseded-input rule is "keyed on the
  record's own input list rather than on the bucket"
  (`crates/ravel-maintain/src/sweep.rs:2910-2916`), and a test pins that a
  stray commit record and its data survive
  (`crates/ravel-maintain/tests/superseded_head_gate.rs:1791-1796`). That
  test's own comment contradicts the seal comments: "the hour is sealed
  against compaction, not against a straggling writer"
  (`crates/ravel-maintain/tests/superseded_head_gate.rs:1756-1758`).
- The erasure rewrite over a bucket that already has a record reads only
  that record's parts (`crates/ravel-maintain/src/erasure_rewrite.rs:544-563`),
  so it never erases a subject held in a stray L0. The completion gate does
  see it: it rebuilds the served set from every listed commit record
  (`crates/ravel-maintain/src/erasure_rewrite.rs:3305-3321`) and blocks the
  request (`:3333-3337`). The rewrite pass meanwhile reports
  `AlreadyApplied` (`crates/ravel-maintain/src/erasure_rewrite.rs:2537-2543`).
  The request is stuck, with its filter still in force.

### 2.3 How a late L0 becomes a partial cover

The #4449 race is a compaction that stalls past its claim lease between its
re-list and its record PUT while an erasure rewrite publishes the same
bucket; the two records have different create-only keys, so both land
(`crates/ravel-maintain/tests/erasure_claim_pause.rs:1-9`). The landed fix
makes the compaction record dominated when one live rewrite holds every
input it names (`crates/ravel-catalog/src/catalog.rs:6134-6139`), which is
the case when "both passes planned from one listing"
(`docs/adrs/1029-advisory-compaction-claims.md:1083-1086`).

A late L0 is what makes the two listings differ. The rewrite plans and
re-lists over `{1, 2}`. Record 3 lands. The compaction plans and re-lists
over `{1, 2, 3}` with no rewrite record visible yet, then stalls in its PUT.
The rewrite publishes `R{1, 2}`; the compaction publishes `C{1, 2, 3}`. `C`
names an input `R` does not hold, so it is not dominated and is pushed as a
conflict (`crates/ravel-catalog/src/catalog.rs:6142-6146`), warned as "may
serve rows the rewrite erased or serve rows twice (#4449)"
(`crates/ravel-catalog/src/catalog.rs:6026-6033`). Every later erasure pass
over the bucket fails with `MultipleLiveRecords`
(`crates/ravel-maintain/src/erasure_rewrite.rs:648-651`). ADR-1029 leaves
this case open by name: "A record whose inputs a live rewrite covers only in
part stays a conflict"
(`docs/adrs/1029-advisory-compaction-claims.md:1112-1113`).

So the partial cover needs three things at once: the #4449 stall, a
compaction and a rewrite racing on one bucket, and **two maintenance
listings of one sealed bucket that disagree about its L0 set**. The first
two are accepted and handled by dominance. The third is what #4561 can
remove.

## 3. The fence options

### 3.0 The measure all of (a) and (b) share

"Past the seal" has to be decided without a writer clock, and identically by
every process that asks. One measure does both:

> A commit record in hour `H` is **late** when the store's `last_modified`
> for it is at or after the cutoff
> `C(H) = end(H) + max_flush_lifetime + clock_skew_allowance`.

`C(H)` is a function of the hour in the record's key and of configuration
(`crates/ravel-maintain/src/bucket.rs:36-40`,
`crates/ravel-maintain/src/config.rs:2778-2781`). `last_modified` is
assigned by the store and returned on every LIST entry and HEAD
(`crates/ravel-object-store/src/lib.rs:204-212`). A commit record is
create-only (`crates/ravel-commit/src/publish.rs:120`), so its stamp is set
once. The verdict uses no reader clock and no writer clock, so two processes
with different clocks reach the same verdict about the same record.

What that does to store-versus-maintain skew: skew no longer changes the
verdict. It changes only when a lister may trust that every on-time record
is already visible. A lister whose clock runs ahead of the store can LIST
before the store's clock has reached `C(H)`, and an on-time record can still
arrive afterwards. That is closed by making the lister wait on the store's
clock as well as its own, using the observation the S3 adapter already keeps
from response `Date` headers: `observed_store_time_ns` "only under-reports",
so a caller "may use it to bound how far *behind* the store its own clock
is" (`crates/ravel-object-store/src/lib.rs:950-964`). `O >= C(H) + settle`
therefore proves the store's clock has passed the cutoff.

Limits of the measure, which every option built on it inherits:

- `last_modified` "may have 1-second granularity"
  (`docs/object-store-contract.md:409`). The cutoff is whole seconds at any
  whole-second configuration; the implementation truncates `C(H)` to a whole
  second so a second-granular and a millisecond-granular reading of one
  record give the same verdict.
- The stamp is when the store stamped the request, which is not when the
  record became visible to a LIST. The gap is the store's own processing
  time. Nothing on this checkout bounds it; `settle` (section 4.1) is the
  allowance for it. **[external]**
- A store whose objects are copied or restored by a tool that assigns new
  timestamps makes every commit record of every sealed hour look late.
  **[external]** for which tools do. This is the failure mode that decides
  between (a) and (b).
- The contract today restricts the field: "it exists for advisory age
  decisions (GC age checks, claim expiry) only ... it may only cost
  duplicated work, never correctness"
  (`docs/object-store-contract.md:409-416`). An option is inside that
  sentence only if a wrong stamp cannot make an answer wrong.

### 3.1 Option (a): fold and reader side

Rule: every resolution ignores (or quarantines) a late commit record.

- **Reads that change:** all of them. The listing resolution
  (`process_bucket`, `crates/ravel-catalog/src/catalog.rs:3728`), the fold's
  classifier (`classify_bucket`, `crates/ravel-catalog/src/fold.rs:7882`),
  both token paths (`crates/ravel-catalog/src/catalog.rs:4538`, `:4822`),
  seal-divergence verification
  (`crates/ravel-catalog/src/seal_divergence.rs:233`), the erasure
  completion gate (`crates/ravel-maintain/src/erasure_rewrite.rs:3305-3321`),
  migrate's raw-served set (`crates/ravel-maintain/src/migrate.rs:1716`) and
  the sweep's referenced-L0 set (`crates/ravel-maintain/src/sweep.rs:924`),
  which today is what keeps a late record's data object from being an
  orphan (`crates/ravel-maintain/src/sweep.rs:758-766`).
- **Can an acknowledged write be lost: yes.** A strict write acked outside
  the clock contract (section 1.2) is token-readable today and would stop
  being readable. A record that lands between the maintain seal and the fold
  seal is folded and served today (section 2.1) and would be hidden. Once
  the sweep stops counting the record as a reference, its data object is
  quarantined after the orphan age gate
  (`crates/ravel-maintain/src/config.rs:2783-2791`).
- **Other failure modes:** a re-stamped store hides every L0 of every sealed
  hour that has no compaction record. During a rolling upgrade an old reader
  serves a late record and a new reader does not, so two replicas answer one
  query differently. A wrong stamp makes an answer wrong, which is outside
  the contract sentence above.
- **Existing store:** a late L0 already present disappears from reads at
  upgrade. An existing conflicting compaction record is not repaired: it
  still names the late input and its parts still hold that input's rows.
- **Needs:** a new ADR that reverses ADR-0018's "stays included"
  (`docs/adrs/0018-l0-l1-compaction.md:185-189`) with an amendment marker
  there, a change to the `last_modified` contract, and a compatibility gate
  for the mixed-version window. No persistent format changes.
- **Cost:** no extra requests on listing paths; the token path's single GET
  must surface the stamp. Six or more resolution sites across three signals,
  each with its own tests.

Rejected for the tag. It becomes safe only after the writer stops
acknowledging late records on store evidence (3.4) and every pre-change
writer is known gone.

### 3.2 Option (b): maintenance planning side

Rule: compaction, migrate and the erasure rewrite plan only from on-time
commit records. Two variants differ in what happens to the late ones.

**(b1) Leave them.** A late L0 stays a raw L0, served exactly as today, kept
by the sweep exactly as today.

- **Reads that change:** none.
- **Can an acknowledged write be lost: no.** Nothing is deleted, moved or
  hidden. A late record is treated the way a late record that arrives after
  a compaction record is treated today (section 2.2).
- **Why it prevents the partial cover:** every planner evaluates the same
  pure predicate over the same store facts, so every planner's input set for
  a sealed bucket is the same set, the on-time records. A compaction that
  publishes beside a rewrite then names exactly the rewrite's inputs, which
  is the dominated case (`crates/ravel-catalog/src/catalog.rs:6134-6139`).
  The third ingredient of section 2.3 is gone.
- **Failure modes:** a wrong or unstable stamp returns the bucket to today's
  behaviour for that record, no worse. A re-stamped store stops compaction,
  migration and RawL0 erasure rewrites of sealed hours that have no record
  yet; nothing is hidden and nothing is served twice; the late counter is
  non-zero on every such bucket, and a configuration switch restores today's
  planning. A late L0 that holds an erasure subject blocks that request's
  completion, as a stray does today
  (`crates/ravel-maintain/src/erasure_rewrite.rs:3333-3337`). A late L0
  below a format floor is counted by the migrate walk as still served raw
  (`crates/ravel-maintain/src/migrate.rs:2119-2127`) and holds the floor
  down, as a stray does today.
- **Existing store:** a late L0 already present is unchanged. An existing
  conflicting record is unchanged; this prevents new ones and repairs none
  (section 5.3).
- **Needs:** no format change and no key change. A decision record
  (ADR-4561, numbered by its issue per `docs/adrs/README.md:5-13`), because
  the rule gives `last_modified` a use the contract's list does not name;
  amendment pointers in ADR-0018 decision 1 and in ADR-1029's 2026-11-21
  amendment (`docs/adrs/README.md:234-238`). It stays inside "never
  correctness": a wrong stamp costs a deferred compaction or the old race,
  not a wrong answer.
- **Cost:** no extra request. The stamp is already in the LIST the planner
  issues (`crates/ravel-maintain/src/read.rs:105-107`) and the store-clock
  observation is an in-process read. A bucket becomes plannable `settle`
  later than it seals.

**(b2) Reclaim or quarantine them in the sweep.** Same planning rule, and
the sweep also moves a late L0 out of the bucket.

- **Can an acknowledged write be lost: yes**, for the same records as
  option (a), since a quarantined record is gone from every read.
- Rejected until 3.4 exists. It is the right end state, not a pre-tag step.

### 3.3 Option (c): writer side, a commit PUT that cannot land late

What the store offers:

- **A condition on time: none.** The commit PUT is already conditional, on
  the key not existing (`crates/ravel-commit/src/publish.rs:120`). The
  adapter's conditional vocabulary is create-if-absent and CAS on version
  (`docs/object-store-contract.md:250-252`); there is no "reject after
  instant" precondition in it.
- **Signature age.** S3 refuses a request whose signing time is too far
  from its own clock, and the adapter surfaces that as
  `StoreError::ClockSkewed` (`crates/ravel-object-store/src/lib.rs:712-718`).
  That is a store-enforced bound on how late a signed request may *arrive*.
  The window is 15 minutes **[external]**, which is wider than the 5-minute
  gap between the writer's bound and the maintain seal and narrower than the
  20-minute gap to the fold seal. It is not configurable and it does not
  bound how long the store takes to apply a request it accepted.
- **An expiring presigned request.** A presigned PUT carries an absolute
  expiry the store checks on its own clock **[external]**. Setting it to
  `end(H) + max_flush_lifetime` would make the store refuse the commit PUT
  on arrival after the writer's own bound, independent of the writer's
  clock. The adapter builds no presigned write today (no `presign` or
  `signed_url` in `crates/ravel-object-store/src/s3.rs`), the create-if-absent
  header and the checksum the commit PUT sends
  (`crates/ravel-commit/src/publish.rs:119-120`) would have to ride on it,
  and `MemoryStore` and any non-S3 backend would need an equivalent or the
  bound is S3-only.

- **Reads that change:** none. **Acknowledged write lost:** no; a refused
  PUT is an abandon, which is already retryable.
- **Failure modes:** bounds arrival, not application, so a residual remains
  and (b) is still needed behind it. A second PUT path beside the adapter's
  for the most durability-critical object in the system.
- **Existing store:** no effect.
- **Needs:** an object-store contract change and an ADR for the new request
  path; no persistent format change.
- **Cost:** no extra requests; a new signing path and its conformance tests.

Not before the tag. Worth its own ticket as the store-side bound.

### 3.4 Option (d1): the writer acknowledges on store evidence

Rule: a flush acks `Ok` only when it holds store evidence that its commit
record's stamp is before `C(H)`; otherwise it answers `Abandoned`.

- Evidence without an extra request: the `Date` of the commit PUT's own
  response is at or after the record's stamp. Today the adapter parses
  response dates into one process-wide "latest wins" value
  (`crates/ravel-object-store/src/lib.rs:950-956`), which is not per
  request, and `put` returns only an etag and a version
  (`crates/ravel-object-store/src/lib.rs:175-178`). The change is to return
  the response date with the outcome. On the `AlreadyExists` branch the
  writer already GETs the record
  (`crates/ravel-commit/src/publish.rs:235-242`).
- Sites: the three `Some(Ok(token))` arms
  (`crates/ravel-ingest/src/shard.rs:1358-1366`,
  `crates/ravel-ingest/src/log_shard.rs:1488-1496`,
  `crates/ravel-ingest/src/span_shard.rs:728-736`).
- **Acknowledged write lost:** no. It removes the strict cases of section
  1.2 at the source: after it, strict `Ok` implies an on-time record. It
  also closes the existing hole where a strict write is acked into an hour
  the fold has sealed (`docs/consistency-model.md:606-617`).
- It does not prevent the partial cover by itself: an unacknowledged late
  record still lands and still enters a listing. It is what makes (b2) and
  (a) safe later, for records written by processes that have it.
- **Needs:** an object-store API addition (not a persistent format), a
  consistency-model change, tests on all three shards. Touches the ack path.

Not before the tag: it changes the acknowledgement path days before a
release, and it is not needed for prevention.

### 3.5 Option (d2): record the sealed set in the store

Rule: the first planner to list a sealed bucket publishes, create-if-absent,
a per-bucket seal object naming the input set it saw; every later planner
plans from that object and treats any commit record it does not name as
late.

- No clock and no `last_modified` at all, so no re-stamp hazard and no
  settle allowance.
- **Reads that change:** none. **Acknowledged write lost:** no.
- **Needs:** a new object and a new key. It cannot live in the bucket's own
  prefix: an unknown key shape there is a hard error for every lister
  (`crates/ravel-maintain/src/read.rs:174-176`), so an older binary would
  fail on it. That makes it a key-layout change, which the invariants
  require an ADR and a new prefix for. The owner declined an ADR for the
  neighbouring store-side closure (one create-only key shared by the two
  record kinds) on 2026-11-20
  (`docs/adrs/1029-advisory-compaction-claims.md:1090-1092`).
- **Failure mode:** a planner with a fast clock seals early and freezes a
  short set; the on-time records it missed become permanent strays.
- **Cost:** one PUT per sealed bucket and one GET per plan.

Not before the tag: a persistent format addition with the full procedure.
It is the fallback if `last_modified` proves unusable on a supported store.

### 3.6 Side by side

| | Reads change | Acked write can be lost | Prevents partial cover | Format or key change | Before the tag |
|---|---|---|---|---|---|
| (a) readers ignore late | all | yes | yes | no | no |
| (b1) planners leave late out | none | no | yes | no | **yes** |
| (b2) b1 plus sweep reclaim | all, via deletion | yes | yes | no | no |
| (c) expiring commit PUT | none | no | narrows, does not close | no | no |
| (d1) ack on store evidence | none | no | no | no | no |
| (d2) seal object | none | no | yes | yes | no |

## 4. Decision

**Implement (b1) before the v0.35.0 tag: maintenance plans only from on-time
commit records, judged by the store's own stamp against a fixed per-hour
cutoff, and leaves late records exactly where and as they are.** A fence can
be made safe before the tag, because this one deletes nothing, hides nothing
and changes no read.

### 4.1 The exact rule

For a bucket of hour `H` with configuration `config`:

- **R1, cutoff.** `C(H) = end(H) + config.seal_margin_ns()`, truncated down
  to a whole second. Same arithmetic as today's seal
  (`crates/ravel-maintain/src/bucket.rs:50-51`).
- **R2, late.** A listed commit record is late when
  `last_modified_unix_ms * 1_000_000 >= C(H)`. Every other listed commit
  record is on time. Compaction records, rewrite records and tombstones are
  never classified.
- **R3, plannable.** A pass may plan the bucket from raw L0 only when both
  hold: its own clock reads `now >= C(H) + SETTLE`; and, read after its
  planning LIST returned, `store.observed_store_time_ns()` is either `None`
  or `>= C(H) + SETTLE`. Otherwise the outcome is the existing `NotSealed`.
  `SETTLE` is a new constant of 60 s, three times the adapter's 20 s request
  timeout (`crates/ravel-object-store/src/s3.rs:271`). It is a chosen
  allowance for the stamp-to-visible gap, not a derived bound (section 7).
  A `None` observation is counted, as the writer counts it
  (`crates/ravel-ingest/src/shard.rs:2514`).
- **R4, inputs.** Compaction, the migrate primitive and a RawL0 erasure
  rewrite take the on-time commit records as their whole L0 set: for the
  minimum-input count, for the inputs they load, and for the input identities
  the published record names.
- **R5, re-list.** The pre-publish re-list compares on-time commit records
  (plus the three other record kinds, as today). A late arrival between plan
  and publish does not abort the pass; an on-time arrival still does.
- **R6, everything else sees every commit record.** The erasure completion
  gate, migrate's raw-served set, the legal-hold check, retention, the sweep
  and every read path keep using the full listing. A late record is served,
  kept, held, expired and counted exactly as today.
- **R7, switch.** `CompactorConfig` gains `late_commit_fence`, `Off` or
  `Enforce`. The library default is `Off`, which is today's behaviour byte
  for byte; the server's maintenance configuration and `ravel-cli maintain`
  set `Enforce` unless `--late-commit-fence off` is given. Section 4.4 says
  why the default sits there.
- **R8, visibility.** Each plan that leaves a late record out logs its key
  at warn and counts it in a new
  `ravel_maintain_late_commit_records_total{signal}`; `ravel-cli maintain`'s
  bucket report prints the late count beside `l0_commit_records`.

Why this prevents the partial cover. Under R1 to R3, a LIST that a plan is
allowed to use runs when the store's clock is past `C(H) + SETTLE`. Every
commit record stamped before `C(H)` is visible to it, given the settle
allowance and the store's list-after-write guarantee the seal lemma already
leans on (`docs/catalog-and-mvcc.md:2812-2815`). Every commit record stamped
at or after `C(H)` is excluded by R2 whenever it arrives. So every plan of
the bucket, by any pass at any later time, has the same L0 set. A compaction
that publishes beside a rewrite then names inputs the rewrite holds in full,
which `erasure_dominance` resolves as dominated
(`crates/ravel-catalog/src/catalog.rs:6134-6139`). No stall length changes
that, because the argument does not use time after the LIST.

Why no acknowledged write is dropped. R6: no code path deletes, moves or
stops serving a commit record because it is late.

### 4.2 Code sites

All in `ravel-maintain` and its two service entry points. No change in
`ravel-ingest`, `ravel-catalog`, `ravel-commit`, `ravel-object-store`,
`proto/`.

| # | Site | Change |
|---|---|---|
| 1 | `crates/ravel-maintain/src/read.rs:66-80` | `BucketListing` keeps each commit key's `last_modified_unix_ms`, gains `late_commit_keys` and the cutoff it was classified under, and a `planning_commit_keys()` accessor. `commit_keys` stays the full set (R6). |
| 2 | `crates/ravel-maintain/src/read.rs:166-185` | `partition_bucket_listing` records the stamp it discards today at `:170`. |
| 3 | `crates/ravel-maintain/src/bucket.rs:47-52` | Add `late_commit_cutoff_ns(config)` and `is_plannable(now_ns, observed_store_ns, config)`. `is_sealed` is unchanged and keeps its other callers. Rewrite the doc comment (section 6, item 2). |
| 4 | `crates/ravel-maintain/src/config.rs:1088-1097`, `:2081-2084`, `:2774-2781` | `LATE_COMMIT_SETTLE_NS`; the `late_commit_fence` field; rewrite the `seal_margin_ns` doc comment. |
| 5 | `crates/ravel-maintain/src/compact.rs:684-712` | R3 around the LIST at `:689`; R4 at the count (`:693-699`) and the load (`:705-712`). |
| 6 | `crates/ravel-maintain/src/rewrite.rs:405-415`, `:428-436` | R5: `same_record_set` compares `planning_commit_keys()`, and `relist_changed` classifies the new listing under the planned listing's cutoff. |
| 7 | `crates/ravel-maintain/src/rewrite.rs:865-891` | R3 and R4 for the migrate primitive. |
| 8 | `crates/ravel-maintain/src/erasure_rewrite.rs:2484-2492`, `:714-715` | R3 for the rewrite pass; R4 for its RawL0 inputs. |
| 9 | `crates/ravel-maintain/src/erasure_rewrite.rs:3305-3310` | No change. A test pins that the completion gate still reads every commit record. |
| 10 | `crates/ravel-maintain/src/migrate.rs:324`, `:2125-2146` | A `NotMigratedReason` for a bucket whose remaining below-target raw L0 is late, so the walk names it and the floor stays down for a stated reason. |
| 11 | `services/ravel-server/src/maintain.rs:1386`, `services/ravel-cli/src/maintain.rs:401`, `:2027` | Set `Enforce`; the `--late-commit-fence` flag; the late count in the bucket report. |
| 12 | `services/ravel-server/src/metrics.rs:6540` (beside the other maintain counters) | Render the new counter and the store-clock-unobserved counter. |

### 4.3 Tests

Each test names the claim it pins. Fixtures: `MemoryStore::set_clock_ms`
sets the stamp of the next PUT
(`crates/ravel-object-store/src/memory.rs:79-82`, `:366`),
`set_observed_store_time_ns` sets the store-clock observation
(`crates/ravel-object-store/src/memory.rs:94`), `FaultStore::hold` parks a
call (`crates/ravel-object-store/src/fault.rs:685`), and the injected
`FixedClock` is the pass's own clock. New file
`crates/ravel-maintain/tests/late_commit_fence.rs` unless noted.

1. **A late L0 never enters a compaction, migrate or rewrite listing.**
   `a_commit_record_stamped_at_the_cutoff_is_left_out_of_every_plan`: three
   inputs, stamps `C(H) - 1 ms`, `C(H)` and `C(H) + 1 h`. Compaction, the
   migrate primitive and a RawL0 erasure rewrite each publish a record whose
   `inputs` are exactly the first identity's set; assert the exact identity
   list, not a count. Shown failing with the fence `Off`.
2. **A late L0 never turns the #4449 race into a partial cover.**
   `a_late_l0_between_the_two_listings_is_a_full_cover`, built on
   `a_compaction_paused_inside_its_record_put_publishes_beside_the_erasure_rewrite`
   (`crates/ravel-maintain/tests/erasure_claim_pause.rs:348`): the rewrite
   plans and re-lists, a late L0 lands, the compaction plans and is parked in
   its record PUT, the rewrite publishes, the gate releases. Assert:
   `erasure_dominance` reports the compaction record dominated and
   `conflicts` empty; a resolve returns every surviving row exactly once and
   no erased row; the erasure rerun is `AlreadyApplied`, not
   `MultipleLiveRecords`. The same script with the fence `Off` must produce
   the conflict, which is the negative control.
3. **An acknowledged write is never dropped.**
   `an_acknowledged_write_that_lands_late_is_served_and_never_swept`: seed
   one late L0, run compaction, a full sweep with the clock aged past
   `grace + max_flush_lifetime` and past the protection horizon, and an
   erasure pass for an unrelated subject. Assert the late commit record and
   its data object are still present, a token resolve and a listing resolve
   both return its rows exactly once, and the fold's classifier includes it.
   Second half in `crates/ravel-compat-tests/tests/e2e.rs` (which already
   drives `set_clock_ms`, `:555-557`): a strict write through a real router
   whose commit PUT is stamped past `C(H)` returns `Ok` with a token, and a
   read with that token returns the rows before and after compaction and
   sweep.
4. **The planner waits on the store's clock.**
   `a_plan_waits_until_the_store_clock_passes_the_cutoff`: own clock sealed,
   observation at `C(H) + SETTLE - 1 s` gives `NotSealed`; at
   `C(H) + SETTLE` the pass runs; `None` runs and increments the unobserved
   counter by exactly one.
5. **The re-list.** `a_late_arrival_does_not_abort_a_publish` and
   `an_on_time_arrival_still_aborts_a_publish`, beside
   `the_pre_publish_relist_aborts_when_the_record_set_changed`
   (`crates/ravel-maintain/tests/erasure_compaction_fence.rs:492`).
6. **Erasure and a late L0.** A late L0 holding the subject: the rewrite's
   record does not name it and the request stays blocked with no `.done`. A
   late L0 not holding the subject: the request completes.
7. **Migrate.** A late below-target L0 is reported under the new
   `NotMigratedReason`, and the walk goes on.
8. **Determinism.** A proptest over arbitrary stamps and listing orders,
   with `MemoryStore::with_page_size(2)`: the on-time and late split is the
   same for every permutation and page boundary, and equals the split
   computed from the stamps directly. Check in its regression seed file.
9. **`Off` is today.** With the fence off, the listing and every outcome in
   an existing compaction test are unchanged.
10. **Antithesis.** A new `inv_always!` at the record publish
    (`crates/ravel-maintain/src/publish.rs:495`, beside the existing one):
    "maintain: a published record names no late commit record", and an
    `inv_sometimes!`: "maintain: a plan left a late commit record out". The
    catalog-maintenance rig jumps clocks forward
    (`antithesis/catalog-maintenance/README.md:442-446`), so one run of it is
    part of the gate for this change: R3 makes a maintainer whose clock
    jumped ahead wait for the store.

### 4.4 Why the library default is `Off`

Three callers publish commit records into hours that sealed long ago, with a
fresh store stamp, on purpose:

- `ravel-cli commit reconstruct` rebuilds lost commit records from data
  objects (`services/ravel-cli/src/reconstruct.rs:1-11`), each a new
  create-if-absent PUT (`services/ravel-cli/src/reconstruct.rs:31-35`). Every
  one is late by R2.
- Benches seed old hours and then compact them, for example
  `crates/ravel-bench/src/bin/compaction_bench.rs:272`.
- Many maintenance tests seed inputs after moving the store's clock; 19
  files under `crates/ravel-maintain` and `crates/ravel-compat-tests` call
  `set_clock_ms`.

With `Enforce` as the library default, the benches would compact nothing
and exit zero, and an unknown number of fixtures would change meaning. With
the default `Off` and the two service entry points setting `Enforce`, the
production maintainer and the Antithesis rigs (which run the real server)
get the fence and nothing else moves. Reconstructed records then need the
operator to run maintenance for that shard with `--late-commit-fence off`
once its writers for those hours are known gone; the reconstruct guide gains
that step. This is the fence's real operating cost and it is stated in the
ADR, not hidden.

### 4.5 ADR and document changes, in the same commits as the code

- **New `docs/adrs/4561-late-commit-fence.md`.** The rule R1 to R8, the
  premise of section 1, the options, the residuals of section 5, and the
  statement that no persistent format, key or proto changes.
- **Amendment markers** (`docs/adrs/README.md:234-238`): ADR-0018 decision 1
  (`docs/adrs/0018-l0-l1-compaction.md:120-126`), whose "complete, stable
  input set" becomes "the on-time set"; ADR-1029's 2026-11-21 amendment
  (`docs/adrs/1029-advisory-compaction-claims.md:1112-1113`), which gains
  "and a late L0 can no longer produce one"; ADR-2708 D3
  (`docs/adrs/2708-due-diligence-p1-fixes.md:209-212`), whose sentence about
  the skew margin covering an in-flight PUT is withdrawn.
- **`docs/object-store-contract.md:409-416`.** Add the planning exclusion to
  the uses of `last_modified`, with the argument that a wrong stamp costs a
  deferred compaction or the old race and never a wrong answer.
- **`docs/catalog-and-mvcc.md:2804-2815`.** Restate the seal lemma as what
  it is: a writer does not *issue* a PUT past its bound; a PUT already
  issued can land later; maintenance excludes such a record by R2 and the
  fold does not see it past its own seal (already said at `:2921-2926`).
- **`docs/consistency-model.md:104-108`, `docs/ingest.md:1046-1049`.**
  Replace "keeps every publish inside the catalog's seal" and "cannot
  publish into an hour the catalog has already sealed".
- **`docs/deletion-and-gc.md:1090-1093`**, which already names "a late L0
  flush into a sealed bucket": point it at the rule.
- **Operations guide and the reconstruct guide.** The counter, what a
  non-zero value means, the switch, the re-stamp failure mode.
- **Code comments**: `crates/ravel-maintain/src/bucket.rs:47-49`,
  `crates/ravel-maintain/src/config.rs:2774-2777`.
- **`changelog.d/4561.fixed.md`**, and the 0.34.0 known-issue note for #4449
  gains the pointer.

### 4.6 Size and risk

| | Files | Lines |
|---|---|---|
| `ravel-maintain` source (sites 1 to 10) | 7 | about 300 |
| Service wiring and metrics (sites 11, 12) | 3 | about 90 |
| New tests | 2 new, 2 extended | about 650 |
| ADR, docs, changelog | 9 | about 350 |

About two days for one person who knows `ravel-maintain`, plus one
catalog-maintenance Antithesis run.

Risk, stated plainly:

- **Data loss: none by construction.** The change removes inputs from
  plans; it adds no delete and no read filter.
- **Liveness: real.** A store whose stamps are wrong or re-assigned stops
  planning for the affected buckets until the switch is thrown. It is loud
  (counter, warn per key) and reversible.
- **`CompactorConfig` gains a field**, and it is built by struct literal in
  at least a dozen places in the services
  (`services/ravel-server/src/compat_maintain.rs:402`, `:1308`, `:1364`;
  `services/ravel-server/src/maintain.rs:5099`, `:5255`); the
  struct-literal guard exists for exactly this.
- **Exact-boundary tests of the planners' seal** may move by `SETTLE` when
  they run with `Enforce`; with the library default `Off` they do not.
- **Effectiveness depends on two store properties** that this checkout
  cannot prove (section 7). When either fails, the result is today's
  behaviour for that bucket, which the dominance counter still reports.

## 5. What the decision does not close, and the follow-ups

<!-- section 5 -->

## 6. Claims in the ADRs, docs and properties that this reading shows false

<!-- section 6 -->

## 7. What this document could not verify

<!-- section 7 -->
