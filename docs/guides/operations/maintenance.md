# Maintenance (day 2)

**Only a `--mode maintain` process runs maintenance.** Compaction, retention,
the sweeper that issues the deletes, and the at-rest integrity scrubber all run
in that mode and in no other. A process in `--mode all` runs ingest, the query
API, the catalog fold and alert evaluation, and none of the maintenance loops. A
deployment made only of `all` processes therefore never compacts an object and
never deletes one: its L0 segments accumulate unmerged, retention windows have
no effect, and nothing reclaims storage. The quickstart is exactly such a
deployment, by design.

The catalog fold runs on a timer in `maintain` and in `all`, and in no other
mode. A `gateway` or `query` process folds only when an operator calls
`POST /api/v1/admin/fold`, which stays available in `all` and `query`.

If you are wondering why nothing is being reclaimed, check for a `maintain`
process before anything else.

- [Running the maintenance loop](#running-the-maintenance-loop)
- [Catalog fold and verify](#catalog-fold-and-verify)
- [Compaction](#compaction)
- [Garbage collection and retention](#garbage-collection-and-retention)
- [The at-rest integrity scrubber](#the-at-rest-integrity-scrubber)
- [Format migration](#format-migration)
- [Legal hold](#legal-hold)
- [Reclaiming a pre-namespacing cache directory](#reclaiming-a-pre-namespacing-cache-directory)
- [The maintenance and inspection commands](#the-maintenance-and-inspection-commands)

## Running the maintenance loop

**Continuously.** `ravel-server --mode maintain` runs the loop per tenant, over
all three signals and every shard, on `--maintain-interval-secs` (default 300).
It needs a backend that reports the `multipart` capability, and it serves no
ingest or query routes. It still binds `--listen-http` for liveness.

`--maintain-tenant <name>`, repeatable, names a tenant this process maintains in
addition to every tenant named by `--tenant-token` or `--tenant-token-file`.
It is required for a
deployment that authenticates through OIDC or mTLS, because those tenants are
only known once a request arrives and maintenance has no other way to learn
about them.

Four flags size how much work one tick does and how quickly a stuck unit is
visible:

| Flag | Default | What to change it for |
|---|---|---|
| `--maintain-interval-secs` | `300` | How often each tenant's tick runs. `0` is refused at startup. |
| `--maintain-unit-concurrency` | `4` | How many owned units this process maintains at once within one tenant's tick, so one pathological unit cannot starve the rest. Raise it on a process that owns many units and has spare request concurrency to spend, lower it on a host shared with other work. It is clamped to at least 1, and `0` degrades to a sequential walk rather than deadlocking. |
| `--maintain-stalled-after-intervals` | `3` | Consecutive failed ticks a unit must accrue, with no intervening success, before it counts as stalled. Lower it to be paged sooner on a flaky unit at the cost of noise from transient faults; raise it to tolerate a noisier store at the cost of a slower page. A single success resets that unit's counter. |
| `--maintain-interior-reverify` | `6h` | The slow safety-net cadence for hours that are neither at the head nor the tail of the keyspace this tick. An interior bucket's memoized state is re-verified no less often than this, and the sweeper runs a full-keyspace pass on the same cadence instead of its per-tick head-and-tail pass. Head and tail hours are unaffected and are evaluated every tick. Zero disables the safety net, which makes every interior bucket always due. |

**One-shot.** Every loop also has a `ravel-cli` form for inspection or for
running one pass by hand. See
[the maintenance and inspection commands](#the-maintenance-and-inspection-commands).

A unit here is one `(tenant, signal, shard)` triple. In a multi-replica maintain
deployment, units are distributed across the live workers so that summed across
every live worker each unit is owned exactly by one of them. The metric families
that report ownership, stalls and merge memory are catalogued in
[the observability guide](../observability.md).

## Catalog fold and verify

The fold is a query-cost optimization, not a durability mechanism. Resolve
always falls back to listing commit records directly, so a folder that never
runs, crashes or falls behind never loses or hides data. It only makes queries
pay listing cost over a wider window.

That cost is real, though. Two cases list commit records per bucket on every
query: the open window above the fold watermark, bounded by `max_ingest_lag`
(default 2h), and any tenant with folding disabled or not yet caught up. That
listing path does not scale past roughly 10,000 commit records in one bucket, so
a tenant whose fold has stalled behind a heavy load will feel it at query time
before anything else goes wrong.

### Which processes fold

The scheduled fold runs in `maintain` and in `all`, and nowhere else.

A `maintain` process folds only the `(tenant, signal)` pairs it owns, using the
same rendezvous hash and the same heartbeat live set that distribute
maintenance units. Scaling `maintain` from one replica to
N therefore divides the fold work N ways instead of running N full copies of
it, and a non-owner issues no request at all for a pair it does not own: the
ownership test is pure computation over the tenant listing and it runs before
every per-tenant read, that pair's `t/<hash>/config` lifecycle record and its
`HEAD` peek included. The one request a tick makes that no owned pair accounts
for is the single delimited listing of `t/` that finds the tenants in the first
place. Ownership is evaluated per tick, so a replica that leaves the fleet
hands its pairs to the survivors with no operator action.

The handover bound is 570 s (9 min 30 s) at the defaults, not the liveness
window alone. Three terms add up, and each is the worst case of the one before
it:

| Term | Default | Seconds |
|---|---|---|
| Liveness window, `liveness_factor * heartbeat_interval` | 3 x 60 s | 180 |
| The survivor's next heartbeat tick, which is when it re-lists and recomputes the live set | 60 s | 60 |
| The survivor's next fold tick, `--fold-interval-secs` plus up to 10% jitter | 300 s + 30 s | 330 |
| **Sum** | | **570** |

570 s bounds the survivor's first fold tick over the pair. It is not always
the tick that folds it. If the departed replica folded the pair just before it
left, the pair's `HEAD` stays younger than `--fold-interval-secs` for 300 s
after departure, and a survivor tick inside that span skips the pair as
already fresh. The skipped tick came before the 300 s mark, and the survivor's
next fold tick is at most 330 s (300 s plus 10% jitter) after it, so the
re-fold lands within 300 + 330 = 630 s (10 min 30 s) of departure. A survivor
tick at or after the 300 s mark folds the pair on that tick, inside the 570 s
above. A fold earlier than just before departure only moves the 300 s mark
earlier, so 630 s after departure bounds the re-fold in every case,
ignoring clock skew between replicas.

The departed replica's heartbeat record stops being refreshed, but nothing
reacts to that on its own: a survivor only drops it once it reads the record
at more than `3 * H` old, and it reads on its own heartbeat cadence, which is
the second term. The recomputed set then reaches the fold through a `watch`
channel the fold reads at the top of each of its own ticks, which is the
third. The liveness window alone bounds when the departure becomes visible,
not when the pairs are folded again.

Nothing here is a durability bound: an unfolded pair costs query listing and
nothing else. A shorter `--fold-interval-secs` shortens the third term
proportionally; the heartbeat interval and the liveness factor carry no flag
and are always the defaults above.

A `maintain` process still serves no query surface. It folds because the fold
is scheduled work over the same tenant list it already walks, and because the
catalog it writes is read by the query processes.

A `--mode all` process is a solo fleet of one: its live set is itself, so it
owns and folds everything.

`gateway` and `query` processes do not fold on a timer. They scale on request
load, and an extra replica there used to mean another full copy of the fold. A
`query` process keeps the on-demand route (`POST /api/v1/admin/fold`, mounted
in `all` and `query`), so a one-off fold by hand is still available there. A
`gateway` process mounts no fold route at all and cannot fold by any route,
which was already true before the scheduled fold moved.

A deployment with neither a `maintain` nor an `all` process never folds on a
timer, and its catalog stays unsealed until someone calls that route. Run at
least one `maintain` process, which is the same advice the top of this page
gives for compaction and retention.

`--disable-fold` turns the scheduled task off. `--fold-interval-secs` (default
300) controls only how often it wakes up to check for newly sealed hours; it has
no bearing on when an hour becomes eligible to seal. Both flags configure the
scheduled fold and nothing else, so a `gateway` or `query` process refuses to
start when either one is passed explicitly and names the flag and the mode in
the error. Accepting a flag that mode would ignore is how a fleet ends up
believing it has turned the fold off on a process that was never going to run
it.

Disabling the fold has one monitoring consequence to know: the fold-liveness
gauge (`ravel_catalog_fold_last_success_timestamp_seconds`) never advances past
its `0` sentinel with no fold to stamp it, so the `RavelCatalogFoldStalled`
alert in the observability guide pages about ten minutes after start. That is
consistent with the unsealed span growing, but if you run a fleet with the fold
intentionally off, drop that alert for it rather than leave it paging. A fleet
with no scheduled-fold process at all renders that gauge nowhere, and the same
alert's `absent()` arm fires instead. Such a fleet can still render
`ravel_catalog_fold_cycles_total` and `ravel_catalog_fold_failures_total`: a
`query` process reports both for the on-demand route it keeps.

### The seal margin, and why it matters

A fold seals an hour only once:

```
now >= hour_end + max_flush_lifetime + clock_skew_allowance + fold_safety_margin
```

The defaults are 1h, 5m and 15m, so 1h20m in total. Those three margins give
every writer's flush for that hour time to land before the fold treats it as
closed. If you widen `max_flush_lifetime`, so writers hold flushes open longer,
or widen the tolerated wall-clock skew between writers and the folder, and you
do not review `fold_safety_margin` at the same time, you make the clock-skew
failure mode below reachable.

### Folding a tenant whose writers have exited

The 1h20m margin exists for writers that are still running. After a bulk load
whose loader process has exited, nothing can publish into those hours any more,
and waiting the margin out only costs query time: until the fold covers them,
every query pays one commit-record read per segment.

```sh
ravel-cli catalog fold --tenant <name> --shards <n> --signal <signal> \
  --max-flush-lifetime 0s
```

This drops only the flush-lifetime term. The clock-skew allowance and the fold
safety margin still apply, a 20 minute margin, so the hour currently being
written is still not sealed. The report's `seal_margin` line shows the sum
actually used.

**Do not use it while a writer for that tenant is live.** A commit record
published into a bucket this fold already sealed is never picked up by a later
incremental fold, which re-lists only hours after the watermark. The repair is
the HEAD-deletion rebuild in
[troubleshooting](troubleshooting.md#queries-are-missing-recently-written-data).

### Routine verification

```sh
ravel-cli catalog verify --tenant <name> --signal <signal>
```

`catalog verify` re-lists every sealed commit record for one signal and diffs it
against that signal's snapshot, printing counts of entries missing from or
mismatched against the snapshot and exiting nonzero on any divergence. It only
lists and compares and never mutates, so it is safe to run at any time against a
live tenant.

Run it on a schedule, and after you deploy or reconfigure seal margins. It is
the cheapest way to catch a clock-skew divergence before it is noticed at query
time. Run it once per signal the tenant actually writes: a tenant's logs
snapshot is a separate object from its metrics snapshot, and `--signal` defaults
to metrics, so on a logs-only tenant the default invocation reports "nothing to
verify" and tells you nothing.

The same signal-per-object rule applies to folding. The background fold task
covers all three signals, but `ravel-cli catalog fold` folds the one signal
`--signal` names. Folding metrics on a logs-only tenant reports an entry count
of zero and publishes an empty metrics HEAD.

## Compaction

After an ingest-hour bucket is sealed, which is its end plus
`max_flush_lifetime` and `clock_skew_allowance`, so no further commit can
appear, the compactor rewrites its many small L0 segments into a handful of
large L1 segments. It publishes one compaction record naming the L0 inputs it
superseded. It never deduplicates, and it checks before publishing that the L1
outputs hold exactly as many records as the L0 inputs, so a query over the L1
output returns the same rows as a query over the L0 inputs.

This is the primary win of running maintenance at all: object count per hour
drops from thousands to a handful, and every query over that hour pays
proportionally fewer requests.

Compaction is signal-generic. Metrics, logs and spans go through the same code.

To compact by hand, one bucket or one whole tenant and signal:

```sh
ravel-cli maintain compact-bucket --tenant <t> --signal <metrics|logs|spans> \
  --shard <n> --hour <n> [--dry-run] [--no-claim]

ravel-cli maintain compact-tenant --tenant <t> --signal <metrics|logs|spans> \
  [--shards <n>] [--from-hour <n>] [--to-hour <n>] [--bucket-concurrency <n>] \
  [--dry-run] [--no-claim]
```

Under per-role storage credentials, run both with the Maintain credential, not
Admin: they take compaction claims and write L1 segments and compaction
records, which only the Maintain policy grants (see
[the Admin credential](deployment.md#the-admin-credential)).

`compact-tenant` discovers the hours itself, walking each shard's ingest hours
ascending and stopping at the first unsealed one, because every later hour is
unsealed too. It streams one line per bucket as each completes, then a summary
of compacted, already-compacted, not-sealed, below-minimum, tombstoned,
claim-skipped and claim-cancelled counts, segments written, wall time, the
failure count and the concurrency it used. A bucket whose compaction errors
does not abort its siblings: the walk completes, prints each failed bucket's
own outcome line, and exits nonzero with an aggregate naming how many failed
and how many succeeded. A clean run exits zero, and a not-sealed bucket is a
reported outcome rather than a failure.

`--bucket-concurrency N` runs up to N buckets at once and is refused at 0. Each
concurrent bucket gets a per-bucket share of the merge cursor budget, the whole
budget divided by N, so the memory envelope of an N-bucket run stays inside one
host. A merge that no longer fits its share fails closed with a typed
budget-exceeded error rather than growing past it. `--bucket-concurrency 1`, the
default, is the fully sequential walk.

With no `--shards` and no provisioning record, `compact-tenant` refuses and names
the tenant. With both, the two must agree.

**`--max-flush-lifetime` on either command is a safety override, not a tuning
knob.** It overrides the compactor's flush lifetime for that invocation, with the
same humantime grammar as the server flag (`30m`, `1h5m`, `0s`). A bucket is
sealed only once `now >= hour_end + max_flush_lifetime + clock_skew_allowance`,
so a freshly finished load waits over an hour before its final hours can be
compacted, and lowering this seals them at once. It is unsafe below the ingest
path's real flush lifetime: a bucket a writer is still flushing into can then be
sealed and compacted, and that writer's later-published object is missed by the
compaction. Use the override only for a tenant known to be quiescent, such as
one whose bulk load has finished.

### L1 segment size

Two targets decide where the compactor closes one L1 segment and starts the
next. A log segment closes on whichever it reaches first; a span merge reads
only the memory split target and a metrics merge only the stored-size target:

- The **memory split target**, `l1_part_memory_target_bytes`, sizes the
  decoded record heap one segment holds while it is merged. It is a split
  point, not a ceiling: a span merge checks it only between traces, so a
  segment can run past it by a whole trace. A metrics merge does not read it.
  Set it with
  `--maintain-l1-part-memory-target-bytes` on the server and
  `--l1-part-memory-target-bytes` on `compact-bucket` and `compact-tenant`.
- The **stored-size target**, `max_l1_part_bytes` (default 256 MiB), is the
  cap on the encoded object size of a log or metrics segment, measured by
  encoding the segment. Span merges do not read it. Set it with
  `--max-l1-part-bytes` on `compact-bucket` and `compact-tenant`; the server
  has no flag for it.

When the memory split target is not set, the log merge's target is derived
from the memory budget, and the span merge's target stays 256 MiB because span
segments have no stored-size target to cap them:

```text
budget = host memory - 2 GiB overhead reserve - merge cursor budget  (at least 0)
target = max( min( budget / 8 / concurrent_merges,
                   claim lease * 10 MiB/s / 2,
                   8 GiB ),
              256 MiB )
```

The three terms inside `min` are the memory share, the claim lease and the
ceiling; the 256 MiB floor is applied last. The merge cursor budget is the
memory a merge may hold in its cursors on top of the segment it is building
(20 GiB by default, no shipped flag changes it), so the derivation does not
hand the segment memory the cursors already claim. The lease term keeps the
segment small enough to encode and upload inside the claim lease (the server's
startup check assumes 10 MiB/s and warns when the lease is under twice the
transfer time): 300 s allows 1500 MiB.

The server's host memory is its effective memory (`MemTotal` lowered to a
cgroup limit; the same detector `ravel-cli` calls) and its concurrent merges
are `--maintain-unit-concurrency`; it deducts the cursor budget once, so at a
unit concurrency above 1 it undercounts what that many concurrent merges can
hold in their cursors. For `ravel-cli` the host memory is `MemTotal` from
`/proc/meminfo`, lowered to a cgroup memory limit when one is set (`sysctl
hw.memsize` on macOS), and the concurrent merges are `--bucket-concurrency` (1
for `compact-bucket`). `compact-tenant` splits the one 20 GiB cursor budget
between its concurrent buckets, so the whole 20 GiB is deducted whatever
`--bucket-concurrency` is. When the host memory cannot be read, the target
falls back to 256 MiB, `ravel-cli` prints a note on stderr saying so, and a
`--mode maintain` server logs a warning. An explicit flag wins over the
derivation, is used as given for both log and span merges, and is refused at 0.

Worked figures, from a segment of 256 MiB being about 32k rows and 2.1 MB
stored on the 104-column schema below and scaled linearly from there:

| Host and command | Budget | Target | Bound by | Rows | Stored |
| --- | --- | --- | --- | --- | --- |
| Server, 30 GiB, 4 units | 30 - 2 - 20 = 8 GiB | 256 MiB | share (equal to the floor) | 32k | 2.1 MB |
| `compact-bucket`, 32 GiB | 32 - 2 - 20 = 10 GiB | 1.25 GiB | share | 160k | 10.5 MB |
| `compact-bucket`, 64 GiB | 42 GiB | 1.46 GiB | claim lease | 188k | 12.3 MB |
| Server, 64 GiB, 1 unit, `--maintain-claim-lease 1200s` | 64 - 2 - 20 = 42 GiB | 5.25 GiB | share | 672k | 44 MB |

The 256 MiB floor binds while the budget is at most 2 GiB per concurrent
merge: `compact-bucket` keeps 256 MiB up to a 24 GiB host, `compact-tenant`
up to `22 GiB + 2 GiB * --bucket-concurrency`, and a server at the default
`--maintain-unit-concurrency` of 4 up to a 30 GiB host. A lease too short for
256 MiB (under about 51 s) still gets 256 MiB, and the server's startup check
then warns, which is the signal to raise the lease.

To get bigger log segments, raise one of the three things the derivation reads:
the host's memory, the claim lease (`--maintain-claim-lease` on the server; the
CLI takes the compactor's default), or set the flags. There is no flag for the
cursor budget. Setting `--l1-part-memory-target-bytes` above 256 MiB without
`--max-l1-part-bytes` leaves the log stored-size cap at 256 MiB, so the log
merge runs exact-encode probes once a segment's uncompressed payload reaches
that cap, each a clone and encode of the whole in-progress segment, and the
server has no stored-size flag to raise the cap with. Give `compact-bucket` and
`compact-tenant` both flags when you set the larger target by hand.

Which setting decides segment boundaries depends on the codec. Logs (RLOG) read
the RLOG memory split target and the RLOG stored-size cap, and their segment
bytes also depend on the compaction zstd level. Spans (RSPAN) read the span
merge's memory split target only: 256 MiB unless `--l1-part-memory-target-bytes`
was set explicitly, and the `rspan_l1_part_memory_target_bytes` line names it.
Metrics (RSEG) read the stored-size cap (`max_l1_part_bytes`) only and never
read a memory target. When the memory target is derived the RLOG stored-size
cap is set to the same value, so the memory target closes every segment and no
probe runs; `--max-l1-part-bytes` sets the RLOG and the metrics cap together.
None of them is part of a compaction record's identity. Two processes that
resolve different log targets cut the same bucket into different segments: on
a 30 GiB host at the default unit concurrency of 4 the server derives 256 MiB
per merge and `compact-bucket` 1 GiB.

When a run fails with "compaction converged on a prior record that references
part ... which is absent" (or with the `AlreadyExists` variant that says the
part was gone when HEAD-verified), the message names the settings that decide
part boundaries: rebuilding that part reproduces its key only with the values
the run that wrote the record used. The record writer's report or startup log
names the memory target it resolved. No shipped command rebuilds the part
today: once a bucket's listing carries a compaction record,
`compact-bucket`, `compact-tenant` and the server's maintenance loop return
`AlreadyCompacted` and build nothing, so re-running them with other flags
changes nothing. The record keeps pointing at the absent part, and the
superseded-input sweep deletes the bucket's L0 inputs once the record is older
than the protection horizon, which turns the gap into data loss. Meanwhile,
hold the inputs with a [legal hold](#legal-hold) on that shard
(`ravel-cli hold set --tenant <id> --signal <signal> --shard <n>`) until a
repair command ships.

Decoded heap runs far ahead of stored bytes on a wide schema, which is why the
derivation matters. On a 104-column schema a decoded record is about 8 KiB, so
the old fixed 256 MiB target closed segments at about 32k rows and 2.1 MB
stored. The 1.25 GiB target a 32 GiB host derives holds about 160k rows, about
10.5 MB stored, five times the rows per segment (scaled linearly from the
256 MiB figures). The stored-size target stays the operator's cap: set
`--max-l1-part-bytes` to bound object size regardless of how much memory the
host has, below the derived target if you want it to bind.

Each run says which values it used and why. `compact-bucket` and
`compact-tenant` print one line per codec in their report, for example
`rlog_l1_part_memory_target_bytes: 1342177280 (resolved from a memory budget of
10737418240 over 1 concurrent merge; bound by the memory share, budget / 8 /
merges)` from `compact-bucket` on a 32 GiB host, or
`rlog_l1_part_memory_target_bytes: 1073741824 (set by flag)`, followed by
`rlog_max_l1_part_bytes:` (the log merge's stored-size cap),
`rspan_l1_part_memory_target_bytes:` (the span merge's target) and
`max_l1_part_bytes:` (the cap metrics merges read). The `bound by` clause names
the derivation term that decided the target: the memory share, the claim lease,
the 256 MiB floor or the 8 GiB ceiling. The server logs them once at startup as
a `performance default resolved` line with
`setting=rlog_l1_part_memory_target_bytes`, `source` set to `derived`, `flag` or
`fallback`, `bound` naming the term (`memory_share`, `claim_lease`, `floor` or
`ceiling`), `rlog_max_l1_part_bytes`, `rspan_l1_part_memory_target_bytes` and
`max_l1_part_bytes`.

### Running compact-tenant beside a live cluster

The background supervisor and a `compact-bucket` or `compact-tenant` run can
reach the same sealed bucket at the same time. Both ask for the bucket's claim
first, and the one refused the claim does not merge. The CLI asks for a claim
on every bucket before merging it, whatever its size, one claim per bucket at
any `--bucket-concurrency`, all under one process id per invocation. The report
header prints that id on its `claims:` line, and it is the holder a supervisor
names when it skips a bucket the CLI holds.

A bucket refused its claim is not merged. It prints `outcome=ClaimSkipped`
with the reason, the claim's `work_id` (the last segment of its key,
`sys/maintain/claims/compaction/<work_id>`), the holder's process id
(`unknown` when there is no readable claim to name one), the claim's expiry
as `claim_expiry_unix_ms` and the earliest useful retry point as
`retry_after_unix_ms`, and is counted in `claim_skipped`. A bucket that loses
its claim mid-merge, because another process took it over or the claim object
is gone, stops without publishing, prints `outcome=ClaimCancelled` with the
checkpoint it stopped at, and is counted in `claim_cancelled`. Parts it had
already written stay in the store, content-addressed, for a later run to
reuse. Neither counts as a failure: the walk carries on
and exits zero unless some other bucket failed, the same as for a not-sealed
bucket. `compact-bucket` prints the same two outcomes and also exits zero.

To compact a skipped bucket yourself, rerun at `retry_after_unix_ms`, not at
`claim_expiry_unix_ms`: the retry point tracks the printed expiry only for
`held_by_another`, and for the other three reasons the printed expiry is the
wrong instant to wait for. If the holder finished its merge by then, the rerun
reports the bucket as already compacted. The four reasons are:

- `held_by_another`: a live claim, held by the process id printed. The retry
  point is one millisecond past the printed expiry.
- `steal_lost`: the claim had expired and this run tried to steal it, but
  another contender's steal won the compare-and-swap first. The printed expiry
  is the one already in the past, and the winner has just written a fresh
  lease, so the retry point is a full lease from the moment of the loss.
- `unreadable_claim`: the claim object does not decode, so it is never stolen
  (never overwrite what you cannot read) and never deleted programmatically.
  The bucket stays held, for compaction and for erasure alike, until an
  operator removes the claim object: a run that went ahead without the claim
  could publish a compaction built from data an erasure of the same bucket has
  already removed. Retrying does not clear it: every retry reports the same
  reason. You can tell it from a live holder because the holder prints as
  `unknown` and the reason stays `unreadable_claim` on every retry, and once
  the object is older than one lease the run logs a warning naming the claim's
  key. Remove that one key by hand, and only once no compaction, migration or
  erasure of that bucket is running. A claim written by a newer release in a
  format this release cannot read looks the same, so rolling back across such
  a release leaves every bucket the newer release claimed held this way.
- `vanished_twice`: the claim key disappeared between the create and the read
  that followed it, twice in a row. Nothing is held, so the printed expiry is
  `0` and the retry point is immediate.

`--dry-run` takes no claims. `--no-claim` takes none either, for repair work
when a claim is in the way. Between two compactions that is safe, because the
compaction record's create-if-absent still decides which output is published,
though the merge may duplicate one another maintainer is running. Against an
erasure of the same bucket the only check left is the re-list each run makes
just before it publishes, which leaves a short window, so avoid `--no-claim`
while an erasure request for the tenant is pending. `maintain migrate` takes
the same claims, prints the same `claims:` line, and accepts the same
`--no-claim`. Its `--dry-run` also takes no claims, but it is not compaction's
plan report: it skips the walk and runs only the read-only re-audit (see
[Re-encoding compaction parts](#re-encoding-compaction-parts)).

### Compaction claim metrics

Between two compactions a claim only saves work: they still converge on one
compaction record through its `CreateIfAbsent` and content-addressed parts, so
there a claim bug can only waste work. Between a compaction and an erasure of
the same bucket the claim is also what keeps them apart: their records have
different keys, so the create-if-absent does not, and a compaction that
published after the erasure would serve the erased data again. The claim and
the re-list before each publish are what stop that, so a claim bug there can
delay an erasure, and with the re-list's short window it is no longer only a
matter of wasted work. Both the background supervisor's per-unit tick and
`ravel-cli maintain compact-bucket` / `compact-tenant` / `migrate` take claims; `--no-claim` on the CLI and
`--maintain-claims off` on the server (below) are the two ways to opt a run
out.

The server's `/metrics` endpoint renders five claim counters, one family per
signal. They count this server's own maintenance supervisor only: a
`ravel-cli maintain compact-*` run is a separate process, reports its outcomes
in its walk summary, and never moves these counters.

- `ravel_maintain_claims_acquired_total` -- claims taken: fresh, taken over
  from an expired claim, or taken back from this process's own leftover
  claim.
- `ravel_maintain_claims_stolen_total` -- the subset of the above taken over
  from an expired claim. A crash or a restart leaves an expired claim behind
  too, and so does a failed run whose process stopped maintaining that
  bucket, so steals on their own are expected after any of those; a process
  that is still running takes its own leftover claim back as an acquisition
  instead.
- `ravel_maintain_claims_lost_total` -- claims this process held and lost
  before publishing: another process took the claim over after its lease
  expired, or the claim object was deleted, and this run cancelled at its
  next checkpoint. A lifecycle rule or a manual delete on the claim prefix
  also moves it, so rule that out before raising the lease.
- `ravel_maintain_claim_renew_failures_total` -- renewals that failed with a
  store error, distinct from a lost claim. The run stops with an error and
  leaves its claim in place. The same process takes it back on its next pass
  and compacts the bucket; any other process waits for the claim to expire.
- `ravel_maintain_claims_skipped_total` -- bucket evaluations that did not
  compact, or erasure rewrites that published nothing, because they could not
  take the bucket's claim: most often an unexpired claim held the bucket,
  but also a lost steal race, a claim that could not be read, or one that
  vanished twice (something outside the protocol is deleting claims). A held
  bucket adds one per maintenance pass until the claim expires. The pass
  that observed the claim logs a skip line with the reason; the later passes
  of the same hold count without logging.

The acquired, stolen, lost and skipped counts are gathered per shard pass and
added when the pass completes; a pass that ends in an error drops what it had
counted, so read them as lower bounds. A renewal store error is counted where
it surfaces, so `ravel_maintain_claim_renew_failures_total` is not affected.

Alert on lost claims, not on steals:
`rate(ravel_maintain_claims_lost_total[15m]) > 0` held for 30 minutes (`for:
30m`). A lost claim means a run was still working when its lease expired and
another process took the bucket over, so the lease is shorter than that
deployment's merges: raise `--maintain-claim-lease`. A steal is the other
side of that event, but it also follows every crash and restart, so a steal
rate alone over-alerts.

Two flags configure claiming, and a third no longer has any effect:

- `--maintain-claim-lease <DURATION>` (default `300s`): how long a claim stays
  live without a renewal. A lease below twice the time to encode and PUT the
  largest L1 segment at a conservative rate logs a startup warning, not a
  refusal; zero is refused outright.
- `--maintain-claim-min-input-bytes <BYTES>` (default 64 MiB): has no effect
  and will be removed. With claims on, every bucket is claimed whatever its
  size, because the claim also keeps a compaction and an erasure rewrite of
  the same bucket from publishing over each other. The flag is still accepted
  so existing deployments start unchanged; zero is still refused.
- `--maintain-claims on|off` (default `on`): the fleet-wide escape hatch.
  `off` disables claiming everywhere on that process, for a store whose
  qualification record predates the CAS probes, or for an emergency. Two
  racing compactions still converge at the compaction record and the loser
  just pays its merge first, but with claims off a compaction and an erasure
  rewrite of the same bucket are kept apart only by the re-list each runs
  just before it publishes, which leaves a short window.

## Garbage collection and retention

Ravel deletes data through two independent triggers, both driven by the
maintenance loop or by the matching one-shot command. Objects are immutable
throughout: deletion removes whole objects and nothing is ever modified in
place.

**Age-based retention.** If a sealed bucket's newest event is older than the
tenant's retention window, Ravel writes a durable retention tombstone for it,
which immediately excludes the whole bucket from new query snapshots. Retention
is off by default; see
[configuring it](configuration.md#age-based-retention) for the flags and the
floor its window is validated against. Retention runs before compaction, so an
expired bucket is tombstoned rather than compacted first.

**The sweeper** is the only component that issues a delete. All three of its
rules re-verify their precondition against a fresh listing immediately before
each delete, and every delete is idempotent:

1. **Orphan collection**: an L0 data object with no commit record, older than
   `grace + max_flush_lifetime`. The writer interlock guarantees such an object
   can never gain a commit record later, so deleting it cannot orphan a future
   reader.
2. **Superseded-input sweep**: the L0 commit records and data objects that a
   compaction record names, once `now >= record.created_unix_ns +
   protection_horizon`. Records are deleted before data objects, so a crash
   mid-sweep never leaves a commit record pointing at a deleted object.
3. **Unreferenced L1 cleanup**: an L1 object that no compaction record in its
   bucket references, once a compaction record exists for that bucket and the
   object is older than `grace + max_compaction_lifetime`.

Retention's own physical sweep deletes everything in a tombstoned bucket (L0
records, compaction records, L0 data, L1 segments, and the tombstone last) once
`now >= retired_at_ns + protection_horizon`, and only after a verifying listing
shows the bucket empty but for its tombstone.

Before that first delete the sweep reads the trailer of every data object in
the bucket, one 16-byte ranged GET per object, and checks its format version
against the running build's reader window for the bucket's own format: metric
segments for metrics, log segments for logs, span segments for spans. If any
object carries a version this build does not read, the sweep deletes nothing in
that bucket, leaves the tombstone in place, logs a warning naming the versions,
and counts the objects on `held_out_of_window_objects_total` (an in-process
counter, not yet on the scrape endpoint; see below). Such an object is usually
not corrupt: the other side of a rolling upgrade, or the build a rollback
returns to, reads it. An object at a retired version older than this build's
window is held the same way, and neither finishing a rollout nor rolling back
clears that hold, because no current build reads it. This is what makes a binary rollback across a format bump
lose queryability rather than data, for all three signals: the rolled-back
build cannot query the newer objects, but it does not delete them, and the
bucket is retired normally once a build that reads them runs again. A trailer
with a bad magic, signal, reserved field or footer length is corruption and is
swept as usual. The check reads the trailer only, not the footer checksum, so
damage confined to the version field reads as an unreadable version and holds
the bucket rather than sweeping it.

The alerts shard is swept by the same three rules on every tick, for each
tenant whose alert unit the process owns, whatever `--alert-retention` is: the
alert evaluator can abandon a write under any window, and this sweep is what
reclaims it. A full sweep of that shard costs six listings even when there is
nothing in it, so a tenant with nothing to sweep pays two bounded listings
instead: one of its whole alert keyspace (`t/<tenant_hash>/a/`, which holds
every commit record, L0 data object and L1 segment the rules look at) and one
of the quarantine copies taken from it. When both come back empty the sweep is
skipped for that tick; when either listing fails, the sweep runs, since a failed
listing has not shown the keyspace empty. A tenant that runs alert rules has an
alert state memo under that keyspace, so with a nonzero `--alert-retention` the
memo read already shows the keyspace is in use and the sweep runs without the
extra listings. Under `--alert-retention 0` no memo is read, so such a tenant
pays the keyspace listing and then the sweep's six, seven listings a tick where
it paid six before. With
the default 90-day window a tenant with no alert rules pays one memo GET and
three listings per tick (the retention sweep's own check of the alert commit
prefix, then the two above); with `--alert-retention 0`, two listings. For 1000
such tenants on a 5-minute tick that is about 3000 LIST requests per tick on the
default window, and 2000 under `0`, where running the sweep would cost about
7000 and 6000.

### The two timing values

- `grace`, default 24h, is the floor for the orphan and unreferenced-L1 age
  gates.
- `protection_horizon`, default 25 h 5 min (`max_query_duration` 1 h plus
  `grace` 24 h plus the 5 min clock-skew allowance), is the gap between a
  deletion anchor (a
  compaction record's creation time, or a tombstone's retirement time) and
  physical deletion. A query resolved just before the anchor then still has time
  to read the inputs it pinned.

Both are `ravel-server` flags, `--gc-grace` and `--gc-protection-horizon`, and
they feed the real compactor rather than only a startup check. **In maintain
mode each one must equal the value stored in the durable `sys/gc` object or the
process refuses to start**, and the query deadline must be less than or equal to
the stored maximum query duration. That makes changing a horizon a deliberate
two-step operation rather than a rolling configuration change. See
[the configuration page](configuration.md#retention-and-garbage-collection-configuration)
for the order to change them in.

`ravel-cli maintain sweep` reads the same `sys/gc` object. It sweeps on the
stored protection horizon, grace and maximum flush lifetime, and refuses before it sweeps
when the stored horizon does not cover its own 5 min clock-skew allowance, or
is below its 1 h maximum compaction lifetime plus four times that allowance
(1 h 20 min), the checks the server's maintain mode runs at startup. On a bucket with no `sys/gc`
it bootstraps the object from the maintain defaults, as the server does; a
`--dry-run` uses those defaults without writing the object. The stored maximum
query duration and HEAD cache TTL set its pinned-query window, as they do the
server's.

`ravel-cli gc-config set` refuses, and writes nothing, a protection horizon
below either of two bounds: `max_query_duration + grace +
clock_skew_allowance`, and `max_compaction_lifetime + 4 *
clock_skew_allowance`. The second is checked against this build's
compiled 1 h maximum compaction lifetime, and both against the 5 min default
skew allowance unless `--clock-skew-allowance` is given. It only binds a
deployment that shortens `max_query_duration` and `grace` far below their
defaults: a compaction or rewrite run that could still publish over a record's
inputs must not outlive their horizon. The maintain process and `maintain
sweep` re-check both at startup against their own values.

### The pinned-query window

The retention and superseded-input sweeps do not delete an object the moment
the live HEAD stops naming it. The first pass that finds it unnamed writes an
unnamed-since marker under `t/<tenant_hash>/<signal>/maint/unn/` and holds it;
a later pass deletes it once `max_query_duration + head_cache_ttl + 4 *
clock_skew_allowance` (1 h 20 min 30 s with defaults) has passed on the
sweeper's clock. Expect every expired bucket and superseded object
to report `pinned_window` for at least one pass. Retention re-evaluates a
tombstoned bucket on every maintain tick, so its physical deletion lands about
one window later than before. The superseded-input sweep reaches an interior
hour only on the full sweep (`maintain_interior_reverify`, default 6 h): one
full sweep writes the marker and a later one deletes once it has aged, so an
interior superseded input goes up to two full-sweep intervals after its
horizon, one more than before. An erasure request's `.dreq`, which carries the
subject identifier, is kept until every chain it holds has a marker older than
the window, so it too lives that much longer. A one-shot `ravel-cli maintain sweep` writes the
marker and holds on its first run; a second run once the window has passed
deletes. `--dry-run` writes no marker.

The maintain role needs delete and list on `t/*/*/maint/*`
(deploy/iam/maintain.json): a refused marker delete keeps a retention
bucket's tombstone, and blocks a candidate whose marker must be replaced, on
every pass.

The guarantee is complete only once every maintain process runs a build with
the gate and `sys/gc` has been moved to format version 2 (below). Until then an
older maintain process can still delete by the old rule, with no window, beside
a newer one.

### Upgrading `sys/gc` to format version 2

`sys/gc` format version 2 records the HEAD cache TTL every query-mode server
process is held to. A fresh bucket is still bootstrapped at version 1, which
records none, and against version 1 a query-mode server process is held to the
compiled default of 30 s. A build that reads only version 1 refuses a version 2
object, and nothing writes version 1 back, so the flip is a one-way ratchet. In
order:

1. Upgrade every `ravel-server` process in every mode (`gateway`, `query`,
   `maintain`, and the combined `all` mode; each reads `sys/gc` at startup
   and refuses to start on a format version it does not know) and every
   `ravel-cli` binary that reads `sys/gc`
   (`gc-config show` and `set`, `parquet sweep`, `maintain sweep`) to a build
   that reads version 2. `ravel-cli gc-config show` prints the stored
   `format_version`.
2. Run `ravel-cli gc-config set` with the current values and
   `--head-cache-ttl <duration>`, for example `--head-cache-ttl 30s`. This
   writes version 2. The value must be at least 30 s, this build's own query
   HEAD cache TTL, which no server flag lowers; `set` refuses a smaller one,
   since every query process would then refuse to start.

From then on an older build refuses to start against the bucket. A query
process whose catalog runs on a HEAD cache TTL above the recorded one refuses to
start too. A later `gc-config set` without `--head-cache-ttl` keeps version 2
and the recorded TTL.

## The at-rest integrity scrubber

The checksum hierarchy, a whole-object hash at write time and per-section
checksums on read, is otherwise verified only when a query happens to touch the
covered bytes. Bytes nobody queries are never checked. The scrubber re-verifies
them on a schedule instead. It runs only in `--mode maintain`, spawned per
process alongside the maintenance loop.

Each tick it re-discovers tenants from storage and, for every unit, verifies a
bounded slice of that shard's committed data objects, the L0 segments plus the
compaction and rewrite output parts the catalog still serves (not a superseded
generation's, not an overlap loser's, and none in a tombstoned bucket): a
section checksum re-check plus a whole-object rehash against the recorded
content hash. The [observability guide](../observability.md) records the
lineage filter and the `level` label a mismatch is counted under. A
persisted per-shard cursor in object storage holds a start-after marker: each
tick lists the shard's commit records strictly after it and stops once the tick's
budget is filled, so apart from the count that opens a rotation, a tick's LIST
and GET count follows its budget, not the corpus size. When the listing runs out
past the marker, the rotation rolls over and the next tick starts again from the
head, so every object is visited once per rotation.

The budget is recomputed every tick (hourly, or every `P` when `P` is shorter)
from what the rotation has observed:

- Every rotation opens with one LIST-only pass that counts the shard's listing
  entries. This pass lists the whole commit prefix, once per rotation.
- Every later tick recounts the rotation's tail window, the last two ingest
  hours the previous count met plus anything after them, with another
  LIST-only pass, and adds the window's growth to the rotation's estimate. A
  shard that keeps committing raises its own budget; a commit from any writer
  in those hours is counted, and one that lands behind the marker is counted
  too although it waits for the next rotation, so the estimate errs high.
- The rotation is allotted `min(P, retention / 2)` (see below). A tick may
  consume the entries still to cover divided by the ticks left before that
  deadline, never fewer than the sustained rate `ceil(estimate * tick /
  window)` and never more than four times it, and it may issue eight store
  requests per allowed entry. Every listing page the walk draws (an hour
  re-list included), every record GET attempt whether it succeeded or not, and
  four requests per object verified count against the request cap; the
  LIST-only count passes do not. A tick always attempts at least one unit, even
  one that alone exceeds the budget.
- When the entries a tick would need exceed four times the sustained rate, the
  rotation cannot finish inside its window: the tick logs both numbers with
  the window and `--scrub-period`, and increments `ravel_scrub_behind_total`.

A GET that fails with a retryable error (throttled, timeout, transient), of a
commit record or of an object it names, stops the tick with the marker behind
that unit, and the next tick retries all of it. The hold is capped: after six
consecutive held ticks on one unit (six hours of tick cadence at the
default one-hour tick, less when a short `--scrub-period` shrinks the tick),
the next tick moves past the unit and counts each of its records and objects
that still fails on `ravel_scrub_unreadable_total{reason="retry_exhausted"}`.
`ravel_scrub_marker_held_ticks` reads the current count for the signal's worst
shard. Any other GET error except
not-found, and a record whose bytes do not decode, is counted once on
`ravel_scrub_unreadable_total` (at the level of the object or record that
failed, `reason="access_denied"` or `reason="permanent"`) and the marker moves
past it, so one unreadable record cannot pin the rotation. These are not
counted on `ravel_scrub_checksum_mismatch_total`, which counts only bytes that
were read and did not match. A record or object deleted after it was listed is
logged and skipped.

It detects and never repairs. An anomaly is reported; there is no redundant copy
to repair a corrupt segment from.

### Sizing `--scrub-period`

The period `P` is the operator-facing budget knob. Because the content tier must
read each object in full to rehash it:

```
sustained scrub read bandwidth = corpus_bytes / P
```

A larger corpus or a shorter `P` costs proportionally more read bandwidth, and
`P` is also the worst-case staleness before any given object is re-verified.
The default is `7d`. A zero or unparseable duration fails startup rather than
rotating in a tight loop.

A tenant with a retention window gets a shorter rotation when half that window
is shorter than `P`: the rotation is allotted `min(P, retention / 2)`. The walk
goes oldest hour first, so a rotation as long as the retention window would
reach each object at about the age retention deletes it; half the window
reaches every object by about half its retained life, while the rotation keeps
pace. A 7-day
retention with the default `P` therefore rotates every 3.5 days, at twice the
read bandwidth the formula above gives for `P`. A retention window shorter than
a tick (more precisely, one whose half fits in a single tick) gives every tick
the budget of a whole rotation, so every tick normally walks a full rotation,
including the LIST-only count of the whole commit prefix that opens it, and
that count's cost is not charged against the tick's budget.

This is the one scheduled task whose cost scales with data volume rather than
metadata volume, so size `P` against the corpus you actually have, and watch
`ravel_scrub_cursor_position` to confirm rotations keep pace. That gauge, the
held-ticks gauge, and the five scrubber counters are catalogued in
[the observability guide](../observability.md); the alarms that matter are in
[troubleshooting](troubleshooting.md).

### Cursor compatibility across builds

A cursor written by an earlier release (0.18.0 or before) loads with defaults
for every field it lacks, and its progress restarts: the next tick opens a
fresh rotation from the head of the shard. During a mixed-version rolling upgrade, each version's cursor
write drops the fields only the other version knows, so the rotation restarts
each time a shard's ownership flips between versions. Nothing is corrupted, the
cursor stays in object storage throughout, and once every maintain process runs
one version the rotation proceeds normally.

### It needs no policy change

The scrubber's reads, commit records and data objects, are already covered by
the Maintain role's existing read and list grants. Its one write, the per-shard
cursor, is placed under the existing `maint/` control prefix, which the Maintain
role's write grant already names. Enabling it requires no storage-policy change.

## Format migration

```sh
ravel-cli maintain migrate --tenant <t> --signal <metrics|logs|spans> \
  [--shards <n>] [--target-version <n>] [--family <name>] [--budget-records <n>] \
  [--no-claim] [--dry-run] [--reencode-compaction-parts]
```

`migrate` raises a `(tenant, signal, format family)`'s recorded format floor to a
target on-object format version. One invocation:

1. walks buckets in shard and ingest-hour order from a durable cursor, rewriting
   every sealed, un-tombstoned bucket that still has an L0 commit record below
   the target version and served raw. A bucket that already carries a compaction
   or rewrite record is visited too, because a record can leave an input served
   raw. This reuses the compaction rewrite
   primitive, so the rewrite is bucket-atomic and produces a compaction record
   exactly as compaction does. A bucket that serves nothing below the target
   raw, carries no rewrite record, and whose compaction records hold parts
   below the target is re-encoded when `--reencode-compaction-parts` is given
   (see [re-encoding compaction parts](#re-encoding-compaction-parts)) and
   reported as `reencode_blocked` otherwise;
2. stops early and persists the cursor once `--budget-records` is spent (`0`,
   the default, is unlimited; re-run to resume), or, once the walk drains,
   re-audits fresh and raises the floor only if that re-audit finds zero records
   below the target. The budget counts records, not requests: the walk reads
   the compaction and rewrite records of every sealed, un-tombstoned bucket it
   passes, and a `reencode_blocked` bucket spends none of the budget, so the
   budget does not bound how many requests one invocation makes.

A refused raise, reported as "FOUND STRAGGLERS", means the fresh re-audit found
objects that still exist below the target, some of which queries still read. It
reports three counts, because they are blocked for different reasons:
`l0_commit_records`, `l1_compaction_parts`, and `rewrite_record_parts`.
`l0_commit_records` is the one that is entirely live; the two part figures count
the parts of every record a bucket still LISTS, which includes records the
resolver no longer serves and a `sweep` deletes (see `rewrite_parts` below).

`l0_commit_records` moves on a re-run for the part of it that was merely not
yet sealed when the walk passed, or that landed after it, and for any bucket
listed as `not_migrated` (see below). Of the other two:

- a **below-target compaction part** is re-encoded only by a run with
  `--reencode-compaction-parts`, and only in a bucket whose one compaction
  record survives supersession (see
  [re-encoding compaction parts](#re-encoding-compaction-parts)). Without the
  flag, re-running `migrate` reports the same `l1_compaction_parts` figure and
  names the bucket on a `reencode_blocked` line. The exception is a
  `--target-version` above the version the running build writes: a part
  recorded at the version this build writes still counts in the figure, but a
  bucket whose below-target parts are all at that version gets no
  `reencode_blocked` line, because a re-encode by this build cannot carry them
  further. Such a part needs a newer build, not the flag. No `blocked_bucket` line is
  printed for one, except when the bucket's authoritative compaction records
  are all at the target and the below-target parts belong to records that lost
  their overlap (see `losing_record_parts` below). The figure is over listed
  records, so it can also fall without a `migrate` run: a compaction record
  that a later rewrite record or a re-encode superseded stays listed until a
  `sweep` deletes it and its parts, the same way a superseded predecessor
  rewrite does;
- a **below-target rewrite part** is never migrated by design (see
  `rewrite_parts` below);
- a **below-target L0 input only a losing compaction record names** is served
  raw and the walk cannot migrate it (see `loser_only_inputs` below).

`migrate` names each bucket in the last two categories on its own line, and
each bucket held below the target only by a losing compaction record's parts.
A bucket that qualifies both as `loser_only_inputs` and as
`losing_record_parts` gets the `losing_record_parts` line only; the two clear
the same way:

```
buckets_blocked: 3
blocked_bucket: shard=0 hour=100 reason=rewrite_parts below_target=2
blocked_bucket: shard=3 hour=47 reason=loser_only_inputs
blocked_bucket: shard=3 hour=52 reason=losing_record_parts below_target=1
# Re-running migrate does not clear any blocked bucket above; it reports the same list again. ...
# A loser_only_inputs bucket clears only when retention ages those inputs out: ...
# A losing_record_parts bucket's authoritative compaction records are at the target, ...
```

Every other line of the report is `key: value`; the explanatory prose is
prefixed with `# ` so a parser reading those lines does not take a sentence
fragment for a key.

`buckets_blocked` is how many `blocked_bucket` lines there are, and it covers
the buckets THIS INVOCATION EXAMINED. The `rewrite_parts` and
`losing_record_parts` lines come from the re-audit, which reads every shard, so
they are complete; the loser-only half comes from the walk, so an invocation
that resumed from a cursor does not re-report the loser-only buckets an earlier
invocation found. When `buckets_blocked` is non-zero, re-running is not the
remedy. A `loser_only_inputs` or `losing_record_parts` block is not something a
command clears; a `rewrite_parts` block can be, when it lists a superseded
predecessor a `sweep` removes (see below).

**`loser_only_inputs`.** An L0 input that only a losing compaction record names
is served raw and the walk cannot migrate it: a new record over that subset
joins the same overlap component and loses to the existing winner. Retention
aging those inputs out is the only thing that clears it in this build. A later
authoritative compaction covering them would, but none is ever published:
compaction refuses a bucket that already carries a compaction record, so the
bucket's record set is closed.

**`losing_record_parts`.** The bucket's authoritative compaction records all
have their parts at or above the target, but compaction records that lost
their overlap to them carry `below_target` output parts below it, summed over
those losing records. Where `loser_only_inputs` is about the raw L0 inputs only
a losing record names, this is about the losing record's own output parts.
Nothing serves them, but they still count in `l1_compaction_parts`, and the
floor is still refused over them: a build older than the rule that picks one
authoritative record per overlap may still serve them. Re-running `migrate`
does not clear it, and neither does `sweep`: neither reclaims a losing record's
parts. Retention does, when it ages the bucket out, subject to the
format-version hold that keeps an object this build cannot read. That is not a
command you run. A bucket whose authoritative records are themselves below the
target is not named this way. Nor is a bucket whose rewrite record parts are
below the target: it gets its `rewrite_parts` line only. A bucket that lists a
rewrite record whose parts are all at the target is named this way when its
losing records carry below-target parts. A compaction record a live rewrite
record supersedes counts on neither side of this test: it is never a losing
record here, even when it lost its overlap, and a below-target one that won its
overlap does not stop the bucket's losers being named: `sweep` deletes it and
its parts together with that rewrite, and
until then its below-target parts count in `l1_compaction_parts` without a
`blocked_bucket` line. A `sweep` can therefore change the entry of a bucket
that lists a rewrite record. Once it deletes a winning record the rewrite
superseded, a losing record that overlapped only that winner becomes
authoritative, and a bucket whose authoritative records are below the target
is not named this way, so its `losing_record_parts` line goes away while those
parts still count in `l1_compaction_parts`.

**`rewrite_parts`.** The bucket holds a live selective-erasure rewrite record
(the durable steady state of a bucket an erasure request touched), and
`below_target` of the surviving parts were written at the erasure-time format
version, below the target. `migrate` never rewrites them. Publishing a migration
output over inputs a rewrite already covers puts two record sets on one bucket,
and a snapshot including both resurrects the records that rewrite deliberately
dropped; an output that re-applies the same drops is an erasure rewrite with all
of selective erasure's request-binding obligations, which this job does not
have. So the parts are counted, so the floor is never raised over them, and the
bucket is named.

`below_target` counts the parts of every rewrite record the bucket still LISTS,
not only its live one. A superseding rewrite does not delete the record it
supersedes; `sweep` does, on its own schedule, so a superseded predecessor's
parts keep counting until then. That is what makes the second clearing path
below a two-step one.

It clears one of two ways. Retention ages the bucket out, subject to the
format-version hold that keeps an object this build cannot read; or the
below-target parts stop being listed. The erasure request that produces a
superseding rewrite at the current output version is not something you trigger:
the erasure driver does it on its own schedule. The `sweep` that removes a
superseded predecessor, though, is a command you run: `ravel-cli maintain sweep`
for that tenant, signal and shard deletes a superseded predecessor rewrite record
once the superseding rewrite is past the protection horizon (the horizon is
anchored on the superseding record's own `created_unix_ns`, not the
predecessor's). So when a bucket is blocked only by a
superseded predecessor whose successor is already at the current output version, a
`sweep` alone clears it. When the live rewrite is itself still below target, a
superseding rewrite has to land first AND then be swept, so that path stays a
two-step one. Until the below-target parts are gone the family's floor stays where
it is, which is the correct outcome rather than a fault: raising it would claim a
format floor over objects that still exist below the target, some of which queries
still read (a superseded predecessor is not one of them, but the current output
version's own below-target parts are).

Blocked buckets are narrowed to those cases deliberately. The rewrite
primitive also refuses a bucket when a concurrent compaction or erasure lands
between the walk's listing and its own, which is harmless and converges on a
later run. Because the refusal alone cannot tell the two apart, the walk
re-reads the bucket after a refusal and reports it only when a below-target
record is still served raw.

The re-audit's liveness definition excludes a bucket's pre-rewrite L0 commit
records once an authoritative compaction or rewrite record supersedes them.
Those records are dead, sweepable leftovers of a rewrite this same invocation may have
just performed, not stragglers. Because of that, a clean migration converges and
raises the floor in one invocation, and running `sweep` in between is never
required for it to converge. The sweeper's superseded-input rule still deletes
those records on its own schedule, which is storage reclamation, not a
correctness precondition.

### Re-encoding compaction parts

A bucket whose one compaction record holds parts below the target is converged
by re-encoding: `migrate --reencode-compaction-parts` reads those parts, writes
them again at the current version, and publishes a version 2 compaction record
that supersedes the old one. The flag is off by default, and turning it on is a
rollout decision, not a tuning knob:

- every reader and maintainer in the fleet must already run a build that reads
  version 2 compaction records. A build that cannot read one fails every
  resolve of that bucket's records, for queries and maintenance alike;
- the release before the one you are running must read version 2 compaction
  records too, so a one-release rollback stays safe. Once a version 2 record is
  written there is no rollback past a build that reads them. The record is
  immutable, and leaving the flag off afterwards only stops new ones;
- the superseded record and its parts stay listed, and keep counting in
  `l1_compaction_parts`, until `sweep` deletes them. That takes two sweep
  passes: the first pass that finds the superseded record past the protection
  horizon and unnamed by HEAD only writes its unnamed-since marker, and a pass
  at least the pinned-query window later (1 h 20 min 30 s at the defaults; see
  [The pinned-query window](#the-pinned-query-window)) deletes it. The run that
  re-encodes a bucket therefore still reports "FOUND STRAGGLERS" for it, and the
  format floor rises on the first `migrate` run after that second pass.

Re-encoding takes the bucket's claim exactly as a migration does, and
`--no-claim` takes none.

The report counts the buckets the walk did not re-encode and the buckets whose
rewrite published nothing, each with one line per bucket and a `# ` line saying
what clears them:

```
records_migrated: 0
buckets_reencode_blocked: 2
reencode_blocked: shard=0 hour=100 reason=writer_disabled below_target=1
reencode_blocked: shard=2 hour=9 reason=contested_overlap largest_component=2
# A writer_disabled bucket's one compaction record holds below_target parts under the target. ...
# A contested_overlap or multiple_records bucket is not re-encoded by any run, ...
buckets_not_migrated: 1
not_migrated: shard=1 hour=100 path=reencode reason=claim_skipped claim_reason=held_by_another
# Each not_migrated bucket published nothing this run. This run drained the walk and cleared its cursor, ...
```

A `reencode_blocked` line has one of three reasons:

- `writer_disabled below_target=<n>`: the bucket can be re-encoded, and `<n>`
  of its record's parts are below the target, but the run did not have
  `--reencode-compaction-parts`;
- `contested_overlap largest_component=<n>`: an overlap component of the bucket
  holds `<n>` compaction records, so it is not re-encoded;
- `multiple_records records=<n>`: `<n>` compaction records survive
  supersession, each alone in its overlap, and a re-encode rewrites a bucket's
  one record only.

The last two are not cleared by any run; retention clears them when it ages
the bucket out, subject to the format-version hold.

A `not_migrated` line names the path the bucket was dispatched to
(`l0_migration` or `reencode`) and why its rewrite published nothing:

- `claim_skipped claim_reason=<reason>`: the run could not take the bucket's
  claim, with the same reasons `compact-bucket` prints (`held_by_another`,
  `steal_lost`, `unreadable_claim`, `vanished_twice`);
- `cancelled checkpoint=<point>`: the run took the claim, lost it, and stopped
  at `<point>` (`input_set`, `merge_loop`, `part_boundary` or `publish`) before
  publishing;
- `record_set_changed`: the bucket's records changed before the publish;
- `publish_abandoned`: the run passed its compaction deadline before the
  publish.

When a `not_migrated` bucket is retried depends on how the run ended, and the
`# ` line under the list says which case applies:

- the walk drained and the re-audit found stragglers: the run cleared its
  cursor, so the next `migrate` run starts over and retries every one;
- the run stopped on its budget: its cursor is saved past every bucket it
  examined, so the next run resumes after them, and a `not_migrated` bucket is
  retried by the first run after the walk drains, which starts over from the
  beginning. Holding the cursor back instead would let a bucket another process
  holds stop the walk from reaching the buckets after it;
- the walk drained and the re-audit raised the floor: another writer carried
  the bucket to the target after the walk passed it, and nothing is left to
  retry.

The exit code follows how the run ended. A run that stops on its budget exits
zero, whatever `blocked_bucket`, `reencode_blocked` and `not_migrated` lines it
printed, so a loop that re-runs `migrate` until the walk drains is not stopped
by a bucket a later run retries or that only retention clears. The run that
drains the walk exits nonzero while any bucket is left below the target: every
`blocked_bucket`, `reencode_blocked` and unresolved `not_migrated` bucket still
holds parts or records below the target, and the fresh re-audit counts them as
stragglers. It exits zero when it raises the floor, even with `not_migrated`
lines printed.

`--dry-run` does not run the walk. It runs the read-only re-audit, prints the
three below-target figures and any `blocked_bucket` lines, takes no claim,
writes nothing (no part, record, cursor or floor), and exits zero:

```
claims: off (--dry-run)
dry_run: true
reencode_compaction_parts: true
...
l0_commit_records: 0
l1_compaction_parts: 1
rewrite_record_parts: 0
buckets_blocked: 0
# Dry run: the walk did not run and nothing was written. ...
```

### Auditing what versions are live

```sh
ravel-cli maintain audit-versions --tenant <t> [--shards <n>]
```

This audits live on-object format versions across all three signals, reading the
supported window from each reader's own source so a future version bump cannot
make the audit stale. It exits nonzero on any anomaly.

Beside each signal's histogram it prints every recorded format floor with its
basis and a classification against the records it just read: `current`,
`stale` (records newer than the basis, or a wider shard range), `contradicted`
(a live record below the floor), or `unknown` (the floor records no basis,
which is true of every floor raised so far). It also exits nonzero when any
floor is `contradicted`.

Each format supports exactly one version and carries no reader for the
previous one, so any live object at another version is an anomaly to re-ingest,
not a migration target:

| Format | Supported version | Anomalies |
|---|---|---|
| Metric segments | v7 | Every other version, including v6. |
| Log segments | v4 | v1, v2 and v3. |
| Span segments | v4 | Every other version. |

### Rolling a format bump: readers before writers

Read this section as the procedure that applies from the v1.0 release onward.
Ravel has not reached v1.0, and before it the reader window holds exactly one
version: a bump deletes the version-N reader in the same change that introduces
N+1, so backward compatibility may break outright, no fleet ever reads both
versions, and objects left at version N become unreadable rather than
convergeable. On a pre-v1.0 build the whole sequence below collapses into one
step, deploy the new build everywhere and re-ingest or discard whatever was
written at version N, and a bump is a forward-only, non-rollbackable
data-migration event. Steps 1 to 4 become live at v1.0, when the window widens
to N/N-1.

When a release bumps a bulk data-object format from version N to N+1, roll the
fleet in this order and never the reverse:

1. Deploy the release that **reads** N+1 to every process that opens objects,
   which is query, maintenance and the catalog fold, and confirm it is live
   fleet-wide. A process that writes N+1 before its peers can read it produces
   objects the rest of the fleet fails closed on, with a typed unsupported
   version error rather than a silent misread. Writers must never lead.
2. Only then enable writing N+1, so compaction and flush emit the new version.
   From this point new and rewritten objects are N+1, and existing N objects
   stay readable for as long as the reader window covers them.
3. Converge the existing N objects. Retention ages them out for free, and
   `migrate` rewrites the rest and raises each format floor once a fresh
   re-audit confirms nothing below N+1 survives. Watch `audit-versions` for the
   remaining below-target population.
4. Delete the reader for the retired version N only once every bucket's recorded
   floor is at or above N+1, which is a checkable fact from the floors `migrate`
   raised, and do it in its own later reviewed change.

## Legal hold

```sh
ravel-cli hold set --tenant <id> --scope <prefix> [--reason <text>]
ravel-cli hold clear --tenant <id> --scope <prefix>
ravel-cli hold list --tenant <id>
```

These write and read the audit records that both maintenance drivers check
before any destructive pass.

One shard's objects live under three sibling prefixes, `.../l0/<shard>/`,
`.../c/<shard>/` and `.../l1/<shard>/`, and each is checked independently, so a
hold naming only one of them covers part of a shard and not the rest. The
`--signal` and `--shard` form writes all three in a single command, and is the
way to hold one shard. `hold set --scope` refuses a scope that reaches into one
or two of the three without covering all of them, and names the sugar in the
refusal. A broader scope is still accepted, because it cannot be partial: a
whole tenant (`t/<tenant_hex>/`) or a whole signal (`t/<tenant_hex>/<signal>/`)
covers all three prefixes of every shard it spans. So is a scope that reaches
none of them, such as one under `maint/`. `hold clear` accepts any scope,
partial ones included: the fold matches a clear to a set by the exact scope
string, so refusing a partial clear would leave a hold written before this rule
with no way to release it.

Under a hold the physical retention sweep is all-or-nothing. If any key it
would delete is held, including a commit record or the retention tombstone, it
deletes nothing that pass, leaves the tombstone in place, counts the bucket, and
reports `SweptPartial`. It does not delete around the hold: the commit records
name the data objects and the tombstone keeps the bucket excluded, so deleting
those while keeping the held bytes would leave bytes nothing can read and
nothing can later sweep.

The count is `ravel_maintain::retention::held_by_lease_buckets_total`, the
process-wide seam for
`ravel_maintain_retention_held_by_lease_buckets_total`, one per bucket per
declining pass. Like the version-hold counter beside it
(`held_out_of_window_objects_total`) it is a seam today and not yet on the
scrape endpoint. A held bucket is a bucket kept past its retention window,
so this rises for as long as the hold stands, which is expected; it goes flat
again once the hold is cleared and the next pass retires the bucket. A total
that keeps rising after every hold is cleared means some scope is still
matching, and `hold list` shows which.

**The hold is not effective the instant the command returns.** Each maintenance
tick refreshes its hold snapshot once, before its destructive pass, so a hold set
after that tick's refresh is not honored until the next one. The exposure window
is one `--maintain-interval-secs` interval, five minutes by default.

After placing an urgent hold, run `ravel-cli hold list --tenant <id>` and
confirm the scope is present before assuming the data is protected. `hold set`
returning success means only that the record was written, not that a maintenance
pass has picked it up.

Legal-hold records themselves are undeletable by every role, Maintain included.

## Reclaiming a pre-namespacing cache directory

A node's local read cache once wrote entry files at
`<cache-dir>/<shard>/<file>`. It now writes them under a per-instance namespace
subdirectory, `<cache-dir>/<namespace>/<shard>/<file>`, so two caches over one
directory cannot evict or miscount each other's files. A directory warmed by a
binary from before that change still holds files at the old rootless layout.
Those files are inert: nothing seeds, evicts, or counts them, and no live code
path reads or writes that layout. They occupy disk, up to the old cache budget, on top of the current
per-namespace budgets, and nothing deletes them implicitly.

`cache reclaim-legacy` is the deliberate operator step that reclaims them:

```sh
ravel-cli cache reclaim-legacy --cache-dir <dir>          # dry run: lists only
ravel-cli cache reclaim-legacy --cache-dir <dir> --apply  # deletes
```

It is a local filesystem tool: it takes no `--store` and never touches object
storage. **Dry run by default.** With no `--apply` it prints each legacy file it
would delete and their total bytes, and deletes nothing. `--apply` deletes the
matching entry files, regular files at their own canonical legacy path, and
then removes a legacy shard directory only when that leaves it empty. A
current namespace directory, a foreign file (which keeps its shard directory
in place), a symlink or directory carrying an entry-shaped name, and anything
outside `--cache-dir` are never touched.

**Safe to run while the node is live.** No live code path reads or writes the
legacy `<cache-dir>/<shard>` layout, so deleting it races no read, write, or
eviction on the running cache.

**Downgrade story.** A binary rolled back to the pre-namespacing layout reads
`<cache-dir>/<shard>` again and finds whatever reclaim left behind. After
`--apply` it finds nothing there and starts with an empty disk cache. That is a
cold start, not data loss: the next reads refetch from object storage, the same
as any fresh node. The local cache is disposable by construction.

## The maintenance and inspection commands

Every subcommand shares the same store flags as `ravel-server`. The full flag
list is in [the generated CLI reference](../../reference/ravel-cli-flags.md).

| Command | Does |
|---|---|
| `maintain compact-bucket` | One compaction pass over a single sealed bucket, printing the outcome. `--dry-run` computes the same plan and writes nothing. |
| `maintain compact-tenant` | Compacts every sealed bucket of one tenant and signal across shards. See [compaction](#compaction). |
| `maintain sweep --tenant <t> --signal <s> --shard <n> [--dry-run]` | One sweep pass (orphan collection, superseded inputs, unreferenced L1) over a shard. It prints the orphans quarantined, the orphan copies to quarantine that were refused, the quarantined objects reaped, the superseded records and data deleted, the superseded deletes refused, the superseded objects held because HEAD names them or cannot be read, the chain groups a legal hold kept, the unreferenced parts deleted, whether the pass covered the whole shard, and a line when the orphan breaker tripped or was overridden. It also prints the superseded objects held on the pinned-query window, the unnamed-since markers written, reset and retired, and what the orphan-marker reap did. It sweeps on the protection horizon, grace and maximum flush lifetime stored in `sys/gc`, holds a candidate HEAD no longer names until its unnamed-since marker is older than the stored maximum query duration plus HEAD cache TTL plus four times its clock-skew allowance, and refuses when the stored horizon does not cover its clock-skew allowance or is below its maximum compaction lifetime plus four times that allowance. `--dry-run` reports the eligible set and deletes nothing, and writes no marker. |
| `maintain status --tenant <t> --signal <s> --shard <n> --hour <n>` | Reports one bucket's state: sealed, tombstoned, compacted, L0 record count, superseded-input count, L1 segments present, unreferenced count. Read-only. |
| `maintain audit-versions --tenant <t> [--shards <n>]` | Audits live on-object format versions and classifies each format floor. Exits nonzero on any anomaly or contradicted floor. |
| `maintain migrate` | Raises a format floor. See [format migration](#format-migration). |
| `maintain verify-custody --tenant <t> [--shards <n>]` | Re-verifies the content-addressed chain at rest: every live data object's key-embedded hash against its actual content hash, and every surviving compaction record's referenced inputs. An input the sweeper already legitimately reclaimed past its protection horizon is reported separately, not as an anomaly. Read-only; exits nonzero on any anomaly. |
| `catalog list --tenant <t> [--hours <n>] [--shards <n>]` | Lists the commit records the catalog resolves for that tenant over the last N hours. `--shards` must match what the data was written with. |
| `catalog fold` | One-shot fold for one tenant and signal. See [catalog fold and verify](#catalog-fold-and-verify). |
| `catalog inspect --tenant <t> [--signal <s>]` | Decodes and prints that signal's HEAD and every referenced snapshot part: watermark, keys, hashes, entry counts. It names the signal both as a word and as the numeric value read off the object, so a HEAD stamped with a different signal than the one asked for is visible. It reports rather than errors when no HEAD exists yet. |
| `catalog verify` | Diffs the sealed record history against the snapshot. See [routine verification](#routine-verification). |
| `commit reconstruct` | Rebuilds record-less L0 data objects' commit records from their own footers. Stop maintenance first; see [troubleshooting](troubleshooting.md#commit-records-were-deleted-out-of-band). |
| `cache reclaim-legacy --cache-dir <dir> [--apply]` | Reclaims pre-namespacing local read-cache files. See [reclaiming a pre-namespacing cache directory](#reclaiming-a-pre-namespacing-cache-directory). Local filesystem only; dry run without `--apply`. |
| `segment inspect <path-or-key>` | Parses one metric segment: trailer, footer fields, section list, decoded series count. |
| `commit decode <key>` | Decodes one commit record: identity, referenced data object key, size and hash, sample and series counts, timestamps. |
| `commit decode-compaction <key>` | Decodes one compaction record: identity, input set hash, each input identity, and each output segment's summary. |
| `commit decode-tombstone <key>` | Decodes one retention tombstone: identity, retirement time, retention window, observed record count. |

`segment inspect` and `commit decode` accept either a local file path or an
object-store key. A path that exists on disk is read directly; otherwise it is
fetched from the configured store.

Every command that walks tenant data opens its report with the store it
resolved, and refuses a walk that reaches no data at all on a defaulted memory
store. See [choosing a credential source](configuration.md#choosing-a-credential-source).

## Background

Decision records behind this page:
[L0 to L1 compaction](../../adrs/0018-l0-l1-compaction.md),
[age-based retention](../../adrs/0019-age-based-retention.md),
[the metric index and catalog fold](../../adrs/0020-metric-index.md),
[maintenance safety and coverage](../../adrs/0048-maintenance-safety-and-coverage.md),
[leased distributed maintenance](../../adrs/0065-leased-distributed-maintenance.md),
[durability hardening](../../adrs/0059-durability-hardening.md),
[format migration machinery](../../adrs/0066-format-migration-machinery.md),
[bounded-memory log compaction merge](../../adrs/0979-bounded-memory-rlog-compaction-merge.md),
[advisory compaction claims](../../adrs/1029-advisory-compaction-claims.md),
and [alerts and audit signals](../../adrs/0040-alerts-and-audit-signals.md).
