# Maintenance (day 2)

Run at least one `--mode maintain` process. **Only a `--mode maintain` process
runs maintenance**: compaction, retention, the sweeper that issues the
deletes, and the at-rest integrity scrubber.

- A process in `--mode all` runs ingest, the query API, the catalog fold and
  alert evaluation. It runs none of the maintenance loops.
- A deployment made only of `all` processes never compacts an object and
  never deletes one. Its L0 segments accumulate, retention windows have no
  effect, and nothing reclaims storage. The quickstart is such a deployment.
- The catalog fold runs on a timer in `maintain` and in `all`, and in no other
  mode. A `gateway` or `query` process folds only when an operator calls
  `POST /api/v1/admin/fold`, which stays available in `all` and `query`. See
  [which processes fold](#which-processes-fold).

If nothing is reclaimed, first make sure that a `maintain` process runs.

- [Running the maintenance loop](#running-the-maintenance-loop)
- [Catalog fold and verify](#catalog-fold-and-verify)
- [Compaction](#compaction)
- [Garbage collection and retention](#garbage-collection-and-retention)
- [The at-rest integrity scrubber](#the-at-rest-integrity-scrubber)
- [Format migration](#format-migration)
- [Legal hold](#legal-hold)
- [Reclaiming a pre-namespacing cache directory](#reclaiming-a-pre-namespacing-cache-directory)
- [Repairing a forged Parquet table version](#repairing-a-forged-parquet-table-version)
- [The maintenance and inspection commands](#the-maintenance-and-inspection-commands)

## Running the maintenance loop

**Continuously.** `ravel-server --mode maintain` runs the loop per tenant, over
all three signals and every shard, on `--maintain-interval-secs` (default 300).
It needs a backend that reports the `multipart` capability. It serves no
ingest or query routes, and it still binds `--listen-http` for liveness.

`--maintain-tenant <name>`, repeatable, names a tenant that this process
maintains in addition to every tenant named by `--tenant-token` or
`--tenant-token-file`. A deployment that authenticates through OIDC or mTLS
requires it. Those tenants are known only after a request arrives, and
maintenance has no other way to learn about them.

Four flags size how much work one tick does and how quickly a stuck unit is
visible:

| Flag | Default | What to change it for |
|---|---|---|
| `--maintain-interval-secs` | `300` | How often each tenant's tick runs. `0` is refused at startup. |
| `--maintain-unit-concurrency` | `4` | How many owned units this process maintains at once within one tenant's tick, so one pathological unit cannot starve the rest. Raise it on a process that owns many units and has spare request concurrency to spend, lower it on a host shared with other work. It is clamped to at least 1, and `0` degrades to a sequential walk rather than deadlocking. |
| `--maintain-stalled-after-intervals` | `3` | Consecutive failed ticks a unit must accrue, with no intervening success, before it counts as stalled. Lower it to be paged sooner on a flaky unit at the cost of noise from transient faults; raise it to tolerate a noisier store at the cost of a slower page. A single success resets that unit's counter. |
| `--maintain-interior-reverify` | `6h` | The slow safety-net cadence for hours that are neither at the head nor the tail of the keyspace this tick. An interior bucket's memoized state is re-verified no less often than this, and the sweeper runs a full-keyspace pass on the same cadence instead of its per-tick head-and-tail pass. Head and tail hours are unaffected and are evaluated every tick. Zero disables the safety net, which makes every interior bucket always due. |

**One-shot.** Every loop also has a `ravel-cli` form, for inspection or to run
one pass by hand. See
[the maintenance and inspection commands](#the-maintenance-and-inspection-commands).

A unit is one `(tenant, signal, shard)` triple. In a multi-replica maintain
deployment, the live workers share the units so that one live worker owns each
unit. A unit's owner runs its retention and compaction. The sweep pass
(superseded inputs, unreferenced parts, orphan collection) of every shard of a
`(tenant, signal)` pair runs on the owner of the pair's shard 0, which is the
process that folds the pair, so a hold the sweep finds on any shard reaches
that pair's fold. [The observability guide](../observability.md) catalogues the metric
families that report ownership, stalls and merge memory.

## Catalog fold and verify

The fold lowers query cost. It is not a durability mechanism. Resolve always
falls back to a direct listing of commit records, so a fold that never runs,
crashes or falls behind never loses or hides data. Queries then pay listing
cost over a wider window.

Two cases list commit records per bucket on every query:

- The open window above the fold watermark, bounded by `max_ingest_lag`
  (default 2h).
- Any tenant with folding disabled or not yet caught up.

That listing path does not scale past roughly 10,000 commit records in one
bucket. A tenant whose fold stalls behind a heavy load sees the cost first at
query time.

### Which processes fold

| Mode | Scheduled fold | On-demand `POST /api/v1/admin/fold` |
|---|---|---|
| `maintain` | Yes, for the `(tenant, signal)` pairs it owns | No. It serves no query surface. |
| `all` | Yes, for every pair. Its live set is itself. | Yes |
| `query` | No | Yes |
| `gateway` | No | No. It cannot fold by any route. |

A deployment with neither a `maintain` nor an `all` process never folds on a
timer. Its catalog stays unsealed until someone calls the on-demand route. Run
at least one `maintain` process.

A `maintain` process folds only the `(tenant, signal)` pairs that it owns. It
uses the same rendezvous hash and the same heartbeat live set that distribute
maintenance units.

- Scaling `maintain` from one replica to N divides the fold work N ways. It
  does not run N full copies.
- A non-owner issues no request for a pair that it does not own. The ownership
  test is pure computation over the tenant listing. It runs before every
  per-tenant read, including that pair's `t/<hash>/config` lifecycle record
  and its `HEAD` peek.
- A tick makes one request that no owned pair accounts for: the single
  delimited listing of `t/` that finds the tenants.
- Ownership is evaluated per tick. A replica that leaves the fleet hands its
  pairs to the survivors with no operator action.

At the defaults, the handover bound is 570 s (9 min 30 s). The liveness window
alone bounds when the departure becomes visible. It does not bound when the
pairs are folded again. Three terms add up, and each is the worst case of the
one before it:

| Term | Default | Seconds |
|---|---|---|
| Liveness window, `liveness_factor * heartbeat_interval` | 3 x 60 s | 180 |
| The survivor's next heartbeat tick, which is when it re-lists and recomputes the live set | 60 s | 60 |
| The survivor's next fold tick, `--fold-interval-secs` plus up to 10% jitter | 300 s + 30 s | 330 |
| **Sum** | | **570** |

570 s bounds the survivor's first fold tick over the pair. The re-fold lands
within 630 s (10 min 30 s) of departure in every case, ignoring clock skew
between replicas:

- If the departed replica folded the pair just before it left, the pair's
  `HEAD` stays younger than `--fold-interval-secs` for 300 s after departure.
  A survivor tick inside that span skips the pair as already fresh.
- The survivor's next fold tick is at most 330 s (300 s plus 10% jitter) after
  the skipped tick. The re-fold therefore lands within 300 + 330 = 630 s.
- A survivor tick at or after the 300 s mark folds the pair on that tick,
  inside the 570 s.
- A fold earlier than just before departure only moves the 300 s mark earlier.

Neither figure is a durability bound. An unfolded pair costs query listing and
nothing else. A shorter `--fold-interval-secs` shortens the third term
proportionally. The heartbeat interval and the liveness factor carry no flag
and are always the defaults above.

Two flags configure the scheduled fold and nothing else:

- `--disable-fold` turns the scheduled task off.
- `--fold-interval-secs` (default 300) controls only how often the task wakes
  to check for newly sealed hours. It has no bearing on when an hour becomes
  eligible to seal.

A `gateway` or `query` process refuses to start when either flag is passed
explicitly. The error names the flag and the mode.

If you disable the fold, change your alerts:

- The fold-liveness gauge
  (`ravel_catalog_fold_last_success_timestamp_seconds`) never advances past
  its `0` sentinel, because no fold stamps it. The `RavelCatalogFoldStalled`
  alert in the observability guide then pages about ten minutes after start.
- The page is consistent with the unsealed span growing. If you run a fleet
  with the fold intentionally off, drop that alert for that fleet.
- A fleet with no scheduled-fold process renders that gauge nowhere, and the
  alert's `absent()` arm fires.
- Such a fleet can still render `ravel_catalog_fold_cycles_total` and
  `ravel_catalog_fold_failures_total`. A `query` process reports both for the
  on-demand route.

### The seal margin, and why it matters

A fold seals an hour only once:

```
now >= hour_end + max_flush_lifetime + clock_skew_allowance + fold_safety_margin
```

The defaults are 1h, 5m and 15m, so 1h20m in total. The three margins give
every writer's flush for that hour time to land before the fold treats the
hour as closed.

If you widen `max_flush_lifetime`, review `fold_safety_margin` at the same
time. Do the same if you widen the tolerated wall-clock skew between writers
and the fold. If you do not, the clock-skew divergence that
[routine verification](#routine-verification) detects becomes reachable.

### Fold after writers exit

After a bulk load whose loader process has exited, you can fold the loaded
hours without the 1h20m wait:

```sh
ravel-cli catalog fold --tenant <name> --shards <n> --signal <signal> \
  --max-flush-lifetime 0s
```

**Do not use this command while a writer for that tenant is live.** A commit
record published into a bucket that this fold already sealed is never picked
up by a later incremental fold, which re-lists only hours after the watermark.
The repair is the HEAD-deletion rebuild in
[troubleshooting](troubleshooting.md#queries-are-missing-recently-written-data).

The margin exists for writers that still run. After the loader exits, nothing
can publish into those hours. Until the fold covers them, every query pays one
commit-record read per segment.

The command drops only the flush-lifetime term. The clock-skew allowance and
the fold safety margin still apply, a 20 minute margin, so the hour that is
being written is still not sealed. The report's `seal_margin` line shows the
sum that the run used.

### Routine verification

```sh
ravel-cli catalog verify --tenant <name> --signal <signal>
```

`catalog verify` re-lists every sealed commit record for one signal and diffs
the list against that signal's snapshot. It prints counts of entries missing
from the snapshot or mismatched against it, and exits nonzero on any
divergence. It only lists and compares and never mutates, so you can run it
at any time against a live tenant.

Run it on a schedule, and after you deploy or reconfigure seal margins. It
catches a clock-skew divergence before a query does.

Run it once per signal that the tenant writes. A tenant's logs snapshot is a
separate object from its metrics snapshot, and `--signal` defaults to metrics.
On a logs-only tenant the default invocation reports "nothing to verify".

The same rule applies to a fold by hand. The background fold task covers all
three signals, but `ravel-cli catalog fold` folds the one signal that
`--signal` names. Folding metrics on a logs-only tenant reports an entry count
of zero and publishes an empty metrics HEAD.

## Compaction

The compactor rewrites the many small L0 segments of a sealed ingest-hour
bucket into a handful of large L1 segments. Object count per hour drops from
thousands to a handful, and every query over that hour pays proportionally
fewer requests.

- A bucket is sealed at its end plus `max_flush_lifetime` and
  `clock_skew_allowance`. After that, no further commit can appear.
- The compactor publishes one compaction record that names the L0 inputs it
  superseded.
- It never deduplicates. Before it publishes, it checks that the L1 outputs
  hold the same number of records as the L0 inputs. A query over the L1 output
  therefore returns the same rows as a query over the L0 inputs.
- Metrics, logs and spans go through the same code.

To compact by hand, one bucket or one whole tenant and signal:

```sh
ravel-cli maintain compact-bucket --tenant <t> --signal <metrics|logs|spans> \
  --shard <n> --hour <n> [--dry-run] [--no-claim]

ravel-cli maintain compact-tenant --tenant <t> --signal <metrics|logs|spans> \
  [--shards <n>] [--from-hour <n>] [--to-hour <n>] [--bucket-concurrency <n>] \
  [--dry-run] [--no-claim]
```

Under per-role storage credentials, run both with the Maintain credential, not
Admin. They take compaction claims and write L1 segments and compaction
records, which only the Maintain policy grants (see
[the Admin credential](deployment.md#the-admin-credential)).

`compact-tenant` discovers the hours itself:

- It walks each shard's ingest hours in ascending order and stops at the first
  unsealed one, because every later hour is unsealed too.
- It streams one line per bucket as each completes. Then it prints a summary:
  the compacted, already-compacted, not-sealed, below-minimum, tombstoned,
  claim-skipped and claim-cancelled counts, segments written, wall time, the
  failure count and the concurrency it used.
- A bucket whose compaction errors does not abort its siblings. The walk
  completes and prints each failed bucket's own outcome line. Then it exits
  nonzero with an aggregate that names how many failed and how many succeeded.
- A clean run exits zero. A not-sealed bucket is a reported outcome and not a
  failure.
- With no `--shards` and no provisioning record, `compact-tenant` refuses and
  names the tenant. With both, the two must agree.

`--bucket-concurrency N` runs up to N buckets at once and is refused at 0.
`--bucket-concurrency 1`, the default, is the fully sequential walk. Each
concurrent bucket gets a per-bucket share of the merge cursor budget, the
whole budget divided by N. The memory envelope of an N-bucket run therefore
stays inside one host. A merge that no longer fits its share fails closed with
a typed budget-exceeded error and does not grow past the share.

**`--max-flush-lifetime` on either command is a safety override, not a tuning
knob.** Use it only for a tenant known to be quiescent, such as one whose bulk
load has finished. It is unsafe below the ingest path's real flush lifetime. A
bucket that a writer still flushes into can then be sealed and compacted, and
the compaction misses that writer's later-published object.

- The flag overrides the compactor's flush lifetime for that invocation. It
  has the same humantime grammar as the server flag (`30m`, `1h5m`, `0s`).
- A bucket is sealed only once
  `now >= hour_end + max_flush_lifetime + clock_skew_allowance`. A freshly
  finished load therefore waits over an hour before its final hours can be
  compacted. A lower value seals them at once.

### L1 segment size

Two targets decide where the compactor closes one L1 segment and starts the
next:

- The **memory split target**, `l1_part_memory_target_bytes`, sizes the
  decoded record heap that one segment holds while it is merged. It is a split
  point, not a ceiling. A span merge checks it only between traces, so a
  segment can run past it by a whole trace. Set it with
  `--maintain-l1-part-memory-target-bytes` on the server and
  `--l1-part-memory-target-bytes` on `compact-bucket` and `compact-tenant`.
- The **stored-size target**, `max_l1_part_bytes` (default 256 MiB), is the
  cap on the encoded object size of a log or metrics segment, measured by
  encoding the segment. Set it with `--max-l1-part-bytes` on `compact-bucket`
  and `compact-tenant`. The server has no flag for it.

Which target a merge reads depends on the codec:

| Codec | Memory split target | Stored-size target |
|---|---|---|
| Logs (RLOG) | Read. The segment closes on the target that it reaches first. | Read |
| Spans (RSPAN) | Read. It is 256 MiB unless `--l1-part-memory-target-bytes` was set explicitly, and the `rspan_l1_part_memory_target_bytes` line names it. | Not read |
| Metrics (RSEG) | Not read | Read |

- Log segment bytes also depend on the compaction zstd level.
- When the memory target is derived, the RLOG stored-size cap is set to the
  same value. The memory target then closes every segment and no probe runs.
- `--max-l1-part-bytes` sets the RLOG and the metrics cap together.
- None of these settings is part of a compaction record's identity. Two
  processes that resolve different log targets cut the same bucket into
  different segments. On a 30 GiB host at the default unit concurrency of 4,
  the server derives 256 MiB per merge and `compact-bucket` 1 GiB.

When the memory split target is not set, the log merge's target is derived
from the memory budget. The span merge's target stays 256 MiB, because span
segments have no stored-size target to cap them:

```text
budget = host memory - overhead reserve - merge cursor budget  (at least 0)
reserve = max(min(2 GiB, host memory / 4), 256 MiB)
target = max( min( budget / 8 / concurrent_merges,
                   claim lease * 10 MiB/s / 2,
                   8 GiB ),
              256 MiB )
```

The three terms inside `min` are the memory share, the claim lease and the
ceiling. The 256 MiB floor is applied last.

- The merge cursor budget is the memory that a merge can hold in its cursors
  on top of the segment that it builds. It is 20 GiB by default, and no
  shipped flag changes it. The derivation subtracts it so that the segment
  does not get memory that the cursors already claim.
- The lease term keeps the segment small enough to encode and upload inside
  the claim lease. The server's startup check assumes 10 MiB/s and warns when
  the lease is under twice the transfer time. A lease of 300 s allows
  1500 MiB.
- An explicit flag wins over the derivation, is used as given for both log
  and span merges, and is refused at 0.
- When the host memory cannot be read, the target falls back to 256 MiB.
  `ravel-cli` prints a note on stderr that says so, and a `--mode maintain`
  server logs a warning.

The inputs differ between the server and `ravel-cli`:

| Input | Server | `ravel-cli` |
|---|---|---|
| Host memory | Its effective memory: `MemTotal` lowered to a cgroup limit, from the same detector that `ravel-cli` calls. | `MemTotal` from `/proc/meminfo`, lowered to a cgroup memory limit when one is set (`sysctl hw.memsize` on macOS). |
| Concurrent merges | `--maintain-unit-concurrency` | `--bucket-concurrency` (1 for `compact-bucket`) |
| Cursor budget | Deducted once. At a unit concurrency above 1 the server therefore undercounts what that many concurrent merges can hold in their cursors. | `compact-tenant` splits the one 20 GiB cursor budget between its concurrent buckets, so the whole 20 GiB is deducted whatever `--bucket-concurrency` is. |

Worked figures, from a segment of 256 MiB being about 32k rows and 2.1 MB
stored on the 104-column schema below and scaled linearly from there:

| Host and command | Budget | Target | Bound by | Rows | Stored |
| --- | --- | --- | --- | --- | --- |
| Server, 30 GiB, 4 units | 30 - 2 - 20 = 8 GiB | 256 MiB | share (equal to the floor) | 32k | 2.1 MB |
| `compact-bucket`, 32 GiB | 32 - 2 - 20 = 10 GiB | 1.25 GiB | share | 160k | 10.5 MB |
| `compact-bucket`, 64 GiB | 42 GiB | 1.46 GiB | claim lease | 188k | 12.3 MB |
| Server, 64 GiB, 1 unit, `--maintain-claim-lease 1200s` | 64 - 2 - 20 = 42 GiB | 5.25 GiB | share | 672k | 44 MB |

Decoded heap is far larger than stored bytes on a wide schema. On a
104-column schema a decoded record is about 8 KiB. The 1.25 GiB target that a
32 GiB host derives holds five times the rows per segment of the 256 MiB
target.

The 256 MiB floor binds while the budget is at most 2 GiB per concurrent
merge:

- `compact-bucket` keeps 256 MiB up to a 24 GiB host.
- `compact-tenant` keeps it up to `22 GiB + 2 GiB * --bucket-concurrency`.
- A server at the default `--maintain-unit-concurrency` of 4 keeps it up to a
  30 GiB host.
- A lease too short for 256 MiB (under about 51 s) still gets 256 MiB. The
  server's startup check then warns, which is the signal to raise the lease.

To get bigger log segments, raise one of the three things that the derivation
reads: the host's memory, the claim lease, or the flags. Set the claim lease
with `--maintain-claim-lease` on the server. The CLI takes the compactor's
default. There is no flag for the cursor budget.

When you set the larger target by hand, give `compact-bucket` and
`compact-tenant` both flags. `--l1-part-memory-target-bytes` above 256 MiB
without `--max-l1-part-bytes` leaves the log stored-size cap at 256 MiB. The
log merge then runs exact-encode probes once a segment's uncompressed payload
reaches that cap. Each probe is a clone and encode of the whole in-progress
segment. The server has no stored-size flag to raise the cap with.

The stored-size target stays the operator's cap. Set `--max-l1-part-bytes` to
bound object size regardless of how much memory the host has. Set it below
the derived target if you want it to bind.

Each run says which values it used and why. `compact-bucket` and
`compact-tenant` print one line per codec in their report:

- `rlog_l1_part_memory_target_bytes:`, for example
  `rlog_l1_part_memory_target_bytes: 1342177280 (resolved from a memory budget of
  10737418240 over 1 concurrent merge; bound by the memory share, budget / 8 /
  merges)` from `compact-bucket` on a 32 GiB host, or
  `rlog_l1_part_memory_target_bytes: 1073741824 (set by flag)`. The `bound by`
  clause names the derivation term that decided the target: the memory share,
  the claim lease, the 256 MiB floor or the 8 GiB ceiling.
- `rlog_max_l1_part_bytes:`, the log merge's stored-size cap.
- `rspan_l1_part_memory_target_bytes:`, the span merge's target.
- `max_l1_part_bytes:`, the cap that metrics merges read.

The server logs them once at startup as a `performance default resolved` line.
The line has `setting=rlog_l1_part_memory_target_bytes`, `source` set to
`derived`, `flag` or `fallback`, `bound` naming the term (`memory_share`,
`claim_lease`, `floor` or `ceiling`), `rlog_max_l1_part_bytes`,
`rspan_l1_part_memory_target_bytes` and `max_l1_part_bytes`.

#### Record names an absent part

A run can fail with "compaction converged on a prior record that references
part ... which is absent", or with the `AlreadyExists` variant that says the
part was gone when HEAD-verified.

Hold the inputs at once with a [legal hold](#legal-hold) on that shard
(`ravel-cli hold set --tenant <id> --signal <signal> --shard <n>`), and keep
the hold until a repair command ships. The record keeps pointing at the absent
part. The superseded-input sweep deletes the bucket's L0 inputs once the
record is older than the protection horizon, which turns the gap into data
loss.

- No shipped command rebuilds the part today. Once a bucket's listing carries
  a compaction record, `compact-bucket`, `compact-tenant` and the server's
  maintenance loop return `AlreadyCompacted` and build nothing. A rerun with
  other flags changes nothing.
- The message names the settings that decide part boundaries. A rebuild of
  that part reproduces its key only with the values that the run that wrote
  the record used.
- The record writer's report or startup log names the memory target that it
  resolved.

### Compaction beside a live cluster

The background supervisor and a `compact-bucket` or `compact-tenant` run can
reach the same sealed bucket at the same time. Both ask for the bucket's claim
first, and the one that is refused the claim does not merge.

The CLI asks for a claim on every bucket before it merges the bucket, whatever
the bucket's size. It takes one claim per bucket at any
`--bucket-concurrency`, all under one process id per invocation. The report
header prints that id on its `claims:` line. A supervisor names that holder
when it skips a bucket that the CLI holds.

A run reports two claim outcomes. Neither counts as a failure. The walk
continues and exits zero unless some other bucket failed, the same as for a
not-sealed bucket. `compact-bucket` prints the same two outcomes and also
exits zero.

- `outcome=ClaimSkipped`: the bucket was refused its claim and is not merged.
  The line has the reason, the claim's `work_id` (the last segment of its key,
  `sys/maintain/claims/compaction/<work_id>`), the holder's process id
  (`unknown` when there is no readable claim to name one), the claim's expiry
  as `claim_expiry_unix_ms` and the earliest useful retry point as
  `retry_after_unix_ms`. The bucket is counted in `claim_skipped`.
- `outcome=ClaimCancelled`: the bucket lost its claim mid-merge, because
  another process took the claim over or the claim object is gone. The run
  stops without publishing and prints the checkpoint that it stopped at. The
  bucket is counted in `claim_cancelled`. Parts that the run had already
  written stay in the store, content-addressed, for a later run to reuse.

To compact a skipped bucket yourself, rerun at `retry_after_unix_ms`, not at
`claim_expiry_unix_ms`. The retry point tracks the printed expiry only for
`held_by_another`. If the holder finished its merge by then, the rerun reports
the bucket as already compacted. The four reasons are:

- `held_by_another`: a live claim, held by the process id printed. The retry
  point is one millisecond past the printed expiry.
- `steal_lost`: the claim had expired and this run tried to steal it, but
  another contender's steal won the compare-and-swap first. The printed expiry
  is the one already in the past. The winner has just written a fresh lease,
  so the retry point is a full lease from the moment of the loss.
- `unreadable_claim`: the claim object does not decode. See below.
- `vanished_twice`: the claim key disappeared between the create and the read
  that followed it, twice in a row. Nothing is held, so the printed expiry is
  `0` and the retry point is immediate.

An `unreadable_claim` holds the bucket, for compaction and for erasure alike,
until an operator removes the claim object:

1. Confirm the reason. The holder prints as `unknown`, and the reason stays
   `unreadable_claim` on every retry. Once the object is older than one lease,
   the run logs a warning that names the claim's key.
2. Make sure that no compaction, migration or erasure of that bucket runs.
3. Remove that one key by hand.

Ravel never steals an unreadable claim and never deletes it programmatically.
A run that continues without the claim can publish a compaction built from
data that an erasure of the same bucket already removed. A claim written
by a newer release in a format that this release cannot read looks the same.
A rollback across such a release therefore leaves every bucket that the newer
release claimed held this way.

`--dry-run` and `--no-claim` take no claims. `--no-claim` is for repair work
when a claim is in the way:

- Between two compactions it is safe. The compaction record's create-if-absent
  still decides which output is published. The merge can duplicate one that
  another maintainer runs.
- Avoid `--no-claim` while an erasure request for the tenant is pending.
  Against an erasure of the same bucket the only check left is the re-list
  that each run makes just before it publishes, which leaves a short window.

`maintain migrate` takes the same claims, prints the same `claims:` line, and
accepts the same `--no-claim`. Its `--dry-run` also takes no claims, but it is
not compaction's plan report. It skips the walk and runs only the read-only
re-audit (see [Re-encoding compaction parts](#re-encoding-compaction-parts)).

### Compaction claim metrics

A claim does two jobs:

- Between two compactions, a claim only saves work. The two still converge on
  one compaction record through its `CreateIfAbsent` and content-addressed
  parts, so a claim bug there can only waste work.
- Between a compaction and an erasure of the same bucket, the claim also keeps
  them apart. Their records have different keys, so the create-if-absent does
  not. A compaction that publishes after the erasure serves the erased data
  again. The claim and the re-list before each publish stop that. A claim bug
  there can delay an erasure. With the re-list's short window, such a bug
  costs more than wasted work.

The background supervisor's per-unit tick takes claims. So do
`ravel-cli maintain compact-bucket`, `compact-tenant` and
`migrate`. `--no-claim` on the
CLI and `--maintain-claims off` on the server are the two ways to opt a run
out.

The server's `/metrics` endpoint renders five claim counters, one family per
signal. They count this server's own maintenance supervisor only. A
`ravel-cli maintain compact-*` run is a separate process. It reports its
outcomes in its walk summary and never moves these counters.

- `ravel_maintain_claims_acquired_total`: claims taken. A claim is fresh,
  taken over from an expired claim, or taken back from this process's own
  leftover claim.
- `ravel_maintain_claims_stolen_total`: the subset of the above taken over
  from an expired claim. Expect steals after a crash, a restart, or a failed
  run whose process stopped maintaining that bucket, because each leaves an
  expired claim behind. A process that still runs takes its own leftover claim
  back as an acquisition.
- `ravel_maintain_claims_lost_total`: claims that this process held and lost
  before publishing. Another process took the claim over after its lease
  expired, or the claim object was deleted, and this run cancelled at its next
  checkpoint. A lifecycle rule or a manual delete on the claim prefix also
  moves it, so rule that out before you raise the lease.
- `ravel_maintain_claim_renew_failures_total`: renewals that failed with a
  store error, distinct from a lost claim. The run stops with an error and
  leaves its claim in place. The same process takes it back on its next pass
  and compacts the bucket. Any other process waits for the claim to expire.
- `ravel_maintain_claims_skipped_total`: bucket evaluations that did not
  compact, or erasure rewrites that published nothing, because they could not
  take the bucket's claim. Most often an unexpired claim held the bucket. The
  other causes are a lost steal race, a claim that could not be read, or one
  that vanished twice (something outside the protocol is deleting claims). A
  held bucket adds one per maintenance pass until the claim expires. The pass
  that observed the claim logs a skip line with the reason. The later passes
  of the same hold count without logging.

Read the acquired, stolen, lost and skipped counts as lower bounds. They are
gathered per shard pass and added when the pass completes, and a pass that
ends in an error drops what it had counted. A renewal store error is counted
where it surfaces, so `ravel_maintain_claim_renew_failures_total` is not
affected.

Alert on lost claims, not on steals:
`rate(ravel_maintain_claims_lost_total[15m]) > 0` held for 30 minutes (`for:
30m`). A lost claim means that a run was still working when its lease expired
and another process took the bucket over. The lease is then shorter than that
deployment's merges, so raise `--maintain-claim-lease`. A steal is the other
side of that event, but it also follows every crash and restart, so a steal
rate alone over-alerts.

Two flags configure claiming, and a third has no effect:

- `--maintain-claim-lease <DURATION>` (default `300s`): how long a claim stays
  live without a renewal. Zero is refused. A lease below twice the time to
  encode and PUT the largest L1 segment at a conservative rate logs a startup
  warning and is not refused.
- `--maintain-claims on|off` (default `on`): `off` disables claiming
  everywhere on that process. Use it for a store whose qualification record
  predates the CAS probes, or for an emergency. Two racing compactions still
  converge at the compaction record, and the loser pays its merge first. With
  claims off, a compaction and an erasure rewrite of the same bucket are kept
  apart only by the re-list that each runs just before it publishes, which
  leaves a short window.
- `--maintain-claim-min-input-bytes <BYTES>` (default 64 MiB): has no effect
  and will be removed. With claims on, every bucket is claimed whatever its
  size, because the claim also keeps a compaction and an erasure rewrite of
  the same bucket from publishing over each other. The flag is still accepted
  so that existing deployments start unchanged. Zero is still refused.

### A log object compaction cannot rewrite

The RLOG writer refuses a `stream_attrs` blob the reader cannot decode (a
resource or scope attribute nested past the decoder's cap, or a scope name or
version that is not UTF-8). A log object written before the writer checked
this can still carry one, and compaction cannot write its records
into an L1 segment. Compaction leaves that object out of the merge and merges
the rest of the bucket. The compaction record does not name the skipped object,
so no sweep deletes it as superseded, and the catalog keeps serving it as an
L0 object.

Queries still read the object. A query that has to decode its stream
attributes fails with a typed error rather than returning its records: a SQL
query that projects `attrs` or a declared attribute column, a metric query
over logs that decodes the object's stream labels, and a logs query while an
erasure request is pending for the tenant's logs. These failures do not depend
on compaction: they are the same whether or not compaction has skipped the
object.

What an operator sees:

- `ravel_maintain_compaction_inputs_skipped_total{signal="logs",reason="unwritable_stream_attrs"}`
  (or `signal="audit"` for the query-audit shard) moves by one per skipped
  object. The counter is per process and resets to 0 on restart. Each object
  counts once per process, and only when compaction reads it: once the rest of
  its bucket is compacted no later evaluation reads the object, so after a
  restart the counter stays at 0 while the object remains. Alert on an
  increase, not on a level.
- One `WARN` line per object per process, `compaction skipped an input object
  it cannot rewrite`, carrying `object_key` (the object's storage key),
  `reason` and `error` (the decoder's refusal), with the bucket's signal,
  shard and hour. The warning line is the durable trace of a skip; the counter
  is not.
- The rest of the bucket compacts as usual when at least
  `min_compaction_inputs` (default 2) healthy inputs remain. When fewer remain,
  including when every input is such an object, nothing is published and the
  bucket reports below-minimum. `ravel_maintain_l0_records_pending` then counts
  the healthy inputs that remain and not the skipped objects, so a bucket whose
  inputs are all skipped adds 0 to it. The process that skipped the object does
  not read it again; a bucket left this way has no compaction record, so each
  new process, and this one after a restart, reads the object once, counts it
  and warns.
- The evaluation that reads and skips the object takes and completes the
  bucket's claim, as any compaction does. A later evaluation, in the same
  process, of a bucket left below the minimum by its skipped objects returns
  before the claim and takes none.
- A bucket whose listing holds fewer than `min_compaction_inputs` L0 records
  is never read by compaction, so an object of this kind alone in such a bucket
  is not counted or warned. It is counted in
  `ravel_maintain_l0_records_pending` like any other L0 record.

No maintenance path rewrites the object. Retention expiry deletes it with the
rest of its bucket. There is no supported manual procedure to remove or repair
it yet: a data object's key is derived from its content, so it cannot be
rewritten in place, and deleting only the data object leaves its commit record
pointing at an object that no longer exists. Leaving it until retention removes
it changes nothing for reads, since the failures above do not depend on
compaction. The exception is erasure:

- In a bucket with a compaction record, the erasure rewrite rewrites only the
  record's outputs and never reaches the skipped object, so an erasure request
  whose window covers the object stays pending.
- In a bucket with no compaction record (every input skipped, or too few left
  to meet the minimum), the erasure rewrite of that bucket finds the object
  before it merges, writes nothing to the bucket and reports it blocked. See
  [A log object an erasure rewrite cannot
  rewrite](#a-log-object-an-erasure-rewrite-cannot-rewrite).

`ravel-cli maintain migrate`, which skips no input, does not rewrite a bucket
that has no compaction record and holds the object. It skips that bucket,
walks on to the later ones, and leaves the family's floor where it is while
the object remains; the same section describes what it prints. A
`compact-bucket` or `compact-tenant` run skips the object the same
way and logs the same warning, but it is a separate process with no `/metrics`
endpoint, so it moves no server counter.

### A log object an erasure rewrite cannot rewrite

The erasure rewrite of a bucket with no compaction record reads every live L0
object in it. When one of them carries a `stream_attrs` blob the RLOG writer
refuses (the object described in [A log object compaction cannot
rewrite](#a-log-object-compaction-cannot-rewrite)), the rewrite checks every
input before it merges, builds and publishes nothing for the bucket, and
reports the bucket blocked by that object. Unlike compaction, it does not
leave the object out and rewrite the rest.

A blocked bucket writes nothing, so every live object in it, healthy ones
included, stays live. The completion check counts each of them as live raw
L0, so an erasure request whose event-time window reaches any live object of
the bucket stays pending while the object exists: its `.dreq` and its
query-time filter stay, and no `.done` is written. The blocked bucket does
not hold back anything else. Every other bucket of the tenant and signal is
rewritten and verified on the same tick, and a request whose window reaches
no live object of a blocked bucket completes. The pass still takes and
completes the bucket's claim, and counts it like any other.

In a bucket that does have a compaction record the rewrite rewrites only that
record's parts and never reaches the object, so it is not blocked, counted or
warned about there. A request whose window covers the object stays pending in
that case too.

What an operator sees:

- `ravel_maintain_erasure_unwritable_objects_total{signal="logs",reason="unwritable_stream_attrs"}`
  moves by one per blocking object. It counts objects, not passes: each
  object counts once per process however many ticks it blocks. The counter
  resets to 0 on restart, and the first pass after a restart that tries to
  rewrite the bucket for a pending request counts the object again. Alert on
  an increase, not on a level.
- One `WARN` line per object per process, `erasure rewrite of a bucket is
  blocked by an input object it cannot rewrite`, carrying `object_key` (the
  object's storage key, escaped), `reason` and `error` (the decoder's
  refusal), with the bucket's tenant, signal, shard and hour.
- Every request whose window reaches a live object of the bucket stays
  pending with no `.done`, on every tick, while the object exists.

`ravel-cli maintain migrate` skips such a bucket in the same way. The run
prints one line per skipped bucket, followed by a `# ` line that says what it
means:

```
buckets_unwritable_skipped: 1
unwritable_skipped: shard=0 hour=495001 reason=unwritable_stream_attrs object_key="t/.../logs/..."
# Each unwritable_skipped bucket holds a log object whose stream_attrs blob the RLOG writer refuses (issue #2580). ...
```

The walk goes on to the next bucket, and the skipped bucket's below-target L0
records count against `--budget-records` as a migrated bucket's would. The
fresh re-audit counts those records as stragglers, so the floor is not raised
while the object remains. A run that skipped a bucket exits nonzero however it
ended, including a run that stopped on its budget, because a later run that
resumes from the cursor does not name that bucket again. Re-running migrate is
not the remedy.

No maintenance path removes or repairs the object, and there is no supported
manual procedure to remove or repair it yet, for the reasons given in [A log
object compaction cannot rewrite](#a-log-object-compaction-cannot-rewrite):
its key is derived from its content, and deleting only the data object leaves
its commit record pointing at an object that no longer exists. Retention
expiry deletes it with the rest of its bucket. Until then, every request it
holds stays pending, with its `.dreq` and query-time filter in place.

## Garbage collection and retention

Ravel deletes data through two independent triggers. The maintenance loop or
the matching one-shot command drives both. Objects are immutable throughout:
deletion removes whole objects and nothing is ever modified in place.

**Age-based retention.** If a sealed bucket's newest event is older than the
tenant's retention window, Ravel writes a durable retention tombstone for it.
The tombstone immediately excludes the whole bucket from new query snapshots.
Retention is off by default. See
[configuring it](configuration.md#age-based-retention) for the flags and the
floor that its window is validated against. Retention runs before compaction,
so an expired bucket is tombstoned and not compacted first.

**The sweeper** is the only component that issues a delete. Each of its three
rules re-verifies its precondition against a fresh listing immediately before
each delete, and every delete is idempotent:

1. **Orphan collection**: an L0 data object with no commit record, older than
   `grace + max_flush_lifetime`. The writer interlock guarantees that such an
   object can never gain a commit record later, so deleting it cannot orphan a
   future reader.
2. **Superseded-input sweep**: the L0 commit records and data objects that a
   compaction record names, once `now >= record.created_unix_ns +
   protection_horizon`. Records are deleted before data objects, so a crash
   mid-sweep never leaves a commit record pointing at a deleted object.
3. **Unreferenced L1 cleanup**: an L1 object that no compaction record in its
   bucket references, once a compaction record exists for that bucket and the
   object is older than `grace + max_compaction_lifetime`.

Retention's own physical sweep deletes everything in a tombstoned bucket: L0
records, compaction records, L0 data, L1 segments, and the tombstone last. It
does so once `now >= retired_at_ns + protection_horizon`, and only after a
verifying listing shows the bucket empty but for its tombstone.

### The format-version hold

Before its first delete, the retention sweep reads the trailer of every data
object in the bucket, one 16-byte ranged GET per object. It checks the
format version against the running build's reader window for the bucket's own
format: metric segments for metrics, log segments for logs, span segments for
spans.

If any object carries a version that this build does not read, the sweep
holds the bucket:

- It deletes nothing in that bucket and leaves the tombstone in place.
- It logs a warning that names the versions.
- It counts the objects on
  `ravel_maintain_retention_held_out_of_window_objects_total`, once per
  object per pass. A rising total means retention is holding data past its
  window: finish the upgrade, complete `maintain migrate`, or roll back.

Such an object is usually not corrupt. The other side of a rolling upgrade, or
the build that a rollback returns to, reads it. A binary rollback across a
format bump therefore loses queryability and not data, for all three signals.
The rolled-back build cannot query the newer objects, but it does not delete
them. The bucket is retired normally once a build that reads them runs again.

An object at a retired version older than this build's window is held the
same way. Neither finishing a rollout nor rolling back clears that hold,
because no current build reads the object.

A trailer with a bad magic, signal, reserved field or footer length is
corruption and is swept as usual. The check reads the trailer only, not the
footer checksum. Damage confined to the version field therefore reads as an
unreadable version and holds the bucket.

### The alerts shard

The same three rules sweep the alerts shard on every tick, for each tenant
whose alert unit the process owns, whatever `--alert-retention` is. The alert
evaluator can abandon a write under any window, and this sweep reclaims it.

A full sweep of that shard costs six listings, also when the shard is empty.
A tenant with nothing to sweep therefore pays two bounded listings instead:

- One of its whole alert keyspace (`t/<tenant_hash>/a/`), which holds every
  commit record, L0 data object and L1 segment that the rules look at.
- One of the quarantine copies taken from it.

When both come back empty, the sweep is skipped for that tick. When either
listing fails, the sweep runs, because a failed listing has not shown the
keyspace empty.

| Tenant | `--alert-retention` | Cost per tick |
|---|---|---|
| Runs alert rules | Nonzero | The sweep, without the two extra listings. The tenant has an alert state memo under that keyspace, so the memo read already shows that the keyspace is in use and the sweep runs without the two extra listings. |
| Runs alert rules | `0` | Seven listings. No memo is read, so the tenant pays the keyspace listing and then the sweep's six. |
| No alert rules | Default 90-day window | One memo GET and three listings: the retention sweep's own check of the alert commit prefix, then the two above. |
| No alert rules | `0` | Two listings. |

For 1000 tenants with no alert rules on a 5-minute tick, that is about 3000
LIST requests per tick on the default window, and 2000 under
`--alert-retention 0`. Running the sweep costs about 7000 and 6000.

### The two timing values

- `grace`, default 24h, is the floor for the orphan and unreferenced-L1 age
  gates.
- `protection_horizon`, default 25 h 5 min, is the gap between a deletion
  anchor and physical deletion. The default is `max_query_duration` 1 h plus
  `grace` 24 h plus the 5 min clock-skew allowance. The anchor is a compaction
  record's creation time, or a tombstone's retirement time. A query resolved
  just before the anchor then still has time to read the inputs that it
  pinned.

Both are `ravel-server` flags, `--gc-grace` and `--gc-protection-horizon`, and
the compactor uses them. **In maintain mode each one must equal the value
stored in the durable `sys/gc` object or the process refuses to start.** The
query deadline must also be less than or equal to the stored maximum query
duration. A horizon change is therefore a two-step operation and not a rolling
configuration change. See
[the configuration page](configuration.md#retention-and-garbage-collection-configuration)
for the order to change them in.

`ravel-cli maintain sweep` reads the same `sys/gc` object:

- It sweeps on the stored protection horizon, grace and maximum flush
  lifetime.
- It refuses before it sweeps when the stored horizon does not cover its own
  5 min clock-skew allowance.
- It also refuses when the stored horizon is below its 1 h maximum compaction
  lifetime plus four times that allowance (1 h 20 min). The server's maintain
  mode runs the same checks at startup.
- On a bucket with no `sys/gc`, it bootstraps the object from the maintain
  defaults, as the server does. A `--dry-run` uses those defaults and does not
  write the object.
- The stored maximum query duration and HEAD cache TTL set its pinned-query
  window, as they do the server's.

`ravel-cli gc-config set` refuses a protection horizon below either of two
bounds, and writes nothing:

- `max_query_duration + grace + clock_skew_allowance`.
- `max_compaction_lifetime + 4 * clock_skew_allowance`. This bound is checked
  against this build's compiled 1 h maximum compaction lifetime.

Both bounds use the 5 min default skew allowance unless
`--clock-skew-allowance` is given. The second bound binds only a deployment
that shortens `max_query_duration` and `grace` far below their defaults. It
exists because a compaction or rewrite run that can still publish over a
record's inputs must not outlive their horizon. The maintain process and
`maintain sweep` re-check both bounds at startup against their own values.

### The pinned-query window

The retention and superseded-input sweeps do not delete an object at the
moment that the live HEAD stops naming it:

1. The first pass that finds the object unnamed writes an unnamed-since marker
   under `t/<tenant_hash>/<signal>/maint/unn/` and holds the object.
2. A later pass deletes the object once `max_query_duration + head_cache_ttl
   + 4 * clock_skew_allowance` has passed on the sweeper's clock. That is
   1 h 20 min 30 s with defaults.

Expect every expired bucket and superseded object to report `pinned_window`
for at least one pass. The window delays each deletion as follows:

- **Retention.** Retention re-evaluates a tombstoned bucket on every maintain
  tick, so its physical deletion lands about one window after its horizon.
- **Superseded inputs in an interior hour.** The superseded-input sweep
  reaches an interior hour only on the full sweep
  (`maintain_interior_reverify`, default 6 h). One full sweep writes the
  marker and a later one deletes once the marker has aged. An interior
  superseded input therefore goes up to two full-sweep intervals after its
  horizon.
- **Erasure requests.** An erasure request's `.dreq`, which carries the
  subject identifier, is kept until every chain that it holds has a marker
  older than the window. It stays that much longer too.
- **One-shot sweeps.** A one-shot `ravel-cli maintain sweep` writes the marker
  and holds on its first run. A second run after the window has passed
  deletes. `--dry-run` writes no marker.

The maintain role needs delete and list on `t/*/*/maint/*`
(deploy/iam/maintain.json). A refused marker delete keeps a retention
bucket's tombstone on every pass. It also blocks, on every pass, a candidate
whose marker must be replaced.

The guarantee is complete only once every maintain process runs a build with
the gate and `sys/gc` has been moved to format version 2 (below). Until then an
older maintain process can still delete by the old rule, with no window, beside
a newer one.

### Upgrading `sys/gc` to format version 2

`sys/gc` format version 2 records the HEAD cache TTL that every query-mode
server process is held to. A fresh bucket is still bootstrapped at version 1,
which records none. Against version 1, a query-mode server process is held to
the compiled default of 30 s.

The upgrade is one-way. A build that reads only version 1 refuses a version 2
object, and nothing writes version 1 back. In order:

1. Upgrade every `ravel-server` process in every mode (`gateway`, `query`,
   `maintain`, and the combined `all` mode) to a build that reads version 2.
   Each reads `sys/gc` at startup and refuses to start on a format version
   that it does not know.
2. Upgrade every `ravel-cli` binary that reads `sys/gc` (`gc-config show` and
   `set`, `parquet sweep`, `maintain sweep`) to a build that reads version 2.
   `ravel-cli gc-config show` prints the stored `format_version`.
3. Run `ravel-cli gc-config set` with the current values and
   `--head-cache-ttl <duration>`, for example `--head-cache-ttl 30s`. This
   writes version 2. The value must be at least 30 s, this build's own query
   HEAD cache TTL, which no server flag lowers. `set` refuses a smaller value,
   because every query process would then refuse to start.

After step 3:

- An older build refuses to start against the bucket.
- A query process whose catalog runs on a HEAD cache TTL above the recorded
  one also refuses to start.
- A later `gc-config set` without `--head-cache-ttl` keeps version 2 and the
  recorded TTL.

## The at-rest integrity scrubber

The scrubber re-verifies stored bytes on a schedule. Without it, the checksum
hierarchy (a whole-object hash at write time and per-section checksums on
read) is verified only when a query touches the covered bytes, and bytes that
nobody queries are never checked. The scrubber runs only in `--mode maintain`,
spawned per process alongside the maintenance loop.

It detects and never repairs. An anomaly is reported. There is no redundant
copy to repair a corrupt segment from.

Each tick, the scrubber re-discovers tenants from storage. For every unit, it
verifies a bounded slice of that shard's committed data objects:

- The slice covers the L0 segments plus the compaction and rewrite output
  parts that the catalog still serves. It excludes a superseded generation's
  parts, an overlap loser's parts, and every part in a tombstoned bucket.
- The check is a section checksum re-check plus a whole-object rehash against
  the recorded content hash.
- The [observability guide](../observability.md) records the lineage filter
  and the `level` label that a mismatch is counted under.

A persisted per-shard cursor in object storage holds a start-after marker.
Each tick lists the shard's commit records strictly after the marker and
stops once the tick's budget is filled. Apart from the count that opens a
rotation, a tick's LIST and GET count therefore follows its budget and not
the corpus size. When the listing runs out past the marker, the rotation
rolls over and the next tick starts again from the head. Every object is
visited once per rotation.

The budget is recomputed every tick (hourly, or every `P` when `P` is shorter)
from what the rotation has observed:

- Every rotation opens with one LIST-only pass that counts the shard's listing
  entries. This pass lists the whole commit prefix, once per rotation.
- Every later tick recounts the rotation's tail window with another LIST-only
  pass, and adds the window's growth to the rotation's estimate. The tail
  window is the last two ingest hours that the previous count met, plus
  anything after them.
- A shard that keeps committing raises its own budget. A commit from any
  writer in those hours is counted. One that lands behind the marker is
  counted too, although it waits for the next rotation, so the estimate errs
  high.
- The rotation is allotted `min(P, retention / 2)` (see below). A tick can
  consume the entries still to cover divided by the ticks left before that
  deadline, never fewer than the sustained rate `ceil(estimate * tick /
  window)` and never more than four times it, and it can issue eight store
  requests per allowed entry.
- These count against the request cap: every listing page that the walk draws
  (an hour re-list included), every record GET attempt whether it succeeded or
  not, and four requests per object verified. The LIST-only count passes do
  not.
- A tick always attempts at least one unit, even one that alone exceeds the
  budget.
- When the entries that a tick would need exceed four times the sustained
  rate, the rotation cannot finish inside its window. The tick logs both
  numbers with the window and `--scrub-period`, and increments
  `ravel_scrub_behind_total`.

The scrubber handles a failed GET by its error:

| Failure | What the scrubber does |
|---|---|
| A retryable error (throttled, timeout, transient) on a commit record or on an object that it names | Stops the tick with the marker behind that unit. The next tick retries all of it. |
| Six consecutive held ticks on one unit | The next tick moves past the unit and counts each of its records and objects that still fails on `ravel_scrub_unreadable_total{reason="retry_exhausted"}`. |
| Any other GET error except not-found, or a record whose bytes do not decode | Counted once on `ravel_scrub_unreadable_total`, at the level of the object or record that failed, with `reason="access_denied"` or `reason="permanent"`. The marker moves past it, so one unreadable record cannot pin the rotation. |
| A record or object deleted after it was listed | Logged and skipped. |

Six held ticks are six hours of tick cadence at the default one-hour tick, and
less when a short `--scrub-period` shrinks the tick.
`ravel_scrub_marker_held_ticks` reads the current count for the signal's worst
shard. Unreadable records and objects are not counted on
`ravel_scrub_checksum_mismatch_total`, which counts only bytes that were read
and did not match.

### Sizing `--scrub-period`

The period `P` sets the scrubber's read budget and the worst-case staleness
before any given object is re-verified. The content tier must read each
object in full to rehash it:

```
sustained scrub read bandwidth = corpus_bytes / P
```

A larger corpus or a shorter `P` costs proportionally more read bandwidth.
The default is `7d`. A zero or unparseable duration fails startup.

A tenant with a retention window gets a shorter rotation when half that window
is shorter than `P`: the rotation is allotted `min(P, retention / 2)`.

- The walk goes oldest hour first. A rotation as long as the retention window
  would reach each object at about the age at which retention deletes it. Half
  the window reaches every object by about half its retained life, while the
  rotation keeps pace.
- A 7-day retention with the default `P` therefore rotates every 3.5 days, at
  twice the read bandwidth that the formula above gives for `P`.
- A retention window whose half fits in a single tick gives every tick the
  budget of a whole rotation. Every tick then normally walks a full rotation,
  including the LIST-only count of the whole commit prefix that opens it. That
  count's cost is not charged against the tick's budget.

The scrubber is the one scheduled task whose cost scales with data volume and
not with metadata volume. Size `P` against the corpus that you have, and watch
`ravel_scrub_cursor_position` to confirm that rotations keep pace.
[The observability guide](../observability.md) catalogues that gauge, the
held-ticks gauge, and the five scrubber counters. The alarms that matter are
in [troubleshooting](troubleshooting.md).

### Cursor compatibility across builds

A cursor written by an earlier release (0.18.0 or before) loads with defaults
for every field that it lacks, and its progress restarts. The next tick opens
a fresh rotation from the head of the shard.

During a mixed-version rolling upgrade, each version's cursor write drops the
fields that only the other version knows. The rotation therefore restarts
each time a shard's ownership flips between versions. Nothing is corrupted,
and the cursor stays in object storage throughout. Once every maintain process
runs one version, the rotation proceeds normally.

### No policy change needed

Enabling the scrubber requires no storage-policy change:

- Its reads, commit records and data objects, are covered by the Maintain
  role's existing read and list grants.
- Its one write, the per-shard cursor, is under the existing `maint/` control
  prefix, which the Maintain role's write grant already names.

## Format migration

```sh
ravel-cli maintain migrate --tenant <t> --signal <metrics|logs|spans> \
  [--shards <n>] [--target-version <n>] [--family <name>] [--budget-records <n>] \
  [--no-claim] [--dry-run] [--reencode-compaction-parts]
```

`migrate` raises a `(tenant, signal, format family)`'s recorded format floor to a
target on-object format version. One invocation:

1. Walks buckets in shard and ingest-hour order from a durable cursor. It
   rewrites every sealed, un-tombstoned bucket that still has an L0 commit
   record below the target version and served raw.
   - A bucket that already carries a compaction or rewrite record is visited
     too, because a record can leave an input served raw.
   - The rewrite reuses the compaction rewrite primitive. It is bucket-atomic
     and produces a compaction record as compaction does.
   - Some buckets serve nothing below the target raw, carry no rewrite
     record, and have compaction records that hold parts below the target.
     With `--reencode-compaction-parts`, the walk re-encodes such a bucket
     (see [re-encoding compaction parts](#re-encoding-compaction-parts)).
     Without the flag, it reports the bucket as `reencode_blocked`.
2. Stops early and persists the cursor once `--budget-records` is spent. `0`,
   the default, is unlimited. Run the command again to resume.
3. Once the walk drains, re-audits fresh. It raises the floor only if that
   re-audit finds zero records below the target.

The budget counts records, not requests, so it does not bound how many
requests one invocation makes. The walk reads the compaction and rewrite
records of every sealed, un-tombstoned bucket that it passes, and a
`reencode_blocked` bucket spends none of the budget.

A clean migration converges and raises the floor in one invocation. You never
need to run `sweep` in between for it to converge. The re-audit excludes a
bucket's pre-rewrite L0 commit records once an authoritative compaction or
rewrite record supersedes them. Those records are dead, sweepable leftovers
of a rewrite, possibly one that this same invocation just
performed. The sweeper's
superseded-input rule still deletes them on its own schedule, which is
storage reclamation and not a correctness precondition.

### A refused raise

A refused raise, reported as "FOUND STRAGGLERS", means that the fresh re-audit
found objects that still exist below the target, some of which queries still
read. It reports three counts, because they are blocked for different
reasons: `l0_commit_records`, `l1_compaction_parts`, and
`rewrite_record_parts`.

`l0_commit_records` is the one count that is entirely live. The two part
figures count the parts of every record that a bucket still LISTS. That
includes records that the resolver no longer serves and that a `sweep` deletes
(see `rewrite_parts` below).

What moves each count:

- **`l0_commit_records`** moves on a re-run for the part of it that was not
  yet sealed when the walk passed, or that landed after it. It also moves for
  any bucket listed as `not_migrated` (see below).
- **A below-target L0 input that only a losing compaction record names** is
  served raw, and the walk cannot migrate it (see `loser_only_inputs` below).
- **`l1_compaction_parts`**: a below-target compaction part is re-encoded only
  by a run with `--reencode-compaction-parts`, and only in a bucket whose one
  compaction record survives supersession (see
  [re-encoding compaction parts](#re-encoding-compaction-parts)). Without the
  flag, a re-run of `migrate` reports the same `l1_compaction_parts` figure
  and names the bucket on a `reencode_blocked` line.
- **`rewrite_record_parts`**: a below-target rewrite part is never migrated,
  by design (see `rewrite_parts` below).

Three more facts apply to `l1_compaction_parts`:

- **A target above the version that the running build writes.** With such a
  `--target-version`, a part recorded at the version that this build writes
  still counts in the figure. A bucket whose below-target parts are all at
  that version gets no `reencode_blocked` line, because a re-encode by this
  build cannot carry them further. Such a part needs a newer build, not the
  flag.
- **No `blocked_bucket` line.** No `blocked_bucket` line is printed for a
  below-target compaction part, with one exception: the bucket's authoritative
  compaction records are all at the target, and the below-target parts belong
  to records that lost their overlap (see `losing_record_parts` below).
- **The figure can fall without a `migrate` run.** The figure is over listed
  records. A compaction record that a later rewrite record or a re-encode
  superseded stays listed until a `sweep` deletes it and its parts, the same
  way a superseded predecessor rewrite does.

### Blocked buckets

`migrate` names on its own line each bucket that a rewrite part or a
loser-only input blocks. It also names each bucket held below the target only
by a losing compaction record's parts. A bucket that qualifies both as
`loser_only_inputs` and as `losing_record_parts` gets the
`losing_record_parts` line only, and the two clear the same way:

```
buckets_blocked: 3
blocked_bucket: shard=0 hour=100 reason=rewrite_parts below_target=2
blocked_bucket: shard=3 hour=47 reason=loser_only_inputs
blocked_bucket: shard=3 hour=52 reason=losing_record_parts below_target=1
# Re-running migrate does not clear any blocked bucket above; it reports the same list again. ...
# A loser_only_inputs bucket clears only when retention ages those inputs out: ...
# A losing_record_parts bucket's authoritative compaction records are at the target, ...
```

Every other line of the report is `key: value`. The explanatory prose is
prefixed with `# `, so a parser that reads those lines does not take a
sentence fragment for a key.

When `buckets_blocked` is non-zero, a re-run is not the remedy:

| Reason | What clears it |
|---|---|
| `loser_only_inputs` | No command. Retention clears it when it ages those inputs out. |
| `losing_record_parts` | No command. Retention clears it when it ages the bucket out, subject to the format-version hold. |
| `rewrite_parts` | Retention, subject to the format-version hold. A `sweep` clears it when the line lists only a superseded predecessor. |

`buckets_blocked` is the number of `blocked_bucket` lines, and it covers the
buckets THIS INVOCATION EXAMINED:

- The `rewrite_parts` and `losing_record_parts` lines come from the re-audit,
  which reads every shard, so they are complete.
- The loser-only lines come from the walk. An invocation that resumed from a
  cursor does not re-report the loser-only buckets that an earlier invocation
  found.

The walk reports a bucket as blocked only in those cases. The rewrite
primitive also refuses a bucket when a concurrent compaction or erasure lands
between the walk's listing and its own. That refusal is harmless and converges
on a later run. The refusal alone cannot tell the two apart, so the walk
re-reads the bucket after a refusal. It reports the bucket only when a
below-target record is still served raw.

**`loser_only_inputs`.** An L0 input that only a losing compaction record names
is served raw and the walk cannot migrate it. A new record over that subset
joins the same overlap component and loses to the existing winner. In this
build, only retention aging those inputs out clears it. A later authoritative
compaction that covers them would also clear it, but none is ever published:
compaction refuses a bucket that already carries a compaction record, so the
bucket's record set is closed.

**`losing_record_parts`.** The bucket's authoritative compaction records all
have their parts at or above the target. Compaction records that lost their
overlap to them carry `below_target` output parts below it, summed over those
losing records. `loser_only_inputs` is about the raw L0 inputs that only a
losing record names. This reason is about the losing record's own output
parts.

- Nothing serves those parts, but they still count in `l1_compaction_parts`,
  and the floor is still refused over them. A build older than the rule that
  picks one authoritative record per overlap can still serve them.
- Neither a re-run of `migrate` nor `sweep` clears it, because neither
  reclaims a losing record's parts.
- A bucket whose authoritative records are themselves below the target is not
  named this way.
- A bucket whose rewrite record parts are below the target is not named this
  way either. It gets its `rewrite_parts` line only.
- A bucket that lists a rewrite record whose parts are all at the target is
  named this way when its losing records carry below-target parts.

A compaction record that a live rewrite record supersedes counts on neither
side of this test:

- It is never a losing record here, even when it lost its overlap.
- A below-target one that won its overlap does not stop the bucket's losers
  from being named.
- `sweep` deletes it and its parts together with that rewrite. Until then its
  below-target parts count in `l1_compaction_parts` without a `blocked_bucket`
  line.

A `sweep` can therefore change the entry of a bucket that lists a rewrite
record. Once it deletes a winning record that the rewrite superseded, a losing
record that overlapped only that winner becomes authoritative. A bucket whose
authoritative records are below the target is not named this way. Its
`losing_record_parts` line therefore goes away, while those parts still count
in `l1_compaction_parts`.

**`rewrite_parts`.** The bucket holds a live selective-erasure rewrite record,
which is the durable steady state of a bucket that an erasure request touched.
`below_target` of the surviving parts were written at the erasure-time format
version, below the target. `migrate` never rewrites them, for two reasons:

- A migration output published over inputs that a rewrite already covers puts
  two record sets on one bucket. A snapshot that includes both resurrects the
  records that the rewrite dropped.
- An output that re-applies the same drops is an erasure rewrite, with all of
  selective erasure's request-binding obligations. This job does not have
  them.

The parts are therefore counted, the floor is never raised over them, and the
bucket is named.

`below_target` counts the parts of every rewrite record that the bucket still
LISTS, not only its live one. A superseding rewrite does not delete the record
that it supersedes. `sweep` does, on its own schedule, so a superseded
predecessor's parts keep counting until then.

A `rewrite_parts` block clears in one of two ways:

- Retention ages the bucket out, subject to the format-version hold that keeps
  an object that this build cannot read.
- The below-target parts stop being listed:
  1. An erasure request produces a superseding rewrite at the current output
     version. You do not trigger it. The erasure driver does it on its own
     schedule. This step is needed only when the live rewrite is itself still
     below the target.
  2. Run `ravel-cli maintain sweep` for that tenant, signal and shard. It
     deletes a superseded predecessor rewrite record once the superseding
     rewrite is past the protection horizon. The horizon is anchored on the
     superseding record's own `created_unix_ns`, not the predecessor's.

When a bucket is blocked only by a superseded predecessor whose successor is
already at the current output version, a `sweep` alone clears it.

Until the below-target parts are gone, the family's floor stays where it is.
That is the correct outcome and not a fault. A raise would claim a format
floor over objects that still exist below the target, some of which queries
still read. A superseded predecessor is not one of them, but the current
output version's own below-target parts are.

### Re-encoding compaction parts

`migrate --reencode-compaction-parts` converges a bucket whose one compaction
record holds parts below the target. It reads those parts, writes them again
at the current version, and publishes a version 2 compaction record that
supersedes the old one. Re-encoding takes the bucket's claim as a migration
does, and `--no-claim` takes none.

The flag is off by default. Turn it on as a rollout decision, and only when
these conditions hold:

- Every reader and maintainer in the fleet already runs a build that reads
  version 2 compaction records. A build that cannot read one fails every
  resolve of that bucket's records, for queries and maintenance alike.
- The release before the one that you run also reads version 2 compaction
  records, so that a one-release rollback stays safe.

**Once a version 2 record is written, there is no rollback past a build that
reads them.** The record is immutable. Leaving the flag off afterwards only
stops new ones.

The superseded record and its parts stay listed, and keep counting in
`l1_compaction_parts`, until `sweep` deletes them. That takes two sweep
passes:

1. The first pass that finds the superseded record past the protection
   horizon and unnamed by HEAD only writes its unnamed-since marker.
2. A pass at least the pinned-query window later deletes it. The window is
   1 h 20 min 30 s at the defaults. See
   [The pinned-query window](#the-pinned-query-window).

The run that re-encodes a bucket therefore still reports "FOUND STRAGGLERS"
for it. The format floor rises on the first `migrate` run after that second
pass.

The report counts the buckets that the walk did not re-encode and the buckets
whose rewrite published nothing. Each count has one line per bucket and a `# `
line that says what clears them:

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
  `--reencode-compaction-parts`.
- `contested_overlap largest_component=<n>`: an overlap component of the bucket
  holds `<n>` compaction records, so it is not re-encoded.
- `multiple_records records=<n>`: `<n>` compaction records survive
  supersession, each alone in its overlap, and a re-encode rewrites a bucket's
  one record only.

No run clears the last two. Retention clears them when it ages the bucket out,
subject to the format-version hold.

A `not_migrated` line names the path that the bucket was dispatched to
(`l0_migration` or `reencode`) and why its rewrite published nothing:

- `claim_skipped claim_reason=<reason>`: the run could not take the bucket's
  claim, with the same reasons `compact-bucket` prints (`held_by_another`,
  `steal_lost`, `unreadable_claim`, `vanished_twice`).
- `cancelled checkpoint=<point>`: the run took the claim, lost it, and stopped
  at `<point>` (`input_set`, `merge_loop`, `part_boundary` or `publish`) before
  publishing.
- `record_set_changed`: the bucket's records changed before the publish.
- `publish_abandoned`: the run passed its compaction deadline before the
  publish.

When a `not_migrated` bucket is retried depends on how the run ended. The `# `
line under the list says which case applies:

| How the run ended | When the bucket is retried |
|---|---|
| The walk drained and the re-audit found stragglers | The run cleared its cursor, so the next `migrate` run starts over and retries every one. |
| The run stopped on its budget | Its cursor is saved past every bucket that it examined, so the next run resumes after them. The first run after the walk drains starts over from the beginning and retries the bucket. |
| The walk drained and the re-audit raised the floor | Another writer carried the bucket to the target after the walk passed it. Nothing is left to retry. |

The cursor is not held back for a `not_migrated` bucket. If it were, a bucket
that another process holds would stop the walk from reaching the buckets after
it.

The exit code follows how the run ended:

- A run that stops on its budget exits zero, whatever `blocked_bucket`,
  `reencode_blocked` and `not_migrated` lines it printed. A loop that re-runs
  `migrate` until the walk drains is therefore not stopped by a bucket that a
  later run retries or that only retention clears.
- The run that drains the walk exits nonzero while any bucket is left below
  the target. Every `blocked_bucket`, `reencode_blocked` and unresolved
  `not_migrated` bucket still holds parts or records below the target, and the
  fresh re-audit counts them as stragglers.
- The run that drains the walk exits zero when it raises the floor, even with
  `not_migrated` lines printed.
- A run that printed any `unwritable_skipped` line exits nonzero however it
  ended, a budget stop included. See [A log object an erasure rewrite cannot
  rewrite](#a-log-object-an-erasure-rewrite-cannot-rewrite).

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

### Auditing live versions

```sh
ravel-cli maintain audit-versions --tenant <t> [--shards <n>]
```

This command audits live on-object format versions across all three signals.
It reads the supported window from each reader's own source, so a version bump
cannot make the audit stale. It exits nonzero on any anomaly.

Beside each signal's histogram it prints every recorded format floor with its
basis and a classification against the records that it just read:

- `current`.
- `stale`: records newer than the basis, or a wider shard range.
- `contradicted`: a live record below the floor. The command also exits
  nonzero when any floor is `contradicted`.
- `unknown`: the floor records no basis, which is true of every floor raised
  so far.

Each format supports one version and carries no reader for the previous one.
Any live object at another version is therefore an anomaly to re-ingest, not a
migration target:

| Format | Supported version | Anomalies |
|---|---|---|
| Metric segments | v7 | Every other version, including v6. |
| Log segments | v4 | v1, v2 and v3. |
| Span segments | v4 | Every other version. |

### Rolling a format bump

The four steps below apply from the v1.0 release onward, when the reader
window widens to N/N-1. Ravel has not reached v1.0.

**Before v1.0, a format bump is a forward-only data-migration event that you
cannot roll back.** The reader window holds one version. A bump deletes the
version-N reader in the same change that introduces N+1:

- Backward compatibility can break outright.
- No fleet ever reads both versions.
- Objects left at version N become unreadable and cannot be converged.

On a pre-v1.0 build the whole sequence is one step. Deploy the new build
everywhere, and re-ingest or discard whatever was written at version N.

When a release bumps a bulk data-object format from version N to N+1, roll the
fleet in this order and never the reverse:

1. Deploy the release that **reads** N+1 to every process that opens objects,
   which is query, maintenance and the catalog fold. Confirm that it is live
   fleet-wide. Writers must never lead. A process that writes N+1 before its
   peers can read it produces objects that the rest of the fleet fails closed
   on, with a typed unsupported version error and not a silent misread.
2. Only then enable writing N+1, so that compaction and flush emit the new
   version. From this point new and rewritten objects are N+1. Existing N
   objects stay readable for as long as the reader window covers them.
3. Converge the existing N objects. Retention ages them out at no cost, and
   `migrate` rewrites the rest and raises each format floor once a fresh
   re-audit confirms that nothing below N+1 survives. Watch `audit-versions`
   for the remaining below-target population.
4. Delete the reader for the retired version N only once every bucket's
   recorded floor is at or above N+1. The floors that `migrate` raised make
   that a checkable fact. Do it in its own later reviewed change.

## Legal hold

```sh
ravel-cli hold set --tenant <id> --scope <prefix> [--reason <text>]
ravel-cli hold clear --tenant <id> --scope <prefix>
ravel-cli hold list --tenant <id>
```

These commands write and read the audit records that both maintenance drivers
check before any destructive pass. Legal-hold records themselves are
undeletable by every role, Maintain included.

**The hold is not effective the instant the command returns.** After you place
an urgent hold, run `ravel-cli hold list --tenant <id>` and confirm that the
scope is present before you assume that the data is protected.

- `hold set` returning success means only that the record was written, not
  that a maintenance pass has picked it up.
- Each maintenance tick refreshes its hold snapshot once, before its
  destructive pass. A hold set after that tick's refresh is not honored until
  the next one.
- The exposure window is one `--maintain-interval-secs` interval, five minutes
  by default.

### Hold scopes

To hold one shard, use the `--signal` and `--shard` form. One shard's objects
are under three sibling prefixes, `.../l0/<shard>/`, `.../c/<shard>/` and
`.../l1/<shard>/`, and each is checked independently. A hold that names only
one of them covers part of a shard and not the rest. The `--signal` and
`--shard` form writes all three in a single command.

| Scope | `hold set --scope` |
|---|---|
| Reaches into one or two of the three prefixes without covering all of them | Refused. The refusal names the `--signal` and `--shard` form. |
| A whole tenant (`t/<tenant_hex>/`) or a whole signal (`t/<tenant_hex>/<signal>/`) | Accepted. It covers all three prefixes of every shard that it spans, so it cannot be partial. |
| Reaches none of the three, such as one under `maint/` | Accepted. |

`hold clear` accepts any scope, partial ones included. The fold matches a
clear to a set by the exact scope string. A refused partial clear would
therefore leave a hold written before this rule with no way to release it.

### Retention under a hold

Under a hold the physical retention sweep is all-or-nothing. If any key that
it would delete is held, including a commit record or the retention tombstone,
the sweep:

- deletes nothing that pass,
- leaves the tombstone in place,
- counts the bucket,
- reports `SweptPartial`.

It does not delete around the hold. The commit records name the data objects
and the tombstone keeps the bucket excluded. Deleting those while the held
bytes stay would leave bytes that nothing can read and nothing can later
sweep.

The count is `ravel_maintain_retention_held_by_lease_buckets_total` on
`/metrics`, one per bucket per declining pass, summed over every signal.

A held bucket is a bucket kept past its retention window, so the count rises
for as long as the hold stands. That is expected. The count goes flat again
once the hold is cleared and the next pass retires the bucket. A total that
keeps rising after every hold is cleared means that some scope still matches,
and `hold list` shows which.

## Reclaiming a pre-namespacing cache directory

`cache reclaim-legacy` deletes local read-cache files that a binary from
before cache namespacing left behind:

```sh
ravel-cli cache reclaim-legacy --cache-dir <dir>          # dry run: lists only
ravel-cli cache reclaim-legacy --cache-dir <dir> --apply  # deletes
```

It is a local filesystem tool. It takes no `--store` and never touches object
storage.

- **Dry run by default.** With no `--apply` it prints each legacy file that it
  would delete and their total bytes, and deletes nothing.
- **`--apply`** deletes the matching entry files, which are regular files at
  their own canonical legacy path. Then it removes a legacy shard directory
  only when that leaves the directory empty.
- **Never touched**: a current namespace directory, a foreign file (which
  keeps its shard directory in place), a symlink or directory that carries an
  entry-shaped name, and anything outside `--cache-dir`.
- **Safe to run while the node is live.** No live code path reads or writes
  the legacy `<cache-dir>/<shard>` layout, so the delete races no read, write,
  or eviction on the running cache.
- **After a downgrade.** A binary rolled back to the pre-namespacing layout
  reads `<cache-dir>/<shard>` again and finds whatever reclaim left behind.
  After `--apply` it finds nothing there and starts with an empty disk cache.
  That is a cold start, not data loss. The next reads refetch from object
  storage, the same as on any fresh node. The local cache is disposable.

The legacy files exist because the layout changed:

- A node's local read cache writes entry files under a per-instance namespace
  subdirectory, `<cache-dir>/<namespace>/<shard>/<file>`. Two caches over one
  directory therefore cannot evict or miscount each other's files.
- A directory warmed by a binary from before that layout still holds files at
  the rootless layout, `<cache-dir>/<shard>/<file>`.
- Those files are inert. Nothing seeds, evicts, or counts them.
- They occupy disk, up to the old cache budget, on top of the current
  per-namespace budgets. Nothing deletes them implicitly.

## Repairing a forged Parquet table version

A Parquet table's definition is its newest manifest version under
`t/<hash>/pq/t/<table>/v/`, and each `CREATE`, `CREATE OR REPLACE` or `DROP`
writes the next number. No statement writes either of these:

- A version above 4294967296 (2^32).
- A `.pqm` key under a table's `v/` prefix whose slot is not a version number:
  the wrong length, an extra path segment such as
  `t/<hash>/pq/t/<table>/v/q/v/<20 digits>.pqm`, 20 digits too large for a
  `u64`, twenty zeros, or not all digits.

Such a key was put straight into the bucket, for example with a stolen Query
credential, which can create manifest versions.

Ravel skips such a key:

- Queries and DDL use the newest version at or below the bound. The table
  keeps its last legitimate definition and DDL on it still works.
- `parquet ls` and `parquet sweep` skip it too. The sweep neither deletes it
  nor deletes the versions beneath it because of it.
- The first listing in each process that finds one logs a `warn` line. The
  line names the tenant hash, the table, the highest such key's version
  characters and the repair command. `parquet ls` prints a line for each table
  that has one.
- A process tracks at most 4096 such tables. Past that, only one in every 1024
  listings of a further table logs the line.
- Until the flagged keys are removed, every resolve of the table lists all of
  them. Many of them make every query and DDL statement on the table slower.

The bound removes the automatic wedge from versions above 2^32 and from keys
that name no version. A forged version at or below the bound still blocks DDL
or serves as the table until you remove it with `--delete-version` after you
check the audit log. See
[forged version within the bound](#forged-version-within-the-bound). The root
fix is to narrow the Query role's manifest grant so that it cannot put such a
version at all. That fix is not done yet.

To remove the flagged keys:

1. List the table's versions under a credential that can read manifests,
   such as the Query role, to see who wrote each one and with what
   statement. Nothing is deleted without `--delete`:

   ```sh
   ravel-cli parquet repair --tenant acme --table clicks
   ```

   Each key prints escaped, with `stored_unix_ms` (when the store wrote it,
   by the store's clock), `created_by` and `statement`. Versions above the
   bound are marked `FLAGGED: above the version bound`. Keys under the prefix
   whose slot names no version are marked `FLAGGED: names no version`.
2. Rotate the credential that the version was written with. The repair
   removes the version, not the access that wrote it.
3. Delete the flagged versions with the Maintain credential, the only role
   that can delete manifest versions:

   ```sh
   ravel-cli parquet repair --tenant acme --table clicks --delete
   ```

   It deletes only the flagged keys and refuses to delete any version at or
   below the bound. The Maintain role can read manifests, which the sweep
   needs to check a manifest's MAC, so this run also prints `created_by` and
   `statement`. A credential that cannot read them gets them reported as
   unreadable. That does not change which keys it deletes.

### Forged version within the bound

A forged version at or below the bound looks like any other version and is
not flagged:

- As the table's highest version, it is the table's definition.
- One at the bound itself leaves no next version, so every `CREATE OR
  REPLACE` or `DROP` on the table is refused with a 422 that names the version
  bound.

`parquet sweep` deletes a table's superseded versions only when it can
attribute the newest version to Ravel's DDL writer:

- On a keyed bucket, run the sweep with `--tenant-hash-key-file`. It reads
  each table's newest version and deletes the versions beneath it only when
  that version carries a valid MAC under a key derived from the deployment
  key. A forged version has no valid MAC, so the sweep keeps every version
  beneath it and prints the table as held. A manifest written at format
  version 1 carries no MAC either, so a table is held this way until its next
  DDL statement writes a version that does. This build still writes format
  version 1: the release that writes version 2 follows once every reader
  reads it, so until then a keyed sweep holds every table.
- On an unkeyed bucket nothing can be authenticated. The sweep instead keeps
  the versions beneath the newest until the newest is 168 times the grace old
  (a week at `--grace 1h`). Remove a forged version within that window. If
  the sweep has run, restore the noncurrent object versions, if the bucket
  keeps them.

The audit log decides whether a version is forged:

- Every DDL statement that the server runs writes an `attempted` audit record
  before it touches storage (see [the audit guide](../audit.md)). A manifest
  version with no `attempted` record for its table at or shortly before its
  `stored_unix_ms` was therefore not written by Ravel.
- This holds under `--audit-mode required`, the default. Under `best-effort` a
  statement can run without its record, so also check
  `ravel_audit_write_failures_total` for that window.
- A matching record does not prove the version legitimate, because the
  record's text cannot name the version number that it went on to write. Also
  compare its statement with the version's `statement`.

To check and remove a suspect version:

1. List the table under the Query role, as in step 1 above. Note the suspect
   version's number, `stored_unix_ms`, `created_by` and `statement`.
2. As the tenant, read its DDL records around that time:

   ```sql
   SELECT ts_ns,
          attrs['query.status'] AS status,
          attrs['query.text'] AS statement
   FROM audit
   WHERE attrs['kind'] = 'query'
     AND attrs['query.language'] = 'sql'
     AND ts_ns >= TIMESTAMP '2026-10-04T09:00:00'
     AND ts_ns <  TIMESTAMP '2026-10-04T10:00:00'
   ORDER BY ts_ns;
   ```

   No `attempted` row naming the table before the version's
   `stored_unix_ms` means that no statement the server ran wrote it.
3. Rotate the credential, as in step 2 above.
4. Delete that one version with the Maintain credential:

   ```sh
   ravel-cli parquet repair --tenant acme --table clicks --delete-version 4294967296
   ```

   It deletes only that version's key and prints it. Before it touches the
   store, it refuses zero and any version above the bound (those are what
   `--delete` removes). It also refuses a version that is not listed. The next
   statement on the table numbers from the newest version left beneath it.

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
| `parquet repair --tenant <t> --table <name> [--delete \| --delete-version N]` | Lists one Parquet table's manifest version keys and flags those above the version bound and those naming no version; `--delete` removes exactly the flagged ones, and `--delete-version N` removes exactly version N (1 to the bound). See [repairing a forged Parquet table version](#repairing-a-forged-parquet-table-version). |
| `cache reclaim-legacy --cache-dir <dir> [--apply]` | Reclaims pre-namespacing local read-cache files. See [reclaiming a pre-namespacing cache directory](#reclaiming-a-pre-namespacing-cache-directory). Local filesystem only; dry run without `--apply`. |
| `segment inspect <path-or-key>` | Parses one metric segment: trailer, footer fields, section list, decoded series count. |
| `commit decode <key>` | Decodes one commit record: identity, referenced data object key, size and hash, sample and series counts, timestamps. |
| `commit decode-compaction <key>` | Decodes one compaction record: identity, input set hash, each input identity, and each output segment's summary. |
| `commit decode-tombstone <key>` | Decodes one retention tombstone: identity, retirement time, retention window, observed record count. |

`segment inspect` and `commit decode` accept either a local file path or an
object-store key. A path that exists on disk is read directly. Any other path
is fetched from the configured store.

Every command that walks tenant data opens its report with the store that it
resolved. It refuses a walk that reaches no data at all on a defaulted memory
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
