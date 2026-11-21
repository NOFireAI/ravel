# Maintenance (day 2)

Keep a running deployment compact, within retention and readable by current
builds: compact small segments, delete expired and superseded data, verify
the catalog and stored objects, and raise on-object format versions.

**Only a `--mode maintain` process runs maintenance**: compaction, retention,
the sweeper that issues the deletes, and the at-rest integrity scrubber. A
deployment made only of `--mode all` processes never compacts an object and
never deletes one. Its L0 segments accumulate, retention windows have no
effect, and nothing reclaims storage. The quickstart is such a deployment. If
nothing is reclaimed, first make sure that a `maintain` process runs.
[Process modes](configuration.md#process-modes) lists what each mode runs,
and [which processes fold](#which-processes-fold) covers the catalog fold.

| Task | Section |
|---|---|
| Run maintenance continuously | [Running the maintenance loop](#running-the-maintenance-loop) |
| Compact a bucket or a tenant | [Compaction](#compaction) |
| Apply retention and delete superseded data (the sweep) | [Garbage collection and retention](#garbage-collection-and-retention) |
| Fold or verify the catalog | [Catalog fold and verify](#catalog-fold-and-verify) |
| Check stored objects for corruption | [The at-rest integrity scrubber](#the-at-rest-integrity-scrubber) |
| Raise a format version | [Format migration](#format-migration), [Rolling a format bump](#rolling-a-format-bump) |
| Keep data past its retention window | [Legal hold](#legal-hold) |
| Remove legacy cache files | [Reclaiming a pre-namespacing cache directory](#reclaiming-a-pre-namespacing-cache-directory) |
| Repair a forged Parquet table version | [Repairing a forged Parquet table version](#repairing-a-forged-parquet-table-version) |
| Run one pass by hand | [The maintenance and inspection commands](#the-maintenance-and-inspection-commands) |

## Running the maintenance loop

**Continuously.** `ravel-server --mode maintain` runs the loop per tenant, over
all three signals and every shard, on `--maintain-interval-secs` (default 300).
It needs a backend that reports the `multipart` capability. It serves no
ingest or query routes, and it still binds `--listen-http` for liveness.
It also requires `--maintain-key-file`, the keys that sign and verify the
registry records maintain processes divide the work through, and refuses to
start without it; see
[the maintain key file](deployment.md#the-maintain-key-file).

`--maintain-tenant <name>`, repeatable, names a tenant that this process
maintains in addition to every tenant named by `--tenant-token` or
`--tenant-token-file`. A deployment that authenticates through OIDC or mTLS
requires it. Those tenants are known only after a request arrives, and
maintenance has no other way to learn about them.

These flags size how much work one tick does, where the two-level log merge
writes its level-1 runs, and how quickly a stuck unit is visible:

| Flag | Default | What to change it for |
|---|---|---|
| `--maintain-interval-secs` | `300` | How often each tenant's tick runs. `0` is refused at startup. |
| `--maintain-unit-concurrency` | `4` | How many owned units this process maintains at once within one tenant's tick, so one pathological unit cannot starve the rest. Raise it on a process that owns many units and has spare request concurrency to spend, lower it on a host shared with other work. It is clamped to at least 1, and `0` degrades to a sequential walk rather than deadlocking. |
| `--maintain-scratch-dir` | unset | The local directory the two-level RLOG merge writes its level-1 runs under. Set it to compact a log bucket whose cursor merge would decode more than 16 rows per input record and more than 10 million rows in all; without it that bucket is refused and counted under `ravel_maintain_compaction_refused_total{reason="geometry"}`. The directory is a cache: each unit's runs are wiped when the unit ends and nothing durable is read from it, so an `emptyDir` serves. Give each process its own directory, because the first two-level merge locks `ravel-scratch.lock` in it for the life of the process; a second process refuses the bucket under `reason="scratch_locked"`. Size it at about 1.3 times the stored input bytes of the largest bucket it merges: a unit is admitted only when that much is free, and is otherwise refused under `reason="scratch_headroom"`. |
| `--maintain-scratch-max-bytes` | unset | A cap on the bytes the scratch directory may hold, for a directory on a volume larger than its share, such as an `emptyDir` with a size limit on the node's root disk. A unit is admitted against the smaller of the volume's free bytes and this cap less the bytes already under the directory. `0`, or the flag without `--maintain-scratch-dir`, is refused at startup. |
| `--maintain-level1-workers` | derived | How many level-1 groups of one two-level merge run at once, each under its share of the unit's memory ceiling. Omitted, it is derived per bucket from the unit's share of the usable cores, the usable cores divided by `--maintain-unit-concurrency` and at least 1, and the largest count whose groups still fit. `0`, or the flag without `--maintain-scratch-dir`, is refused at startup. |
| `--maintain-level2-workers` | derived | How many level-2 range workers one two-level merge runs, each merging its own contiguous share of the bucket's ranges into its own parts. Omitted, it is derived jointly with `--maintain-level1-workers` from the same core share. No more workers run than the bucket has ranges, and a count whose level-2 read side does not fit the unit's memory ceiling is refused. `0`, or the flag without `--maintain-scratch-dir`, is refused at startup. |
| `--maintain-run-block-records` | `1024` | Records per block of a level-1 run; it changes what level 2 reads and holds per run. Leave it unset outside a measurement. `0`, or the flag without `--maintain-scratch-dir`, is refused at startup. |
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
record published into a bucket that this fold sealed early is picked up by a
later fold only until that fold's own seal margin passes the hour; after that
it is not picked up, because the reconcile window re-lists hours below the
watermark but skips a bucket holding only level-0 commit records.
The repair is the HEAD-deletion rebuild in
[troubleshooting](troubleshooting.md#queries-are-missing-recently-written-data).

The margin exists for writers that still run. After the loader exits, nothing
can publish into those hours. Until the fold covers them, every query pays one
commit-record read per segment.

The command drops only the flush-lifetime term. The clock-skew allowance and
the fold safety margin still apply, a 20 minute margin, so the hour that is
being written is still not sealed. The report's `seal_margin` line shows the
sum that the run used.

To seal that hour as well, assert that every writer has stopped:

```sh
ravel-cli catalog fold --tenant <name> --shards <n> --signal <signal> \
  --writers-stopped
```

`--writers-stopped` seals through the hour the fold runs in and every earlier
one, without moving the fold's clock, and the report's `seal_through_hour` line
names that hour. The same warning applies, to the current hour too. A logs
load can do this itself: `ravel-cli load --fold-after-load` folds once the load
succeeds, sealing through the latest ingest hour it wrote, and prints the
result in its summary. It refuses to start when the logs HEAD has already
sealed the current hour, since every object written into the sealed hour would
then be invisible to queries without a commit token. Seal after compacting, not before: a later fold
in the same hour has nothing left to seal, so it does not pick up compaction
records published into the sealed hours. The watermark advances only once the
natural seal margin passes the next hour, or on another `--writers-stopped`
fold (or `--fold-after-load` load) run in a later hour. See
[Sealed hours](../../catalog-and-mvcc.md#sealed-hours).

### Fold with a retention window

A fold by hand applies a retention window only when the tenant's durable
config record carries one. To give it the deployment default that the
maintain pods run with, pass the same value as their `--retention-default`:

```sh
ravel-cli catalog fold --tenant <name> --shards <n> --signal <signal> \
  --retention-default 30d
```

The tenant's own window still wins when its record sets one. With a window,
the fold's frontier reconcile also re-lists the snapshot-named hours at or
approaching the retention frontier, so it observes retention tombstones in
hours far outside the fixed reconcile window and drops their entries from
HEAD. The report's `frontier_hours_reconciled` line counts those hours.
Without a window, a tenant whose record sets none gets no frontier reconcile.
The flag takes a humantime duration and refuses `0`.

### The fold under the format write policy

On a keyed bucket (`--tenant-hash-key-file`), the server's scheduled and
on-demand folds follow the `sys/compat` write policy. Before a fold cycle
starts, it checks every catalog family it writes: `catalog-head`, `csnap`,
`npost`, `cstat` and `kidx`. A family's active version is a ceiling. The
check is made against the highest version this build's fold of the signal can
write for the family (for `kidx` that is 2 on every signal), and a HEAD
whose content needs less still carries the lower version. A store with no
record folds as it did before the policy.
The cycle is refused and writes nothing when one of these holds:

- a family it writes is active below that highest version
- the record names a version this build does not read
- the process's last successful read of `sys/compat` is 10 minutes old or
  more, or no read has succeeded and the view is not blind: a GET of
  `sys/compat` has returned an object, or the latest failed read was not an
  `AccessDenied`
- the record is missing after an activation

The refusal is logged with the family and counted in
`ravel_compat_publish_refused_total{family}`. Queries keep resolving through
the commit-record listing until a build that writes the active version folds.
`ravel-cli catalog fold` installs no policy and writes what this build
writes.

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
  --shard <n> --hour <n> [--dry-run] [--no-claim] \
  [--scratch-dir <path>] [--scratch-max-bytes <n>] [--level1-workers <n>]

ravel-cli maintain compact-tenant --tenant <t> --signal <metrics|logs|spans> \
  [--shards <n>] [--from-hour <n>] [--to-hour <n>] [--bucket-concurrency <n>] \
  [--dry-run] [--no-claim] \
  [--scratch-dir <path>] [--scratch-max-bytes <n>] [--level1-workers <n>]
```

Reports go to stdout and log lines go to stderr. With `RUST_LOG` unset,
`ravel-cli` logs warnings from every crate plus the compactor's info lines,
such as the RLOG merge's `rlog merge read mode` line. A set `RUST_LOG`
replaces that default entirely, and `RUST_LOG=off` silences it.

Under per-role storage credentials, run both with the Maintain credential, not
Admin. They take compaction claims and write L1 segments and compaction
records, which only the Maintain policy grants (see
[the Admin credential](deployment.md#the-admin-credential)).

`compact-tenant` discovers the hours itself:

- It walks each shard's ingest hours in ascending order and stops at the first
  unsealed one, because every later hour is unsealed too.
- It streams one line per bucket as each completes. Then it prints a summary:
  the compacted, already-compacted, not-sealed, below-minimum, tombstoned,
  claim-skipped, claim-cancelled and held-out-of-window counts, segments
  written, wall time, the failure count and the concurrency it used.
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

### Two-level merge scratch

A logs bucket with many inputs can need the two-level RLOG merge, which writes level-1 runs to a local scratch directory. Both commands take the
same five flags as `ravel-server`'s `--maintain-scratch-dir`,
`--maintain-scratch-max-bytes`, `--maintain-level1-workers`,
`--maintain-level2-workers` and `--maintain-run-block-records`. For sizing, see
[Compaction scratch](configuration.md#compaction-scratch).

| Flag | Meaning |
|---|---|
| `--scratch-dir <path>` | Directory the level-1 runs go under. It is a cache: nothing durable is read from it, and each unit's subdirectory is wiped when the unit ends. The first two-level merge locks `ravel-scratch.lock` in it and removes the unit subdirectories a dead process left; other files are kept and count against `--scratch-max-bytes`. A directory another process holds is refused as `ScratchLocked`; a unit is admitted only when about 1.3 times its input bytes is free, and is otherwise refused as `ScratchHeadroom`. `compact-tenant` merges `--bucket-concurrency` buckets through the one directory at once, so size it there at about 1.3 times the summed input bytes of that many of the largest buckets. Omitted, a bucket whose cursor merge would decode more than 16 rows per input record is refused as `GeometryUnsupported`. |
| `--scratch-max-bytes <n>` | Cap on the bytes the directory may hold. A unit is admitted against the smaller of the volume's free bytes and the cap less the bytes already there. Omitted, the volume's free bytes alone decide. |
| `--level1-workers <n>` | Level-1 groups of one bucket merged at once, each under its share of the unit's memory ceiling. Omitted, it is derived per bucket, jointly with the level-2 count, from the bucket's share of the usable cores. The share is the usable cores divided by `compact-tenant`'s `--bucket-concurrency` (1 for `compact-bucket`), at least 1, so 8 buckets at once on 16 cores derive their workers from 2 cores each. |
| `--level2-workers <n>` | Level-2 range workers of one bucket: the merged stream order is cut into ranges, each closed at the first stream end at which it holds 2,500,000 records (the last may hold fewer), and each worker merges its own contiguous share of them into its own parts. Omitted, it is derived jointly with `--level1-workers`. Never more workers run than the bucket has ranges, and a count whose level-2 read side does not fit the unit's memory ceiling is refused. The count does not change where parts are cut. |
| `--run-block-records <n>` | Records per block of a level-1 run, 1,024 by default. It changes the runs' blocks, and so what level 2 reads and holds per run. |

By default `W1` (level-1 workers) and `W2` (level-2 workers) are derived
together: of the level-1 counts up to the bucket's core share, the one whose
pair scores the smallest `1/W1 + 1/W2` is kept, ties to the smaller `W1`.
Leave both flags unset in normal operation. Set
`--level1-workers` or `--level2-workers` to pin a count for a measurement or
to compare runs, and `--run-block-records` only to measure level 2's read
cost against the run block size.

A zero value, or `--scratch-max-bytes`, `--level1-workers`,
`--level2-workers` or `--run-block-records` without `--scratch-dir`, is
refused before any store request. A bucket that went through the two-level
merge adds a `two_level: level1_groups=<n> level1_workers=<n>
level2_workers=<n> level2_ranges=<n>` line after `outcome: Compacted` in the
`compact-bucket` report; `compact-tenant` appends the same four figures to
that bucket's line. The worker counts are the ones the merge ran with, after
any cap, not the flags' values, and `level2_ranges` is the number of ranges
level 2 cut. A refused bucket's outcome names the reason.

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
  as `claim_expiry_unix_ms`, the earliest useful retry point as
  `retry_after_unix_ms`, and the next time a running maintenance process looks
  at the bucket as `recheck_after_unix_ms`. The bucket is counted in
  `claim_skipped`.
- `outcome=ClaimCancelled`: the bucket lost its claim mid-merge, because
  another process took the claim over or the claim object is gone. The run
  stops without publishing and prints the checkpoint that it stopped at. The
  bucket is counted in `claim_cancelled`. Parts that the run had already
  written stay in the store, content-addressed, for a later run to reuse.

To compact a skipped bucket yourself, rerun at `retry_after_unix_ms`, not at
`claim_expiry_unix_ms`. The retry point tracks the printed expiry only for
`held_by_another`. If the holder finished its merge by then, the rerun reports
the bucket as already compacted. For `unreadable_claim` the two later instants
can differ: `recheck_after_unix_ms` is at most one claim lease plus the jitter
past the observation, and `retry_after_unix_ms` is the 24 h steal threshold.
For every other reason they are equal. A one-off rerun by hand should use
`retry_after_unix_ms`, since a rerun at `recheck_after_unix_ms` against a
still-present unreadable claim skips the bucket again. `version_protected_claim`
is the one exception: it has no retry point at all (see below). The five
reasons are:

- `held_by_another`: a live claim, held by the process id printed. The retry
  point is one millisecond past the printed expiry.
- `steal_lost`: the claim had expired and this run tried to steal it, but
  another contender's steal won the compare-and-swap first. The printed expiry
  is the one already in the past. The winner has just written a fresh lease,
  so the retry point is a full lease from the moment of the loss.
- `unreadable_claim`: the claim object does not decode as a claim of a
  `format_version` this build knows, and it is not yet older than the 24 h
  lease ceiling plus this process's jitter. See below.
- `version_protected_claim`: the claim's `format_version` is 0 (never written
  by any build) or above `CLAIM_FORMAT_VERSION` (written by a newer build).
  Unlike `unreadable_claim` this is never stolen, at any age. See below.
- `vanished_twice`: the claim key disappeared between the create and the read
  that followed it, twice in a row. Nothing is held, so the printed expiry is
  `0` and the retry point is immediate.

An `unreadable_claim` holds the bucket, for compaction and for erasure alike,
for at most 24 hours after the claim object was last written, plus a jitter of
at most 10% of the configured claim lease (30 s at the 300 s default). A lease
configured longer than 24 h counts as 24 h here, so the jitter is at most 10%
of 24 h. The printed expiry is the object's last write plus 24 h, and the
retry point is one millisecond past that expiry plus the jitter. A run at or
after the retry point takes the claim over with a compare-and-swap on the
version it read, logs a warning that names the claim's key, and then compacts
or erases holding the claim. If anything writes the claim object between the
read and the takeover, the takeover fails and the bucket is retried later.
Ravel never deletes a claim object.

A running maintenance process does not wait for the retry point before it looks
again. It re-checks a bucket held by an unreadable claim about once per
configured claim lease plus the jitter. Each re-check costs one rejected
CreateIfAbsent PUT, one GET and one HEAD of the claim key, the same as any
contended claim request. At the 300 s default lease one process re-checks a
stuck bucket at most 288 times a day (24 h / 300 s; the jitter and the extra
millisecond make it slightly fewer), so at most 288 rejected PUTs per stuck
bucket per day for each running process: every maintenance process that holds
the bucket in its schedule pays it. If the key is gone, it claims the bucket
and compacts. If the key is still there and younger than the 24 h ceiling, it
skips the bucket again. Past the ceiling, it takes the claim over as described
above.

A run never continues without the claim. Such a run could publish a compaction
built from data that an erasure of the same bucket already removed. A claim
written by a newer release, in a `format_version` this release does not know,
is version-protected rather than unreadable (see `version_protected_claim`
above): a rollback across such a release holds every bucket that the newer
release claimed until a build that knows the version runs again, not for a
bounded 24 hours.

A claim object that something rewrites more often than once every 24 hours
holds its bucket for as long as the rewrites continue. Find and stop the
writer: only principals with write access to `sys/maintain/` can do this.

A `version_protected_claim` is a different hazard and has no 24 h ceiling, no
retry point, and no automatic end (see Background, retained-data
compatibility). Its
`format_version` is either 0, meaning no build ever wrote this claim in a
format any reader of this protocol recognizes, or above `CLAIM_FORMAT_VERSION`,
meaning a newer build claimed the bucket and this build cannot read enough of
the claim to judge its age or steal it safely. This process logs the hazard
once per claim, at `warn`, naming the claim object's key
(`sys/maintain/claims/compaction/<work_id>`), the first time it observes the
claim; later passes over the same held bucket count in
`ravel_maintain_claims_skipped_total` without repeating the log line. The
bucket's compaction or erasure stays pending until one of:

1. A build that knows the claimed `format_version` runs and reclaims it, or
2. An operator deletes the claim object by hand, the same removal described
   below for `unreadable_claim`, after confirming with
   `ravel-cli maintain` or a direct `GET` on the claim key that its
   `format_version` is genuinely out of this build's known range and not a
   decode failure that `unreadable_claim` would otherwise cover.

To end a hold sooner than 24 hours:

1. Confirm the reason. The holder prints as `unknown`, and the reason stays
   `unreadable_claim` on every retry.
2. Make sure that no compaction, migration or erasure of that bucket runs.
3. Remove that one key by hand. A running maintenance process picks the
   bucket up at its next re-check, within one claim lease plus the jitter.

`--dry-run` and `--no-claim` take no claims. `--no-claim` is for repair work
when a claim is in the way:

- Between two compactions it is safe. The compaction record's create-if-absent
  still decides which output is published. The merge can duplicate one that
  another maintainer runs.
- Avoid `--no-claim` while an erasure request for the tenant is pending.
  Against an erasure of the same bucket the only check left is the re-list
  that each run makes just before it publishes, which leaves a short window.
  A compaction that publishes inside that window leaves the erased sample
  readable, with duplicate rows, and no reader reports it: a query returns
  those rows without an error. Only a rerun of the erasure does, by failing
  on the bucket's two live records.

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

The background supervisor's per-unit tick takes claims. So does the server's
background migrate (`--background-migrate`), on the same clock and lease as
the tick but under its own process id, so each stands down for a bucket the
other holds. The server logs that id at startup, beside its own, in the line
`background migrate takes bucket claims under its own process id`. So do
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
  other causes are a lost steal race, a claim that could not be read, a claim
  at a `format_version` this build does not know (never stolen, at any age;
  see `version_protected_claim` above), or one that vanished twice (something
  outside the protocol is deleting claims). A held bucket adds one per
  maintenance pass for as long as it is held; for every reason but
  `version_protected_claim` that ends when the claim expires, and for
  `version_protected_claim` it does not end on its own at all. The pass that
  observed the claim logs a skip line with the reason. The later passes of the
  same hold count without logging.

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
  everywhere on that process, for the tick and the background migrate alike.
  Use it for a store whose qualification record
  predates the CAS probes, or for an emergency. Two racing compactions still
  converge at the compaction record, and the loser pays its merge first. With
  claims off, a compaction and an erasure rewrite of the same bucket are kept
  apart only by the re-list that each runs just before it publishes, which
  leaves a short window. A compaction or migration that publishes inside it
  leaves the erased sample readable, with duplicate rows, and no reader
  reports it. Only a rerun of the erasure does, by failing on the bucket's
  two live records.
- `--maintain-claim-min-input-bytes <BYTES>` (default 64 MiB): has no effect
  and will be removed. With claims on, every bucket is claimed whatever its
  size, because the claim also keeps a compaction and an erasure rewrite of
  the same bucket from publishing over each other. The flag is still accepted
  so that existing deployments start unchanged. Zero is still refused.

### Commit records that land after the seal

A writer stops issuing commit PUTs for hour `H` at `end(H) +
max_flush_lifetime`, but the store can apply a PUT it already received later
than that. Such a record is a late commit record. Readers serve it as an
extra segment. Without a fence, two maintenance passes over the same bucket
could list different commit records, and a compaction could then publish
next to an erasure rewrite that it only partly overlaps.

The late-commit fence closes that. It is on by default in `ravel-server`'s
maintenance loop and in `ravel-cli maintain compact-bucket`, `compact-tenant`
and `migrate`:

- The cutoff of hour `H` is `C(H) = end(H) + max_flush_lifetime +
  clock_skew_allowance`, 65 minutes past the hour at the defaults, truncated
  to a whole second.
- A compaction, a format migration or a raw-L0 erasure rewrite of the bucket
  plans only from the commit records that the store stamped (`LastModified`)
  before `C(H)`. A record stamped at or after `C(H)` is left out of the plan.
  It is still served, kept by the sweep, held by a legal hold and expired by
  retention, the same as any other record.
- A pass plans the bucket only once its own clock and the store's clock are
  both at least `C(H) + 60 s`. The pass reads the store's clock from the
  `Date` of a response it gets after its planning LIST. Before that the
  bucket reports `NotSealed`, so the first compaction of an hour starts about
  a minute later than it did before the fence.

What you see:

- `ravel_maintain_late_commit_records_total{signal}` counts the late records
  each plan left out, and a warn line names each one. A late record stays in
  its bucket, so every pass that plans the bucket counts it again.
- `ravel_maintain_store_clock_unobserved_total{signal}` counts the plans that
  got no store clock reading. Those plans rely on this process's clock alone.
  The S3 adapter reads the store's clock from every response, so this counter
  should stay at zero against S3.
- `ravel-cli maintain status ... --hour <n>` prints `late_commit_records`
  under `l0_commit_records`.
- `maintain migrate` names a bucket whose remaining below-target commit
  records are late under `not_migrated` with
  `reason=late_commit_records records=<n>`. It migrates the bucket's on-time
  records. The format floor stays down while the late records are stored.

A late record stays raw for the life of its bucket. An erasure request whose
window overlaps it stays blocked while the record is stored, and so does a
format floor raise. Both were already true of a late record that landed
after a bucket's compaction. The fence extends them to a record that lands
between the seal and the first plan.

A late record can also land after an erasure request has completed. No
rewrite erased it, so the erasure-request sweep keeps that request's `.dreq`
past its horizon, and with it the query filter that hides the subject, while
the record is stored and the request's window may overlap its event times. A
request with no window is kept by any late record of its tenant and signal.
The sweep counts each keep in
`ravel_maintain_dreq_held_by_superseded_inputs_total` and logs a warn line
naming the bucket. The keep ends when the record is deleted or retention
tombstones the bucket.

`--late-commit-fence off` plans from every listed commit record as soon as
the bucket seals, which was the behaviour before the fence. Use it for a
bucket whose commit records were all written after it sealed:

- records that `ravel-cli commit reconstruct` rebuilt;
- a store restored or copied by a path that gives every object a new
  `LastModified`.

With the fence on, such a bucket is never compacted, and every pass counts
all of its records as late. Run the affected shard's maintenance once with the
flag, after the writers for those hours are gone, then go back to the
default.

Before you do, check that no completed erasure request's window overlaps the
bucket's late records. The compaction carries their rows into its parts, and
a completed request's rewrite never revisits them. Once the sweep reclaims
the late records, the request's `.dreq` is deleted and those rows are served
again.

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

### An input outside this build's reader window

During a rolling upgrade across a format bump, the newer side can write an L0
object whose format version the older side does not read. When compaction on
the older side reads that object's catalog, the reader refuses its version.
Compaction then reads the whole object and compares its blake3 hash with the
`content_hash` in the object's commit record:

- **The hash matches.** The object is intact data that another build reads.
  Compaction holds the bucket: it writes nothing to it and publishes no
  compaction record, and the bucket's outcome is `HeldOutOfWindow`. The scan
  goes on to later hours, which compact as usual.
- **The hash does not match.** Every reader checks the version before the
  footer checksum, so a flipped version byte reads the same as a newer object.
  Only the content hash tells them apart. A mismatch is corruption, and the
  scan fails with the reader's own error, as for any other undecodable input.

For an RLOG input the refused version can also be a section's own version: a
trailer this build reads whose POSTINGS or KEY_IDX section carries a version
this build cannot read. Compaction treats it the same
way, hash check included, and the hold names the section next to that
section's version.

For metrics, a newer writer also stamps its own version on the commit record,
and compaction refuses a record stamped newer than this build writes before it
loads any input. Compaction then asks that input's reader as well. If the
reader refuses the object's version too, the hash decides as above. If the
reader accepts the object, or the hash does not match, the scan fails with
the refusal of the newer record.

Only a version refusal is checked against the hash. Any other decode error
fails the scan, but a held bucket can hide a corrupt input in the same
bucket. For logs and spans, compaction loads a bucket's inputs in their
listed order and stops at the first error, so an intact out-of-window input
listed before a corrupt one holds the bucket and that pass does not report
the corruption. For metrics, a newer commit record stops compaction before
any other input is loaded, so a corrupt input anywhere in the bucket goes
unread for as long as the hold lasts. Either way the corruption surfaces once
the held input no longer stops the load.

What an operator sees for a held bucket:

- `ravel_maintain_buckets_held_out_of_window_total` moves by one per held
  bucket per pass, summed over every signal. The counter is per process and
  resets to 0 on restart. Alert on an increase, not on a level.
- One `WARN` line per held bucket per pass, `compaction held a bucket`,
  carrying `object_key` (the first refused object's storage key) and
  `version`, with the bucket's signal, shard and hour.
- The unit is not counted as stalled. A held bucket is a completed scan, not a
  failed one.

A held bucket is retried on every pass. The scan's advisory cursor does not
move past it, and the memo never records it as finished. The hold ends in one
of two ways:

- **A build that reads the object runs here.** The bucket compacts on that
  build's first pass, once the upgrade finishes.
- **Retention tombstones the bucket.** Compaction skips a tombstoned bucket
  before it loads any input, so no later pass reads the held object for
  compaction. The retention sweep still refuses to delete the out-of-window
  object (see [The format-version hold](#the-format-version-hold)), so the
  data stays in the bucket. With retention off, only the first way ends the
  hold.

Rolling back the newer side does not clear the hold: the object stays in the
bucket, and only a build that reads it can compact it.

Because the memo never records a held bucket as finished, each pass plans it
again: the bucket listing and its commit-record GETs, the input loads up to
the refusal (for metrics, a footer read of the refused object), and, when
maintenance is coordinated, the claim's acquire, renewal and completion
writes under `sys/maintain/claims/`. On top of that, each pass costs a
whole-object GET and a blake3 hash of the held object. That GET is counted
under the `catalog_read` request phase, and the hash runs on the CPU read gate
under the `compaction` site. The data signals already visit every sealed
bucket of a shard on each pass, skipping the ones the memo records as
finished, so a hold adds no other bucket to that walk.
The query-audit shard's compaction pass resumes from its advisory cursor
instead, and the cursor stops before the first held hour. While a bucket is
held, every pass therefore walks again every later hour of that shard, one
bucket listing each, from the held hour to the newest sealed hour. Nothing
bounds that span except the end of the hold: the query-audit shard is never
tombstoned, so for it the hold ends when a build that reads the object runs
here, or when the query-audit age sweep deletes the held record.

`ravel-cli maintain compact-tenant` prints `outcome=HeldOutOfWindow
version=<n>` for a held bucket, with ` section=<name>` appended when `<n>` is
the version of an RLOG section the reader refused rather than the object's
trailer format version, counts it on the summary's
`held_out_of_window` line, and treats it as a success, not a failure.
`compact-bucket` prints `outcome: HeldOutOfWindow`. Neither moves a server
counter. A bucket held for unknown section kinds (below) prints
`outcome=HeldUnknownSection version=<n> unknown_section_kinds=[..]` and is
counted on the same `held_out_of_window` line.

### An input with section kinds this build does not know

A newer writer can add a section kind to an object whose version this build
still reads. The reader skips a section kind it does not know, so the object
decodes, but a rewrite of it would drop that section.
Compaction, the erasure rewrite and `migrate` therefore refuse such an input
before they write anything for its bucket:

- Compaction holds the bucket with outcome `HeldUnknownSection`, carrying
  the object's version and the unknown kinds, and writes no part and no
  compaction record. The version is the one in the object's trailer, the
  version it was written at, not the one its commit record names.
  `ravel_maintain_buckets_held_unknown_section_total`, rendered by a
  `--mode maintain` server, moves by one per held bucket per pass, apart
  from `ravel_maintain_buckets_held_out_of_window_total`. The server's
  per-scan log lines carry it as `held_unknown_section`, apart from
  `held_out_of_window`, as do `ScanReport` and `MaintainReport` in the
  library. One `WARN` line per held
  bucket names the object and the unknown kinds. The bucket is retried on
  every pass, as an out-of-window input is, and compacts once a build that
  knows the section kinds runs here.
- `migrate` holds the bucket under the `unknown_section` reason, with the
  object's key, its trailer version and the unknown kinds, and raises no
  floor while it holds it. A `--reencode-compaction-parts` run holds such a
  bucket the same way and goes on with the rest of its walk.
- The erasure rewrite fails with an `UnknownSectionKinds` error when it
  reads the input's catalog, before it builds or writes anything.

The check runs with or without a compat policy.

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

The query-audit age sweep applies the same hold to the expired audit records
it would delete. It probes the trailer of every L0 and L1 data object those
records name, and one object outside this build's reader window holds the
whole ingest hour: no record or object in that hour is deleted that pass, and
later hours are still swept. Each held object is counted on the same
`ravel_maintain_retention_held_out_of_window_objects_total` counter. A corrupt
audit object is deleted as before.

Compaction holds a live bucket with such an input too, and checks the
object's content hash to tell an out-of-window object from a corrupt one. See
[An input outside this build's reader
window](#an-input-outside-this-builds-reader-window).

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

`ravel-cli gc-config set` refuses, and writes nothing for, a protection
horizon below either of the two bounds that
[the configuration page](configuration.md#retention-and-garbage-collection-configuration)
gives. Both bounds use the 5 min default skew allowance unless
`--clock-skew-allowance` is given, and the second is checked against this
build's compiled 1 h maximum compaction lifetime. The second bound binds only
a deployment that shortens `max_query_duration` and `grace` far below their
defaults. The maintain process and `maintain sweep` re-check both bounds at
startup against their own values.

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

## Writer versions under `sys/compat`

`sys/compat` records which format version each family is written at.
`ravel-maintain` reads it through a compat policy,
`CompactorConfig::compat_policy`. `ravel-server --mode maintain` installs
one on a keyed bucket (one started with `--tenant-hash-key-file`) and
renders the counters below on `/metrics`. `ravel-cli maintain migrate` and
the other `ravel-cli` maintenance commands install none. With no policy,
every maintenance writer writes exactly the bytes it wrote before the policy
existed, except that an input with unknown section kinds is held (see
[An input with section kinds this build does not know](#an-input-with-section-kinds-this-build-does-not-know)).

Under a policy, each maintenance run takes its write versions once, at its
start, before its claim and before any PUT: compaction, the erasure rewrite,
`migrate` and its re-encode, and the query-audit, reshard-audit and
legal-hold writers. The run keeps those versions to its end, even if the family is
activated again while it runs. A run is refused at its start when:

- the process's last successful read of `sys/compat` is 10 minutes old or
  older, or no read has succeeded, unless the view is blind and the
  bucket's provisioning record carries no `compat_epoch` (`view_stale`).
  The view is blind while no read has succeeded, no GET of `sys/compat` has
  returned an object, and the latest failed read, if any, was an
  `AccessDenied`;
- `sys/compat` is missing while the bucket's provisioning record carries a
  fence (`compat_record_missing`);
- a family's active version is one this build cannot write
  (`active_version_not_writable`);
- the run's publish lifetime is longer than the `max_publish_lifetime`
  recorded in `sys/compat` (`publish_lifetime_above_compat`). The
  audit and legal-hold writers' lifetime is their 30 s write budget,
  and the build refuses to compile if that budget is not inside the default
  1 h.

A refused run writes nothing. It counts once under
`ravel_compat_publish_refused_total{family,reason}`, with the reason in
parentheses above, and logs one `WARN` line, `refusing to publish under the
sys/compat write policy`. The maintenance loop counts a refused compaction
as no run and goes on with the rest of the pass. A refused erasure leaves
its request pending, so a later pass tries it again.

During the run, every PUT attempt, retries included, checks the view's age
first, under the same rule. A view that reaches 10 minutes refuses the PUT
and abandons the run (`view_stale`) once it has seen a record, or when the
bucket's provisioning record carried a `compat_epoch` at the run's start or
the run owes the bucket a fence. A store that was never activated keeps
writing at its baseline. A record PUT is also abandoned once the recorded
`max_publish_lifetime` has passed since the run started
(`publish_lifetime_expired`). Data objects already written by an abandoned
run are unreferenced, and garbage collection removes them as orphans.

When a run writes a version above the family's baseline and the bucket's
provisioning record is fenced below the epoch at which that family was
activated (its `activated_epoch` in `sys/compat`), the run raises the
bucket's fence to that epoch before its first PUT. When a run writes more
than one such family, the fence goes to the highest of their activation
epochs. The fence is one conditional write, not retried. If it fails, the
run is refused (`fence_failed`) and nothing is written.

The `compaction-record` family is a ceiling for compaction and `migrate`,
not a version they write. A compaction record is written at the version
its content needs: version 1 for a plain compaction, version 2 only for a
`--reencode-compaction-parts` run. Compaction and `migrate` are refused
(`active_version_not_writable`) when `compaction-record` is active at a
version this build cannot write. Activating `compaction-record` at 2 does
not make a plain compaction write version 2 records, and it owes the
bucket no fence.

Two more refusals apply to migration:

- `migrate --target-version` refuses a target that is not the family's
  active version (`target_not_active`), before it lists anything.
- `migrate --reencode-compaction-parts` writes version 2 compaction
  records, so it is refused unless the `compaction-record` family is active
  at 2 (`compaction_record_not_active`).

`ravel_maintain::background_migrate_unit` is the background form of
`migrate`: one budgeted walk toward the family's active version, for one
tenant, signal and family. It does nothing without a policy, reads only the
provisioning record once the family's floor has reached the active version,
and reports a refused run as an outcome rather than an error. While
`sys/compat` marks the family's certificate `contradicted`, it walks the
bucket even when the floor has reached the active version: a late write
landed below the floor, so the floor no longer proves the bucket clean. It
walks a contradicted family once per certificate and late-write watermark:
a walk that drains with no bucket held and whose re-audit finds no
straggler records the certificate version, certification time and
watermark it ran under at the
migrate cursor key, and later calls skip the family (a cursor GET, no LIST)
until one of the three changes. A walk that leaves a straggler, such as a
late write in an hour not yet sealed, records nothing, so the next call
walks again. A failed read of that record walks the family.

### Compat maintenance units

A `--mode maintain` process started with `--tenant-hash-key-file` runs one
`sys/compat` maintenance task beside the maintenance loop. The task waits
until the process is in the maintain live set, then ticks once an hour,
starting at once. Each tick reads
`sys/compat` and, for each of `rseg`, `rlog` and `rspan` whose entry
carries a certificate, runs two steps. One live maintain process owns each
family, through the unit key `compat-late-write/<family>`:

1. **The late-write pass**, when the watermark
   (`late_writes_verified_through_unix_ns`) is at or below the
   certificate's certified time, or is a day old. The pass
   (`ravel_maintain::late_write_pass`) lists the commit, compaction and
   rewrite records of every signal of the family, GETs those whose
   `LastModified` is after the watermark minus the clock-skew allowance,
   and on a clean result CAS-advances the watermark to its own start. A
   record below the
   certified version sets `contradicted` on the family entry and logs an
   `ERROR` line naming up to five record keys. The remedy: roll back to
   the release that reads both versions, `migrate`, and re-certify.
2. **Certify finalization**, on an uncontradicted family. A `certify` run
   whose certificate CAS landed but whose format-floor raises did not
   finish reports `FinalizationPending` until a late-write pass that
   started after the certificate has completed. The first tick after that
   pass finishes the raises. With no raises pending, the step is refused as
   already certified and writes nothing. It never starts a census.

The tick logs each step: one `INFO` line per completed pass (records
examined, new watermark) and per finished or pending finalization, and one
`WARN` line per failed step, which the next tick retries. The watermark,
`contradicted` and finalization-pending state of each certified family also
render on the maintain process's `/metrics` as
`ravel_compat_late_write_watermark_seconds`,
`ravel_compat_late_write_watermark_age_seconds` (scrape time minus the
watermark; above 25 hours, passes are not completing),
`ravel_compat_certificate_contradicted` and
`ravel_compat_certify_finalization_pending`
([observability guide](../observability.md#format-write-policy-ravel_compat_)).

With `--background-migrate`, each tick also runs `background_migrate_unit`
for each provisioned metrics, logs and spans signal of every discovered
tenant whose fold unit this process owns, with a budget of 10,000 L0
records per tenant and signal. A walk the budget stops resumes from its
cursor on the next tick. Each walk logs one `INFO` line, or a `WARN` line
naming the held buckets when some were held; a unit already at the active
version logs nothing. The flag is refused outside `--mode maintain` and
without `--tenant-hash-key-file`. It is off by default.

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
   The raised floor records that re-audit as its basis: the live entries it
   counted (live L0 commit records plus every compaction and rewrite part),
   the newest creation time among its records, and the shard range it
   scanned.

Some buckets cannot be carried forward by this build. The walk holds such a
bucket: it writes nothing there, logs a warning, and goes on to the next
bucket. Each hold has one of three reasons:

- `input_out_of_window`: an input data object's format version, or the
  version of an RLOG section it carries, is outside this build's reader
  window. A part that `--reencode-compaction-parts` would re-encode is held
  under this reason only when its bytes match the content hash its record
  carries.
- `newer_than_writable`: a metrics input's commit record, or, for any
  signal, a part of the compaction record that `--reencode-compaction-parts`
  would re-encode, is recorded at a format version above the one this build
  writes.
- `corrupt_input`: an input data object fails to decode, or a part that
  `--reencode-compaction-parts` would re-encode fails its content hash
  check. The re-encode checks the hash of every metrics part, and of a logs
  or spans part when the reader refuses its version.

A run that held any bucket does not re-audit and does not raise the floor,
whatever the rest of the walk found. It prints its report, then one line per
held bucket, and exits nonzero:

```text
held: shard 0 hour 100 reason=newer_than_writable record_key="t/.../l1.d060aa3a8525ee29.cmt" version=6
```

The detail after the reason is `object_key=` and `version=` for
`input_out_of_window`, followed by `section=<name>` when the version is that
RLOG section's and not the object's trailer format version, `record_key=` and
`version=` for
`newer_than_writable`, and `detail=` for `corrupt_input`.

Every listing the walk and its re-audit plan from is strict, including the
per-bucket listing each L0 migration and each force 2 re-encode plans from. A
key under a listed prefix that the census cannot address fails the run with an
`UnaddressableKey` error naming the prefix, the first such key and how many
there are. Nothing is certified: the walk stops and no floor is raised. The one
exception is the pre-publish re-list a rewrite makes after its last claim
checkpoint: that listing is shared with compaction and the erasure rewrite,
both of which plan from non-strict listings, and it stays non-strict to match
them. It is not the listing a migration decision rests on, only a last check
that the record set did not change under the pass, so a short re-list cannot
certify a floor. An unaddressable key that lands after the strict planning
listing is caught by the walk's strict re-audit, which runs before any floor
is raised.

A cursor stored under a tag this build does not know, such as one written by
a newer build, is left in place. The run walks from the beginning, writes no
cursor at a budget stop, deletes none when the walk drains, and logs one
warning.

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

A run that held a bucket also refuses the raise, but it does not reach the
re-audit and prints no verification line. Its `held:` lines name each bucket
and reason (see [format migration](#format-migration)). Re-running does not
clear an `input_out_of_window` or `newer_than_writable` hold under the same
build: finish the upgrade or roll forward to a build that reads and writes
the version. A `corrupt_input` hold names a damaged object. A run that failed
with `UnaddressableKey` certified nothing either, and a re-run fails the same
way while the key it names is still listed.

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
- `part_index_refusal refusal=<kind>`: the bucket's RLOG merge was refused
  because its parts do not fit the part index (`part_key_space` or
  `part_range_window`), so no record was published. A `part_range_window`
  refusal can leave parts that the same merge had already written, named by
  no record, as compaction does on the same refusal. The run
  completes the bucket's claim and the walk goes on to later buckets. The
  bucket's L0 inputs stay live, so the floor does not rise past it, and a
  retry with the same inputs is refused the same way. Re-running `migrate`
  is not the remedy: retention clears the bucket when it ages the bucket out,
  subject to the format-version hold. Unless the re-audit raised the floor, a
  `#` line under the list says so, and the straggler error names these
  buckets too.

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
  by v0.23.0 or earlier, before `migrate` began recording one.

A floor that `migrate` just raised is `current` until a record newer than its
basis lands or the shard range grows. A `migrate` run at the version a floor
already holds appends no floor entry (it still rewrites any bucket below the
target and saves or clears its resume cursor), so an `unknown` floor stays `unknown` until a
later run raises that family to a higher version.

Each format supports one version and carries no reader for the previous one.
Any live object at another version is therefore an anomaly to re-ingest, not a
migration target:

| Format | Supported version | Anomalies |
|---|---|---|
| Metric segments | v7 | Every other version, including v6. |
| Log segments | v5 | Every other version, including v4. |
| Span segments | v5 | Every other version, including v4. |

### Rolling a format bump

A new format version is activated, never just deployed. Installing a build
that can write version N+1 of a family changes nothing it writes: every
writer takes its version from `sys/compat`, and the family stays at N until
you activate N+1. The procedure is activate, migrate, certify, retire, in
that order. The [compatibility contract](../../compatibility.md) states which
releases may run beside each other, which families follow which regime, and
which upgrade and downgrade paths are supported.

Two things run the procedure. Five `ravel-cli compat` subcommands make the
operator's steps: `init` adopts the store, `activate` raises a family's
version, `certify` writes the certificate a reader-retiring release needs,
`verify` runs the late-write pass on demand, and `status` prints
`sys/compat`. A `ravel-server --mode maintain` process does the rest: the
background migration, the daily late-write pass and certify finalization
(see [Compat maintenance units](#compat-maintenance-units)). No `compat`
subcommand migrates anything.

Every `compat` subcommand takes the global `--tenant-hash-key-file`, given
before the subcommand (`ravel-cli --tenant-hash-key-file <key> compat
status`), next to the usual store flags. `sys/compat` is MAC-signed under
that deployment key, so without it every subcommand refuses. Each one runs
under the Admin credential, the `ravel-cli` default. Every subcommand takes
`--json`, which prints its report as one JSON object; a refusal prints
`{"outcome": "refused", "reason": ...}`. They share these exit codes:

| Exit | Meaning |
|---|---|
| 0 | The command ran and nothing refused it. |
| 1 | An error, not a refusal: a store, listing or GET fault, a `sys/compat` MAC or decode fault, an unreadable key file, or an `activate` whose 8 conditional-write attempts all lost a race. The message goes to stderr as `Error: ...` and `--json` prints nothing. Rerun once the cause is fixed. |
| 2 | A command-line usage error (a missing `--family`, an unknown flag), from the argument parser before the command runs. No refusal exits 2. |
| 3 | Contradicted: `certify` found a member below the target version, or `verify` found a late write or a family whose certificate is `contradicted`. |
| 4 | Pending: resumable work is unfinished. `activate`'s fence walk stopped, or `certify` stopped on its budget or has a finalization waiting for a late-write pass. Rerun the same command. |
| 5 | Refused: a precondition the operator must fix, and `sys/compat` was not changed. The reason is printed. |

On this release only `compaction-record` has a writable version above its
baseline (2, for `--reencode-compaction-parts`); `rseg`, `rlog` and `rspan`
each write one version, so `compat activate` refuses each of them with
`NotWritable` (exit 5).

The procedure, for family F from version N to N+1:

1. **Grant the read, before anything else.** Every role that runs a
   `ravel-server` mode needs `s3:GetObject` on `sys/compat`. The shipped
   roles in `deploy/iam/` grant it to the gateway, query and maintain
   roles. A process whose role lacks it never sees an activation: on a
   fenced bucket its writers refuse as stale, and the audit and legal-hold
   writers, which write to no provisioned bucket, keep writing version N.
   The Admin and maintain roles also grant `s3:PutObject` on
   `sys/compat`, because both write the record. The Admin role runs
   `compat init`, `activate`, `certify`, `verify` and `status`; it also
   writes the provisioning records the fence walk raises and puts and
   deletes the certify cursor under `sys/maintain/certify/`. An `rlog`
   activation's walk also writes each tenant's `t/<tenant>/a/alert-lease`,
   which the shipped Admin role grants. The maintain role runs the
   late-write pass, certify finalization and the background migration.
   Certify reads the provisioning records of every signal of the family,
   including `t/<tenant>/a/prov` and `t/<tenant>/u/prov`, which never
   exist, and the cursor under `sys/maintain/certify/`, which is absent
   once a run finishes. S3 answers a GET of an absent key with
   `AccessDenied`, not `NotFound`, unless the role may list that prefix, so
   the shipped maintain role grants `s3:ListBucket` on
   `t/<tenant>/a/prov`, `t/<tenant>/u/prov` and `sys/maintain/certify/*`
   beside its reads. The
   credential each `ravel-cli` command takes, and the `sys/compat` and
   `sys/maintain/certify/*` rows, are in
   [the IAM templates' README](../../../deploy/iam/README.md), sections
   "Which credential each `ravel-cli` command takes" and "Control-plane
   keys outside the data path". Every process also needs
   `--tenant-hash-key-file`: on an unkeyed bucket a process builds no view
   of `sys/compat` and every writer emits its built-in version. Step 6's
   `--writer-roles-can-read-compat` flag is the attestation that the read
   grant holds, and it must hold at every activation.
2. **Roll out the baseline release** with a plain rolling update. No
   activation is needed, and v0.33.x processes may run beside it until step
   5.
3. **Adopt the store**, once:

   ```sh
   ravel-cli --tenant-hash-key-file <key> compat init \
     --baseline-major 0 --baseline-minor 34 --max-publish-lifetime 1h --json
   ```

   `--baseline-major` and `--baseline-minor` name the baseline release
   recorded in `sys/compat` (defaults 0 and 34), and
   `--max-publish-lifetime` is the publish lifetime `P` (default `1h`).
   The command runs a census of every tenant's config, provisioning
   records, catalog objects, bucket records and data trailers, and on a
   clean result creates `sys/compat` at this build's baseline versions
   with `compat_epoch` 0, `P`, and the version histogram it found. It
   lists only `t/<tenant>/`, so objects under `quarantine/` are not
   checked. An identical existing record is a no-op.

   It exits 0 with `outcome` `created`, or `already-initialized` for the
   no-op. The JSON report carries `families` (each `family` with its
   `active_write_version`), `adopted` (`family`, `version`, `count` for
   every version the census found) and `census` (`tenants`,
   `objects_checked`, `objects_ignored`, `objects_vanished`; `null` when
   the record already existed). Check `adopted` against what you expect the
   store to hold. It exits 5, writing nothing, on these refusals:
   - `Unkeyed`: no `--tenant-hash-key-file`;
   - `InvalidPublishLifetime`: `--max-publish-lifetime` is not positive;
   - `NoBaselineVersion`: this build has no single baseline version for a
     family;
   - `Unreadable`, `UnknownBucketEntry` or `NoFamilyForSignal`: an object
     the census found cannot be read or classified;
   - `OutsideReadSet`: objects at a version this build does not read. The
     census groups them by tenant, signal, family and version, and the
     message gives the number of groups and the first group in full (its
     tenant, signal, family, version, object count and one sample key). Re-ingest or discard them; the
     retention version hold keeps them meanwhile;
   - `ProvisioningFenced`: a provisioning record already carries a
     `compat_epoch`, so `sys/compat` existed and was lost. Restore it from
     bucket versioning instead;
   - `RecordDiffers`: `sys/compat` exists and differs from the record
     `init` would write.

   A failed listing or GET, a key under a census prefix that cannot be
   addressed, or an entry under `t/` that is not a tenant, exits 1; rerun
   once the store is reachable, or remove the stray entry.
4. **Roll out the release that introduces N+1** everywhere, with a plain
   rolling update. It writes N until step 6, so rolling back to the
   baseline is still safe.
5. **Terminate every pre-baseline process.** Before the first activation
   above the baseline, stop every v0.33.x process that can reach the
   bucket, including pods outside the operator's Deployments and any
   `ravel-cli` run. Confirm it with the deployment's own inventory of
   everything holding the bucket's credentials: the operator's Deployment
   rollout status is necessary, not sufficient. The ravel-operator does
   not perform or verify this step.
   - What a missed process can do: write audit records at the older RLOG
     version, which the late-write pass finds later, marking the
     certificate contradicted and so blocking retirement; land a data
     object from its teardown flush, which the certify census and the
     late-write pass flag; and write metadata, idempotency markers,
     `t/<tenant>/enc` and `t/<tenant>/config` with no fence at all.
   - **What a missed process can do that nothing detects:** its maintain
     loop deletes alert history and audit objects by age with no version
     hold. No fence and no pass sees a deletion; this step is the only
     control over it.
   - What it cannot do: make a reader return a wrong answer from a data
     object it writes. Every data object it writes is at a version the
     baseline reads, and once the older reader is retired such an object
     fails closed with a typed error. Its unfenced rewrites of
     `t/<tenant>/config`, `t/<tenant>/m/meta` and `t/<tenant>/enc`, and
     its deletion of alert history, which changes the alert state a
     rebuild produces, are not covered by this.

   Step 6's `--pre-baseline-processes-terminated` flag is the attestation
   that this step is done.
6. **Activate N+1**:

   ```sh
   ravel-cli --tenant-hash-key-file <key> compat activate \
     --family <family> --version <N+1> \
     --pre-baseline-processes-terminated \
     --writer-roles-can-read-compat \
     --activated-by <operator> --clock-skew-allowance 1m --json
   ```

   Both attestation flags are required, and without either one the command
   exits 5 before reading anything; with neither, the refusal names both.
   `--pre-baseline-processes-terminated` attests step 5. The flag does not
   close the residual the contract's decision record names for step 5 (see
   [Residuals](../../compatibility.md#residuals)): "The irreversible
   residual is a pre-baseline maintain process deleting alert history,
   and audit objects, by age with no version hold after an activation; no
   fence and no pass detects a deletion, so the prerequisite is its only
   control."
   `--writer-roles-can-read-compat` attests step 1's read grant for every
   writer role; `--pre-baseline-processes-terminated` does not imply it.
   The CLI cannot check the grant, so the flag leaves a residual: a writer
   role not granted read on `sys/compat` keeps writing the baseline
   version to unprovisioned paths and to buckets it first touches after
   the activation. The record's `activated_by` keeps both attestations as
   `<operator>; attests: pre-baseline-processes-terminated,
   writer-roles-can-read-compat`, where `<operator>` is `--activated-by`
   (default `operator`); `sys/compat` has no other field for them. For `rlog`, give
   `--alert-rule-tenant <tenant-hash-hex>` once for every tenant the alert
   rules file names, so its lease is parked even before it holds an alert
   key.

   The command raises the family's active version and the store-wide
   `compat_epoch` together in one conditional write, making up to 8
   attempts in all (7 retries) when it loses the race or the store returns
   a retryable error. Then it walks every tenant: it fences each
   provisioning record at the family's activation epoch and, for `rlog`,
   parks every discoverable tenant's alert lease. It records
   `fence_complete_unix_ns` only after a walk with no failure. Exit codes:
   - 0, `fence_walk.complete` `true`: done. `outcome` is `activated`, or
     `resumed-pending-walk` when this run finished a walk an earlier run
     left pending.
   - 4, `fence_walk.complete` `false`: the version is active but the walk
     stopped; `fence_walk.failure` says where. Fix the cause and rerun the
     same command. A family whose walk is pending is resumed, not
     activated again (`outcome` `resumed-pending-walk`), and the rerun
     walks every tenant, counting the finished ones under
     `buckets_already_fenced` and `leases_already_parked`. Do not
     certify until a run exits 0: certify refuses `FenceWalkIncomplete`.
   - 5, refused before any write: `Unkeyed` (no `--tenant-hash-key-file`),
     a missing `--pre-baseline-processes-terminated` (the reason names the
     flag and its residual), `WriterRolesCannotReadCompat` (a missing
     `--writer-roles-can-read-compat`; the reason names the flag and its
     residual), `RecordAbsent` (run step 3 first),
     `FamilyNotInRecord` (`sys/compat` has no entry for the family),
     `NotWritable` (this build does not write the requested version), or
     `NotAboveActive` or `VersionJump` (the version is not exactly one
     above the active version; no step lowers a version).
   - 1: a store or `sys/compat` fault, or `Contended`: all 8 attempts
     lost the conditional write or met a retryable store error. Rerun.

   The JSON report carries `family`, `active_write_version`,
   `stamp_ahead_of_clock_ns` and `fence_walk` (`tenants`,
   `buckets_raised`, `buckets_already_fenced`, `buckets_unprovisioned`,
   `leases_created`, `leases_replaced`, `leases_already_parked`,
   `complete`, `fence_complete_unix_ns`, `failure`). A non-null
   `stamp_ahead_of_clock_ns` means the activation was stamped further past
   this host's clock than `--clock-skew-allowance`, because the process
   that stamped the family's previous `fence_complete_unix_ns` ran on a
   clock that far ahead. Find that clock before certifying.

   Writers pick the activation up on their next `sys/compat` read, within
   60 seconds, and a writer whose last successful read is 10 minutes old
   refuses to publish. From the activation on, a build that cannot write
   N+1 refuses the family's publishes, so rolling back to it stops those
   writes; once an N+1 object exists, that rollback is a downgrade. See
   [Rollback versus downgrade](../../compatibility.md#rollback-versus-downgrade).
7. **Migrate** the objects still at N. Retention ages some out at no cost.
   Start the migration by restarting the maintain processes with the flag
   added, under the maintain credential:

   ```sh
   ravel-server --mode maintain --tenant-hash-key-file <key> --background-migrate ...
   ```

   The flag is off by default, has no environment variable, and the
   process refuses to start with it outside `--mode maintain` or without
   `--tenant-hash-key-file`. Each hourly tick of the compat maintenance
   task, the first as soon as the process joins the maintain live set,
   walks every owned tenant's provisioned metrics, logs and spans signals
   toward the active version under the policy, 10,000 L0 records per
   tenant and signal per tick, resuming from a cursor (see
   [Compat maintenance units](#compat-maintenance-units)).
   Watch the maintain process's log:
   - `INFO` `background migrate: walked toward the active version`, with
     `tenant`, `signal`, `family`, `active_version`, `buckets_examined`,
     `buckets_migrated` and `records_migrated`: progress. A unit already
     at the active version logs nothing, so the lines stop once every
     unit is done.
   - `WARN` `background migrate: walked with held buckets; the floor stays
     unraised`, with `holds`: a held bucket blocks certification. The hold
     reasons are under [Format migration](#format-migration).
   - `WARN` `background migrate: refused by the compat policy`, with
     `reason`.
   - `WARN` `background migrate: unit failed; retried next tick` and
     `background migrate: no unit ran this tick; retried next tick`.

   Confirm the result per tenant with `ravel-cli maintain audit-versions
   --tenant <t>` (see [Auditing live versions](#auditing-live-versions)).
   Alternatively run `ravel-cli maintain migrate` under the Maintain
   credential; see [Format migration](#format-migration) for its holds and
   exit codes. That command installs no `sys/compat` policy and writes this
   build's current version.
8. **Certify**, for `rseg`, `rlog` or `rspan` only, once migration is
   done:

   ```sh
   ravel-cli --tenant-hash-key-file <key> compat certify \
     --family <family> --version <N+1> \
     --max-flush-lifetime 1h --t-req 30s --clock-skew-allowance 1m \
     [--budget-records <units>] --json
   ```

   It refuses to start its census before the activation plus
   `H + P + T_req` and before the newest fence time plus `G_pre`, each
   plus the clock-skew allowance; `H` is 10 minutes and `G_pre` is
   `3 h + 2 × max_flush_lifetime + P + T_req`. The newest fence time is
   taken over the provisioned buckets of the family's own signals only
   (metrics for `rseg`, logs for `rlog`, spans for `rspan`; alerts and
   audit are never provisioned), and a bucket born fenced, whose fence
   time is 0, is left out.
   `--max-flush-lifetime` (default `1h`) is raised to the value `sys/gc`
   records when that is larger, and `--t-req` (default `30s`) is the store
   request timeout. With the defaults and `P` of 1 hour, `G_pre` is
   6 h 0 m 30 s, so the earliest certify is 6 h 1 m 30 s after the last
   bucket was fenced. The census reads every live member object's trailer
   and stops, writing no certificate, on any member below N+1, any member
   it cannot read, any trailer that disagrees with its record's stamp, and
   any incomplete listing. `--budget-records` caps the
   `(tenant, signal, shard)` units one run covers (unset is no cap); a run
   saves its progress to the cursor `sys/maintain/certify/<family>/cursor`
   when the budget stops it and when its census finishes, and the next run
   resumes from there. The census also fences, at the activation epoch,
   every provisioned bucket it reaches that is still unfenced, such as one
   a blind or lagging gateway provisioned after the activation's walk, so
   certify can write provisioning records. A bucket it fences that way has
   a fence time of now, and the recheck after the census then refuses
   with `BucketRecentlyFenced` until that time plus `G_pre` and the
   clock-skew allowance, about 6 hours with the defaults; rerun then. On a
   clean census it writes the
   certificate, sets the late-write watermark to the census start, raises
   the format floors, and deletes the cursor. Exit codes, by `outcome`:
   - 0, `certified`: the certificate is written. The report carries
     `version`, `member_count`, `buckets_scanned` and
     `newest_created_unix_ns`. Go to step 9.
   - 3, `contradicted`: `member_key` is a live member at `found_version`,
     below N+1. Migrate it (step 7) and certify again.
   - 5, `held-member`: `member_key` is at a `version` this build cannot
     read. Re-ingest or discard it.
   - 4, `incomplete`: `--budget-records` stopped the run after
     `units_examined` units. Rerun the same command.
   - 4, `finalization-pending`: an earlier run wrote the certificate
     (`version`, `certified_unix_ns`) but did not finish raising the
     floors, and no late-write pass has run since (`watermark_unix_ns`).
     The maintain process finishes the raises on its first tick after that
     pass (step 9); a rerun after that pass finishes them too and exits 0.
   - 5, refused: the reason is printed. The ones an operator meets are the
     time gate (`now` and `not_before`, in Unix nanoseconds; rerun after
     `not_before`), a bucket fenced too recently for its `G_pre`,
     `FenceWalkIncomplete` (rerun `compat activate`), a bucket fenced below
     the activation epoch, a tenant set that changed during the census, an
     incomplete shard listing, a member absent, corrupt or whose trailer
     disagrees with its stamp, `sys/compat` changed or replaced during the
     run (rerun), `ConcurrentCertify` (another certify run created or
     replaced the certify cursor first, so this run's conditional write on
     it failed; nothing holds the cursor, and a rerun resumes from whatever
     cursor is there), a
     family that is not class A, and a family already certified at or
     above the target.
   - 1: a store or decode fault, or an entry under `t/` that is not a
     tenant or that certify cannot address (remove it). Rerun; units examined since the last
     saved cursor are examined again, so a large store is cheaper to
     certify in `--budget-records` steps.
9. **Verify late writes.** A `--mode maintain` process started with
   `--tenant-hash-key-file` runs the late-write pass for every certified
   family under the maintain credential: on its first tick after the
   certificate, and then once a day. It needs no `--background-migrate`.
   See [Compat maintenance units](#compat-maintenance-units) for the pass,
   its log lines and its gauges. To run the pass now, or to read the
   state:

   ```sh
   ravel-cli --tenant-hash-key-file <key> compat verify [--family <family>] \
     --clock-skew-allowance 1m --json
   ravel-cli --tenant-hash-key-file <key> compat status --json
   ```

   `verify` runs the pass for `--family`, or for every class-A family that
   carries a certificate; a `--family` that is not `rseg`, `rlog` or
   `rspan` runs no pass. It exits 3 when a pass found a late write or any
   family is `contradicted`, and 0 otherwise. A pass whose certificate was
   replaced while it ran (`certificate_replaced` `true`) marks nothing
   `contradicted`, so a late write it found exits 3 with that family's
   `contradicted` still `false`; rerun `verify` against the new
   certificate. Its JSON report adds `late_write_passes`, one per family
   the pass ran for (`family`, `examined`, `certified`,
   `certificate_replaced`, `watermark_unix_ns`, and `late_writes`, each a
   `record_key` and `version`). `status` runs no pass and exits 0. With
   `--json` both print, per family, `active_write_version`,
   `activated_epoch`, `activated_unix_ns`, `activated_by`,
   `fence_complete_unix_ns`, `contradicted`, `watermark_unix_ns`,
   `finalization_pending` and `certificate` (`version`,
   `certified_unix_ns`, `member_count`, `newest_created_unix_ns`,
   `buckets_scanned`, or `null`), beside the record's
   `baseline_release_major`, `baseline_release_minor`, `compat_epoch` and
   `max_publish_lifetime_ns`. The text output, without `--json`, omits
   `activated_unix_ns`, `activated_by`, `newest_created_unix_ns` and
   `late_write_passes`. A store with no `sys/compat` prints
   `outcome` `absent` and exits 0; an unkeyed run exits 5, and a fault
   exits 1. `finalization_pending` is `true` exactly when the family
   carries a certificate, is not `contradicted`, and its watermark is at or
   below `certificate.certified_unix_ns`: from the certify run, which sets
   the watermark to its census start, until a late-write pass moves it
   past the certified time. It is `false` for a contradicted certificate.
   The maintain process's `ravel_compat_certify_finalization_pending`
   gauge and the watermark refusal of the
   [retired-reader startup gate](#retired-reader-startup-gate) use the
   same predicate, so while it is `true` a release that retires the
   family's older reader refuses to start.

   A late write below N+1 marks the certificate contradicted: roll back to
   the release that reads both versions, migrate, and certify again.
10. **Retire** the N reader in a later release, once `compat status`
    shows the family with `contradicted` `false`, `certificate.version` at
    or above N+1 and `watermark_unix_ns` greater than
    `certificate.certified_unix_ns`. That release starts only on that
    state, and refuses a store that has `sys/tenancy` but no `sys/compat`
    (see [Retired-reader startup gate](#retired-reader-startup-gate)).
    Rolling back from it to the release that reads both versions is safe.

### A writer below a recorded format floor

A recorded floor says that a bucket holds no live object of its family below
the floor's version. A writer keeps that true. It refuses to publish an object
below the floor of the bucket it writes into. The usual cause is a
rollback: `migrate` raised a floor to N+1, and a build that writes N is
running again.

What you see:

- At startup, each ingest (`all`, `gateway`) and `maintain` process reads the
  provisioning record of every tenant named by `--tenant-token`,
  `--tenant-token-file` and `--maintain-tenant`, for metrics, logs and spans.
  That is one GET per tenant and signal. It logs one warning per tenant,
  signal and family whose floor is above the version this build writes, with
  both numbers, and sets `ravel_format_floor_above_writer{family}` to the
  count. The process starts either way. Queries, the fold and retention run
  as normal, and so does the retention version hold. A tenant that is not
  named by a flag is not checked at startup. Its writes are still checked.
- On ingest, the check runs when a write is admitted, before it is
  buffered. It uses the provisioning record the router already caches for
  routing, so it adds no store read. A refused write, strict or buffered, is
  answered with a retryable error naming the family, the floor and the
  version (HTTP 503), and nothing of it is buffered. A floor raised while a
  writer runs takes effect once the writer reads the record again. A write
  does that before it routes when the router's copy is more than 60 seconds
  old. Rows buffered before that re-read are checked again when their flush
  opens: they are dropped and counted, the flush makes no PUT, and a strict
  write waiting on them gets the same error. A buffered write among them was
  already acknowledged, so those rows are lost.
- A provisioning record whose floor history fails validation, in any family,
  fails the ingest router's read of that record the way a corrupt generation
  history does. Writes for that tenant and signal are then refused with
  `StaleProvisioningView` (HTTP 503): at once for a tenant the router has no
  cached view of, and once the cached view's grace window ends otherwise. The
  same holds for a record the store cannot serve at all, such as one whose
  stored checksum no longer matches its body. After a failed read the router
  does not read that tenant's record again for 1 second, then 2, 4 and so on,
  doubling on each consecutive failure up to 30 seconds; a successful read
  resets it. Failures of reads already in flight when a window was armed do
  not extend it, so a burst of writes that all fail once waits 1 second. A write inside that window is refused, or routed on the cached
  view inside its grace window, without a store read. Each failed read logs
  one ERROR line naming the tenant hash, the signal, the cause and the delay
  before the next read, and counts once under
  `ravel_ingest_provisioning_read_failures_total{mode,signal,cause}`, where
  `cause` is `store` (the GET failed) or `decode` (the record did not decode
  or failed validation). That counter counts reads;
  `ravel_ingest_stale_provisioning_flushes_total` counts the refused writes,
  which outnumber the reads. The first-write provisioning check on every
  ingest request treats a store error as a pass, logs a WARN, and backs off
  the same way, so the request is answered by the router's decision. A
  record it reads but cannot accept (undecodable, an unsupported version,
  misfiled, a corrupt generation history) fails the request, and requests
  inside the same backoff window fail with the same error without reading
  the record again. A
  `maintain` compaction or erasure rewrite of the tenant's buckets fails its
  floor read and writes nothing.
- In a `maintain` process, a compaction or erasure rewrite reads the bucket's
  provisioning record before it claims the bucket or writes anything. That is
  one GET per run. A refused run builds and publishes nothing, and the
  maintenance loop moves on to the next bucket. An erasure request whose
  rewrite is refused stays pending, so its deletion waits too.
  `ravel-cli maintain migrate` and the `ravel-cli` compaction commands check
  each bucket the same way; `migrate` stops at the first refused one.
- Every refusal logs a warning. An ingest or `maintain` refusal counts under
  `ravel_format_floor_refused_total{family}`. `ravel-cli` renders no
  `/metrics`, so its refusal is the command's error, naming the family, the
  floor and the version, and a nonzero exit.

What to do. Pick one of two ways out:

1. Roll forward to a build that writes at or above the floor. The floor
   stays true, so nothing else is needed. If the rollback also wrote below
   the floor, run `migrate` as well (see the second way).
2. Keep writing on the rolled-back build with `--write-below-format-floor` on
   its ingest and `maintain` processes. Each publish below the floor then
   lands, logs a warning and counts under
   `ravel_format_floor_writes_below_total{family}`. The floor is never
   lowered, but it is no longer true: `audit-versions` reports it
   `contradicted` and exits nonzero, so the old reader cannot be retired on
   it. Once you roll forward, run `migrate` for the family. It rewrites the
   objects below the target. A floor already at the target is not raised
   again, so after the rewrite that floor reads `stale` rather than
   `current`. It reads `current` again only after a later version raise
   appends a new floor for the family.

### Format crossings at startup

A build whose reader window no longer covers a stored version refuses those
objects at query and compaction time. `ravel-server` can report such entries
when it starts, before any listener binds:

```sh
ravel-server ... --format-window-check off      # the default: no check
ravel-server ... --format-window-check warn
ravel-server ... --format-window-check refuse
```

The check is off by default because it is not free: `warn` and `refuse`
download every statically known tenant's folded catalog before any listener
binds, which can take up to `--format-window-check-timeout` (default `20s`),
close to a liveness probe's budget. Turn on `warn` (or `refuse`) for the first
starts of a new release, when a format crossing is possible, and back off once
those starts have reported clean. Under `off` startup makes no request for the
check and logs nothing about it at info level.

For each statically known tenant and each of metrics, logs, spans, alerts and
audit, the check reads the catalog HEAD and every snapshot part that HEAD
names. It counts the entries whose recorded `segment_format_version` is
outside the reader window of that signal's format: RSEG for metrics, RLOG for
logs, alerts and audit, RSPAN for spans. The window comes from each reader's
own constant, the same source `audit-versions` reads.

The statically known tenants are those named by `--tenant-token`,
`--tenant-token-file`, `--maintain-tenant` and the rules in
`--alert-rules-file`. A deployment that authenticates every tenant through
OIDC or mTLS and names none of these checks nothing, and startup logs that
no tenant was checked.

- `warn` logs one `WARN` line per (signal, version) with the entry count, the
  number of tenants and the reader window, then starts.
- `refuse` exits non-zero before any listener binds. The error names each
  signal and version, for example
  `signal alerts version 4 (1 entries in 1 tenants; reader window RLOG 5..=5)`.
- A store error, a corrupt HEAD, or a snapshot part that does not match the
  blake3 its HEAD names is a check that could not run, not a clean result.
  `warn` logs it with the tenant and signal; `refuse` exits non-zero and says
  the check could not run.
- A HEAD or snapshot part whose format version is above the one this build
  reads was written by a newer build, and the message says so rather than
  calling it corrupt. It is still a check that could not run. The remedy is to
  upgrade this process to the build that wrote it, not to re-ingest.
- A snapshot part that is not found may have been swept by a fold that
  replaced the HEAD after the check read it. The check re-reads that HEAD and
  its parts once; a second not-found is a check that could not run.
- A HEAD GET denied with `AccessDenied` is a check that could not run, and
  its message names the cause: on S3, a role without an `s3:ListBucket` grant
  naming the HEAD key gets 403 for an absent HEAD, so it cannot tell absent
  from denied. The shipped roles in `deploy/iam/` carry that grant for all
  five signals; see "Bootstrap keys" in `deploy/iam/README.md`.

The check reads up to 8 (tenant, signal) pairs at once and stops at
`--format-window-check-timeout` (default `20s`), so startup stays inside a
liveness probe's budget. The pairs not finished by then are reported as not
checked: `warn` logs each tenant and signal not checked and starts, and
`refuse` exits non-zero saying the check could not run to completion. Raise
the timeout for a deployment with many statically known tenants or large
catalogs.

Request cost, per tenant and per signal: one logical read (GET) of the HEAD,
plus one per snapshot part when a HEAD exists; the store may split a large
part into ranged requests, and retries add more. A (tenant, signal) never
folded costs the one read that finds no HEAD, so five reads per tenant is the
floor. The check issues no LIST, no HEAD request and no write. Byte cost: it
downloads every statically known tenant's folded catalog, each HEAD and every
part it names, on every start that runs the check.

Limits:

- Commit records above the HEAD watermark, the hours not yet folded, are not
  read. Finding them needs a LIST of the commit prefix. For metrics, logs
  and spans, `ravel-cli maintain audit-versions` lists the live records and
  covers them.
- Only metrics, logs and spans are ever folded: the server's fold loop and
  `ravel-cli catalog fold --signal` cover those three. No component folds
  alerts or audit, so those two signals have no catalog HEAD and the check
  reports nothing for them. The alert evaluator itself counts an alert
  object at a version this build cannot read: one older than this build
  reads is skipped (`ravel_alert_history_records_skipped_total`), and one
  newer refuses the evaluator's tick
  (`ravel_alert_history_newer_version_refusals_total`). No startup check
  covers audit objects.
- The version checked is the one the writer recorded in the catalog entry.
  The reader still checks each object's own trailer when it opens it.

On a pre-v1.0 build a crossing found here has one remedy, as described
above: re-ingest or discard the objects at the retired version. Start with
`warn` to see the counts, and use `refuse` on a deployment that must not
serve with part of its catalog unreadable.

### Retired-reader startup gate

A release that drops the reader for an older version of `rseg`, `rlog` or
`rspan` checks `sys/compat` before it resolves the tenancy marker, in every
mode. For each family whose reader it retired, it starts only when the
family entry carries a certificate at or above the
oldest version it reads, the certificate is not `contradicted`, and the
late-write watermark is past the certified time. Otherwise the process
exits before serving, with an error naming the family and the remedy. A
bucket with neither `sys/compat` nor `sys/tenancy` and nothing under `t/`
is a fresh store and starts. A bucket with `sys/tenancy` and no
`sys/compat`, or with data under `t/` and neither marker (a bucket written
before the tenancy marker existed), is refused with an error naming
`ravel-cli compat init`, which adopts the store from the release that reads
both versions. The gate checks for `t/` data with one LIST of its first
page; a failed LIST refuses, after the startup retries when the error is
transient.
`sys/compat` without a configured
`--tenant-hash-key-file` is refused too, because the certificates cannot be
authenticated. A watermark more than 7 days old logs a `WARN` line and
still starts.

No current release has retired a reader: the gate's reader set is empty,
and it returns without a request to the store.

## Legal hold

```sh
ravel-cli hold set --tenant <id> --scope <prefix> [--reason <text>]
ravel-cli hold clear --tenant <id> --scope <prefix>
ravel-cli hold list --tenant <id>
```

These commands write and read the audit records that both maintenance drivers
check before any destructive pass. Legal-hold records themselves are
undeletable by every role, Maintain included.

**A hold is honored by every delete batch that re-reads holds after the record
is durable.** A legal hold whose set record was durable before a delete batch
re-read the hold set is honored by that batch; every delete batch re-reads the
hold set immediately before its first delete, and a batch that cannot read it
deletes nothing.

- `hold set` returns once the record is durable, and prints
  `hold set: <scope>; honored by every delete batch that re-reads holds after
  now` for each scope.
- A delete batch is a superseded chain group, a retention bucket, a shard's
  orphan quarantine moves or quarantine purges, a signal's erasure-request,
  catalog object or idempotency-marker deletes, a shard's unreferenced part
  deletes, or an alert or query-audit retention batch
  ([deletion and GC](../../deletion-and-gc.md)). A running pass's later
  batches honor the hold; it does not wait for the next maintenance tick.
- The residual is a batch already deleting: a hold written after that batch's
  re-read and before its delete requests return is not honored by that batch.
  Run `ravel-cli hold list --tenant <id>` to confirm the scope is present.
- A batch whose re-read fails deletes nothing and counts on
  `ravel_maintain_legal_hold_refresh_failures_total`, the same counter a
  failed per-tick refresh moves.

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

A bucket whose verifying listing finds a key the store cannot address under
its commit or `l1/` prefix also keeps its tombstone, and is counted in
`ravel_maintain_retention_unaddressable_held_buckets_total` once per pass.
Unlike the hold count, this one must not stay nonzero: inspect the keys under
the bucket's prefixes, which the sweep's warning names.

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
   below the bound. The Maintain role cannot read manifests, so this run
   reports `created_by` and `statement` as unreadable. That does not change
   which keys it deletes.

   A key whose characters the store's path encoding changes, such as `~`,
   is listed escaped and marked `undeletable by Ravel`, with the key a delete
   of it would reach instead. `--delete` never sends it a delete. When the
   key is flagged, `--delete` prints it as skipped and exits non-zero naming
   it; a key that is not flagged, such as one that is not a manifest key at
   all, is only listed and does not fail the run. Delete that exact key with
   the Maintain credential through an S3 tool.

### Forged version within the bound

A forged version at or below the bound looks like any other version and is
not flagged:

- As the table's highest version, it is the table's definition.
- One at the bound itself leaves no next version, so every `CREATE OR
  REPLACE` or `DROP` on the table is refused with a 422 that names the version
  bound.

**Act before the next `parquet sweep` runs.** That sweep also deletes the
legitimate versions beneath the forged one once it is past the grace. If the
sweep has run, restore the noncurrent object versions, if the bucket keeps
them.

The audit log decides whether a version is forged:

- Every DDL statement that the server runs writes an `attempted` audit record
  before it touches storage (see [the audit guide](../audit.md)). A manifest
  version with no `attempted` record for its table at or shortly before its
  `stored_unix_ms` was therefore not written by Ravel.
- This holds under `--audit-mode required`. Under `best-effort` a statement
  can run without its record, so
  also check `ravel_audit_write_failures_total` for that window. Under `off`,
  the default, the server writes no record at all, and the audit log cannot
  decide the question.
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
[retained-data compatibility](../../adrs/4233-retained-data-compatibility-contract.md),
and [alerts and audit signals](../../adrs/0040-alerts-and-audit-signals.md).
