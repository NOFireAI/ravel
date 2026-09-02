# Maintenance (day 2)

**Only a `--mode maintain` process runs maintenance.** Compaction, retention,
the sweeper that issues the deletes, and the at-rest integrity scrubber all run
in that mode and in no other. A process in `--mode all` runs ingest, the query
API, the catalog fold and alert evaluation, and none of the maintenance loops. A
deployment made only of `all` processes therefore never compacts an object and
never deletes one: its L0 segments accumulate unmerged, retention windows have
no effect, and nothing reclaims storage. The quickstart is exactly such a
deployment, by design.

The catalog fold is the exception in the other direction: it runs in every mode
except `maintain`.

If you are wondering why nothing is being reclaimed, check for a `maintain`
process before anything else.

- [Running the maintenance loop](#running-the-maintenance-loop)
- [Catalog fold and verify](#catalog-fold-and-verify)
- [Compaction](#compaction)
- [Garbage collection and retention](#garbage-collection-and-retention)
- [The at-rest integrity scrubber](#the-at-rest-integrity-scrubber)
- [Format migration](#format-migration)
- [Legal hold](#legal-hold)
- [The maintenance and inspection commands](#the-maintenance-and-inspection-commands)

## Running the maintenance loop

**Continuously.** `ravel-server --mode maintain` runs the loop per tenant, over
all three signals and every shard, on `--maintain-interval-secs` (default 300).
It needs a backend that reports the `multipart` capability, and it serves no
ingest or query routes. It still binds `--listen-http` for liveness.

`--maintain-tenant <name>`, repeatable, names a tenant this process maintains in
addition to every tenant named by `--tenant-token`. It is required for a
deployment that authenticates through OIDC or mTLS, because those tenants are
only known once a request arrives and maintenance has no other way to learn
about them.

Four flags size how much work one tick does and how quickly a stuck unit is
visible:

| Flag | Default | What to change it for |
|---|---|---|
| `--maintain-interval-secs` | `300` | How often each tenant's tick runs. |
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

`--disable-fold` turns the background task off. `--fold-interval-secs` (default
300) controls only how often it wakes up to check for newly sealed hours; it has
no bearing on when an hour becomes eligible to seal.

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
the troubleshooting page, under queries missing recently written data.

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
superseded. It copies pages verbatim and never decodes a sample, so a query over
the L1 output returns identical results to a query over the L0 inputs.

This is the primary win of running maintenance at all: object count per hour
drops from thousands to a handful, and every query over that hour pays
proportionally fewer requests.

Compaction is signal-generic. Metrics, logs and spans go through the same code.

To compact by hand, one bucket or one whole tenant and signal:

```sh
ravel-cli maintain compact-bucket --tenant <t> --signal <metrics|logs|spans> \
  --shard <n> --hour <n> [--dry-run]

ravel-cli maintain compact-tenant --tenant <t> --signal <metrics|logs|spans> \
  [--shards <n>] [--from-hour <n>] [--to-hour <n>] [--bucket-concurrency <n>] [--dry-run]
```

`compact-tenant` discovers the hours itself, walking each shard's ingest hours
ascending and stopping at the first unsealed one, because every later hour is
unsealed too. It streams one line per bucket as each completes, then a summary
of compacted, already-compacted, not-sealed, below-minimum and tombstoned
counts, segments written, wall time, the failure count and the concurrency it
used. A bucket whose compaction errors does not abort its siblings: the walk
completes, prints each failed bucket's own outcome line, and exits nonzero with
an aggregate naming how many failed and how many succeeded. A clean run exits
zero, and a not-sealed bucket is a reported outcome rather than a failure.

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

### The two timing values

- `grace`, default 24h, is the floor for the orphan and unreferenced-L1 age
  gates.
- `protection_horizon`, default 25h, is the gap between a deletion anchor (a
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

## The at-rest integrity scrubber

The checksum hierarchy, a whole-object hash at write time and per-section
checksums on read, is otherwise verified only when a query happens to touch the
covered bytes. Bytes nobody queries are never checked. The scrubber re-verifies
them on a schedule instead. It runs only in `--mode maintain`, spawned per
process alongside the maintenance loop.

Each tick it re-discovers tenants from storage and, for every unit, verifies a
bounded slice of that shard's committed L0 data objects: a section checksum
re-check plus a whole-object rehash against the recorded content hash. A
persisted per-shard cursor advances the slice each tick, so a full rotation over
the corpus completes in about the configured period.

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

This is the one scheduled task whose cost scales with data volume rather than
metadata volume, so size `P` against the corpus you actually have, and watch
`ravel_scrub_cursor_position` to confirm rotations keep pace. That gauge and the
three scrubber anomaly counters are catalogued in
[the observability guide](../observability.md); the alarms that matter are on
the troubleshooting page.

### It needs no policy change

The scrubber's reads, commit records and data objects, are already covered by
the Maintain role's existing read and list grants. Its one write, the per-shard
cursor, is placed under the existing `maint/` control prefix, which the Maintain
role's write grant already names. Enabling it requires no storage-policy change.

## Format migration

```sh
ravel-cli maintain migrate --tenant <t> --signal <metrics|logs|spans> \
  [--shards <n>] [--target-version <n>] [--family <name>] [--budget-records <n>]
```

`migrate` raises a `(tenant, signal, format family)`'s recorded format floor to a
target on-object format version. One invocation:

1. walks buckets in shard and ingest-hour order from a durable cursor, rewriting
   every sealed, un-tombstoned, not-yet-compacted bucket that still has an L0
   commit record below the target version. This reuses the compaction rewrite
   primitive, so the rewrite is bucket-atomic and produces a compaction record
   exactly as compaction does;
2. stops early and persists the cursor once `--budget-records` is spent (`0`,
   the default, is unlimited; re-run to resume), or, once the walk drains,
   re-audits fresh and raises the floor only if that re-audit finds zero records
   below the target.

A refused raise, reported as "FOUND STRAGGLERS", means the fresh re-audit found
genuine live data still below the target: a bucket too recently landed to be
sealed and migrated yet, or data that arrived after the walk passed. Re-run
`migrate`; that data migrates once it is sealed.

The re-audit's liveness definition already excludes a bucket's pre-rewrite L0
commit records once that bucket carries a compaction or rewrite record. Those
records are dead, sweepable leftovers of a rewrite this same invocation may have
just performed, not stragglers. Because of that, a clean migration converges and
raises the floor in one invocation, and running `sweep` in between is never
required for it to converge. The sweeper's superseded-input rule still deletes
those records on its own schedule, which is storage reclamation, not a
correctness precondition.

### Auditing what versions are live

```sh
ravel-cli maintain audit-versions --tenant <t> [--shards <n>]
```

This audits live on-object format versions across all three signals, reading the
supported window from each reader's own source so a future version bump cannot
make the audit stale. It exits nonzero on any anomaly.

Each format currently supports exactly one version and carries no reader for the
previous one, so any live object at another version is an anomaly to re-ingest,
not a migration target:

| Format | Supported version | Anomalies |
|---|---|---|
| Metric segments | v7 | Every other version, including v6. |
| Log segments | v4 | v1, v2 and v3. |
| Span segments | v4 | Every other version. |

### Rolling a format bump: readers before writers

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
before any destructive pass. A `--signal` and `--shard` form writes all the
prefixes one shard needs in a single command, so the partial-hold mistake is not
reachable from the CLI.

**The hold is not effective the instant the command returns.** Each maintenance
tick refreshes its hold snapshot once, before its destructive pass, so a hold set
after that tick's refresh is not honored until the next one. The exposure window
is one `--maintain-interval-secs` interval, five minutes by default.

After placing an urgent hold, run `ravel-cli hold list --tenant <id>` and
confirm the scope is present before assuming the data is protected. `hold set`
returning success means only that the record was written, not that a maintenance
pass has picked it up.

Legal-hold records themselves are undeletable by every role, Maintain included.

## The maintenance and inspection commands

Every subcommand shares the same store flags as `ravel-server`. The full flag
list is in [the generated CLI reference](../../reference/ravel-cli-flags.md).

| Command | Does |
|---|---|
| `maintain compact-bucket` | One compaction pass over a single sealed bucket, printing the outcome. `--dry-run` computes the same plan and writes nothing. |
| `maintain compact-tenant` | Compacts every sealed bucket of one tenant and signal across shards. See [compaction](#compaction). |
| `maintain sweep --tenant <t> --signal <s> --shard <n> [--dry-run]` | One sweep pass (orphan collection, superseded inputs, unreferenced L1) over a shard, printing the four delete counts. `--dry-run` reports the eligible set and deletes nothing. |
| `maintain status --tenant <t> --signal <s> --shard <n> --hour <n>` | Reports one bucket's state: sealed, tombstoned, compacted, L0 record count, superseded-input count, L1 segments present, unreferenced count. Read-only. |
| `maintain audit-versions --tenant <t> [--shards <n>]` | Audits live on-object format versions. Exits nonzero on any anomaly. |
| `maintain migrate` | Raises a format floor. See [format migration](#format-migration). |
| `maintain verify-custody --tenant <t> [--shards <n>]` | Re-verifies the content-addressed chain at rest: every live data object's key-embedded hash against its actual content hash, and every surviving compaction record's referenced inputs. An input the sweeper already legitimately reclaimed past its protection horizon is reported separately, not as an anomaly. Read-only; exits nonzero on any anomaly. |
| `catalog list --tenant <t> [--hours <n>] [--shards <n>]` | Lists the commit records the catalog resolves for that tenant over the last N hours. `--shards` must match what the data was written with. |
| `catalog fold` | One-shot fold for one tenant and signal. See [catalog fold and verify](#catalog-fold-and-verify). |
| `catalog inspect --tenant <t> [--signal <s>]` | Decodes and prints that signal's HEAD and every referenced snapshot part: watermark, keys, hashes, entry counts. It names the signal both as a word and as the numeric value read off the object, so a HEAD stamped with a different signal than the one asked for is visible. It reports rather than errors when no HEAD exists yet. |
| `catalog verify` | Diffs the sealed record history against the snapshot. See [routine verification](#routine-verification). |
| `commit reconstruct` | Rebuilds record-less L0 data objects' commit records from their own footers. Stop maintenance first; see the troubleshooting page. |
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
