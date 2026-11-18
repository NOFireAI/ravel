# catalog-maintenance Antithesis workload

Task T5 of ADR-4326 (decisions 1, 4 and 5): fold, compaction and the sweep
running concurrently against live ingest, under faults, on one shared bucket.
The ingest-durability workload (`../README.md`) covers the write and read
paths; this one covers what the catalog's maintenance does to data already
written.

## Deployment

| Service | Role |
|---|---|
| `faketime` | Copies libfaketime, the run wrapper and the clock offset file (`+0`) onto the shared `faketime` volume, then exits. |
| `rustfs` | The object store, and the only durable component. RustFS 1.0.0, the release ingest-durability runs, as the pinned `1.0.0-glibc` image (pinned by digest), under the same glibc libfaketime as every other container. There is no musl build of libfaketime here, and RustFS's default image is static-pie musl, which no `LD_PRELOAD` reaches. |
| `init` | Creates the bucket and runs `ravel-cli store qualify`, then exits. |
| `ravel1`, `ravel2` | `ravel-server --mode all`: ingest, SQL, and the scheduled fold every 15 s. |
| `maint1`, `maint2` | `ravel-server --mode maintain`: the scheduled fold every 15 s, and compaction, retention and the sweep every 20 s. |
| `driver` | The workload. The only container excluded from faults, and the only writer of the clock offset file. |

Every container except `faketime` starts through `/faketime/run`, under
libfaketime; see "The clock jump" and "The sweep jumps" below.

Does `--mode all` run maintenance? Only the fold. `Mode::runs_scheduled_fold`
is true for `all` and `maintain`. Compaction, retention and the sweep
(`maintain::spawn`) run only in `--mode maintain`. That mode requires
`--maintain-key-file` (ADR-3130), and every other mode refuses the flag,
`all` included. So in this topology, compaction and the sweep come from
`maint1`/`maint2` and from the driver's on-demand `ravel-cli` runs. Folds come
from all four servers and from the driver.

The maintain key is generated into the server image at build time
(`ravel/Dockerfile`), so no key is checked in. Both maintain containers run
that one image, so they share the key and admit each other into one worker
set.

No retention window is configured: neither the servers nor the driver pass
`--retention-default`, and the tenant has no config record. So the sweep may
delete superseded and orphaned objects but never a live record, and the
multiset property below holds across sweeps.

Images, entrypoint store wait, label stamping, the launch-time label check and
the instrumentation check are the ingest-durability workload's, under the tag
`catalog-maintenance` and the test name `ravel-catalog-maintenance`. A mutant
run uses the tag `catalog-maintenance-mutants` and the test name
`ravel-catalog-maintenance-mutant` (see "Mutants").

## Seeding (ADR-4326 decision 5)

During setup, before any ravel-server starts, the driver:

1. Writes a Parquet file of `SEED_HOURS` batches with `SEED_RECORDS_PER_HOUR`
   rows each, plus a mapping that stores the rows as logs carrying the same
   `wl.write`/`wl.rec` attributes as live OTLP writes.
2. Runs `ravel-cli --store s3 --tenant-hash-unkeyed load --signal logs --shards
   4 --batch-rows <per hour> --ingest-hour-stride <SEED_HOUR_STRIDE>
   --ingest-hour-start <unix hour>` on the fresh tenant `cm-tenant`. Batch `i`
   lands in ingest hour `start + i * stride`. The start is chosen so the last
   batch's hour lies `stride + 3` hours before the driver's current hour, so
   every seeded hour is sealed already.
3. Checks that the load's report names the planned rows and hours; any other
   report fails setup. Records every seeded `(write, record)` in its model.
4. PUTs the seed marker `antithesis/catalog-maintenance/seeded`.

Every ravel-server entrypoint (`ravel/entrypoint.sh`) waits for that marker
before it execs the server. `ravel-cli load` refuses a tenant that already
holds a HEAD or a commit, and stops if a fold runs during the load, so no
server may ingest or fold first. A driver that finds the marker already
present refuses to start: it would have no model of the earlier records.

The load writes one object per (shard, hour), and compaction skips a bucket
with fewer than two inputs (`DEFAULT_MIN_COMPACTION_INPUTS`). So seeded hours
fold but never compact. Compaction happens on the live hours: the driver's
strict OTLP writes go into the same tenant, and the 500 ms flush cadence
gives each live hour many level-0 objects. A live hour seals only once its
end plus the one-hour maximum flush lifetime, the skew allowance and the
fold safety margin have passed. So a run that must see a compaction either
runs for hours or jumps the clock, which this workload does once per
timeline (below). A superseded compaction input becomes deletable only a
protection horizon and a pinned-query window later still, so once a
compaction record is seen the workload jumps the clock twice more.

## Driver rules

| Rule | What it does |
|---|---|
| `write_logs` | A strict OTLP log write to `ravel1` or `ravel2`, retried on no response, 429 or 503. |
| `fold` | `ravel-cli catalog fold --tenant cm-tenant --signal logs --shards 4`, then `catalog inspect --json`. |
| `compact` | `ravel-cli maintain compact-tenant --tenant cm-tenant --signal logs --shards 4`, then the upper-bound check on one node. |
| `sweep` | `ravel-cli maintain sweep --tenant cm-tenant --signal logs --shard <0..3>`, which deletes what the GC rules allow. |
| `jump_clock` | Rarely, once at least `CLOCK_JUMP_MIN_ACKED` (20) live writes are acked and no write is in flight, moves every clock forward by `CLOCK_JUMP_SECS`. Once per timeline. After that jump and once an audit decoded a compaction record, the same rule instead takes the sweep jumps below, also once per timeline. |
| `corrupt_head` | Rarely, PUTs garbage over the HEAD through raw S3. A read then falls back to listing and the next fold rebuilds HEAD. |
| `audit` | The store audit below. |
| `check_bounds` | The upper-bound check below, on one node. |
| `eventually` | Rarely (and always after `WORKLOAD_EVENTUALLY_AFTER_SECS` when that is set): stops faults, waits up to 300 s for every node to serve, jumps the clocks if no rule did, folds, compacts, audits, takes the sweep jumps if a compaction record was seen and no rule took them, sweeps every shard, folds and audits again, runs the full multiset check and ends the timeline. |

### The multiset checks

A no-token SQL scan's freshness is bounded by listing, and a commit landing
in an hour already sealed stays out of it (docs/consistency-model.md). So
while faults run, a scan proves only upper bounds: no seeded record stored
more than once, no live record stored more often than its attempts allow, no
row naming a record that was never written. Absences are not judged then.

After faults stop, `eventually` checks the whole multiset: a no-token scan of
each node, re-read for up to two minutes while its only finding is absent
records, must hold every seeded record exactly once, and a read of every
acked live write with its commit tokens must find each at least once. A read
that fails for two minutes counts as a violation, not as a pass.

### The store audit

1. `catalog inspect --json` gives the HEAD key and its `created_unix_ns`.
2. `catalog inspect` (text) prints every key HEAD names: each snapshot part
   (`part[i] ... key=`), its column statistics object (`column_stats (field
   7): key=`) and each key-index leaf (`field=... key=`). It then fetches
   and decodes every part. A part it cannot fetch fails the command with
   `failed to fetch part <key>: object not found`, and the audit counts
   that key as named. A part fetch failing for any other cause gives no
   verdict for that pass.
3. `ravel-cli rlog footprint --tenant cm-tenant --shards 4 --json` lists
   every data object the catalog resolves for the tenant and fetches each. A
   data object the store does not hold fails the command with `failed to
   fetch <key>: object not found`; the audit counts that key as resolved only
   when it is a data object key under `t/<tenant hash>/l/l0/` or
   `t/<tenant hash>/l/l1/` ending in `.` followed by `DATA_SUFFIX` (`rseg`,
   in `crates/ravel-commit/src/keys.rs`), the suffix Ravel gives every data
   object, logs included. Any other failure (a
   transient or skew error, a fetch of `sys/tenancy` or of a HEAD) gives no
   verdict on resolved data objects for that pass.
4. LIST the tenant prefix (`t/<tenant hash>/`). A page counts only when curl
   exited 0 and the body ends with `</ListBucketResult>`; a page that does
   not is requested again, and a listing that still fails gives no verdict,
   never an empty bucket.
5. `catalog inspect --json` again. If HEAD changed during steps 1 to 5, or
   is corrupt or absent, the audit skips steps 6 and 7.
6. Cross-check the text parse against the JSON form of the same HEAD: the
   same part keys in the same order and the same number of key-index leaves.
   A parse that found nothing or a different set fails the audit, since it
   would otherwise check nothing and pass.
7. Decode one listed compaction record (`l1.<hash>.cmt`) with `ravel-cli
   commit decode-compaction <key>`, and one level-0 commit record with
   `ravel-cli commit decode <key>`.
8. The sweep check, over the LIST of step 4, whether or not step 5 skipped
   steps 6 and 7. It runs after step 5, so its decodes do not widen the window
   HEAD must hold still in, and the `audit` rule runs it after releasing its
   read gate, since it touches only the store and the driver's own record of
   listed keys: a decode or GET that a clock jump's skew window fails
   confirms nothing, and the decode is retried by a later pass. Decode up to 16 listed compaction records not decoded before
   (`ravel-cli commit decode-compaction <key>`; records are immutable, so
   each is decoded once) for their inputs. Each input names its level-0
   commit record (in the record's own directory) and its data object
   (`t/<tenant hash>/l/l0/<shard>/<writer>.<epoch>.<seq>.*.rseg`). An input
   key that an earlier LIST held, that this LIST lacks, and that a GET then
   finds absent is a swept input, counted once across audits. This LIST
   then joins the keys seen.

Missing decoder: no `ravel-cli` command prints the commit-record entries of
a snapshot part. `catalog inspect` decodes each part but prints only its
entry count, and `rlog footprint` reports data objects, not commit records.
So the audit checks every object HEAD and its part refs name (parts,
`.cstat`, `.kidx` leaves) and every data object the catalog resolves, but
not the commit records inside a part. Closing that gap needs a `ravel-cli
catalog inspect --entries` (or similar) decoder. The driver does not decode
parts itself.

## Properties

Every literal starts with `catalog: ` or `clock: `.

| Literal | Type | Where | Holds when |
|---|---|---|---|
| `catalog: no scan shows a phantom or an excess copy` | always | `compact`, `check_bounds` | A no-token scan, while faults may run, stores no seeded record more than once, no live record more often than its attempts allow, and no record that was never written. |
| `catalog: every object HEAD names exists` | always | `audit` | Every key the stable HEAD names is in the LIST taken while HEAD did not change, and the text parse of that HEAD matches its JSON form. |
| `catalog: every data object the catalog resolves exists` | always | `audit` | Every data object `rlog footprint` reports is in that LIST. |
| `catalog: the system recovers once faults stop` | always | `eventually` | Every node is ready and answers SQL, and every maintenance node is ready, within 300 s of faults stopping. |
| `catalog: compaction preserves the acked multiset` | always | `eventually` | After faults stop, a fold and a compaction: each seeded record is stored exactly once, each acked live record at least once (read with its commit tokens), no live record more often than its attempts allow, no row names a record that was never written, and every read answered. |
| `catalog: a sealed seeded hour was folded into HEAD` | sometimes | every `catalog inspect --json` | A part of HEAD with entries covers some seeded hour (inside its `min_hour..=watermark_hour`, not retired). |
| `clock: an hour ingested live was sealed by a fold` | sometimes | every `catalog inspect --json` | A part of HEAD with entries covers the hour of some acked live write. |
| `catalog: a compaction record was published` | sometimes | `audit` | A listed `l1.*.cmt` record decoded through `commit decode-compaction`. |
| `catalog: sweep deleted a superseded compaction input` | sometimes | `audit` | Evaluated once an audit holds a decoded compaction record: a commit record or data object of one of its inputs was in an earlier LIST, is absent from this one, a GET finds it absent, and no earlier audit counted it. |
| `catalog: the audit checked the data objects the catalog resolves` | sometimes | `audit` | `rlog footprint` reported at least one data object. |
| `catalog: a listed commit record decoded through ravel-cli` | sometimes | `audit` | A listed level-0 commit record decoded through `commit decode`. |
| `clock: the driver's clock jumped forward` | reachable | `jump_clock`, `eventually` | The jump reached the driver's own clock. |
| `clock: the clocks jumped past the sweep's protection horizon and pinned-query window` | reachable | `jump_clock`, `eventually` | Both sweep jumps reached the driver's own clock. |
| `catalog: an on-demand sweep succeeded` | reachable | `sweep`, the sweep jumps, `eventually` | `maintain sweep` exited 0. |
| `catalog: the workload starts` | reachable | startup | |
| `catalog: an on-demand fold succeeded` | reachable | `fold` | |
| `catalog: HEAD was overwritten with garbage through raw S3` | reachable | `corrupt_head` | |
| `catalog: inspect reported a corrupt HEAD` | reachable | every `catalog inspect --json` | |
| `clock: the driver could not write the clock offset` | unreachable | `jump_clock`, the sweep jumps | |
| `clock: the clock jump did not reach the driver` | unreachable | `jump_clock`, the sweep jumps | The driver's clock did not move by the jump within one reread plus 30 s. |
| `catalog: the seeded load was refused` | unreachable | setup | |
| `catalog: the seeded load reported other rows than planned` | unreachable | setup | |

## The clock jump

A live hour seals only `max_flush_lifetime + clock_skew_allowance +
fold_safety_margin` (3600 + 300 + 900 s) after it ends, and compaction needs
a sealed hour with two or more inputs. A timeline is far shorter than that,
so every container runs under libfaketime with one wall-clock offset and the
driver moves it forward once per timeline (ADR-4326, clock-jump amendment).

- The `faketime` service puts `libfaketimeMT.so.1` (libfaketime 0.9.13,
  built in `ravel/Dockerfile` from a tarball pinned by sha256), the run
  wrapper and the offset file `/faketime/offset` (`+0`) on a shared volume.
  rustfs loads the library from there without shipping it.
- Every other container starts through `/faketime/run`, which exits 70
  unless the library is preloaded, the offset file is readable, a `+1d`
  probe moves `date` by a day, and the container's binary
  (`FAKETIME_CHECK_BINARY`) is dynamically linked against glibc. A container
  whose clock could not jump never starts, so setup fails rather than the
  run passing without the jump.
- The driver refuses to start if `CLOCK_JUMP_SECS` is below 8400 s (3600 s
  for an hour acked at its first second, plus the sealing delay above) or
  the offset file does not hold `+0`. The config jumps 9000 s.
- `jump_clock` writes the new offset by replacing the file whole. libfaketime
  rereads it at most every `FAKETIME_CACHE_DURATION` (2 s) per process, so
  the jump lands in every process within about 2 s. Inside that window a
  process that has not reread is 9000 s behind one that has, beyond SigV4's
  900 s skew limit, so a request between them (a server's background request
  to rustfs included) may fail. The driver holds its write gate from before
  the offset write until its own clock moved and one more reread passed.
- Monotonic clocks are not faked, so timeouts and leases measured on them
  are untouched. Shell waits in the entrypoints use `/proc/uptime`.

## The sweep jumps

The sweep deletes a superseded compaction input (GC rule 2,
`crates/ravel-maintain/src/sweep.rs`) only when all of these hold:

- HEAD no longer names it, which needs a fold after the compaction;
- `now >= created_unix_ns + protection_horizon` of the compaction record.
  The default horizon is `max_query_duration + grace + clock_skew_allowance`
  (3600 + 86400 + 300 = 90300 s, `DEFAULT_PROTECTION_HORIZON_NS`);
- an unnamed-since marker (ADR-1133) exists that an earlier sweep past the
  horizon wrote, and `max_query_duration + head_cache_ttl + 4 *
  clock_skew_allowance` (3600 + 30 + 1200 = 4830 s) has passed since it.

One jump cannot satisfy the third condition: the marker is written by the
first deleting pass past the horizon, with the time of that pass. So once the
sealing jump reached the driver and an audit decoded a compaction record,
the driver, holding its write gate throughout:

1. LISTs the tenant prefix, so every input key a later deletion removes was
   seen;
2. moves every clock forward by `CLOCK_SWEEP_HORIZON_JUMP_SECS` (config
   91200 s), with the same offset write, checks and reread wait as the
   sealing jump;
3. folds, then sweeps all four shards, which writes the markers;
4. moves every clock forward by `CLOCK_SWEEP_WINDOW_JUMP_SECS` (config
   5400 s), the same way.

The next sweep, from `maint1`/`maint2`, the `sweep` rule or the
`eventually`, deletes the inputs. The driver refuses to start if either
sweep jump is below its term (90300 s and 4830 s), computed from the shipped
defaults; no grace or horizon flag is passed anywhere, so the run deletes
under the shipped `sys/gc` defaults. Each jump is as far beyond SigV4's 900 s
skew limit as the sealing jump is, and the gate hold across each reread
window is the same mitigation. A token read after the sweep resolves a
deleted level-0 commit record through the compaction record's input list
(docs/catalog-and-mvcc.md), so the `eventually` checks the multiset and the
HEAD audit after the deletion.

`scripts/clock-spike.sh <out-dir> [seconds]` runs the local loop and reports
on the jumps alone. It writes `<out-dir>/clock-spike-result.txt` and exits 0
only if `local-run.sh` passed; `clock: the driver's clock jumped
forward`, `clock: an hour ingested live was sealed by a fold`, `catalog: a
compaction record was published`, `clock: the clocks jumped past the sweep's
protection horizon and pinned-query window` and `catalog: sweep deleted a
superseded compaction input` held; and all six clock probes (`date` in
rustfs, ravel1, ravel2, maint1 and maint2, and the `Last-Modified` rustfs
stamps on a probe PUT) read at least the three jumps' total minus 60 s ahead
of the host.
The last line is `clock jump: PASS` or `clock jump: FAIL`.

## Mutants

ADR-4326 decision 3 (as amended by #4335) gives the catalog area five
mutants. Each is an `inv_mutant!("catalog/<stem>")` switch in ravel-server's
library code, compiled in only by the `antithesis-mutants` cargo feature and
selected at start by `RAVEL_ANTITHESIS_MUTANT`; a name ravel-server does not
register makes it refuse to start. `formal/tla/catalog/antithesis-mutants.tsv`
names the property each one must make false, and `local-run.sh` reads it from
there.

| Mutant | Switch | Property it makes false | Asserted in |
|---|---|---|---|
| `catalog/lost-cas-proceeds` | a fold whose HEAD CAS was refused carries on as if it had won | `catalog: a fold reports as folded only a HEAD the store holds` | `crates/ravel-catalog/src/fold.rs` |
| `catalog/sweep-superseded-no-head-gate` | the superseded-input sweep takes every group's HEAD gate as clear without consulting HEAD | `catalog: a sweep never deletes an object the current HEAD names` | `crates/ravel-maintain/src/sweep.rs` |
| `catalog/corrupt-head-ignores-delete-gate` | both delete paths treat an undecodable HEAD as naming nothing | `catalog: a delete gate never clears under an unreadable HEAD`, or its sibling on the unreferenced-part sweep, `catalog: a catalog sweep never deletes under an unreadable HEAD` | `crates/ravel-maintain/src/reachability.rs`, `crates/ravel-maintain/src/sweep.rs` |
| `catalog/compaction-loser-overwrites` | a publisher that loses the record's create-only PUT writes its own payload over the winner's | `catalog: a published compaction record is never replaced` | `crates/ravel-maintain/src/publish.rs` |
| `catalog/compaction-swaps-record` | compaction moves its last input identity to a writer id no writer is issued, before building the parts | `catalog: compaction preserves the acked multiset` | the driver (`driver/src/main.rs`) |

The corrupt-HEAD mutant switches off the HEAD guard of both delete passes,
and each pass asserts `CorruptHeadFailsClosedOnDeletePaths` under its own
literal, so either literal going false counts. The `.tsv` names the first.

Only the four ravel-server containers run a mutant. The driver's on-demand
fold, compaction and sweep go through `ravel-cli`, which no image builds
with `antithesis-mutants`, so a mutant fires only in the servers' scheduled
folds (all four) and in `maint1`/`maint2`'s compaction and sweep. The sweep
and corrupt-HEAD mutants need a sweep that reaches a delete, so they fire
only after the sweep jumps ("The sweep jumps"); the corrupt-HEAD one also
needs the driver's `corrupt_head` rule to land before a scheduled sweep.

`MUTANTS=1 scripts/build.sh` builds the mutant images:
`ravel-antithesis-server:catalog-maintenance-mutants`, whose ravel-server is
built with `--features sql,antithesis,antithesis-mutants` (the Dockerfile's
`SERVER_FEATURES` build argument), and
`ravel-antithesis-driver:catalog-maintenance-mutants`, built over it. Both
carry the `org.ravel.antithesis.ref` label, and the server image also
`org.ravel.antithesis.features`. Besides the instrumentation check, the build
checks that the server binary registers the five catalog mutant names. The
default build (no `MUTANTS`) is unchanged: features `sql,antithesis`, tag
`catalog-maintenance`.

`RAVEL_ANTITHESIS_MUTANT=catalog/<stem>` selects a mutant for `local-run.sh`
and `launch.sh`. Both pass it to `scripts/mutant-config.sh`, which writes a
copy of `config/` where every workload image names its `-mutants` tag and the
ravel-server environment's `RAVEL_ANTITHESIS_MUTANT: ""` names the mutant.
It refuses, and so both scripts refuse:

- a name that is not one of the five `catalog/` mutants in the `.tsv` (the
  message lists them);
- a copy whose rewrite did not move every image line and set exactly one
  selector.

Both scripts also refuse when `ravel-antithesis-server:catalog-maintenance-mutants`
lacks `antithesis-mutants` in its `org.ravel.antithesis.features` label: a
server built without the feature ignores the variable, so the run would test
no mutant. With the variable unset or empty both run the plain config,
unchanged.

A mutant run of `local-run.sh` exits 0 only when the mutant's property was
evaluated false at least once (it appears in `violated.tsv`). Every other
always-type assertion that went false, and every other gate that failed, is
printed as `REPORT (mutant run, not gated): ...` and does not change the exit
code; a mutant that breaks only another property fails the run, as decision
3 requires. Setup failures that leave no evidence (compose up, setup
complete, `first_start_workload`, unparsable SDK output) still exit 1. The
self-check and a mutant run cannot be combined.

`launch.sh` launches a mutant run under the test name
`ravel-catalog-maintenance-mutant`, with `(mutant <name>)` appended to the
description and `--param attrs.mutant=<name>`; a plain run passes
`attrs.mutant=none`.

## Build, run locally, launch

The same steps as ingest-durability, from this directory's `scripts/`:

- `scripts/build.sh [server|driver|all]`. The driver's build context is
  `antithesis/` and it copies `ravel-cli` from the server image, so build the
  server first.
- `scripts/local-run.sh <out-dir> [seconds]` starts the stack under the
  run's compose file, runs the workload for `seconds` (default 240) before
  the driver's forced `eventually`, and exits 0 only if all of these hold:
  - `faketime` and `init` exited 0, and setup completed within 900 s;
  - the driver was still running after setup, then exited 0 having logged
    `eventually: ending the timeline`;
  - every other container is running or exited 0;
  - the offset file holds the three jumps' total, and every clock probe read
    a number at least that total minus 60 s ahead of the host
    (`clock-probe.txt`);
  - every container's SDK output was collected;
  - no always-type assertion was violated (`violated.tsv`), and none that
    must be hit was never hit (`never-hit.tsv`);
  - `catalog: the workload starts`, `clock: the driver's clock jumped
    forward`, `catalog: a sealed seeded hour was folded into HEAD`, `clock:
    an hour ingested live was sealed by a fold`, `catalog: a compaction
    record was published`, `clock: the clocks jumped past the sweep's
    protection horizon and pinned-query window`, `catalog: an on-demand
    sweep succeeded` and `catalog: sweep deleted a superseded compaction
    input` each held.
- `WORKLOAD_INJECT_SWAPPED_RECORD=1 scripts/local-run.sh <out-dir>` is the
  self-check. The model expects one seeded record moved to another write, so
  `catalog: compaction preserves the acked multiset` must appear in
  `violated.tsv`, and no assertion other than it and `catalog: no scan shows
  a phantom or an excess copy` may. The other gates still apply.
- `RAVEL_ANTITHESIS_MUTANT=catalog/<stem> scripts/local-run.sh <out-dir>
  [seconds]` is a mutant control run over the `-mutants` images (see
  "Mutants"); it exits 0 only when that mutant's property was violated.
- `scripts/clock-spike.sh <out-dir> [seconds]` (above).
- `scripts/export-images.sh`, `scripts/import-images.sh`, `scripts/launch.sh
  <hours>`. `launch.sh` validates and launches with a 15-minute setup
  timeout, above the driver's own 600 s setup budget. `MUTANTS=1` moves the
  two image scripts to the `-mutants` images (saved as
  `<name>.mutants.tar.zst`), and `RAVEL_ANTITHESIS_MUTANT` makes `launch.sh`
  launch a mutant run.

A smoke and a mutant run, on the build host and then on the launching
machine:

```sh
# build host (x86_64, Docker)
antithesis/catalog-maintenance/scripts/build.sh
MUTANTS=1 antithesis/catalog-maintenance/scripts/build.sh
# ok ravel-antithesis-server:catalog-maintenance-mutants: /symbols/ravel-server registers the 5 catalog mutants

antithesis/catalog-maintenance/scripts/local-run.sh out/plain
# OK: no always-type assertion violated or unhit, ...
for m in lost-cas-proceeds sweep-superseded-no-head-gate corrupt-head-ignores-delete-gate \
    compaction-loser-overwrites compaction-swaps-record; do
  RAVEL_ANTITHESIS_MUTANT=catalog/$m antithesis/catalog-maintenance/scripts/local-run.sh out/$m
done
# mutant OK: catalog/<stem> violated "<its property>"   (once per mutant)

antithesis/catalog-maintenance/scripts/export-images.sh s3://<bucket>/<prefix>
MUTANTS=1 antithesis/catalog-maintenance/scripts/export-images.sh s3://<bucket>/<prefix>

# launching machine
antithesis/catalog-maintenance/scripts/import-images.sh s3://<bucket>/<prefix>
MUTANTS=1 antithesis/catalog-maintenance/scripts/import-images.sh s3://<bucket>/<prefix>
antithesis/catalog-maintenance/scripts/launch.sh 12 "catalog-maintenance smoke"
RAVEL_ANTITHESIS_MUTANT=catalog/lost-cas-proceeds \
  antithesis/catalog-maintenance/scripts/launch.sh 12
```

The `weekly` file in this directory is the marker that opts the workload
into the weekly run (ADR-4326 decision 4). This tree has no
`.github/workflows/antithesis-weekly.yml` yet (#4308), so nothing reads the
marker yet.

## New external dependencies

The driver adds `arrow-array`, `arrow-schema` and `parquet` (major version
59, the major the repository root's Cargo.lock pins at 59.3.0) to write the
seed file. The driver has its own Cargo.lock, which resolves them to 59.3.3.
It reuses the ingest-durability driver's model and client through a path
dependency.
