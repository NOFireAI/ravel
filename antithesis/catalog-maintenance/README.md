# catalog-maintenance Antithesis workload

Task T5 of ADR-4326 (decisions 1, 4 and 5): fold, compaction and the sweep
running concurrently against live ingest, under faults, on one shared bucket.
The ingest-durability workload (`../README.md`) covers the write and read
paths; this one covers what the catalog's maintenance does to data already
written.

## Deployment

| Service | Role |
|---|---|
| `rustfs` | The object store, and the only durable component. Same image pin as ingest-durability. |
| `init` | Creates the bucket and runs `ravel-cli store qualify`, then exits. |
| `ravel1`, `ravel2` | `ravel-server --mode all`: ingest, SQL, and the scheduled fold every 15 s. |
| `maint1`, `maint2` | `ravel-server --mode maintain`: the scheduled fold every 15 s, and compaction, retention and the sweep every 20 s. |
| `driver` | The workload. The only container excluded from faults. |

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
`catalog-maintenance` and the test name `ravel-catalog-maintenance`.

## Seeding (ADR-4326 decision 5)

During setup, before any ravel-server starts, the driver:

1. Writes a Parquet file of `SEED_HOURS` batches with `SEED_RECORDS_PER_HOUR`
   rows each, plus a mapping that stores the rows as logs carrying the same
   `wl.write`/`wl.rec` attributes as live OTLP writes.
2. Runs `ravel-cli --store s3 --tenant-hash-unkeyed load --signal logs --shards
   4 --batch-rows <per hour> --ingest-hour-stride <SEED_HOUR_STRIDE>
   --ingest-hour-start <unix hour>` on the fresh tenant `cm-tenant`. Batch `i`
   lands in ingest hour `start + i * stride`. The start is chosen so the last
   batch is three hours before the driver's current hour, so every seeded hour
   is sealed already.
3. Records every seeded `(write, record)` in its model.
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
end plus the one-hour maximum flush lifetime and the skew allowance have
passed. So a run that must see a compaction either runs for hours or uses
the clock spike below.

## Driver rules

| Rule | What it does |
|---|---|
| `write_logs` | A strict OTLP log write to `ravel1` or `ravel2`, retried on no response, 429 or 503. |
| `fold` | `ravel-cli catalog fold --tenant cm-tenant --signal logs --shards 4`, then `catalog inspect --json`. |
| `compact` | `ravel-cli maintain compact-tenant --tenant cm-tenant --signal logs --shards 4`, then the multiset check. |
| `sweep` | `ravel-cli maintain sweep --tenant cm-tenant --signal logs --shard <0..3>`. |
| `corrupt_head` | Rarely, PUTs garbage over the HEAD through raw S3. A read then falls back to listing and the next fold rebuilds HEAD. |
| `audit` | The store audit below. |
| `check_multiset` | The multiset check below, on one node. |

### The store audit

1. `catalog inspect --json` gives the HEAD key and its `created_unix_ns`.
2. `catalog inspect` (text) prints every key HEAD names: each snapshot part
   (`part[i] ... key=`), its column statistics object (`column_stats (field
   7): key=`) and each key-index leaf (`field=... key=`). It then fetches
   and decodes every part. A part it cannot fetch fails the command with
   `failed to fetch part <key>`, and the audit counts that key as named.
3. LIST the tenant prefix (`t/<tenant hash>/`).
4. `catalog inspect --json` again. If HEAD changed during steps 1 to 4, or
   is corrupt or absent, the audit skips this pass.
5. Decode one listed compaction record (`l1.<hash>.cmt`) with `ravel-cli
   commit decode-compaction <key>`, and one level-0 commit record with
   `ravel-cli commit decode <key>`.

Missing decoder: no `ravel-cli` command prints the entries of a snapshot
part, which are the commit records and data objects the part names. `catalog
inspect` decodes each part but prints only its entry count. So the audit
checks every object HEAD and its part refs name (parts, `.cstat`, `.kidx`
leaves). It does not check the data objects or commit records inside a part.
Closing that gap needs a `ravel-cli catalog inspect --entries` (or similar)
decoder. The driver does not decode parts itself.

## Properties

Every literal starts with `catalog: ` or `clock: `.

| Literal | Type | Where | Holds when |
|---|---|---|---|
| `catalog: a sealed seeded hour was folded into HEAD` | sometimes | every `catalog inspect --json` | HEAD's `watermark_hour` is at or above some seeded hour. |
| `catalog: a compaction record was published` | sometimes | `audit` | A listed `l1.*.cmt` record decoded through `commit decode-compaction`. |
| `catalog: every object HEAD names exists` | always | `audit` | Every key the stable HEAD names is in the LIST taken while HEAD did not change. |
| `catalog: compaction preserves the acked multiset` | always | `compact`, `check_multiset` | Each seeded record is stored exactly once. Each acked live record is stored at least once. No live record is stored more often than its attempts allow. No row names a record that was never written. A verdict whose only finding is absent records is re-read for up to two minutes first. |
| `clock: an hour ingested live was sealed by a fold` | sometimes | every `catalog inspect --json` | HEAD's `watermark_hour` is at or above the hour of some acked live write. |

The driver also reports these, for coverage only:

- `catalog: the workload starts` (reachable).
- `catalog: an on-demand fold succeeded` (reachable).
- `catalog: HEAD was overwritten with garbage through raw S3` (reachable).
- `catalog: inspect reported a corrupt HEAD` (reachable).
- `catalog: a listed commit record decoded through ravel-cli` (sometimes).
- `catalog: the seeded load was refused` (unreachable).

## Clock spike

`local/docker-compose.faketime.yaml` runs every container, rustfs included,
under libfaketime with one wall-clock offset, read from the shared file
`/faketime/offset` (initially `+0`). A `faketime` service copies
`libfaketimeMT.so.1` from the server image into a shared volume, so rustfs can load it
without shipping it. Monotonic clocks are not faked.

`scripts/clock-spike.sh <out-dir> [before] [after] [offset]` does the
following:

1. Runs the local loop with that override.
2. After `before` seconds, writes `offset` (default `+4h`) to the file.
3. Checks that each container's clock moved by at least two hours. rustfs is
   checked through the `Last-Modified` it stamps on a probe PUT.
4. Lets the workload run `after` more seconds.
5. Exits 0 only if `clock: an hour ingested live was sealed by a fold` held at
   least once.

If any clock did not move, for example because the rustfs image ignores
`LD_PRELOAD`, the script exits 2 and prints `none: needs clock control`. The
rows that need the spike are then recorded that way.

## Build, run locally, launch

The same steps as ingest-durability, from this directory's `scripts/`:

- `scripts/build.sh [server|driver|all]`. The driver's build context is
  `antithesis/` and it copies `ravel-cli` from the server image, so build the
  server first.
- `scripts/local-run.sh <out-dir> [seconds]`. Exits 0 when no always-type
  assertion was violated.
- `WORKLOAD_INJECT_SWAPPED_RECORD=1 scripts/local-run.sh <out-dir>` is the
  self-check. The model expects one seeded record moved to another write, so
  `catalog: compaction preserves the acked multiset` must appear in
  `violated.tsv`. The script exits 0 only if it does.
- `scripts/clock-spike.sh <out-dir>`.
- `scripts/export-images.sh`, `scripts/import-images.sh`, `scripts/launch.sh
  <hours>`.

## New external dependencies

The driver adds `arrow-array`, `arrow-schema` and `parquet` (59, the versions
the repository's own Cargo.lock pins) to write the seed file. It reuses the
ingest-durability driver's model and client through a path dependency.
