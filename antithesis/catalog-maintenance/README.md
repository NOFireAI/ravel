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
| `s3proxy` | The `s3-proxy` binary of the driver image. Every ravel-server container reaches rustfs through it (`RAVEL_S3_ENDPOINT=http://s3proxy:9000`); `init`, the driver and its `ravel-cli` reach rustfs directly. It forwards every byte unchanged until the driver sets a rule over its control port (`s3proxy:9001`); see "The forced races". Its 512-connection cap needs an open-file limit of 5200 (`proxy::NOFILE_NEEDED`): the service sets `nofile` to 65536, and the proxy raises its soft limit to the hard limit and refuses to start below 5200. `local-run.sh` fails a run whose s3proxy log has any `Too many open files` line (`scripts/proxy-log-check.sh`). |
| `driver` | The workload. The only container excluded from faults, and the only writer of the clock offset file. |

Every container except `faketime` and `s3proxy` starts through
`/faketime/run`, under libfaketime; see "The clock jump" and "The sweep jumps"
below. `s3proxy` signs nothing and keeps no wall-clock state, so the real
clock serves it. It is a container of its own, not a thread of the driver,
because the driver is excluded from faults: a killed `s3proxy` is a network
outage between the servers and the store, and it restarts with no rules.

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

Images, label stamping, the launch-time label check and the instrumentation
check are the ingest-durability workload's, under the tag
`catalog-maintenance` and the test name `ravel-catalog-maintenance`. As there,
the servers run under an init process with a 300 s `--startup-store-deadline`
and no entrypoint store wait; their entrypoint only waits for the seed marker.
A mutant run uses the tag `catalog-maintenance-mutants` and the test name
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

The load also writes the tenant's provisioning record `t/<tenant hash>/l/prov`.
The seed is the only call that names a shard count. The driver's fold,
compaction and footprint calls pass no `--shards`, so each takes the count
from that record. The driver bounds its sweep passes, its bucket scans and the
`sweep` rule's shard by the shard ceiling: the largest count of any generation
in the record activated by the current hour, as `compact-tenant` computes it.
The driver reads the record with a raw S3 GET once at setup, and again each
time its clock enters a new hour, since a reshard activates two or more hours
after it is appended. A tenant without a record is still at 4 shards. If the
record cannot be read or decoded, setup fails, and later the driver keeps the
ceiling it last read.

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
| `fold` | `ravel-cli catalog fold --tenant cm-tenant --signal logs`, then `catalog inspect --json`. |
| `compact` | `ravel-cli maintain compact-tenant --tenant cm-tenant --signal logs`, then the upper-bound check on one node. |
| `sweep` | `ravel-cli maintain sweep --tenant cm-tenant --signal logs --shard <n>`, a shard below the shard ceiling, which deletes what the GC rules allow. |
| `jump_clock` | Rarely, once at least `CLOCK_JUMP_MIN_ACKED` (20) live writes are acked and no write is in flight, moves every clock forward by `CLOCK_JUMP_SECS`. Once per timeline. After that jump and once an audit decoded a compaction record, the same rule instead takes the sweep jumps below, also once per timeline. |
| `corrupt_head` | Rarely, PUTs garbage over the HEAD through raw S3. A read then falls back to listing and the next fold rebuilds HEAD. |
| `race_folds` | Rarely, once HEAD is known: one fold race ("The forced races"). |
| `race_records` | Rarely, once HEAD is known: one record race ("The forced races"). |
| `sweep_unfolded` | Rarely, once HEAD is known and at most once per timeline: the unfolded-record sweep ("The forced races"). |
| `sweep_unreadable_head` | Rarely, once HEAD is known and at most once per timeline: the unreadable-HEAD sweep ("The forced races"). |
| `audit` | The store audit below. |
| `check_bounds` | The upper-bound check below, on one node. |
| `token_lifecycle_read` | While faults may run: reads one acked write back with its commit tokens on a drawn node, drawing the write from all of them, from those with a compacted input, or from those whose input commit record the sweep check confirmed deleted, and drawing whether to read it twice on that node ("Token reads across the lifecycle"). |
| `eventually` | Rarely (and always after `WORKLOAD_EVENTUALLY_AFTER_SECS` when that is set): stops faults, waits up to 300 s for every node to serve, jumps the clocks if no rule did, runs up to six fold races until a server fold lost its CAS, up to five record races until a server's record PUT lost, the unfolded-record sweep unless one already reached its situation, and the unreadable-HEAD sweep, then folds, compacts, audits, folds and audits again and token-reads up to six writes with a compacted input, takes the sweep jumps if a compaction record was seen and no rule took them, sweeps every shard, folds and audits again, in the local loop has `local-run.sh` restart a node, token-reads up to six writes whose input commit record was swept, runs the full multiset check and ends the timeline. |

The forced eventually runs inside the state machine, like a drawn one: once
`WORKLOAD_EVENTUALLY_AFTER_SECS` passes, the timer only sets a flag, and the
next rule to start runs the eventually from its Hegel worker. The timer runs
it itself only if no rule has started it 600 s later, when the machine has
ended. Once faults stop, every rule halts its thread for good instead of
returning, and the process ends through the eventually's exit. A rule that
returned would let Hegel's concurrent engine finish the round and schedule
the next one, every rule a no-op, recording each step as fast as four
workers ask for rules: the driver grew at about 240 MB/s until it was
OOM-killed. A started flag makes the eventually exclusive, so a drawn and a
forced one never both run.

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

### Token reads across the lifecycle

A commit token promises read-your-write: a read with the token returns the
write's records whatever happened to the objects holding them since. The
catalog resolves a token by an exact GET of its commit record, and once that
record is gone, through the compaction record that names it
(docs/catalog-and-mvcc.md, steps 4 and 5). Each of those branches is a place
where a stale answer could come back: the obsolete input beside its
compacted replacement, or nothing. `token_lifecycle_read` and the
`eventually` read single writes back at each state of their input's
lifecycle and judge every answer exactly.

Which writes: acked live writes whose model allows one stored copy at most
(`Write::max_copies() == 1`, a single attempt, acknowledged). For those the
model's bounds are exact, so every record must come back exactly once. A
retried write may hold two copies and is never read here.

The read: `POST /api/v1/sql` on one node, counting `(wl.write, wl.rec)` rows of
that write only, over its event time plus and minus 10 minutes, with every
token of its acknowledgement as `min_commit_token`. The verdict comes from
the ingest-durability model (`judge_logs` with the write expected): no
phantom, no record more than once, none missing. A 200 with no rows is a
verdict and fails. A 503 carrying the stable message `requested commit token
is not yet visible; retry` (an unsatisfiable token) is retried once after
2 s, as the message asks; refused again, it is a verdict and fails, since an
acknowledged token always resolves while no retention is configured. Every
other failure (no response, another 503, 504, 500, 422, 400, an unparsable
body) gives no verdict: it is counted by class in the details
(`tally`) and in the driver log (`token reads by outcome:`), never passed.

The state of each token's input, decided from what the store shows, never
from a Ravel resolver of the read itself. A token decodes (base64url of
`v2:<shard>:<writer>:<epoch>:<seq>:<hour>`, `CommitToken::encode`) to its
commit record key, the same `<writer>.<epoch>.<seq:020>.cmt` key a compaction
record's decoded input names. Before the read the driver LISTs the token's
bucket (`l/c/<shard>/<hour>/`), decodes every compaction record there it has
not decoded (`commit decode-compaction`), and checks the commit record (in
that LIST, else a GET). After the read it GETs the commit record again.

| State | Evidence |
|---|---|
| before compaction | No decoded compaction record names the input. |
| compacted, commit record present | A compaction record listed before the read names the input, and the commit record was still in the store after the read, so throughout it: deleted keys never come back. |
| HEAD named the compaction output | The last audit whose HEAD held still (its `created_unix_ns` the same before and after) has a watermark at or above the input's hour, so HEAD alone serves that hour, and its `rlog footprint` resolved an output part of the covering record (`l/l1/<shard>/<hour>/<input_set_hash16>.`) and no data object of the input. This is the one state whose evidence goes through `ravel-cli`'s catalog resolve: no command prints a part's entries ("Missing decoder" below). The claim is that HEAD had named the output before the read; `corrupt_head` takes only the read gate too, so HEAD may be garbage during the read itself. |
| input commit record swept | A compaction record names the input, and the commit record was absent before the read (absent from the LIST and a GET found nothing). On a node that never cached the record, the read takes the catalog's fallback, where the SUT evaluates `commit: a commit token never resolves to a stale answer` with the answer `superseded by compaction`. A node that read the record before (every node lists every unsealed hour's records) serves the token from its cache, so only a restarted node gets there: in the local loop, the eventually reads its swept writes on the node it had `local-run.sh` restart. |
| node restarted since the write | The read's node acknowledged one write after this one was acknowledged (or acknowledged this one), and a later write, sent after that answer arrived, routed at the same shard count under another writer identity. A ravel-server's log router builds one shard set per shard count, the first time that count routes, and each set draws its own writer identity for the life of the process, so a new identity at one count is a new process. A reshard rebuilds the set without a restart, so the count each acknowledgement was routed at is part of the evidence: the count the provisioning record gives the send's hour, the answer's hour and every token's hour alike, read as of the driver's last read of the record. An attempt whose hours route at two counts, or lie past what that read can prove (its hour and the next, since a reshard activates two or more hours after it is appended), counts for neither side. One counter orders every attempt's send and answer in real time. |

With `twice` the same tokens are read again at once on the same node: the
second read finds whatever the first one cached there (a present commit
record, the bucket's compaction records). A swept commit record is not
cached, so the second read of a swept input on a restarted node takes the
fallback again.

How this closes #4490: the ingest-durability driver reads at "now", so the
listing path caches every commit record before the token loop and the
fallback never runs; nothing there compacts or sweeps. Here the same
caching holds: a ravel-server keeps every commit record it read, and both
nodes read every live record while its hour was unsealed, so a read of a
swept input on a node that has run since then is served from that cache and
never GETs the record. The fallback, and with it `assert_token_never_stale`
and the SUT's `commit: a commit token's stale-answer evidence read
evaluated`, runs only for a read of a swept input on a node with a cold
cache: a node restarted after the hour was folded (by a fault in
Antithesis, by the eventually's requested restart in the local loop). Reads
after HEAD named the compaction output run `include_token_l0` past its
early return on any node. `local-run.sh` requires the SUT assertion in a
plain run.

### The store audit

1. `catalog inspect --json` gives the HEAD key and its `created_unix_ns`.
2. `catalog inspect` (text) prints every key HEAD names: each snapshot part
   (`part[i] ... key=`), its column statistics object (`column_stats (field
   7): key=`) and each key-index leaf (`field=... key=`). It then fetches
   and decodes every part. A part it cannot fetch fails the command with
   `failed to fetch part <key>: object not found`, and the audit counts
   that key as named. A part fetch failing for any other cause gives no
   verdict for that pass.
3. `ravel-cli rlog footprint --tenant cm-tenant --json` lists
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
   is corrupt or absent, the audit skips steps 6 and 7. Otherwise, when
   `rlog footprint` succeeded, its keys and HEAD's watermark become the HEAD
   evidence a later token read decides "HEAD named the compaction output"
   from.
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
| `catalog: a token read returns each acked record of its write exactly once` | always | `token_lifecycle_read`, `eventually` | Every token read that gave a verdict returned each record of the write exactly once and nothing else, and was not refused as unsatisfiable twice ("Token reads across the lifecycle"). Reads without a verdict are counted, not judged. |
| `catalog: a token read ran after its input was compacted` | sometimes | `token_lifecycle_read`, `eventually` | A read gave a verdict while a compaction record named one of the write's inputs and that input's commit record was still in the store. |
| `catalog: a token read ran after HEAD named the compaction output` | sometimes | `token_lifecycle_read`, `eventually` | A read gave a verdict after a stable HEAD served one of the write's inputs from the covering record's output. |
| `catalog: a token read ran after its input commit record was swept` | sometimes | `token_lifecycle_read`, `eventually` | A read gave a verdict while a compaction record named one of the write's inputs and that input's commit record was gone. |
| `catalog: a token read ran twice on one node` | sometimes | `token_lifecycle_read`, `eventually` | Both reads of a `twice` pair gave a verdict. |
| `catalog: a token read ran on a node restarted since the write` | sometimes | `token_lifecycle_read`, `eventually` | A read gave a verdict on a node that acknowledged under a new writer identity after the write. A fault restarts nodes in Antithesis; the local loop restarts `ravel2` once (see `local-run.sh` below). |
| `catalog: a sealed seeded hour was folded into HEAD` | sometimes | every `catalog inspect --json` | A part of HEAD with entries covers some seeded hour (inside its `min_hour..=watermark_hour`, not retired). |
| `clock: an hour ingested live was sealed by a fold` | sometimes | every `catalog inspect --json` | A part of HEAD with entries covers the hour of some acked live write. |
| `catalog: a compaction record was published` | sometimes | `audit` | A listed `l1.*.cmt` record decoded through `commit decode-compaction`. |
| `catalog: sweep deleted a superseded compaction input` | sometimes | `audit` | Evaluated once an audit holds a decoded compaction record: a commit record or data object of one of its inputs was in an earlier LIST, is absent from this one, a GET finds it absent, and no earlier audit counted it. |
| `catalog: the audit checked the data objects the catalog resolves` | sometimes | `audit` | `rlog footprint` reported at least one data object. |
| `catalog: a listed commit record decoded through ravel-cli` | sometimes | `audit` | A listed level-0 commit record decoded through `commit decode`. |
| `catalog: a server fold lost its HEAD CAS to a fold one sealed hour ahead` | sometimes | `race_folds`, `eventually` | The skewed `ravel-cli` fold rebuilt HEAD with the hour ahead sealed, and the store refused the held server HEAD PUT (`held-result` reports 412 or 409), its version replaced by the skewed fold. |
| `catalog: a server's compaction record PUT lost to a concurrent compaction` | sometimes | `race_records`, `eventually` | The record key the held server PUT names was in the store before the driver released that PUT, the PUT was released within the 18 s hold cap, and `held-result` reports 412 or 409 for that key. |
| `catalog: HEAD stayed unreadable while the clocks passed the protection horizon` | sometimes | `sweep_unreadable_head`, `eventually` | HEAD still held the sweep's own garbage after the clocks moved past the protection horizon and the 150 s wait for the servers' sweeps. Server HEAD PUTs are refused throughout, so this alone does not show a server swept; `local-run.sh` also requires the SUT's `catalog: delete pass refused on an unreadable HEAD`, hit when a server's sweep reads the undecodable HEAD and stops. |
| `catalog: the clocks passed a compaction record's protection horizon and pinned-query window while HEAD had not folded it` | sometimes | `sweep_unfolded`, `eventually` | HEAD's bytes were the same after both jumps and the servers' sweeps as before the compaction, the record was still there, and the store held at least one of its inputs before the jumps. |
| `clock: the driver's clock jumped forward` | reachable | `jump_clock`, `eventually` | The jump reached the driver's own clock. |
| `clock: the clocks jumped past the sweep's protection horizon and pinned-query window` | reachable | `jump_clock`, `eventually` | Both sweep jumps reached the driver's own clock. |
| `catalog: an on-demand sweep succeeded` | reachable | `sweep`, the sweep jumps, `eventually` | `maintain sweep` exited 0. |
| `catalog: the workload starts` | reachable | startup | |
| `catalog: an on-demand fold succeeded` | reachable | `fold` | |
| `catalog: HEAD was overwritten with garbage through raw S3` | reachable | `corrupt_head` | |
| `catalog: inspect reported a corrupt HEAD` | reachable | every `catalog inspect --json` | |
| `clock: the driver could not write the clock offset` | unreachable | `jump_clock`, the sweep jumps | |
| `clock: the clock jump did not reach the driver` | unreachable | `jump_clock`, the sweep jumps | The driver's clock did not move by the jump within one reread plus 30 s. |
| `clock: HEAD changed while a clock move landed` | unreachable | every clock move | HEAD differed after the move landed from the HEAD that held still before it ("HEAD holds still before every move"), so a delete after it can break the gate's TTL premise. A harness condition, not a Ravel defect. |
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

### HEAD holds still before every move

A query process caches HEAD for `head_cache_ttl` (30 s) on its monotonic
clock, which no jump moves. ADR-1133's delete gate assumes a superseded
object stays in place, in real time, for at least that long after HEAD stops
naming it: the gate's pinned-query window (4830 s) counts the TTL in. A jump
makes that wall-clock window pass in seconds, so without a wait a sweep
could delete an input a server still served from its cached HEAD (#4571).
So before every move, the sealing jump, both sweep jumps and every
forced-race nudge, the driver:

1. has the proxy refuse every server's PUT of HEAD (`freeze-key`), so no
   fold moves HEAD meanwhile, and renews that rule every second, so a
   restarted proxy gets it back;
2. reads HEAD through raw S3 every second and waits until it has held still
   for `head_cache_ttl` plus the store client's 20 s request timeout (50 s).
   The second term covers a server's read of HEAD started just before HEAD
   changed, which can return the old HEAD that late and is then cached for
   the TTL. A changed or unreadable HEAD, or a freeze the proxy did not
   confirm, starts the wait again. After four such spans without one, or
   when no `catalog inspect` has shown the HEAD key so HEAD can be neither
   frozen nor read, the move is given up and nothing is written. The sealing
   jump and each sweep jump (the window jump on its own, once the horizon
   jump landed) are then taken by a later rule or the eventually, and a
   forced race reports not reached;
3. computes the move from its clock after the wait, so a nudge aimed at a
   window still lands in it, writes the offset as above, and keeps HEAD
   frozen until one more reread passed, so every process moved before any
   fold can move HEAD again (`thaw-key`).

The driver sends no query across a move, since every move runs under the
write gate. The wait does not cover a query that the driver's 30 s client
timeout gave up on but that its server is still running. Each move's
`clock: jumped` line records `head_quiet_s` and whether a read after the
landing found HEAD changed anyway (`head_moved_while_landing`, null when
either read failed; only a proxy restarted inside the landing's 3 s lets a
fold through then). A change also fails the unreachable assertion `clock:
HEAD changed while a clock move landed`, so a cloud run attributes what
follows to the harness. `local-run.sh` fails a run with a move whose
`head_quiet_s` is below the span or whose `head_moved_while_landing` is not
false, and checks that there is one such line per move. The wait
adds about 56 s to each move, so up to 18 moves add about 1000 s to an
eventually.

The TTL comes from the server configuration. No flag in
`config/docker-compose.yaml` and no `gc-config set` in `ravel/init.sh`
overrides it, so every server runs the compiled default, which `sys/gc`
records (format version 1 records none, and `gc-config show` prints the
compiled one). The compose states that value to the driver as
`HEAD_CACHE_TTL_SECS`. The driver refuses to start if it is not the TTL the
sweep jumps were sized with, and, once every node serves, if `ravel-cli
gc-config show` reports another. A clock test checks that the compose and
`init.sh` still set no other TTL.

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
3. folds, then sweeps every shard below the shard ceiling, which writes the
   markers;
4. moves every clock forward by `CLOCK_SWEEP_WINDOW_JUMP_SECS` (config
   22200 s), the same way.

Each move first holds HEAD still for 50 s ("HEAD holds still before every
move"). For the window jump that wait is what keeps the gate's premise: the
fold in step 3 is the one that stopped naming the inputs, and the jump after
it is what lets their markers' window pass, so the inputs stay in place for
at least the TTL and a late HEAD read after HEAD stopped naming them. Before
#4571 the window jump followed the fold by a few seconds.

The window jump passes the interior re-verify interval (21600 s) as well as
the pinned-query window. A maintain server sweeps the whole keyspace only
when that long passed since its last full pass, and only a full pass reaches
the hours the horizon jump left behind. The servers' first pass after the
horizon jump is a full one, and it now runs during the wait before the
window jump, while the markers are still inside their window. A 5400 s
window jump would leave their next full pass 6 h of wall time away, so only
the driver's own `ravel-cli` sweep would delete the inputs. That sweep
reports no SDK assertions, so `catalog: a sweep never deletes an object the
current HEAD names` would go unevaluated in a timeline whose sweep jumps
are its last moves. For the same reason the driver, still holding its write
gate after the window jump, waits up to 150 s (the wait the unfolded sweep
gives the servers' sweeps) for a LIST to show a superseded input gone before
any sweep of its own can run, and prints how long that took. Only an input
the LIST taken just before the window jump still held counts, so one
deleted earlier in the timeline cannot end the wait.

The next sweep, from `maint1`/`maint2`, the `sweep` rule or the
`eventually`, deletes the inputs. The driver refuses to start if either
sweep jump is below its term (90300 s, and 21600 s for the window jump),
computed from the shipped defaults; no grace or horizon flag is passed
anywhere, so the run deletes under the shipped `sys/gc` defaults. Each jump is as far beyond SigV4's 900 s
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
stamps on a probe PUT) read at least the total of the three jumps and every
forced-race move ("The forced races") minus 60 s ahead of the host.
The last line is `clock jump: PASS` or `clock jump: FAIL`.

## The forced races

Four mutants change what a server does only in a situation that faults
and the planned jumps alone rarely or never produce: `lost-cas-proceeds`
needs a fold that loses its HEAD CAS to a HEAD it did not build,
`compaction-loser-overwrites` needs a compaction record PUT that loses to a
concurrent compaction of the same bucket,
`sweep-superseded-no-head-gate` needs a full sweep pass past a compaction
record's protection horizon and pinned-query window while HEAD has not
folded the record, and `corrupt-head-ignores-delete-gate` needs a server
sweep past the protection horizon while HEAD does not decode. Each mutant's
property is evaluated only there, so a run that never gets there passes the
mutant (ADR-4326 decision 3). The driver forces each situation with a rule
(`race_folds`, `race_records`, `sweep_unfolded`, `sweep_unreadable_head`),
runs each again in the `eventually` (the first three until reached once, the
unreadable-HEAD sweep once), and asserts reaching it as a
sometimes-assertion that `local-run.sh` requires in a plain run.

Every step is something the system must survive anyway: every clock moves
forward together, never back; a `ravel-cli` maintenance run happens
concurrently with the servers', which the CLI documents as safe; a store
answers some PUTs late or refuses them. Each rule holds the driver's write
gate throughout, so no live write and no planned jump lands inside a race.
Each clock move outside the planned jumps prints `clock: moved forward by
<N> s outside the planned jumps (<reason>)`, and the local loop adds those
moves to the offset it expects.

### The fold race (`lost-cas-proceeds`)

1. Move every clock forward so the next fold seal is 150 s away (60 to
   225 s is the race's window).
2. PUT garbage over HEAD through raw S3, so every fold rebuilds from the
   listing and the on-demand fold route passes its rate gate.
3. `s3proxy`: `pass`, then `hold-key <HEAD key>`: the next PUT of the HEAD key
   from any server is held, unsent, until `release` or 18 s.
4. `POST /api/v1/admin/fold` on one `--mode all` server (alternating by race
   number). It reads the garbage HEAD, rebuilds, and sends its HEAD CAS PUT,
   which the proxy holds. Poll `held` (bounded, well under the hold cap) until
   it names the HEAD key; a background scheduled fold's HEAD PUT held first is
   the same race. If none is held in time (the server answered first: nothing
   eligible, or the on-demand rate gate), `pass` and report the race not
   reached.
5. Run `ravel-cli catalog fold --json` to completion with its clock 240 s ahead
   of every other clock (`FAKETIME=+<offset + 240>`), inside the 300 s
   `clock_skew_allowance` the seal margin is sized for: it seals the hour that
   seals next, which the server did not, so it rebuilds a different HEAD. The
   CLI reaches rustfs directly, not through the proxy, so its CAS lands while
   the server's PUT is held, replacing the garbage version.
6. `release`. The held server PUT reaches rustfs after the skewed HEAD landed;
   its CAS was conditioned on the garbage version, now gone, so rustfs answers
   `412 Precondition Failed` (or `409`). Clean code reports `lost_cas` and
   retries; the mutant proceeds as if it had won, and the SUT's `catalog: a
   fold reports as folded only a HEAD the store holds` goes false. Read
   `held-result`, then `pass`.
7. A server fold lost to the skewed fold when the CLI rebuilt with the hour
   ahead sealed and `held-result` reports a 412 (or 409) for the HEAD key; a
   200 never counts, and the server's own answer (`lost_cas`, or `published`
   under the mutant) is kept only in the details. Asserted as `catalog: a
   server fold lost its HEAD CAS to a fold one sealed hour ahead`, and the
   SUT's own `catalog: fold HEAD CAS lost` is required too.

Why legitimate: a clock 240 s ahead is inside the skew the seal margin
allows, so the skewed fold seals nothing an acknowledged write can still land
in. A store answering one PUT late, below the server's 20 s request timeout,
while a concurrent fold the skew allowance permits publishes, is a delay the
system must survive; two folds of one HEAD racing their CAS is the TLA model's
fold interleaving.

### The record race (`compaction-loser-overwrites`)

1. Find a `(hour, shard)` bucket of the current or previous hour with two or
   more level-0 commit records and no compaction record, after up to 16 live
   writes if none has one.
2. `s3proxy`: `pass`, so no rule is set while the clocks move.
3. Move every clock forward until that hour is sealed for compaction (hour
   end + `max_flush_lifetime + clock_skew_allowance`, 3900 s, plus 30 s),
   then settle: wait until the move reached the driver's clock, then 3 s
   more (`FAKETIME_CACHE_DURATION`, 2 s, plus 1 s), so it reached every
   faked clock, rustfs's included. A PUT signed on a clock the move had not
   reached yet would be refused for skew (`403 RequestTimeTooSkewed`), which
   is not the race.
4. `s3proxy`: `hold-record`: the next PUT of a compaction record key from any
   server is held, unsent, until `release` or 18 s. `maint1`/`maint2`
   compact the bucket and send the record's create-only PUT, which the proxy
   holds.
5. Poll `held` every 250 ms for up to 120 s. A held PUT of another key, or
   one older than 6 s when first seen, is released and the next one held.
6. Run `ravel-cli maintain compact-bucket --shard <s> --hour <h> --no-claim`
   on the held record's bucket. It plans from the same sealed inputs and
   publishes the same record key with its own `created` time.
7. Once the CLI finished or the held PUT is 14 s old, GET the record key,
   then `release`. The server's create-only PUT reaches rustfs after the
   CLI's, and rustfs answers `412 Precondition Failed` (or `409`); clean code
   GETs the winner and converges, the mutant PUTs its own bytes over it, and
   the SUT's `catalog: a published compaction record is never replaced` goes
   false. Read `held-result` until it names the held key.
8. `pass`. Asserted as `catalog: a server's compaction record PUT lost to a
   concurrent compaction` when the CLI's record was in the store before the
   release, the held PUT was released no older than the 18 s hold cap, and
   `held-result` reports 412 or 409 for the held key. A 200 (the server won),
   a 403 or 5xx (the store refused the PUT for another reason), or no
   result never counts. The details keep `published_before_release` and
   `held_status`. The `eventually` runs up to five record races.

Why legitimate: the hold is a network delay of one request after the
sender's last LIST, shorter than the server's 20 s request timeout. Two
compactions of one sealed bucket racing to its create-only record PUT is the
interleaving the TLA model and `--no-claim` both allow: the record's
create-if-absent decides the winner.

### The unfolded-record sweep (`sweep-superseded-no-head-gate`)

1. Find a compactable bucket as above.
2. `s3proxy`: `pass`, then `refuse-records`: every server's compaction record
   PUT answers `503 SlowDown`.
3. Move every clock forward until the hour is sealed for the fold (hour end
   + 4800 s, plus 30 s), fold through `ravel-cli` until HEAD's watermark
   reaches the hour, and check the bucket is still compactable (the servers'
   compactions published nothing).
4. `s3proxy`: `refuse-key <HEAD key>`: every server's HEAD PUT answers 503,
   so no server fold can fold a new record into HEAD. GET HEAD's bytes.
5. `ravel-cli maintain compact-bucket --no-claim` publishes the bucket's one
   record. Decode its inputs (`commit decode-compaction`) and LIST the
   tenant prefix.
6. Move every clock forward by `CLOCK_SWEEP_HORIZON_JUMP_SECS` (91200 s),
   past the record's protection horizon, and wait 150 s: the servers' sweeps
   pass the horizon. Clean code finds the inputs named by HEAD and keeps them;
   the mutant takes the HEAD gate as clear and writes their unnamed-since
   markers.
7. Move every clock forward by 22200 s: past the 4830 s pinned-query window
   since those markers, and past `DEFAULT_INTERIOR_REVERIFY_NS` (6 h), so the
   next pass of each `maint` process is a full-keyspace sweep that reaches the
   record's hour, now interior. Wait up to 150 s, ending early when an input
   is gone. The mutant deletes one, and the SUT's `catalog: a sweep never
   deletes an object the current HEAD names` goes false.
8. Assert `catalog: the clocks passed a compaction record's protection horizon
   and pinned-query window while HEAD had not folded it` when HEAD's bytes did
   not change, the record is still there and the store held some input
   before the jumps. Run the store audit, whose `catalog: every object HEAD
   names exists` also catches a deleted input. Then `pass`.

Why legitimate: refused PUTs are store faults the servers must tolerate, the
clocks only move forward, and the CLI compaction is an ordinary concurrent
compaction. The deleting pass is the servers' own scheduled sweep. Clean code
keeps every input, because HEAD still names it.

### The unreadable-HEAD sweep (`corrupt-head-ignores-delete-gate`)

1. Take the known HEAD key. `s3proxy`: `pass`, then `refuse-key <HEAD key>`:
   every server's HEAD PUT answers 503, so no server fold can replace the
   garbage below.
2. PUT garbage over HEAD through raw S3, numbered per sweep (`antithesis:
   not a catalog HEAD (unreadable-HEAD sweep <n>)`). LIST the tenant prefix:
   the snapshots under `catalog/<signal>/snap/` and the superseded inputs of
   every decoded compaction record it still holds go into the details.
3. Move every clock forward by `CLOCK_SWEEP_HORIZON_JUMP_SECS` (91200 s),
   past the sweep's protection horizon, and wait 150 s: the servers'
   scheduled sweeps run with every candidate past the horizon and HEAD
   undecodable. Clean code refuses both delete passes (`catalog: delete pass
   refused on an unreadable HEAD`); the mutant reads the garbage as naming
   nothing, and the SUT's `catalog: a catalog sweep never deletes under an
   unreadable HEAD` or `catalog: a delete gate never clears under an
   unreadable HEAD` goes false.
4. GET HEAD and LIST the tenant prefix again; the snapshots and inputs gone
   since step 2 go into the details. Assert `catalog: HEAD stayed unreadable
   while the clocks passed the protection horizon` only when HEAD still holds
   this sweep's garbage. With server HEAD PUTs refused that alone does not
   show a server swept; the SUT's `catalog: delete pass refused on an
   unreadable HEAD`, which `local-run.sh` requires beside it, does. A HEAD
   rebuilt during the wait means the sweeps may have read a valid HEAD.
5. `pass`, then `ravel-cli catalog fold` rebuilds HEAD from the listing, and
   `catalog inspect` reads it.

The rule `sweep_unreadable_head` runs this at most once per timeline, and
the `eventually` runs it once more after the unfolded-record sweep.

Why legitimate: a HEAD that does not decode is a state the catalog must fail
closed on (`CorruptHeadFailsClosedOnDeletePaths`), the same garbage
`corrupt_head` writes; refused PUTs are store faults; the clocks only move
forward. The deleting pass is the servers' own scheduled sweep, and the
mutant is selected only in the four ravel-server containers, so any delete
comes from them. Clean code deletes nothing while HEAD is unreadable.

The proxy (`driver/src/proxy.rs`, `driver/src/bin/s3_proxy.rs`) forwards
every byte unchanged, Host header included, so each SigV4 signature still
verifies at rustfs, and parses only request heads, to frame bodies and to
match a PUT's key against the rules: `hold-record` and `hold-key <key>` hold
one PUT until `release`, and `refuse-records`/`refuse-key <key>` answer it 503
unsent. A held PUT, under either hold rule, is forwarded on a dedicated
upstream connection so the proxy can read its response status, and
`held-result` reports that `<key> <status>`, which the fold race and the
record race read to tell a lost CAS or create (412/409) from a win (200) or a
refusal for another reason (403, 5xx). Setting either hold rule clears the
last result. Only PUTs are judged. A killed or restarted proxy has no rules, so a
race it interrupts leaves the servers on the plain path and the race reports
not reached.

Each relayed connection runs on two threads, and a network fault can leave a
peer that never sends, reads or closes, so the proxy bounds both (#4568). It
relays at most 512 connections at once, four times the four servers'
fault-free peak of about 130 (264 threads, sampled every 2 s over a plain
local loop). Past the cap, or when a thread cannot be spawned, it answers
`503 SlowDown` unread and closes, as for a refused PUT, and the servers'
store client retries. Every relayed and control socket times out a read or
write blocked for 60 s and drops the connection. That is above the servers'
20 s store request timeout, which bounds every wait inside a request (a held
one included), and above their 30 s pool idle timeout, which bounds every
wait between requests, so only a connection its server no longer uses is
dropped. The control port takes at most 8 connections and answers one more
`error: busy`.

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

The workload's scripts also select the mutants of two more areas (ADR-4326
decision 3), over the same `-mutants` images. `scripts/mutant-tsv.sh` lists the
three areas, `catalog`, `resharding` and `maintenance`, and reads each one's
names from `formal/tla/<area>/antithesis-mutants.tsv`, column 2 of a line
whose column 2 starts with `<area>/` (a `none:` line names no mutant):

| Area | Mutants |
|---|---|
| `catalog` | the five above |
| `resharding` | `resharding/no-writer-fence`, `resharding/token-validated-against-count` |
| `maintenance` | `maintenance/mo-diverge-overwrites-record`, `maintenance/mo-missing-part-reports-converged` |

A name `<area>/<stem>` selects its area by the part before `/`: the scripts
accept it when that area's `.tsv` lists it, and `local-run.sh` reads its
property from that same `.tsv`.

The scripts accepting a name does not make its mutant falsifiable here. This
profile never reshards (the driver seeds 4 shards and never changes the
count), and a `resharding/` mutant changes behaviour only across a shard-count
change, so it changes nothing a timeline here can see. The record race
does make a server a compaction loser, but over the same inputs with the
winner part present, so the situation a `maintenance/` mutant needs (a
divergent input set, or a missing winner part) never arises. Either way the
mutant's property is never falsified, and a local or Antithesis run of it
exits 1 at the end. Do not launch one before the reshard profile and its
forced races (#4340) land.

The corrupt-HEAD mutant switches off the HEAD guard of both delete passes,
and each pass asserts `CorruptHeadFailsClosedOnDeletePaths` under its own
literal, so either literal going false counts. The `.tsv` names the first.

Only the four ravel-server containers run a mutant. The driver's on-demand
fold, compaction and sweep go through `ravel-cli`, which no image builds
with `antithesis-mutants`, so a mutant fires only in the servers' scheduled
folds (all four) and in `maint1`/`maint2`'s compaction and sweep. The corrupt-HEAD mutant
needs a server sweep past the protection horizon while HEAD does not decode;
the driver's `corrupt_head` rule landing before a scheduled sweep after the
sweep jumps ("The sweep jumps") rarely gets there, so the unreadable-HEAD
sweep forces it. The lost-CAS, record-overwrite and sweep mutants need the
other situations "The forced races" put the servers in.

`MUTANTS=1 scripts/build.sh` builds the mutant images:
`ravel-antithesis-server:catalog-maintenance-mutants`, whose ravel-server is
built with `--features sql,antithesis,antithesis-mutants` (the Dockerfile's
`SERVER_FEATURES` build argument), and
`ravel-antithesis-driver:catalog-maintenance-mutants`, built over it. Both
carry the `org.ravel.antithesis.ref` label, and the server image also
`org.ravel.antithesis.features`. Besides the instrumentation check, the build
checks that the server binary registers every mutant name of the three areas
and refuses unless the per-area counts are exactly catalog 5, resharding 2 and
maintenance 2, the mutants ADR-4326 decision 3 ships. The default build (no
`MUTANTS`) is unchanged: features `sql,antithesis`, tag
`catalog-maintenance`.

`RAVEL_ANTITHESIS_MUTANT=<area>/<stem>` selects a mutant for `local-run.sh`
and `launch.sh`. Both pass it to `scripts/mutant-config.sh`, which writes a
copy of `config/` where every workload image names its `-mutants` tag and the
ravel-server environment's `RAVEL_ANTITHESIS_MUTANT: ""` names the mutant.
It refuses, and so both scripts refuse:

- a name that is not one of the nine mutants the three areas' `.tsv` files
  list, including a `none:` stem and a name of another area such as
  `lifecycle/` or `commit/` (the message lists the nine);
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

## The expected set

The ravel-server binary in the server image carries every area's
assertions, not only the catalog's: the `commit: ` always-type assertions
are registered in it, and Antithesis counts one that no process
reached as failing. This workload drives the catalog area only, so it is
judged against the areas it owns (ADR-4326, per-workload amendment):

- the areas whose mutants `formal/tla/catalog/antithesis-mutants.tsv` names
  (the part of the mutant column before `/`, today `catalog`), matched as the
  literal prefix `<area>: `;
- every assertion the workload's own driver declares (`sdk-driver.jsonl`),
  which covers the `clock: ` literals.

The prefix is the exact test: every catalog literal in the SUT
(`crates/ravel-catalog/src/fold.rs`, `crates/ravel-maintain/src/{publish,
sweep,reachability}.rs`) starts with `catalog: `, and every driver literal
with `catalog: ` or `clock: `. The sixth column of
`formal/tla/catalog/traceability.md` is not: it names only the properties
that carry a TLA invariant, not the reachability and exploration assertions
around them.

`local-run.sh` splits `never-hit.tsv` into `never-hit-owned.tsv`, each line
a gate failure, and `never-hit-other-areas.tsv`, printed under "not this
workload's" and never a failure.

Triage of an Antithesis report applies the same rule. An always-type
assertion that "was never hit" and starts with neither an owned prefix nor
`clock: ` (the `commit: ` ones, for example) is another workload's to reach;
note it and move on. A violated assertion is a finding whatever its area,
since a false evaluation means the code reached it, and so is an owned one
never hit.

## Build, run locally, launch

The same steps as ingest-durability, from this directory's `scripts/`:

- `scripts/build.sh [server|driver|all]`. The driver's build context is
  `antithesis/` and it copies `ravel-cli` from the server image, so build the
  server first. The driver image also carries `s3-proxy`, which the
  instrumentation check covers like `driver`, and `ravel-cli-ant`, which it
  covers too: ravel-cli built with its `antithesis` feature and the coverage
  flags (`ravel/Dockerfile`, stage `cli-ant`), never with
  `antithesis-mutants`. Only ravel-cli appends a resharding generation, so
  only an instrumented one reaches those assertions. The stock `ravel-cli`
  stays uninstrumented and runs every call above. The driver runs
  `ravel-cli-ant` only through `AntCli` (`driver/src/cli.rs`), which points
  its SDK output at `/tmp/sdk-cli.jsonl`; no rule calls it yet.
- `scripts/local-run.sh <out-dir> [seconds]` starts the stack under the
  run's compose file, runs the workload for `seconds` (default 240) before
  the driver's forced `eventually`, and exits 0 only if all of these hold:
  - `faketime` and `init` exited 0, and setup completed within 900 s;
  - `ravel2` was restarted once (`docker restart`, `node-restart.txt`) when
    the eventually asked for it. The local compose sets
    `WORKLOAD_LOCAL_RESTART_NODE=ravel2`; with it, the eventually prints
    `eventually: local loop: restart http://ravel2:4318 now` before its
    token reads after the sweep, waits up to 300 s for `local-run.sh` to
    restart the node and create `/tmp/local-restart-done` in the driver
    container, waits for every node to serve, writes once to `ravel2` (its
    new writer identity is the restart evidence), and then reads every
    swept write on `ravel2`. A ravel-server keeps every commit record it
    read in memory, and both nodes read every live write's record while its
    hour was unsealed, so without a restart no read GETs a swept record and
    the token fallback never runs. In Antithesis the variable is unset and
    the faults restart nodes;
  - the driver was still running after setup, then exited 0 having logged
    `eventually: ending the timeline`;
  - every other container (`s3proxy` included) is running or exited 0;
  - the offset file holds the total of the three jumps and every move the
    driver's log reports as `clock: moved forward by <N> s outside the
    planned jumps`, and every clock probe read a number at least that total
    minus 60 s ahead of the host (`clock-probe.txt`);
  - every container's SDK output was collected. The driver container's
    `/tmp/sdk-cli.jsonl`, from `ravel-cli-ant`, is collected as
    `sdk-cli.jsonl` when it exists, and its absence is no failure. It is
    judged with the others below, but never counts as the driver's own
    assertions;
  - no always-type assertion was violated (`violated.tsv`), and none of this
    workload's that must be hit was never hit (`never-hit-owned.tsv`; see
    "The expected set" below);
  - `catalog: the workload starts`, `clock: the driver's clock jumped
    forward`, `catalog: a sealed seeded hour was folded into HEAD`, `clock:
    an hour ingested live was sealed by a fold`, `catalog: a compaction
    record was published`, `clock: the clocks jumped past the sweep's
    protection horizon and pinned-query window`, `catalog: an on-demand
    sweep succeeded` and `catalog: sweep deleted a superseded compaction
    input` each held, and so did each forced race's situation ("The forced
    races"): `catalog: fold HEAD CAS lost`, `catalog: a server fold lost
    its HEAD CAS to a fold one sealed hour ahead`, `catalog: a server's
    compaction record PUT lost to a concurrent compaction`, `catalog: the
    clocks passed a compaction record's protection horizon and pinned-query
    window while HEAD had not folded it` and `catalog: HEAD stayed
    unreadable while the clocks passed the protection horizon`, together with
    the SUT's `catalog: delete pass refused on an unreadable HEAD`, which
    shows a server's sweep read that HEAD and stopped;
  - the token reads reached every state: `catalog: a token read ran after
    its input was compacted`, `catalog: a token read ran after HEAD named
    the compaction output`, `catalog: a token read ran after its input
    commit record was swept`, `catalog: a token read ran twice on one node`
    and `catalog: a token read ran on a node restarted since the write`, and
    the SUT's `commit: a commit token's stale-answer evidence read
    evaluated`, which holds only when a token read took the catalog's
    fallback.
- The local loop's bound on the driver's run after setup is `seconds` plus
  4800 s: the `eventually` runs the forced races before its fold.
- `WORKLOAD_INJECT_SWAPPED_RECORD=1 scripts/local-run.sh <out-dir>` is the
  self-check. The model expects one seeded record moved to another write, so
  `catalog: compaction preserves the acked multiset` must appear in
  `violated.tsv`, and no assertion other than it and `catalog: no scan shows
  a phantom or an excess copy` may. The other gates still apply.
- `WORKLOAD_INJECT_STALE_TOKEN_RESULT=1 scripts/local-run.sh <out-dir>` is
  the token-read self-check. Every token read that answers rows has its
  smallest returned record counted twice before the model judges it, as an
  obsolete input served beside its replacement would be, so `catalog: a
  token read returns each acked record of its write exactly once` must
  appear in `violated.tsv`, and no other assertion may. The other gates still
  apply. The two self-checks and a mutant run are separate runs.
- `RAVEL_ANTITHESIS_MUTANT=<area>/<stem> scripts/local-run.sh <out-dir>
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
# ok ravel-antithesis-server:catalog-maintenance-mutants: /symbols/ravel-server registers the 9 mutants (catalog 5, resharding 2, maintenance 2)

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
dependency. It names `reqwest` 0.13 (`blocking`, `json`) directly, for the
fold race's on-demand fold posts; the ingest-durability driver already
builds that version with those features, so the lock gains no new package.
