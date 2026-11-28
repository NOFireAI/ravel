# lifecycle-gc Antithesis workload

Selective subject erasure (ADR-0064), legal holds (ADR-0042) and the GC
that removes superseded and retired objects, run against live ingest under
faults. The driver judges every read with its own oracle: it knows which
subject each record carries and where, when each write was acknowledged, and
when each erasure request was submitted and observed complete. It never
calls a Ravel matcher, so a matcher that disagrees with the query path
(#4503) is caught by a read rather than agreed with. It is the lifecycle
area's profile in ADR-4326: the SUT's lifecycle assertions and four lifecycle
mutants run here (`formal/tla/lifecycle/traceability.md` and
`antithesis-mutants.tsv`).

## Deployment

The catalog-maintenance topology, and ravel-server built from that
workload's server Dockerfile (`sql,antithesis`, libfaketime and its scripts,
the maintain key) as `ravel-antithesis-server:lifecycle-gc`.

| Service | Role |
|---|---|
| `faketime` | Puts libfaketime, the run wrapper and the offset file (`+0`) on the shared volume, then exits. |
| `rustfs` | The object store, RustFS 1.0.0 glibc, strict durability. The only durable component. |
| `init` | Creates and qualifies the bucket, then exits. |
| `ravel1`, `ravel2` | `--mode all`: ingest, SQL, the scheduled fold every 15 s. Two tenants: `lc-tenant` and the retention tenant `lc-retire`. |
| `maint1`, `maint2` | `--mode maintain` every 20 s for both tenants: fold, compaction, the erasure rewrite, completion (`.done`) and `.dreq` sweep, the superseded-input sweep, and retention (`--retention-tenant lc-retire=246m`, the shortest window the ADR-0019 floor allows). Bucket claims on (the default). |
| `driver` | The workload. Excluded from faults; the only writer of the clock offset file. |

The server entrypoints wait for the driver's ready marker
(`antithesis/lifecycle-gc/ready`, through the image's `SEED_MARKER`), so no
server starts before the driver has checked that the bucket is fresh.

Profiles: `default` as above; `claims-off` runs maint2 with
`--maintain-claims off`, which is supported and known unsafe (#4449: a
compaction and an erasure rewrite of one bucket are then fenced only by the
pre-publish re-list). Use it to look for #4449, not as a pass/fail gate.

## Data model

- Subjects `s0`..`s5` may be erased; `keep-0` and `keep-1` never are.
- Every record carries a unique id: log attribute `wl.id`, or a metrics
  sample's value. Each record carries its subject (`user_id`) in exactly
  one place, drawn per record: the log record's attributes, its resource's
  or its scope's (each log record gets its own resource and scope), or for
  metrics the series label.
- A write is one attempt, never retried, so an acknowledged record must be
  stored exactly once. Only a strict 200 with no partial success and a
  commit token is an acknowledgement; buffered writes and every other
  outcome are indefinite (0 or 1 copies, never judged for presence).
- Events are ordered by one driver sequence counter, never by wall time.
- Victims of a request for subject S on a signal: every record of that
  signal with subject S whose write was strictly acknowledged before the
  request's first `erase submit` attempt started. A write acknowledged
  during the submit could land in an hour the request's scope did not
  cover, so it is left unconstrained, as are later and indefinite writes.
- Survivors: every strictly acknowledged record whose subject has no request
  on that signal in any state (a submit with an unknown outcome may still
  have erased its subject).
- The retention tenant's records are written and never read back: their
  hours exist to be retired. Each carries an event time 6600 s behind ingest
  time, inside the 2 h `max_ingest_lag`, so its hour expires soon after the
  sealing jump.

## Rules

| Rule | What it does |
|---|---|
| `write_logs`, `write_metrics` | One OTLP write of 1 to 6 records to a drawn node, 15 % buffered. |
| `write_retired` | One strict write of one log record to the retention tenant. |
| `read` | SQL scan of every workload record of a drawn signal on a drawn node, half with the latest writes' commit tokens; judged for the erasure property and the over-count property. |
| `erase` | Rarely, `ravel-cli erase submit --matcher user_id=<s>` with a drawn request id, retried with the same id while the outcome is unknown. At most 4 requests per timeline. |
| `observe` | LIST `t/<hex>/<sig>/del/`: a listed `.done` marks the request completed; a `.dreq` absent from the LIST and from a HEAD marks it swept. Sometimes also LISTs `c/` for `rw.*.cmt` rewrite records. |
| `hold_set` | Rarely, `ravel-cli hold set --signal <sig> --shard <n>` (at most 2 active), then LISTs the three held prefixes as the baseline. |
| `check_holds` | The hold property over every active hold. |
| `hold_clear` | Rarely, judges the holds once more, stops judging one held at least three maintain ticks, and clears it. |
| `jump_clock` | Rarely: once an erasure is acked and 20 writes are, the 9000 s sealing jump; after a `.done` is observed, a horizon jump (91200 s), at most twice from this rule. |
| `eventually` | Rarely (or after `WORKLOAD_EVENTUALLY_AFTER_SECS`): see below. |

Every rule first runs the forced eventually when its deadline has passed and
parks its thread for good once the eventually has started. A rule that
returned after that point would let hegel schedule no-op rounds and grow
without bound (the catalog-maintenance driver OOM, #4471 round 4). The
forced eventually has no thread of its own; it runs inside the first rule
that sees its deadline.

The eventually, all with faults stopped:

1. Wait for every node; submit one erasure per signal that has none (the
   erasable subject with the most acked records); make sure the retention
   tenant has an acknowledged write; judge the holds; take the sealing jump.
2. Wait up to 240 s for every request on an unheld signal to complete (a
   hold defers every `.done` on its signal); judge and clear every hold and
   wait for every request to complete, all before any horizon jump. A
   rewrite published more than the fold's 26 h reconcile window after its
   hour sealed is left to the sweep's targeted re-fold request, which a
   quiet system may never act on, so its `.dreq` stayed held for good.
3. The retention jumps: two jumps of 9000 s, three maintain ticks apart.
   After the first, every retention-tenant hour has expired and the
   maintain nodes tombstone it. The shipped sweep then leaves the bucket
   alone until its protection horizon (90300 s) has passed. A sweep that
   skips that gate (the `delete-before-horizon` mutant) goes on to write the
   bucket's unnamed-since marker once the fold drops it from HEAD, and the
   second jump ages that marker past the pinned-query window (4830 s) while
   the horizon stays more than a day ahead, so that sweep deletes inside the
   protection window.
4. Place a hold on logs shard 0, judge it once its grace has passed (so a
   held key gone later was deleted after the grace), and keep it across two
   horizon jumps (markers, then deletion past the pinned-query window),
   judging it after each. Before the second, the driver puts an object of no known
   bucket-entry shape into the tenant's legal-hold shard (`u/c/0000/`,
   beside a hold record), so every hold read fails closed until it removes
   the object two maintain ticks after the jump: the
   `refresh-failure-is-no-hold` mutant's situation, a failed hold read
   while there is something to delete. Then clear the hold.
5. Until every acked request's `.dreq` is gone (at most seven horizon jumps
   per timeline): wait for completion, jump past the horizon, wait for the
   sweep. These jumps also take the retention tombstones past their horizon,
   and the retention sweep deletes the retired hours.
6. Scan every node for each signal and read every survivor with its writes'
   commit tokens, then exit 0.

The `jump_clock` rule takes no horizon jump while a hold is active, for the
reason in step 2.

## Properties

| Literal | Type | Evaluated |
|---|---|---|
| `erasure: an erased subject is never returned after its request` | always | Every read issued after a request's ack: no victim of that request returned. The details split the hits by whether the driver had already observed the request's `.done` or `.dreq` removal when the read was issued (hits only after completion are the #4503 shape) and by placement. |
| `lifecycle: every acked survivor is read exactly once` | always | In the eventually: every node's full scan, and every token read of a batch of survivors with their writes' commit tokens, returns each survivor exactly once. A scan that gives no answer for two minutes, or a token read for 30 s, is no verdict: counted in the details and printed, never passed, as the catalog-maintenance driver counts its token reads. A signal no node could scan fails the property. |
| `lifecycle: no read returns a record more often than it was written` | always | Every successful read: no id more often than written (once), and no id the driver never wrote. |
| `gc: an object under an acked hold is not deleted` | always | Every hold check: a key in the hold's baseline LIST that is absent from a later LIST and absent on HEAD, last seen present at least `HOLD_GRACE_SECS` (120 s) after the hold's ack, fails. A delete batch whose hold re-read came before the ack may still delete (ADR-0042, the delete-batch re-read amendment's residual); the grace covers that batch. Its limit: with `HOLD_GRACE_SECS` 120 and `--maintain-interval-secs` 20, a maintenance build that judged holds from the per-tick snapshot (the #4501 defect) also deletes inside the grace, so neither this property nor the SUT's HeldObjectNeverDeleted (judged from the batch's own re-read) catches a #4501 regression; its regression guards are ravel-maintain's `tests/legal_hold_mid_pass.rs` and `tests/legal_hold_delete_batch.rs`, and a tighter oracle is tracked in a follow-up issue. |
| `erasure: an erasure request was acked` | reachable | `erase submit` exited 0 and printed the `.dreq` key. |
| `erasure: a rewrite record was observed` | sometimes | An `observe` LIST of `c/` held an `rw.*.cmt`. |
| `erasure: a .done was observed` | reachable | |
| `erasure: a .dreq removal was observed` | reachable | |
| `erasure: a read after a .dreq removal ran` | sometimes | A read issued after a `.dreq` removal returned at least one survivor acknowledged before that request, so it was not vacuously empty. |
| `gc: a hold was placed` | reachable | |
| `gc: the hold reads were made to fail` | reachable | The eventually's hold-shard object is in place. |
| `clock: the clocks passed the protection horizon` | reachable | A horizon jump reached the driver's clock. |
| `clock: the retention jumps ran` | reachable | |
| `lifecycle: the workload starts` | reachable | |
| `clock: the driver could not write the clock offset` | unreachable | |
| `clock: the clock jump did not reach the driver` | unreachable | |

Reads that fail (errors, timeouts) are counted as inconclusive and printed;
the eventually's last line reports the count.

The SUT's own lifecycle assertions, compiled into ravel-server's
`antithesis` build, that a plain local loop must reach and pass:
`gc: no object is deleted inside its protection window`
(`crates/ravel-maintain/src/retention.rs`), `gc: a held object is never
deleted` and `gc: a failed hold refresh sweeps nothing`
(`crates/ravel-maintain/src/legal_hold.rs`, the second also at the end of each
tick in `services/ravel-server/src/maintain.rs`), and `erasure: a completion
is written only when no pre-rewrite object is still served` and `erasure: a
completion is written only once every bucket open at its acknowledgement is
judged` (`services/ravel-server/src/maintain.rs`); and the reachables
`gc: a failed hold refresh skips the maintain tick` and `gc: retention
physical sweep delete`.

## Mutants

ADR-4326 decision 3 gives the lifecycle area four mutants, each an
`inv_mutant!("lifecycle/<stem>")` switch in ravel-server's library code,
compiled in only by the `antithesis-mutants` cargo feature and selected at
start by `RAVEL_ANTITHESIS_MUTANT`. `formal/tla/lifecycle/antithesis-mutants.tsv`
names the property each must make false, and `local-run.sh` reads it from
there.

| Mutant | Switch | Property it makes false | Situation in this workload |
|---|---|---|---|
| `lifecycle/refresh-failure-is-no-hold` | a failed hold read is taken as an empty hold set, at the tick's refresh and at each delete batch's re-read | `gc: a failed hold refresh sweeps nothing` | the eventually's failed hold reads across the second hold-phase horizon jump |
| `lifecycle/delete-before-horizon` | the retention sweep skips its protection-horizon gate | `gc: no object is deleted inside its protection window` | the eventually's second retention jump |
| `lifecycle/rewrite-keeps-erased-records` | the metrics rewrite keeps the erased samples | `erasure: a rewrite's output is its inputs minus the erased records` | every metrics erasure |
| `lifecycle/complete-ignores-served-set` | completion skips the completion gate's whole blocked set: the served-set check and the open-bucket check, which the Rust guard unions | `erasure: a completion is written only once every bucket open at its acknowledgement is judged` (ADR-4326, the completion-mutant amendment). The served-set property, `erasure: a completion is written only when no pre-rewrite object is still served`, is expected to hold in the same run (`local-run.sh` reports a violation of it and does not gate on it); the capture-lane test `completion_waits_for_the_served_set` Part B shows the mutant breaking it. The driver's erasure property may go false, and is reported, not gated | a completion tick while a bucket open at the request's acknowledgement is still unsealed: a request acked after the sealing jump reaches its first non-deferred tick before its ack-open hour seals, the gate blocks it without reading that bucket, and the mutant writes `.done` over the block. The completion gate resolves the commit-prefix listing and never reads HEAD. |

Only the four ravel-server containers run a mutant; the driver's
`ravel-cli` is never built with `antithesis-mutants`.

`MUTANTS=1 scripts/build.sh` builds `ravel-antithesis-server:lifecycle-gc-mutants`
(`--features sql,antithesis,antithesis-mutants`, labelled
`org.ravel.antithesis.features`) and the driver over it under the same tag,
and checks that the server binary registers the four lifecycle mutant names.
`RAVEL_ANTITHESIS_MUTANT=lifecycle/<stem>` selects a mutant for `local-run.sh`,
and `launch.sh push --mutant lifecycle/<stem>` renders the config for one.
Both go through `scripts/mutant-config.sh`, which writes a copy of `config/`
in which every workload image names its `-mutants` tag and the ravel-server
environment's `RAVEL_ANTITHESIS_MUTANT: ""` names the mutant; it refuses a
name that is not one of the four, and both scripts refuse a `-mutants` server
image whose features label lacks `antithesis-mutants`.

A mutant run of `local-run.sh` exits 0 only when the mutant's property was
evaluated false at least once. Every other violated always-type assertion
and every other failed gate is printed as `REPORT (mutant run, not gated)`.

## The clock

The `.dreq` of a completed request is removed only once its `.done` is a
protection horizon old (90300 s), the superseded-input sweep holds nothing of
it, and every chain holding it has an unnamed-since marker older than the
pinned-query window (4830 s); an interior hour is swept on the six-hour full
sweep only (docs/deletion-and-gc.md). So the driver jumps 9000 s to seal the
covered hours, then 91200 s per step, which passes the horizon, the window
and the full-sweep cadence each time; removal takes two to three such jumps
after `.done`. The two 9000 s retention jumps come between the sealing jump
and the first horizon jump. Each jump holds the rules' write gate, waits
until the driver's own clock moved and one more libfaketime reread passed,
then waits for every node to serve.

## Build, run locally, launch

On the Linux build host, from a clean checkout of the commit to build:

- `scripts/build.sh [server|driver|all]` builds the server image from this
  checkout (or, with `SERVER_FROM=<image>`, retags an existing one) and the
  driver, copying `ravel-cli` from the server image, and checks their
  instrumentation. `MUTANTS=1` builds the mutant images (above).
- `scripts/local-run.sh <out-dir> [seconds]` runs the stack, forces the
  eventually after `seconds` (default 240), and exits 0 only if the driver
  ended the timeline, its peak memory stayed under `RSS_LIMIT_MB` (500),
  every container is running or exited 0, no always-type assertion was
  violated or never hit except those expected, and every required
  assertion (the driver's progress and forced situations, the SUT
  assertions listed under Properties, the driver's erasure property) held.
  `PROFILE=claims-off`, `WORKLOAD_PLACEMENTS=record` (or any subset of
  record, resource, scope), `EXPECT_VIOLATED=<file>` (messages, one per
  line, that must be violated) and `RAVEL_ANTITHESIS_MUTANT` adjust it.
- Self-test: `WORKLOAD_INJECT_SERVED_ERASED=1 WORKLOAD_PLACEMENTS=record
  scripts/local-run.sh <out-dir>`. The driver adds one victim to every read
  judged after a request completed; `erasure: an erased subject is never
  returned after its request` must be violated, and nothing else may.
  Record placement only, so the server's own #4503 failure does not make the
  self-test pass on an unfixed image.
- Negative control: on a server without the #4503 fix, the default
  placements violate the same property once a `.dreq` is swept, with
  resource and scope hits after completion only.
- `scripts/launch.sh push [--profile default|claims-off] [--mutant
  lifecycle/<stem>]` on the build host pushes both images and an OCI config
  image (buildx, `oci-mediatypes=true`) pinned by digest to the tenant
  registry, using `DOCKER_CONFIG=/root/.antithesis/docker-config`, and
  prints the config image reference. `scripts/launch.sh launch
  <config-image@digest> <hours> [profile]` runs `snouty launch
  --config-image` from any machine snouty is logged in on; no docker is
  needed there. With `RAVEL_ANTITHESIS_MUTANT` set it launches
  `ravel-lifecycle-gc-mutant` with `attrs.mutant`.
- `snouty validate config/` on the build host needs both images in the local
  docker image store.
