# ingest-durability

An Antithesis workload (index: [antithesis/README.md](../README.md)). Tests Ravel's ingest acknowledgement and visibility contract
(docs/consistency-model.md) under faults: a strictly acknowledged write is
never lost, is readable with its commit tokens, and is visible to queries once
faults stop; no stored record or sample is a phantom, and no stored log record
is stored more often than the attempts that could have written it (sample-level
duplication is not asserted for metrics).

## Deployment

`config/docker-compose.yaml`:

- `rustfs`: the only durable component, a RustFS container (the release and
  digest the repository's quickstart stacks and the object-store contract CI
  lane already run). RustFS supports every operation Ravel requires,
  including create-if-absent and compare-and-swap. RustFS's documentation
  (docs/operations/durability-modes.md in rustfs/rustfs) describes its default
  mode, `strict`, as what it has always shipped: payload, metadata and
  directory entries are fsynced before a PUT is acknowledged. The compose file
  sets that mode explicitly, but the 1.0.0 image does not log the mode it
  resolved, so this rests on RustFS's documentation rather than on an
  observation. Ravel promises only the store's own durability floor, so a store
  that lost acked PUTs would show up as a Ravel loss. When a durability failure
  follows a `rustfs` kill, suspect the store first. MinIO was not chosen because nothing in this repository runs
  against it, while RustFS is the backend the contract CI lane checks.
- `init`: a one-shot that creates the bucket and runs `ravel-cli store
  qualify`, which ravel-server requires before it serves.
- `ravel1`, `ravel2`: two `ravel-server --mode all` processes over one bucket.
  Each one ingests, serves queries and folds the catalog. Two `all` processes
  over one bucket are supported: the fold's HEAD compare-and-swap serializes
  them (docs/catalog-and-mvcc.md). There is no `maintain` process, so nothing
  compacts or deletes. The flush delays are 500 ms (strict) and 5 s (idle).
- `driver`: the workload, excluded from faults.

## Driver and model

The driver is Rust, a [hegel](https://hegel.dev) concurrent state machine
(`driver`). Ravel has no client library: the driver speaks
OTLP/HTTP and the SQL endpoint itself. Each rule picks a node at random:

- `write_logs` and `write_metrics` send one OTLP/HTTP export. The mode is
  strict (60%), `strict-arrival` (20%) or buffered (20%). Some writes are gzip.
  Some have a short 2 s client timeout, which drops the request while the server
  is still working on it. Logs fan out over 1 to 4 streams, so one request can
  span several shards. Half of the log writes carry an
  `x-ravel-idempotency-key`. A write with no definite outcome (no response,
  503, 429) is retried up to twice, the way an OTLP exporter retries. An
  acknowledged keyed write is sometimes deliberately retried with the same key.
- `read_with_tokens` reads one acknowledged write back through
  `POST /api/v1/sql` with its `min_commit_token`s.
- `scan_without_token` reads every workload row in the window, with no token.
- `eventually` is a rare draw. It stops faults (through the
  `ANTITHESIS_STOP_FAULTS` command the platform sets in every container; unset
  in the local loop, which injects none), waits for both nodes to serve, checks
  every acknowledged write and then ends the timeline.

Every record carries its write id and its index within the write: the
`wl.write` and `wl.rec` attributes on logs, and the value
`write_id * 100 + index` on samples. A query result therefore maps every
stored row to exactly one record sent. The model registers a write and an
in-flight attempt before the request is sent. Each attempt is then recorded as
one of:

| Attempt | Meaning |
|---|---|
| acked | strict 200, no partial success, an `x-ravel-commit-token` header |
| buffered ack | buffered 200; never asserted present |
| partial / refused / no response | may or may not be stored, now or later, for the rest of the timeline |

A write's records must be present once any attempt is acked. Each record may
be stored at most once per attempt. An acknowledgement whose token header is
byte-identical to an earlier one's is an idempotency replay and adds no copy.

## Properties

Guarantees (`assert_always` / `assert_unreachable`), each citing the section of
docs/consistency-model.md it implements:

| Assertion message | Where | Contract |
|---|---|---|
| a read with an acked log write's commit tokens returns every record of that write | driver | Acknowledgement semantics (strict); Read-your-write |
| a read with an acked metrics write's commit tokens returns every sample of that write | driver | same |
| every stored workload log record belongs to a write the workload sent | driver | Acknowledgement semantics; Visibility (atomic per object) |
| every stored workload sample matches a sample the workload sent | driver | same |
| no log record is stored more often than the attempts that could have written it | driver | Duplicates and idempotency (at-least-once, at most one copy per delivery) |
| a keyed log retry that replays the original commit token stores no new copy | driver | Opt-in client idempotency key, replay contract |
| a clean strict 200 response carries a commit token | driver | Acknowledgement semantics (strict) |
| a buffered-mode acknowledgement carries no commit token | driver | Buffered mode |
| a well-formed write was refused as non-retryable (HTTP 400) | driver, unreachable | Rejection |
| the system recovers once faults stop | driver, eventually | Architecture: every process is disposable |
| acked writes are readable with their commit tokens once faults stop | driver, eventually | Acknowledgement semantics: no crash may lose a strict ack |
| acked writes are visible without a token once faults stop | driver, eventually | Catalog snapshot staleness (listing-immediate freshness in healthy operation) |
| logs ingest: an idempotency marker read back undecodable | ravel-server, unreachable | markers are immutable, written once with create-if-absent |

A no-token read during faults asserts only the upper bounds: no phantoms and
no excess copies. Freshness without a token is not guaranteed while faults
run. The no-token visibility assertion in the eventually can also fail for the
documented clock exceptions under "Catalog snapshot staleness". A large
forward clock skip that seals an hour while a commit is still in flight into
it is one such case. Triage a failure of that assertion against the token-read
assertion beside it: when token reads pass, the data is durable and only
visibility is in question.

Exploration assertions:

- In the driver: the workload starts; each write outcome (acked, buffered
  ack, no response, 503, 429, partial success); a strict ack spanning more
  than one shard; a write acknowledged on retry after an indefinite attempt; a
  keyed retry that replays the original token, and one that stores a new copy;
  a token read refused as unavailable; a no-token read that sees an acked
  write; a record found more than once; a write with no definite outcome later
  found stored.
- In ravel-server, behind the off-by-default `antithesis` cargo feature
  (`services/ravel-server/src/{logs_ingest,ingest,otlp_http,main}.rs`): an
  idempotency replay; a marker lookup that failed closed; a marker write that
  failed after a durable commit; a partial multi-shard commit (logs and
  metrics); `AckTimeout`, `Abandoned` and `ShardUnavailable` answers (logs and
  metrics); a graceful drain started by a signal; and a drain that did not
  complete cleanly.

Some of these may stay unreached even in long runs, so read their absence as
coverage, not as a finding. The 429 shed needs a process-wide budget to fill.
`ShardUnavailable` needs a dead shard actor. An unclean drain needs a stop
fault during a store outage. A partial success needs a clock skip larger than
the 10 minute future-skew bound in the middle of a request.

The flush, retry and abandon logic itself lives in `ravel-ingest`. It carries
no assertion sites yet, so its outcomes are observed at the handler instead.
The image's ravel-server and ravel-cli are both built with the `antithesis`
feature, which also reaches ravel-commit, ravel-ingest, ravel-catalog,
ravel-maintain and ravel-fleet through `ravel-invariant` (ADR-4326), so an
assertion site added in one of those crates reports from this workload without
an image change.

## Patterns

None adopted. Ravel has no stateful client library, so the stateful-clients
pattern does not apply. Every node state worth marking is visible inside one
node, at the handler that sees the outcome, so log forwarding is not needed.
The normal log level is enough, so retroactive logging is not needed either.

## Out of scope

The scope is ingest durability only:

- Spans, OTLP/gRPC, Prometheus Remote Write and OTAP are out. They reach the
  same router and commit path after decode, and the logs and metrics pipelines
  cover that path.
- Warm-up, the partition cap, bulk load (`ravel-cli load`), compaction,
  retention and GC (no `maintain` process), resharding, and multi-tenant
  admission limits are out.

## Known findings the harness works around

- ravel-server used to exit about 3 s after start when the store did not
  answer a startup read, even on a transient connect error (#4325), and nothing
  here restarts it. The server now retries its startup store reads while the
  store returns a retryable error, up to `--startup-store-deadline`. The compose
  file sets that to 300 s, far longer than the 15 s default, because no probe
  or supervisor restarts a container that exits. The entrypoint no longer waits
  for the store before starting the server, so runs exercise that retry. When
  "the system recovers once faults stop" fails, look first for "refusing to
  start" in the down node's log.
- `snouty validate` on an Apple Silicon Mac needs Docker Desktop's Rosetta
  emulation. Under its QEMU fallback, the amd64 `ravel-cli` in `init` crashes
  with a segmentation fault, and setup never completes. Validate on an amd64 host,
  or turn Rosetta on. `SKIP_VALIDATE=1 scripts/launch.sh ...` launches without
  the local validation, for use once the same config passed on an amd64 host.

## New external dependencies

`antithesis_sdk` 0.4, pinned once in `crates/ravel-invariant`, and
`antithesis-instrumentation` 0.1, in ravel-server and ravel-cli, each only
under an `antithesis` feature. No published build enables that feature, and
scripts/guards/check-release-profile.sh refuses one that does.
They add `libloading`, `linkme`, `rand` 0.8, `rand_chacha` 0.3 and
`rustc_version_runtime` to Cargo.lock. The driver is its own Cargo workspace,
with its own lock file, and the repository's workspace never builds it.

## Mutants

The commit area's negative configs (formal/tla/commit/negative/) that break a
safety property are switches in the shipped code, each an
`inv_mutant!("commit/<stem>")` call (ADR-4326 decision 3). They compile in only
with ravel-server's `antithesis-mutants` feature, which only the
`ingest-durability-mutants` image tag carries: `MUTANTS=1 scripts/build.sh`
builds ravel-server with `sql,antithesis,antithesis-mutants`, stamps the list
into the `org.ravel.antithesis.server-features` label, and checks that
`/symbols/ravel-server` holds every mutant name (a plain build checks that it
holds none). The plain build is unchanged.

`RAVEL_ANTITHESIS_MUTANT=commit/<stem>` in ravel1's and ravel2's environment
turns one switch on. A server built without the feature ignores the variable,
so `scripts/launch.sh` and `scripts/local-run.sh` refuse a mutant run unless
the `-mutants` server image carries the feature label, and refuse any name
that is not a mutant line of formal/tla/commit/antithesis-mutants.tsv. That
file names the property each mutant must make false; the configs it marks
`none:` are reachability obligations or out of scope, not switches.

| Mutant | Switch | Property it makes false |
|---|---|---|
| `commit/ack-before-commit` | a metrics shard's `commit_flush` (crates/ravel-ingest/src/shard.rs) acks its strict waiters before the data object and the commit record are written | `commit: a strict metrics ack names a durable commit record` (crates/ravel-ingest/src/router.rs) |
| `commit/commit-before-data` | `put_data_object` (crates/ravel-commit/src/publish.rs) returns success without the PUT | `commit: a commit record never exists without its data object` (crates/ravel-commit/src/publish.rs) |
| `commit/mismatched-identity-idempotent` | every shard actor draws one fixed writer identity (`draw_writer_identity`, crates/ravel-ingest/src/router.rs), so two writers reuse a commit key, and a commit record that already exists with other content is taken as an idempotent match (crates/ravel-commit/src/publish.rs) | `commit: one commit identity holds one content hash` (crates/ravel-commit/src/publish.rs) |
| `commit/marker-before-all-shards` | a keyed log write's receipt waits for its first shard's ack only (`await_strict_acks`, crates/ravel-ingest/src/log_router.rs), so the idempotency marker is written ahead of the other shards' commits | `commit: an idempotency marker implies every shard's commit is durable` (crates/ravel-ingest/src/idempotency.rs) |

`ack-before-commit` changes the metrics path only; the log path's acks are
untouched, so the driver's log read-back cannot fail under it. The driver's
metrics read-back (`a read with an acked metrics write's commit tokens returns
every sample of that write`) may fail as a knock-on effect; the gated property
is the server's own.

A mutant run passes when its property was evaluated false at least once. The
local loop gates on exactly that and lists any other always-property that went
false, without gating on it. An Antithesis mutant run is named
`ravel-ingest-durability-mutant` with the `attrs.mutant` parameter set to the
mutant (`none` on a plain run); its report should show the expected property
failing.

## Build, run locally, launch

Build on a Linux amd64 host, never on a laptop:

```sh
antithesis/ingest-durability/scripts/build.sh            # both images plus the instrumentation checks
antithesis/ingest-durability/scripts/local-run.sh out/run1 300
WORKLOAD_EVENTUALLY_AFTER_SECS=90 antithesis/ingest-durability/scripts/local-run.sh out/run2 600
antithesis/ingest-durability/scripts/export-images.sh s3://<bucket>/<prefix>
```

Then, on the machine that launches runs:

```sh
antithesis/ingest-durability/scripts/import-images.sh s3://<bucket>/<prefix>
antithesis/ingest-durability/scripts/launch.sh <max-compute-hours> "<description>"
```

The compose file names the workload's images by local-only tags,
`ravel-antithesis-server:ingest-durability` and
`ravel-antithesis-driver:ingest-durability`, plus the pinned
`ghcr.io/rustfs/rustfs:1.0.0` digest. snouty pushes a local-only image into
the tenant registry, but skips an image it finds served by another registry,
which Antithesis could not pull. The ECR tags
(`819709950457.dkr.ecr.us-east-1.amazonaws.com/ravel-antithesis/...`) only
move the images from the build host to the launching machine.

Instrumentation uses the compile-time coverage flags with codegen-units 1.
The Antithesis server and cli builds turn the release profile's thin LTO off,
because compile-time instrumentation does not survive LTO. The published
image keeps LTO. Unstripped binaries are in `/symbols` of each image.

Once #4308 lands, the workload also runs weekly from
`.github/workflows/antithesis-weekly.yml` (Tuesday 03:07 UTC, at most 240
compute hours), which calls `scripts/build.sh all` and `scripts/launch.sh` as
they are. The `weekly` marker file in this directory opts the workload in
(ADR-4326 decision 4); until #4308 lands, nothing reads it.

A smoke run is a plain run with a small budget; a mutant run builds, moves and
launches the `-mutants` images. On the build host:

```sh
MUTANTS=1 antithesis/ingest-durability/scripts/build.sh
RAVEL_ANTITHESIS_MUTANT=commit/commit-before-data \
    antithesis/ingest-durability/scripts/local-run.sh out/mutant1 300
MUTANTS=1 antithesis/ingest-durability/scripts/export-images.sh s3://<bucket>/<prefix>
```

Then, on the machine that launches runs:

```sh
antithesis/ingest-durability/scripts/launch.sh 1 "ingest-durability smoke"
MUTANTS=1 antithesis/ingest-durability/scripts/import-images.sh s3://<bucket>/<prefix>
RAVEL_ANTITHESIS_MUTANT=commit/commit-before-data \
    antithesis/ingest-durability/scripts/launch.sh <max-compute-hours> "ingest-durability mutant"
```

The `-mutants` archives are saved as `<name>-mutants.tar.zst` beside the plain
ones, so one prefix holds both. `MUTANTS=1` needs a tag ending in `-mutants`
and a `-mutants` tag needs `MUTANTS=1` (`scripts/image-tag.sh`), so a mutant
build never lands under the plain tag.

The harness expects these fault flags (`scripts/launch.sh`):

```sh
--fault-tolerance high_availability --fault-exclusions '^driver$' --setup-timeout 10m
```

Driver environment for the local loop: `WORKLOAD_EVENTUALLY=off` turns the
eventually off, `WORKLOAD_EVENTUALLY_AFTER_SECS=N` runs it after N seconds,
and `WORKLOAD_MAX_WRITES` caps the writes per timeline (the compose file sets 2500; the eventually is proven at about 2400 writes).
`WORKLOAD_INJECT_MISSING_RECORD=1` is a self-check: the model then expects one
record more than is sent for each log write whose id is 3 modulo 50 (about one
log write in 50; ids are shared with metrics writes, so about one write of any
signal in 75 at the default rule weights), and the presence assertions must
fail.
