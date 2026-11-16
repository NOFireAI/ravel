# Antithesis workloads

## ingest-durability

Tests Ravel's ingest acknowledgement and visibility contract
(docs/consistency-model.md) under faults: a strictly acknowledged write is
never lost, is readable with its commit tokens, and is visible to queries once
faults stop; no stored record is a phantom or stored more often than the
attempts that could have written it.

### Deployment

`ingest-durability/config/docker-compose.yaml`:

- `rustfs`: the only durable component, a RustFS container (the release and
  digest the repository's quickstart stacks and the object-store contract CI
  lane already run). RustFS supports every operation Ravel requires,
  including create-if-absent and compare-and-swap. Its default durability mode,
  `strict`, fsyncs payload, metadata and directory entries before it acknowledges
  a PUT. The compose file sets that mode explicitly, so a killed `rustfs`
  container never loses an acknowledged PUT. Ravel promises only the store's
  own durability floor, so a store that lost acked PUTs would show up as a
  Ravel loss. MinIO was not chosen because nothing in this repository runs
  against it, while RustFS is the backend the contract CI lane checks.
- `init`: a one-shot that creates the bucket and runs `ravel-cli store
  qualify`, which ravel-server requires before it serves.
- `ravel1`, `ravel2`: two `ravel-server --mode all` processes over one bucket.
  Each one ingests, serves queries and folds the catalog. Two `all` processes
  over one bucket are supported: the fold's HEAD compare-and-swap serializes
  them (docs/catalog-and-mvcc.md). There is no `maintain` process, so nothing
  compacts or deletes. The flush delays are 500 ms (strict) and 5 s (idle).
- `driver`: the workload, excluded from faults.

### Driver and model

The driver is Rust, a [hegel](https://hegel.dev) concurrent state machine
(`ingest-durability/driver`). Ravel has no client library: the driver speaks
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
- `eventually` is a rare draw. It stops faults, waits for both nodes to serve,
  checks every acknowledged write and then ends the timeline.

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

### Properties

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

The flush, retry and abandon logic itself lives in `ravel-ingest`. It carries
no assertions yet: the SDK is linked only into ravel-server. Its outcomes are
observed at the handler instead.

### Patterns

None adopted. Ravel has no stateful client library, so the stateful-clients
pattern does not apply. Every node state worth marking is visible inside one
node, at the handler that sees the outcome, so log forwarding is not needed.
The normal log level is enough, so retroactive logging is not needed either.

### Out of scope

The scope is ingest durability only:

- Spans, OTLP/gRPC, Prometheus Remote Write and OTAP are out. They reach the
  same router and commit path after decode, and the logs and metrics pipelines
  cover that path.
- Warm-up, the partition cap, bulk load (`ravel-cli load`), compaction,
  retention and GC (no `maintain` process), resharding, and multi-tenant
  admission limits are out.

### New external dependencies

`antithesis_sdk` 0.4 and `antithesis-instrumentation` 0.1, in ravel-server,
only under the `antithesis` feature. No published build enables that feature.
They add `libloading`, `linkme`, `rand` 0.8, `rand_chacha` 0.3 and
`rustc_version_runtime` to Cargo.lock. The driver is its own Cargo workspace,
with its own lock file, and the repository's workspace never builds it.

### Build, run locally, launch

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

The images are `819709950457.dkr.ecr.us-east-1.amazonaws.com/ravel-antithesis/ravel-server:ingest-durability`,
`.../ravel-antithesis/driver:ingest-durability`, and the pinned
`ghcr.io/rustfs/rustfs:1.0.0` digest.

Instrumentation uses the compile-time coverage flags with codegen-units 1.
The Antithesis server build turns the release profile's thin LTO off,
because compile-time instrumentation does not survive LTO. The published
image keeps LTO. Unstripped binaries are in `/symbols` of each image.

The workload also runs weekly from `.github/workflows/antithesis-weekly.yml`
(Tuesday 03:00 UTC, at most 240 compute hours), which calls `scripts/build.sh
all` and `scripts/launch.sh` as they are.

The harness expects these fault flags (`scripts/launch.sh`):

```sh
--fault-tolerance high_availability --fault-exclusions '^driver$' --setup-timeout 10m
```

Driver environment for the local loop: `WORKLOAD_EVENTUALLY=off` turns the
eventually off, `WORKLOAD_EVENTUALLY_AFTER_SECS=N` runs it after N seconds,
and `WORKLOAD_MAX_WRITES` caps the writes per timeline (default 5000).
