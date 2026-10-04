# Concepts

Read these concepts once, in order, before the other pages. Each section
assumes the section before it. The [architecture overview](architecture.md)
shows where each idea lives in the running system.
[docs/consistency-model.md](consistency-model.md) is the normative statement
of the guarantees summarised here.

The [glossary](#glossary) defines every Ravel term and expands every acronym.

## Durable and disposable

Object storage holds every durable byte. A bucket and the objects in it are
the whole durable state of Ravel. Everything else is a cache or a
computation. Ravel has no write-ahead log, no replicated block device, no
quorum of stateful nodes, and no local disk whose loss loses data.

Every compute process is disposable, which is a stronger claim than
stateless. A process can hold state: buffered records, a resolved snapshot, a
read cache, a membership view. It never holds state that another process
needs to recover. No restart path reads a file that another process wrote
locally. No correctness argument depends on a particular process coming back.

If any process dies at any instant, every strictly acknowledged write
survives. A replacement process rebuilds what it needs from the store.

This model gives four things:

- Compute and storage scale independently, because adding or removing a
  process moves no data.
- Recovery is a restart, because there is nothing local to reconcile.
- Backup, replication, and disaster recovery are bucket-level problems with
  bucket-level tools.
- There is no leader to elect, no quorum to lose, and no split brain to
  detect. The conditional writes of the store are the only arbiter.

The model also has costs:

- Every durable step is a network round trip. So the floor on visibility
  latency is object-store latency.
- Requests are a bill. So request count is an output of every read path (see
  [guides/cost-model.md](guides/cost-model.md)).
- Local caches accelerate reads and must never be read as truth. Every cache
  entry is keyed by immutable content. A cold pass and a warm pass over the
  same data differ only in requests issued, never in rows returned.
- A multi-object write cannot be atomic. A process that dies between two PUTs
  leaves an object that nothing references. That object is invisible to
  queries, and a background sweep removes it later.

## Tenants, signals, and shards

A tenant is the isolation unit. Admission limits, retention, encryption key
epoch, shard count, and authentication are all per tenant. No query can
resolve across two tenants.

A tenant never appears in an object key. A hash of it does, so every key
under a tenant's data begins `t/<tenant_hash>/`. This has three effects:

- A key listing cannot reveal customer names.
- A tenant identifier with awkward characters or unbounded length cannot
  shape the keyspace.
- Ravel's own `/metrics` output can carry a `tenant_hash` label with a closed
  allowlist, in place of an unbounded set of names.

The key layout is in [docs/catalog-and-mvcc.md](catalog-and-mvcc.md).

A signal is one of three things: metrics, logs, or spans. A signal is the
axis along which Ravel separates storage. It is not a table or a schema,
although the SQL surface exposes one table per signal. Each signal has its
own columnar segment format (RSEG, RLOG, RSPAN), its own catalog HEAD
pointer, its own shard count, and its own maintenance schedule. Two signals
of the same tenant share only the bucket and the tenant's configuration
record.

A shard is the unit of ingest concurrency. The router hashes each record's
identity to a shard. Each shard is one single-threaded actor with a bounded
queue. So ordering within a shard needs no locks, and back pressure is a full
queue and never an unbounded buffer. Maintenance work is also partitioned
over shards. So a tenant's shard count sets both its write throughput ceiling
and its per-bucket object count.

A shard count is pinned durably at the tenant's first write for a signal.
That first write creates a provisioning record per (tenant, signal). The
record holds the count as the first entry of a generation history. Every
later ingest, catalog resolve, and maintenance pass validates its configured
count against the generation that the history makes active for the hour it
touches.

- A statically configured tenant whose count disagrees refuses to start.
- A dynamically discovered tenant whose count disagrees fails that one
  request.

The shard index is part of the object key. With a silently changed count, a
writer and a reader use different key sets. A query then answers from a
subset of the tenant's data without knowing it.

A change to the count appends an entry to that history with a future
activation hour. The history is never edited. A writer routes with the
generation that is active for the hour it writes. A reader derives the shard
fan-out of each hour it lists from the same history.

## Segments and commit records

A shard actor accumulates records until a flush trigger fires. It then builds
one immutable columnar segment in memory and writes it to the store. That
segment is a data object: it holds telemetry bytes and nothing else. It is
written once and never modified.

The shard then creates a commit record, which is a small second object. It
names the data object, its shard, its writer identity and sequence number,
its event time bounds, and its record count. The commit record is the
publish. A query sees only data that some commit record names. So until the
second PUT succeeds, no query can see the first object.

The order of the two PUTs matters. A crash between them leaves a data object
that no commit record names. That object is invisible to every query, and a
later sweep deletes it after a grace period. In the reverse order, a crash
leaves a commit record that names bytes that possibly never landed, which is
a corrupt catalog. The chosen order can leave only an object that nobody
reads. So Ravel never needs a repair pass over the catalog.

Both objects are immutable, and the commit record is created with a
conditional create-if-absent put. Two writers that race onto one commit key
cannot both win. One gets the object. The other gets a typed already-exists
error and re-reads. Immutability and conditional creation let many processes
write to one bucket with no coordination beyond the store.

Segments come in two levels:

- An L0 segment is what a shard actor flushed: one per flush, small, and
  numerous.
- An L1 segment is what compaction produces from many L0 segments of the same
  bucket: fewer, larger, and cheaper to scan.

Both levels are immutable segments. The level says only who wrote the segment
and roughly how large it is. The formats are frozen contracts, one per
signal: [RSEG](segment-format.md) for metrics,
[RLOG](log-segment-format.md) for logs, and
[RSPAN](span-segment-format.md) for spans.

## Acknowledgement and visibility

Acknowledgement is what the write API told the client. Visibility is whether
a query can see the data. They are separate properties with separate rules.

### Acknowledgement

There are two acknowledgement modes.

Strict acknowledgement is the default. An export is acknowledged only after
every batch it contributed to has its L0 data object durably stored and its
commit record created. The response carries a commit token set in the
`x-ravel-commit-token` header, as a comma-separated list. The set has one
token per shard that the request's points flushed through. After a strict
acknowledgement, no crash of any Ravel process can lose that data.
Object-store durability is the floor: the data survives anything that the
object store survives.

Buffered acknowledgement is opt-in per request. The response comes after
admission and enqueue to a shard actor, and it returns no commit token. It is
never described as durable. A crash between the acknowledgement and the flush
loses the buffered window, which the maximum flush delay bounds. A clean
shutdown drains that window, so an orderly restart loses nothing. An abrupt
process death does lose it.

Buffered mode is offered on OTLP ingest only. Remote Write is strict-only,
and it ignores a buffered-mode header on a request.

In both modes, admission failures such as limits, authentication, and quota
reject before anything is buffered. So a rejection is never a silent loss.

### Visibility

A batch becomes visible to queries when its commit record exists.
Commit-record creation is atomic, so visibility is atomic per data object: a
query sees all of a segment's rows or none of them. Visibility latency is the
flush delay plus the data PUT plus the commit PUT. The flush delay is a
budget that the operator configures, and is not a fixed constant.

The two properties combine as follows:

- A strict acknowledgement implies visible, because the commit record existed
  before the response.
- A buffered acknowledgement implies only that the data was admitted. It
  does not imply that the data is stored or visible. Visibility follows when
  the flush completes.
- Visibility can arrive with no acknowledgement at all. That is the crash
  case where the commit PUT succeeded and the response never reached the
  client.

The normative text is [docs/consistency-model.md](consistency-model.md),
sections
[Acknowledgement semantics](consistency-model.md#acknowledgement-semantics)
and [Visibility semantics](consistency-model.md#visibility-semantics).

## Read-your-write

Read-your-write in Ravel is caller-driven and opt-in. A caller that holds
commit tokens from a strict acknowledgement passes them back to a query API
as `min_commit_token`, repeatable, once per token. Each token fully
determines its commit-record key. So the catalog does not search: it GETs
those keys and includes their segments in the resolved snapshot. If a token
names a key that cannot be satisfied, the query fails with a typed
`unsatisfiable token` error. The answer is either the write or an explicit
error, and never silently stale data.

Without a token, a query sees some recent consistent snapshot. Listing
behaviour bounds its freshness, and nothing guarantees it. An unqualified
query issued immediately after a write can include that write or omit it, and
no configuration changes that. What the query does see is always a consistent
snapshot. A caller that needs the stronger property has a mechanism that
costs one header.

Two consequences follow:

- Only strict acknowledgement gives you a token. So buffered mode and
  read-your-write are mutually exclusive.
- Read-your-write is per token. It is not per client and not per session. A
  caller that writes to three shards and presents two of the three tokens
  gets two of the three writes.

The normative text is
[Read-your-write](consistency-model.md#read-your-write).

## The catalog

A snapshot, a fold, and HEAD are three different things.

A snapshot is a logical, immutable set of segments. A query resolves one
snapshot and uses it for its entire execution. So commits, compactions, and
deletions that land mid-query cannot change its answer.

A snapshot part is one piece of a published snapshot. It is immutable and
content-addressed, and it holds the folded state of one sealed ingest hour.
The word "part" alone is ambiguous in this repository. So a piece of a
snapshot is always a snapshot part, and a data object is always a segment.

A fold turns commit records into snapshot parts. It runs as a background task
per tenant and signal, and also on demand for one tenant and signal. It lists
the commit records whose ingest hour has sealed, writes them into snapshot
parts, and publishes a new HEAD. Folding is not compaction: it touches no
telemetry bytes and produces no new segments. It is a cost optimisation only,
and it never changes which commits a query sees.

HEAD is the catalog's one mutable object, one per (tenant, signal). It is a
pointer that names the current snapshot parts, and it is written only with a
compare-and-swap on its version. So two folders that race are safe: the
compare-and-swap of the loser fails, the loser re-reads, and nothing is
corrupted. A query begins with one GET of HEAD, which pins the snapshot that
it uses for the rest of its execution.

A few other per-tenant records change after they are written, each under its
own rule:

- Admission usage is overwritten on every reconciliation interval.
- Alert state moves by compare-and-swap.
- The encryption key-epoch record is append-only.
- The operator tool rewrites tenant configuration.

None of them is part of the catalog, and a snapshot names none of them.

A fold that runs immediately after a load finds nothing eligible and
publishes nothing. An ingest hour seals only after the maximum flush lifetime
plus a clock-skew allowance plus a fold safety margin has passed. That result
is correct and is not a failure. The on-demand fold reports it as a distinct
status and not as success.

Listing serves everything that the catalog does above the sealing watermark.
That is why listing behaviour bounds unqualified freshness. The protocol is
in [docs/catalog-and-mvcc.md](catalog-and-mvcc.md), and the isolation
property is [Snapshot isolation](consistency-model.md#snapshot-isolation).

## Background maintenance

Three background loops reshape the bucket: compaction, retention, and the
sweep. Only a `maintain` mode process runs them.

`all` mode runs ingest, query, the catalog fold, and alert evaluation in one
process. It does not compact, expire, or sweep anything. So a deployment with
no `maintain` process deletes no durable data, and its L0 segments accumulate
unmerged. Its one delete is the reap of dead ingest processes' admission
snapshots. The quickstart stack runs `all` mode alone, which is fine for an
evaluation.

Maintenance supervisors derive the tenant set from storage by listing tenant
prefixes, and not from a flag. So no configuration can silently exclude a
tenant from retention. A discovery failure skips and retries the cycle, and
does not fall back to an empty set.

The live maintain workers partition the work by rendezvous hashing over a
heartbeat-derived live set. So N replicas divide the work, and no replica
does all of it.

### Compaction

Compaction makes L1 segments from many L0 segments of one (tenant, signal,
shard, ingest hour) bucket. It streams blocks and does not materialise the
inputs. It is publish-then-supersede:

1. The run writes its L1 segments.
2. The run checks that the record counts of its inputs and its outputs are
   equal. If they are not equal, the run aborts, because publish is the point
   of no return.
3. The run publishes one compaction record that names its exact input set,
   with a conditional create-if-absent put. Nothing about the inputs is
   mutated or removed at publish time.

Two compactors that race on one bucket converge. Create-if-absent picks one
record as the winner, and the segments of the loser are unreferenced objects
that age out.

Two records that name different but overlapping input sets are rarer. To
serve both records returns the overlapping records twice for logs and spans,
which have no query-time deduplication. So the resolver picks one
authoritative record per overlap group, in this order:

1. The largest input set, which leaves the fewest inputs to serve raw.
2. The smallest input-set hash.
3. The record key, as the final fallback.

The resolver serves any input that the winner does not name as a raw segment.
It ignores the segments of the other records and raises a metric. So no input
is served twice or dropped while an operator reconciles the bucket.
`ravel_catalog_compaction_input_set_conflicts_total` counts the buckets in
that state. No command reconciles one, so a rise in the metric is the signal
to investigate the bucket by hand.

The sweep and the erasure completion gate follow the same choice. An input
that only a losing record names is still the input that a query reads. So it
is not deleted as superseded and is not treated as already rewritten.

### Retention

Retention expires data by age against the tenant's configured policy. Like
every deletion in Ravel, it is a durable transaction first, which is a
tombstone object. Logical exclusion from newly resolved snapshots comes next,
and only then physical removal.

### The sweep

The sweep is the physical removal step, and it sits under garbage collection
as the umbrella term. It deletes objects that nothing references, behind
grace periods and a mass-orphan circuit breaker. So a listing anomaly that
makes half the bucket look unreferenced withholds deletions and does not
amplify them. Every sweep pass is stateless and restartable from zero, and
every delete is idempotent.

### Effect on query answers

Compaction and the sweep cannot change a query answer. A snapshot resolved
before, during, or after either loop returns the same rows.

- Compaction never deduplicates, and it conserves the record count. A
  snapshot that sees a compaction record excludes the inputs it names. For
  metrics, query-time deduplication also collapses any duplicate that slips
  through. So every intermediate state of a compaction is query-correct, and
  so is the overlap state that the resolver handles.
- The sweep physically removes only what no live snapshot references, and
  only after a protection horizon.

Retention is different by design. A tombstoned bucket is excluded from every
snapshot resolved after the tombstone. A snapshot pinned before the tombstone
keeps reading the bucket until the horizon passes. The guarantee is
[Deletion and GC](consistency-model.md#deletion-and-gc) in the consistency
model, and the mechanics are in
[deletion and garbage collection](deletion-and-gc.md).

## Consistency boundaries

There is no cross-shard ordering guarantee. A query snapshot can include
commit N+1 of one shard and not commit M of another. The wall-clock order in
which those commits were created does not change that. Nothing in the write
path establishes a total order across shards. A total order needs the
coordination that the disposable-process model refuses.

Per writer and shard, commits are sequenced. Each shard actor writes its
commits under a monotonically increasing sequence number for its writer
identity and epoch. So within one shard the history is a line. That is all
the ordering that Ravel offers. Any other ordering claim must be built from
commit tokens.

For example, a client writes two batches whose records hash to different
shards. The client cannot assume that a later query sees both or neither: the
query can see either one alone. A client that needs both must hold the commit
tokens from both writes and present them together. That turns an ordering
question into a per-token inclusion question that the catalog can answer.

Delivery is at-least-once. A client retry after a lost acknowledgement
re-ingests the batch, and both copies are stored.

- For metrics that is harmless. Queries deduplicate by series and timestamp,
  as Prometheus collapses a doubly scraped sample.
- For logs and spans, two identical records are legitimate data. An opt-in
  idempotency key makes a retry a no-op: the marker object is written before
  the acknowledgement, so a replay finds the marker and stores nothing new.
  Without that key, Ravel does not offer end-to-end deduplication.

The boundaries are per (tenant, signal). No transaction, snapshot, or
ordering guarantee spans two tenants or two signals of one tenant. A
correlated read across signals, such as a metric exemplar to its trace,
resolves one snapshot per signal.

## Failure and retry

Every failure in the write path lands in one of four places. Which of the two
PUTs had completed decides the outcome. The full table is the
[crash matrix](consistency-model.md#crash-matrix-strict-mode). Under strict
acknowledgement:

- **A crash before the data PUT** stores nothing at all. The client has no
  acknowledgement and retries. For the store, the retry is the first attempt.
- **A crash after the data PUT and before the commit PUT** leaves the data
  object present and unreferenced. It is invisible to every query, and
  garbage collection removes it after the grace period. The client's retry
  writes a fresh pair of objects.
- **A crash after the commit PUT and before the response** is the only
  ambiguous case, and it is ambiguous for the client only. The data is
  durable and visible, but the client never learned that. An unkeyed retry
  stores a duplicate. Query-time deduplication handles the duplicate for
  metrics, and logs and spans show two records. A keyed retry replays the
  idempotency marker and stores nothing new, because the marker PUT precedes
  the acknowledgement.
- **A crash after the response** changes nothing. The data is durable and
  visible, and there is nothing to retry.

Retries are safe everywhere because no persisted step is a read-modify-write.
Data objects are content-addressed, commit records and markers are created
with create-if-absent, and HEAD moves only under compare-and-swap.

Where an API cannot tell you which side of a boundary a failure fell on, it
says so:

- A 503 from the on-demand fold means that the outcome is unknown. Retry the
  call. A 503 never means that nothing was written.
- Authentication and validation errors are returned before any store work
  happens. So a 401, 400, or 403 does guarantee that nothing was written.

## Glossary

Each entry is the canonical definition of a term. Where the repository has
used another word for the same thing, the entry names the alias. A reader can
meet an alias in a command, an API response, a metric label, or an object
key. This glossary is also the only place that expands acronyms. Other pages
use them bare.

- **acknowledgement mode**: how a write is acknowledged, either `strict` or
  `buffered`. Alias: `mode`, which also names a process's job in a
  deployment. Say acknowledgement mode when the subject is durability and
  mode when the subject is a process.
- **admission**: the per-tenant gate a write passes before it is buffered:
  body size, byte rate, series and stream caps, series-creation rate, and
  event-time skew. An admission failure is a rejection, never a loss.
- **CEL**: Common Expression Language, the expression language the Kubernetes
  API server evaluates the operator's validation rules in.
- **commit record**: the small immutable object that names a data object and
  publishes it. Created with a conditional create-if-absent put. Its
  existence is what makes a batch visible.
- **commit token**: the opaque value a strict acknowledgement returns, one
  per shard the request's points flushed through. It fully determines its
  commit-record key, and presenting it to a query API is how a caller gets
  read-your-write.
- **compaction**: making L1 segments from L0 segments of one bucket. Aliases:
  `fold`, `merge`. Neither names the operation: a fold publishes a catalog
  snapshot and touches no telemetry bytes, and a merge is what compaction
  does to its inputs internally.
- **disk tier**: the read cache's optional local-disk layer, holding raw
  compressed byte ranges. See tier.
- **fold**: publishing a catalog snapshot from commit records whose ingest
  hour has sealed. Aliases: `snapshot`, `compact`. A snapshot is the thing
  published, not the act of publishing it, and compaction is a different
  operation on different objects.
- **folder**: the background task that runs a fold for one (tenant,
  signal). `maintain` and `all` run one, and a `maintain` process folds only
  the pairs it owns. Two folders that race are serialized by the HEAD
  compare-and-swap.
- **garbage collection**: the umbrella term for removing data that is no
  longer needed, covering retention, erasure, and the sweep. The physical
  deletion step inside it is the sweep.
- **HEAD**: the single mutable pointer object per (tenant, signal) naming the
  current catalog snapshot parts. Written only by compare-and-swap on its
  version.
- **HRW**: highest random weight, also called rendezvous hashing. The
  deterministic assignment Ravel uses to divide work or replicas over a
  changing set of members without a coordinator, chosen because adding or
  removing one member moves only that member's share.
- **IMDSv2**: Instance Metadata Service version 2, the EC2 endpoint the
  `instance-role` storage credential path fetches short-lived credentials
  from. Ravel speaks version 2 only.
- **ingest hour**: the one-hour event-time bucket a record's timestamp falls
  in. It appears in commit and L1 keys, it is the unit a catalog fold seals
  and a compaction run covers, and it is event time, not arrival time.
- **L0 segment**: a segment written by a shard actor at flush. Small and
  numerous.
- **L1 segment**: a segment written by compaction from many L0 segments of
  one bucket. Larger and fewer. Alias: calling it a `part`, which is wrong
  twice over, since `part` already names a snapshot part and a piece of a
  multipart upload.
- **MAD**: median absolute deviation, the median of the absolute deviations
  from the median. The analytics stage uses it as a dispersion estimator that
  a single outlier cannot move.
- **mode**: a process's job in a deployment, one of `all`, `gateway`,
  `query`, and `maintain`, selected with `--mode`. Aliases: `tier`, `role`.
  Tier is the cache, and role belongs to a storage credential. The flag
  keeps its spelling.
- **Object Lock**: the S3 feature that holds an object version under a
  retention period, enabled per bucket and applied per object version either
  as a bucket default retention or as a per-object retention. Ravel sets
  neither, and [docs/object-store-contract.md](object-store-contract.md)
  states which mechanism does.
- **OTAP**: OpenTelemetry Arrow Protocol, the bidirectional gRPC protocol
  that carries telemetry as Arrow record batches. Feature-gated in Ravel. See
  [docs/otap-ingest.md](otap-ingest.md).
- **OTLP**: OpenTelemetry Protocol, the wire protocol Ravel's primary ingest
  surface speaks, over both HTTP and gRPC.
- **RAM tier**: the read cache's in-memory layer. See tier.
- **retention**: age-based expiry of a tenant's data under its configured
  policy. A durable tombstone first, then exclusion from new snapshots, then
  physical removal by the sweep.
- **RLOG**: Ravel Log Segment Format, the columnar segment format for logs.
  Frozen contract. See [docs/log-segment-format.md](log-segment-format.md).
- **RPO**: recovery point objective, the amount of recent data a recovery is
  allowed to lose. Ravel publishes a number only from a real rehearsal. See
  [guides/disaster-recovery.md](guides/disaster-recovery.md).
- **RSEG**: Ravel Segment Format, the columnar segment format for metrics.
  Frozen contract. See [docs/segment-format.md](segment-format.md).
- **RSPAN**: Ravel Span Segment Format, the columnar segment format for
  spans. Frozen contract. See
  [docs/span-segment-format.md](span-segment-format.md).
- **RTO**: recovery time objective, how long a recovery is allowed to take.
  Published on the same rehearsal-only basis as RPO.
- **S3-FIFO**: the read cache's eviction policy, built from static
  first-in-first-out queues with a small admission queue and a ghost queue,
  chosen over least-recently-used because a one-hit-wonder scan does not
  evict the working set.
- **segment**: an immutable data object holding telemetry. Qualified as an L0
  segment or an L1 segment where the level matters. Aliases: `part`, `file`.
  Part is a snapshot part, and file is what an operating system has.
- **shard**: the unit of ingest concurrency, one single-threaded actor with a
  bounded queue. Also the unit maintenance work is partitioned over. A
  tenant's shard count per signal is pinned durably at its first write for
  that signal.
- **signal**: metrics, logs, or spans. Each has its own segment format,
  catalog HEAD, shard count, and maintenance schedule.
- **snapshot**: the logical, immutable set of segments a query resolves once
  and uses for its whole execution.
- **snapshot part**: one immutable, content-addressed piece of a published
  catalog snapshot, holding one sealed ingest hour. Alias: `part`, which is
  ambiguous on its own.
- **SSE-KMS**: server-side encryption with a key from a key management
  service. It protects object bytes at rest in the store, and nothing else:
  the local cache directory is not encrypted by it.
- **storage credential role**: the grant set a set of storage credentials
  carries, such as the permission to write commit records but not to delete
  them. Alias: `role`, which also names a process's job and an EC2 instance
  role. The IAM role names in the credential guide keep their spelling.
- **sweep**: deleting objects that nothing references, the physical step
  inside garbage collection. Behind grace periods and a mass-orphan circuit
  breaker. Aliases: `reap`, `clean`.
- **tenant**: the isolation unit. Admission limits, retention, encryption key
  epoch, shard count, and authentication are all per tenant. Never appears in
  an object key.
- **tenant hash**: the hash of a tenant identifier that appears in every
  object key in place of the identifier, and in the `tenant_hash` metric
  label.
- **tier**: a cache layer, either the RAM tier or the disk tier. Alias:
  `level`. Tier never means a process's job, a deployment role, or a
  disaster-recovery level.
- **typed attribute column**: an attribute key promoted to a native column in
  a segment, so predicates and projections on it prune and decode like any
  other column. Alias: `typed column`. The `ravel-cli` subcommand that adds
  one is `typed-attr-column set`, and the server flag is
  `--typed-attr-column`. Their help text calls the result a declared typed
  attribute column, which means the same thing.
- **UDAF**: user-defined aggregate function, an aggregate Ravel registers
  with the SQL engine beyond the engine's own set.
- **UDF**: user-defined function, a scalar function Ravel registers with the
  SQL engine, such as the label accessors on the `samples` table.
- **UDTF**: user-defined table function, a function that returns a table
  rather than a value. Ravel registers none, and the SQL surface's tables are
  the registered tables only.
- **WORM**: write once read many, storage that forbids overwriting or
  deleting an object version for a retention period. Ravel's bucket
  protection contract asks for it on the protected prefixes, and
  [docs/object-store-contract.md](object-store-contract.md) states the
  mechanism that applies it.
- **writer**: the identity a shard actor writes under, carried in commit and
  data object keys as a writer id and epoch. Commits are sequenced per
  writer and shard, and a restarted process takes a new epoch rather than
  reusing a sequence number.
