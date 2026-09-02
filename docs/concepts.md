# Concepts

This page holds the smallest set of ideas you need before any other page in
the tree makes sense. Read it once, in order: each section assumes the one
above it. The [architecture overview](architecture.md) then shows where each
idea lives in the running system, and
[docs/consistency-model.md](consistency-model.md) is the normative statement
of the guarantees summarised here.

The [glossary](#glossary) at the end is the single definition of every Ravel
term, and the one place acronyms are expanded. Where the repository has used
several words for one thing, the glossary names the winner and names the
alias, so a word you meet in a command or an API response is never a dead
end.

## Durable and disposable

Object storage holds every durable byte. There is no write-ahead log, no
replicated block device, no quorum of stateful nodes, and no local disk whose
loss loses data. A bucket, plus the objects in it, is the whole of Ravel's
durable state. Everything else is a cache or a computation.

Every compute process is disposable, which is a stronger claim than
stateless. A process may hold plenty of state: buffered records, a resolved
snapshot, a read cache, a membership view. What it never holds is state that
another process needs in order to recover. No restart path reads a file
another process wrote locally, and no correctness argument depends on a
particular process coming back. Kill any process at any instant and every
strictly acknowledged write survives; a replacement process rebuilds what it
needs by reading the store.

That buys four things. Compute and storage scale independently, because
adding or removing a process moves no data. Recovery is a restart rather than
a repair, because there is nothing local to reconcile. Backup, replication,
and disaster recovery are bucket-level problems with bucket-level tools, not
Ravel problems. And a whole class of distributed-systems failure is absent by
construction: there is no leader to elect, no quorum to lose, and no split
brain to detect, because the store's own conditional writes are the only
arbiter.

It also costs. Every durable step is a network round trip, so the floor on
visibility latency is object-store latency, not memory latency. Requests are
a real bill, which is why request count is a first-class output of every read
path (see [guides/cost-model.md](guides/cost-model.md)). Local caches
accelerate reads and must never be read as truth, so every cache entry is
keyed by immutable content and a cold pass and a warm pass over the same data
differ only in requests issued, never in rows returned. And because a
multi-object write cannot be atomic, a process that dies between two PUTs
leaves an object that nothing references. Ravel plans for that outcome
instead of preventing it: the object is invisible to queries and a background
sweep removes it later.

## Tenants, signals, and shards

A tenant is the isolation unit. Admission limits, retention, encryption key
epoch, shard count, and authentication are all per tenant, and no query can
resolve across two of them. A tenant never appears in an object key: a hash
of it does, so every key under a tenant's data begins `t/<tenant_hash>/`.
That is deliberate. A key listing cannot be mined for customer names, a
tenant identifier with awkward characters or unbounded length cannot shape
the keyspace, and Ravel's own `/metrics` output can carry a `tenant_hash`
label with a closed allowlist rather than an unbounded set of names. The
exact key layout is in
[docs/catalog-and-mvcc.md](catalog-and-mvcc.md).

A signal is one of three things: metrics, logs, or spans. It is not a table
and not a schema, though the SQL surface happens to expose one table per
signal. A signal is the axis along which Ravel's storage is separated: each
one has its own columnar segment format (RSEG, RLOG, RSPAN), its own catalog
HEAD pointer, its own shard count, and its own maintenance schedule. Nothing
is shared between two signals of the same tenant except the bucket and the
tenant's configuration record.

A shard is the unit of ingest concurrency. The router hashes each record's
identity to a shard, and each shard is one single-threaded actor with a
bounded queue, so ordering within a shard needs no locks and back pressure is
a full queue rather than an unbounded buffer. Shards are also the unit that
maintenance work is partitioned over, so a tenant's shard count sets both its
write throughput ceiling and its per-bucket object count.

A shard count is pinned durably at the tenant's first write for a signal.
That first write creates a write-once provisioning record per (tenant,
signal) holding the count, and every later ingest, catalog resolve, and
maintenance pass validates its configured count against that record. A
statically configured tenant whose count disagrees refuses to start; a
dynamically discovered one fails that one request. The reason is that the
shard index is part of the object key, so a silently changed count would
write and read different key sets, and a query would answer from a subset of
the tenant's data without knowing it. Changing the count is therefore an
append to a generation history with an activation hour, never an edit, and
readers derive the shard fan-out of a given hour from that history.

## Segments and commit records

A shard actor accumulates records until a flush trigger fires, then builds
one immutable columnar segment in memory and writes it to the store. That
segment is a data object: it holds telemetry bytes and nothing else. It is
written once and never modified.

The shard then creates a commit record, a small second object that names the
data object, its shard, its writer identity and sequence number, its event
time bounds, and its record count. The commit record is the publish. A query
sees only data that some commit record names, so until the second PUT
succeeds the first object might as well not exist.

Two objects, in that order, and the order is the whole design. A crash
between them leaves a data object that no commit record names, which is
invisible to every query and which a later sweep deletes after a grace
period. The reverse order would leave a commit record naming bytes that may
never have landed, which is a corrupt catalog rather than a bit of wasted
storage. Because the failure mode of the chosen order is "an object nobody
reads" and the failure mode of the other order is "a reader follows a
dangling reference", Ravel never needs a repair pass over the catalog.

Both objects are immutable, and the commit record is created with a
conditional create-if-absent put. Two writers that somehow raced onto one
commit key cannot both win: one gets the object, the other gets a typed
already-exists error and re-reads. Immutability plus conditional creation is
what lets many processes write to one bucket with no coordination beyond the
store itself.

Segments come in two levels. An L0 segment is what a shard actor flushed: one
per flush, small, and numerous. An L1 segment is what compaction produces
from many L0 segments of the same bucket: fewer, larger, and cheaper to scan.
Both are segments and both are immutable; the level says only who wrote it
and roughly how large it is. The formats are frozen contracts, one per
signal: [RSEG](segment-format.md) for metrics,
[RLOG](log-segment-format.md) for logs, and
[RSPAN](span-segment-format.md) for spans.

## Acknowledgement and visibility, which are different things

Acknowledgement is what the write API told the client. Visibility is whether
a query can see the data. They are separate properties with separate rules,
and conflating them is the most common way to misread Ravel's guarantees.

There are two acknowledgement modes. Under strict acknowledgement, the
default, an export is acknowledged only after every batch it contributed to
has its L0 data object durably stored and its commit record created. The
response carries a commit token set, one token per shard the request's points
flushed through, in the `x-ravel-commit-token` header as a comma-separated
list. After a strict acknowledgement, no crash of any Ravel process may lose
that data: object-store durability is the floor, and the data survives
anything the object store survives.

Under buffered acknowledgement, opt-in per tenant or per request, the
response comes after admission and enqueue to a shard actor. It returns no
commit token, and it is never described as durable, because a crash between
the acknowledgement and the flush loses the buffered window, bounded by the
maximum flush delay. A clean shutdown drains that window, so an orderly
restart loses nothing; an abrupt process death does. Buffered mode is offered
on OTLP ingest only. Remote Write is strict-only, and a buffered-mode header
on a Remote Write request is ignored rather than honoured. In both modes,
admission failures such as limits, authentication, and quota reject before
anything is buffered, so a rejection is never a silent loss.

Visibility is the other property. A batch becomes visible to queries when its
commit record exists, and commit-record creation is atomic, so visibility is
atomic per data object: a query sees all of a segment's rows or none of them,
never half. Visibility latency is the flush delay plus the data PUT plus the
commit PUT, and the flush delay is a configured operator budget rather than a
fixed constant.

Putting the two together: a strict acknowledgement implies visible, because
the commit record existed before the response. A buffered acknowledgement
implies neither stored nor visible, only admitted; visibility follows
whenever the flush completes. And visibility can arrive without any
acknowledgement at all, which is exactly the crash case where the commit PUT
succeeded and the response never reached the client. The normative text is
[docs/consistency-model.md](consistency-model.md), sections
[Acknowledgement semantics](consistency-model.md#acknowledgement-semantics)
and [Visibility semantics](consistency-model.md#visibility-semantics).

## Read-your-write

Read-your-write in Ravel is caller-driven and opt-in. A caller that holds
commit tokens from a strict acknowledgement passes them back to a query API
as `min_commit_token`, repeatable, once per token. Each token fully
determines its commit-record key, so the catalog does not search: it GETs
those exact keys and includes their segments in the resolved snapshot. If a
token names a key that cannot be satisfied, the query fails with a typed
`unsatisfiable token` error. The answer is therefore either the write or an
explicit error, never silently stale data.

Without a token, a query sees some recent consistent snapshot, and freshness
is bounded by listing behaviour rather than guaranteed. That is the honest
limit, and it is worth stating plainly: an unqualified query issued
immediately after a write may or may not include that write, and no
configuration turns "may" into "must". What Ravel guarantees instead is that
whatever the query does see is a consistent snapshot, and that a caller who
needs the stronger property has a mechanism that costs one header.

Two consequences follow. First, only strict acknowledgement gives you a
token, so buffered mode and read-your-write are mutually exclusive by
construction rather than by accident. Second, read-your-write is per token,
not per client and not per session: a caller that writes to three shards and
presents two of the three tokens has asked for two of the three writes, and
gets exactly that. The normative text is
[Read-your-write](consistency-model.md#read-your-write).

## The catalog: fold, snapshot, HEAD

Three words name three different things, and the tree has used them
interchangeably in the past. They are not interchangeable.

A snapshot is a logical, immutable set of segments. A query resolves one
snapshot and uses it for its entire execution, so commits, compactions, and
deletions that land mid-query cannot change its answer. A snapshot part is
one piece of a published snapshot, immutable and content-addressed, holding
the folded state of one sealed ingest hour. The word "part" alone is
ambiguous in this repository, so a piece of a snapshot is always a snapshot
part, and a data object is always a segment.

A fold is the act of turning commit records into snapshot parts. It runs as a
background task per tenant and signal, and also on demand for one tenant and
signal. It lists the commit records whose ingest hour has sealed, writes them
into snapshot parts, and publishes a new HEAD. Folding is not compaction: it
touches no telemetry bytes and produces no new segments. It is a cost
optimisation only, and it never changes which commits a query sees.

HEAD is the catalog's one mutable object, one per (tenant, signal): a
pointer naming the current snapshot parts, written only with a
compare-and-swap on its version. Two folders that race are therefore safe:
the loser's compare-and-swap fails, it re-reads, and nothing is corrupted. A
query begins with one GET of HEAD, which pins the snapshot it will use for
the rest of its execution. A few other per-tenant records change after they
are written, each under its own rule: admission usage is overwritten on every
reconciliation interval, alert state moves by compare-and-swap, the
encryption key-epoch record is append-only, and tenant configuration is
rewritten by the operator tool. None of them is part of the catalog, and a
snapshot names none of them.

One consequence surprises operators the first time. An ingest hour seals only
after the maximum flush lifetime plus a clock-skew allowance plus a fold
safety margin has passed, so a fold run immediately after a load finds
nothing eligible and publishes nothing. That is the correct answer, not a
failure, and the on-demand fold reports it as a distinct status rather than
as success. Everything the catalog does above the sealing watermark is served
by listing, which is why unqualified freshness is bounded by listing
behaviour. The exact protocol is in
[docs/catalog-and-mvcc.md](catalog-and-mvcc.md), and the isolation property
is [Snapshot isolation](consistency-model.md#snapshot-isolation).

## Background maintenance: compaction, retention, sweep

Three background loops reshape the bucket, and only a `maintain` mode
process runs them. `all` mode runs ingest, query, the catalog fold, and
alert evaluation in one process; it does not compact, expire, or sweep
anything. A deployment with no `maintain` process therefore never deletes an
object, and its L0 segments accumulate unmerged. The quickstart stack runs
`all` mode alone, which is fine for an evaluation. Maintenance supervisors
derive the tenant set from storage by listing tenant prefixes, not from a
flag, so no configuration can silently exclude a tenant from retention; a
discovery failure skips and retries the cycle rather than falling back to an
empty set. Work is partitioned across the live maintain workers by
rendezvous hashing over a heartbeat-derived live set, so N replicas divide
the work instead of each doing all of it.

Compaction makes L1 segments from many L0 segments of one (tenant, signal,
shard, ingest hour) bucket, streaming blocks rather than materialising the
inputs. It is publish-then-supersede: the run writes its L1 segments, then
publishes one compaction record with a conditional create-if-absent put that
names its exact input set. Nothing about the inputs is mutated or removed at
publish time. Before that put, the run checks that the record counts of its
inputs and its outputs are exactly equal and aborts if they are not, because
publish is the point of no return. Two compactors racing on one bucket
converge: create-if-absent picks one record as the winner, and the loser's
segments are unreferenced objects that age out.

Retention expires data by age against the tenant's configured policy. Like
every deletion in Ravel it is a durable transaction first, a tombstone
object, then logical exclusion from newly resolved snapshots, and only then
physical removal.

The sweep is the physical removal step, and "sweep" is the word for it; it
sits under garbage collection as the umbrella term. It deletes objects that
nothing references, behind grace periods and a mass-orphan circuit breaker,
so a listing anomaly that makes half the bucket look unreferenced withholds
deletions instead of amplifying them. Every sweep pass is stateless and
restartable from zero, and every delete is idempotent.

None of the three can change a query answer, and that is a design constraint
rather than a hope. Compaction is a verbatim page copy that never
deduplicates, and query-time deduplication collapses the duplicate candidates
if a snapshot happens to include both an L0 input and its L1 replacement, so
every intermediate state of a compaction is query-correct. Retention and the
sweep physically remove only what no live snapshot references, and only after
a protection horizon. A snapshot resolved before, during, or after any of
these loops returns the same rows. The deletion mechanics are in
[Deletion and GC](consistency-model.md#deletion-and-gc).

## Consistency boundaries

There is no cross-shard ordering guarantee. A query snapshot may include
commit N+1 of one shard and not commit M of another, regardless of the
wall-clock order in which those commits were created. Nothing in the write
path establishes a total order across shards, because doing so would need
exactly the coordination the disposable-process model refuses.

Per writer and shard, commits are sequenced. Each shard actor writes its
commits under a monotonically increasing sequence number for its writer
identity and epoch, so within one shard the history is a line, not a set.
That is the whole of the ordering Ravel offers, and every other ordering
claim a reader might want has to be built from commit tokens instead.

What that means in practice: a client that writes two batches whose records
hash to different shards cannot assume that a later query sees both or
neither. It can see either one alone. A client that needs both must hold the
commit tokens from both writes and present them together, which turns an
ordering question into a per-token inclusion question the catalog can answer
exactly.

Delivery is at-least-once, so a client retry after a lost acknowledgement
re-ingests the batch and both copies are stored. For metrics that is
harmless, because queries deduplicate by series and timestamp, exactly as
Prometheus would collapse a doubly scraped sample. For logs and spans, where
two identical records are legitimate data, an opt-in idempotency key makes a
retry a no-op: the marker object is written before the acknowledgement, so a
replay finds the marker and stores nothing new. Ravel does not offer
end-to-end deduplication without that key, and does not pretend to.

Finally, the boundaries are per (tenant, signal). There is no transaction, no
snapshot, and no ordering guarantee that spans two tenants or two signals of
one tenant. A correlated read across signals, such as a metric exemplar to
its trace, resolves one snapshot per signal and is documented as such.

## Failure and retry

Every failure in the write path lands in one of four places, and the outcome
of each is decided by which of the two PUTs had completed. The full table is
the [crash matrix](consistency-model.md#crash-matrix-strict-mode); in prose,
under strict acknowledgement:

A crash before the data PUT stores nothing at all. The client has no
acknowledgement, retries, and the retry is the first attempt as far as the
store is concerned.

A crash after the data PUT and before the commit PUT leaves the data object
present and unreferenced. It is invisible to every query, garbage collection
removes it after the grace period, and the client's retry writes a fresh pair
of objects. This is the window the two-object order exists to make harmless.

A crash after the commit PUT and before the response is the only genuinely
ambiguous case, and it is ambiguous for the client, not for Ravel. The data
is durable and visible, but the client never learned that. An unkeyed retry
stores a duplicate, handled by query-time deduplication for metrics and
visible as two records for logs and spans. A keyed retry replays the
idempotency marker and stores nothing new, because the marker PUT precedes
the acknowledgement.

A crash after the response changes nothing: the data is durable and visible,
and there is nothing to retry.

Retries are safe everywhere because no persisted step is a read-modify-write.
Data objects are content-addressed, commit records and markers are created
with create-if-absent, and HEAD moves only under compare-and-swap. Where an
API cannot tell you which side of a boundary a failure fell on, it says so
rather than guessing: a 503 from the on-demand fold means the outcome is
unknown and the call should be retried, never that nothing was written.
Authentication and validation errors, by contrast, are returned before any
store work happens, so a 401, 400, or 403 does guarantee nothing was written.

## Glossary

This is the canonical definition of each term. Where the repository has used
another word for the same thing, the alias is named, because a reader who
meets it in a command, an API response, a metric label, or an object key
needs to know it is the same idea. It is also the only place acronyms are
expanded; other pages use them bare.

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
- **OTAP**: OpenTelemetry Arrow Protocol, the bidirectional gRPC protocol
  that carries telemetry as Arrow record batches. Feature-gated in Ravel; see
  [docs/otap-ingest.md](otap-ingest.md).
- **OTLP**: OpenTelemetry Protocol, the wire protocol Ravel's primary ingest
  surface speaks, over both HTTP and gRPC.
- **RAM tier**: the read cache's in-memory layer. See tier.
- **retention**: age-based expiry of a tenant's data under its configured
  policy. A durable tombstone first, then exclusion from new snapshots, then
  physical removal by the sweep.
- **RLOG**: Ravel Log Segment Format, the columnar segment format for logs.
  Frozen contract; see [docs/log-segment-format.md](log-segment-format.md).
- **RPO**: recovery point objective, the amount of recent data a recovery is
  allowed to lose. Ravel publishes a number only from a real rehearsal; see
  [guides/disaster-recovery.md](guides/disaster-recovery.md).
- **RSEG**: Ravel Segment Format, the columnar segment format for metrics.
  Frozen contract; see [docs/segment-format.md](segment-format.md).
- **RSPAN**: Ravel Span Segment Format, the columnar segment format for
  spans. Frozen contract; see
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
  `--typed-attr-column`; their help text calls the result a declared typed
  attribute column, which means the same thing.
- **UDAF**: user-defined aggregate function, an aggregate Ravel registers
  with the SQL engine beyond the engine's own set.
- **UDF**: user-defined function, a scalar function Ravel registers with the
  SQL engine, such as the label accessors on the `samples` table.
- **UDTF**: user-defined table function, a function that returns a table
  rather than a value. Ravel registers none, and the SQL surface's tables are
  the registered tables only.
- **WORM**: write once read many, a bucket policy that forbids overwriting or
  deleting an object version for a retention period. Ravel's bucket
  protection contract asks for it, as Object Lock in compliance mode, on the
  deployment records, the provisioning records, the commit records, and the
  catalog HEAD history, paired with versioning so that a HEAD
  compare-and-swap creates a new locked version rather than overwriting one.
  The contract is in [docs/object-store-contract.md](object-store-contract.md).
- **writer**: the identity a shard actor writes under, carried in commit and
  data object keys as a writer id and epoch. Commits are sequenced per
  writer and shard, and a restarted process takes a new epoch rather than
  reusing a sequence number.
