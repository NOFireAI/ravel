# Commit publication, acknowledgement, retry, read-your-write

TLC checked this finite model under the bounds and assumptions below. It
verifies the protocol design; implementation conformance is argued in
`traceability.md` and asserted by the Rust tests named there, not proved.
Safety and liveness are stated separately, and every liveness result names
the fairness it needed.

## What the state means

| Model variable | Ravel state |
|---|---|
| `phase[f]` | where one pinned flush stands: pinned, data durable, committed, acknowledged, abandoned past its lifetime, stopped by split brain, or retired by a crash |
| `pinned[f]` | the bytes and their content hash, fixed once at flush open and reused by every retry |
| `openedAt[f]` | the flush-open tick that anchors the lifetime deadline |
| `retries[f]` | the store-retry budget one flush has spent |
| `shardDead[s]` | a shard whose actor died of split brain; every later write to it fails |
| `ackKind[f]` | what the client was told: nothing, strict, buffered, a timeout, or an error |
| `marker` | the logs and spans idempotency marker object |
| `lastPut` | a witness of what the commit PUT actually returned, read off the store rather than asserted by the action |
| `publishedAt[f]` | the tick at which a commit-record write landed, so "nothing is published after the deadline" is checkable |
| `store` and its siblings | the shared object store, instantiated from `common/RavelObjectStore.tla` |

A flush identity is one `(writer, shard)` pair. The writer stands for a
`(writer_id, epoch)` and `seq` is allocated at pin time, so one pair names
one flush. A crash **retires** its identity rather than reusing it, because
the restarted process mints a fresh writer id; a different pair models that
restarted process.

Query visibility is exactly "a commit record exists". Nothing here models
segment contents, series identity or query evaluation.

## Assumptions, stated rather than checked

- **Data-object PUT idempotency.** `put_data_object` returns success on
  `AlreadyExists` with no read-back, so no property here can detect a data
  key bound to different bytes. Safety rests on the pinning invariant plus
  the key layout, which the model takes as given. The commit record's
  idempotency is the opposite: it **is** checked, because its
  `AlreadyExists` path reads the winner back and compares content hashes.
- **The segment encoder.** Bytes are an abstract element and a content hash
  is that same element.
- **The object store honours its own contract.** Every store operation comes
  from the shared module.

## Out of scope

- Commit-record reconstruction (ADR-0058), which derives a record's
  `created_unix_ns` from the store's advisory `last_modified`. No property
  here reads `last_modified` and that path is not modelled at all.
- Cross-shard atomicity and ordering, which Ravel does not offer. The model
  must be able to **reach** a state with one shard durable and another not;
  see the reachability obligations below.

## Two obligations that are the absence of a guarantee

TLC checks invariants, so each is written as a predicate that must **fail**.
The run is correct exactly when TLC reports the violation, and a variant that
removed the behaviour would leave it green.

Both run in the negative lane, because "TLC must report this violated" is
exactly the harness's negative contract:

- `negative/no-cross-shard-atomicity.cfg`. A state with one shard's commit
  durable and another's not must be reachable.
- `negative/at-least-once-duplicate-reachable.cfg`, with no idempotency key.
  A retry after a lost acknowledgement must be able to leave two durable
  commit records holding the same content. `AtLeastOnce` alone would be
  satisfied by exactly-once delivery, which is why this obligation exists.
  `dedup-mutant.cfg` is the same configuration with `RetryDedups` set: the
  duplicate becomes unreachable and TLC exits 0, which is what proves the
  obligation is not vacuous. The harness's negative lane accepts only a
  must-violate expectation, so that one run is done by hand and recorded in
  `results.md`.

## A modelling error the invariants caught

Two invariants failed on the first correct-form run and both were the model's
fault, not the protocol's. They are recorded because they show the invariants
are load-bearing:

- The first version let a crashed flush re-pin its own identity with
  different content. The shipped writer cannot: it mints a fresh writer id.
  `RetrySamePinnedFlushIdempotent` caught it, and the crash now retires the
  identity while a separate `ReuseIdentity` action models accidental reuse as
  the hazard it is, defended by the hash compare.
- The first version claimed an abandoned flush leaves nothing durable. A
  commit whose **response was lost** is durable even though its writer then
  gives up and reports an error, which is the documented ambiguity rather
  than a violation. The invariant now reads the `publishedAt` witness and
  says no write was issued after the deadline.

## Running

```sh
scripts/check-tla.sh smoke -a commit
scripts/check-tla.sh negative -a commit
scripts/check-tla.sh traceability -a commit
```

`exhaustive.cfg`, `live.cfg` and `dedup-mutant.cfg` carry the remaining
configurations; `results.md` records what each produced.
