# ADR-0002: Two-object commit protocol with create-if-absent commit records

Status: Accepted (2026-07-26)

## Context

An uploaded data object must not be visible until published, retries must be
idempotent, and a crash between upload and publish must leave only an
invisible orphan. There is no coordination service; the object store's
conditional-write primitives are the only consensus we get.

## Alternatives

1. Single self-committing object (data object visibility = existence): listing
   races and partial multipart uploads make "exists" ambiguous, and orphan
   cleanup becomes indistinguishable from committed data.
2. Central metadata service with its own store: forbidden stateful dependency.
3. Data object + separate immutable commit record written with
   create-if-absent; commit record existence is the sole visibility truth.

## Decision

Option 3. Sequence: serialize L0 → blake3 content hash → PUT data object
(unique key, overwrite mode is safe because the key embeds writer identity,
sequence, and content hash) → verify → PUT commit record with
`create-if-absent` → ack with commit token.

Commit record key: `t/<tenant_hash>/<signal>/c/<shard>/<ingest_hour>/
<writer_id>.<epoch>.<seq>.cmt`. Retrying the same logical commit hits the same
key; on `AlreadyExists` the writer GETs the record, verifies content hash
equality, and treats it as success (idempotent) or a fatal split-brain error
(mismatch).

Commit token = `v1:<shard>:<writer_id>:<epoch>:<seq>` (opaque to clients).

Orphan rule: data objects without a commit record are invisible and GC-eligible
after a grace period (default 24 h) keyed on object creation time.

## Consequences

- Two PUTs per flush; commit records are small (<2 KiB) so cost is dominated
  by the data PUT.
- Visibility is atomic: a reader either lists the commit record or does not.
- Writer epochs (new epoch per process start, monotonic seq within epoch) make
  duplicate publication detectable without coordination.
- Requires create-if-absent from the store, part of the mandatory production
  capability set (see object-store contract).
