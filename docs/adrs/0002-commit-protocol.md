# ADR-0002: Two-object commit protocol with create-if-absent commit records

Status: Accepted

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

Option 3. Sequence: serialize L0 → blake3 content hash → PUT data object with
`create-if-absent` (the key embeds writer identity, sequence, and content
hash, so a retried PUT that finds the object already present is idempotent
success) → PUT commit record with `create-if-absent` → ack with commit token.

Commit record key: `t/<tenant_hash>/<signal>/c/<shard>/<ingest_hour>/
<writer_id>.<epoch>.<seq>.cmt`. Retrying the same logical commit hits the same
key; on `AlreadyExists` the writer GETs the record, verifies content hash
equality, and treats it as success (idempotent) or a fatal split-brain error
(mismatch).

Commit token (opaque to clients): the token format shipped as v2, not the
v1 shape this ADR originally recorded; see "Superseded details" below.

Orphan rule: data objects without a commit record are invisible and GC-eligible
after a grace period (default 24 h) keyed on object creation time.

## Superseded details

This ADR's decision (two objects, commit-record existence as the sole
visibility truth, create-if-absent idempotency) stands. Two specifics it
originally stated are superseded by ADR-0010; that ADR is authoritative:

- The data PUT is `create-if-absent` with no separate verify step (the
  content-addressed key makes a present object identical by construction);
  the "→ verify →" step in the original sequence never shipped
  (crates/ravel-commit/src/publish.rs).
- The commit token is v2, `v2:<shard>:<writer_id>:<epoch>:<seq>:
  <ingest_hour_bucket>` base64url-encoded, carrying the ingest_hour_bucket
  the v1 shape lacked (ADR-0010 §2). Use ADR-0010 for the current token
  format and full commit sequence.

## Consequences

- Two PUTs per flush; commit records are small (<2 KiB) so cost is dominated
  by the data PUT.
- Visibility is atomic: a reader either lists the commit record or does not.
- Writer epochs (new epoch per process start, monotonic seq within epoch) make
  duplicate publication detectable without coordination.
- Requires create-if-absent from the store, part of the mandatory production
  capability set (see object-store contract).
