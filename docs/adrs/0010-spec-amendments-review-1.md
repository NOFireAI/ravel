# ADR-0010: Spec amendments from the first adversarial design review

Status: Accepted

## Context

An adversarial review of the commit protocol, catalog resolution, RSEG v1,
object-store contract, and tenant-isolation specs found correctness flaws
that are cheap to fix before any writer ships and expensive after. This ADR
fixes the decisions.

## Decisions

1. Pinned flush identity (fixes retry non-idempotency, the top finding).
   At flush open the writer pins an immutable identity: {seq, ingest_hour
   bucket, serialized segment bytes, blake3}. Retries of the data PUT or the
   commit PUT reuse all four verbatim. A retry MUST NOT re-serialize, MUST
   NOT accrete new samples, and MUST NOT re-read the clock. CommitRecord
   gains an `ingest_hour_bucket` field so the bucket is self-describing.
   With bytes pinned, an AlreadyExists with a different content hash is
   genuinely split-brain and crashing is correct.

2. Commit token v2 and token sets. Tokens carry the ingest-hour bucket:
   `v2:<shard>:<writer_id>:<epoch>:<seq>:<hour_bucket>`. A token fully
   determines its commit-record key, so read-your-write resolves by exact
   GET/HEAD of that key, never by re-listing. Ingest acks return a token
   set (one per shard flushed); query APIs accept multiple tokens.

3. Writer identity rules. `writer_id` MUST be freshly random per process
   start and MUST NOT be derived from stable identity (hostname, pod name,
   shard index, config). `seq` is monotonic per (writer_id, epoch, shard);
   gaps are permitted and carry no meaning. `epoch` is informational only.

4. RSEG page and trailer checksum coverage. The page crc32c covers
   series_id (16 bytes, as a crc seed prefix) plus enc, comp, and payload,
   so a flipped encoding byte or a mis-planned range fails closed instead
   of decoding garbage. footer_crc32c covers the footer proto plus all
   trailer bytes except the crc field itself. Section/page
   `uncompressed_len` must match the decompressed size exactly and is
   capped by config. Duplicate known section kinds are Corrupted.
   Timestamp delta accumulation is overflow-checked and validated against
   series bounds. Per-series sample sort is stable.

5. Cross-segment duplicate-timestamp order. Query-layer dedup by
   (series_id, ts) under the total order (commit created_unix_ns,
   writer_epoch, writer_seq, in-page index); last wins. Sample values
   compare by f64 bit pattern everywhere (NaN payloads and -0.0 are
   significant; staleness markers are NaN payloads).

6. Series identity hardening. SeriesId::compute rejects any component
   longer than u16::MAX bytes and more than u16::MAX labels (Result, not
   silent truncation). Admission limits keep real inputs far below this;
   the hash contract no longer depends on unwritten code.

7. Data objects PUT with CreateIfAbsent (AlreadyExists = success; the key
   embeds the content hash). Data keys embed 16 hex chars of the blake3
   (was 8). Readers never address by `CommitRecord.object_key`: the key is
   reconstructed from record fields and a mismatch is a fatal invariant
   breach. After the suffix GET, readers verify footer tenant_hash, shard,
   writer_id, epoch, seq against the commit record.

8. Event-time skew is bounded at admission: samples with
   event_ts > ingest_ts + max_future_skew (default 10 m) are rejected;
   samples with event_ts < ingest_ts - max_ingest_lag are rejected (both
   configurable). The catalog listing window derives its correctness from
   these admission bounds plus a clock_skew_allowance pad on `now`.

9. shard_count is immutable per generation; the generation history is
   append-only; the shard-index domain of hour `h` is `0..scan_count(h)`
   (ADR-0052, online resharding, supersedes this item's original "immutable
   per (tenant, signal) in v1"). The durable provisioning record
   (`t/<tenant_hash>/<signal>/prov`, ADR-0050 §5) holds the scalar
   `shard_count` and an append-only `generations` history; resolvers derive
   the per-hour shard set from it. A reshard appends a new generation with a
   future `activation_hour` under `CasVersion`; existing data is never moved
   or re-keyed.

10. Commit-record cache: keyed by full object key, entries validated
    against expected tenant_hash/signal/shard on hit, bounded per tenant,
    invalidated by deletion transactions (or TTL-bounded once deletion
    exists).

11. GC interlock: writers abandon a flush after max_flush_lifetime
    (default 1 h) and never publish it afterward; orphan GC only considers
    objects older than grace + max_flush_lifetime and re-verifies
    commit-record absence immediately before each delete. GC protection
    horizon >= max_query_duration + grace. Store NotFound on a pinned
    segment surfaces as SnapshotInvalidated; the frontend re-resolves and
    retries once.

12. Object-store contract: conditional-put failures map mode-dependently
    (AlreadyExists under CreateIfAbsent, PreconditionFailed under CasEtag)
    with a real-S3/MinIO conformance test; list is paginated with an
    explicit cross-page guarantee; PutOptions gains an upload checksum;
    ObjectMeta gains an opaque Version distinct from Etag (GCS
    generations); Suffix(0) is InvalidRange; StoreError gains
    AccessDenied; Capabilities covers all mandatory rows.

13. Tenant-hash claim corrected: unkeyed BLAKE3 prevents reading tenant
    names from keys but allows offline confirmation of guessed ids by
    anyone with list access. A deployment-keyed hash is available via
    config for deployments that need enumeration resistance.

    Correction (ADR-0050 §3, EC3): the last sentence was never true when
    written. No keyed derivation or key-loading config existed in the
    workspace; the claim was aspirational. ADR-0050 §3 makes the keyed
    variant real, default (for buckets created after it), and durable: the
    scheme is pinned per bucket by the `sys/tenancy` marker, keyed via
    `blake3::derive_key("ravel-tenant-v2", deployment_key)` with the key
    loaded from `--tenant-hash-key-file`, and a wrong key is a startup
    refusal rather than a silent parallel namespace. Existing unkeyed
    buckets stay unkeyed permanently; there is no re-key migration.

## Consequences

- proto/ravel/commit.proto gains field 19 (ingest_hour_bucket) before any
  record exists; no migration.
- ravel-types CommitToken encoding becomes v2; SeriesId::compute returns
  Result. In-flight implementation branches reconcile at merge.
- docs/catalog-and-mvcc.md, docs/segment-format.md,
  docs/object-store-contract.md, docs/consistency-model.md updated in
  place; they remain the normative contracts.
