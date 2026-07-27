# ADR-0019: Age-based retention via bucket tombstones and horizon-gated sweep

Status: Accepted (2026-07-27). Implementation plan and tickets:
docs/compaction-retention-plan.md. Builds on the sweeper, key-layout, and
horizon machinery of ADR-0018 but is independently adoptable: retention
of never-compacted L0 buckets needs no compactor.

## Context

Nothing in Ravel deletes data by age; a long-lived bucket only grows
(docs/guides/operations.md). Retention introduces the second deletion
trigger after ADR-0018's supersession sweep, and the first one that
destroys data rather than a redundant copy of it. The
consistency-model already promises the shape: "a durable tombstone
transaction, then logical exclusion from new snapshots, then physical
removal via GC". Tenancy is per ADR-0009; windows must be per tenant.

## Alternatives

1. Resolver-side age filtering from config, with GC trailing behind.
   Rejected: visibility would depend on every resolver holding the same
   retention config at the same moment; a stale node re-includes data
   another node's sweeper is deleting, turning config rollout lag into
   query failures and split-brain visibility. Config must never be a
   correctness input to resolution.
2. Object-store lifecycle rules (S3 native expiration). Rejected: the
   store deletes data objects independently of commit records, producing
   records that point at nothing (SnapshotInvalidated storms, permanent
   `unsatisfiable token` states), and durability arguments are made
   against the ObjectStoreBackend contract, never a vendor feature.
3. Per-object tombstones. Rejected: doubles object churn in exactly the
   buckets we are trying to shrink, and age expiry is inherently a
   bucket-granular event given hour-bucketed keys.
4. Bucket-granularity tombstone, then horizon-gated physical sweep
   (chosen).

## Decision

1. **Window.** Per-tenant retention window R over event time. Enforcement
   unit is the ingest-hour bucket. A bucket is expired when it is sealed
   (same rule as ADR-0018) and every record in it (L0 commit records and
   compaction records) has `max_event_ts < now - R`. R is an exact
   floor: no sample younger than R is ever excluded. The cost of bucket
   granularity is bounded over-retention, not under-retention.
2. **Tombstone.** New protobuf message `RetentionTombstone` (additive):
   identity fields, ingest_hour_bucket, retired_at_ns (injected clock),
   the R applied, and the record count observed. Written with
   CreateIfAbsent to a fixed per-bucket key in the commit prefix, so the
   existing single LIST discovers it:

   ```
   t/<th>/m/c/<shard>/<hour>/retire.tmb
   ```

   A durable tombstone is irreversible: raising R later never resurrects
   a tombstoned bucket. Shrinking R and growing it back is therefore a
   destructive operation and is documented as such.
3. **Visibility.** A resolver that lists a tombstone excludes the entire
   bucket (L0 records, compaction records, parts) from the snapshot. A
   min_commit_token referencing a tombstoned bucket resolves as
   satisfied-with-zero-segments: the data was deliberately deleted, which
   is not the lost-data condition `unsatisfiable token` exists to expose.
4. **Physical sweep.** When `now >= retired_at_ns + protection_horizon`,
   delete the bucket's commit records, compaction records, L0 data
   objects, and L1 parts; delete the tombstone last, only after a
   verifying LIST shows the bucket prefixes empty. Every delete is
   idempotent (NotFound = Ok); a crash anywhere leaves the tombstone in
   place, so exclusion holds and the next sweep finishes the job.
   Record-less data objects left by a mid-sweep crash also converge via
   the orphan GC rule. The horizon between tombstone and physical delete
   is what protects pinned in-flight queries, anchored on the durable
   retired_at_ns exactly as supersession anchors on the compaction
   record's created_unix_ns.
5. **Config.** Deployment config maps tenant to R; default is no
   retention. This follows the ADR-0010 §9 shard_count precedent
   (config now, per-tenant manifest object when that lands). Only the
   sweeper reads R; resolvers never do (see alternative 1). Config
   validation rejects R below `max_ingest_lag + max_flush_lifetime +
   clock_skew_allowance` plus one bucket span, so a bucket can never be
   tombstoned before it is sealed.
6. **Interaction with compaction.** The retention check runs before the
   compaction trigger, so expired buckets are never compacted first. If
   a racing compactor publishes into a bucket that just got tombstoned,
   the tombstone's bucket-wide exclusion covers the new record and parts,
   and the sweep deletes them; the compactor also checks for a tombstone
   before starting, as an efficiency measure only.
7. **Timing bounds (documented, not configurable).** Visibility of
   expired data ends within R + one bucket span + max_future_skew + one
   sweep interval. Physical bytes are gone within that plus the
   protection horizon and one further sweep interval. Both bounds are
   over-retention only.

## Consequences

- Ravel gains its first age-triggered destruction of acknowledged data;
  docs/guides/operations.md's "nothing deletes data" section is replaced
  by the retention and GC description, including the irreversibility
  warning.
- The same tombstone-then-sweep machinery is the natural substrate for
  future explicit deletion (tenant offboarding, GDPR-style deletes);
  those need their own ADR for selection semantics but no new visibility
  mechanism.
- Storage growth becomes bounded for tenants with configured windows;
  capacity planning guidance changes accordingly.
- The commit-record cache (ADR-0010 §10) gains its long-promised
  invalidation trigger: entries for a bucket are dropped when a tombstone
  is observed. Until then cache entries remain immutable-safe because
  exclusion happens at resolution, before cache consultation matters.
- v1 population shrinks from the old end even where compaction never
  ran, completing the ADR-0018 §8 retirement story.
