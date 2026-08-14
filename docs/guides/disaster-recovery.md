# Disaster recovery posture

This document states plainly what Ravel does and does not provide for
disaster recovery. It is deliberately honest about the gaps rather than
implying a mechanism the system does not have (ADR-0058 decision 5). Read it
before you rely on any assumption about surviving the loss of your object
store bucket.

## What Ravel does not provide

- **No built-in backup mechanism.** Ravel writes to exactly one bucket and
  reads from exactly one bucket. There is no snapshot job, no point-in-time
  export, no second copy written anywhere by any Ravel process. `S3Config`
  holds one bucket and one region (ADR-0055 Context); there is no code path
  that copies objects elsewhere.
- **No cross-region failover.** There is no replica bucket Ravel knows about,
  no failover target, and no mechanism that redirects reads or writes to a
  second location on an outage. "Replica-bucket failover" is not a Ravel
  feature; it describes a hypothetical operator action, not a half-built
  capability (ADR-0058 Context).
- **No RTO/RPO guarantee.** Ravel makes no recovery-time or recovery-point
  promise. If the bucket is lost, the data addressed by that bucket is lost;
  there is no bounded window of acceptable loss the system enforces or
  restores to.

## The one thing that is true today

The bucket is a **single point of loss**. Every durable byte — data objects,
commit records, compaction records, tombstones, catalog snapshots, the
control objects under `sys/` — lives only there. Correctness depends on it
entirely (docs/consistency-model.md: object storage is the source of truth;
compute processes are disposable, see [operations.md](operations.md), section
"Disposability").

The only mitigation available today is the object store's own durability
(for S3, its published per-object durability), optionally complemented by
**S3 versioning and Object Lock**. Ravel's `store qualify` reports, for
information only, whether Object Lock/versioning appears enabled on the bucket
(ADR-0055 section 3), but Ravel cannot act on it: `object_store` exposes no
per-PUT retention header, so this is a signal to the operator, not an enforced
control (ADR-0042 decision 3, ADR-0055 section 3).

Versioning and Object Lock are a **complement to, not a substitute for, real
backups.** They protect against overwrite and delete within the same bucket
and account; they do nothing if the bucket, the account, or the region itself
is lost. Enabling them is worthwhile and cheap, but do not mistake them for a
disaster-recovery plan.

## What a real DR posture would require (future work, not built here)

A genuine DR posture is named here as future work, not delivered by this
document. Two shapes are plausible:

- **Periodic bucket-to-bucket copy with an explicit staleness bound.** A
  scheduled copy (for example `aws s3 sync`, or a lifecycle-driven
  replication job) to a second bucket, with a stated maximum staleness (the
  RPO you accept). Because Ravel's objects are immutable and content-
  addressed, a copy that is internally consistent as of some point in time is
  a usable restore source; the bound is how far behind that point you allow it
  to run.
- **S3 Cross-Region Replication (CRR), with its caveats documented and
  accepted.** CRR is **asynchronous** and gives **no cross-bucket atomic
  writes and no strongly-consistent listing** across the replica. Several of
  this system's correctness arguments depend on exactly those properties on
  the live bucket — the commit-then-visible ordering, the seal/GC/compaction
  reasoning, and the strongly-consistent LIST the sweeper's orphan re-verify
  relies on (docs/consistency-model.md, docs/catalog-and-mvcc.md). A naive
  failover that simply pointed Ravel at a CRR replica would therefore
  **silently violate invariants the system assumes hold**: a data object could
  be present without its commit record (or vice versa) at the replica because
  replication reordered or lagged them, and a LIST against the replica could
  miss recently written keys. Adopting CRR as a DR target means accepting and
  documenting these caveats explicitly, and treating a failover as a
  deliberate, verified operation (re-run `ravel-cli maintain verify-custody`
  and `ravel-cli catalog verify` against the replica before trusting it), not
  an automatic redirect.

Neither is built today. This document exists so the gap is stated rather than
discovered during an incident.

## The one failure mode Ravel does close today

Loss of **commit records** for a shard — an accidental delete, a bad S3
lifecycle rule, a fat-fingered prefix delete — is recoverable, as long as the
data objects those records named still exist. `ravel-cli commit reconstruct`
(ADR-0058) rebuilds each record-less L0 data object's commit record from the
object's own footer, one `(tenant, signal, shard)` at a time, writing
`CreateIfAbsent` (it never overwrites or deletes).

This is a narrow recovery path, not a general DR mechanism: it recovers lost
*metadata* when the *data* survives, not lost data. If the data objects are
also gone, reconstruction has nothing to rebuild from. Run it with maintenance
stopped for the affected tenant, then verify, following the step-by-step
runbook in [operations.md](operations.md), section "Stop maintenance before
restoring or reconstructing commit records".

## Summary

| Concern | Status today |
|---|---|
| Backup mechanism | None built in |
| Cross-region failover | None built in |
| RTO / RPO guarantee | None |
| Bucket durability | Object store's own, plus optional versioning/Object Lock (informational probe only, not enforced) |
| Commit-record loss (data intact) | Recoverable via `ravel-cli commit reconstruct` (ADR-0058) |
| Periodic bucket copy / CRR | Future work, not built; CRR's async/no-CAS/no-consistent-listing caveats must be accepted explicitly |
