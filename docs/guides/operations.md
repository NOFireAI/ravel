# Operations

Configure, deploy, run and repair a Ravel cluster with the four pages below.
They are for the person who owns the deployment. That person chooses the
object store and its credentials, brings the cluster up, keeps it compacting
and reclaiming, and is paged when it stops. The pages use the vocabulary in
[Concepts](../concepts.md).

For the spelling, environment variable and default of a flag, use the
generated [server flag reference](../reference/ravel-server-flags.md) and
[CLI flag reference](../reference/ravel-cli-flags.md). The pages below explain
how to choose a value.

- **[Configuration (day 0)](operations/configuration.md)** is what to decide
  before you start anything. It covers the storage backend and its
  credentials, the four storage credential roles and their policies,
  encryption, admission limits, and tenancy. It also covers the read cache
  tiers, retention and garbage-collection configuration, the durable shard
  count, the logs fetch policy, and the per-tenant declarations that change
  query cost. Several of these choices are permanent for the life of a bucket.
- **[Deployment (day 1)](operations/deployment.md)** brings a cluster up for
  the first time, in the order that the steps must occur. It covers store
  qualification, the bucket protection contract, the first deployment against
  a fresh bucket, and the Admin credential. It also covers readiness and the
  store reachability probe, durable auth refresh, the dedicated fragment
  listener, and federation to a remote cluster.
- **[Maintenance (day 2)](operations/maintenance.md)** runs the cluster. It
  covers the catalog fold, compaction, garbage collection and retention, the
  at-rest integrity scrubber, format migration, legal hold, and the
  maintenance and inspection commands. Read its first paragraph before you
  decide that you do not need a maintenance process.
- **[Troubleshooting](operations/troubleshooting.md)** is what to do when
  something is wrong. Each entry gives the symptom, the likely cause, how to
  confirm it, and the corrective action. The procedures where the obvious
  first action makes things worse are at the top.

When you budget host memory, remember the catalog's two per-tenant record
caches: they are bounded per tenant rather than by a share of the process
memory budget, so they cost up to 45 MB per actively-queried tenant on top of
the carved read-cache shares. Both halves are enforced in bytes, 22.5 MB each:
neither a commit record nor a compaction record has a bounded size (each
carries a repeated field the format does not cap), so each cache charges every
entry an estimate of the live heap it holds and evicts until the tenant's
summed charge is inside its budget. [Caching](caching.md) has the derivation
and the sizing.

Related guides:

- [Observability](observability.md) for the metric families and the label
  allowlist.
- [Caching](caching.md) for read-cache sizing.
- [Disaster recovery](disaster-recovery.md) for bucket-level backup and
  restore.
- [Kubernetes](kubernetes.md) for the operator.
- [Cost model](cost-model.md) for predicting the request bill.

<!-- These anchors are kept so deep links written against the single-page
     version of this guide keep resolving after the split. -->
<a id="the-admin-credential"></a>
<a id="storage-credential-roles-adr-0055"></a>
<a id="declared-typed-attribute-columns-adr-0090"></a>
