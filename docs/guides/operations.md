# Operations

How to configure, deploy, run and repair a Ravel cluster. It is written for the
person who owns the deployment: whoever chooses the object store and its
credentials, brings the cluster up, keeps it compacting and reclaiming, and gets
paged when it stops. It assumes the vocabulary in [Concepts](../concepts.md) and
does not repeat it. For the exact spelling, environment variable and default of
any flag, use the generated
[server flag reference](../reference/ravel-server-flags.md) and
[CLI flag reference](../reference/ravel-cli-flags.md); the pages below explain
how to choose a value, not what the flags are called.

- **[Configuration (day 0)](operations/configuration.md)** is what to decide
  before you start anything: the storage backend and its credentials, the four
  storage credential roles and their policies, encryption, admission limits, the
  read cache tiers, retention and garbage-collection configuration, tenancy, the
  durable shard count, the logs fetch policy, and the per-tenant declarations
  that change query cost. Several of these choices are permanent for the life of
  a bucket.
- **[Deployment (day 1)](operations/deployment.md)** is bringing a cluster up
  for the first time, in the order the steps have to happen: qualifying the
  store, the bucket protection contract, the first deployment against a fresh
  bucket, the Admin credential, readiness and the store reachability probe,
  durable auth refresh, the dedicated fragment listener, and federating to a
  remote cluster.
- **[Maintenance (day 2)](operations/maintenance.md)** is running it: the
  catalog fold, compaction, garbage collection and retention, the at-rest
  integrity scrubber, format migration, legal hold, and the maintenance and
  inspection commands. Read its first paragraph before you conclude you do not
  need a maintenance process.
- **[Troubleshooting](operations/troubleshooting.md)** is what to do when
  something is wrong, as symptom, likely cause, how to confirm and corrective
  action. The procedures where acting on the obvious first makes things worse
  are at the top.

Related guides: [observability](observability.md) for the metric families and
the label allowlist, [caching](caching.md) for read-cache sizing,
[disaster recovery](disaster-recovery.md) for bucket-level backup and restore,
[Kubernetes](kubernetes.md) for the operator, and [cost model](cost-model.md)
for predicting the request bill.

<!-- These anchors are kept so deep links written against the single-page
     version of this guide keep resolving after the split. -->
<a id="the-admin-credential"></a>
<a id="storage-credential-roles-adr-0055"></a>
<a id="declared-typed-attribute-columns-adr-0090"></a>
