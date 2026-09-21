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

The catalog's per-tenant record caches are sized from the shard count, the
signal count and the configured max flush delay, not a flat constant:
`shards * 6 signals * ceil(3600 / max_flush_delay_seconds) * 3 unsealed
hours`, floored at 10,000 entries and capped at 30,000. One capacity bounds
**two** caches per tenant, the commit-record cache and the L1
compaction-record cache, each independently, so the worst case is twice that
entry count: at the cap, 30,000 x 750 bytes per cached record x 2 caches =
45 MB per actively-queried tenant, which is what the cap is chosen to hold
constant across every deployment shape (`--shards 64` derives 2,073,600
entries and 3.1 GB per tenant uncapped). Budget it as 45 MB times the number
of tenants queried concurrently: 100 of them is 4.5 GB worst case, and idle
tenants are reclaimed by idle-tenant eviction. These are not among the
memory-budget-carved caches [caching](caching.md) describes, so that product
comes out of what is left after the carved shares, not out of them. The cost
of the cap is that a tenant whose unsealed tail exceeds 30,000 records does
not keep the whole tail cached and pays per-record GETs on every resolve; a
coarser `--max-flush-delay` or a lower shard count shrinks the tail itself.

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
