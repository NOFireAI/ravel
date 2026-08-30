# Per-tenant shard overrides: cutting request cost per tenant

`spec.shardOverrides` lets an operator lower (or raise) one tenant's shard
count without touching the cluster-wide, immutable `spec.shards`. It is the
operator wiring for Ravel's online resharding mechanism, and it is the
primary operator-facing per-tenant cost control. Cost is linear
in shards, so four shards to one is a 4x reduction in ingest PUTs and a 4x
reduction in read LIST cost for that tenant.

This changes no format, no protocol, and no query result. It changes how many
shards a tenant's data is spread across from a future hour onward.

## Turning it on

```yaml
apiVersion: ravel.nofire.ai/v1alpha1
kind: RavelCluster
metadata:
  name: prod
spec:
  # ... image, shards, storage, tenantTokensSecretRef ...
  shardOverrides:
    leadHours: 4
    tenants:
      acme: 1
      globex: 2
```

Each reconcile cycle, the operator compares every named tenant's current
shard count (the last generation already recorded for that tenant, per
signal) against its target here. When they differ, it drives one
`append_generation` call per `(tenant, signal)` — metrics, logs, and spans
independently — through the same object store the operator already uses for
`sys/auth` reconciliation. The new count activates `leadHours` hours after
the reconcile that scheduled it, never immediately: this is the same future
activation `ravel-cli provision reshard` requires, so a
router already mid-flight with the old count keeps routing correctly until
the boundary passes.

### Fields

| Field | Type | Default | Notes |
|---|---|---|---|
| `tenants` | map | `{}` | Tenant name to target shard count. A tenant absent here is left alone. |
| `leadHours` | integer | `2` | Hours of lead time before the new count activates. Rejected outright below 2 (never silently clamped up) — the same minimum `ravel-cli provision reshard` enforces. |

A tenant named here that has never ingested anything yet (no provisioning
record) is not silently skipped: the attempt still runs and is refused with
the same `NoRecordToReshard` error `ravel-cli provision reshard` would give,
visible in the operator's logs. A misconfigured or not-yet-provisioned
tenant never blocks the rest of the cluster's reconcile, or that tenant's
other signals: each `(tenant, signal)` reshard attempt is independent and
best-effort, exactly like the existing `sys/auth` reconcile step.

## What actually changes

`spec.shards` still renders into every tier's `--shards` flag and stays
immutable after creation — that invariant is unrelated
to this feature and is not relaxed by it. `shardOverrides` does not touch a
Deployment at all. It writes directly to each tenant's durable provisioning
record in object storage, the same record `ravel-ingest`'s router and
`ravel-query`'s scan planner already read on every request. Lowering shards
for one tenant therefore changes what those already-running pods do for
that tenant, without a rollout.

## What it costs

These costs are real and must be weighed before lowering a tenant's count,
not discovered afterward.

- **A tenant's ingest throughput is bounded by its shard count.** Each shard
  is a separate flush stream owned by one shard actor and one single-threaded
  merge loop. A tenant at one shard funnels its entire ingest volume through
  that one actor no matter how many gateway replicas or CPU cores the cluster
  has. If a low-shard tenant's traffic grows, the correct response is raising
  its shard count back up (also via `shardOverrides`), not adding replicas.
- **A tenant at one shard concentrates onto shard index 0.** Every one-shard
  tenant hashes to the same shard index, so several tenants pinned to one
  shard apiece all land their entire volume on shard 0 of whichever replica
  hosts it, while sibling shard indices on that replica idle for those
  tenants. This is the mirror image of `ingestAffinity`'s subset concentration
  (`docs/guides/ingest-affinity.md`): there it is replicas, here it is shard
  indices within a tenant.
- **Maintenance and compaction units get coarser.** The maintenance
  ownership protocol partitions work as `(tenant, signal, shard)` units. Fewer shards
  means fewer, larger units per tenant-signal: less parallelism available to
  the maintain tier for that tenant, and a single compaction or GC pass over
  a bigger slice of data.
- **This lever does not reach logs or spans' own request-cost drivers.**
  Shard count divides PUT and LIST cost the same way for every signal, but
  logs and spans carry additional per-record cost structure that a future
  change addresses separately. Lowering shard count here is not a
  substitute for that.

## Reading it back

The override is a target, not an instantaneous state. To see what a tenant is
actually routing under right now, read its provisioning record's generation
history directly (`docs/guides/inspecting-data.md`) rather than assuming
`shardOverrides` took effect the instant it was applied — it takes effect at
the scheduled activation hour, `leadHours` after the reconcile that appended
it.
