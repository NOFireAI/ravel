# Per-tenant shard overrides

Use `spec.shardOverrides` to lower or raise the shard count of one tenant.
The cluster-wide `spec.shards` stays immutable. `shardOverrides` is the
operator wiring for the online resharding mechanism, and it is the primary
cost control for a tenant. Cost is linear in shards: a move from four shards
to one is a 4x reduction in ingest PUTs and a 4x reduction in read LIST cost
for that tenant.

An override changes no format, no protocol and no query result. It changes
how many shards hold the data of a tenant from a future hour onward.

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

On each reconcile cycle, the operator compares the current shard count of
every named tenant with its target. The current count is the last generation
recorded for that tenant, per signal. When the two differ, the operator makes
one `append_generation` call per `(tenant, signal)`. Metrics, logs and spans
are independent of each other. The call goes through the same object store
that the operator uses for `sys/auth` reconciliation.

The new count activates `leadHours` hours after the reconcile that scheduled
it, never immediately. `ravel-cli provision reshard` requires the same future
activation. A router that still uses the old count routes correctly until the
boundary passes.

### Fields

| Field | Type | Default | Notes |
|---|---|---|---|
| `tenants` | map | `{}` | Tenant name to target shard count. A tenant absent here is left alone. |
| `leadHours` | integer | `2` | Hours of lead time before the new count activates. Rejected outright below 2, never silently clamped up: the same minimum `ravel-cli provision reshard` enforces. |

The operator also runs the attempt for a named tenant that has no
provisioning record, which is a tenant that has ingested nothing. The attempt
is refused with the `NoRecordToReshard` error that
`ravel-cli provision reshard` gives. The refusal is visible in the operator
logs.

Each `(tenant, signal)` reshard attempt is independent and best-effort, like
the `sys/auth` reconcile step. A misconfigured or unprovisioned tenant does
not block the reconcile of the rest of the cluster. It does not block the
other signals of that tenant.

## What changes

`shardOverrides` writes to the durable provisioning record of each tenant in
object storage. The router of `ravel-ingest` and the scan planner of
`ravel-query` read that record on every request. A lower shard count
therefore changes what the running pods do for that tenant, without a
rollout.

`shardOverrides` does not touch a Deployment. `spec.shards` still renders
into the `--shards` flag of all three Deployments and stays immutable after
creation.

## What it costs

Weigh these costs before you lower the shard count of a tenant.

- **The shard count bounds the ingest throughput of a tenant.** Each shard
  is a separate flush stream. One shard actor and one single-threaded merge
  loop own it. A tenant at one shard sends all of its ingest volume through
  that one actor, for any number of gateway replicas or CPU cores. If the
  traffic of a low-shard tenant grows, raise its shard count with
  `shardOverrides`. More replicas do not help.
- **A tenant at one shard concentrates onto shard index 0.** Every one-shard
  tenant hashes to the same shard index. Several one-shard tenants put all
  of their volume on shard 0 of the replica that hosts it. The other shard
  indices on that replica stay idle for those tenants.
  [`ingestAffinity`](ingest-affinity.md) concentrates tenants on replicas in
  the same way.
- **Maintenance and compaction units get coarser.** The maintenance
  ownership protocol partitions work as `(tenant, signal, shard)` units.
  Fewer shards give fewer, larger units per tenant and signal. The maintain
  Deployment has less parallelism for that tenant. One compaction or
  garbage-collection pass covers a larger slice of data.
- **Shard count does not reach every request-cost driver of logs and
  spans.** Shard count divides PUT and LIST cost the same way for every
  signal. Logs and spans also carry per-record cost structure that shard
  count does not change. A lower shard count is not a substitute for work
  on that cost.

## Reading it back

No shipped command prints the generation history of a provisioning record.
Read the log lines of the operator.

A successful append logs at info level. The line names the tenant, the
signal, the count it moved from and to, and the generation number:

```
shardOverrides reconcile: appended a new shard generation
```

To get the activation hour, add `leadHours` to the timestamp of the success
line.

A refusal logs at warn level with the same fields plus the error. A tenant
with no provisioning record (`NoRecordToReshard`) or a contended write is
visible only there.

## Background

- The online resharding mechanism is
  [ADR-0052](../adrs/0052-online-resharding.md).
- Shard count as the primary per-tenant cost control is ADR-0076 decision 2.
- The immutability of `spec.shards` is ADR-0034 decision 2.
- The `(tenant, signal, shard)` maintenance unit is ADR-0065.
