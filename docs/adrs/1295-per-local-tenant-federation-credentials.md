# ADR-1295: Remote federation credentials keyed by local tenant

Status: Accepted (2026-09-14)

## Context

ADR-0071 gave a coordinator a `--remote-cluster` flag carrying one bearer
credential per remote. The remote authenticates that credential through its own
tenant resolver and derives the tenant from it, overwriting whatever
`tenant_hash` the wire carried. That half is correct: a coordinator cannot name
a tenant on a remote, and a remote returns only what its own resolution of the
presented credential authorizes.

What was missing is the other half. A credential authorizes one tenant's data on
the remote, so it belongs to one tenant on the coordinator too, and nothing said
which. `RemoteClusterConfig` had no tenant field and `parse_remote_clusters`
rejected unknown keys, so the mapping could not be written at all. One
`Federation` was built per process and installed on the shared engine, and
`Federation::fetch` dispatched to every configured remote regardless of the
caller's `tenant_hash`, which it passed only as a wire field the remote
overwrote.

On a coordinator serving more than one local tenant, every local tenant's metric
selectors and discovery calls therefore fanned out under the same credential.
Each local tenant received the remote tenant's series, and the remote tenant's
data reached whichever local tenant asked. The scope was metric selectors
(`Signal::Metrics`; ADR-1103 answers log selectors locally with a warning) plus
`/api/v1/series`, `/api/v1/labels` and `/api/v1/label/<name>/values`, which union
remote `(series_id, labels)` pairs and so disclose the remote tenant's label
namespace.

A prior step refused `--remote-cluster` outright whenever the resolver could
resolve more than one local tenant. That closed the exposure but left the
multi-tenant coordinator with no correct configuration, because the mapping the
correct configuration needs could not be expressed.

## Decision

Key each remote cluster's credential by the local tenant it belongs to, and keep
the startup refusal for the configurations that still cannot be expressed.

1. **One remote cluster entry maps to one local tenant.** A `--remote-cluster`
   spec takes an optional `tenant` key naming that local tenant;
   `RemoteClusterConfig::tenant` carries it as a `TenantId` and
   `ravel_query::distrib::RemoteCluster::tenant` as the `TenantHash` derived from
   it at `Federation` construction, under the process-wide scheme
   `install_tenant_hash_scheme` resolved from the bucket's tenancy marker.

   Several local tenants sharing one remote endpoint is several specs, each with
   its own `name` and its own `credential-file`. There is deliberately no syntax
   for naming several local tenants on one spec. Such a syntax would put two
   local tenants back behind one credential, which is the exposure this ADR
   removes, and it would be the shortest thing to write.

2. **Selection happens before dispatch.** `Federation::fetch` filters
   `self.remotes` by the caller's `tenant_hash` and dispatches only to the
   survivors. A local tenant with no mapped remote presents no credential,
   issues no request, and pays no cost. Filtering rather than post-filtering a
   response is what makes that true: a response test would still have dialed the
   remote under a credential that tenant has no claim to.

   `Federation::remotes_for` is the single place the mapping is applied, and
   `has_remotes_for` is derived from it, so a caller asking "does this tenant
   federate at all" and `fetch` cannot disagree. The two log-selector warnings in
   the engine (ADR-1103 decision 5) key on `has_remotes_for` rather than on the
   presence of a federation context, so a tenant whose whole query is local is
   not told that part of it was not federated.

3. **An unmapped local tenant gets local data only, reported complete.** No
   error, no warning, and no `partial: true`. A remote a tenant holds no
   credential for is outside its query, not missing from it. Marking it partial
   would make every non-federating tenant's response permanently degraded and
   would make the partial-coverage signal useless for the case it exists for, an
   unreachable remote a tenant genuinely does federate to.

4. **An unkeyed remote still serves every local tenant, and is still refused on a
   multi-tenant coordinator.** `tenant: None` is the shape of every deployment
   written before this ADR, and it stays byte-identical for them.
   `ensure_federation_tenant_mapping` (replacing
   `ensure_federation_single_tenant`) refuses an unkeyed spec whenever the
   resolver can resolve more than one local tenant, naming every such spec and
   the `tenant` key as the remedy. Every configuration the old refusal covered is
   still refused, because every one of them is unkeyed; what changes is that a
   mapped spec now starts instead.

5. **A mapping that can never fire is refused too.** Where the tenant set is
   fully known (static bearer tokens only, no resolver deriving a tenant from a
   request), a `tenant` naming a tenant outside it fails startup. Its only
   symptom otherwise would be a remote that quietly answers nobody, which is the
   same silent-empty failure this ADR is about, one layer over. Under a dynamic
   resolver the static map is not the tenant set, so the check does not apply.

6. **The remote's own resolution is unchanged.** The remote still derives its
   tenant from the presented credential and overwrites the wire `tenant_hash`.
   This ADR decides which local tenant may present a given credential; the remote
   still decides what that credential is entitled to see. The two are
   independent, and neither substitutes for the other.

## Consequences

- A multi-tenant coordinator can federate, which no configuration allowed
  before. `services/ravel-server/tests/federation_e2e.rs` gains the first tests
  in that file with two local tenants: one for the query half and one for the
  discovery half that `/api/v1/series`, `/api/v1/labels` and
  `/api/v1/label/<name>/values` share. Both assert that the mapped tenant does
  receive the remote's series first, so an unreachable remote fails there rather
  than passing the isolation assertion for the wrong reason.
- Existing single-tenant deployments are unaffected: no spec changes, no
  behavior changes, and the pre-existing tests construct unkeyed remotes
  unchanged.
- The remote's `LabelSet` is still decoded from the wire rather than re-derived
  locally, and this ADR deliberately does not change that. `SeriesId::compute`
  takes the `TenantId` the series belongs to, and for a remote series that is the
  REMOTE's tenant, which the coordinator does not know and by design never
  learns: the remote resolves it from the credential and never reports it. So the
  coordinator cannot re-derive the identity to check the labels against. That is
  a "how much do we trust a remote's content" property, orthogonal to the "which
  remote answers for which local tenant" property decided here; a remote able to
  lie about labels can equally lie about samples, and the existing decode
  validation (a malformed frame is corruption, never a skippable coverage gap)
  is what bounds it.
- Federation still assumes disjoint series identity across clusters (ADR-0071
  wave 5). Nothing here changes the cross-cluster tie-break limitation.
- `docs/guides/distributed-query.md`, `docs/guides/operations/deployment.md`,
  `docs/query-engine.md` and the generated flag reference all described
  federation as single-tenant and are updated with the mapping.
