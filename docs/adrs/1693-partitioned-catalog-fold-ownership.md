# ADR-1693: Partitioned catalog fold ownership

Status: Accepted (2026-09-16). Issue #1693. Amends ADR-0065 decision 2 (the
ownership rule now also gates the ADR-0020 catalog fold) and changes which
modes run the scheduled fold.

## Context

Maintenance work is partitioned across processes. Each maintain-role process
writes a heartbeat under `sys/maintain/workers/<process_id>`
(crates/ravel-fleet/src/worker_set.rs:105-112), lists its siblings on the
same cadence, and owns a `(tenant, signal, shard)` unit when it wins the
rendezvous hash over the live set (worker_set.rs:152-164; ADR-0065 decisions
1 and 2). The maintenance tick gates every per-unit step on
`WorkerSet::owns_unit` (services/ravel-server/src/maintain.rs:1303), and the
per-signal sweeps are gated on ownership of shard 0 of the pair
(maintain.rs:1647-1648, :1691).

The catalog fold is not partitioned. `fold::spawn` starts one loop per
signal in every mode except maintain (services/ravel-server/src/lib.rs:2796,
:2837-2843; docs/guides/operations/maintenance.md:12-13). Each tick
re-discovers every tenant and calls `run_tenant_tick` for each one
(services/ravel-server/src/fold.rs:233-244). The tick peeks at HEAD first
and skips a fresh one, which keeps a routine tick at two GETs per tenant and
signal. At every hourly seal boundary every process performs the whole fold:
the bucket LISTs, the commit-record GETs, and the part PUTs, and then all but
one lose the HEAD CAS (crates/ravel-catalog/src/fold.rs:1929-1931), which
comes after the work. Results stay correct, because parts are
content-addressed and a duplicate PUT is an idempotent `AlreadyExists`
(docs/catalog-and-mvcc.md:492-493). Cost scales with process count instead
of tenant count: 30 processes over 1,000 tenants and 3 signals cost 180,000
GETs per five minutes at rest and 29 discarded folds per pair per hour.

`WorkerSet::new` takes no key prefix and `heartbeat_key` is fixed
(worker_set.rs:110-112, :274-288), so "a fold-role heartbeat prefix" is a
new `sys/` namespace, and the object key layout is a frozen contract. The
only live set that exists is the maintain one. The maintain process already
carries a `WorkerSet` built at startup in every mode
(services/ravel-server/src/lib.rs:2789-2794) and a heartbeat task that
publishes each fresh live set over a `watch` channel
(services/ravel-server/src/maintain.rs:893-950).

Two other facts shape the choice. The quickstart runs one `--mode all`
process and no maintain process (deploy/docker-compose/ravel.yml:80-82;
maintenance.md:3-10), and `all` is defined as gateway plus query in one
process (docs/architecture.md:29-33). The fold-liveness alert aggregates
`max by (signal)` across the fleet precisely because a replica whose peers
fold on schedule does no folding of its own
(docs/guides/observability.md:303-349). T11e hands hours the sweep held on a
named snapshot to the next fold through an in-process channel, which assumes
the sweep and the fold for a unit run in the same process.

## Decision

1. **The scheduled fold runs in the maintain role, partitioned by the
   existing maintain live set.** `fold::spawn` runs in `Mode::Maintain`. In
   each tick, after discovery, a `(tenant, signal)` pair is folded only when
   `worker.owns_unit(live_set, tenant, signal, 0)` holds, using the process's
   one `WorkerSet` and the live set the maintenance heartbeat task publishes.
   Shard 0 is the unit key, the same convention the per-signal sweeps use,
   so the process that sweeps a pair's catalog objects is the process that
   folds it. The ownership check comes before the HEAD freshness peek, so an
   unowned pair costs zero requests. No new heartbeat prefix and no second
   membership identity exist.

2. **Gateway and query processes stop running the scheduled fold.** They
   keep the on-demand `POST /api/v1/admin/fold` route where it is mounted
   today (services/ravel-server/src/lib.rs:2562-2570), because that route is
   an operator trigger behind the query credential, not scheduled traffic.
   `ravel-cli catalog fold` is unchanged.

3. **`Mode::All` keeps folding, unpartitioned.** `all` is the single-process
   shape, and it runs no maintenance loop and writes no heartbeat. It folds
   every discovered tenant under its solo live set, which is today's
   behavior exactly. A deployment that runs several `all` replicas keeps
   today's duplicated fold cost, and the maintenance guide says so.

4. **A deployment with no maintain process and no `all` process never
   folds.** That is stated, not softened. Correctness is unaffected: resolve
   falls back to listing (maintenance.md:64-73). Query cost grows with the
   unsealed span, and the fold-liveness alert fires, which is the correct
   signal for a fleet that was told to fold and has nobody to do it.

5. **The fold liveness family renders only where the scheduled fold runs.**
   `ravel_catalog_fold_last_success_timestamp_seconds` is rendered in
   `maintain` and `all`. A gateway or query process no longer renders a
   permanently-zero series. The alert rule's `max by (signal)` aggregation
   already tolerates replicas that do not fold, so it needs no change; its
   comment about "an intentionally maintain-only fleet" that never folds
   becomes stale and is corrected.

6. **The fold in the maintain process reads the maintain context's injected
   clock**, not a fresh `SystemClock` (fold.rs:265 today), so a test that
   drives membership and folding advances one clock.

```mermaid
flowchart TB
    subgraph store [object store]
        HB[(sys/maintain/workers/*)]
        CAT[(t/.../catalog HEAD and parts)]
    end
    subgraph m1 [maintain process 1]
        H1[heartbeat task] -->|write, list| HB
        H1 -->|watch: live set| T1[maintenance tick]
        T1 -->|owns tenant A, shard 0| F1[fold A: HEAD peek, LIST, GET, PUT, CAS]
        T1 -->|not owner of B| X1[skip: zero requests]
    end
    subgraph m2 [maintain process 2]
        H2[heartbeat task] -->|write, list| HB
        H2 -->|watch: live set| T2[maintenance tick]
        T2 -->|owns tenant B, shard 0| F2[fold B]
        T2 -->|not owner of A| X2[skip]
    end
    F1 --> CAT
    F2 --> CAT
    subgraph gw [gateway and query processes]
        G[no scheduled fold; on-demand route only]
    end
    subgraph one [all process]
        A1[fold every tenant, solo live set]
    end
    A1 --> CAT
```

## Rejected alternatives

- **A new fold-role heartbeat prefix that every gateway and query process
  writes.** It adds a `sys/` namespace to the frozen key layout, a PUT and a
  LIST per process per heartbeat interval across the whole fleet, and a
  second membership identity per process. The maintain live set already
  exists and already partitions per-tenant catalog work.
- **Gateway and query processes join the maintain live set.** Rendezvous
  ownership would then assign compaction and retention units to processes
  that run no maintenance loop. Those units would starve permanently, which
  is ADR-0065's stuck-owner hazard made structural.
- **Keep every process folding and let jitter pick a winner.** The CAS comes
  after the LISTs, GETs, and PUTs, so every loser still pays the whole fold.
  Jitter changes who wins, not what it costs.
- **Leader election for the fold.** ADR-0065 rejected a leader for
  maintenance (its rejected alternative 2) because a leader lease is a
  single point of stall and a second protocol beside membership. The fold
  is smaller than maintenance, not different in kind.
- **Static partitioning by replica ordinal without a membership view.** A
  dead ordinal's tenants are folded by nobody until a human notices. The
  live set exists so that takeover is automatic.
- **Stop folding in `all` too, and require a maintain process everywhere.**
  It breaks the quickstart and every single-process deployment for no cost
  saving: one process cannot duplicate its own work.

## Consequences

- Fold cost scales with tenant count. At rest each pair costs two GETs per
  tick on one process; at a seal boundary each pair is folded once. The
  worked example drops from 180,000 GETs per five minutes to 6,000, and
  from 30 folds per pair per hour to 1.
- Fold latency is bounded by maintain capacity. The fold now shares the
  maintain process's discovery cadence and its `unit_concurrency`
  (services/ravel-server/src/lib.rs:2793), so a fleet with one maintain
  replica folds all tenants from that one process. Scaling folding means
  scaling the maintain Deployment, whose replica count the operator already
  exposes (deploy/k8s/operator/crd.yaml:348).
- During a membership transition, at most `3 * H` plus one heartbeat, two
  maintain processes may both fold one pair. The HEAD CAS serializes them
  and the duplicate parts are content-addressed, so the overlap costs bounded
  duplicate reads, the same bound ADR-0065 states for every other unit.
- Operator-visible changes: the operator's fold tuning moves from the
  gateway tier to the maintain tier (`--disable-fold`,
  `--fold-interval-secs`; services/ravel-operator/src/crd.rs:174, :530;
  reconcile.rs:823). Setting either on a gateway or query process becomes a
  startup error, not a silent no-op. The maintenance guide's opening
  paragraph and the troubleshooting table gain the rule: a fleet with no
  maintain process and no `all` process does not fold, and queries pay
  listing cost for the whole unsealed span.
- T11e's refold channel holds by construction: the sweep that finds a
  named-snapshot block and the fold that reconciles it run in the same
  maintain process for the same unit key.
- ADR-0065 decision 2's list of ownership-gated work gains the fold, keyed
  on shard 0 of the pair. docs/catalog-and-mvcc.md records the ownership
  rule in its fold section.
- Follow-up work, as tasks:
  1. ravel-server: pass the maintain `WorkerSet`, the live-set `watch`
     receiver, and the injected clock into `fold::spawn`; gate
     `run_tenant_tick` on `owns_unit(.., 0)` before the HEAD peek; spawn
     the fold in `Mode::Maintain` and `Mode::All` only; refuse the fold flags
     in other modes. Land the acceptance test the ticket names: two
     `Catalog` instances with distinct process ids over one `MemoryStore`
     behind a `FaultStore`, a live set of two, one tick each, exactly one
     fold, and zero LIST calls from the non-owner.
  2. ravel-server metrics: render the fold family in `maintain` and `all`
     only; update the render tests that pin `mode="all"`.
  3. ravel-operator: move the `fold` field to the maintain tier with a CRD
     description update; reject it on the gateway tier.
  4. docs: maintenance.md (lines 3-13 and the fold section),
     docs/catalog-and-mvcc.md fold section, docs/architecture.md:29-33,
     docs/guides/observability.md:297-301 and the ADR-0065 decision 2
     amendment paragraph.
