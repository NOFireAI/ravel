# ADR-0048: Maintenance safety and coverage

Status: Accepted

## Context

This work remediates five maintenance gaps: legal hold is dead code in
every production path; the maintenance tenant set is derived from CLI
flags, so an OIDC- or mTLS-only deployment maintains nothing; orphan GC
has no mass-orphan circuit breaker, so out-of-band commit-record loss
converts into physical data loss on a ~25 h fuse (the same breaker also
reduces the namespace-deletion blast radius for the commit prefix);
compaction publishes with no record-count conservation check; and the
orphan-GC re-verify pays a full-shard LIST per deletion candidate.

Each gap was re-verified against current `main` before this design.
Most points hold; two were already partly addressed:

- **Confirmed current.** Both shipped maintenance drivers pass
  `&NoLeases` into retention/compaction and the sweeper
  (services/ravel-server/src/maintain.rs:218,251;
  services/ravel-cli/src/maintain.rs:121). `LegalHoldCheck`,
  `write_hold_set`, `write_hold_clear`, and `shard_hold_scopes` are fully
  built and tested in crates/ravel-maintain/src/legal_hold.rs, and no
  code under services/ references any of them: there is no surface to
  set, clear, or list a hold. `run_pipeline`
  (crates/ravel-maintain/src/compact.rs:102-131) and `publish_record`
  (crates/ravel-maintain/src/publish.rs:50) make no comparison between
  input `sample_count` and part `sample_count`, though both sums are in
  hand at publish. `sweep_orphans`
  (crates/ravel-maintain/src/sweep.rs:133-174) re-lists the entire
  commit prefix of the shard (`referenced_l0_identities`) once per
  deletion candidate, and no circuit breaker of any kind exists in the
  sweep or retention paths.
- **The observability claim is stale.** An earlier assessment stated
  Ravel exports no runtime telemetry. Since then ADR-0044 section 4
  landed: `GET /metrics` is mounted unconditionally in every mode
  (services/ravel-server/src/metrics.rs, services/ravel-server/src/lib.rs),
  with a typed label allowlist that already includes `tenant_hash` and
  `signal`. The alarm surfaces in this ADR are therefore new counter and
  gauge families on that existing endpoint, not a new endpoint.
- **The tenant-set finding is partly stale.** A prior change added a
  `--maintain-tenant` flag, `merge_fold_tenants`
  (services/ravel-server/src/config.rs:417), and a startup warning when
  OIDC/mTLS is configured but the merged list is empty
  (services/ravel-server/src/main.rs:98-107). The core finding stands:
  the set is still derived only from flags, so coverage depends on an
  operator hand-maintaining a list that storage already knows, and the
  warning fires only when the list is entirely empty. A stale-but-
  non-empty list (a tenant onboarded via OIDC after deployment) is
  silent, exactly the failure class this addresses.

One design constraint discovered during verification shapes the
re-verify remediation: an L0 data key
(`t/<tenant>/<sig>/l0/<shard>/<writer>.<epoch>.<seq>.<hash16>.rseg`)
does not carry the flush's pinned ingest hour. The hour exists only in
commit keys and in signal-specific object footers, and the sweeper is
deliberately signal-generic (key-only, never a segment byte). A naive
fix of "scope the re-verify LIST to the candidate's hour
bucket" is therefore not directly implementable without breaking that
design; the decision below achieves the same cost bound differently.

No part of this decision touches a frozen persistent format. Hold
records are the existing ADR-0040 audit records (RLOG format, existing
`u` signal keyspace, `Signal::Audit` shard `AUDIT_HOLD_SHARD`); tenant
discovery reads the existing `t/<tenant_hash>/` layout with a delimited
LIST; the circuit breaker and the conservation gate are in-memory checks
over values already durable. No new object kind, key prefix, proto
field, or format version is introduced, so the format-change process is
not triggered.

## Decision

### 1. Legal hold is wired into both maintenance drivers

`run_tick` (services/ravel-server/src/maintain.rs) calls
`LegalHoldCheck::refresh(store, tenant)` once per tenant per tick,
before any destructive pass, and passes the resulting snapshot as the
`LeaseCheck` to both `scan_and_maintain_with_memo` and `sweep_shard`,
replacing `&NoLeases`. The CLI driver (`ravel-cli maintain sweep`, its
only destructive command; `compact`, `status`, `audit-versions`, and
`verify-custody` never delete) does the same refresh before its pass.
There is no flag to skip the refresh in either driver.

Refresh failure fails safe: if `refresh` errors, the tenant's entire
tick (retention, compaction, and sweep) is skipped, a warning is logged,
and a failure counter is incremented; it is retried next tick. The
driver never falls back to `NoLeases` on error, because that would
convert a transient store fault into an unprotected delete pass.
`NoLeases` remains in the library for tests and for the vacuous-gate
documentation role it already has.

A hold set after a tick's refresh is not seen until the next tick; the
exposure window is one maintain interval (default 5 minutes) and is
documented in the operations guide. This mirrors how `RetentionConfig`
is loaded once per pass.

### 2. The hold surface is a `ravel-cli hold` subcommand

`ravel-cli hold set --tenant <id> --scope <prefix> [--reason <text>]`,
`ravel-cli hold clear --tenant <id> --scope <prefix>`, and
`ravel-cli hold list --tenant <id>` live in a new
services/ravel-cli/src/hold.rs module. Set and clear write the existing
immutable ADR-0040 records through `write_hold_set` / `write_hold_clear`
with a fresh `Uuid` and the CLI's `now_ns()`; list is
`LegalHoldCheck::refresh` plus printing `active_prefixes()`.

Two validations on top of the library's non-empty-scope check: the scope
must fall under the named tenant's own `t/<tenant_hex>/` prefix (a scope
outside it can never protect that tenant's data and is always operator
error), and a `--signal`/`--shard` convenience form writes all three
prefixes from `shard_hold_scopes` so the documented
L0-only-hold mistake is impossible to make from the CLI.

The surface is CLI-only for now; it uses the operator's store
credentials the same way every other admin command
(`maintain sweep`, `verify-custody`) already does. An authenticated
HTTP admin API is deferred (rejected alternative 1), not precluded: the
records are the contract, and any future API writes the same records.

### 3. The maintenance tenant set is derived from storage

A new `discover_tenants(store)` function (crates/ravel-maintain) issues
`list_delimited("t/")` and parses each common prefix `t/<32 hex>/` into
a `TenantHash`. A prefix under `t/` that does not parse as exactly 32
hex characters is a fail-loud typed error, consistent with the
key-shape discipline everywhere else, never a silent skip.

The server's maintenance task changes from one-loop-per-flag-tenant to a
supervisor loop: at the start of each tick cycle it re-enumerates
tenants from storage, then runs the existing per-tenant `run_tick` for
each discovered tenant (the `MaintainMemo` is already keyed by tenant
and spans them). The fold task refreshes its tenant set from the same
discovery on the same cadence. A tenant that first writes data mid-run
is picked up on the next cycle without a restart.

`--tenant-token` and `--maintain-tenant` stop being the source of the
set and become an optional restriction: when either names any tenant,
maintenance and fold run only for the intersection of the discovered
set and the flag set (staged rollouts, emergency scoping). Discovered
prefixes excluded by the restriction are counted, not silently ignored.

Discovery failure (the LIST errors) skips the whole cycle with a logged
warning and a failure counter; the supervisor never treats a failed
enumeration as "no tenants", because an empty run is indistinguishable
from healthy idleness, which is the exact silence this design removes.

**What alarms.** Three families on the existing `/metrics` endpoint,
with default alert rules shipped in the operations guide:
`ravel_maintain_tenants_discovered` and
`ravel_maintain_tenants_maintained` (gauges; alert when
`maintained < discovered`, i.e. any prefix has data but no maintenance,
and when `maintained == 0` in a mode that runs maintenance),
`ravel_maintain_tenant_discovery_failures_total` (counter; alert on
increase). The `maintained < discovered` condition is exactly "a prefix
has data but no maintaining owner": under this design it
can only arise from the deliberate flag restriction or a discovery
fault, and both are visible.

### 4. Orphan GC gets a mass-orphan circuit breaker

`sweep_orphans` is restructured from interleaved verify-and-delete into
three phases: (a) candidate selection (the existing unreferenced + age
gate + lease checks over one listing), (b) one fresh strongly consistent
LIST of the shard's commit prefix, dropping any candidate whose identity
now appears (the batched re-verify, decision 5), then (c) the breaker
gate, then deletes.

The breaker trips when the final candidate count is at least
`orphan_breaker_min_count` (default 50) AND exceeds
`orphan_breaker_max_ratio` (default 0.10) of the L0 data objects listed
in the shard this pass. Both knobs live in `CompactorConfig`. The
two-part condition keeps tiny shards from tripping on noise (2 orphans
out of 5 objects is 40% but not a mass event) while any genuinely mass
orphan population — the signature of out-of-band commit-record loss —
trips regardless of shard size.

**Halt-and-alarm semantics.** A tripped pass deletes zero orphans: the
gate sits before the first delete, so the pass is all-or-nothing. The
`SweepReport` gains `orphan_breaker_tripped: bool` and
`orphans_withheld: usize`; the driver logs at error level and increments
`ravel_maintain_orphan_breaker_tripped_total` (labels: `tenant_hash`,
`signal`). The other two sweep rules and retention are unaffected and
still run: they are anchored on durable records an operator or compactor
deliberately wrote (compaction records, tombstones), not on the absence
of records, which is the only signal orphan GC has and the only one
mass record loss forges. The halt is sticky: the breaker re-trips every
tick until either the commit records are restored (candidates fall
below threshold) or an operator overrides deliberately.

**Deliberate operator override.** `CompactorConfig` gains
`force_orphan_gc: bool` (default false). The server never sets it; the
only way to set it is the one-shot
`ravel-cli maintain sweep --override-orphan-breaker`, which runs a
single overridden pass, logs what the breaker would have withheld, and
honors `--dry-run` for a preview. Requiring a human to run a separate
command with an explicit flag — after the runbook's
stop-and-investigate step — is the point: a mass-orphan state is
precisely the state in which the system's own record-absence signal is
suspect, so no automatic resume exists (rejected alternative 3).

### 5. The orphan re-verify is batched, once per pass

The per-candidate full-shard LIST is replaced by phase (b)
above: exactly one fresh strongly consistent LIST of the commit prefix
per pass, between candidate selection and the delete phase, shared by
every candidate. Cost drops from O(candidates x commit records) LIST
pages to one extra LIST per pass.

The safety argument is unchanged. The correctness anchor for orphan GC
is the writer interlock plus the age gate (a record-less object older
than `grace + max_flush_lifetime` can never legally gain a record); the
re-verify is a defense-in-depth check against a record that landed
between the first listing and the delete. Batching narrows its window
from "immediately before each delete" to "after candidate selection,
before the pass's deletes" — a window still bounded by one pass's
delete loop, protecting against the same class of straggler. The
hour-scoped per-candidate variant is rejected
because the candidate's pinned hour is not derivable from the data key
(Context above; rejected alternative 4).
docs/consistency-model.md's "Deletion and GC" wording is updated in the
same commit as the code change.

### 6. Compaction asserts record-count conservation before publish

`publish_record` (crates/ravel-maintain/src/publish.rs) gains one gate,
placed after the abandonment-deadline check and before the record is
assembled or PUT:

```
sum(inputs[i].record.sample_count) == sum(parts[j].part.sample_count)
```

computed in u64 with checked addition. Equality is exact: compaction is
a verbatim page-byte copy for all three signals and never dedups (RLOG
and RSPAN report their record counts through the same `sample_count`
field, crates/ravel-maintain/src/rlog.rs:312). A mismatch returns a new
typed `MaintainError::ConservationViolation` carrying both sums and the
bucket identity: no record is PUT, the L0 inputs remain live and
queryable, the resolver never sees the lossy parts, and the driver
increments `ravel_maintain_conservation_aborts_total` and logs at error
level. The abandoned parts are content-addressed and collectable by
sweep rule 3 exactly like any other abandoned run's parts. The gate
also runs under `dry_run`, so a dry-run compaction of a bucket that
would trip it reports the violation.

Sitting inside `publish_record` (rather than in `run_pipeline` or the
codecs) puts the check on the single choke point every signal's
pipeline already flows through, and makes it directly testable by
calling `publish_record` with mismatched parts.

## Rejected alternatives

1. **An HTTP admin API for hold set/clear/list instead of the CLI.**
   Lost because the server has no authenticated admin plane: every
   existing resolver authenticates a *tenant*, and letting a tenant
   credential place or clear its own legal holds inverts the custody
   model (holds are placed *on* tenants by operators/legal). An admin
   authz design is ADR-sized on its own. The CLI writes the same
   immutable ADR-0040 records with operator store credentials, exactly
   like every other admin command, so a future API is additive.
2. **A durable tenant-registry object instead of `LIST t/` discovery.**
   Lost because a registry is a second source of truth that can drift
   from the prefixes actually holding data — the same
   config-asserts-reality failure class this epic exists to remove. It
   would need a write path, a repair path, and a new mutable object
   kind (a key-layout change), and buys nothing at current scale where
   one delimited LIST enumerates every tenant. Revisit only if tenant
   counts make the per-cycle LIST material.
3. **Automatic breaker resume (server retries or auto-clears after N
   ticks or after a re-verify).** Lost because in the mass-orphan state
   the system's only signal — commit-record absence — is exactly what
   out-of-band record loss forges. Every input the server could consult
   to "re-verify" is the same corrupted-world view that tripped the
   breaker. Only a human can distinguish mass record loss from a
   legitimate mass abandonment, so the halt stays sticky until a
   deliberate, flagged, one-shot CLI override.
4. **Hour-scoped re-verify per candidate.** Lost because the L0 data
   key carries no ingest hour; recovering the pinned hour needs either
   signal-specific footer reads (breaking the sweeper's key-only,
   signal-generic design) or a guessed hour window from `last_modified`
   (reintroducing a clock assumption into a delete path). The batched
   single-LIST re-verify reaches the same O(1)-LISTs-per-pass cost with
   no new assumptions.
5. **Post-publish conservation audit (verify-custody-style) instead of
   a pre-publish gate.** Lost because the moment the record is
   published the resolver excludes the L0 inputs, so every query in the
   detection window returns plausible incomplete results, and once the
   sweep passes the protection horizon the inputs are physically gone
   and the loss is permanent. The pre-publish gate costs one addition
   per input over values already in memory and closes the window to
   zero.
6. **Keeping flag-derived tenant sets and relying on the existing
   `--maintain-tenant` flag plus the startup warning.** Lost because it
   makes lifecycle coverage depend on an operator hand-synchronizing a
   flag list with tenant onboarding, and the warning only fires when
   the list is entirely empty: a stale non-empty list (one OIDC tenant
   added last week) folds, compacts, retains, and GCs nothing for the
   new tenant, silently, forever. Storage already knows the answer.

## Consequences

- Three acceptance scenarios become passing tests: an OIDC-only server
  maintains every tenant with data, a held bucket survives a retention
  tick through real server wiring and a production mechanism exists to
  set the hold, and a lossy merge aborts before publish.
- Deletion breadth increases by design: tenants that previously had no
  maintenance (OIDC/mTLS deployments) start being compacted and swept
  on upgrade. Age-based retention remains opt-in via the existing
  `--retention-*` flags, so no age deletion starts that an operator did
  not configure; the newly covered deletes are the orphan/superseded/
  unreferenced rules, now additionally gated by holds and the breaker.
- Per-cycle maintenance cost grows by one delimited LIST of `t/` plus,
  per tenant per tick, one hold refresh (a LIST of the audit hold shard
  and a GET per hold object). The hold shard is a control-plane shard
  holding only legal-hold records (query-audit volume lives on a
  separate shard by design), so this stays small; if a pathological
  hold history ever makes it material, snapshot/memoization is a local
  optimization behind the same `refresh` seam.
- `run_tick` and the sweep entry points change shape (hold snapshot
  threaded through; `SweepReport` gains two additive fields; new
  `CompactorConfig` knobs with safe defaults). All are in-process APIs;
  nothing durable changes, and no ADR-frozen format is touched.
- The server's per-tenant maintenance loops become a discovery-driven
  supervisor; `--maintain-tenant` semantics narrow from "adds tenants"
  to "restricts to these tenants", which is a documented behavior
  change for the flag (release-noted; the flag stays valid).
- New metric families (`ravel_maintain_*` counters and gauges) render
  on the existing ADR-0044 `/metrics` endpoint within its existing
  label allowlist (`tenant_hash`, `signal`, `mode`); default alert
  rules and the breaker-override runbook (including the
  stop-maintain-first step) land in
  docs/guides/operations.md in the same commits as the behavior.
- Out of scope, deliberately: the footer-derived commit-record
  reconstruction tool (the other half of that remediation), per-tenant
  quotas, and the credential split. The breaker bounds the blast radius
  of record loss to "halted and alarmed" but does not repair it; the
  rebuild tool is its own piece of work.
