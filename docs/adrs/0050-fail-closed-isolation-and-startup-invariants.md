# ADR-0050: Fail-closed isolation and startup invariants

Status: Accepted

## Context

One consistent pattern runs across the isolation and startup surface:
controls that the design promises are, in the running binary, either
advisory (a doc comment), degrade-open (a `tracing::warn!` plus
fallback), or absent. This ADR decides the fail-closed shape for seven
of them. Each was re-verified against current `main` before designing
for it; two points were stale or wrong, noted below.

**The mTLS resolver is unverified.** `MtlsResolver`
(crates/ravel-query/src/http/tenant.rs:389-449) maps the plain
`x-ravel-client-cert-cn` header to a `TenantId` with no verification.
`build_auth_resolver` (services/ravel-server/src/tenant.rs:82) installs
it into the single `FallbackResolver` chain shared by every listener,
and gRPC metadata is copied into the same `HeaderMap` the resolvers read
(`otlp_grpc.rs`'s `metadata_to_headers`, `flight_auth.rs`). The module
documentation itself warns that an unsanitized gRPC vhost is a live
bypass. Nothing enforces the documented trust precondition; enabling
`--mtls-enabled` hands tenant selection to any client that can reach any
listener whose header path is not stripped by a proxy.

**The snapshot fast path degrades open on a foreign `tenant_hash`.** Commit-record decode fails closed on a foreign
`tenant_hash`, but the snapshot fast path degrades open: a `tenant_hash`
mismatch on the catalog HEAD (snapshot_resolve.rs:332-333) or on a
postings object (snapshot_resolve.rs:275-276) logs a warning and falls
back to listing. A mispointed HEAD is treated as a performance event,
not an isolation fault, and the raw LIST has already returned foreign
key metadata before the check runs. By contrast a `shard_count` mismatch
on the same HEAD is already a hard `CatalogError::FieldMismatch`
(snapshot_resolve.rs:340-346), so the loud-error precedent exists in the
same function.

**The deployment-keyed tenant hash is not implemented.** It was
described as opt-in; it is not implemented at all.
`TenantId::hash()` (crates/ravel-types/src/lib.rs:55)
is unkeyed BLAKE3 over `"ravel-tenant-v1" || tenant_id`, and no keyed
derivation or key-loading config exists anywhere in the workspace. The
normative docs are wrong on this point: docs/catalog-and-mvcc.md:43-44
and ADR-0010 section 13 both state a deployment-keyed variant "is
available via config". This ADR corrects that record and makes the keyed
variant real, default, and durable. The cost asymmetry is the driver:
switching the hash later relocates every object of every tenant, so this
is the one decision here that gets structurally harder with every byte
ingested.

**The GC protection constraint lives in three unlinked configs.** `protection_horizon >= max_query_duration
+ grace` protects every pinned reader from the GC sweeper, and it lives
in three unlinked per-process configs: the maintain sweep config
(crates/ravel-maintain/src/config.rs:83-86, a doc comment saying "must
satisfy"), the query deadline (crates/ravel-query/src/config.rs,
`DEFAULT_DEADLINE` 30 s), and the Flight ticket ceiling
(crates/ravel-sql/src/flight/mod.rs:117-152, whose own docs note there
is no process-wide GC configuration to read). Nothing validates the
constraint anywhere, and the three values can be deployed independently.

**`shard_count` lives only in process config**
(crates/ravel-catalog/src/config.rs:47,
crates/ravel-ingest/src/config.rs:33). Resolution iterates
`0..config.shard_count` (catalog.rs:289), so a lower configured value
silently omits every series in the missing shards. The one existing
check — the fold HEAD carries `shard_count` (proto/ravel/catalog.proto
field 4) and resolve errors loudly on mismatch — fires only when a
Phase 2 snapshot HEAD exists and is readable. Phase 1 listing, fresh
tenants, and every path before the first fold are silent, which is
exactly the missing-shard failure.

**Store capabilities are asserted, never exercised.** `Capabilities`
(crates/ravel-object-store/src/lib.rs:287) is a struct the backend
constructor fills in; `check_capabilities`
(services/ravel-server/src/store.rs) compares flags against
`Capabilities::mandatory()` and nothing ever exercises the backend. A
store that advertises `consistent_list` but delivers eventual listing
silently violates the seal lemma and can make orphan-GC re-verify see
false absence.

**`/readyz` never reflects store health, though the surrounding facts have moved.**
`/readyz` is a startup-completion latch that never flips back
(services/ravel-server/src/health.rs), performing no store I/O by
documented design. However, the earlier claim that Ravel has no metrics
surface is no longer true: `GET /metrics` exists on every mode's HTTP
listener (ADR-0044 section 4,
services/ravel-server/src/metrics.rs), which changes the design space —
a background store-probe gauge now has somewhere to land. Separately,
`--maintain-tenant` now exists (services/ravel-server/src/config.rs:60),
partially addressing the maintenance-coverage gap; that is outside this
scope but the staleness is noted for the record.

### Frozen-format impact

Two decisions here touch frozen contracts and follow the format-change
procedure (one ADR, explicit versioning, dual-reader answer, checksum
review, fuzz coverage, CLI inspector updates):

- The keyed tenant hash changes the *derivation* of the
  `t/<tenant_hash>/` prefix value, not its shape. The keyed derivation
  gets a new domain string (`ravel-tenant-v2`); the unkeyed
  `ravel-tenant-v1` derivation is untouched and remains valid for the
  deployments pinned to it. Series identity, commit tokens, and RSEG are
  unaffected.
- The durable `shard_count` record, the tenancy marker, the GC-config
  object, and the qualification record are additive key-layout entries:
  a new reserved root prefix `sys/` and a new per-(tenant, signal) key
  `t/<tenant_hash>/<sig>/prov`. New protobuf messages land in a new file
  `proto/ravel/sys.proto`; no existing field is renumbered or reused.
  docs/catalog-and-mvcc.md's key table is amended in the same change
  that ships each key.

Dual-reader answers: the hash scheme is fixed per bucket at bucket
birth, so one binary carries both derivations forever, selected once at
startup by a durable marker — no object is ever rewritten. The new sys
objects have no old-format data to read; absence has an explicit
adoption semantics per object, defined below.

## Decision

Seven decisions, one rule: **when an isolation or durability
precondition cannot be verified, Ravel refuses to start or fails the
request with a typed error. No warn-and-continue on any of these
paths.**

### 1. The mTLS resolver runs only on a dedicated listener

`--mtls-enabled` now requires `--mtls-listener <addr>`, a third listener
served in addition to `--listen-http` and `--listen-grpc`. The
`MtlsResolver` is installed only in the resolver chain used by routes
served on that listener. The chains built for the public HTTP and
gRPC/Flight listeners never contain it, so `x-ravel-client-cert-cn` on
those listeners is inert — not stripped, not trusted, simply never read.

Startup refuses (process exits nonzero before binding any listener, in
`Cli::validate`, where inconsistent auth flags already fail today) when:

- `--mtls-enabled` is set without `--mtls-listener`;
- `--mtls-listener` equals the `--listen-http` or `--listen-grpc`
  address;
- `--dev-insecure-tenant-header` is combined with `--mtls-listener` on
  the same address;
- `--mtls-header` is set without `--mtls-enabled` (existing behavior,
  retained).

The operator contract becomes: point the TLS-terminating,
header-stripping proxy at the mTLS listener and nothing else at it
(network policy); the public listeners are safe against header forgery
by construction, not by proxy hygiene. Misconfiguration is a refusal to
start, never a warning. A forged header on the Flight listener is inert
because the Flight listener's chain cannot resolve the header at all.

### 2. `tenant_hash` mismatch fails closed everywhere

A `tenant_hash` mismatch on the catalog HEAD or on a postings object
becomes a hard `CatalogError::FieldMismatch`, the same class as the
existing `shard_count` mismatch. No fallback to listing. The query fails
with a 5xx carrying the object key and field, never any foreign bytes.

Additionally, every listing helper on the resolve path asserts that each
returned key begins with the requesting tenant's prefix; a violation is
the same hard error.

A new counter, `ravel_catalog_isolation_breach_total`, lands beside the
existing catalog anomaly counters and is rendered at `/metrics`; the
default alert rules shipped with the operations guide page on any
increase. What an operator sees: failing queries for the affected
(tenant, signal) with an explicit isolation-fault error string, plus a
nonzero breach counter — an incident, not a latency blip.

Non-isolation mismatches (postings content-hash, entry-count, snapshot
part hash) keep their degrade-to-listing behavior: they indicate
corruption or staleness of tenant-local derived data, which listing
fallback handles correctly, and they carry no cross-tenant signal.

### 3. The tenant hash is deployment-keyed by default, pinned per bucket

**Derivation.** The keyed variant is
`blake3::keyed_hash(k, tenant_id)[0..16]` where `k =
blake3::derive_key("ravel-tenant-v2", deployment_key)` and
`deployment_key` is 32 bytes loaded from `--tenant-hash-key-file` (file,
not env var, so the secret stays out of process listings). The unkeyed
`ravel-tenant-v1` derivation is unchanged.

**Pinning.** A new immutable root object `sys/tenancy` (protobuf,
`CreateIfAbsent`) records the bucket's scheme (`v1-unkeyed` or
`v2-keyed`) and, for keyed buckets, a key fingerprint
(`blake3::derive_key("ravel-tenant-v2-fingerprint", deployment_key)`,
truncated — a fingerprint of the key, never the key). Every mode reads
it at startup:

- Marker present: the configured scheme and key fingerprint must match,
  or the process refuses to start. This turns "wrong key deployed" from
  a silent parallel namespace (every prefix recomputes, the bucket looks
  empty, new writes land beside old data) into a startup refusal.
- Marker absent, bucket has no `t/` prefixes: a fresh bucket. The
  process writes the marker from its config. The default is keyed: a
  fresh bucket with no `--tenant-hash-key-file` refuses to start unless
  `--tenant-hash-unkeyed` is explicitly passed (single-tenant and dev
  deployments may reasonably opt out; the choice is recorded and
  permanent).
- Marker absent, `t/` data exists: a pre-ADR bucket. Only the unkeyed
  derivation has ever existed in code, so the data is unkeyed by
  construction; the process writes a `v1-unkeyed` marker (logged, and
  counted at `/metrics`) and continues. This is the entire migration
  story for existing deployments: they are pinned unkeyed, permanently.

**No re-key migration.** Existing unkeyed buckets stay unkeyed. Moving a
bucket between schemes relocates every object and is explicitly not
built; a deployment that requires enumeration resistance starts a new
keyed bucket and drains into it operationally. This is stated in the
operations guide.

**Key custody.** For keyed buckets the deployment key becomes
tier-0 durable state outside the bucket, and the docs say so: losing it
makes every `t/<hash>/` prefix unattributable — bytes intact, data
gone. Two mitigations ship with the feature: the fingerprint in
`sys/tenancy` (detects wrong-key before damage), and a per-tenant
recovery manifest — at a tenant's first write, keyed deployments write
`sys/t/<tenant_hash>` containing the tenant id encrypted with an AEAD
key derived from the deployment key (`CreateIfAbsent`, immutable). Bucket
plus key is therefore always sufficient to reconstruct the full
tenant-id-to-prefix mapping; the bucket alone still reveals nothing.

`ravel-cli` grows `tenancy show` (prints the marker and scheme) and
accepts the key file wherever it computes tenant prefixes today.
docs/catalog-and-mvcc.md:43-44 and the ADR-0010 section 13 claim are
corrected in the same change.

### 4. The GC constraint is validated once, durably, in `sys/gc`

A new durable object `sys/gc` (protobuf, versioned) holds the
deployment-wide values: `protection_horizon_ns`, `grace_ns`,
`max_query_duration_ns`, `max_flush_lifetime_ns`. It is written once at
bucket bootstrap via `CreateIfAbsent` (from the maintain defaults, which
already satisfy the constraint) and mutated only by `ravel-cli gc-config
set`, which enforces `protection_horizon >= max_query_duration + grace`
at write time and swaps with `CasVersion`.

Every mode validates itself against `sys/gc` at startup and refuses to
start on violation:

- maintain: its configured horizon and grace must equal the stored
  values (process flags become must-match, not independent knobs);
- query modes: the engine deadline must be <= stored
  `max_query_duration`;
- Flight SQL: the ticket TTL ceiling must be <=
  `protection_horizon - grace`.

The constraint is thereby enforced at exactly two choke points: the
single mutation path (CLI, at write time) and each process's startup
(against the single durable truth). A process that cannot read `sys/gc`
on a bootstrapped bucket does not start; there is no "assume defaults"
path, because assumed defaults are precisely the three-config drift this
replaces.

### 5. `shard_count` becomes a durable, startup-checked property

A new immutable per-(tenant, signal) provisioning record at
`t/<tenant_hash>/<sig>/prov` (protobuf: `tenant_hash`, `signal`,
`shard_count`, format floor, `created_unix_ns`) is written with
`CreateIfAbsent` at the tenant's first write for that signal; a racing
loser re-reads and validates. Consumers:

- Ingest router construction, catalog construction / first resolve per
  (tenant, signal), and each maintain per-tenant loop validate the
  configured `shard_count` against the record. Static tenant sets
  (tokens, `--maintain-tenant`) validate at startup and refuse to start
  on mismatch; dynamically resolved tenants validate at first touch and
  fail the request with a typed error (plus a counter) — never silently
  serve a subset of shards.
- The existing fold-HEAD `shard_count` check stays; the record extends
  the same loud `FieldMismatch` semantics to Phase 1 listing and fresh
  tenants, closing the missing-shard window.

**Adoption for pre-ADR data.** A (tenant, signal) with data but no
record is adopted exactly once, at first ingest or maintenance touch:
the adopter delimiter-lists the shard prefixes under `.../l0/` and
`.../c/` and, only if no observed shard index >= the configured count,
writes the record from config. If a higher shard index is observed, the
configured value is provably hiding data and the process refuses
(startup) or fails the request (dynamic) without writing anything.
Empty shards cannot be lost by this check because they hold nothing to
hide. `ravel-cli provision adopt` runs the same code path for operators
who want adoption done before an upgrade rollout.

`shard_count` remains immutable per (tenant, signal). Resharding (a
shard-epoch map consulted by resolution) is real future work and is
explicitly deferred to its own ADR; this decision makes the current
value safe, not changeable.

### 6. Store backends are qualified empirically, once per bucket

A new `conformance` module in ravel-object-store implements an
empirical qualification suite run against a live backend under a scratch
prefix (`sys/qualify/<run-id>/`): `CreateIfAbsent` single-winner under
concurrent same-key writers; `CasVersion` stale-precondition rejection;
immediate list-after-write visibility over repeated put-then-list
cycles; cross-page listing consistency; multipart-complete visibility.
The suite can only falsify consistency claims, not prove them — the
docs say so plainly; a pass is qualification, not proof.

`ravel-cli store qualify` runs the suite and, on pass, records
`sys/qualification` (backend endpoint identity, suite version, pass
timestamp) via `CreateIfAbsent`. Server startup on a production store
kind refuses to start when the record is absent or its suite version is
below the binary's required floor; `MemoryStore` (the semantics oracle)
is exempt. Qualification is once per bucket, not per boot: a per-boot
probe would add write cost and fleet-restart herding while adding no
proof, and a backend that regresses after qualification is exactly what
the tenant_hash hard errors, the fold-divergence checks, and `catalog verify`
exist to catch downstream.

### 7. `/readyz` reflects store reachability, with hysteresis

`/readyz` stays free of per-probe I/O. Each process runs one background
store probe: a small GET of `sys/tenancy` every `--store-probe-interval`
(default 30 s, jittered) — a single fixed object, so the added load is
one GET per process per interval. After K consecutive failures (default
4), readiness flips to 503; the first success flips it back. Readiness
is now the AND of the existing startup latch and probe health.

The two objections documented in health.rs are addressed rather than
overridden: kubelet-frequency store calls (solved: probing is on its own
cadence, `/readyz` reads an atomic), and single-blip mass ejection
(solved: roughly two minutes of hysteresis at defaults; a store outage
that long means every data path is failing, and marking the fleet
unready is the truthful signal — traffic fails fast at the LB instead of
timing out per request). The probe also exports
`ravel_store_reachable` (gauge) and a probe-failure counter at
`/metrics`, with a default alert rule, so operators see the outage even
where nothing consumes readiness.

## Rejected alternatives

1. **Keep the mTLS resolver on all listeners and document the proxy
   requirement (status quo).** The trust precondition is unverifiable at
   runtime and the failure mode is total (any-tenant authentication).
   Lost because a startup refusal is cheap and turns a silent bypass
   into a visible misconfiguration.
2. **Terminate mTLS in Ravel and verify client certificates directly.**
   Rebuilds a TLS stack and a second, weaker parser of certificate
   identity that ADR-0042 decision 6 deliberately delegated to the
   proxy. Lost because the dedicated-listener split achieves the same
   trust boundary with configuration, not cryptography.
3. **Warn-and-degrade on `tenant_hash` mismatch (status quo).** Optimizes
   availability over integrity on a signal whose only honest reading is
   "isolation fault". Forensically invisible today. Lost to the
   fail-closed rule; the loud-error precedent already exists in the same
   function for `shard_count`.
4. **Re-key existing unkeyed buckets to the keyed hash.** A full copy of
   every object, plus dual-read during the copy, imposed on every
   existing deployment at upgrade. Lost because pinning costs nothing,
   is honest ("the choice is permanent, made at bucket birth"), and the
   keyed default still lands before meaningful data volume exists in
   new buckets.
5. **Per-tenant salt objects, or random hashes with an in-bucket
   directory, instead of a deployment key.** A salt readable by anyone
   with list access defeats enumeration resistance; a directory makes
   every auth resolution consult mutable durable state and breaks
   offline prefix derivation for every CLI tool. Lost on both security
   and dependency grounds.
6. **Key via environment variable with no durable marker.** Deploying
   the wrong key silently recomputes every prefix and splits the
   namespace. Lost to the marker-plus-fingerprint design, which converts
   that scenario into a startup refusal.
7. **Enforce the GC constraint via documentation and the k8s operator.**
   The operator covers only k8s deployments, and documentation is the
   mechanism that already failed (that is the finding). Lost to a single
   durable authority every process validates against.
8. **Validate each process's own config locally, without a shared
   object.** Catches internal inconsistency but not cross-process drift,
   which is the actual hazard (maintain lowering the horizon while
   Flight keeps a 24 h ticket ceiling). Lost because it fails the
   "validated once" requirement.
9. **Infer `shard_count` from listing at every startup.** Racy against
   empty hours, silent for legitimately empty shards, and converts a
   config error into inferred behavior instead of a refusal. Lost to an
   explicit durable record with explicit adoption.
10. **Keep relying on the fold-HEAD `shard_count` check alone (status
    quo).** Absent for fresh tenants and for Phase 1 listing — exactly
    the missing-shard window. Lost because the record closes the window the
    existing check structurally cannot.
11. **Run the conformance probe at every server startup.** Repeated
    write cost, thundering-herd on fleet restarts, and false comfort — a
    passing probe at boot proves nothing about behavior an hour later.
    Lost to once-per-bucket qualification with a durable record.
12. **A hardcoded qualified-backend allowlist.** Rejects conforming
    unknown backends and blesses a known gateway that regresses in a
    point release. Lost because the property worth gating on is
    behavior, not vendor identity.
13. **`/readyz` performs a live store call per probe.** Kubelet-frequency
    S3 cost and single-blip fleet ejection — the objections health.rs
    already documents are correct. Lost to the background probe.
14. **Keep `/readyz` green and export only a metrics gauge.** Leaves the
    one interface Kubernetes acts on
    lying during the exact outage it exists to signal, and deployments
    without alerting stay blind. Lost because hysteresis removes the
    mass-ejection objection that motivated it.

## Consequences

- Startup becomes deliberately stricter. New refusal conditions: mTLS
  listener misconfiguration; tenancy scheme or key-fingerprint mismatch;
  fresh keyed bucket without a key; `sys/gc` violation or unreadability;
  `shard_count` disagreement with the provisioning record; missing or
  stale store qualification. Every one replaces a silent wrong behavior
  with a visible failed deploy.
- New durable objects and key-layout entries: root prefix `sys/`
  (`tenancy`, `gc`, `qualification`, `t/<hash>` recovery manifests,
  `qualify/` scratch) and `t/<tenant_hash>/<sig>/prov`. All are
  additive; docs/catalog-and-mvcc.md's key table is amended in the same
  changes. New messages land in `proto/ravel/sys.proto`; property tests
  cover corrupt and truncated inputs with typed errors; `ravel-cli`
  gains inspectors (`tenancy show`, `gc-config show`, `provision`,
  `store qualify`).
- For keyed buckets the deployment key is a new tier-0 durability
  dependency outside the object store — a deliberate exception to
  "object storage is the source of truth", documented in the DR
  runbook, and bounded by the in-bucket encrypted manifest (bucket +
  key always suffices for full recovery).
- Existing unkeyed deployments keep their layout forever; enumeration
  resistance applies to buckets created after this ADR. The
  `ravel-tenant-v1` derivation and all v1-pinned data remain valid
  indefinitely (dual-derivation binary, selected by the marker).
- Operational load added: one GET per process per probe interval, one
  qualification run per bucket, one adoption listing per pre-existing
  (tenant, signal). All O(1) per deployment, none per request.
- `/readyz` flipping on store outages changes rollout semantics:
  deployments gated on readiness will (correctly) halt while the store
  is unreachable. The operations guide documents this and the K-failure
  hysteresis knobs.
- Forged mTLS header, lower `shard_count` on restart, and `/readyz`
  under store outage become automated tests; the isolation fault becomes
  observable at `/metrics`.
- Not solved here, explicitly: resharding (needs a shard-epoch ADR),
  per-tenant credentials, and per-tenant KMS. Nothing in this ADR
  forecloses them; the provisioning record
  gives a future epoch map a durable home.
