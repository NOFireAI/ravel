# ADR-0066: format migration machinery and restart-free tenant lifecycle

Status: Accepted

## Context

This ADR closes two gaps: no data-format migration machinery, and
tenant-lifecycle changes requiring a fleet restart (maintenance
discovers tenants live, but ingest auth maps and provisioning stay
config-bound; ADR-0052 gives an additive proto-field precedent but no
general migration harness).

Every persistent format in Ravel is a frozen contract, and nearly every one is already version-tagged in its encoded bytes with a fail-closed check. The gap is not detection; it is that nothing exists between "the reader fails closed on an old version" and "wipe the bucket". ADR-0027 made that explicit and temporary: "No migration tooling. Development object stores holding pre-v5 objects are wiped or re-ingested. Acceptable only because nothing has shipped" (decision 6), and "this policy expires at the first public release" (decision 7). ADR-0032 (RLOG v2) and ADR-0045/0054 (RSPAN) applied the same single-version, no-dual-reader policy, each noting it is a pre-release state, not a standing rule. This ADR builds what ADR-0027 decision 7 promises, before the formats are frozen in anger.

### What is already versioned, per format

**Bulk data objects.** All three carry a 16-byte trailer with a crc-covered u16 version and a 4-byte format-family magic, and fail closed on any version other than the single supported one:

- RSEG (metrics): magic `RSG1`, version 6 only (`crates/ravel-segment/src/format.rs:47`, gate `reader.rs:85-87`), typed `SegmentError::UnsupportedVersion(u16)`. The nested SERIES_IDX section carries its own version byte with its own typed error (`sparse.rs:52`, `:167-170`).
- RLOG (logs): magic `RLG1`, version 2 only (`crates/ravel-logseg/src/footer.rs:22`, gate `:250-254`), but the error is stringly typed (`LogSegError::Corrupted(format!(...))`), not a distinct variant. The nested POSTINGS section is the one multi-version decoder in the tree: it accepts versions 1 and 2 and records which it saw (`postings.rs:350-354`, `:521-522`; ADR-0049's deliberate carve-out).
- RSPAN (spans): magic `RSP1`, version 3 only (`crates/ravel-rspan/src/footer.rs:16,24`), also stringly typed.

**Commit-family metadata records** (immutable protobufs, field numbers frozen): `CommitRecord`, `CompactionRecord`, `RetentionTombstone` each carry their own `format_version` (= 1) with a typed check (`crates/ravel-commit/src/record.rs:127-131`), and `CommitRecord.segment_format_version` (proto field 17) plus `CompactionPart.segment_format_version` (field 11) record the trailer version of every live data object. The catalog therefore knows the format version of every live segment without a single data-object GET. `ravel-cli maintain audit-versions` (`services/ravel-cli/src/maintain.rs:277-408`) already exploits this: an operator-triggered walk of a whole tenant, histogramming live L0/L1 objects by version from commit and compaction records alone, exiting nonzero on any anomaly. Its own doc comment states the gap this ADR closes: "there is no migration path, only this report".

**Catalog snapshot objects** (derived state, rebuilt continuously by the fold): `.csnap` parts have magic `RCS1` + envelope version byte + a redundant header `format_version` cross-check (`crates/ravel-catalog/src/snapshot_format/part.rs:95-133`); `.npost` postings have magic `RNP1` + version byte; the HEAD is a bare protobuf with `format_version` checked at `snapshot_format/head.rs:29-33`. One hazard found in this survey: the fold treats any HEAD decode failure, including `UnsupportedHeadVersion`, as `HeadState::Corrupt` and CAS-overwrites it (`crates/ravel-catalog/src/fold.rs:866-869`, `:796-798`). Under a rolling upgrade a lagging process would clobber a newer-format HEAD written by an upgraded one. Migration machinery makes this path load-bearing, so Decision 2 fixes it.

**Sys/control objects**: `TenancyMarker`, `TenantRecoveryManifest`, `ProvisioningRecord`, `GcConfig` all carry `format_version` documented as a reader floor: "a reader that only knows a lower version refuses rather than misreading a future layout as v1" (`proto/ravel/sys.proto`). ADR-0052 established the additive-evolution precedent in writing: adding the `generations` field did not bump the floor, with the reasoning recorded inline in the proto.

**Identity and token encodings** (`crates/ravel-types`): versioned by domain separation rather than a readable tag. `SeriesId` hashes the literal prefix `ravel-series-v1\0` (`lib.rs:415`); the version is hashed, never stored, so a bump is a silent identity split, not a decode error, and no v2 domain string has ever existed. `TenantHash` derivation is the one identity format with deployment-level pinning: the immutable `sys/tenancy` marker records the scheme and a process whose build disagrees refuses to start (ADR-0050 §3). The commit token carries a literal `v2:` prefix but an unknown version collapses into the same flat `TypeError::InvalidCommitToken` as garbage input (`lib.rs:526-554`).

**Object key layout**: no version component anywhere, by design. A v6 and a hypothetical v7 RSEG share the `.rseg` suffix (`crates/ravel-commit/src/keys.rs:15-17`); the only key discriminators are suffix and level directory. Version truth is byte-level plus commit-record metadata, and this ADR keeps it that way.

### Prior format changes, in practice

RSEG went v1 through v6. Through v5 every bump kept a dual reader dispatching on all prior versions; ADR-0017 called out the cost of a "third permanent reader-dispatch branch", and ADR-0049 recorded the sharper datum that hand-mirrored version literals cost "sixteen sites across three tasks for the RSEG bump alone". ADR-0027 then deleted all but the latest version and declared the single-version pre-release policy; ADR-0032 (RLOG v1 to v2) and ADR-0054 (RSPAN v3) followed it with "no dual-reader path". ADR-0050 shipped the tenant-hash v2 scheme with, in its own words, "the entire migration story" being that pre-existing buckets are pinned unkeyed permanently — and its rejected alternative 4 is precisely the rewrite-everything migration this ADR must also reject. ADR-0052 changed a persistent contract additively (generation-versioned `shard_count`, no data movement) and, in its hybrid alternative, explicitly left open "optional background re-generation compaction later (rewrite an old generation's L1 parts through the existing compaction supersession machinery, which already handles token-to-superseded-object resolution)", noting it "needs its own ADR". Decision 5 below is that ADR-ed rewrite machinery, generalized to format versions.

Two structural facts about compaction bound what "rewrite" can mean:

- Every compaction output is a brand-new object PUT with `CreateIfAbsent`, stamped with the current format version (`crates/ravel-maintain/src/build.rs:70-72`, `rlog.rs:103`, `rspan_codec.rs:80`), superseding its inputs through the compaction record. Rewrite-shaped migration inherits durability, custody (ADR-0042 holds bind to objects; sweep is hold-aware), and commit-token resolution for free only if it rides this exact path.
- RSEG compaction copies page bytes verbatim, never decoded (`build.rs:1-14`; ADR-0018: "a re-layout, not a rewrite"). So the existing compactor can carry a trailer/section-layer version bump but not a page-grammar change; a page-grammar migration needs a true decode-and-re-encode primitive that does not exist today. RLOG and RSPAN compaction genuinely re-encode already.

And one blocking fact: today's readers accept exactly one version, so the compactor cannot open an old-version object at all (`crates/ravel-maintain/src/read.rs:257` via the single-version gates). Any convergence mechanism presupposes the N/N-1 reader window of Decision 1.

The format-change skill already requires every format ADR to answer "the dual-reader question" and keep both read paths "until retention clears the old data" — but nothing today can make old data converge faster than retention, verify that it has converged, or say when the old read path may be deleted. That is the machinery gap. (The skill also still asserts "data written today must be readable by every future version", which the pre-release ADRs contradict; Consequences amends it to state both regimes.)

### Tenant lifecycle today

Every tenant-shaped input is a clap flag parsed once into an immutable `ServerConfig`: the bearer token map (`--tenant-token`, `services/ravel-server/src/config.rs:58`), per-tenant admission limits (`--limits-file`, `config.rs:237`, installed once at `lib.rs:496-503` — `admission.rs:422-424` says outright "changing limits is a restart", codified by ADR-0051 §3), per-tenant retention, indexed fields, alert rules, and `--shards`. There is no SIGHUP handler, no file watch, no reload endpoint (`main.rs:392-410` handles shutdown signals only). The operations guide documents restart as the mechanism: "To add, remove, or rotate a tenant token, restart ravel-server with a different flag set" (`docs/guides/operations.md:1060-1062`). The k8s operator makes the restart structural: a tenant-token Secret change restamps a checksum annotation and rolls the Deployments (`services/ravel-operator/src/reconcile.rs:44,:378,:1010`).

What "narrowed" means, precisely: maintenance, fold, and scrub discover tenants live by LISTing `t/` (`crates/ravel-maintain/src/discover.rs:23`, `services/ravel-server/src/tenant_discovery.rs:47`) — but only when the restriction set is empty. Any deployment using `--tenant-token` gets a startup-frozen allowlist (`main.rs:109` unions token tenants into `config.fold_tenants` once), so a newly onboarded tenant is discovered-but-excluded until restart, and — worse — removing a token to "deprovision" a tenant leaves its data present but no longer compacted, swept, or retention-enforced, since the maintenance cycle iterates only maintained tenants. Deprovisioning as such does not exist; ADR-0019 deferred offboarding and nothing since picked it up.

One mechanism already does restart-free per-tenant config, and it is the model to generalize: `GenerationSwitch` (`crates/ravel-ingest/src/generation.rs:11-59`) caches each tenant's durable `prov` record and re-reads it on a 60s horizon, failing the flush closed if the re-read fails — which is why `ravel-cli provision reshard` changes fleet-wide routing with no restart. ADR-0057 adds the fleet-wide transport shape (self-owned snapshot keys, interval reads off the hot path, staleness rule, last-known-value on read failure). Nothing applies either pattern to auth, limits, or lifecycle today.

## Decision

Classify every persistent format into one of four migration classes with a named convergence mechanism per class; record per-(tenant, signal) format floors in the durable provisioning record so old-version read paths are retired on evidence instead of hope; build the rewrite machinery inside maintenance as a variant of compaction, not a new service; and move tenant lifecycle from process flags to durable control objects with bounded-staleness refresh, the pattern the codebase already trusts twice.

### 1. Version lifecycle policy: N/N-1 window, readers first

ADR-0027's single-version policy is superseded at (and only at) first public release, exactly as its decision 7 anticipated. From then on, for the bulk data-object formats (Class A below):

- Writers always emit the current version N. There is never a "write the old format for compatibility" mode.
- Readers accept N and N-1. Support for N-1 is deleted only when every bucket's recorded format floor (Decision 3) is >= N; that deletion is its own reviewed change citing the floors.
- Rollout is readers-before-writers: a release that writes N+1 requires a fleet already reading N+1. With Decision 2 this is safe rather than merely intended: a lagging process meeting N+1 data halts loudly instead of misbehaving.
- A version bump still requires an ADR, a version bump, and the format-change skill procedure — the machinery lowers the cost of carrying a transition, not the bar for starting one.
- Version constants stay single-sourced in each format crate (`VERSION_V6`, `footer::VERSION`), which audit-versions and the migration job read, so a bump does not repeat ADR-0049's sixteen-hand-edited-sites experience.

Until first release, ADR-0027 stands unchanged; this ADR's machinery lands exercised by tests and dry-runs rather than by carrying real dual versions in anger.

### 2. Fail-closed-on-newer, everywhere, typed

Every decoder of a persistent format must, on a version newer than it knows, return a typed error distinct from corruption, and no caller may treat that error as absence, corruption, or a miss. (Sole deliberate exception: the local disk cache, where old-version-equals-miss is correct semantics, `crates/ravel-cache/src/disk.rs:143-148`.) Concretely:

- RLOG and RSPAN trailer version failures become typed `UnsupportedVersion(u16)` variants instead of `Corrupted(String)`, matching RSEG.
- The catalog fold distinguishes `UnsupportedHeadVersion` from a corrupt HEAD and fails its cycle instead of CAS-clobbering (`fold.rs:866-869`) — the rolling-upgrade hazard from Context. This is a hard prerequisite for multi-part fold (ADR-0063).
- The commit token parser surfaces an unknown version prefix as its own typed error rather than flat `InvalidCommitToken`, so a future v3 token is diagnosable at the client boundary.

### 3. Durable format floors in the provisioning record

Extend `ProvisioningRecord` additively (no reader-floor bump; the ADR-0052 `generations` reasoning applies verbatim) with an append-only list of format floors:

```
message FormatFloor {
  uint32 family = 1;         // enum: RSEG, RLOG, RSPAN, CSNAP, ...
  uint32 floor_version = 2;  // no live object of family < this version
  sfixed64 raised_unix_ns = 3;
  string raised_by = 4;      // job identity, informational
}
repeated FormatFloor format_floors = 7;  // CAS append, like generations
```

A recorded floor F for family X asserts that no live object of family X below version F exists for this (tenant, signal). Floors are raised only by the verification step of the migration job (Decision 5) after an audit-versions enumeration over current commit and compaction records comes back clean at >= F, and never lower. The load-bearing consumer is release engineering: deleting version V's read support is legal only when every bucket's floor exceeds V — a checkable fact, where today there is only ADR-0027's wipe-and-hope. This is deliberately the ADR-0052 shape: version facts live in the per-(tenant, signal) durable record, appended under CAS, never rewritten.

### 4. Convergence, by migration class

**Class A — bulk data objects (RSEG, RLOG, RSPAN).** Large, immutable, never rewritten in place. Three convergence forces, in preference order:

1. Retention: old-version objects age out with their hour buckets at zero marginal cost. Deployments whose retention window is shorter than their release cadence converge on this alone.
2. Rewrite-on-touch: compaction outputs are always current-version, so L0 converges through the normal maintenance loop once the N-1 reader exists. Additionally, maintenance treats "live L1 part with `segment_format_version` < current" as compaction-eligible at low priority under the existing maintenance cost budget, so compacted data converges opportunistically too. Caveat honored from Context: this carries trailer/section-layer bumps through the existing verbatim-copy pipeline; a page-grammar bump routes through the re-encode primitive of Decision 5 instead.
3. The operator-triggered migration job (Decision 5) for the tail neither force reaches fast enough, and for verify-and-raise-floor.

**Class B — derived catalog objects (.csnap, .npost, HEAD).** Rebuildable from commit records by construction; the fold rewrites them continuously. A version bump needs no migration tool: the upgraded fold emits the new version, supersession GCs the old parts, and dual-read is needed only across the rolling-upgrade window. Multi-part fold (ADR-0063) is exactly such a bump and is this rule's first consumer.

**Class C — immutable metadata records (commit records, compaction records, tombstones, sys/* objects, idempotency markers).** Never rewritten; commit-record immutability is a repo invariant and migration machinery gets no exemption. Default evolution is additive protobuf change (frozen field numbers, new fields only — the ADR-0052 precedent, now normative). A genuinely incompatible change requires a new record kind under a new key suffix, dual-listed alongside the old kind until retention tombstones the old records' hour buckets; the reader-floor `format_version` these records already carry keeps an incompatible in-place edit detectable and refused.

**Class D — identity and domain-hash encodings (series-identity domain string, tenant-hash scheme, commit-token version).** Not migratable by generic machinery: a bump splits identity rather than failing a decode. The obligation here is containment, not migration: the active version is pinned per bucket in a durable control object — the `sys/tenancy` `TenantHashScheme` pattern, extended to record the series-identity domain and token version — and a process whose build disagrees refuses to start. An actual identity re-key is out of scope here; each such event is its own ADR (as the unkeyed-tenant-hash re-key already is).

### 5. The migration job: a compaction variant, resumable, verifying

A new maintenance operation, `ravel-cli maintain migrate`, also runnable under server maintain-mode, per (tenant, signal, format family, target version):

- Discovery walks commit and compaction records (the audit-versions enumeration; never LISTs data objects) selecting live objects with `segment_format_version < target`.
- Rewrite is a single-input compaction: read the old object with the N-1 reader, decode and re-encode with the current writer (the new primitive; RLOG/RSPAN reuse their existing re-encode paths, RSEG gains one), publish through the existing compaction publish path — new `CreateIfAbsent` object, compaction record superseding the input, record-count conservation check (sum in == sum out) — so the old object becomes sweepable exactly like any superseded input. Commit-token resolution and ADR-0042 custody continuity are inherited from the supersession machinery, which ADR-0052's hybrid alternative already identified as the sanctioned vehicle; held buckets defer deletion via the existing hold-aware sweep, so a migration never fights a legal hold.
- Resumability: a durable per-(tenant, signal, family) cursor in the advisory CAS maint-cursor pattern (`crates/ravel-maintain/src/scan.rs`), so a killed job resumes rather than restarts. Rate and cost ride the existing maintenance budget; the job is idempotent by construction.
- Verification and floor raise: after a clean walk, re-run the audit enumeration from current records; any straggler exits nonzero and the floor is not raised; otherwise CAS-append the floor (Decision 3).
- Role: runs under the maintenance credential (ADR-0055). Query and ingest roles never rewrite objects; there is no read-path migration.
- Once leased maintenance (ADR-0065) lands, `migrate` becomes a leased work kind like compaction and sweep; until then it follows today's maintain-mode single-runner rules.

### 6. Tenant lifecycle without restart

Move per-tenant lifecycle state from flags to durable control objects, read on a bounded-staleness horizon — the `GenerationSwitch` pattern (60s refresh, fail-closed on unrefreshable state) with ADR-0057's transport discipline (interval reads off the hot path, staleness rule, last-known-value on transient read failure, a staleness metric).

- **A per-tenant config record**, `t/<tenant_hash>/config` (protobuf with a reader-floor `format_version`, CAS-versioned writes): admission-limit overrides, per-tenant retention, indexed-field config, and a lifecycle state: `active`, `suspended` (refuse ingest/query, keep maintenance), `offboarding` (refuse ingest/query, maintenance and retention keep running until the data is gone). Defaults still come from flags/limits-file at startup; the durable record overrides them per tenant. The admission controller's existing `set_tenant_limits` is re-invoked from the refresh loop, so the hot path is untouched — only the numbers change, exactly as ADR-0057 did for fleet thresholds. ADR-0051 §3 ("changing limits is a restart") is amended by this decision.
- **Auth without restart**: the static bearer-token map becomes a durable `sys/auth` object holding keyed token hashes (blake3 keyed by the deployment key, the tenant-hash-v2 pattern) mapped to tenant ids — never plaintext tokens in the bucket. The resolver refreshes it on the horizon, with an on-miss re-read (rate-limited) so a freshly provisioned token authenticates within seconds, and removal takes effect within the horizon, fail-closed. OIDC/mTLS deployments already derive tenant from claims and are untouched.
- **The maintenance allowlist thaw**: `config.fold_tenants` stops being a startup-frozen union; the restriction set is re-derived each discovery cycle from the durable records, so a new tenant is maintained from its first cycle and an offboarding tenant keeps retention enforcement until empty — closing the removed-token-disables-retention hazard from Context.
- **Operator**: the CRD's tenant Secret continues to hold tokens, but the operator writes the hashed map and per-tenant config objects instead of restamping pod templates; a tenant change becomes a control-object write, not a Deployment roll.
- Staleness semantics are explicit and asymmetric: grants (new tenant, raised limit) may take up to the horizon to appear everywhere; revocations are also horizon-bounded, and the horizon is a documented, configurable constant of the same order as `GenerationSwitch`'s 60s. A process that cannot refresh past a hard multiple of the horizon treats auth-affecting state as stale and fails closed for lifecycle-gated operations, mirroring ADR-0052's prov-staleness rule.

Deleting an offboarded tenant's data is selective deletion's (ADR-0064) mechanism, not this ADR's; the `offboarding` lifecycle state is the hook it consumes.

## Rejected alternatives

**Unbounded dual-read (every reader understands every historical version forever).** The pre-ADR-0027 reality: five RSEG versions in weeks, each dragging a permanent reader-dispatch branch, goldens, fuzz seeds, bench builders, and an amendment layer (ADR-0017 named the cost; ADR-0049 measured the version-literal sprawl at sixteen sites for one bump). Rejected because the cost is recorded history here, and because "every consumer handles every version" spreads that cost to the fold, the compactor, the CLI inspectors, and every future reader. The N/N-1 window plus evidence-based retirement keeps reader surface proportional to two versions, not the version count.

**Stop-the-world offline migration on upgrade.** Rewrite everything to N before the fleet runs N. Violates the deliverable ("does not require rewriting all data at once"); its cost grows with stored volume forever; ADR-0050 rejected exactly this shape for the tenant-hash re-key ("a full copy of every object, plus dual-read during the copy, imposed on every existing deployment at upgrade"), and ADR-0052 rejected it again for resharding — doubling storage and PUT spend, an unbounded degraded window, and dangling every issued commit token. Rejected on the same grounds.

**A dedicated migration service.** Rejected: every compute process is disposable, and maintenance already owns "walk a tenant's records and rewrite objects under a budget" (compaction, sweep, retention, scrub), with leases arriving via ADR-0065. A separate service duplicates that machinery, needs its own credential role under ADR-0055, and adds a standing component for a rare-by-design operation.

**Version tags in object keys (`.rseg7` suffixes or a version path segment).** Would let migration find old objects by LIST alone. Rejected: the key layout is itself a frozen contract, the suffix is version-free on purpose (`keys.rs:15-17`), and commit-record metadata already answers "which version is this object" without a GET. A second source of version truth in keys can only ever disagree with the first.

**Rewrite-on-read (the query path migrates what it touches).** Rejected: it puts writes on the read path, and under ADR-0055 the query role deliberately holds no write grant — a migration mechanism that requires violating the credential boundary is not a mechanism. It also converts read latency into write amplification at the worst moment (a hot query over cold old data).

**Bump protobuf package names (`ravel.commit.v2`) as the versioning mechanism for metadata.** Rejected: package version names the schema namespace, not the data; a package bump forks every message type and call site while the wire bytes stay additive anyway. The existing convention — frozen field numbers, additive fields, per-message reader-floor `format_version` — already covers Class C with none of that churn.

**For tenant lifecycle: SIGHUP / file-watch reload of the limits and token files.** Fixes the restart, keeps the architecture wrong: a file is per-process state, so a fleet applies changes raggedly with no record of who converged, and under the operator the file is a Secret mount whose change already implies a pod roll. Also contradicts the founding invariant that object storage is the only durable backend — tenant existence is durable state and belongs there. Rejected as the mechanism (a dev-mode convenience may still exist).

**For tenant lifecycle: control-plane push (config service or watch channel).** Rejected: a new always-on coordination dependency and a second source of truth against the S3-only architecture. Bounded-staleness polling of durable objects is the established, tested pattern (GenerationSwitch, ADR-0057), and its staleness bound is already this codebase's accepted consistency currency.

## Consequences

- ADR-0027 is superseded at first public release on the terms its own decision 7 set; until then it stands, and this ADR's Class A machinery is exercised by tests and dry-runs.
- The format-change skill is amended: state the format's migration class (A-D) and convergence plan in every format ADR; for Class A, land the N-1 reader before any N writer ships; deleting an old version's reader requires citing every bucket's floor. The skill's "readable by every future version" sentence is corrected to state both regimes (pre-release single-version per ADR-0027; post-release N/N-1 window per this ADR) — today the carve-out lives only inside ADR-0027 and the skill contradicts it.
- `docs/segment-format.md`, `docs/log-segment-format.md`, `docs/catalog-and-mvcc.md` each gain a normative migration paragraph; ADR-0051 §3 and `docs/guides/admission-limits.md` and `operations.md` drop "restart to change"; README/PROGRESS updated in the same commits as the code (doc-currency rule).
- Interaction with multi-part fold (ADR-0063): it is a Class B bump — envelope version bump, fold regenerates, dual-read across the rolling window only. Decision 2's HEAD fix must land before its format change dispatches, or a lagging fold process will CAS-clobber its new-format HEAD during rollout. This ADR should sequence that task first for its benefit.
- Interaction with selective deletion (ADR-0064): its erasure rewrite (read object, drop a subject's records, write new object, supersede) is structurally Decision 5's rewrite primitive with a different conservation predicate (this ADR: exact conservation; ADR-0064: exact minus the erased set). The primitive is built once in ravel-maintain and shared; whichever lands second consumes it. ADR-0064 also consumes the `offboarding` lifecycle state as its whole-tenant entry point. Both ADRs must reference this split.
- Interaction with leased maintenance (ADR-0065): `migrate` becomes a leased work kind when leases land; its cursor and budget design must not assume a single runner in a way it would have to unwind.
- New failure surface accepted: a floor CAS-appended over a stale audit would be a false assertion; hence the raise re-audits from current records after the walk and the whole raise rides behind the same publish-side guards as compaction. Named as an acceptance test.
- New failure surface accepted: bounded-staleness auth means a revoked token can authenticate for up to the horizon; this is documented and bounded, versus today's alternative of a fleet restart per revocation. The fail-closed staleness rule converts an unreachable store into refused lifecycle-gated operations, the same availability trade ADR-0052 made for prov staleness.
- Cost: the migration job's rewrite is a read+PUT per object, bounded by the maintenance budget; floors add one small CAS append per (tenant, signal, family) per migration; tenant-config refresh adds one GET per tenant per horizon per process, the same order as the existing GenerationSwitch reads.

## Out-of-scope findings, reported not fixed

1. **Live rolling-upgrade bug**: the catalog fold treats an `UnsupportedHeadVersion` HEAD as corrupt and CAS-clobbers it (`crates/ravel-catalog/src/fold.rs:866-869`, `:796-798`). A lagging process would overwrite a newer-format HEAD during any future rollout. Folded into this ADR as Decision 2, but it is a live hazard today, independent of whether this ADR is approved, and gates multi-part fold (ADR-0063).
2. **Spec/code contradiction**: the format-change skill asserts "data written today must be readable by every future version" and "keep both paths", but ADR-0027/0032/0045/0054 delete old read paths pre-release; the carve-out lives only in ADR-0027 decision 7. The skill was never amended.
3. **Stale doc pointers**: `docs/adrs/README.md` still lists ADR-0045 as "RSPAN v2 and v3" (amended to v2/v4 by ADR-0054), and `crates/ravel-segment/src/reader.rs:3,:135` and `lib.rs:4` doc comments still say "v5 only" while the gate is v6.
4. Minor: `CommitRecord.segment_format_version` is copied through unvalidated (`record.rs:97`); many tests hardcode `1` and nothing rejects it.

## Amendment: `family` is a lowercase string, not an enum

The implementation shipped `FormatFloor.family` as `string family = 1` (lowercase format-family
id: `"rseg"`, `"rlog"`, `"rspan"`, mirroring each format's trailer magic),
not the `uint32 family` enum sketched in Decision 3. Reason: by the time it
landed, the typed `UnsupportedVersion` work had already established no
family-naming convention — those variants carry only a numeric version, no
family tag — so there was no enum to be consistent with, and a free-form
string keeps a new format family (a future signal, or a sub-format within an
existing one) addable without a proto change, matching the additive-evolution
discipline the rest of this record follows. Decoding enforces `family` is
non-empty and lowercase, fail-closed on either violation, so the field is not
an unconstrained string in practice — see `ravel_catalog::provisioning::FloorDefect`.

## Amendment (2026-08-30): Class B convergence splits by binding grain

Decision 4's Class B definition asserts that a derived catalog object converges
automatically because "the fold rewrites them continuously," so a version bump
"needs no migration tool." That premise holds only for a **whole-set-bound**
derived object — one every fold regenerates in full (`.csnap`, `.npost`, HEAD):
the upgraded fold emits the new version, supersession GCs the old parts, and
convergence is free.

It does **not** hold for a **part-bound** derived object — one bound to a
specific snapshot part's content hash so that a sealed part is carried forward
by reference and never rewritten (ADR-0913 §2a binds `.magg` state this way so
that "sealed history's states survive every fold untouched"; ADR-0942 re-keys
`.cstat` the same way). Continuous rewrite is exactly what Class B relies on for
free convergence, and *not* rewriting sealed state is exactly what makes
per-part binding economical: they are the same property with opposite signs, so
one class rule cannot describe both. The concrete consequence, already observed:
an idle, fully-compacted tenant folds nothing and rebuilds nothing. The
incremental fold lists only hours after the previous watermark plus a bounded
reconcile window, sealed parts are carried forward by reference and never
re-listed, and the build reuses the prior baseline for any entry already in it
(`crates/ravel-catalog/src/fold.rs:1515`, `baseline.get(&identity)`). A
part-bound object relying on Class B's automatic convergence to gain coverage on
such a tenant gains none, silently, because the gap degrades to fall-back-to-scan
rather than to an error.

Class B therefore has two convergence sub-cases:

- **Whole-set-bound derived objects** (`.csnap`, `.npost`, HEAD) converge by
  continuous fold rewrite, exactly as the original definition states. A version
  bump needs no migration tool: the upgraded fold emits the new version and
  supersession GCs the old parts, with dual-read only across the rolling-upgrade
  window. Multi-part fold (ADR-0063) remains this sub-case's first consumer.
- **Part-bound derived objects** (`.magg` per ADR-0913, re-keyed `.cstat` per
  ADR-0942) converge by retention (a part ages out with its hour bucket at zero
  marginal cost) plus rewrite-on-touch (a fold that touches a part re-emits its
  state at the current version, so the live tail converges on its own) plus
  **a named backfill pass**: an operator-triggered rebuild fold, with the
  derived-state baseline forced to `None`, that re-lists and re-folds sealed
  parts once so they gain current-version coverage. That pass is Decision 5's
  operator-triggered migration job applied to the sealed-history tail neither
  retention nor rewrite-on-touch reaches. The live tail is Class B as written;
  only the sealed tail needs the pass.

**Obligation.** Every format ADR that binds derived state per part must name its
backfill trigger explicitly. A part-bound derived object without one is inert on
a quiescent, fully-compacted tenant — precisely the reference-corpus shape — and
the inertness reads as a slow query, not an error, so it is invisible unless the
ADR states the trigger. ADR-0913 §7 (the "one maintenance pass that forces a
rebuild fold over unchanged parts" for `.magg`) and ADR-0942 (the
operator-triggered stats-rebuild pass that forces the `.cstat` baseline to
`None` and re-folds sealed parts) are the two format ADRs that already satisfy
this obligation.

This amendment corrects only the Class B convergence claim. Classes A, C, and D,
decision 1's readers-before-writers rule, and decision 3's format-floor
mechanism are unchanged.
