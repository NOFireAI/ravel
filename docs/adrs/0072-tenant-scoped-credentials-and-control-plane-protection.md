# ADR-0072: Tenant-scoped credentials and control-plane write protection

Status: Accepted

## Context

ADR-0055 scoped storage credentials per role (Gateway / Query / Maintain /
Admin), enforced at the S3/MinIO IAM layer. That closed the "one
credential does everything" finding for processes, but the per-tenant
half of ADR-0055 was never designed: a leaked Maintain-role
credential can still read every tenant's objects, and any credential
with write access to `sys/*` or `t/<hash>/<sig>/prov` can roll back or
destroy control-plane state. The shared-credential blast radius is the
top surviving reason Ravel is not approved for hostile multi-tenant
deployments.

Three facts discovered while researching this ADR constrain the design:

1. **A per-tenant data-plane credential cannot replace the role
   credential.** Tenant discovery is one `list_delimited("t/")` from
   Maintain, Gateway and Query (`crates/ravel-maintain/src/discover.rs`),
   fleet query admission lives at bucket root (`admission/query/<pid>`),
   and `sys/*` is read by every role. Every process needs a
   tenant-agnostic credential no matter what; per-tenant credentials
   would be additive plumbing on top, with a per-tenant mint/refresh
   lifecycle Ravel has no machinery for. `S3Config` cannot even express
   a temporary credential today: it has no session-token field.
2. **Cryptographic scoping already has a designed, unwired mechanism.**
   ADR-0062's `KmsRoutingStore` routes tenant-prefixed PUTs to
   per-tenant SSE-KMS keys. Once wired (ADR-0062), a role credential
   without `kms:Decrypt` on a tenant's key cannot read that tenant's
   plaintext even though S3 `GetObject` succeeds at the IAM layer. That
   converts the cross-tenant-read blast radius from "every tenant's
   data" to "ciphertext plus metadata" without any credential
   lifecycle machinery.
3. **In-process WORM enforcement is impossible on the current store
   trait.** `object_store` 0.14 exposes no Object Lock or versioning
   API, and opening a direct-SDK side channel was explicitly rejected
   by ADR-0042/ADR-0055. The conformance probe
   (`crates/ravel-object-store/src/conformance.rs`) can report
   Enabled / Disabled / Unknown, and today it is informational only:
   a bucket with no Object Lock and no versioning boots cleanly, and
   `sys/tenancy` deletion bricks the deployment.

Two defects found in the shipped artifacts fold into this ADR's scope
because fixing them changes the same templates and docs:

- The ADR-0055 IAM policy templates in `docs/guides/operations.md`
  condition `s3:ListBucket` on prefixes like `t/*/*/l0/*`. Under AWS
  `StringLike`, a delimited listing of the bare `t/` prefix (tenant
  discovery) matches none of them: applying the shipped Gateway, Query,
  or Maintain policy verbatim breaks tenant discovery, fold, and cache
  warm. Nothing tests the templates.
- ADR-0062 section 2d writes the audit prefix as `t/<hash>/u/0/**`;
  real audit keys are `t/<hash>/u/<l0|c|l1>/<shard:04>/...`. An IAM
  prefix transcribed from the ADR matches nothing.

Separately, operator token revocation was reported lost across
restarts. The code says the premise is narrower and worse: `sys/auth`
(the durable token map) has **no writer in any shipped
binary** -- `upsert_token` / `remove_token` have zero production
callers, the operator does not depend on `ravel-catalog`, and
operator-managed deployments hardcode the unkeyed tenant hash, which
disables the durable resolver entirely. Revocation cannot be "lost";
it never durably happens.

## Decision

Tenant isolation becomes cryptographic; control-plane protection
becomes a startup-checked bucket contract; the durable auth map gets an
owner and a revoke-by-tenant primitive; the shipped IAM templates get
fixed and tested. Per-tenant *data-plane credentials* are rejected.

![Trust boundaries after ADR-0072](assets/0072-trust-boundary.svg)

### 1. `S3Config` learns temporary credentials

`S3Config` gains `session_token: Option<String>` (env
`RAVEL_S3_SESSION_TOKEN`, flag `--s3-session-token`), passed through to
the S3 client, plus optional `credentials_file: Option<PathBuf>`
(`--s3-credentials-file`): a JSON file holding
`{access_key_id, secret_access_key, session_token}` re-read when it
changes on disk, so an external process (Kubernetes secret rotation, an
STS sidecar, IRSA-style mounting) can rotate short-lived role
credentials without a restart. Ravel itself never calls STS: the
rejected credential-broker stance of ADR-0055 stands. This decision
only makes externally minted short-lived credentials expressible.

### 2. Per-tenant isolation is delivered by key custody, not credentials

ADR-0062 wires `KmsRoutingStore` into the single store construction
site (`services/ravel-server/src/store.rs`) behind
`--tenant-kms-config`, with epoch-0 bootstrap per
`crates/ravel-catalog/src/key_epoch.rs`. This ADR adds the posture
statement that work amends the guides with: in a hostile multi-tenant
deployment, each tenant's KMS key policy grants decrypt to the Ravel
role principals only for that deployment, so a leaked role credential
alone yields ciphertext; compromise requires the credential *and* KMS
grants. The operations guide gains the corresponding key-policy
template next to the IAM role templates.

### 3. Bucket protection contract, checked fail-closed at startup

`docs/object-store-contract.md` gains a normative "Required bucket
configuration" section (absorbing ADR-0064 section 7): versioning
guidance, the two sanctioned lifecycle rules, and Object Lock
(compliance mode, per-key-class retention guidance) on the protected
prefixes -- `sys/*`, `t/*/*/prov`, commit records `t/*/*/c/*`, and
`t/*/catalog/*/*` HEAD history.

`ravel-server` gains `--require-bucket-protection` (default off for
compatibility; the operator sets it for production profiles). At
startup the existing probes run; `ObjectLockStatus::Disabled` or a
versioning misconfiguration is then fatal, `Unknown` (a backend whose
probe cannot answer, e.g. plain MinIO without lock support) logs a
single loud warning and sets `ravel_bucket_protection_unknown 1` so
fleets alarm on it. Enforcement stays at the bucket/IAM layer per
ADR-0042; this decision makes silently unprotected production
deployments impossible, not lock semantics in-process.

### 4. `sys/auth` gets an owner; revocation gets a durable primitive

- `ravel-catalog` gains `remove_tokens_by_tenant(tenant_id)` (and a
  `replace_tenant_tokens` companion) so revocation needs no plaintext:
  entries carry the tenant ID in the clear, only the token is a keyed
  hash (the requested primitive).
- `ravel-cli` gains `tenant token upsert|revoke|list` (Admin role),
  making the CLI the first shipped writer of `sys/auth`.
- `services/ravel-operator` takes a `ravel-catalog` dependency and
  reconciles `sys/auth` from the CRD token Secret: upsert on new
  values, `remove_tokens_by_tenant` for tenants absent from the Secret.
  Because removal is by tenant name, it is correct after an operator
  restart with no in-memory history. The operator also stops
  hardcoding `--tenant-hash-unkeyed` when the CRD carries a deployment
  key, so `DurableBearerResolver` actually constructs on managed
  clusters (prerequisite for any of this to matter; the migration story
  is tracked separately).

### 5. Fix and test the shipped IAM templates

`operations.md` templates get corrected so every role that performs
tenant discovery may list the bare `t/` prefix (delimited, keys never
readable beyond the role's read grants), and the ADR-0062 audit-prefix
misstatement is corrected in place with an amendment note. A new test
target validates the JSON templates structurally: every prefix pattern
in the policies must match at least one key shape produced by
`ravel-commit`'s key constructors, and the discovery listing must be
admitted for Gateway, Query, and Maintain -- so a template edit that
breaks a real key shape fails CI instead of a production deployment.

## Rejected alternatives

- **Per-tenant data-plane credentials (STS-per-tenant).** Every process
  still needs a tenant-agnostic credential for discovery, `sys/*`, and
  admission (facts above), so per-tenant credentials add a mint/refresh/
  cache lifecycle and a second failure mode on every request path while
  removing no required grant from the role credential. Decision 2
  achieves the isolation goal (leaked role credential cannot read
  cross-tenant plaintext) with machinery that already exists.
- **In-process authorization layer.** Re-rejected per ADR-0055: it
  guards only compromised-credential-outside-Ravel paths when the IAM
  layer already does, and it cannot guard a compromised process.
- **Direct-SDK Object Lock side channel.** Re-rejected per ADR-0042:
  it forks the store abstraction for one call. The probe + fail-closed
  flag reaches the same operational outcome (no silently unprotected
  fleet) without it.
- **Operator-side plaintext token cache made durable:** persists secrets
  the design deliberately never persists; revoke-by-tenant needs no
  plaintext at all.

## Consequences

- A leaked single-role credential in a KMS-routed deployment yields
  ciphertext for other tenants' data objects; control-plane rollback
  and deletion require defeating bucket versioning/Object Lock, which
  `--require-bucket-protection` guarantees is configured on fleets
  that opt in. The residual risk is a leaked Admin credential plus KMS
  grants, which is the platform-operator trust boundary, not a Ravel
  one.
- Revocation becomes durable and restart-safe on operator-managed
  clusters; the static `--tenant-token` path keeps its documented
  reconcile-latency window (up to 300 s).
- `MemoryStore`/`FaultStore` and MinIO-without-lock deployments run
  unchanged (probe answers Unknown; flag stays off in dev profiles).
- The IAM templates become tested artifacts; future key-layout changes
  that invalidate a policy prefix fail CI.
- Deliberately out of scope: per-tenant credentials (rejected), rekey
  migration for unkeyed-hash buckets (tracked separately),
  audit-log export off-bucket.

## Amendment: `sys/auth` entries get an ownership marker

Decision 4's `remove_tokens_by_tenant` reconcile was unsafe as shipped:
the operator's remove pass ran over every tenant absent from its Secret,
including a tenant `ravel-cli tenant token upsert` had provisioned by
hand and the operator never managed. A hash-only `sys/auth` entry carries
no plaintext to recover, so that revocation was unrecoverable, not just
wrong.

`TokenHashEntry` (proto/ravel/sys.proto) gains field 3,
`optional string managed_by`: additive, new field number, no existing
field renumbered. `"operator"` marks an entry the operator's reconcile
loop wrote from a `tenantTokensSecretRef` Secret; `"cli"` marks one
`ravel-cli tenant token upsert` wrote (the CLI's default, overridable
with `--managed-by` for an operator-adjacent workflow that wants a
different tag). Absent is unmanaged: every entry a pre-amendment writer
ever wrote, and any entry a post-amendment writer creates without
declaring an owner.

The operator's remove/replace pass is scoped to `managed_by == "operator"`
only. A CLI-provisioned tenant and an unmanaged (absent-marker) tenant
both survive a reconcile whose Secret does not name them; an
operator-managed tenant absent from the Secret is still revoked. This is
the load-bearing compatibility rule, not the field's mere presence: an
operator that filtered on "absent OR operator" instead would still wipe
every pre-amendment entry on its next reconcile.

`AUTH_TOKEN_MAP_FORMAT_VERSION` moves 1 -> 2. The bump is a floor signal,
not a wire necessity -- `managed_by` is `optional` and additive, so a
version-1-only reader already skips the unknown field safely, and a v2+
reader accepts a stored 1 or 2 unconditionally (an absent marker decodes
identically either way: unmanaged). Every writer now writes 2.

Also fixed in the same change: `replace_tenant_tokens` compares the
tenant's resulting entry set against its current one and returns
`SetOutcome::Unchanged` without writing when they match (it previously
rewrote the whole map every call), and the operator wraps each `sys/auth`
primitive call in a bounded CAS retry and no longer aborts Deployment/
Service reconciliation on a sys/auth failure -- both were reconcile-loop
defects the ownership marker didn't by itself fix. See PROGRESS.md for
the full defect list.

## Amendment: `token_hash` is globally unique; last writer takes ownership

A follow-up review found the ownership marker above could itself brick
`sys/auth`: three doors all end with two entries sharing one `token_hash`
persisted in the same object. `decode_map` already refuses to decode a
duplicate hash (it always has), so once such an object exists, every
subsequent read of `sys/auth` -- and therefore every `ravel-server`'s
bounded-staleness refresh -- fails closed, deployment-wide, until the
object is hand-repaired out of band.

- Door 1: an unmanaged/v1 entry for `(tenant, token)`, then an
  operator-scoped `replace_tenant_tokens(tenant, [token], Some("operator"))`
  for the same pair. The old scoped `retain` only dropped entries matching
  `(tenant_id, managed_by)` exactly; the unmanaged entry's absent marker
  never matches `Some("operator")`, so it survived, and `extend` appended a
  second entry with the identical hash.
- Door 2: `upsert_token_owned` matches purely on `token_hash` and overwrites
  `managed_by` in place -- correct, hash-keyed takeover semantics on its
  own -- but a CLI upsert of an operator-owned token flips that hash's
  owner to `"cli"`, and the operator's next scoped replace no longer sees
  it as its own (same bug as door 1, triggered by a legitimate ownership
  change instead of a pre-amendment entry).
- Door 3 (pre-existing, predates the ownership marker): two tenants whose
  Secret-provisioned token values collide, converged one tenant at a time
  by the operator's per-tenant reconcile loop.

**Decision: `token_hash` is unique across the whole map, globally, not
just within a `(tenant_id, managed_by)` scope. The last writer of a given
hash takes ownership of it outright** -- `replace_tenant_tokens` now drops
any pre-existing entry whose hash collides with a desired entry before
extending, regardless of that entry's tenant or owner, in addition to its
existing same-scope drop. Takeover was chosen over refusing a
cross-scope collision outright because it matches `upsert_token_owned`'s
existing hash-keyed semantics (already shipped, already the CLI's
behavior) and never bricks a legitimate migration -- reusing a token
value across a `managed_by` change, or (door 3) two tenants that happen
to share a token value, converges to a single readable entry rather than
failing the reconcile. The tradeoff: door 3's two tenants sharing one
token value is inherently ambiguous (which tenant does the token
authenticate as is undefined by the input), and takeover resolves it to
"whichever call ran last" rather than surfacing it as a caller error --
acceptable because a shared token value across tenants is an input
defect the deployer controls (the Secret), not a value this module can
validate against other tenants' plaintexts without ever storing
plaintext.

**Superseded in part:** the door 3 "takeover" call above turned out wrong
in practice -- see the amendment below, "cross-tenant token collisions
are refused, not taken over." Global hash-uniqueness and the same-tenant
takeover (doors 1 and 2) still stand; only the cross-tenant branch changed.

Belt-and-suspenders: `write_map`, the one function every `sys/auth`
write funnels through, now runs the same duplicate-`token_hash` check
`decode_map` runs on read, and refuses (before issuing any store call) a
map that would leave two entries sharing a hash. This makes door 3 safe
even for a future write path that does not itself de-duplicate by
hash: it fails the write with a typed `Corrupt(DuplicateTokenHash)`
error instead of silently persisting an object no one can read back.

### `AUTH_TOKEN_MAP_FORMAT_VERSION` stays unconditional, and what that means for upgrade order

The proto doc for `AuthTokenMap.format_version` previously read "= 1 for
a map with no entry carrying `managed_by`; ... always writes 2 going
forward" -- self-contradictory, and not what the code (`auth_token_map.rs`,
`AUTH_TOKEN_MAP_FORMAT_VERSION = 2` unconditionally) does. Resolved by
keeping the code's behavior and correcting the doc: every writer built
after this amendment stamps `format_version = 2` on every write, never 1,
regardless of whether any entry in the resulting map actually carries
`managed_by`. Content-dependent versioning was rejected: it would make
the stamped version depend on which other entries happen to be in the map
at write time (added, then removed, then re-added by an unrelated
tenant), a second source of non-obvious behavior on top of the takeover
rule above.

Consequence for upgrade order: `decode_map`'s guard
(`proto.format_version > AUTH_TOKEN_MAP_FORMAT_VERSION`) means a
pre-amendment `ravel-server` build (`AUTH_TOKEN_MAP_FORMAT_VERSION == 1`)
refuses a stored `format_version = 2` object outright -- fail-closed, the
same as any other future-format guard in this codebase, not a misread.
Because the stamp is unconditional, this triggers on the *first* write any
amended writer (the operator, or `ravel-cli tenant token upsert`) makes to
`sys/auth`, even one with zero `managed_by`-carrying entries. Every
`ravel-server` in a deployment must therefore be upgraded to a build that
understands `format_version = 2` *before* any amended writer's first
`sys/auth` write, not merely before the first `managed_by`-tagged entry
appears -- a lagging old server otherwise loses `sys/auth` entirely (its
bounded-staleness refresh fails closed) until it, too, is upgraded.

## Amendment: cross-tenant token collisions are refused, not taken over

The door 3 takeover decision above does not converge. Two tenants (say
`acme` and `globex`) whose Secret-provisioned token values collide are
both reconciled by the operator's per-tenant loop, in the same order,
every cycle. Under takeover, whichever of the two runs last in a given
cycle wins the entry -- and then the OTHER tenant's turn comes on the
very next cycle, takes it back, and returns `Updated`. The two tenants
fight over the one hash forever: every reconcile cycle issues a
`sys/auth` PUT, and every PUT flips which tenant the shared token
authenticates as. This is worse than the bug the takeover decision was
written to fix -- doors 1 and 2 converge to a stable single entry after
one takeover; door 3 under takeover never converges at all.

**Decision, corrected: distinguish the two collision shapes instead of
handling them alike.**

1. SAME `tenant_id`, different `managed_by` (doors 1 and 2 -- a cli to
   operator migration, or an unmanaged pre-amendment entry meeting a
   scoped replace for the first time): takeover, unchanged from the
   amendment above. Re-tagging `managed_by` in place is safe because the
   token continues to authenticate the same tenant; there is nothing to
   arbitrate.
2. DIFFERENT `tenant_id`, same `token_hash` (door 3): refuse the write
   outright with a typed `AuthTokenMapError::CrossTenantTokenCollision {
   token_fingerprint, existing_tenant, attempted_tenant }`, returned
   before any store call. Never take the token over.

The reason takeover is wrong specifically for door 3, and only for door
3: `tenant_for_token` is a lookup function, `token_hash -> tenant_id`.
Doors 1 and 2 change who manages an entry that already resolves to one
tenant -- the function stays well-defined throughout. Door 3 asks the
function to return two different answers for the same input depending on
which tenant last called `replace_tenant_tokens` -- there is no
deterministic tenant for that token to resolve to, and no amount of
takeover ordering makes one exist. Refusing is the only choice that
keeps `tenant_for_token` a function: the ambiguity is in the deployer's
input (the Secret), not something this module can resolve on the input's
behalf, so the correct behavior is to surface it as a caller-visible
error, not to silently arbitrate it every cycle.

This is applied at every writer that can create a token_hash collision,
not only `replace_tenant_tokens`: `upsert_token` and `upsert_token_owned`
now refuse a hash already owned by a different tenant the same way (they
previously re-pointed it, silently changing which tenant the token
authenticated as -- the single-call analogue of the same bug). A caller
that genuinely wants to move a token from one tenant to another calls
`remove_token` (or `remove_tokens_by_tenant`) for the old tenant first,
then `upsert_token` for the new one -- two calls, an explicit revoke
in between, never an implicit re-point through one.

Convergence for the operator's reconcile loop specifically: tenant ids
are reconciled in a fixed order (`BTreeMap<String, Vec<u8>>` iteration,
alphabetical), so on the cycle that first creates the hash, the
lexicographically-first colliding tenant's write runs first and
succeeds; every later tenant sharing that hash, that cycle and every
cycle after, hits `CrossTenantTokenCollision` and is skipped. The
winning tenant's own subsequent cycles hit `replace_tenant_tokens`'s
`current_hashes == desired_hashes` fast path (`SetOutcome::Unchanged`,
zero store calls) as long as its desired token set does not change. Both
outcomes are pure functions of persisted map state plus reconcile order,
so the result is deterministic, and steady state is zero `sys/auth`
writes from either tenant -- the flap is gone. A `CrossTenantTokenCollision`
in the operator's per-tenant loop logs the fingerprint and both tenant
ids (never the token) and skips that tenant for the cycle; it does not
abort the reconcile pass, so a healthy tenant's tokens and every
workload's Deployment/Service reconciliation still proceed.

The `write_map` duplicate-hash guard from the amendment above is
unchanged and stays as belt-and-suspenders: with the refusal now sitting
in front of every writer, no code path should ever again attempt to
persist two entries sharing a hash, but the guard remains the backstop
that turns a future writer's oversight into a typed decode-time refusal
instead of a silent brick.
