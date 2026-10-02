# IAM policy templates

Four per-role templates: `gateway.json`, `query.json`, `maintain.json`,
`admin.json`. Replace `my-ravel-bucket` and the KMS key placeholder before
use. `crates/ravel-commit/tests/iam_templates.rs` pins the resource and
action set of every statement in all four files; a hand edit that drifts
from the pinned shape fails that suite.

## Commit records (`t/*/*/c/*`) are deletable by design

`DenyDeleteProtected` in `maintain.json` denies delete on `sys/tenancy`,
`sys/qualification`, `sys/gc`, `t/*/*/prov`, `t/*/catalog/*/HEAD`, the
legal-hold audit shard (`t/*/u/*/0000/*`), the durable token map `sys/auth`,
the write-once recovery manifests `sys/t/*`, the append-only KMS key-epoch
records `t/*/enc`, and the Parquet location grants records `t/*/pq/grants`.
Commit records are absent from
that list on purpose: `MaintainDelete` grants delete on `t/*/*/c/*`
because the maintenance sweep physically removes a commit record once
it is superseded, and an IAM deny there would make every sweep pass fail.

The other three templates (`gateway.json`, `query.json`, `admin.json`) deny
the whole catalog family, `t/*/catalog/*/*`, instead: none of those roles
deletes a catalog object, so nothing narrower is needed there. Maintain is
the one role where narrowing to `HEAD` alone is load-bearing.

All four templates also deny delete on `sys/auth`, `sys/t/*` and `t/*/enc`: no
role deletes any of them on its normal path, a deleted `sys/auth` reads as
absent and installs an empty token map that revokes every durable token,
`sys/t/*` recovery manifests are write-once (ADR-0050), and a deleted
`t/<hash>/enc` reads as "no per-tenant key was ever configured", so
`verify-custody` stops checking the tenant and the next startup rewrites its
epoch history from scratch. They deny delete on `t/*/pq/grants` too: nothing
deletes a Parquet location grants record, and a deleted one reads as a tenant
with no grants, so every query over that tenant's Parquet tables is refused
until each location is granted again.

Catalog snapshot and index objects (`t/*/catalog/*/snap/*`,
`t/*/catalog/*/idx/*`) used to be caught by the same `t/*/catalog/*/*`
pattern that also covers `HEAD`, which put them behind this deny too. That
was a bug (issue #1847): the unreferenced-catalog sweep runs under the
Maintain role and deletes exactly those superseded snapshot and index
objects, so the shipped templates refused every sweep delete outright
rather than merely delaying it, and catalog garbage was never reclaimed.
The deny above is narrowed to the `HEAD` pointer alone -- the object the
sweep never deletes -- and `MaintainDelete` below now grants delete on
`snap/` and `idx/` to match what the sweep already does.

This is a separate list from the Object Lock compliance-mode prefixes in
`docs/object-store-contract.md`'s "Required bucket configuration" section,
which does include commit records: they get bucket-layer, per-object
retention, exactly the same as the other protected prefixes. That retention
protects object versions, not the key. A compromised credential that sends a
`DeleteObjects` or a PUT with no version id still succeeds: on the versioned
bucket Object Lock requires, the delete adds a delete marker and the PUT
adds a new current version. The locked version stays in storage, and
recoverable, until its retain-until. The two lists disagree on commit
records for a reason, not by accident: IAM's `DenyDeleteProtected` blocks
Maintain's own role from ever deleting the prefix, which would break the
sweep; Object Lock's per-object retention does not refuse the sweep's
delete at all. Ravel's delete names no version id, so on a locked commit
record it lands as a delete marker and the sweep carries on as it would on
an unlocked one; it does not pause. What the retention extends is how long
the locked version physically remains: it is removed once its retain-until
has passed and the noncurrent-version expiration rule has fired. See
`docs/object-store-contract.md`'s "Required bucket configuration" section
for the full retention/GC interaction and the physical-removal bound.

## Delete grants per role

Each template's delete authority, read from its `Allow` statements. Every
template also carries the shared `DenyDeleteProtected` deny listed above; the
grants below are what remains deletable after that deny applies.

- **Gateway** (`gateway.json`): `GatewayAdmissionDelete` grants
  `s3:DeleteObject` on `t/????????????????????????????????/?/admission/*`
  only, and the gateway deletes no durable object. Each ingest process
  overwrites its own per-signal admission snapshot
  `t/<tenant_hash>/<signal>/admission/<process_id>.snapshot`, and the
  admission reconcile (`reap_keys`, `crates/ravel-ingest/src/reconcile.rs`)
  deletes the snapshots of processes past the reap horizon. A snapshot is
  mutable per-process state, rewritten every reconcile interval, not data.
  No `Deny` in `gateway.json` covers the prefix. The pattern uses IAM's
  single-character `?` because `*` matches across `/`: `t/*/*/admission/*`
  would also match `t/<tenant_hash>/pq/t/admission/v/<version>.pqm`, the
  manifests of a Parquet table named `admission`. A tenant hash is 32 hex
  characters and a signal prefix one, so the `?` form matches every snapshot
  and no manifest. The read, write and list grants on `t/*/*/admission/*`
  and on other key segments (including `c`, `l0`, `l1`, `idem`, `maint`,
  `u`, `catalog`, `del` and `a`) keep the cross-`/` match and still reach a
  Parquet table with that name, until those table names are reserved.
- **Query** (`query.json`): no delete grant at all, and nothing on the query
  path deletes. A draining query worker overwrites its own
  `sys/query/workers/` record with a stamp no reader accepts as live, and the
  maintain role reaps dead records (issue #1828).
- **Admin** (`admin.json`): `AdminQualifyDelete` grants delete on
  `sys/qualify/*`, and `AdminProbeDelete` on `sys/pq-probe/*`. Both are
  scratch: the qualification run's objects, and the one object the Parquet
  bucket probe writes and deletes before it returns.
- **Maintain** (`maintain.json`): `MaintainDelete` grants delete on
  `t/*/*/l0/*`, `t/*/*/c/*`, `t/*/*/l1/*`, `t/*/*/idem/*`, `t/*/u/*/0001/*`,
  `t/*/*/del/*.dreq`, `t/*/catalog/*/snap/*`, `t/*/catalog/*/idx/*`,
  `sys/maintain/workers/*`, `sys/query/workers/*`,
  `quarantine/t/*/*/l0/*`, and `t/*/pq/t/*`. These are the objects the
  compaction, supersession, retention, erasure-request, unreferenced-catalog,
  dead-worker reap, quarantine-reaper and Parquet manifest sweeps physically
  remove. The Parquet manifest sweep is `ravel-cli parquet sweep`, run under
  this credential. The query-worker reap
  runs on the maintain process that owns a fixed rendezvous unit (one per view
  of the maintain membership) and judges each key by its LIST
  metadata, so `MaintainList` also carries `sys/query/workers/*` and
  `MaintainRead` does not.
  The catalog half of that list also needs reads, which are easy to miss
  because two of the three fail silently rather than refusing the pass:
  `MaintainRead` carries `t/*/catalog/*/HEAD` (the sweep resolves what is
  referenced), `t/*/catalog/*/snap/*` (the reachability pass GETs every
  part HEAD names, and an AccessDenied there aborts the whole pass for
  that signal), and `t/*/catalog/*/idx/*` (the scrub tick's covering-
  postings read, which returns "no postings" on a denial and so disables
  the postings scrub tier without an error). `MaintainList` carries the
  two catalog prefixes, without which the sweep is refused at its first
  `ListBucket`.

### Erasure-request objects: the three grants the `.dreq` sweep needs

ADR-0064 section 6 grants Maintain three things on the erasure keyspace, in
two sentences of the same bullet: "Query and Maintain gain read on `del/**`
(resolve-time listing; pass scoping)", and "Maintain gains delete on
`del/*.dreq` **only**" with "`del/*.done` joins the deny-delete set for every
role including Maintain".

The erasure-request sweep is `sweep_erasure_requests` in
`crates/ravel-maintain/src/sweep.rs`. It runs under the Maintain role and
retires a request object at
`t/<tenant_hash>/<signal>/del/<request_id>.dreq` once its erasure is complete,
past the post-completion protection horizon, and no longer held by a legal
hold or a still-resolvable superseded input. The lifecycle makes six
object-store calls on the `del/` prefix, across two roles, and each needs its
own grant. The set is derived from every call site that touches the prefix,
not from the sweep alone: scoping it to one function is how an earlier draft
shipped a delete grant whose own listing was refused.

| Call | S3 operation | Grant |
|---|---|---|
| `erase.rs` `store.put(&key, ...)` (`ravel-cli erase submit`) | `s3:PutObject` | `AdminWrite` `t/*/*/del/*.dreq` |
| `erasure_rewrite.rs` `store.get(&key, GetRange::Full)` on each pending `.dreq` | `s3:GetObject` | `MaintainRead` `t/*/*/del/*` |
| `maintain.rs` `write_erasure_completion` | `s3:PutObject` | `MaintainWrite` `t/*/*/del/*.done` |
| `sweep.rs` `list_all(store, &keys::del_prefix(tenant, signal))` | `s3:ListBucket` with `prefix=t/<tenant_hash>/<signal>/del/` | `MaintainList` `s3:prefix` `t/*/*/del/*` |
| `sweep.rs` `store.get(&meta.key, GetRange::Full)` on each listed `.done` | `s3:GetObject` | `MaintainRead` `t/*/*/del/*` |
| `sweep.rs` `store.delete(dreq_key)` | `s3:DeleteObject` | `MaintainDelete` `t/*/*/del/*.dreq` |

IAM is default-deny, so no grant here is useful on its own. The sweep is
refused with `AccessDenied` on the `ListBucket` before it reaches a single
request object, which makes the delete grant unreachable; and without the
`.done` write the sweep's completion lookup always misses, so every request
counts as still pending and none is ever deleted.

Two scopes are deliberately narrower than the ADR writes them. The delete is
`*.dreq` and not `del/*`, because completion records are permanent erasure
evidence and no role may delete them. `AdminWrite` is `t/*/*/del/*.dreq` where
ADR-0055's role table writes `t/*/*/del/*` (docs/adrs/0055-storage-credential-scoping.md:639-640),
because `ravel-cli erase submit` writes only the `.dreq`; the `.done` is
written by Maintain. Both are tightenings, recorded here so neither reads as
drift against the ADR. The read is `del/*` as the ADR writes it, because both object
shapes under the prefix are fetched — the `.done` by the sweep, the `.dreq` by
the rewrite pass.

`maintain_template_covers_every_erasure_request_sweep_call` in
`crates/ravel-commit/tests/iam_templates.rs` asserts the three `sweep.rs` rows
against the key constructor each call uses, and asserts that no pattern
reaching `del/` reaches anything outside it.
`erasure_lifecycle_calls_outside_the_sweep_are_reachable` asserts the other
three rows the same way.
`maintain_template_grants_delete_on_erasure_requests` asserts the `.done` and
`del/`-prefix exclusions on the delete side, and
`every_role_grants_exactly_the_expected_pattern_set` pins every pattern string
by exact equality.

An operator who applied a copy of `maintain.json` or `admin.json` older than
these grants must re-apply it. Re-applying restores the lifecycle: `ravel-cli
erase submit` can write a `.dreq`, the rewrite pass can read it and write the
matching `.done`, and the sweep then lists the prefix, reads each completion,
and deletes every request object whose protection horizon has elapsed and that
no hold retains, so the backlog drains over the passes that follow rather than
instantly. Requests whose horizon has not elapsed, or that a legal hold or a
still-resolvable superseded input holds, are kept by design and are not part of
that backlog. A partial re-apply clears nothing: a copy carrying the delete but
not the list is refused at the `ListBucket`, and one carrying list, read and
delete but not the `.done` write leaves every request looking still-pending, so
the sweep deletes none of them.

One known gap remains open and is NOT closed by the grants above. It is
tracked separately and is not created by this change.

- `query.json` grants no list or read under `del/`, while the resolver LISTs
  `t/<tenant_hash>/<signal>/del/` per resolve to attach pending predicates
  (`crates/ravel-commit/src/keys.rs`, `del_prefix`). ADR-0064's same bullet
  gives Query that read.

### Quarantined orphans: the three grants the quarantine lifecycle needs

ADR-0058 decision 6 has the orphan sweep quarantine an orphaned L0 data object
instead of deleting it: copy first, delete the original second, and physically
remove the copy only after a second horizon (`quarantine_horizon_ns`, 7 days by
default) has elapsed. The copy lives in a TOP-LEVEL `quarantine/` key space,
not under `t/`, so none of the tenant-scoped patterns above reaches it.

Both halves run under the Maintain role, in
`crates/ravel-maintain/src/sweep.rs`: `sweep_orphans` phase (d) makes the copy
through `quarantine_object`, and `sweep_quarantine` is the reaper. The
lifecycle makes five object-store calls, and the set below is derived from all
five rather than from the reaper alone; two of them land on the live key and so
need no new pattern.

| Call | S3 operation | Grant |
|---|---|---|
| `quarantine_object` `store.get(src, GetRange::Full)` on the live orphan | `s3:GetObject` | `MaintainRead` `t/*/*/l0/*` (already present) |
| `quarantine_object` `store.put(dest, ...)` on `quarantine_key(original, ns)` | `s3:PutObject` | `MaintainWrite` `quarantine/t/*/*/l0/*` |
| `sweep_orphans` `store.delete(&meta.key)` on the live orphan, after the copy | `s3:DeleteObject` | `MaintainDelete` `t/*/*/l0/*` (already present) |
| `sweep_quarantine` `list_all(store, &quarantine_l0_data_prefix(...))` | `s3:ListBucket` with `prefix=quarantine/t/<tenant_hash>/<signal>/l0/<shard>/` | `MaintainList` `s3:prefix` `quarantine/t/*/*/l0/*` |
| `sweep_quarantine` `store.delete(&meta.key)` on the quarantined copy | `s3:DeleteObject` | `MaintainDelete` `quarantine/t/*/*/l0/*` |

No role is granted `s3:GetObject` or a `quarantine/` list prefix outside
Maintain. No *code* path reads a quarantined object back: the reaper decides
from the key alone (`parse_quarantine_timestamp` and
`original_key_from_quarantine` both parse the key, and the hold check reads the
lease, not the object).

**A restore path does exist, and no shipped template authorizes it.**
`docs/guides/operations/troubleshooting.md` step 2 has an operator list
`quarantine/t/<tenant_hash>/` recursively, GET each object, and copy it back
to the live key stripped of the `quarantine/` prefix and the `/q<ns>` suffix.
It is a documented human procedure rather than a `ravel-cli` command -- the
runbook says so outright ("There is no `ravel-cli` command for this yet") --
which is exactly why deriving grants from code call sites alone missed it. A
runbook is a call site.

That grant is deliberately NOT added here. It belongs in `admin.json`, it
widens an operator role's reach over a keyspace holding data that was
quarantined rather than deleted, and it deserves its own review rather than
riding along with the Maintain fix. Until it lands, an operator following
that runbook must use credentials outside these templates. Tracked in
issue #1978.

IAM is default-deny, so no grant here is useful on its own. Without the write
the copy is refused and the sweep quarantines nothing, which is where a
template predating this change stops; the original is deleted only after the
copy succeeds, so nothing is lost, but nothing is reclaimed either. Without the
list the reaper is refused at its first `ListBucket` and never sees a copy,
which makes the delete unreachable. Without the delete the reaper lists copies
it can never remove, and the quarantine grows for the life of the deployment.

`maintain_template_covers_every_quarantine_call` in
`crates/ravel-commit/tests/iam_templates.rs` asserts all five rows against the
template's **Allow** patterns, with witness keys built from the same key
constructors the calls use rather than from hand-written strings. It also pins
the top-level premise (no pattern outside `quarantine/` reaches a quarantined
copy, and no other role's template reaches one at all) and the tightness of the
three new patterns.

It does not subtract the Deny statements, and that distinction is not
academic. Row 3 asserts the live-orphan delete over every witness
`l0_data_keys()` produces, including the legal-hold audit key
`t/<hash>/u/l0/0000/<writer>...rseg`, which `DenyDeleteProtected`'s
`t/*/u/*/0000/*` matches: an Allow reaches that key and the effective policy
still refuses the delete, which is what legal hold is for. So read the table
as "an Allow reaches this call", not as "this call succeeds". The Allow/Deny
relationship is covered separately, by
`delete_deny_and_allow_overlap_exactly_where_expected` and
`every_allow_deny_key_overlap_is_named_by_the_deny`; the reachability tests
here follow the convention `erasure_lifecycle_calls_outside_the_sweep_are_reachable`
set, which reads Allow patterns only.
`every_role_grants_exactly_the_expected_pattern_set` pins every pattern string
by exact equality.

An operator who applied a copy of `maintain.json` older than these grants must
re-apply it. Re-applying lets the orphan sweep quarantine again; each copy it
writes becomes reapable once that copy's own `quarantine_horizon_ns` elapses,
so the quarantine drains over subsequent passes rather than at once. A partial
re-apply clears nothing: a copy carrying the delete but not the list is refused
at the `ListBucket`, and one carrying list and delete but not the write never
gets an orphan into the quarantine to begin with.

Two known gaps are recorded here and are NOT closed by the grants above.

- The new delete pattern `quarantine/t/*/*/l0/*` also reaches the quarantined
  copy of a legal-hold audit object (`quarantine/t/<hash>/u/l0/0000/...`), which
  `DenyDeleteProtected`'s `t/*/u/*/0000/*` does not cover, since the quarantine
  key is not under `t/`. Nothing reaches that state today: the legal-hold audit
  shard is never swept for orphans, so no copy of one is ever written, and the
  reaper independently refuses a held key by resolving
  `original_key_from_quarantine` and checking the lease. The protection is in
  code rather than in the deny, which is a weaker posture than the live keyspace
  has.

- **The derivation above covers the S3 axis of the PUT, not the KMS axis.**
  Per-tenant KMS routing (ADR-0062) decides by the literal `t/` prefix, and a
  quarantine key is not under `t/`, so the copy is written under the
  deployment-default key rather than the tenant's. The live original is
  deleted once the copy lands, so for `quarantine_horizon_ns` (7 days by
  default) the only surviving copy of that tenant's data is encrypted under
  the wrong key, and destroying the tenant key to crypto-shred them does not
  make it unreadable. That is a defect in the quarantine lifecycle rather
  than in this template, but this template's write grant is what lets the
  write happen on a shipped deployment, so it is recorded here. Tracked in
  issue #1979.

## Control-plane keys outside the data path

Twenty control-plane keys and prefixes are read or written by a server role or
by Admin on its normal path. Admin reads all of them through its blanket `t/*`
and `sys/*` reads, so its column below lists writes and deletes only. Every
grant names the one key or prefix the calls use:

| Key | Gateway | Query | Maintain | Admin |
|---|---|---|---|---|
| `sys/tenancy` | get, put | get, put | get, put | put |
| `sys/qualification` | get | get | get | put |
| `sys/qualify/*` | | | | put, delete |
| `sys/pq-probe/*` | | | | put, delete |
| `sys/gc` | get | get | get, put | put |
| `sys/auth` | get | get | | put |
| `sys/t/*` | put | | | |
| `sys/maintain/workers/*` | | | list, get, put, delete | |
| `sys/maintain/memo/*` | | | list, get, put | |
| `sys/maintain/claims/compaction/*` | | | get, put | |
| `sys/query/workers/*` | | list, get, put | list, delete | |
| `admission/query/*` | | list, get, put | | |
| `t/*/config` | get | get | get | put |
| `t/*/enc` | get, put | get, put | get, put | |
| `t/*/m/meta` | get, put | get | | |
| `t/*/a/alert-lease` | | get, put | | |
| `t/*/a/state/latest` | | get, put | get | |
| `t/*/*/admission/*` | list, get, put | | | |
| `t/????????????????????????????????/?/admission/*` | delete | | | |
| `t/*/pq/grants` | | get | | put |
| `t/*/pq/t/*` | | list, get | list, delete | |

The maintain `get` and `put` on the three `sys/maintain/` prefixes are the one
`sys/maintain/*` grant in `MaintainRead` and `MaintainWrite`. The other rows
were derived from the data path or are covered in the sections above, except
the call groups below, which the templates missed until issues #1995, #2340
and #2350:

| Call | Mode | S3 operation | Grant |
|---|---|---|---|
| `DurableAuthState::refresh` reads `sys/auth` (startup, refresh horizon, token miss) | `gateway`, `query`, `all`, keyed bucket | `s3:GetObject` | `GatewayRead` and `QueryRead` `sys/auth` |
| `ravel-cli tenant token upsert` and `revoke` write `sys/auth` (`CreateIfAbsent`, then `CasVersion`) | Admin | `s3:PutObject` | `AdminWrite` `sys/auth` (the read is `AdminRead` `sys/*`) |
| `RecoveryManifestWriter::ensure` writes `sys/t/<tenant_hash>` (`CreateIfAbsent`) on a keyed tenant's first ingest request | `gateway`, `all` | `s3:PutObject` | `GatewayWrite` `sys/t/*` |
| `read_all_memo_snapshots` lists `sys/maintain/memo/` on a maintain warm start | `maintain` | `s3:ListBucket` with `prefix=sys/maintain/memo/` | `MaintainList` `s3:prefix` `sys/maintain/memo/*` (the GETs and the snapshot PUT fall under `MaintainRead` and `MaintainWrite` `sys/maintain/*`) |
| `read_config` reads `t/<tenant_hash>/config` (the tenant config record, ADR-0066) | `maintain`, `query`, `gateway` | `s3:GetObject` | `MaintainRead`, `QueryRead` and `GatewayRead` `t/*/config` |
| `ravel-cli typed-attr-column set`, `clustering-key set` and `clear`, and `bloom-scope set` write `t/<tenant_hash>/config` (`CreateIfAbsent`, then `CasVersion`) | Admin | `s3:PutObject` | `AdminWrite` `t/*/config` (the read is `AdminRead` `t/*`) |
| `bootstrap_tenant_epoch` reads `t/<tenant_hash>/enc`, then `record_key_epoch` writes it (`CreateIfAbsent` for epoch 0, `CasVersion` for each appended epoch), for each tenant in `--tenant-kms-config` at startup | every mode | `s3:GetObject`, `s3:PutObject` | `GatewayRead`, `QueryRead`, `MaintainRead`, `GatewayWrite`, `QueryWrite` and `MaintainWrite` `t/*/enc` (Admin reads it for `verify-custody` through `AdminRead` `t/*`) |
| The ingest metadata sink reads and writes `t/<tenant_hash>/m/meta` (`CreateIfAbsent`, then `CasVersion`); the query metadata cache reads it for `/api/v1/metadata` | `gateway`, `all` (write); `query`, `all` (read) | `s3:GetObject`, `s3:PutObject` | `GatewayRead`, `GatewayWrite` and `QueryRead` `t/*/m/meta` |
| The alert evaluator: `acquire_lease` writes `t/<tenant_hash>/a/alert-lease` (`CreateIfAbsent`, or a GET then `CasVersion`), `read_alert_state_memo` reads and `write_alert_state_memo` overwrites `t/<tenant_hash>/a/state/latest`, and each transition is published as an L0 data object and a commit record (`CreateIfAbsent`) | `query`, `all` | `s3:GetObject`, `s3:PutObject` | `QueryRead` `t/*/a/alert-lease` and `t/*/a/state/latest`; `QueryWrite` `t/*/a/alert-lease`, `t/*/a/state/latest`, `t/*/a/l0/*` and `t/*/a/c/*` |
| Alert retention: `alert_keep_set` reads `t/<tenant_hash>/a/state/latest`, and `alert_keyspace_is_empty` lists `t/<tenant_hash>/a/` and `quarantine/t/<tenant_hash>/a/` | `maintain` | `s3:GetObject`, `s3:ListBucket` | `MaintainRead` `t/*/a/state/latest`; `MaintainList` `s3:prefix` `t/*/a/` and `quarantine/t/*/a/` |
| `ravel-cli tenant parquet-grant add` and `remove` write `t/<tenant_hash>/pq/grants` through `replace_whole` (`crates/ravel-pqtable/src/grants.rs`: a GET, then a PUT with `CreateIfAbsent` or `CasVersion`) | Admin | `s3:PutObject` | `AdminWrite` `t/*/pq/grants` (the read is `AdminRead` `t/*`) |
| `parquet-grant add` qualifies the target bucket with `probe_not_ravel_bucket` (`crates/ravel-object-store/src/external/probe.rs`), which PUTs `sys/pq-probe/<32 hex chars>` and DELETEs it before returning | Admin | `s3:PutObject`, `s3:DeleteObject` | `AdminWrite` and `AdminProbeDelete` `sys/pq-probe/*` |
| The Parquet table provider (`crates/ravel-sql/src/parquet.rs`, built from `services/ravel-server/src/query.rs`) resolves a table through `crates/ravel-pqtable/src/resolve.rs`: `newest` (through `versions`) lists `t/<tenant_hash>/pq/t/<table>/v/`, `read_version` GETs the manifest, and `grants::list` GETs `t/<tenant_hash>/pq/grants` | `query`, `all` | `s3:ListBucket`, `s3:GetObject` | `QueryList` `s3:prefix` `t/*/pq/t/*`; `QueryRead` `t/*/pq/t/*` and `t/*/pq/grants` |
| `ravel-cli parquet sweep` (`crates/ravel-pqtable/src/sweep.rs`): `plan` lists `t/<tenant_hash>/pq/t/`, the CLI wrapper reads `sys/gc` for the deployment's grace floor, and `execute` deletes each superseded manifest | Maintain credential | `s3:ListBucket`, `s3:GetObject`, `s3:DeleteObject` | `MaintainList` `s3:prefix` `t/*/pq/t/*`; `MaintainDelete` `t/*/pq/t/*`; `sys/gc` is already in `MaintainRead` |
| The admission reconcile's `reap_keys` (`crates/ravel-ingest/src/reconcile.rs`) deletes the snapshots `t/<tenant_hash>/<signal>/admission/<process_id>.snapshot` of processes past the reap horizon | `gateway`, `all` | `s3:DeleteObject` | `GatewayAdmissionDelete` `t/????????????????????????????????/?/admission/*` |

The last row lists only prefixes; nothing below `t/*/a/` but the memo is a
control-plane key, and the alert data objects and commit records the evaluator
writes are data-path keys rather than control-plane ones. They are in the
table because the Query role's write grants were derived from the fold and
query-audit paths and reached neither.

Without the `sys/auth` read, every durable auth refresh fails, so durable
bearer tokens never resolve in a process that has not refreshed yet, and stop
resolving once a cached map passes its hard-stale bound.
Without the Admin write, every token upsert and revoke is refused. Without the
manifest write, ingest proceeds but the tenant has no recovery manifest and
the writer retries, and logs a warning, on every later request. Without the
memo list, every maintain cycle logs a warning and retries, and the process
runs cold until the list succeeds. Without the `t/*/config` read, `read_config`
propagates the refusal (it swallows only `NotFound`), so retention passes fail
under `maintain`, the durable `typed_attr_columns` override never resolves under
`query` and statements run on the base schema, and the config-limits refresh
fails under `gateway` so per-tenant admission overrides never apply. Without the
Admin config write, every `ravel-cli` tenant config write is refused. Without
the `t/*/enc` read or write, a process started with `--tenant-kms-config`
refuses to start, in whichever mode it runs: `read_record` passes only
`NotFound` through as absence. Without the `t/*/m/meta` grants, metric metadata
is never persisted and `/api/v1/metadata` serves nothing; both sides log the
refusal and carry on. Without the alert lease write, every evaluator tick
reports the lease unavailable and evaluates no rule, and without the transition
writes every rule that changes state fails. Without the maintain memo read,
alert retention is skipped for the tenant every tick, and without the two list
prefixes the retention gate logs a warning for every tenant with no alert
history and runs the orphan sweep anyway. Without the grants record write,
every `parquet-grant add` and `remove` is refused, and without the probe PUT
every `add` is refused before it writes; without the probe delete, each probe
leaves its object behind. Without the Query list and reads, every Parquet table
query is refused. Without the Maintain list, `parquet sweep` is refused before
it sees a manifest, and without the delete every superseded manifest stays.
Without the gateway's admission delete, each reap is refused and logged, and
dead processes' snapshots accumulate under the prefix every reconcile lists.

No server role may write `sys/auth`: the only production writers are
`ravel-cli` under Admin and the operator, which uses the shared credential
named by `spec.storage.s3.credentials_secret_ref` rather than any of these
templates. The tenant config record `t/<tenant_hash>/config` is likewise
written only by `ravel-cli` under Admin, never by a server role. The Maintain
role reads the alert state memo and writes neither it nor the lease, and no role
deletes either; nothing releases the lease. No `ravel-cli` command writes
`t/<tenant_hash>/enc`, and nothing lists it.

`sys/auth`, `sys/t/*` and `t/*/enc` are also deny-delete in every template (see
the delete-grant section above): no role deletes any of them, a deleted
`sys/auth` installs an empty token map that revokes every durable token,
recovery manifests are write-once, and key-epoch records are append-only
history whose loss reads as "no per-tenant key was ever configured".

`t/*/pq/grants` is deny-delete in every template, and `t/*/pq/t/*` is
deletable by Maintain alone. The Parquet manifest delete sits with Maintain
rather than Admin because the only command that issues it, `ravel-cli parquet
sweep`, runs under the Maintain credential.

Gateway and Query carry no write on `sys/gc`, although every mode runs the
`sys/gc` bootstrap at startup: creating it stays with Maintain and Admin, and a
fresh bucket needs the `maintain` process started first, or the object created
with `ravel-cli gc-config set` under Admin (see
`docs/guides/operations/deployment.md`, "The first deployment against a fresh
bucket").

### Which credential each `ravel-cli` command takes

`ravel-cli` takes the Admin credential by default. Three commands take the
Maintain credential instead: `parquet sweep`, `maintain compact-bucket` and
`maintain compact-tenant`. The sweep deletes Parquet table manifests, and the
two compaction commands take claims under `sys/maintain/claims/compaction/`
and write L1 segments and compaction records, all of which `maintain.json`
grants and `admin.json` does not. Admin gains nothing for them. `ravel-cli`
builds no per-tenant KMS routing store, so these writes, like every other
`ravel-cli` write, land under the bucket's default encryption rather than a
routed tenant's key.

Three more mutating commands need grants `admin.json` does not carry and are
not assigned a credential here: `maintain sweep` deletes segment and commit
objects, `maintain migrate` rewrites them, and `catalog fold` writes catalog
objects. Under these templates each is refused under Admin.

### Parquet table DDL

The server does not run Parquet table DDL yet: `crates/ravel-sql/src/ddl.rs`
writes manifests and runs the same bucket probe, but nothing in
`ravel-server` calls it. When DDL is wired into the server, the Query role will
also need `s3:PutObject` on `t/*/pq/t/*` for the manifest writes, and
`s3:PutObject` and `s3:DeleteObject` on `sys/pq-probe/*` for the probe. No
template grants those today.

## Bucket-configuration reads: granted by no template

No template here grants the read-only bucket-configuration actions
`s3:GetBucketVersioning`, `s3:GetLifecycleConfiguration`,
`s3:GetReplicationConfiguration` and `s3:GetBucketObjectLockConfiguration`,
which `ravel-cli store verify-protection` and the bucket lines of
`ravel-cli store qualify` read (and which `ravel-server
--require-bucket-protection` reads at startup). Under these templates those
reads are refused, the commands report every bucket condition as unknown, and
`verify-protection` exits 2. Run them with an identity outside these templates
that holds those four actions. Whether a shipped role should carry them is
issue #2230.
