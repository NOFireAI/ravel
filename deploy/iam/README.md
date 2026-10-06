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
  and no manifest. This rests on the policy layer treating `?` as exactly
  one character, as AWS IAM does; a layer that ignores `?` or reads it as
  `*` turns the statement into a delete over `t/*/*/admission/*`, so check
  that before applying these templates to another S3-compatible store. The
  read, write and list grants on `t/*/*/admission/*`
  and on other key segments (`c`, `l0`, `l1`, `idem`, `maint`, `u`,
  `catalog`, `del` and `a`) keep the cross-`/` match, so they would reach the
  manifests of a Parquet table with that name. No such table can exist:
  `validate_table` (`crates/ravel-pqtable/src/names.rs`) refuses each of those
  segments as a table name (ADR-2040's 2026-10-03 IAM segment amendment).
- **Query** (`query.json`): `QueryProbeDelete` grants `s3:DeleteObject` on
  `sys/pq-probe/*` only, the scratch object the Parquet bucket probe writes
  and deletes before it returns when HTTP DDL runs a `CREATE EXTERNAL TABLE`
  (see "Parquet table DDL" below). The query path deletes nothing else. A
  draining query worker overwrites its own
  `sys/query/workers/` record with a stamp no reader accepts as live, and the
  maintain role reaps dead records (issue #1828).
- **Admin** (`admin.json`): `AdminQualifyDelete` grants delete on
  `sys/qualify/*`, and `AdminProbeDelete` on `sys/pq-probe/*`. Both are
  scratch: the qualification run's objects, and the one object the Parquet
  bucket probe writes and deletes before it returns.
- **Maintain** (`maintain.json`): `MaintainDelete` grants delete on
  `t/*/*/l0/*`, `t/*/*/c/*`, `t/*/*/l1/*`, `t/*/*/idem/*`, `t/*/*/maint/*`,
  `t/*/u/*/0001/*`, `t/*/*/del/*.dreq`, `t/*/catalog/*/snap/*`,
  `t/*/catalog/*/idx/*`, `sys/maintain/workers/*`, `sys/query/workers/*`,
  `quarantine/t/*/*/l0/*`, and `t/*/pq/t/*`. These are the objects the
  compaction, supersession, retention, erasure-request, unreferenced-catalog,
  dead-worker reap, quarantine-reaper and Parquet manifest sweeps physically
  remove, plus the unnamed-since markers (ADR-1133) under `maint/unn/` that
  the retention and superseded-input sweeps and the orphan-marker reaper
  delete. The scan cursor, the other occupant of `maint/`, is overwritten and
  never deleted. The Parquet manifest sweep is `ravel-cli parquet sweep`, run under
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
  `ListBucket`, and `t/*/*/maint/*` for the signal-wide LIST of
  `t/<tenant_hash>/<signal>/maint/unn/` that the marker gate and the
  orphan-marker reaper share.

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

### Unnamed-since markers: the four calls the marker gate makes

ADR-1133 has the retention and superseded-input sweeps write an unnamed-since
marker the first time a pass finds a delete candidate the live HEAD no longer
names, at `t/<tenant_hash>/<signal>/maint/unn/<shard>/<ingest_hour>/<stem>.unn`,
and an orphan-marker reaper clean up the ones whose anchor is gone. All of it
runs under the Maintain role, in `crates/ravel-maintain/src/reachability.rs`
and `crates/ravel-maintain/src/unnamed_marker.rs`.

| Call | S3 operation | Grant |
|---|---|---|
| `put_marker` `store.put(key, ...)` (`CreateIfAbsent`) | `s3:PutObject` | `MaintainWrite` `t/*/*/maint/*` |
| `get_marker` `store.get(key, GetRange::Full)`, and the reaper's body GET in `reap_listed` | `s3:GetObject` | `MaintainRead` `t/*/*/maint/*` |
| `ensure_marker_listing` and `reap_after_pass` `list_all(store, &unnamed_marker_prefix(tenant, signal))`, also `reap_orphan_unnamed_markers` | `s3:ListBucket` with `prefix=t/<tenant_hash>/<signal>/maint/unn/` | `MaintainList` `s3:prefix` `t/*/*/maint/*` |
| `delete_marker` (retirement, a re-named candidate, an anchor mismatch) and the reaper's `store.delete` in `reap_listed` | `s3:DeleteObject` | `MaintainDelete` `t/*/*/maint/*` |

A refused marker delete keeps a retention bucket's tombstone, and blocks a
candidate whose marker must be replaced, on every pass, so the delete grant is
load-bearing. `DenyDeleteProtected`'s `t/*/u/*/0000/*` also matches every
key on audit shard 0: the marker (`t/<hash>/u/maint/unn/0000/...`) and both
anchor classes, the retention tombstone under `t/<hash>/u/c/0000/` and any
compaction or rewrite record there. Those anchors are never deleted, so the
reaper never reaches the marker delete: it HEADs a marker's anchor first and
moves on while the anchor is present (`reap_listed`,
`crates/ravel-maintain/src/unnamed_marker.rs`). A marker written for an audit
shard-0 anchor, one per gated hour bucket in the common case, stays for good.
`maintain_template_covers_every_unnamed_marker_call` in
`crates/ravel-commit/tests/iam_templates.rs` witnesses all four grants.

## Catalog objects: the scheduled fold runs under Maintain

Since ADR-1693 the scheduled catalog fold runs on the maintain tier:
`Mode::runs_scheduled_fold` (`services/ravel-server/src/config.rs`) is true
for `maintain` and `all` only. `fold_inner` (`crates/ravel-catalog/src/fold.rs`)
reads the current HEAD and the objects it names, lists and reads the commit
buckets, and publishes per signal:

| Call | Mode | S3 operation | Grant |
|---|---|---|---|
| `discover_bucket_listings` lists `commit_shard_hour_prefix` for each (shard, hour) bucket and reads the commit records it finds | `maintain`, `all` | `s3:ListBucket`, `s3:GetObject` | `MaintainList` `s3:prefix` and `MaintainRead` `t/*/*/c/*` |
| The HEAD read, and the reuse-baseline reads of the parts, `.cstat` and `.npost` objects the HEAD names | `maintain`, `all` | `s3:GetObject` | `MaintainRead` `t/*/catalog/*/HEAD`, `t/*/catalog/*/snap/*` and `t/*/catalog/*/idx/*` |
| `part_object_key`: PUT `t/<tenant_hash>/catalog/<signal>/snap/<hour>.<hash16>.csnap` (`CreateIfAbsent`; any refusal but `AlreadyExists` aborts the fold) | `maintain`, `all` | `s3:PutObject` | `MaintainWrite` `t/*/catalog/*/snap/*` |
| `column_stats_object_key`: PUT `t/<tenant_hash>/catalog/<signal>/idx/<hour>.<hash16>.cstat` (`CreateIfAbsent`, when typed columns are declared; a refusal aborts the fold) | `maintain`, `all` | `s3:PutObject` | `MaintainWrite` `t/*/catalog/*/idx/*` |
| `postings_object_key`: PUT `t/<tenant_hash>/catalog/<signal>/idx/<hour>.<hash16>.npost` (`CreateIfAbsent`; a refusal is logged and the HEAD is published without postings) | `maintain`, `all` | `s3:PutObject` | `MaintainWrite` `t/*/catalog/*/idx/*` |
| `head_object_key`: PUT `t/<tenant_hash>/catalog/<signal>/HEAD` (`CasVersion`, or `CreateIfAbsent` when no HEAD exists) | `maintain`, `all` | `s3:PutObject` | `MaintainWrite` `t/*/catalog/*/HEAD` |

Until issue #2382 `MaintainWrite` named none of the three catalog patterns,
so on a per-role deployment every scheduled fold with something to publish
was refused at its first catalog write and the catalog stopped advancing.
`t/*/catalog/*/HEAD` stays in `maintain.json`'s `DenyDeleteProtected`; that
statement denies deletes only, so it does not cancel the HEAD write.
`maintain_template_covers_the_scheduled_fold_catalog_writes` in
`crates/ravel-commit/tests/iam_templates.rs` pins the HEAD, `snap/` and
`idx/` rows, the commit-record list and get, and the deny-delete on HEAD;
`EXPECTED_PATTERNS` pins the exact pattern spellings, which is what tells
the `.cstat` and `.npost` rows apart.

The catalog grants per role:

| Key | Gateway | Query | Maintain | Admin |
|---|---|---|---|---|
| `t/*/catalog/*/HEAD` | list, get, put | list, get, put | get, put | list, get |
| `t/*/catalog/*/snap/*` | list, get, put | list, get, put | list, get, put, delete | list, get |
| `t/*/catalog/*/idx/*` | list, get, put | list, get, put | list, get, put, delete | list, get |

Gateway's and Query's grants are the whole-family `t/*/catalog/*/*` list and
read plus the three puts, carried over from when both modes ran the
scheduled fold. Query still mounts the on-demand `POST /api/v1/admin/fold`
route. Gateway folds by no route; removing its catalog grants is a separate
least-privilege decision recorded on issue #2382. Admin lists and reads the
catalog through `t/*`. Maintain's two deletes are the unreferenced-catalog sweep
(see "Delete grants per role" above).

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
| `parquet-grant add` qualifies the target bucket with `probe_not_ravel_bucket` (`crates/ravel-object-store/src/external/probe.rs`), which PUTs `sys/pq-probe/<32 hex chars>` and DELETEs it before returning, including when the PUT is reported failed; a failed inline DELETE is retried by a background DELETE, which does not run if the process exits first | Admin | `s3:PutObject`, `s3:DeleteObject` | `AdminWrite` and `AdminProbeDelete` `sys/pq-probe/*` |
| The Parquet table provider (`crates/ravel-sql/src/parquet.rs`, built from `services/ravel-server/src/query.rs`) resolves a table through `crates/ravel-pqtable/src/resolve.rs`: `newest` (through `versions`) lists `t/<tenant_hash>/pq/t/<table>/v/`, `read_version` GETs the manifest, and `grants::list` GETs `t/<tenant_hash>/pq/grants` | `query`, `all` | `s3:ListBucket`, `s3:GetObject` | `QueryList` `s3:prefix` `t/*/pq/t/*`; `QueryRead` `t/*/pq/t/*` and `t/*/pq/grants` |
| HTTP Parquet DDL: `POST /api/v1/sql` runs `CREATE [OR REPLACE] EXTERNAL TABLE` and `DROP TABLE` through `execute_ddl` (`crates/ravel-sql/src/ddl.rs`). `resolve::newest` lists `t/<tenant_hash>/pq/t/<table>/v/` and GETs the newest manifest; `CREATE` also GETs `t/<tenant_hash>/pq/grants` and runs `probe_not_ravel_bucket`, which PUTs `sys/pq-probe/<random>` (`Overwrite`) and DELETEs it; `writer::apply` (`crates/ravel-pqtable/src/writer.rs`) PUTs `t/<tenant_hash>/pq/t/<table>/v/<version>.pqm` with `CreateIfAbsent`, its only put, for both `CREATE` and `DROP` | `query`, `all` | `s3:ListBucket`, `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` | `QueryManifestCreate` `t/????????????????????????????????/pq/t/*/v/????????????????????.pqm`, conditioned on `StringEquals` `s3:if-none-match` `*`; `QueryWrite` and `QueryProbeDelete` `sys/pq-probe/*`; the list and reads are the Parquet table provider's row above |
| `ravel-cli parquet sweep` (`crates/ravel-pqtable/src/sweep.rs`): `plan` lists `t/<tenant_hash>/pq/t/`, the CLI wrapper reads `sys/gc` for the deployment's grace floor, and `execute` deletes each superseded manifest | Maintain credential | `s3:ListBucket`, `s3:GetObject`, `s3:DeleteObject` | `MaintainList` `s3:prefix` `t/*/pq/t/*`; `MaintainDelete` `t/*/pq/t/*`; `sys/gc` is already in `MaintainRead` |
| `ravel-cli parquet repair` (`crates/ravel-pqtable/src/repair.rs`): lists one table's `t/<tenant_hash>/pq/t/<table>/v/` keys, and with `--delete` or `--delete-version` deletes the flagged or the named manifest key; it never reads a manifest, since Maintain cannot | Maintain credential | `s3:ListBucket`, `s3:DeleteObject` | `MaintainList` `s3:prefix` `t/*/pq/t/*`; `MaintainDelete` `t/*/pq/t/*` |
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
query is refused. Without the Query manifest create, every HTTP `CREATE` and
`DROP` is refused at its manifest write, and without the Query probe PUT every
`CREATE` is refused at its bucket probe. Without the Maintain list,
`parquet sweep` is refused before it sees a manifest, and without the delete
every superseded manifest stays.
Without the gateway's admission delete, each reap is refused and logged, and
dead processes' snapshots accumulate under the prefix every reconcile lists.

No server role may write `sys/auth`: the only production writers are
`ravel-cli` under Admin and the operator, which uses the shared credential
named by `spec.storage.s3.credentials_secret_ref` rather than any of these
templates. The tenant config record `t/<tenant_hash>/config` is likewise
written only by `ravel-cli` under Admin, never by a server role. The Maintain
role reads the alert state memo and writes neither it nor the lease, and no role
deletes either; nothing releases the lease. No `ravel-cli` command creates
`t/<tenant_hash>/enc`: the four that take `--tenant-kms-config` refuse a
tenant whose record is absent or names a different current key. Their one
write to it, under the Maintain credential, completes a record holding only
the bootstrap epoch 0 that a server began, recording the key their own file
names as epoch 1, so they must run with the file the servers run with (see
"Which credential each `ravel-cli` command takes" below). Nothing lists it.

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
bucket"). On AWS S3 each role also needs to see an absent `sys/gc` as absent
rather than refused; see "Bootstrap keys" below.

### Which credential each `ravel-cli` command takes

`ravel-cli` takes the Admin credential by default. Seven commands take the
Maintain credential instead, because `maintain.json` grants everything they
issue and `admin.json` does not:

- `parquet sweep` deletes superseded Parquet table manifests.
- `parquet repair` lists a table's manifest keys and, with `--delete` or
  `--delete-version`, deletes forged ones (see "Parquet table DDL" above for
  when).
- `maintain compact-bucket` and `maintain compact-tenant` take claims under
  `sys/maintain/claims/compaction/` and write L1 segments and compaction
  records.
- `maintain migrate` rewrites L1 segments and compaction records, writes and
  deletes its cursor, and raises the format floor in
  `t/<tenant_hash>/<signal>/prov` (its grants are listed below).
- `maintain sweep` reads legal holds, commit records and the catalog, deletes
  superseded and expired L0, L1 and commit objects, copies orphans to
  `quarantine/` and deletes them there, and creates `sys/gc` on a fresh
  bucket.
- `catalog fold` lists and reads commit records and segment data and writes
  catalog snapshot parts, `HEAD` and index objects, the same keys the
  scheduled fold in maintain mode writes. `query.json` also grants all of it.

Admin gains nothing for them.

Under `--tenant-kms-config`, the Maintain-credential commands that write
tenant data take the same flag as the servers and route the same way:
`maintain compact-bucket`, `maintain compact-tenant`, `maintain migrate` and
`catalog fold`. Each builds the servers' KMS routing store and reads its own
tenant's `t/<tenant_hash>/enc` epoch record first. When the record's current
key is the file's it writes nothing to it. When the record is absent, or its
current key differs, it refuses the whole command before any write: only
ravel-server's startup records a configured or changed key (ADR-0062 decision
1b), and the record is append-only, so start ravel-server with the file
first. A `--dry-run` reads the record and refuses the same way. The one write
a command makes to the record (`MaintainWrite` `t/*/enc`, under the bucket
default like every control record) completes a record holding only the
bootstrap epoch 0, a first configuration a server began and did not finish.
It then writes that tenant's L1 segments, compaction records, catalog
snapshot parts, `HEAD` and index objects, and `maintain migrate`'s cursor and
floor raise in `prov`, under the tenant's key (`MaintainTenantKms`, which
already grants `kms:Encrypt` and `kms:GenerateDataKey*`). No grant changes
for it.
`maintain_cli_data_writes_are_routed_and_maintain_can_encrypt_them` in
`crates/ravel-commit/tests/iam_templates.rs` checks those key classes against
this template. `parquet sweep` writes nothing, and `maintain sweep` writes
only its unnamed-since markers under `t/`, so neither takes the flag. No Admin
command takes it: Admin is decrypt-only on the tenant keys, so its control
records (provisioning records, legal holds, reconstructed commit records,
erasure requests, the tenant config record and the Parquet grants record)
land under the bucket's default encryption whatever the file says. That is
why `admin` is the one role in `ROUTED_WRITE_EXEMPT_ROLES`.

`maintain migrate` takes the Maintain credential (ADR-0066 decision 5).
`maintain.json` grants its reads, its L1 part and compaction-record writes
and its cursor write. The two calls that end a walk are the delete of its
`t/<tenant_hash>/<signal>/maint/migrate/<family>/cursor` object, which runs
after every completed walk and stops the run on a refusal, and the
`CasVersion` write of `t/<tenant_hash>/<signal>/prov` that raises the format
floor after a clean re-audit. The delete falls under `MaintainDelete`
`t/*/*/maint/*`, since IAM's `*` matches `/`. The floor raise is
`MaintainProvCas`, a CAS-only grant (see "Provisioning records: conditioned
writes" below). `maintain_template_covers_every_maintain_migrate_call` in
`crates/ravel-commit/tests/iam_templates.rs` checks a list of the command's
calls against this template. That list is written by hand from the code, not
derived from it, so a new call in `migrate.rs` needs a new entry there; the
cursor key in it is a format string mirroring `migrate_cursor_key`, which is
private to `ravel-maintain`. Issue #2359 changes the migrate cursor; the
grants here are for the cursor as it is today.

### Parquet table DDL

`POST /api/v1/sql` runs `CREATE [OR REPLACE] EXTERNAL TABLE` and `DROP TABLE`
through `execute_ddl` (`crates/ravel-sql/src/ddl.rs`) in `query` and `all`
mode, under the Query credential. `query.json` grants exactly what it issues
against the Ravel bucket (ADR-0055, HTTP DDL amendment):

- `QueryManifestCreate`: `s3:PutObject` on
  `t/????????????????????????????????/pq/t/*/v/????????????????????.pqm`,
  conditioned on `StringEquals` `s3:if-none-match` `*`. `writer::apply` puts
  every manifest version with `CreateIfAbsent`, which the S3 backend sends as
  `If-None-Match: *`, so the role can create a new version and cannot
  overwrite an existing one. A `DROP` writes a dropped version through the
  same put. On a conflict the backend HEADs the key, which `QueryRead`
  already covers. The tenant hash is spelled as 32 `?` so the write reaches
  only `t/<tenant_hash>/pq/t/`, never a `pq/t/` segment deeper in another
  keyspace; this rests on the same single-character `?` reading the gateway's
  admission delete does. The version is spelled as 20 `?` and the `.pqm`
  suffix (`manifest_key` in `crates/ravel-pqtable/src/keys.rs`), so only a
  key shaped like a manifest version is writable. The table segment has to be
  `*`, because table names run from 1 to 63 bytes; IAM's `*` also matches
  `/`, so the grant also reaches keys such as
  `t/<tenant_hash>/pq/t/a/b/v/<20 chars>.pqm`, a key under a real table's own
  `v/` prefix with an extra path segment
  (`t/<tenant_hash>/pq/t/hits/v/q/v/<20 chars>.pqm`, where the `*` binds
  `hits/v/q`), or a version slot that is not 20 digits. Every such key is
  still inside that tenant's manifest keyspace, which the role can already
  create versions in. A `.pqm` key under a real table's `v/` prefix whose
  slot names no version (the wrong length, an extra path segment, too large
  for a `u64`, zero, or not all digits) is skipped, counted and warned about
  by every listing, as a version above the bound is, and
  `ravel-cli parquet repair --delete` removes it. A key whose segment between
  `pq/t/` and `/v/` is not a single valid table name, such as the `a/b` one,
  is still refused as foreign: it makes the tenant's `parquet ls` and
  manifest sweep fail with a foreign-key error, though no table's resolve,
  until the Maintain credential deletes it; validating that segment is issue
  #2510. That is the same class of harm as the maximal-version wedge below,
  confined to the manifest keyspace.

  Creating a new version is not harmless: the newest version is the table
  for every reader. A compromised Query credential can define, redefine or
  drop any table of any tenant. What bounds it is that every table
  definition is checked at every resolve against the tenant's location
  grants record `t/<tenant_hash>/pq/grants`, which Query cannot write, so a
  forged definition can only reach locations the tenant has granted. The
  server's own DDL authorization does not bind the IAM credential: anything
  holding it can put a manifest directly. The version bound
  (`MAX_MANIFEST_VERSION`, 2^32, in `crates/ravel-pqtable/src/keys.rs`)
  removes the automatic wedge from a version above it, such as one numbered
  `u64::MAX`, and from a key that names no version: no DDL statement writes
  either, readers and the writer skip both when choosing the newest
  version, the first listing in each process logs one at `warn` naming the
  table, and `ravel-cli parquet sweep` neither deletes one nor treats it as a
  successor. Remove them with
  `ravel-cli parquet repair --tenant <tenant> --table <table> --delete`
  under the Maintain credential (`MaintainDelete` on `t/*/pq/t/*`); without
  a delete flag the command lists the table's versions and flags those, and
  `--delete` never deletes a version at or below the bound. Until they are
  removed, every resolve of the table lists all of them. The bound does not
  stop a wedge by a forged version at or below it: one exactly at the bound
  leaves the writer no next version, so DDL on the table is refused, and
  one below it that is the highest is the table's definition, which the
  sweep then treats as the successor of the legitimate versions beneath it.
  Either stays in effect until an operator removes it with
  `ravel-cli parquet repair --tenant <tenant> --table <table> --delete-version <N>`,
  also under Maintain, after checking the DDL audit log: every statement the
  server runs records `attempted` before any store call, so a version with
  no matching record was not written by the server. Maintain cannot read
  manifests, so run the listing under Query first to see each version's
  `created_by` and `statement`; restore the noncurrent object versions if
  the sweep already deleted legitimate ones and the bucket keeps them.
  Narrowing this grant, item 1 of issue #2430, remains the root fix. The
  full procedure is in
  [the maintenance guide](../../docs/guides/operations/maintenance.md#repairing-a-forged-parquet-table-version).
- `QueryWrite` gains `sys/pq-probe/*`, and `QueryProbeDelete` grants
  `s3:DeleteObject` on `sys/pq-probe/*` only: before every `CREATE`,
  `probe_not_ravel_bucket` PUTs `sys/pq-probe/<random>` (`Overwrite`) and
  DELETEs it inline before it returns, including when the PUT is reported
  failed. A probe dropped mid-flight (the statement deadline), or one whose
  inline DELETE failed, has its drop guard issue the same DELETE from a
  background task, under the same grant. Nothing in Ravel reaps that prefix,
  so a lifecycle rule on `sys/pq-probe/` bounds what a probe leaves behind
  when that background delete fails, when the process exits or is killed
  before it runs, or when the guard fires with no tokio runtime.

The manifest listing, manifest reads and grants-record read were already in
`QueryList` and `QueryRead`. The `LOCATION` listing, HEAD and footer reads and
both qualification probes' reads go to the external bucket under its own
credential profile, not under any template here. `DROP` deletes nothing.

## Provisioning records: conditioned writes

The provisioning record `t/<tenant_hash>/<signal>/prov` (ADR-0050 section 5)
holds a tenant's shard generations and format floors. Nothing writes it
unconditionally. `crates/ravel-catalog/src/provisioning.rs` writes it in three
functions: `write_record_race_safe` with `CreateIfAbsent` (reached from
`validate_or_adopt` under `CreateFromConfig` or `AdoptIfData`), and
`append_generation` and `raise_format_floor` with `CasVersion`. The S3 backend
sends `CreateIfAbsent` as `If-None-Match: *` and `CasVersion` as
`If-Match: <etag>`, and AWS documents both `s3:if-none-match` and
`s3:if-match` as condition keys IAM evaluates on `PutObject`, so every
template grants the record through
conditioned statements only (ADR-0055, prov write conditions amendment):

| Call | Role | Put mode | Grant |
|---|---|---|---|
| `ProvisioningRecordWriter::ensure` (`services/ravel-server/src/provisioning.rs`), `validate_or_adopt` with `CreateFromConfig` on a tenant's first ingest write | Gateway | `CreateIfAbsent` | `GatewayProvCreate` |
| `validate_static_provisioning` (same file, called from `main.rs` at startup), `validate_or_adopt` with `AdoptIfData` for every statically known tenant whose data predates its record, in `gateway`, `maintain` and `all` mode | Gateway, Maintain | `CreateIfAbsent` | `GatewayProvCreate`, `MaintainProvCreate` |
| The maintain tick (`services/ravel-server/src/maintain.rs`), `validate_or_adopt` with `AdoptIfData` per tenant and signal | Maintain | `CreateIfAbsent` | `MaintainProvCreate` |
| `ravel-cli maintain migrate`, `raise_format_floor` after a clean re-audit (`crates/ravel-maintain/src/migrate.rs`) | Maintain | `CasVersion` | `MaintainProvCas` |
| `ravel-cli provision adopt`, `validate_or_adopt` with `AdoptIfData` | Admin | `CreateIfAbsent` | `AdminProvCreate` |
| `ravel-cli provision reshard`, `append_generation` | Admin | `CasVersion` | `AdminProvCas` |

A `query` process runs the startup check with `RefuseIfCommittedDataHidden`
(`static_absent_policy` in `services/ravel-server/src/provisioning.rs`) and
never adopts: it validates a present record and refuses startup on one it
cannot read. For an absent record it lists only the commit prefix
`t/<tenant_hash>/<signal>/c/`, which `QueryList` already admits through
`t/*/*/c/*`, and refuses startup with `AdoptionWouldHideData` when committed
data sits on a shard index at or above its `--shards`, because it reads such
a tenant through that shard count and would leave those shards out (ADR-0050
section 5). Otherwise it passes without writing anything. It never lists
`l0/`, since Query serves committed data only. Adoption belongs to ingest,
maintenance and the CLI. The read role therefore holds no provisioning write,
so an adopting Query startup would be refused at the write. `QueryList` does
admit `t/*/*/l0/*`, for the reason "Data objects a query reads" below gives,
so the template would not refuse an `l0/` listing: what keeps the startup
check from issuing one is `static_absent_policy`, and the ravel-server test
`query_mode_startup_over_in_range_committed_data_lists_commits_and_writes_nothing`
fails any `l0/` listing.

Each create-only statement is `s3:PutObject` conditioned on `StringEquals`
`s3:if-none-match` `*`, the form `QueryManifestCreate` uses. Each CAS-only
statement is `s3:PutObject` conditioned on `Null` `s3:if-match` `false`: a PUT
that sends `If-Match` passes, and an unconditional PUT or a create-if-absent
PUT that sends no `If-Match` is refused. Every one of them names exactly three
resources, one per provisioned signal:

```
t/????????????????????????????????/m/prov
t/????????????????????????????????/l/prov
t/????????????????????????????????/s/prov
```

IAM's `*` matches `/`, so `t/*/*/prov` would also reach nested keys that end
in `/prov` (for example `t/<tenant_hash>/m/c/<shard>/<hour>/prov` or
`t/<tenant_hash>/m/del/prov`) and the `prov` key of the alerts, audit and
profiles signals, none of which has a provisioning record. The tenant hash is
spelled as 32 `?`, the way `QueryManifestCreate` spells it, so the statements
name the record keys of metrics, logs and spans. An IAM `?` also matches `/`,
so the patterns are not a proof that no other string matches, but no real key
of another shape does: a tenant segment of 31 or 33 characters, a nested key
ending in `/prov` and an unprovisioned signal's `prov` key all fall outside
them.
`DenyDeleteProtected` keeps the broader `t/*/*/prov`, since a broad deny is
the safe direction.

Gateway holds the create-only grant alone, because no path under its
credential appends a generation or raises a floor. The operator's reshard
calls `append_generation` too, under the shared credential named by
`spec.storage.s3.credentials_secret_ref` rather than any template here.
`ravel-cli load` creates the record too, with `CreateFromConfig`, so a
create-only grant covers it.

Without the create grant, a Maintain process with a statically known tenant
whose data predates its record fails at startup with `AccessDenied`, and the
maintain tick skips that tenant's signal on every tick. Without the CAS grant,
`maintain migrate` finishes its rewrite and cannot raise the floor.

What the conditions buy, for the metrics, logs and spans records: a
compromised Gateway credential can create a missing record and cannot replace
an existing one, and a compromised Query credential cannot write one at all. A
compromised Maintain or Admin credential can still rewrite a record through a
CAS PUT, since it can read the current ETag first; that is the overwrite gap
ADR-0055 section 3 already names. `DenyDeleteProtected` still denies delete
on `t/*/*/prov` in all four templates, so no role can delete a record and
recreate it.

One key outside those three is still writable: `t/<tenant_hash>/u/prov`, the
`prov` key of the audit signal, falls under the unconditioned `t/*/u/*`
PutObject that `QueryWrite` and `AdminWrite` grant for audit records. The
audit signal has no provisioning record, so nothing legitimate is there to
replace; a planted one could widen the shard range an audit scan reads or make
audit reads fail, and cannot hide any record.

## Bootstrap keys: a list grant on each absent key a role reads

On AWS S3, a GET or HEAD of an absent key is answered with 403,
not 404, unless the credential holds an `s3:ListBucket` grant whose `s3:prefix`
condition covers that key. A live check against AWS on 2026-10-03 (recorded on
issue #2332) confirmed this, and confirmed that a `ListBucket` statement whose
`s3:prefix` condition names exactly the key turns the answer into 404 while a
key the condition does not name keeps answering 403.

Every server role reads control-plane keys whose absence is a normal state: a
fresh bucket has no `sys/tenancy` or `sys/gc` yet, a tenant with no overrides
has no config record, and so on. The code treats `NotFound` on each as that
state and any other error, `AccessDenied` included, as a failed read. Without a
list grant covering the key, every such read on a fresh bucket or a new tenant
fails, and start order cannot help, since the first process to start is refused
the read of each object before it can create it.

Each server template therefore carries two list statements naming exactly the
keys that role reads this way:

- `GatewayListBootstrapKeys`, `QueryListBootstrapKeys` and
  `MaintainListBootstrapKeys` name the fixed keys under `StringEquals`.
- `GatewayListTenantBootstrapKeys`, `QueryListTenantBootstrapKeys` and
  `MaintainListTenantBootstrapKeys` name the per-tenant keys under
  `StringLike`, with the tenant hash spelled as 32 `?` and no `*`.

They are two statements because IAM requires every operator in one `Condition`
block to match, so `StringEquals` and `StringLike` together in one statement
would admit nothing. The existing list statements are unchanged.

| Key | Gateway | Query | Maintain | Read by, and what absence means |
|---|---|---|---|---|
| `sys/qualification` | yes | yes | yes | `qualification::enforce` at startup; absent refuses startup with the error naming `ravel-cli store qualify` |
| `sys/tenancy` | yes | yes | yes | `resolve_and_pin` at startup; absent means no process has pinned the scheme yet, and this one writes the marker |
| `sys/gc` | yes | yes | yes | `bootstrap_gc_config` at startup; absent means a fresh bucket, and the process tries to create it |
| `sys/auth` | yes | yes | | `DurableAuthState::refresh` on a keyed bucket; absent is an empty token map |
| `t/<tenant_hash>/config` | yes | yes | yes | `read_config`; absent means the tenant runs on the deployment defaults |
| `t/<tenant_hash>/enc` | yes | yes | yes | `bootstrap_tenant_epoch` at startup under `--tenant-kms-config`; absent means no epoch is recorded yet, and the process records the first ones |
| `t/<tenant_hash>/m/meta` | yes | yes | | the metadata sink (creates it) and the metadata cache (serves nothing) |
| `t/<tenant_hash>/<signal>/prov` | `m`, `l`, `s` | `m`, `l`, `s`, `p`, `a`, `u` | `m`, `l`, `s` | `validate_or_adopt` and the shard-generation reads; absent means a tenant with no write yet. The query catalog reads it for whichever signal a query resolves, including signals that never get a record, so Query names one pattern per signal letter rather than a `?` that would admit any one-character segment |
| `t/<tenant_hash>/catalog/<signal>/HEAD` | | | `m`, `l`, `s` | the scheduled fold's `get_head`; absent means the first fold |
| `t/<tenant_hash>/a/state/latest` | | yes | yes | the alert evaluator (folds the full history) and alert retention (checks the commit prefix) |
| `t/<tenant_hash>/pq/grants` | | yes | | `grants::list`; absent is an empty grant list |
| `t/<tenant_hash>/<signal>/idem/<keyhash32>.<ingest_hour>.idm` | `l`, `s` | | | `read_marker` on every keyed log or span write, once per hour of the dedup window; absent means no marker at that hour. The keyhash and the hour are spelled one `?` per character, as the tenant hash is |

The catalog `HEAD` that Gateway and Query read was already covered by their
`t/*/catalog/*/*` list prefix, and Maintain's scrub cursor and other keys
under `t/*/*/maint/*` by `MaintainList`, so those need no new grant.

These statements list nothing else. A list request whose prefix is one of
these keys can return only that key, since no key the system writes begins
with one of them and continues. They admit no `sys/` or `t/<tenant_hash>/`
listing, no key one segment deeper, no tenant segment wider or narrower than
a tenant hash, and no `prov` record under a letter no signal uses.
`crates/ravel-commit/tests/iam_templates.rs` pins the exact condition values
per role, checks that each read above is admitted by its role's statement,
and checks that the bootstrap statements admit no other
key: neither a sibling key the role does not read (`sys/auth` for Maintain),
nor a deeper key, a listing prefix, a 31- or 33-character tenant segment, or
`t/<tenant_hash>/x/prov` for Query.

Some keys are deliberately left out. `sys/t/<tenant_hash>`, the alert lease
and the compaction claims are written with a create-if-absent PUT first and
read only once that PUT reports the object exists. A data object that a
concurrent compaction deleted is a race, not a bootstrap state, so it is not
named here: `QueryList` names the `l0/` and `l1/` prefixes with a `*` for it
(see the next section).

## Data objects a query reads: a list grant so a deleted one reads as missing

On AWS S3, a GET of an absent key answers 404 only when one of the caller's
`s3:ListBucket` grants has an `s3:prefix` value that matches the key itself,
and 403 otherwise. Measured on a real S3 bucket on 2026-10-05: under a
`StringLike` grant on `t/*/*/l0/*`, a GET of an absent L0 key answered 404;
under a grant on `t/` alone, a GET of an absent L1 key answered 403.

A query pins its segment set when it resolves and then GETs each segment. A
compaction or retention delete can remove one in between. Both query engines
re-resolve and retry once, and only on `NotFound`:

| Call | Mode | S3 operation | Grant |
|---|---|---|---|
| The PromQL engine's segment fetch (`crates/ravel-query/src/engine.rs`) and the SQL executor's (`crates/ravel-sql/src/executor.rs`, `SqlError::is_segment_not_found`) GET each pinned L0 and L1 segment, and re-resolve once when a GET reports `NotFound` | `query`, `all` | `s3:GetObject`; the 404 needs `s3:ListBucket` | `QueryRead` `t/*/*/l0/*` and `t/*/*/l1/*`; `QueryList` `s3:prefix` `t/*/*/l0/*` and `t/*/*/l1/*` |

Until issue #2462 `QueryList` named neither prefix, so on AWS S3 a query
whose read raced such a delete was refused with `AccessDenied` and failed
instead of retrying. The list grant lets Query list the data prefixes; it
could already read every object under them, and list the commit records and
the catalog that name them.

`query_lists_the_data_prefixes_so_a_deleted_segment_reads_as_missing` in
`crates/ravel-commit/tests/iam_templates.rs` derives the keys Query may GET
and Maintain can delete from the two templates and checks that a `QueryList`
value matches each one. The same check over every server role against every
other role's deletes finds two kinds of key no list grant of the reader
matches, both pinned by `deletable_reads_without_a_list_grant_per_server_role`:
Gateway's `t/*/*/idem/*` read and Maintain's `t/*/*/idem/*` delete meet on the
test's literal `idem/` witnesses, a shape no marker has (a real marker key is
matched by `GatewayListTenantBootstrapKeys`), and Maintain's `t/*/u/*` read
reaches the audit signal's admission snapshot, which Gateway's reap deletes and
no Maintain path reads.

## Idempotency markers: the keyed-ingest lookup

A log or span request carrying `x-ravel-idempotency-key` looks up its marker
before its own data is written, and writes the marker after its data commits.
The marker key ends in the ingest hour the original request pinned, which a
retry cannot know, so the lookup is a GET per hour of the dedup window, from
one hour ahead of the current hour back 24 hours. It GETs the hour ahead, the
current hour and the one before it together, and stops there if any holds a
marker (3 GETs); otherwise it GETs the other 23 hours, at most 8 at a time. The
newest marker found wins. A miss is 26 GETs. It lists nothing. The calls are in
`crates/ravel-ingest/src/idempotency.rs`:

| Call | Mode | S3 operation | Grant |
|---|---|---|---|
| `read_marker` GETs `marker_key(tenant, signal, key, hour)` for each hour of the window, and `write_marker` GETs the winner's marker after losing a create race | `gateway`, `all` | `s3:GetObject` | `GatewayRead` `t/*/*/idem/*` |
| the same probe of an absent marker, which AWS S3 answers 404 only under a list grant on that key | `gateway`, `all` | `s3:ListBucket` with `prefix=t/<tenant_hash>/<signal>/idem/<keyhash32>.<ingest_hour>.idm`, signal `l` or `s` | `GatewayListTenantBootstrapKeys` `s3:prefix` `t/????????????????????????????????/l/idem/????????????????????????????????.????????T??.idm` and the same with `s` |
| `write_marker` PUTs `t/<tenant_hash>/<signal>/idem/<keyhash32>.<ingest_hour>.idm` (`CreateIfAbsent`) | `gateway`, `all` | `s3:PutObject` | `GatewayWrite` `t/*/*/idem/*` |

The list grant names a marker key exactly, as the bootstrap keys above do, so
a list request it admits returns at most that one marker key. It adds no
listing of the `idem/` directory beyond what `GatewayList`'s pre-existing `t/`
prefix already allows: a list of `t/` returns every key under it, markers
included. Metrics requests take no key and do no lookup.

If a probe fails with a store error other than not-found, on a bucket whose
policy predates the list grant (where an absent marker answers 403) or for any
other reason, the keyed request fails with HTTP 503 or gRPC `UNAVAILABLE`
before its own data is written, so the client's retry is safe. The same
happens when the lookup is still running at half of the request's
acknowledgement deadline (`ack_deadline`), for instance because S3 is
throttling the probes. The lookup and the write share that one budget: the
lookup may use half, and the write gets what is left. The response
says that the idempotency marker lookup failed and the write is safe to
retry, and nothing about the store; the gateway logs the
failed GET, its key and the store error (or, past the deadline, the deadline)
at WARN and counts the refusal on
`ravel_ingest_idempotency_lookup_failures_total`. It never writes without the
lookup, since that would store a duplicate of a request that already landed. A
request without a key is unaffected.
`crates/ravel-commit/tests/iam_templates.rs` pins both grants for the marker
key and checks that no gateway list grant's `s3:prefix` admits a list request
for the `idem/` directory or any other prefix beside a marker key.

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
