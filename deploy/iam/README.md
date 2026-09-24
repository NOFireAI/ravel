# IAM policy templates

Four per-role templates: `gateway.json`, `query.json`, `maintain.json`,
`admin.json`. Replace `my-ravel-bucket` and the KMS key placeholder before
use. `crates/ravel-commit/tests/iam_templates.rs` pins the resource and
action set of every statement in all four files; a hand edit that drifts
from the pinned shape fails that suite.

## Commit records (`t/*/*/c/*`) are deletable by design

`DenyDeleteProtected` in `maintain.json` denies delete on `sys/tenancy`,
`sys/qualification`, `sys/gc`, `t/*/*/prov`, `t/*/catalog/*/HEAD`, and the
legal-hold audit shard (`t/*/u/*/0000/*`). Commit records are absent from
that list on purpose: `MaintainDelete` grants delete on `t/*/*/c/*`
because the maintenance sweep physically removes a commit record once
it is superseded, and an IAM deny there would make every sweep pass fail.

The other three templates (`gateway.json`, `query.json`, `admin.json`) deny
the whole catalog family, `t/*/catalog/*/*`, instead: none of those roles
deletes a catalog object, so nothing narrower is needed there. Maintain is
the one role where narrowing to `HEAD` alone is load-bearing.

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
retention so a compromised credential cannot delete or overwrite one
before its retention period elapses, exactly the same as the other
protected prefixes. The two lists disagree on commit records for a
reason, not by accident: IAM's `DenyDeleteProtected` blocks Maintain's own
role from ever deleting the prefix, which would break the sweep; Object
Lock's per-object retention only delays a delete Maintain is allowed to
attempt, and only for the retention period an operator chose. That
retention period is the one bound the mechanism setting it must respect:
kept inside the sweep's own default retention window (`protection_horizon`,
about 25 hours with defaults), the sweep pauses on a locked commit record
for at most that long and then completes; a longer period pauses the
sweep on that record for the difference. See
`docs/object-store-contract.md`'s "Required bucket configuration" section
for the full retention/GC interaction.

## Delete grants per role

Each template's delete authority, read from its `Allow` statements. Every
template also carries the shared `DenyDeleteProtected` deny listed above; the
grants below are what remains deletable after that deny applies.

- **Gateway** (`gateway.json`): no delete grant at all. The ingest path writes
  and reads objects; it deletes nothing.
- **Query** (`query.json`): no delete grant at all. The read path deletes
  nothing.
- **Admin** (`admin.json`): `AdminQualifyDelete` grants delete on
  `sys/qualify/*` only.
- **Maintain** (`maintain.json`): `MaintainDelete` grants delete on
  `t/*/*/l0/*`, `t/*/*/c/*`, `t/*/*/l1/*`, `t/*/*/idem/*`, `t/*/u/*/0001/*`,
  `t/*/*/del/*.dreq`, `t/*/catalog/*/snap/*`, `t/*/catalog/*/idx/*`,
  `sys/maintain/workers/*`, and `quarantine/t/*/*/l0/*`. These are the objects
  the compaction, supersession, retention, erasure-request,
  unreferenced-catalog, dead-worker, and quarantine-reaper sweeps physically
  remove.
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

The erasure-request sweep is `sweep_erasure_requests_inner` in
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

### Worker heartbeats: the four grants the maintain fleet needs

ADR-0065 decision 1 has every maintain process stamp a heartbeat at
`sys/maintain/workers/<process_id>` and derive unit ownership by rendezvous
over the set of keys that are still live. A process that stops heartbeating
must have its key removed, or the set never shrinks.

The lifecycle is `crates/ravel-fleet/src/worker_set.rs`, driven once per
maintain tick from `services/ravel-server/src/maintain.rs`. All four axes are
exercised, and a runbook is not involved: every one of these is a call in the
serving path.

| Call | Axis | Grant |
|---|---|---|
| `write_heartbeat` `store.put(&heartbeat_key(&process_id), ..)` (worker_set.rs:358) | `s3:PutObject` | `MaintainWrite` `sys/maintain/*` (already present) |
| `live_set_read` `list_all(store, WORKERS_PREFIX)` (worker_set.rs:403) | `s3:ListBucket` with `prefix=sys/maintain/workers/` | `MaintainList` `s3:prefix` `sys/maintain/workers/*` (already present) |
| `live_set_read` `store.get(&meta.key, GetRange::Full)` on each in-window sibling (worker_set.rs:424) | `s3:GetObject` | `MaintainRead` `sys/maintain/*` (already present) |
| `reap_keys` `store.delete(key)` past the reap horizon (worker_set.rs:463, from maintain.rs:1111) | `s3:DeleteObject` | `MaintainDelete` `sys/maintain/workers/*` |

Only the delete was missing, and it was missing completely: `MaintainDelete`
named no `sys/` resource at all. IAM is default-deny, so this was not a
narrowing, it was a refusal of every reap the maintain fleet has ever
attempted.

The cost is not the failed delete. Dead heartbeat keys are never removed, so
the prefix `live_set_read` LISTs once per tick grows with every maintain
process that has ever run against the bucket, and the per-tick LIST cost grows
with it. That is the same unbounded-LIST cost issue #1679 removed for
admission snapshots, and #1679's fix assumed this delete succeeded.

The delete pattern is deliberately narrower than the `sys/maintain/*` the read
and write axes use. The memo snapshots (`sys/maintain/memo/`) and the ADR-1029
compaction claims (`sys/maintain/claims/compaction/`) share that prefix, the
reaper is the only deleter under it, and neither of those is the reaper's to
remove. ADR-1029 is explicit that an unconditional delete of a claim is the
one write that would break its advisory guarantee.

`maintain_template_covers_every_worker_heartbeat_call` in
`crates/ravel-commit/tests/iam_templates.rs` asserts each of the four rows
above against the shipped template, with the witness key built by
`heartbeat_key` itself rather than written out, so a change to the key shape
moves the test with the code. It also asserts the tightness premise (no
pattern reaching a heartbeat reaches anything outside `sys/maintain/`) and
that neither gateway nor query reaches one on any axis; admin is excluded on
purpose, since its blanket `sys/*` read and list are the operator role's
deliberate posture over the whole control plane.

An operator who applied a copy of `maintain.json` older than this grant must
re-apply it. Until they do, the reap stays refused: every dead worker's
heartbeat key remains in the bucket, the per-tick `ListBucket` over
`sys/maintain/workers/` keeps growing, and nothing else in a running system
reports it, because `reap_keys` treats a failed delete as a key to retry next
tick rather than as an error to surface. Re-applying drains the accumulated
keys over subsequent ticks rather than at once. Nothing else in the maintain
role stops working in the meantime: the other three axes were always granted,
so heartbeating and live-set reads continue, and ownership stays correct. What
degrades is cost, monotonically.

### The mechanical check

`scripts/guards/check-iam-keyspace-axes.sh` is the cargo-free guard that makes
this defect class fail without anyone remembering to write a reachability test
for a particular prefix. It discovers every control-plane key space the code
names (`sys/`, `quarantine/`, `admission/` roots, from `const NAME: &str` and
`format!` literals), requires each to carry a manifest entry declaring its
owner roles and, per axis, either a call site or a reason it is unused, and
then checks each used axis against that owner's template. It runs in
`scripts/gates.sh` and CI's `doc-scripts` job.

It catches "this axis is granted nowhere under this key space", which is the
shape all six instances of the class had. It does NOT catch "granted, but too
narrowly", and it does not reach tenant-rooted (`t/...`) key spaces at all:
those are composed by the constructors in `crates/ravel-commit/src/keys.rs`
through `format!("{prefix}{...}")` chains whose components are const
interpolations and match-arm literals, and deriving a glob from them soundly
needs constant folding rather than a text scan. The per-lifecycle reachability
tests remain the precise check, and the tables above remain the record.
