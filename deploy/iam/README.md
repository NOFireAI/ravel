# IAM policy templates

Four per-role templates: `gateway.json`, `query.json`, `maintain.json`,
`admin.json`. Replace `my-ravel-bucket` and the KMS key placeholder before
use. `crates/ravel-commit/tests/iam_templates.rs` pins the resource and
action set of every statement in all four files; a hand edit that drifts
from the pinned shape fails that suite.

## Commit records (`t/*/*/c/*`) are deletable by design

`DenyDeleteProtected` in `maintain.json` denies delete on `sys/tenancy`,
`sys/qualification`, `sys/gc`, `t/*/*/prov`, `t/*/catalog/*/*`, and the
legal-hold audit shard (`t/*/u/*/0000/*`). Commit records are absent from
that list on purpose: `MaintainDelete` grants delete on `t/*/*/c/*`
because the maintenance sweep physically removes a commit record once
it is superseded, and an IAM deny there would make every sweep pass fail.

The `t/*/catalog/*/*` entry in that deny list is in tension with the same
argument: the unreferenced-catalog sweep runs under the Maintain role and
deletes the superseded snapshot and index objects the pattern covers, so the
shipped templates refuse those deletes outright rather than merely delaying
them. That tension is real and is tracked separately.

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
  and `t/*/*/del/*.dreq`. These are the objects the compaction, supersession,
  retention, and erasure-request sweeps physically remove.

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
hold or a still-resolvable superseded input. One pass makes three object-store
calls on the `del/` prefix, and each needs its own grant:

| Call in `sweep.rs` | S3 operation | Grant |
|---|---|---|
| `list_all(store, &keys::del_prefix(tenant, signal))` | `s3:ListBucket` with `prefix=t/<tenant_hash>/<signal>/del/` | `MaintainList` `s3:prefix` `t/*/*/del/*` |
| `store.get(&meta.key, GetRange::Full)` on each listed `.done` | `s3:GetObject` | `MaintainRead` `t/*/*/del/*.done` |
| `store.delete(dreq_key)` | `s3:DeleteObject` | `MaintainDelete` `t/*/*/del/*.dreq` |

IAM is default-deny and the calls run in that order, so the delete grant alone
does nothing: the pass is refused with `AccessDenied` on the `ListBucket`
before it reaches a single request object. The read grant is needed for the
same reason one step later, because the completion record carries the
timestamp the protection horizon is measured from and the sweep decodes it on
every completed request.

The grant scopes are narrower than the ADR's `del/**` read, and deliberately
so: `*.done` and not `del/*` on the read, because the sweep parses request ids
out of the listed `.dreq` KEYS and never fetches a `.dreq` body; `*.dreq` and
not `del/*` on the delete, because completion records are permanent erasure
evidence and no role may delete them.

`maintain_template_covers_every_erasure_request_sweep_call` in
`crates/ravel-commit/tests/iam_templates.rs` asserts each row of that table
against the key constructor the call uses, and asserts that no pattern
reaching `del/` reaches anything outside it.
`maintain_template_grants_delete_on_erasure_requests` asserts the `.done` and
`del/`-prefix exclusions on the delete side, and
`every_role_grants_exactly_the_expected_pattern_set` pins all three pattern
strings by exact equality.

An operator who applied a copy of `maintain.json` older than all three grants
must re-apply it. Re-applying restores the sweep's ability to run: on the next
pass it lists the prefix, reads each completion, and deletes every request
object whose protection horizon has elapsed and that no hold retains, so the
backlog drains over the passes that follow rather than instantly. Requests
whose horizon has not elapsed, or that a legal hold or a still-resolvable
superseded input holds, are kept by design and are not part of that backlog.
Re-applying a copy that carries the delete but not the list and read grants
clears nothing at all: the pass is still refused at the `ListBucket`.

Two known gaps in this template remain open and are NOT closed by the grants
above. Both are tracked separately; neither is created by this change.

- `pending_erasure_requests` in
  `crates/ravel-maintain/src/erasure_rewrite.rs` GETs each pending `.dreq`
  body to plan the rewrite. `MaintainRead` reaches `.done` only, so the
  rewrite pass is refused on that read.
- `query.json` grants no list or read under `del/`, while the resolver LISTs
  `t/<tenant_hash>/<signal>/del/` per resolve to attach pending predicates
  (`crates/ravel-commit/src/keys.rs`, `del_prefix`). ADR-0064's same bullet
  gives Query that read.
