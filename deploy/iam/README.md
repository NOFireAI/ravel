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
hold or a still-resolvable superseded input. The lifecycle makes five
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

One scope is narrower than the ADR's `del/**`: the delete is `*.dreq` and not
`del/*`, because completion records are permanent erasure evidence and no role
may delete them. The read is `del/*` as the ADR writes it, because both object
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
