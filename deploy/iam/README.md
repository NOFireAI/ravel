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

### Erasure-request objects: `t/*/*/del/*.dreq`

ADR-0064 section 6: "Maintain gains delete on `del/*.dreq` **only**", and
"`del/*.done` joins the deny-delete set for every role including Maintain".

The erasure-request sweep runs under the Maintain role and retires a request
object at `t/<tenant_hash>/<signal>/del/<request_id>.dreq` once its erasure is
complete, past the post-completion protection horizon, and no longer held by a
legal hold or a still-resolvable superseded input
(`crates/ravel-maintain/src/sweep.rs`). IAM is default-deny, so until this
grant was added the shipped template refused that delete on every request
object: completed `.dreq` objects accumulated and the query-time exclusion
filter that reads them grew without bound.

The pattern is `*.dreq`, not `del/*`, because the completion records
(`del/<request_id>.done`) are permanent erasure evidence and no role may delete
them. `maintain_template_grants_delete_on_erasure_requests` in
`crates/ravel-commit/tests/iam_templates.rs` asserts both halves against real
key constructors, and
`every_role_grants_exactly_the_expected_pattern_set` pins the resource list
above by exact equality.

An operator who applied `maintain.json` before this grant existed must
re-apply it: the backlog of refused requests stays in the store until the
Maintain credential actually holds the delete.
