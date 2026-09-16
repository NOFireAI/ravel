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
  `t/*/*/l0/*`, `t/*/*/c/*`, `t/*/*/l1/*`, `t/*/*/idem/*`, and
  `t/*/u/*/0001/*`. These are the objects the compaction, supersession, and
  retention sweeps physically remove.

### A second gap: erasure-request objects

`MaintainDelete` grants no `t/*/*/del/*`. The erasure-request sweep runs under
the Maintain role and deletes the request objects it has completed at
`t/<tenant_hash>/<signal>/del/<request_id>.dreq`
(`crates/ravel-maintain/src/sweep.rs`). IAM is default-deny, so with no `Allow`
covering that prefix the shipped Maintain template refuses that delete and the
completed `.dreq` object is left in place. That is a second tension between the
shipped templates and the sweeps that run under them, alongside the
`t/*/catalog/*/*` one above, and is tracked separately.
