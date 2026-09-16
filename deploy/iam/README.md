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
