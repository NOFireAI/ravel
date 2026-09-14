# ADR-0531: What "first public release" means for the format-lifecycle policy

Status: Proposed (recommendation; needs owner confirmation before the naming
half lands)

This ADR decides nothing about any byte of any format. It settles a naming
question the format-lifecycle ADRs left ambiguous, and records the rollback
stance those ADRs never wrote down. It is issue #531 (documentation
reconciliation) and the second unlanded half of issue #530 / #1775 (a
documented rollback stance).

## Context

Three format-lifecycle documents pivot on one phrase, "first public release,"
and they no longer agree on what it triggered:

- ADR-0027 (Status line and decision 7) says its single-supported-version,
  delete-the-old-reader-in-the-same-change, no-migration regime "expires at the
  first public release," at which point ADR-0066's N/N-1 reader window and the
  decode-and-re-encode migration primitive take over.
- ADR-0066 decision 1 says that "from first public release onward" the bulk
  data-object formats (Class A) get an N/N-1 reader window, and deleting a
  version's read support is legal only once every bucket's recorded format floor
  is >= N. Until first release, "ADR-0027 stands unchanged; this ADR's machinery
  lands exercised by tests and dry-runs."
- CHANGELOG.md declares 0.9.0 the "First public release."

Read literally and together, those three say the N/N-1 window was in force from
0.9.0. But the shipped binary did the opposite after 0.9.0:

- 0.10.0 introduced RSEG v7 and deleted the v6 reader outright, with no
  migration path, explicitly "under the pre-release single-version policy
  (ADR-0027)" (ADR-0092 migration-class header and decision 7; CHANGELOG 0.10.0
  entry: "There is no v6 reader and no v6-to-v7 migration path").
- 0.11.0 deleted the RLOG v3 reader the same way, again citing ADR-0027
  decision 7 and ADR-0066 decision 1.
- At HEAD, `ravel_segment::SUPPORTED_VERSIONS` holds exactly one version (v7);
  the N/N-1 machinery exists and is exercised by tests but carries no live
  second version.

So an operator who reads the ADRs and the format docs concludes stored data
survives upgrades, upgrades 0.9.x to 0.10.0, and finds every stored metrics
segment failing typed `UnsupportedVersion`, after which retention's version
hold (issue #530, ADR-0066 decision 2) declines to delete the now-unreadable
objects but cannot make them readable again. That is the harm this
reconciliation exists to remove.

The genuine question underneath is whether "first public release" in the
lifecycle ADRs means:

- **Reading A: 0.9.0**, as the changelog states. Then the N/N-1 window has been
  in force since 0.9.0, the 0.10.0 v6 deletion and the 0.11.0 RLOG v3 deletion
  were policy violations that shipped, and the project owes a retroactive
  correction and a migration path for any 0.9.x store that upgraded.
- **Reading B: a distinct, not-yet-reached format-lifecycle activation
  milestone**, separate from the software's first public release at 0.9.0. Then
  the format lifecycle is still under ADR-0027's pre-release regime at HEAD,
  ADR-0066's window is staged (machinery present, exercised by tests and
  dry-runs) but not yet governing, and the 0.10.0 and 0.11.0 deletions were the
  intended pre-release behavior, not violations.

Only Reading B is consistent with what the binary actually did. ADR-0092
decision 7 is explicit that v7's single-version window is "the intended
pre-release behavior, not a gap," and that a real two-version window would be a
deliberate future flip. Reading A would mean two shipped releases silently broke
the policy their own changelog entries cite, and it makes every existing 0.10.0
and 0.11.0 changelog entry contradict the policy; Reading B makes those entries
correct as written and requires editing no changelog history.

## Decision (recommended, pending owner confirmation)

1. For the format-lifecycle policy only, "first public release" in ADR-0027
   (decision 7, Status line) and ADR-0066 (decision 1) denotes a **format-
   lifecycle activation milestone that has not yet occurred**. It is distinct
   from the software's first public release (0.9.0, CHANGELOG.md), which is a
   marketing/packaging fact about the binary, not a guarantee about on-object
   format stability. The activation milestone is declared explicitly, in its own
   reviewed change, when the project commits to carrying real dual versions.

2. Until that milestone is declared, ADR-0027's pre-release regime remains in
   force for the bulk data-object formats: exactly one supported version at a
   time, the previous version's read and write support deleted in the same
   change that introduces the new one, and no in-place migration across a bump.
   ADR-0066's N/N-1 machinery stays staged and exercised by tests and dry-runs,
   as decision 1 already says for the pre-release period.

3. This confirms, rather than reverses, ADR-0092 decision 7 and the 0.10.0 and
   0.11.0 reader deletions: they acted correctly under the pre-release regime,
   because the activation milestone had not (and has not) occurred.

The owner must confirm point 1 before the reconciling edits to ADR-0027,
ADR-0066, and ADR-0092 land. If the owner instead chooses Reading A, this ADR is
withdrawn and a separate change records the retroactive-correction obligation
for the shipped 0.10.0/0.11.0 deletions; the operator-facing posture edits in
the format docs (below) hold under either reading and are not gated on the
choice.

## Rollback stance (issue #530 / #1775, the second unlanded bullet)

This is the decision to record, not only a procedure to write. Under the regime
in force at HEAD (Reading B, point 2 above):

- **What is safe to roll back to.** A build may be rolled back to any earlier
  build whose supported-version window still contains every on-object version
  present in the store. Because writers only ever emit the current version and
  each bump deletes the previous reader in the same change, this in practice
  means: a rollback is safe only across builds that share the same on-object
  format version. Rolling an object store forward through a format bump and then
  rolling the binary back to a build that predates the bump is safe **only if no
  object was written at the new version**, since the old build cannot read the
  new version.

- **The irreversible step, after which rollback is not safe.** The first write
  at the new format version. Once any object at version N exists in the store, a
  build whose window tops out at N-1 cannot read it and will fail closed with
  typed `UnsupportedVersion`. Retention's version hold (issue #530) will decline
  to delete such an object under a build that cannot read it, so the object is
  not lost, but it is not queryable until a build whose window includes its
  version runs again. A format bump is therefore a **non-rollbackable
  data-migration event** under the current regime: plan it as forward-only.

- **When this stance changes.** When the activation milestone (point 1) is
  declared and the reader window moves to N/N-1, a single bump becomes rollback-
  safe across exactly one version boundary, and the `maintain migrate` primitive
  can convert a store forward before the old reader is retired. The floor-raise
  ordering that makes that safe (a version's read support is deleted only once
  every bucket's format floor exceeds it) is exercised end to end in
  `crates/ravel-maintain/src/migrate.rs`; that machinery is ready, but it is not
  the governing posture until the milestone is declared.

## Consequences

- The operator-facing format docs (docs/segment-format.md,
  docs/span-segment-format.md, README.md) state the HEAD posture plainly: the
  reader admits exactly one version, a format bump is a non-rollbackable
  data-migration event, and the N/N-1 window is staged but not yet in force.
  These edits are factual at HEAD and hold under either reading of "first public
  release."

- ADR-0027, ADR-0066, and ADR-0092 each gain a pointer to this ADR at the
  "first public release" reference, so a reader meeting the ambiguous phrase is
  sent to the decision record rather than left to guess. Their core decisions
  are not rewritten here; that waits on owner confirmation of point 1.

- A documentation/code consistency check
  (scripts/check-format-version-docs.py, wired into scripts/gates.sh and CI)
  ties the version claims in the format docs to each crate's
  `SUPPORTED_VERSIONS`, so the docs and the reader's admitted set cannot silently
  diverge again. The check fails closed: if it cannot find a version claim to
  compare on either side, that is a failure, not a pass.

- ADR-0092 decision 7 still names the constructors `SupportedVersions::single`
  and `SupportedVersions::n_and_prev`. Those were replaced by the
  `SegmentVersion::WINDOW` single-source model in the issue #530 change (see the
  ADR-0066 #530 amendment); the names in ADR-0092 are stale but its decision is
  unchanged. Recorded here rather than rewritten into the historical ADR.
