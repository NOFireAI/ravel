# ADR-0531: What "first public release" means for the format-lifecycle policy

Status: Accepted (2026-09-19)

This ADR decides nothing about any byte of any format. It settles a naming
question the format-lifecycle ADRs left ambiguous, and records the rollback
stance those ADRs never wrote down. It is issue #531 (documentation
reconciliation) and the second unlanded half of issue #530 / #1775 (a
documented rollback stance).

The owner decided on 2026-09-19, on the reading recommended below (Reading B),
and named the milestone: **the format-lifecycle activation milestone is the
v1.0 release: until v1.0 ships, a bulk data-object format change may break
backward compatibility outright rather than carry a reader window, so
ADR-0066's N/N-1 window is not opened before v1.0 and the supported-version
window stays at exactly one version.** Decision point 1 below states that in
full; points 2 and 3 follow from it unchanged.

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
- 0.12.0 deleted the RLOG v3 reader the same way, again citing ADR-0027
  decision 7 and ADR-0066 decision 1 (CHANGELOG 0.12.0 Removed).
- 0.11.0 is the one deviation, and it cuts against the pre-release regime
  rather than for it: it introduced RLOG v4 while keeping the v3 reader, so
  that release shipped a two-version reader ("The reader accepts v3 and v4;
  writers emit v4", CHANGELOG 0.11.0). ADR-0027 decision 7 does not permit
  that, and no ADR authorised it at the time. ADR-0892 later closed the window
  and treats it as an unretired predecessor reader, not as the N/N-1 policy
  starting early: its line 56 says the window "is not yet in force" in the same
  document that describes the v3+v4 reader.
- At HEAD, `ravel_segment::SUPPORTED_VERSIONS` holds exactly one version (v7);
  the N/N-1 machinery exists and is exercised by tests but carries no live
  second version.

So an operator who reads the ADRs and the format docs concludes stored data
survives upgrades, upgrades 0.9.x to 0.10.0, and finds every stored metrics
segment failing typed `UnsupportedVersion`. At that release the outcome was
worse than unreadable: retention's version hold did not exist yet, so the
horizon-gated sweep deleted those objects on their ordinary age schedule. The
hold (issue #530, ADR-0066 decision 2) is in `## [Unreleased]` and has shipped
in no release; at HEAD it would keep the metrics objects, unreadable but not
lost. That is the harm this reconciliation exists to remove.

The genuine question underneath is whether "first public release" in the
lifecycle ADRs means:

- **Reading A: 0.9.0**, as the changelog states. Then the N/N-1 window has been
  in force since 0.9.0, the 0.10.0 v6 deletion and the 0.12.0 RLOG v3 deletion
  were policy violations that shipped, and the project owes a retroactive
  correction and a migration path for any 0.9.x store that upgraded.
- **Reading B: a distinct, not-yet-reached format-lifecycle activation
  milestone**, separate from the software's first public release at 0.9.0
  (the owner chose this reading on 2026-09-19 and fixed the milestone at the
  v1.0 release). Then
  the format lifecycle is still under ADR-0027's pre-release regime at HEAD,
  ADR-0066's window is staged (machinery present, exercised by tests and
  dry-runs) but not yet governing, and the 0.10.0 and 0.12.0 deletions were the
  intended pre-release behavior, not violations. 0.11.0's two-version reader is
  a deviation under either reading and is treated separately below.

Only Reading B is consistent with what the binary actually did. ADR-0092
decision 7 is explicit that v7's single-version window is "the intended
pre-release behavior, not a gap," and that a real two-version window would be a
deliberate future flip. Reading A would mean two shipped releases silently broke
the policy their own changelog entries cite, and it makes every existing 0.10.0
and 0.12.0 changelog entry contradict the policy; Reading B makes those entries
correct as written and requires editing no changelog history.

0.11.0 is not evidence for either reading, and it should not be read as
support for this recommendation. Shipping a v3+v4 reader is outside ADR-0027
decision 7 under Reading B and outside ADR-0066's readers-before-writers
sequencing under Reading A. It was closed in the next minor release by
ADR-0892, which describes the window as unretired rather than as policy, so it
does not establish that the N/N-1 window ever opened. Point 2 below was
confirmed in the knowledge that one shipped bump deviated from the regime it
confirms.

## Decision

1. For the format-lifecycle policy only, "first public release" in ADR-0027
   (decision 7, Status line) and ADR-0066 (decision 1) denotes the **v1.0
   release**, which has not happened. It is distinct from the software's first
   public release (0.9.0, CHANGELOG.md), which is a marketing/packaging fact
   about the binary, not a guarantee about on-object format stability. Until
   v1.0 ships, Ravel may break backward compatibility: a bulk data-object
   format change may delete the previous version's reader outright rather than
   carry a reader window, and stored objects at the retired version become
   unreadable. What that means for ADR-0066's N/N-1 reader window is exact: the
   window is **not opened before v1.0**, and the supported-version window stays
   at exactly one version (`SUPPORTED_VERSIONS` one entry wide per Class A
   format) until the v1.0 release. Opening it is a separate reviewed change
   made at v1.0, not an event that happens on its own.

2. Until v1.0, ADR-0027's pre-release regime remains in force for the bulk
   data-object formats: exactly one supported version at a time, the previous
   version's read and write support deleted in the same change that introduces
   the new one, and no in-place migration across a bump. ADR-0066's N/N-1
   machinery stays staged and exercised by tests and dry-runs, as decision 1
   already says for the pre-release period.

3. This confirms, rather than reverses, ADR-0092 decision 7 and the 0.10.0 and
   0.12.0 reader deletions: they acted correctly under the pre-release regime,
   because v1.0 had not (and has not) shipped. It says nothing about 0.11.0's
   two-version RLOG reader, which no reading of the phrase authorises and which
   ADR-0892 has already closed.

Reading A is therefore rejected: the shipped 0.10.0 and 0.12.0 reader deletions
carry no retroactive-correction obligation, and no changelog history is edited.
The operator-facing posture edits in the format docs (below) were factual at
HEAD under either reading and are not affected by the choice.

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
  typed `UnsupportedVersion`. For metrics (RSEG), retention's version hold
  (issue #530) declines to delete such an object under a build that cannot read
  it, so the object is not lost, but it is not queryable until a build whose
  window includes its version runs again. **That hold covers metrics only.**
  `held_out_of_window` returns early for any other signal
  (`crates/ravel-maintain/src/retention.rs:590`), so logs (RLOG) and spans
  (RSPAN) keep the unconditional horizon-gated sweep until the remaining half of
  issue #530 lands. For those two the irreversible step is a data-loss risk and
  not only a queryability one: an object written at the new version and left
  unreadable by a rolled-back build is deleted once its retention horizon
  elapses. A format bump is therefore a **non-rollbackable data-migration
  event** under the current regime: plan it as forward-only, and for logs and
  spans treat the rollback window as bounded by the retention horizon.

- **When this stance changes.** At the v1.0 release (point 1), when the reader
  window moves to N/N-1, a single bump becomes rollback-
  safe across exactly one version boundary, and the `maintain migrate` primitive
  can convert a store forward before the old reader is retired. The floor-raise
  ordering that makes that safe (a version's read support is deleted only once
  every bucket's format floor exceeds it) is exercised end to end in
  `crates/ravel-maintain/src/migrate.rs`; that machinery is ready, but it is not
  the governing posture until v1.0 ships.

## Consequences

- The operator-facing format docs (docs/segment-format.md,
  docs/log-segment-format.md, docs/span-segment-format.md, README.md) state the
  HEAD posture plainly: the
  reader admits exactly one version, a format bump is a non-rollbackable
  data-migration event, and the N/N-1 window is staged and does not open before
  v1.0. Those docs and docs/guides/operations/maintenance.md name v1.0 as the
  milestone, so the rule an operator reads is a date-free but concrete one
  rather than an undecided event.

- ADR-0027, ADR-0066, ADR-0092, and ADR-0892 each carry a pointer to this ADR at
  the "first public release" reference, so a reader meeting the ambiguous phrase
  is sent to the decision record rather than left to guess, and each pointer
  names v1.0. ADR-0892 is not a passing mention of the phrase: it is the ADR
  that deleted a shipped reader on the strength of it, and
  docs/log-segment-format.md sends readers there by name. Their core decisions
  are not rewritten: this ADR fixes what their milestone phrase denotes, nothing
  else.

- A documentation/code consistency check
  (scripts/check_format_version_docs.py, wired into scripts/gates.sh and CI)
  ties the version claims in the format docs to each crate's
  `SUPPORTED_VERSIONS`, so the docs and the reader's admitted set cannot silently
  diverge again. The check fails closed: if it cannot find a version claim to
  compare on either side, that is a failure, not a pass.

- ADR-0092 decision 7 still names the constructors `SupportedVersions::single`
  and `SupportedVersions::n_and_prev`. Those were replaced by the
  `SegmentVersion::WINDOW` single-source model in the issue #530 change (see the
  ADR-0066 #530 amendment); the names in ADR-0092 are stale but its decision is
  unchanged. Recorded here rather than rewritten into the historical ADR.

- Two doc comments in the format crates still carry the ambiguous phrase and
  read, after 0.9.0 shipped, as if the window were already due to open:
  `SupportedVersions::n_and_prev` is "Reserved for first public release"
  (`crates/ravel-logseg/src/footer.rs`), and RSEG's `SUPPORTED_VERSIONS` is
  "ready for the first post-release bump" (`crates/ravel-segment/src/format.rs`).
  Under this ADR both mean v1.0. No code behaviour is affected, and no code is
  changed here; the wording is recorded as a follow-up.
