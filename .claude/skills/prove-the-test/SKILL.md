---
name: prove-the-test
description: Use when writing a regression test, a fault-injection test, a differential or property test, or when citing any test as evidence that a bug is fixed or a path is covered - before the claim, not after review finds the test vacuous
---

# Prove the test can fail

A test proves something only after you watch it fail for the stated
reason. A test that cannot detect the defect it is named for passes
review, ships, and then costs extra fleet dispatches and review rounds
when the "covered" path turns out to be untested.

## The one required step

Before you cite a test as proof, break the code and watch the test catch
it. Revert only the production fix (or flip the one line the test
guards), run the test, and record: the line you flipped, and the failure
message. If you cannot name a single production line that makes the test
fail, the test is vacuous. Rewrite it.

## Known vacuity shapes (each one has shipped in a real test)

- A fault-injection test that never asserts the fault fired. A
  `FaultStore` test must assert the occurrence counter, not only the
  call's success.
- A fixture below the threshold that gates the path under test. A
  370-byte segment "tested" the paged-fetch path that only runs above
  `DEFAULT_WHOLE_OBJECT_THRESHOLD`; the fetch loop never executed.
- One literal reused across cases that claim separation. Every call site
  passed `TenantHash([7u8; 16])`, so no test could catch a cross-tenant
  leak.
- An input too small to exercise the property. A tie-break test on two
  elements passed against the unfixed code, because unstable sort keeps
  order on short inputs.
- A multi-shard or multi-part scenario whose fixture fits in one shard
  or one block, so both compared sets are `{0}` and always equal.

## Red flags - stop and run the required step

- "The test passes, so the fix works."
- "I reasoned it is sound." (Reasoned-sound is where the defects live;
  proved-unsound gets different scrutiny.)
- "The fixture is smaller but the logic is the same."
- Reusing one constant everywhere because it is convenient.

When a spec asks for this proof (fleet-task-spec's "Soundness claims need
a failing test"), the report must name the flipped line. When you review
someone else's test, ask the same question: which line flips to make this
fail?
