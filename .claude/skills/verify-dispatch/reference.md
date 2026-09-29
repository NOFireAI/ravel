# verify-dispatch: why each check exists

Background for SKILL.md.

## Defects that shipped behind an executor's "gates green"

- A cross-crate field rename that broke an untouched crate's build. A
  crate-scoped `-p` gate run is what let it through, which is why tier 1
  is always workspace-wide.
- A stale test fixture after a type gained a required field.
- A branch that did not compile once the incremental cache was
  invalidated, which is why tier 1 uses a fresh `CARGO_TARGET_DIR`.
- An unguarded array index that panics on corrupt but plausible input:
  a `usize::try_from` rejected negative keys but not out-of-range positive
  ones, and the panic surfaced only once a property test was added.
- A grouped `MIN`/`MAX` accumulator with the wrong float ordering, the
  same bug DataFusion's own grouped `MIN`/`MAX` shipped: a NaN poisoned
  later comparisons, `-0.0` and `0.0` compared equal so arrival order
  picked the winner, and an all-infinite group never displaced the seed.
- An error-redaction catch-all that reported real 422s as fake 503s.
- A format writer that dropped a sort invariant its own reader required.
  Order drift like this fails no test; it fails a byte-size or
  performance gate much later, far from the change.
- A result branch that deleted diagrams and their references from
  unrelated docs with every CI gate passing; only a scope comparison
  catches that.

## Vacuous test shapes seen in practice

A 370-byte segment "testing" the paged-fetch path that only runs above
`DEFAULT_WHOLE_OBJECT_THRESHOLD`; one tenant hash reused across cases that
claimed cross-tenant separation; a tie-break test on an input so small the
unfixed code passed it. A vacuous test found after merge costs a full
extra dispatch round to replace.

## Why tier 2 never acts on its own

Apart from the cross-crate build break, the defects above needed a
dedicated audit or a differential test harness to surface, not a generic
reviewer. A narrow subagent check is a lead, not a verdict, and a generic
"find bugs" pass produces plausible findings that each have to be
disproven.
