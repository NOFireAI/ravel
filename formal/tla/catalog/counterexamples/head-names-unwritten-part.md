# head-names-unwritten-part

Negative control. Switch `HeadNamesUnwrittenPart = TRUE` waives the CAS
precondition that the snapshot part be published before HEAD names it.

Violated invariant: `HeadNamesOnlyCompleteParts` (safety, TLC exit 12).

## Trace shape

1. Ingest publishes an L0 commit, the clock advances past the fold seal, and a
   folder starts a fold, computing a non-empty staged entry set for the sealed
   watermark.
2. The folder skips `DoFoldPutPart`: no part is written to `snapParts`.
3. `DoFoldCas` fires anyway (the switch makes `partWritten` unnecessary) and
   wins the create-if-absent, so HEAD becomes valid naming a `[wm, entries]`
   part that is absent from `snapParts`.

At the post-CAS state `head.status = "valid"` but
`[wm |-> head.wm, entries |-> head.entries] \notin snapParts`, so
`HeadNamesOnlyCompleteParts` is false.

## Why it is the right control

The correct protocol publishes the content-addressed part first
(`crates/ravel-catalog/src/fold.rs::part_object_key`) and only then CAS-swaps
HEAD, so a reader that follows HEAD always finds the object. Dropping the
ordering is exactly the dangling-HEAD bug the invariant forbids, and the model
falsifies it precisely because the ordering is otherwise enforced.
