# Mutant: a multipart part publishes into the store before Complete

An adversarial mutant of `RavelObjectStore.tla`, reverted after the run.

## The edit

Make `MultipartPart` publish the staged content into the durable store while
the upload is still active:

    MultipartPart(u, c) ==
        /\ uploads[u].active
        /\ uploads' = [uploads EXCEPT ![u].content = c]
        /\ store' = [store EXCEPT ![uploads[u].key].present = TRUE,
                                  ![uploads[u].key].content = c]
        /\ UNCHANGED <<lastModified, versionCounter, listState>>

## The run and the TLC line

Because `MultipartInvisibleUntilComplete` is listed ninth in `smoke.cfg`,
`ReadAfterWrite` (listed fifth) is reported first for this mutant. To pin the
multipart invariant specifically, the mutant was run against a scratch config
that declares only that invariant:

    INVARIANT MultipartInvisibleUntilComplete

TLC exit 12, with:

    Error: Invariant MultipartInvisibleUntilComplete is violated.

The invariant compares `store[uploads[u].key]` against `mpBegin[u]`, the store
record captured at begin, so an early publish is a real store delta the
invariant sees, not a ghost flip. The correct model keeps parts in the
ephemeral `uploads` variable and only `MultipartComplete` calls `WriteVisible`.
