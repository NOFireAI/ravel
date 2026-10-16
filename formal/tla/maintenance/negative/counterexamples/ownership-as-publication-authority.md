# ownership-as-publication-authority

Switch: `OwnerPublishOverwrite = TRUE`. Expected: `QueryVisibleDataCorrectUnderDuplicateOwnership` violated (exit 12).

Trace: worker 1 puts the part of variant `iA`, then publishes the terminal
record of unit 1 with content `<<1, iA>>` -- the CreateIfAbsent winner, latched
in `firstRecord[1]`. Worker 1 (still an in-view owner) then takes the broken
`BrokenOwnerPublish` step with variant `iB`: because the switch replaces the
record's CreateIfAbsent with Overwrite, the record's content becomes
`<<1, iB>>` while `firstRecord[1]` remains `<<1, iA>>`. The invariant's clause
`Present(rec) => ContentOf(rec) = firstRecord[u]` fails.

Why it matters: ownership must not be publication authority. Once ownership can
overwrite the record, a duplicate owner (or the same owner with a divergent
input set) mutates query-visible data. The shipped publish path is
CreateIfAbsent, which keeps the record immutable.
