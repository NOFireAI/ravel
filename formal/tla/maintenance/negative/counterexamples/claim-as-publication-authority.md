# claim-as-publication-authority

Switch: `ClaimIsPublicationAuthority = TRUE`. Expected: `ClaimGrantsNoPublicationAuthority` violated (exit 12).

Trace: worker 1 acquires the claim of unit 1 and puts the parts of both variants.
Holding the claim, it takes the broken `BrokenClaimPublish` step with variant
`iA`, winning the record (`firstRecord[1] = <<1, iA>>`). Still holding the claim,
it takes a second `BrokenClaimPublish` with variant `iB`: because the switch lets
a claim holder skip CreateIfAbsent and Overwrite the record, the record's content
becomes `<<1, iB>>` while `firstRecord[1]` stays `<<1, iA>>`. The clause
`Present(rec) => ContentOf(rec) = firstRecord[u]` fails.

Why it matters: a claim is advisory (ADR-1029 decision 2). The moment a claim
grants the right to Overwrite the record, two holders (a paused stale holder and
the thief that took over) publish divergent records. The publish path must never
read the claim; it stays CreateIfAbsent.
