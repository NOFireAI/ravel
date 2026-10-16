# Negative control: compaction-ignores-rewrite

Switch: `CompactionIgnoresRewrite = TRUE` (the compactor publishes a record set
without checking its listing for a live erasure rewrite record, dropping the
`CompactionOutcome::RewritePresent` refusal). All other switches at base, and in
particular `SerializeCompactionAndRewrite` stays `TRUE`, so the control isolates
the listing guard rather than also removing the driver's per-bucket
serialisation. This is the model half of issue #1289.

Target invariant: `AtMostOneLiveRecordSetServed`. TLC exit 12.

```text
Error: Invariant AtMostOneLiveRecordSetServed is violated.
```

`25156 states generated, 8600 distinct states found`, depth 9, finished in 1s.

Trace, seven states:

1. `Init`. `raw1` present with `objContent["raw1"] = {rec1, rec2}`, `head =
   {raw1}`, nothing superseded.
2. `RequestErasure`. Writes `dreqR1`, `erasureRequested = {s1}`. `rec1` is the
   record whose subject is `s1`.
3. `StartRewrite("A")`. Takes the lease (`leaseOwner = "A"`), records
   `rwInputs[A] = {raw1}` from its listing, `rwPhase[A] = "listed"`.
4. `PublishRewrite("A")`. Writes `rwA` CreateIfAbsent with
   `objContent["rwA"] = {rec2}`, marks `superseded = {raw1}`.
5. `ExpireLease`. `leaseOwner = "none"`. The lease is a rendezvous-hash
   ownership window with no fencing token (ADR-0065 decision 2), so this is an
   environment step, not something either pass chooses.
6. `StartCompaction`. With the guard off, the compactor takes the lease and
   lists `raw1` as a live input despite `rwA` being present, so
   `cmpPhase = "listed"`.
7. `PublishCompaction`. Writes `cmpA` with
   `objContent["cmpA"] = {rec1, rec2}`.

At state 7 both `rwA` and `cmpA` are present, neither is superseded, and both
have a predecessor in `superseded` (`raw1`), so the invariant's cardinality is 2
and it fails. The store content shows the concrete harm rather than just the
count: `objContent["rwA"] = {rec1, rec2} \ {rec1} = {rec2}`, but
`objContent["cmpA"] = {rec1, rec2}`. `cmpA` re-derives the erased record from an
input the rewrite had already superseded, which is the resurrection ADR-0064
decision 3 point 5 forbids.

With the guard on (`CompactionIgnoresRewrite = FALSE`, the shipped value)
`StartCompaction` is disabled once any object in `RewriteOut` is present, state
6 has no successor along this path, and the invariant holds across both the
smoke and exhaustive configurations.

## What the guard does not cover

The refusal is a list-time observation with no compare-and-swap
(`cdce1722`), so it closes only the case where the rewrite record is already
durable when the compactor lists. The interleaving where the compactor lists
first and the rewrite publishes in between is not closed by the listing guard;
in the shipped system it is closed by the maintenance driver running at most
one of the two passes over a bucket at a time. That serialisation is a separate
modelled assumption, `SerializeCompactionAndRewrite`, and TLC shows it is
load-bearing rather than belt-and-braces.

Probe: a scratch copy of the model and `smoke.cfg` under `/tmp` with
`SerializeCompactionAndRewrite = FALSE` and `CompactionIgnoresRewrite = FALSE`,
so the shipped listing guard is left ON and only the driver's serialisation is
removed. TLC 1.7.4, `-workers 2 -Xmx2g`:

```text
Error: Invariant AtMostOneLiveRecordSetServed is violated.
```

TLC exit 12, `18885 states generated, 6954 distinct states found`, depth 8,
finished in 1s. Trace: `RequestErasure`, `StartRewrite("A")`, `ExpireLease`,
`StartCompaction`, `PublishRewrite("A")`, `PublishCompaction`. The compactor
lists while `rwPhase[A] = "listed"`, so its listing legitimately holds no
rewrite record and the guard has nothing to refuse on; `rwA` becomes durable
only afterwards, and the compactor then publishes `cmpA` over the same
now-superseded input.

So the guard's condition is not what keeps the pair safe when the two passes
overlap. `SerializeCompactionAndRewrite` is therefore not a negative control
switch (it does not isolate a guard the code implements at the object-store
level); it records which layer the safety comes from, and the answer is the
driver, not the store. If the driver ever runs compaction and erasure rewrite
concurrently over one bucket, the list-time refusal alone does not prevent two
live record sets over a superseded input, and the code needs a real fence: a
compare-and-swap on a per-bucket lease, or a compaction key a concurrent
rewrite publish can invalidate. This is the model's answer to issue #1289 for
the concurrent case; the Rust half is a separate change.
