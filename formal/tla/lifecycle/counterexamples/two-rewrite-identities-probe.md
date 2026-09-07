# Probes: the second rewrite identity is reachable (issue #1221)

Round eight added a second rewrite identity (`RewriteIds = {"A", "B"}`) and a
second rewrite output object (`RewriteOut = {"rwA", "rwB"}`). Every existing
safety invariant is now quantified over both, and both hold, so the question a
passing run cannot answer on its own is whether the new behaviours are reached
at all rather than pruned by a guard. Three probes, each an invariant asserting
the behaviour is impossible, each expected to be VIOLATED. All three run
against a scratch copy of the model and `smoke.cfg` under `/tmp` with every
switch at its shipped value, TLC 1.7.4, `-workers 2 -Xmx2g`.

## Probe 1: two identities hold a listing at the same time

```tla
ProbeNoTwoIdentitiesListed ==
    ~(rwPhase["A"] = "listed" /\ rwPhase["B"] = "listed")
```

```text
Error: Invariant ProbeNoTwoIdentitiesListed is violated.
```

TLC exit 12, `2075 states generated, 935 distinct states found`, depth 6.
Trace: `RequestErasure`, `StartRewrite("A")`, `ExpireLease`,
`StartRewrite("B")`. This is the race ADR-0065 permits and issue #1221 asks
about: identity B starts after A's ownership window has expired and before A
has published anything, so both hold a resolved input set at once. Neither has
a fencing token to lose, because there is none (ADR-0065 decision 2, rejected
alternative 1).

`StartRewrite`'s lease conjunct is `leaseOwner \in {"none", id}`, so B cannot
take the listing while A still owns the lease; the expiry is what admits it.
That makes `ExpireLease` load-bearing for this probe rather than decoration,
and it is deliberately left unfair in `FairSpec` so liveness does not depend on
a lease expiring.

## Probe 2: the second rewrite output is materialised

```tla
ProbeRwBUnreachable == ~PresentObj("rwB")
```

```text
Error: Invariant ProbeRwBUnreachable is violated.
```

TLC exit 12, `19573 states generated, 7031 distinct states found`, depth 8.
Trace: `RequestErasure`, `StartCompaction`, `ExpireLease`,
`PublishCompaction`, `StartRewrite`, `PublishRewrite`. `TargetOf` maps a
resolved input set of `CompactOut` to `rwB`, so `rwB` is the rewrite of a
compaction output, and it is reached whenever the compaction lands before the
rewrite pass lists.

## Probe 3: a rewrite supersedes a derived record set

```tla
ProbeNoRewriteOfCompaction ==
    ~(PresentObj("rwB") /\ "cmpA" \in superseded)
```

```text
Error: Invariant ProbeNoRewriteOfCompaction is violated.
```

TLC exit 12, `18833 states generated, 6775 distinct states found`, depth 8.
Trace: `RequestErasure`, `StartCompaction`, `PublishCompaction`, `ExpireLease`,
`StartRewrite`, `PublishRewrite`.

This closes the gap round seven recorded and could not close. That round found
that `RecordSetContent`'s read of the live `objContent` (rather than the static
`InitContent`) was unexercised, because `RewriteOut` named exactly one object,
`Predecessors("rwA")` was fixed to `RawInputs`, raw-input content is immutable
by assumption, and no action produced a second record set for a further rewrite
to consume. With a compaction output in the model, `Predecessors("rwB") =
CompactOut`, and `objContent["cmpA"]` is written by `PublishCompaction` rather
than by `Init`, so the current-state read is now the only read that gives the
right answer. `RawInputContentAssumedImmutable` still pins the raw inputs.
