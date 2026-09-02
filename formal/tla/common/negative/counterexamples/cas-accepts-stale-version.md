# Counterexample: CAS accepts a stale or absent version

Config: `negative/cas-accepts-stale-version.cfg`
(`CasAcceptsStale = TRUE`). Expected: `CasNeedsFreshVersion` violated,
TLC exit 12.

## What the switch breaks

A CasVersion put must apply only when the caller's expected version equals
the key's current version; otherwise the store returns PreconditionFailed
and applies nothing. The switch makes `DoCas` fall through to a plain
overwrite when the version is stale or the key is absent, and sets the ghost
flag `casStale`.

## Why it is a real bug if it ever held

`CasNeedsFreshVersion` is simply `~casStale`: no successful CAS ever ran
against a version that was not current. The switch drives `casStale` to
TRUE, so the invariant fails. A store with this bug would let two racing
compare-and-swap callers both "win" against the same base version, losing
one update: the exact lost-update race CAS exists to prevent. TLC reaches
the violating state after one write followed by a CAS quoting the wrong
version.

The correct model (switch FALSE) passes: a stale CAS is a no-op
(PreconditionFailed), so `casStale` stays FALSE.
