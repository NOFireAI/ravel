# Counterexample: lost-response write drops its effect

Config: `negative/lost-response-not-applied.cfg`
(`LostResponseDropsEffect = TRUE`). Expected: `LostResponseEffectApplied`
violated, TLC exit 12.

Observed TLC line:

    Error: Invariant LostResponseEffectApplied is violated.

## What the switch breaks

A lost response is an acknowledgement failure: the write reached durable
storage but the caller saw a network error on the response. The correct model
applies the effect and the caller observes `Failure`. The switch makes
`DoLostOverwrite` skip the store mutation (`UNCHANGED mcStoreVars`) while still
reporting `lastOp.outcome = "Failure"` and the content it meant to write,
modeling a store that in fact dropped an acknowledged-durable write.

## Why it is a real bug if it ever held

`LostResponseEffectApplied` reads the store: a `lostOverwrite` step whose
outcome is `Failure` must still leave `lastOp.after.present` with the written
content. The switch produces a `Failure` step whose store record never changed,
so the invariant fails (and `ReadAfterWrite` fails too, since `lastWritten`
names content the store never holds). Code that treats a lost response as "not
written" and skips a retry would silently lose an acknowledged-durable write.
TLC reaches the violating state after a single `DoLostOverwrite`.

The invariant is listed before `ReadAfterWrite` in the negative cfg so TLC
reports `LostResponseEffectApplied` first. The correct model (switch FALSE)
passes: a lost response applies the effect, so retrying is safe.
