# Counterexample: lost-response write drops its effect

Config: `negative/lost-response-not-applied.cfg`
(`LostResponseDropsEffect = TRUE`). Expected: `ReadAfterWrite` violated,
TLC exit 12.

## What the switch breaks

A LostResponse is an acknowledgement failure: the write reached durable
storage but the caller saw a network error on the response. The correct
model applies the effect and records it in `lastWritten`, so a later read
observes it. The switch makes `DoLostOverwrite` skip the store mutation
(`UNCHANGED mcStoreVars`) while still recording the content in
`lastWritten`, modeling a caller that assumes the write never landed and a
store that in fact dropped it.

## Why it is a real bug if it ever held

`ReadAfterWrite` says: for every key, if `lastWritten[k]` names content,
then the key is present with that content. The switch produces a state where
`lastWritten[k]` names content the store never holds. Any code that treats a
lost response as "not written" and skips a retry would silently lose an
acknowledged-durable write. TLC reaches the violating state after a single
`DoLostOverwrite`.

The correct model (switch FALSE) passes: a lost response applies the effect,
so retrying is safe and a read always sees the write.
