# Counterexample: provider_idempotency authorizes a second attempt

Config: `negative/provider-reauthorizes.cfg`
(`ProviderReauthorizes = TRUE`; smoke bounds with
`Policies = {"provider_idempotency"}`).
Expected: `PolicyControlsRedispatch` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant PolicyControlsRedispatch is violated.

Trace (16 states, depth 16; 2114 distinct states found). Action names are
read off the state.

    State 1:  <Initial predicate>
    State 2:  CreateHead
    State 3:  Read(w1)
    State 4:  Takeover(w1) (epoch 1)
    State 5:  Read(w1)
    State 6:  BeginTurn(w1) (turn 1)
    State 7:  Read(w1)
    State 8:  RecordStep(w1, h1)
    State 9:  Read(w1)
    State 10: CommitIntent(w1, h1) (policy provider_idempotency)
    State 11: Read(w1)
    State 12: AuthorizeFirst(w1) (attempt 1)
    State 13: Dispatch(w1) (provider call 1, attempt 1, executed)
    State 14: LoseResponse(w1)
    State 15: Read(w1) (attempt 1 ambiguous)
    State 16: BrokenProviderReauthorizes(w1) (effect "authorized",
              attempt 2)

## What the switch breaks

Under provider_idempotency an ambiguous attempt is not replaced: the
runner's `Coordinator::run_step` re-dispatches the journaled effect id and
attempt with no HEAD write (`crates/ravel-agents-runtime/src/effects.rs:330`,
the `(EffectStatus::Authorized, RecoveryPolicy::ProviderIdempotency)` arm),
and the provider deduplicates on the effect id. The correct model's
`Reauthorize` does the same. The switch authorizes attempt 2 instead.

## Why it is a real bug if it ever held

Attempt numbers are what the result endpoint fences on: once attempt 2 is
journaled, a result the provider returns for attempt 1 is an older
attempt's and `Kernel::record_effect_result` answers `LateResultIgnored`
(`crates/ravel-agents-core/src/kernel.rs:1856`). The provider has executed
once and will answer every dispatch of the effect id with that execution,
so a policy that renumbers its attempts can discard the only result it will
ever get. `PolicyControlsRedispatch` requires that under
provider_idempotency only attempt 1 is ever journaled or dispatched.
