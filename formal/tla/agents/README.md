# Agents TLA+ models

TLA+ models of the agent HEAD protocol in ADR-3331, as the three modules
ADR-3331 decision 13 names:

- `AgentProtocol` (task T5 of epic #3331): enqueue with request identity and
  receipts, root publication, takeover, turn completion and rebase,
  cancellation, destroy and recreate, and timer delivery through wake hints.
  A turn's handler is a black box that completes or fails.
- `AgentEffects` (task T17): what happens inside one active turn. Durable
  steps and effect intents journaled under keys that follow the input, attempt
  authorization under the tool's recovery policy (`provider_idempotency`,
  `at_least_once`, `manual`), the external provider with response loss,
  result publication by the dispatcher or the result-completion endpoint,
  replay and replay drift, admin reconciliation of a manual effect, takeover,
  crash, retry after a failure, and cancellation of the running turn.
- `AgentIntegrated` (task T17): one agent end to end. A client enqueue with
  its payload PUT, lost responses and resolution, takeover, one tool step
  under each policy, the turn output and the final commit, the client's
  cancel, and a timer generation whose schedule, cancel, hint and sweep race
  the client and the turn on the same HEAD.

Each `<Name>.tla` is a spec; each `MC<Name>.tla` is its model-checking entry
point, which extends it with the broken-behavior switches, the reachability
probes, the state view and the worker symmetry. Every spec instantiates the
shared `RavelObjectStore.tla` (under `../common`) unchanged, so every payload
PUT, turn output PUT, audit PUT, HEAD create, HEAD CAS, hint write and hint
delete obeys the object-store contract rather than a hand-rolled map.

The sections whose heading starts "AgentProtocol:" describe `AgentProtocol`;
"Refinement" states how the other two modules relate
to it, and "AgentEffects" and "AgentIntegrated" describe those two.
"Differences from the kernel", "Gaps" and "Exhaustive runs" cover all three.

## What these models claim (ADR-1113 D12)

TLC checked each finite model under the bounds and assumptions in its `.cfg`.
The models check the protocol design under those bounds. Whether the Rust
implementation conforms is a separate question: the traceability table argues
it, and the Rust tests named there assert it.
[`traceability.md`](traceability.md) maps each action and property to the
kernel, store and runtime symbols and tests that implement it today (the
effect rows cite the coordinator in `ravel-agents-runtime`); the remaining
runtime rows (lease takeover, worker dispatch, worker crash, wake hints and the
sweep, and liveness) sit under its design heading, pending T9 (#3343), T15
(#3348) and later tasks. Safety and liveness are stated separately below, and
every liveness result carries its fairness assumptions. The object
store's own conformance to its contract is an assumption, stated as such and
not checked here.

[`results.md`](results.md) records every measured run (task T20, #3354:
`-workers 2`, `-Xmx4g`, revision `8ee0bd2`), one row per run with its run id,
and `bands.tsv` carries one row per enrolled smoke, live, exhaustive and
positive config, each citing the `results.md` row it rests on. A row pins a
distinct-state band around the measured figure with depth waived by `-`, so a
change that narrows what the run reaches fails that lane:

| Config | Band (distinct) | Measured | Row |
|---|---|---|---|
| `MCAgentProtocol.smoke.cfg` | 286000 to 292000 | 289101 | R1 |
| `MCAgentProtocol.exhaustive.cfg` (MaxTimerGen 1) | 1253000 to 1280000 | 1266296 | R12 |
| `positive/stale-turn-rebase-unreachable.cfg` | 286000 to 292000 | 289101 | R4 |
| `positive/two-generations.cfg` | 240000 to 246000 | 243171 | R5 |
| `MCAgentEffects.smoke.cfg` | 219000 to 226000 | 222504 to 222728 | R2, R7 to R9, R20 |
| `MCAgentEffects.exhaustive.cfg` | 1925000 to 1965000 | 1942312 to 1944848 | R11, R16, R17 |
| `MCAgentEffects.live.cfg` | 125500 to 130000 | 127749 | R18 |
| `MCAgentIntegrated.smoke.cfg` | 422000 to 434000 | 427920 | R3 |
| `MCAgentIntegrated.live.cfg` | 13350 to 13900 | 13617 | R19 |
| `positive/integrated-timer-race.cfg` | 1170000 to 1210000 | 1190621 | R6 |

The `AgentEffects` rows carry a range because their distinct count moved
between repeat runs under `VIEW`, `SYMMETRY` and two workers. The two
exhaustive rows keep `budget_s` at 3000 (ADR-3331 decision 13).
`MCAgentIntegrated.exhaustive.cfg` has no row and stays under `unenrolled/`,
where the exhaustive lane does not find it, because it did not finish inside
3000 s (see "Exhaustive runs"). The exhaustive lane is safety-only for
`AgentEffects`: its exhaustive config carries no `PROPERTY` line, since
`VIEW` and `SYMMETRY` are sound for invariants only, and its liveness runs in
the `live` lane.

## AgentProtocol: scope

Modelled, each as its own action unless "AgentProtocol: folded steps" below says otherwise:

- One mutable HEAD per agent key, written `CreateIfAbsent` once and
  `CasVersion` afterwards. `rev` (root_revision) rises by one on every write
  and is part of the HEAD content.
- Immutable dependencies (request payloads, turn output objects) written
  `CreateIfAbsent` before the CAS that links them.
- Client enqueue with a request id and payload hash: the enqueue CAS links the
  payload, allocates `accept_seq` and records the receipt. Same id and same
  hash returns the recorded handle; same id and a different hash is a
  conflict.
- A lost response on a HEAD CAS (`CasHeadLostResponse`,
  `CompleteTurnLostResponse`): the effect applies and the caller holds an
  unknown outcome, resolved by re-reading HEAD.
- Takeover as an epoch-advancing CAS; turns bound to one accepted input,
  completed or failed in one CAS; `CompleteTurnRebase` reapplies a completion
  across a concurrent write only when owner, epoch, active turn and base state
  version still match.
- Worker crash with a turn output written or a completion outcome unknown.
- Cancellation as a HEAD command; destroy (tombstone) and recreate with an
  incarnation bump.
- One timer per agent with a generation; scheduling and cancelling it are
  HEAD CASes fenced by incarnation, not by owner. One advisory hint per armed generation; the sweep fires a due
  hint with one CAS that marks the generation delivered and enqueues the timer
  message, and deletes a hint whose generation is no longer armed. A hint may
  be absent (never written yet), stale, or listed early.

Out of scope, by design (see "Refinement"): step intents, attempt
authorization, external execution, replay and manual resolution; history
paging; quotas and `BudgetExceeded`; format versions.

## Refinement

`AgentProtocol` treats a turn's handler as a black box that completes or
fails. `AgentEffects` opens that box for one accepted input, and
`AgentIntegrated` joins the two for one agent. The relation between them is a
correspondence argued here, not a refinement mapping TLC checks: no module
instantiates another, and each is checked against its own properties.

- `AgentEffects` takes the input as accepted before the first state and has no
  enqueue, timers, tombstone or recreate; those stay `AgentProtocol`'s. Its
  HEAD carries what `AgentProtocol`'s does for one input (owner, epoch, active
  turn with its cancel request, results) plus the step transcript and the
  effect journal. Its actors (`ac`) are the workers and the admin.
- `AgentIntegrated` keeps the store variables of both, the client (`cl`) and
  the answered handle (`acked`) of `AgentProtocol`, and a per-worker operation
  (`wk`) that plays both `AgentProtocol`'s worker and `AgentEffects`'s owner;
  `sw` is the sweep. It restricts both models: one tenant, one agent, one
  incarnation, one client request, one step and one effect per client turn, a
  timer input whose handler runs no step or effect, and no admin, so an effect in doubt stays in
  doubt. Replay drift and the retry after a failure are `AgentEffects`'s only.
- The protocol properties take `AgentProtocol`'s form for one agent and one
  incarnation, and the effect properties take `AgentEffects`'s form; the
  smoke, exhaustive and positive configs check all fourteen together (see
  "AgentIntegrated").
- Five steps `AgentProtocol` folds for its budget are separate actions in
  `AgentIntegrated`: the client's answer (`Ack`), the payload PUT's lost
  response (`PutPayloadLostResponse`), the re-read that resolves an unknown
  enqueue outcome (`Resolve`), a crashed worker's restart (`Recover`), and the
  sweep's read before its fire (`SweepRead`). Its negatives
  `integrated-ack-before-cas`, `integrated-resolve-trusts-unknown` and
  `integrated-sweep-overwrites-head` and its probes `integrated-resolved-reachable`
  and `integrated-recovered-reachable` exercise those splits.
- Three callers in `AgentIntegrated` still fold a read into their CAS, each
  acting on the current version with nothing written in between: the client's
  cancel, the timer schedule and cancel, and the result-completion endpoint
  (`ResultCallback`). `AgentEffects` folds the cancel and the result endpoint
  the same way. A CAS on a stale read fails with no effect and is retried, so a
  separate read only adds stuttering states.

## AgentProtocol: assumptions

- The store honours its contract (`common/RavelObjectStore.tla`). Nothing
  else about the store is assumed.
- No real time. Takeover timing, lease expiry and timer due-ness
  (`TimerDue`) are nondeterministic environment steps; no safety property
  reads a clock.
- Handler code is a black box: a turn either completes (one new state
  version) or fails (state unchanged). Nothing here claims worker code is
  deterministic.
- A transient store failure applies nothing and leaves the caller free to
  retry, so it is a stuttering step.

## AgentProtocol: properties

Safety (invariants), all in `MCAgentProtocol.smoke.cfg` and
`MCAgentProtocol.exhaustive.cfg`: `AcceptedImpliesRecoverable`,
`PublishedReferencesComplete`, `RequestIdentityStable`,
`OneLogicalCompletion`, `TurnCommitAtomic`, `NoStaleOwnerCommit`,
`NoLostConcurrentEnqueue`, `MonotonicIdentity`,
`TimerGenerationDeliveredOnce`, `CancellationRespected`,
`TenantIncarnationIsolation`. Each reads the store, the `lastCas` witness (the
HEAD read from the store just before and just after the HEAD write the step
made) or the `acked` handles, never a flag the checked action sets about
itself.

Liveness, in `MCAgentProtocol.exhaustive.cfg` only, under `MCFairSpec`:

- `InputEventuallyResolved`: an accepted, uncancelled input is eventually
  completed, failed or cancelled, or its agent destroyed.
- `DueTimerEventuallyDelivered`: a due, armed timer generation is eventually
  delivered, cancelled, or its agent destroyed.

Fairness assumed for both (`Fairness` in `AgentProtocol.tla`): weak fairness
per worker on `Takeover`, `BeginTurn`, `PutTurnOutput`, `CompleteTurn` and
`CompleteTurnRebase` (a compatible worker stays available and keeps acting),
and per agent key on `CreateHead`, `WriteHint` and `FireTimer` (the scheduler
writes its hint and the sweep keeps serving due hints). Eventual store
availability is the absence of an infinite run of transient-failure stutters,
which weak fairness on those actions already rules out. Contention is bounded
because each client sends once and the optional root writes stop at
`MaxRevision`.

## AgentProtocol: configs and bounds

Every config: one tenant, one agent, two workers, `MaxEpoch = 2`.

| Config | Clients, inputs, request ids, hashes | MaxRevision | MaxTimerGen | Checks |
|---|---|---|---|---|
| `MCAgentProtocol.smoke.cfg` | 2 each | 6 | 0 | the 11 invariants, under `VIEW View` and `SYMMETRY WorkerSymmetry` |
| `MCAgentProtocol.exhaustive.cfg` | 1 each | 4 | 1 | the 11 invariants and both liveness properties, no VIEW or SYMMETRY |
| `positive/two-generations.cfg` | 1 each | 4 | 2 | the 11 invariants, under `VIEW View` and `SYMMETRY WorkerSymmetry` |
| `positive/stale-turn-rebase-unreachable.cfg` | 2 each | 6 | 0 | the 11 invariants with `StaleTurnRebase` on, under `VIEW View` and `SYMMETRY WorkerSymmetry` |

The bounds are split because the full product does not fit a budget: in
T17's local runs, two clients and two workers with one timer generation, even
at `MaxRevision = 4` and with the view and symmetry, did not finish, and
neither did the one-client exhaustive config at `MaxRevision = 6`. Those
runs were not repeated for T20 and their figures are not recorded. So
competing senders are checked without timers (smoke), and timers and
liveness are checked with one sender and `MaxRevision = 4` (exhaustive). A
second timer generation does not fit either: the exhaustive config at
`MaxTimerGen = 2` had 2879377 distinct states with 1058019 queued when the
3000 s budget ran out (2 workers, 4 GiB; `results.md` row R13). A
`FireTimer` that ignored the generation passes every
config at `MaxTimerGen = 1`, so the clean spec's invariants at a second
generation are checked by the positive config `positive/two-generations.cfg`:
every switch off, one client, one input, two workers, `MaxRevision = 4`,
`MaxEpoch = 2`, `MaxTimerGen = 2`, the smoke config's view and symmetry, the
eleven invariants and no liveness (243171 distinct states at depth 31, 26 s
with 2 workers; row R5). Under those bounds a reschedule of an armed generation, a
hint left behind for a superseded generation, and a delivered generation 2
are each reached; a scratch probe invariant per case was violated by TLC
(exit 12). A `FireTimer` whose generation check was replaced by
`h.timer.st = "armed"` fails that config with `Error: Invariant
TimerGenerationDeliveredOnce is violated.` (exit 12, a 7-state trace); that
mutant was run on a scratch copy and is not shipped. The negative
`stale-timer-generation-fired` and the `second-generation-reachable` probe
run at the same timer bounds. The exhaustive lane checks `MaxTimerGen = 1`,
the largest bound that finished inside its 3000 s budget (1082 s, row R12).

`View` drops the store's version integers and keeps, for each held CAS token,
only whether it equals the current HEAD version. Nothing reads a version
except that comparison, and versions are never reused, so a token that is not
current never becomes current again. `WorkerSymmetry` permutes workers; no
action, invariant or initial state names a particular worker. Both reductions
are sound for invariants only, which is why the exhaustive config, the one
with `PROPERTY` lines, uses neither.

## AgentProtocol: negative controls

Each switch in `MCAgentProtocol.tla` adds one broken action; each config in
`negative/` turns exactly one on and pins the property it must violate in its
`.expect`. `negative/counterexamples/<name>.md` quotes the TLC line and trace
for each.

| Config | Switch | Violates |
|---|---|---|
| `ack-before-cas` | `AckBeforeCas` | `AcceptedImpliesRecoverable` |
| `head-before-dependency` | `HeadBeforeDependency` | `PublishedReferencesComplete` |
| `overwrite-stale-root` | `OverwriteStaleRoot` | `NoLostConcurrentEnqueue` |
| `epoch-checked-outside-cas` | `EpochCheckedOutsideCas` | `NoStaleOwnerCommit` |
| `cursor-without-result` | `CursorWithoutResult` | `TurnCommitAtomic` |
| `request-hash-mismatch-accepted` | `RequestHashMismatchAccepted` | `RequestIdentityStable` |
| `receipt-discarded` | `ReceiptDiscarded` | `RequestIdentityStable` |
| `rebase-ignores-cancel-request` | `RebaseIgnoresCancelRequest` | `CancellationRespected` |
| `stale-timer-generation-fired` | `StaleTimerGenerationFired` | `TimerGenerationDeliveredOnce` |
| `early-hint-forgotten` | `EarlyHintForgotten` | `DueTimerEventuallyDelivered` (exit 13) |

The first eight are the smoke config with one switch on. The two timer
negatives cannot be: the smoke config has no timers. `stale-timer-generation-fired`
uses the exhaustive bounds at `MaxTimerGen = 2` with the smoke config's view,
symmetry and invariants; `early-hint-forgotten` is a liveness negative under `MCFairSpec`
with one worker and `MaxEpoch = 1`, and no view or symmetry.

`rebase-ignores-cancel-request` commits, on the re-read root, the result
computed from the root read before the output PUT, so a cancellation
requested in between is lost and the input completes. Its trace is in
`negative/counterexamples/rebase-ignores-cancel-request.md`.

`stale-turn-rebase` was a negative while a cancel cleared the active turn: a
rebase that skipped only its active-turn check gave a cancelled input a
second, completed result. Since a cancel only marks the active turn, the
active turn moves only through the owner's own commit or through a takeover,
which changes the epoch, so the rebase's owner and epoch checks already
refuse every root on which the turn check would. TLC finds no violation with
the switch on, and the config now runs in the positive lane as
`positive/stale-turn-rebase-unreachable.cfg` (switch on, all eleven
invariants, smoke bounds, 289101 distinct states as in smoke; row R4). In the model
the turn check is defensive.

Reachability probes, each a config with every switch off and one probe
invariant that must be violated (exit 12): `crash-reachable`
(`CrashUnreachable`), `lost-response-reachable` (`LostResponseUnreachable`),
`takeover-reachable` (`TakeoverUnreachable`), `competing-senders-reachable`
(`CompetingSendersUnreachable`) and, at the stale-timer bounds rather than the
smoke config, `second-generation-reachable` (`SecondGenerationUnreachable`).
Four read store or operation state;
`CrashUnreachable` reads `crashedOnce`, which `WorkerCrash` records, so it
witnesses that the crash action fired rather than that it was enabled. A
violation shows the probe's config reaches that situation; for the four
probes at smoke bounds the smoke run checks the eleven invariants on those
states, and for `second-generation-reachable` only the stale-timer negative
shares its bounds, with its mutant on.

## AgentProtocol: folded steps

ADR-3331 decision 13 asks for every read, immutable write, HEAD CAS,
acknowledgement, crash, recovery and discovery to be its own action. To fit the
budgets this model folds some of them. Each fold below joins steps with no
other write by the same caller between them; whether it loses behaviour is
argued here, not checked.

- A HEAD read is folded into the CAS that acts on it (`CasNow`) for cancel,
  takeover, turn start, timer schedule and cancel, timer firing and the
  completion rebase. A CAS on a stale read fails with no effect and the caller
  re-reads, so a separate read step only adds states that stutter.
- The client's `ReadHead` folds the read, the receipt lookup (acknowledgement
  of a duplicate send, or the conflict answer) and the payload PUT. The
  re-read after an unknown outcome is `ReadHead` from the `unknown` state; the
  spec has no separate `EnqueueResolve` action.
- The acknowledgement is part of the successful enqueue CAS (`EnqueueCas`) and
  of `ReadHead`; there is no separate `Ack` action. The `AckBeforeCas`
  negative shows the order is checked.
- There is no `PutDependency` or `PutDependencyLostResponse` action. The
  payload PUT is in `ReadHead` and the turn output PUT is `PutTurnOutput`.
  A lost response on a content-addressed `CreateIfAbsent` leaves the object
  present, and the retry gets `AlreadyExists` and proceeds, so it is not
  modelled separately.
- `WorkerCrash` returns the worker to idle; there is no separate
  `WorkerRecover`.
- There is no separate `SweepRead`: `FireTimer` reads HEAD in its CAS step.
  A sweep pass that lists a hint before its generation is due keeps it and
  writes nothing, so it is a stutter with no action of its own (the comment
  on `FireTimer` says so); `early-hint-forgotten.cfg` checks that deleting
  it instead is caught.

## AgentEffects

Scope and assumptions are in the module header of `AgentEffects.tla`. In
short: one HEAD holding one accepted input; turns bound to it by its owner and
retried after a retryable failure under a new turn that keeps the step
transcript and the effect journal; one durable step and one effect intent per
turn; attempt authorization by the owner under the tool's policy; a provider
that executes a dispatched attempt and whose response can be lost; result
publication by the dispatcher (`RecordHeldResult`) or by the result-completion
endpoint (`ResultCallback`); replay from the journal, and drift that blocks the
turn; admin reconciliation of a manual effect (`AdminAuthorize`,
`AdminRecordResult`), each behind an immutable audit object written before the
CAS; takeover; worker crash and recovery; and the client's cancel. Every read,
immutable write, HEAD CAS, crash, recovery, dispatch, provider response,
response loss and result record is its own action, except the two folded reads
named in "Refinement". The provider's own log (`calls`) is the witness for
what executed.

Safety (invariants), in `MCAgentEffects.smoke.cfg`,
`MCAgentEffects.exhaustive.cfg` and `MCAgentEffects.live.cfg`:

- `StepTranscriptCompatible`: a step or intent key keeps the tool identity and
  input hash it was first journaled with, a recorded effect result is never
  rewritten, and the journal never goes back to a lower attempt.
- `DispatchRequiresCommittedAuthorization`: every dispatch the provider saw is
  of an attempt the journal holds an authorization for.
- `PolicyControlsRedispatch`: each authorized attempt is dispatched at most
  once, except under `provider_idempotency`, where an ambiguous attempt is
  re-dispatched under the same effect id and attempt; under `manual` no
  attempt after the first is authorized or dispatched without a reconcile
  audit object, and no turn result lands while the effect is in doubt; under
  `provider_idempotency` only attempt 1 is ever journaled or dispatched and
  the provider executes at most once; `at_least_once` is otherwise
  unconstrained.
- `EffectResultFromExecution`: a recorded result is the journaled attempt's,
  and came from a physical execution of that attempt. A result from an older
  attempt leaves HEAD unchanged, as `Kernel::record_effect_result` answers
  `LateResultIgnored` (`crates/ravel-agents-core/src/kernel.rs:1856`).
- `CancellationRespected` (effects form): a commit on a turn with a cancel
  request publishes it cancelled; once the request is on the root the owner
  journals no new step or intent and authorizes no new attempt (only a
  reconcile audit object admits one); nothing rewrites a published result;
  and once the input ended cancelled no write changes its effect, so a result
  arriving after that is evidence only (`kernel.rs:1851` and `1856`).

Liveness, in `MCAgentEffects.live.cfg` only, under `MCFairSpec`:
`InputSettledOrEffectBlocked`, an input on the inbox eventually leaves it, or
the effect reaches a blocked terminal: it awaits reconciliation (in doubt, or a
reconcile attempt the admin has not settled), the run spent `MaxAttempt` on an
ambiguous attempt, or the turn is blocked by replay drift while an attempt is
authorized without a result (`BlockedWithPendingAttempt`, see "Gaps"). Fairness
(`Fairness` in `AgentEffects.tla`): weak fairness on `CreateHead` and, per
worker, on `Read`, `Takeover`, `BeginTurn`, `RecordStep`, `CommitIntent`,
`AuthorizeFirst`, `Reauthorize`, `ResumeAuthorized`, `PutTurnOutput`,
`CompleteTurn`, `CommitCancelled`, `FailTurn` on a blocked turn, `Dispatch`,
`Respond`, `RecordHeldResult` and `Recover`. The admin, the client's cancel
and the result endpoint are not fair: reconciliation is an operator's choice,
so an in-doubt effect may wait forever, which is why the property names the
blocked terminals. The probe `effect-in-doubt-blocks-input` shows the
property without them (`InputEventuallySettled`) fails.

| Config | Workers, epochs | Policies | Hashes | MaxTurns, MaxAttempt | Checks |
|---|---|---|---|---|---|
| `MCAgentEffects.smoke.cfg` | 2, 2 | all three | 1 | 2, 2 | the 5 invariants, under `VIEW View` and `SYMMETRY WorkerSymmetry` |
| `MCAgentEffects.exhaustive.cfg` | 2, 2 | all three | 2 | 2, 2 | the 5 invariants, under `VIEW View` and `SYMMETRY WorkerSymmetry` |
| `MCAgentEffects.live.cfg` | 1, 1 | all three | 2 | 2, 2 | the 5 invariants and `InputSettledOrEffectBlocked`, no VIEW or SYMMETRY |

The smoke config has one input hash, so replay drift is reached only in the
exhaustive and live configs (and in the `step-input-drift` negative, which
adds the second hash). The live config runs at one worker and one epoch
because liveness cannot use the view or the symmetry; the worker crashes and
recovers instead of being taken over.

Negative controls, each the smoke bounds with one switch on in
`MCAgentEffects.tla` and its `.expect` naming the property:

| Config | Switch | Violates |
|---|---|---|
| `dispatch-before-intent` | `DispatchBeforeIntent` | `DispatchRequiresCommittedAuthorization` |
| `ambiguous-manual-attempt-retried` | `AmbiguousManualAttemptRetried` | `PolicyControlsRedispatch` |
| `commit-while-in-doubt` | `CommitWhileInDoubt` | `PolicyControlsRedispatch` |
| `step-input-drift` | `StepInputDrift` | `StepTranscriptCompatible` |
| `late-result-overwrites` | `LateResultOverwrites` | `StepTranscriptCompatible` |
| `authorize-ignores-cancel-request` | `AuthorizeIgnoresCancelRequest` | `CancellationRespected` |
| `provider-reauthorizes` | `ProviderReauthorizes` | `PolicyControlsRedispatch` |
| `older-result-recorded` | `OlderResultRecorded` | `EffectResultFromExecution` |
| `cancelled-result-recorded` | `CancelledResultRecorded` | `CancellationRespected` |

`ambiguous-manual-attempt-retried` and `commit-while-in-doubt` run with the
`manual` policy only, `late-result-overwrites` and `older-result-recorded`
with `at_least_once` only (the policy under which two executions of one
effect are reachable), `provider-reauthorizes` with `provider_idempotency`
only, and `step-input-drift` with two input hashes.
`authorize-ignores-cancel-request` breaks the refusal at
`crates/ravel-agents-core/src/kernel.rs:1764`, `late-result-overwrites` and
`older-result-recorded` the `LateResultIgnored` answers at `kernel.rs:1835`
and `1856`, `cancelled-result-recorded` the cancelled-input answer at
`kernel.rs:1851`, and `provider-reauthorizes` the runner's same-attempt
re-dispatch at `crates/ravel-agents-runtime/src/effects.rs:330`. Each trace
is in `negative/counterexamples/<name>.md`.

Reachability probes, every switch off, each violating its probe invariant
(exit 12) unless noted: `duplicate-execution-reachable`
(`DuplicateExecutionUnreachable`, `at_least_once` only: a lost response and a
second authorization execute the effect twice), `blocked-effect-reachable`
(`BlockedEffectUnreachable`, `manual` only: the owner's next authorization
puts the effect in doubt), `reconciled-effect-reachable`
(`ReconciledEffectUnreachable`, `manual` only: an admin reconciles behind an
audit object), `effect-worker-crash-reachable` (`CrashUnreachable`: a crash
with an operation in flight or a provider response unrecorded), and the
liveness probe `effect-in-doubt-blocks-input` (`InputEventuallySettled`, exit
13, `manual` only, live bounds).

## AgentIntegrated

Scope and assumptions are in the module header of `AgentIntegrated.tla`, and
its relation to the other two modules is in "Refinement". Safety
(invariants), in `MCAgentIntegrated.smoke.cfg`,
`unenrolled/MCAgentIntegrated.exhaustive.cfg`, `MCAgentIntegrated.live.cfg`
and `positive/integrated-timer-race.cfg`: the nine protocol properties
`AcceptedImpliesRecoverable`, `PublishedReferencesComplete`,
`RequestIdentityStable`, `OneLogicalCompletion`, `TurnCommitAtomic`,
`NoStaleOwnerCommit`, `NoLostConcurrentEnqueue`, `MonotonicIdentity` and
`TimerGenerationDeliveredOnce` in `AgentProtocol`'s form for one agent and one
incarnation, and the five effect properties `StepTranscriptCompatible`,
`DispatchRequiresCommittedAuthorization`, `PolicyControlsRedispatch`,
`EffectResultFromExecution` and `CancellationRespected` in `AgentEffects`'s
form. `TenantIncarnationIsolation` is not here: one tenant, one agent and one
incarnation leave it nothing to check.

Liveness, in `MCAgentIntegrated.live.cfg` only, under `MCFairSpec`:

- `InputSettledOrEffectBlocked`: every accepted input eventually has a
  result, or the client's effect is blocked (in doubt, with no admin here, or
  authorized at `MaxAttempt` without a result).
- `EnqueueEventuallyAnswered`: the client is eventually answered with a
  handle.

Fairness (`Fairness` in `AgentIntegrated.tla`): weak fairness on
`CreateHead`, `ClientRead`, `Resolve`, `Ack`, `WriteHint`, `SweepRead` and
`SweepFire`, and per worker on `Read`, `Takeover`, `BeginTurn`, `RecordStep`,
`CommitIntent`, `AuthorizeFirst`, `Reauthorize`, `PutTurnOutput`,
`CompleteTurn`, `CommitCancelled`, `Dispatch`, `Respond`, `RecordHeldResult`
and `Recover`; strong fairness on `PutPayload` and `EnqueueCas`, since a lost
response sends the client back to the read and so disables them again; the
client finishes the enqueue it started. The client's cancel, timer scheduling
and cancelling, and the result endpoint are not fair.

| Config | Workers, epochs | Policies | MaxTimerGen | Checks |
|---|---|---|---|---|
| `MCAgentIntegrated.smoke.cfg` | 2, 2 | all three | 0 | the 14 invariants, under `VIEW View` and `SYMMETRY WorkerSymmetry` |
| `positive/integrated-timer-race.cfg` | 1, 1 | `at_least_once`, `manual` | 1 | the 14 invariants, under `VIEW View` and `SYMMETRY WorkerSymmetry` |
| `unenrolled/MCAgentIntegrated.exhaustive.cfg` (not run by any lane: it does not finish in 3000 s) | 2, 2 | all three | 1 | the 14 invariants, under `VIEW View` and `SYMMETRY WorkerSymmetry` |
| `MCAgentIntegrated.live.cfg` | 1, 1 | all three | 0 | the 14 invariants and both liveness properties, no VIEW or SYMMETRY |

Every config has one client and `MaxAttempt = 2`. The bounds are split the
same way as `AgentProtocol`'s: two workers with the timer on is the exhaustive
config, which did not finish in 3000 s (see "Exhaustive runs"), so the required
lanes check takeover without the timer (smoke) and the timer race with one
worker that crashes and restarts (`positive/integrated-timer-race.cfg`).

Negative controls, each with one switch on in `MCAgentIntegrated.tla`:

| Config | Switch | Bounds | Violates |
|---|---|---|---|
| `integrated-ack-before-cas` | `AckBeforeCas` | smoke | `AcceptedImpliesRecoverable` |
| `integrated-resolve-trusts-unknown` | `ResolveTrustsUnknown` | timer race | `AcceptedImpliesRecoverable` |
| `integrated-sweep-overwrites-head` | `SweepOverwritesHead` | timer race | `NoLostConcurrentEnqueue` |
| `integrated-dispatch-before-intent` | `DispatchBeforeIntent` | smoke | `DispatchRequiresCommittedAuthorization` |
| `integrated-complete-ignores-cancel` | `CompleteIgnoresCancel` | smoke | `CancellationRespected` |

`integrated-resolve-trusts-unknown` needs the timer: at smoke bounds no other
writer moves HEAD before the first enqueue, so the enqueue CAS cannot fail.
Each negative's trace is in `negative/counterexamples/<name>.md`.

Reachability probes, every switch off, each violating its probe invariant
(exit 12) unless noted: `integrated-takeover-reachable`
(`TakeoverUnreachable`, epoch 2), `integrated-resolved-reachable`
(`ResolvedUnreachable`, a lost enqueue response answered by `Resolve`),
`integrated-recovered-reachable` (`RecoveredUnreachable`, a crashed worker
restarts), `integrated-tool-turn-completed-reachable`
(`ToolTurnCompletedUnreachable`, the client turn commits completed with its
effect result recorded), `integrated-duplicate-execution-reachable`
(`DuplicateExecutionUnreachable`, `at_least_once` only),
`integrated-blocked-effect-reachable` (`BlockedEffectUnreachable`, `manual`
only), `integrated-timer-cancel-race-reachable` (`TimerCancelRaceUnreachable`,
timer race bounds: the client's input settles cancelled on a HEAD that also
delivered a timer generation), and the liveness probe
`integrated-effect-in-doubt-blocks-input` (`InputEventuallySettled`, exit 13,
`manual` only, live bounds).

## Differences from the kernel

Where the model and `ravel-agents-core`'s kernel disagreed, the kernel is the
intended behaviour and the model follows it:

- Recreate clears the owner and restarts the timer generation at 0, as
  `Kernel::create` does; the epoch is kept. A wake hint names the incarnation
  it was written for, and `FireTimer` fires only a hint of the current
  incarnation. `MonotonicIdentity` checks the timer generation only across
  writes within one incarnation, and checks no turn counter (the kernel has
  none).
- Destroy leaves an armed timer armed, as `Kernel::tombstone` does, and
  `FireTimer` refuses to fire on a tombstoned root. `DueTimerEventuallyDelivered`
  counts a destroyed agent as settled for its timer.
- Cancelling the active turn's input records the request on the turn
  (`creq`) and leaves the input pending; the turn's next commit (complete,
  fail or rebase) publishes a cancelled result and leaves the state version
  alone, as `Kernel::complete_turn` and `Kernel::fail_turn` do with
  `CancellationState::Requested`. A repeated request writes nothing.
  `CancellationRespected` now allows a cancelled input to stay pending only
  as the requested active turn's input.
- `ScheduleTimer` and `CancelTimer` are fenced by incarnation, as the
  kernel's `check_incarnation` fences them, so any caller of the current
  incarnation may issue them, with or without an owner.
- `ScheduleTimer` is enabled while a generation is armed: it reschedules
  under the next generation, which supersedes the armed one, as the kernel's
  `schedule` does. Being environment writes now, timer schedule and cancel
  are bounded by `MaxRevision` like the other optional root writes.

Remaining differences, each also stated in its `traceability.md` row:

- The model's `turns` counter names output objects and survives a recreate;
  the kernel derives turn ids and has no counter.
- The store keys a wake hint by its due bucket; the models key it by
  generation.

`AgentEffects` and `AgentIntegrated` follow the kernel's effect rules:
`Kernel::authorize_attempt` refuses an owner authorization for an effect whose
input is not the active turn's (`crates/ravel-agents-core/src/kernel.rs:1752`)
or is being cancelled (`kernel.rs:1764`), and refuses the owner on an effect in
doubt (`kernel.rs:1777`); `Kernel::record_effect_result` answers
`LateResultIgnored` without a publish for a different attempt's result after
completion (`kernel.rs:1835`), and for a result of an older attempt or any
result once the input ended cancelled (`kernel.rs:1851` and `1856`). Under
`provider_idempotency` the models re-dispatch the journaled effect id and
attempt with no HEAD write, as `Coordinator::run_step` does
(`crates/ravel-agents-runtime/src/effects.rs:330`). The models admit admin
reconciliation only on an effect in doubt; the runtime also admits two paths
that write nothing, a `reconcile_attach` on a completed effect
(`effects.rs:409`, which re-submits the recorded attempt) and a
`reconcile_authorize` that replays the same authorized attempt
(`effects.rs:456`), as traceability rows 66 and 67 record. The
differences that remain, each with the property that rests on it:

- An owner that reads a cancel request on its turn journals no new step. The
  kernel's `Kernel::require_running` (`kernel.rs:1007`), which
  `Kernel::record_step` and `Kernel::commit_intent` use, checks the turn fence
  and the replay block but not the request, and the runtime journals the step
  before `refuse_cancelled` runs (`effects.rs:196` and `657`). The runtime does
  refuse the intent and every authorization under a request (the
  `refuse_cancelled` calls in `run_step`). `CancellationRespected` (effects
  form) states the stricter rule; the step half is test debt in its
  `traceability.md` row.
- An owner that reads a cancel request does not latch a `manual` attempt in
  doubt. The kernel admits that latch under a request (`kernel.rs:1762`, the
  `latch` exemption); the runtime calls `refuse_cancelled` before it
  (`effects.rs:338`) and the model's `Reauthorize` requires a running turn
  without a request.
- An owner authorizes only on an unblocked turn. The kernel's owner
  authorization checks the owner fence and that the effect's input is the
  active turn's (`kernel.rs:1752`), not the replay block; the runtime never
  reaches an authorization on a blocked turn, because `journal_step` goes
  through `Kernel::record_step` first.
- An attempt the reconciler authorized is dispatched by the reconciler alone,
  and the owner leaves it alone. The runtime keeps that in memory (its open
  attempts, `Coordinator::prune_open_attempts`, `effects.rs:641`); the root has
  no field naming the reconciler as that attempt's dispatcher, so the model
  reads its audit object.
- `AdminAuthorize` and `AdminRecordResult` write an immutable audit object
  before their CAS, and `PolicyControlsRedispatch` reads it. The kernel
  appends the audit event to HEAD in the same publish
  (`push_audit`, `kernel.rs:1799` for an attempt and `kernel.rs:1863` for a
  result).
- The owner writes the turn output only after its effect has a result.
  `Kernel::complete_turn` refuses an effect in doubt (`kernel.rs:1242`) but
  accepts one authorized without a result, on the ground that a handler that
  completed awaited its attempt; the model encodes that as an ordering on the
  owner.
- A step is recorded completed in one CAS. The kernel also accepts a
  `Started` record first and replaces it with the completed one
  (`kernel.rs:1654`); the module header of `AgentEffects.tla` argues it adds no
  case.

## Gaps

- The HEAD-listing half of the reconciliation sweep is not modelled. Timer
  discovery goes only through hints, so "fair discovery" in ADR-3331's
  liveness condition is weak fairness on `WriteHint` and `FireTimer` in
  `AgentProtocol`, and on `WriteHint`, `SweepRead` and `SweepFire` in
  `AgentIntegrated`.
- A hint is written once per generation; duplicated hints are not modelled.
- One tenant (`Tenants = {t1}`) and one agent in every config of the three
  modules. `TenantIncarnationIsolation` is checked in `AgentProtocol` across
  incarnations, not across tenants or agents.
- Each worker crashes at most once. In `AgentProtocol` it crashes only with a
  turn output written or a completion outcome unknown.
- `BlockedWithPendingAttempt` (a design gap, not a model bug): a turn blocked
  by replay drift while its attempt is authorized without a result cannot be
  failed (`Kernel::fail_turn` answers `NoAuthorizedAttemptPending`) and the
  runner authorizes nothing on a blocked turn, so only the dispatcher's own
  response or the result endpoint moves it. If neither arrives the input is
  stuck; `InputSettledOrEffectBlocked` in `AgentEffects` lists the state as a
  blocked terminal rather than claim it settles.
- A cancel does not settle an input whose effect is in doubt:
  `Kernel::complete_turn` refuses, so the input waits for an admin. In
  `AgentIntegrated`, which has no admin, it waits forever, and its liveness
  property lists the in-doubt effect as a blocked terminal.
- Under `provider_idempotency` the provider is assumed to deduplicate on the
  effect id, so a re-dispatch of attempt 1 executes nothing new and answers
  with the first execution's result. That is an assumption about the
  provider, not something the kernel or the runner can check.
- `AgentEffects` holds one accepted input, one step and one intent per turn,
  at most two turns and two attempts; enqueue and timers are not in it.
  `AgentIntegrated` has one client request, no replay drift, no retry after a
  failure and no admin.
- A transient store failure is a stutter, not an action.
- The first client's agent, request id and payload are pinned in `Init`;
  every other choice is the same up to renaming.
- Timers and liveness are not checked with two competing clients, and
  competing senders are not checked with timers in `AgentProtocol` (see
  "AgentProtocol: configs and bounds").
- Not modelled from ADR-3331 decisions 3 to 8: handler version and digest in
  the rebase check and `IncompatibleHandler`; events, effect intents and timer
  changes in the turn completion CAS (timers change only through their own
  CAS); the handler retry policy and dead-letter; the `AlreadyFired`
  answer (a cancel on a delivered generation is not enabled); the receipt index as pages; the HEAD size cap and
  quotas; format versions and `UnsupportedFormat`; operation states other than
  pending and resolved.
- `AgentIntegrated` has no exhaustive check with two workers and a timer:
  its exhaustive config does not finish inside 3000 s and sits under
  `unenrolled/` (see "Exhaustive runs"). `AgentProtocol` is checked
  exhaustively at one timer generation only.

## Exhaustive runs

Budgets and the nightly bound rest on the T20 runs in
[`results.md`](results.md) (fleet executor, x86_64, 16 cores, `-workers 2`,
`-Xmx4g`, revision `8ee0bd2`). Each exhaustive row's `budget_s` is 3000 s.
The nightly runner's speed was not measured: these wall times are this
host's.

- `MCAgentProtocol.exhaustive.cfg` at `MaxTimerGen = 1` finished: 1266296
  distinct states at depth 27 in 1082 s, both liveness properties checked
  (row R12). At `MaxTimerGen = 2` (a scratch copy) it did not finish: stopped
  at the 3000 s budget with 2879377 distinct states and 1058019 queued (row
  R13). The nightly exhaustive lane checks `MaxTimerGen = 1`.
- `MCAgentEffects.exhaustive.cfg` finished three times, 1942312 to 1944848
  distinct states at depth 48 in 110 s to 150 s (rows R11, R16, R17). It has
  no timers.
- `MCAgentIntegrated.exhaustive.cfg` did not finish at `MaxTimerGen = 1`:
  stopped at the 3000 s budget with 44348194 distinct states at BFS level 35
  and 5350152 queued (row R14). At `MaxTimerGen = 2` (a scratch copy):
  stopped at the 3000 s budget with 53745308 distinct states at BFS level 26
  and 14786866 queued (row R15). No violation was found in the states either run
  reached. It stays under `unenrolled/`, which the exhaustive lane does not
  read, with no `bands.tsv` row: a lane expected to time out would turn the
  nightly red every night. Its invariants are checked to completion at the
  smoke bounds (`MaxTimerGen = 0`, row R3) and, with one timer generation and
  one worker, by `positive/integrated-timer-race.cfg` (row R6).

## Running

```sh
scripts/check-tla.sh smoke -a agents
scripts/check-tla.sh live -a agents
scripts/check-tla.sh positive -a agents
scripts/check-tla.sh negative -a agents
scripts/check-tla.sh exhaustive -a agents
```

The `live` lane runs `MCAgentEffects.live.cfg` and
`MCAgentIntegrated.live.cfg` (`AgentProtocol` has no live config: its liveness
runs in its exhaustive config).
