# Agents TLA+ model

A TLA+ model of the agent HEAD protocol in ADR-3331 (task T5 of epic #3331):
enqueue with request identity and receipts, root publication, takeover, turn
completion and rebase, cancellation, and timer delivery through wake hints. It
is the `MCAgentProtocol` module that ADR-3331 decision 13 names; the other two
modules that decision names are not here yet (see "Refinement" below).

`AgentProtocol.tla` is the spec; `MCAgentProtocol.tla` is the model-checking
entry point that extends it with the broken-behavior switches, the reachability
probes, the state view and the worker symmetry. The spec instantiates the shared
`RavelObjectStore.tla` (under `../common`) unchanged, so every payload PUT, turn
output PUT, HEAD create, HEAD CAS, hint write and hint delete obeys the
object-store contract rather than a hand-rolled map.

## What this model claims (ADR-1113 D12)

TLC checked this finite model under the bounds and assumptions in each `.cfg`.
This model checks the protocol design under those bounds; implementation conformance is argued
in the traceability table and asserted by the named Rust tests, not proved.
[`traceability.md`](traceability.md) maps each action and property to the
kernel and store symbols and tests that implement it today; the runtime rows
(lease takeover, worker dispatch, worker crash, wake hints and the sweep, and
liveness) sit under its design heading, pending T9 (#3343) and later tasks. Safety
and liveness are stated separately below, and every liveness result carries its
fairness assumptions. The object store's own conformance to its contract is an
assumption, stated as such and not checked here.

No `results.md` exists yet: recorded figures are task T20. `bands.tsv`
carries two rows. The `MCAgentProtocol.smoke.cfg` row pins the required lane
at 286000 to 292000 distinct states (measured 289101 with 2 workers) with
depth waived by `-`, so a change that narrows what the smoke run reaches fails
that lane. The `MCAgentProtocol.exhaustive.cfg` row sets only `budget_s`
(3000, ADR-3331 decision 13); its distinct band is the full `0`-`999999999`
range and its depth cells are `-`, so the harness enforces nothing on it
until T20 measures it on the nightly runner. The harness requires the distinct
cells to be integers, so that range is the placeholder for an unmeasured row;
`formal/tla/README.md` does not define an unmeasured form.

## Scope

Modelled, each as its own action unless "Folded steps" below says otherwise:

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

## Refinement: AgentEffects and AgentIntegrated

ADR-3331 decision 13 names three modules for this area. `MCAgentEffects`
(active turn, step intent, attempt authorization, external execution with
response loss, result publication, replay, manual resolution) and
`MCAgentIntegrated` (takeover, enqueue, one tool step, final completion, a
timer and cancellation race) follow in task T17. This module treats a turn's
handler as a black box that completes or fails, so `StepTranscriptCompatible`,
`DispatchRequiresCommittedAuthorization` and `PolicyControlsRedispatch`, the
dispatch-before-intent, ambiguous-manual-retry and step-input-drift negatives,
and the duplicate-execution and blocked-effect probes all belong to those
modules and are not here.

## Assumptions

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

## Properties

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

## Configs and bounds

Both configs: one tenant, one agent, two workers, `MaxEpoch = 2`.

| Config | Clients, inputs, request ids, hashes | MaxRevision | MaxTimerGen | Checks |
|---|---|---|---|---|
| `MCAgentProtocol.smoke.cfg` | 2 each | 6 | 0 | the 11 invariants, under `VIEW View` and `SYMMETRY WorkerSymmetry` |
| `MCAgentProtocol.exhaustive.cfg` | 1 each | 4 | 1 | the 11 invariants and both liveness properties, no VIEW or SYMMETRY |

The bounds are split because the full product does not fit a budget. In
local runs, two clients and two workers with one timer generation, even at
`MaxRevision = 4` and with the view and symmetry, passed 2.3 million distinct
states without finishing, and the one-client exhaustive config at
`MaxRevision = 6` had not finished after 9 minutes (about 866,000 distinct
states). So competing senders
are checked without timers (smoke), and timers and liveness are checked with
one sender and `MaxRevision = 4` (exhaustive). A second timer generation does
not fit either: the exhaustive config at `MaxTimerGen = 2` passed 3.9 million
distinct states with 1.4 million queued when the 3000 s budget ran out
(2 workers, 2 GiB). So only the two configs that run at `MaxTimerGen = 2`
reach a replaced generation: `stale-timer-generation-fired`, whose mutant TLC
catches at the first stale fire, which a cancelled generation already
produces at depth 10, and the `second-generation-reachable` probe, which
asserts that a root carrying generation 2 is reached under those bounds. A
`FireTimer` that ignored the generation passes every config at
`MaxTimerGen = 1`. The probe lists only its own invariant. The config that
checks the eleven invariants at those bounds is `stale-timer-generation-fired`,
and it runs with its mutant switched on, so no shipped config checks the
clean spec's invariants on a generation-2 state; a hand run of the exhaustive
config at `MaxTimerGen = 2` is the only check of that today. Raising the
exhaustive bound is T20's call once the nightly lane has measured it.

`View` drops the store's version integers and keeps, for each held CAS token,
only whether it equals the current HEAD version. Nothing reads a version
except that comparison, and versions are never reused, so a token that is not
current never becomes current again. `WorkerSymmetry` permutes workers; no
action, invariant or initial state names a particular worker. Both reductions
are sound for invariants only, which is why the exhaustive config, the one
with `PROPERTY` lines, uses neither.

## Negative controls

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
invariants, smoke bounds, 289101 distinct states as in smoke). In the model
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

## Folded steps

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

Remaining differences, each also stated in its `traceability.md` row:

- The model's `turns` counter names output objects and survives a recreate;
  the kernel derives turn ids and has no counter.

## Gaps

- The HEAD-listing half of the reconciliation sweep is not modelled. Timer
  discovery goes only through hints, so "fair discovery" in ADR-3331's
  liveness condition is weak fairness on `WriteHint` and `FireTimer`.
- A hint is written once per generation; duplicated hints are not modelled.
- One tenant (`Tenants = {t1}`) and one agent in both configs.
  `TenantIncarnationIsolation` is checked across incarnations, not across
  tenants or agents.
- Each worker crashes at most once, and only with a turn output written or a
  completion outcome unknown.
- A transient store failure is a stutter, not an action.
- The first client's agent, request id and payload are pinned in `Init`;
  every other choice is the same up to renaming.
- Timers and liveness are not checked with two competing clients, and
  competing senders are not checked with timers (see "Configs and bounds").
- Not modelled from ADR-3331 decisions 3 to 8: handler version and digest in
  the rebase check and `IncompatibleHandler`; events, effect intents and timer
  changes in the turn completion CAS (timers change only through their own
  CAS); the handler retry policy and dead-letter; the `AlreadyFired`
  answer (a cancel on a delivered generation is not enabled); the receipt index as pages; the HEAD size cap and
  quotas; format versions and `UnsupportedFormat`; operation states other than
  pending and resolved.
- `bands.tsv` uses a placeholder for the unmeasured exhaustive row (see the
  claims section); T20 measures it.

## Running

```sh
scripts/check-tla.sh smoke -a agents
scripts/check-tla.sh negative -a agents
scripts/check-tla.sh exhaustive -a agents
```
