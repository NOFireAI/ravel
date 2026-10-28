# ravel-agents-sim

Deterministic simulation of Ravel Agents (ADR-3331 decision 13). The
production `ravel-agents-store` and `ravel-agents-runtime` crates run over
`MemoryStore` and `FaultStore` under the step scheduler; every recorded
history is checked against the sequential reference machine.

## Running

`cargo test -p ravel-agents-sim` is the only caller. Nothing in the
workspace depends on this crate (`ravel-agents-core`'s
`sim_crate::no_workspace_crate_depends_on_the_sim_crate` checks it), and CI
runs it in the sharded workspace test job (`check-shard` in
`.github/workflows/ci.yml`, `cargo nextest run --locked --workspace
--cargo-profile ci-opt --partition hash:N/M`); the `check` job aggregates
the shards (`needs: [changes, check-shard]`). Every schedule runs on a paused
`current_thread` runtime and the injected clock, so the crate has no wall
clock wait.

## Schedules

The schedules live in `src/schedules/`. Each one has a fixed test with a
hand-written script and a batch over the 64 seeds `0x5eed0000..0x5eed0040`
(`schedules::seeds`), or over the 64 seeds from `RAVEL_SIM_SEED` when that
is set. A batch checks every run against the reference machine
and then asserts exact totals. A scheduler or production change that moves
any total fails the batch.

| Schedule | Fixed test | Batch test and pinned total |
|---|---|---|
| Competing senders | `senders::competing_senders_take_distinct_accept_seqs` | `senders::competing_senders_across_seeds`: 108 HEAD CAS conflicts |
| Duplicate and conflict | `senders::same_request_twice_is_a_duplicate`, `senders::same_id_different_payload_conflicts` | `senders::duplicate_and_conflict_across_seeds`: 36 duplicates, 92 conflicts |
| Lost CAS response (acceptance test) | `senders::lost_cas_response_followed_by_later_revisions_still_resolves_the_same_operation`, `senders::a_driver_taking_the_newest_receipt_diverges` | `senders::lost_cas_response_across_seeds`: lost write landed first/second/third 40/13/11 times, 45 conflicts |
| Worker killed at a publication boundary | `workers::worker_killed_{before,after}_{begin_turn_cas,output_put,commit_cas}` (six tests) | `workers::worker_kills_across_seeds`: 52 crashes, 30 runs where `DuplicateDelivery` fired (each a lost acknowledgement of an applied write or a conflicted CAS answered `Transient`), 3 inferred events, 2 stale-owner fences, 0 false negatives |
| Suspended worker after a takeover | `workers::suspended_worker_commit_is_refused_after_takeover` | `workers::suspended_worker_across_seeds`: 31 takeovers before the suspended commit |
| Enqueue racing a completion | `races::enqueue_racing_completion_rebases_and_keeps_the_input` | `races::enqueue_racing_completion_across_seeds`: 42 commit-time conflicts |
| Cancel racing a completion | `races::cancel_racing_completion_one_wins` | `races::cancel_racing_completion_across_seeds`: cancel won 46 of 64 |
| Cold restart, second coordinator | `workers::cold_restart_with_a_second_coordinator` | `workers::cold_restart_across_seeds`: 113 of 128 remaining turns run by the restarted worker |
| Typed errors | `errors::*` (eight tests, one per refusal) | `errors::contended_refusals_across_seeds`: client `a` accepted 63 times, 85 conflicts |
| Effect kill points, provider idempotency | `effects::provider_idempotency_{killed_before_tool_call,killed_after_execution,suspended_before_result_publication}` | `effects::provider_idempotency_kills_across_seeds`: 114 crashes, 29 inferred, 144 dispatches of 128 distinct attempts, 128 executions, 0 duplicates |
| Effect kill points, at least once and manual | `effects::at_least_once_{killed_before_tool_call,killed_after_execution,late_result_after_takeover_is_evidence}`, `effects::manual_{killed_before_tool_call,killed_after_execution,suspended_before_result_publication}` | `effects::at_least_once_duplicate_execution_is_reachable_and_manual_policy_never_redispatches`: at least once 140 executions with 12 duplicates; manual 0 duplicates, 15 in doubt, 15 reconciled, each authorized attempt dispatched by the next step of the process that reconciled it |
| Cancel with an open attempt | `effects::cancelled_turn_with_open_attempt_does_not_wedge_the_next_step`, `effects::late_result_after_cancel_is_evidence` | `effects::cancels_across_seeds`: 54 crashes, 43 cancelled, 2 duplicates |
| Conflicting results | `effects::conflicting_results_for_one_attempt_are_idempotency_conflict` | `effects::conflicting_reporters_across_seeds`: 64 conflicts |
| Handler version change | `effects::handler_version_change_while_a_turn_is_suspended` | `effects::handler_version_changes_across_seeds`: 134 crashes, 39 polls refused `IncompatibleHandler`, 0 duplicates |
| Unknown handle, journal and failed attempt | `effects::query_of_a_handle_with_no_input`, `effects::journaled_step_with_a_failed_attempt_replays_the_failure` | none |

The "after" kill points use `DuplicateDelivery` to lose the answer to a
write that applied. Each kill test then runs a replacement worker
(`schedules::drain`) and checks one terminal result per input.

The effect batches with more than one worker run the workers one at a
time (`relay` in `schedules/effects.rs`), with the clock past the lease
before each one after the first, so each worker takes over from a dead
or abandoned owner. Two workers polling concurrently reach the takeover
race listed under known gaps, and that race would fail every batch that
reaches it.

The test `workers::lost_commit_ack_then_takeover_is_a_known_false_negative`
pins a divergence the runtime documents: a commit that applied with its
answer lost, settled after a takeover, answers `OwnershipLost`.

## Turning a failed batch into a named regression test

A failing `sim_assert` prints the batch seed, the seed the scheduler drew
from, the script and the trace of held calls, for example:

```text
batch seed: 1592590387 (0x5eed0033)
scheduler seed: 1592590387 (0x5eed0033)
test: schedules::workers::worker_kills_across_seeds
replay: RAVEL_SIM_SEED=1592590387 cargo test -p ravel-agents-sim -- --exact schedules::workers::worker_kills_across_seeds
schedule: ScheduleScript { steps: [...] }
```

The two seeds differ under `relay`, which derives each later worker's
seed from the batch seed. The replay line sets `RAVEL_SIM_SEED` to the
batch seed, and a batch starts from it, so the failing seed runs first.
The batch's pinned totals then fail too, since they hold only for the
default seeds. To keep a failure as a regression test:

1. Copy the batch seed from the message.
2. Add a `#[test]` beside the batch. It calls the batch's schedule function
   with that seed as a literal.
3. Assert the exact counts that run returns.
4. Name the test after the behaviour, not the seed.
5. When the failure is a defect in the store, runtime or kernel, mark the
   test `#[ignore = "suspected defect: ..."]`, asserting what the
   specification says, and report it.

Real example: the first run of `workers::worker_kills_across_seeds` failed
on a stale-owner divergence. Two seeds of the batch hit it: `0x5eed001a`
and `0x5eed0033` (decimal 1592590387). The second has no crash, so it is the
one pinned. In that run `w1` lost the answer to a HEAD write, `w2` took
over and began the same turn again, and `w1`'s commit was then fenced
`OwnershipLost`. The reference expected `Committed`
there, because it has no worker identity. The production fence is
correct, so `workers::past_known_divergences` drops that call and counts
it. The run is pinned as `workers::stale_owner_commit_after_takeover_is_fenced`.

## Known gaps

Each of these is a test in the crate:

- `races::cancel_of_an_accepted_input_reports_requested_false` (ignored):
  production answers `requested: true` when it cancels an input no turn
  has bound. ADR-3331 and the wire doc say `requested: false`. Tracked
  on #3404.
- `effects::takeover_retried_on_fresh_head_fences_a_live_turn` (ignored):
  two workers poll at once. One begins the turn while the other's
  takeover loses its CAS; the store's publish loop re-applies the takeover
  on the fresh HEAD, the kernel checks only the observed epoch, and the
  live turn is fenced `OwnershipLost` before its lease ran out.
  `Coordinator::takeover_due` documents a takeover as allowed only while
  the owner holds no active turn or its lease and grace ran out, a check
  the retry never repeats. 25 of the 64 seeds reach it with no fault.
  Tracked on #3415.
- `workers::stale_owner_commit_after_takeover_is_fenced`: the reference
  machine cannot express the ownership fence.
- `workers::lost_commit_ack_then_takeover_is_a_known_false_negative`: an
  applied commit can answer `OwnershipLost`.
- `drive::map_error` reports `OutcomeUnknown` as unmappable. No schedule
  leaves one unsettled. `checked` fails on any unmappable call.
