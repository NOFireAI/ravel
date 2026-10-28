# ravel-agents-sim

Deterministic simulation of Ravel Agents (ADR-3331 decision 13). The
production `ravel-agents-store` and `ravel-agents-runtime` crates run over
`MemoryStore` and `FaultStore` under the step scheduler; every recorded
history is checked against the sequential reference machine.

## Running

`cargo test -p ravel-agents-sim` is the only caller. Nothing in the
workspace depends on this crate (`ravel-agents-core`'s
`sim_crate::no_workspace_crate_depends_on_the_sim_crate` checks it), and CI
runs it in the workspace test job (`check` in `.github/workflows/ci.yml`,
`cargo nextest run --workspace`). Every schedule runs on a paused
`current_thread` runtime and the injected clock, so the crate has no wall
clock wait.

## Schedules

The schedules live in `src/schedules/`. Each one has a fixed test with a
hand-written script and a batch over the 64 seeds `0x5eed0000..0x5eed0040`
(`schedules::seeds`). A batch checks every run against the reference machine
and then asserts exact totals. A scheduler or production change that moves
any total fails the batch.

| Schedule | Fixed test | Batch test and pinned total |
|---|---|---|
| Competing senders | `senders::competing_senders_take_distinct_accept_seqs` | `senders::competing_senders_across_seeds`: 108 HEAD CAS conflicts |
| Duplicate and conflict | `senders::same_request_twice_is_a_duplicate`, `senders::same_id_different_payload_conflicts` | `senders::duplicate_and_conflict_across_seeds`: 36 duplicates, 92 conflicts |
| Lost CAS response (acceptance test) | `senders::lost_cas_response_followed_by_later_revisions_still_resolves_the_same_operation`, `senders::a_driver_taking_the_newest_receipt_diverges` | `senders::lost_cas_response_across_seeds`: lost write landed first/second/third 40/13/11 times, 45 conflicts |
| Worker killed at a publication boundary | `workers::worker_killed_{before,after}_{begin_turn_cas,output_put,commit_cas}` (six tests) | `workers::worker_kills_across_seeds`: 52 crashes, 30 lost acknowledgements, 3 inferred events, 2 stale-owner fences, 0 false negatives |
| Suspended worker after a takeover | `workers::suspended_worker_commit_is_refused_after_takeover` | `workers::suspended_worker_across_seeds`: 31 takeovers before the suspended commit |
| Enqueue racing a completion | `races::enqueue_racing_completion_rebases_and_keeps_the_input` | `races::enqueue_racing_completion_across_seeds`: 42 commit-time conflicts |
| Cancel racing a completion | `races::cancel_racing_completion_one_wins` | `races::cancel_racing_completion_across_seeds`: cancel won 46 of 64 |
| Cold restart, second coordinator | `workers::cold_restart_with_a_second_coordinator` | `workers::cold_restart_across_seeds`: 113 of 128 remaining turns run by the restarted worker |
| Typed errors | `errors::*` (eight tests, one per refusal) | `errors::contended_refusals_across_seeds`: client `a` accepted 63 times, 85 conflicts |

The "after" kill points use `DuplicateDelivery` to lose the answer to a
write that applied. Each kill test then runs a replacement worker
(`schedules::drain`) and checks one terminal result per input.

The test `workers::lost_commit_ack_then_takeover_is_a_known_false_negative`
pins a divergence the runtime documents: a commit that applied with its
answer lost, settled after a takeover, answers `OwnershipLost`.

## Turning a failed batch into a named regression test

A failing `sim_assert` prints the seed, the script and the trace of held
calls, for example:

```text
seed: 1592590387
replay: RAVEL_SIM_SEED=1592590387 cargo test -p ravel-agents-sim
schedule: ScheduleScript { steps: [...] }
```

The schedules pass an explicit seed to the scheduler, so `RAVEL_SIM_SEED`
does not narrow a batch. To reproduce a failure:

1. Copy the seed from the message.
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
  has bound. ADR-3331 and the wire doc say `requested: false`.
- `workers::stale_owner_commit_after_takeover_is_fenced`: the reference
  machine cannot express the ownership fence.
- `workers::lost_commit_ack_then_takeover_is_a_known_false_negative`: an
  applied commit can answer `OwnershipLost`.
- `drive::map_error` reports `OutcomeUnknown` as unmappable. No schedule
  leaves one unsettled. `checked` fails on any unmappable call.
