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

`crates/ravel-agents-sim/nightly.sh [log-file]` runs the same tests with
`--nocapture` and checks that every schedule's batch printed exactly one
line of the form

```text
sim-batch <schedule> base=<first seed> seeds=<count> pages=<max page entries> totals=<...> pinned=<...> match|unpinned
```

It exits 1 when the tests fail or a schedule's line is missing, doubled or
unknown (`schedules::pinned` prints the line; the schedule list is in the
script). `.github/workflows/sim-nightly.yml` runs it every night twice: on
the default batch, then on a fresh window of 512 seeds per schedule whose
first seed is the UTC day number times 1,000,000. The job summary carries
both sets of lines and the replay command for the window.

## Schedules

The schedules live in `src/schedules/`. Each one has a fixed test with a
hand-written script and a batch over the 64 seeds `0x5eed0000..0x5eed0040`
(`schedules::seeds`). `RAVEL_SIM_SEED` (decimal) moves the first seed and
`RAVEL_SIM_SEEDS` sets the count; a value that does not parse, or a count of
0, fails every batch (`schedules::tests::a_batch_reads_its_base_and_count_and_refuses_what_does_not_parse`).
A batch checks every run against the reference machine. On the default
batch it then asserts exact totals, so a scheduler or production change that
moves any total fails it; any other batch prints its totals as `unpinned`
and is checked per seed only
(`schedules::tests::a_batch_line_names_its_verdict`).

`RAVEL_AGENTS_PAGE_ENTRIES` sets the store's `max_page_entries` for every
rig (`drive::sim_limits`; default 64, the production default). At 2 every
paged list seals after two entries, so the batches exercise sealed inbox,
receipts, results and timers pages and `Rig::head` loads them before a
check. The pins in the table below are for the default page size; at 2 each
batch asserts its own pin set (`schedules::pinned_by_page`), and any other
size prints `unpinned`. CI's features-agents job, the agents lane of
`scripts/gates.sh` and the sim-nightly workflow run the default batch at 2
as well as at the default size. Every page GET and PUT is a scheduler gate, so a
smaller page moves where the seeded script runs out, and past it the
scheduler releases the lowest actor first. Four totals move at 2 for that
reason, no invariant changes. The last three were re-measured at both page
sizes after the worker poll stopped listing the tenant (issue #3353, see
below): that change moved the default pins, and the same fewer store calls
per poll moved the pins at 2 (lost CAS response 40 to 25 conflicts, worker
kills 4 to 3 inferred and 1 to 0 stale-owner fences, cold restart 108 to
104 turns).

| Batch | Default | At 2 | Why it moves |
|---|---|---|---|
| `races::enqueue_racing_completion_across_seeds` | 42 conflicts | 31 | `r2` seals nothing; `r3` always seals a receipts page, and seals an inbox page too when `w` has not committed, each written before its CAS, so the script runs out sooner and `w`'s commit lands unraced more often |
| `senders::lost_cas_response_across_seeds` | 28 conflicts | 25 | the page writes ahead of each send's CAS spend more of the script on calls that cannot conflict; the 44/11/9 landing positions do not move |
| `workers::worker_kills_across_seeds` | 2 inferred, 0 stale-owner fences | 3 inferred, 0 stale-owner fences | the page reads and writes move which call a scripted release lands on, and one more run settles a lost answer by inference |
| `workers::cold_restart_across_seeds` | 112 turns | 104 | `c`'s page writes and result-page reads release `c` ahead of `w-restarted` past the script, leaving more inputs to the drain |

The single-seed reproductions in `workers.rs` (seeds 0x5eed00db, 20000471
and 4000067) run at the default page size whatever the variable says,
since each names an interleaving found there.

| Schedule | Fixed test | Batch test and pinned total |
|---|---|---|
| Competing senders | `senders::competing_senders_take_distinct_accept_seqs` | `senders::competing_senders_across_seeds`: 108 HEAD CAS conflicts |
| Duplicate and conflict | `senders::same_request_twice_is_a_duplicate`, `senders::same_id_different_payload_conflicts` | `senders::duplicate_and_conflict_across_seeds`: 36 duplicates, 92 conflicts |
| Lost CAS response (acceptance test) | `senders::lost_cas_response_followed_by_later_revisions_still_resolves_the_same_operation`, `senders::a_driver_taking_the_newest_receipt_diverges` | `senders::lost_cas_response_across_seeds`: lost write landed first/second/third 44/11/9 times, 28 conflicts |
| Worker killed at a publication boundary | `workers::worker_killed_{before,after}_{begin_turn_cas,output_put,commit_cas}` (six tests) | `workers::worker_kills_across_seeds`: 28 crashes, 19 runs where `DuplicateDelivery` fired (each a lost acknowledgement of an applied write or a conflicted CAS answered `Transient`), 2 inferred events, 0 stale-owner fences, 0 false negatives, 0 sends retried past the CAS bound, 0 escaped `OutcomeUnknown` |
| Suspended worker after a takeover | `workers::suspended_worker_commit_is_refused_after_takeover` | `workers::suspended_worker_across_seeds`: 31 takeovers before the suspended commit |
| Enqueue racing a completion | `races::enqueue_racing_completion_rebases_and_keeps_the_input` | `races::enqueue_racing_completion_across_seeds`: 42 commit-time conflicts |
| Cancel racing a completion | `races::cancel_racing_completion_one_wins` | `races::cancel_racing_completion_across_seeds`: cancel won 46 of 64 |
| Cold restart, second coordinator | `workers::cold_restart_with_a_second_coordinator` | `workers::cold_restart_across_seeds`: 112 of 128 remaining turns run by the restarted worker |
| Typed errors | `errors::*` (eight tests, one per refusal) | `errors::contended_refusals_across_seeds`: client `a` accepted 63 times, 85 conflicts |
| Effect kill points, provider idempotency | `effects::provider_idempotency_{killed_before_tool_call,killed_after_execution,suspended_before_result_publication}` | `effects::provider_idempotency_kills_across_seeds`: 114 crashes, 29 inferred, 144 dispatches of 128 distinct attempts, 128 executions, 0 duplicates |
| Effect kill points, at least once and manual | `effects::at_least_once_{killed_before_tool_call,killed_after_execution,late_result_after_takeover_is_evidence}`, `effects::manual_{killed_before_tool_call,killed_after_execution,suspended_before_result_publication}` | `effects::at_least_once_duplicate_execution_is_reachable_and_manual_policy_never_redispatches`: at least once 140 executions with 12 duplicates; manual 0 duplicates, 15 in doubt, 15 reconciled, each authorized attempt dispatched by the next step of the process that reconciled it |
| Cancel with an open attempt | `effects::cancelled_turn_with_open_attempt_does_not_wedge_the_next_step`, `effects::late_result_after_cancel_is_evidence` | `effects::cancels_across_seeds`: 54 crashes, 43 cancelled, 2 duplicates |
| Worker dead between its step journal and its cancel check | `effects::a_worker_dead_between_its_step_journal_and_its_cancel_check_is_fenced` (seed 7000441): 1 crash, 1 inferred `OwnershipLost`, 1 cancelled | covered by `effects::cancels_across_seeds` |
| Client send starved past the store's CAS bound | `workers::a_send_starved_past_the_cas_bound_is_refused_and_retried` (seed 20000471): 1 crash, 1 `StoreUnavailable` send, retried | covered by `workers::worker_kills_across_seeds` |
| Timer fire racing a cancel | `timers::a_cancel_{before_the_fire_wins_and_the_sweep_fires_nothing,at_the_fire_that_lands_first_wins,at_the_fire_that_lands_second_is_already_fired,after_the_fire_is_already_fired}` | `timers::fire_cancel_race_across_seeds`: cancel won 43, already fired 21 |
| Duplicate hint delivery | `timers::a_premature_hint_fires_nothing_and_is_kept`, `timers::a_hint_in_a_later_bucket_is_not_listed`, `timers::a_duplicate_hint_fires_its_generation_once`, `timers::two_sweepers_on_one_hint_fire_it_once` | `timers::duplicate_hint_delivery_across_seeds`: 128 HEAD reads, 125 hints deleted |
| Lost hint PUT | `timers::a_lost_hint_put_is_delivered_by_the_sweep_within_its_interval`, `timers::a_lost_hint_put_due_on_a_pass_fires_on_that_pass`, `timers::a_sweep_whose_fire_write_fails_is_a_store_unavailable_refusal` | `timers::lost_hint_puts_across_seeds`: firing pass indexes sum to 286 |
| Crash around the hint write | `timers::a_crash_between_the_root_write_and_the_hint_write_still_fires` | `timers::crashes_around_the_hint_write_across_seeds`: 42, 7 and 15 runs per crash point |
| Recovery storm beside telemetry ingest | `storm::recovery_storm_leaves_telemetry_ingest_within_its_own_budget` | `storm::recovery_storms_across_seeds`: 393 passes, 4 LISTs and 8 HEAD reads each, longest ingest wait at the band (`INGEST_WAIT_BAND`, the sweep's per-pass request bound) |
| Conflicting results | `effects::conflicting_results_for_one_attempt_are_idempotency_conflict` | `effects::conflicting_reporters_across_seeds`: 64 conflicts |
| Handler version change | `effects::handler_version_change_while_a_turn_is_suspended` | `effects::handler_version_changes_across_seeds`: 134 crashes, 39 polls refused `IncompatibleHandler`, 0 duplicates |
| Unknown handle, journal and failed attempt | `effects::query_of_a_handle_with_no_input`, `effects::journaled_step_with_a_failed_attempt_replays_the_failure` | none |

The lost CAS response, worker kill and cold restart totals moved when a
worker poll stopped listing the tenant itself (issue #3353): the poll path
no longer issues `discover()`'s LIST and HEAD GET calls, so a poll makes
fewer store calls. Fewer calls are fewer points where the scheduler
interleaves processes, so the scripted crashes and lost acknowledgements
land at other calls, and fewer of them land at all. The same shift moved
the batch's one stale-owner run out of the 64 seeds; it is pinned from
seed `0x5eed00db` instead. It moved two single-seed pins found under the
old poll as well: the starved send is pinned from seed 20000471, which
also crashes a worker (no seed in the 512-seed windows of days 1 to 450
starves the send with no other event; 392000268 also fences a stale
owner), and the escaped `OutcomeUnknown` from seed 4000067. Each worker process sweeps on every idle poll with one LIST a
pass (`drive::poll_sweep`), which goes to the directory half; a client
process sweeps with the runtime's default caps (`Rig::client_proc`), so a
pass a timer schedule runs lists both halves.

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
The batch is then not the default one, so its totals print as `unpinned`
and only the per-seed checks apply. To keep a failure as a regression test:

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
Since the poll change above no seed of the batch reaches that shape, and
the test runs seed `0x5eed00db`, past the batch, which does.

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
- `workers::a_lost_begin_turn_answer_after_a_spent_retry_is_settled_by_the_poll`
  (ignored): a poll whose `BeginTurn` CAS lost its answer once and was
  retried, then lost the retry's answer too, hands `OutcomeUnknown` to the
  worker instead of settling it (issue #3425). The runtime spends one retry
  flag on both `OwnershipLost` and `OutcomeUnknown`. Seed 4000067 reaches
  it, and so do 12000370 and 16000166 in 512-seed windows; none of the
  default seeds does.
  `drive::map_error` reports `OutcomeUnknown` as unmappable, and
  `workers::kills` counts this one shape (`escaped_unknown`, pinned 0 on the
  default batch, and 1 by
  `workers::a_lost_begin_turn_answer_after_a_spent_retry_escapes_the_poll_today`)
  instead of failing the batch. `checked` fails on any other unmappable call.
- `drive` maps a crashed step whose root holds only its journaled start,
  after a cancel, to an inferred `OwnershipLost`
  (`drive::tests::a_step_crashed_after_journaling_past_a_cancel_infers_ownership_lost`):
  production journals the step before its cancel check, so the dead
  worker's step lands past the cancel the reference applied first.
