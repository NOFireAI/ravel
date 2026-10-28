# Agents results

TLC checked these finite models under the bounds and assumptions in each
`.cfg`. This checks the protocol design under those bounds; whether the Rust
implementation conforms is argued in `traceability.md` and asserted by the
Rust tests it names. Every figure below comes from a run made for task T20
(#3354) on the host described here; `bands.tsv` cites the row each band rests
on.

- Revision: `8ee0bd2564ab9b5cb70ea27c76f41d11723c9c35` (tree
  `4c08bec3441a1cc5e7e0584a427256b538c25b85`, the suffix of every
  `check-tla.sh` run id below).
- Tool: `tla2tools` 1.7.4 (TLC2 version 2.19), Java 21.0.13 (Temurin).
- Host: fleet executor, x86_64, 16 cores, 30.6 GiB memory.
- TLC settings on every run: `-workers 2`, `-Xmx4g`, `-XX:+UseParallelGC`,
  breadth-first search, `-deadlock` (every agents cfg sets
  `CHECK_DEADLOCK FALSE`). No run ran out of heap at 4 GiB.
- Harness rows carry the `check-tla.sh` run id (`RAVEL_TLA_WORKERS=2`,
  `RAVEL_TLA_XMX=4g`) and its `last-run.tsv` figures. Direct rows ran TLC with
  the same flags outside the harness, on a scratch copy of the cfg where the
  MaxTimerGen column says so; their run id is the UTC start time and a tag.
- Concurrency: R13 and R14 (13:47 to 14:37 UTC) and R15 (from 14:11 UTC)
  ran beside each other and beside R4 to R12 and R16 to R19, at most four TLC
  processes at once with two workers each. Wall times are this host's under
  that load.

## Runs

| Row | Run id | Lane | Config | MaxTimerGen | States | Distinct | Depth | Wall | Result |
|---|---|---|---|---|---|---|---|---|---|
| R1 | `20261028T134505Z-4c08bec` | smoke | `MCAgentProtocol.smoke.cfg` | 0 | 1131064 | 289101 | 26 | 31 s | PASS |
| R2 | `20261028T134505Z-4c08bec` | smoke | `MCAgentEffects.smoke.cfg` | - | 726839 | 222728 | 45 | 12 s | PASS |
| R3 | `20261028T134505Z-4c08bec` | smoke | `MCAgentIntegrated.smoke.cfg` | 0 | 1455417 | 427920 | 44 | 23 s | PASS |
| R4 | `20261028T134742Z-4c08bec` | positive | `positive/stale-turn-rebase-unreachable.cfg` | 0 | 1131064 | 289101 | 26 | 32 s | PASS |
| R5 | `20261028T134742Z-4c08bec` | positive | `positive/two-generations.cfg` | 2 | 1155643 | 243171 | 31 | 26 s | PASS |
| R6 | `20261028T134742Z-4c08bec` | positive | `positive/integrated-timer-race.cfg` | 1 | 3641066 | 1190621 | 45 | 62 s | PASS |
| R7 | direct `20261028T141157Z` Esmoke1 | smoke (repeat) | `MCAgentEffects.smoke.cfg` | - | 726701 | 222675 | 45 | 16 s | PASS |
| R8 | direct `20261028T141213Z` Esmoke2 | smoke (repeat) | `MCAgentEffects.smoke.cfg` | - | 726431 | 222626 | 45 | 15 s | PASS |
| R9 | direct `20261028T141228Z` Esmoke3 | smoke (repeat) | `MCAgentEffects.smoke.cfg` | - | 726559 | 222639 | 45 | 16 s | PASS |
| R10 | `20261028T135032Z-4c08bec` | negative | all 42 `negative/*.cfg` | - | - | - | - | - | 42 VIOLATED as expected |
| R11 | `20261028T135202Z-4c08bec` | exhaustive | `MCAgentEffects.exhaustive.cfg` | - | 6873961 | 1942312 | 48 | 110 s | PASS |
| R12 | `20261028T135202Z-4c08bec` | exhaustive | `MCAgentProtocol.exhaustive.cfg` | 1 | 4444815 | 1266296 | 27 | 1082 s | PASS |
| R13 | direct `20261028T134701Z` P2 | exhaustive (scratch) | `MCAgentProtocol.exhaustive.cfg` | 2 | 7807396 | 2879377 | 19 | 3001 s | NOT FINISHED: stopped at the 3000 s budget, 1058019 states queued |
| R14 | direct `20261028T134701Z` I1 | exhaustive (scratch) | `unenrolled/MCAgentIntegrated.exhaustive.cfg` | 1 | 184332281 | 44348194 | 35 | 3001 s | NOT FINISHED: stopped at the 3000 s budget, 5350152 states queued |
| R15 | direct `20261028T141116Z` I2 | exhaustive (scratch) | `unenrolled/MCAgentIntegrated.exhaustive.cfg` | 2 | 201468006 | 53745308 | 26 | 3000 s | NOT FINISHED: stopped at the 3000 s budget, 14786866 states queued |
| R16 | direct `20261028T141254Z` Eexh1 | exhaustive (repeat) | `MCAgentEffects.exhaustive.cfg` | - | 6874592 | 1942586 | 48 | 150 s | PASS |
| R17 | direct `20261028T141524Z` Eexh2 | exhaustive (repeat) | `MCAgentEffects.exhaustive.cfg` | - | 6883160 | 1944848 | 48 | 117 s | PASS |
| R18 | `20261028T134958Z-4c08bec` | live | `MCAgentEffects.live.cfg` | - | 267170 | 127749 | 43 | 20 s | PASS |
| R19 | `20261028T134958Z-4c08bec` | live | `MCAgentIntegrated.live.cfg` | 0 | 28110 | 13617 | 35 | 6 s | PASS |
| R20 | `20261028T150259Z-e449059` | smoke (final tree) | `MCAgentEffects.smoke.cfg` | - | 726016 | 222504 | 45 | 12 s | PASS |

MaxTimerGen is `-` for `AgentEffects`, which has no timers. For an unfinished
run, the states, distinct and queued figures are TLC's last progress line
before the stop and the depth is the BFS level that line reports; they are
not totals. R13, R14 and R15 found no violation in the states they reached.
A first launch of R15 at 13:47 UTC failed before exploring any state (TLC
exit 150, a module-table lookup failure while three TLC processes started
side by side on the same module directory); it was relaunched unchanged as
R15.

`AgentEffects` configs use `VIEW` and `SYMMETRY` and their distinct count
moves between runs of the same config under two workers: 222504 to 222728
for the smoke config (R2, R7 to R9, R20) and 1942312 to 1944848 for the
exhaustive config (R11, R16, R17). The other configs gave the same distinct
figure each time they were run (R1 and R4 share a state space). The bands are
wide enough for that spread.

`MCAgentProtocol.exhaustive.cfg` at MaxTimerGen 1 measured 1266296 distinct
states (R12). Its header comment and the T17 notes gave 362907 from an
earlier run, which no run on this revision reproduced; the comment now cites
R12.

## MaxTimerGen and the exhaustive bound

| Module | MaxTimerGen 1 | MaxTimerGen 2 | Exhaustive lane checks |
|---|---|---|---|
| `AgentProtocol` | finished, 1082 s (R12) | not finished in 3000 s (R13) | MaxTimerGen 1 |
| `AgentIntegrated` | not finished in 3000 s (R14) | not finished in 3000 s (R15) | nothing: the config stays under `unenrolled/` |
| `AgentEffects` | no timers | no timers | its exhaustive config (R11) |

The exhaustive row for `AgentProtocol` stays at MaxTimerGen 1, the largest
bound that finished inside the 3000 s `budget_s`. The second timer generation
is checked by `positive/two-generations.cfg` (R5) at one client and
`MaxRevision = 4` under the view and symmetry, without liveness.
`AgentIntegrated` does not fit at MaxTimerGen 1, so it keeps no exhaustive row
and its config stays unenrolled; its invariants are checked to completion at
MaxTimerGen 0 by the smoke config (R3), and with one timer generation at one
worker by `positive/integrated-timer-race.cfg` (R6).

## What each lane checks

- smoke (ci.yml's TLA job): the three smoke configs to completion, each inside
  its band (R1 to R3). It checks every invariant at the smoke bounds; it
  checks no liveness and no timer in `AgentProtocol` or `AgentIntegrated`.
- negative (ci.yml's TLA job): each of the 42 negative configs must violate
  its named property (R10). It checks that each switch is caught; it pins no
  state count, since TLC stops at the first counterexample.
- positive: the three positive configs to completion inside their bands (R4 to
  R6). It checks the second timer generation in `AgentProtocol` and the timer
  race in `AgentIntegrated` with one worker; it checks no liveness.
- live (tla-nightly): the two live configs to completion inside their bands
  (R18, R19), safety and liveness at one worker and one epoch. It does not
  check liveness with takeover, and `AgentProtocol` has no live config: its
  liveness runs in its exhaustive config.
- exhaustive (tla-nightly): `MCAgentProtocol.exhaustive.cfg` at MaxTimerGen 1
  with both liveness properties (R12) and `MCAgentEffects.exhaustive.cfg`,
  safety only (R11), each inside its band and its 3000 s budget. It does not
  check `AgentProtocol` at a second timer generation, nor `AgentIntegrated`
  with two workers and a timer: those runs did not finish (R13, R14).

## Distinguishing check

With `MCAgentIntegrated.live.cfg`'s band narrowed to 13350 to 13600, below the
measured 13617, `scripts/check-tla.sh live -a agents` failed with exit 1:

```
check-tla: agents bands: MCAgentIntegrated.live.cfg distinct=13617 outside [13350,13600]
check-tla: agents/MCAgentIntegrated live: figures outside declared band (log: .cache/tla/logs/agents-MCAgentIntegrated-live.log)
```

The band was then restored to 13350 to 13900.
