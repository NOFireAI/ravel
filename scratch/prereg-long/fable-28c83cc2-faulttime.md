# Decision: C (narrowed: drawn decrease + forged read; fence stays eventually-only)

**Reasoning.**

Checked the drawn-rule machinery (main.rs:2983-3221): the fold race, record race, both tamper races and both sweeps are each a drawn rule (weights 1, draw probabilities 0.02–0.03) repeated in the eventually until reached once — exactly ADR-4326's forced-situation amendment norm. The reshard situations deviate (README "Not here", pre-registered), so the deviation is deliberate but against the norm, and the epic's stated purpose is checking these invariants *under* crashes, pauses and partitions. Resharding is decision 2's flagship clock-control scope (`StaleWriterFailsClosed`, `TokenResolvesAcrossReshards`); confirming them only after faults stop is the gap worth closing — but only where closing it is sound:

- **Decrease + straggler: worth a drawn variant.** No decrease exists before the eventually, so token resolution above the active count never runs under faults at all. The assertions are fault-safe: `assert_token_resolves_across_reshards` (catalog.rs:5291) uses `inv_always_if_known!` — store errors other than NotFound evaluate nothing; `observe_ack` skips attempts whose hours route at two counts. No false-positive surface.
- **Fence: keep eventually-only.** (1) `catalog: the system recovers once faults stop` after `thaw-read` (reshard.rs:416) is an Always whose premise is faults stopped; run under faults, a partition starving `wait_serving` past 300 s is a false counterexample that trips stop rule 1. (2) The fence's SUT Always in `try_grace_extend` (generation.rs:1672) is judged from the router's own `now_ns`/`refreshed_at_ns`, so it is self-consistent and already evaluates under faults organically: natural fault-refused record re-reads trigger grace routing, and the drawn 9000 s sealing jump exceeds the 2 h lead, so horizon crossings and fenced writes can occur under faults without the forced rule. The forced refuse-read adds determinism, not fault-time coverage, and is confounded with proxy faults.
- **Forged read: cheap, piggyback after the drawn decrease.** Retry loop already classes 503/no-response apart from unsatisfiable.
- **Mutants: eventually-only suffices** (ADR-4326 decision 3 needs only property-false), so smoke + 4 mutants launch now unchanged — nothing in them judges fault-time coverage.

**Conditions.**

1. Launch smoke, then mutants, as pre-registered (75 h; local falsification precondition unchanged).
2. Drawn-variant PR in parallel: no new assertion literals (keeps the 126-property set stable), rebuild, re-falsify all four mutants locally at the new ref, rerun the plain local run, re-register the long run before launch.
3. Report for smoke/mutants must not claim: "the stale-writer fence, the 6→4 decrease, the straggler path and forged-token validation were exercised under fault injection." Claimable: falsifiability (mutants), deterministic reachability, increase + activation writes and grace routing under faults, catalog races across both reshards from fault-varied pre-states.

**Drawn-variant spec.**

`decrease_drawn` rule: preconditions — sealing jump landed, `eventually_on`, low draw (~0.02), at-most-once-per-timeline flag (as `unfolded_started`); eventually keeps its cycles as backstop when unreached. Serial under the gate write guard for the whole cycle: ensure-increase nudge, `provision` decrease, nudge to activation−30 s, burst, boundary wait, anchor writes (clock moves and write attribution require exclusive writes; reuse `decrease_cycle` with a reason parameter). Straggler reads may release the gate. Forged read after the cycle, gate-free, keeping the 3-attempt retry.

**Prereg amendment text to post:**

> Amendment to the reshard-ownership wave pre-registration: the smoke and the four mutant runs launch as registered. The long run is deferred and will be re-registered: a drawn decrease-with-burst rule (and the forged read after it) is added first so the straggler and token-resolution paths run while faults are on. The fence stays eventually-only: its recovery Always assumes faults stopped, and its SUT Always already evaluates under faults through natural read failures and the drawn sealing jump. No assertion literal changes; the long run's re-registration carries new images, new local falsifications and the same 126-property set. Smoke and mutant reports will not claim fault-time coverage of the fence, the decrease or the forged read.

**Out-of-scope note:** none found; tree matches b37d51e4f as required.
