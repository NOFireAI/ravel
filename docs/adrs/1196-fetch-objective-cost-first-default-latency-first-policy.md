# ADR-1196: Keep the cost-first fetch default, add a latency-first policy

- Status: Proposed
- Date: 2026-09-03
- Refs: #1196, #1191, #1170, #1007, #1185, ADR-0904, ADR-0996, ADR-0046

## Context

The fetch objective decides, for every logs scan, whether to read an object whole
or to range-read the blocks a projection needs. Today that decision is made
purely on money.

`resolve_logs_fetch` derives a byte scalar from the active `StoreCostProfile`
(`crates/ravel-query/src/config.rs:255-324`). ADR-0904 defines the unit as a pure
price ratio: "`price_per_request / price_per_byte` has units of bytes, so a byte
value expresses every dollar preference exactly." The only shipped profile,
`s3-intra-region-2026`, prices transfer and retrieval at zero
(`crates/ravel-types/src/cost_profile.rs:149-156`), so `resolve_cost_based_rate`
returns `u64::MAX` (`config.rs:301-309`) and whole-object reads always win.
Nothing in the model expresses time.

Measured on the reference machine, true cold, 42 statements, 2,617 objects /
11.24 GB (#1185):

| Configuration | Bytes | Time | GET requests |
|---|---|---|---|
| `cost-based` (stock) @ concurrency 256 | 463.79 GB | 486.0 s | 104,780 |
| `byte-minimal` @ concurrency 256 | 150.28 GB | **285.8 s** | 570,752 |

**The money-minimising objective selects the slower plan, by 41%.** That is the
finding this ADR exists to answer. It is not an argument that the objective is
wrong: intra-region bytes really are free, so cost-based is right about the bill.
It is an argument that the bill is not the only thing an operator may be
optimising, and that today they cannot say so.

Two things this ADR does not claim, both refuted earlier in the investigation:

- That `DEFAULT_LOG_REQUEST_COST_BYTES` is mispriced by ~27x at high concurrency.
  Both `byte-minimal` rows used that same scalar and issued identical requests
  and identical bytes, so it was never the variable between the losing and
  winning runs.
- That raising concurrency alone delivers the win. On the whole-object path it
  measured 7.4%, which does not survive the per-statement floor discrepancy in
  the same data. Concurrency is what makes the ranged path viable (596.3 s of
  transfer at 32 against 171.0 s at 256), not a win in itself. ADR-1195 covers
  the knob.

## Decision

**Cost-first remains the default.** `cost-based` at the reference profile keeps
resolving to whole-object reads. A deployment that has not asked for anything
else keeps the cheaper S3 bill, and the published stock ClickBench entry
continues to reflect it.

**Add `latency-first` as a named policy.** Selected explicitly by the operator,
it prefers the plan that finishes sooner where the two objectives disagree,
which in practice means ranged reads for narrow projections. It is an intent, not
a tuning constant: it says "spend requests to save wall time", and the engine
decides how.

The trade is stated in the flag's own documentation, not buried: **5.45x the GET
requests** (570,752 against 104,780 over one pass) for **41% less cold time**.
Intra-region transfer is free and requests are billed, so this is money for time
at a rate the operator, not Ravel, should choose.

**`latency-first` carries a memory precondition, and says so.** At the
concurrency it needs to pay off, the ranged path is not currently survivable: one
long-lived server running the same statements was OOM-killed where the
whole-object server completed (#1185, #1170). Until in-flight fetch memory is
bounded, the policy ships with its supported concurrency documented as bounded,
and the flag's docs point at #1170 and #1007. Shipping it silently at a setting
that kills the process would be worse than not shipping it.

**Both ClickBench entries get published**, labelled: a stock entry on default
settings, and a tuned entry naming `latency-first` and the concurrency it uses.
Upstream accepts labelled tuned entries, so the default stays honest about the
bill while the benchmark shows what the engine can do.

## Rejected alternatives

**Make `byte-minimal` the default.** It is the fast configuration and it needs no
new policy name. Rejected twice over: at the default concurrency it measured
712.4 s against 525.0 s, 36% *worse*, so as a default it is a regression for
anyone who does not also raise concurrency; and at the concurrency where it wins
it OOMs a long-lived server. A default must be safe at its own defaults.

**Give `cost-based` a time term, so one policy balances both.** The intellectually
tidier answer, and the direction a future ADR may take. Rejected for now because
the model has no live time input: bandwidth and latency are properties of the
instance, not of the store, and `StoreCostProfile` is explicitly "this
deployment's object-store prices" (`crates/ravel-types/src/cost_profile.rs:110-140`).
Putting NIC bandwidth into a profile named `s3-intra-region-2026`, shared across
every instance type, is a category error. A named policy expresses the operator's
intent without pretending to have measured their hardware.

**Set a non-zero `transfer_nanodollars_per_gib` so `cost-based` stops
saturating.** One line, no new fields. Rejected because it lies about the bill to
obtain a behaviour, and the number required has no defensible value in dollars.

**Tune `--logs-request-cost-bytes` and add no policy.** The flag exists, wins over
policy (`crates/ravel-query/src/config.rs:263-267`), and reaches the same routing
today. It stays the right escape hatch for an operator experimenting. Rejected as
the answer because it asks operators to express an intent as a magic byte count,
and it does not make the resulting configuration safe.

## Consequences

- An operator can ask for latency and get it, at a stated price in requests.
- The default S3 bill does not change for anyone who does not opt in.
- Two ClickBench entries to maintain, and the tuned one cannot be published until
  the memory work makes its configuration survivable. That ordering is the point:
  the tuned entry is a claim about what the engine does, and it must not be a
  claim about a configuration that dies at statement 31.
- Cost-regression bands move on the tuned lane only. `[data_gets]`
  (`crates/ravel-bench/cost_regression_bands.toml:38`, `kind = "exact"`) and the
  two-sided `[modeled_request_cost]` (`:56-58`) both fail on a 5.45x request
  change and must be re-banded per lane. `[bytes]` is one-sided and a reduction
  passes it (`:6-10`), so it does not.
- `[peak_memory]` is `expected = false` in that file, so the memory property this
  policy depends on has no gate today. It should become an emitted and enforced
  figure in the tuned lane before that lane is published.
- The tests pinning cost-based's present resolution stay green and unmodified:
  `policy_to_rate_mapping_table_is_pinned` and
  `cost_based_high_saturation_boundary_is_pinned`
  (`crates/ravel-query/src/config.rs:483,708`). This ADR adds a policy; it does
  not alter the existing ones.
