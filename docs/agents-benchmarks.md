# Ravel Agents cost benchmarks

The agents cost benchmarks measure what Ravel Agents costs in object-storage
requests, bytes, latency and memory, per operation, and check every figure
against a band written down before the first run. They are the measurement
half of [ADR-3331](adrs/3331-ravel-agents-durable-agent-runtime.md) decision
15. The storage protocol they measure is in
[agents.md](agents.md).

## What runs

The harness lives in `crates/ravel-agents-sim/src/bench/` and the runner in
`crates/ravel-agents-sim/benches/agents_cost.rs`. Every scenario runs on an
in-memory store (`MemoryStore` wrapped in `FaultStore`, plus a latency layer
on a paused tokio clock), with fixed operation counts and seed 3361. Nothing
depends on the host's speed: latency is the injected latency, measured on
the injected clock.

Each scenario runs twice where it can. `warm` reuses one coordinator and one
registered worker across the whole run; `cold` builds a fresh coordinator and
worker for every round, so every turn pays the activation and takeover a
restarted process pays.

| scenario | what it drives | temps |
|---|---|---|
| hot | one agent, rounds of one lead send then three concurrent sends, a turn and a step per send | warm, cold |
| slow | as hot, on a store with ten times the latency plus a seeded jitter of up to another ten times | warm, cold |
| many | many agents, two messages, turns and steps each | warm, cold |
| dormant | a large population of agents with one finished turn each, beside a few active agents; asserts the dormant agents see no request | warm, cold |
| index-p64, index-p2 | one agent, a long run of messages and turns, with index pages of 64 and of 2 entries | warm, cold |
| storm | agents mid-turn under a dead worker, taken over by sweep passes in the shape of the T22 storm schedule | cold |
| discovery-nN-POS | N agents in the directory, the target at the start, middle or end; measures how many 5 s sweep passes find it | cold |
| baseline | the simulated telemetry ingest loop, as a reference point; issues no agents request | na |

Sizes: `small` (the default, and what the tests run) and `full`. The exact
counts for each are in the "Scenario sizes" table of
[bench/agents/prereg.md](../bench/agents/prereg.md). Discovery runs N = 100
on small and N = 100, 1,000 and 10,000 on full.

## How to run

```sh
cargo bench -p ravel-agents-sim --bench agents_cost            # small
cargo bench -p ravel-agents-sim --bench agents_cost -- full
AGENTS_BENCH_SIZE=full cargo bench -p ravel-agents-sim --bench agents_cost
```

The run exits 0 only when every figure is inside its band, each figure is
printed once and no more, and every exact band row matched a figure. An unknown
size exits 64. `AGENTS_BENCH_REVISION` overrides the revision in the stamp;
otherwise it is `git rev-parse HEAD`. `cargo test -p ravel-agents-sim bench`
runs the same check on the small size, one test per scenario plus the whole
run.

## Report format

The first line is the stamp: what the run measured and on what.

```text
agents-bench-stamp revision=<sha> store=memory+fault seed=3361 clock=tokio-paused size=small heap=stats_alloc
```

Then one line per figure, then a summary:

```text
agents-bench <scenario>.<temp>.<op>.<phase>.<metric> value=<n> unit=<unit> [kind=<kind>]
agents-bench-summary figures=<n> failures=<n>
```

- `op` is what a figure is normalised by: `message` (a send), `turn` (poll
  plus commit), `step`, `activation` (loading an agent onto a coordinator),
  `pass` (a sweep pass), `assign` (discovery of the target), `ingest`, or
  `run` for whole-run figures.
- `phase` is the layer that issued the request: `resolve` (HEAD and
  directory reads), `store` (object bodies), `publish` (HEAD CAS), `other`
  (deletes), `index` (`page_gets` and `planner_misses`), plus `ops`,
  `latency`, `heap`, `sweep`, `isolation` and `telemetry` for figures that
  are not request counts.
- Request metrics are `get`, `put`, `cas`, `list`, `delete` and `conflict`
  (a CAS or create that lost), per operation. Phases are never pooled: a
  report whose phases were summed into one counter fails the check.
- Latency is `p50_ms`, `p99_ms` and `max_ms` per operation, nearest rank, on
  the injected clock.

A failure prints `agents-bench FAIL <key>: <reason>` on stderr.

### Byte kinds

Every byte figure names which bytes it counts, and a byte figure without a
`kind=` fails the check.

| kind | what it counts |
|---|---|
| `object-body` | object bodies at the store client boundary, in (read) or out (written), retries included, never compressed; not wire bytes |
| `heap-live-delta` | the largest sampled rise of live heap bytes over the run, from a `stats_alloc` global allocator; a sampled peak, not a high-water mark |

### Prices

`bench/agents/prices.toml` holds per-request and per-GB prices, labelled
`assumption` with a date and a source. The runner prints dollars per million
operations as `agents-bench-usd` lines derived from the measured counts. No
band is ever on a dollar figure, so a price change cannot turn a run red.

## Bands

The band table is in [bench/agents/prereg.md](../bench/agents/prereg.md),
committed before the first measurement. Its dated revision section restates
the rows the first run showed were predicted on the wrong split; the
original rows stay as written. The headline bands:

| figure | band | basis |
|---|---|---|
| message, warm, uncontended: resolve GET, store PUT, publish CAS | 1.5, 1, 1 per message | bench/agents/prereg.md:68 |
| turn, warm: resolve GET | 2.5 per turn (many) | bench/agents/prereg.md:332 |
| turn, cold: resolve GET, publish CAS | 3, 3 per turn (takeover on every turn) | bench/agents/prereg.md:332 |
| LIST on message, turn or step | 0 | bench/agents/prereg.md:68 |
| dormant agents' requests | 0 | bench/agents/prereg.md:68 |
| baseline agents requests | 0 | bench/agents/prereg.md:68 |
| discovery sweep passes | at most ceil(N/64) | bench/agents/prereg.md:68 |
| discovery delay | at most ceil(N/64) x 5000 ms | bench/agents/prereg.md:123 |

Discovery measures how long an unassigned agent waits to be found: a sweep
pass reads at most 64 directory entries, so a target at the end of a
directory of N agents is found on pass ceil(N/64), which at the 5 s interval
is 157 passes and about 13 minutes for N = 10,000.

## Reports

[bench/agents/REPORT.md](../bench/agents/REPORT.md) holds the last recorded
run, with its stamp.
