# reshard-ownership Antithesis profile

A profile of the catalog-maintenance workload (ADR-4326 decision 4,
`../catalog-maintenance/README.md`): the same topology, images, scripts and
driver crate, with the tenant resharded from 4 to 6 shards and back under the
running workload. It owns the `resharding` and `maintenance` areas, whose
mutants run here (ADR-4326, per-workload amendment), and the `catalog` area
of the workload it runs whole ("The expected set"). It implements D5 item 6
of the #4340 Phase E step 5 decision, with the pre-registered rows of its D6.

## What differs from catalog-maintenance

- `config/docker-compose.yaml` is the catalog-maintenance compose file with
  the driver container running `/usr/local/bin/driver-reshard`. Every other
  service and setting is the same, so every catalog race, clock jump, audit,
  sweep and token read of that workload runs here too, across the reshards.
- `driver-reshard` is the catalog-maintenance driver built with its `reshard`
  cargo feature (`../catalog-maintenance/driver/src/reshard.rs`), shipped in
  the same driver image. It is a binary of its own because the Antithesis SDK
  registers every assertion a binary holds when it starts: the plain driver
  carries none of this profile's literals, so catalog-maintenance's expected
  set does not change.
- The scripts are catalog-maintenance's, run with
  `WORKLOAD_PROFILE=reshard-ownership` (`scripts/profile.sh`).

## The reshard sequence

`provision reshard` appends a generation; only `ravel-cli` does that, so the
driver runs it through `ravel-cli-ant`, the instrumented CLI whose SDK output
goes to its own `/tmp/sdk-cli.jsonl` (`AntCli`, `driver/src/cli.rs`). Every
other CLI call stays on the stock `ravel-cli`, and every call omits
`--shards` except the seed, so each takes the count from the provisioning
record (D3).

1. **Increase, before the sealing jump.** The first time the sealing jump
   runs, a drawn rule or the eventually, the driver first runs `ravel-cli-ant
   provision reshard --tenant cm-tenant --signal logs --shard-count 6
   --lead-hours 2`. The generation activates at the CLI's hour plus 2, and the
   9000 s jump is longer than that lead, so it always activates it. From then
   on live writes route at 6 shards.
2. **Activation writes** (the eventually, first of its forced situations):
   writes of 32 records over 32 streams until both added shards, 4 and 5,
   have acknowledged one, at most 24. A stream's shard follows from its
   resource name, which is the same in every write, so a narrower write may
   never reach one of them. A folded post-activation hour then holds data at
   both.
3. **The fence** (D2). A write acknowledged by each `--mode all` node, so each
   router holds a cached view of the record. Then the proxy's `refuse-read
   t/<h>/l/prov` answers every GET and HEAD of the record 503. After 75 s,
   longer than the routers' 60 s refresh interval, a write to each node
   finds its view stale and its re-read refused, and routes on the
   grace-extended view. Every clock then moves to 60 s past the start of hour
   H + 2, where H is the hour when the stale-view wait ends, past every view's
   lead horizon
   (`hour(refreshed) + min_lead_hours`), without waiting for the nodes to
   serve, since no SQL answers while the record cannot be read. A write to
   each node is then fenced (`503 provisioning view stale`). `thaw-read` and
   `pass` lift the refusal, and every node must serve again within 300 s.
4. **Decrease with a straggler burst** (D4 part 2). The driver appends 6 to 4
   with the same lead, moves every clock to 30 s before its activation, and
   sends one write every 50 ms through the last 2 s before it (40 writes,
   `burst_delays`). A write routed at 6 that flushes in the activation hour
   is a straggler: a token at shard 4 or 5 in an hour that routes at 4. Once
   the activation hour began, each node acknowledges one write routed at 4
   (the anchor a later restart's new identity is compared with, since
   restart evidence needs two acknowledgements at one count), and each
   straggler (up to 4, single-attempt writes only) is read back with its
   tokens on both nodes, which resolves it above the active count. A cycle with no straggler is repeated once, after an
   increase back to 6.
5. **Forged token read** (D4 part 1). An acknowledged write's first token
   moved to a shard at its hour's active count, with sequence number
   `u64::MAX`, so its commit record was never written (`forge`). Each node
   must refuse it as unsatisfiable, which makes the SUT evaluate `resharding:
   a commit token whose record is durable resolves across a reshard` with the
   record absent.

Then the eventually runs catalog-maintenance's own sequence (fold and record
races, the unfolded and unreadable-HEAD sweeps, fold, compaction, audits,
sweep jumps, the restart and token reads, the multiset check), all after both
reshards. Every clock move prints `clock: moved forward by <N> s outside the
planned jumps (reshard: ...)`, which `local-run.sh` adds to the offset it
expects.

The driver keeps the record's generation history (`audit::ShardHistory`). It
bounds sweeps and bucket scans by the shard ceiling, which stays 6 after the
decrease (shards 4 and 5 keep their data), and decides each
acknowledgement's routed count from it, which the restart evidence of the
token reads needs: a reshard rebuilds a router's shard set under a new writer
identity with no restart (`../catalog-maintenance/README.md`, "Token reads
across the lifecycle").

## Properties

The driver's own literals start with `reshard: ` (besides the
catalog-maintenance driver's `catalog: ` and `clock: `).

| Literal | Type | Where | Holds when |
|---|---|---|---|
| `reshard: a generation activated` | reachable | every acknowledged write | A token named a shard the increase added. |
| `reshard: an acknowledged write sits below the shard count it was routed at` | always | every acknowledged write once the record holds a second generation | Every token's shard is below the count the attempt's send hour, answer hour and token hours all route at; attempts whose hours route at two counts (stragglers among them) are not judged. |
| `reshard: a write routed after the decrease activated was acknowledged` | reachable | every acknowledged write | The always above held for a write sent at or after the decrease's activation and routed at 4. |
| `reshard: the clocks passed the writers' lead horizon while the provisioning record's reads were refused` | sometimes | the fence | The refusal was set, the move landed and the clock is past the horizon. The details carry each node's grace and fenced write. |
| `catalog: the system recovers once faults stop` | always | the fence, after `thaw-read` (details `after: thaw-read`), and the eventually | Every node serves within 300 s. |
| `reshard: a write routed before a decrease landed at a retired shard in its activation hour` | sometimes | each decrease cycle | A burst write's token is at shard 4 or 5 in the decrease's activation hour. |
| `reshard: a forged token for a record never written was refused as unsatisfiable` | sometimes | the forged read, per node | The read answered 503 with the unsatisfiable-token message. |
| `reshard: HEAD's footprint named data of a folded post-activation hour at every shard the increase added` | sometimes | every audit with a stable HEAD | For acknowledged tokens at shards 4 and 5 in hours that route at 6 and that HEAD has folded, `rlog footprint` resolved a data object of each shard: the token's level-0 object or an output part of its bucket (`audit::new_shard_keys`). |

The SUT's `resharding: ` assertions this profile reaches are in
`crates/ravel-catalog/src/provisioning.rs` (the generation append, through
`ravel-cli-ant`), `crates/ravel-ingest/src/generation.rs` (grace routing and
the fence) and `crates/ravel-catalog/src/catalog.rs` (token resolution).

## Required in a plain local run

`local-run.sh` requires everything catalog-maintenance requires, and, for this
profile:

- `resharding: generation append CAS won` (in `sdk-cli.jsonl`) and `reshard: a
  generation activated`;
- on `ravel1` and on `ravel2`, each from its own SDK file: `resharding: a
  stale writer routed on a grace-extended view`, `resharding: a writer past
  the lead horizon was fenced`, and `resharding: grace routing continues only
  within the lead horizon of the last refresh; past it the writer stops`
  evaluated true;
- the fence situation, and `catalog: the system recovers once faults stop`
  held after the `thaw-read`;
- a straggler, `resharding: a token resolved at a shard index above the active
  count`, `reshard: a write routed after the decrease activated was
  acknowledged` and `reshard: an acknowledged write sits below the shard count
  it was routed at`;
- the forged token's refusal and `resharding: a commit token whose record is
  durable resolves across a reshard`;
- the new-count footprint check.

## The expected set

The owned areas are `resharding` and `maintenance`, the areas whose `.tsv`
files name the mutants that run here, and `catalog`, since the profile runs
the whole catalog-maintenance workload and every catalog mutant runs here
too; plus the driver's own literals. Every other area's unreached
always-assertion is listed, not gated (`../catalog-maintenance/README.md`,
"The expected set"). Every `catalog: ` always-assertion was hit in each of
three local runs of this profile, so owning the area adds no pending entry.

Two owned always-assertions have no forced situation here yet.
`scripts/profile.sh` names both in `PROFILE_PENDING`, and `local-run.sh`
lists them as pending, never as a gate failure, until a situation reaches
each and its entry is removed:

- `maintenance: a loser observing a divergent input-set hash fails closed and
  never overwrites the record` is evaluated only when a compaction loser sees
  a divergent input set, which the record race never produces (it races two
  compactions over the same inputs). The forced situation that reaches it is
  D5 item 7 of the step 5 decision, not built yet.
- `maintenance: a seeded memo entry is never fresher than its snapshot` is
  evaluated only when a maintain worker warm-starts from a peer's memo
  snapshot (`crates/ravel-maintain/src/scan.rs`), which happens when the two
  workers' starts and membership changes stagger against each other's
  snapshot writes: a race, not a situation the driver forces. It held in two
  of three local runs of this profile and was never hit in the third. A
  forced maintain restart in the local loop, as `ravel2`'s, would reach it.

## Mutants

Every mutant the scripts accept runs here, over the `-mutants` images
(`MUTANTS=1 scripts/build.sh`):

```sh
WORKLOAD_PROFILE=reshard-ownership RAVEL_ANTITHESIS_MUTANT=resharding/no-writer-fence \
  antithesis/catalog-maintenance/scripts/local-run.sh out/no-writer-fence
WORKLOAD_PROFILE=reshard-ownership RAVEL_ANTITHESIS_MUTANT=resharding/token-validated-against-count \
  antithesis/catalog-maintenance/scripts/local-run.sh out/token-validated
```

- `resharding/no-writer-fence`: past the horizon the mutant routes on the
  stale view, so the fence's writes make `resharding: grace routing continues
  only within the lead horizon of the last refresh; past it the writer stops`
  false on both nodes.
- `resharding/token-validated-against-count`: the mutant refuses a token
  whose shard is at or above its hour's active count, so a straggler's read
  makes `resharding: a commit token whose record is durable resolves across a
  reshard` false.
- The two `maintenance/` mutants need D5 item 7's forced situations; until
  they land a run of one exits 1.

## Build, run locally, launch

```sh
antithesis/catalog-maintenance/scripts/build.sh            # driver-reshard is in the driver image
WORKLOAD_PROFILE=reshard-ownership antithesis/catalog-maintenance/scripts/local-run.sh out/reshard
WORKLOAD_PROFILE=reshard-ownership antithesis/catalog-maintenance/scripts/launch.sh 12 "reshard-ownership smoke"
```

`launch.sh` launches under the test name `ravel-reshard-ownership`
(`ravel-reshard-ownership-mutant` for a mutant run) with
`attrs.workload=reshard-ownership`.

The `weekly` file opts the profile into the weekly run (ADR-4326 decision 4).

## Not here

- ADR-4326 decision 4 lists a third maintain worker for this profile; the
  step 5 decision's build list does not, and the compose runs two.
- The reshard situations run in the eventually (and the increase at the
  sealing jump, drawn or forced), not as drawn rules of their own, so under
  Antithesis the fence, the decrease and the forged read run after faults
  stop.
- No property of its own checks `EveryAdmittedWriteInScanSet`; neither the
  step 5 decision nor its plan names one for this profile.
