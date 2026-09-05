# Ravel TLA+ verification

Machine-checked models of Ravel's concurrency and durability contracts, and
the harness that runs them. Decided by ADR-1113; this directory is task T1
(wave 1): the harness, the shared object-store module, and the CI lane.

## Layout

```
formal/tla/
  README.md                this file
  common/                  shared object-store contract (the first area)
    RavelObjectStore.tla   the contract module (operators, no config)
    MCRavelObjectStore.tla  model-check entry: drives the contract, states invariants
    smoke.cfg              fast safety config (symmetry-reduced)
    exhaustive.cfg         full safety + liveness config (nightly)
    bands.tsv              per-config distinct-state and depth bands (optional)
    traceability.md        requirement -> invariant -> Rust source table
    results.md             recorded figures and the bands they must stay in
    counterexamples/       prose note per adversarial module mutant
    negative/              negative-control configs (must fail)
      <name>.cfg           a config with exactly one broken-behavior switch
      <name>.expect        the exit code and property that config must violate
      counterexamples/     prose note per negative: what breaks and why
```

An **area** is any directory under `formal/tla` that carries a smoke config,
either a bare `smoke.cfg` or a per-module `MC<Spec>.smoke.cfg`. Every `MC*.tla`
in the directory is a model-check entry module, and an area may hold more than
one. For each module and kind (smoke, exhaustive) the harness prefers the
per-module `MC<Spec>.<kind>.cfg` and falls back to the bare `<kind>.cfg` (valid
only where the area has a single spec); it fails when a smoke module has no
config. New areas planned by ADR-1113:

| Area | Contract modeled | Status |
|---|---|---|
| common | object-store put/get/delete/list/multipart | this task (T1) |
| commit | commit publication, acknowledgement, retry, read-your-write | planned (T2) |
| catalog | catalog fold, snapshots, compaction, MVCC | planned (T3) |
| lifecycle | retention, erasure, legal holds, physical GC | planned (T4) |
| resharding | generation-versioned online resharding | planned (T5) |
| maintenance | maintenance ownership (shipped) and advisory claims (proposed) | planned (T6) |

## Running

Requires Java 17 or newer (Temurin 21 is what CI uses). The harness resolves
Java from `RAVEL_TLA_JAVA` if set, else `java` on `PATH`, and exits 2 if none
is usable. It needs network access on first run only to fetch the TLC jar,
unless you supply one with `RAVEL_TLA_TOOLS_JAR` (see below). The
traceability lane runs no TLC and needs no Java at all.

The per-model wall-clock ceiling requires GNU `timeout(1)`. Linux ships it as
`timeout`; on macOS install it with `brew install coreutils`, which provides
it as `gtimeout`. The harness resolves the binary once at startup, before
any model runs, and exits 2 with a one-line refusal if neither is GNU
coreutils' `timeout` (a look-alike that doesn't support `--kill-after` is
rejected the same as no binary at all). Every TLC invocation runs under
`timeout --kill-after=30 <budget>`: TERM at the budget, KILL 30 seconds
later if TERM was ignored. Either way the run is reported as a timeout, not
left running and not read as a pass. CI runs on Ubuntu, which ships GNU
`timeout`, so it needs no extra setup.

```sh
scripts/check-tla.sh smoke            # fast safety, every area (budget 300s/cfg)
scripts/check-tla.sh negative         # every negative control must fail correctly
scripts/check-tla.sh traceability     # every traceability.md source ref resolves
scripts/check-tla.sh ci               # smoke + negative + traceability, one run id (the CI lane)
scripts/check-tla.sh all              # ci, then exhaustive, under one run id
scripts/check-tla.sh exhaustive       # full safety + liveness (nightly, budget 3600s/cfg)
scripts/check-tla.sh smoke -a common  # scope any subcommand to one area
```

`ci` and `all` record every model under a single run id, so `last-run.tsv` is
one coherent run rather than a config's rows overwriting the previous config's.
Exit codes: `0` pass, `1` a check failed, `2` no usable Java or GNU
timeout(1) unavailable. A subcommand or
`-a` area that does not exist, and `-a` with no value, fail immediately.

### The TLC jar

Fetched once into `.cache/tla/tla2tools-<version>.jar` (gitignored) and pinned
by sha256. A cached jar whose checksum does not match the pin is refused
rather than used, and a fresh download that fails the checksum is deleted:

```
check-tla: cached .../tla2tools-1.7.4.jar has sha256 <got>, expected <pin>; refusing to run (delete it to re-fetch)
```

For an air-gapped or reproducible build, set `RAVEL_TLA_TOOLS_JAR` to an
operator-supplied jar. It is used as-is after its sha256 is verified against
the same pin, and it is never downloaded: a mismatch refuses to run rather
than silently fetching a different jar.

```
check-tla: RAVEL_TLA_TOOLS_JAR sha256 <got> != expected <pin>; refusing to run (not downloading)
```

### Logs and figures

Per-config TLC output goes to `.cache/tla/logs/<area>-<kind>.log`. Every
model-check run truncates and rewrites `.cache/tla/last-run.tsv`, one row per
config:

```
run-id  area  cfg  states  distinct  depth  seconds  result
```

`run-id` is a UTC timestamp joined to the working tree hash
(`git rev-parse HEAD^{tree}`), so a row names the exact source it measured.
`result` is `PASS`, `FAIL`, `TIMEOUT`, `BAND` (a PASS run whose figures fell
outside its band), or `VIOLATED` (a negative control that failed as intended).

Bands are optional and live in each area's `bands.tsv`, one row per config
(`cfg`, `min_distinct`, `max_distinct`, `min_depth`, `max_depth`). When a row
exists the harness enforces it on a PASS run and fails outside it; a run
outside the band is a regression to investigate, not a band to widen.
Negative controls stop at the first counterexample TLC finds, which under
`-workers auto` is not deterministic, so they carry no band. `results.md`
records the figures a run produced and the bands they must stay in.

## Negative controls

Each area proves its invariants are load-bearing by shipping configs that
must fail. A negative config is the correct model plus exactly one CONSTANT
switch that breaks one behavior (never an edited copy of the module). Its
`.expect` file pins the outcome, two lines:

```
exit=12
property=LostResponseEffectApplied
```

`exit=12` is a safety (invariant) violation; `exit=13` a temporal (liveness)
one. `property=` names the invariant or property that must be the one
reported violated, so a config that fails for the wrong reason still fails
the check. If the `.expect` names an invariant the config does not actually
violate, the harness reports the mismatch, for example:

```
check-tla: common negative cas-accepts-stale-version: expected exit=12 violating WrongName, got exit=12
```

For a safety negative the harness matches `Invariant <property> is violated`
in the TLC log. TLC 1.7.4 prints no property name on a temporal violation
(only `Temporal properties were violated.`), so for an `exit=13` negative the
harness generates a config that declares exactly the expected property (every
other `PROPERTY` line stripped, the expected one appended) and runs that: a
violation can then only be that property, and a wrong `property=` name fails
to resolve the operator instead of passing.

A negative config's entry module is named on its first line with the
`\* module: MC<Spec>` convention; where the area has a single MC module that
line may be omitted and the harness uses it.

## Traceability

`traceability.md` is a five-column table (ADR-1113 D8): the TLA+ action or
property, its meaning, the Rust path and symbol it pins, an existing test, and
any new test still needed. The third column is required and resolved: it is a
repo-relative `crates/<...>.rs::Symbol::...` reference, and `check-tla.sh
traceability` fails if the `.rs` path is missing or any `::`-separated symbol
is absent from that file. A `.tla` reference in that column is rejected, since
a row must cite the Rust implementation, not the model. Any further
`crates/...` reference in the other columns (an existing test, a symbol named
in the new-test note) is resolved the same way, so a renamed operator or moved
test breaks the check instead of drifting silently.

### Mutants

The `counterexamples/` note beside the negative switches records the CONSTANT
controls. The area-level `counterexamples/` directory records **mutants**: a
one-line edit to the module text (not a switch) that deletes a precondition,
run once to prove the invariant catches it, then reverted. Each note quotes
the edit and the exact `Invariant <name> is violated` line TLC printed, so a
reviewer can see which invariant is load-bearing for which clause without
rerunning it.

## What the common model establishes

`RavelObjectStore.tla` models the object-store contract
(`docs/object-store-contract.md`) as a single-key-space store with per-key
presence, content, and a global monotonic version. `MCRavelObjectStore.tla`
drives it with two clients over a small key set. Each mutating client action
calls the store operator and records, in a single witness `lastOp`, the
caller-visible outcome alongside the store record before and after the step;
the invariants read that witness and the store, never a switch or a ghost that
merely restates one. The invariants:

- **CreateIfAbsentWinnerUnique**: at most one CreateIfAbsent wins per presence
  interval (a second create on a present key gets AlreadyExists).
- **CreateOutcomeMatchesEffect**: a create's outcome matches its store delta
  (an Ok create made an absent key present; an AlreadyExists create changed
  nothing).
- **CasOutcomeMatchesEffect**: a CasVersion put succeeds only against the
  current version of a present key; any other outcome leaves the record
  unchanged, so a stale or absent-key version is a no-op.
- **ReadAfterWrite**: a durable write (including one whose response was lost)
  is visible to a later read.
- **LostResponseEffectApplied**: a write whose response was lost still applied
  its effect even though the caller saw Failure.
- **TransientLeavesNothing**: a transient failure applied nothing and the
  caller saw Failure.
- **DeleteIdempotent**: after a delete the key is absent, and a delete of an
  absent key changed no observable state.
- **VersionsNeverReused**: every version the global counter mints is new
  (a minted-version set never falls behind the mint count), so a delete never
  resets the counter, create/delete/create mints a fresh version, and a CAS
  holding a pre-delete token cannot succeed against the new object.
- **MultipartInvisibleUntilComplete**: no part of an in-progress multipart
  upload is visible before Complete (the target key still equals the record
  captured at begin).
- **ListingConsumersConsistent**: the deduplicating consumer equals the
  delivered support and the counting consumer equals the total number of
  deliveries, so a repeated delivery is observable and a counting consumer
  that silently deduplicates is caught.
- **ListEventuallyComplete** (liveness, exhaustive only): every started
  listing eventually returns every key present when it began.

Per ADR-1113 D12, this establishes the object-store contract's put-mode,
delete, multipart-visibility, and listing-completeness properties hold under
concurrent clients, lost responses, and transient failures, for the bounded
configuration in `results.md`. It is a bounded model check, not a proof for
all key-space and client sizes.

### Modeled state versus Ravel

| Model | Ravel |
|---|---|
| `store[k].present / .content / .version` | an object key's existence, bytes, and ETag/generation |
| `PutOverwrite / PutCreateIfAbsent / PutCasVersion` | `PutMode::Overwrite / CreateIfAbsent / CasVersion` |
| AlreadyExists / PreconditionFailed no-op | `ObjectStore` put error variants |
| `lastModified` (advisory, only claim-expiry reads) | object last-modified time, advisory per the contract |
| `uploads[client]` | an in-flight multipart upload handle |
| `listState.snapshot / .delivered` | a paginated `list` that may repeat keys (delivered is a per-key bag) and need not show keys created mid-scan |

`lastModified` and `uploads` are ephemeral modeling state, not part of the
durable object model (ADR-1113 D2 names store and version); they exist so the
advisory-timestamp and multipart-visibility properties can be stated.
