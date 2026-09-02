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
    traceability.md        requirement -> invariant -> source ref table
    results.md             pre-registered state-count bands
    negative/              negative-control configs (must fail)
      <name>.cfg           a config with exactly one broken-behavior switch
      <name>.expect        the exit code and invariant that config must violate
      counterexamples/     prose note per negative: what breaks and why
```

An **area** is any directory under `formal/tla` that contains a `smoke.cfg`.
Its model-check entry module is the single `MC*.tla` file in the directory
(naming convention the harness relies on). New areas planned by ADR-1113:

| Area | Contract modeled | Status |
|---|---|---|
| common | object-store put/get/delete/list/multipart | this task (T1) |
| commit | commit publication, acknowledgement, retry, read-your-write | planned (T2) |
| catalog | catalog fold, snapshots, compaction, MVCC | planned (T3) |
| lifecycle | retention, erasure, legal holds, physical GC | planned (T4) |
| resharding | generation-versioned online resharding | planned (T5) |
| maintenance | maintenance ownership (shipped) and advisory claims (proposed) | planned (T6) |

## Running

Requires Java 17 or newer (Temurin 21 is what CI uses) and network access on
first run to fetch the TLC jar. The harness resolves Java from
`RAVEL_TLA_JAVA` if set, else `java` on `PATH`, and exits 2 if none is usable.
The per-model wall-clock ceiling needs coreutils `timeout` (`gtimeout` from
Homebrew coreutils on macOS); without either the run proceeds unbounded and
the harness says so once. CI and the fleet executors always have `timeout`.

```sh
scripts/check-tla.sh smoke            # fast safety, every area (budget 300s/cfg)
scripts/check-tla.sh negative         # every negative control must fail correctly
scripts/check-tla.sh traceability     # every traceability.md source ref resolves
scripts/check-tla.sh all              # smoke + negative + traceability (the CI lane), then exhaustive
scripts/check-tla.sh exhaustive       # full safety + liveness (nightly, budget 3600s/cfg)
scripts/check-tla.sh smoke -a common  # scope any subcommand to one area
```

Exit codes: `0` pass, `1` a check failed, `2` no usable Java.

### The TLC jar

Fetched once into `.cache/tla/tla2tools-<version>.jar` (gitignored) and pinned
by sha256. A cached jar whose checksum does not match the pin is refused
rather than used, and a fresh download that fails the checksum is deleted:

```
check-tla: cached .../tla2tools-1.7.4.jar has sha256 <got>, expected <pin>; refusing to run (delete it to re-fetch)
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
`result` is `PASS`, `FAIL`, `TIMEOUT`, or `VIOLATED` (a negative control that
failed as intended). Expected state-count bands are pre-registered in each
area's `results.md`; a run outside them is a regression to investigate, not
a band to widen.

## Negative controls

Each area proves its invariants are load-bearing by shipping configs that
must fail. A negative config is the correct model plus exactly one CONSTANT
switch that breaks one behavior (never an edited copy of the module). Its
`.expect` file pins the outcome, two lines:

```
exit=12
property=ReadAfterWrite
```

`exit=12` is a safety (invariant) violation; `exit=13` a temporal (liveness)
one. `property=` names the invariant or property that must be the one
reported violated, so a config that fails for the wrong reason still fails
the check. If the `.expect` names an invariant the config does not actually
violate, the harness reports the mismatch, for example:

```
check-tla: common negative cas-accepts-stale-version: expected exit=12 violating ReadAfterWrite, got exit=12
```

## Traceability

`traceability.md` is a three-column table: requirement, the invariant or
property that pins it, and a source ref. The ref is a repo-relative path,
optionally `path:Symbol`. `check-tla.sh traceability` fails if any path is
missing or any named symbol is absent from its file, so a renamed operator or
moved doc breaks the check instead of drifting silently.

## What the common model establishes

`RavelObjectStore.tla` models the object-store contract
(`docs/object-store-contract.md`) as a single-key-space store with per-key
presence, content, and a monotonic version. `MCRavelObjectStore.tla` drives
it with two clients over a small key set and states the invariants:

- **CreateIfAbsentWinnerUnique**: at most one CreateIfAbsent wins per presence
  interval (a second create on a present key gets AlreadyExists).
- **CasNeedsFreshVersion**: a CasVersion put applies only against the current
  version; a stale or absent version is PreconditionFailed and a no-op.
- **ReadAfterWrite**: a durable write (including one whose response was lost)
  is visible to a later read.
- **DeleteIdempotent**: after a delete the key is absent, and a repeat delete
  is a no-op.
- **MultipartInvisibleUntilComplete**: no part of an in-progress multipart
  upload is visible before Complete.
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
| `listState.snapshot / .returned` | a paginated `list` that may repeat keys and need not show keys created mid-scan |

`lastModified` and `uploads` are ephemeral modeling state, not part of the
durable object model (ADR-1113 D2 names store and version); they exist so the
advisory-timestamp and multipart-visibility properties can be stated.
