# store-contract

Checks Ravel's object-store contract (docs/object-store-contract.md, the
TLA+ model under formal/tla/common/) against RustFS under faults. The driver
uses Ravel's own `S3Store` (crates/ravel-object-store) and runs no
ravel-server (ADR-4326 decision 4). Each property is one row of
formal/tla/common/traceability.md, whose "Antithesis property" column names
it.

## Deployment

`config/docker-compose.yaml`:

- `rustfs`: the store under test. It uses the same RustFS release, digest and
  `strict` durability settings as ingest-durability.
- `init`: a one-shot from the driver image (`driver/init.sh`). It waits for
  the store to answer, then creates the bucket. A bucket that already exists
  counts as success.
- `driver`: the workload. It is the only container excluded from faults.

No service has a restart policy.

## Driver

The driver is a [hegel](https://hegel.dev) concurrent state machine
(`driver/src/main.rs`) with at most four rules running at once. Each rule
runs one action from `driver/src/model.rs`. An action works under a fresh
key prefix `sc/<run>/<action>/<id>/`, and every body it writes is unique to
that action and that write. So the action knows every write that could have
touched its keys, and a read names the write it returned.

Reads retry retryable errors, up to 40 times at 250 ms. When a setup write
fails, or a read never settles, the action is inconclusive: it prints why and
asserts nothing.

The driver uses two stores over one bucket:

- The normal store has `STORE_LIST_PAGE_SIZE` keys per listing page (default
  2).
- The hurried store has a request timeout of `STORE_HURRIED_TIMEOUT_MS`
  (default 5 ms), so some of its writes fail mid-flight. Only the transient
  action writes through it.

## Properties

Every message starts with `store: `. Every property is an `assert_always`
except the transient one. Antithesis also fails an `assert_always` that is
never reached, so each assertion also proves that its action ran.

| TLA+ property | Message | What the action does and asserts |
|---|---|---|
| CreateIfAbsentWinnerUnique | `store: a create-if-absent key has exactly one winner` | 2 to 4 concurrent creates of one fresh key. At most one is answered `Ok`. A present key holds one contender's bytes. If one create was answered `Ok`, the key holds its bytes under the version that create returned. A contender answered `PreconditionFailed` never holds the key. An absent key means no create was answered `Ok` or `AlreadyExists`. |
| CasOutcomeMatchesEffect | `store: a CAS outcome matches its effect` | Write A, then CAS B with A's version. `Ok` means the key holds B under the returned version. `PreconditionFailed` means it still holds A under A's version. Any other failure means it holds A or B. Once B is stored, a CAS of C with A's stale version is not `Ok` and does not store C. A CAS on an absent key is not `Ok` and leaves the key absent. |
| ReadAfterWrite | `store: a successful write is immediately readable with its version` | 1 to 3 overwrites. After each `Ok`, a read returns its bytes and the version the put returned. |
| DeleteIdempotent | `store: a delete is idempotent and touches only its key` | Delete a written key twice, and delete a key that was never written. Each delete is answered `Ok` or a retryable error. A key whose delete succeeded is absent, and so is the never-written key. A sibling key keeps its bytes. When the listing completes, the prefix lists only the sibling. |
| VersionsNeverReused | `store: a version is never reused after a delete` | Create, delete, then create again with different bytes. The second version differs from the first. A CAS holding the first version is not `Ok`, and the key keeps the second write's bytes. |
| MultipartInvisibleUntilComplete | `store: a multipart upload is invisible until complete` | Upload one small part, or one 5 MiB part and a small last part. After each staged part, a read finds the key absent, and when the listing completes the key is not listed. After `complete` answers `Ok`, the key holds every part in order. After a failed `complete`, the key holds nothing or the whole object. After `abort`, it holds nothing. |
| ListEventuallyComplete | `store: a paged list ends complete within its page bound` | Write 3 to 9 keys, then list them page by page. The listing ends within one page per page-size keys, plus one. It returns every key whose write was answered `Ok`, and no key outside the prefix's writes. A page answered with a retryable error is retried up to 7 more times. |
| LostResponseEffectApplied | `store: a lost-response overwrite took effect` | An overwrite whose response the caller drops. The caller learns only that it did not fail before an acknowledgement. The key holds its bytes afterwards. |
| TransientLeavesNothing | `store: a failed write leaves nothing or the whole write` | A create through the hurried store. When it fails with a retryable error, the key is absent or holds exactly the whole body, never a partial or foreign object. |

The transient property is reached only when a fault, or the hurried store's
timeout, fails a write. So it is an `assert_always_or_unreachable`, paired
with `assert_reachable` "store: a write failed with a retryable error"
(ADR-4326 decision 1). The driver also asserts that these are reachable:

- "store: the workload starts"
- "store: a create race had a loser"
- "store: a stale CAS was refused"
- "store: a failed write was later found applied": a lost response observed
  for real, a failed write whose effect landed.

On the real store, the lost-response action exercises only "an acknowledged
overwrite is applied". The case where the caller saw a failure but the write
still landed is what "store: a failed write was later found applied"
records.

## Mutants

The common area's negative configs (formal/tla/common/negative/) cannot be
built into RustFS. Each one is instead a switch in `BrokenStore`
(`driver/src/mutant.rs`), the wrapper every action's store goes through.
`RAVEL_ANTITHESIS_MUTANT=common/<stem>` turns one switch on. An empty or
unset value turns none on. Any other value makes the driver exit 2 before
setup completes. formal/tla/common/antithesis-mutants.tsv names the property
each mutant must make false.

| Mutant | Switch | Property it makes false |
|---|---|---|
| `common/lost-response-not-applied` | `put_dropping_response` reports success without writing | `store: a lost-response overwrite took effect` |
| `common/cas-accepts-stale-version` | a CAS put is sent as an overwrite | `store: a CAS outcome matches its effect` (and, as a side effect, `store: a version is never reused after a delete`) |
| `common/list-never-progresses` | every listing page is empty, and its next token is the token it was given (a fixed one for the first page) | `store: a paged list ends complete within its page bound` |

`cargo test` pins each switch against `MemoryStore`. The pinning tests are:

- `model::tests::broken_cas_store_is_caught`
- `model::tests::dropped_lost_response_is_caught`
- `model::tests::stalled_listing_is_caught`
- `model::tests::each_mutant_breaks_only_its_own_property`: each mutant
  leaves the other two mutants' properties holding.

`model::tests::every_property_holds_against_memory_store` shows every
property holding with no mutant.

## Versions on S3

On S3, and on RustFS, the version `S3Store` returns is the object's ETag,
which is derived from its content. Create, delete and create again with the
same bytes would therefore get the same version back. The driver writes
unique bodies, so the property above checks the contract as Ravel uses it,
where no two writes carry the same bytes. It does not check the TLA+ model's
global version counter.

## Build, run locally, launch

On the Linux build host (x86_64, Docker):

```sh
antithesis/store-contract/scripts/build.sh
# ok ravel-antithesis-driver:store-contract: /symbols/driver links the shim and carries sancov sections

antithesis/store-contract/scripts/local-run.sh out/plain 300
# setup complete after <n>s
# assertions: <n>, would fail: <n>
# OK: no always-type assertion violated

for m in lost-response-not-applied cas-accepts-stale-version list-never-progresses; do
  RAVEL_ANTITHESIS_MUTANT=common/$m antithesis/store-contract/scripts/local-run.sh out/$m 120
done
# mutant OK: common/<stem> violated "<its property>"   (once per mutant)
```

The "would fail" list of a fault-free local run may name only the
fault-dependent assertions: "store: a failed write was later found applied",
or the transient pair if no hurried write failed. The run fails on any
violated always-type assertion, or when no SDK output was collected. A
mutant run passes only when the property that mutant names was violated.
`WORKLOAD_MAX_ACTIONS=N` makes the driver end its timeline after N actions,
and the run then waits for the driver to exit.

To move the image and launch from the machine that holds the Antithesis and
registry credentials:

```sh
antithesis/store-contract/scripts/export-images.sh s3://<bucket>/<prefix>   # build host
antithesis/store-contract/scripts/import-images.sh s3://<bucket>/<prefix>   # launching machine
antithesis/store-contract/scripts/launch.sh <max-compute-hours> [description]
RAVEL_ANTITHESIS_MUTANT=common/cas-accepts-stale-version \
  antithesis/store-contract/scripts/launch.sh <max-compute-hours>
```

`launch.sh` refuses to launch in these cases:

- the driver image's `org.ravel.antithesis.ref` label is missing, or differs
  from the checkout's HEAD;
- a mutant name is not listed in antithesis-mutants.tsv.

For a mutant run, it launches a copy of the config with that mutant
selected, under the test name `ravel-store-contract-mutant`. Fault flags:
`--fault-tolerance high_availability --fault-exclusions '^driver$'
--setup-timeout 10m`.

The `weekly` file in this directory is the marker that opts the workload
into the weekly run (ADR-4326 decision 4). This tree has no
`.github/workflows/antithesis-weekly.yml` yet (#4308), so nothing reads the
marker yet.

## New external dependencies

The driver is its own Cargo workspace with its own Cargo.lock. It uses
`antithesis_sdk` 0.4, `antithesis-instrumentation` 0.1 and `hegeltest` 0.52.6,
the versions ingest-durability uses. Its other direct dependencies are
already in the repository's workspace. The root Cargo.lock does not hold
these crates: `hegeltest`, `hegeltest-c` and `hegeltest-macros`, and the
`dashu-base`, `dashu-int` and `num-modular` crates hegel pulls in.
ingest-durability's own lock already holds all six.
