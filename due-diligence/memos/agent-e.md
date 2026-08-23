# Agent E: Rust implementation

Frozen commit 527a16db2e4d47b2924e4de4a4db32d7583fda33. Scope: 28 crates under
`crates/`, 4 services under `services/`. Method: read/grep/find only; no cargo
build/clippy/test run (per charter, treated as environment facts that the
chair's workspace clippy `-D warnings` and full nextest suites are green).

## Verdict

The memory-safety and panic-hygiene posture is strong and largely as
documented. `unsafe_code = "forbid"` holds workspace-wide with zero overrides.
The clippy `unwrap_used`/`expect_used = "warn"` gate holds: every one of the
293 `#[allow(...)]` overrides in `src/` trees sits on a `#[cfg(test)]` module
or test item, so no test-only allow leaks into a production path. Hostile-input
parsers (segment/logseg/rspan footers and pages, remote-write snappy, OTAP
IPC, OTLP protobuf) validate bounds and cap decompression before allocation,
with proptest/fuzz-mutation corpora behind each. Channels are all bounded; no
`unbounded_channel` in production. Error taxonomy is principled
(`StoreError::is_retryable`, `WriteError::is_retryable`).

The one material gap is the PromQL surface: no query-length cap and no
recursion-depth guard on a recursive-descent AST that is parsed, type-checked,
evaluated, and dropped recursively. A hostile deeply-nested query can overflow
the query worker's stack and abort the shared query process (SIGABRT is not
catchable). That is the top finding (P1). Everything else is P2/P3.

## Evidence

### unsafe: forbid holds (VERIFIED)

`Cargo.toml:18` sets `unsafe_code = "forbid"` at `[workspace.lints.rust]`, with
a comment explaining forbid-over-deny precisely to block a scoped
`#[allow(unsafe_code)]` smuggle. Grep for `allow(unsafe_code` across
`crates/`+`services/` returns zero matches. The single `unsafe ` grep hit is a
string literal in a test (`crates/ravel-sim/src/fault_plan.rs:442`,
`"seed {seed}: unsafe fault key ..."`), not an unsafe block. Every crate and
service Cargo.toml opts into `[lints] workspace = true` (checked all 32; none
missing).

### unwrap/expect: no leak into production (VERIFIED)

293 `#[allow(clippy::unwrap_used|expect_used)]` occurrences in `src/` trees
(excluding the `ravel-bench` benchmarking crate). A brace-aware scan that skips
any item guarded by `#[cfg(test)]`/`#[cfg(all(test...))]` found ZERO
`unwrap()`/`expect(` in production items. The 8 files whose allow attribute is
not within 6 lines of a test marker were each confirmed to be `#[cfg(test)]`
modules or a test file mounted under `#[cfg(test)] mod`:
`crates/ravel-query/src/distrib/tests.rs` (mounted at
`crates/ravel-query/src/distrib/mod.rs:27-28` under `#[cfg(test)]`),
`crates/ravel-query/src/cache_correctness.rs` (`lib.rs:4-5` `#[cfg(test)]`),
`crates/ravel-tracing-export/src/tests.rs` (`lib.rs:249-250`),
`services/ravel-server/src/erasure_e2e.rs` (`lib.rs:14-15`),
`services/ravel-server/src/tests.rs` (`lib.rs:58-59`,
`#[cfg(all(test, feature = "sql"))]`),
`services/ravel-server/src/query.rs:439` (`#[cfg(all(test, feature = "sql"))]`
test module), `services/ravel-ingest-router/src/tests.rs` (`lib.rs:56-57`).
The bench/sim/failure-test crates (`ravel-bench`, `ravel-sim`,
`ravel-failure-tests`, `ravel-promql-difftest`) are not depended on by any
production crate (grep of Cargo.tomls confirms only comments reference them),
so their allows are out of the production graph.

### Decompression capped before allocation (VERIFIED across all codecs)

- Remote-write snappy: `crates/ravel-remote-write/src/snappy.rs:27-32` reads
  `decompress_len` (varint header only) and rejects `len > cap` before
  `vec![0u8; len]`. Tests `cap_enforced_before_allocation`,
  `truncated_body_is_typed_error_not_panic`, and two proptests
  (`arbitrary_bytes_never_panic`, `mutated_valid_payload_never_panics`).
- Segment sections: `crates/ravel-segment/src/reader.rs:402-436` checks
  `uncompressed_len > max_section_uncompressed_bytes` first, then for LZ4 also
  validates the 4-byte size prefix against the cap before
  `decompress_size_prepended`, and zstd decompresses into a `capacity`-bounded
  buffer; post-check `decompressed.len() != uncompressed_len` rejects a lying
  frame (`reader.rs:438-443`). `ReaderLimits` defaults: section 1 GiB, page
  64 MiB (`format.rs:242-253`).
- Logseg page and rspan block: `crates/ravel-logseg/src/page.rs:66-99` and
  `crates/ravel-rspan/src/block.rs:863-883` reject `uncomp_len > max_uncomp`
  before decompress and re-check the decoded length equals `uncomp_len`.
- OTAP IPC: `crates/ravel-otap/src/stream.rs:224-235` bounds the zstd decoder
  with `Read::take(cap + 1)` so a spoofed/absent content-size header cannot
  force growth, plus per-batch row-count, dictionary-byte, and schema-count
  budgets (`stream.rs:386-418`).

### Body/message limits before work (VERIFIED)

OTLP HTTP `DefaultBodyLimit::max(16 MiB)` (`otlp_http.rs:295`), gzip decompress
capped at 64 MiB with a compressed-size pre-check that consumes no tokens
(`otlp_http.rs:201-256`). Remote-write compressed 16 MiB / decompressed 64 MiB
(`remote_write.rs:31,41`). gRPC `max_decoding_message_size(16 MiB)`
(`lib.rs:1524-1569`). A process-wide in-flight ingest-concurrency semaphore is
checked first in every handler, ahead of tenant resolution
(`otlp_http.rs:462-465`, `ingest_concurrency.rs`), and a shared byte budget
sheds before buffering (`WriteError::BufferBudgetExceeded`).

### Reachable panics are invariant guards, not hostile-input paths

29 `panic!`/`unreachable!` in production (0 `todo!`/`unimplemented!`
workspace-wide). Classified:
- PromQL dispatch `unreachable!` (`binop.rs:64,94,109,205,217`,
  `aggregate.rs`, `functions/mod.rs:806`, `eval.rs:1270`): each guards a match
  arm the promql-parser grammar cannot produce (e.g. a non-arithmetic token in
  `apply_arith`, `ManyToMany` cardinality outside set operators). Reachable
  only if promql-parser's own type check is wrong; not driven by query text
  that parses.
- Ingest actor `panic!` (`shard.rs:764` split-brain on pinned flush identity,
  `shard.rs:1243` semaphore-closed; same shape in `span_shard.rs`,
  `log_shard.rs`): fail-loud on a broken durability invariant, by design
  (crash rather than corrupt). `shard.rs:764` runs inside a spawned flush task
  (`JoinSet`), so its panic drops the ack oneshot and surfaces as
  `WriteError::ShardUnavailable` to that flush's waiters
  (`router.rs:410-420`); the actor loop survives. `shard.rs:1243` is on the
  actor task and would kill one shard actor; there is no supervisor that
  respawns shard actors (spawned once in `router.rs:104-129`), so a killed
  actor degrades 1/N of a tenant-set's ingest until process restart, counted
  via `record_shard_death` (`router.rs:429-433`). Its trigger
  (semaphore closed) is documented unreachable.
- `services` `unreachable!` (`maintain.rs:140`, `scrub.rs:124`,
  `ingest_concurrency.rs:96`, operator/cli/router config paths): all guard
  internal enum-domain invariants, not request data.

### Error taxonomy (VERIFIED principled)

`StoreError` (`ravel-object-store/src/lib.rs:383-413`) separates NotFound,
AlreadyExists, PreconditionFailed, AccessDenied, Throttled, Timeout, Corrupted,
InvalidRange, Transient, Permanent, with `is_retryable()` returning true only
for Throttled/Timeout/Transient. `WriteError`
(`ravel-ingest/src/error.rs:9-84`) distinguishes ShardUnavailable, AckTimeout,
Abandoned, SegmentBuild (non-retryable input error), SeriesIdCollision,
SeriesValueKindMismatch, StaleProvisioningView, BufferBudgetExceeded, with a
documented `is_retryable()` and gateway status mapping (429 vs 503). The
S3 mapper (`s3.rs:420+`) documents why an exhausted-retry `Generic` maps to
Transient and calls out the `retry_timeout` substring hazard.

### Async correctness (SUPPORTED)

No `unbounded_channel` in production; shard channels are
`mpsc::channel(config.channel_depth)` and backpressure propagates through the
awaited send (`router.rs:110,391`; `shard.rs:1235-1247` documents the single
flush-trigger block point). Cancellation: strict-mode ack uses
`tokio::time::timeout` over `join_all` of oneshots (`router.rs:406-408`); a
dropped ack sender (actor panic) is caught and mapped to ShardUnavailable
(`router.rs:414-420`). Flush deadline uses `tokio::select!` racing an injected
`Clock::sleep` rather than the real timer, keeping test determinism
(`shard.rs:664-682`). No mutex is held across `.await` in production (the two
`.lock().await` hits are a test gauge lock and a tokio `Mutex` guarding a
JoinHandle in `audit_pipeline.rs:255`). Server shutdown is wired through
oneshot senders per listener (`lib.rs:431-478`).

## Failure scenarios

1. Hostile PromQL nesting aborts the query process. A `POST /api/v1/query`
   body up to `MAX_BODY_BYTES = 1 MiB` (`http/handlers.rs:31`) of nested
   parens/unary (`((((...`, `----...`, or `1+1+1+...`) produces an AST of
   depth ~O(body length). `promql_parser::parser::parse` builds it, `check_ast`
   recurses over it, `Evaluator::eval_expr` recurses (`eval.rs:747-757`:
   `Paren`/`Unary`/`Binary` arms recurse with no depth counter), and `Drop` of
   the nested `Box<Expr>` recurses. No length cap and no depth guard exist on
   this path (grep for depth/recursion/query.len in `ravel-promql` and
   `ravel-query` finds only subquery-grid budgets, `config.rs:8-11`
   max_series/max_samples, which bound evaluation width, not AST depth). The
   evaluation runs synchronously on the tokio worker inside
   `tracing::debug_span!(...).in_scope(...)` (`engine.rs:456-457`), so a stack
   overflow aborts the whole ravel-server (or query pod) process, taking every
   concurrent tenant's in-flight query with it. `tokio::time::timeout`
   (`engine.rs:430`) does not help: a stack overflow is not a slow future, and
   SIGABRT is not catchable by `catch_unwind`. Multi-tenant blast radius on a
   shared surface.

2. Shard-actor panic degrades ingest with no restart. If any invariant guard
   in the actor loop fires (`shard.rs:1243`, or a bug reaching one of the
   fail-loud paths), that shard actor task ends and is never respawned; all
   series hashing to that shard return `ShardUnavailable` until the process is
   restarted. This is observable (shard_deaths metric) and the router keeps
   serving other shards, so it is graceful degradation, not a full outage, but
   there is no supervisor/restart layer.

3. Blocking disk IO on a tokio worker. `TieredCache::get_or_fetch`
   (`ravel-cache/src/tiered.rs:152-167`) calls the synchronous
   `DiskCache::get`/`insert` (which do `fs::File::open` + read/`rename`,
   `disk.rs:454-492,598+`) directly inside an async closure, with no
   `spawn_blocking`. On a disk-tier hit/miss this blocks the worker thread for
   the duration of a local file read. It is opt-in (disk tier off by default)
   and every failure degrades to a miss, but under a hot disk tier it can stall
   an executor thread. P3.

## Tests or commands run

Read-only. Representative greps (all under the repo root):
- `grep -rn "allow(clippy::unwrap_used|expect_used)" crates/*/src services/*/src`
  -> 293 (bench excluded), all cfg(test)-gated (brace-aware Python scan
  returned 0 production hits).
- `grep -rn "allow(unsafe_code" crates services` -> 0.
- Pattern totals across `crates`+`services`: `todo!/unimplemented!` 0;
  `unwrap()/expect(` 9100 (essentially all test/bench); `panic!/unreachable!`
  386 total, 29 in production items; `unbounded` 8 (all test strings/idents, 0
  channels); `unsafe ` 1 (test string).
- Inspected corpora referenced as evidence: `ravel-segment/tests/fuzz_mutation.rs`
  (proptest full_decode/point_probe over mutated bytes),
  `ravel-otap/tests/decode_panic_caught.rs::arrow_boundary_panic_is_typed_error_not_unwind`,
  `ravel-logseg/tests/corrupt.rs` (proptest), `ravel-remote-write/src/snappy.rs`
  tests, `ravel-cache/src/disk.rs` tests
  (`short_and_garbage_entries_never_panic_under_age_check`,
  `every_disk_failure_degrades_to_a_miss`).

## Unknowns

- PromQL stack-overflow (finding 1) is not executed; depth-to-overflow is
  inferred from a recursive evaluator, a recursive-descent AST, and a 2 MiB
  default worker stack. STRONGLY SUPPORTED, not VERIFIED. The exact nesting
  depth that overflows was not measured.
- OTLP nested-value recursion (`logs_normalize.rs:391-423` convert_value,
  `traces_normalize.rs:555`) recurses on decoded `AnyValue` array/kvlist trees
  with no explicit depth cap. It is mitigated by prost's default decode
  recursion limit (100 nested messages), which caps the tree depth before
  convert_value ever runs. WEAKLY VERIFIED (relies on the known prost default;
  not confirmed the workspace leaves it at default). If a future prost bump or
  a `no_recursion_limit` change lifts it, this becomes a second stack-overflow
  vector.
- Effect of a deployment building with `panic = "abort"` on the two
  `catch_unwind` guards (`ravel-otap/src/stream.rs:439`,
  `ravel-ingest-router/src/lib.rs:254`): the release/dev/ci profiles do not set
  `panic` (`Cargo.toml:175-194`), so default unwind holds and the guards work.
  A downstream profile override would silently defeat them. NOT ASSESSED beyond
  the profiles present.
- Did not audit `ravel-analytics`, `ravel-affinity`, `ravel-fleet`,
  `ravel-tenant-resolve` internals line-by-line; scanned for the patterns above
  and found nothing off-pattern.

## Severity-ranked findings

### P1 - PromQL parser/evaluator has no recursion-depth or query-length guard
Evidence label: STRONGLY SUPPORTED.
A hostile deeply-nested PromQL query (up to the 1 MiB POST body cap) is parsed,
type-checked, evaluated, and dropped by recursion with no depth counter,
overflowing the query worker's stack and aborting the shared query process. Not
catchable by the surrounding `tokio::time::timeout` or by `catch_unwind`.
Multi-tenant DoS on a shared surface. Fix: cap query byte length (Prometheus
uses no cap but bounds header/body sizes far below 1 MiB for the query string)
and/or add an explicit AST-depth check before evaluation, or evaluate on a
thread with a bounded, guarded stack (e.g. `stacker`).
File:line: `crates/ravel-query/src/http/handlers.rs:31`,
`crates/ravel-promql/src/eval.rs:747-757`,
`crates/ravel-query/src/engine.rs:448-457`.

### P2 - No supervisor/restart for shard actors; an actor-loop panic permanently degrades a shard
Evidence label: VERIFIED.
Shard actors are spawned once at router construction and never respawned. A
panic on the actor task (the semaphore-closed guard, or any future fail-loud
path that runs on the actor rather than in a spawned flush) ends that actor;
every series hashing to it returns `ShardUnavailable` until process restart.
Observable via `shard_deaths`, and other shards keep serving, so it is
degradation rather than outage, but there is no recovery short of restart.
File:line: `crates/ravel-ingest/src/router.rs:104-129,391-433`,
`crates/ravel-ingest/src/shard.rs:1241-1247`.

### P2 - OTLP nested attribute recursion relies on prost's default decode limit, not an in-crate guard
Evidence label: WEAKLY VERIFIED.
`convert_value` recurses over decoded `AnyValue` array/kvlist trees with no
in-crate depth cap. Safe today only because prost caps decode nesting at 100.
This coupling is undocumented at the recursion site and would break silently if
the decode limit is ever lifted. Add an explicit depth cap in the normalizer,
or assert the decode limit.
File:line: `crates/ravel-otlp/src/logs_normalize.rs:391-423`,
`crates/ravel-otlp/src/traces_normalize.rs:555-575`.

### P3 - Blocking file IO on the tokio runtime in the disk cache tier
Evidence label: VERIFIED.
`TieredCache::get_or_fetch` calls synchronous `DiskCache::get`/`insert`
(`fs::File::open`, read, `rename`) inline in an async closure with no
`spawn_blocking`. Opt-in and miss-degrading, but stalls a worker thread on a
hot disk tier.
File:line: `crates/ravel-cache/src/tiered.rs:152-167`,
`crates/ravel-cache/src/disk.rs:79,454-492,598`.

### P3 - `catch_unwind` guards depend on default unwind panic strategy
Evidence label: SUPPORTED.
The arrow-decode and router-sweep `catch_unwind` guards convert third-party
panics to typed errors only under `panic = "unwind"`. Current profiles leave
`panic` at default (unwind), so they work; a downstream `panic = "abort"`
override would defeat them without any compile error. Worth a comment pinning
the assumption.
File:line: `crates/ravel-otap/src/stream.rs:439`,
`services/ravel-ingest-router/src/lib.rs:254`, `Cargo.toml:175-194`.

### Pattern table

| Pattern | Production-path count | Test-path count | Worst instance |
|---|---|---|---|
| unwrap()/expect( | 0 | ~9100 (incl. bench/sim) | none in production; all under `#[cfg(test)]` |
| panic!/unreachable! | 29 (invariant guards) | ~357 | `shard.rs:1243` actor-loop panic, no respawn (P2) |
| todo!/unimplemented! | 0 | 0 | none |
| unbounded_channel | 0 | 0 | none (all channels bounded mpsc) |
| allow(unwrap/expect) overrides in src/ | 0 leaking | 293 (all cfg(test)-gated) | none reach production |
| allow(unsafe_code) | 0 | 0 | none; `unsafe_code = "forbid"` |
| unsafe blocks | 0 | 0 | none |

## Confidence

High on the positives (forbid/lints/decompression caps/bounded channels/error
taxonomy): these are directly verifiable by grep and reading the guarding code,
and the chair's clean clippy run corroborates the lint enforcement. Medium-high
on the P1 PromQL finding: the code path (recursive parse/check/eval/drop, no
depth or length cap, synchronous on the worker, 1 MiB reachable body) is
unambiguous, but the exact overflow depth was not executed. Medium on the
prost-recursion mitigation for OTLP, which rests on a known-but-unpinned
default. Not assessed: the internals of four smaller crates and the effect of
non-default `panic` profiles.
