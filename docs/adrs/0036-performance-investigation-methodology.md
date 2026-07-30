# ADR-0036: Performance investigation methodology and scope

## Context

We want a rigorous, evidence-based performance investigation of Ravel:
architecture and data-path review, a performance model per workload, a
reproducible benchmark baseline, profiling, an I/O architecture review
(including an io_uring suitability call), an Arrow zero-copy and
buffer-ownership assessment, and a prioritized, staged optimization plan.
Only after that plan exists should implementation begin.

Four research passes (architecture and data flow, benchmark and
profiling inventory, I/O model, Arrow integration) were run against the
current codebase to ground this ADR. The finding that decides the shape
of this ADR is this: **every benchmark number in `BENCHMARKS.md` is
measured against the in-process `MemoryStore` backend.** There is no
S3/MinIO measurement anywhere in the repository. This is not a gap we
are discovering; the repo already says so about itself: issue #27
states outright that the 347k pts/s ingest figure "isn't a usable
reference point" for anything comparative, because it excludes real
object-store latency.

The catalog-fold result makes the risk concrete: folding cuts catalog
resolve requests 723x-7,159x, but wall-clock improvement on MemoryStore
is only 2.1-2.2x, because MemoryStore has no per-request latency to
amortize. The documented expectation is that this gap "widens
enormously" against real S3. Any bottleneck ranking built only on
today's numbers would very likely rank object-store round-trip count
below where it belongs, and that ranking would invert the moment real
network latency enters the picture.

Two other constraints shape the plan:

- Local development happens on darwin. Linux `perf`, flamegraphs,
  off-CPU analysis, and hardware performance counters do not exist
  here; profiling runs need the Linux reference host
  (`ci-16gb-fsn1-1`), which `BENCHMARKS.md` already documents as
  running "often under co-resident CI load" (it doubles as an Actions
  runner). Profiling and benchmark methodology must account for that
  noise rather than discover it after the fact.
- No profiling tooling exists in the repo today (no flamegraph, dhat,
  pprof, or iai; no `.cargo/config.toml`; the `profile_hotspots` bin
  referenced in `BENCHMARKS.md` was retired with ADR-0027 and no longer
  exists). `[profile.release] debug = 1` is already set workspace-wide,
  which is the one piece already in place for line-level profiling.

## Decision

**This ADR decides methodology, sequencing, and two structural
questions the research already answers. It does not decide a ranked
bottleneck list or an optimization backlog** - those are measured
outputs of the first execution wave, not inputs to this document.

### Sequencing

Wave 1 of this epic is measurement infrastructure, not optimization:

1. An S3/MinIO benchmark panel added alongside every existing
   MemoryStore benchmark that currently lacks one, so every number this
   investigation relies on has a real-object-store counterpart. Request
   count remains a useful supplementary signal, not the primary one,
   once real latency is measurable.
2. Missing benchmark coverage for workloads named in scope but
   currently absent: concurrent readers/writers, cold-cache behavior,
   end-to-end PromQL/SQL query latency, and the RLOG (logs) path -
   none of these have any benchmark today.
3. Minimal profiling tooling (flamegraph generation and allocation
   profiling at minimum) wired in behind a documented, reproducible
   procedure that runs on the Linux reference host, with the
   co-resident-CI-noise caveat handled by reporting distributions
   (median, p95/p99, and variance or confidence interval) rather than
   single-run averages, and by noting load conditions at run time.
4. A written Phase 7 deliverable (architecture summary, baseline
   results with environment details, ranked bottleneck inventory with
   evidence and confidence level, prioritized proposals, staged
   execution sequence) produced from Wave 1's actual measurements.

Implementation waves (Wave 2 onward) are scoped from that document, not
from this ADR, and each rides the epic's normal per-wave checkpoint
review before landing.

### Settled now, no further measurement needed

**io_uring: rejected on structural grounds.** The research confirms
Ravel's data plane has no local file I/O to accelerate: no WAL, no
mmap, no temp-file spill, no local cache. The only production-path
local-disk touch anywhere in the workspace is reading a credentials
file once at CLI startup. Every hot-path byte moves over HTTPS to S3
via reqwest/hyper on tokio's epoll-based reactor. io_uring's file-I/O
advantages have no target here, and using it for the socket path would
mean replacing the async runtime underneath hyper - a stack change out
of all proportion to any bottleneck this investigation has found. This
question is closed; it does not need a Wave 1 task.

**Arrow zero-copy claim in `docs/arrow-datafusion-plan.md` does not
match the implementation, and the doc needs correcting.** Section 2 of
that plan states SQL scan batches are built from the segment SoA
surface via "buffer adoption, not a copy." The actual code
(`crates/ravel-sql/src/scan.rs:333-362` and `:505-517`) transposes SoA
vectors into an intermediate `Vec<ScanRow>` (~48 bytes per sample), then
gathers a fresh `Vec` per column per batch before calling `Array::from`.
The `from` call adopts that per-batch scratch Vec, never the segment
decode output - so no copy is eliminated relative to a naive scan, only
relocated later in the pipeline. This is a confirmed, file:line-level
finding, not a hypothesis, and it corrects existing documentation. It
is recorded here as a finding; whether and how to reduce that
transpose is a Wave 1/2 measurement-and-proposal question, since its
actual query-latency impact against real S3 latency is unmeasured.

### Explicitly deferred

Any change that would decode RSEG values directly into Arrow buffers
without transformation requires `VAL_RAW_F64` payload alignment in the
segment format, which the existing Arrow/DataFusion plan already
documents as "physically impossible in v1." RSEG is a frozen format
(`docs/segment-format.md`); changing its layout requires the
format-change procedure, its own ADR, and a version bump under
ADR-0027's single-version pre-release rule. This investigation may
recommend such a change as a scored, evidenced proposal, but no format
change is authorized by this ADR, and no implementation wave under this
epic may touch the RSEG layout.

### Benchmark and profiling protocol

- Every benchmark run reports environment: CPU, memory, storage/
  filesystem, OS/kernel, Rust toolchain, compiler profile and feature
  flags, dataset size and distribution, concurrency level, cache state,
  and warm-up procedure, per `docs/benchmarking.md`'s existing
  methodology, extended to cover the S3/MinIO panel.
- Report distributions (median, p95, p99) and a variance or confidence
  measure, not single averages, for every new or re-run benchmark.
- Profiling runs that need Linux-only tools are dispatched to the Linux
  reference host, not attempted on the local darwin machine; the
  co-resident-CI-noise caveat is recorded alongside every such result.

## Rejected alternatives

**Start ranking bottlenecks and implementing optimizations directly
from today's numbers.** Rejected: every number available today is
MemoryStore-only, the repo's own issue #27 already disowns that number
as non-representative, and the catalog-fold result demonstrates
concretely how a MemoryStore-measured win (2.1-2.2x) can understate a
real-object-store win (documented as expected to be far larger) by an
order of magnitude or more. Ranking now would optimize against the
wrong cost model.

**Investigate io_uring as an open question before deciding.** Rejected:
the architecture research already establishes there is no local file
I/O in the data plane to accelerate. Spending a Wave 1 task
re-confirming this with benchmarks would burn effort on a question the
existing code structure already answers definitively.

**Fix the Arrow scan transpose immediately, without measurement.**
Rejected: the transpose is a confirmed inefficiency, but its actual
contribution to end-to-end SQL query latency is unmeasured, particularly
against real S3 fetch latency where the transpose's CPU cost may be
proportionally small. It becomes a scored Wave 1/2 proposal instead of
an immediate fix, so it can be prioritized against the S3-latency
findings rather than fixed in isolation.

**Profile exclusively on the shared CI host without addressing noise.**
Rejected: `BENCHMARKS.md` already flags that host as running under
co-resident CI load. Treating single runs from that host as reliable
would reintroduce exactly the kind of unrepresentative number this ADR
exists to move away from.

**Pursue true zero-copy RSEG decode as part of this epic's
implementation waves.** Rejected: it requires an RSEG format change,
which is a frozen-contract decision needing its own ADR and version
bump. Bundling it into this epic would make the epic's implementation
waves depend on a separate architectural decision this ADR is not
making.

## Consequences

- Wave 1 produces measurement infrastructure and a written Phase 7
  optimization plan, not merged performance changes. That plan, not
  this ADR, is what subsequent implementation waves are decomposed
  from.
- Two questions this investigation was asked to answer are answered
  now: io_uring is rejected structurally, and the Arrow
  zero-copy-adoption claim in `docs/arrow-datafusion-plan.md` is
  confirmed inaccurate and needs a documentation correction (tracked as
  a Wave 1 task, since it is a one-file, low-risk fix with a clear
  acceptance test: the doc no longer claims buffer adoption where a
  transpose exists).
- Any proposal that would require changing the RSEG format is out of
  scope for this epic's implementation waves; it is recorded as a
  deferred, separately-gated follow-up if the measurements support it.
- Benchmarking and profiling standardize on reporting distributions and
  environment detail, and on running Linux-only tooling on the Linux
  reference host with its noise caveat documented alongside results.
