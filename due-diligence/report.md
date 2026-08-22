# Ravel Technical Due Diligence: Architecture, Correctness, Security and Production Readiness

## 1. Executive verdict

NOT YET WRITTEN. Filled in last, after evidence and rebuttals.

## 2. Review provenance and methodology

Subject of review: the Ravel repository at commit `527a16db2e4d47b2924e4de4a4db32d7583fda33`, committed 2026-08-22T22:53:40+03:00 (branch `main` at dispatch time, reviewed as a frozen detached checkout). The dispatch clone carries a single squashed commit and no tags, so commit history and release tags were not available as evidence and, per the review charter, were not used to infer maturity.

Environment: Linux 6.8.0-60-generic, 8 cores, 15 GiB RAM, 100 GB free disk. Toolchain: rustc 1.97.1 (2026-07-14), cargo 1.97.1. cargo-nextest present; cargo-deny and cargo-audit not installed at start (installation attempted, see evidence appendix). Docker, MinIO, kind, Kubernetes: probed and unavailable; every check requiring them is marked NOT ASSESSED (environmental).

Scope of the tree: 28 library crates and 4 service binaries (`ravel-server`, `ravel-ingest-router`, `ravel-operator`, `ravel-cli`), 690 Rust source files totaling about 408k lines, 104 ADRs, normative format documents under `docs/`, protobuf definitions under `proto/`, Kubernetes manifests and operator under `deploy/` and `services/ravel-operator`, CI under `.github/workflows/`.

Method: twelve independent specialist investigations (agents A through L, memos in `due-diligence/memos/`), each restricted to its charter section and forbidden from citing another agent's memo as evidence; a second adversarial pass over every Critical/High finding (`due-diligence/rebuttals.md`); build, lint, and targeted test execution on this host (`due-diligence/evidence/commands.md`). Evidence labels used throughout: VERIFIED, STRONGLY SUPPORTED, IMPLEMENTED WEAKLY VERIFIED, DOCUMENTED CLAIM, CONTRADICTED, UNKNOWN, NOT IMPLEMENTED, NOT ASSESSED.

## 3. Architecture in one page

Ravel is a multi-tenant telemetry database (metrics, logs, spans, alert records) whose only durable state is an S3-compatible bucket. Every process (`ravel-server` in `all`, `gateway`, `query`, or `maintain` mode) is disposable: it holds caches and in-flight buffers, never state whose loss changes acknowledged durability. Ingest accepts OTLP (gRPC and HTTP), OTLP-Arrow, and Prometheus Remote Write 1.0/2.0. A gateway authenticates and resolves the tenant, admission-limits the request, and routes points by `hash(tenant, series_id) % shard_count` to single-threaded shard actors with bounded queues. Each actor buffers up to a flush budget (default 2 s), builds an immutable segment (RSEG for metrics, RLOG for logs, RSPAN for spans), and publishes it with a two-object protocol: data PUT, then a commit-record PUT with create-if-absent semantics. In strict mode (default) the client is acknowledged only after the commit PUT succeeds, and receives commit tokens for read-your-write.

Readers discover data through the commit records, never data objects: a query LISTs commit records per shard and ingest hour for the recent window, and reads folded catalog snapshot parts (behind a CAS'd HEAD pointer) for sealed history. A query resolves one pinned snapshot and uses it for its whole execution. Duplicate samples across segments are resolved at query time by a deterministic total order, which is what makes compaction (L0 to L1, publish-then-supersede with a conservation gate) safe to run concurrently with reads: an L0 input and its L1 replacement can both appear in a snapshot and yield the same answer. Deletion is three-phase everywhere: a durable record first (compaction record, retention tombstone, erasure request), logical exclusion from new snapshots second, physical sweeping third, gated by a protection horizon that budgets query duration, grace, and clock skew, all pinned in a durable `sys/gc` config object.

Query surfaces: PromQL (own evaluator, Prometheus HTTP API shape), SQL over DataFusion (cargo feature `sql`), Flight SQL (feature `flight-sql`), an analytics stage, and alert evaluation that stores alert state as ordinary durable records, not process memory. Distributed query is opt-in and cost-gated: workers self-register via heartbeat objects in the bucket, coordinators rendezvous-hash slices to peers, and cross-cluster federation reaches remote clusters only through their public API with separate credentials. A Kubernetes operator (`ravel-operator`) and an ingest affinity router (`ravel-ingest-router`) round out the services.

```mermaid
flowchart TD
    C[OTLP gRPC/HTTP, OTLP-Arrow, Remote Write 1.0/2.0] --> GW[Gateway: auth, tenant resolve, admission limits]
    GW --> RT["Router: hash(tenant, series_id) % shards"]
    RT --> SA[Shard actors: bounded mpsc, buffer, build immutable segment]
    SA -->|1. data PUT| S3[(S3 bucket: the only durable state)]
    SA -->|2. commit PUT, create-if-absent| S3
    SA -->|strict ack + commit tokens| C
    S3 --> CAT[Catalog: LIST commit records open hours + folded snapshot parts behind CAS HEAD]
    CAT --> QF["Query frontend: /api/v1/query*, SQL, Flight SQL"]
    QF --> RD[Segment reader: footer suffix GET, prune, ranged page GETs]
    RD --> EV[PromQL evaluator / DataFusion pipeline, query-time dedup]
    MW[Maintain mode: compaction L0-L1, retention, GC sweeper, erasure rewrite] --> S3
    MW -->|publish-then-supersede, horizon-gated deletes| S3
    QW[Peer query workers: heartbeat keys in bucket, rendezvous-hashed slices] <-->|SeriesFetch gRPC, fragment token| QF
```

The load-bearing design choices: object storage is used as the coordination layer (create-if-absent for commit atomicity and compaction races, CAS for catalog HEAD, heartbeat objects for membership); correctness never depends on LIST ordering, only on LIST completeness over bounded windows; event time is never trusted for discovery (commit records bucket by ingest hour, with admission-enforced skew bounds that make listing windows sound); and every optimization (catalog snapshots, indexes, caches, pruning) is constrained to widen, never narrow, the read set, with fail-closed degradation to plain listing.

## 4. The strongest parts of the design

These are merits established by code and executed tests, not by documentation volume.

1. The acknowledgement point is honest, and everything downstream is built to keep it honest. The strict ack fires only after two conditional PUTs (content-addressed data object, create-if-absent commit record), identity is pinned before the first byte leaves the process, retries are idempotent by construction, and a same-key-different-content collision is a fatal SplitBrain rather than a silent overwrite. Two investigators independently reconstructed the same state machine from code, and the ambiguous cases (applied-but-response-lost on both PUTs) are exercised by fault-injection tests that assert the fault fired. Most systems document a durability story; this one made the ambiguous-response cases unit-testable.

2. Overlap harmlessness as the compaction safety argument. Compaction never dedups; queries always dedup under one deterministic total order (last-writer by (created_unix_ns, epoch, seq, page index), then value bit pattern). Therefore any snapshot containing an L0 input, its L1 replacement, or both yields identical results, and compaction needs no locks, no fencing, and no reader coordination. The correctness burden moves to one place (the dedup order), which is shared bit-for-bit across PromQL, SQL, and the distributed protocol (which ships per-sample provenance on the wire specifically to preserve it). This is a genuinely elegant load-bearing invariant, in the engineering sense: it converts a family of race conditions into one testable function.

3. Deletion is uniformly three-phase with a durable, deployment-wide horizon contract. Every deletion family (compaction supersession, age retention, orphan GC, GDPR erasure, idempotency markers) writes a durable record first, excludes logically second, sweeps physically third. The protection horizon is not a per-process flag: it lives in a CAS'd `sys/gc` object, is validated at write time, and is re-asserted at maintain startup against the running sweeper's own clock-skew declaration, so a skew-uncovered horizon can neither be written nor run against. The fail-closed asymmetries are consistently in the safe direction (an unreadable catalog HEAD blocks retention; an absent one proceeds).

4. Optimizations are constrained to widen, never narrow. Catalog snapshots, the metric-name index, pruning, and caches all follow one rule: a failure or staleness anywhere degrades to more listing and more reading, never to missing data. The one place this rule is violated (the fold's bounded reconcile window interacting with the superseded-input sweep, finding R2) is visible precisely because the rest of the system holds the rule.

5. Event time is never trusted for discovery. Commit records bucket by ingest hour; admission enforces skew bounds (+10 m future, -2 h lag) that are exactly what make the catalog's listing window sound; a broken receiver clock fails loudly (compiled floor, 503) rather than acking data into an unreachable bucket. The coupling between the admission bound and the listing window, the classic silent-loss trap in event-time systems, is documented as a paired-config discipline and asserted at startup where copies exist.

6. Verification culture matches the ambition. The consistency document opens with "tests in tests/failure/ assert every claim here" and this is largely true: a crash matrix as executable tests, a FaultStore that injects by operation kind and occurrence with asserted counters, MemoryStore as a semantics oracle with pagination stress, golden-byte suites per format, mutation fuzzing, a PromQL differential suite against a real pinned Prometheus binary (executed in this review: 91/91), an SQL differential against an independent oracle, and a seeded whole-system simulation checking durability/visibility/conservation invariants per cycle. The conformance tables in the docs are generated from the differential runs, so the documented compatibility gaps are measured, not claimed.

7. Multi-tenancy and admission are structural, not bolted on. Tenant identity flows from a durable hash-scheme marker; object keys, catalogs, caches, admission, and metrics are all tenant-hash-scoped; per-tenant SSE-KMS and legal holds exist; and the admission layering (body cap, byte-rate bucket, structural caps, series caps) is ordered before expensive work. (Depth of the security review is in section 15.)

What keeps these merits from summing to "production ready" is not their quality but their youth: single-version formats pre-release, thin store qualification against non-AWS backends, the fold-window defect family, and the operational surface a section-17/22 reader will find still maturing.

## 5. Top findings and blockers

NOT YET WRITTEN.

## 6. Production-readiness scorecard

NOT YET WRITTEN.

## 7. Claim Verification Matrix

NOT YET WRITTEN.

## 8. Consistency and durability analysis

Finding first: the strict-acknowledgement claim ("after a strict ack, no crash of any Ravel process may lose that data") is VERIFIED as a claim about Ravel processes, conditional on the object store honoring conditional PUT, strong read-after-write, and strong complete LIST. Two independent investigators (memos A and B) reconstructed the protocol from code without consulting each other and reached the same state machine, and the failure-injection suites exercise the ambiguous cases rather than only the happy path. The claim's boundary is equally clear: it does not cover bucket loss, lifecycle misconfiguration, credential compromise, or a lying backend, and the code and docs say so rather than blur it.

The commit protocol, from code (crates/ravel-commit/src/publish.rs, crates/ravel-ingest/src/shard.rs): a flush pins its full identity (writer UUID, epoch, sequence, ingest hour, content hash) before the first PUT. The data object PUT is create-if-absent at a key embedding that identity plus a blake3-derived hash; AlreadyExists is success because only a prior attempt of the same pinned flush can have created the key. The commit record PUT is create-if-absent at a key fully derived from the record's identity; on AlreadyExists the writer GETs the existing record and compares content hashes: equal is idempotent success, different is a fatal SplitBrain that panics the shard actor rather than picking a side (publish.rs:71-187, shard.rs:758-767). The ack (with commit token) fires only after the commit PUT resolves; every failure path nacks (shard.rs:529-588).

Commit-protocol state machine (condensed from memo B; full table in due-diligence/memos/agent-b.md):

| State | Event | Externally observable result |
|---|---|---|
| Nothing stored | crash | no ack; client retries; bucket unchanged |
| Data PUT ambiguous (applied, ack lost) | retry | AlreadyExists treated as success; exactly one data object |
| Data PUT exhausted | abandon | nack; applied bytes become an invisible orphan, GC'd after ~25 h |
| Data exists, commit absent, crash | crash | invisible forever; GC after grace; client retry duplicates |
| Commit PUT applied, response lost | retry | AlreadyExists + equal content hash = idempotent success, one record |
| Commit AlreadyExists, different hash | split brain | fatal, shard actor dies loudly; no silent overwrite |
| Commit durable, ack lost | client retry | both copies durable; metrics dedup at query; logs/spans visibly duplicated unless idempotency key used |
| After ack | any process crash | data queryable; strict-ack holds |
| Commit present, data unavailable | query | typed SnapshotInvalidated after one re-resolve; wrong data never served |

Idempotency and duplicates: delivery is at-least-once and the docs refuse to claim exactly-once. Metrics collapse at query time under a deterministic total order; logs and spans get user-visible duplicates on retry-after-lost-ack, narrowed by an opt-in idempotency key whose marker is written after commit and before ack, and deliberately not written on multi-shard partial commits because a marker there would make the retry skip the shard that never committed (a correct, subtle decision: honest duplication over silent loss). VERIFIED against crates/ravel-ingest/src/log_error.rs:78-112 and the failure tests in crates/ravel-failure-tests (ack_and_duplicates.rs, retry_and_restart.rs, crash_matrix.rs), which this review executed: 20/20 passed.

What strict-ack does not mean, and where the honest residuals sit:

- Acknowledged-durable is not verified-stored. No upload checksum reaches the wire (an object_store 0.14 API gap) and there is no post-PUT ETag comparison, so bytes corrupted between the local CRC32C pre-flight and S3 storage are acked and discovered only at read time, as a typed error, never as wrong data (s3.rs:45-59, 497-514). P2; the read-side checksum hierarchy plus the scrub tooling make this loss-not-corruption, but an ack is a weaker receipt than it appears.
- Ravel's own orphan GC is the sharpest knife pointed at acknowledged data: an out-of-band loss of commit records (lifecycle rule, prefix delete, persistent backend LIST omission) makes acked data objects look like orphans, and after ~25 h the sweeper deletes them permanently. The mass-orphan circuit breaker only trips at >= 50 candidates AND > 10% of the shard, so small or thinly spread loss is silently destroyed. The project itself calls this the most dangerous flaw in its durability posture (ADR-0058) and ships partial mitigations (breaker, batched re-verify, reconstruction CLI). P1; see the rebuttal record for the challenge and resolution.
- Buffered mode loses its bounded window on crash, by contract, with no token issued. This is a visible opt-in trade, not a caveat buried in docs.

Consistency model as implemented: per-(writer, shard) commit sequencing, no cross-shard ordering, read-your-write via commit tokens whose keys are reconstructed from the token itself (never through listing or snapshots, so tokens survive resharding and folds), and bounded-staleness listing snapshots for token-less reads. Event time is never trusted for discovery; admission bounds skew (default +10 m future, -2 h lag), which is exactly what makes the catalog's listing window sound. The one genuinely irreducible clock dependency on the write side is the ingest-hour bucketing, and even a bad clock fails loudly (a compiled floor rejects nonsense buckets with 503 rather than acking data into an unreachable hour).

Verdict for this section: durability engineering is the strongest part of the system. The design distinguishes durability, visibility, idempotency, and availability with unusual discipline, the failure matrix in docs/consistency-model.md matches the code and is executable (and was executed) as tests, and the remaining risks are either external-to-Ravel (bucket-level threats, addressed as DR posture) or explicitly acknowledged residuals with named mitigations.

## 9. Catalog, snapshot and commit-token correctness

Finding first: snapshot resolution is correct for the open (recent) window and for token reads, and every catalog index failure mode degrades to wider listing rather than to missing data, with one significant exception. The fold's reconcile machinery has a 26-hour window anchored on the target hour, and a compaction or rewrite record published more than 26 hours behind the fold watermark is never applied to the folded snapshot. That stale snapshot then keeps naming L0 objects that the superseded-input sweep (which has no HEAD-reachability gate, unlike retention) physically deletes about 25 hours later. Result: persistent, unalarmed SnapshotInvalidated 503s for every non-token query touching those hours. This is the most important single defect found in the review (P1, availability and visibility, not data loss: the L1 replacement parts hold every sample). It is not hypothetical sequencing: the stale state is pinned as expected behavior by an in-tree test (crates/ravel-catalog/src/fold.rs:3507-3557, `reconcile_ignores_late_record_outside_window`), and the triggering operations are ordinary (first enablement of `--mode maintain` on a bucket with history, a maintain outage longer than about a day, a compaction catch-up campaign). Full mechanics in memo C (CE1) and the rebuttal record.

How discovery works when healthy: readers LIST commit records per (shard, ingest hour) for the window above the fold watermark, and read immutable snapshot parts behind a CAS'd HEAD pointer for sealed history below it. The fold is a cost optimization with a soundness rule: HEAD/part read failures, corrupt parts, stale cached HEADs, racing folders on the CAS all degrade to Phase-1 listing (crates/ravel-catalog/src/snapshot_resolve.rs), and a snapshot entry whose object was retired resolves NotFound -> SnapshotInvalidated -> one re-resolve. The soundness rule holds everywhere except the reconcile-window case above, where the degradation path is not taken because the snapshot still serves entries it believes are live.

Commit tokens are the strongest correctness primitive in the read path: a token fully determines its commit-record key, resolution GETs that exact key (with separate NotFound and transient retry budgets), falls back through compaction-input and rewrite-effective-input chains, resolves tombstoned buckets as deliberately-retired (zero segments, not an error), and otherwise fails with a typed unsatisfiable-token error rather than silently serving stale data (catalog.rs:1900-2150). Tokens survive resharding by construction (resolution never consults shard_count) and are unaffected by the fold defect above. VERIFIED by code and by the executed test suites (ravel-catalog: part of the 975-test core batch, all passing).

Snapshot isolation: one snapshot per query, pinned for the query's lifetime; mid-query commits, compactions, and deletions do not affect it; the GC protection horizon (durably pinned in `sys/gc`, enforced at write time and re-asserted at maintain startup against the running sweeper's own clock-skew config) is what makes the pin safe without any store-side lease. The two-fence design (you cannot write a skew-uncovered horizon, and a sweeper with a larger skew config than the written one refuses to start) is careful engineering: memo C attempted a pinned-reader-vs-sweeper counterexample and failed within declared parameters (CE7).

Other residuals, all bounded and mostly documented: a writer clock more than `clock_skew_allowance` (5 m) ahead near an hour boundary publishes into a bucket the reader's listing window does not yet reach (self-healing staleness); a folder clock past `fold_safety_margin` can seal an hour early, making a late commit invisible to token-less queries until an operator HEAD rebuild (documented exception, detect-only scrub exists); resolve attaches erasure predicates via one unconditional `del/` LIST per resolve (correctness-critical and always on). Late L0 commits into an already-folded hour are skipped by reconcile deliberately (L0-only buckets are not diffed), which is sound exactly as long as the seal-margin arithmetic and writer abandonment interlock hold; that assumption is declared, tested at the margin arithmetic, and violated only by clock mis-declaration (CE4, P3).

## 10. Compaction, retention and GC safety

Finding first: no counterexample was found that destroys durable, acknowledged data through compaction, retention, or GC, across eleven attempted races (memo C). The deletion discipline (durable record first, logical exclusion second, horizon-gated physical sweep third; every delete idempotent; every sweep restartable from zero) is applied uniformly across all four deletion families, and the dangerous decisions are conservative in the right direction: fail-closed on unreadable state, re-verify immediately before delete, alarm-and-stop on divergence. Three real defects were found, all in the completeness/lifecycle direction rather than the destroy-live-data direction.

What holds (each attempted and defended, evidence in memo C):

- Compaction is publish-then-supersede with a pre-publish sample-count conservation gate: outputs must conserve the exact input sample count or the run aborts and publishes nothing, leaving inputs live. Compaction never dedups (dedup is query-time), which is what makes overlap harmless: a snapshot seeing both an L0 input and its L1 replacement returns identical results. Racing compactors converge via create-if-absent (the loser verifies the winner's input-set hash, repairs missing parts, and stands down; divergent input sets alarm and delete nothing).
- Orphan GC requires an identity to be absent from two strongly consistent LISTs (initial plus one fresh batched re-verify), an age gate behind the writer-abandonment interlock (writers never publish a flush older than max_flush_lifetime; GC's gate is grace + max_flush_lifetime, ~25 h vs minutes of transport lifetime), and the mass-orphan circuit breaker in front of an all-or-nothing delete.
- Retention is tombstone-first, horizon-gated, with an ADR-0020 HEAD-reachability gate: a bucket still named by the live catalog snapshot is never physically swept (present-but-unreadable HEAD blocks fail-closed; absent HEAD proceeds, correctly, since no snapshot names anything). Deletion order is fixed and ends with a verifying LIST before the tombstone itself is removed.
- A pinned reader cannot lose its inputs to any horizon-gated rule within declared parameters (the sys/gc two-fence design, section 9).

The three defects:

1. P1: the superseded-input sweep has no HEAD-reachability gate, and combined with the fold's bounded reconcile window it deletes L0 objects a stale folded snapshot still names (the section 9 finding; the delete is legal, the snapshot is wrong, the outcome is persistent 503s on affected hours). The asymmetry is glaring precisely because retention got the gate after what docs/consistency-model.md calls "the shipped failure", and rule 2 never did. The fix shape exists in-tree.
2. P2 (CONTRADICTED doc claim): retention never completes on a bucket that went through a selective-erasure rewrite. Expiry reads only commit and compaction records, never rewrite records (crates/ravel-maintain/src/retention.rs:410-424), so a rewrite-only bucket never expires; and physical_sweep never deletes rewrite record keys (retention.rs:616-619), so an already-tombstoned bucket ends in permanent SweptPartial with the pass re-running every tick. Over-retention and unbounded re-work on exactly the buckets subject to GDPR erasure; contradicts docs/consistency-model.md:438 ("everything in a tombstoned bucket") and ADR-0019's R + horizon bound. No test covers the rewrite-record-in-retention combination.
3. P1 (erasure-specific, half-acknowledged in ADR-0064): the erasure completion gate verifies through a live listing, not through the folded snapshot, so `.done` (and later the `.dreq` predicate sweep) can land while an out-of-window folded snapshot still serves pre-rewrite inputs. If the `.dreq` is swept first, erased-subject records become servable again until the superseded-input sweep removes the inputs; if the sweep goes first, the hours degrade to the persistent-503 state. For a system advertising DSAR erasure with bounded deadlines, a window where erased data is served again is a compliance defect even though it is not data loss.

Structural answers to the charter's standing questions: GC failure is a cost leak, not a correctness risk (everything is conservative; the one exception is the orphan-GC/commit-record-loss interaction, which is a durability finding, section 8). Compaction can fall behind ingestion, and the consequence is bounded and typed (the 25k per-query S3 request budget binds on hot open hours; queries 422 rather than degrade silently), but catch-up compaction over a deep backlog is exactly the operation that triggers defect 1, which turns "compaction fell behind" from a cost problem into an availability incident. The compaction hierarchy is single-level per (shard, hour) with no cross-hour L2 (named as future work in ADR-0018); long-retention large tenants will eventually be bounded by one-part-per-(shard, hour) read amplification, a scaling boundary rather than a defect.

## 11. Distributed query and federation

NOT YET WRITTEN.

## 12. Data formats and upgrade compatibility

Finding first: the durable formats (RSEG for metrics, RLOG for logs, RSPAN for spans, protobuf commit/catalog records) are engineered to a high standard for corruption safety, and to a deliberately low standard, pre-release, for version compatibility. The project's own policy (ADR-0027) is a single supported version per format until first public release: RSEG v6 read support was deleted in the same change that shipped v7, there is no v6-to-v7 migration path, and a reader fails closed with a typed `UnsupportedVersion` on any other version (crates/ravel-segment/src/reader.rs:88, 158-161). ADR-0066 supersedes this at first release with an N/N-1 reader window and a decode-and-re-encode migration primitive, but at the frozen commit that policy is declared, not active. Consequence for adopters: an upgrade that bumps a format version makes existing objects unreadable (the accepted disposition is wipe or re-ingest), and a rolling deployment across a format bump has no overlap window. This is a coherent pre-1.0 stance, stated plainly in the ADRs and CHANGELOG, but it is a hard production-readiness gate: no deployment holding data anyone cares about should cross a format-bump upgrade until the ADR-0066 window ships.

Corruption safety is where the format engineering earns credit:

- Checksum hierarchy: whole-object blake3 recorded in the commit record, footer and per-section CRC32C verified on read, structural validation with checked arithmetic on every section range (validate_sections_v7 enforces mandatory kinds, exclusive section pairs, ranges within the page region, and uncompressed-length limits before any section byte is fetched: reader.rs:138-163).
- Reader limits bound decompression before allocation (ReaderLimits with uncompressed-size caps; a dedicated test pins the uncompressed page size cap: crates/ravel-segment/tests/uncompressed_page_size_cap.rs).
- Hostile-input testing exists and runs: golden-byte suites pin the exact on-disk layouts (golden_bytes_v7.rs for RSEG, golden_bytes_v3.rs for RLOG, golden_bytes_v4.rs for RSPAN, plus a Remote Write golden), mutation-fuzz suites (fuzz_mutation in ravel-segment and ravel-otap) assert corrupt inputs produce typed errors rather than panics or wrong data, and structural-validator negative tests cover the malformed-footer space. This review executed them: 10/10 and 5/5 fuzz-mutation tests passed, and the corrupt-input tests in ravel-failure-tests (corruption.rs) passed.
- Version numbers are reserved forever and never reused (format.rs:8-12), and unknown versions fail typed, so a mixed-version accident is loud, not silent.

Two compatibility observations. First, the format version numbers have moved fast (RSEG at v7, RLOG at v3, RSPAN at v4 per the golden suites, against normative docs describing v1 baselines with amendment layers), which is fine under the single-version policy but means the frozen-contract discipline (docs/segment-format.md rewritten as self-contained v7) is doing real work; the golden suites are what keep the docs honest. Second, the protobuf surfaces (proto/ravel) follow additive-field discipline with unknown-field tolerance, and the queryfrag distributed protocol carries an explicit protocol version (bumped to 3 for per-sample dedup provenance, with refusal semantics for mismatched peers), so intra-fleet mixed-binary operation within one format generation is designed for; it is the cross-format-generation case that is deliberately unsupported pre-release.

Verdict: corruption behavior VERIFIED (tests executed); upgrade compatibility across format bumps NOT IMPLEMENTED at this commit, by documented policy, with a credible successor policy (ADR-0066) not yet in force. Adoption decisions should treat every format bump before first release as a data-migration event.

## 13. PromQL, SQL, query correctness

NOT YET WRITTEN.

## 14. Rust engineering assessment

NOT YET WRITTEN.

## 15. Security and multi-tenancy threat model

NOT YET WRITTEN.

## 16. Observability-product assessment

NOT YET WRITTEN.

## 17. SRE, operations, Kubernetes review

NOT YET WRITTEN.

## 18. Disaster-recovery assessment

NOT YET WRITTEN.

## 19. Performance and scalability analysis

NOT YET WRITTEN.

## 20. Cloud cost model

NOT YET WRITTEN.

## 21. Verification and test-quality assessment

NOT YET WRITTEN.

## 22. Build, dependency, release and supply-chain assessment

NOT YET WRITTEN.

## 23. Failure and chaos matrix

The central question per row is not "does it recover" but "can this fault convert acknowledged or visible correct data into silently absent or incorrect data". Rows marked Verified cite executed tests or code paths confirmed by two independent memos.

| Fault | Ingest consequence | Query consequence | Correctness impact | Availability impact | Recovery | Verified? |
|---|---|---|---|---|---|---|
| Gateway crash | in-flight unacked requests lost; clients retry | none | none (at-least-once; metrics dedup) | brief, per-replica | restart; stateless | Yes: crash_matrix.rs, restart tests executed |
| Shard actor panic | that shard 1/N of keyspace nacks ShardUnavailable | none | none | partial ingest loss until process restart (no in-process actor restart) | process restart | Yes: router.rs:40-47, 391-420 (memo A P3) |
| Query node crash | none | in-flight queries fail; clients retry | none | brief | restart; stateless | Yes: design + heartbeat staleness tests |
| Maintain crash | none | none short-term | none | compaction/GC/fold/erasure stall; cost grows; erasure deadlines at risk | restart from zero, stateless per pass | Yes: sweep/retention restartability tests |
| S3 timeout | retries within flush deadline, else nack | typed error/retry | none | ack latency up; readiness probe flips after 4 failures | automatic | Yes: s3_http_faults.rs executed (503/429/timeout retry tests) |
| S3 throttling | as above | budget-bounded retries | none | degraded | automatic | Yes: backoff tests executed |
| Failed data PUT | nack after budget | n/a | none; orphan possible, GC'd after ~25 h | none | client retry | Yes: crash_matrix.rs executed |
| Failed commit PUT | nack; data object orphaned | n/a | none | none | client retry; orphan GC | Yes: crash_matrix.rs executed |
| Ambiguous PUT (applied, response lost) | retry hits AlreadyExists; idempotent | n/a | none (content-hash verified; different content = fatal SplitBrain, never silent) | none | automatic | Yes: fault_tolerance.rs, retry_and_restart.rs executed |
| LIST failure | flush unaffected | resolve fails typed; retry | none | transient query errors | automatic | Yes: FaultStore tests |
| Truncated/corrupt object | n/a | typed Corrupted, never wrong data | loss (if no other copy), not corruption | affected segments unreadable | scrub detects (7-day period); no repair path (no redundant copy) | Yes: corruption.rs, fuzz_mutation executed |
| Missing committed object | n/a | SnapshotInvalidated, one re-resolve, typed failure | none (fails loudly) | affected hours 503 | depends on cause; see fold-window row | Yes: engine.rs:1077-1142 + tests |
| Commit records lost out-of-band | n/a | data invisible immediately | acked data PERMANENTLY DELETED by orphan GC after ~25 h if below breaker thresholds | affected data gone | reconstruction CLI before the gate; nothing after | Mechanism verified (R1); P1 finding |
| Compaction record lands >26 h behind fold watermark | none | after horizon sweep: persistent 503 on affected hours | visibility loss, not data loss (L1 parts complete) | hours unreadable for weeks/indefinitely, no alarm | operator HEAD rebuild; retention frontier eventually | Mechanism verified (R2); P1 finding |
| Erasure rewrite out-of-window | none | erased subject may become servable again after .dreq sweep | compliance violation window | or persistent 503 (same as above) | same fix as fold-window row | Verified consequence (R3); P1 finding |
| Worker heartbeat stale | none | slices re-dispatched or run locally | none | none to degraded | membership reconverges within 3xH | Code + tests (memo A); heartbeat keys never reaped (P2 cost) |
| Fragment worker dies mid-slice | none | coordinator re-dispatches or runs locally | none claimed | none | automatic | Deferred to memo D |
| Network partition (writers split) | both sides keep writing; UUID identity prevents collision | union at resolve | none | none | automatic | Yes: memo A scenario 3 |
| Compactor races writer (late commit into sealing bucket) | commit lands | resolver serves L0 alongside parts (overlap-harmless) | none within seal margins; fast-folder clock exception documented | none | seal-divergence scrub detects | Yes: memo C CE5/CE6 + tests |
| Retention races reader | none | pinned snapshot protected by horizon arithmetic | none within declared params | none | n/a | Yes: sweep_crash_matrix rows executed |
| GC false positive (orphan) | n/a | n/a | requires two consistent LISTs to omit an existing record = store contract violation | n/a | breaker for mass case | Yes: memo C CE8 |
| Clock skew (writer) | >5 m: data lands in future bucket, temporarily invisible to token-less reads | self-heals as reader clock advances | none permanent (token reads unaffected) | staleness | automatic | Yes: catalog.rs:1262-1276 (memo A P3) |
| Clock skew (sweeper beyond declared allowance) | none | none | early destruction by the skew delta (retention); pinned-reader deletion (GC) | n/a | mis-declaration residual, documented | Code verified (memo C CE7/CE10) |
| Mixed binary versions (same format generation) | protocol-versioned queryfrag refuses mismatched peers | as left | none | reduced distributed capacity | upgrade completes | Code (section 12); not chaos-tested |
| Mixed format generations (e.g. v6 objects under v7 binary) | n/a | typed UnsupportedVersion | none (fail-closed) | affected objects unreadable | wipe/re-ingest (pre-release policy) | Yes: reader version gate + ADR-0027 |
| KMS unavailable (SSE-KMS tenants) | PUTs fail, nack | GETs fail typed | none | tenant unavailable | automatic on KMS recovery | Code path only; NOT ASSESSED live |
| Disk cache corrupt/deleted | none | cache miss, refetch | none (ADR-0046: cache-delete-safe) | latency | automatic | Code + doc claim; not executed here |
| Store LIST omits keys persistently (contract violation) | none visible | data silently invisible; orphan GC may destroy (see commit-record-loss row) | potential silent loss | silent | qualification gate is the only defense; suite is thin (4 probes) | P2 finding (memo B) |

Deterministic whole-system simulation: this review additionally ran the repo's seeded simulation harness (ravel-sim, ADR-0068), which drives real ingest, fold, compaction, sweep, and query cycles under injected faults and checks read-your-write, strict-ack-implies-durable, compaction query equivalence, record-count conservation, and no-leak-past-horizon invariants every cycle. See the evidence appendix for the batch result.

## 24. Documentation and implementation drift

NOT YET WRITTEN.

## 25. Competitive architectural context

Ravel's closest architectural relatives are Thanos, Grafana Mimir, Cortex, Loki, and Tempo (object-store-backed observability), VictoriaMetrics (vertically efficient TSDB), and ClickHouse-based stacks (columnar analytics). The comparison that matters is where durability lives and what an operator must run to keep it.

Where Ravel is structurally stronger:

- No stateful ingest tier. Mimir, Cortex, Loki, and Tempo acknowledge writes into a replicated ingester ring (WAL plus local disk, replication factor, StatefulSets, careful rollout ordering, hand-off or flush-on-shutdown procedures). Ravel acknowledges only after the object store has both the data object and the commit record, so the entire class of ingester-loss incidents (WAL corruption, under-replicated hand-off, PVC scheduling deadlocks, zone-loss quorum math) does not exist. This is the single largest operational simplification in the design, and it is real, not cosmetic: the strict-ack path was exercised in this review by test and by code reading (sections 8 and 21).
- No separate metadata service. Thanos needs a compactor singleton and store-gateway sharding over an index cache; Mimir needs a ring for every component plus a compactor. Ravel's catalog is derived entirely from commit-record keys plus a folded snapshot behind a CAS pointer, with every failure mode degrading to wider listing (section 9). There is no consensus system anywhere (no etcd, no memberlist gossip ring), and worker membership for distributed query is heartbeat objects in the same bucket.
- Read-your-write tokens. Neither Thanos nor Mimir gives a client a token that pins its own write into a later query's snapshot. Ravel's commit tokens are a clean answer to the listing-freshness gap every object-store-backed system has.
- One deletion discipline. Compaction supersession, age retention, orphan GC, and GDPR erasure all follow durable-record-first, exclude-second, sweep-third with one shared protection horizon stored in the bucket (`sys/gc`), where Thanos and Mimir spread deletion safety across component flags that can drift per process.

Where Ravel pays for it:

- Write-path latency and small-object economics. Every shard flush is at least two PUTs (data plus commit). Mimir's ingesters absorb millions of samples per second into memory and write large blocks every two hours; Ravel writes objects every flush interval (default 2 s budget) per active (tenant, signal, shard). At low volume per shard this makes many small objects, which is why L0-L1 compaction and the request cost model (section 20) matter so much more here than in block-per-2h designs. The economics are workable but must be actively managed with shard counts and flush budgets (section 19, 20).
- Ingest tail latency is object-store tail latency. Strict mode cannot answer faster than a PUT round trip. Systems with a local-WAL ack point will always beat it on p99 ack latency; Ravel's counter-offer (buffered mode) gives up the durability claim explicitly.
- Query cold path is LIST plus many GETs. Thanos/Mimir store-gateways hold downloaded index headers on local disk; Ravel resolves snapshots per query (mitigated by folded snapshots, caches, and budgets). The absence of a downsampled tier (acknowledged in the README) makes wide-range queries read every raw hour, where Thanos has 5m/1h downsampling.
- Maturity and ecosystem. The alternatives have years of production hardening, operational folklore, and integration surface (Loki's LogQL, Tempo's TraceQL, Mimir's Alertmanager integration). Ravel's product surfaces are younger and thinner (section 16), and there is no migration tooling of comparable depth.

Against ClickHouse-based observability stacks: Ravel trades ClickHouse's raw scan speed and mature SQL for zero stateful nodes and exact PromQL semantics. A ClickHouse stack needs disk management, replication, and its own ingestion pipeline; it will out-scan Ravel on large analytical queries. Ravel's SQL surface (DataFusion over RSEG/RLOG) is credible for observability queries but is not competing with a mature OLAP engine on breadth or optimizer sophistication.

The honest summary: Ravel occupies a point in the design space (object store as the only stateful component, strict remote-durability ack, exact-by-default semantics) that no mature system occupies. Thanos and Mimir moved durability to object storage for sealed blocks but kept a stateful ingest edge; Ravel removes the edge and pays in request economics and ack latency. Whether that trade wins depends almost entirely on workload shape and object-store pricing, which is why the cost model in section 20 is load-bearing for adoption decisions.

## 26. Production adoption recommendation

NOT YET WRITTEN.

## 27. Production-readiness exit criteria

NOT YET WRITTEN.

## 28. Recommended next experiments

NOT YET WRITTEN.

## 29. Final risk register

NOT YET WRITTEN.

## 30. Evidence appendix

NOT YET WRITTEN. See `due-diligence/evidence/commands.md` for the running command log.
