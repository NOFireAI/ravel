# Agent I memo: performance, scalability, and economics

## Verdict

Ravel's unit economics are dominated by S3 request charges, not storage or compute, and at shipped defaults they are unfavorable by roughly one order of magnitude against tuned configuration and by one to two orders of magnitude against established object-store-backed systems (Mimir, Thanos, Loki) at comparable load; the repository itself knows this (ADR-0076 models 97 percent of the bill as requests) and ships real levers (replica affinity, per-tenant resharding, flush cadence), but two structural floors remain after every lever is pulled: a hard 3 second strict-visibility ceiling caps the flush-cadence lever at about 2.5 seconds (services/ravel-server/src/config.rs:1023, enforced at config.rs:2025), giving every strict-mode (tenant, signal, shard, replica) buffer a permanent floor of about 69,000 PUTs/day, and the background fabric (fleet-global admission reconciliation every 10 s per tenant-signal-process, whole-shard orphan-GC LISTs every 300 s tick, per-query uncacheable resolve LISTs) adds a request stream the shipped cost guide's formula does not include. My model puts a modest single-team deployment at roughly $350/month of requests against $10 of storage, a serious 100-tenant service at roughly $33,000/month at defaults (about $10,000 tuned), and a 1,000-tenant deployment at roughly $135,000/month with levers already applied. The performance envelope is honest but thin: the repo deliberately commits no cross-host numbers, all committed measurements are memory-store or loopback-MinIO component measurements, and there is no measured ingest p99, query p50/p95/p99, throttling, restart-under-load, or backlog-recovery result against real S3 anywhere in the tree. Correctness engineering is strong and read amplification on sealed data is genuinely good (one-shot L0 to L1 compaction, roughly 2x total write amplification); the architecture stops being economic, before it stops working, in the many-tenant strict-mode SaaS shape.

## Model parameters

All values below are shipped defaults read from code on this tree; "assumed" rows are my workload assumptions, not repo values.

| Parameter | Value | Source | Status |
|---|---|---|---|
| shard_count (ingest + server) | 4 | crates/ravel-ingest/src/config.rs:216; services/ravel-server/src/config.rs:57-58 | default |
| max_flush_delay (strict/fast tier) | 2 s | crates/ravel-ingest/src/config.rs:214 | default |
| max_flush_delay_idle (bufferless-waiter tier) | 40 s | crates/ravel-ingest/src/config.rs:221 | default |
| min_flush_bytes | 256 KiB | crates/ravel-ingest/src/config.rs:222 | default |
| target_bytes (size trigger) | 8 MiB | crates/ravel-ingest/src/config.rs:218 | default (never fires at modeled loads, per ADR-0076) |
| max_inflight_flushes | 1 | crates/ravel-ingest/src/config.rs:228; services/ravel-server/src/config.rs:508 | default |
| Strict visibility budget ceiling | 3 s (caps max_flush_delay at ~2.5 s for strict) | services/ravel-server/src/config.rs:1023, 2025-2035; budget = delay + 500 ms reserve, crates/ravel-ingest/src/config.rs:55, 234-235 | default, startup-enforced |
| PUTs per flush | 2 (data + commit record) | docs/catalog-and-mvcc.md "Commit sequence"; docs/guides/cost-model.md:13 | structural |
| Admission reconcile interval R | 10 s; per cycle per (tenant, signal, process): 1 PUT + 1 LIST + 1 GET per sibling | crates/ravel-ingest/src/reconcile.rs:91, 284-390; services/ravel-server/src/config.rs:366-380 | default |
| Maintain/query-worker heartbeat | 60 s PUT + LIST + sibling GETs, liveness 3H | crates/ravel-fleet/src/worker_set.rs:55-62; docs/architecture.md:269-270 | default |
| Store probe | 1 GET / 30 s / process | docs/architecture.md:109-111 | default |
| fold_interval_secs | 300 | services/ravel-server/src/config.rs:143-144 | default |
| maintain_interval_secs (tick) | 300 | services/ravel-server/src/config.rs:148-149 | default |
| Fold no-op guard | fold skips unless watermark advances (~hourly) | services/ravel-server/src/fold.rs:260-267; crates/ravel-catalog/src/fold.rs:804-808 | verified in code |
| fold_reconcile_window_hours | 26 (re-list 27 buckets x shards per advancing fold) | crates/ravel-catalog/src/config.rs:85 | default |
| max_ingest_lag / clock skew / fold margin | 2 h / 5 m / 15 m | crates/ravel-catalog/src/config.rs:6, 8, 20 | default |
| Seal margin (bucket immutable after end) | 1 h 05 m; watermark trails now by ~2 h 20 m worst case | crates/ravel-maintain/src/config.rs:130-132, 426-429; crates/ravel-catalog/src/config.rs:16-20 | derived from defaults |
| Compaction: max_l1_part_bytes / min inputs / footer probe | 256 MiB / 2 / 64 KiB | crates/ravel-maintain/src/config.rs:137, 139, 142 | default |
| grace / protection horizon | 24 h / 25 h 05 m (L0 lives ~26-27 h after supersession) | crates/ravel-maintain/src/config.rs:146, 163-164 | default |
| Orphan GC | whole-shard l0/ LIST every pass, every 300 s tick, per (tenant, signal, shard) | crates/ravel-maintain/src/sweep.rs:93-95, 220-225 | structural (acknowledged in code) |
| Interior reverify (full sweep cadence) | 6 h | crates/ravel-maintain/src/config.rs:217 | default |
| Record cache / head cache / head TTL | 10,000 records per tenant / 10,000 entries / 30 s | crates/ravel-catalog/src/config.rs:12, 37, 22 | default |
| Read cache RAM tier | 256 MiB process-wide; disk tier NOT wired (`--cache-dir` fails startup); spans uncached | services/ravel-server/src/config.rs:613-626, 976; docs/guides/caching.md:46-50, 136-141 | default / gap |
| max_segments / max_series / max_samples / deadline / fetch_concurrency | 1024 / 10k / 10M / 30 s / 8 | crates/ravel-query/src/config.rs:6-15 | default |
| Per-query S3 request budget | derived: ceil(3600/delay) x 3/2 x shards + 5000 = 15,800 at 4 shards, 2 s | crates/ravel-query/src/config.rs:25-32, 63-76 | default (server derives; library fallback uses 500 ms reference, config.rs:43-44) |
| Segment fetch | whole object <= 512 KiB in 1 GET; else 64 KiB suffix + coalesced ranges; 16 concurrent GETs shared | crates/ravel-query/src/fetcher.rs:71-89, 111 | default |
| Catalog LIST cap / prefix-scan crossover | 100,000 / 720 buckets | crates/ravel-catalog/src/config.rs:62, 124 | default |
| Keyed log/span idempotency | +1 prefix LIST per keyed request, +1 marker PUT per committed keyed request | docs/consistency-model.md:172-211; docs/catalog-and-mvcc.md idem section | structural, opt-in |
| Admission caps | 200k active series and streams per (tenant, signal) shipped; 1M library default | services/ravel-server/src/config.rs:2668-2669; crates/ravel-ingest/src/admission.rs:94-95 | default |
| Distribution gate | 256 MiB or 256 segments | crates/ravel-query/src/distrib/partition.rs:39, 48 | default (measured on loopback only, ADR-0074) |
| S3 prices: PUT/LIST $5.00/M, GET $0.40/M, storage $0.023/GB-mo; LIST page = 1000 keys; DELETE free | pricing assumption stated by charter | n/a | assumed |
| Encoded bytes ~= 0.5 x wire bytes; L0 object ~= 9.6 KB/s per busy buffer x cadence (ADR-0076's measured-load figure) | docs/adrs/0076:19-32 | assumed / documented claim |

## Cost and performance model

All numbers in this section are DERIVED from the defaults above, never measured. The repo's own formula (docs/guides/cost-model.md:13, verified against the shard/flush code paths):

```
ingest PUTs/day      = 2 x tenants x signals x shards x replicas_receiving x (86400 / effective_cadence_s)
admission req/day    = (tenant,signal) pairs x processes x 8640 x (2 + siblings)          [reconcile.rs:284-390]
orphan LISTs/day     = units x 288 ticks x ceil(l0_objects_per_shard_prefix / 1000)
  where l0_objects_per_shard_prefix ~= flushes/day into that prefix x (27/24)              [25h05m horizon + compaction lag]
compaction GETs/day  ~= 1.0-1.2 x L0 objects/day (64 KiB suffix probe covers typical L0 whole)
resolve LISTs/query  = shards x open_hour_buckets(2-3) x pages(1-8)   -- uncacheable by design
cold GETs/query      ~= 2 x flushes_in_unsealed_window (1 commit-record GET + 1 data GET each)
warm GETs/query      ~= 2 x new flushes since last poll (record + segment caches absorb the rest)
per-query budget     = ceil(3600/delay) x 1.5 x shards + 5000 = 15,800 at defaults
```

Strict ack latency (derived + one memory-store measurement): p50 ~= cadence/2 + 2 PUT RTTs; p99 ~= cadence + retry tail. Measured 2,174 ms p99 at 2 s cadence on the memory store (docs/guides/cost-model.md:65); real S3 adds the PUT tail, so ~2.3-2.6 s derived. Write amplification to storage: L0 bytes written once, rewritten once into L1 (single-level, one-shot per bucket, min 2 inputs), so ~2x bytes plus per-object commit overhead. That is genuinely low; the cost problem is object count, not bytes.

### Workload S (Small: one team)

Assumed: 1 tenant, 2 signals (metrics + logs) both continuous strict, 1 `--mode all` process, 4 shards, 50 GB/day wire (~15 GB/day stored), 30 d retention, 20 dashboard panels at 30 s refresh, no alerting fleet, no distributed query.

| Item | Volume/day | $/day |
|---|---|---|
| Ingest PUTs (8 buffers x 43,200 flushes x 2) | 691,200 | 3.46 |
| Query resolve LISTs (57,600 queries x ~24) | ~1.38M | 6.91 |
| Orphan-GC LISTs (8 units x 288 x 49 pages) | ~113k | 0.56 |
| Admission reconcile PUT+LIST (2 pairs x 8,640 x 2) | ~35k | 0.17 |
| Fold + zoned sweep LISTs | ~15k | 0.07 |
| Compaction part/record PUTs | ~3k | 0.02 |
| GETs (compaction ~415k + query ~350k + probes) | ~0.77M | 0.31 |
| **Requests total** | **~3.0M/day, ~0.7M objects created/day** | **~11.5 ($345/mo)** |
| Storage (450 GB) | | $10.35/mo |

Requests are ~97 percent of the bill, matching ADR-0076's small-end claim (docs/adrs/0076:237-240). Note the read side already exceeds the write side: the per-query LIST floor at a 30 s dashboard cadence costs more than ingest.

### Workload M (Medium: 100-tenant production service)

Assumed: 100 tenants (20 heavy: 3 signals continuous strict; 80 light: 2 signals, ~10 s effective cadence), 4 `--mode all` replicas at defaults (no affinity, writes spray all 4), 4 shards, ~1 TB/day wire (~0.5 TB/day stored), 30 d retention, 5 alert rules/tenant at 60 s, 200 dashboard panels at 30 s.

| Item | Volume/day | $/day |
|---|---|---|
| Ingest PUTs (63.6M flushes x 2) | 127.2M | 636 |
| Query resolve LISTs (~1.3M queries x ~30) | ~39M | 195 |
| Orphan-GC LISTs | ~20.7M | 103 |
| Admission reconcile PUT+LIST | 15.2M | 76 |
| Fold/zoned/frontier LISTs, compaction PUTs | ~2M | 10 |
| GETs (compaction 76M + query ~127M + admission 22.8M) | ~226M | 90 |
| **Requests total** | **~430M/day, ~127M objects created/day** | **~1,110 ($33.3k/mo)** |
| Storage (15 TB) | | $345/mo (1%) |

With the shipped levers applied (affinity subset S=2, light tenants resharded to 1, per ADR-0076 decisions 1-2): ingest drops to ~32M PUTs/day and the total to roughly $9-12k/month, consistent with ADR-0076's "roughly an order of magnitude" claim (docs/adrs/0076:231-235). The admission-reconcile, orphan-GC, and per-query LIST floors do not shrink proportionally; they become the dominant residue.

### Workload L (Large: architectural limits)

Assumed: 1,000 tenants (100 heavy 3-signal, 900 light 2-signal), 16 ingest replicas WITH affinity S=2 already applied, light tenants still at 4 shards, 10 TB/day wire (~4 TB/day stored), 30 d retention, 8 query nodes, 5 rules/tenant plus 2,000 panels.

| Item | Volume/day | $/day |
|---|---|---|
| Ingest PUTs (228M flushes x 2) | 456M | 2,281 |
| Query resolve LISTs (~13M queries x ~20) | ~260M | 1,300 |
| Orphan-GC LISTs | ~75M | 377 |
| Admission reconcile PUT+LIST | ~72.6M | 363 |
| GETs (compaction 274M + query ~456M + admission 36M) | ~766M | 306 |
| **Requests total** | **~1.63B/day, ~456M objects created/day** | **~4.6k ($139k/mo)** |
| Storage (120 TB) | | $2,760/mo (2%) |

At true defaults (16-way spray, no affinity) ingest alone is ~3.6B PUTs/day (~$18k/day): the levers are mandatory, not optional, at this scale. Average fleet PUT rate ~5,300/s is spread across tenant/shard prefixes and stays under S3 per-prefix limits; the limit reached first is economic, not technical.

### Sensitivity

- Flush interval: everything on both sides scales ~1/cadence (PUTs, open-hour GETs, per-query budget, ack latency). The lever saturates at ~2.5 s for any strict-serving deployment (config.rs:1023 ceiling); order-of-magnitude further reduction requires buffered mode (40 s idle tier), which changes the ack contract.
- Shard count: linear in ingest PUTs, resolve LISTs, sweep LISTs, and sealed-segment counts; 4 to 1 is 4x on all of them (docs/guides/shard-overrides.md). Floor is 1.
- Tenant count: strictly linear; cross-tenant coalescing is rejected on isolation grounds (docs/adrs/0076:244-271), so there is a hard per-tenant floor: at minimum config (1 shard, S=1, 2.5 s) a strict (tenant, signal) costs ~69k PUTs/day ~= $10.4/month before any data volume; buffered idle cadence lowers that to ~$1.3/month. Free-tier or per-seat SaaS shapes are uneconomic in strict mode.
- Replica count: linear at defaults (buffers are per replica); affinity divides by (replicas/subset). Affinity is documented but not default-on.
- Cardinality: bounded by the 200k active-series cap per (tenant, signal); beyond it workloads must split into more tenants, re-entering the linear tenant term (docs/guides/cost-model.md:97-100).
- Query window: sealed windows are cheap (snapshot + L1 parts, ~4-8 GETs/part); but max_segments = 1024 caps sealed fan-out at ~10.7 days at 4 shards x 1 part/hour before HTTP 422 (crates/ravel-query/src/config.rs:6); raisable by flag.
- Cache hit rate: caches are per process, RAM only (disk tier unwired), so fleet GET volume scales with query-node count, and resolve LISTs are structurally uncacheable (they are the visibility mechanism). The 10,000-record per-tenant record cache (catalog/config.rs:12) is smaller than a busy tenant's unsealed-window record count (~14,400 at 2 replicas x 4 shards x 2 h), so the busiest tenants thrash exactly the cache that matters most.

### Comparison to established systems (derived, not measured)

Mimir/Thanos upload one block per tenant per 2 h from each ingester set: request bills at Workload M are typically tens of dollars/month, with cost shifted into stateful compute (ingesters, store-gateways, compactors, local NVMe). Loki flushes a chunk per stream at ~1 MB or idle timeout: a 1 TB/day deployment runs a few million PUTs/day, order $500-1,500/month. Ravel at Workload M is ~$33k default / ~$10k tuned, i.e. roughly 10-30x Loki and >100x Mimir on the S3 bill, partially offset by genuinely stateless, small compute (no WAL disks, no replay, tested crash-anywhere model). The trade is explicit and honest in-repo; whether it wins depends on how much the buyer values disposable compute over request fees.

## Benchmark credibility

Finding first: there is no committed, credible end-to-end performance envelope against real object storage anywhere in this tree, and the repo is unusually honest about that; every committed number is a memory-store or loopback-MinIO component measurement, clearly labeled, and the ClickBench guide explicitly refuses to publish numbers ("never reports a number: it is the procedure", docs/guides/clickbench.md:8; "figures do not reproduce across hosts", :25).

Inventory of every benchmark claim found:

| Claim | Where | Class | Credibility |
|---|---|---|---|
| Flush-cadence change: 200 to 60 flushes/30 s, ack p99 616.7 ms to 2,174.4 ms, write amp 3.44x to 2.16x | docs/guides/cost-model.md:61-66 | component, synthetic, memory store | VERIFIED as a request-count property (store-independent by design); latency figures memory-store only |
| ~45-55M PUTs/day, $7-8k/mo at 100 tenants / 1 TB/day | docs/adrs/0076:7-10 | analytical model (independent review) | DOCUMENTED CLAIM, explicitly modeled; my own model is same order |
| Ingest CPU profile: ~56% memory movement, 99.4% in write path, 4M points accepted in 1 s on 4-core aarch64 | docs/ingest.md:649-699 | component, synthetic, memory store, 648 samples | IMPLEMENTED WEAKLY VERIFIED; self-caveated (host-bounded, on-CPU only) |
| 38.4 MB charged / 69.3 MB resident at 50k series; logs ratio unmeasured | docs/ingest.md:557-572 | component, synthetic | DOCUMENTED CLAIM with honest gap statement |
| Per-request latency ~1-5 ms loopback, ~15-80 ms "projected" real S3; sparse-probe crossover NOT metered | crates/ravel-query/src/fetcher.rs:100-110; docs/query-engine.md:139-144 | microbenchmark + projection | DOCUMENTED CLAIM; code says the governing crossover was not measured |
| Distribution thresholds 256 MiB / 256 segments from measured crossover | docs/adrs/0074:123-156; partition.rs:39,48 | component, synthetic, loopback MinIO on one CI host | IMPLEMENTED WEAKLY VERIFIED; cross-host crossover explicitly unmeasured (adr:153-156) |
| Criterion micro benches (segment encode, series-id hash, logseg encode/scan, otap decode, sql scans, kway merge) | crates/*/benches/ | microbenchmark | exist, no committed results |
| Bench bins: ingest_bench, query_latency_bench, sql_latency_bench (cold/warm), s3_e2e_bench, compaction_bench, catalog_resolve_bench, parquet_baseline, read/selective read accounting | crates/ravel-bench/src/bin/ | e2e-in-process harnesses | harness quality is high (accounting invariants are themselves tested, e.g. crates/ravel-bench/tests/sql_latency_object_count_invariant.rs); no committed results |
| Real-dollar S3 cost benchmark | tracked as issue #79, not done | n/a | NOT IMPLEMENTED (docs/guides/cost-model.md:77-81) |

Missing dimensions (all UNKNOWN): ingest p50/p99 and strict-ack latency on real S3; sustained throughput ceiling per process on S3; query p50/p95/p99 for any corpus on S3; cold vs warm on S3; high-cardinality reads; large-range reads; compaction under ingest load; S3 throttling (503 SlowDown) behavior under load; pod restart under load; CPU/RSS under production shapes; measured S3 request counts vs this model; cost per TB and per query; backlog recovery time after a maintain outage. The 100-seed simulation harness (ADR-0068) validates correctness, not performance. A project with this correctness posture but no measured envelope should not receive full production-readiness marks on performance; the honest statement is "requests-dominated cost is understood and modeled in-repo; latency and throughput above the component level are unmeasured."

## Failure scenarios (economic and scale)

1. Cost runaway on replica scale-out (economic). Buffers are per replica; at defaults (no affinity) every ingest replica added multiplies every active tenant's PUT bill with no throughput benefit per tenant. Blast radius: the whole bill; probability: high in any autoscaled default deployment. Workaround: ADR-0080 affinity (S=2). Fix: make subset routing the default. Proof test: two-replica vs four-replica ingest of identical load asserting flush-PUT counts within 10 percent.
2. Maintenance lag couples to read availability (scale). If compaction stalls k hours, unsealed L0 hours accumulate at up to 3,600 requests per shard-hour (records + data at 2 s cadence, 2 replicas); the derived per-query budget (15,800 at defaults) covers only ~2 busy shard-open-hours x 4 shards; a 4 h stall makes every cold query on a busy tenant 422 (RequestBudgetExceeded) until compaction catches up, and cold latency at 16 concurrent GETs x 15-80 ms is tens of seconds against the 30 s deadline. Preconditions: busy tenant, cold caches, maintain outage or contention. Workaround: raise --max-s3-requests, warm caches. Fix: scale the budget by observed unsealed-hour count, or admit sealed L1 early. Proof: e2e with maintain paused 4 h at 2 s cadence asserting queries still answer.
3. Per-query LIST floor under alerting (economic). Every rule evaluation resolves independently; resolve LISTs are uncacheable by design. 1,000 tenants x 5 rules at 60 s ~= $1,300/day at Large. Fix: share one resolve per (tenant, signal) per tick across that tenant's rules. Proof: request-count assertion in the alerting e2e.
4. Background fabric floor absent from the cost guide (economic, doc gap). Admission reconciliation (2 requests + sibling GETs per tenant-signal-process per 10 s, reconcile.rs:284-390) and orphan-GC whole-shard LISTs (sweep.rs:93-95) together are ~$180/day at Medium, ~$740/day at Large; cost-model.md's formula covers flush PUTs only. Workaround: raise R; nothing tunes orphan LIST frequency short of the 6 h interior cadence. Fix: hour-bucketed l0 keys (format ADR) or a per-tick LIST budget; document both streams in cost-model.md.
5. Strict-mode floor blocks the last lever (economic). MAX_STRICT_VISIBILITY_BUDGET_NS = 3 s means cadence cannot exceed ~2.5 s for strict tenants; the ~$10/month per (tenant, signal) minimum makes many-tenant strict SaaS shapes permanently request-bound. Workaround: buffered mode (documented crash-loss window). This is a designed trade, stated here as the economic boundary.
6. Long-range queries 422 at defaults (scale, P2/P3). max_segments = 1024 vs shards x hours L1 parts caps sealed windows at ~10.7 days (4 shards); raisable by --max-segments (config.rs:468), but the default surprises month-window dashboards.

## Tests or commands run

Heavy validation (workspace clippy, ~5,400 tests, doctests, 100-seed simulation) pre-ran green on this host per panel charter; I ran no builds or test sweeps. My commands were read-only inspection, all exit 0: `ls`/`find` over crates, services, docs, benches; `Read` of crates/ravel-ingest/src/config.rs, crates/ravel-catalog/src/config.rs, crates/ravel-maintain/src/config.rs, crates/ravel-query/src/config.rs, crates/ravel-query/src/fetcher.rs (lines 61-130), crates/ravel-maintain/src/sweep.rs (60-240), crates/ravel-ingest/src/reconcile.rs (280-390), services/ravel-server/src/config.rs (40-420 and targeted), services/ravel-server/src/fold.rs (230-347), services/ravel-server/src/admission_reconcile.rs, crates/ravel-catalog/src/fold.rs (790-1030), docs/architecture.md, docs/catalog-and-mvcc.md, docs/query-engine.md (100-330, 746-856), docs/ingest.md (620-699), docs/guides/cost-model.md, docs/adrs/0076, docs/README.md; `grep -n` for every default cited above (decisive outputs quoted in the parameter table's file:line column). No benchmark was executed; every workload number above is derived, and labeled so.

## Unknowns

- Real-S3 latency distributions for PUT, GET, LIST under Ravel's concurrency limits; every latency figure above is derived or loopback.
- Actual encoded bytes per sample/row for production-shaped data (codec benches exist, no committed results); my storage figures assume 2:1 versus wire.
- Multipart PUT part-count for 256 MiB L1 parts (affects compaction PUT count modestly; not located in this pass).
- Whether fold's classify GETs are fully absorbed by the shared record cache under concurrent resolve load.
- Real workload mix (strict vs buffered, batch cadence); my heavy/light splits are assumptions and the totals scale linearly in them.
- S3 throttling behavior (per-prefix 503s) under the orphan-GC LIST bursts; untested anywhere in-repo.

## Severity-ranked findings

- P1. Request-dominated economics at defaults with a structural strict-mode floor: defaults yield ~$33k/mo at a 100-tenant/1 TB/day service (requests ~99 percent of bill); levers reduce ~3x but the 3 s visibility ceiling (config.rs:1023) plus rejected cross-tenant coalescing (ADR-0076) leave a ~$10/mo-per-strict-(tenant,signal) floor. Blocks many-tenant strict-mode SaaS economics. Evidence: model above; ADR-0076's own numbers agree. Fix: buffered-mode default for low-value signals plus commit-record batching (deferred in ADR-0076); proof: issue #79's real-dollar bench.
- P1 (conditional). Maintenance lag turns into read unavailability on busy tenants: derived budget covers ~2 busy open shard-hours; a stalled compactor makes cold queries 422 fleet-wide for that tenant. Scenario, preconditions, fix, and proof test in Failure scenarios 2.
- P2. No measured performance envelope against real S3 (ingest p99, query percentiles, throttling, restart, backlog recovery); production readiness on performance is asserted only by architecture, not evidence. Benchmarks section above.
- P2. Background request fabric (admission reconcile 10 s cadence; orphan-GC whole-shard LIST per 300 s tick) is a growing cost floor excluded from the shipped cost formula; ~$180/day at Medium. Evidence: reconcile.rs:91, 284-390; sweep.rs:93-95; cost-model.md:13 omits both.
- P2. Uncacheable per-resolve LIST floor makes dashboards and alert rules the dominant cost at small scale and ~$1.3k/day at Large; no resolve sharing across a tenant's rules.
- P2. Read caches are per-process RAM only: disk tier unwired (`--cache-dir` fails startup, config.rs:616-626; caching.md:136-141), spans uncached, record cache (10k/tenant) undersized versus a busy tenant's unsealed window; fleet GET volume scales with query-node count.
- P3. max_segments = 1024 caps sealed query windows at ~10.7 days at default shards before 422; raisable but undocumented as a window bound.
- P3. Doc drift: docs/query-engine.md:217 still states the superseded flat 25,000 request budget (ADR-0075 replaced it with the derivation, ravel-query/src/config.rs:63-76); library reference cadence 500 ms (config.rs:44) no longer matches the shipped 2 s server default. Reported, not fixed, per repo rules.
- Credit where due: two-object commit overhead is the honest price of its crash matrix; L0-to-L1 write amplification ~2x is excellent; per-query request/byte accounting is wired end to end and itself tested; ADR-0076 is a model of candid internal cost analysis; refusing to commit non-reproducible benchmark numbers is the right discipline, it just leaves the envelope unmeasured.

## Confidence

High on the write-path request model (formula verified in code and corroborated by ADR-0076's independent numbers), on every configuration default cited (each read directly at file:line on this tree), and on the structural findings (strict ceiling, orphan LIST shape, admission cadence, uncached LIST floor). Medium on the read-path and background absolute dollar figures: they depend on assumed query/alert mixes and tenant activity splits, stated inline, and scale linearly in them. Low on all latency figures and on the competitor comparison magnitudes: no measurement was possible in scope, and the repo itself contains none against real S3; these are labeled derived throughout.
