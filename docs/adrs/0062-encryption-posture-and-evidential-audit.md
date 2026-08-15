# ADR-0062: encryption posture and evidential audit

Status: Accepted

## Context

### Encryption: what ADR-0042 promises vs what runs

ADR-0042 decision 1 promises per-tenant SSE-KMS: "`S3Config` gains an optional `kms_key_id` ... per-tenant, sourced the same way tenant tokens are configured today," with BYOK meaning "the tenant supplies their own `kms_key_id`."

What shipped is the field, not the capability. `S3Config.kms_key_id` exists and the builder honors it (`crates/ravel-object-store/src/s3.rs:142-191`, with builder-divergence tests), but:

- Every construction site passes `None`: `services/ravel-server/src/store.rs` (both the flag path and the env path), `services/ravel-cli/src/store.rs:88`, and every bench/test harness. No CLI flag or env var sets it; the config is dead in the running binary.
- More fundamentally, the field is on the wrong axis. One `S3Store` is built at startup, wrapped once in `InstrumentedStore`, and the single `Arc<dyn ObjectStoreBackend>` is shared by every route and background task (this is ADR-0055's documented starting point). `kms_key_id` is a property of that one shared builder, so even if wired it would be one deployment-wide key. "Per-tenant" is unreachable by design against the current shape — the per-tenant-KMS gap.

One assessment rated the implement path "nearly as hard to retrofit as the tenant-hash change ... architecturally unreachable against the single shared store builder and needs per-tenant store handles plus key-epoch metadata that have no design". That cost estimate assumes threading per-tenant store handles through every call site. Reading the code, there is a materially cheaper implementation, detailed under Decision 1: every tenant-scoped object key already begins with `t/<tenant_hash>/`, `ObjectStoreBackend` methods all take the key, and SSE-KMS decryption on GET is transparent (S3 records the key in object metadata; the reader needs `kms:Decrypt` permission, not client-side key selection). Key selection is therefore decidable per-operation from the key alone, inside a single decorator, with no trait change and no call-site change.

Why this cannot be deferred: data objects are immutable, so every object written before per-tenant keys exist is under the shared/default key forever. Cryptographic erasure and BYOK custody then only cover data written after the switch; making them cover history is a full-fleet rewrite. This is the same "cheap at zero volume, impossible at 100 PB" class as the unkeyed tenant hash (these "become full-fleet re-key/rewrite migrations at 100 PB; per-tenant SSE-KMS is nearly as hard"). The window is open now and closes with volume. The directive is to decide, not defer.

### Audit: what exists today

The audit trail is ADR-0042 decision 4, built as:

- `ravel_maintain::write_query_audit` (`crates/ravel-maintain/src/query_audit.rs`): one immutable RLOG object plus one commit record per query, on `Signal::Audit` shard `QUERY_AUDIT_SHARD=1` (legal hold is shard 0), written data-object-first then commit record — the ingest durability order (`crates/ravel-maintain/src/audit_write.rs`).
- Record fields: `kind=query`, `query.language`, `query.tenant` (hex hash), `query.status`, `query.window_start_ns`/`end_ns`, and `query.text` **verbatim, untruncated** (query_audit.rs:50-52, 162).
- Call sites: `POST /api/v1/sql` (`services/ravel-server/src/sql.rs:182-260`) and Flight SQL (`crates/ravel-sql/src/flight/service.rs:157`). Both are fire-and-log: on write failure they `tracing::error!` and return the query result anyway (sql.rs:226-259 documents this as deliberate).

The five gaps, against that code:

- **Coverage.** The PromQL surface is entirely unaudited: `/api/v1/query`, `/query_range`, `/labels`, `/label/{name}/values`, `/series` (`crates/ravel-query/src/http/mod.rs:71-88`), plus `/api/v1/query_exemplars` and `/api/v1/analytics` in `ravel-server`. Flight SQL audit already landed; the remaining gap is PromQL (and the ravel-server-side read endpoints). The Flight audit's status can also read `Ok` for a stream that fails after its first batch, because the record is written at stream construction, not completion.
- **Lossiness.** Best-effort by construction: an S3 outage drops audit records while queries still return (they may be served from the ADR-0046 read cache), which voids the trail as evidence precisely during the incident window. There is no fail-closed audit mode.
- **Unbounded keyspace.** `Signal::Audit` is excluded from `MAINTAINED_SIGNALS` (`services/ravel-server/src/maintain.rs:88`), so nothing compacts, retains, or sweeps it. Two PUTs per query, forever: ~260M immortal objects/month at 100 queries/s, with query PUT spend doubled per query served.
- **PII.** `query.text` verbatim means label-matcher values and SQL string literals — the classic homes of user identifiers, emails, IPs — persist in a keyspace outside every retention control (and, per the unbounded-keyspace gap, outside deletion entirely).
- A structural conflict this ADR must resolve: ADR-0055 §3 denies `s3:DeleteObject` on `t/<hash>/u/**` (the whole audit prefix) for **every** role, including Maintain. Bounding the audit keyspace requires deleting expired audit objects. The deny-delete boundary and the retention requirement cannot both hold over the same prefix; ADR-0055 needs a scoped amendment (Decision 2d).

### Adjacent, reusable machinery

- **ADR-0055 (landed)** scopes credentials per role at the IAM layer with zero trait changes. It gives per-tenant KMS two things: (a) the precedent that storage-security posture belongs at the backend's native layer, not as an in-process check; (b) the Query/Maintain/Gateway role policies onto which per-key `kms:Decrypt`/`kms:GenerateDataKey` grants attach naturally. It does **not** provide per-tenant identity resolution at the store layer — roles are per-process, not per-tenant — so a tenant-scoped KMS key cannot piggyback on a per-role credential. What it can piggyback on is the key prefix, which is already the tenant boundary IAM policies use.
- **ADR-0044/0061 accounting** (`crates/ravel-types/src/accounting.rs`) provides the exact injection-seam precedent for audit coverage: every PromQL/SQL/Flight handler already holds an injected `Arc<dyn QueryCostRecorder>` called at end-of-request — but audit needs an async, awaited variant since durability must gate the response. `ravel-query` and `ravel-sql` already depend on `ravel-maintain`, so the sink trait lives there with no new dependency edges.
- **Bounded-keyspace precedent**: every other per-request keyspace in the system is bounded by the same two mechanisms — compaction (RLOG L0 -> L1) and horizon-gated retention sweep — driven off `MAINTAINED_SIGNALS`. The audit shard was excluded deliberately (query_audit.rs module doc) because legal-hold records share `Signal::Audit`; the shard split (hold=0, query=1) that made exclusion safe also makes *selective inclusion* safe.

## Decision

### 1. Encryption: implement per-tenant SSE-KMS, via key-prefix routing

**Implement, not amend-and-retreat.** Per-tenant SSE-KMS with key-epoch metadata ships now, while data volume is ~zero, using a mechanism that is an order of magnitude cheaper than the assumed expensive design:

**1a. `KmsRoutingStore` decorator** in `crates/ravel-object-store`: an `ObjectStoreBackend` implementation wrapping the existing builder machinery. On every write operation (put, multipart) it parses the leading `t/<tenant_hash>/` from the key; if that tenant has a configured KMS key, the write is delegated to a lazily-built, cached per-tenant `S3Store` whose builder was given that tenant's `kms_key_id` (the s3.rs:142-191 plumbing that exists today, finally with a caller). All other operations — every read, list, head, delete, and any key outside `t/` (`sys/*`, `maint/*`) — delegate to the default store: SSE-KMS decryption is server-side and transparent given `kms:Decrypt`, so reads never need key selection. No `ObjectStoreBackend` trait change, no call-site change anywhere in `ravel-ingest`, `ravel-maintain`, `ravel-catalog`, `ravel-query`, or the services; the decorator composes under `InstrumentedStore` exactly where `S3Store` sits today. This is the same "one seam, zero threading" shape ADR-0055 used to avoid the in-process-authorization retrofit, applied to key selection.

**1b. Key configuration and key-epoch record.** Per-tenant keys are operator configuration (`--tenant-kms-key <tenant>=<key-arn>`, repeatable, or the config-file equivalent — the same sourcing style as tenant tokens, as ADR-0042 promised). The first time a tenant's key is configured or changed, the server records it in a new per-tenant CAS object `t/<hash>/enc` (`CasVersion`, the `prov`/`sys/gc` pattern): an append-within-record list of epochs, each `{epoch, key_arn (or "default"), activated_ns}`. The epoch record is what makes rotation and erasure auditable: every object's write time locates it in exactly one epoch, so "which key encrypts what" is answerable from the bucket, and `verify-custody` gains an epoch-consistency check. This is an additive new object key; this ADR is the required ADR for it, and no existing layout changes (no version bump).

**1c. Single-key posture becomes real, and remains the default.** The deployment-wide `--s3-kms-key` flag (and `RAVEL_S3_KMS_KEY`) finally wires the existing `S3Config.kms_key_id` for the default store, so the shared-key posture stops being fictional too. A tenant with no per-tenant key gets the deployment default (shared key, or bucket-default SSE) — ADR-0042's "unchanged behavior" clause, now truthfully documented.

**1d. BYOK and revocation semantics, stated honestly.** BYOK = the tenant's key ARN is configured and the tenant's own KMS key policy grants Ravel's role(s) usage. Revocation makes every object under that key's epochs unreadable — cryptographic erasure — and simultaneously makes that tenant's ingest/compaction/query fail with `AccessDenied`. The server treats per-tenant KMS denial as that tenant's outage, not a process fault: fail closed for the tenant, unaffected for everyone else. Compaction re-encrypts under the current epoch as a side effect of rewriting L0 into L1, so rotation converges over time for compacted data; L0 written under old epochs and never compacted retains the old epoch until retention removes it (recorded, not hidden).

**1e. ADR-0042 amendment.** ADR-0042 decision 1 is amended to describe this mechanism (prefix-routed per-tenant stores + epoch record + default-key fallback) instead of the per-tenant-`S3Config` framing that shipped as dead config. The amendment states plainly what was true until this work landed: the server used a single implicit posture with `kms_key_id` unset.

Named non-goals (unchanged gaps, referenced not re-litigated): the ADR-0046 local read cache holds cross-tenant plaintext on disk (separate concern, separate work); true per-object Object Lock (ADR-0042 decision 3 / ADR-0055 §3 informational probe).

### 2. Audit: one evidential pipeline for every query surface

**2a. Coverage: every read surface, one seam.** A new async sink trait (`ravel-maintain`, e.g. `QueryAuditSink`) mirroring the `QueryCostRecorder` seam, held as `Arc<dyn QueryAuditSink>` by each handler's state. Every surface that executes a resolved tenant's read submits one `AuditEvent` and **awaits durability before returning the response**: PromQL `query`, `query_range`, `labels`, `label_values`, `series`; `query_exemplars`; `analytics`; SQL HTTP (migrated from its direct `write_query_audit` call); Flight SQL — moved from stream construction to stream completion, so the recorded status is the stream's final outcome, closing the status-accuracy gap as part of this redesign. Events carry `query.language` values distinguishing the surface (`sql`, `promql`, `labels`, `series`, `exemplars`, `analytics`) so the record shape stays one schema. Requests rejected before tenant resolution remain unaudited (unchanged: nothing to attribute).

**2b. Non-lossy via group commit, not local durability.** A single `AuditPipeline` in `ravel-server` batches submitted events and flushes on `max_batch` records or `max_age` (default 25 ms, configurable): one RLOG object containing the batch's records plus one commit record, written in the existing `audit_write.rs` durability order. Every submitter awaits its batch's flush result; the response is released only after the audit record is durable in object storage. This respects the invariant directly — the only buffer is in memory, and nothing in it is ever acknowledged: a crash before flush destroys the buffered records *and* the un-responded queries together, so no acknowledged query ever lacks a durable audit record ("non-lossy" is defined as exactly that property, and it is the strongest property any system can offer without lying — records for responses never sent are not evidence of anything).

Failure semantics: if the flush fails, every query in the batch fails (HTTP 503 / Flight `Unavailable`), `audit_mode=required` being the default. During an S3 outage queries fail closed instead of running unaudited — the precise inversion of the lossiness gap. `audit_mode=best-effort` remains available as an explicit, documented opt-out (dev, single-tenant labs); choosing it is visible configuration, per "approximation is opt-in and visible."

Cost and latency: the response tail gains up to `max_age` plus one dual-PUT round trip (S3 PUT p50 ~10-30 ms). PUT count drops from 2/query to 2/flush — at 100 queries/s and 25 ms batching, from 200 PUTs/s to <=80/s worst case and far fewer under load, directly shrinking the keyspace growth rate before retention even runs.

**2c. Bounded keyspace: maintain the query-audit shard.** `Signal::Audit` shard `QUERY_AUDIT_SHARD=1` joins the maintained set as a fourth maintenance target with its own policy knob: RLOG compaction (existing machinery, new signal/shard parameter) and a dedicated `audit_retention` window (default 90 d, configurable; deployments with regulatory retention set it to their obligation). Sweep remains horizon-gated and passes through `LegalHoldCheck`, so a placed legal hold protects audit evidence exactly as it protects data. Legal-hold shard 0 stays excluded from maintenance (its growth is per operator action, not per query) and stays deny-delete forever.

**2d. ADR-0055 amendment (required by 2c).** The deny-delete prefix `t/<hash>/u/**` narrows to the legal-hold shard's prefix. Real audit object keys carry their shard as a four-digit segment (`t/<hash>/u/<l0|c|l1>/<shard:04>/…`), so in prefix terms that narrowing is `t/<hash>/u/*/0000/**`, not `t/<hash>/u/0/**` — see the amendment below, which corrects this section's original text. Maintain's delete grant gains the query-audit shard's objects, which the retention sweep — the same code path, same role, same horizon gating as every other signal — now legitimately deletes. No other role gains anything; Query's grant remains append-only `Put` on the query-audit shard. The amendment is narrow and mechanical, and this ADR records it rather than leaving ADR-0055 contradicted in place.

**2e. PII policy: keyed tokenization, plaintext by explicit opt-in.**

- Fields defined as non-PII and kept as-is: tenant hash (pseudonymous by construction, already the only identity the ADR-0044 span policy allows), language/surface, status, window bounds, timestamps.
- The PII carrier is query text. Default posture: the audit record stores a **structure-preserving redacted form** — the parsed query (PromQL AST / SQL AST; both parsers already exist on these paths) re-rendered with every literal and label-matcher value replaced by a deterministic token `blake3::keyed_hash(audit_token_key, value)` (truncated, prefixed, e.g. `⟨t:9f3a2c…⟩`). Selector names, label *names*, operators, functions, and structure remain readable, so the record still shows exactly what shape of query ran, over which time range, by which tenant.
- The token key (`RAVEL_AUDIT_TOKEN_KEY`) is deployment-held, outside the bucket. Keyed, not plain, hashing for the same reason the tenant hash is moving to keyed: label values are low-entropy (emails, user IDs) and an unkeyed digest is a dictionary attack away from plaintext. Determinism preserves evidential value: equal values tokenize equally across records, so an investigator can trace "the same value was queried by these requests across this window," and one who *holds the key* can compute the token of a known suspect value for targeted confirmation — without the key, bulk recovery is not possible.
- `audit_text=plaintext` is an explicit opt-in flag for deployments whose compliance regime requires verbatim text; it is documented as storing PII under the audit retention window (which, after 2c, actually exists — today's verbatim text is retained forever, which is the PII gap).
- Redaction happens at the surface, on the parsed structure, before the event is submitted — never by regex over raw text, and the raw text never leaves the handler.

Gaps closed: per-tenant KMS by 1a-1e; coverage by 2a; lossiness by 2b; unbounded keyspace by 2b+2c+2d; PII by 2e. Plus the Flight status-accuracy gap by 2a's stream-completion audit.

## Rejected alternatives

**Encryption**

- **Amend ADR-0042 to declare single-shared-key as the deliberate posture (the "honest retraction").** Rejected. It is the cheaper fallback, but it spends the only cheap window: objects are immutable, so the day per-tenant keys are wanted, every byte written since is a rewrite-to-re-encrypt migration — the "hardest to fix later" class, and these "get strictly more expensive with volume." The retraction would also permanently delete BYOK/cryptographic-erasure from the compliance story ADR-0042 was written to provide, while the actual observed retrofit cost (one decorator in one crate, config, an additive CAS record, docs) is a fraction of what the implement path was priced at — that estimate assumed per-tenant store handles threaded through every call site, which the key-prefix already makes unnecessary. When the honest-amendment path and the implement path are this close in cost, the directive ("decide deliberately, not defer") resolves toward the one that keeps the capability.
- **Per-tenant store handles threaded through call sites** (the assumed expensive design). Rejected as the expensive strawman: it re-runs exactly the retrofit ADR-0055 already rejected for authorization ("threading a caller-identity value through every call site"), for a selection decision the object key alone already determines.
- **Ravel-managed envelope encryption.** Already rejected by ADR-0042 (massive key-management scope SSE-KMS provides at the storage layer); nothing here changes that reasoning. Reaffirmed.
- **Defer the decision.** Forbidden by the directive, and wrong on the merits: deferral is the amend-only outcome without the honesty.

**Audit**

- **Local durable spool (disk WAL) with retry after outage.** Rejected outright: durability depending on local disk violates a CLAUDE.md invariant that is never a valid trade-off, and a destroyed pod loses the spool — it converts "records dropped during the outage" into "records dropped if the pod dies during the outage," which is not non-lossy, just less often lossy.
- **Synchronous per-query dual-PUT (no batching), fail-closed.** Gets the non-lossy property but keeps the exact 2-PUTs-per-query growth and doubled PUT spend (~260M objects/month at 100 q/s). Group commit gives the same guarantee with the PUT rate decoupled from query rate; per-query sync writes survive only as the degenerate `max_batch=1` configuration.
- **Route audit records through the ingest pipeline as ordinary logs.** Tempting reuse of batching/durability machinery, rejected: audit must be server-written and isolated from tenant admission/quota (a tenant hitting its ingest quota must not suppress its own audit trail); `Mode::Query` processes run no ingest routers at all (`lib.rs` builds them only for Gateway/All), and ADR-0055 grants Query append-only audit writes, not ingest writes.
- **Bound read cost only (rollup/index) without deletion.** Rejected: an aggregation layer over an immortal keyspace bounds queries over the records, not the keyspace; the keyspace gap is about unbounded object count and spend.
- **Drop query text entirely, or hash the whole text as one opaque digest.** Rejected: destroys the trail's evidential value (which query shape, which ranges, which values correlate) — the bar is that the trail be usable as evidence. Whole-text hashing also breaks on trivial formatting differences. Keyed per-value tokenization preserves structure and correlation with the PII removed.
- **Unkeyed hashing of values.** Rejected for the same reason the unkeyed tenant hash is being retired: low-entropy identifiers fall to offline dictionary attacks.

## Consequences

- **Two ADR amendments ride along**: ADR-0042 decision 1 (mechanism rewritten to match reality, historical posture stated plainly) and ADR-0055 §1/§3 (deny-delete narrowed to the legal-hold shard; Maintain gains the query-audit-shard delete used by retention). Both are in-place amendments with a dated note, the established pattern in ADR-0055 itself.
- **New object key `t/<hash>/enc`** (CAS, additive; this ADR authorizes it — no existing layout changes, no version bumps). New config surface: `--s3-kms-key`, `--tenant-kms-key`, `--audit-mode`, `--audit-text`, audit `max_batch`/`max_age`, `audit_retention`, `RAVEL_AUDIT_TOKEN_KEY`.
- **`RAVEL_AUDIT_TOKEN_KEY` and per-tenant key ARNs are out-of-bucket durability-adjacent state**, same class as the deployment-keyed tenant hash key: losing the token key loses targeted-confirmation ability (records remain valid); losing KMS key config is recoverable from the `enc` records.
- **Query tail latency gains the audit flush** (<= `max_age` + one dual-PUT RTT) on every audited surface, in `required` mode. Dashboards polling PromQL pay it too; `max_age` tuning and the documented best-effort opt-out are the relief valves. Before/after numbers are recorded when the implementation lands.
- **Fail-closed coupling**: in `required` mode an S3 outage now fails queries that the read cache could have served. This is the deliberate trade — the exact complaint was that queries outlive the trail.
- **KMS cost**: SSE-KMS adds KMS requests per PUT for keyed tenants; S3 Bucket Keys (bucket-side configuration, documented in operations.md) reduce this. MinIO KMS (KES) compatibility gets a docs note; the decorator itself is backend-agnostic since it only builds more `S3Store`s.
- **IAM/role interaction**: per-tenant BYOK key policies must grant the ADR-0055 role principals (Gateway/Query/Maintain) usage; operations.md's policy templates gain the KMS statements. Revoked key = that tenant fails closed, others unaffected (new failure-path tests with `FaultStore` asserting the injected fault fired).
- **verify-custody** gains key-epoch consistency checking; audit records for admin actions (key configured/rotated) are written like every other admin audit record.
- **The legal-hold shard remains unbounded** — deliberately: growth is per operator action, and its deny-delete protection is load-bearing. Recorded so a successor review doesn't re-flag it as an oversight.
- **README, docs/guides/operations.md, docs/catalog-and-mvcc.md (new `enc` key), and the query-audit module docs** update in the same commits as the behavior, per doc-currency rule.

## Amendment: correct section 2d's audit-prefix transcription

ADR-0072 found that section 2d wrote the legal-hold shard's
deny-delete prefix as `t/<hash>/u/0/**`. That form is wrong: the shard is
not a bare path segment, it is a four-digit zero-padded segment nested
under the signal directory, exactly as `docs/catalog-and-mvcc.md` and
`crates/ravel-commit/src/keys.rs` (`data_key`, `commit_key`, and every
sibling builder) define for every signal. A real legal-hold audit key
looks like `t/<hash>/u/l0/0000/<writer_id>.<epoch>.<seq>.<hash16>.rseg` or
`t/<hash>/u/c/0000/<ingest_hour>/<writer_id>.<epoch>.<seq>.cmt`, never
`t/<hash>/u/0/...`. The correct prefix, in wildcard terms, is
`t/*/u/*/0000/*`.

This was a transcription error in this ADR's prose only: ADR-0055's own
amendment (the one that actually specifies the deny-delete and
`MaintainDelete` wildcards) already used the correct `t/*/u/*/0000/*` /
`t/*/u/*/0001/*` forms, and `docs/guides/operations.md`'s shipped IAM
policies were never generated from this ADR's wrong text, so no policy
ever carried the error. The risk was transcription forward, not backward:
anyone implementing an IAM prefix directly from this ADR's section 2d
would have written a pattern matching no real key. Section 2d above is
corrected in place; `crates/ravel-commit/tests/iam_templates.rs` (added by
ADR-0072) now checks every shipped policy prefix against real key shapes
built by `ravel-commit`'s own constructors, so a future recurrence of this
mismatch fails CI instead of shipping silently.
