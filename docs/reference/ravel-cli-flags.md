# ravel-cli command-line flags

Every flag `ravel-cli` accepts, organised by subcommand. The global flags come
first, then one table per subcommand, each with its environment variable,
default, and the first line of its help. The tables are generated from the
binary's clap definition by walking its command tree.

This page is generated. Do not edit the tables by hand: regenerate them with

```sh
RAVEL_UPDATE_CLI_REFERENCE=1 cargo test -p ravel-cli
```

<!-- BEGIN GENERATED FLAGS -->
## Global flags

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--s3-access-key` | `RAVEL_S3_ACCESS_KEY` |  |  |
| `--s3-auth` | `RAVEL_S3_AUTH` | `static` | Where `--store s3` gets its credentials (ADR-0106). `static` (the default) is unchanged behavior: `--s3-access-key` and `--s3-secret-key` are both required. `instance-role` drops that requirement and fetches short-lived credentials from the EC2 instance metadata service instead; combining it with any inline credential flag is refused rather than resolved by precedence |
| `--s3-bucket` | `RAVEL_S3_BUCKET` |  |  |
| `--s3-credentials-file` | `RAVEL_S3_CREDENTIALS_FILE` |  | Path to a JSON file of `{access_key_id, secret_access_key, session_token}` that an external process rotates on disk (ADR-0072 decision 1). Read once at construction (an unreadable or malformed file is an error) and re-read lazily on the request path when its mtime changes. Wins over the inline key flags. Only meaningful under `--s3-auth static` |
| `--s3-endpoint` | `RAVEL_S3_ENDPOINT` |  |  |
| `--s3-instance-metadata-endpoint` | `RAVEL_S3_INSTANCE_METADATA_ENDPOINT` |  | Base URL of the EC2 instance metadata service, used only under `--s3-auth instance-role` (ADR-0106). Unset uses the AWS link-local address; a value redirects IMDS for tests and unusual deployments |
| `--s3-region` | `RAVEL_S3_REGION` |  |  |
| `--s3-secret-key` | `RAVEL_S3_SECRET_KEY` |  |  |
| `--s3-session-token` | `RAVEL_S3_SESSION_TOKEN` |  | Temporary AWS session token paired with `--s3-access-key` / `--s3-secret-key` for STS-issued credentials (ADR-0072 decision 1). Ignored when `--s3-credentials-file` is set: the file wins. Only meaningful under `--s3-auth static` |
| `--store` |  |  | Which object store to run against. Unset means `memory`, the empty in-process store: a walk-shaped command over tenant data then reports `store: memory (default)` in its header, and refuses a walk that reaches no data at all rather than reporting zero counters at exit 0. An explicit `--store memory` keeps that zero-count report |
| `--tenant-hash-key-file` |  |  | Path to the bucket's 32-byte deployment key (64 hex characters or 32 raw bytes), needed to address a v2-keyed bucket's tenant prefixes |
| `--tenant-hash-unkeyed` |  |  | Assert the bucket is v1-unkeyed. An unkeyed or absent marker resolves to v1 without this, but it makes the expectation explicit; mutually exclusive with --tenant-hash-key-file |

## segment

Inspect an RSEG segment (trailer, footer, sections, series count)

_No flags._

### segment inspect

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `<PATH>` |  |  | Local file path or object store key |

## rlog

Inspect an RLOG log segment (footer, sections, skip index, directories)

_No flags._

### rlog inspect

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `<PATH>` |  |  | Local file path or object store key |

## rspan

Inspect an RSPAN span segment (footer, sections, skip index)

_No flags._

### rspan inspect

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `<PATH>` |  |  | Local file path or object store key |

## commit

Fetch and decode a commit record

_No flags._

### commit decode

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `<KEY>` |  |  | Local file path or object store key |

### commit decode-compaction

Decode and print a CompactionRecord (proto)

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `<KEY>` |  |  | Local file path or object store key |

### commit decode-tombstone

Decode and print a RetentionTombstone (proto)

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `<KEY>` |  |  | Local file path or object store key |

### commit reconstruct

Reconstruct lost L0 commit records for one shard from the record-less data objects' own footers (ADR-0058 decision 2). Scoped to a single (tenant, signal, shard) to bound blast radius. Writes CreateIfAbsent only, never overwrites or deletes an existing record; exits nonzero if any candidate failed

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--shard` |  |  |  |
| `--signal` |  |  |  |
| `--tenant` |  |  |  |

## catalog

List commit records via the catalog

_No flags._

### catalog list

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--hours` |  | `1` | How many hours back from now to list commit records for |
| `--shards` |  | `4` |  |
| `--tenant` |  |  |  |

### catalog fold

One-shot catalog fold for one (tenant, signal)

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--max-flush-lifetime` |  |  | Override the fold's `max_flush_lifetime` (humantime duration, e.g. `30m`, `0s`; the same grammar and unit as the `maintain compact-bucket` / `compact-tenant` flag and ravel-server's `--gc-max-flush-lifetime`). An hour seals only at its end plus this plus the clock-skew allowance plus the fold safety margin, so a freshly finished load waits over an hour before its last hours can be folded; lowering this seals them sooner. The flag asserts that no writer is still flushing, not that this host's clock is exact: the clock-skew allowance and the fold safety margin keep their defaults. UNSAFE under a live writer: a commit record published into a bucket this fold already sealed is never picked up by a later incremental fold, which re-lists only hours after the watermark. The default is the safe 1h; use this only for a tenant known quiescent, such as one whose bulk load has finished and whose writer process has exited |
| `--shards` |  | `4` |  |
| `--signal` |  | `metrics` | Which signal's snapshot to fold. Defaults to metrics, so an existing invocation keeps its meaning |
| `--tenant` |  |  |  |

### catalog inspect

Decode and print HEAD and every referenced snapshot part for one (tenant, signal)

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--signal` |  | `metrics` | Which signal's HEAD to decode. Defaults to metrics |
| `--tenant` |  |  |  |

### catalog verify

Re-list sealed commit records for one (tenant, signal) and diff against that signal's snapshot; exits nonzero if the snapshot mismatches sealed history. A missing snapshot is reported (nothing folded yet) and exits zero

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--signal` |  | `metrics` | Which signal's snapshot to verify. Defaults to metrics |
| `--tenant` |  |  |  |

## maintain

Run and inspect maintenance: compaction, sweep, retention, version audit

_No flags._

### maintain compact-bucket

Run one compaction pass over a single sealed bucket

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--dry-run` |  |  | Compute the plan and report it, but write no L1 parts or record |
| `--hour` |  |  |  |
| `--max-flush-lifetime` |  |  | Override the compactor's `max_flush_lifetime` (humantime duration, e.g. `30m`, `0s`; the same grammar and unit as ravel-server's `--gc-max-flush-lifetime`). A bucket seals only at its hour's end plus this plus the clock-skew allowance, so lowering it seals buckets sooner. UNSAFE below the ingest path's real flush lifetime: a bucket a writer is still flushing into can then be sealed and compacted, and that writer's later-published object is missed by the compaction. The default is the safe 1h; use this only for a tenant known quiescent, such as one whose bulk load has finished |
| `--shard` |  |  |  |
| `--signal` |  |  |  |
| `--tenant` |  |  |  |

### maintain compact-tenant

Compact every sealed bucket of a whole tenant signal: walk each shard's ingest hours and run the same per-bucket compaction `compact-bucket` runs, so an operator no longer has to guess the hour numbers or write a per-(shard, hour) shell loop

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--bucket-concurrency` |  | `1` | Number of buckets to compact CONCURRENTLY. Buckets are independent by construction (disjoint per-(shard, hour) input sets, separate content-addressed parts, separate CAS-published records), so the walk is embarrassingly parallel: N > 1 runs up to N buckets' compactions at once. Default 1, which is today's fully sequential behavior byte-for-byte (report line order included). Refused at 0 |
| `--dry-run` |  |  | Compute each bucket's plan and report it, but write no L1 parts or records |
| `--from-hour` |  |  | First ingest-hour bucket to consider, inclusive. Omit to start at each shard's oldest present hour |
| `--input-read-concurrency` |  |  | Number of per-input reads a compaction keeps in flight at once (the commit-record GET and catalog load per input). Raise it to hide store round-trip latency on a many-input bucket; it never changes output bytes. Default 8 (the compactor default); values below 1 act as 1 |
| `--l1-part-memory-target-bytes` |  |  | The decoded record-heap size at which a merge closes an in-progress L1 part (a split target, not a peak-memory bound: a merge can overshoot it, e.g. by a whole trace on the RSPAN path, so size the host for path-specific overshoot). Lower it for smaller parts on a small host; raise it for fewer, larger parts. Refused at 0. Default 256 MiB (the compactor default) |
| `--max-flush-lifetime` |  |  | Override the compactor's `max_flush_lifetime` (humantime duration, e.g. `30m`, `0s`; the same grammar and unit as ravel-server's `--gc-max-flush-lifetime`). A bucket seals only at its hour's end plus this plus the clock-skew allowance, so lowering it seals buckets sooner. UNSAFE below the ingest path's real flush lifetime: a bucket a writer is still flushing into can then be sealed and compacted, and that writer's later-published object is missed by the compaction. The default is the safe 1h; use this only for a tenant known quiescent, such as one whose bulk load has finished |
| `--max-l1-part-bytes` |  |  | Bound the encoded/on-object bytes a merge writes before it closes an L1 part (the stored-size target). A part closes on whichever of this and --l1-part-memory-target-bytes is reached first. Refused at 0. Default 256 MiB (the compactor default) |
| `--shards` |  |  | Shard count to walk (shards `0..N`). Omit to resolve it from the tenant's durable shard-count provisioning record; given together with a record, the two must agree. With neither flag nor record the command errors, naming the tenant |
| `--signal` |  |  |  |
| `--tenant` |  |  |  |
| `--to-hour` |  |  | Last ingest-hour bucket to consider, inclusive. Omit to stop at the current hour |

### maintain sweep

Run one sweep pass (orphan GC, superseded, unreferenced parts) over a shard

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--dry-run` |  |  | Compute the eligible set and report it, but delete nothing |
| `--override-orphan-breaker` |  |  | Force exactly one overridden pass through a tripped mass-orphan circuit breaker (ADR-0048 decision 4). The breaker never auto-resumes; this is the only way to clear it, and only for this one invocation |
| `--shard` |  |  |  |
| `--signal` |  |  |  |
| `--tenant` |  |  |  |

### maintain status

Report a bucket's maintenance state (read-only; no --dry-run needed)

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--hour` |  |  |  |
| `--shard` |  |  |  |
| `--signal` |  |  |  |
| `--tenant` |  |  |  |

### maintain audit-versions

Audit live on-object format versions for a tenant (both signals)

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--shards` |  | `4` |  |
| `--tenant` |  |  |  |

### maintain migrate

Migrate a (tenant, signal, format family) up to a target format version, then raise its recorded format floor once a fresh re-audit confirms nothing below the target survives. Resumable and bounded: re-run to resume from the durable cursor after a budget stop. The re-audit already excludes a bucket's pre-rewrite commit records once that bucket has been rewritten (they are dead, sweepable leftovers, not stragglers), so a clean run converges and raises the floor in one invocation with no interleaved `sweep` needed. A refused raise ("FOUND STRAGGLERS") therefore means genuine below-target live data (e.g. still-unsealed or newly landed); re-run migrate once it has settled

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--budget-records` |  | `0` | Maximum L0 records to migrate this invocation before persisting the cursor and returning (0 = unlimited; drain the whole walk) |
| `--family` |  |  | Lowercase format-family identifier the floor is keyed by. Defaults to the signal's canonical family (metrics=rseg, logs=rlog, spans=rspan) |
| `--shards` |  | `4` |  |
| `--signal` |  |  |  |
| `--target-version` |  |  | Target format version to raise the floor to. Defaults to the signal's current supported on-object version |
| `--tenant` |  |  |  |

### maintain verify-custody

Re-verify the content-addressed chain for a tenant at rest (both signals): every live data object's content still hashes to the hash16 its key embeds, and every compaction record's referenced inputs still match. Read-only; no --dry-run (it never writes or deletes)

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--shards` |  | `4` |  |
| `--tenant` |  |  |  |
| `--versioning-aware` |  |  | Also list noncurrent (prior) versions under the tenant's keys and report "deleted but recoverable as prior version" as a distinct anomaly class (ADR-0064 §7, S4-12). The ObjectStoreBackend contract exposes no versioned listing, so against a real backend this reports an honest gap rather than an anomaly |

## store

Object store backend qualification (ADR-0050 section 6)

_No flags._

### store qualify

Run the conformance suite against the configured backend and, on a pass, record the outcome at `sys/qualification`

_No flags._

## hold

Place, clear, and list legal holds (ADR-0048 decision 2): the only production mechanism to set a hold

_No flags._

### hold set

Place a legal hold, writing an immutable ADR-0040 audit record. Either `--scope` alone, or `--signal` together with `--shard` (the sugar, which writes all three `shard_hold_scopes` prefixes)

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--reason` |  |  |  |
| `--scope` |  |  |  |
| `--shard` |  |  |  |
| `--signal` |  |  |  |
| `--tenant` |  |  |  |

### hold clear

Release a legal hold, writing an immutable ADR-0040 audit record. Same `--scope` or `--signal`/`--shard` sugar as `hold set`

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--scope` |  |  |  |
| `--shard` |  |  |  |
| `--signal` |  |  |  |
| `--tenant` |  |  |  |

### hold list

List a tenant's currently active legal holds

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--tenant` |  |  |  |

## erase

Submit and inspect selective (GDPR/CCPA subject) erasure requests (ADR-0064 decision 1). Runs under the Admin credential, the same operator-only posture as `hold`

_No flags._

### erase submit

Submit an immutable erasure request: a conjunction of exact-match label/attribute matchers plus an optional event-time window, and an optional free-text reason. Written `.dreq` with CreateIfAbsent; prints the assigned request_id. A request id is generated unless `--request-id` is given (supply it to retry a prior submit idempotently)

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--matcher` |  |  | Exact-match predicate matcher `key=value`, repeatable; the request matches a record only when every matcher holds (logical AND). At least one is required |
| `--reason` |  |  | Optional free-text operator reason |
| `--request-id` |  |  | Reuse an explicit request id (UUID) instead of generating one, to retry a prior submit idempotently under CreateIfAbsent |
| `--signal` |  |  |  |
| `--tenant` |  |  |  |
| `--window-end-ns` |  | `0` | Optional exclusive event-time window end (unix ns) |
| `--window-start-ns` |  | `0` | Optional inclusive event-time window start (unix ns). Both bounds zero (the default) means no event-time restriction |

### erase status

Report an erasure request's state: pending (a `.dreq`, no `.done`), completed (a `.done`, with per-bucket dropped counts and any deferral cause), or unknown. Omit `--request-id` to list every request for the (tenant, signal)

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--request-id` |  |  |  |
| `--signal` |  |  |  |
| `--tenant` |  |  |  |

## idem

Inspect an idempotency marker object (ADR-0051 section 5)

_No flags._

### idem inspect

Fetch and decode an idempotency marker by its exact object key (`t/<tenant_hash>/<signal>/idem/<keyhash32>.<ingest_hour>.idm`)

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `<KEY>` |  |  | Object store key of the marker |

## tenancy

Inspect the bucket's tenant-hash scheme marker (ADR-0050 section 3)

_No flags._

### tenancy show

Print the bucket's `sys/tenancy` marker: its scheme and, for a keyed bucket, the key fingerprint. With `--tenant-hash-key-file`, also derives that key's fingerprint and reports whether it matches the marker (the same wrong-key check the server makes at startup, offline)

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--tenant-hash-key-file` |  |  | Optional 32-byte deployment key file (64 hex chars or 32 raw bytes) to verify against the marker's fingerprint |

## provision

Manage the durable shard_count provisioning record (ADR-0050 section 5)

_No flags._

### provision adopt

Adopt pre-ADR data into a `shard_count` provisioning record, ahead of a server touching the tenant (ADR-0050 section 5). Runs the same adoption path the server runs at ingest/maintenance: writes the record only when every observed shard index is below `--shards`, and refuses (writing nothing) when a higher shard index proves the value would hide data

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--shards` |  |  | The configured shard_count to adopt at (the server's `--shards`) |
| `--signal` |  |  | Restrict to one signal; omit to adopt metrics, logs, and spans |
| `--tenant` |  |  | Tenant id (hashed under the bucket's pinned scheme) |

### provision reshard

Reshard a (tenant, signal) online (ADR-0052): append a new shard generation to its provisioning record under CasVersion and write a control-plane audit record. Existing data is never moved or re-keyed; only future data (from the activation hour onward) routes with the new count. The activation is placed `--lead-hours` in the future, which must be at least ceil(C) + 1 = 2 hours so every live writer observes the new generation before it activates or fail-stops on record staleness

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--lead-hours` |  | `2` | Hours ahead of now to activate the new generation. Must be >= 2 (ceil(C) + 1 with the default 60s refresh interval C). Defaults to 2 |
| `--shard-count` |  |  | The new shard_count for the appended generation (1..=10000) |
| `--signal` |  |  | The signal to reshard |
| `--tenant` |  |  | Tenant id (hashed under the bucket's pinned scheme) |

## gc-config

Show or set the durable deployment-wide GC configuration `sys/gc` (ADR-0050 section 4)

_No flags._

### gc-config show

Print the durable `sys/gc` values (protection horizon, grace, max query duration, max flush lifetime), or report that the bucket is not yet bootstrapped

_No flags._

### gc-config set

Write a full new `sys/gc`, enforcing `protection_horizon >= max_query_duration + grace + clock_skew_allowance` at write time and swapping the durable object with `CasVersion`. All durations are humantime strings (e.g. `25h5m`)

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--clock-skew-allowance` |  |  | Cross-host clock-skew allowance the horizon must cover (e.g. `5m`). The constraint input that closes S1-02; must match the sweepers' `clock_skew_allowance`. Not stored in `sys/gc`. Defaults to 5m when omitted |
| `--grace` |  |  | Shared grace period for the GC age gates (e.g. `24h`) |
| `--max-flush-lifetime` |  |  | Longest a flush may stay open (e.g. `1h`) |
| `--max-query-duration` |  |  | Longest a single query may run (e.g. `1h`) |
| `--protection-horizon` |  |  | Horizon between a deletion anchor and physical deletion (e.g. `25h5m`) |

## typed-attr-column

Show or set a tenant's durable declared typed attribute columns for the `logs` SQL table (ADR-0090 decision 1), in `TenantConfig.typed_attr_columns` at `t/<tenant_hash>/config`. A query-serving process picks a change up within its declared-column staleness horizon; no restart is needed

_No flags._

### typed-attr-column show

Print the tenant's durable declaration, or report that it is unset (in which case the deployment default, from ravel-server's `--typed-attr-column` flags, applies)

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `<TENANT>` |  |  | The tenant whose declaration to print |

### typed-attr-column set

Replace the tenant's declaration wholesale, validating it first and swapping the record with `CasVersion` so a concurrent write is a reported conflict rather than a silent overwrite. Not additive and with no per-key remove: pass the full intended list. Passing no declaration writes an explicit empty one, which means "this tenant declares nothing" and is distinct from having no override at all

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--from-mapping` |  |  | Derive the declaration from a `load --mapping` TOML instead of positional `KEY:TYPE` specs: every `[[attribute]]` and `[[resource_attribute]]` entry becomes a declared column of the same-named type. `f64`-typed entries are skipped with a per-key warning on stderr (there is no `f64` declared column type); the rest are written through the same CAS whole-list replace. Mutually exclusive with positional `KEY:TYPE` specs |
| `<KEY:TYPE>` |  |  | The declaration, as `KEY:TYPE` specs in schema-append order, where TYPE is one of str/i64/bool/bytes (case-insensitive). A key may contain `:`; the type is split off the right. Mutually exclusive with `--from-mapping` |
| `<TENANT>` |  |  | The tenant whose declaration to replace |

## tenant

Manage the durable deployment-wide bearer-token map `sys/auth` (ADR-0072 decision 4): the writer of `sys/auth`

_No flags._

### tenant token

Manage bearer tokens in `sys/auth`

_No flags._

#### tenant token upsert

Map a bearer token to a tenant, hashing it under the deployment key. The plaintext is hashed and dropped, never persisted

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--deployment-key-file` |  |  | Path to the bucket's 32-byte deployment key (64 hex characters or 32 raw bytes); the same key used for `--tenant-hash-key-file` |
| `--managed-by` |  | `cli` | Which writer owns this entry's lifecycle, stamped onto it (ADR-0072 decision 4 amendment). The operator's reconcile loop only ever removes or replaces entries tagged "operator"; anything else (the "cli" default, or a caller's own tag) is never touched by an operator reconcile |
| `--tenant` |  |  | The tenant this token authenticates as |
| `--token` |  |  | The bearer token, in the clear. Prefer a shell mechanism that avoids process-list/history exposure (e.g. `--token "$(cat f)"`) |

#### tenant token revoke

Remove every token mapped to a tenant. Needs no plaintext token: entries carry the tenant id in the clear, so this is correct even when the caller has never seen the tenant's tokens

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--deployment-key-file` |  |  | Path to the bucket's 32-byte deployment key (64 hex characters or 32 raw bytes); the same key used for `--tenant-hash-key-file` |
| `--tenant` |  |  | The tenant to revoke every token for |

#### tenant token list

List every entry's tenant id and a short token fingerprint. Never prints a raw token hash or plaintext

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--deployment-key-file` |  |  | Path to the bucket's 32-byte deployment key (64 hex characters or 32 raw bytes); the same key used for `--tenant-hash-key-file` |

## load

Bulk-import a Parquet file into the logs signal (ADR-0089)

| Flag | Environment variable | Default | Help |
| --- | --- | --- | --- |
| `--batch-rows` |  | `10000` | Rows per Strict flush. One flush is one RLOG object per involved shard, so on a large load this is the lever that controls how many RLOG objects the load leaves behind (a first-order query-cost variable). Must be at least 1; 0 is rejected. Defaults to `DEFAULT_BATCH_ROWS` (10000), leaving current behaviour unchanged |
| `--decode-queue-batches` |  | `2` | Number of decoded batches allowed to sit queued between the Parquet decode/build stage and the shard writers (issue #680). A bounded channel decouples the two: the reader decodes batch N+1 (and, with `--read-cursors > 1`, stride-reads several row-group regions in parallel) while the encoders write batch N, so decode and encode overlap instead of running in lockstep. The reader blocks when the channel is full, so the queue holds at most this many built batches; the extra memory is roughly this count times one batch's built size, on top of `--pipeline-depth`'s in-flight-write working set. Defaults to 2. Must be at least 1; 0 is rejected |
| `--mapping` |  |  | Path to the `--mapping` TOML (source columns to record fields) |
| `--max-flush-delay` |  |  | How long a shard buffer may age before the router flushes it, regardless of `--target-bytes` (issue #801). A humantime duration (`2s`, `10m`, `1h5m`). Unset leaves the router's default (2s), so an omitted flag changes a load's object layout not at all |
| `--max-inflight-flushes` |  | `4` | Number of flushes one shard may have in flight at once (issue #807). This bounds the shard actor's own flush pipeline, PER SHARD: the loader writes one RLOG object per batch per involved shard, and at `1` a shard actor must wait for the previous object's PUT and commit-record publish before it starts the next one, so a second batch landing on the same shard queues behind the first even when `--pipeline-depth` has already handed both to the router. The resulting ceiling on genuinely concurrent flushes is roughly `--shards` x this value, capped additionally by `--pipeline-depth` (the loader never keeps more than that many writes outstanding, so a value above `--pipeline-depth` cannot be reached). Defaults to `DEFAULT_MAX_INFLIGHT_FLUSHES`, which tracks `--pipeline-depth`'s own default (4) so the inner window never re-serialises what the outer one made concurrent. Setting it below `--pipeline-depth` makes each shard's excess batches queue on this semaphore, and they still have to clear it inside the 60s Strict ack deadline. On this bulk path it costs no extra memory: the resident flush working set is whatever the outstanding batches carry and `--pipeline-depth` already caps that, so this knob only decides whether those objects are encoded and PUT concurrently or one at a time. A Strict write's acknowledgement is unchanged by the setting: each flush answers its own waiters only after its own data object and its own commit record have landed. `1` restores one-flush-per-shard behavior. `0` is rejected: it is a semaphore no flush can ever acquire, which would deadlock the shard |
| `--parquet` |  |  | Path to the source Parquet file |
| `--pipeline-depth` |  | `4` | Number of Strict writes allowed in flight at once. Each batch's write is one S3 PUT round trip per involved shard; at depth `1` the loader submits one write and waits for its ack before building or submitting the next, so that round-trip latency is serial and the machine has nothing to run in between. Raising the depth lets up to this many writes overlap, hiding the PUT latency behind later batches' encode and I/O. Defaults to `DEFAULT_PIPELINE_DEPTH` (4), which is where the measured 2.94x on the 100M-row ClickBench corpus comes from (ADR-0807); `1` restores the old one-batch-at-a-time behavior. The cost is memory: each in-flight write keeps its built batch resident until its ack, so the live working set scales by roughly the depth (see docs/guides/clickbench.md for how this stacks with the `--batch-rows` x `--shards` product). The reported durable-token list is unaffected by the depth. It is always exactly the batches strictly before the failing one, in submission order, followed by whatever a batch submitted after the failing one had committed: on a failure the loader resolves every outstanding write before returning rather than abandoning it, so the report equals what landed at any depth, and a resume from it does not re-ingest rows that already committed. `0` is rejected |
| `--read-cursors` |  |  | Number of parallel stride read cursors over the Parquet file's row groups (issue #560). A file sorted by a resource-attribute column (e.g. ClickBench's `hits.parquet`, sorted by `CounterID`) puts one value's rows in one contiguous run, so a single sequential reader fills each `--batch-rows` batch with just that one value: one `shard_for_log` hash, one shard, no spread across `--shards`. K cursors each read a disjoint, near-even, far-apart partition of the file's row groups, and each batch is assembled from a contiguous run out of every live cursor, so one batch's rows span K different regions of the file instead of one. Omit for automatic sizing (`min(--shards, row-group count)`, floored at 1); an explicit value is clamped to `[1, row-group count]`. `1` is exactly today's sequential read. `0` is rejected |
| `--shards` |  | `4` | Configured shard count. Validated against (or, for a fresh signal, written to) the durable provisioning record, exactly as the server does at first touch; the router resolves the active generation from that record. Defaults to the server's default of 4 |
| `--target-bytes` |  | `1` | Estimated in-memory bytes a shard's buffer accumulates before it flushes as one RLOG object (issue #801). At the default `1` every batch flushes as its own object the moment it is written: one object per involved shard per batch, `--batch-rows` sets its size, and no buffer lingers. A larger value lets a shard hold several batches' records in one buffer until the target is reached, so objects grow without any more Arrow batches being held in memory -- unlike raising `--batch-rows`, whose memory cost is linear because each batch is buffered whole |
| `--tenant` |  |  | Target tenant id (hashed under the bucket's pinned scheme) |

<!-- END GENERATED FLAGS -->
