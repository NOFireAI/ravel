# Commit Protocol, Catalog, and MVCC

Companion to ADR-0002, ADR-0003, and ADR-0010. This is the implementer
contract for `ravel-commit` and `ravel-catalog`.

## Key layout (all under one bucket root)

```
t/<tenant_hash>/m/l0/<shard>/<writer_id>.<epoch>.<seq>.<hash16>.rseg      data
t/<tenant_hash>/m/c/<shard>/<ingest_hour>/<writer_id>.<epoch>.<seq>.cmt   commit
t/<tenant_hash>/m/l1/<shard>/<ingest_hour>/<input_set_hash16>.<part:04>.<hash16>.rseg   L1 part
t/<tenant_hash>/m/c/<shard>/<ingest_hour>/l1.<input_set_hash16>.cmt       compaction record
t/<tenant_hash>/m/c/<shard>/<ingest_hour>/rw.<input_set_hash16>.cmt       rewrite record (selective erasure; ADR-0064)
t/<tenant_hash>/m/c/<shard>/<ingest_hour>/retire.tmb                      retention tombstone
t/<tenant_hash>/m/maint/<shard>/cursor                                    advisory scan cursor
t/<tenant_hash>/<signal>/del/<request_id>.dreq                          erasure request (CreateIfAbsent, immutable; ADR-0064)
t/<tenant_hash>/<signal>/del/<request_id>.done                          erasure completion (CreateIfAbsent, immutable, PII-free; ADR-0064)
t/<tenant_hash>/<signal>/prov                                           shard_count provisioning record (write-once, additive; ADR-0050 §5)
t/<tenant_hash>/enc                                                     per-tenant KMS key-epoch record (CAS append-only, additive; ADR-0062 §1b)
t/<tenant_hash>/config                                                  per-tenant lifecycle/limits/retention config (CAS whole-record replace, additive; ADR-0066 §6)
t/<tenant_hash>/<signal>/idem/<keyhash32>.<ingest_hour>.idm              idempotency marker (logs/spans; additive)
t/<tenant_hash>/catalog/<signal>/snap/<watermark>.<hash16>.csnap         snapshot part (immutable)
t/<tenant_hash>/catalog/<signal>/HEAD                                    head pointer (mutable, CAS)
t/<tenant_hash>/catalog/<signal>/idx/<watermark>.<hash16>.npost         name postings (immutable, phase 5)
sys/qualification                                                       store qualification record (write-once, additive)
sys/qualify/<run-id>/...                                                store qualification scratch objects (transient)
sys/tenancy                                                             tenant-hash scheme marker (write-once, additive; ADR-0050 §3)
sys/auth                                                                deployment-wide keyed-token-hash -> tenant map (CAS whole-record replace, additive; ADR-0066 §6)
sys/t/<tenant_hash>                                                     per-tenant recovery manifest (keyed buckets only, write-once; ADR-0050 §3)
admission/query/<process_id>.snapshot                                   fleet-global query concurrency snapshot (root-level, per-process, Overwrite; ADR-0061 §2)
sys/maintain/workers/<process_id>                                       maintain-worker liveness heartbeat (root-level, per-process, Overwrite; ADR-0065 §1)
sys/maintain/memo/<process_id>                                          maintain-worker durable memo snapshot for warm start/handoff (root-level, per-process, Overwrite; ADR-0065 §3)
```

The compaction/retention key shapes (ADR-0018, ADR-0019;
docs/compaction-retention-plan.md §3.1) and the selective-erasure key shapes
(`rw.` rewrite records and the `del/` request/completion prefix, ADR-0064) are
additive: existing keys and their meaning are untouched.

`admission/query/<process_id>.snapshot` (ADR-0061 §2) is the fleet-global
query concurrency ceiling's keyspace. Each query-serving process writes its
current in-flight query count to the single key named by its own stable
process id and reads every sibling's key under the `admission/query/` prefix,
computing a fleet-wide reconciled total the local admission check enforces
against (the ADR-0057 count-cap reconciliation pattern, reused for a query
resource). It is deliberately a **root-level** key, outside any tenant's
`t/<tenant_hash>/` space and carrying no tenant dimension at all, because the
ceiling is fleet-global rather than per-tenant: the resource it bounds is
aggregate query fan-out across all tenants, which has no single tenant to scope
under — unlike ADR-0057's ingest admission snapshot, which is per-(tenant,
signal) and lives under `t/<tenant_hash>/<signal>/admission/`. The key is
owned exclusively by the one process that names it, so it is written with a
plain `Overwrite` (no CAS, no concurrent writer to race); a stale or missing
snapshot is self-correcting on the next reconciliation interval and, past the
`2R` staleness window, is treated as contributing zero rather than trusted. The
body is a small JSON object (`format_version`, `in_flight`, `snapshot_unix_ns`)
rather than a protobuf message, because the keyspace is owned entirely by
`ravel-query`'s query-admission module, single-writer per key and ephemeral, so
its body is an internal contract of that module rather than a cross-crate frozen
format. Nothing deletes these snapshots (staleness detection replaces a sweep),
so no role holds a delete grant for the prefix.

`sys/maintain/workers/<process_id>` (ADR-0065 §1) is the maintain role's
worker-membership keyspace. Every maintain-role process writes, on a heartbeat
interval `H` (default 60s), a small versioned-tag protobuf `WorkerHeartbeat`
(`format_version`, `process_id`, `started_unix_ns`, `heartbeat_unix_ns`) to the
single key named by its own stable process id, with a plain `Overwrite` (one
writer per key, no CAS). On the same cadence each process lists the
`sys/maintain/workers/` prefix and GETs siblings to compute the **live set** --
itself plus every sibling whose heartbeat is within `3 * H` of the reader's
clock -- and every process partitions the `(tenant_hash, signal, shard)` unit
space over that live set by rendezvous (highest-random-weight) hashing, so N
replicas divide the maintenance work rather than each paying for all of it.
This is membership/ownership, deliberately **not** a lease (the existing
`LeaseCheck` trait is the unrelated GC reader-protection gate). It is a
root-level key with no tenant dimension, like the query-admission snapshot
above; a stale or missing heartbeat is self-correcting on the next interval and,
past the `3 * H` window, its owner is treated as gone and its units are taken
over (idempotent overlap during the transition, never lost or double-counted
work). Nothing deletes these heartbeats (staleness detection replaces a sweep).

`sys/maintain/memo/<process_id>` (ADR-0065 §3) is the maintain role's durable
per-worker memo snapshot: the same self-owned, root-level, `Overwrite` pattern
beside the heartbeat prefix. On its discovery cadence each maintain process
writes a compact summary of the `(tenant, signal, shard)` buckets it has
verified terminal -- per unit a frontier run (the longest contiguous
same-state terminal run) plus a sparse RLE exception list of the other
terminal runs outside it, so the retention-window interior is one run and the
object stays KBs even at large retention -- **debounced**, so a tick that
verified nothing new writes nothing (the debounce compares the timestamp-free
body, so it rides the existing tick and needs no separate timer). On startup,
and whenever a membership change moves ownership of a unit to this process, the
worker lists this prefix and GETs every sibling's snapshot (its own previous one
included), seeding its in-memory memo from the freshest non-stale entries for
the units it now owns, so a restart or ownership handoff warm-starts instead of
rescanning the retention window cold. A snapshot is trusted only while it is
within one memo re-verify interval (default 1h) of the reader's clock: past that
every entry it carries would already be individually stale, so an older snapshot
holds nothing that could suppress a read and is ignored. Like the heartbeat, the
payload is a versioned-tag advisory record -- membership/warm-start state,
deliberately **not** a lease -- and losing, staling, or corrupting it costs at
most a rescan of the affected units, never correctness (the ADR-0003
HEAD-pointer precedent). Nothing deletes these snapshots (staleness detection
replaces a sweep; a bounded cleanup sweep is a future step if it ever matters).

`sys/qualification` and the `sys/qualify/` prefix (ADR-0050 §6) are
additive root-level keys, outside any tenant's `t/<tenant_hash>/` space.
`sys/qualification` is written once per bucket by `ravel-cli store
qualify` (services/ravel-cli/src/qualify.rs) via `CreateIfAbsent` after a
passing conformance run; it is never overwritten, and server startup on a
production store kind reads it to refuse starting when the record is
absent or its suite version is stale. `sys/qualify/<run-id>/...` holds the
scratch objects the conformance suite (crates/ravel-object-store/src/
conformance.rs) writes and reads while probing conditional-write and
listing consistency under a fresh `run-id` each run; these objects are
transient probe fixtures, not durable state, and carry no lifecycle
guarantee beyond the run that created them.

`t/<tenant_hash>/<signal>/prov` (ADR-0050 §5, EC5) is the durable
`shard_count` provisioning record: a per-(tenant, signal) object holding
`tenant_hash`, `signal`, `shard_count`, a `format_version` floor,
`created_unix_ns`, an append-only `generations` history (ADR-0052), and an
append-only `format_floors` history (ADR-0066 §3): per-format-family floors
below which no live object exists for this (tenant, signal), raised only,
never lowered, CAS-appended by `ravel_catalog::raise_format_floor`
(proto/ravel/sys.proto `ProvisioningRecord`). It is
written with `CreateIfAbsent` at the tenant's first write for that signal
(`ravel_catalog::validate_or_adopt`), so a racing loser re-reads and
validates against the winner rather than erroring. It lives under the
tenant's own prefix, alongside that signal's `l0/` and `c/` shard data, not
in the bucket-root `sys/` space, because it is per-tenant state. Every
ingest, catalog-resolve, and maintenance touch validates the configured
`shard_count` against it: a statically-known tenant's disagreement refuses
startup, a dynamic tenant's disagreement fails that one request, and a query
never resolves over a subset of shards. A (tenant, signal) with pre-ADR data
but no record is adopted once (the record is written from config) only when
every observed shard index is below the configured `shard_count`; a higher
observed index proves the value would hide data and refuses without writing.
`shard_count` is immutable per generation; the generation history is
append-only; the shard-index domain of hour `h` is `0..scan_count(h)`
(ADR-0052, online resharding). A reshard appends a
`(generation, shard_count, activation_hour)` entry to this record under
`CasVersion` (`ravel_catalog::append_generation`); every existing byte of
history is immutable, and the scalar `shard_count` field stays equal to
generation 0's count. Readers derive the per-hour shard fan-out from the
history via `ravel_catalog::scan_count` rather than a single static count.

`t/<tenant_hash>/enc` (ADR-0062 §1b, epic EL) is the durable per-tenant
KMS key-epoch record: a tenant-scoped object (not per-signal, since a
tenant's KMS key applies across every signal) holding `tenant_hash`, a
`format_version` floor, `created_unix_ns`, and an append-only `epochs`
history, each epoch `{epoch, key_arn (empty = deployment default),
activated_ns}` (proto/ravel/sys.proto `KeyEpochRecord`/`KeyEpoch`). It is a
wholly new additive object type; ADR-0062 authorizes it, no existing layout
changes and no version bump is required. The first time a tenant's key is
configured or changed, one epoch is appended
(`ravel_catalog::record_key_epoch`): epoch 0 is bootstrapped with
`CreateIfAbsent` (a racing loser surfaces a typed CAS conflict and re-reads,
the `ProvisioningRecord` first-write precedent), and every later rotation is
appended under `CasVersion` with an `activated_ns` strictly past the last and
a `key_arn` different from the last (the `append_generation` reshard pattern).
Every existing byte of history is immutable. Because data objects are
immutable, every object's write time locates it in exactly one epoch, so
"which key encrypts what" is answerable from the bucket alone;
`ravel-cli maintain verify-custody` uses `ravel_catalog::epoch_for_write` to
flag any live object whose write time predates the tenant's first recorded
epoch as a custody anomaly. A tenant with no `enc` record has only ever used
the deployment default key, which is not an error (`read_epochs_from_store`
returns `Ok(None)`). This record is durable audit metadata only; it is not
read on the live write path (the routing decorator carries the active key),
so a slightly stale reader cannot misroute a write.

`t/<tenant_hash>/config` (ADR-0066 §6, epic EM) is the durable per-tenant
config record that moves tenant lifecycle state, admission-limit overrides,
retention, and indexed-field config off process flags and into durable state.
A tenant-scoped object (not per-signal, since these apply across every signal)
holding `tenant_hash`, a `format_version` floor, a `lifecycle_state`
(`active` / `suspended` / `offboarding`), optional admission-limit and
retention overrides, an optional indexed-field set, and `created`/`updated`
timestamps (proto/ravel/sys.proto `TenantConfigRecord`). Defaults still come
from flags/limits-file at startup; a field present here overrides the default
for this tenant, an absent one leaves the default in place. Unlike the
append-only `prov`/`enc` histories, it is mutated by **whole-record
CAS-replace** (`ravel_catalog::set_tenant_config`, the `sys/gc` `set_gc_config`
pattern): a config override is mutable current state whose latest value is the
only one that matters and which must support lowering a limit or clearing an
override, so the record is read for its version and swapped in place under
`CasVersion`; a concurrent write is a typed conflict the loser re-reads, never
a silent overwrite. On a tenant with no record the first write bootstraps with
`CreateIfAbsent`. A tenant with no `config` record runs entirely on the
deployment defaults (`read_config` returns `Ok(None)`). This record is durable
control state only; the bounded-staleness refresh loop that reads it on a
horizon and re-invokes the admission controller's `set_tenant_limits` is a
separate concern (epic EM, EM-T8).

`sys/auth` (ADR-0066 §6, epic EM) is the durable, deployment-wide bearer-token
map replacing the startup-frozen `--tenant-token` allowlist: a bucket-root
object (never under a tenant prefix, since an entry maps a token to whichever
tenant it grants) holding a `format_version` floor, a 16-byte deployment-key
fingerprint, and `entries` mapping a token hash to a tenant id
(proto/ravel/sys.proto `AuthTokenMap`/`TokenHashEntry`). The bucket **never**
holds a plaintext token: each `token_hash` is
`blake3::keyed_hash(deployment_key, token)`, so the map is useless without the
deployment key and a low-entropy token is not recoverable by an offline
dictionary attack (the tenant-hash-v2 keyed-hashing pattern). The stored
fingerprint lets a reader configured with the wrong deployment key refuse
rather than silently mismatch every hash. Like `t/<tenant_hash>/config` it is
mutated by whole-record CAS-replace (`ravel_catalog::upsert_token` /
`remove_token`): the map is current-state and revocation (removing an entry) is
a first-class operation an append-only history cannot express. A deployment
with no provisioned tokens (or one using only OIDC/mTLS) has no `sys/auth`
object, which is not an error (`read_auth_map` returns `Ok(None)`). The
resolver refresh loop (rate-limited on-miss re-read, fail-closed revocation) is
EM-T8's concern; this module supplies only the object shape, the keyed hash,
and the CAS read/write helpers.

- `keyhash32` (idempotency marker keys only, ADR-0051 §5): 32 lowercase hex
  chars, the first 16 bytes of `blake3("ravel-idem-v1" || tenant_id ||
  client_key)`, where `tenant_id` is the logical tenant identifier (not
  `tenant_hash`) and `client_key` is the caller's opaque
  `x-ravel-idempotency-key`. The `idem/` prefix is additive: no existing
  read, resolve, or sweep path lists it (commit resolution lists `c/…`, the
  orphan sweep lists `l0/…`; the fail-loud unknown-key rule below applies
  only to the `c/` prefix), and markers older than the dedup window
  (default 24h, from the `ingest_hour` in the file name) are deleted by
  `ravel_maintain::sweep::sweep_idempotency_markers` (ADR-0051 §5, epic
  #452 EB-9), not by this crate: it LISTs the coarser
  `t/<tenant_hash>/<signal>/idem/` prefix (no `keyhash32`, since the sweep
  has no client key to scope by), deletes every marker whose `ingest_hour`
  is more than `CompactorConfig::idem_dedup_window_hours` (default 24h)
  behind the sweep's current ingest-hour bucket, and skips (rather than
  errors on) any key under the prefix that fails to parse as
  `<keyhash32>.<ingest_hour>.idm`. Under `CompactorConfig::dry_run` it counts
  what it would delete without calling `delete`.
- Marker body byte layout and checksum coverage: see "Idempotency marker
  body layout" below.
- `input_set_hash16`: first 16 hex chars of the blake3 digest over the
  compaction record's sorted `inputs` list (canonical encoding, sorted by
  `(writer_id, writer_epoch, writer_seq)`). `hash16` on an L1 part is the
  part object's own blake3, same convention as an L0 data key. `part` is
  zero-padded 4 digits.
- Compaction records, rewrite records, and the retention tombstone live in
  the same `c/<shard>/<ingest_hour>/` prefix as L0 commit records, so the
  existing one-LIST-per-bucket resolution path discovers every shape without
  a second LIST. Filenames are disjoint by construction:
  `<writer_id>.<epoch>.<seq>.cmt` (L0 commit), `l1.<input_set_hash16>.cmt`
  (compaction record), `rw.<input_set_hash16>.cmt` (selective-erasure rewrite
  record, ADR-0064 decision 3), `retire.tmb` (tombstone, fixed name). A key in
  this prefix matching none of these shapes is a fail-loud error (surfaced to
  metrics), never silently skipped: layout drift must be visible, not
  swallowed. `keys::partition_bucket_entry` classifies all four shapes, so
  every caller handles a rewrite record explicitly rather than by wildcard.
  This matters most in `crates/ravel-catalog/src/fold.rs`: the fold treats an
  unrecognized shape as layout drift and skips it with a warning, so an
  unclassified `rw.` key would have had its supersession of the erased inputs
  ignored by the index fold, letting erased records reappear in a folded
  snapshot.
- Selective-erasure request and completion records (ADR-0064 decision 1) live
  under a separate `t/<tenant_hash>/<signal>/del/` prefix, not in `c/`, so the
  bucket-resolution LIST never sees them; the resolver LISTs `del/` once per
  resolve to attach pending predicates (decision 2). A `.dreq` is written
  `CreateIfAbsent`, is immutable, necessarily names the subject, and is deleted
  after its `.done` exists plus the protection horizon (decision 5). A `.done`
  is written `CreateIfAbsent`, is immutable, carries no plaintext subject
  identifier (only a blake3 predicate hash and per-bucket dropped counts), and
  is permanent audit evidence. The `rw.<input_set_hash16>.cmt` rewrite record's
  `input_set_hash16` is the first 16 hex chars of the blake3 digest defined by
  `ravel_commit::erasure::compute_rewrite_input_set_hash` (domain-separated,
  over the record's superseded input set or superseded record key plus its
  sorted applied request ids), a distinct domain from the compaction
  `input_set_hash` so the two can never collide.
- The maint cursor (`m/maint/<shard>/cursor`) is advisory mutable state,
  updated by CAS, the same exemption from the immutability rule that the
  ADR-0003 HEAD pointer has. Losing or corrupting it costs a rescan, never
  correctness; it carries no durability role and is not a manifest.

- `tenant_hash`: hex, 32 chars (ADR-0009). The derivation is pinned per
  bucket at bucket birth by the `sys/tenancy` marker (ADR-0050 §3), and one
  binary carries both derivations, selected once at startup:
  - v1-unkeyed: `blake3("ravel-tenant-v1" || tenant_id)[0..16]`. The
    original derivation and the permanent scheme for every pre-ADR-0050
    bucket. Unkeyed BLAKE3 hides tenant names from keys but lets anyone with
    list access confirm a guessed id offline.
  - v2-keyed (the default for buckets created after ADR-0050):
    `blake3::keyed_hash(k, tenant_id)[0..16]` where `k =
    blake3::derive_key("ravel-tenant-v2", deployment_key)` and the 32-byte
    deployment key is loaded from `--tenant-hash-key-file`. Enumeration
    resistant: without the key, prefixes reveal nothing about which tenants
    exist. `sys/tenancy` records a 16-byte fingerprint of the key (never the
    key), so a wrong key is a startup refusal, not a silent parallel
    namespace, and keyed buckets write a `sys/t/<tenant_hash>` recovery
    manifest (the tenant id encrypted under an AES-256-GCM key derived from
    the deployment key) at each tenant's first write.

  Correction: this table and ADR-0010 §13 previously stated a
  deployment-keyed variant was "available via config". It was never
  implemented until ADR-0050 §3 (EC3); the keyed variant described above is
  the real, default, durable design. There is no re-key migration between
  schemes (ADR-0050 §3; docs/guides/operations.md).
- `m` = metrics signal. Logs `l`, spans `s`, profiles `p` reserved.
  Alerts `a` and audit `u` (ADR-0040) share `l`'s RLOG segment format
  verbatim - no new byte layout, only two new signal-keyspace prefixes.
- `shard`: zero-padded 4-digit decimal. `shard_count` is immutable per
  generation; the generation history is append-only; the shard-index domain
  of hour `h` is `0..scan_count(h)` (ADR-0052, superseding ADR-0010 §9's
  "immutable per (tenant, signal)"). A reshard appends a new generation with
  a future `activation_hour`; existing data is never moved or re-keyed, and
  reads derive the per-hour shard set from the history.
- `ingest_hour`: `YYYYMMDDTHH` UTC formatted from the pinned
  `ingest_hour_bucket` (unix hours) of the flush. Never recomputed on retry.
- `writer_id`: UUIDv4 assigned per process start. MUST be freshly random;
  MUST NOT be derived from hostname, pod name, shard index, or any config
  (ADR-0010 §3). `epoch`: u64, informational (unix seconds at startup).
  `seq`: u64 monotonic per (writer_id, epoch, shard), zero-padded 20 digits
  so lexicographic = numeric order. Gaps are permitted (abandoned flushes)
  and carry no meaning: never infer completeness from seq continuity.
- `hash16`: first 16 hex chars of the object's blake3.
- `signal` (catalog keys only): the same one-letter signal prefix as the
  data/commit keys (`m` for metrics), scoping the snapshot index per
  (tenant, signal) the same way `shard_count` and the commit layout already
  are (docs/metric-index-plan.md 3).
- `watermark` (catalog keys only): the snapshot part's watermark hour,
  formatted as the same `YYYYMMDDTHH` text as `ingest_hour`. Informational
  for operators; HEAD's `watermark_hour` field is authoritative, never this
  string.
- Snapshot parts are content-addressed: `hash16` is the blake3 of the part's
  full encoded bytes, computed over the final object, so two folders that
  fold the same input independently write the same key and
  `PutMode::CreateIfAbsent` `AlreadyExists` is idempotent success, exactly
  like data objects (ADR-0010 §7).
- Superseded snapshot parts (`catalog/<signal>/snap/`) and name-postings
  objects (`catalog/<signal>/idx/`) are swept by
  `ravel_maintain::sweep::sweep_unreferenced_catalog_objects` (EH-T4,
  issue #741), the fifth GC sweep rule alongside orphan GC, the
  superseded-input and unreferenced-part sweeps, and the idempotency-marker
  sweep. Every fold that rewrites a part or postings object writes a new
  content-addressed key and swaps HEAD, leaving the old object in place
  (docs/metric-index-plan.md 4 step 8, the "orphan part" crash-matrix row);
  without this rule each such fold leaks one object. The rule LISTs the two
  prefixes first, then GETs the current `catalog/<signal>/HEAD`, treats every
  `parts[].key` and the optional `postings.key` as referenced, and deletes any
  object under the two prefixes that HEAD does not name once its
  `last_modified` age exceeds `CompactorConfig::protection_horizon_ns`. A fresh
  re-verify GET of HEAD is taken immediately before the delete loop (the same
  batched-re-verify shape orphan GC uses for its commit-prefix LIST), so an
  object a fold's HEAD CAS named between the two reads is spared. Like the
  idempotency-marker sweep, it is per (tenant, signal) rather than per shard
  (catalog objects carry no shard dimension) and consults the
  `LeaseCheck`/legal-hold gate before every delete; under
  `CompactorConfig::dry_run` it counts what it would delete without calling
  `delete`. A present, decodable HEAD is the rule's only anchor: an absent HEAD
  sweeps nothing for the (tenant, signal), exactly like a bucket with neither a
  compaction record nor a tombstone in the unreferenced-part sweep. This is
  because a recovery fold rebuilding from no HEAD (`HeadState::Absent`/`Corrupt`)
  recomputes and re-PUTs every part, and a non-tail span keys on its stable
  `watermark_hour`, so the recomputed key is byte-identical to any surviving old
  object: the PUT returns `AlreadyExists` and the fold adopts the old object
  *without rewriting it* (its `last_modified` stays old) before naming it in the
  HEAD it is about to CAS. With no HEAD to compare against, such an object is
  indistinguishable from a part a fold is mid-flight on, so it must be left
  alone. The `protection_horizon_ns` gate is therefore a reader-pinning buffer
  here (`max_query_duration + grace`), not a writer interlock: unlike the orphan
  and unreferenced-part gates, whose lifetime terms mirror a real writer
  abandonment deadline, adoption-via-`AlreadyExists` never refreshes
  `last_modified`, so an object's age does not bound the fold that may adopt it;
  what bounds the writer race is the no-anchor rule plus the pre-delete HEAD
  re-verify. A HEAD present but undecodable fails the pass without deleting, so
  a corrupt HEAD can never make the live snapshot look unreferenced.

### Idempotency marker body layout

The object at an `idem/<keyhash32>.<ingest_hour>.idm` key (ADR-0051 §5,
`crates/ravel-ingest/src/idempotency.rs`) is a small versioned, checksummed
frame, header then payload:

| bytes | field | encoding |
|---|---|---|
| 0..4 | magic | `RIDM`, fixed |
| 4..6 | version | u16 LE, currently `1` |
| 6..10 | crc32c | u32 LE |
| 10..18 | `written_count` | u64 LE |
| 18..20 | token-set length | u16 LE, byte length of the field below |
| 20.. | token set | UTF-8, the `x-ravel-commit-token` header value: one `CommitToken::encode()` output per shard the request's points flushed through, comma-separated |

Checksum coverage: the crc32c at bytes 6..10 covers `magic || version ||
payload` (bytes 0..6 followed by everything from byte 10 onward) — the crc
field itself is excluded, same as any self-describing checksum has to
exclude its own bytes. Folding the header into the checksum means a
corrupted `magic` or `version` byte is caught here rather than surfacing
later as a misdecode under a future version's body layout. A checksum
mismatch, truncation, bad magic, or malformed payload are all typed decode
errors; the caller treats every one of them as a marker miss (fail-open to
at-least-once, ADR-0051 §5), never a panic.

## Pinned flush identity

At flush open the writer fixes, immutably, for the lifetime of the flush:

1. `seq` (allocated once, never reused for a different flush),
2. `ingest_hour_bucket` = ingest wall clock at flush open, unix hours,
3. the serialized segment bytes,
4. the blake3 content hash of those bytes.

Every retry of any step below reuses these verbatim. A retry MUST NOT
re-serialize, MUST NOT accrete newly arrived samples, and MUST NOT re-read
the clock. New samples always go to the next flush. A flush that cannot
complete within `max_flush_lifetime` (default 1 h) is abandoned: the writer
MUST NOT publish its commit record afterward (GC interlock, ADR-0010 §11);
its buffered points are reported as failed to any strict-mode waiters.

## Sealed hours

Definition. For an ingest-hour bucket H (unix hours), let
`end(H) = (H + 1) * 3600 s`. H is **sealed** at wall time T iff:

```
T >= end(H) + max_flush_lifetime + clock_skew_allowance + fold_safety_margin
```

with `max_flush_lifetime` (default 1 h) and `clock_skew_allowance`
(default 5 m) as configured for the tenant's writers and catalog, and
`fold_safety_margin` a catalog config (default 15 m).

Seal lemma: the commit-record set of a sealed bucket is immutable. Proof
sketch from the rules above: `ingest_hour_bucket` is pinned at flush open
from the writer's clock ("Pinned flush identity"); a flush older than
`max_flush_lifetime` is abandoned and MUST NOT be published afterward (GC
interlock, ADR-0010 §11); so the last possible publish for bucket H happens
before `end(H) + max_flush_lifetime` on the writer's clock, which is within
`clock_skew_allowance` of true time. `fold_safety_margin` absorbs the
folder's own clock error. Therefore one strongly consistent LIST of a
sealed bucket (the store contract's listing guarantee, the same one orphan
GC relies on, docs/consistency-model.md "Deletion and GC") observes the
full and final set.

Clock assumption, stated plainly: the folder's clock error must be smaller
than `fold_safety_margin`. This is the same class of assumption the system
already makes about writer clocks (`clock_skew_allowance`) and it fails
detectably, not silently: `ravel-cli catalog verify` re-lists sealed
buckets and diffs them against the snapshot, and a rebuild repairs any
divergence because commit records remain the ground truth.

Config discipline: `max_flush_lifetime` and `clock_skew_allowance` may only
be raised for writers after every folder's seal computation uses the raised
values (deployment ordering: folders before writers). Lowering them is
always safe for sealing.

(docs/metric-index-plan.md 2, ADR-0020.)

## Fold reconcile pass (ADR-0063 section 4)

The incremental fold lists only the buckets for hours strictly after the
previous fold's watermark (`incremental_buckets`, hours
`(watermark_hour_old, watermark_hour_new]`). A compaction record (`l1.*.cmt`),
a retention tombstone (`retire.tmb`), or a selective-erasure rewrite record
(`rw.*.cmt`, ADR-0064) can be published into an hour long after that hour
was sealed and folded into a past fold's output; because no later fold ever
re-lists an already-folded hour, such a late record would otherwise never be
applied to the snapshot short of a full HEAD-corruption rebuild. Under the
single ever-rewritten part of the pre-EH design this was masked (the one
part was recomputed every cycle). Under EH-T2's sealed parts, which are
deliberately carried forward by reference and never rewritten, it must be
addressed directly.

The rewrite-record trigger is load-bearing, not incidental: ADR-0064 §3.1
scopes the rewrite pass to already-sealed buckets by construction, so a
rewrite ALWAYS lands in an already-folded hour. Without this trigger, a
folded snapshot would keep serving a rewrite's pre-erasure inputs
indefinitely for any hour outside the reconcile window -- the input objects
stay physically present (GET-able) until the horizon-gated sweep runs, so
there is no NotFound-driven re-resolve to force a refresh the way a deleted
object would provide. See the amendment note in
docs/adrs/0064-selective-subject-erasure.md §4 for the window-width
consequence this has for erasure specifically (the 26h default reconcile
window is far narrower than the rewrite pass's own scope, which can target
any sealed bucket regardless of age).

Each incremental fold, after the incremental-bucket processing and before the
part spans are cut, runs a reconcile pass over a bounded window of
already-sealed hours:

- **Window.** Hours in `[watermark_hour_old - fold_reconcile_window_hours,
  watermark_hour_old]`, inclusive at both ends. `watermark_hour_old` is the
  boundary the incremental range excludes, so the reconcile window and the
  incremental range are adjacent, never overlapping. `fold_reconcile_window_hours`
  defaults to 26 hours: `protection_horizon` (24 h, the age gate before the
  sweeper may physically delete a superseded compaction input) plus slack.
  Because the sweeper only deletes an input after that horizon, any record
  whose supersession could invalidate a snapshot entry is observed by a
  reconcile pass before its inputs can disappear.
- **Skipped when redundant.** The pass does not run on the first fold for a
  tenant (no previous watermark exists) or on a rebuilt fold (absent, corrupt,
  or unreadable-part HEAD): a rebuild already re-derives every hour from the
  commit layout, so reconcile would repeat that work.
- **Cheap common case.** Each window bucket is re-listed (bounded by the same
  `fold_bucket_concurrency` semaphore the incremental path uses). A bucket
  holding only immutable L0 records cannot have changed since it was folded
  (seal lemma above), so it is skipped with no record GET. Only a bucket whose
  listing contains a compaction record, a tombstone, or a rewrite record is
  classified and diffed. When nothing late has landed, the pass costs only
  the window LISTs and every unchanged sealed part is still carried forward
  by reference.
- **Diff and apply.** A triggered bucket is classified by the same
  commit/compaction/tombstone logic the incremental path applies
  (`Catalog::classify_bucket`), yielding what the bucket should currently
  contribute. That is compared, ignoring order, against what the in-progress
  entry set already reflects for that `(shard, hour)` (found by each entry's
  own `ingest_hour_bucket`; those entries were seeded from the previous fold's
  own output). If they differ — a late compaction supersedes L0 inputs
  previously folded in directly, or a late tombstone means the hour now
  contributes nothing — exactly that bucket's entries are replaced and every
  other entry is left untouched.
- **Dirty parts rebuild, never carry forward.** A previous HEAD's sealed part
  whose `[min_hour, watermark_hour]` range covers a changed hour is marked
  dirty and excluded from the by-content-hash carry-forward reuse: it always
  goes through the normal re-encode-and-PUT path. The changed content already
  yields a different blake3, so a dirty part is re-PUT under a new
  content-addressed key regardless; the explicit exclusion is a fail-safe that
  refuses to keep stale content even in a hash coincidence.
- **One CAS.** Reconcile findings fold into the same fold attempt's single
  HEAD `CasVersion` write. There is never a second CAS: HEAD remains the only
  mutable object and the only unit of atomic visibility.
- **Postings.** A reconcile that changed any hour forces a full postings
  rebuild this fold, since the forward postings merge assumes append-only,
  stable-ordinal growth that a supersession or removal breaks. (A change that
  introduces an L1 entry suppresses postings entirely, as elsewhere.)

Invariant this closes: a compaction record or retention tombstone landing in
an already-folded hour is now eventually applied — once that hour's own
bucket falls within `[watermark_hour_old - fold_reconcile_window_hours,
watermark_hour_old]` on some later fold — rather than never. The window is
on the *target hour bucket*, not on how recently the late record was
published: a compaction landing hours after its bucket sealed is caught on
the very next fold (this is the common case the window is sized for), while
a retention tombstone's bucket is typically far outside the window by the
time retention runs (tombstones normally have a retention period of days,
so this path is legal but uncommon in practice; see
`reconcile_applies_late_tombstone`). A late record whose bucket falls
outside the window is deliberately not picked up: a stated, bounded
staleness tradeoff, not a bug. (This closes a latent correctness gap that
predates the epic; see ADR-0063 Consequences.)

## Commit sequence (strict mode)

1. Pin the flush identity (above).
2. PUT data object with `PutMode::CreateIfAbsent`. A CRC32C checksum is
   computed and verified locally before upload as a pre-flight guard; it is
   not sent to the store, because no shipped backend accepts a wire-level
   upload checksum (`capabilities().upload_checksum == false` on S3;
   wire-level verification is pending issue #251). `AlreadyExists` is
   success: the key embeds the content hash, so the stored bytes are
   identical by construction.
3. PUT commit record with `PutMode::CreateIfAbsent`.
   - `AlreadyExists`: GET the record. Same `content_hash`: success (a
     previous attempt landed; ack path continues). Different: fatal
     split-brain; crash loudly. With identity pinned, this cannot fire on a
     benign retry.
4. Ack all requests in the flush with the commit token.

Crash between 2 and 3 leaves an orphan data object: invisible, GC-eligible
only after `grace + max_flush_lifetime` with a re-verify before delete
(ADR-0010 §11).

Commit record: `ravel.commit.v1.CommitRecord` protobuf: tenant_hash, signal,
shard, writer_id, epoch, seq, ingest_hour_bucket, object_key, object_size,
content_hash (32B), sample_count, series_count, min/max event ts, min/max
ingest ts, format_version, created_unix_ns.

`object_key` is informational. Readers MUST reconstruct the data key from
(tenant_hash, signal, shard, writer_id, epoch, seq, content_hash) and treat
any mismatch with the stored `object_key` as a fatal invariant breach
(ADR-0010 §7). After the suffix GET, the segment reader MUST verify footer
tenant_hash, shard, writer_id, epoch, seq against the commit record.

## Commit tokens

Token (opaque to clients): base64url of
`v2:<shard>:<writer_id>:<epoch>:<seq>:<ingest_hour_bucket>`.

A token fully determines its commit-record key. Ingest acks return one
token per shard the request's points flushed through; the HTTP/gRPC surface
carries them as a comma-separated list in `x-ravel-commit-token`.

## Snapshot resolution (Phase 1)

`Catalog::resolve(tenant, signal, range, min_tokens, now_ns) -> Snapshot`

1. Discover the commit keys for every shard 0..shard_count and every
   ingest_hour bucket overlapping `[range.start_ns - max_ingest_lag, now_ns +
   clock_skew_allowance]` (max_ingest_lag default 2 h, clock_skew_allowance
   default 5 m, config). Two traversals produce the identical key set
   (ADR-0056); resolve picks between them on the width of the listing suffix:
   - **Per-bucket loop** (narrow/warm windows): LIST
     `t/<th>/m/c/<shard>/<hour>/` per bucket (paginated; callers dedup keys).
     Cost `shard_count * hours`, one LIST per bucket, empty buckets included.
     Prunes best behind a folded snapshot watermark, which shortens the listed
     suffix to the post-watermark buckets.
   - **Prefix scan** (wide windows, at or above
     `prefix_list_crossover_requests` suffix buckets, default 720): one drained
     recursive LIST per shard over `t/<th>/m/c/<shard>/` (paginated), grouping
     the returned keys client-side by `(shard, ingest_hour)` and keeping the
     buckets in the window. Cost `O(objects / page_size)`, independent of
     window width, so an epoch-width window over a sparse tenant costs a
     handful of pages instead of one LIST per empty hour. The store's LIST
     takes only a prefix and a continuation token (no start-after), so the scan
     reads every key in the shard subtree, including hours the window excludes;
     those are dropped client-side.
   Both traversals then partition the keys identically (step 2 onward).
2. Partition the listed keys by shape (L0 commit record, compaction
   record, selective-erasure rewrite record, tombstone; ADR-0018, ADR-0019,
   ADR-0064, docs/compaction-retention-plan.md §3.5). A key matching none of
   the four shapes is a fail-loud error, not a skip. Decode all records.
   Cache decoded records keyed by FULL object key; validate
   tenant_hash/signal/shard fields against the expected values on every hit;
   bound the cache per tenant. Records are immutable and never invalidated,
   except: observing a tombstone for a bucket invalidates that bucket's
   cached commit and compaction records (the trigger ADR-0010 §10 promises).
3. Tombstone present: the bucket contributes nothing to the snapshot.
   Otherwise, build ONE unified exclusion mechanism over the bucket
   (ADR-0064 unifies rewrite supersession with compaction's, rather than
   running two parallel mechanisms):
   - An `excluded` set of L0 identities `(writer_id, writer_epoch,
     writer_seq)`, and a `superseded_records` set of whole compaction/rewrite
     record keys whose output parts are superseded.
   - Each **compaction record** contributes its input list to `excluded`.
   - Each **rewrite record** contributes its *effective* inputs to `excluded`
     exactly as a compaction record does: its own `inputs` when set, or --
     when it names a `superseded_record_key` instead -- the inputs of the
     compaction/rewrite record that key names, chased through any
     rewrite-of-a-rewrite chain until a record with `inputs` set directly is
     reached (a bounded, cycle-checked walk; an over-deep or cyclic chain is
     a typed error, never a hang). Every record a rewrite supersedes as a
     whole is added to `superseded_records`.
   - Include each compaction record's parts and each rewrite record's output
     parts as segment refs, filtered by per-part event bounds, UNLESS that
     record's key is in `superseded_records`. A superseded record's parts are
     never included: overlap harmlessness does NOT hold across a rewrite
     (the rewrite's output deliberately lacks records its predecessor's parts
     contain, so including both would resurrect erased records; ADR-0064
     decision 3 point 5). Rewrite output parts fold in as L1-equivalent
     entries, keyed by the rewrite's own `input_set_hash`.
   - Include any L0 record not in `excluded` normally, and raise an
     interlock-violation metric if its created_unix_ns postdates the newest
     compaction/rewrite record (it should have been sealed before that ran).
   Two compaction records in one bucket with different input_set_hash:
   include both parts sets and all L0s not covered by either (correct
   under overlap harmlessness; ADR-0018), and alarm loudly (§3.6 row 11).
4. Filter the remaining L0 commit records: [min_event_ts, max_event_ts]
   overlaps the query range.
5. For each `min_token`: reconstruct its commit key and GET it directly
   (never by re-listing). Present: ensure it is in the snapshot set (its
   event range might not overlap; include it anyway so read-your-write
   holds). Absent: fall back to GETting the bucket's compaction record(s)
   (cacheable, same as step 2) and check the token's writer identity
   against each record's input list. Found in an input list: satisfied via
   that record's parts. A rewrite record (ADR-0064) is treated the same:
   if the token's identity is among a live (non-superseded) rewrite's
   effective inputs, it is satisfied via that rewrite's erased output parts,
   and a rewrite-superseded compaction record's parts are never served. A
   tombstone present for the bucket: satisfied with zero segments (the data
   was retired, not lost). Neither found after one retry: error
   `unsatisfiable token`, surfaced as 5xx.
6. Attach pending selective-erasure predicates (ADR-0064 decision 2). Once
   per resolve -- independent of the per-bucket fan-out and of whether any
   physical rewrite has run -- LIST `t/<th>/<sig>/del/` exactly once. For
   every `.dreq` erasure request found, decode and structurally validate it
   and verify its observed key against its own identity fields (ADR-0010 §7),
   then attach it to the snapshot's `pending_erasure`. `.done` completion
   records (PII-free audit evidence, no predicate) are recognized and
   skipped; any other shape under `del/` is fail-loud layout drift. The
   directory is empty for the common no-erasure case, costing exactly one
   LIST and nothing more. This delivers the visibility bound: a `.dreq`
   durable before a resolve is always seen by that resolve, so a query whose
   snapshot resolves after the request ack can never return matching records
   -- attachment depends on nothing a later rewrite pass does. The scan /
   materialization layer (EJ-T3) is what filters query results against these
   predicates; resolution only discovers and attaches them.
7. Snapshot = the resulting segment set plus `pending_erasure`, pinned for
   the query lifetime; later commits, compactions, rewrites, erasure
   requests, or deletions do not affect a running query. A store NotFound on
   a pinned segment surfaces as SnapshotInvalidated; the frontend re-resolves
   and retries the query once (ADR-0010 §11).

`SegmentRef` carries a level discriminator. L0 refs keep the existing
commit-record provenance fields. L1 part refs carry (ingest_hour,
input_set_hash, part_index, content_hash, object_size, event bounds) and
reconstruct the part key from those fields rather than trusting any
stored string (ADR-0010 §7, same discipline as the L0 data key). Snapshot
ordering stays a deterministic total order across mixed levels: L1 parts
sort into the same (provenance, shard, writer_id) tiebreak chain as L0
segments (see "Cross-segment duplicate samples" below) using the
compaction record's created_unix_ns in place of a commit record's, and
input_set_hash plus part_index as the final tiebreaks in place of
writer_id/epoch/seq, since a part has no writer identity of its own.

The listing window is sound because admission bounds event-time skew
(ADR-0010 §8): points with `event_ts > ingest_ts + max_future_skew`
(default 10 m) or `event_ts < ingest_ts - max_ingest_lag` are rejected at
ingest. Late arrivals within bounds land in the current ingest hour and
stay discoverable via the `now`-anchored upper bound.

The window's upper bound is anchored on `now_ns`, not on `range.end_ns`, so a
client-supplied `range.start_ns` near the epoch spans one bucket per (shard,
ingest_hour) from that start all the way to the current hour, regardless of
how narrow `range.end_ns` is. The per-bucket loop's cost for that is
`shard_count * hour_buckets`, growing by one every wall-clock hour; the
prefix scan collapses it to `O(objects / page_size)`, which is exactly why
resolve switches to the prefix scan for wide windows (ADR-0056).

`Catalog::estimated_catalog_requests` still reports the per-bucket worst case
`shard_count * hour_buckets + SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND` (issue
#635, ADR-0044 decision 3): it is a true upper envelope of whichever
traversal runs (the prefix scan issues strictly fewer requests than the
per-bucket loop it replaces) and is threaded into the cost accounting. It is
no longer the admission gate for wide windows. Instead:

- A window whose per-bucket cost would exceed
  `CatalogConfig::max_catalog_list_requests` (default 100,000) is routed to
  the prefix scan rather than refused, because the prefix scan does not
  amplify one-object-worth-of-data into thousands of empty LISTs.
- The prefix scan carries a runtime LIST cap at the same ceiling: it aborts
  with `WindowTooWide` before issuing a page that would take it over, so a
  single resolve still never issues more than `max_catalog_list_requests`
  catalog LISTs. Only a scan whose *actual object volume* is unsustainable is
  refused; a wide-but-sparse window is served.

The refusal is never a silent narrowing to a partial result (exact semantics
by default). The typed error carries the count and the limit so the caller
can narrow its own range and retry; it maps to HTTP 422 on every query
endpoint.

## Snapshot resolution (Phase 2)

Once folding (docs/metric-index-plan.md 4) is live, step 1 of the Phase 1
algorithm above is replaced by a snapshot-backed lookup that degrades to
Phase 1 listing on any index failure; min-token resolution and snapshot
pinning are unchanged:

1. Attempt snapshot read: GET HEAD (cached with a short TTL, default 30 s,
   config `head_cache_ttl`). Decode, validate signal/shard_count against
   the catalog's own config (a shard_count mismatch is a loud error:
   ADR-0010 §9 makes changing it forbidden) and validate tenant_hash
   against the requesting tenant (per ADR-0050 §2, a hard
   `CatalogError::FieldMismatch`, never a fallback: an isolation breach,
   not a performance event). Fetch parts not in the decoded-part cache
   (immutable, keyed by part key, verified against HEAD's blake3 before
   decode, bounded per tenant by `snapshot_cache_parts`); a postings
   object's tenant_hash is checked the same hard-fail way before its
   entries are trusted.
2. On any other failure in step 1 (HEAD absent, corrupt, part missing or
   hash-mismatched, postings content-hash or entry-count mismatch): log,
   fall back to Phase 1 full listing for the whole window. Queries never
   fail and never silently narrow because of index state. A part GET
   NotFound races GC of a just-superseded part; re-read HEAD once before
   falling back. tenant_hash and shard_count mismatches are excluded from
   this fallback: both fail the query instead (previous step, and ADR-0010
   §9).
3. With a snapshot at watermark W: for window buckets with `hour <= W`,
   take entries from the parts (hour-major sort makes this a contiguous
   range scan per part), filter by event-time overlap exactly as Phase 1
   does. For window buckets with `hour > W`, LIST and GET-decode as in
   Phase 1.
4. min-token resolution: unchanged, exact commit-key GETs, never through
   the snapshot.
5. Build `SegmentRef`s from snapshot entries by reconstructing the data key
   from identity fields, the same reconstruct-don't-trust rule as commit
   records (ADR-0010 §7); dedup by data key across the snapshot/listing/
   token sources, sort by the dedup total order ("Cross-segment duplicate
   samples" below), return the pinned `Snapshot`.

Every LIST call on the resolve path, in both phases, asserts that each
returned key begins with the requesting tenant's prefix (per ADR-0050 §2,
the same hard `CatalogError::FieldMismatch` as a tenant_hash mismatch): a
backend or key-layout bug that hands back a foreign key is an isolation
breach, never a silently dropped or served key. Both this and the HEAD/
postings tenant_hash checks above increment
`ravel_catalog_isolation_breach_total`, rendered at `/metrics` beside the
existing `ravel_catalog_interlock_violations_total` and
`ravel_catalog_compaction_input_set_conflicts_total` anomaly counters
(default alert rule: docs/guides/operations.md). Unlike those two, which
tally a harmless-overlap anomaly the query still resolves past, every
increment of the isolation-breach counter corresponds to a failed query.

Soundness rests entirely on the seal lemma above: for sealed buckets, the
fold's LIST equals any later LIST, so serving them from the snapshot
returns exactly what Phase 1 listing would; open buckets keep Phase 1
listing verbatim, so the window formula, the event-overlap filter, and the
admission-time skew bounds that make it sound (ADR-0010 §8) are untouched.
An index failure degrades performance only, never correctness: this is a
derived, rebuildable index, never a durability or correctness dependency.

(docs/metric-index-plan.md 5.1, ADR-0020.)

## Query cost accounting

`resolve` and `resolve_pruned` account for every store call and cache access
they make on the caller's behalf when driven through their
`*_with_accounting` counterparts (`resolve_with_accounting`,
`resolve_pruned_with_accounting`; ADR-0044, issue #421). The plain `resolve`
and `resolve_pruned` entry points are unchanged and pass a discarded
`QueryAccounting` handle, so every existing caller keeps its current
signature and behavior.

`Catalog::guarded_get` and `Catalog::guarded_list_all` are the only two
places a resolve issues an S3 request (both Phase 1 listing and Phase 2
snapshot reads funnel through them), so accounting is recorded there, never
at a call site: one `AccountedOp::Get` per GET attempt and one
`AccountedOp::List` per LIST page, both unconditional; bytes are added only
for a successful GET, matching `InstrumentedStore`'s convention that a
failed request and a LIST move no bytes. `resolve`'s two funnels never issue
a HEAD.

All five caches (`RecordCache`, `CompactionRecordCache`, `HeadCache`,
`PartCache`, `PostingsCache`) record a cache hit or miss on every lookup and
add the cached object's original wire-encoded byte size on a hit; a miss
does not double-count bytes the funnel GET that filled the cache already
recorded. `HeadCache` additionally carries a process-wide capacity bound
(`head_cache_capacity`, default 10,000 (tenant, signal) entries, FIFO
eviction), closing the one cache of the five that previously had a TTL but
no bound on the number of tenants it could grow to hold.

## MVCC rules

- Snapshots are logical sets of immutable segments. Compaction (Phase 2)
  publishes a transaction adding outputs and removing inputs; running
  queries keep their pinned set.
- GC deletes an object only when all hold: unreachable from any snapshot
  within the protection horizon (>= max_query_duration + grace), not
  lease-protected, and older than grace + max_flush_lifetime; commit-record
  absence (for orphans) is re-verified immediately before each delete.
- Deletion (retention/tombstone) is a durable transaction that excludes
  segments from new snapshots first; physical removal follows via GC.

## Cross-segment duplicate samples

Queries dedup by (series_id, ts) under the provenance order
(commit created_unix_ns, writer_epoch, writer_seq, in-page index); the
greatest wins. Values compare by f64 bit pattern (ADR-0010 §5).

That provenance order is not total across segments: two same-shard segments
from different writers can tie on (created_unix_ns, writer_epoch, writer_seq)
because seq is monotonic only per (writer_id, epoch, shard) (ADR-0010 §3). To
make the resolved snapshot's segment order a deterministic total order, the
catalog sort appends shard then writer_id as final tiebreaks after the
provenance components. writer_id is a per-segment identity component, so no
two distinct segments can tie on the full key; the order never depends on
arrival, insertion, or map iteration order.
