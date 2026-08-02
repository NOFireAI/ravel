# Object Store Contract

Rust trait: `ravel_object_store::ObjectStoreBackend`. All Ravel durability
arguments are made against THIS contract, never against a specific vendor.
Amended by ADR-0010 §12.

## Operations

```rust
#[async_trait]
pub trait ObjectStoreBackend: Send + Sync + 'static {
    /// Write a complete object.
    async fn put(&self, key: &str, data: Bytes, opts: PutOptions) -> Result<PutOutcome, StoreError>;
    /// Read whole object or a byte range. Suffix(n) = last n bytes, n > 0.
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError>;
    /// Begin a multipart upload. Only backends reporting `multipart` provide
    /// it; the default implementation refuses. See "Multipart upload" below.
    async fn put_multipart<'a>(&'a self, key: &str)
        -> Result<Box<dyn MultipartUpload + 'a>, StoreError>;
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError>;
    /// Paginated recursive prefix listing, lexicographic order.
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError>;
    /// One-level listing: entries directly under prefix plus common sub-prefixes.
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError>;
    async fn delete(&self, key: &str) -> Result<(), StoreError>; // idempotent: NotFound => Ok
    fn capabilities(&self) -> Capabilities;
}

pub enum PutMode { Overwrite, CreateIfAbsent, CasVersion(Version) }
pub struct PutOptions { pub mode: PutMode, pub checksum: Option<UploadChecksum> }
pub enum UploadChecksum { Crc32c(u32) /* extend per backend */ }
pub enum GetRange { Full, Range(u64, u64) /* [start, end) */, Suffix(u64) }
pub struct PutOutcome { pub etag: Etag, pub version: Version }
pub struct GetOutcome { pub data: Bytes, pub etag: Etag, pub version: Version, pub total_size: u64 }
pub struct ObjectMeta { pub key: String, pub size: u64, pub etag: Etag, pub version: Version, pub last_modified_unix_ms: i64 }
pub struct ListPage { pub objects: Vec<ObjectMeta>, pub next: Option<PageToken> }
pub struct DelimitedList { pub objects: Vec<ObjectMeta>, pub common_prefixes: Vec<String> }

#[async_trait]
pub trait MultipartUpload: Send {
    /// Append one part. `checksum` is a local pre-flight (see below).
    async fn put_part(&mut self, data: Bytes, checksum: Option<UploadChecksum>) -> Result<(), StoreError>;
    /// Publish every part so far as one object, atomically.
    async fn complete(&mut self) -> Result<PutOutcome, StoreError>;
    /// Discard the upload; the object must not exist afterwards.
    async fn abort(&mut self) -> Result<(), StoreError>;
}
```

`Etag` is content identity (equality checks). `Version` is an opaque
precondition token for CAS: S3 etag, GCS generation, Azure etag. The two
coincide on S3 and differ elsewhere; commit-protocol code uses only
`Version` for CAS and only `Etag` for content-identity assertions.

`StoreError` variants (exhaustive for callers' retry logic):
`NotFound`, `AlreadyExists`, `PreconditionFailed`, `AccessDenied`,
`Throttled { retry_after_ms }`, `Timeout`, `Corrupted(msg)` (checksum or
range mismatch), `InvalidRange(msg)`, `Transient(msg)`, `Permanent(msg)`.

Retry classification: `Throttled`, `Timeout`, `Transient` are retryable with
jittered exponential backoff. `AlreadyExists` on `CreateIfAbsent` is a
*protocol signal*, not an error to retry. `AccessDenied` is permanent and
alerts differently (misconfigured credentials or prefix policy).

### Semantics adapters MUST honor

- Conditional-put failure maps by mode: under `CreateIfAbsent` a
  precondition failure surfaces as `AlreadyExists`; under `CasVersion` as
  `PreconditionFailed`. A conformance test asserts both against real S3
  and MinIO (the memory oracle alone cannot catch a uniform mapping).
- Concurrent conditional writes racing the same key may surface as a
  transient conflict; after retry the loser must land on
  `AlreadyExists`/`PreconditionFailed` per mode.
- Listing is paginated (S3 pages at 1000 keys). Cross-page guarantee: any
  key created before the first page request is returned; keys created
  during the scan may or may not appear; a key MAY appear more than once
  and callers MUST dedup by key.
- `Suffix(0)` and zero-length `Range` are `InvalidRange`.
- `Range(start, end)` is half-open; the HTTP Range header is inclusive, so
  adapters emit `bytes=start-(end-1)`. Boundary conformance tests required
  (exact object/section/page ends).
- `last_modified` may have 1-second granularity. Never order commits by it;
  it exists for GC age checks only.

## Mandatory capabilities (production)

Every mode requires these; production startup fails if any is false.

| Capability | Flag | Used by |
|---|---|---|
| Strongly consistent create + read-after-write | consistent_read | commit visibility |
| Strongly consistent list-after-write | consistent_list | commit discovery |
| `CreateIfAbsent` conditional put | create_if_absent | commit records, data objects |
| Version CAS put | cas_version | catalog HEAD pointers |
| Byte-range + suffix reads | suffix_range | footer-first segment reads |
| Paginated prefix listing | prefix_list | discovery, GC |

Each row is a `Capabilities` flag, and the set is exactly
`Capabilities::mandatory()`; `ravel_server::store::check_capabilities`
enforces it before the backend is used. Optional: batch delete, lifecycle
expiration, SSE/KMS headers.

### Mode-conditional capabilities

| Capability | Flag | Required by |
|---|---|---|
| Multipart upload | multipart | `--mode maintain` (forward-looking, see below) |

`multipart` is not in `Capabilities::mandatory()`;
`required_capabilities(Mode::Maintain)` adds it, and no other mode requires it.
That gate is **forward-looking**, not a description of current behavior: today
`ravel-maintain` writes its compaction outputs as single-PUT content-addressed
objects (its `build.rs` records this, citing issue #243), and no production
caller invokes `put_multipart` yet. The maintain-mode requirement stands so
that once compaction does stream large L1/L2 segments as multipart uploads, the
backend is already known to serve the create/upload-part/complete/abort
sequence rather than discovering the gap at runtime. `MemoryStore` and
`S3Store` both report `multipart: true` and implement the sequence (issue
#243), so `--mode maintain` starts against the memory oracle and against any
S3-compatible endpoint whether or not any caller exercises the path yet.
(`S3Store::put` does take an internal multipart path above its threshold, but
that is a size-driven implementation detail of `put`, not a caller reaching for
`put_multipart`; see "When `put()` uses it" below.)

### Multipart upload

`ObjectStoreBackend::put_multipart(key)` returns a `MultipartUpload` handle: a
sequence of `put_part` calls followed by exactly one `complete` or `abort`. The
flag and the method must agree — a backend reporting `multipart: false` MUST
refuse `put_multipart` with `Permanent`, which is what the default trait
implementation does, and the contract suite asserts both directions.

**Part bounds.** Enforced locally by every backend, at the call that violates
them, rather than deferred to the server's `CompleteMultipartUpload`:

| Rule | Value | Constant |
|---|---|---|
| Minimum size, any part but the last | 5 MiB | `MULTIPART_MIN_PART_SIZE` |
| Minimum size, last part | 1 byte (no part may be empty) | — |
| Maximum parts per upload | 10 000 | `MULTIPART_MAX_PARTS` |

These are S3's own limits. A short part is legal while it is the last one, so
it is the *next* `put_part` that fails, naming the part that became non-final.
A `complete` with zero parts fails. Every one of these failures is
`StoreError::Permanent` (caller misuse, never retryable) and leaves no object.

**Ordering.** Parts are ordered by the sequence of `put_part` calls, not by the
order they finish; an implementation may upload them concurrently.

**Visibility and abort.** Nothing is readable at `key` until `complete`
returns `Ok`; an incomplete, aborted, dropped, or crashed upload never becomes
a visible object, not even a truncated one (S3's own multipart guarantee, and
the reason compaction's crash story degrades to wasted work rather than corrupt
state). `abort` releases the uploaded parts. It is best effort against the
server but final for the handle: an upload S3 never heard the abort for leaves
orphaned parts, which are billed until a bucket lifecycle rule reaps them, and
never a readable object. Operators SHOULD configure such a rule
(`AbortIncompleteMultipartUpload`) on Ravel buckets.

**Retry and failure.** Nothing retries internally beyond what `object_store`'s
client already does per request. A `put_part` that still fails *poisons the
handle*: the first failure surfaces the classified `StoreError` a `put` would
(so the original cause stays visible), but the handle is now dead, and every
later `put_part` and `complete` fails with a non-retryable `Permanent` error.
The documented recovery is to `abort` and restart the whole upload, never to
retry the part. This is not a Ravel policy choice but what `object_store`'s S3
upload permits: `S3MultiPartUpload::put_part` fixes the part's index
synchronously at call time and `complete` errors unless it holds exactly that
many parts, so a retried part lands at a *new* index and the hole the failed
part left can never be filled (retrying it is the live-lock issue #296 fixes). A
part-sequence violation (an empty part, or a non-final part below the minimum)
poisons the handle the same way (issue #297): a later `complete` errors rather
than publishing a truncated object. A checksum mismatch is the one *recoverable*
rejection: it does not poison, so the caller may re-send the same bytes with the
correct checksum. `complete` and `abort` consume the handle logically; a second
call on the same handle fails with `Permanent` rather than re-issuing a request
against a spent upload id. `abort` stays callable on a poisoned handle, so the
caller can still release the uploaded parts.

**Write mode.** `complete` publishes unconditionally, exactly like
`PutMode::Overwrite`. There is no multipart `CreateIfAbsent` or `CasVersion`:
`object_store` 0.14's `PutMultipartOptions` carries tags, attributes, and
extensions, with no `PutMode`, so no precondition can ride on
`CompleteMultipartUpload`. Callers needing create-once semantics must write
keys that are unique by construction; Ravel's data objects and compaction
parts are content-addressed, so they are.

**When `put()` uses it.** `S3Store::put` switches from one PUT to a multipart
upload above `s3::MULTIPART_THRESHOLD` (16 MiB), cutting the payload into
`s3::MULTIPART_PART_SIZE` (8 MiB) parts with at most 4 in flight. The
threshold is two whole parts, so the multipart path never produces a
degenerate single-part upload, and every part but the last is exactly 8 MiB —
uniform non-final part sizes, which the strictest S3-compatible backends (R2)
require. 8 MiB parts keep the 10 000-part ceiling at 80 GiB, far above any
object Ravel writes. The switch is invisible to callers: same `PutOutcome`,
same bytes back. It applies to `Overwrite` only; a `CreateIfAbsent` or
`CasVersion` put stays on the single-PUT path at every size (bounded by S3's
5 GiB single-request limit) rather than silently dropping its precondition.
`MemoryStore::put` has no threshold: there is no transport to chunk.

**Checksum coverage.** `put_part`'s optional `UploadChecksum` is verified
per part, before the part is sent, with exactly the reach `PutOptions::checksum`
has on the same backend: a real check on `MemoryStore`, a local pre-flight
against the caller's buffer on `S3Store` (see "Upload checksums" — nothing can
be put on the wire through `object_store` 0.14). A mismatch fails that
`put_part` with `Corrupted`, does not count as a part, and leaves the upload
open, so the caller may re-send the same bytes with a correct checksum. There
is **no whole-object checksum** for a multipart upload: `complete` takes no
checksum argument, and the object never exists as one buffer to digest. Ravel's
integrity guarantee for these objects is therefore read-time only, from the
footer/section/page crc32c hierarchy (docs/segment-format.md), same as for any
other object.

**Observability and faults.** `InstrumentedStore` passes `put_multipart`
through uncounted: a multipart upload is a handle, not a call, and no `StoreOp`
describes its parts. `put()`'s own above-threshold multipart path is counted,
as one `put`, because that is what the caller invoked. `FaultStore` likewise
passes through: multipart operations are not fault sites today, so no fault
test can be built on one (like reordered completion). Both gaps are
observability/testing gaps, not correctness ones.

### Upload checksums (best effort, never startup-gating)

`upload_checksum` is a `Capabilities` flag, but it is NOT mandatory and no
mode may require it. `PutOptions::checksum` is honored on a best-effort
basis: backends that can put a caller-supplied CRC32C on the wire do, and
those that cannot report `upload_checksum: false` and are still startable.

This is a limitation of Ravel's S3 client library, not a gap in any
particular server. `object_store` 0.14's `AmazonS3` client exposes no
per-request checksum hook and no way to attach a caller-supplied precomputed
digest to an outgoing request (its only knob,
`AmazonS3Builder::with_checksum_algorithm`, is whole-client, SHA-256 or
CRC64NVME only, and always computes the digest itself). So `S3Store` reports
`upload_checksum: false` against every S3-compatible endpoint, MinIO and AWS
S3 included, regardless of what those servers support. Requiring the flag at
startup therefore made the only durable backend permanently unusable in
every mode rather than catching a real regression (issue #251).

What still holds with the flag false:

- `put()` runs the CRC32C as a local pre-flight against its input buffer on
  every backend, rejecting a caller/payload mismatch with `Corrupted` before
  any network call. The contract suite asserts this
  (`assert_upload_checksum_verification`).
- Read-time integrity is the real backstop against corrupted bytes
  surviving: the footer/section/page crc32c hierarchy
  (docs/segment-format.md) verifies data on every read of format-bearing
  bytes, independent of whether a wire-level upload checksum existed.

Upload checksums are CRC32C-class integrity checks against transport
corruption; they do not verify blake3. blake3 in commit records is an
idempotency and identity discriminator, not a transport check.

### Backend support notes

AWS S3 since Dec 2020 provides strong read-after-write and list
consistency; S3 conditional writes (If-None-Match/If-Match) provide
CreateIfAbsent and CAS. GCS: generation preconditions. Azure: etags +
leases. MinIO supports the full mandatory set; like AWS S3, its
server-side upload checksums are unreachable through `object_store`'s
client, which is why `S3Store` reports `upload_checksum: false` for both.

## Runtime qualification (executable contract)

`Capabilities` is self-reported: a backend declares `consistent_list: true`
because its adapter believes the vendor provides it, not because anything
checked. Adversarial review finding S5-20 is that nothing did — a backend
that advertises S3 compatibility but actually delivers eventually consistent
listing was trusted silently, and the resulting failures at the commit layer
looked like data loss rather than a misconfigured store.

`crates/ravel-object-store/src/conformance.rs` is this contract turned into
a suite that empirically probes a live backend rather than reading its
declared flags. `run_conformance_suite(store, scratch_prefix)` runs, under a
throwaway key prefix:

- `ConditionalWriteCreateIfAbsent` — two concurrent `CreateIfAbsent` puts to
  the same key: exactly one must win and the loser must observe
  `AlreadyExists` (the losing-writer outcome the "Semantics adapters MUST
  honor" section above requires).
- `ConditionalWriteCasVersion` — a `CasVersion` put against a stale version
  must fail `PreconditionFailed`, not silently overwrite.
- `ConsistentReadAfterWrite` — a `get` immediately following a `put` returns
  the just-written bytes, repeated over several keys to catch a
  read-your-writes gap that only shows up intermittently.
- `ConsistentListAfterWrite` — a `list` immediately following a `put`
  includes the new key, repeated the same way, to catch eventual-consistency
  listing rather than trusting the `consistent_list` flag.

Each probe returns a `ProbeResult` naming which `Property` it checked, so a
failure reads "this backend cannot do conditional writes" or "this backend's
listing is eventually consistent" instead of a bare pass/fail — an operator
does not have to guess which mandatory capability the backend actually
lacks.

ADR-0050 section 6 also names cross-page listing consistency and
multipart-complete visibility as probes for this suite; neither is
implemented yet. `CONFORMANCE_SUITE_VERSION` exists precisely so a later
addition can be told apart from the four probes qualifying a bucket today.

This is a runtime, once-per-bucket check, not a replacement for the
compile-time contract suite below: `tests/contract.rs` is a development-time
proof that each adapter *implementation* honors the trait, run in CI against
all three backends including a real MinIO endpoint. `conformance.rs` is an
operator-facing probe of one specific *deployment* — the actual configured
endpoint and bucket — because the adapter can be correct while the vendor
serving it is not (a misconfigured storage class, a proxy in front of the
bucket, a non-S3 vendor's compatibility gap).

`ravel-cli store qualify` runs this suite against the backend configured by
the CLI's usual `--store`/`RAVEL_S3_*` flags and, on a full pass, writes a
JSON record to `sys/qualification` via `CreateIfAbsent`:

```json
{
  "suite_version": 1,
  "backend_identity": "s3://<bucket>@<endpoint>",
  "qualified_unix_ns": 1234567890000000000,
  "passed_properties": ["conditional_write_create_if_absent", "..."]
}
```

`CreateIfAbsent` makes qualification once-per-bucket: a second `store
qualify` run against an already-qualified bucket leaves the existing record
untouched and reports it instead of overwriting it, per ADR-0050 section 6.
A failing run writes nothing new to `sys/qualification`; its process exit
names every failing property. The command only ever writes under
`sys/qualify/<run-id>/` (a handful of small scratch objects the suite does
not delete afterward — each run's key is unique, so this is unbounded
untracked storage a runbook should sweep periodically, not a correctness
issue) and the single `sys/qualification` key; it never reads, lists, or
writes any tenant-prefixed key, so it is safe to run against a bucket that
already holds production data.

## Implementations

1. `MemoryStore`: reference implementation and semantics oracle; strong
   consistency, monotonic etags/versions, injectable clock. Note the fake
   clock defaults to 0: GC-grace tests must set it explicitly. Its multipart
   upload buffers parts in the handle (nothing to chunk in process) but
   enforces the same part bounds and produces exactly the object a single
   `put` of the concatenated parts would, from the same etag/version counter.
2. `FaultStore<S>`: wraps any backend; deterministic seeded fault plan
   injecting: timeouts, throttling, partial-write-then-error (object must
   NOT become visible), failed conditional writes, duplicate delivery (op
   applied, error returned, modeling ack loss), corrupt range responses,
   etag change between reads (real bytes under a new etag, so a
   snapshot-pinning caller must abort), `NotFound` blips, transient/permanent
   errors. Every fault site counted; plans scriptable per-operation-index and
   per-key-pattern, and as ordered per-key sequences (a distinct outcome on
   each successive matching call). Reordered completion is deliberately NOT
   implemented: it is an aspiration recorded in the fault module docs, not an
   injectable fault, so no reordering test can be built on this store today.
3. `S3Store`: `object_store` crate adapter (AWS S3 + MinIO via endpoint
   override), honoring every MUST above.

### Instrumentation decorator

`InstrumentedStore<S>` wraps any backend and counts, per operation, calls, `ok`
count, failures by `StoreError` variant, bytes (returned for `get`, offered for
`put`), and a fixed-bucket latency histogram, read as a snapshot off a shared
`StoreMetrics` handle. Observability only, never correctness-bearing, and a
zero behavior change: every method delegates and forwards its result verbatim,
and `capabilities()` passes straight through, so the startup gate still sees
the wrapped backend's own declaration. `ravel-server`'s `build_store` wraps
whichever backend it built, unconditionally in every mode, and the contract
suite runs its full assertion set through the decorator to prove transparency.

The contract suite in `crates/ravel-object-store/tests/contract.rs` runs
against all three, multipart included (`assert_multipart_upload` is part of
`run_contract_suite`, written against the trait so it holds for the oracle, the
wrappers, and a real endpoint alike). Two multipart assertions are S3-shaped and
run only against a live endpoint: the composite `"<digest>-<partcount>"` ETag,
which proves the parts really went out as parts rather than being buffered into
one PUT, and `put()`'s own threshold switch. The `S3Store` case is gated on
`RAVEL_MINIO_URL`; the CI
`object-store-contract` job (`.github/workflows/ci.yml`) stands up MinIO,
creates the bucket, sets that variable, and asserts the gated test executed
rather than skipping. This job is required: S3 is the only durable backend,
so an adapter regression must fail CI.

## Rules for callers

- Never infer visibility from a successful data PUT; only commit records
  confirm publication (ADR-0002).
- All GETs of format-bearing bytes verify embedded checksums; etag
  inequality between ranged reads of the same immutable object aborts with
  `Corrupted` (data objects are created with CreateIfAbsent, so rewrites
  cannot produce differing content for one key).
- Every caller passes a deadline; trait impls honor cancellation by drop.
