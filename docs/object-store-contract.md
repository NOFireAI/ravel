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
| Multipart upload | multipart | `--mode maintain` only (large L1/L2 segments) |

Compaction is the only path that writes multipart objects, so `multipart` is
required for maintain mode and for no other mode. It is not in
`Capabilities::mandatory()`; `required_capabilities(Mode::Maintain)` adds it.

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

## Implementations

1. `MemoryStore`: reference implementation and semantics oracle; strong
   consistency, monotonic etags/versions, injectable clock. Note the fake
   clock defaults to 0: GC-grace tests must set it explicitly.
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
against all three. The `S3Store` case is gated on `RAVEL_MINIO_URL`; the CI
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
