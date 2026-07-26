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

| Capability | Flag | Used by |
|---|---|---|
| Strongly consistent create + read-after-write | consistent_read | commit visibility |
| Strongly consistent list-after-write | consistent_list | commit discovery |
| `CreateIfAbsent` conditional put | create_if_absent | commit records, data objects |
| Version CAS put | cas_version | catalog HEAD pointers |
| Byte-range + suffix reads | suffix_range | footer-first segment reads |
| Object checksums on upload | upload_checksum | L0 integrity |
| Paginated prefix listing | prefix_list | discovery, GC |
| Multipart upload | multipart | large L1/L2 segments (Phase 2) |

Each row is a `Capabilities` flag; production startup fails if a mandatory
flag is false (multipart becomes mandatory in Phase 2). Optional: batch
delete, lifecycle expiration, SSE/KMS headers.

Upload checksums are CRC32C-class integrity checks against transport
corruption; they do not verify blake3. blake3 in commit records is an
idempotency and identity discriminator; read-time integrity comes from the
footer/section/page crc32c hierarchy (docs/segment-format.md).

AWS S3 since Dec 2020 provides strong read-after-write and list
consistency; S3 conditional writes (If-None-Match/If-Match) provide
CreateIfAbsent and CAS. GCS: generation preconditions. Azure: etags +
leases. MinIO: full support.

## Implementations

1. `MemoryStore`: reference implementation and semantics oracle; strong
   consistency, monotonic etags/versions, injectable clock. Note the fake
   clock defaults to 0: GC-grace tests must set it explicitly.
2. `FaultStore<S>`: wraps any backend; deterministic seeded fault plan
   injecting: timeouts, throttling, partial-write-then-error (object must
   NOT become visible), failed conditional writes, duplicate delivery (op
   applied, error returned, modeling ack loss), reordered completion,
   corrupt range responses, `NotFound` blips, transient/permanent errors.
   Every fault site counted; plans scriptable per-operation-index and
   per-key-pattern.
3. `S3Store`: `object_store` crate adapter (AWS S3 + MinIO via endpoint
   override), honoring every MUST above.

## Rules for callers

- Never infer visibility from a successful data PUT; only commit records
  confirm publication (ADR-0002).
- All GETs of format-bearing bytes verify embedded checksums; etag
  inequality between ranged reads of the same immutable object aborts with
  `Corrupted` (data objects are created with CreateIfAbsent, so rewrites
  cannot produce differing content for one key).
- Every caller passes a deadline; trait impls honor cancellation by drop.
