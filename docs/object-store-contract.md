# Object Store Contract

Rust trait: `ravel_object_store::ObjectStoreBackend`. All Ravel durability
arguments are made against THIS contract, never against a specific vendor.

## Operations

```rust
#[async_trait]
pub trait ObjectStoreBackend: Send + Sync + 'static {
    /// Write a complete object.
    async fn put(&self, key: &str, data: Bytes, opts: PutOptions) -> Result<PutOutcome, StoreError>;
    /// Read whole object or a byte range. Suffix(n) = last n bytes.
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError>;
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError>;
    /// Non-recursive=false prefix listing, lexicographic order, strongly consistent.
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError>;
    async fn delete(&self, key: &str) -> Result<(), StoreError>; // idempotent: NotFound => Ok
    fn capabilities(&self) -> Capabilities;
}

pub enum PutMode { Overwrite, CreateIfAbsent, CasEtag(Etag) }
pub struct PutOptions { pub mode: PutMode }
pub enum GetRange { Full, Range(u64, u64) /* [start, end) */, Suffix(u64) }
pub struct PutOutcome { pub etag: Etag }
pub struct GetOutcome { pub data: Bytes, pub etag: Etag, pub total_size: u64 }
pub struct ObjectMeta { pub key: String, pub size: u64, pub etag: Etag, pub last_modified_unix_ms: i64 }
```

`StoreError` variants (exhaustive for callers' retry logic):
`NotFound`, `AlreadyExists`, `PreconditionFailed`, `Throttled { retry_after }`,
`Timeout`, `Corrupted(msg)` (checksum/range mismatch), `Transient(msg)`,
`Permanent(msg)`, `Unsupported(capability)`.

Retry classification: `Throttled`, `Timeout`, `Transient` are retryable with
jittered exponential backoff. `AlreadyExists` on `CreateIfAbsent` is a
*protocol signal*, not an error to retry.

## Mandatory capabilities (production)

| Capability | Used by |
|---|---|
| Strongly consistent create + read-after-write | commit visibility |
| Strongly consistent list-after-write | commit discovery |
| `CreateIfAbsent` conditional put | commit records, idempotency |
| Etag/generation CAS put | catalog HEAD pointers |
| Byte-range + suffix reads | footer-first segment reads |
| Object checksums on upload | L0 integrity |
| Prefix listing | discovery, GC |
| Multipart upload | large L1/L2 segments (Phase 2) |

Optional (degrade gracefully): batch delete, lifecycle expiration (GC falls
back to explicit deletes), SSE/KMS headers.

AWS S3 since Dec 2020 provides strong read-after-write and list consistency;
S3 conditional writes (If-None-Match/If-Match, 2024) provide CreateIfAbsent
and CAS. GCS: generation preconditions. Azure: etags + lease. MinIO: full
support. Backends missing a mandatory capability fail startup.

## Implementations

1. `MemoryStore`: reference implementation; strong consistency, etags as
   monotonic counters. The semantics oracle for tests.
2. `FaultStore<S>`: wraps any backend; deterministic seeded fault plan
   injecting: timeouts, throttling, partial-write-then-error (object must NOT
   become visible), stale list (configurable bounded staleness for testing
   detection; the production contract forbids it), failed conditional writes,
   duplicate delivery (op applied, error returned, modeling ack loss),
   reordered completion, corrupt range responses, `NotFound` blips,
   transient/permanent errors. Every fault site counted; plans scriptable
   per-operation-index and per-key-pattern.
3. `S3Store`: `object_store` crate adapter (AWS S3 + MinIO via endpoint
   override). Maps `PutMode::Create` → CreateIfAbsent, `PutMode::Update` →
   CAS, `GetRange::Suffix` → suffix range.

## Rules for callers

- Never infer visibility from a successful data PUT; only commit records
  confirm publication (ADR-0002).
- All GETs of format-bearing bytes verify embedded checksums; etag mismatch
  between ranged reads of the same object aborts with `Corrupted`.
- Every caller passes a deadline; the trait impls honor cancellation by drop.
