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
    /// As list, but begins strictly after start_after in key order.
    async fn list_after(&self, prefix: &str, start_after: Option<&str>, page: Option<PageToken>)
        -> Result<ListPage, StoreError>;
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

### HTTP client timeouts (S3 adapter)

The `S3Store` adapter builds its `object_store` `AmazonS3` client on an
explicit `ClientOptions` (`S3HttpConfig`), so the request, connect, and
pool-idle timeouts are deliberate values this repo chose, not whatever the
dependency happens to default to. The `S3HttpConfig` doc comment carries the
per-value reasoning; the values (defaults, overridable via
`S3Store::with_http_config`) are:

| Value | Configured | Inherited default it replaces |
|---|---|---|
| Request timeout (connect → body complete) | 20 s | 30 s |
| Connect timeout (TCP + TLS) | 3 s | 5 s |
| Pool idle timeout | 30 s | ~90 s (reqwest, uncapped) |
| HTTP/2 keep-alive interval / ack timeout | 10 s / 10 s, while-idle on | disabled |

The request timeout must stay above the tail of the largest single request on
the wire (an 8 MiB multipart part or a whole-object GET) even on a badly
degraded connection, so it is set conservatively at 20 s pending a tail-latency
measurement to tighten it; a timeout below the real tail turns a slow-but-
succeeding request into a retry storm. The HTTP/2 keep-alive knobs are set for
correctness but are inert under the client's HTTP/1.1 default (which we keep:
HTTP/2 is slower for bulk S3 transfers), so under HTTP/1.1 connection liveness
comes from the connect, request, and pool-idle timeouts.

**A fixed timeout requires a bounded request, so the adapter bounds one.** Every
request size is chosen by this crate except one: a whole-object read, whose size
is whatever the object is. `max_l1_part_bytes` defaults to 256 MiB, 32x an 8 MiB
multipart part, so no fixed timeout can cover both. `S3Store::get` therefore
never issues an unranged GET: `GetRange::Full` is served as ranged requests of
at most `S3HttpConfig::max_request_body_bytes()` bytes each, derived from the
configured `request_timeout` as
`(request_timeout - 6 s of connect/TLS/first-byte allowance) * 625 000 B/s`
(a 5 Mbps floor rate), clamped to `[1 MiB, 8 MiB]`: the upper bound is the
multipart part size, so read and write share one largest-request-on-the-wire,
and the lower bound stops a tight `request_timeout` splitting a whole-object
read so finely that the per-request round trips cost more than the bound buys
back. At the default 20 s the derived value is 8 MiB, ~13.4 s at the floor rate,
inside the 14 s transfer budget; a compile-time assertion in `s3.rs` pins the
inequality. Below a `request_timeout` of about 7.7 s the 1 MiB floor binds and
the formula above no longer predicts the chunk size. A caller-supplied `GetRange::Range` is passed
through unsplit: the caller sized that request itself.

Cost of the split, in requests and wire bytes as transferred, excluding
`object_store`'s internal per-request retries:

| Object size | Requests | Wire bytes |
|---|---|---|
| 0 | 2 (one 416-refused ranged probe, then one unranged) | 0 body bytes |
| 1 byte .. bound | 1 | the object |
| above the bound | `ceil(size / bound)`, up to 4 in flight | the object, plus one response header set per additional request |

The ranges partition the object exactly, so no byte is fetched twice. Every
request after the first carries the first's ETag as an `If-Match`, so an object
overwritten mid-read fails the read (retryable) rather than splicing two
versions together; data objects are immutable, and the mutable pointer keys are
all far below the bound, so this is a guard rather than a live path.

**Worst-case wall time for one logical operation.** A request timeout is a
retryable error, and `object_store` runs its own internal retry loop
(`RetryConfig`, unchanged: `max_retries = 10`, `retry_timeout = 180 s`,
jittered exponential backoff), stopping before it *starts* a retry once 180 s
have elapsed since the first attempt. The final in-flight attempt still runs
its full 20 s, so one logical operation can spend about `retry_timeout +
request_timeout` = 180 s + 20 s ≈ 200 s inside the adapter before surfacing an
error. Callers bound this from above: every caller passes a deadline and the
trait honors cancellation by drop, so the query deadline (usually well under
180 s) is what ends one operation in practice.

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
- `list_after(prefix, start_after, page)` returns exactly the keys `list`
  would, minus every key `<= start_after`: each returned key compares
  strictly greater than `start_after`, in the same lexicographic order and
  under the same pagination and cross-page guarantee as `list`.
  `start_after == None` is identical to `list`. `start_after` need not name
  an existing key and is typically a prefix string that sorts before the
  first key the caller wants, letting the caller skip a whole key sub-range
  server-side rather than paging through and discarding it (S3
  `ListObjectsV2` `start-after`, exclusive; the memory oracle resumes its
  ordered map strictly after the marker). A `start_after` at or below the
  listed prefix excludes no key under it. Overriding the default (which
  lists from the prefix and drops `<= start_after` in the client) is a
  performance property only; the visible result set is identical.
- `Suffix(0)` and zero-length `Range` are `InvalidRange`.
- `Range(start, end)` is half-open; the HTTP Range header is inclusive, so
  adapters emit `bytes=start-(end-1)`. Boundary conformance tests required
  (exact object/section/page ends).
- `last_modified` may have 1-second granularity. Never order commits by it;
  it exists for advisory age decisions (GC age checks, claim expiry) only.
  It is server-assigned, which is the whole point of the widening: it is the
  one time base every contender shares, so an advisory expiry judged against
  it needs no agreement between node clocks (ADR-1029 §1). "Advisory" is
  load-bearing here -- an age decision read from this field may be wrong
  under skew or granularity, so it may only cost duplicated work, never
  correctness.

## Mandatory capabilities (production)

Every mode requires these; production startup fails if any is false.

| Capability | Flag | Used by |
|---|---|---|
| Create + read-after-write consistency | consistent_read | commit visibility |
| List-after-write consistency | consistent_list | commit discovery |
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
objects (its `build.rs` records this), and no production
caller invokes `put_multipart` yet. The maintain-mode requirement stands so
that once compaction does stream large L1/L2 segments as multipart uploads, the
backend is already known to serve the create/upload-part/complete/abort
sequence rather than discovering the gap at runtime. `MemoryStore` and
`S3Store` both report `multipart: true` and implement the sequence, so
`--mode maintain` starts against the memory oracle and against any
S3-compatible endpoint whether or not any caller exercises the path yet.
(`S3Store::put` does take an internal multipart path above its threshold, but
that is a size-driven implementation detail of `put`, not a caller reaching for
`put_multipart`; see "When `put()` uses it" below.)

### Multipart upload

`ObjectStoreBackend::put_multipart(key)` returns a `MultipartUpload` handle: a
sequence of `put_part` calls followed by exactly one `complete` or `abort`. The
flag and the method must agree: a backend reporting `multipart: false` MUST
refuse `put_multipart` with `Permanent`, which is what the default trait
implementation does, and the contract suite asserts both directions.

**Part bounds.** Enforced locally by every backend, at the call that violates
them, rather than deferred to the server's `CompleteMultipartUpload`:

| Rule | Value | Constant |
|---|---|---|
| Minimum size, any part but the last | 5 MiB | `MULTIPART_MIN_PART_SIZE` |
| Minimum size, last part | 1 byte (no part may be empty) | n/a |
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
never a readable object. **A bucket that Ravel writes multipart uploads to MUST
configure `AbortIncompleteMultipartUpload`**, with a cleanup period of 7 days or
less. This is not a recommendation: a failed abort or a future dropped
mid-upload can leave billable parts indefinitely, and nothing in Ravel reaps
them, so the lifecycle rule is the only mechanism that bounds the cost. Because `put()`'s
above-threshold multipart path aborts best effort and discards the result
(it never retries or blocks on the abort), that failure is otherwise silent:
`S3Store::multipart_abort_failures` counts aborts whose request returned an
error, and `S3Store::multipart_uploads_unreaped` counts multipart uploads that
ended without a successful abort for any reason (a failed abort, or a future
dropped mid-upload with its abort unresolved). Note the two do not partition
cleanly: if the future is cancelled while `upload.abort().await` is still
pending, the guard increments `multipart_uploads_unreaped` while
`multipart_abort_failures` does not, even though an abort request WAS attempted
and may well have been applied server-side. So the difference between them
counts "no confirmed abort outcome", which includes both "no abort was issued"
and "an abort was issued and its result is unknown".

Both counters cover only uploads that reached the point where the guard is
armed. `put_via_multipart` arms `UnreapedGuard` *after*
`put_multipart(&path).await` returns, so if `CreateMultipartUpload` is accepted
server-side and the future is cancelled before its response arrives, an open
upload exists that neither counter ever sees. That window is bounded by the
lifecycle rule and by nothing else, which is a further reason the rule is
required rather than advisory.

Read both as **outcomes that were not confirmed**, not as a count of billable
orphans. An abort can return an error after S3 has already applied the
operation, and in particular an abort issued after an ambiguous `complete()`
error can fail precisely *because the upload already completed*, leaving a
visible object and nothing to reap, while both counters rise. So a non-zero
value means "reconcile against S3's own list of open uploads", not "this many
orphans exist". Both read a
process-local `AtomicU64` on the `S3Store`; a hard process crash increments
neither, so that case stays inferable only from S3's own list of open uploads.

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
part left can never be filled (retrying it would live-lock). A
part-sequence violation (an empty part, or a non-final part below the minimum)
poisons the handle the same way: a later `complete` errors rather
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
degenerate single-part upload, and every part but the last is exactly 8 MiB:
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
against the caller's buffer on `S3Store` (see "Upload checksums": nothing can
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
as one `put`, because that is what the caller invoked. `FaultStore` carries no
scripted (`Rule`/`Sequence`) fault on a multipart part, but part completions
are hold sites for its test-only completion-ordering gate (ADR-0059 decision
5): `complete()` can hold each submitted part until a `GateHandle` releases it,
so a test can drive parts completing out of submission order and confirm the
assembled object stays byte-correct. The scripted-fault gap is an
observability/testing gap, not a correctness one, and no production path
depends on part completion order.

### Upload checksums (opt-in, never startup-gating)

`upload_checksum` is a `Capabilities` flag, but it is NOT mandatory and no
mode may require it. When a backend reports `upload_checksum: true`, it
guarantees a write is verified against corruption between the caller and the
server: a body that does not match is rejected with `Corrupted` and no object
becomes visible. When it reports `false`, only the local pre-flight below runs
and transport corruption is caught at read time instead.

**Two-part integrity, and what each part covers.** `put()` runs the caller's
`PutOptions::checksum` (CRC32C) as a local pre-flight against its input buffer
on every backend, rejecting a caller/payload mismatch with `Corrupted` before
any network call. This is the caller -> our-buffer half. The contract suite
asserts it (`assert_upload_checksum_verification`). The our-buffer -> server
half is what `upload_checksum: true` adds.

**`MemoryStore`** verifies `PutOptions::checksum` against the bytes it received
and always reports `upload_checksum: true`; it is the semantics oracle for the
capability. The contract suite pins the promise with
`upload_checksum_store_rejects_corrupt_in_flight`: `FaultStore`'s `CorruptBody`
fault flips the payload between caller and store, and a store claiming the
capability must reject it (asserting the fault counter proves the corruption
fired).

**`S3Store`** is opt-in via `S3HttpConfig::upload_integrity` (`UploadIntegrity`):

- `Off` (default) attaches no checksum and reports `upload_checksum: false`.
  This is the historical behavior, kept as the default because a checksum
  header an endpoint does not support turns every write into an error.
- `Crc64Nvme` / `Sha256` configure `object_store`'s whole-client
  `AmazonS3Builder::with_checksum_algorithm`, so it computes that digest over
  the exact payload and sends it as `x-amz-checksum-crc64nvme` /
  `x-amz-checksum-sha256`; S3 verifies-or-rejects on receipt. The capability
  then reports `true`.

Two limits of `object_store` 0.14's `AmazonS3` client shape this. First, it
exposes no per-request checksum hook and no way to attach a caller-supplied
precomputed digest (`PutRequest::with_payload` computes the digest itself), and
its algorithm knob offers only SHA-256 or CRC64-NVME. So the attached checksum
is *not* the caller's CRC32C from `PutOptions::checksum`; it is a separate,
stronger (64-bit) digest `object_store` computes over the same buffer the
pre-flight just checked, so the caller's bytes are still covered end to end.
Second, a backend that *silently ignores* the header cannot be detected in the
adapter: `object_store`'s `PutResult` carries only `e_tag`/`version`, never the
response headers in which S3 echoes a honored checksum. A backend that
*rejects* the header fails the PUT loudly (a surfaced `StoreError`), so the
detectable failure mode fails safe; the undetectable one is why a non-`Off`
mode is a deployment-level assertion that the configured endpoint honors the
chosen algorithm, made visible through the capability flag rather than probed.
The startup warn/downgrade choice this implies lives in the server wiring that
reads `capabilities()`, not in this crate.

`upload_checksum` is not in `Capabilities::mandatory()` and gates no mode, so
`S3Store` starts in every mode under either setting. Read-time integrity is the
backstop regardless: the footer/section/page crc32c hierarchy
(docs/segment-format.md) verifies data on every read of format-bearing bytes,
independent of whether a wire-level upload checksum existed.

There is no per-part or whole-object upload checksum for a multipart upload:
`object_store`'s `UploadPart` takes no checksum-algorithm value and `complete`
takes no digest, so a multipart part keeps only the local CRC32C pre-flight (see
"Checksum coverage" under "Multipart upload"). `put()`'s own above-threshold
multipart path is therefore not covered by `upload_integrity`.

Upload checksums are CRC32C-class integrity checks against transport
corruption; they do not verify blake3. blake3 in commit records is an
idempotency and identity discriminator, not a transport check.

### Backend support notes

AWS S3 since Dec 2020 provides strong read-after-write and list
consistency; S3 conditional writes (If-None-Match/If-Match) provide
CreateIfAbsent and CAS. GCS: generation preconditions. Azure: etags +
leases. MinIO supports the full mandatory set. Server-side upload checksums
are reachable through `object_store`'s whole-client
`with_checksum_algorithm` (SHA-256 / CRC64-NVME), which
`S3HttpConfig::upload_integrity` opts into; SHA-256 is the broadly supported
choice (AWS S3 and MinIO), CRC64-NVME needs a recent endpoint. The default is
`Off`, so `S3Store` reports `upload_checksum: false` unless a mode is
configured (see "Upload checksums").

### Credentials

`S3Config` selects a credential source explicitly through `auth`
(`S3AuthMode`), never by inferring one from the absence of keys. The default
is `S3AuthMode::Static`, which is every deployment today and behaves exactly
as before ADR-0106.

**Static mode (ADR-0072 decision 1).** Takes long-lived `access_key_id` /
`secret_access_key`, an optional temporary `session_token` for STS-issued or
IRSA-style credentials, and an optional `credentials_file` for credentials
an external process rotates on disk (a Kubernetes secret mount, an STS
sidecar). Ravel never calls STS itself; `credentials_file` only makes an
externally-minted rotating credential expressible. When both are set, the
file wins. The file is read once at `S3Store::new`, eagerly: an unreadable
or malformed file fails construction with a typed `StoreError` (fail fast at
startup). After that it is re-read lazily, only on request-path credential
access, and only when its mtime has changed since the last read; there is no
background thread and no timer. A successful re-read swaps the cached
credential atomically, so a request already in flight finishes on whatever
credential it already obtained. A read or parse failure while rotating
never fails the request: the last-good credential is kept and a
rate-limited warning is logged, never a panic.

**Instance-role mode (ADR-0106).** `auth = S3AuthMode::InstanceRole` fetches
short-lived credentials from the EC2 instance metadata service (IMDSv2) on
the link-local address, so an EC2 deployment stores no static key at all.
`access_key_id`, `secret_access_key`, `session_token`, and `credentials_file`
must all be absent; setting `auth = InstanceRole` together with any of them is
a configuration error, and `S3Store::new` rejects it with a typed `StoreError`
at construction (there is no precedence question, because the mix is refused
outright). `instance_metadata_endpoint` overrides the IMDS base URL (default
`http://169.254.169.254`); it is an operator-facing knob on the same trust
boundary as every other `--s3-*` setting and exists so tests and unusual
deployments can redirect IMDS.

The provider is IMDSv2 only: it `PUT`s for a session token, then `GET`s the
role document (`AccessKeyId`, `SecretAccessKey`, `Token`, `Expiration`). Any
non-success from the metadata endpoint, including a `403` from a disabled or
hop-limited IMDS, is a typed error, never a downgrade to the token-less
IMDSv1 flow. The first fetch runs eagerly at `S3Store::new` under a bounded
timeout, so an instance misconfigured for a role fails at startup rather than
on its first S3 request, and construction never hangs indefinitely. The
credential is cached and refreshed on the request path once the clock comes
within 5 minutes of its `Expiration`. A transient refresh failure keeps
serving the cached credential while it is still unexpired; once the cached
credential has actually expired, the request fails with a typed error, a
failure counter (`S3Store::credential_refresh_failures`, mirroring
`credential_rotation_failures`) increments, and a warning is logged
rate-limited to once per 60s. Credentials live only in memory: never written
to disk, and redacted from the provider's `Debug`.

**SSE-KMS under an instance role.** `kms_key_id` works unchanged in either
mode, because S3 performs the KMS call server-side on every PUT. When
`kms_key_id` is set with `auth = InstanceRole`, the instance role's IAM
policy must grant `kms:GenerateDataKey` and `kms:Decrypt` on that key, since
the role is the identity S3 evaluates for the encryption and decryption
calls.

## Runtime qualification (executable contract)

`Capabilities` is self-reported: a backend declares `consistent_list: true`
because its adapter believes the vendor provides it, not because anything
checked. The problem is that nothing did: a backend
that advertises S3 compatibility but actually delivers eventually consistent
listing was trusted silently, and the resulting failures at the commit layer
looked like data loss rather than a misconfigured store.

`crates/ravel-object-store/src/conformance.rs` is this contract turned into
a suite that empirically probes a live backend rather than reading its
declared flags. `run_conformance_suite(store, scratch_prefix)` runs, under a
throwaway key prefix:

- `ConditionalWriteCreateIfAbsent`: two concurrent `CreateIfAbsent` puts to
  the same key: exactly one must win and the loser must observe
  `AlreadyExists` (the losing-writer outcome the "Semantics adapters MUST
  honor" section above requires).
- `ConditionalWriteCasVersion`: a `CasVersion` put against a stale version
  must fail `PreconditionFailed`, not silently overwrite.
- `ConsistentReadAfterWrite`: a `get` immediately following a `put` returns
  the just-written bytes, repeated over several keys to catch a
  read-your-writes gap that only shows up intermittently.
- `ConsistentListAfterWrite`: a `list` immediately following a `put`
  includes the new key, repeated the same way, to catch eventual-consistency
  listing rather than trusting the `consistent_list` flag.

Each probe returns a `ProbeResult` naming which `Property` it checked, so a
failure reads "this backend cannot do conditional writes" or "this backend's
listing is eventually consistent" instead of a bare pass/fail, so an operator
does not have to guess which mandatory capability the backend actually
lacks.

ADR-0050 section 6 also names cross-page listing consistency and
multipart-complete visibility as probes for this suite; neither is
implemented yet. `CONFORMANCE_SUITE_VERSION` exists precisely so a later
addition can be told apart from the four probes qualifying a bucket today.

This is a runtime, once-per-bucket check, not a replacement for the
compile-time contract suite below: `crates/ravel-object-store/tests/contract.rs`
is a development-time
proof that each adapter *implementation* honors the trait, run in CI against
all three backends including a real MinIO endpoint. `conformance.rs` is an
operator-facing probe of one specific *deployment*, the actual configured
endpoint and bucket, because the adapter can be correct while the vendor
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
not delete afterward: each run's key is unique, so this is unbounded
untracked storage a runbook should sweep periodically, not a correctness
issue) and the single `sys/qualification` key; it never reads, lists, or
writes any tenant-prefixed key, so it is safe to run against a bucket that
already holds production data.

## Required bucket configuration (ADR-0064 §7, ADR-0072 decision 3)

Everything above is a property of the `ObjectStoreBackend` *adapter*.
ADR-0064 §7 additionally names bucket-level *configuration*
Ravel's deletion and retention guarantees depend on, orthogonal to the
adapter contract:

1. **Versioning.** Object versioning must be either OFF, or ON and paired
   with a noncurrent-version expiration rule (plus expired-delete-marker
   cleanup) on every `t/` prefix. Versioning ON without that pairing is an
   unsupported configuration: it silently turns every Ravel delete
   (retention sweep, ADR-0064 selective erasure) into a soft delete,
   inverting the system's deletion guarantees while everything above this
   layer keeps reporting success.
2. **No other lifecycle expiration or archival-transition rule** may target
   any Ravel-owned prefix. A storage-class transition or an expiration rule
   added for cost reasons can silently delete or relocate a commit record,
   a manifest, or provenance data outside any path Ravel's own retention
   logic controls.
3. **Two sanctioned lifecycle rules**, and only these:
   - `AbortIncompleteMultipartUpload` (REQUIRED, 7 days or less) cleans up
     abandoned multipart uploads. **Its absence violates the bucket
     configuration contract**, because nothing in Ravel reaps abandoned
     uploads: a failed abort or a future dropped mid-upload leaves billable
     parts indefinitely, and this rule is the only mechanism that bounds them.
   - The noncurrent-version expiration rule required by point 1 when
     versioning is ON.
4. **Object Lock, compliance mode**, on the protected prefixes: `sys/*`,
   `t/*/*/prov`, commit records `t/*/*/c/*`, and `t/*/catalog/*/*` HEAD
   history. These are the objects whose immutability the commit and
   catalog layers assume as a given (see "Data objects, commit records,
   manifests, and index objects are immutable"; this section is that
   invariant's bucket-level enforcement point). Object Lock is what makes
   that assumption hold even against a compromised or misconfigured
   credential that can otherwise issue deletes: compliance mode refuses
   deletion or overwrite for the configured retention period, with no
   principal (including the bucket owner) able to shorten or remove it.
   Subject identifiers that must remain erasable under ADR-0064 live in
   *values*, never in *object keys or names*, precisely so Object Lock on
   these prefixes never conflicts with a legitimate erasure request.

Enforcement stays at the bucket/IAM layer (ADR-0042 decision 3): nothing
in this crate can configure or verify Object Lock or lifecycle policy
in-process, because `object_store` 0.14 exposes no such API and this crate
never opens a second, direct-SDK side channel. What this crate *can* do is
report what a backend affirmatively discloses, via
[`ObjectLockProbeSource`]/`probe_object_lock` and
[`BucketConfigProbeSource`]/`probe_bucket_config` (see "Runtime
qualification" above) plus `bucket_config_alarms`, which turns an observed
[`BucketConfigProbe`] into `"ALARM:"`-prefixed strings for a genuine
contract violation (point 1's versioning/expiration pairing) and
`"NOTE:"`-prefixed strings where the probe cannot establish compliance. The
multipart-abort rule is REQUIRED (point 3), so its absence is a contract
violation and not an advisory gap; it is reported under `"NOTE:"` only because
this crate calls no vendor API that can observe the rule, so the probe cannot
determine compliance either way. The prefix reflects the limits of the probe,
never a weaker requirement. Every production backend reports
[`ObjectLockStatus::Unknown`] and every `BucketConfigProbe` field
`Unknown` through these traits today, since there is no vendor API this crate
calls to populate anything else, so today these probes are informational
only, exactly as ADR-0055 §3 designed them, and their reporting stays
that way regardless of the flag below.

`ravel-server --require-bucket-protection` (ADR-0072 decision 3, default
OFF, env `RAVEL_REQUIRE_BUCKET_PROTECTION`) turns the same probes into a
startup gate instead of a print statement, so a deployment cannot go into
production silently unprotected:

- `ObjectLockStatus::Disabled`, or a `bucket_config_alarms` `"ALARM:"`
  entry (the versioning-without-expiration misconfiguration), is fatal:
  the server refuses to start with a typed error.
- `ObjectLockStatus::Unknown`, the case every backend reachable only
  through `ObjectStoreBackend` reports today, since no adapter can
  actually answer this query, logs one warning and sets the
  `ravel_bucket_protection_unknown` gauge to `1`, so a fleet can alarm on
  it without being blocked by it.
- `ObjectLockStatus::Enabled` with no alarms starts clean, gauge at `0`.

With the flag off (the default), none of this runs, and startup behavior
is unchanged from before this gate existed. The flag makes an
unprotected production deployment *visible and refusable*; it does not
and cannot make Object Lock or lifecycle policy real in-process: that
capability is still reserved for its own trait-extending ADR per
ADR-0042 decision 3.

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
   each successive matching call). Completion ordering is controlled by a
   separate, test-only hold-and-release primitive (ADR-0059 decision 5), not by
   the scripted fault plan: `FaultStore::hold` registers a gate that matches a
   call the same way a rule does (`op` + key substring + occurrence) and blocks
   the matching call inside the store until a `GateHandle` releases that
   specific held call. It controls *when* a concurrently in-flight call's result
   becomes visible to its caller, composing with rather than replacing a
   scripted fault. This is a test primitive on the wrapper, not a general store
   capability, and its existence is not a claim that production code depends on
   completion order (it does not, per ADR-0059).
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

`calls` is completions, not attempts: it counts one per logical operation, so
the `object_store` retry loop that runs *below* this decorator (`RetryConfig`,
default `max_retries = 10`) is invisible to it: one `get()` that retried nine
times is one `calls`/`get` while the provider bills ten HTTP requests (issue
#928). The billed count is carried in a separate per-operation `attempts`
counter on the same `StoreMetrics` block, filled in by the S3 adapter's counting
HTTP connector (`S3Store::with_metrics`, installed via
`AmazonS3Builder::with_http_connector`), which records one attempt per HTTP
request `object_store` issues, retries included. `attempts >= calls` holds
exactly when every store the decorator counts a `calls` on records its attempts
into the same `StoreMetrics` handle: a store built with `S3Store::new` (no
handle) wrapped in `InstrumentedStore::with_metrics` would count `calls` while
recording no `attempts`, so the relation is a property of the wiring, not of the
decorator. `ravel-server` establishes it for the whole S3 chain by handing one
handle to the base `S3Store` and, under `--tenant-kms-config`, to every
per-tenant KMS-routed store (`KmsRoutingStore::new`); a store built without the
handle would make `ravel_store_attempts_total` under-report for the traffic it
serves (issue #928).

`attempts` is the billed HTTP request count, not a retry counter. Retries are
one reason it exceeds `calls`, and not the only one: a whole-object read and a
multipart write each issue several HTTP requests per logical call, so `attempts`
exceeds `calls` for `get`/`put` even when nothing retried. Read `attempts` as
what the provider bills, and do not read `attempts - calls` as retry overhead;
isolating retries specifically would need a separate counter this does not add. The connector
wraps the default reqwest client and delegates unchanged, so `RetryConfig` and
every retry behavior above stay exactly as documented: this observes the loop,
it does not alter it. A backend that issues no HTTP (`MemoryStore`) leaves
`attempts` at zero. `ravel-server` exports it as `ravel_store_attempts_total`
beside `ravel_store_calls_total`.

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

`crates/ravel-object-store/tests/s3_http_faults.rs` covers what neither the
contract suite nor `s3.rs`'s classification unit tests can: the retry, backoff,
and multipart behavior `S3Store` exhibits *on the wire*. It stands up a fake S3
endpoint (axum, loopback, ephemeral port), points `S3Store` at it through the
ordinary `S3Config::endpoint` override, and scripts per-request faults --- 503,
429, a `SlowDown` error body as both a 503 and the 200-with-error body S3
documents for `CompleteMultipartUpload`, 403 `AccessDenied`, a connection
dropped mid-response, and a multipart sequence failing after some parts
succeeded. Because the endpoint records every request with a timestamp, the
assertions are on what the server saw: a throttled GET/PUT is really re-sent, a
403 is really sent once, the pause between attempts really grows, and a failed
multipart upload really leaves no object at the key. No live endpoint and no
Docker, so it runs in the default `cargo test`, unlike the MinIO-gated
assertions above.

### Per-tenant KMS routing decorator

`KmsRoutingStore` wraps a default backend and routes writes (`put`,
`put_multipart`) for a tenant with a configured KMS key to a lazily-built,
cached per-tenant `S3Store` built from the default `S3Config` with only
`kms_key_id` overridden (ADR-0062 decision 1a). Routing is decided per call
from the object key alone: every tenant-scoped key begins with
`t/<tenant_hash_hex>/`, so no trait change and no per-tenant handle threaded
through call sites. Every read (`get`, `head`, `list`, `list_delimited`),
`delete`, and any non-`t/`-prefixed or malformed-tenant-segment key delegates
unconditionally to the default store: SSE-KMS decryption on GET is
server-side and transparent given `kms:Decrypt`, so a reader never selects a
key. `capabilities()` passes straight through the default store's
declaration, same as the instrumentation decorator.

Per-tenant stores are cached for the process lifetime (`Box::leak`'d to
`&'static`, bounded by the number of distinct configured tenants) so a
`put_multipart` handle can satisfy the trait's lifetime without a
self-referential owner. The cache is keyed by tenant hash together with the
ARN it was built under: registering a new key for a tenant that already has
a cached store (`set_tenant_key`) does not retroactively re-encrypt anything
already written (objects are immutable), but the next write for that tenant
rebuilds the cache entry under the new key rather than silently continuing
to route through the store built under the superseded one.

The contract suite runs `KmsRoutingStore` (wrapping `MemoryStore`, no tenant
key configured) through the full assertion set the same way it does
`InstrumentedStore`, proving the decorator is transparent when no tenant has
opted into per-tenant routing. It does not exercise the routing branch
itself (a live per-tenant `S3Store` has no endpoint under test); routing is
covered by `kms_routing`'s own unit tests instead, including key rotation
and `put_multipart` routing.

## Rules for callers

- Never infer visibility from a successful data PUT; only commit records
  confirm publication (ADR-0002).
- All GETs of format-bearing bytes verify embedded checksums; etag
  inequality between ranged reads of the same immutable object aborts with
  `Corrupted` (data objects are created with CreateIfAbsent, so rewrites
  cannot produce differing content for one key).
- Every caller passes a deadline; trait impls honor cancellation by drop.
