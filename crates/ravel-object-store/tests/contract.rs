//! Shared contract test suite (docs/object-store-contract.md), run against
//! every `ObjectStoreBackend` implementation this crate ships: the memory
//! oracle at its default page size and at a tiny one to force pagination,
//! `FaultStore` wrapping the oracle with an empty plan (must be fully
//! transparent), and -- gated on `RAVEL_MINIO_URL` / `RAVEL_FLOCI_URL` --
//! `S3Store` against a real MinIO and against a real floci.
//!
//! This is an integration-test binary, a crate in its own right, so it
//! inherits `[lints] workspace = true` (including `clippy::expect_used`
//! promoted to a hard error by `-D warnings`); the allow below is
//! crate-level, not per-function, because every assertion here uses
//! `expect`/`expect_err` to fail with a readable message.
#![allow(clippy::expect_used)]

use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as OsPath;
use object_store::{MultipartUpload, ObjectStoreExt, PutPayload};
use ravel_object_store::fault::{FaultPlan, FaultStore};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{S3Config, S3Store};
use ravel_object_store::{
    Capabilities, GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, UploadChecksum,
    list_all,
};

/// Runs every contract assertion against `store`. Each assertion gets its
/// own key sub-prefix under `root` so they can share one backend instance
/// (and, for the MinIO case, one long-lived bucket) without colliding.
async fn run_contract_suite(store: &dyn ObjectStoreBackend, root: &str) {
    assert_create_if_absent_atomicity(store, &format!("{root}/create-if-absent/")).await;
    assert_cas_version_semantics(store, &format!("{root}/cas/")).await;
    assert_range_and_suffix_reads(store, &format!("{root}/range/")).await;
    assert_paginated_listing_completeness(store, &format!("{root}/list/")).await;
    assert_delimited_listing(store, &format!("{root}/delim/")).await;
    assert_idempotent_delete(store, &format!("{root}/delete/")).await;
    assert_upload_checksum_verification(store, &format!("{root}/checksum/")).await;
}

async fn assert_create_if_absent_atomicity(store: &dyn ObjectStoreBackend, prefix: &str) {
    let key = format!("{prefix}k");
    store
        .put(
            &key,
            Bytes::from_static(b"first"),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("first create-if-absent must succeed");
    let err = store
        .put(
            &key,
            Bytes::from_static(b"second"),
            PutOptions::create_if_absent(),
        )
        .await
        .expect_err("second create-if-absent on the same key must fail");
    assert!(matches!(err, StoreError::AlreadyExists), "got {err:?}");
    let got = store.get(&key, GetRange::Full).await.expect("get");
    assert_eq!(
        &got.data[..],
        b"first",
        "losing write must not have applied"
    );
}

async fn assert_cas_version_semantics(store: &dyn ObjectStoreBackend, prefix: &str) {
    let key = format!("{prefix}k");
    let put1 = store
        .put(&key, Bytes::from_static(b"v1"), PutOptions::default())
        .await
        .expect("initial put");

    // A well-formed but wrong version token: a real version issued for a
    // *different* object, not a hand-rolled string. On S3 a `Version` is an
    // ETag, and ETags are quoted opaque tokens (`"d41d8cd9..."`); a
    // synthetic unquoted string is not just stale, it's a malformed
    // precondition header, so the server would reject it before ever
    // reaching the mode-aware precondition-failure path this assertion
    // means to exercise. A real version from elsewhere is guaranteed
    // well-formed and guaranteed not to match `key`, on every backend.
    let other = store
        .put(
            &format!("{prefix}other"),
            Bytes::from_static(b"unrelated"),
            PutOptions::default(),
        )
        .await
        .expect("unrelated put for a well-formed-but-wrong version token");

    let stale_err = store
        .put(
            &key,
            Bytes::from_static(b"stale"),
            PutOptions {
                mode: PutMode::CasVersion(other.version),
                checksum: None,
            },
        )
        .await
        .expect_err("stale CAS must fail without applying");
    assert!(
        matches!(stale_err, StoreError::PreconditionFailed),
        "got {stale_err:?}"
    );
    let got = store.get(&key, GetRange::Full).await.expect("get");
    assert_eq!(&got.data[..], b"v1", "stale CAS must not have applied");

    store
        .put(
            &key,
            Bytes::from_static(b"v2"),
            PutOptions {
                mode: PutMode::CasVersion(put1.version.clone()),
                checksum: None,
            },
        )
        .await
        .expect("CAS against the current version must succeed");
    let got = store.get(&key, GetRange::Full).await.expect("get");
    assert_eq!(&got.data[..], b"v2");

    // The version moved forward: reusing the now-superseded version fails.
    let err = store
        .put(
            &key,
            Bytes::from_static(b"v3"),
            PutOptions {
                mode: PutMode::CasVersion(put1.version),
                checksum: None,
            },
        )
        .await
        .expect_err("reusing a superseded version must fail");
    assert!(matches!(err, StoreError::PreconditionFailed), "got {err:?}");
}

async fn assert_range_and_suffix_reads(store: &dyn ObjectStoreBackend, prefix: &str) {
    let key = format!("{prefix}k");
    store
        .put(
            &key,
            Bytes::from_static(b"0123456789"),
            PutOptions::default(),
        )
        .await
        .expect("put");

    let suffix = store.get(&key, GetRange::Suffix(4)).await.expect("suffix");
    assert_eq!(&suffix.data[..], b"6789");
    assert_eq!(suffix.total_size, 10);

    let clamped = store
        .get(&key, GetRange::Suffix(1000))
        .await
        .expect("suffix larger than the object must clamp, not error");
    assert_eq!(&clamped.data[..], b"0123456789");

    let err = store
        .get(&key, GetRange::Suffix(0))
        .await
        .expect_err("Suffix(0) must be InvalidRange");
    assert!(matches!(err, StoreError::InvalidRange(_)), "got {err:?}");

    let range = store.get(&key, GetRange::Range(2, 5)).await.expect("range");
    assert_eq!(&range.data[..], b"234");

    let clamped_range = store
        .get(&key, GetRange::Range(2, 1000))
        .await
        .expect("range end past the object must clamp, not error");
    assert_eq!(&clamped_range.data[..], b"23456789");

    let err = store
        .get(&key, GetRange::Range(5, 5))
        .await
        .expect_err("zero-length range must be InvalidRange");
    assert!(matches!(err, StoreError::InvalidRange(_)), "got {err:?}");

    let full = store
        .get(&key, GetRange::Range(0, 10))
        .await
        .expect("exact full-object range");
    assert_eq!(&full.data[..], b"0123456789");
}

async fn assert_paginated_listing_completeness(store: &dyn ObjectStoreBackend, prefix: &str) {
    let mut keys: Vec<String> = (0..7).map(|i| format!("{prefix}{i}")).collect();
    keys.sort();
    for key in &keys {
        store
            .put(key, Bytes::from_static(b"x"), PutOptions::default())
            .await
            .expect("put");
    }

    // Manual pagination loop: exercises `PageToken` continuation directly
    // (as opposed to only the `list_all` convenience helper), so it holds
    // whether the backend pages at 2 entries or 1000.
    let mut seen = std::collections::HashSet::new();
    let mut ordered = Vec::new();
    let mut token = None;
    loop {
        let page = store.list(prefix, token).await.expect("list page");
        for meta in &page.objects {
            if seen.insert(meta.key.clone()) {
                ordered.push(meta.key.clone());
            }
        }
        match page.next {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    assert_eq!(
        ordered, keys,
        "listing must be complete, deduplicated, and lexicographically ordered"
    );

    let all = list_all(store, prefix).await.expect("list_all");
    let all_keys: Vec<_> = all.into_iter().map(|m| m.key).collect();
    assert_eq!(
        all_keys, keys,
        "list_all must agree with the manual pagination loop"
    );
}

async fn assert_delimited_listing(store: &dyn ObjectStoreBackend, prefix: &str) {
    for key in [
        format!("{prefix}x/1"),
        format!("{prefix}x/2"),
        format!("{prefix}y/1"),
        format!("{prefix}z"),
    ] {
        store
            .put(&key, Bytes::from_static(b"x"), PutOptions::default())
            .await
            .expect("put");
    }
    let listing = store.list_delimited(prefix).await.expect("list_delimited");
    let mut common_prefixes = listing.common_prefixes.clone();
    common_prefixes.sort();
    assert_eq!(
        common_prefixes,
        vec![format!("{prefix}x/"), format!("{prefix}y/")]
    );
    let direct: Vec<_> = listing.objects.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(direct, vec![format!("{prefix}z").as_str()]);
}

async fn assert_idempotent_delete(store: &dyn ObjectStoreBackend, prefix: &str) {
    let key = format!("{prefix}k");
    store
        .delete(&key)
        .await
        .expect("deleting a missing key must succeed");
    store
        .put(&key, Bytes::from_static(b"v"), PutOptions::default())
        .await
        .expect("put");
    store.delete(&key).await.expect("delete existing key");
    assert!(matches!(store.head(&key).await, Err(StoreError::NotFound)));
    store
        .delete(&key)
        .await
        .expect("deleting an already-deleted key must still succeed");
}

/// Exercises the client-side CRC32C pre-flight every backend runs against
/// its own input buffer before writing. This is NOT proof of on-the-wire
/// integrity: for `S3Store` (see `s3::S3Store::capabilities`) `object_store`
/// 0.14 gives no way to attach the checksum to the outgoing request, so this
/// assertion only shows the local mismatch check works, not that corruption
/// in transit would be caught. `s3_store_reports_upload_checksum_unsupported`
/// below is what proves that gap is declared honestly via `capabilities()`.
async fn assert_upload_checksum_verification(store: &dyn ObjectStoreBackend, prefix: &str) {
    let good_key = format!("{prefix}good");
    let bad_key = format!("{prefix}bad");
    let data = Bytes::from_static(b"contract-suite-checksum-payload");
    let good = crc32c::crc32c(&data);

    store
        .put(
            &good_key,
            data.clone(),
            PutOptions::default().with_checksum(UploadChecksum::Crc32c(good)),
        )
        .await
        .expect("correct checksum must be accepted");
    let got = store.get(&good_key, GetRange::Full).await.expect("get");
    assert_eq!(got.data, data);

    let err = store
        .put(
            &bad_key,
            data.clone(),
            PutOptions::default().with_checksum(UploadChecksum::Crc32c(good ^ 1)),
        )
        .await
        .expect_err("wrong checksum must be rejected");
    assert!(matches!(err, StoreError::Corrupted(_)), "got {err:?}");
    assert!(
        matches!(store.head(&bad_key).await, Err(StoreError::NotFound)),
        "an object failing checksum verification must not have been created"
    );
}

/// Probes every flag in [`Capabilities::mandatory()`] against a live
/// endpoint, one assertion per capability name (ADR-0034 decision 8: a
/// candidate backend either satisfies the mandatory set or the ADR's named
/// fallback applies, so a failure here has to say *which* capability failed,
/// not just which suite step tripped).
///
/// Deliberate overlap with [`run_contract_suite`]: `create_if_absent`,
/// `cas_version`, `suffix_range`, and `prefix_list` are each already proven
/// by a suite assertion, and this re-runs those assertions under their
/// capability names on their own key sub-prefixes. The duplicated round
/// trips are a few dozen requests against a local container, and in exchange
/// a gate failure names the capability the ADR's decision turns on.
/// `consistent_read` and `consistent_list` are probed directly here because
/// no suite assertion isolates them.
async fn assert_mandatory_capabilities(store: &dyn ObjectStoreBackend, prefix: &str) {
    // consistent_read: read-after-write and read-after-overwrite, with no
    // sleep and no retry loop. An eventually-consistent backend fails here.
    let key = format!("{prefix}consistent-read/k");
    store
        .put(&key, Bytes::from_static(b"one"), PutOptions::default())
        .await
        .expect("put");
    let got = store
        .get(&key, GetRange::Full)
        .await
        .expect("consistent_read: read-after-write");
    assert_eq!(
        &got.data[..],
        b"one",
        "consistent_read: a just-written object must be readable immediately"
    );
    store
        .put(&key, Bytes::from_static(b"three"), PutOptions::default())
        .await
        .expect("overwrite");
    let got = store
        .get(&key, GetRange::Full)
        .await
        .expect("consistent_read: read-after-overwrite");
    assert_eq!(
        &got.data[..],
        b"three",
        "consistent_read: an overwrite must be visible immediately, never a stale body"
    );
    assert_eq!(
        store.head(&key).await.expect("head").size,
        5,
        "consistent_read: head must reflect the overwrite immediately"
    );

    // consistent_list: the listing reflects a creation and a deletion
    // immediately, which is what the catalog scan depends on.
    let list_prefix = format!("{prefix}consistent-list/");
    let listed = format!("{list_prefix}k");
    store
        .put(&listed, Bytes::from_static(b"v"), PutOptions::default())
        .await
        .expect("put");
    let keys: Vec<String> = list_all(store, &list_prefix)
        .await
        .expect("consistent_list: list after create")
        .into_iter()
        .map(|m| m.key)
        .collect();
    assert_eq!(
        keys,
        vec![listed.clone()],
        "consistent_list: a just-written key must appear in the listing immediately"
    );
    store.delete(&listed).await.expect("delete");
    let after_delete: Vec<String> = list_all(store, &list_prefix)
        .await
        .expect("consistent_list: list after delete")
        .into_iter()
        .map(|m| m.key)
        .collect();
    assert!(
        after_delete.is_empty(),
        "consistent_list: a just-deleted key must be gone from the listing immediately, got {after_delete:?}"
    );

    // create_if_absent: the atomicity the commit protocol's single-writer
    // claim rests on (ADR-0002).
    assert_create_if_absent_atomicity(store, &format!("{prefix}create-if-absent/")).await;
    // cas_version: If-Match semantics, mapped to PreconditionFailed.
    assert_cas_version_semantics(store, &format!("{prefix}cas-version/")).await;
    // suffix_range: the RSEG footer read (docs/segment-format.md).
    assert_range_and_suffix_reads(store, &format!("{prefix}suffix-range/")).await;
    // prefix_list: complete, ordered, paginated recursive listing plus the
    // one-level delimited form the catalog uses.
    assert_paginated_listing_completeness(store, &format!("{prefix}prefix-list/")).await;
    assert_delimited_listing(store, &format!("{prefix}prefix-list-delimited/")).await;

    // upload_checksum, the one mandatory flag left. `S3Store::capabilities()`
    // is a constant of the adapter, not a function of the endpoint, so this
    // asserts the same thing for floci as it would for MinIO or real AWS S3:
    // every mandatory flag is declared supported except `upload_checksum`,
    // which `object_store` 0.14 cannot put on the wire at all (see
    // `s3_store_reports_upload_checksum_unsupported` above and the module
    // docs on `s3::S3Store`). Written as an exact equality rather than
    // `satisfies(&mandatory())` so movement in either direction -- a backend
    // gaining a capability or the adapter losing one -- fails here instead of
    // passing silently. Note what this therefore records: the declared set
    // does NOT satisfy `mandatory()`, because of that adapter-wide gap and
    // for no reason to do with the endpoint under test.
    let mut expected = Capabilities::mandatory();
    expected.upload_checksum = false;
    assert_eq!(
        store.capabilities(),
        expected,
        "declared capabilities must be the full mandatory set minus the \
         adapter-wide upload_checksum gap"
    );
    // The behavior behind the flag still has to hold: the local CRC32C
    // pre-flight rejects a mismatch without creating the object.
    assert_upload_checksum_verification(store, &format!("{prefix}upload-checksum/")).await;
}

/// Builds the raw `object_store` client the multipart probe needs.
///
/// [`ObjectStoreBackend`] has no multipart method (nothing in Ravel writes
/// multipart objects yet, and `S3Store::capabilities()` reports `multipart:
/// false`), so the adapter cannot be asked whether the endpoint behind it
/// supports multipart. The probe drops to the same `object_store` client
/// `S3Store` wraps, then reads the result back *through* `S3Store` so what
/// it asserts is bytes Ravel can actually see. This mirrors `S3Store::new`'s
/// builder configuration field for field; if the two diverge, this probe
/// stops exercising the real client, so they must stay in sync.
fn raw_s3_client(config: &S3Config) -> AmazonS3 {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_region(&config.region)
        .with_access_key_id(&config.access_key_id)
        .with_secret_access_key(&config.secret_access_key)
        .with_allow_http(config.allow_http)
        .with_virtual_hosted_style_request(!config.force_path_style);
    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint.clone());
    }
    builder
        .build()
        .expect("raw AmazonS3 client must build from a valid S3Config")
}

/// S3's minimum size for any part but the last, so the probe drives a real
/// two-part upload instead of a degenerate single-part one.
const MULTIPART_PART_SIZE: usize = 5 * 1024 * 1024;
/// Short final part, also the size of the trailing read below.
const MULTIPART_TAIL_SIZE: usize = 1024;

/// Multipart probe: `Mode::Maintain` requires the `multipart` capability on
/// top of [`Capabilities::mandatory()`] (services/ravel-server `store.rs`
/// `required_capabilities`), so a backend that cannot serve
/// `CreateMultipartUpload` / `UploadPart` / `CompleteMultipartUpload` /
/// `AbortMultipartUpload` cannot carry compaction.
async fn assert_multipart_upload(config: &S3Config, store: &dyn ObjectStoreBackend, prefix: &str) {
    let key = format!("{prefix}object");
    // Position-dependent bytes: parts assembled out of order, a dropped
    // part, or a duplicated one all change the checksum, which a constant
    // fill pattern would hide.
    let payload: Vec<u8> = (0..MULTIPART_PART_SIZE + MULTIPART_TAIL_SIZE)
        .map(|i| (i % 251) as u8)
        .collect();
    let expected_crc = crc32c::crc32c(&payload);

    let client = raw_s3_client(config);
    let mut upload = client
        .put_multipart(&OsPath::from(key.as_str()))
        .await
        .expect("multipart: CreateMultipartUpload must succeed");
    upload
        .put_part(PutPayload::from(payload[..MULTIPART_PART_SIZE].to_vec()))
        .await
        .expect("multipart: UploadPart 1 (5 MiB, S3's minimum non-final part) must succeed");
    upload
        .put_part(PutPayload::from(payload[MULTIPART_PART_SIZE..].to_vec()))
        .await
        .expect("multipart: UploadPart 2 (short final part) must succeed");
    upload
        .complete()
        .await
        .expect("multipart: CompleteMultipartUpload must succeed");

    // Read back through `S3Store`, comparing checksums rather than 5 MiB
    // buffers so a failure prints a diagnosis instead of megabytes of bytes.
    let got = store
        .get(&key, GetRange::Full)
        .await
        .expect("multipart: full read of the completed object");
    assert_eq!(
        got.total_size,
        payload.len() as u64,
        "multipart: completed object must report the summed part length"
    );
    assert_eq!(
        got.data.len(),
        payload.len(),
        "multipart: full read must return every byte of every part"
    );
    assert_eq!(
        crc32c::crc32c(&got.data),
        expected_crc,
        "multipart: parts must reassemble in order, with no gap and no duplication"
    );
    assert_eq!(
        store.head(&key).await.expect("multipart: head").size,
        payload.len() as u64,
        "multipart: head must report the assembled size"
    );

    // Proof the server really took the multipart path rather than collapsing
    // the upload into one PUT: S3 gives a multipart object the composite
    // `"<digest>-<partcount>"` ETag form, and the count must be the two parts
    // sent above. Without this, a backend that quietly buffered both parts
    // and wrote a single object would satisfy every byte-level assertion
    // here while implementing none of the multipart API compaction needs.
    let etag = got.etag.0.trim_matches('"').to_string();
    assert!(
        etag.ends_with("-2"),
        "multipart: completed object must carry the composite 2-part ETag \
         proving the parts were uploaded separately, got {etag:?}"
    );

    // The read shape Ravel actually uses against large segments: a footer
    // suffix read of a multipart-written object.
    let suffix = store
        .get(&key, GetRange::Suffix(MULTIPART_TAIL_SIZE as u64))
        .await
        .expect("multipart: suffix read");
    assert_eq!(
        &suffix.data[..],
        &payload[MULTIPART_PART_SIZE..],
        "multipart: suffix read must return the tail of the assembled object"
    );

    // A range straddling the part boundary: proves the parts were joined,
    // not merely stored side by side.
    let straddle = store
        .get(
            &key,
            GetRange::Range(
                MULTIPART_PART_SIZE as u64 - 4,
                MULTIPART_PART_SIZE as u64 + 4,
            ),
        )
        .await
        .expect("multipart: range read straddling the part boundary");
    assert_eq!(
        &straddle.data[..],
        &payload[MULTIPART_PART_SIZE - 4..MULTIPART_PART_SIZE + 4],
        "multipart: a range spanning two parts must be contiguous"
    );

    // Abort must leave nothing readable. Compaction's crash story assumes an
    // interrupted upload never becomes a visible object (ADR-0034 decision
    // 3: orphaned uploads degrade to wasted work, not to corrupt state).
    let aborted_key = format!("{prefix}aborted");
    let mut aborted = client
        .put_multipart(&OsPath::from(aborted_key.as_str()))
        .await
        .expect("multipart: CreateMultipartUpload for the abort probe");
    aborted
        .put_part(PutPayload::from(vec![7u8; MULTIPART_TAIL_SIZE]))
        .await
        .expect("multipart: UploadPart for the abort probe");
    aborted
        .abort()
        .await
        .expect("multipart: AbortMultipartUpload must succeed");
    assert!(
        matches!(store.head(&aborted_key).await, Err(StoreError::NotFound)),
        "multipart: an aborted upload must not be visible as an object"
    );
}

#[tokio::test]
async fn memory_store_contract() {
    let store = MemoryStore::new();
    run_contract_suite(&store, "memory").await;
}

#[tokio::test]
async fn memory_store_paged_contract() {
    let store = MemoryStore::with_page_size(2);
    run_contract_suite(&store, "memory-paged").await;
}

#[tokio::test]
async fn fault_store_empty_plan_contract() {
    let store = FaultStore::new(MemoryStore::new(), FaultPlan::empty());
    run_contract_suite(&store, "fault-empty").await;
}

/// Proves the path taken for issue #181: `object_store` 0.14's `AmazonS3`
/// client has no per-request checksum hook and no way to attach a
/// caller-supplied CRC32C to an outgoing `put` (its only integrity knob,
/// `AmazonS3Builder::with_checksum_algorithm`, is a whole-client setting
/// limited to SHA-256/CRC64NVME, and always has the crate compute the
/// digest itself). So `S3Store` must report `upload_checksum` as
/// unsupported rather than claim a mandatory capability
/// (docs/object-store-contract.md "Mandatory capabilities") it cannot honor
/// on the wire; production startup must fail loudly on this, per the
/// contract, rather than trust integrity this adapter does not provide.
/// No `RAVEL_MINIO_URL` gate needed: `AmazonS3Builder::build` only
/// validates configuration, it never talks to the network.
#[test]
fn s3_store_reports_upload_checksum_unsupported() {
    let config = S3Config {
        bucket: "test-bucket".to_string(),
        region: "us-east-1".to_string(),
        endpoint: Some("http://localhost:0".to_string()),
        access_key_id: "test".to_string(),
        secret_access_key: "test".to_string(),
        allow_http: true,
        force_path_style: true,
    };
    let store = S3Store::new(config).expect("dummy config must build without network access");
    assert!(
        !store.capabilities().upload_checksum,
        "S3Store must not claim upload_checksum support object_store 0.14 cannot provide"
    );
}

/// Real S3/MinIO conformance test (ADR-0010 §12: the memory oracle alone
/// cannot catch a backend's conditional-put mapping). Gated on
/// `RAVEL_MINIO_URL` so the suite skips cleanly wherever no MinIO is
/// reachable (e.g. this sandbox, most laptops, unconfigured CI runners).
///
/// Optional overrides: `RAVEL_MINIO_BUCKET` (must already exist -- this
/// crate does not create buckets), `RAVEL_MINIO_ACCESS_KEY`,
/// `RAVEL_MINIO_SECRET_KEY`, `RAVEL_MINIO_REGION`.
#[tokio::test]
async fn minio_contract() {
    let Ok(url) = env::var("RAVEL_MINIO_URL") else {
        println!("skipping MinIO contract test: RAVEL_MINIO_URL not set");
        return;
    };
    let bucket =
        env::var("RAVEL_MINIO_BUCKET").unwrap_or_else(|_| "ravel-object-store-test".to_string());
    let access_key_id =
        env::var("RAVEL_MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_access_key =
        env::var("RAVEL_MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let region = env::var("RAVEL_MINIO_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let allow_http = url.starts_with("http://");

    let config = S3Config {
        bucket,
        region,
        endpoint: Some(url),
        access_key_id,
        secret_access_key,
        allow_http,
        force_path_style: true,
    };
    // A small page size, like `memory_store_paged_contract`, so the
    // pagination assertion exercises `list_with_offset` continuation
    // against the real bucket instead of fitting in a single page.
    let store = S3Store::with_page_size(config, 2)
        .expect("S3Store::with_page_size must succeed with a valid config");

    // The bucket persists across runs; root every key under a run-unique
    // prefix so the pagination/completeness assertions only ever see keys
    // this run wrote, then best-effort clean them up afterward.
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the epoch")
        .as_nanos();
    let root = format!("contract-{run_id}");

    run_contract_suite(&store, &root).await;

    let leftovers = list_all(&store, &format!("{root}/"))
        .await
        .expect("list_all for cleanup");
    for meta in leftovers {
        let _ = store.delete(&meta.key).await;
    }
}

/// Real floci conformance test: the capability gate ADR-0034 decision 8 makes
/// the first task of the k8s epic. floci is a LocalStack-class S3 emulator
/// (single native-binary container, S3 on port 4566, path-style addressing),
/// proposed as the fake backend for the kind development environment and the
/// k8s CI lane. Whether its S3 implements Ravel's mandatory capability set
/// and multipart is the open question this test answers; the ADR's fallback
/// if it does not is MinIO, which is already proven in this repo's CI.
///
/// Gated on `RAVEL_FLOCI_URL` exactly like [`minio_contract`], so the suite
/// skips cleanly wherever no floci is reachable. Optional overrides:
/// `RAVEL_FLOCI_BUCKET` (must already exist -- this crate does not create
/// buckets), `RAVEL_FLOCI_ACCESS_KEY`, `RAVEL_FLOCI_SECRET_KEY`,
/// `RAVEL_FLOCI_REGION`. The credential defaults are floci's documented
/// dummy values; it accepts any credentials.
///
/// Beyond the shared suite this runs [`assert_mandatory_capabilities`] and
/// [`assert_multipart_upload`], the two probes that decide the ADR's
/// question: every mode enforces `Capabilities::mandatory()` at startup and
/// `Mode::Maintain` additionally enforces `multipart`.
#[tokio::test]
async fn floci_contract() {
    let Ok(url) = env::var("RAVEL_FLOCI_URL") else {
        println!("skipping floci contract test: RAVEL_FLOCI_URL not set");
        return;
    };
    let bucket =
        env::var("RAVEL_FLOCI_BUCKET").unwrap_or_else(|_| "ravel-object-store-test".to_string());
    let access_key_id = env::var("RAVEL_FLOCI_ACCESS_KEY").unwrap_or_else(|_| "test".to_string());
    let secret_access_key =
        env::var("RAVEL_FLOCI_SECRET_KEY").unwrap_or_else(|_| "test".to_string());
    let region = env::var("RAVEL_FLOCI_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let allow_http = url.starts_with("http://");

    // `force_path_style: true` matches what `ravel-server` hardcodes and what
    // floci addresses natively; its virtual-hosted form needs a
    // `localhost.floci.io` DNS name Ravel never configures.
    let config = S3Config {
        bucket,
        region,
        endpoint: Some(url),
        access_key_id,
        secret_access_key,
        allow_http,
        force_path_style: true,
    };
    // A small page size, as in `minio_contract`, so the pagination assertion
    // exercises `list_with_offset` continuation against the real bucket.
    let store = S3Store::with_page_size(config.clone(), 2)
        .expect("S3Store::with_page_size must succeed with a valid config");

    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the epoch")
        .as_nanos();
    let root = format!("contract-floci-{run_id}");

    run_contract_suite(&store, &root).await;
    assert_mandatory_capabilities(&store, &format!("{root}/mandatory/")).await;
    assert_multipart_upload(&config, &store, &format!("{root}/multipart/")).await;

    let leftovers = list_all(&store, &format!("{root}/"))
        .await
        .expect("list_all for cleanup");
    for meta in leftovers {
        let _ = store.delete(&meta.key).await;
    }
}
