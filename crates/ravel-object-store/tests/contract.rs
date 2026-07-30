//! Shared contract test suite (docs/object-store-contract.md), run against
//! every `ObjectStoreBackend` implementation this crate ships: the memory
//! oracle at its default page size and at a tiny one to force pagination,
//! `FaultStore` wrapping the oracle with an empty plan (must be fully
//! transparent), and -- gated on `RAVEL_MINIO_URL` -- `S3Store` against a
//! real MinIO.
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
    assert_mandatory_capabilities(store);
    assert_create_if_absent_atomicity(store, &format!("{root}/create-if-absent/")).await;
    assert_cas_version_semantics(store, &format!("{root}/cas/")).await;
    assert_range_and_suffix_reads(store, &format!("{root}/range/")).await;
    assert_paginated_listing_completeness(store, &format!("{root}/list/")).await;
    assert_delimited_listing(store, &format!("{root}/delim/")).await;
    assert_idempotent_delete(store, &format!("{root}/delete/")).await;
    assert_upload_checksum_verification(store, &format!("{root}/checksum/")).await;
}

/// Every backend the suite runs must report at least the capability set
/// ravel-server's startup gate requires (`Capabilities::mandatory`, checked by
/// `ravel_server::store::check_capabilities` for every non-maintain mode). A
/// backend may report more than the mandatory set: `MemoryStore` supports
/// `upload_checksum` on the wire, `S3Store` cannot (issue #251), and both are
/// startable. Asserting `satisfies` rather than equality keeps that difference
/// legal while still failing the moment a backend loses a flag production
/// needs.
fn assert_mandatory_capabilities(store: &dyn ObjectStoreBackend) {
    let caps = store.capabilities();
    assert!(
        caps.satisfies(&Capabilities::mandatory()),
        "backend must satisfy the mandatory capability set the server gates \
         startup on, got {caps:?}"
    );
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
/// digest itself). So `S3Store` must report `upload_checksum` as unsupported
/// rather than claim on-the-wire integrity it cannot honor.
///
/// Because that gap is permanent and applies to every S3-compatible endpoint,
/// `upload_checksum` is not startup-gating: it is not in
/// `Capabilities::mandatory()` and no mode may require it
/// (docs/object-store-contract.md, "Upload checksums"; issue #251). This test
/// therefore also pins `S3Store`'s reported set to exactly the mandatory set,
/// so the server's startup gate cannot start rejecting `--store s3` again.
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
    assert_eq!(
        store.capabilities(),
        Capabilities::mandatory(),
        "S3Store must report exactly the mandatory set, so the server's \
         startup gate accepts --store s3 in every non-maintain mode (#251)"
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
