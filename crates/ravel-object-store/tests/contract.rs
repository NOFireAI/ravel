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
use ravel_object_store::fault::{FaultPlan, FaultStore};
use ravel_object_store::instrument::{StoreErrorClass, StoreOp};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{MULTIPART_THRESHOLD, S3Config, S3Store};
use ravel_object_store::{
    Capabilities, GetRange, InstrumentedStore, MULTIPART_MIN_PART_SIZE, ObjectStoreBackend,
    PutMode, PutOptions, StoreError, UploadChecksum, list_all,
};

/// Runs every contract assertion against `store`. Each assertion gets its
/// own key sub-prefix under `root` so they can share one backend instance
/// (and, for the MinIO case, one long-lived bucket) without colliding.
async fn run_contract_suite(store: &dyn ObjectStoreBackend, root: &str) {
    assert_satisfies_mandatory_capabilities(store);
    assert_create_if_absent_atomicity(store, &format!("{root}/create-if-absent/")).await;
    assert_cas_version_semantics(store, &format!("{root}/cas/")).await;
    assert_range_and_suffix_reads(store, &format!("{root}/range/")).await;
    assert_paginated_listing_completeness(store, &format!("{root}/list/")).await;
    assert_delimited_listing(store, &format!("{root}/delim/")).await;
    assert_idempotent_delete(store, &format!("{root}/delete/")).await;
    assert_upload_checksum_verification(store, &format!("{root}/checksum/")).await;
    assert_multipart_upload(store, &format!("{root}/multipart/")).await;
}

/// Every backend the suite runs must report at least the capability set
/// ravel-server's startup gate requires (`Capabilities::mandatory`, checked by
/// `ravel_server::store::check_capabilities` for every non-maintain mode). A
/// backend may report more than the mandatory set: `MemoryStore` supports
/// `upload_checksum` on the wire, `S3Store` cannot (issue #251), and both are
/// startable. Asserting `satisfies` rather than equality keeps that difference
/// legal while still failing the moment a backend loses a flag production
/// needs.
///
/// Distinct from [`assert_mandatory_capabilities`] below: this is a cheap,
/// synchronous check of the backend's static `capabilities()` declaration,
/// run for every backend the suite exercises. The other function does live
/// per-flag round trips against a real S3-compatible endpoint and is called
/// only from the S3/MinIO/floci-specific tests.
fn assert_satisfies_mandatory_capabilities(store: &dyn ObjectStoreBackend) {
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

    // Declared capabilities must be exactly the mandatory set plus
    // `multipart`, which `S3Store` implements (issue #243) and which
    // `required_capabilities(Mode::Maintain)` demands. `upload_checksum` is not
    // part of `Capabilities::mandatory()` (issue #251: it is a permanent
    // `object_store` 0.14 client-library limitation, not an endpoint-specific
    // gap, so requiring it made the only durable backend unstartable
    // everywhere). `S3Store::capabilities()` is a constant of the adapter, not
    // a function of the endpoint, so this asserts the same thing for floci as
    // it would for MinIO or real AWS S3. Written as an exact equality rather
    // than `satisfies(..)` so movement in either direction -- a backend gaining
    // a capability or the adapter losing one -- fails here instead of passing
    // silently.
    assert_eq!(
        store.capabilities(),
        s3_expected_capabilities(),
        "declared capabilities must be exactly the mandatory set plus multipart \
         (issues #251, #243)"
    );
    // The behavior behind the flag still has to hold: the local CRC32C
    // pre-flight rejects a mismatch without creating the object.
    assert_upload_checksum_verification(store, &format!("{prefix}upload-checksum/")).await;
}

/// S3's minimum size for any part but the last, so the probe drives a real
/// two-part upload instead of a degenerate single-part one.
const MULTIPART_PART_SIZE: usize = MULTIPART_MIN_PART_SIZE;
/// Short final part, also the size of the trailing read below.
const MULTIPART_TAIL_SIZE: usize = 1024;

/// Position-dependent bytes: parts assembled out of order, a dropped part, or
/// a duplicated one all change the content, which a constant fill would hide.
fn multipart_payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Multipart probe, run against every backend (issue #243).
/// `Mode::Maintain` requires the `multipart` capability on top of
/// [`Capabilities::mandatory()`] (services/ravel-server `store.rs`
/// `required_capabilities`), so a backend that cannot serve the
/// create/upload-part/complete/abort sequence cannot carry compaction.
///
/// Written entirely against [`ObjectStoreBackend::put_multipart`], so it holds
/// for the memory oracle, the wrappers, and `S3Store` against a real endpoint
/// alike, and the flag and the behavior are checked against each other in both
/// directions: a backend declaring `multipart: false` must refuse the call.
/// The S3-only proof that parts really went out separately (the composite
/// ETag) lives in [`assert_multipart_composite_etag`].
async fn assert_multipart_upload(store: &dyn ObjectStoreBackend, prefix: &str) {
    if !store.capabilities().multipart {
        // `expect_err` is unavailable here: the `Ok` type is a boxed trait
        // object, which cannot be `Debug`.
        match store.put_multipart(&format!("{prefix}unsupported")).await {
            Err(StoreError::Permanent(_)) => {}
            Err(other) => panic!("multipart: false must refuse with Permanent, got {other:?}"),
            Ok(_) => panic!("a backend declaring multipart: false must refuse put_multipart"),
        }
        return;
    }

    let key = format!("{prefix}object");
    let payload = multipart_payload(MULTIPART_PART_SIZE + MULTIPART_TAIL_SIZE);

    let mut upload = store
        .put_multipart(&key)
        .await
        .expect("multipart: starting the upload must succeed");
    upload
        .put_part(Bytes::from(payload[..MULTIPART_PART_SIZE].to_vec()), None)
        .await
        .expect("multipart: part 1 (5 MiB, the non-final-part minimum) must succeed");

    // Mid-upload the object does not exist: an interrupted compaction upload
    // must never be readable as a truncated object (ADR-0034 decision 3,
    // orphaned uploads are wasted work, not corrupt state).
    assert!(
        matches!(store.head(&key).await, Err(StoreError::NotFound)),
        "multipart: an incomplete upload must not be visible"
    );

    upload
        .put_part(Bytes::from(payload[MULTIPART_PART_SIZE..].to_vec()), None)
        .await
        .expect("multipart: part 2 (short final part) must succeed");
    upload
        .complete()
        .await
        .expect("multipart: complete must succeed");

    // Byte-for-byte equality, asserted without printing 5 MiB on failure.
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
    assert!(
        got.data[..] == payload[..],
        "multipart: parts must reassemble in order, with no gap and no \
         duplication (crc {:08x}, expected {:08x})",
        crc32c::crc32c(&got.data),
        crc32c::crc32c(&payload)
    );
    assert_eq!(
        store.head(&key).await.expect("multipart: head").size,
        payload.len() as u64,
        "multipart: head must report the assembled size"
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

    // A completed upload is spent: no second complete, no late part.
    let err = upload
        .complete()
        .await
        .expect_err("multipart: completing twice must fail");
    assert!(matches!(err, StoreError::Permanent(_)), "got {err:?}");

    // Abort must leave nothing readable, and must not resurrect the key
    // later either: the object is simply never created.
    let aborted_key = format!("{prefix}aborted");
    let mut aborted = store
        .put_multipart(&aborted_key)
        .await
        .expect("multipart: starting the abort probe must succeed");
    aborted
        .put_part(Bytes::from(multipart_payload(MULTIPART_PART_SIZE)), None)
        .await
        .expect("multipart: part 1 of the abort probe must succeed");
    aborted
        .abort()
        .await
        .expect("multipart: abort must succeed");
    assert!(
        matches!(store.head(&aborted_key).await, Err(StoreError::NotFound)),
        "multipart: an aborted upload must not be visible as an object"
    );
    assert!(
        matches!(
            store.get(&aborted_key, GetRange::Full).await,
            Err(StoreError::NotFound)
        ),
        "multipart: an aborted upload must not be readable, not even partially"
    );
    let err = aborted
        .put_part(Bytes::from_static(b"late"), None)
        .await
        .expect_err("multipart: a part after abort must fail");
    assert!(matches!(err, StoreError::Permanent(_)), "got {err:?}");
    let err = aborted
        .complete()
        .await
        .expect_err("multipart: completing an aborted upload must fail");
    assert!(matches!(err, StoreError::Permanent(_)), "got {err:?}");

    assert_multipart_sequence_rules(store, &format!("{prefix}rules/")).await;
    assert_multipart_part_checksum(store, &format!("{prefix}part-checksum/")).await;
}

/// The part-sequence rules every backend enforces locally, so an illegal
/// sequence fails at the offending call with a typed error rather than at
/// `complete` with a backend-specific `EntityTooSmall` (or, on the oracle, not
/// at all). None of these may leave an object behind.
/// What `S3Store` declares: [`Capabilities::mandatory()`] plus `multipart`
/// (implemented in issue #243, required by `required_capabilities(Mode::
/// Maintain)`) and minus `upload_checksum` (unsatisfiable, issue #251).
/// `mandatory()` itself keeps `multipart: false`, because multipart is
/// mode-conditional rather than universally required.
fn s3_expected_capabilities() -> Capabilities {
    Capabilities {
        multipart: true,
        ..Capabilities::mandatory()
    }
}

async fn assert_multipart_sequence_rules(store: &dyn ObjectStoreBackend, prefix: &str) {
    // No parts at all.
    let empty_key = format!("{prefix}no-parts");
    let mut empty = store
        .put_multipart(&empty_key)
        .await
        .expect("start upload with no parts");
    let err = empty
        .complete()
        .await
        .expect_err("multipart: completing with zero parts must fail");
    assert!(matches!(err, StoreError::Permanent(_)), "got {err:?}");
    assert!(
        matches!(store.head(&empty_key).await, Err(StoreError::NotFound)),
        "multipart: a rejected complete must not create the object"
    );

    // A zero-length part.
    let zero_key = format!("{prefix}zero-length-part");
    let mut zero = store
        .put_multipart(&zero_key)
        .await
        .expect("start upload for the zero-length part probe");
    let err = zero
        .put_part(Bytes::new(), None)
        .await
        .expect_err("multipart: an empty part must be rejected");
    assert!(matches!(err, StoreError::Permanent(_)), "got {err:?}");
    zero.abort().await.expect("abort after a rejected part");

    // A non-final part below the 5 MiB minimum: legal as the last part, so it
    // is the *next* part that makes it illegal, and that is the call that
    // fails. A single short part still completes fine (asserted after).
    let small_key = format!("{prefix}small-non-final-part");
    let mut small = store
        .put_multipart(&small_key)
        .await
        .expect("start upload for the short-non-final-part probe");
    small
        .put_part(Bytes::from(multipart_payload(MULTIPART_TAIL_SIZE)), None)
        .await
        .expect("multipart: a short part is legal until another follows it");
    let err = small
        .put_part(Bytes::from(multipart_payload(MULTIPART_TAIL_SIZE)), None)
        .await
        .expect_err("multipart: a part following a sub-5-MiB part must be rejected");
    assert!(matches!(err, StoreError::Permanent(_)), "got {err:?}");
    small
        .abort()
        .await
        .expect("abort after a rejected sequence");
    assert!(
        matches!(store.head(&small_key).await, Err(StoreError::NotFound)),
        "multipart: a rejected sequence must not create the object"
    );

    // A rejected part poisons the handle (issue #297): a `complete` afterward
    // must error rather than publish the truncated single short part. The
    // sequence check runs before any backend call, so this covers the memory
    // oracle and the S3-shaped path alike. `abort` stays callable throughout.
    let poisoned_key = format!("{prefix}poisoned-then-complete");
    let mut poisoned = store
        .put_multipart(&poisoned_key)
        .await
        .expect("start upload for the poison-then-complete probe");
    poisoned
        .put_part(Bytes::from(multipart_payload(MULTIPART_TAIL_SIZE)), None)
        .await
        .expect("multipart: a short part is legal until another follows it");
    let err = poisoned
        .put_part(Bytes::from(multipart_payload(MULTIPART_TAIL_SIZE)), None)
        .await
        .expect_err("multipart: the part making a short part non-final must be rejected");
    assert!(matches!(err, StoreError::Permanent(_)), "got {err:?}");
    // The poisoned handle refuses a further part non-retryably...
    let err = poisoned
        .put_part(Bytes::from(multipart_payload(MULTIPART_PART_SIZE)), None)
        .await
        .expect_err("multipart: a poisoned handle must refuse further parts");
    assert!(matches!(err, StoreError::Permanent(_)), "got {err:?}");
    assert!(
        !err.is_retryable(),
        "the poison error must be non-retryable"
    );
    // ...and `complete` errors instead of publishing a truncated object.
    let err = poisoned
        .complete()
        .await
        .expect_err("multipart: completing after a rejected part must fail, not truncate");
    assert!(matches!(err, StoreError::Permanent(_)), "got {err:?}");
    assert!(
        matches!(store.head(&poisoned_key).await, Err(StoreError::NotFound)),
        "multipart: a poisoned upload must not publish a truncated object"
    );
    poisoned
        .abort()
        .await
        .expect("multipart: abort stays callable on a poisoned handle");
    assert!(
        matches!(store.head(&poisoned_key).await, Err(StoreError::NotFound)),
        "multipart: aborting a poisoned upload leaves no object"
    );

    // The legal degenerate case: one part, smaller than the minimum, is a
    // complete object. `S3Store::put` never produces this shape (it only goes
    // multipart above 16 MiB), but a caller streaming parts may.
    let single_key = format!("{prefix}single-short-part");
    let single_payload = multipart_payload(MULTIPART_TAIL_SIZE);
    let mut single = store
        .put_multipart(&single_key)
        .await
        .expect("start single-short-part upload");
    single
        .put_part(Bytes::from(single_payload.clone()), None)
        .await
        .expect("multipart: a lone short part must be accepted");
    single
        .complete()
        .await
        .expect("multipart: a single-part upload must complete");
    let got = store
        .get(&single_key, GetRange::Full)
        .await
        .expect("read the single-part object");
    assert_eq!(&got.data[..], &single_payload[..]);
}

/// Per-part checksums behave like `put`'s: a local CRC32C pre-flight that
/// rejects a caller/payload mismatch before anything is sent, leaves the part
/// uncounted, and leaves the upload usable. There is no whole-object checksum
/// for a multipart upload; see docs/object-store-contract.md.
async fn assert_multipart_part_checksum(store: &dyn ObjectStoreBackend, prefix: &str) {
    let key = format!("{prefix}object");
    let part = multipart_payload(MULTIPART_TAIL_SIZE);
    let good = crc32c::crc32c(&part);

    let mut upload = store
        .put_multipart(&key)
        .await
        .expect("start upload for the part-checksum probe");
    let err = upload
        .put_part(
            Bytes::from(part.clone()),
            Some(UploadChecksum::Crc32c(good ^ 1)),
        )
        .await
        .expect_err("multipart: a part whose checksum does not match must be rejected");
    assert!(matches!(err, StoreError::Corrupted(_)), "got {err:?}");
    // The rejected part was never a part, so the upload is still open and the
    // caller can send the same bytes with the right checksum.
    upload
        .put_part(
            Bytes::from(part.clone()),
            Some(UploadChecksum::Crc32c(good)),
        )
        .await
        .expect("multipart: a part with a matching checksum must be accepted");
    upload
        .complete()
        .await
        .expect("multipart: complete after a rejected part");
    let got = store
        .get(&key, GetRange::Full)
        .await
        .expect("read the object back");
    assert_eq!(
        &got.data[..],
        &part[..],
        "multipart: the rejected part must not appear in the object"
    );
}

/// S3-only proof that the parts really went out as parts: S3 gives a multipart
/// object the composite `"<digest>-<partcount>"` ETag form, and the count must
/// be the number of `put_part` calls. Without it, an adapter that quietly
/// buffered every part and wrote one PUT would satisfy every byte-level
/// assertion in [`assert_multipart_upload`] while implementing none of the
/// multipart API compaction needs.
async fn assert_multipart_composite_etag(store: &dyn ObjectStoreBackend, prefix: &str) {
    let key = format!("{prefix}object");
    let mut upload = store
        .put_multipart(&key)
        .await
        .expect("multipart: starting the composite-ETag upload must succeed");
    upload
        .put_part(Bytes::from(multipart_payload(MULTIPART_PART_SIZE)), None)
        .await
        .expect("multipart: part 1 must succeed");
    upload
        .put_part(Bytes::from(multipart_payload(MULTIPART_TAIL_SIZE)), None)
        .await
        .expect("multipart: part 2 must succeed");
    let outcome = upload
        .complete()
        .await
        .expect("multipart: complete must succeed");
    let etag = outcome.etag.0.trim_matches('"').to_string();
    assert!(
        etag.ends_with("-2"),
        "multipart: completed object must carry the composite 2-part ETag \
         proving the parts were uploaded separately, got {etag:?}"
    );
    assert_eq!(
        outcome.etag.0, outcome.version.0,
        "S3Store derives both Etag and Version from the response ETag"
    );
}

/// `S3Store::put` itself switches to multipart above
/// [`ravel_object_store::s3::MULTIPART_THRESHOLD`], and the switch must be
/// invisible to the caller: same `PutOutcome` shape, same bytes back. The
/// composite ETag part count proves the threshold path was actually taken
/// (a 16 MiB + 1 byte payload cut into 8 MiB parts is three parts).
async fn assert_put_above_threshold_uses_multipart(store: &dyn ObjectStoreBackend, prefix: &str) {
    let key = format!("{prefix}object");
    let payload = multipart_payload(MULTIPART_THRESHOLD + 1);
    let outcome = store
        .put(&key, Bytes::from(payload.clone()), PutOptions::default())
        .await
        .expect("put above the multipart threshold must succeed");
    let etag = outcome.etag.0.trim_matches('"').to_string();
    assert!(
        etag.ends_with("-3"),
        "put above the threshold must have uploaded 3 parts (8 MiB, 8 MiB, 1 \
         byte), which the composite ETag records, got {etag:?}"
    );
    let got = store
        .get(&key, GetRange::Full)
        .await
        .expect("read back the multipart-written object");
    assert!(
        got.data[..] == payload[..],
        "a multipart-written object must read back byte for byte (crc {:08x}, \
         expected {:08x})",
        crc32c::crc32c(&got.data),
        crc32c::crc32c(&payload)
    );

    // A conditional put of the same size stays on the single-PUT path,
    // because multipart completion cannot carry a precondition: it must still
    // behave exactly like `CreateIfAbsent` at any size.
    let conditional_key = format!("{prefix}conditional");
    store
        .put(
            &conditional_key,
            Bytes::from(payload.clone()),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("a large create-if-absent put must succeed on the single-PUT path");
    let single_etag = store
        .head(&conditional_key)
        .await
        .expect("head the conditionally written object")
        .etag
        .0
        .trim_matches('"')
        .to_string();
    assert!(
        !single_etag.contains('-'),
        "a create-if-absent put must not go multipart (its precondition would \
         be lost); expected a non-composite ETag, got {single_etag:?}"
    );
    let err = store
        .put(
            &conditional_key,
            Bytes::from(payload),
            PutOptions::create_if_absent(),
        )
        .await
        .expect_err("the second create-if-absent must still fail at this size");
    assert!(matches!(err, StoreError::AlreadyExists), "got {err:?}");
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

/// The instrumentation decorator must be transparent (issue #272): every
/// contract assertion, capability check included, holds identically with it in
/// the path, because it forwards each result verbatim and passes
/// `capabilities()` through. The counter assertions afterward prove the suite
/// really ran *through* the decorator rather than past it, which is what makes
/// this a transparency test instead of a second memory-oracle run.
#[tokio::test]
async fn instrumented_memory_store_contract() {
    let store = InstrumentedStore::new(MemoryStore::new());
    let metrics = store.metrics();
    run_contract_suite(&store, "instrumented-memory").await;

    let snap = metrics.snapshot();
    for op in StoreOp::ALL {
        assert!(
            snap.op(op).calls > 0,
            "the suite must have driven {} through the decorator",
            op.name()
        );
    }
    // The suite writes objects and reads ranges, so both byte counters moved,
    // and its deliberate failures (AlreadyExists, PreconditionFailed,
    // InvalidRange, NotFound, Corrupted) are classified rather than lost.
    assert!(snap.put.bytes > 0, "put bytes must have been counted");
    assert!(snap.get.bytes > 0, "get bytes must have been counted");
    for op in StoreOp::ALL {
        let block = snap.op(op);
        assert_eq!(
            block.errors_total(),
            block.calls - block.ok,
            "{}: every failed call must land in exactly one error class",
            op.name()
        );
    }
    assert!(
        snap.put.error_count(StoreErrorClass::AlreadyExists) > 0
            && snap.put.error_count(StoreErrorClass::PreconditionFailed) > 0
            && snap.put.error_count(StoreErrorClass::Corrupted) > 0
            && snap.get.error_count(StoreErrorClass::InvalidRange) > 0
            && snap.head.error_count(StoreErrorClass::NotFound) > 0,
        "the suite's scripted failures must be classified, got {snap:?}"
    );
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
/// therefore also pins `S3Store`'s reported set to the mandatory set plus
/// `multipart`, so the server's startup gate cannot start rejecting
/// `--store s3` again.
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
        s3_expected_capabilities(),
        "S3Store must report exactly the mandatory set plus multipart, so the \
         server's startup gate accepts --store s3 in every mode (#251, #243)"
    );
}

/// The startup gate `--mode maintain` applies, asserted against the real
/// `S3Store` without an endpoint (issue #243): `required_capabilities(Mode::
/// Maintain)` is `mandatory()` with `multipart: true`, and this is the
/// predicate `ravel_server::store::check_capabilities` evaluates. Restated here
/// rather than imported, because ravel-object-store must not depend on
/// ravel-server; the equality assertion in
/// `s3_store_reports_upload_checksum_unsupported` above is what keeps the two
/// from drifting apart silently.
#[test]
fn s3_store_satisfies_maintain_mode_capabilities() {
    let store = S3Store::new(S3Config {
        bucket: "test-bucket".to_string(),
        region: "us-east-1".to_string(),
        endpoint: Some("http://localhost:0".to_string()),
        access_key_id: "test".to_string(),
        secret_access_key: "test".to_string(),
        allow_http: true,
        force_path_style: true,
    })
    .expect("dummy config must build without network access");
    let maintain_required = Capabilities {
        multipart: true,
        ..Capabilities::mandatory()
    };
    assert!(
        store.capabilities().satisfies(&maintain_required),
        "S3Store must satisfy maintain mode's capability set so --mode maintain \
         can start against an S3-compatible backend, got {:?}",
        store.capabilities()
    );
    assert!(
        MemoryStore::new()
            .capabilities()
            .satisfies(&maintain_required),
        "MemoryStore must satisfy maintain mode's capability set too"
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
    // The two S3-shaped multipart proofs the memory oracle cannot give: that
    // the parts went out as parts (composite ETag) and that `put` itself takes
    // the multipart path above its threshold (issue #243).
    assert_multipart_composite_etag(&store, &format!("{root}/multipart-etag/")).await;
    assert_put_above_threshold_uses_multipart(&store, &format!("{root}/multipart-threshold/"))
        .await;

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
/// Beyond the shared suite (which now includes [`assert_multipart_upload`] for
/// every backend) this runs [`assert_mandatory_capabilities`] and the two
/// S3-shaped multipart proofs, together deciding the ADR's question: every mode
/// enforces `Capabilities::mandatory()` at startup and `Mode::Maintain`
/// additionally enforces `multipart`.
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
    let store = S3Store::with_page_size(config, 2)
        .expect("S3Store::with_page_size must succeed with a valid config");

    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the epoch")
        .as_nanos();
    let root = format!("contract-floci-{run_id}");

    run_contract_suite(&store, &root).await;
    assert_mandatory_capabilities(&store, &format!("{root}/mandatory/")).await;
    assert_multipart_composite_etag(&store, &format!("{root}/multipart-etag/")).await;
    assert_put_above_threshold_uses_multipart(&store, &format!("{root}/multipart-threshold/"))
        .await;

    let leftovers = list_all(&store, &format!("{root}/"))
        .await
        .expect("list_all for cleanup");
    for meta in leftovers {
        let _ = store.delete(&meta.key).await;
    }
}
