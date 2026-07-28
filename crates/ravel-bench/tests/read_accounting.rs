//! Unit tests for the read-path GET accounting wrappers (issue #97 phase b).
//!
//! Gated on `parquet-baseline`: without the feature this file compiles to
//! nothing, so `cargo test -p ravel-bench` (the standard gate) is unaffected.
#![cfg(feature = "parquet-baseline")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use ravel_bench::read_accounting::{CountingBackend, CountingObjectStore};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};

use object_store::memory::InMemory;
use object_store::path::Path as OsPath;
use object_store::{GetOptions, GetRange as OsGetRange, ObjectStore, ObjectStoreExt, PutPayload};

// -------------------------------------------------------- RSEG-side wrapper

#[tokio::test]
async fn counting_backend_scripts_exact_gets_and_bytes() {
    let inner = MemoryStore::new();
    // A 100-byte object.
    let body = Bytes::from((0u8..100).collect::<Vec<u8>>());
    inner
        .put("seg", body, PutOptions::default())
        .await
        .expect("seed");
    let store = CountingBackend::new(inner);

    // Scripted sequence: a 64-byte suffix, two bounded ranges (10 and 5
    // bytes), and one full read (100 bytes). Four GETs, 179 bytes.
    store
        .get("seg", GetRange::Suffix(64))
        .await
        .expect("suffix");
    store
        .get("seg", GetRange::Range(0, 10))
        .await
        .expect("range a");
    store
        .get("seg", GetRange::Range(20, 25))
        .await
        .expect("range b");
    store.get("seg", GetRange::Full).await.expect("full");

    let (gets, bytes, heads) = store.snapshot();
    assert_eq!(gets, 4, "one GET per get() call");
    assert_eq!(bytes, 64 + 10 + 5 + 100, "sum of returned range lengths");
    assert_eq!(heads, 0);
}

#[tokio::test]
async fn counting_backend_counts_head_separately_and_resets() {
    let inner = MemoryStore::new();
    inner
        .put("k", Bytes::from_static(b"payload"), PutOptions::default())
        .await
        .expect("seed");
    let store = CountingBackend::new(inner);

    store.head("k").await.expect("head");
    store.get("k", GetRange::Full).await.expect("get");
    let (gets, bytes, heads) = store.snapshot();
    assert_eq!(gets, 1);
    assert_eq!(bytes, 7);
    assert_eq!(heads, 1, "head is not a GET");

    store.reset();
    assert_eq!(store.snapshot(), (0, 0, 0));
    // put/list/delete never move the GET counters.
    store
        .put("k2", Bytes::from_static(b"x"), PutOptions::default())
        .await
        .expect("put");
    store.list_delimited("").await.expect("list");
    store.delete("k2").await.expect("delete");
    assert_eq!(store.snapshot(), (0, 0, 0));
}

#[tokio::test]
async fn counting_backend_suffix_is_clamped_to_object_size() {
    let inner = MemoryStore::new();
    inner
        .put("small", Bytes::from_static(b"abcd"), PutOptions::default())
        .await
        .expect("seed");
    let store = CountingBackend::new(inner);
    // A suffix larger than the object returns the whole 4-byte object.
    let got = store
        .get("small", GetRange::Suffix(64))
        .await
        .expect("suffix");
    assert_eq!(got.data.len(), 4);
    assert_eq!(store.snapshot(), (1, 4, 0));
}

// --------------------------------------------------- object_store wrapper

async fn seed_inmemory(bytes: usize) -> Arc<dyn ObjectStore> {
    let inner = InMemory::new();
    let body: Vec<u8> = (0..bytes).map(|i| i as u8).collect();
    inner
        .put(&OsPath::from("obj"), PutPayload::from(body))
        .await
        .expect("seed");
    Arc::new(inner)
}

#[tokio::test]
async fn counting_object_store_counts_ranges_and_bytes() {
    let store = CountingObjectStore::new(seed_inmemory(1000).await);
    let path = OsPath::from("obj");

    // get_range -> routed through get_opts: 1 GET, 100 bytes.
    let a = store.get_range(&path, 0..100).await.expect("get_range");
    assert_eq!(a.len(), 100);

    // A suffix via get_opts: 1 GET, 40 bytes.
    let opts = GetOptions {
        range: Some(OsGetRange::Suffix(40)),
        ..Default::default()
    };
    let r = store.get_opts(&path, opts).await.expect("suffix");
    assert_eq!(r.bytes().await.expect("bytes").len(), 40);

    let (gets, bytes, heads) = store.snapshot();
    assert_eq!(gets, 2, "one GET per range read");
    assert_eq!(bytes, 140);
    assert_eq!(heads, 0);
}

#[tokio::test]
async fn counting_object_store_head_and_head_flavored_get_opts_are_not_gets() {
    let store = CountingObjectStore::new(seed_inmemory(256).await);
    let path = OsPath::from("obj");

    store.head(&path).await.expect("head");
    // A head-flavored get_opts (size probe) also counts as a head, not a GET.
    let opts = GetOptions {
        head: true,
        ..Default::default()
    };
    store.get_opts(&path, opts).await.expect("head get_opts");

    let (gets, bytes, heads) = store.snapshot();
    assert_eq!(gets, 0);
    assert_eq!(bytes, 0);
    assert_eq!(heads, 2);

    store.reset();
    assert_eq!(store.snapshot(), (0, 0, 0));
}

#[tokio::test]
async fn counting_object_store_get_ranges_funnels_through_get_opts() {
    // 4 MiB object so two ranges can sit far enough apart that object_store's
    // range-coalescing (which merges gaps under ~1 MiB into one GET) leaves
    // them as two distinct GETs.
    let store = CountingObjectStore::new(seed_inmemory(4 << 20).await);
    let path = OsPath::from("obj");

    let ranges = [100u64..150, 3_000_000..3_000_030];
    let out = store.get_ranges(&path, &ranges).await.expect("get_ranges");
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].len(), 50);
    assert_eq!(out[1].len(), 30);

    let (gets, bytes, _) = store.snapshot();
    assert_eq!(gets, 2, "two ranges >1 MiB apart = two GETs");
    assert_eq!(bytes, 80);
}
