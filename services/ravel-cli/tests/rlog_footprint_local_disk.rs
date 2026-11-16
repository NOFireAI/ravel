//! `ravel-cli rlog footprint` with a local file at a store key's path.
//!
//! The test changes the working directory, which is process-wide, so it runs
//! alone in this test binary.
#![allow(clippy::expect_used)]

use ravel_cli::rlog_footprint;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use rlog_objects::{object_a, object_b};

mod rlog_objects;

/// A key resolved from the catalog is fetched from the store even when a local
/// file sits at the same path; only an explicit target reads local disk. The
/// path is relative to the working directory, since an absolute one starts
/// with `/`, which no store key can, and a relative one from the package
/// directory into `target/` needs a `..` segment, which no store key can hold
/// either. So the test works from `CARGO_TARGET_TMPDIR`.
#[tokio::test]
async fn catalog_keys_are_read_from_the_store_not_local_disk() {
    std::env::set_current_dir(env!("CARGO_TARGET_TMPDIR")).expect("cd");
    let dir = tempfile::Builder::new()
        .prefix("rlog-footprint-")
        .tempdir_in(".")
        .expect("tempdir");
    let a = object_a();
    let b = object_b();
    let path = std::path::Path::new(dir.path().file_name().expect("name")).join("same.rlog");
    std::fs::write(&path, &b).expect("write b");
    let key = path.to_str().expect("utf-8 path").to_string();
    assert!(ravel_object_store::is_addressable_key(&key), "{key}");
    let store = MemoryStore::new();
    store
        .put(
            &key,
            bytes::Bytes::from(a.clone()),
            ravel_object_store::PutOptions::default(),
        )
        .await
        .expect("put a");

    let from_store = rlog_footprint::footprint_keys(&store, std::slice::from_ref(&key))
        .await
        .expect("keys");
    assert_eq!(from_store.total.total_bytes, a.len() as u64);
    assert_eq!(from_store.total.record_count, 12);

    let from_disk = rlog_footprint::footprint_targets(&store, std::slice::from_ref(&key))
        .await
        .expect("targets");
    assert_eq!(from_disk.total.total_bytes, b.len() as u64);
    assert_eq!(from_disk.total.record_count, 6);
}
