//! Request-shape coverage for the ranged RLOG reader (issue #275).
//!
//! Before the ranged reader, the RLOG merge fetched every input `.rlog` object
//! whole twice: once in `load_input_catalog` (to read the footer) and once in
//! `build_parts` (to open a whole-object reader). This test wraps the store in
//! a per-key, per-range-kind counter and asserts, after a real compaction, that
//! no input data object is fetched whole at all -- it is read only by a footer
//! suffix probe plus section/block ranged GETs -- so the second whole download
//! is gone and raw resident bytes are bounded to one part plus one stream.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use common::*;
use ravel_maintain::{CompactionOutcome, CompactorConfig, FixedClock, compact_bucket};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
};

/// Per-key GET counts split by range kind.
#[derive(Default, Clone, Copy)]
struct GetCounts {
    full: u64,
    range: u64,
    suffix: u64,
}

/// A behavior-preserving decorator that counts GETs per key and range kind.
/// Only `get` is instrumented; every method delegates to the inner store.
struct CountingStore {
    inner: MemoryStore,
    gets: Mutex<HashMap<String, GetCounts>>,
}

impl CountingStore {
    fn new() -> Self {
        CountingStore {
            inner: MemoryStore::new(),
            gets: Mutex::new(HashMap::new()),
        }
    }

    /// Total full-object GETs across keys containing `substr`.
    fn full_gets_containing(&self, substr: &str) -> u64 {
        self.gets
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.contains(substr))
            .map(|(_, c)| c.full)
            .sum()
    }

    /// Total ranged GETs across keys containing `substr`.
    fn range_gets_containing(&self, substr: &str) -> u64 {
        self.gets
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.contains(substr))
            .map(|(_, c)| c.range)
            .sum()
    }

    /// Total suffix GETs across keys containing `substr`.
    fn suffix_gets_containing(&self, substr: &str) -> u64 {
        self.gets
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.contains(substr))
            .map(|(_, c)| c.suffix)
            .sum()
    }

    /// Number of distinct keys containing `substr` that received a suffix GET.
    fn keys_suffix_probed(&self, substr: &str) -> usize {
        self.gets
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, c)| k.contains(substr) && c.suffix > 0)
            .count()
    }
}

#[async_trait]
impl ObjectStoreBackend for CountingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        {
            let mut g = self.gets.lock().unwrap();
            let e = g.entry(key.to_string()).or_default();
            match range {
                GetRange::Full => e.full += 1,
                GetRange::Range(..) => e.range += 1,
                GetRange::Suffix(..) => e.suffix += 1,
            }
        }
        self.inner.get(key, range).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.inner.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// Each RLOG input is read by a footer suffix probe plus section/block ranged
/// GETs, and never fetched whole -- so the pre-#275 "fetched whole twice" is
/// gone. Data objects carry the `.rseg` extension (the shared L0 filename shape
/// across signals); commit records (`.cmt`) are still read whole and are not
/// asserted here.
#[tokio::test]
async fn each_rlog_input_read_by_range_never_whole() {
    let store = CountingStore::new();
    // Two compactable L0 `.rlog` inputs (streams {0,1} and {0,2}); seeding is
    // PUT-only, so every GET counted below comes from the compaction itself.
    let bucket = seed_rlog_two_inputs(&store).await;

    let clock = FixedClock::new(sealed_now_ns());
    let outcome = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket)
        .await
        .expect("compact");
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));

    // No input data object is fetched whole (before #275 each was, twice).
    assert_eq!(
        store.full_gets_containing(".rseg"),
        0,
        "no input .rseg object fetched whole"
    );
    // Each of the two inputs is footer-probed exactly once (the suffix GET),
    // then read by ranged section and block GETs.
    assert_eq!(
        store.keys_suffix_probed(".rseg"),
        2,
        "each input footer-probed once"
    );
    assert!(
        store.range_gets_containing(".rseg") > 0,
        "inputs read by ranged section/block GETs"
    );
}

/// Measured request shape over a 100-input bucket, for the record (run with
/// `--nocapture` to see the counts). The load-bearing assertion is the same:
/// zero whole-object GETs of any input `.rseg`, where the pre-#275 merge did
/// exactly `2 * inputs` (one whole fetch in `load_input_catalog`, one in
/// `build_parts`). Every input is instead read by one footer suffix probe plus
/// ranged section and block GETs.
#[tokio::test]
async fn request_counts_for_100_input_bucket() {
    const INPUTS: u64 = 100;
    const STREAMS_PER_INPUT: u32 = 3;

    let store = CountingStore::new();
    for i in 0..INPUTS {
        let records: Vec<_> = (0..STREAMS_PER_INPUT)
            .map(|s| log_record(s, 1_000 + i as i64, "line"))
            .collect();
        seed_rlog_input(
            &store,
            uuid::Uuid::from_u128(u128::from(i) + 1),
            1,
            i + 1,
            &records,
        )
        .await;
    }

    let clock = FixedClock::new(sealed_now_ns());
    let outcome = compact_bucket(&store, &clock, &CompactorConfig::default(), &logs_bucket())
        .await
        .expect("compact");
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));

    let full = store.full_gets_containing(".rseg");
    let suffix = store.suffix_gets_containing(".rseg");
    let range = store.range_gets_containing(".rseg");
    println!(
        "[requests:rlog] inputs={INPUTS} .rseg GETs: full={full} suffix={suffix} range={range} \
         (pre-#275 full would be {})",
        2 * INPUTS
    );

    assert_eq!(
        full, 0,
        "no input .rseg object fetched whole (was 2*inputs)"
    );
    assert_eq!(
        store.keys_suffix_probed(".rseg") as u64,
        INPUTS,
        "each input footer-probed once"
    );
}
