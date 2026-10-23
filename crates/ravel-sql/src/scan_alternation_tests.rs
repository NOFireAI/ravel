//! A repeated `logs` scan over more data than the read cache holds, end to end
//! through `SqlExecutor::execute` (issue #3192): consecutive executions of the
//! same scan publish their segments in opposite directions, so each reads
//! first what the previous one read last, while it is still resident.

#![allow(clippy::expect_used)]

use std::sync::Arc;

use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_object_store::fault::{FaultPlan, FaultStore};
use ravel_object_store::instrument::InstrumentedStore;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_query::{CacheFetchError, LogSegmentFetcher, SegmentFetcher, SpanSegmentFetcher};
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};

use crate::config::SqlConfig;
use crate::executor::SqlExecutor;
use crate::executor::tests::{log_record, publish_log_records, sql_request};

const OBJECTS: u64 = 8;
const RECORDS: i64 = 200;
const OBJECT_SPAN_NS: i64 = 1_000_000;

const WINDOW: TimeRange = TimeRange {
    start_ns: 0,
    end_ns: OBJECT_SPAN_NS * OBJECTS as i64,
};

/// A pushed content predicate, so the scan takes the planned route rather
/// than the predicate-free fast path, that every record's `body` matches, so
/// every execution reads all eight objects.
const STATEMENT: &str = "SELECT ts, body FROM logs WHERE has_word(body, 'query')";

/// Eight 200-record RLOG objects on one tenant's `logs` lane, object `i` at
/// `ts` `i * 1,000,000` onward, and their sizes.
async fn eight_object_lane() -> (Arc<dyn ObjectStoreBackend>, TenantHash, Vec<u64>) {
    let tenant_hash = TenantId::new("scan-alternation".to_string()).hash();
    let memory = MemoryStore::new();
    let mut sizes = Vec::new();
    for i in 0..OBJECTS {
        let base = i as i64 * OBJECT_SPAN_NS;
        let records: Vec<_> = (0..RECORDS)
            .map(|r| log_record("api", base + r * 100))
            .collect();
        let key = publish_log_records(&memory, tenant_hash, Signal::Logs, i + 1, &records).await;
        let bytes = memory
            .get(&key, GetRange::Full)
            .await
            .expect("get the data object")
            .data;
        sizes.push(bytes.len() as u64);
    }
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(InstrumentedStore::new(FaultStore::new(
        memory,
        FaultPlan::empty(),
    )));
    (store, tenant_hash, sizes)
}

/// An executor over `store` whose logs fetcher reads through `cache`, at one
/// SQL partition so each execution reads its objects in one sequence.
fn executor(store: Arc<dyn ObjectStoreBackend>, cache: Arc<Cache<CacheFetchError>>) -> SqlExecutor {
    let catalog = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    let config = SqlConfig {
        engine: ravel_query::EngineConfig {
            sql_partition_count: Some(1),
            ..ravel_query::EngineConfig::default()
        },
        ..SqlConfig::default()
    };
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()).with_cache(cache),
        SpanSegmentFetcher::new(store),
        config,
        1 << 30,
    )
}

/// Four executions of one filtered statement over eight objects through a
/// read cache that holds four and a half of them. The cache's entry cap is
/// its whole size, so its ghost window is one key, as it is under a 64 MiB
/// entry cap over objects far smaller than that: an object evicted from
/// probation is forgotten before the loop comes back to it.
///
/// The first execution reads every object cold, in snapshot order, and
/// leaves the last four resident. The second runs in reverse, so it reads
/// those four first and is served all of them (2792 bytes). The third and
/// fourth alternate again and are each served three 698-byte objects (2094
/// bytes). One partition reads the objects one after another, so the figures
/// are exact. Only the read cache's own served bytes are counted; the
/// catalog's reads go through no read cache here.
///
/// FLIP: publishing every execution in the same visit order (the code
/// before issue #3192) reads each object after the one four ahead of it
/// evicted it from probation, so every run serves 0 bytes from the read
/// cache.
#[tokio::test]
async fn a_repeated_scan_larger_than_the_cache_is_served_from_it_on_warm_runs() {
    let (store, tenant_hash, sizes) = eight_object_lane().await;
    let largest = sizes.iter().copied().max().expect("objects");
    let capacity = largest * 4 + largest / 2;
    let cache: Arc<Cache<CacheFetchError>> =
        Arc::new(Cache::new(CacheLimits::new(capacity, 4096, capacity)));
    let exec = executor(store, Arc::clone(&cache));

    let mut served = Vec::new();
    for _ in 0..4 {
        let before = cache.metrics().snapshot().bytes_served;
        let output = exec
            .execute(tenant_hash, &sql_request(STATEMENT, WINDOW))
            .await
            .expect("the statement runs");
        assert_eq!(
            output.output.num_rows(),
            (RECORDS * OBJECTS as i64) as usize,
            "every record matches"
        );
        served.push(cache.metrics().snapshot().bytes_served - before);
    }

    assert_eq!(
        sizes,
        vec![673, 695, 696, 698, 698, 698, 698, 698],
        "the fixture's object sizes"
    );
    assert!(
        served[2] > 0 && served[3] > 0,
        "warm runs 3 and 4 are served from the read cache: {served:?}"
    );
    assert_eq!(
        served,
        vec![0, 2792, 2094, 2094],
        "read-cache bytes served per execution"
    );
}
