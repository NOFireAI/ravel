//! The bulk-scan size trigger end to end through `SqlExecutor::execute`: the
//! bytes a statement read on its last successful execution, not the bytes its
//! lane resolves to, decide whether its next execution reads as BulkScan.

#![allow(clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::FaultStore;
use ravel_object_store::instrument::InstrumentedStore;
use ravel_object_store::memory::MemoryStore;
use ravel_types::accounting::{AccessIntent, AccountedOp};
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};

use crate::declared::{DeclaredColumn, DeclaredType, KeyIndexDeclaration, StaticDeclaredColumns};
use crate::executor::SqlExecutor;
use crate::executor::tests::{
    executor_over, log_record, publish_log_records, sql_request, wrap_fixture_store,
};

const WINDOW: TimeRange = TimeRange {
    start_ns: 0,
    end_ns: 2_000_000,
};

/// The threshold every test here runs with: below the lane's 1,368 resolved
/// bytes, above [`EARLY_PRUNED`]'s 1,241 read bytes, below [`WHOLE_LANE`]'s
/// 1,914.
const MIN_BYTES: u64 = 1_300;

/// Filters on the late object's time range, so the scan skips the early
/// object: it reads the catalog (546 bytes) and the late object (695).
const EARLY_PRUNED: &str = "SELECT body FROM logs \
     WHERE ts >= TIMESTAMP '1970-01-01 00:00:00.001' AND severity_text = 'ERROR'";

/// Filters on a field no object's statistics prune, so the scan reads both
/// objects (1,368 bytes) after the catalog (546).
const WHOLE_LANE: &str = "SELECT ts, body FROM logs WHERE severity_text = 'ERROR'";

type FixtureStore = Arc<InstrumentedStore<FaultStore<MemoryStore>>>;

/// Two 200-record RLOG objects on one tenant's `logs` lane, the first at
/// `ts` 0..19,900 ns and the second at 1,000,000..1,019,900 ns, and the sum
/// of their object sizes, which is what the lane's resolve returns.
async fn two_object_lane() -> (FixtureStore, TenantHash, u64) {
    let tenant_hash = TenantId::new("size-trigger".to_string()).hash();
    let memory = MemoryStore::new();
    let early: Vec<_> = (0..200).map(|i| log_record("api", i * 100)).collect();
    let late: Vec<_> = (0..200)
        .map(|i| log_record("api", 1_000_000 + i * 100))
        .collect();
    let early_key = publish_log_records(&memory, tenant_hash, Signal::Logs, 1, &early).await;
    let late_key = publish_log_records(&memory, tenant_hash, Signal::Logs, 2, &late).await;
    let store = wrap_fixture_store(memory);
    let mut resolved = 0;
    for key in [early_key, late_key] {
        resolved += store.head(&key).await.expect("head").size;
    }
    (store, tenant_hash, resolved)
}

/// An executor over `store` with the size trigger at [`MIN_BYTES`] and
/// `user_id` declared as a KEY_IDX field.
fn trigger_executor(store: FixtureStore) -> SqlExecutor {
    let declared =
        StaticDeclaredColumns::new(vec![DeclaredColumn::new("user_id", DeclaredType::Str)])
            .with_key_index(KeyIndexDeclaration {
                columns: vec!["user_id".to_string()],
                trace_id: false,
            });
    executor_over(store)
        .with_declared_column_source(Arc::new(declared))
        .with_cache_bulk_scan_min_bytes(Some(MIN_BYTES))
}

/// Run `sql` once and return the intent it read under and the bytes it read:
/// cache-served plus store GET bytes, every phase.
async fn run(exec: &SqlExecutor, tenant_hash: TenantHash, sql: &str) -> (AccessIntent, u64) {
    let outcome = exec
        .execute(tenant_hash, &sql_request(sql, WINDOW))
        .await
        .expect("the statement runs");
    let accounting = outcome.accounting;
    (
        accounting.access_intent,
        accounting.cache_bytes + accounting.s3_bytes(AccountedOp::Get),
    )
}

/// A filtered `logs` statement over a lane whose resolve (1,368 bytes) is at
/// or above the threshold (1,300), but which itself reads 1,241 bytes, stays
/// Interactive on its first execution and on its second.
///
/// FLIP: comparing the lane's resolved object bytes to the threshold, as the
/// trigger did before, reads both executions as BulkScan.
#[tokio::test]
async fn a_statement_that_reads_less_than_the_threshold_stays_interactive_over_a_larger_lane() {
    let (store, tenant_hash, resolved) = two_object_lane().await;
    assert_eq!(resolved, 1_368, "the lane's resolved object bytes");
    let exec = trigger_executor(store);

    let first = run(&exec, tenant_hash, EARLY_PRUNED).await;
    let second = run(&exec, tenant_hash, EARLY_PRUNED).await;

    assert_eq!(first, (AccessIntent::Interactive, 1_241), "first execution");
    assert_eq!(
        second,
        (AccessIntent::Interactive, 1_241),
        "second execution"
    );
}

/// A filtered `logs` statement that reads 1,914 bytes against a 1,300-byte
/// threshold is Interactive on its first execution, having no record yet, and
/// BulkScan on its second. A different statement text over the same lane has
/// no record of its own and is Interactive. An equality on a KEY_IDX field
/// stays Interactive however many bytes it read.
///
/// FLIP: comparing the lane's resolved object bytes to the threshold reads
/// every first execution as BulkScan; dropping the KEY_IDX check from the
/// trigger reads the key lookup's second execution as BulkScan.
#[tokio::test]
async fn a_statement_that_reads_the_threshold_is_bulk_from_its_second_execution() {
    let (store, tenant_hash, _) = two_object_lane().await;
    let exec = trigger_executor(store);

    let whole_lane = [
        run(&exec, tenant_hash, WHOLE_LANE).await,
        run(&exec, tenant_hash, WHOLE_LANE).await,
        run(&exec, tenant_hash, WHOLE_LANE).await,
    ];
    assert_eq!(
        whole_lane,
        [
            (AccessIntent::Interactive, 1_914),
            (AccessIntent::BulkScan, 1_914),
            (AccessIntent::BulkScan, 1_914),
        ],
        "the same statement text, three executions"
    );

    let other_text = "SELECT count(*) FROM logs WHERE severity_text = 'ERROR'";
    assert_eq!(
        run(&exec, tenant_hash, other_text).await,
        (AccessIntent::Interactive, 1_914),
        "a different statement text over the same lane"
    );

    let key_lookup = "SELECT body FROM logs WHERE user_id = 'u-1'";
    let key_runs = [
        run(&exec, tenant_hash, key_lookup).await,
        run(&exec, tenant_hash, key_lookup).await,
        run(&exec, tenant_hash, key_lookup).await,
    ];
    assert_eq!(
        key_runs,
        [(AccessIntent::Interactive, 1_914); 3],
        "a KEY_IDX equality, three executions"
    );
}
