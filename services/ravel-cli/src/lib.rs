//! Library surface behind the `ravel-cli` binary, split out so integration
//! tests can drive the catalog subcommands in-process against a shared
//! `MemoryStore` (a subprocess per invocation, as `tests/segment_inspect.rs`
//! uses, would give each `ravel-cli catalog ...` call its own empty
//! in-memory store, making a chained fold -> inspect -> verify scenario
//! impossible to construct without a persistent S3/MinIO backend).

pub mod catalog;
pub mod gc_config;
pub mod hold;
pub mod idem;
pub mod maintain;
pub mod provision;
pub mod qualify;
pub mod reconstruct;
pub mod store;
pub mod tenancy;

use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ns() -> anyhow::Result<i64> {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH)?;
    i64::try_from(dur.as_nanos()).map_err(|_| anyhow::anyhow!("system clock too far in the future"))
}
