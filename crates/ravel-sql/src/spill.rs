//! Bounded ephemeral spill scratch: the per-query directory, its lifetime, and
//! its accounting (ADR-0954).
//!
//! Spill here is per-query ephemeral execution state. It is never committed, is
//! never read back by any process other than the one that wrote it, and is
//! never a recovery source: object storage stays the only durable backend
//! (ADR-0013). Everything in this module is therefore scoped to one query and
//! removed when that query's session drops, whether it completed, failed, or
//! was cancelled.
//!
//! # Directory layout
//!
//! [`SpillScratch::create`] makes `<configured dir>/ravel-spill-<pid>-<n>` and
//! hands only that subdirectory to DataFusion's disk manager, which in turn
//! creates its own `datafusion-*` temporary directory inside it. Two nested
//! guards then both have to fail for anything to survive the query: DataFusion's
//! `TempDir` drops with the `RuntimeEnv`, and [`SpillScratch`]'s own `Drop`
//! removes the subdirectory whole. The configured directory itself is never
//! created, moved, or removed by this crate; a query that finds it missing or
//! unwritable fails with [`SqlError::SpillUnavailable`] rather than creating
//! one, so a typo in the configuration cannot silently scatter scratch across
//! a node's filesystem.
//!
//! Cleaning up scratch left by a process that died mid-query is explicitly NOT
//! done here (no startup sweep, no node-wide or per-tenant scratch quota):
//! those need an owner outside a single query's lifetime and are follow-up
//! work.
//!
//! # What the figures count
//!
//! [`SpillCounts`] names each figure's unit because two of them are bytes of
//! different kinds and must never be summed or compared:
//!
//! - `bytes_written` is bytes as they sit in the spill files: Arrow IPC, after
//!   whatever spill compression the session configured. Not decoded Arrow
//!   bytes, not wire bytes, not bytes charged to the memory pool.
//! - `bytes_read` would be bytes streamed back from those files. DataFusion
//!   54's `SpillMetrics` carries `spill_count`/`spilled_bytes`/`spilled_rows`
//!   and no read-side counter, and its `SpillManager::read_spill_as_stream`
//!   records nothing, so this crate cannot source it: it is `None`, meaning
//!   "not measured", never `0`, which would claim nothing was read.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use datafusion::physical_plan::ExecutionPlan;

use crate::config::SpillConfig;
use crate::error::SqlError;

/// Distinguishes the scratch directories of queries running concurrently in one
/// process. Paired with the process id so two processes sharing a configured
/// directory cannot collide either.
static SCRATCH_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// One query's scratch subdirectory, removed when this value drops.
///
/// Held by the query's [`PinnedQuery`](crate::PinnedQuery) and moved into its
/// [`PinnedStream`](crate::PinnedStream), declared last there so it drops after
/// the `SessionContext` that owns the files inside it.
#[derive(Debug)]
pub struct SpillScratch {
    dir: PathBuf,
}

impl SpillScratch {
    /// Create this query's scratch subdirectory under `config.dir`.
    ///
    /// The configured directory must already exist and be a writable
    /// directory. All three checks are one `create_dir` on the subdirectory
    /// plus a `metadata` on the parent: a missing parent, a parent that is a
    /// regular file, a read-only parent, and a full volume each surface as
    /// [`SqlError::SpillUnavailable`] here, before any operator has run, rather
    /// than as an opaque IO error from deep inside a spilling operator.
    pub(crate) fn create(config: &SpillConfig) -> Result<SpillScratch, SqlError> {
        let root = config.dir.as_path();
        let metadata = std::fs::metadata(root).map_err(|err| {
            SqlError::SpillUnavailable(format!(
                "configured spill directory {} cannot be read: {err}",
                root.display()
            ))
        })?;
        if !metadata.is_dir() {
            return Err(SqlError::SpillUnavailable(format!(
                "configured spill directory {} is not a directory",
                root.display()
            )));
        }
        let sequence = SCRATCH_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let dir = root.join(format!("ravel-spill-{}-{sequence}", std::process::id()));
        std::fs::create_dir(&dir).map_err(|err| {
            SqlError::SpillUnavailable(format!(
                "spill scratch directory {} could not be created: {err}",
                dir.display()
            ))
        })?;
        Ok(SpillScratch { dir })
    }

    /// The directory handed to DataFusion's disk manager.
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }
}

impl Drop for SpillScratch {
    fn drop(&mut self) {
        // Best effort by necessity: `Drop` cannot fail, and a scratch
        // directory that survives a crashed process is a follow-up's problem
        // (see the module doc). Nothing downstream reads it, so a failure to
        // remove costs disk, never correctness.
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// One query's spill totals, read off the executed plan's own DataFusion
/// counters after the stream drains, the way
/// [`SqlStats`](crate::SqlStats)'s block counters already are.
///
/// Every figure is zero (and `bytes_read` is `None`) for a query that did not
/// spill, which is every query on the default configuration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SpillCounts {
    /// Spill files this query created, summed over every operator that spilled
    /// (DataFusion's `spill_count`).
    pub files: u64,
    /// Bytes written into those files: spill-file bytes on disk (Arrow IPC,
    /// after the session's spill compression). NOT decoded Arrow bytes and NOT
    /// wire bytes; see the module doc.
    pub bytes_written: u64,
    /// Rows written into those files (DataFusion's `spilled_rows`). For a
    /// grouped aggregation these are partial-aggregate state rows, not input
    /// rows.
    pub rows_written: u64,
    /// Bytes streamed back from spill files, or `None` when unmeasured.
    /// Always `None` on DataFusion 54, which exposes no read-side spill
    /// counter (module doc). `None` rather than `0` so a reader cannot mistake
    /// "not measured" for "nothing was read".
    pub bytes_read: Option<u64>,
    /// Wall-clock time this query held at least one spill file open, sampled at
    /// each poll of the query's output stream
    /// ([`PinnedStream`](crate::PinnedStream)). A sampled window, not the
    /// operators' in-spill CPU time: it includes whatever else ran between two
    /// polls that both observed an open spill file, and it is exactly
    /// [`Duration::ZERO`] for a query that never spilled.
    pub duration: Duration,
}

impl SpillCounts {
    /// Whether this query spilled at all.
    pub fn spilled(&self) -> bool {
        self.files > 0 || self.bytes_written > 0 || self.rows_written > 0
    }
}

/// One spilling operator's share of [`SpillCounts`], which is what makes a
/// spill attributable: "the aggregate spilled 4 files" and "the exchange
/// spilled 4 files" are different findings, and a pooled total cannot tell
/// them apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorSpill {
    /// The operator's `ExecutionPlan::name()`, e.g. `AggregateExec`.
    pub operator: String,
    /// Spill files this operator created.
    pub files: u64,
    /// Spill-file bytes on disk this operator wrote (see
    /// [`SpillCounts::bytes_written`]).
    pub bytes_written: u64,
    /// Rows this operator wrote to spill files.
    pub rows_written: u64,
}

/// Sum the spill counters over `plan` and its descendants, and record each
/// operator that contributed a nonzero one.
///
/// Reads the counters DataFusion's spilling operators already maintain rather
/// than counting anything a second time, and reaches every operator by walking
/// `children()`, so a spill under a coalesce or a repartition is attributed to
/// the operator that wrote it however the optimizer nested it.
pub(crate) fn accumulate_spill_counts(
    plan: &Arc<dyn ExecutionPlan>,
    totals: &mut SpillCounts,
    by_operator: &mut Vec<OperatorSpill>,
) {
    if let Some(metrics) = plan.metrics() {
        let sum = |name: &str| metrics.sum_by_name(name).map_or(0, |v| v.as_usize() as u64);
        let files = sum("spill_count");
        let bytes_written = sum("spilled_bytes");
        let rows_written = sum("spilled_rows");
        if files > 0 || bytes_written > 0 || rows_written > 0 {
            totals.files += files;
            totals.bytes_written += bytes_written;
            totals.rows_written += rows_written;
            by_operator.push(OperatorSpill {
                operator: plan.name().to_string(),
                files,
                bytes_written,
                rows_written,
            });
        }
    }
    for child in plan.children() {
        accumulate_spill_counts(child, totals, by_operator);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// The guard removes its own subdirectory and nothing else: the configured
    /// directory, and anything else already in it, survive.
    #[test]
    fn the_scratch_guard_removes_only_its_own_subdirectory() {
        let root = tempfile::tempdir().expect("temp root");
        let sibling = root.path().join("not-ours");
        std::fs::create_dir(&sibling).expect("sibling dir");

        let config = SpillConfig {
            dir: root.path().to_path_buf(),
            max_bytes: 1 << 20,
        };
        let scratch = SpillScratch::create(&config).expect("scratch created");
        let dir = scratch.dir().to_path_buf();
        std::fs::write(dir.join("spill-0"), b"payload").expect("write into scratch");
        assert!(dir.is_dir());

        drop(scratch);
        assert!(!dir.exists(), "the scratch subdirectory must be removed");
        assert!(sibling.is_dir(), "an unrelated sibling must survive");
        assert!(root.path().is_dir(), "the configured root must survive");
    }

    /// Two concurrent queries under one configured directory get distinct
    /// scratch directories, so one query's cleanup cannot delete another's
    /// in-flight spill files.
    #[test]
    fn concurrent_queries_get_distinct_scratch_directories() {
        let root = tempfile::tempdir().expect("temp root");
        let config = SpillConfig {
            dir: root.path().to_path_buf(),
            max_bytes: 1 << 20,
        };
        let first = SpillScratch::create(&config).expect("first scratch");
        let second = SpillScratch::create(&config).expect("second scratch");
        assert_ne!(first.dir(), second.dir());
        assert!(first.dir().is_dir() && second.dir().is_dir());
    }

    /// A missing configured directory is refused, not created. The check runs
    /// before any operator, so the query fails with nothing written anywhere.
    #[test]
    fn a_missing_configured_directory_is_refused_and_not_created() {
        let root = tempfile::tempdir().expect("temp root");
        let missing = root.path().join("absent");
        let config = SpillConfig {
            dir: missing.clone(),
            max_bytes: 1 << 20,
        };
        let err = SpillScratch::create(&config).expect_err("a missing directory must be refused");
        assert!(matches!(err, SqlError::SpillUnavailable(_)));
        assert!(
            !missing.exists(),
            "the configured directory must never be created by this crate"
        );
    }

    /// A configured path that is a regular file, not a directory, is refused.
    #[test]
    fn a_configured_path_that_is_a_file_is_refused() {
        let root = tempfile::tempdir().expect("temp root");
        let file = root.path().join("a-file");
        std::fs::write(&file, b"not a directory").expect("write file");
        let config = SpillConfig {
            dir: file,
            max_bytes: 1 << 20,
        };
        let err = SpillScratch::create(&config).expect_err("a file must be refused");
        assert!(matches!(err, SqlError::SpillUnavailable(_)));
    }

    #[test]
    fn zeroed_counts_report_no_spill_and_no_measured_read() {
        let counts = SpillCounts::default();
        assert!(!counts.spilled());
        assert_eq!(counts.duration, Duration::ZERO);
        assert_eq!(
            counts.bytes_read, None,
            "unmeasured must stay distinguishable from zero"
        );
    }
}
