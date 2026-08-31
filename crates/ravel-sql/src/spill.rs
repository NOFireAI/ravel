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
//! [`SpillScratch::create`] makes
//! `<configured dir>/ravel-spill-<pid>-<nonce>-<n>` and
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

use std::hash::{BuildHasher, RandomState};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use datafusion::physical_plan::ExecutionPlan;

use crate::config::SpillConfig;
use crate::error::SqlError;

/// Distinguishes the scratch directories of queries running concurrently in one
/// process. Paired with the process id so two processes sharing a configured
/// directory cannot collide either.
static SCRATCH_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Per-process random component of a scratch directory name.
///
/// The process id and the sequence counter are both reconstructible: a process
/// that crashes leaves its scratch behind by design (module doc), and after the
/// OS reuses its process id the next process starts its own sequence at 0 and
/// would rebuild the identical name. `create_dir` then fails `AlreadyExists`
/// and a perfectly usable spill root refuses the first eligible query. This
/// makes the name unpredictable instead, so a stale directory cannot be named
/// by a later process at all.
///
/// Not a liveness probe on purpose: ADR-0954 rejects process liveness as proof
/// of scratch ownership (a reused pid is live and owns nothing of the dead
/// process's), so the fix is a name that does not collide, never a decision
/// about whether the stale directory is abandoned.
///
/// `RandomState`'s keys are seeded from the OS once per process, which is
/// exactly the property needed here and needs no dependency and no clock (this
/// crate's library logic takes no `SystemTime::now`).
static SCRATCH_NONCE: OnceLock<u64> = OnceLock::new();

/// Attempts [`SpillScratch::create`] makes before it gives up on a colliding
/// name. Each attempt draws a fresh sequence number, so reaching this bound
/// means every one of them collided: not a name clash any more, but a scratch
/// root that cannot be written, which is a [`SqlError::SpillUnavailable`].
const SCRATCH_NAME_ATTEMPTS: u32 = 8;

/// This process's scratch nonce, computed once.
fn scratch_nonce() -> u64 {
    *SCRATCH_NONCE.get_or_init(|| RandomState::new().hash_one(std::process::id()))
}

/// The name of the next scratch subdirectory: process id, this process's
/// nonce, and a fresh in-process sequence number, so two queries in one process
/// never collide and no other process can reconstruct the name.
fn next_scratch_name() -> String {
    let sequence = SCRATCH_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!(
        "ravel-spill-{}-{:016x}-{sequence}",
        std::process::id(),
        scratch_nonce()
    )
}

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
    ///
    /// A name that is already taken is the one IO failure that is not a
    /// refusal: it is retried with a fresh name (see [`next_scratch_name`] and
    /// [`SpillScratch::create_named`]), because scratch left behind by a
    /// crashed process must not make a usable spill root refuse the next
    /// query.
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
        Self::create_named(root, next_scratch_name)
    }

    /// Create the first of up to [`SCRATCH_NAME_ATTEMPTS`] names `name` yields
    /// that is not already taken under `root`.
    ///
    /// Only `AlreadyExists` is retried. Every other error -- a read-only
    /// parent, a parent removed between the check and here, a full volume --
    /// surfaces as [`SqlError::SpillUnavailable`] on the spot, with no further
    /// attempt: those say the scratch root is unusable, and retrying a
    /// different name under it would only repeat the same failure.
    ///
    /// Takes the name generator as an argument so a test can hand it a
    /// deliberately colliding name; production passes
    /// [`next_scratch_name`].
    fn create_named(
        root: &Path,
        mut name: impl FnMut() -> String,
    ) -> Result<SpillScratch, SqlError> {
        let mut collided = Vec::new();
        for _ in 0..SCRATCH_NAME_ATTEMPTS {
            let dir = root.join(name());
            match std::fs::create_dir(&dir) {
                Ok(()) => return Ok(SpillScratch { dir }),
                Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                    collided.push(dir);
                }
                Err(err) => {
                    return Err(SqlError::SpillUnavailable(format!(
                        "spill scratch directory {} could not be created: {err}",
                        dir.display()
                    )));
                }
            }
        }
        Err(SqlError::SpillUnavailable(format!(
            "no spill scratch directory could be created under {}: \
             all {SCRATCH_NAME_ATTEMPTS} candidate names already exist ({})",
            root.display(),
            collided
                .iter()
                .map(|dir| dir.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )))
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
        // Spill figures are typed `MetricValue::SpillCount`/`SpilledBytes`/
        // `SpilledRows` variants, not named `Count`s. DataFusion 54.1.0's
        // `MetricsSet::sum_by_name` matches only named metrics and returns
        // `false` for every spill variant, so reading them by name always
        // yields zero even for a query that spilled; the typed accessors are
        // the only way to read them.
        let files = metrics.spill_count().unwrap_or(0) as u64;
        let bytes_written = metrics.spilled_bytes().unwrap_or(0) as u64;
        let rows_written = metrics.spilled_rows().unwrap_or(0) as u64;
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

    /// A stale scratch directory whose name collides with the one this query
    /// would pick does not fail the query: the next name is tried, and the
    /// stale directory is left exactly where it is (nothing here decides
    /// whether it is abandoned, which is what ADR-0954 rejects).
    #[test]
    fn a_colliding_stale_directory_is_retried_not_refused() {
        let root = tempfile::tempdir().expect("temp root");
        let stale = root.path().join("ravel-spill-stale");
        std::fs::create_dir(&stale).expect("stale dir");
        std::fs::write(stale.join("left-behind"), b"from a crashed process")
            .expect("stale contents");

        let mut names = ["ravel-spill-stale", "ravel-spill-fresh"].into_iter();
        let scratch = SpillScratch::create_named(root.path(), || {
            names.next().expect("two names offered").to_string()
        })
        .expect("a taken name must be retried, not refused");

        assert_eq!(
            scratch.dir(),
            root.path().join("ravel-spill-fresh"),
            "create must move on to the next candidate name"
        );
        assert!(
            stale.join("left-behind").is_file(),
            "the stale directory and its contents must be left untouched"
        );
    }

    /// Exhausting every candidate name is a `SpillUnavailable`, not a retry
    /// loop: at that point the root is unusable, which the query must be told.
    #[test]
    fn every_candidate_name_taken_is_spill_unavailable() {
        let root = tempfile::tempdir().expect("temp root");
        std::fs::create_dir(root.path().join("ravel-spill-taken")).expect("taken dir");

        let mut attempts = 0u32;
        let err = SpillScratch::create_named(root.path(), || {
            attempts += 1;
            "ravel-spill-taken".to_string()
        })
        .expect_err("every name taken must be refused");

        assert!(matches!(err, SqlError::SpillUnavailable(_)), "got {err:?}");
        assert_eq!(
            attempts, SCRATCH_NAME_ATTEMPTS,
            "create must try exactly {SCRATCH_NAME_ATTEMPTS} names before refusing"
        );
    }

    /// A name a dead process left behind cannot be reconstructed by a later
    /// process, whatever process id the OS hands it: the nonce is per-process
    /// and the pid-and-sequence part of the name is not sufficient to build it.
    ///
    /// This is the property that keeps the retry above from being the only
    /// defense. The pre-fix name was `ravel-spill-<pid>-<sequence>` with the
    /// sequence starting at 0 in every process, so the name below is exactly
    /// what a crashed process with this pid would have left, and exactly what
    /// the first eligible query of the reusing process would have asked for.
    #[test]
    fn a_scratch_name_cannot_be_reconstructed_from_the_process_id_alone() {
        let root = tempfile::tempdir().expect("temp root");
        let reused_pid_name = format!("ravel-spill-{}-0", std::process::id());
        std::fs::create_dir(root.path().join(&reused_pid_name)).expect("stale dir");

        let config = SpillConfig {
            dir: root.path().to_path_buf(),
            max_bytes: 1 << 20,
        };
        let scratch = SpillScratch::create(&config).expect("scratch created");
        let name = scratch
            .dir()
            .file_name()
            .and_then(|name| name.to_str())
            .expect("a utf-8 scratch name")
            .to_string();

        assert_ne!(
            name, reused_pid_name,
            "the name must carry more than the process id and the sequence"
        );
        let nonce = name
            .strip_prefix(&format!("ravel-spill-{}-", std::process::id()))
            .and_then(|rest| rest.split('-').next())
            .expect("the name is ravel-spill-<pid>-<nonce>-<sequence>")
            .to_string();
        assert_eq!(
            nonce.len(),
            16,
            "the nonce is 16 hex digits of per-process randomness; got {nonce:?}"
        );
        assert!(
            nonce.chars().all(|c| c.is_ascii_hexdigit()),
            "the nonce is hex; got {nonce:?}"
        );
        assert_eq!(
            nonce,
            format!("{:016x}", scratch_nonce()),
            "one nonce per process, stable across queries"
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
