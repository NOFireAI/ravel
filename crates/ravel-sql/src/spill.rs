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
//! On Unix that subdirectory is created `0o700` explicitly rather than at the
//! ambient umask, so a default `0o022` cannot leave one query's spilled rows
//! readable by every local user. It is the only permission control this crate
//! has over spill: the files beneath it are created by DataFusion's disk
//! manager. The configured directory's own mode is the operator's.
//!
//! Cleaning up scratch left by a process that died mid-query needs an owner
//! outside a single query's lifetime: [`SpillRootOwner`] is that owner. A
//! `--cache-dir`-configured process acquires one root,
//! `<cache_dir>/sql-spill/<instance_id>`, for its whole lifetime, with an
//! exclusive `flock` on a lock file inside it as its live-ownership proof
//! (ADR-0954 requirement 7 explicitly rejects a pid for this: a reused pid is
//! live and owns nothing of the dead process's directory, while a `flock` is
//! released by the kernel the instant the owning process's last handle to it
//! closes, however that happens). At startup, after acquiring its own root, a
//! process calls [`SpillRootOwner::sweep_orphaned_spill_roots`] to remove
//! every sibling root whose lock it can take itself; a sibling whose
//! ownership cannot be settled either way is left in place and logged, never
//! guessed at. There is still no node-wide or per-tenant scratch quota;
//! that remains follow-up work.
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

/// Creates one query's scratch subdirectory owner-only rather than at the
/// ambient umask.
///
/// Non-recursive, exactly like `create_dir`, so a name already taken still
/// surfaces as `AlreadyExists` and [`SpillScratch::create_named`]'s retry is
/// unaffected. This directory's mode is the whole of the permission control
/// available here: the spill files inside it are created by DataFusion's disk
/// manager, not by this crate.
#[cfg(unix)]
fn create_scratch_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new().mode(0o700).create(dir)
}

#[cfg(not(unix))]
fn create_scratch_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir(dir)
}

/// The smallest share of the process spill ceiling worth granting one query
/// (ADR-0954 requirement 2, issue #2416), or the whole ceiling when that is
/// smaller. A query that cannot reserve this much runs with spill disabled.
pub const MIN_SPILL_RESERVATION_BYTES: u64 = 64 * 1024 * 1024;

/// The process-wide spill ceiling and the bytes of it live queries hold
/// (ADR-0954 requirement 2, issue #2416). Each query granted spill reserves
/// its scratch cap here before its disk manager is built, and releases it when
/// its scratch directory is removed, so the caps of concurrent queries never
/// sum past the ceiling.
#[derive(Debug)]
pub(crate) struct SpillBudget {
    ceiling: u64,
    reserved: Arc<AtomicU64>,
}

impl SpillBudget {
    pub(crate) fn new(ceiling: u64) -> Self {
        SpillBudget {
            ceiling,
            reserved: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Bytes currently reserved by live queries.
    #[cfg(test)]
    fn reserved(&self) -> u64 {
        self.reserved.load(Ordering::Acquire)
    }

    /// Reserve `min(want, what remains of the ceiling)` in one atomic step, or
    /// `None` when that is below [`MIN_SPILL_RESERVATION_BYTES`] (or below the
    /// ceiling, when the ceiling itself is smaller).
    pub(crate) fn reserve(&self, want: u64) -> Option<SpillReservation> {
        let floor = MIN_SPILL_RESERVATION_BYTES.min(self.ceiling).max(1);
        let mut current = self.reserved.load(Ordering::Acquire);
        loop {
            let grant = want.min(self.ceiling.saturating_sub(current));
            if grant < floor {
                return None;
            }
            match self.reserved.compare_exchange_weak(
                current,
                current + grant,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(SpillReservation {
                        reserved: Arc::clone(&self.reserved),
                        bytes: grant,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }
}

/// One query's share of a [`SpillBudget`], returned to it on drop.
#[derive(Debug)]
pub(crate) struct SpillReservation {
    reserved: Arc<AtomicU64>,
    bytes: u64,
}

impl SpillReservation {
    /// The scratch cap this reservation grants its query.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for SpillReservation {
    fn drop(&mut self) {
        self.reserved.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// One query's scratch subdirectory, removed when this value drops.
///
/// Held by the query's [`PinnedQuery`](crate::PinnedQuery) and moved into its
/// [`PinnedStream`](crate::PinnedStream), declared last there so it drops after
/// the `SessionContext` that owns the files inside it. It also holds the
/// query's [`SpillReservation`], which is released after the directory is
/// removed.
#[derive(Debug)]
pub struct SpillScratch {
    dir: PathBuf,
    _reservation: Option<SpillReservation>,
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
            match create_scratch_dir(&dir) {
                Ok(()) => {
                    return Ok(SpillScratch {
                        dir,
                        _reservation: None,
                    });
                }
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

    /// Hold `reservation` until this scratch directory is removed.
    pub(crate) fn holding(mut self, reservation: SpillReservation) -> Self {
        self._reservation = Some(reservation);
        self
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

/// Name of the owner lock file inside each process's `--cache-dir` spill root
/// (ADR-0954 requirement 7, issue #2416). Its exclusive `flock` is that
/// process's live-ownership proof: ADR-0954 explicitly rejects a pid, in the
/// directory name or anywhere else, as ownership proof, because a reused pid
/// is live and owns nothing of the dead process's directory. A `flock` is
/// instead released by the kernel the instant the owning process's last
/// handle to it closes, however that happens -- clean exit, crash, `SIGKILL`
/// -- so another process can tell "still owned" from "abandoned" by trying to
/// take the lock itself, never by reading the directory's name or checking
/// pid liveness.
const OWNER_LOCK_FILE_NAME: &str = ".owner.lock";

/// Name prefix of an orphaned spill root a sweep has moved aside for
/// deletion. A sweep creates such a name only while it holds the orphan's
/// lock, so the tree under it belongs to no live process: a later sweep
/// deletes it without a lock check.
const SWEPT_NAME_PREFIX: &str = ".swept-";

/// `remove_dir_all`, where a tree that is already gone counts as removed: two
/// sweeps may delete the same moved-aside tree at once.
fn remove_tree(dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_dir_all(dir) {
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

/// Delete a tree an earlier sweep moved aside under [`SWEPT_NAME_PREFIX`] but
/// did not finish removing. One that is already gone (this sweep's own, or
/// a concurrent sweep's, listed after its removal) is not logged.
fn remove_swept(dir: &Path) {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {
            tracing::info!(
                dir = %dir.display(),
                "removed a SQL spill root an earlier sweep moved aside"
            );
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            tracing::warn!(
                dir = %dir.display(),
                error = %err,
                "could not remove a SQL spill root an earlier sweep moved aside"
            );
        }
    }
}

/// How long [`lock_in_place`] keeps retrying a lock that is held while the
/// lock file is still in place: a sweep holds it only from its lock to its
/// rename, so contention that outlasts this is a live owner.
const OWNER_LOCK_WAIT_ROUNDS: u32 = 20;
const OWNER_LOCK_WAIT: Duration = Duration::from_millis(10);

/// Whether `path` still names the file `file` was opened from: same device,
/// same inode. A root a sweep moved away, or a lock file created again in its
/// place, no longer matches.
#[cfg(unix)]
fn still_names(file: &std::fs::File, path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (file.metadata(), std::fs::metadata(path)) {
        (Ok(held), Ok(current)) => held.dev() == current.dev() && held.ino() == current.ino(),
        _ => false,
    }
}

#[cfg(not(unix))]
fn still_names(_file: &std::fs::File, path: &Path) -> bool {
    path.exists()
}

/// Take the exclusive lock on `lock_file`, opened from `lock_path` in the
/// root `dir`. `Ok(true)`: locked, and the path still names the locked file.
/// `Ok(false)`: a sweep moved the root away, so the lock (if taken) is on a
/// root nobody can reach; the caller builds the root again. An error of kind
/// `WouldBlock`: a live process owns the root.
fn lock_in_place(lock_file: &std::fs::File, lock_path: &Path, dir: &Path) -> std::io::Result<bool> {
    for _ in 0..OWNER_LOCK_WAIT_ROUNDS {
        match lock_file.try_lock() {
            Ok(()) => return Ok(still_names(lock_file, lock_path)),
            Err(std::fs::TryLockError::WouldBlock) => {
                if !still_names(lock_file, lock_path) {
                    return Ok(false);
                }
                std::thread::sleep(OWNER_LOCK_WAIT);
            }
            Err(std::fs::TryLockError::Error(err)) => return Err(err),
        }
    }
    Err(std::io::Error::new(
        ErrorKind::WouldBlock,
        format!(
            "SQL spill root {} is locked by another live process: two processes share \
             one instance id under this cache directory",
            dir.display()
        ),
    ))
}

/// This process's exclusive hold on its `--cache-dir` spill root
/// (ADR-0954 requirement 7, issue #2416):
/// `<cache_dir>/sql-spill/<instance_id>`, created owner-only, with an
/// `.owner.lock` file this value holds an exclusive `flock` on for as long as
/// it is alive. Held for the process's lifetime (the caller keeps it, not
/// this module); dropping it closes the lock file, which releases the lock
/// immediately, including on a crash -- the kernel does this regardless of
/// how the process exits.
pub struct SpillRootOwner {
    dir: PathBuf,
    _lock: std::fs::File,
}

impl SpillRootOwner {
    /// Create (idempotently) and take ownership of
    /// `<cache_dir>/sql-spill/<instance_id>`. `instance_id` is whatever
    /// identity the process already carries for its own artifacts; this does
    /// not mint one.
    ///
    /// Fails only on an I/O error: the directory could not be created (or
    /// found existing), or its lock file could not be opened or locked. An
    /// error of kind `WouldBlock` means another live process holds this exact
    /// `(cache_dir, instance_id)` pair -- an instance identity collision, not
    /// the expected startup path.
    ///
    /// A peer's startup sweep can move this root away between the open and
    /// the lock (ADR-0954 requirement 7, issue #2416). The lock is therefore
    /// kept only once the lock file's path still names the file locked; when
    /// it does not, the root is created again and locked once more, and a
    /// second such loss is an error.
    pub fn acquire(cache_dir: &Path, instance_id: &str) -> std::io::Result<SpillRootOwner> {
        Self::acquire_with(cache_dir, instance_id, &mut || {})
    }

    /// [`Self::acquire`], running `before_lock` between opening the lock file
    /// and locking it, which is where a test puts a peer's sweep.
    fn acquire_with(
        cache_dir: &Path,
        instance_id: &str,
        before_lock: &mut dyn FnMut(),
    ) -> std::io::Result<SpillRootOwner> {
        let sql_spill_root = cache_dir.join(crate::config::SQL_SPILL_SUBDIR);
        let dir = crate::config::cache_spill_dir(cache_dir, instance_id);
        let lock_path = dir.join(OWNER_LOCK_FILE_NAME);
        for _ in 0..2 {
            std::fs::create_dir_all(&sql_spill_root)?;
            match create_scratch_dir(&dir) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::AlreadyExists => {}
                Err(err) => return Err(err),
            }
            let lock_file = match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path)
            {
                Ok(file) => file,
                // The root was moved away between its creation and the open.
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            };
            before_lock();
            if lock_in_place(&lock_file, &lock_path, &dir)? {
                return Ok(SpillRootOwner {
                    dir,
                    _lock: lock_file,
                });
            }
        }
        Err(std::io::Error::other(format!(
            "SQL spill root {} was moved away by another process's startup sweep twice \
             while this process was taking its lock",
            dir.display()
        )))
    }

    /// This process's own spill root, `<cache_dir>/sql-spill/<instance_id>`.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Remove every sibling spill root under `<cache_dir>/sql-spill` whose
    /// owner is provably gone (ADR-0954 requirement 7, issue #2416): a
    /// process other than this one that cannot be, because its lock file can
    /// be exclusively locked here and now. Never removes `self.dir()`, never
    /// looks above `<cache_dir>/sql-spill`, and never removes a sibling whose
    /// ownership this call cannot settle one way or the other -- an
    /// unreadable directory, a missing lock file, or any lock error other
    /// than contention is left in place and logged at WARN, not guessed at. A
    /// sibling whose lock a live process holds is left in place without a log
    /// line. A tree an earlier sweep moved aside under a `.swept-` name is
    /// deleted without a lock check, and an entry gone by the time it is
    /// reached is skipped without a log line.
    ///
    /// Call once at process startup, after [`SpillRootOwner::acquire`].
    pub fn sweep_orphaned_spill_roots(&self) {
        let Some(sql_spill_root) = self.dir.parent() else {
            return;
        };
        let entries = match std::fs::read_dir(sql_spill_root) {
            Ok(entries) => entries,
            Err(err) => {
                tracing::warn!(
                    dir = %sql_spill_root.display(),
                    error = %err,
                    "could not list SQL spill roots to sweep"
                );
                return;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "could not read a SQL spill root directory entry"
                    );
                    continue;
                }
            };
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_dir() {
                continue;
            }
            let candidate = sql_spill_root.join(entry.file_name());
            if candidate == self.dir {
                continue;
            }
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with(SWEPT_NAME_PREFIX)
            {
                remove_swept(&candidate);
                continue;
            }
            self.sweep_one(&candidate);
        }
    }

    /// Settle, and act on, one sibling spill root's ownership.
    fn sweep_one(&self, candidate: &Path) {
        self.sweep_one_with(candidate, &mut || {}, &mut || {});
    }

    /// [`Self::sweep_one`], running `after_lock` once the orphan's lock is
    /// held and before the root is moved away, and `after_release` once the
    /// lock is released and before the moved tree is deleted: the two points
    /// where a test puts a peer's `acquire` of the same root.
    ///
    /// Ownership is settled and the root removed under one lock hold: the
    /// root is renamed aside while the lock is held, so its path is free (and
    /// any `acquire` that opened the old lock file sees it moved) before the
    /// lock is released, and the renamed tree is deleted afterwards.
    fn sweep_one_with(
        &self,
        candidate: &Path,
        after_lock: &mut dyn FnMut(),
        after_release: &mut dyn FnMut(),
    ) {
        let lock_path = candidate.join(OWNER_LOCK_FILE_NAME);
        let lock_file = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
        {
            Ok(file) => file,
            // Moved away or removed since the listing, by another sweep or by
            // this one: there is nothing left to settle.
            Err(_) if !candidate.exists() => return,
            Err(err) => {
                tracing::warn!(
                    dir = %candidate.display(),
                    error = %err,
                    "cannot prove ownership of this SQL spill root (no readable owner lock); \
                     leaving it in place"
                );
                return;
            }
        };
        match lock_file.try_lock() {
            Ok(()) => {
                // A new owner re-created the root after this sweep opened the
                // old lock file: that root is live, not this one.
                if !still_names(&lock_file, &lock_path) {
                    return;
                }
                after_lock();
                let Some(parent) = candidate.parent() else {
                    return;
                };
                let swept = parent.join(format!(
                    "{SWEPT_NAME_PREFIX}{}-{:016x}-{}",
                    std::process::id(),
                    scratch_nonce(),
                    SCRATCH_SEQUENCE.fetch_add(1, Ordering::Relaxed)
                ));
                if let Err(err) = std::fs::rename(candidate, &swept) {
                    tracing::warn!(
                        dir = %candidate.display(),
                        error = %err,
                        "owner of this SQL spill root is gone, but it could not be moved aside \
                         for removal; leaving it in place"
                    );
                    return;
                }
                drop(lock_file);
                after_release();
                match remove_tree(&swept) {
                    Ok(()) => {
                        tracing::info!(
                            dir = %candidate.display(),
                            "removed orphaned SQL spill root: its owner process is gone"
                        );
                    }
                    Err(err) => {
                        tracing::warn!(
                            dir = %swept.display(),
                            error = %err,
                            "owner of this SQL spill root is gone, but it could not be removed"
                        );
                    }
                }
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                // Owned by a live process. Expected steady state; nothing to log.
            }
            Err(std::fs::TryLockError::Error(err)) => {
                tracing::warn!(
                    dir = %candidate.display(),
                    error = %err,
                    "cannot prove ownership of this SQL spill root; leaving it in place"
                );
            }
        }
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

    /// The scratch subdirectory is owner-only under every usual umask (a
    /// umask can only narrow the mode the builder asks for).
    /// DataFusion's disk manager writes the spill files inside it, so this
    /// mode is what keeps another local user from reading one query's spilled
    /// rows. The bits asserted here are the ones `create_scratch_dir` sets
    /// explicitly; this test neither reads nor changes the umask.
    ///
    /// FLIP (non-vacuity): restore `std::fs::create_dir(&dir)` in
    /// `SpillScratch::create_named`. Under `umask 022` the assertion then
    /// reads `0o755`.
    #[cfg(unix)]
    #[test]
    fn the_scratch_directory_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().expect("temp root");
        let config = SpillConfig {
            dir: root.path().to_path_buf(),
            max_bytes: 1 << 20,
        };
        let scratch = SpillScratch::create(&config).expect("scratch created");
        let mode = std::fs::metadata(scratch.dir())
            .expect("scratch metadata")
            .permissions()
            .mode();
        assert_eq!(
            mode & 0o777,
            0o700,
            "one query's spill files must not be readable by other local users"
        );
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

    /// `acquire` creates `<cache_dir>/sql-spill/<instance_id>` owner-only and
    /// holds an exclusive lock on it, matching [`crate::config::cache_spill_dir`]'s
    /// path construction exactly (the sweep and the server's resolved-line log
    /// both depend on this agreeing).
    #[test]
    fn acquire_creates_the_shared_cache_dir_path() {
        let root = tempfile::tempdir().expect("temp root");
        let owner = SpillRootOwner::acquire(root.path(), "inst-1").expect("acquire must succeed");
        assert_eq!(
            owner.dir(),
            crate::config::cache_spill_dir(root.path(), "inst-1"),
            "the owner's directory must match the shared path-construction helper"
        );
        assert!(owner.dir().is_dir());
    }

    /// Two [`SpillRootOwner`]s cannot acquire the same `(cache_dir,
    /// instance_id)` pair at once: the second's `try_lock` contends with the
    /// first's live lock.
    #[test]
    fn acquiring_the_same_root_twice_while_the_first_is_alive_fails() {
        let root = tempfile::tempdir().expect("temp root");
        let _first =
            SpillRootOwner::acquire(root.path(), "inst-1").expect("first acquire must succeed");
        let second = SpillRootOwner::acquire(root.path(), "inst-1");
        assert!(
            second.is_err(),
            "a second live owner of the same root must be refused"
        );
    }

    /// The sweep removes a sibling root whose owner is provably gone: nobody
    /// holds its `.owner.lock`, so this process's own `try_lock` on it
    /// succeeds.
    ///
    /// FLIP (sweep-by-age, non-vacuity): replace `sweep_one`'s try-lock check
    /// with one that removes any sibling whose directory is at least as old
    /// as `Duration::ZERO` (i.e. every sibling, unconditionally on age). This
    /// test still passes (the dead sibling is still removed), but
    /// `sweep_leaves_a_sibling_with_a_live_owner` then fails: age cannot tell
    /// a live owner from a dead one. See that test for the actual failing
    /// assertion.
    #[test]
    fn sweep_removes_a_sibling_whose_owner_is_provably_gone() {
        let root = tempfile::tempdir().expect("temp root");
        let owner = SpillRootOwner::acquire(root.path(), "inst-1").expect("acquire");

        let dead_dir = root
            .path()
            .join(crate::config::SQL_SPILL_SUBDIR)
            .join("inst-dead");
        std::fs::create_dir_all(&dead_dir).expect("dead sibling dir");
        std::fs::write(dead_dir.join(OWNER_LOCK_FILE_NAME), b"")
            .expect("dead sibling lock file, held by nobody");

        owner.sweep_orphaned_spill_roots();

        assert!(
            !dead_dir.exists(),
            "a sibling with no live lock holder must be removed"
        );
    }

    /// The sweep leaves a sibling root alone while its owner is still alive:
    /// trying to take its lock would block, which is exactly the proof that
    /// it is still owned.
    ///
    /// FLIP (sweep-everything-not-ours, non-vacuity): replace `sweep_one`
    /// with an unconditional `remove_dir_all` on every sibling that is not
    /// `self.dir`. This test then fails: the live sibling's directory no
    /// longer exists after the sweep.
    #[test]
    fn sweep_leaves_a_sibling_with_a_live_owner() {
        let root = tempfile::tempdir().expect("temp root");
        let owner = SpillRootOwner::acquire(root.path(), "inst-1").expect("acquire inst-1");
        let live_owner = SpillRootOwner::acquire(root.path(), "inst-2").expect("acquire inst-2");

        owner.sweep_orphaned_spill_roots();

        assert!(
            live_owner.dir().is_dir(),
            "a sibling root whose owner is still alive must survive the sweep"
        );
    }

    /// A sibling whose ownership cannot be settled (no lock file to test at
    /// all) is left in place rather than guessed at either way.
    #[test]
    fn sweep_leaves_a_sibling_with_no_lock_file() {
        let root = tempfile::tempdir().expect("temp root");
        let owner = SpillRootOwner::acquire(root.path(), "inst-1").expect("acquire");

        let unprovable_dir = root
            .path()
            .join(crate::config::SQL_SPILL_SUBDIR)
            .join("inst-unprovable");
        std::fs::create_dir_all(&unprovable_dir).expect("unprovable sibling dir");

        owner.sweep_orphaned_spill_roots();

        assert!(
            unprovable_dir.is_dir(),
            "a sibling with no owner lock to test must be left in place, not guessed at"
        );
    }

    /// The `dir` field of every WARN event `f` emits on this thread, `-` for
    /// one without it.
    fn warned_dirs(f: impl FnOnce()) -> Vec<String> {
        use tracing_subscriber::layer::SubscriberExt as _;

        #[derive(Clone, Default)]
        struct Warnings(Arc<std::sync::Mutex<Vec<String>>>);
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Warnings {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                if *event.metadata().level() != tracing::Level::WARN {
                    return;
                }
                struct Dir(String);
                impl tracing::field::Visit for Dir {
                    fn record_debug(
                        &mut self,
                        field: &tracing::field::Field,
                        value: &dyn std::fmt::Debug,
                    ) {
                        if field.name() == "dir" {
                            self.0 = format!("{value:?}");
                        }
                    }
                }
                let mut dir = Dir("-".to_string());
                event.record(&mut dir);
                self.0.lock().expect("not poisoned").push(dir.0);
            }
        }
        // With one dispatcher registered, a callsite's interest is cached from
        // whichever thread reaches it first; a second, process-wide one keeps
        // interest computed across every live dispatcher instead.
        static KEEP_TWO_DISPATCHERS: OnceLock<tracing::Dispatch> = OnceLock::new();
        KEEP_TWO_DISPATCHERS
            .get_or_init(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()));
        let warnings = Warnings::default();
        tracing::subscriber::with_default(tracing_subscriber::registry().with(warnings.clone()), f);
        warnings.0.lock().expect("not poisoned").clone()
    }

    /// A tree an earlier sweep moved aside and did not finish removing (here a
    /// `.swept-` folder with a file in it and no owner lock) is removed at
    /// the next startup's sweep, with no WARN. A real orphan beside it is
    /// still removed and an unprovable sibling still WARNs, so the capture is
    /// live.
    ///
    /// Prove-the-test: drop the `.swept-` branch in
    /// `sweep_orphaned_spill_roots`, or keep the branch and skip the folder
    /// instead of calling `remove_swept`, and the folder survives; make every
    /// lock-open error silent and the unprovable sibling's WARN is missing.
    #[test]
    fn a_stale_swept_folder_is_removed_at_startup_without_a_warning() {
        let root = tempfile::tempdir().expect("temp root");
        let sql_spill = root.path().join(crate::config::SQL_SPILL_SUBDIR);
        let swept = sql_spill.join(format!("{SWEPT_NAME_PREFIX}4242-00000000deadbeef-0"));
        std::fs::create_dir_all(swept.join("ravel-spill-1-2-3")).expect("swept tree");
        std::fs::write(swept.join("ravel-spill-1-2-3").join("part"), b"spill").expect("swept file");
        let dead = orphan(root.path(), "inst-dead");
        let unprovable = sql_spill.join("inst-unprovable");
        std::fs::create_dir_all(&unprovable).expect("unprovable sibling dir");

        let warned = warned_dirs(|| {
            let owner = SpillRootOwner::acquire(root.path(), "inst-1").expect("acquire");
            owner.sweep_orphaned_spill_roots();
        });

        assert!(!swept.exists(), "the moved-aside tree must be removed");
        assert!(!dead.exists(), "the orphan must be removed");
        assert!(unprovable.is_dir(), "the unprovable sibling must stay");
        assert_eq!(
            warned,
            vec![unprovable.display().to_string()],
            "only the unprovable sibling warns"
        );
    }

    /// A sibling the listing returned but that is gone by the time the sweep
    /// reaches it (moved aside and deleted by a concurrent sweep, or by this
    /// one) is skipped silently, while a sibling that is present and has no
    /// owner lock still WARNs.
    ///
    /// Prove-the-test: drop the `!candidate.exists()` arm in
    /// `sweep_one_with` and the vanished row WARNs; make every lock-open
    /// error silent and the present row does not WARN.
    #[test]
    fn a_sibling_gone_before_the_sweep_reaches_it_is_not_a_warning() {
        let root = tempfile::tempdir().expect("temp root");
        let owner = SpillRootOwner::acquire(root.path(), "inst-1").expect("acquire");
        let sql_spill = root.path().join(crate::config::SQL_SPILL_SUBDIR);
        let vanished = sql_spill.join("inst-vanished");
        let present = sql_spill.join("inst-unprovable");
        std::fs::create_dir_all(&present).expect("unprovable sibling dir");

        let warned = warned_dirs(|| {
            owner.sweep_one(&vanished);
            owner.sweep_one(&present);
        });

        assert_eq!(warned, vec![present.display().to_string()]);
    }

    const MIB: u64 = 1024 * 1024;

    /// ADR-0954 requirement 2 (issue #2416): the ceiling bounds the sum of
    /// live reservations. Two queries whose caps sum to the ceiling are both
    /// granted, a third is refused while they live, and dropping one returns
    /// its bytes so the next query is granted exactly those.
    #[test]
    fn reservations_never_sum_past_the_ceiling_and_return_on_drop() {
        let budget = SpillBudget::new(192 * MIB);
        let first = budget.reserve(128 * MIB).expect("the first query fits");
        let second = budget
            .reserve(128 * MIB)
            .expect("64 MiB remain, which is the minimum");
        assert_eq!((first.bytes(), second.bytes()), (128 * MIB, 64 * MIB));
        assert_eq!(budget.reserved(), 192 * MIB);
        assert!(budget.reserve(128 * MIB).is_none(), "nothing remains");

        drop(first);
        assert_eq!(budget.reserved(), 64 * MIB);
        let third = budget
            .reserve(192 * MIB)
            .expect("the released bytes return");
        assert_eq!(third.bytes(), 128 * MIB);
        drop((second, third));
        assert_eq!(budget.reserved(), 0);
    }

    /// Less than the minimum left is a refusal, not a sliver of a cap; a
    /// ceiling below the minimum still grants one query the whole of it.
    #[test]
    fn a_remainder_below_the_minimum_is_refused() {
        let budget = SpillBudget::new(128 * MIB);
        let held = budget.reserve(128 * MIB - MIB).expect("fits");
        assert!(
            budget.reserve(128 * MIB).is_none(),
            "1 MiB is below the minimum"
        );
        drop(held);

        let small = SpillBudget::new(64 * 1024);
        let only = small
            .reserve(64 * 1024)
            .expect("a small ceiling still grants");
        assert_eq!(only.bytes(), 64 * 1024);
        assert!(small.reserve(1).is_none());
    }

    /// The reservation a scratch directory holds is released when the
    /// directory is removed, on the same drop.
    #[test]
    fn a_scratch_directory_releases_its_reservation_on_drop() {
        let root = tempfile::tempdir().expect("root");
        let budget = SpillBudget::new(256 * MIB);
        let config = SpillConfig {
            dir: root.path().to_path_buf(),
            max_bytes: 256 * MIB,
        };
        let reservation = budget.reserve(256 * MIB).expect("fits");
        let scratch = SpillScratch::create(&config)
            .expect("scratch")
            .holding(reservation);
        assert_eq!(budget.reserved(), 256 * MIB);
        let dir = scratch.dir().to_path_buf();
        drop(scratch);
        assert!(!dir.exists());
        assert_eq!(budget.reserved(), 0);
    }

    /// The sweep never touches its own root.
    #[test]
    fn sweep_never_removes_its_own_root() {
        let root = tempfile::tempdir().expect("temp root");
        let owner = SpillRootOwner::acquire(root.path(), "inst-1").expect("acquire");
        owner.sweep_orphaned_spill_roots();
        assert!(
            owner.dir().is_dir(),
            "the sweep must never remove its own root"
        );
    }

    /// An orphaned root under `root`, with an owner lock nobody holds.
    fn orphan(root: &Path, instance_id: &str) -> PathBuf {
        let dir = crate::config::cache_spill_dir(root, instance_id);
        std::fs::create_dir_all(&dir).expect("orphan dir");
        std::fs::write(dir.join(OWNER_LOCK_FILE_NAME), b"").expect("orphan lock file");
        dir
    }

    /// The owner holds the lock on the file its root's lock path names, the
    /// root is writable, and nobody else can take the root.
    fn assert_usable(owner: &SpillRootOwner, root: &Path, instance_id: &str) {
        let lock_path = owner.dir().join(OWNER_LOCK_FILE_NAME);
        assert!(
            still_names(&owner._lock, &lock_path),
            "the owner's lock must be on the lock file its root names, not a removed one"
        );
        std::fs::write(owner.dir().join("probe"), b"x").expect("the root is writable");
        let contender = std::fs::File::open(&lock_path).expect("the lock file exists");
        assert!(
            matches!(contender.try_lock(), Err(std::fs::TryLockError::WouldBlock)),
            "the owner's lock is held"
        );
        let entries: Vec<_> = std::fs::read_dir(root.join(crate::config::SQL_SPILL_SUBDIR))
            .expect("readable")
            .map(|entry| entry.expect("entry").file_name())
            .collect();
        assert!(
            entries.iter().any(|name| name == instance_id)
                && entries
                    .iter()
                    .all(|name| !name.to_string_lossy().starts_with(".swept-")),
            "the root is in place and the swept tree is gone: {entries:?}"
        );
    }

    /// ADR-0954 requirement 7 (issue #2416), the peer starting while the sweep
    /// holds the orphan's lock: the peer opens the orphan's lock file while
    /// the sweep holds it, then the sweep moves the root away and removes it.
    /// The peer must end up owning a usable root, not a lock on the removed
    /// file and not a refusal.
    #[test]
    fn an_acquire_racing_the_sweeps_lock_gets_a_usable_root() {
        let root = tempfile::tempdir().expect("temp root");
        let sweeper = SpillRootOwner::acquire(root.path(), "inst-1").expect("sweeper");
        let candidate = orphan(root.path(), "inst-dead");

        let mut peer = None;
        sweeper.sweep_one_with(
            &candidate,
            &mut || {
                let (opened, wait) = std::sync::mpsc::channel();
                let cache_dir = root.path().to_path_buf();
                peer = Some(std::thread::spawn(move || {
                    SpillRootOwner::acquire_with(&cache_dir, "inst-dead", &mut || {
                        let _ = opened.send(());
                    })
                }));
                wait.recv().expect("the peer opened the orphan's lock file");
            },
            &mut || {},
        );

        let owner = peer
            .expect("the sweep reached its lock")
            .join()
            .expect("the peer thread")
            .expect("the peer gets a root");
        assert_usable(&owner, root.path(), "inst-dead");
    }

    /// The peer starting after the sweep released the orphan's lock and
    /// before it deleted anything: the sweep's removal must not reach the
    /// root the peer now owns, which is what deciding ownership and removing
    /// the root under one lock hold means.
    #[test]
    fn an_acquire_between_the_sweeps_release_and_its_removal_keeps_its_root() {
        let root = tempfile::tempdir().expect("temp root");
        let sweeper = SpillRootOwner::acquire(root.path(), "inst-1").expect("sweeper");
        let candidate = orphan(root.path(), "inst-dead");

        let mut peer = None;
        sweeper.sweep_one_with(&candidate, &mut || {}, &mut || {
            peer = Some(SpillRootOwner::acquire(root.path(), "inst-dead"));
        });

        let owner = peer
            .expect("the sweep released the lock")
            .expect("the peer gets a root");
        assert_usable(&owner, root.path(), "inst-dead");
    }

    /// The same race from the other side: the peer has opened its root's lock
    /// file but not locked it when a sweep takes the lock and removes the
    /// root. The peer's lock is then on a removed file, which it must notice
    /// and replace with a usable root.
    #[test]
    fn an_acquire_whose_root_is_swept_before_it_locks_gets_a_usable_root() {
        let root = tempfile::tempdir().expect("temp root");
        let sweeper = SpillRootOwner::acquire(root.path(), "inst-1").expect("sweeper");
        orphan(root.path(), "inst-dead");

        let mut swept = false;
        let owner = SpillRootOwner::acquire_with(root.path(), "inst-dead", &mut || {
            if !swept {
                swept = true;
                sweeper.sweep_orphaned_spill_roots();
            }
        })
        .expect("the peer gets a root");
        assert!(swept);
        assert_usable(&owner, root.path(), "inst-dead");
    }

    /// A root swept away under `acquire` on both attempts is an error naming
    /// the root, not a third attempt and not an owner of a removed root.
    #[test]
    fn an_acquire_that_loses_its_root_twice_refuses() {
        let root = tempfile::tempdir().expect("temp root");
        let sweeper = SpillRootOwner::acquire(root.path(), "inst-1").expect("sweeper");
        let mut sweeps = 0;
        let err = match SpillRootOwner::acquire_with(root.path(), "inst-dead", &mut || {
            sweeps += 1;
            sweeper.sweep_orphaned_spill_roots();
        }) {
            Ok(_) => panic!("a root lost on both attempts must not be owned"),
            Err(err) => err,
        };
        assert_eq!(sweeps, 2, "exactly two attempts");
        assert!(
            err.to_string().contains("inst-dead") && err.to_string().contains("twice"),
            "the error names the root and the cause: {err}"
        );
    }
}
