//! The logs loader's memory budget (`--load-memory-bytes`, issue #2626,
//! ADR-2614 decision 5).
//!
//! Every built batch is charged to the budget from before the decoder builds
//! it until the last flush carrying its rows finishes: an estimate before the
//! build (the previous batch's measured bytes per row, 0 for the first batch),
//! corrected to the built batch's measured
//! [`ravel_logseg::ColumnarLogBatch::heap_bytes`] once it exists. The decoder
//! waits for room and never fails, so the budget bounds what the pipeline
//! holds in batches; memory it does not hold in batches (the process itself,
//! the Parquet read cursors' decode buffers, each flush's writer working set)
//! is the floor, which the derived default leaves out of the host's memory.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ravel_ingest::{IngestByteBudget, IngestByteBudgetLimit};

const MIB: u64 = 1024 * 1024;

/// Process baseline outside every batch, cursor and flush: the runtime, the
/// allocator's own overhead, the Parquet footer and the mapping.
/// Uncalibrated: no measurement isolated it.
pub const LOAD_MEMORY_BASELINE_BYTES: u64 = 128 * MIB;

/// One read cursor's decode buffers. Stage 0 of issue #2613 measured about
/// 410 MB for K=16 cursors at `--batch-rows 500000`, 26 MiB each; this takes
/// the 32 MiB top of the 26-32 MiB range read off that profile.
pub const LOAD_MEMORY_PER_READ_CURSOR_BYTES: u64 = 32 * MIB;

/// One concurrent flush's writer working set (encode and compress buffers,
/// the object being assembled). Wave 2 of issue #2613 measured a median of
/// 877 MB and a highest of 1,134 MB over 16 concurrent flushes at
/// `--batch-rows 500000 --shards 4 --max-inflight-flushes 4`, 55-71 MiB each;
/// this takes the top of that range.
pub const LOAD_MEMORY_PER_FLUSH_BYTES: u64 = 72 * MIB;

/// The budget when `--load-memory-bytes` is unset and host memory cannot be
/// read ([`ravel_maintain::detect_host_memory_total_bytes`] returned `None`).
/// Uncalibrated: a figure that
/// holds several default-sized batches on any host this loader has run on.
pub const LOAD_MEMORY_FALLBACK_BYTES: u64 = 4 * 1024 * MIB;

/// How long apart the stall flusher's two samples are: the decoder must have
/// waited for memory, with no charge coming back, across both before the
/// shard buffers are flushed to make room.
pub const LOAD_STALL_FLUSH_PERIOD: Duration = Duration::from_millis(250);

/// What the operator asked for, before the read-cursor count that sizes the
/// floor is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadMemoryRequest {
    /// `--load-memory-bytes` as given.
    Flag(u64),
    /// No flag: derive the budget from the host or cgroup memory total read
    /// at start, `None` when it could not be read.
    Derived { host_total_bytes: Option<u64> },
    /// No budget: batches are written uncharged through
    /// `LogIngestRouter::write_columnar`. Not reachable from the CLI; it is
    /// the reference a test compares a budgeted load's objects against.
    Unbudgeted,
}

impl LoadMemoryRequest {
    /// `flag` when given, else the host's memory read now.
    pub fn from_flag_on_host(flag: Option<u64>) -> Self {
        match flag {
            Some(bytes) => Self::Flag(bytes),
            None => Self::Derived {
                host_total_bytes: ravel_maintain::detect_host_memory_total_bytes(),
            },
        }
    }

    /// The line logged when the fallback is in use, `None` otherwise.
    pub fn fallback_warning(&self) -> Option<String> {
        matches!(
            self,
            Self::Derived {
                host_total_bytes: None
            }
        )
        .then(|| {
            format!(
                "warning: host memory could not be read; the loader's memory budget is the \
                 {LOAD_MEMORY_FALLBACK_BYTES} byte fallback. Set --load-memory-bytes to size it \
                 for this host."
            )
        })
    }
}

/// Called once with the load's budget as soon as it exists, so a test can
/// watch the decoder wait on it.
pub type LoadBudgetHook = Arc<dyn Fn(&Arc<IngestByteBudget>) + Send + Sync>;

/// The memory budget's inputs to one load.
#[derive(Clone)]
pub struct LoadMemoryOptions {
    pub request: LoadMemoryRequest,
    /// The stall flusher's sampling period ([`LOAD_STALL_FLUSH_PERIOD`] from
    /// the CLI); a test shortens it rather than sleep on the real one.
    pub stall_flush_period: Duration,
    pub on_budget: Option<LoadBudgetHook>,
    /// The one-batch warning ([`LoadMemory::one_batch_warning`]) once the
    /// load gives it. The report carries it too, but a failed load drops the
    /// report.
    pub one_batch_warning: Arc<OnceLock<String>>,
}

impl LoadMemoryOptions {
    pub fn new(request: LoadMemoryRequest) -> Self {
        Self {
            request,
            stall_flush_period: LOAD_STALL_FLUSH_PERIOD,
            on_budget: None,
            one_batch_warning: Arc::default(),
        }
    }
}

/// Where a load's memory budget came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LoadMemorySource {
    /// `--load-memory-bytes` as given.
    #[default]
    Flag,
    /// The host's memory less the floor, 0 when the floor is not below it.
    Host {
        /// The host or cgroup memory total read at start.
        total_bytes: u64,
    },
    /// Host memory could not be read; [`LOAD_MEMORY_FALLBACK_BYTES`].
    Fallback,
}

/// A resolved loader memory budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LoadMemory {
    /// The bytes built batches may hold at once. A batch larger than this is
    /// admitted only when nothing else is held.
    pub budget_bytes: u64,
    /// The estimated memory outside the budget, from the constants above.
    pub floor_bytes: u64,
    pub source: LoadMemorySource,
}

impl LoadMemory {
    /// The floor for `read_cursors` cursors and `concurrent_flushes` flushes
    /// (shards times `--max-inflight-flushes`).
    pub fn floor_bytes(read_cursors: usize, concurrent_flushes: u64) -> u64 {
        LOAD_MEMORY_BASELINE_BYTES
            .saturating_add(LOAD_MEMORY_PER_READ_CURSOR_BYTES.saturating_mul(read_cursors as u64))
            .saturating_add(LOAD_MEMORY_PER_FLUSH_BYTES.saturating_mul(concurrent_flushes))
    }

    /// Resolves `request` once the load's read-cursor count is known: the flag
    /// as given, the host's memory less the floor (0 when the floor is not
    /// below it), or [`LOAD_MEMORY_FALLBACK_BYTES`]. `None` for
    /// [`LoadMemoryRequest::Unbudgeted`]. A derived budget is never refused
    /// here: below one batch the load admits one batch at a time and says so
    /// ([`Self::one_batch_warning`]); only a flag of 0 is an error.
    pub fn resolve(
        request: LoadMemoryRequest,
        read_cursors: usize,
        concurrent_flushes: u64,
    ) -> Result<Option<Self>, String> {
        let floor_bytes = Self::floor_bytes(read_cursors, concurrent_flushes);
        let (budget_bytes, source) = match request {
            LoadMemoryRequest::Unbudgeted => return Ok(None),
            LoadMemoryRequest::Flag(0) => {
                return Err("--load-memory-bytes must be at least 1; 0 was given".to_string());
            }
            LoadMemoryRequest::Flag(bytes) => (bytes, LoadMemorySource::Flag),
            LoadMemoryRequest::Derived {
                host_total_bytes: Some(total_bytes),
            } => (
                total_bytes.saturating_sub(floor_bytes),
                LoadMemorySource::Host { total_bytes },
            ),
            LoadMemoryRequest::Derived {
                host_total_bytes: None,
            } => (LOAD_MEMORY_FALLBACK_BYTES, LoadMemorySource::Fallback),
        };
        Ok(Some(Self {
            budget_bytes,
            floor_bytes,
            source,
        }))
    }

    /// Whether the operator set the budget. Only an explicit budget below one
    /// batch refuses the load.
    pub fn is_explicit(&self) -> bool {
        self.source == LoadMemorySource::Flag
    }

    /// The refusal when one built batch does not fit an explicit budget.
    pub fn batch_too_large(&self, batch_bytes: u64, batch_rows: usize) -> String {
        format!(
            "one batch of {batch_rows} rows measured {batch_bytes} bytes, more than the \
             loader's memory budget of {} bytes ({}; estimated non-batch floor {} bytes). Lower \
             --batch-rows or raise --load-memory-bytes.",
            self.budget_bytes,
            self.source_label(),
            self.floor_bytes
        )
    }

    /// The warning when one built batch does not fit a derived budget, which
    /// the load then runs under one batch at a time.
    pub fn one_batch_warning(&self, batch_bytes: u64, batch_rows: usize) -> String {
        format!(
            "warning: one batch of {batch_rows} rows measured {batch_bytes} bytes, more than the \
             loader's derived memory budget of {} bytes ({}; estimated non-batch floor {} \
             bytes). The load continues admitting one batch at a time, an effective budget of \
             one batch ({batch_bytes} bytes for this one). Set --load-memory-bytes, or lower \
             --batch-rows, --read-cursors, --shards or --max-inflight-flushes.",
            self.budget_bytes,
            self.source_label(),
            self.floor_bytes
        )
    }

    /// A short description of [`Self::source`] for the summary and errors.
    pub fn source_label(&self) -> String {
        match self.source {
            LoadMemorySource::Flag => "--load-memory-bytes".to_string(),
            LoadMemorySource::Host { total_bytes } => {
                format!("host memory {total_bytes} bytes less the floor")
            }
            LoadMemorySource::Fallback => "fallback, host memory unreadable".to_string(),
        }
    }

    pub(super) fn budget(&self) -> Arc<IngestByteBudget> {
        IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(self.budget_bytes))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn resolve(request: LoadMemoryRequest) -> LoadMemory {
        LoadMemory::resolve(request, 4, 4)
            .expect("resolves")
            .expect("budgeted")
    }

    #[test]
    fn floor_sums_baseline_cursors_and_flushes() {
        assert_eq!(
            LoadMemory::floor_bytes(16, 16),
            (128 + 16 * 32 + 16 * 72) * MIB
        );
    }

    #[test]
    fn flag_wins_and_host_derives_less_the_floor() {
        let floor = LoadMemory::floor_bytes(4, 4);
        let flag = resolve(LoadMemoryRequest::Flag(5_000));
        assert_eq!(flag.budget_bytes, 5_000);
        assert_eq!(flag.source, LoadMemorySource::Flag);
        assert!(flag.is_explicit());

        let host = resolve(LoadMemoryRequest::Derived {
            host_total_bytes: Some(floor + 7),
        });
        assert_eq!(host.budget_bytes, 7);
        assert_eq!(
            host.source,
            LoadMemorySource::Host {
                total_bytes: floor + 7
            }
        );
        assert!(!host.is_explicit());
        assert!(LoadMemory::resolve(LoadMemoryRequest::Flag(0), 4, 4).is_err());
        assert_eq!(
            LoadMemory::resolve(LoadMemoryRequest::Unbudgeted, 4, 4),
            Ok(None)
        );
    }

    /// Decision A of the issue #2626 fix round: a host at or below the floor
    /// derives a budget of 0, which the loader runs one batch at a time,
    /// rather than refusing a default load.
    #[test]
    fn a_host_at_or_below_the_floor_derives_zero_not_a_refusal() {
        let floor = LoadMemory::floor_bytes(4, 4);
        for total_bytes in [floor, floor - 1, 1] {
            let host = resolve(LoadMemoryRequest::Derived {
                host_total_bytes: Some(total_bytes),
            });
            assert_eq!(host.budget_bytes, 0);
            assert_eq!(host.floor_bytes, floor);
            assert_eq!(host.source, LoadMemorySource::Host { total_bytes });
            let warning = host.one_batch_warning(9_000, 500);
            for needle in [
                format!("host memory {total_bytes} bytes"),
                format!("floor {floor} bytes"),
                "one batch at a time".to_string(),
                "9000 bytes for this one".to_string(),
            ] {
                assert!(warning.contains(&needle), "{needle:?} missing: {warning}");
            }
        }
    }

    #[test]
    fn unreadable_host_uses_the_named_fallback_and_says_so() {
        let request = LoadMemoryRequest::Derived {
            host_total_bytes: None,
        };
        let fallback = resolve(request);
        assert_eq!(fallback.budget_bytes, LOAD_MEMORY_FALLBACK_BYTES);
        assert_eq!(fallback.source, LoadMemorySource::Fallback);
        let warning = request.fallback_warning().expect("fallback is logged");
        assert!(warning.contains(&LOAD_MEMORY_FALLBACK_BYTES.to_string()));
        assert_eq!(LoadMemoryRequest::Flag(9).fallback_warning(), None);
    }
}
