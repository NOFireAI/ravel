//! Catalog decodes on the read CPU gate (ADR-1702 decisions 4 and 6).
//!
//! The codecs in [`crate::snapshot_format`] and the metrics-meta body decoder
//! stay synchronous; the gate is applied here, at their async callers. With no
//! gate the job runs inline on the calling task, exactly as it did before the
//! gate existed. With one, [`ReadGate::run`] decides between inline (below its
//! floor, counted as inline) and the blocking pool (counted as a job).

use ravel_cpu_gate::{CpuGateError, JobSize, ReadGate, ReadSite};

use crate::snapshot_format::SnapshotFormatError;

/// Runs `job` for `site` on `gate`, or inline when there is no gate.
/// `declared` is the unit's declared uncompressed length, the figure the
/// gate compares against its inline floor. The caller moves its input bytes
/// and its memory reservation into `job`, so both stay held while the job
/// waits for a permit.
pub(crate) async fn run_decode<F, R>(
    gate: Option<&ReadGate>,
    site: ReadSite,
    declared: u64,
    job: F,
) -> Result<R, CpuGateError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    match gate {
        None => Ok(job()),
        Some(gate) => gate.run(site, JobSize::Bytes(declared), job).await,
    }
}

/// [`run_decode`] for a [`crate::snapshot_format`] decode: a job the gate
/// cannot complete becomes [`SnapshotFormatError::DecodeJob`], the same error
/// type the decode itself returns, so the caller has one decode-error arm.
pub(crate) async fn run_snapshot_decode<F, R>(
    gate: Option<&ReadGate>,
    site: ReadSite,
    declared: u64,
    job: F,
) -> Result<R, SnapshotFormatError>
where
    F: FnOnce() -> Result<R, SnapshotFormatError> + Send + 'static,
    R: Send + 'static,
{
    run_decode(gate, site, declared, job)
        .await
        .unwrap_or_else(|err| Err(SnapshotFormatError::DecodeJob(err)))
}

/// Gate construction and counter reads shared by the gated-decode tests.
#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Arc;

    use ravel_cpu_gate::{CpuGateConfig, InstantClock, ReadGate, ReadSite};

    /// A read gate with two permits and the given inline floor.
    pub(crate) fn gate(inline_floor_bytes: u64) -> Arc<ReadGate> {
        Arc::new(ReadGate::new(
            CpuGateConfig {
                inline_floor_bytes,
                ..CpuGateConfig::with_permits(2)
            },
            Arc::new(InstantClock::new()),
        ))
    }

    /// `(jobs, inline)` for `site`, read from the gate's snapshot.
    pub(crate) fn counts(gate: &ReadGate, site: ReadSite) -> (u64, u64) {
        gate.snapshot()
            .sites
            .iter()
            .find(|s| s.site == site)
            .map_or((0, 0), |s| (s.jobs, s.inline))
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{counts, gate};
    use super::*;

    #[tokio::test]
    async fn no_gate_runs_the_job_inline_and_counts_nothing() {
        assert_eq!(
            run_decode(None, ReadSite::CatalogPart, 1 << 30, || 7).await,
            Ok(7)
        );
    }

    #[tokio::test]
    async fn the_gate_floor_decides_job_or_inline() {
        let gate = gate(100);
        assert_eq!(
            run_decode(Some(&gate), ReadSite::CatalogPart, 100, || 1).await,
            Ok(1)
        );
        assert_eq!(
            run_decode(Some(&gate), ReadSite::CatalogPart, 99, || 2).await,
            Ok(2)
        );
        assert_eq!(counts(&gate, ReadSite::CatalogPart), (1, 1));
    }

    /// A job that panics on the blocking pool is the decode's own typed
    /// error, for every snapshot-format site, and no panic reaches the caller.
    #[tokio::test]
    async fn a_panicking_snapshot_decode_job_is_a_typed_decode_error() {
        let gate = gate(0);
        for site in [
            ReadSite::CatalogPart,
            ReadSite::CatalogPostings,
            ReadSite::CatalogColumnStats,
        ] {
            let got: Result<(), SnapshotFormatError> =
                run_snapshot_decode(Some(&gate), site, 1, || panic!("decode job panics")).await;
            assert_eq!(
                got,
                Err(SnapshotFormatError::DecodeJob(CpuGateError::Panicked)),
                "{site:?}"
            );
            assert_eq!(counts(&gate, site), (1, 0), "{site:?} ran as a gate job");
        }
    }

    /// A decode error returned by the job passes through unchanged.
    #[tokio::test]
    async fn a_failing_snapshot_decode_job_returns_its_own_error() {
        let gate = gate(0);
        let got: Result<(), SnapshotFormatError> =
            run_snapshot_decode(Some(&gate), ReadSite::CatalogPart, 1, || {
                Err(SnapshotFormatError::BodyCrcMismatch)
            })
            .await;
        assert_eq!(got, Err(SnapshotFormatError::BodyCrcMismatch));
    }
}
