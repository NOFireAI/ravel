//! Process-shareable GET concurrency limiter (ADR-1195).
//!
//! Before this, each fetcher held a private `Arc<Semaphore>`, so concurrent
//! segment fetches within one fetcher were bounded, but two fetchers (the
//! RSEG and RLOG paths inside one engine, or two engines in one process)
//! never shared a bound at all: their permits multiplied instead of adding.
//! `GetLimiter` is the same shape, `Arc`-wrapped so several fetchers -- and,
//! via [`crate::QueryEngine::with_get_limiter`], several engines -- can hold
//! the same instance and draw from one pool of permits.

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::EngineConfigError;

/// [`GetLimiter::acquire`]'s semaphore closed before granting a permit. The
/// semaphore is private to `GetLimiter`, which exposes no `close`, so no
/// caller can ever trigger this; it exists so `acquire` fails typed instead
/// of panicking if that invariant is ever violated.
#[derive(Debug, thiserror::Error)]
#[error("GetLimiter semaphore closed unexpectedly")]
pub struct GetLimiterClosed;

/// A bound on concurrent object-store GETs, shareable across fetchers and
/// engines by cloning the `Arc` it is always held behind.
pub struct GetLimiter {
    semaphore: Arc<Semaphore>,
    permits: usize,
}

impl GetLimiter {
    /// Rejects zero: a zero-permit limiter can never issue a GET, which is
    /// never a meaningful configuration (ADR-1195 "zero is rejected during
    /// configuration resolution").
    pub fn new(permits: usize) -> Result<GetLimiter, EngineConfigError> {
        if permits == 0 {
            return Err(EngineConfigError::ZeroGetLimiterPermits);
        }
        Ok(GetLimiter::new_unchecked(permits))
    }

    /// Builds a limiter with no zero check, for the fetcher builders
    /// (`with_max_concurrent_gets`) that keep their pre-ADR-1195
    /// clamp-zero-to-one behaviour rather than erroring: the caller has
    /// already clamped `permits` to at least 1.
    pub(crate) fn new_unchecked(permits: usize) -> GetLimiter {
        GetLimiter {
            semaphore: Arc::new(Semaphore::new(permits)),
            permits,
        }
    }

    /// The permit count this limiter was built with.
    pub fn permits(&self) -> usize {
        self.permits
    }

    /// Acquires one owned permit. Owned rather than borrowed so a fetcher can
    /// hold it across an `.await` on the store GET itself without borrowing
    /// this limiter; drop the permit to release it back to the pool. Errors
    /// with [`GetLimiterClosed`] if the semaphore is ever closed, which no
    /// current caller can trigger (see that type's doc), so a caller maps it
    /// into its own error type rather than treating it as reachable today.
    pub fn acquire(&self) -> impl Future<Output = Result<OwnedSemaphorePermit, GetLimiterClosed>> {
        let semaphore = Arc::clone(&self.semaphore);
        async move {
            semaphore
                .acquire_owned()
                .await
                .map_err(|_| GetLimiterClosed)
        }
    }
}

impl fmt::Debug for GetLimiter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GetLimiter")
            .field("permits", &self.permits)
            .field("available_permits", &self.semaphore.available_permits())
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn zero_permits_is_rejected() {
        assert_eq!(
            GetLimiter::new(0).err(),
            Some(EngineConfigError::ZeroGetLimiterPermits)
        );
    }

    #[test]
    fn nonzero_permits_reports_itself() {
        let limiter = GetLimiter::new(4).expect("4 permits is valid");
        assert_eq!(limiter.permits(), 4);
        assert_eq!(
            format!("{limiter:?}"),
            "GetLimiter { permits: 4, available_permits: 4 }"
        );
    }

    #[tokio::test]
    async fn acquire_yields_an_owned_permit_and_releases_on_drop() {
        let limiter = GetLimiter::new(1).expect("1 permit is valid");
        let permit = limiter.acquire().await.expect("semaphore is never closed");
        assert_eq!(limiter.semaphore.available_permits(), 0);
        drop(permit);
        assert_eq!(limiter.semaphore.available_permits(), 1);
    }
}
