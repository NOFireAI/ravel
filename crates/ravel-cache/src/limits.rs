/// Bounds on the RAM tier. All three are enforced independently: an entry
/// that would only blow `max_entry_bytes` is rejected even if `max_bytes`
/// has room left, and an insert that fits `max_entry_bytes` can still
/// trigger eviction of other entries to stay under `max_bytes` or
/// `max_entries`.
#[derive(Debug, Clone, Copy)]
pub struct CacheLimits {
    /// Total bytes across every resident entry.
    pub max_bytes: u64,
    /// Total number of resident entries.
    pub max_entries: usize,
    /// Largest single entry the tier will admit. Exceeding this is not an
    /// error: the entry is simply not cached, and the caller still gets
    /// its bytes from whatever fetched them.
    pub max_entry_bytes: u64,
}

impl CacheLimits {
    pub fn new(max_bytes: u64, max_entries: usize, max_entry_bytes: u64) -> Self {
        CacheLimits {
            max_bytes,
            max_entries,
            max_entry_bytes,
        }
    }
}
