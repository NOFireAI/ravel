//! Signal-agnostic counters for IPCrypt rendering outcomes, keyed by
//! `(variant, outcome)` (deliverable 6). Cheap when empty (an empty
//! `HashMap`, no allocation) and mergeable across shards/processes.

use std::collections::HashMap;

use crate::policy::Variant;
use crate::render::RenderOutcome;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IpcryptTally {
    counts: HashMap<(Variant, RenderOutcome), u64>,
}

impl IpcryptTally {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    pub fn record(&mut self, variant: Variant, outcome: RenderOutcome) {
        let entry = self.counts.entry((variant, outcome)).or_insert(0);
        *entry = entry.saturating_add(1);
    }

    pub fn count(&self, variant: Variant, outcome: RenderOutcome) -> u64 {
        self.counts.get(&(variant, outcome)).copied().unwrap_or(0)
    }

    pub fn merge(&mut self, other: &IpcryptTally) {
        for (key, count) in &other.counts {
            let entry = self.counts.entry(*key).or_insert(0);
            *entry = entry.saturating_add(*count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_tally_is_empty() {
        assert!(IpcryptTally::new().is_empty());
    }

    #[test]
    fn record_and_count() {
        let mut tally = IpcryptTally::new();
        tally.record(Variant::Deterministic, RenderOutcome::Encrypted);
        tally.record(Variant::Deterministic, RenderOutcome::Encrypted);
        tally.record(Variant::Prefix, RenderOutcome::Unparsed);
        assert_eq!(
            tally.count(Variant::Deterministic, RenderOutcome::Encrypted),
            2
        );
        assert_eq!(tally.count(Variant::Prefix, RenderOutcome::Unparsed), 1);
        assert_eq!(tally.count(Variant::Prefix, RenderOutcome::Encrypted), 0);
        assert!(!tally.is_empty());
    }

    #[test]
    fn merge_sums_counts() {
        let mut a = IpcryptTally::new();
        a.record(Variant::Deterministic, RenderOutcome::Encrypted);
        let mut b = IpcryptTally::new();
        b.record(Variant::Deterministic, RenderOutcome::Encrypted);
        b.record(Variant::Prefix, RenderOutcome::Unparsed);
        a.merge(&b);
        assert_eq!(a.count(Variant::Deterministic, RenderOutcome::Encrypted), 2);
        assert_eq!(a.count(Variant::Prefix, RenderOutcome::Unparsed), 1);
    }
}
