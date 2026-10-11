//! How many consecutive blocks of one part a read-gate scan job decodes
//! (ADR-1702 decision 4, as amended for issue #2795).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// The bound on one read-gate job that decodes a run of consecutive blocks of
/// one part.
///
/// The job decodes blocks in scan order and starts a further block only while
/// it has decoded fewer than [`max_blocks`](Self::max_blocks) and the decoded
/// bytes of the blocks it holds are below
/// [`max_decoded_bytes`](Self::max_decoded_bytes). A run therefore holds less
/// than `max_decoded_bytes` plus one block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRun {
    /// Blocks one job decodes at most. 0 is read as 1.
    pub max_blocks: usize,
    /// Once the blocks a job holds have decoded this many bytes, the job
    /// decodes no further block.
    pub max_decoded_bytes: u64,
}

/// The run [`LogSegmentFetcher`](crate::LogSegmentFetcher) and
/// [`SpanSegmentFetcher`](crate::SpanSegmentFetcher) default to: up to 8
/// blocks, and no further block once the run holds 16 MiB. One job per block
/// cost about 15% of hot time on the reference box (issue #2795); spread over
/// 8 blocks, a per-job cost of that size is under 2%. The cap bounds the
/// decoded bytes one run holds.
pub const DEFAULT_BLOCK_RUN: BlockRun = BlockRun {
    max_blocks: 8,
    max_decoded_bytes: 16 << 20,
};

impl BlockRun {
    /// One block per job.
    pub const SINGLE: BlockRun = BlockRun {
        max_blocks: 1,
        max_decoded_bytes: u64::MAX,
    };

    /// This run with at most `blocks` blocks per job.
    #[must_use]
    pub fn capped(self, blocks: usize) -> BlockRun {
        BlockRun {
            max_blocks: self.blocks().min(blocks.max(1)),
            ..self
        }
    }

    /// Blocks one job decodes at most, at least 1.
    pub fn blocks(&self) -> usize {
        self.max_blocks.max(1)
    }

    /// Whether a job that has decoded `blocks` blocks totalling
    /// `decoded_bytes` starts another.
    pub fn takes_another(&self, blocks: usize, decoded_bytes: u64) -> bool {
        blocks < self.blocks() && decoded_bytes < self.max_decoded_bytes
    }
}

impl Default for BlockRun {
    fn default() -> Self {
        DEFAULT_BLOCK_RUN
    }
}

/// Sets its flag when dropped. Held by the future awaiting a run job, so a
/// caller that stops waiting tells the job to decode no further block.
pub(crate) struct StopOnDrop(pub(crate) Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_run_stops_at_its_block_count_or_its_byte_cap() {
        let run = BlockRun {
            max_blocks: 3,
            max_decoded_bytes: 100,
        };
        assert!(run.takes_another(0, 0));
        assert!(run.takes_another(2, 99));
        assert!(!run.takes_another(3, 0), "the block count binds");
        assert!(!run.takes_another(1, 100), "the byte cap binds");
        assert!(!BlockRun::SINGLE.takes_another(1, 0));
        let zero = BlockRun {
            max_blocks: 0,
            max_decoded_bytes: 100,
        };
        assert!(zero.takes_another(0, 0), "0 blocks is read as 1");
        assert!(!zero.takes_another(1, 0));
        assert_eq!(run.capped(1).blocks(), 1);
        assert_eq!(run.capped(10).blocks(), 3);
        assert_eq!(run.capped(0).blocks(), 1);
    }
}
