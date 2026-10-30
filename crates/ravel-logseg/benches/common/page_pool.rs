//! A multi-threaded page encode executor (ADR-3492 decision 7) on
//! `std::thread::scope`, shared by the `logseg_encode` bench and the parallel
//! encode tests.

use std::sync::Mutex;
use std::thread;

use ravel_logseg::{LogSegError, PageEncodeDone, PageEncodeExecutor, PageEncodeJob};

/// Runs each block's jobs on `threads` scoped threads. Every worker takes the
/// last job still queued, so jobs start in reverse column order, and the
/// results go back in the order they finished, not job order.
pub struct ScopedPool {
    threads: usize,
    /// Calls that had more than one job.
    pub split_calls: usize,
    /// Calls whose results went back in an order other than job order.
    pub out_of_order_calls: usize,
}

impl ScopedPool {
    pub fn new(threads: usize) -> Self {
        ScopedPool {
            threads,
            split_calls: 0,
            out_of_order_calls: 0,
        }
    }
}

impl PageEncodeExecutor for ScopedPool {
    fn execute(&mut self, jobs: Vec<PageEncodeJob>) -> Result<Vec<PageEncodeDone>, LogSegError> {
        let count = jobs.len();
        let queue = Mutex::new(jobs);
        let finished: Mutex<Vec<(usize, PageEncodeDone)>> = Mutex::new(Vec::with_capacity(count));
        thread::scope(|s| {
            for _ in 0..self.threads.max(1) {
                s.spawn(|| {
                    loop {
                        let job = match queue.lock() {
                            Ok(mut q) => q.pop(),
                            Err(_) => None,
                        };
                        let Some(job) = job else { break };
                        let index = job.index();
                        let done = job.run();
                        if let Ok(mut f) = finished.lock() {
                            f.push((index, done));
                        }
                    }
                });
            }
        });
        let finished = finished
            .into_inner()
            .map_err(|_| LogSegError::PageEncode("a worker panicked".into()))?;
        if count > 1 {
            self.split_calls += 1;
            if finished.iter().enumerate().any(|(pos, (index, _))| pos != *index) {
                self.out_of_order_calls += 1;
            }
        }
        Ok(finished.into_iter().map(|(_, done)| done).collect())
    }
}
