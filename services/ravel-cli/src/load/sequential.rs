//! The sequential write path the metrics and spans loaders share: resolving,
//! draining and harvesting in-flight writes.

use super::*;

/// One in-flight Strict write on a sequential load path: the source rows it
/// carries, the records it carries, and the task running it.
///
/// Shared by the metrics and spans loads, whose write windows differ only in
/// the router's receipt and error types ([`WriteAck`], [`WriteFailure`]) and
/// in which report the acks fold into ([`SequentialReport`]).
pub(super) type Inflight<A, F> = (u64, u64, tokio::task::JoinHandle<Result<A, F>>);

/// A Strict write's success value, reduced to the commit tokens a load report
/// keeps.
pub(super) trait WriteAck {
    fn into_tokens(self) -> Vec<CommitToken>;
}

impl WriteAck for WriteReceipt {
    fn into_tokens(self) -> Vec<CommitToken> {
        self.tokens
    }
}

impl WriteAck for SpanWriteReceipt {
    fn into_tokens(self) -> Vec<CommitToken> {
        self.tokens
    }
}

/// A Strict write's failure, reduced to its message and to whatever sibling
/// shards the router recovered from a partial write. Deliberately not named
/// `durable_tokens`: both concrete error types already have an inherent method
/// of that name, and an inherent method shadows a trait one at every call
/// site, which would make this trait's impls silently recursive.
pub(super) trait WriteFailure: std::fmt::Display {
    fn recovered_tokens(&self) -> &[CommitToken];
}

impl WriteFailure for WriteError {
    fn recovered_tokens(&self) -> &[CommitToken] {
        self.durable_tokens()
    }
}

impl WriteFailure for SpanWriteError {
    fn recovered_tokens(&self) -> &[CommitToken] {
        self.durable_tokens()
    }
}

/// The router call that publishes every shard buffer, so a wait on buffered
/// writes is answered by a flush rather than by the age trigger.
pub(super) trait FlushAll {
    async fn flush_all(&self);
}

impl FlushAll for IngestRouter {
    async fn flush_all(&self) {
        IngestRouter::flush_all(self).await;
    }
}

impl FlushAll for SpanIngestRouter {
    async fn flush_all(&self) {
        SpanIngestRouter::flush_all(self).await;
    }
}

/// Publish every shard buffer once every submitted write is enqueued ahead
/// of the flush request. `None` when no router was built, so nothing was
/// submitted.
pub(super) async fn flush_buffered<R: FlushAll>(router: Option<&R>, enqueue: &mut EnqueueTracker) {
    if let Some(router) = router {
        enqueue.wait_enqueued().await;
        router.flush_all().await;
    }
}

/// The load-report surface the shared in-flight window writes through.
pub(super) trait SequentialReport {
    /// Fold one acked write's tokens and counts into the report.
    fn record_ack(&mut self, rows: u64, records: u64, tokens: Vec<CommitToken>);
    /// Tokens known durable so far, in submission order.
    fn tokens(&self) -> &[CommitToken];
    /// The two figures a failed load hands back.
    fn resume(&self) -> ResumeFigures;
}

impl SequentialReport for MetricsLoadReport {
    fn record_ack(&mut self, rows: u64, records: u64, tokens: Vec<CommitToken>) {
        self.tokens.extend(tokens);
        self.rows_processed += rows;
        self.points_written += records;
    }

    fn tokens(&self) -> &[CommitToken] {
        &self.tokens
    }

    fn resume(&self) -> ResumeFigures {
        ResumeFigures {
            rows_skipped: self.rows_skipped,
            rows_written: self.rows_processed,
        }
    }
}

impl SequentialReport for SpansLoadReport {
    fn record_ack(&mut self, rows: u64, _records: u64, tokens: Vec<CommitToken>) {
        self.tokens.extend(tokens);
        self.rows_processed += rows;
    }

    fn tokens(&self) -> &[CommitToken] {
        &self.tokens
    }

    fn resume(&self) -> ResumeFigures {
        ResumeFigures {
            rows_skipped: self.rows_skipped,
            rows_written: self.rows_processed,
        }
    }
}

/// Resolve one in-flight write, folding its tokens and counts into the report
/// or turning its failure into a [`LoadError::Flush`] that carries the tokens
/// already durable, including any sibling shard the router recovered from a
/// partial write.
pub(super) async fn resolve_sequential_write<A: WriteAck, F: WriteFailure, P: SequentialReport>(
    entry: Inflight<A, F>,
    report: &mut P,
) -> Result<(), LoadError> {
    let (rows, records, handle) = entry;
    match handle.await {
        Ok(Ok(receipt)) => {
            report.record_ack(rows, records, receipt.into_tokens());
            Ok(())
        }
        Ok(Err(err)) => {
            let mut durable = report.tokens().to_vec();
            durable.extend_from_slice(err.recovered_tokens());
            Err(LoadError::Flush {
                durable,
                cause: err.to_string(),
                resume: report.resume(),
            })
        }
        Err(join_err) => Err(LoadError::Flush {
            durable: report.tokens().to_vec(),
            cause: format!("write task failed: {join_err}"),
            resume: report.resume(),
        }),
    }
}

/// Resolve every remaining in-flight write, oldest-first. On the first write
/// error every later write is still resolved and whatever it committed is
/// folded into that error's durable-token list
/// ([`harvest_sequential_after_failure`]), as the steady-state loop does.
pub(super) async fn drain_sequential_inflight<
    A: WriteAck,
    F: WriteFailure,
    P: SequentialReport,
    R: FlushAll,
>(
    inflight: &mut std::collections::VecDeque<Inflight<A, F>>,
    report: &mut P,
    router: Option<&R>,
    enqueue: &mut EnqueueTracker,
) -> Result<(), LoadError> {
    while let Some(entry) = inflight.pop_front() {
        if let Err(mut e) = resolve_sequential_write(entry, report).await {
            harvest_sequential_after_failure(inflight, &mut e, router, enqueue).await;
            return Err(e);
        }
    }
    Ok(())
}

/// Drain the window ahead of a decode refusal, returning the durable-token
/// list the refusal reports and its reason. A write failure found by the drain
/// does not replace the refusal: its tokens (including any it harvested from
/// later writes) become the refusal's durable list and its cause is appended
/// to the reason, since the resume figures then stop at that write rather than
/// at the refused row. Every shard buffer is flushed first, so the drain does
/// not wait out the age trigger on writes no later batch will push out.
pub(super) async fn drain_sequential_before_refusal<
    A: WriteAck,
    F: WriteFailure,
    P: SequentialReport,
    R: FlushAll,
>(
    inflight: &mut std::collections::VecDeque<Inflight<A, F>>,
    report: &mut P,
    reason: String,
    router: Option<&R>,
    enqueue: &mut EnqueueTracker,
) -> (Vec<CommitToken>, String) {
    flush_buffered(router, enqueue).await;
    match drain_sequential_inflight(inflight, report, router, enqueue).await {
        Ok(()) => (report.tokens().to_vec(), reason),
        Err(e) => (
            e.durable_tokens().to_vec(),
            format!("{reason} (an earlier write had also failed: {e})"),
        ),
    }
}

/// Fold whatever the still-outstanding writes committed into an error's
/// durable-token list. The loader cannot stop a shard-actor flush it already
/// handed off, so awaiting the outcome is what keeps the report equal to what
/// landed (issue #800's reasoning on the logs path). Every shard buffer is
/// flushed first, so those writes are answered by that flush rather than by
/// the age trigger.
pub(super) async fn harvest_sequential_after_failure<A: WriteAck, F: WriteFailure, R: FlushAll>(
    inflight: &mut std::collections::VecDeque<Inflight<A, F>>,
    err: &mut LoadError,
    router: Option<&R>,
    enqueue: &mut EnqueueTracker,
) {
    if !inflight.is_empty() {
        flush_buffered(router, enqueue).await;
    }
    let mut recovered: Vec<CommitToken> = Vec::new();
    while let Some((_, _, handle)) = inflight.pop_front() {
        match handle.await {
            Ok(Ok(receipt)) => recovered.extend(receipt.into_tokens()),
            Ok(Err(write_err)) => recovered.extend_from_slice(write_err.recovered_tokens()),
            Err(_) => {}
        }
    }
    if let Some(durable) = err.durable_tokens_mut() {
        durable.extend(recovered);
    }
}
