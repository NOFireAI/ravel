//! The stride dealer as it stood before issue #2613, and the differential
//! tests that hold today's dealer to its batch composition.
//!
//! Issue #2613 made each cursor's Parquet reader decode `ceil(batch_rows / K)`
//! rows at a time instead of `batch_rows`. Which rows land in which batch
//! decides the RLOG objects and the durable-token batch boundaries, so the
//! dealer has to deal exactly what the pre-change dealer dealt over its
//! `batch_rows`-row readers. Comparing today's dealer with itself at two reader
//! sizes cannot show that; these tests compare it with a copy of the old one.

use proptest::prelude::*;

use super::*;

/// [`cursor_take`] and [`collect_spans`] copied from the parent of the #2613
/// change (commit 9086a313f), less the build-start hook. Test-only: `load`
/// never reaches this module. Run it over cursors opened with a
/// `batch_rows`-row reader, as the old loader opened them.
mod reference {
    use super::*;

    /// One reader call at most: rows still buffered from the last Arrow batch,
    /// else the next batch, else `None` with the reader dropped.
    fn pre_2613_reference_cursor_take(
        cur: &mut CursorState,
        want: usize,
    ) -> Result<Option<(RecordBatch, u64)>, String> {
        let buf = loop {
            match cur.buffered.take() {
                Some(buf) if buf.num_rows() > 0 => break buf,
                Some(_) | None => {}
            }
            let Some(reader) = cur.reader.as_mut() else {
                return Ok(None);
            };
            match reader.next() {
                None => {
                    cur.reader = None;
                    return Ok(None);
                }
                Some(Ok(batch)) => cur.buffered = Some(batch),
                Some(Err(e)) => return Err(format!("failed to read Parquet batch: {e}")),
            }
        };
        let total = buf.num_rows();
        let take_n = want.min(total);
        let file_base = cur.partition_base + cur.consumed;
        cur.consumed += take_n as u64;
        if take_n == total {
            Ok(Some((buf, file_base)))
        } else {
            let out = buf.slice(0, take_n);
            cur.buffered = Some(buf.slice(take_n, total - take_n));
            Ok(Some((out, file_base)))
        }
    }

    /// One `pre_2613_reference_cursor_take` call per live cursor per round.
    pub(super) fn pre_2613_reference_collect_spans(
        state: &mut StrideCursors,
        batch_rows: usize,
    ) -> SpanOutcome {
        let live: Vec<usize> = state
            .cursors
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                !(c.reader.is_none() && c.buffered.as_ref().is_none_or(|b| b.num_rows() == 0))
            })
            .map(|(i, _)| i)
            .collect();
        if live.is_empty() {
            return SpanOutcome::Done;
        }

        let l = live.len();
        let base = batch_rows / l;
        let extra = batch_rows % l;

        let mut spans: Vec<(RecordBatch, u64)> = Vec::with_capacity(l);
        for (j, &idx) in live.iter().enumerate() {
            let bonus = usize::from((j + state.deal_offset) % l < extra);
            let share = base + bonus;
            if share == 0 {
                continue;
            }
            match pre_2613_reference_cursor_take(&mut state.cursors[idx], share) {
                Ok(Some((batch, file_base))) if batch.num_rows() > 0 => {
                    spans.push((batch, file_base))
                }
                Ok(_) => {}
                Err(reason) => return SpanOutcome::Failed(reason),
            }
        }
        state.deal_offset = (state.deal_offset + extra) % l;

        if state.skip_rows > 0 {
            spans.retain_mut(|(batch, file_base)| {
                let end = *file_base + batch.num_rows() as u64;
                if end <= state.skip_rows {
                    return false;
                }
                if *file_base < state.skip_rows {
                    let cut = (state.skip_rows - *file_base) as usize;
                    *batch = batch.slice(cut, batch.num_rows() - cut);
                    *file_base += cut as u64;
                }
                true
            });
        }
        SpanOutcome::Spans(spans)
    }
}

use reference::pre_2613_reference_collect_spans;

#[derive(Debug, Clone, Copy)]
enum Dealer {
    /// `collect_spans` over cursors decoding `reader_batch_rows(B, K)` rows,
    /// as `load` opens them.
    Production,
    /// The pre-#2613 dealer over cursors decoding `B` rows.
    Pre2613Reference,
}

/// Open the stride cursors `load` would open over `pq` for `dealer`.
fn open_for(pq: &Path, batch_rows: usize, read_cursors: usize, dealer: Dealer) -> StrideCursors {
    let input = FileInput { path: pq };
    let metadata = read_input_metadata(&input).expect("read metadata");
    let row_group_lens = row_group_row_counts(&metadata);
    let k = resolve_read_cursors(Some(read_cursors), 4, row_group_lens.len());
    let reader_rows = match dealer {
        Dealer::Production => reader_batch_rows(batch_rows, k),
        Dealer::Pre2613Reference => batch_rows,
    };
    let cursors = open_stride_cursors(
        &input,
        &metadata,
        &row_group_lens,
        k,
        batch_rows,
        reader_rows,
    )
    .expect("cursors");
    StrideCursors {
        cursors,
        deal_offset: 0,
        skip_rows: 0,
    }
}

/// One round of `dealer`.
fn deal(state: &mut StrideCursors, batch_rows: usize, dealer: Dealer) -> SpanOutcome {
    match dealer {
        Dealer::Production => collect_spans(state, batch_rows, None),
        Dealer::Pre2613Reference => pre_2613_reference_collect_spans(state, batch_rows),
    }
}

/// Every batch `dealer` deals from `pq` until all cursors are exhausted, each
/// as the file-absolute indices (the `seq` column) of its rows in order.
/// Zero-row batches are kept: the round they appear in is part of the
/// composition.
fn dealt_batches(
    pq: &Path,
    batch_rows: usize,
    read_cursors: usize,
    skip_rows: u64,
    dealer: Dealer,
) -> Vec<Vec<i64>> {
    let mut state = open_for(pq, batch_rows, read_cursors, dealer);
    state.skip_rows = skip_rows;
    let metadata = read_input_metadata(&FileInput { path: pq }).expect("read metadata");
    let total_rows: u64 = row_group_row_counts(&metadata).iter().sum();
    // Generous: every round deals a row or retires a cursor, unless the
    // live-cursor count exceeds `batch_rows` and some shares are zero.
    let round_cap = (total_rows as usize + 2) * (state.cursors.len() + 1);
    let mut batches = Vec::new();
    loop {
        match deal(&mut state, batch_rows, dealer) {
            SpanOutcome::Done => return batches,
            SpanOutcome::Failed(reason) => panic!("{dealer:?}: {reason}"),
            SpanOutcome::Spans(spans) => {
                let mut rows = Vec::new();
                for (span, file_base) in &spans {
                    let seq = span
                        .column_by_name("seq")
                        .expect("seq column")
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("seq is i64");
                    for (i, &v) in seq.values().iter().enumerate() {
                        assert_eq!(
                            v,
                            (*file_base + i as u64) as i64,
                            "{dealer:?}: a span's file_base names its first row"
                        );
                        rows.push(v);
                    }
                }
                batches.push(rows);
            }
        }
        assert!(
            batches.len() <= round_cap,
            "{dealer:?} still dealing after {round_cap} rounds"
        );
    }
}

/// Assert both dealers deal the same batches from `pq`, and that the
/// reference dealt every unskipped row exactly once (so the comparison is not
/// between two empty or truncated sequences). Returns the reference's batches.
fn assert_same_composition(
    what: &str,
    pq: &Path,
    batch_rows: usize,
    read_cursors: usize,
    skip_rows: u64,
) -> Vec<Vec<i64>> {
    let reference = dealt_batches(
        pq,
        batch_rows,
        read_cursors,
        skip_rows,
        Dealer::Pre2613Reference,
    );
    let production = dealt_batches(pq, batch_rows, read_cursors, skip_rows, Dealer::Production);

    let metadata = read_input_metadata(&FileInput { path: pq }).expect("read metadata");
    let total_rows: u64 = row_group_row_counts(&metadata).iter().sum();
    let mut every_row: Vec<i64> = reference.iter().flatten().copied().collect();
    every_row.sort_unstable();
    let expected: Vec<i64> = (skip_rows.min(total_rows) as i64..total_rows as i64).collect();
    assert_eq!(
        every_row, expected,
        "{what}: the reference deals every unskipped row exactly once"
    );

    let sizes = |b: &[Vec<i64>]| b.iter().map(Vec::len).collect::<Vec<_>>();
    assert_eq!(
        sizes(&production),
        sizes(&reference),
        "{what} (batch_rows {batch_rows}, read_cursors {read_cursors}, skip_rows {skip_rows}): \
         batch sizes differ from the pre-#2613 dealer's"
    );
    assert_eq!(
        production, reference,
        "{what} (batch_rows {batch_rows}, read_cursors {read_cursors}, skip_rows {skip_rows}): \
         batch rows differ from the pre-#2613 dealer's"
    );
    reference
}

/// Today's dealer, over cursors decoding `ceil(batch_rows / K)` rows, deals
/// the same sequence of batches, each with the same rows in the same order
/// (zero-row batches included), as the pre-#2613 dealer over cursors
/// decoding `batch_rows` rows.
///
/// Prove-the-test: run against b9f217f97's `cursor_take_spans`, which keeps
/// asking the reader while its share is unfilled, and the (1000, 4) case
/// fails at the second batch (917 rows against 750): the 200-row cursor
/// finds its reader exhausted in round one instead of round two, so the
/// other cursors' shares grow a round early.
#[test]
fn batch_composition_matches_the_pre_2613_dealer() {
    let uneven: &[i64] = &[700, 300, 500, 900, 200];
    // The partitions at K = 4 are 1000, 500, 900 and 200 rows.
    let cases: &[(&str, &[i64], usize, usize, u64)] = &[
        ("uneven row groups", uneven, 1000, 4, 0),
        ("uneven row groups", uneven, 333, 5, 0),
        ("batch_rows not divisible by K", uneven, 1000, 3, 0),
        ("batch_rows not divisible by K", uneven, 450, 7, 0),
        ("batch_rows dividing the file", uneven, 2600, 2, 0),
        (
            "partitions shorter than one share",
            &[1000, 10, 1000, 37],
            900,
            4,
            0,
        ),
        ("no row groups, so one empty partition", &[], 100, 4, 0),
        ("--skip-rows mid-partition", uneven, 1000, 4, 850),
        ("--skip-rows mid-partition", uneven, 333, 5, 1700),
        ("K = 1, batch_rows not dividing the file", uneven, 700, 1, 0),
        ("K = 1, batch_rows dividing the file", uneven, 1300, 1, 0),
    ];
    for &(what, groups, batch_rows, read_cursors, skip_rows) in cases {
        let (_dir, pq, _m) = seq_row_group_fixture(groups);
        let reference = assert_same_composition(what, &pq, batch_rows, read_cursors, skip_rows);
        if (groups, batch_rows, read_cursors, skip_rows) == (uneven, 1000, 4, 0) {
            // The old dealer's sizes as worked out by hand in the #2613
            // review, so the reference is pinned to the dealer it copies.
            let non_empty: Vec<usize> = reference.iter().map(Vec::len).filter(|&n| n > 0).collect();
            assert_eq!(non_empty, vec![950, 750, 667, 233]);
        }
    }
}

proptest! {
    // Each case writes a small Parquet file and deals it twice; 128 cases
    // keep this under a few seconds.
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// [`batch_composition_matches_the_pre_2613_dealer`] over generated files:
    /// row-group count and sizes, batch rows, cursor count and skip.
    #[test]
    fn batch_composition_matches_the_pre_2613_dealer_on_generated_files(
        groups in proptest::collection::vec(1i64..=400, 0..=7),
        batch_rows in 1usize..=1200,
        read_cursors in 1usize..=7,
        skip_per_mille in prop_oneof![Just(0u64), 0u64..=1000],
    ) {
        let total: u64 = groups.iter().map(|&g| g as u64).sum();
        let skip_rows = total * skip_per_mille / 1000;
        let (_dir, pq, _m) = seq_row_group_fixture(&groups);
        assert_same_composition("generated", &pq, batch_rows, read_cursors, skip_rows);
    }
}

/// The RLOG objects a real decode pipeline builds with today's dealer are
/// byte-identical, batch for batch, to the ones the pre-#2613 dealer's
/// batches encode to.
#[tokio::test]
async fn rlog_objects_match_the_pre_2613_dealer() {
    let (_dir, pq, m) = uneven_row_group_fixture();
    for (batch_rows, read_cursors) in [(1000usize, 4usize), (333, 5), (1000, 3), (700, 1)] {
        let mut state = open_for(&pq, batch_rows, read_cursors, Dealer::Pre2613Reference);
        let mut reference = Vec::new();
        loop {
            let spans = match pre_2613_reference_collect_spans(&mut state, batch_rows) {
                SpanOutcome::Done => break,
                SpanOutcome::Failed(reason) => panic!("reference: {reason}"),
                SpanOutcome::Spans(spans) => spans,
            };
            let built = match build_columnar_batch(&spans, &m, &LogIngestLimits::default(), NOW_NS)
            {
                Ok(b) => b,
                Err(_) => panic!("reference batch failed to build"),
            };
            if built.num_rows > 0 {
                reference.push(*blake3::hash(&columnar_object(built)).as_bytes());
            }
        }
        let production =
            decode_object_hashes(&pq, &m, 4, batch_rows, Some(read_cursors), 2, None).await;
        assert!(
            reference.len() >= 2,
            "({batch_rows}, {read_cursors}): the fixture splits into several batches"
        );
        assert_eq!(
            production, reference,
            "({batch_rows}, {read_cursors}): RLOG bytes differ from the pre-#2613 dealer's"
        );
    }
}
