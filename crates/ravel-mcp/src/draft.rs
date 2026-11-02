//! What a SQL statement returns across the port before a tool finishes it
//! (ADR-1374 D4 and D5), and the step that finishes it.
//!
//! [`crate::service::QueryBackend::sql_execute`] returns a [`Draft`]: the
//! result envelope with its data, scope and budget filled in but no cap
//! applied, beside the resolve inputs a D5 cursor pins. Finishing is the
//! tool's step, [`Draft::finish_page`], because only the tool knows its
//! argument hash, whether its ordering is total, and how it encodes a keyset
//! position. The port cannot mint a cursor for it.
//!
//! A tool pages with two calls per page:
//!
//! 1. Page one: `sql_execute`, then [`Draft::finish_page`] with a mint that
//!    builds the cursor from [`PagePins::cursor`] and encodes it with
//!    [`Cursor::mint`].
//! 2. A later page: [`Cursor::redeem`], then `sql_execute` at the pending
//!    redemption's `resolve_at_ns` over its pinned window and tokens, then
//!    [`crate::cursor::PendingRedemption::confirm`] with what that resolve
//!    observed, then [`Draft::pinned_to`] with the confirmed cursor, then
//!    [`Draft::finish_page`] with [`PinScope::of`] that cursor.
//!
//! [`ExplainOutcome`] is the explain operation's answer in this crate's own
//! types, so no arrow or DataFusion type crosses the port.

use ravel_query::erasure::ErasurePredicate;
use ravel_sql::{DeclaredColumn, TargetSignal};
use ravel_types::accounting::CostEstimate;
use ravel_types::{CommitToken, Signal, TenantHash};

use crate::budget::McpEffectiveBudgets;
use crate::cursor::{self, Cursor, CursorError, CursorPosition};
use crate::envelope::{CURSOR_BOUND, Envelope, Row, serialized_str_len};

/// Why a statement's result cannot carry a cursor. A cursor pins exactly one
/// signal snapshot (D5), so every other shape of statement is refused here
/// and its capped result is `ok_bounded`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NoCursorSignal {
    /// The statement reads no table, so there is no snapshot to pin.
    #[error("the statement reads no table, so there is no snapshot to pin")]
    NoTable,
    /// The statement reads only Parquet tables, which resolve from their
    /// manifests and not from a signal snapshot.
    #[error("the statement reads only Parquet tables, which no cursor pin covers")]
    Parquet,
    /// The statement joins Parquet tables with signal tables. The signal
    /// lanes could be pinned, the Parquet manifests could not.
    #[error("the statement reads Parquet tables, which no cursor pin covers")]
    ParquetJoin,
    /// The statement reads more than one signal table.
    #[error("the statement reads {count} signal tables, and a cursor pins one")]
    SeveralSignals { count: usize },
}

/// The signal a cursor over this statement's result pins, from the outcome's
/// target, its count of signal lanes, and whether it read a Parquet table.
pub fn cursor_signal(
    target: TargetSignal,
    signal_lanes: usize,
    reads_parquet: bool,
) -> Result<Signal, NoCursorSignal> {
    if reads_parquet {
        return Err(if signal_lanes == 0 {
            NoCursorSignal::Parquet
        } else {
            NoCursorSignal::ParquetJoin
        });
    }
    match signal_lanes {
        0 => return Err(NoCursorSignal::NoTable),
        1 => {}
        count => return Err(NoCursorSignal::SeveralSignals { count }),
    }
    match target {
        TargetSignal::Metrics => Ok(Signal::Metrics),
        TargetSignal::Logs => Ok(Signal::Logs),
        TargetSignal::Spans => Ok(Signal::Spans),
        TargetSignal::Alerts => Ok(Signal::Alerts),
        TargetSignal::Audit => Ok(Signal::Audit),
        TargetSignal::Parquet => Err(NoCursorSignal::Parquet),
    }
}

/// An executed SQL statement whose envelope is not finished yet, with the
/// inputs a cursor over it pins.
#[derive(Debug, Clone)]
pub struct Draft {
    /// No cap has run on it. `data.rows` is every row the executor returned,
    /// `data.row_count` their count, and `presentation.row_cap_hit` the
    /// executor's own cap.
    pub envelope: Envelope,
    /// The one signal the statement resolved, or why it has none to pin.
    pub signal: Result<Signal, NoCursorSignal>,
    /// The pending erasure predicates of the snapshot the rows came from.
    pub pending_erasure: Vec<ErasurePredicate>,
    /// The declared column set of that snapshot.
    pub declared_columns: Vec<DeclaredColumn>,
    /// The greatest `ingest_hour_bucket` among the segments that snapshot
    /// resolved, in whole hours since the epoch; `None` when it resolved no
    /// segment.
    pub ingest_watermark_hour: Option<u32>,
    /// The instant the snapshot was resolved at.
    pub resolve_at_ns: i64,
}

impl Draft {
    /// A draft over `envelope`, with `visibility.ingest_watermark_hour`
    /// written from `ingest_watermark_hour`. A statement that resolved signal
    /// lanes and got no segments says so through
    /// `visibility.resolved_no_segments`; one that read no signal table never
    /// measured the field and leaves both unset.
    pub fn new(
        envelope: Envelope,
        signal: Result<Signal, NoCursorSignal>,
        pending_erasure: Vec<ErasurePredicate>,
        declared_columns: Vec<DeclaredColumn>,
        ingest_watermark_hour: Option<u32>,
        resolve_at_ns: i64,
    ) -> Draft {
        let mut draft = Draft {
            envelope,
            signal,
            pending_erasure,
            declared_columns,
            ingest_watermark_hour,
            resolve_at_ns,
        };
        draft.report_watermark();
        draft
    }

    fn report_watermark(&mut self) {
        let measured = !matches!(
            self.signal,
            Err(NoCursorSignal::NoTable | NoCursorSignal::Parquet)
        );
        let visibility = &mut self.envelope.visibility;
        match self.ingest_watermark_hour {
            Some(hour) => {
                visibility.ingest_watermark_hour = hour.to_string();
                visibility.resolved_no_segments = false;
            }
            None => {
                visibility.ingest_watermark_hour.clear();
                visibility.resolved_no_segments = measured;
            }
        }
    }

    /// The draft of a later page, reporting the pins `cursor` carries rather
    /// than what this resolve observed. `confirm` has already established
    /// that the observed erasure and declared columns did not move; the
    /// watermark and the resolve instant are page one's by definition, so
    /// every page of one sequence reports one snapshot id and one watermark.
    pub fn pinned_to(mut self, cursor: &Cursor) -> Draft {
        self.signal = Ok(cursor.signal);
        self.pending_erasure = cursor.pending_erasure.clone();
        self.declared_columns = cursor.declared_columns.clone();
        self.ingest_watermark_hour = cursor.ingest_watermark_hour;
        self.resolve_at_ns = cursor.mint_ns;
        self.envelope.visibility.pinned = true;
        self.report_watermark();
        self
    }

    /// Apply the row cap and the byte cap, mint a cursor when a cap stopped
    /// the result and the order allows one, and resolve the status.
    ///
    /// `visibility.snapshot_id` is set from [`cursor::snapshot_id`] over the
    /// draft's pins and `scope` whenever the draft has a signal, capped or
    /// not. `presentation.rows_omitted` counts every row the caller does not
    /// get: the rows over `budgets.max_rows`, the rows the byte cap dropped,
    /// and a trailing group [`PageOrder::Grouped`] held back.
    ///
    /// The envelope is fitted with a stand-in cursor exactly [`CURSOR_BOUND`]
    /// serialized bytes long before `mint` runs, so the token, which is never
    /// longer, cannot push it over the cap. `mint` gets the pins and the last
    /// row the page keeps, uncut by the byte cap, to build the position from.
    /// A token over [`CURSOR_BOUND`] is an `internal` failure, as
    /// [`Envelope::fit`] states.
    ///
    /// No cursor is minted, and a capped result is `ok_bounded`, when the
    /// order is [`PageOrder::Partial`], when the draft has no signal, when no
    /// complete group fits a grouped page, or when `mint` refuses. Each case
    /// but the first carries a `no cursor:` warning naming the reason; the
    /// first is what every tool without a total order already reports.
    /// Under both caps nothing is minted and the status is `ok`.
    pub fn finish_page<M>(
        self,
        budgets: &McpEffectiveBudgets,
        scope: &PinScope,
        order: PageOrder<'_>,
        mint: M,
    ) -> Envelope
    where
        M: FnOnce(&PagePins<'_>, &Row) -> Result<String, CursorError>,
    {
        let Draft {
            mut envelope,
            signal,
            pending_erasure,
            declared_columns,
            ingest_watermark_hour,
            resolve_at_ns,
        } = self;
        let pins = signal.map(|signal| PagePins {
            signal,
            scope,
            pending_erasure: &pending_erasure,
            declared_columns: &declared_columns,
            ingest_watermark_hour,
            resolve_at_ns,
        });
        if let Ok(pins) = &pins {
            envelope.visibility.snapshot_id = hex(&pins.snapshot_id());
        }

        let produced_rows = envelope.data.row_count;
        let max_rows = budgets.max_rows as usize;
        let pristine = envelope.data.rows.clone();
        let row_cap_omitted = pristine.len().saturating_sub(max_rows) as u64;
        envelope.data.rows.truncate(max_rows);
        envelope.presentation.row_cap_hit |= row_cap_omitted > 0;
        envelope.presentation.cursor = None;

        let cap = budgets.max_response_bytes;
        let finish = |mut fitted: Envelope, has_total_order: bool| {
            fitted.presentation.rows_omitted += row_cap_omitted;
            fitted.data.row_count = produced_rows;
            fitted.finish(has_total_order)
        };
        let unpaged = |mut envelope: Envelope, reason: String| {
            envelope.warnings.insert(0, format!("no cursor: {reason}"));
            finish(envelope.fit(cap), false)
        };

        let probe = envelope.clone().fit(cap);
        let presentation = &probe.presentation;
        let capped = presentation.row_cap_hit
            || (presentation.bytes_cap_hit
                && (presentation.rows_omitted > 0 || presentation.cells_truncated > 0));
        if !capped || probe.failure.is_some() || matches!(order, PageOrder::Partial) {
            return finish(probe, false);
        }
        let pins = match pins {
            Ok(pins) => pins,
            Err(reason) => return unpaged(envelope, reason.to_string()),
        };

        let mut placed = envelope.clone();
        placed.presentation.cursor = Some("c".repeat(CURSOR_BOUND - 2));
        let mut fitted = placed.fit(cap);
        let mut kept = fitted.data.rows.len();
        if let PageOrder::Grouped(same_group) = order {
            kept = complete_groups(&pristine, kept, same_group);
            let held_back = fitted.data.rows.len() - kept;
            fitted.data.rows.truncate(kept);
            fitted.presentation.rows_omitted += held_back as u64;
        }
        let Some(last) = kept.checked_sub(1).and_then(|index| pristine.get(index)) else {
            return unpaged(
                envelope,
                "no complete group of rows equal under the order fits one page".to_string(),
            );
        };

        match mint(&pins, last) {
            Ok(token) => {
                let over_bound = serialized_str_len(&token) > CURSOR_BOUND;
                fitted.presentation.cursor = Some(token);
                if over_bound {
                    fitted = fitted.fit(cap);
                }
                finish(fitted, true)
            }
            Err(error) => unpaged(envelope, error.to_string()),
        }
    }
}

/// How many of the first `kept` rows form complete groups: a group still
/// open at the page's end (its next row unknown, or equal to the page's last
/// row) is held back whole, so a cursor taken after the page's last row
/// cannot skip the rest of it.
fn complete_groups(rows: &[Row], kept: usize, same_group: &dyn Fn(&Row, &Row) -> bool) -> usize {
    let Some(last) = kept.checked_sub(1) else {
        return 0;
    };
    let closed = rows
        .get(kept)
        .is_some_and(|next| !same_group(&rows[last], next));
    if closed {
        return kept;
    }
    let mut start = last;
    while start > 0 && same_group(&rows[start - 1], &rows[last]) {
        start -= 1;
    }
    start
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// How a statement's rows are ordered, which decides whether a page may
/// carry a cursor.
#[derive(Clone, Copy)]
pub enum PageOrder<'a> {
    /// No total order: a capped result is `ok_bounded` and nothing is minted.
    Partial,
    /// The order plus its tiebreak is total, so a cursor after any row
    /// resumes exactly after it.
    Total,
    /// A cursor resumes after a whole group of rows equal under the order,
    /// never inside one. The function says whether two rows are in the same
    /// group.
    Grouped(&'a dyn Fn(&Row, &Row) -> bool),
}

/// The pin inputs the tool knows and the statement's outcome does not: who
/// resolved, over which half-open range, against which commit tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinScope {
    pub tenant: TenantHash,
    /// Inclusive.
    pub range_start_ns: i64,
    /// Exclusive.
    pub range_end_ns: i64,
    pub min_commit_watermark: Vec<CommitToken>,
}

impl PinScope {
    /// The scope a redeemed cursor pinned, for a later page.
    pub fn of(cursor: &Cursor) -> PinScope {
        PinScope {
            tenant: cursor.tenant,
            range_start_ns: cursor.range_start_ns,
            range_end_ns: cursor.range_end_ns,
            min_commit_watermark: cursor.min_commit_watermark.clone(),
        }
    }
}

/// Every resolve input a cursor over one page pins.
#[derive(Debug, Clone, Copy)]
pub struct PagePins<'a> {
    pub signal: Signal,
    pub scope: &'a PinScope,
    pub pending_erasure: &'a [ErasurePredicate],
    pub declared_columns: &'a [DeclaredColumn],
    pub ingest_watermark_hour: Option<u32>,
    pub resolve_at_ns: i64,
}

impl PagePins<'_> {
    /// [`cursor::snapshot_id`] over these pins.
    pub fn snapshot_id(&self) -> [u8; 32] {
        cursor::snapshot_id(
            self.scope.tenant,
            self.signal,
            self.scope.range_start_ns,
            self.scope.range_end_ns,
            &self.scope.min_commit_watermark,
            self.pending_erasure,
            self.declared_columns,
            self.resolve_at_ns,
        )
    }

    /// The cursor these pins describe, minted at their resolve instant.
    pub fn cursor(
        &self,
        tool: &str,
        argument_hash: [u8; 32],
        position: CursorPosition,
        deadline_ns: i64,
    ) -> Cursor {
        Cursor {
            tenant: self.scope.tenant,
            tool: tool.to_owned(),
            argument_hash,
            signal: self.signal,
            range_start_ns: self.scope.range_start_ns,
            range_end_ns: self.scope.range_end_ns,
            min_commit_watermark: self.scope.min_commit_watermark.clone(),
            pending_erasure: self.pending_erasure.to_vec(),
            declared_columns: self.declared_columns.to_vec(),
            ingest_watermark_hour: self.ingest_watermark_hour,
            position,
            mint_ns: self.resolve_at_ns,
            deadline_ns,
        }
    }
}

/// One [`CostEstimate`] component the estimator cannot bound for a
/// statement, so its figure is a structural zero and not an estimate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EstimateComponent {
    Requests,
    StoreBytes,
    DecompressedBytes,
}

impl EstimateComponent {
    /// The [`CostEstimate`] field this component is.
    pub fn field_name(self) -> &'static str {
        match self {
            EstimateComponent::Requests => "estimated_requests",
            EstimateComponent::StoreBytes => "estimated_store_bytes",
            EstimateComponent::DecompressedBytes => "estimated_decompressed_bytes",
        }
    }

    /// The component a [`CostEstimate`] field name names, if any.
    pub fn from_field_name(name: &str) -> Option<EstimateComponent> {
        [
            EstimateComponent::Requests,
            EstimateComponent::StoreBytes,
            EstimateComponent::DecompressedBytes,
        ]
        .into_iter()
        .find(|component| component.field_name() == name)
    }
}

/// One column of a statement's effective result schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainColumn {
    pub name: String,
    /// The column's type as its display text.
    pub data_type: String,
}

/// What explaining a statement says, without executing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainOutcome {
    pub target: TargetSignal,
    pub plan_text: String,
    pub window_predicate: Option<String>,
    /// Segments the resolve returned.
    pub segments_resolved: u64,
    pub segments_admitted: u64,
    pub segments_recent_exempt: u64,
    pub estimate: CostEstimate,
    /// The components of `estimate` that bound nothing.
    pub unbounded: Vec<EstimateComponent>,
    /// The effective result schema, in column order.
    pub columns: Vec<ExplainColumn>,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_query::EngineConfig;
    use ravel_sql::DeclaredType;
    use uuid::Uuid;

    use super::*;
    use crate::budget::{McpBudgetConfig, McpRequestBudgets};
    use crate::cursor::{CURSOR_KEY_LEN, CursorKey, GRACE_NS};
    use crate::envelope::{Cell, Column, MAX_RESPONSE_BYTES_FLOOR, Status};

    const TOOL: &str = "ravel_query_sql";
    const ARGS: [u8; 32] = [3u8; 32];
    const NOW_NS: i64 = 1_700_000_000_000_000_000;
    const HORIZON_NS: i64 = NOW_NS + GRACE_NS + 3_600 * 1_000_000_000;

    fn key() -> CursorKey {
        CursorKey::from_process_secret([0x5Au8; CURSOR_KEY_LEN])
    }

    fn budgets(max_rows: u32) -> McpEffectiveBudgets {
        let mut budgets = McpRequestBudgets::default()
            .clamp(&EngineConfig::default(), &McpBudgetConfig::default());
        budgets.max_rows = max_rows;
        budgets.max_response_bytes = MAX_RESPONSE_BYTES_FLOOR;
        budgets
    }

    fn scope() -> PinScope {
        PinScope {
            tenant: TenantHash([0xC1u8; 16]),
            range_start_ns: NOW_NS - 3_600 * 1_000_000_000,
            range_end_ns: NOW_NS,
            min_commit_watermark: vec![CommitToken {
                shard: 0,
                writer_id: Uuid::from_bytes([4u8; 16]),
                epoch: 1,
                seq: 2,
                ingest_hour_bucket: 472_222,
            }],
        }
    }

    /// `rows` rows of one `Int` key column and one `Str` column `width`
    /// bytes wide, keyed `key(i)`.
    fn draft(rows: usize, width: usize, key: impl Fn(usize) -> i128) -> Draft {
        let mut envelope = Envelope::default();
        envelope.ids.query_id = "q".to_owned();
        envelope.ids.audit_ref = "a".to_owned();
        envelope.data.columns = vec![
            Column {
                name: "k".to_owned(),
                r#type: "int".to_owned(),
            },
            Column {
                name: "body".to_owned(),
                r#type: "string".to_owned(),
            },
        ];
        envelope.data.rows = (0..rows)
            .map(|i| vec![Cell::Int(key(i)), Cell::Str("x".repeat(width))])
            .collect();
        envelope.data.row_count = rows as u64;
        Draft::new(
            envelope,
            Ok(Signal::Logs),
            vec![ErasurePredicate::windowless(vec![(
                "region".to_owned(),
                "us-east".to_owned(),
            )])],
            vec![DeclaredColumn::new("http.status_code", DeclaredType::I64)],
            Some(472_223),
            NOW_NS,
        )
    }

    fn key_of(row: &Row) -> i128 {
        match row.first() {
            Some(Cell::Int(key)) => *key,
            other => panic!("not a key cell: {other:?}"),
        }
    }

    /// A mint that encodes a keyset cursor after `row`, the way a paging
    /// tool does.
    fn token_mint(pins: &PagePins<'_>, row: &Row) -> Result<String, CursorError> {
        pins.cursor(
            TOOL,
            ARGS,
            CursorPosition::Keyset {
                tuple: key_of(row).to_le_bytes().to_vec(),
                order_by: vec!["k".to_owned()],
            },
            i64::MAX,
        )
        .mint(&key(), NOW_NS, HORIZON_NS)
    }

    fn serialized_len(envelope: &Envelope) -> u64 {
        serde_json::to_vec(envelope).expect("serializes").len() as u64
    }

    /// 50 rows of 40 KiB under a 10-row cap and the 256 KiB floor: the row
    /// cap drops 40, the byte cap drops more of the 10 left, and the page is
    /// `ok_page` with a cursor after its last row, inside the cap with the
    /// cursor in it, and `rows_omitted` counts both caps' rows.
    ///
    /// FLIP: folding only the byte cap's count (as `Envelope::fit` reports
    /// it) leaves out the 40 rows over the row cap.
    /// FLIP: a finish that applies only the row cap keeps all 10 rows, about
    /// 400 KiB, over the cap.
    /// FLIP: minting after the first or the pristine tenth row instead of the
    /// page's last row decodes to another position.
    #[test]
    fn over_both_caps_a_minted_page_is_ok_page_inside_the_cap() {
        let width = 40 * 1024;
        let draft = draft(50, width, |i| i as i128);
        let budgets = budgets(10);

        let page = draft.finish_page(&budgets, &scope(), PageOrder::Total, token_mint);

        assert_eq!(page.status, Status::OkPage);
        assert!(page.presentation.row_cap_hit);
        assert!(page.presentation.bytes_cap_hit);
        let kept = page.data.rows.len();
        assert!(
            0 < kept && kept < 10,
            "the byte cap must drop rows too: {kept}"
        );
        assert_eq!(page.presentation.rows_omitted, (50 - kept) as u64);
        assert_eq!(page.data.row_count, 50);
        assert!(serialized_len(&page) <= budgets.max_response_bytes);

        let token = page.presentation.cursor.as_deref().expect("a cursor");
        let cursor = Cursor::decode(token, &key()).expect("decodes");
        assert_eq!(
            cursor.position,
            CursorPosition::Keyset {
                tuple: ((kept - 1) as i128).to_le_bytes().to_vec(),
                order_by: vec!["k".to_owned()],
            }
        );
        assert_eq!(cursor.mint_ns, NOW_NS);
        assert_eq!(cursor.ingest_watermark_hour, Some(472_223));
    }

    /// A mint that refuses leaves the page without a cursor, `ok_bounded`,
    /// with a warning that says why; so does a draft with no signal, and a
    /// `Partial` order, which says nothing because no tool without a total
    /// order ever has.
    ///
    /// FLIP: keeping `ok_page` (or a stale stand-in cursor) when the mint
    /// fails hands the caller a page it cannot turn.
    /// FLIP: dropping the warning makes the refusal indistinguishable from a
    /// statement without a total order.
    #[test]
    fn a_refusing_mint_gives_no_cursor_and_says_why() {
        let budgets = budgets(3);
        let refused =
            draft(5, 8, |i| i as i128).finish_page(&budgets, &scope(), PageOrder::Total, |_, _| {
                Err(CursorError::Expired)
            });
        assert_eq!(refused.status, Status::OkBounded);
        assert_eq!(refused.presentation.cursor, None);
        assert_eq!(refused.presentation.rows_omitted, 2);
        assert!(
            refused
                .warnings
                .contains(&"no cursor: cursor has expired".to_owned()),
            "{:?}",
            refused.warnings
        );

        let mut parquet = draft(5, 8, |i| i as i128);
        parquet.signal = Err(NoCursorSignal::Parquet);
        let parquet = parquet.finish_page(&budgets, &scope(), PageOrder::Total, |_, _| {
            panic!("a draft with no signal must not reach the mint")
        });
        assert_eq!(parquet.status, Status::OkBounded);
        assert!(parquet.visibility.snapshot_id.is_empty());
        assert!(
            parquet
                .warnings
                .contains(&format!("no cursor: {}", NoCursorSignal::Parquet))
        );

        let partial = draft(5, 8, |i| i as i128).finish_page(
            &budgets,
            &scope(),
            PageOrder::Partial,
            |_, _| panic!("a partial order must not reach the mint"),
        );
        assert_eq!(partial.status, Status::OkBounded);
        assert!(!partial.warnings.iter().any(|w| w.starts_with("no cursor")));
    }

    /// Under both caps the result is complete: `ok`, no cursor, and the mint
    /// is never called.
    ///
    /// FLIP: minting on every page with a total order calls the mint and
    /// panics here.
    #[test]
    fn under_both_caps_nothing_is_minted_and_the_status_is_ok() {
        let page = draft(3, 8, |i| i as i128).finish_page(
            &budgets(3),
            &scope(),
            PageOrder::Total,
            |_, _| panic!("an uncapped page must not mint"),
        );
        assert_eq!(page.status, Status::Ok);
        assert_eq!(page.presentation.cursor, None);
        assert_eq!(page.presentation.rows_omitted, 0);
        assert_eq!(page.data.rows.len(), 3);
    }

    /// Page one's `snapshot_id` is `cursor::snapshot_id` over its pins, and
    /// page two, resolved later and observing its erasure and declared
    /// columns in another order, reports the same id and page one's
    /// watermark once pinned to the cursor.
    ///
    /// FLIP: deriving page two's id from its own resolve instant (or its own
    /// watermark) instead of the cursor's changes it.
    /// FLIP: hashing anything but `cursor::snapshot_id`'s inputs fails the
    /// first equality.
    #[test]
    fn every_page_reports_page_ones_snapshot_id() {
        let budgets = budgets(2);
        let first = draft(5, 8, |i| i as i128);
        let expected = cursor::snapshot_id(
            scope().tenant,
            Signal::Logs,
            scope().range_start_ns,
            scope().range_end_ns,
            &scope().min_commit_watermark,
            &first.pending_erasure,
            &first.declared_columns,
            NOW_NS,
        );
        let page_one = first.finish_page(&budgets, &scope(), PageOrder::Total, token_mint);
        assert_eq!(page_one.status, Status::OkPage);
        assert_eq!(page_one.visibility.snapshot_id, hex(&expected));
        assert_eq!(page_one.visibility.ingest_watermark_hour, "472223");
        assert!(!page_one.visibility.pinned);

        let token = page_one.presentation.cursor.as_deref().expect("a cursor");
        let pending = Cursor::redeem(
            token,
            &key(),
            scope().tenant,
            TOOL,
            &ARGS,
            NOW_NS + 1,
            NOW_NS + 1 + GRACE_NS + 3_600 * 1_000_000_000,
        )
        .expect("stage one");
        let mut second = draft(5, 8, |i| i as i128 + 2);
        second.resolve_at_ns = pending.resolve_at_ns() + 7;
        second.ingest_watermark_hour = Some(472_299);
        second
            .declared_columns
            .push(DeclaredColumn::new("a", DeclaredType::Str));
        second.declared_columns.reverse();
        let observed = second.declared_columns.clone();
        let cursor = pending
            .confirm(NOW_NS + 2, &[], &[observed[1].clone(), observed[0].clone()])
            .expect_err("an added declared column expires the cursor");
        assert_eq!(cursor, CursorError::Expired);

        let pending = Cursor::redeem(
            token,
            &key(),
            scope().tenant,
            TOOL,
            &ARGS,
            NOW_NS + 1,
            NOW_NS + 1 + GRACE_NS + 3_600 * 1_000_000_000,
        )
        .expect("stage one");
        let mut second = draft(5, 8, |i| i as i128 + 2);
        second.resolve_at_ns = pending.resolve_at_ns() + 7;
        second.ingest_watermark_hour = Some(472_299);
        let confirmed = pending
            .confirm(NOW_NS + 2, &[], &second.declared_columns)
            .expect("nothing moved");
        let page_two = second.pinned_to(&confirmed).finish_page(
            &budgets,
            &PinScope::of(&confirmed),
            PageOrder::Total,
            token_mint,
        );
        assert_eq!(
            page_two.visibility.snapshot_id,
            page_one.visibility.snapshot_id
        );
        assert_eq!(page_two.visibility.ingest_watermark_hour, "472223");
        assert!(page_two.visibility.pinned);
        let next = Cursor::decode(
            page_two.presentation.cursor.as_deref().expect("a cursor"),
            &key(),
        )
        .expect("decodes");
        assert_eq!(
            next.mint_ns, NOW_NS,
            "page three resolves at page one's instant"
        );
    }

    /// A grouped order never ends a page inside a group: rows keyed
    /// 0,0,1,1,1,2 under a 4-row cap keep the complete group 0,0 and hold
    /// back 1,1, which the next page returns whole. A group still open with
    /// no row after it to close it is held back too, and a page with no
    /// complete group gets no cursor.
    ///
    /// FLIP: a `Total` treatment mints after the second 1 and the next page,
    /// resuming after key 1, skips the third.
    /// FLIP: leaving the held-back rows out of `rows_omitted` under-reports
    /// what the caller did not get.
    #[test]
    fn a_grouped_page_holds_back_its_open_trailing_group() {
        let keys = [0, 0, 1, 1, 1, 2];
        let same = |a: &Row, b: &Row| key_of(a) == key_of(b);
        let page = draft(keys.len(), 8, |i| keys[i]).finish_page(
            &budgets(4),
            &scope(),
            PageOrder::Grouped(&same),
            token_mint,
        );
        assert_eq!(page.status, Status::OkPage);
        assert_eq!(
            page.data.rows.iter().map(key_of).collect::<Vec<_>>(),
            [0, 0]
        );
        assert_eq!(page.presentation.rows_omitted, 4);
        let cursor = Cursor::decode(page.presentation.cursor.as_deref().expect("cursor"), &key())
            .expect("decodes");
        assert_eq!(
            cursor.position,
            CursorPosition::Keyset {
                tuple: 0i128.to_le_bytes().to_vec(),
                order_by: vec!["k".to_owned()],
            }
        );

        // The executor's own cap stopped the stream with no row after the
        // page, so whether the last group is complete is unknown.
        let mut open = draft(4, 8, |i| [0, 0, 1, 1][i]);
        open.envelope.presentation.row_cap_hit = true;
        let page = open.finish_page(&budgets(4), &scope(), PageOrder::Grouped(&same), token_mint);
        assert_eq!(
            page.data.rows.iter().map(key_of).collect::<Vec<_>>(),
            [0, 0]
        );
        assert_eq!(page.presentation.rows_omitted, 2);
        assert_eq!(page.status, Status::OkPage);

        let one_group = draft(5, 8, |_| 7).finish_page(
            &budgets(4),
            &scope(),
            PageOrder::Grouped(&same),
            |_, _| panic!("no complete group, nothing to mint after"),
        );
        assert_eq!(one_group.status, Status::OkBounded);
        assert_eq!(one_group.presentation.cursor, None);
        assert_eq!(one_group.data.rows.len(), 4);
        assert_eq!(one_group.presentation.rows_omitted, 1);
    }

    /// A token over the cursor bound is the `internal` failure `fit` states,
    /// never a page with a cut token.
    ///
    /// FLIP: setting the token without re-running the bound check returns
    /// `ok_page` with a 5000-byte cursor.
    #[test]
    fn a_token_over_the_cursor_bound_is_internal() {
        let page = draft(5, 8, |i| i as i128).finish_page(
            &budgets(2),
            &scope(),
            PageOrder::Total,
            |_, _| Ok("t".repeat(5000)),
        );
        assert_eq!(page.status, Status::Error);
        assert_eq!(page.presentation.cursor, None);
        assert_eq!(
            page.failure.map(|failure| failure.class),
            Some(crate::envelope::FailureClass::Internal)
        );
    }

    /// The pinned signal of each statement shape, and the watermark wording
    /// it gets: a signal statement with no segments says it resolved none, a
    /// Parquet one says nothing because it never measured.
    ///
    /// FLIP: treating a Parquet join as its signal lane pins a cursor over a
    /// snapshot that does not cover the Parquet rows.
    /// FLIP: setting `resolved_no_segments` for a Parquet-only statement
    /// claims a resolve that never ran.
    #[test]
    fn cursor_signal_refuses_every_shape_but_one_signal() {
        assert_eq!(
            cursor_signal(TargetSignal::Spans, 1, false),
            Ok(Signal::Spans)
        );
        assert_eq!(
            cursor_signal(TargetSignal::Parquet, 0, true),
            Err(NoCursorSignal::Parquet)
        );
        assert_eq!(
            cursor_signal(TargetSignal::Logs, 1, true),
            Err(NoCursorSignal::ParquetJoin)
        );
        assert_eq!(
            cursor_signal(TargetSignal::Logs, 2, false),
            Err(NoCursorSignal::SeveralSignals { count: 2 })
        );
        assert_eq!(
            cursor_signal(TargetSignal::Metrics, 0, false),
            Err(NoCursorSignal::NoTable)
        );

        let empty = Draft::new(
            Envelope::default(),
            Ok(Signal::Logs),
            vec![],
            vec![],
            None,
            0,
        );
        assert!(empty.envelope.visibility.resolved_no_segments);
        let parquet = Draft::new(
            Envelope::default(),
            Err(NoCursorSignal::Parquet),
            vec![],
            vec![],
            None,
            0,
        );
        assert!(!parquet.envelope.visibility.resolved_no_segments);
        assert!(parquet.envelope.visibility.ingest_watermark_hour.is_empty());
    }

    /// Each component names its own `CostEstimate` field and reads back.
    ///
    /// FLIP: a swapped pair of names fails the round trip of both.
    #[test]
    fn estimate_components_round_trip_their_field_names() {
        for component in [
            EstimateComponent::Requests,
            EstimateComponent::StoreBytes,
            EstimateComponent::DecompressedBytes,
        ] {
            assert_eq!(
                EstimateComponent::from_field_name(component.field_name()),
                Some(component)
            );
        }
        assert_eq!(EstimateComponent::from_field_name("segments"), None);
    }
}
