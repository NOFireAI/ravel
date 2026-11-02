//! `ravel_describe_data`: one signal's effective schema, indexed keys,
//! committed row count, coverage window, freshness watermark, and its metric
//! families, a page of 100 at a time.
//!
//! The family list pages with a D5 keyset cursor. The position is the last
//! served family name's bytes under `order_by: ["name"]`, and a later page
//! asks [`QueryBackend::describe_data`] for the families whose names sort
//! after it in byte order, the order the service sorts the record in. The
//! cursor pins the resolve inputs of page one, so a later page resolves at
//! page one's instant and reports page one's watermark and snapshot id; the
//! redemption is refused when a newer erasure on the signal or a changed
//! declared column set has moved what that pin describes.
//!
//! The family list is not pinned: the metadata record is read fresh for each
//! page. A family added that sorts after the position appears on a later
//! page, one added that sorts before it does not, and `families_total` is the
//! count of the read that served the page, so it can differ between pages.
//!
//! Page one carries a `table` row (signal, segments, committed rows, coverage
//! window, family total), one `column` row per fixed and declared column, one
//! `indexed_key` row per key, then the `family` rows. A later page carries
//! `family` rows only.
//!
//! `budget.actual` counts the catalog resolve only. The metadata record read
//! and the tenant configuration read are not in the service's accounting,
//! and every page says so in `warnings`.

use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::catalog::DescribeDataInput;
use crate::cursor::{Cursor, CursorError, CursorPosition};
use crate::draft::{Draft, PageOrder, PinScope};
use crate::envelope::{
    AnyJson, Budget, Cell, Column, Envelope, Failure, FailureClass, Row, TimeRange,
};
use crate::service::{DescribeOutcome, DescribeRequest, QueryBackend};
use ravel_types::Signal;

use super::ToolContext;
use super::capabilities::refused;

/// The catalog name, which a cursor is bound to.
const TOOL: &str = "ravel_describe_data";

/// Families per page: the service's own page bound.
pub const FAMILIES_PER_PAGE: usize = 100;

/// The keyset order the position is taken under.
const ORDER_BY: &str = "name";

/// The BLAKE3 derive-key context of the cursor's argument hash.
const ARGUMENT_HASH_CONTEXT: &str = "ravel-mcp ravel_describe_data arguments v1";

/// The warning every page carries about what `budget.actual` leaves out.
pub const UNCOUNTED_READS_WARNING: &str = "budget.actual counts the catalog resolve only; \
     the metric metadata record and tenant configuration reads are not counted";

/// Run the tool. An argument the body cannot use, an unknown signal, or a
/// cursor that does not redeem is a failure envelope that reaches no store.
pub async fn run<B: QueryBackend>(args: Value, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let input = match DescribeDataInput::deserialize(args) {
        Ok(input) => input,
        Err(err) => return refused(invalid(format!("{TOOL} arguments: {err}")), ctx),
    };
    let signal = match parse_signal(&input.signal) {
        Ok(signal) => signal,
        Err(failure) => return refused(failure, ctx),
    };
    let argument_hash = argument_hash(&input.signal);
    match input.cursor {
        None => first_page(signal, &input.signal, argument_hash, ctx, backend).await,
        Some(token) => {
            later_page(signal, &input.signal, &token, argument_hash, ctx, backend).await
        }
    }
}

async fn first_page<B: QueryBackend>(
    signal: Signal,
    name: &str,
    argument_hash: [u8; 32],
    ctx: &ToolContext<'_>,
    backend: &B,
) -> Envelope {
    let request = DescribeRequest {
        signal,
        after_name: None,
        limit: FAMILIES_PER_PAGE,
        now_ns: ctx.now_ns,
    };
    let outcome = match backend.describe_data(ctx.tenant_hash, &request).await {
        Ok(outcome) => outcome,
        Err(failure) => return refused(failure, ctx),
    };
    let mut rows = vec![table_row(name, &outcome)];
    rows.extend(column_rows(&outcome));
    rows.extend(indexed_key_rows(&outcome));
    rows.extend(family_rows(&outcome));
    let draft = Draft::new(
        base(name, &outcome, rows, ctx),
        Ok(signal),
        outcome.pending_erasure.clone(),
        outcome.declared_column_set.clone(),
        outcome.ingest_watermark_hour,
        outcome.resolved_at_ns,
    );
    let scope = PinScope {
        tenant: ctx.tenant_hash,
        range_start_ns: 0,
        range_end_ns: i64::MAX,
        min_commit_watermark: Vec::new(),
    };
    finish(draft, &scope, argument_hash, ctx)
}

async fn later_page<B: QueryBackend>(
    signal: Signal,
    name: &str,
    token: &str,
    argument_hash: [u8; 32],
    ctx: &ToolContext<'_>,
    backend: &B,
) -> Envelope {
    let pending = match Cursor::redeem(
        token,
        ctx.cursor_key,
        ctx.tenant_hash,
        TOOL,
        &argument_hash,
        ctx.now_ns,
        ctx.protection_horizon_ns,
    ) {
        Ok(pending) => pending,
        Err(error) => return refused(error.failure(), ctx),
    };
    let after_name = match after_name(pending.position()) {
        Ok(after_name) => after_name,
        Err(error) => return refused(error.failure(), ctx),
    };
    let request = DescribeRequest {
        signal,
        after_name,
        limit: FAMILIES_PER_PAGE,
        now_ns: pending.resolve_at_ns(),
    };
    let outcome = match backend.describe_data(ctx.tenant_hash, &request).await {
        Ok(outcome) => outcome,
        Err(failure) => return refused(failure, ctx),
    };
    let observed_erasure: Vec<(Signal, _)> = outcome
        .pending_erasure
        .iter()
        .map(|predicate| (signal, predicate.clone()))
        .collect();
    let confirmed = match pending.confirm(
        ctx.now_ns,
        &observed_erasure,
        &outcome.declared_column_set,
    ) {
        Ok(confirmed) => confirmed,
        Err(error) => return refused(error.failure(), ctx),
    };

    let mut envelope = base(name, &outcome, family_rows(&outcome), ctx);
    envelope.warnings.push(format!(
        "families_total: {} in the metadata record read for this page; the count \
         can differ between pages of one sequence",
        outcome.families_total
    ));
    let draft = Draft::new(
        envelope,
        Ok(signal),
        outcome.pending_erasure.clone(),
        outcome.declared_column_set.clone(),
        outcome.ingest_watermark_hour,
        outcome.resolved_at_ns,
    )
    .pinned_to(&confirmed);
    finish(draft, &PinScope::of(&confirmed), argument_hash, ctx)
}

/// The envelope every page shares before its rows are capped.
fn base(name: &str, outcome: &DescribeOutcome, rows: Vec<Row>, ctx: &ToolContext<'_>) -> Envelope {
    let mut envelope = Envelope {
        data: crate::envelope::Data {
            columns: vec![
                column("kind", "string"),
                column("name", "string"),
                column("value", "json"),
            ],
            row_count: rows.len() as u64,
            rows,
        },
        ..Envelope::default()
    };
    envelope.scope.signal = name.to_string();
    envelope.scope.table = outcome.table.clone();
    envelope.scope.time_range = Some(TimeRange {
        start_ns: "0".to_string(),
        end_ns: i64::MAX.to_string(),
    });
    envelope.scope.order_by = vec![ORDER_BY.to_string()];
    envelope.coverage.complete = true;
    envelope.accuracy.exact = true;
    envelope.presentation.max_rows = FAMILIES_PER_PAGE as u32;
    envelope.presentation.row_cap_hit = outcome.more_families;
    envelope.budget = Budget {
        effective: AnyJson(json!({
            "max_response_bytes": ctx.budgets.max_response_bytes,
            "deadline_ms": ctx.budgets.deadline.as_millis() as u64,
        })),
        actual: AnyJson(json!({
            "bytes_scanned": outcome.accounting.total_s3_bytes(),
            "store_requests": outcome.accounting.total_s3_requests(),
            "segments_read": outcome.accounting.segments_opened,
            "decompressed_bytes": outcome.accounting.decompressed_bytes,
        })),
        estimate: AnyJson(Value::Object(Map::new())),
        estimate_is_upper_envelope: false,
    };
    if let Some(reason) = &outcome.families_absent_reason {
        envelope.warnings.push(format!("families: {reason}"));
    }
    envelope.warnings.push(UNCOUNTED_READS_WARNING.to_string());
    envelope
}

/// Cap the page and mint its cursor. The family page is already bounded by
/// the service, so the row cap here is the page itself: only the byte cap can
/// cut it, and a cursor is minted when either cut it or the record held more
/// families than the page.
fn finish(
    draft: Draft,
    scope: &PinScope,
    argument_hash: [u8; 32],
    ctx: &ToolContext<'_>,
) -> Envelope {
    let mut budgets = ctx.budgets;
    budgets.max_rows = u32::try_from(draft.envelope.data.rows.len()).unwrap_or(u32::MAX);
    draft.finish_page(&budgets, scope, PageOrder::Total, |pins, last| {
        let position = CursorPosition::Keyset {
            tuple: family_name(last).map(|name| name.as_bytes().to_vec()).unwrap_or_default(),
            order_by: vec![ORDER_BY.to_string()],
        };
        pins.cursor(TOOL, argument_hash, position, i64::MAX).mint(
            ctx.cursor_key,
            ctx.now_ns,
            ctx.protection_horizon_ns,
        )
    })
}

/// The family name a row carries, when it is a `family` row. A page cut
/// before its first family row resumes at the first family: no family name
/// is empty, so the empty tuple names that position unambiguously.
fn family_name(row: &Row) -> Option<&str> {
    match (row.first(), row.get(1)) {
        (Some(Cell::Str(kind)), Some(Cell::Str(name))) if kind == "family" => Some(name),
        _ => None,
    }
}

/// The keyset lower bound a cursor's position names: `None` before the
/// first family. Anything this tool does not mint is
/// [`CursorError::Invalid`], since the MAC already vouched for the token.
fn after_name(position: &CursorPosition) -> Result<Option<String>, CursorError> {
    match position {
        CursorPosition::Keyset { tuple, order_by } if order_by == &[ORDER_BY] => {
            if tuple.is_empty() {
                Ok(None)
            } else {
                String::from_utf8(tuple.clone())
                    .map(Some)
                    .map_err(|_| CursorError::Invalid)
            }
        }
        _ => Err(CursorError::Invalid),
    }
}

fn table_row(name: &str, outcome: &DescribeOutcome) -> Row {
    let coverage = match outcome.coverage {
        Some(window) => json!({
            "min_event_ts_ns": window.min_event_ts_ns.to_string(),
            "max_event_ts_ns": window.max_event_ts_ns.to_string(),
        }),
        None => Value::Null,
    };
    row(
        "table",
        &outcome.table,
        json!({
            "signal": name,
            "segments": outcome.segments,
            "committed_rows": outcome.committed_rows.to_string(),
            "committed_rows_note": "summed from commit records: counted before duplicate \
                                    collapse and before pending erasure",
            "coverage": coverage,
            "families_total": outcome.families_total,
            "families_absent_reason": outcome.families_absent_reason,
        }),
    )
}

fn column_rows(outcome: &DescribeOutcome) -> impl Iterator<Item = Row> + '_ {
    let fixed = outcome.fixed_columns.iter().map(|column| (column, false));
    let declared = outcome.declared_columns.iter().map(|column| (column, true));
    fixed.chain(declared).map(|(column, declared)| {
        row(
            "column",
            &column.name,
            json!({
                "type": column.data_type,
                "nullable": column.nullable,
                "declared": declared,
            }),
        )
    })
}

fn indexed_key_rows(outcome: &DescribeOutcome) -> impl Iterator<Item = Row> + '_ {
    outcome.indexed_keys.iter().map(|key| {
        row(
            "indexed_key",
            key,
            json!({ "source": outcome.indexed_keys_source }),
        )
    })
}

fn family_rows(outcome: &DescribeOutcome) -> Vec<Row> {
    outcome
        .families
        .iter()
        .map(|family| {
            row(
                "family",
                &family.name,
                json!({
                    "type": family.kind,
                    "unit": family.unit,
                    "help": family.help,
                }),
            )
        })
        .collect()
}

fn row(kind: &str, name: &str, value: Value) -> Row {
    let value = match value {
        Value::Object(map) => Cell::Map(map),
        other => Cell::Str(other.to_string()),
    };
    vec![Cell::Str(kind.to_string()), Cell::Str(name.to_string()), value]
}

fn column(name: &str, r#type: &str) -> Column {
    Column {
        name: name.to_string(),
        r#type: r#type.to_string(),
    }
}

fn parse_signal(name: &str) -> Result<Signal, Failure> {
    match name {
        "metrics" => Ok(Signal::Metrics),
        "logs" => Ok(Signal::Logs),
        "traces" => Ok(Signal::Spans),
        other => Err(invalid(format!(
            "unknown signal {other:?}: expected \"metrics\", \"logs\" or \"traces\""
        ))),
    }
}

/// The hash a cursor binds its arguments to: the canonical signal name, the
/// only argument besides the cursor itself.
fn argument_hash(signal: &str) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(ARGUMENT_HASH_CONTEXT);
    hasher.update(signal.as_bytes());
    *hasher.finalize().as_bytes()
}

fn invalid(message: String) -> Failure {
    Failure {
        class: FailureClass::InvalidArgument,
        message,
        counter: None,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::cursor::{CURSOR_KEY_LEN, CursorKey};
    use crate::envelope::Status;
    use crate::service::{DescribedColumn, DescribedCoverage, DescribedFamily};
    use crate::tools::tests::{Stub, block_on, test_context};
    use ravel_query::erasure::ErasurePredicate;
    use ravel_sql::{DeclaredColumn, DeclaredType};
    use ravel_types::TenantId;

    const PAGE_ONE_HOUR: u32 = 480_000;
    const TEN_MINUTES_NS: i64 = 600_000_000_000;

    fn key() -> CursorKey {
        CursorKey::from_process_secret([9u8; CURSOR_KEY_LEN])
    }

    fn names(range: std::ops::Range<usize>) -> Vec<String> {
        range.map(|index| format!("fam_{index:03}")).collect()
    }

    fn outcome(families: &[String], more_families: bool, resolved_at_ns: i64) -> DescribeOutcome {
        DescribeOutcome {
            table: "samples".to_string(),
            fixed_columns: vec![DescribedColumn {
                name: "metric_name".to_string(),
                data_type: "Utf8".to_string(),
                nullable: false,
            }],
            declared_columns: Vec::new(),
            declared_column_set: Vec::new(),
            indexed_keys: Vec::new(),
            indexed_keys_source: "none".to_string(),
            families: families
                .iter()
                .map(|name| DescribedFamily {
                    name: name.clone(),
                    kind: "gauge".to_string(),
                    unit: String::new(),
                    help: String::new(),
                })
                .collect(),
            families_total: 250,
            more_families,
            families_absent_reason: None,
            ingest_watermark_hour: Some(PAGE_ONE_HOUR),
            coverage: Some(DescribedCoverage {
                min_event_ts_ns: 1,
                max_event_ts_ns: 2,
            }),
            segments: 2,
            committed_rows: 10,
            pending_erasure: Vec::new(),
            resolved_at_ns,
            accounting: Default::default(),
        }
    }

    fn stub(outcomes: Vec<DescribeOutcome>) -> Stub {
        let stub = Stub::default();
        stub.describe.borrow_mut().extend(outcomes);
        stub
    }

    fn call(args: Value, ctx: &ToolContext<'_>, backend: &Stub) -> Envelope {
        block_on(run(args, ctx, backend))
    }

    fn family_names(envelope: &Envelope) -> Vec<String> {
        envelope
            .data
            .rows
            .iter()
            .filter_map(|row| family_name(row).map(str::to_string))
            .collect()
    }

    fn cursor_of(envelope: &Envelope) -> String {
        envelope
            .presentation
            .cursor
            .clone()
            .expect("the page carries a cursor")
    }

    /// The context of a later page: the same tenant and key, ten minutes on.
    fn later<'a>(ctx: &ToolContext<'a>) -> ToolContext<'a> {
        ToolContext {
            now_ns: ctx.now_ns + TEN_MINUTES_NS,
            ..*ctx
        }
    }

    /// FLIP: a body that called the backend before validating the signal
    /// panics on the stub's empty queue instead of answering
    /// `invalid_argument`.
    #[test]
    fn an_unknown_signal_is_invalid_argument_and_reaches_no_backend() {
        let key = key();
        let ctx = test_context(&key);
        let backend = Stub::default();
        for signal in ["profiles", "Metrics", "spans", ""] {
            let envelope = call(json!({"signal": signal}), &ctx, &backend);
            assert_eq!(envelope.status, Status::Error, "{signal}");
            assert_eq!(
                envelope.failure.map(|failure| failure.class),
                Some(FailureClass::InvalidArgument),
                "{signal}"
            );
        }
        assert!(backend.described.borrow().is_empty());
    }

    /// FLIP: a body that minted whenever the page held `FAMILIES_PER_PAGE`
    /// families, rather than when the read saw a family after the page,
    /// returns a cursor to a page that would be empty.
    #[test]
    fn exactly_one_full_page_of_families_mints_no_cursor() {
        let key = key();
        let ctx = test_context(&key);
        let backend = stub(vec![outcome(&names(0..100), false, ctx.now_ns)]);
        let envelope = call(json!({"signal": "metrics"}), &ctx, &backend);
        assert_eq!(envelope.status, Status::Ok);
        assert_eq!(envelope.presentation.cursor, None);
        assert!(!envelope.presentation.row_cap_hit);
        assert_eq!(family_names(&envelope), names(0..100));
        let described = backend.described.borrow();
        assert_eq!(described.len(), 1);
        assert_eq!(described[0].limit, FAMILIES_PER_PAGE);
        assert_eq!(described[0].after_name, None);
        assert_eq!(described[0].now_ns, ctx.now_ns);
    }

    /// FLIP: a body that paged by offset, or that resolved page two at its
    /// own clock, sends page two's request with no `after_name` or with the
    /// later `now_ns`; one that re-measured the watermark reports page two's
    /// newer hour; one that served a cursor on the complete last page
    /// returns a third cursor.
    #[test]
    fn a_later_page_resumes_after_the_last_name_at_the_mint_instant_with_page_one_pins() {
        let key = key();
        let ctx = test_context(&key);
        let mut newer = outcome(&names(100..200), true, ctx.now_ns);
        newer.ingest_watermark_hour = Some(PAGE_ONE_HOUR + 5);
        let mut last = outcome(&names(200..250), false, ctx.now_ns);
        last.ingest_watermark_hour = Some(PAGE_ONE_HOUR + 6);
        let backend = stub(vec![outcome(&names(0..100), true, ctx.now_ns), newer, last]);

        let one = call(json!({"signal": "metrics"}), &ctx, &backend);
        assert_eq!(one.status, Status::OkPage);
        assert!(!one.visibility.pinned);
        let token = cursor_of(&one);
        let decoded = Cursor::decode(&token, &key).expect("the cursor decodes");
        assert_eq!(
            decoded.position,
            CursorPosition::Keyset {
                tuple: b"fam_099".to_vec(),
                order_by: vec!["name".to_string()],
            }
        );

        let ctx_two = later(&ctx);
        let two = call(json!({"signal": "metrics", "cursor": token}), &ctx_two, &backend);
        assert_eq!(two.status, Status::OkPage);
        assert!(two.visibility.pinned);
        assert_eq!(family_names(&two), names(100..200));
        assert!(
            two.data
                .rows
                .iter()
                .all(|row| family_name(row).is_some())
        );
        let three = call(
            json!({"signal": "metrics", "cursor": cursor_of(&two)}),
            &ctx_two,
            &backend,
        );
        assert_eq!(three.status, Status::Ok);
        assert_eq!(three.presentation.cursor, None);
        assert_eq!(family_names(&three), names(200..250));

        let described = backend.described.borrow();
        assert_eq!(
            described
                .iter()
                .map(|request| (request.after_name.clone(), request.now_ns))
                .collect::<Vec<_>>(),
            vec![
                (None, ctx.now_ns),
                (Some("fam_099".to_string()), ctx.now_ns),
                (Some("fam_199".to_string()), ctx.now_ns),
            ]
        );
        for page in [&one, &two, &three] {
            assert_eq!(
                page.visibility.ingest_watermark_hour,
                PAGE_ONE_HOUR.to_string()
            );
            assert_eq!(page.visibility.snapshot_id, one.visibility.snapshot_id);
        }
        assert_eq!(one.visibility.snapshot_id.len(), 64);
    }

    /// FLIP: a snapshot id derived without the tenant reports the same id to
    /// two tenants over the same resolve inputs.
    #[test]
    fn the_snapshot_id_differs_across_tenants() {
        let key = key();
        let ctx = test_context(&key);
        let other = ToolContext {
            tenant_hash: TenantId::new("globex").hash(),
            ..ctx
        };
        let backend = stub(vec![
            outcome(&names(0..3), false, ctx.now_ns),
            outcome(&names(0..3), false, ctx.now_ns),
        ]);
        let acme = call(json!({"signal": "metrics"}), &ctx, &backend);
        let globex = call(json!({"signal": "metrics"}), &other, &backend);
        assert_eq!(acme.visibility.snapshot_id.len(), 64);
        assert_eq!(globex.visibility.snapshot_id.len(), 64);
        assert_ne!(acme.visibility.snapshot_id, globex.visibility.snapshot_id);
    }

    /// FLIP: a body that served page two's rows before (or without)
    /// `confirm` returns 100 families here instead of `cursor_expired`.
    #[test]
    fn a_newer_erasure_on_the_signal_expires_the_cursor_and_serves_no_rows() {
        let key = key();
        let ctx = test_context(&key);
        let mut erased = outcome(&names(100..200), true, ctx.now_ns);
        erased.pending_erasure = vec![ErasurePredicate::windowless(vec![(
            "user".to_string(),
            "u1".to_string(),
        )])];
        let backend = stub(vec![outcome(&names(0..100), true, ctx.now_ns), erased]);
        let one = call(json!({"signal": "metrics"}), &ctx, &backend);
        let two = call(
            json!({"signal": "metrics", "cursor": cursor_of(&one)}),
            &ctx,
            &backend,
        );
        assert_eq!(two.status, Status::Error);
        assert_eq!(
            two.failure.as_ref().map(|failure| failure.class),
            Some(FailureClass::CursorExpired)
        );
        assert!(two.data.rows.is_empty());
        assert_eq!(two.presentation.cursor, None);
        assert_eq!(backend.described.borrow().len(), 2);
    }

    /// FLIP: a body that confirmed against page one's own declared set, or
    /// against none, accepts the cursor after the tenant's declared columns
    /// changed.
    #[test]
    fn a_changed_declared_column_set_expires_a_logs_cursor() {
        let key = key();
        let ctx = test_context(&key);
        let logs = |declared: Vec<DeclaredColumn>| {
            let mut logs = outcome(&[], false, ctx.now_ns);
            logs.table = "logs".to_string();
            logs.families_total = 0;
            logs.families_absent_reason = Some("families are a metrics concept".to_string());
            logs.declared_columns = (0..4000)
                .map(|index| DescribedColumn {
                    name: format!("attr_{index:04}_{}", "x".repeat(80)),
                    data_type: "Utf8".to_string(),
                    nullable: true,
                })
                .collect();
            logs.declared_column_set = declared;
            logs
        };
        let pinned = vec![DeclaredColumn::new("service", DeclaredType::Str)];
        let changed = vec![
            DeclaredColumn::new("service", DeclaredType::Str),
            DeclaredColumn::new("region", DeclaredType::Str),
        ];
        let backend = stub(vec![logs(pinned), logs(changed)]);

        // Four thousand wide column rows are over the byte cap, so page one
        // is cut inside its column rows and carries a cursor.
        let one = call(json!({"signal": "logs"}), &ctx, &backend);
        assert_eq!(one.status, Status::OkPage, "{:?}", one.warnings);
        assert!(one.presentation.bytes_cap_hit);
        let decoded = Cursor::decode(&cursor_of(&one), &key).expect("the cursor decodes");
        assert_eq!(decoded.signal, Signal::Logs);
        assert_eq!(
            decoded.position,
            CursorPosition::Keyset {
                tuple: Vec::new(),
                order_by: vec!["name".to_string()],
            }
        );

        let two = call(
            json!({"signal": "logs", "cursor": cursor_of(&one)}),
            &ctx,
            &backend,
        );
        assert_eq!(
            two.failure.as_ref().map(|failure| failure.class),
            Some(FailureClass::CursorExpired)
        );
        assert!(two.data.rows.is_empty());
    }

    /// FLIP: a body that redeemed without its own tool name accepts a token
    /// another tool minted; one whose argument hash ignored the signal
    /// serves a metrics cursor to a logs call.
    #[test]
    fn a_cursor_is_refused_by_another_tool_and_by_another_signal() {
        let key = key();
        let ctx = test_context(&key);
        let backend = stub(vec![outcome(&names(0..100), true, ctx.now_ns)]);
        let one = call(json!({"signal": "metrics"}), &ctx, &backend);
        let token = cursor_of(&one);

        let logs = call(
            json!({"signal": "logs", "cursor": token.clone()}),
            &ctx,
            &backend,
        );
        assert_eq!(
            logs.failure.as_ref().map(|failure| failure.class),
            Some(FailureClass::CursorInvalid)
        );

        let mut foreign = Cursor::decode(&token, &key).expect("the cursor decodes");
        foreign.tool = "ravel_query_sql".to_string();
        let foreign = foreign
            .mint(&key, ctx.now_ns, ctx.protection_horizon_ns)
            .expect("the foreign cursor mints");
        let refused = call(
            json!({"signal": "metrics", "cursor": foreign}),
            &ctx,
            &backend,
        );
        assert_eq!(
            refused.failure.as_ref().map(|failure| failure.class),
            Some(FailureClass::CursorInvalid)
        );
        assert_eq!(backend.described.borrow().len(), 1);
    }

    /// FLIP: a body that took the position through a lossy or re-collated
    /// form (lowercasing, a locale-aware comparison key, a lossy UTF-8
    /// decode) sends a different `after_name` than the bytes it minted.
    #[test]
    fn the_keyset_position_is_the_last_name_byte_for_byte() {
        let key = key();
        let ctx = test_context(&key);
        let page = vec!["Zeta".to_string(), "alpha".to_string(), "\u{e9}t\u{e9}".to_string()];
        let backend = stub(vec![
            outcome(&page, true, ctx.now_ns),
            outcome(&[], false, ctx.now_ns),
        ]);
        let one = call(json!({"signal": "metrics"}), &ctx, &backend);
        let decoded = Cursor::decode(&cursor_of(&one), &key).expect("the cursor decodes");
        assert_eq!(
            decoded.position,
            CursorPosition::Keyset {
                tuple: "\u{e9}t\u{e9}".as_bytes().to_vec(),
                order_by: vec!["name".to_string()],
            }
        );
        let _ = call(
            json!({"signal": "metrics", "cursor": cursor_of(&one)}),
            &ctx,
            &backend,
        );
        assert_eq!(
            backend.described.borrow()[1].after_name.as_deref(),
            Some("\u{e9}t\u{e9}")
        );

        assert_eq!(
            after_name(&CursorPosition::Keyset {
                tuple: vec![0xff, 0xfe],
                order_by: vec!["name".to_string()],
            }),
            Err(CursorError::Invalid)
        );
        assert_eq!(
            after_name(&CursorPosition::RowRange { start: 0, end: 100 }),
            Err(CursorError::Invalid)
        );
        assert_eq!(
            after_name(&CursorPosition::Keyset {
                tuple: b"a".to_vec(),
                order_by: vec!["kind".to_string()],
            }),
            Err(CursorError::Invalid)
        );
    }

    /// FLIP: a body that dropped the service's absent reason, or that minted
    /// a cursor for an empty family page, fails here.
    #[test]
    fn a_signal_with_no_families_is_an_empty_page_with_the_absent_reason() {
        let key = key();
        let ctx = test_context(&key);
        let mut traces = outcome(&[], false, ctx.now_ns);
        traces.table = "spans".to_string();
        traces.families_total = 0;
        traces.families_absent_reason = Some("families are a metrics concept".to_string());
        let backend = stub(vec![traces]);
        let envelope = call(json!({"signal": "traces"}), &ctx, &backend);
        assert_eq!(envelope.status, Status::Ok);
        assert_eq!(envelope.presentation.cursor, None);
        assert!(family_names(&envelope).is_empty());
        assert!(
            envelope
                .warnings
                .contains(&"families: families are a metrics concept".to_string())
        );
        let value = serde_json::to_value(&envelope).expect("envelope serializes");
        assert_eq!(value["data"]["rows"][0][0], json!("table"));
        assert_eq!(
            value["data"]["rows"][0][2]["families_absent_reason"],
            json!("families are a metrics concept")
        );
        assert_eq!(backend.described.borrow()[0].signal, Signal::Spans);
    }

    /// FLIP: a body that wrote a watermark of zero for an empty resolve, or
    /// left `resolved_no_segments` unset, reports a measured hour or the
    /// never-measured warning instead of the empty-resolve one.
    #[test]
    fn an_empty_resolve_reports_no_watermark_and_says_why() {
        let key = key();
        let ctx = test_context(&key);
        let mut empty = outcome(&[], false, ctx.now_ns);
        empty.ingest_watermark_hour = None;
        empty.coverage = None;
        empty.segments = 0;
        empty.committed_rows = 0;
        let backend = stub(vec![empty]);
        let envelope = call(json!({"signal": "metrics"}), &ctx, &backend);
        assert_eq!(envelope.status, Status::Ok);
        assert!(envelope.visibility.resolved_no_segments);
        assert_eq!(envelope.visibility.ingest_watermark_hour, "");
        assert!(
            envelope.warnings.contains(
                &"visibility.ingest_watermark_hour is absent: this operation resolved no segments"
                    .to_string()
            ),
            "{:?}",
            envelope.warnings
        );
        assert_eq!(envelope.visibility.snapshot_id.len(), 64);
    }

    /// FLIP: a page that left the uncounted reads unstated presents
    /// `budget.actual` as the whole cost.
    #[test]
    fn every_page_says_what_budget_actual_leaves_out() {
        let key = key();
        let ctx = test_context(&key);
        let backend = stub(vec![
            outcome(&names(0..100), true, ctx.now_ns),
            outcome(&names(100..101), false, ctx.now_ns),
        ]);
        let one = call(json!({"signal": "metrics"}), &ctx, &backend);
        let two = call(
            json!({"signal": "metrics", "cursor": cursor_of(&one)}),
            &later(&ctx),
            &backend,
        );
        for page in [&one, &two] {
            assert!(
                page.warnings
                    .contains(&UNCOUNTED_READS_WARNING.to_string())
            );
            assert_eq!(
                page.budget.effective.0,
                json!({
                    "max_response_bytes": ctx.budgets.max_response_bytes,
                    "deadline_ms": ctx.budgets.deadline.as_millis() as u64,
                })
            );
        }
        assert!(
            two.warnings
                .iter()
                .any(|warning| warning.starts_with("families_total: 250"))
        );
    }
}
