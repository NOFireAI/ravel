//! `ravel_find_labels`: label names under a selector, or the values of one
//! label (ADR-1374 D2, amendments 2026-09-12 item 5 and 2026-09-13 items 1
//! and 2).
//!
//! Two modes, chosen by the arguments:
//!
//! - `label_name` given: the values of that label, through
//!   [`QueryBackend::label_values`]. `label_name: "__name__"` is the metric
//!   name list.
//! - only `selector` given: the label names of the matching series, through
//!   [`QueryBackend::labels`].
//!
//! A request naming neither is a tenant-wide list and is refused before any
//! store read; a `filter` bounds only the output, so it does not count. The
//! list is observed from data over the window, not declared: the metadata
//! outcomes carry names, a partial flag and warnings, and no basis.
//!
//! The port returns the list before any cap, and this body applies the
//! filter and then the byte cap, so a page is complete for its filter and a
//! truncation report means more matches exist.

use ravel_query::RequestBudgets;
use ravel_query::http::{ApiError, MetadataRequest, include_log_metric_names};
use serde::Deserialize;
use serde_json::Value;

use crate::catalog::FindLabelsInput;
use crate::envelope::{Cell, Envelope, Failure, FailureClass, NextStep};
use crate::service::QueryBackend;

use super::ToolContext;
use super::capabilities::refused_with;

/// D6's segment bound for label resolution. The effective `max_segments` can
/// lower it and never raise it.
pub const LABEL_RESOLUTION_SEGMENTS: usize = 2_000;

/// Which list a call asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Mode {
    Names,
    Values {
        name: String,
        include_log_metric_names: bool,
    },
}

#[derive(Debug, Clone)]
struct Plan {
    mode: Mode,
    filter: Option<String>,
    request: MetadataRequest,
}

type Refusal = (Failure, Vec<NextStep>);

/// Run the tool. Every refusal answers before `backend` is called.
pub async fn run<B: QueryBackend>(args: Value, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let plan = match plan(args, ctx) {
        Ok(plan) => plan,
        Err((failure, next_steps)) => return refused_with(failure, next_steps, ctx),
    };
    let listed = match &plan.mode {
        Mode::Names => backend.labels(ctx.tenant_hash, &plan.request).await,
        Mode::Values {
            name,
            include_log_metric_names,
        } => {
            backend
                .label_values(
                    ctx.tenant_hash,
                    &plan.request,
                    name,
                    *include_log_metric_names,
                )
                .await
        }
    };
    match listed {
        Ok(envelope) => finish(envelope, &plan, ctx),
        Err(failure) => refused_with(failure, Vec::new(), ctx),
    }
}

fn plan(args: Value, ctx: &ToolContext<'_>) -> Result<Plan, Refusal> {
    if args.get("evidence_ref").is_some() {
        return Err(refusal(
            FailureClass::InvalidArgument,
            "evidence_ref is not an argument of ravel_find_labels".to_string(),
        ));
    }
    if args.get("time_range").is_none_or(Value::is_null) {
        return Err(refusal(
            FailureClass::MissingArgument,
            "ravel_find_labels requires time_range".to_string(),
        ));
    }
    let input = FindLabelsInput::deserialize(args).map_err(|err| {
        refusal(
            FailureClass::InvalidArgument,
            format!("ravel_find_labels arguments: {err}"),
        )
    })?;
    for (name, value) in [
        ("selector", &input.selector),
        ("label_name", &input.label_name),
        ("filter", &input.filter),
    ] {
        if value.as_deref().is_some_and(str::is_empty) {
            return Err(refusal(
                FailureClass::InvalidArgument,
                format!("{name} must not be empty; omit it instead"),
            ));
        }
    }
    let window = crate::time::window(&input.time_range).map_err(|f| (f, Vec::new()))?;
    if input.selector.is_none() && input.label_name.is_none() {
        return Err((
            Failure {
                class: FailureClass::MissingArgument,
                message: "ravel_find_labels refuses a tenant-wide list: pass a selector, a \
                          label_name, or both; a filter bounds only the output"
                    .to_string(),
                counter: None,
            },
            vec![NextStep {
                action: "ravel_find_labels".to_string(),
                detail: "send label_name: \"__name__\" for metric names, a selector such as \
                         {job=\"api\"} for its label names, or both for one label's values \
                         under that selector"
                    .to_string(),
            }],
        ));
    }

    let selectors: Vec<String> = input.selector.into_iter().collect();
    let mode = match input.label_name {
        Some(name) => Mode::Values {
            include_log_metric_names: include_log_metric_names(&name, &selectors)
                .map_err(|error| (api_failure(error), Vec::new()))?,
            name,
        },
        None => Mode::Names,
    };
    let query = &ctx.budgets.query;
    let request = MetadataRequest {
        selectors,
        window,
        min_tokens: Vec::new(),
        deadline: ctx.budgets.deadline,
        allow_partial: false,
        now_ns: ctx.now_ns,
        budgets: Some(RequestBudgets {
            max_bytes_scanned: Some(query.max_bytes_scanned),
            max_store_requests: Some(query.max_store_requests),
            max_segments: Some(query.max_segments.min(LABEL_RESOLUTION_SEGMENTS)),
        }),
    };
    Ok(Plan {
        mode,
        filter: input.filter,
        request,
    })
}

/// Filter, then cap: the order that keeps a page complete for its filter.
/// The page cap is `max_response_bytes` alone: this tool advertises no
/// `max_rows`. Rows that do not fit are counted in
/// `presentation.rows_omitted` with `bytes_cap_hit`, and `data.row_count` is
/// the full match count.
fn finish(mut envelope: Envelope, plan: &Plan, ctx: &ToolContext<'_>) -> Envelope {
    if let Some(filter) = &plan.filter {
        envelope.data.rows.retain(
            |row| matches!(row.first(), Some(Cell::Str(text)) if text.contains(filter.as_str())),
        );
        envelope
            .scope
            .predicates_applied
            .push(format!("filter contains {filter:?} (case-sensitive)"));
    }
    envelope.warnings.push(basis(&plan.mode));

    let matched = envelope.data.rows.len() as u64;
    let mut fitted = envelope.fit(ctx.budgets.max_response_bytes);
    fitted.data.row_count = matched;
    fitted.finish(false)
}

/// What the list is, stated once per response: which mode ran and what the
/// list claims.
fn basis(mode: &Mode) -> String {
    let what = match mode {
        Mode::Names => "label names of the series the selector matches".to_string(),
        Mode::Values { name, .. } if name == "__name__" => {
            "metric names (the values of __name__)".to_string()
        }
        Mode::Values { name, .. } => format!("values of label {name}"),
    };
    format!(
        "{what}, observed from data over the window, not a declared set; complete for \
         the window unless coverage.partial is true"
    )
}

fn api_failure(error: ApiError) -> Failure {
    let (class, message) = match error {
        ApiError::BadData(message) => (FailureClass::InvalidArgument, message),
        ApiError::Unsupported(message) => (FailureClass::Unsupported, message),
        ApiError::Corrupt(message) => (FailureClass::Internal, message),
        ApiError::Unavailable(message) => (FailureClass::Unavailable, message),
        ApiError::Timeout(message) => (FailureClass::Deadline, message),
        ApiError::Unauthenticated => (FailureClass::Unauthorized, "unauthenticated".to_string()),
    };
    Failure {
        class,
        message,
        counter: None,
    }
}

fn refusal(class: FailureClass, message: String) -> Refusal {
    (
        Failure {
            class,
            message,
            counter: None,
        },
        Vec::new(),
    )
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::cursor::{CURSOR_KEY_LEN, CursorKey};
    use crate::envelope::{Column, Status};
    use crate::tools::tests::{Stub, block_on, test_context};

    const WINDOW: &str = r#"{"start_ns": "1000", "end_ns": "2000"}"#;

    fn with_window(mut args: Value) -> Value {
        args["time_range"] = serde_json::from_str(WINDOW).expect("window literal");
        args
    }

    fn listed(values: &[&str]) -> Envelope {
        let mut envelope = Envelope::default();
        envelope.data.columns = vec![Column {
            name: "value".to_string(),
            r#type: "string".to_string(),
        }];
        envelope.data.rows = values
            .iter()
            .map(|value| vec![Cell::Str((*value).to_string())])
            .collect();
        envelope.data.row_count = values.len() as u64;
        envelope.coverage.complete = true;
        envelope.accuracy.exact = true;
        envelope
    }

    fn strings(envelope: &Envelope) -> Vec<String> {
        envelope
            .data
            .rows
            .iter()
            .map(|row| match &row[0] {
                Cell::Str(text) => text.clone(),
                other => panic!("not a string cell: {other:?}"),
            })
            .collect()
    }

    fn call(stub: &Stub, args: Value) -> Envelope {
        let key = CursorKey::from_process_secret([31u8; CURSOR_KEY_LEN]);
        let ctx = test_context(&key);
        block_on(run(args, &ctx, stub))
    }

    fn failure_of(envelope: &Envelope) -> &Failure {
        assert_eq!(envelope.status, Status::Error);
        envelope
            .failure
            .as_ref()
            .expect("an error envelope carries one")
    }

    /// Neither a selector nor a label name is a tenant-wide list, refused
    /// before any store read, with a next step naming the bounded forms. A
    /// filter alone bounds only the output and is refused the same way.
    ///
    /// FLIP: counting a filter as a bound lets the second call reach the
    /// stub, which panics; refusing after the listing panics on both.
    #[test]
    fn a_request_with_neither_selector_nor_label_name_is_refused() {
        for args in [json!({}), json!({"filter": "http"})] {
            let stub = Stub::default();
            let envelope = call(&stub, with_window(args.clone()));
            let failure = failure_of(&envelope);
            assert_eq!(failure.class, FailureClass::MissingArgument, "{args}");
            assert_eq!(envelope.next_steps.len(), 1);
            assert_eq!(envelope.next_steps[0].action, "ravel_find_labels");
            assert!(
                envelope.next_steps[0]
                    .detail
                    .contains("label_name: \"__name__\"")
            );
        }
    }

    /// An empty filter is `invalid_argument`, not a match of everything, and
    /// `evidence_ref` is refused rather than accepted and ignored.
    ///
    /// FLIP: treating `""` as no filter, or ignoring `evidence_ref`, reaches
    /// the stub, which panics.
    #[test]
    fn an_empty_filter_and_an_evidence_ref_are_invalid() {
        for args in [
            json!({"label_name": "__name__", "filter": ""}),
            json!({"label_name": "__name__", "evidence_ref": "ev1"}),
            json!({"label_name": ""}),
        ] {
            let stub = Stub::default();
            let envelope = call(&stub, with_window(args.clone()));
            assert_eq!(
                failure_of(&envelope).class,
                FailureClass::InvalidArgument,
                "{args}"
            );
        }
    }

    /// The filter is a case-sensitive substring match, reported in
    /// `scope.predicates_applied`.
    ///
    /// FLIP: a case-insensitive match returns all three values.
    #[test]
    fn the_filter_is_case_sensitive() {
        let stub = Stub {
            listed: Some(listed(&["Api_x", "api_y", "API_z"])),
            ..Stub::default()
        };
        let envelope = call(
            &stub,
            with_window(json!({"label_name": "service", "filter": "api"})),
        );
        assert_eq!(envelope.status, Status::Ok, "{:?}", envelope.failure);
        assert_eq!(strings(&envelope), ["api_y"]);
        assert_eq!(
            envelope.scope.predicates_applied,
            vec!["filter contains \"api\" (case-sensitive)".to_string()]
        );
    }

    /// The filter runs before the page cap, and the cap is the byte cap:
    /// 40,000 values alternate `db_` and `api_`, the 20,000 matches overflow
    /// the 256 KiB floor, and the page is a prefix of the matches whose
    /// length plus `presentation.rows_omitted` is the match count, with
    /// `bytes_cap_hit` set. `max_rows` is 1 and cuts nothing.
    ///
    /// FLIP: capping first fits a prefix of all 40,000 values, so the page
    /// is about half as long and its length plus `rows_omitted` is not
    /// 20,000; applying `max_rows` leaves one row.
    #[test]
    fn the_filter_runs_before_the_page_cap() {
        let values: Vec<String> = (0..20_000)
            .flat_map(|i| [format!("db_{i:05}"), format!("api_{i:05}")])
            .collect();
        let refs: Vec<&str> = values.iter().map(String::as_str).collect();
        let stub = Stub {
            listed: Some(listed(&refs)),
            ..Stub::default()
        };
        let key = CursorKey::from_process_secret([31u8; CURSOR_KEY_LEN]);
        let mut ctx = test_context(&key);
        ctx.budgets.max_rows = 1;
        ctx.budgets.max_response_bytes = 0;
        let envelope = block_on(run(
            with_window(json!({"label_name": "service", "filter": "api"})),
            &ctx,
            &stub,
        ));
        assert_eq!(envelope.status, Status::OkBounded);
        let page = strings(&envelope);
        let expected: Vec<String> = (0..page.len()).map(|i| format!("api_{i:05}")).collect();
        assert_eq!(page, expected);
        assert!(page.len() > 1);
        assert!(envelope.presentation.bytes_cap_hit);
        assert!(!envelope.presentation.row_cap_hit);
        assert_eq!(page.len() as u64 + envelope.presentation.rows_omitted, 20_000);
        assert_eq!(envelope.data.row_count, 20_000);
    }

    /// The resolution is bounded at the smaller of the effective
    /// `max_segments` and D6's 2,000 for label resolution, on both sides of
    /// 2,000, and the list is complete or refused, never partial.
    ///
    /// FLIP: passing the effective value through gives 5,000; passing 2,000
    /// unconditionally gives 2,000 for the 500 case.
    #[test]
    fn max_segments_is_the_smaller_of_effective_and_two_thousand() {
        for (effective, sent) in [(5_000, 2_000), (500, 500)] {
            let key = CursorKey::from_process_secret([32u8; CURSOR_KEY_LEN]);
            let mut ctx = test_context(&key);
            ctx.budgets.query.max_segments = effective;
            let stub = Stub {
                listed: Some(listed(&["up"])),
                ..Stub::default()
            };
            block_on(run(
                with_window(json!({"label_name": "__name__"})),
                &ctx,
                &stub,
            ));
            let metadata = stub.metadata.borrow();
            let (request, _) = &metadata[0];
            let budgets = request.budgets.expect("budgets are sent");
            assert_eq!(budgets.max_segments, Some(sent), "effective {effective}");
            assert!(!request.allow_partial);
        }
    }

    /// The arguments choose the mode, and the response says which ran:
    /// `label_name` alone is that label's values (`__name__` is the metric
    /// names, with log metric names included as the HTTP endpoint decides);
    /// a selector alone is label names; both are one label's values under
    /// the selector.
    ///
    /// FLIP: routing a selector-only call to `label_values`, or computing
    /// `include_log_metric_names` as false for a bare `__name__`, fails the
    /// recorded call.
    #[test]
    fn the_arguments_choose_the_mode_and_the_response_names_it() {
        for (args, call_shape, basis) in [
            (
                json!({"label_name": "__name__"}),
                Some(("__name__".to_string(), true)),
                "metric names (the values of __name__), observed from data over the window",
            ),
            (
                json!({"selector": "{job=\"api\"}"}),
                None,
                "label names of the series the selector matches, observed from data",
            ),
            (
                json!({"selector": "{job=\"api\"}", "label_name": "instance"}),
                Some(("instance".to_string(), false)),
                "values of label instance, observed from data over the window",
            ),
        ] {
            let stub = Stub {
                listed: Some(listed(&["x"])),
                ..Stub::default()
            };
            let envelope = call(&stub, with_window(args.clone()));
            assert_eq!(
                envelope.status,
                Status::Ok,
                "{args}: {:?}",
                envelope.failure
            );
            let metadata = stub.metadata.borrow();
            assert_eq!(metadata.len(), 1);
            assert_eq!(metadata[0].1, call_shape, "{args}");
            assert!(
                envelope.warnings.iter().any(|w| w.starts_with(basis)
                    && w.ends_with("complete for the window unless coverage.partial is true")),
                "{args}: {:?}",
                envelope.warnings
            );
        }
    }
}
