//! A scalar function whose result for a dictionary-typed string argument
//! would be `Dictionary(_, V)` with `V` not a string type is planned as
//! `CAST(<call> AS V)`: every consumer sees `V`, while the function itself
//! still receives the dictionary argument and so evaluates once per
//! dictionary entry.
//!
//! A declared `Str` column reaches Arrow as `Dictionary(Int32, Utf8)`
//! (ADR-0099 decision 5). That is a wire type: the type of a value computed
//! from the column is the function's value type, and a dictionary of
//! integers is not a type `avg`'s exact path, unary minus or a primitive
//! group key accept.
//!
//! [`unpack_dictionary_results`] replaces only those functions in a
//! session's scalar registry with a [`ValueTypedResult`] wrapper. A function
//! is one of them when probing it says so: for one of the argument lists in
//! [`PROBE_ARGUMENTS`], whose first or second argument is a
//! `Dictionary(Int32, Utf8)`, its own return type is a dictionary of
//! non-strings. Each function name is probed once per process.
//!
//! Every other function stays the registered function. An expression planner
//! builds a call from DataFusion's function value, not from the registry
//! (`EXTRACT`, `POSITION`, `SUBSTRING`, `OVERLAY`, `attrs['k']`), and the SQL
//! planner matches a SELECT, HAVING or ORDER BY expression against GROUP BY by
//! expression equality, which compares the function's implementation type.
//! Wrapping such a function would make its planner-built call and its
//! by-name call two different expressions.
//!
//! The wrapper's return type is already `V` when the SQL planner and the
//! analyzer read it, and its `simplify` rewrites the call into the cast of
//! the wrapped function's own call, so the optimized plan executes the
//! unwrapped function.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{ExprSchema, Result as DFResult, ScalarValue};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::preimage::PreimageResult;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, Expr, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, StructFieldMapping, cast,
};
use datafusion::prelude::SessionContext;

/// `V` when `data_type` is `Dictionary(_, V)` and `V` is not a string type.
pub(crate) fn computed_value_type(data_type: &DataType) -> Option<&DataType> {
    match data_type {
        DataType::Dictionary(_, value)
            if !matches!(
                **value,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) =>
        {
            Some(value)
        }
        _ => None,
    }
}

/// One argument of a probe call.
#[derive(Clone, Copy)]
enum ProbeArgument {
    /// A `Dictionary(Int32, Utf8)` column.
    Column,
    /// The literal `1`.
    Integer,
    /// The literal `'a'`.
    Text,
}

use ProbeArgument::{Column, Integer, Text};

/// The argument lists a function is probed with: one dictionary-typed string
/// argument alone, then with the literal arguments the multi-argument string
/// functions take.
const PROBE_ARGUMENTS: [&[ProbeArgument]; 9] = [
    &[Column],
    &[Column, Integer],
    &[Column, Text],
    &[Column, Integer, Integer],
    &[Column, Text, Text],
    &[Column, Integer, Text],
    &[Column, Text, Integer],
    &[Text, Column],
    &[Column, Column],
];

fn probe_argument(argument: ProbeArgument) -> (FieldRef, Option<ScalarValue>) {
    let (data_type, value) = match argument {
        Column => (
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            None,
        ),
        Integer => (DataType::Int64, Some(ScalarValue::Int64(Some(1)))),
        Text => (
            DataType::Utf8,
            Some(ScalarValue::Utf8(Some("a".to_string()))),
        ),
    };
    (Arc::new(Field::new("arg", data_type, true)), value)
}

#[cfg(test)]
thread_local! {
    /// Probes run on this thread, so a test can tell a cached answer from a
    /// fresh probe without racing other tests' sessions.
    static PROBES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Whether `udf`, called with any of [`PROBE_ARGUMENTS`] after the argument
/// coercion its signature asks for, returns a dictionary of non-strings.
fn returns_a_computed_dictionary(udf: &ScalarUDF) -> bool {
    #[cfg(test)]
    PROBES.with(|probes| probes.set(probes.get() + 1));
    PROBE_ARGUMENTS.iter().any(|arguments| {
        let (fields, values): (Vec<FieldRef>, Vec<Option<ScalarValue>>) =
            arguments.iter().copied().map(probe_argument).unzip();
        let Ok(coerced) = fields_with_udf(&fields, udf) else {
            return false;
        };
        let scalar_arguments: Vec<Option<&ScalarValue>> =
            values.iter().map(Option::as_ref).collect();
        udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &coerced,
            scalar_arguments: &scalar_arguments,
        })
        .is_ok_and(|field| computed_value_type(field.data_type()).is_some())
    })
}

/// Each probed function name and whether it is wrapped.
///
/// Keyed by name rather than built once from the first session, because the
/// registry differs by table (a metrics session registers `label`, a logs
/// session `has_word`): a name is probed the first time any session
/// registers it. A name resolves to one implementation per process, so its
/// answer never changes.
static PROBED: LazyLock<Mutex<BTreeMap<String, bool>>> = LazyLock::new(Mutex::default);

/// Replace each function in `ctx`'s scalar registry that returns a
/// dictionary of non-strings for a dictionary-typed string argument with its
/// [`ValueTypedResult`] wrapper. Run after the last scalar registration.
pub(crate) fn unpack_dictionary_results(ctx: &SessionContext) {
    // The registry keys each function under its name and every alias;
    // `register_udf` inserts the wrapper under all of them.
    let functions: BTreeMap<String, ScalarUDF> = ctx
        .state()
        .scalar_functions()
        .values()
        .filter(|udf| !udf.inner().is::<ValueTypedResult>())
        .map(|udf| (udf.name().to_string(), udf.as_ref().clone()))
        .collect();
    let mut probed = PROBED.lock().unwrap_or_else(PoisonError::into_inner);
    for (name, udf) in functions {
        let wrap = *probed
            .entry(name)
            .or_insert_with(|| returns_a_computed_dictionary(&udf));
        if wrap {
            ctx.register_udf(ValueTypedResult::wrap(udf));
        }
    }
}

/// A scalar function whose dictionary-of-non-string result is reported, and
/// planned, as the dictionary's value type. Every other method delegates.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct ValueTypedResult {
    inner: ScalarUDF,
}

impl ValueTypedResult {
    pub(crate) fn wrap(inner: ScalarUDF) -> ScalarUDF {
        ScalarUDF::new_from_impl(Self { inner })
    }

    fn call(&self, args: Vec<Expr>) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(self.inner.clone()), args))
    }
}

fn value_typed_field(field: FieldRef) -> FieldRef {
    match computed_value_type(field.data_type()) {
        Some(value) => Arc::new(field.as_ref().clone().with_data_type(value.clone())),
        None => field,
    }
}

#[warn(clippy::missing_trait_methods)]
impl ScalarUDFImpl for ValueTypedResult {
    fn name(&self) -> &str {
        self.inner.inner().name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.inner().aliases()
    }

    fn display_name(&self, args: &[Expr]) -> DFResult<String> {
        #[allow(deprecated)]
        self.inner.inner().display_name(args)
    }

    fn schema_name(&self, args: &[Expr]) -> DFResult<String> {
        self.inner.inner().schema_name(args)
    }

    fn signature(&self) -> &Signature {
        self.inner.inner().signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        let data_type = self.inner.inner().return_type(arg_types)?;
        Ok(computed_value_type(&data_type)
            .cloned()
            .unwrap_or(data_type))
    }

    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        self.inner
            .inner()
            .with_updated_config(config)
            .map(Self::wrap)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        self.inner
            .inner()
            .return_field_from_args(args)
            .map(value_typed_field)
    }

    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        #[allow(deprecated)]
        self.inner.inner().is_nullable(args, schema)
    }

    fn is_strict(&self) -> bool {
        self.inner.inner().is_strict()
    }

    /// Runs whenever the wrapped call is evaluated before [`Self::simplify`]
    /// replaced it: the simplifier's constant evaluator folds a call over
    /// literal arguments this way, and so does a plan built without
    /// simplification. The inner function runs on the dictionary argument
    /// and its result is cast to the value type.
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let inner_field = {
            let scalars: Vec<Option<&ScalarValue>> = args
                .args
                .iter()
                .map(|arg| match arg {
                    ColumnarValue::Scalar(value) => Some(value),
                    ColumnarValue::Array(_) => None,
                })
                .collect();
            self.inner.inner().return_field_from_args(ReturnFieldArgs {
                arg_fields: &args.arg_fields,
                scalar_arguments: &scalars,
            })?
        };
        let wanted = args.return_field.data_type().clone();
        let result = self.inner.inner().invoke_with_args(ScalarFunctionArgs {
            return_field: inner_field,
            ..args
        })?;
        if result.data_type() == wanted {
            Ok(result)
        } else {
            result.cast_to(&wanted, None)
        }
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> DFResult<ExprSimplifyResult> {
        let expr = match self.inner.inner().simplify(args, info)? {
            ExprSimplifyResult::Simplified(expr) => expr,
            ExprSimplifyResult::Original(args) => self.call(args),
        };
        let data_type = info.get_data_type(&expr)?;
        Ok(ExprSimplifyResult::Simplified(
            match computed_value_type(&data_type) {
                Some(value) => cast(expr, value.clone()),
                None => expr,
            },
        ))
    }

    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        info: &SimplifyContext,
    ) -> DFResult<PreimageResult> {
        self.inner.inner().preimage(args, lit_expr, info)
    }

    fn short_circuits(&self) -> bool {
        self.inner.inner().short_circuits()
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        self.inner.inner().conditional_arguments(args)
    }

    fn evaluate_bounds(&self, input: &[&Interval]) -> DFResult<Interval> {
        self.inner.inner().evaluate_bounds(input)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> DFResult<Option<Vec<Interval>>> {
        self.inner.inner().propagate_constraints(interval, inputs)
    }

    fn output_ordering(&self, inputs: &[ExprProperties]) -> DFResult<SortProperties> {
        self.inner.inner().output_ordering(inputs)
    }

    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> DFResult<bool> {
        self.inner.inner().preserves_lex_ordering(inputs)
    }

    fn strictly_order_preserving(&self, inputs: &[ExprProperties]) -> DFResult<bool> {
        self.inner.inner().strictly_order_preserving(inputs)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        self.inner.inner().coerce_types(arg_types)
    }

    fn struct_field_mapping(
        &self,
        literal_args: &[Option<ScalarValue>],
    ) -> Option<StructFieldMapping> {
        self.inner.inner().struct_field_mapping(literal_args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.inner().documentation()
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        self.inner.inner().placement(args)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::BTreeSet;

    use datafusion::datasource::empty::EmptyTable;
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::memory::MemoryStore;
    use ravel_query::{LogSegmentFetcher, PhaseAccounting};
    use ravel_types::accounting::QueryAccounting;

    use super::*;
    use crate::memory::{CeilingBreach, TenantDelegatingPool, TenantMemoryAccountant};
    use crate::{
        DeclaredColumn, DeclaredType, LogsTableProvider, SessionTable, SpillDecision, SqlConfig,
        build_session, logs_schema_with_declared,
    };

    fn declared() -> Vec<DeclaredColumn> {
        vec![DeclaredColumn::new("Referer", DeclaredType::Str)]
    }

    /// A production `logs` session over a declared `Str` column.
    fn logs_session() -> SessionContext {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let snapshot = ravel_catalog::Snapshot {
            segments: Vec::new(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let table = SessionTable::Logs(Arc::new(
            LogsTableProvider::new(
                snapshot,
                ravel_types::TenantHash([0u8; 16]),
                LogSegmentFetcher::new(store),
                PhaseAccounting::new(),
            )
            .with_declared_columns(declared()),
        ));
        let pool = Arc::new(TenantDelegatingPool::new(
            1 << 30,
            TenantMemoryAccountant::new(1 << 30),
            CeilingBreach::new(),
            QueryAccounting::new(),
        ));
        build_session(
            &SqlConfig::default(),
            pool,
            table,
            false,
            SpillDecision::Disabled,
            &crate::SessionPrincipal::without_capabilities(ravel_types::TenantHash([0u8; 16])),
        )
        .expect("session builds")
    }

    /// Every registry key whose function is a [`ValueTypedResult`], aliases
    /// included.
    fn wrapped_keys(ctx: &SessionContext) -> BTreeSet<String> {
        ctx.state()
            .scalar_functions()
            .iter()
            .filter(|(_, udf)| udf.inner().is::<ValueTypedResult>())
            .map(|(key, _)| key.clone())
            .collect()
    }

    #[test]
    fn a_second_session_does_not_probe_again() {
        let first = logs_session();
        let probes = PROBES.with(std::cell::Cell::get);
        let second = logs_session();
        assert_eq!(
            PROBES.with(std::cell::Cell::get),
            probes,
            "the second session reused every probed answer"
        );
        assert_eq!(wrapped_keys(&first), wrapped_keys(&second));
        assert!(wrapped_keys(&second).contains("length"));
    }

    /// The wrapped set, decided by probing, is exactly the set of registered
    /// functions that SQL planning types as a dictionary of non-strings when
    /// they are not wrapped. The SQL walk is independent of the probe: it
    /// plans statements over the column instead of calling return types.
    #[tokio::test]
    async fn the_wrapped_functions_are_exactly_the_numeric_dictionary_results() {
        let production = logs_session();
        let unwrapped = SessionContext::new();
        let mut canonical = BTreeMap::new();
        for udf in production.state().scalar_functions().values() {
            let udf = match udf.inner().downcast_ref::<ValueTypedResult>() {
                Some(wrapper) => wrapper.inner.clone(),
                None => udf.as_ref().clone(),
            };
            canonical.insert(udf.name().to_string(), udf.clone());
            unwrapped.register_udf(udf);
        }
        unwrapped
            .register_table(
                "logs",
                Arc::new(EmptyTable::new(logs_schema_with_declared(&declared()))),
            )
            .expect("register logs");

        let mut numeric_dictionaries = BTreeSet::new();
        for (name, udf) in &canonical {
            for arguments in [
                "c",
                "c, 1",
                "c, 'a'",
                "c, 1, 2",
                "c, 'a', 'b'",
                "c, 1, 'a'",
                "c, 'a', 1",
                "'a', c",
                "c, c",
            ] {
                let sql = format!(
                    "SELECT {name}({}) FROM logs",
                    arguments.replace('c', "\"Referer\"")
                );
                let Ok(df) = unwrapped.sql(&sql).await else {
                    continue;
                };
                if computed_value_type(df.schema().field(0).data_type()).is_some() {
                    numeric_dictionaries.insert(name.clone());
                    numeric_dictionaries.extend(udf.aliases().iter().cloned());
                }
            }
        }
        let wrapped = wrapped_keys(&production);
        assert_eq!(wrapped, numeric_dictionaries);
        assert_eq!(
            wrapped,
            BTreeSet::from(
                [
                    "ascii",
                    "bit_length",
                    "char_length",
                    "character_length",
                    "coalesce",
                    "length",
                    "octet_length"
                ]
                .map(String::from)
            )
        );
    }
}
