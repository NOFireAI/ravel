//! A scalar function whose result would be `Dictionary(_, V)` with `V` not a
//! string type is planned as `CAST(<call> AS V)`: every consumer sees `V`,
//! while the function itself still receives the dictionary argument and so
//! evaluates once per dictionary entry.
//!
//! A declared `Str` column reaches Arrow as `Dictionary(Int32, Utf8)`
//! (ADR-0099 decision 5). That is a wire type: the type of a value computed
//! from the column is the function's value type, and a dictionary of
//! integers is not a type `avg`'s exact path, unary minus or a primitive
//! group key accept.
//!
//! [`unpack_dictionary_results`] replaces every function in a session's
//! scalar registry with a [`ValueTypedResult`] wrapper, whose return type is
//! already `V` when the SQL planner and the analyzer read it, and whose
//! `simplify` rewrites the call into the cast of the wrapped function's own
//! call, so the optimized plan executes the unwrapped function.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{ExprSchema, Result as DFResult, ScalarValue};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::preimage::PreimageResult;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
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

/// Replace every function in `ctx`'s scalar registry with its
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
    for udf in functions.into_values() {
        ctx.register_udf(ValueTypedResult::wrap(udf));
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

    /// Reached only by a plan the optimizer did not simplify: the inner
    /// function runs on the dictionary argument and its result is cast.
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
