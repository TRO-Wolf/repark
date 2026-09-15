use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{ExprSchema, Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::preimage::PreimageResult;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, Expr, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, StructFieldMapping,
};

pub fn retag_registered_udfs(ctx: &datafusion::prelude::SessionContext) {
    let registered: Vec<(String, Arc<ScalarUDF>)> = ctx
        .state()
        .scalar_functions()
        .iter()
        .map(|(name, udf)| (name.clone(), Arc::clone(udf)))
        .collect();
    for (name, udf) in registered {
        ctx.register_udf(promise_retagged_as(name, udf).as_ref().clone());
    }
}

#[must_use]
pub fn promise_retagged(inner: Arc<ScalarUDF>) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(PromiseRetag { name: None, inner }))
}

#[must_use]
pub fn promise_retagged_as(name: String, inner: Arc<ScalarUDF>) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(PromiseRetag {
        name: Some(name),
        inner,
    }))
}

#[derive(Debug)]
struct PromiseRetag {
    name: Option<String>,
    inner: Arc<ScalarUDF>,
}

impl PartialEq for PromiseRetag {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl Eq for PromiseRetag {}

impl Hash for PromiseRetag {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl ScalarUDFImpl for PromiseRetag {
    fn name(&self) -> &str {
        self.name.as_deref().unwrap_or_else(|| self.inner.name())
    }

    fn aliases(&self) -> &[String] {
        if self.name.is_some() {
            &[]
        } else {
            self.inner.aliases()
        }
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        match &self.name {
            Some(name) if !name.eq_ignore_ascii_case(self.inner.name()) => Ok(format!(
                "{name}({})",
                args.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            )),
            _ => self.inner.inner().schema_name(args),
        }
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        self.inner
            .inner()
            .with_updated_config(config)
            .map(|updated| {
                ScalarUDF::from(PromiseRetag {
                    name: self.name.clone(),
                    inner: Arc::new(updated),
                })
            })
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }

    #[allow(deprecated)]
    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        self.inner.inner().is_nullable(args, schema)
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        self.inner.inner().simplify(args, info)
    }

    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        info: &SimplifyContext,
    ) -> Result<PreimageResult> {
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

    fn evaluate_bounds(&self, input: &[&Interval]) -> Result<Interval> {
        self.inner.inner().evaluate_bounds(input)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.inner.inner().propagate_constraints(interval, inputs)
    }

    fn output_ordering(&self, inputs: &[ExprProperties]) -> Result<SortProperties> {
        self.inner.inner().output_ordering(inputs)
    }

    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> Result<bool> {
        self.inner.inner().preserves_lex_ordering(inputs)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let data_type = args.return_field.data_type().clone();
        let value = self.inner.inner().invoke_with_args(args)?;
        Ok(retag_to_promise(value, &data_type))
    }
}

#[must_use]
pub fn retag_to_promise(value: ColumnarValue, data_type: &DataType) -> ColumnarValue {
    match value {
        ColumnarValue::Array(array) if *array.data_type() == *data_type => {
            ColumnarValue::Array(array)
        }
        ColumnarValue::Array(array) => {
            ColumnarValue::Array(retag_list_element(array.as_ref(), data_type))
        }
        ColumnarValue::Scalar(scalar) if scalar.data_type() == *data_type => {
            ColumnarValue::Scalar(scalar)
        }
        ColumnarValue::Scalar(scalar) => {
            ColumnarValue::Scalar(retag_scalar_element(scalar, data_type))
        }
    }
}

fn retag_scalar_element(scalar: ScalarValue, data_type: &DataType) -> ScalarValue {
    match scalar {
        ScalarValue::List(array) => retag_list_element(array.as_ref(), data_type)
            .as_any()
            .downcast_ref::<ListArray>()
            .map_or_else(
                || ScalarValue::List(array.clone()),
                |list| ScalarValue::List(Arc::new(list.clone())),
            ),
        ScalarValue::LargeList(array) => retag_list_element(array.as_ref(), data_type)
            .as_any()
            .downcast_ref::<LargeListArray>()
            .map_or_else(
                || ScalarValue::LargeList(array.clone()),
                |list| ScalarValue::LargeList(Arc::new(list.clone())),
            ),
        ScalarValue::FixedSizeList(array) => retag_list_element(array.as_ref(), data_type)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .map_or_else(
                || ScalarValue::FixedSizeList(array.clone()),
                |list| ScalarValue::FixedSizeList(Arc::new(list.clone())),
            ),
        other => other,
    }
}

fn retag_list_element(array: &dyn Array, data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::List(element) => {
            if let Some(list) = array.as_any().downcast_ref::<ListArray>()
                && element.data_type() == list.values().data_type()
            {
                return Arc::new(ListArray::new(
                    element.clone(),
                    list.offsets().clone(),
                    list.values().clone(),
                    list.nulls().cloned(),
                ));
            }
        }
        DataType::LargeList(element) => {
            if let Some(list) = array.as_any().downcast_ref::<LargeListArray>()
                && element.data_type() == list.values().data_type()
            {
                return Arc::new(LargeListArray::new(
                    element.clone(),
                    list.offsets().clone(),
                    list.values().clone(),
                    list.nulls().cloned(),
                ));
            }
        }
        DataType::FixedSizeList(element, size) => {
            if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>()
                && element.data_type() == list.values().data_type()
            {
                return Arc::new(FixedSizeListArray::new(
                    element.clone(),
                    *size,
                    list.values().clone(),
                    list.nulls().cloned(),
                ));
            }
        }
        _ => {}
    }
    datafusion::arrow::array::make_array(array.to_data())
}
