use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result, internal_err};
use datafusion::logical_expr::expr::{Alias, ScalarFunction};
use datafusion::logical_expr::{
    ColumnarValue, DmlStatement, Expr, ExprSchemable, LogicalPlan, Projection, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::instant_ts::{is_ltz_timestamp, ltz_timestamp_type};
use crate::spark_string_timestamp::is_string_type;

pub use repark_common::udf_names::{TIMESTAMP_LTZ_CAST_NAME, TRY_TIMESTAMP_LTZ_CAST_NAME};

static TIMESTAMP_LTZ_CAST: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(SparkTimestampLtzCast::new(false))));
static TRY_TIMESTAMP_LTZ_CAST: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(SparkTimestampLtzCast::new(true))));

#[must_use]
pub fn timestamp_ltz_cast_udf(try_cast: bool) -> Arc<ScalarUDF> {
    if try_cast {
        Arc::clone(&TRY_TIMESTAMP_LTZ_CAST)
    } else {
        Arc::clone(&TIMESTAMP_LTZ_CAST)
    }
}

#[must_use]
pub(crate) fn timestamp_ltz_cast_expr(expr: Expr, try_cast: bool) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        timestamp_ltz_cast_udf(try_cast),
        vec![expr],
    ))
}

#[must_use]
pub(crate) fn unwrap_ltz_cast_marker(expr: &Expr) -> Option<(Expr, bool)> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if function.args.len() != 1 {
        return None;
    }
    let try_cast = match function.func.name() {
        TIMESTAMP_LTZ_CAST_NAME => false,
        TRY_TIMESTAMP_LTZ_CAST_NAME => true,
        _ => return None,
    };
    Some((function.args[0].clone(), try_cast))
}

pub(crate) fn retarget_dml_ltz_store_casts(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up(|node| match node {
        LogicalPlan::Dml(statement) => retarget_dml_ltz_input(statement),
        node => Ok(Transformed::no(node)),
    })
    .data()
}

fn retarget_dml_ltz_input(statement: DmlStatement) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Projection(projection) = statement.input.as_ref() else {
        return Ok(Transformed::no(LogicalPlan::Dml(statement)));
    };
    let targets = statement.target.schema();
    if projection.expr.len() != targets.fields().len() {
        return Ok(Transformed::no(LogicalPlan::Dml(statement)));
    }
    let mut schema = DFSchema::empty();
    schema.merge(projection.input.schema());
    let mut changed = false;
    let mut exprs = Vec::with_capacity(projection.expr.len());
    for (item, field) in projection.expr.iter().zip(targets.fields()) {
        if is_ltz_timestamp(field.data_type()) {
            let (rewritten, touched) = retarget_ltz_top(item.clone(), &schema);
            changed |= touched;
            exprs.push(rewritten);
        } else {
            exprs.push(item.clone());
        }
    }
    if !changed {
        return Ok(Transformed::no(LogicalPlan::Dml(statement)));
    }
    let rebuilt = Projection::try_new(exprs, Arc::clone(&projection.input))?;
    Ok(Transformed::yes(LogicalPlan::Dml(DmlStatement {
        input: Arc::new(LogicalPlan::Projection(rebuilt)),
        ..statement
    })))
}

fn retarget_ltz_top(expr: Expr, schema: &DFSchema) -> (Expr, bool) {
    match expr {
        Expr::Alias(alias) => {
            let (inner, touched) = retarget_ltz_top(*alias.expr, schema);
            (
                Expr::Alias(Alias {
                    expr: Box::new(inner),
                    ..alias
                }),
                touched,
            )
        }
        Expr::ScalarFunction(function)
            if function.func.name() == crate::decimal_cast::SPARK_NONNULL_NAME =>
        {
            let mut touched = false;
            let args = function
                .args
                .into_iter()
                .map(|arg| {
                    let (rewritten, hit) = retarget_ltz_top(arg, schema);
                    touched |= hit;
                    rewritten
                })
                .collect();
            (
                Expr::ScalarFunction(ScalarFunction::new_udf(Arc::clone(&function.func), args)),
                touched,
            )
        }
        other => retarget_ltz_leaf(other, schema),
    }
}

fn is_ltz_store_source(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Timestamp(_, None)
            | DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, Some(_))
            | DataType::Date32
            | DataType::Date64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Null
    )
}

fn retarget_ltz_leaf(expr: Expr, schema: &DFSchema) -> (Expr, bool) {
    if unwrap_ltz_cast_marker(&expr).is_some() {
        return (expr, false);
    }
    if let Expr::Cast(cast) = &expr
        && is_ltz_timestamp(cast.field.data_type())
        && cast
            .expr
            .get_type(schema)
            .is_ok_and(|source| is_ltz_store_source(&source))
    {
        return (timestamp_ltz_cast_expr((*cast.expr).clone(), false), true);
    }
    if expr
        .get_type(schema)
        .is_ok_and(|source| is_ltz_store_source(&source))
    {
        return (timestamp_ltz_cast_expr(expr, false), true);
    }
    (expr, false)
}

#[derive(Debug)]
struct SparkTimestampLtzCast {
    try_cast: bool,
    signature: Signature,
}

impl SparkTimestampLtzCast {
    fn new(try_cast: bool) -> Self {
        Self {
            try_cast,
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl PartialEq for SparkTimestampLtzCast {
    fn eq(&self, other: &Self) -> bool {
        self.try_cast == other.try_cast
    }
}

impl Eq for SparkTimestampLtzCast {}

impl Hash for SparkTimestampLtzCast {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkTimestampLtzCast {
    fn name(&self) -> &str {
        if self.try_cast {
            TRY_TIMESTAMP_LTZ_CAST_NAME
        } else {
            TIMESTAMP_LTZ_CAST_NAME
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(ltz_timestamp_type())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args.arg_fields.first().is_none_or(|field| {
            field.is_nullable() || self.try_cast || is_string_type(field.data_type())
        });
        Ok(Arc::new(Field::new(
            self.name(),
            ltz_timestamp_type(),
            nullable,
        )))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!(
            "'{}' must be rewritten by spark_ltz_timestamp_cast before execution",
            self.name()
        )
    }
}
