use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, ListArray, MapArray, RecordBatch, StructArray, new_null_array,
};
use datafusion::arrow::compute::{cast, cast_with_options};
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Float32Type, Float64Type, Schema,
};
use datafusion::common::config::ConfigOptions;
use datafusion::common::format::DEFAULT_CAST_OPTIONS;
use datafusion::common::{
    DFSchema, Result, ScalarValue, exec_datafusion_err, exec_err, internal_err, plan_err,
};
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::{
    ColumnarValue, EmptyRelation, Expr, LogicalPlan, Projection, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility, col,
};
use datafusion::optimizer::{Analyzer, AnalyzerRule};
use datafusion::physical_expr::create_physical_expr;

const LEAF: &str = "__repark_key_leaf__";
const LEAF_CAST: &str = "__repark_key_leaf_cast__";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum KeyPlan {
    Keep,
    Null,
    Leaf(Option<Expr>),
    Struct(Vec<KeyPlan>),
    List(Box<KeyPlan>),
    Map(Box<KeyPlan>),
}

pub(crate) struct LeafCaster {
    rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    config: ConfigOptions,
}

impl LeafCaster {
    pub(crate) fn new(config: &ConfigOptions) -> Result<Self> {
        let mut rules = crate::analyzer_rules_with_higher_order_preparation(Analyzer::new().rules)?;
        rules.extend(crate::analyzer_rules());
        Ok(Self {
            rules,
            config: config.clone(),
        })
    }

    pub(crate) fn plan(&self, from: &DataType, to: &DataType, normalise: bool) -> Result<KeyPlan> {
        if from == to && !(normalise && has_float(to)) {
            return Ok(KeyPlan::Keep);
        }
        match (from, to) {
            (DataType::Null, _) => Ok(KeyPlan::Null),
            (DataType::Struct(from_fields), DataType::Struct(to_fields)) => {
                if from_fields.len() != to_fields.len() {
                    return plan_err!("a struct key must have as many fields as the map key");
                }
                Ok(KeyPlan::Struct(
                    from_fields
                        .iter()
                        .zip(to_fields.iter())
                        .map(|(from_field, to_field)| {
                            self.plan(from_field.data_type(), to_field.data_type(), normalise)
                        })
                        .collect::<Result<Vec<_>>>()?,
                ))
            }
            (DataType::Map(from_entries, _), DataType::Map(to_entries, _)) => Ok(KeyPlan::Map(
                Box::new(self.plan(from_entries.data_type(), to_entries.data_type(), normalise)?),
            )),
            _ => {
                match (list_element(from), list_element(to)) {
                    (Some(from_element), Some(to_element)) => Ok(KeyPlan::List(Box::new(
                        self.plan(from_element.data_type(), to_element.data_type(), normalise)?,
                    ))),
                    (None, None) if from == to => Ok(KeyPlan::Leaf(None)),
                    (None, None) => Ok(KeyPlan::Leaf(Some(self.door_cast(from, to)?))),
                    _ => plan_err!("a map key and its lookup must nest the same way"),
                }
            }
        }
    }

    fn door_cast(&self, from: &DataType, to: &DataType) -> Result<Expr> {
        let leaf = col(LEAF);
        let written = if matches!(to, DataType::Timestamp(_, None)) {
            crate::timestamp_ntz_cast::timestamp_ntz_cast_expr(leaf, false)
        } else {
            Expr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(leaf),
                to.clone(),
            ))
        };
        let schema = DFSchema::try_from(Schema::new(vec![Field::new(LEAF, from.clone(), true)]))?;
        let mut plan = LogicalPlan::Projection(Projection::try_new(
            vec![written.alias(LEAF_CAST)],
            Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(schema),
            })),
        )?);
        for rule in &self.rules {
            plan = rule.analyze(plan, &self.config)?;
        }
        let LogicalPlan::Projection(projection) = plan else {
            return internal_err!("a key leaf cast must plan to one projection");
        };
        let Some(planned) = projection.expr.into_iter().next() else {
            return internal_err!("a key leaf cast must plan to one expression");
        };
        Ok(planned.unalias())
    }
}

fn has_float(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float32 | DataType::Float64 => true,
        DataType::Struct(fields) => fields.iter().any(|field| has_float(field.data_type())),
        DataType::Map(entries, _) => has_float(entries.data_type()),
        other => list_element(other).is_some_and(|element| has_float(element.data_type())),
    }
}

#[must_use]
pub(crate) fn key_by_position_udf(plan: KeyPlan) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(KeyByPosition {
        signature: Signature::any(2, Volatility::Immutable),
        plan,
    }))
}

#[derive(Debug)]
struct KeyByPosition {
    signature: Signature,
    plan: KeyPlan,
}

impl PartialEq for KeyByPosition {
    fn eq(&self, other: &Self) -> bool {
        self.plan == other.plan
    }
}

impl Eq for KeyByPosition {}

impl Hash for KeyByPosition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.plan.hash(state);
    }
}

impl ScalarUDFImpl for KeyByPosition {
    crate::shim_udf_boilerplate!("__repark_key_by_position__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [_, target] = arg_types else {
            return plan_err!("__repark_key_by_position__ takes a key and a typed target");
        };
        Ok(target.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(key) = args.args.first() else {
            return exec_err!("__repark_key_by_position__ takes a key and a typed target");
        };
        let target = args.return_field.data_type();
        let mut props = ExecutionProps::new();
        props.config_options = Some(Arc::clone(&args.config_options));
        match key {
            ColumnarValue::Scalar(value) => {
                let rebuilt = apply(&value.to_array_of_size(1)?, target, &self.plan, &props)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &rebuilt, 0,
                )?))
            }
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(apply(
                array, target, &self.plan, &props,
            )?)),
        }
    }
}

fn list_element(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => Some(element),
        _ => None,
    }
}

fn apply(
    array: &ArrayRef,
    to: &DataType,
    plan: &KeyPlan,
    props: &ExecutionProps,
) -> Result<ArrayRef> {
    match (plan, to) {
        (KeyPlan::Keep, _) => conform_leaf(Arc::clone(array), to),
        (KeyPlan::Null, _) => Ok(new_null_array(to, array.len())),
        (KeyPlan::Struct(plans), DataType::Struct(fields)) => {
            struct_by_position(array, fields, plans, props)
        }
        (KeyPlan::List(plan), DataType::List(field)) => list_by_position(array, field, plan, props),
        (KeyPlan::Map(plan), DataType::Map(entries, sorted)) => {
            map_by_position(array, entries, *sorted, plan, props)
        }
        (KeyPlan::Leaf(cast_expr), _) => {
            let casted = match cast_expr {
                Some(expr) => evaluate_leaf(array, expr, props)?,
                None => Arc::clone(array),
            };
            Ok(normalise_float(conform_leaf(casted, to)?))
        }
        _ => internal_err!("a key plan must follow the shape of its target type"),
    }
}

fn conform_leaf(array: ArrayRef, to: &DataType) -> Result<ArrayRef> {
    if array.data_type() == to {
        return Ok(array);
    }
    Ok(cast_with_options(&array, to, &DEFAULT_CAST_OPTIONS)?)
}

fn evaluate_leaf(array: &ArrayRef, expr: &Expr, props: &ExecutionProps) -> Result<ArrayRef> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        LEAF,
        array.data_type().clone(),
        true,
    )]));
    let df_schema = DFSchema::try_from(Arc::clone(&schema))?;
    let physical = create_physical_expr(expr, &df_schema, props)?;
    let batch = RecordBatch::try_new(schema, vec![Arc::clone(array)])?;
    physical.evaluate(&batch)?.into_array(array.len())
}

fn normalise_float(array: ArrayRef) -> ArrayRef {
    match array.data_type() {
        DataType::Float64 => Arc::new(array.as_primitive::<Float64Type>().unary::<_, Float64Type>(
            |value| {
                if value.is_nan() {
                    f64::NAN
                } else {
                    value + 0.0
                }
            },
        )),
        DataType::Float32 => Arc::new(array.as_primitive::<Float32Type>().unary::<_, Float32Type>(
            |value| {
                if value.is_nan() {
                    f32::NAN
                } else {
                    value + 0.0
                }
            },
        )),
        _ => array,
    }
}

fn struct_by_position(
    array: &ArrayRef,
    fields: &Fields,
    plans: &[KeyPlan],
    props: &ExecutionProps,
) -> Result<ArrayRef> {
    let source = array
        .as_struct_opt()
        .ok_or_else(|| exec_datafusion_err!("a struct key must be a struct array"))?;
    if source.num_columns() != fields.len() || plans.len() != fields.len() {
        return exec_err!("a struct key must have as many fields as the map key");
    }
    let columns = source
        .columns()
        .iter()
        .zip(fields.iter())
        .zip(plans)
        .map(|((column, field), plan)| apply(column, field.data_type(), plan, props))
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        columns,
        source.nulls().cloned(),
    )?))
}

fn list_by_position(
    array: &ArrayRef,
    field: &FieldRef,
    plan: &KeyPlan,
    props: &ExecutionProps,
) -> Result<ArrayRef> {
    let as_list = match array.data_type() {
        DataType::LargeList(element) | DataType::FixedSizeList(element, _) => cast(
            array,
            &DataType::List(Arc::new(Field::new_list_field(
                element.data_type().clone(),
                true,
            ))),
        )?,
        _ => Arc::clone(array),
    };
    let source = as_list
        .as_list_opt::<i32>()
        .ok_or_else(|| exec_datafusion_err!("an array key must be a list array"))?;
    let values = apply(source.values(), field.data_type(), plan, props)?;
    Ok(Arc::new(ListArray::try_new(
        Arc::clone(field),
        source.offsets().clone(),
        values,
        source.nulls().cloned(),
    )?))
}

fn map_by_position(
    array: &ArrayRef,
    entries: &FieldRef,
    sorted: bool,
    plan: &KeyPlan,
    props: &ExecutionProps,
) -> Result<ArrayRef> {
    let source = array
        .as_map_opt()
        .ok_or_else(|| exec_datafusion_err!("a map key must be a map array"))?;
    let rebuilt = apply(
        &(Arc::new(source.entries().clone()) as ArrayRef),
        entries.data_type(),
        plan,
        props,
    )?;
    let rebuilt = rebuilt
        .as_struct_opt()
        .ok_or_else(|| exec_datafusion_err!("map entries must be a struct array"))?
        .clone();
    Ok(Arc::new(MapArray::try_new(
        Arc::clone(entries),
        source.offsets().clone(),
        rebuilt,
        source.nulls().cloned(),
        sorted,
    )?))
}
